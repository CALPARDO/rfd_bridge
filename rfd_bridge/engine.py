"""Transport-independent bridge protocol engine."""

from __future__ import annotations

from collections import defaultdict
import hashlib
import threading
import time
from typing import Callable, Dict, Optional

from .config import BridgeConfig, TopicConfig
from .protocol import (
    CONTROL_TOPIC_ID, FLAG_RELIABLE, fragment_message, Frame, Reassembler,
    StreamParser)
from .reliability import (
    CTRL_ACK, CTRL_HELLO, CTRL_KEEPALIVE, CTRL_NACK, CTRL_PING, CTRL_PONG,
    pack_ack, pack_hello, pack_ping, ReliabilityManager, unpack_ack,
    unpack_hello, unpack_ping)
from .scheduler import FairScheduler, ScheduledMessage
from .transport import Transport


class BridgeEngine:
    def __init__(self, config: BridgeConfig, transport: Transport,
                 on_message: Callable[[int, bytes], None],
                 on_log: Callable[[str, str], None] = lambda level, msg: None):
        self.config = config
        self.transport = transport
        self.on_message = on_message
        self.log = on_log
        self.topics: Dict[int, TopicConfig] = {t.topic_id: t for t in config.topics}
        self.parser = StreamParser(config.link.mtu_payload)
        self.reassembler = Reassembler(
            config.link.max_message_size, config.link.reassembly_timeout_sec,
            config.link.max_reassembly_messages)
        self.scheduler = FairScheduler(
            config.link.bandwidth_limit_bps, config.link.max_queue_bytes)
        self.reliability = ReliabilityManager(
            config.link.ack_timeout_sec, config.link.max_retries,
            config.link.max_reliable_pending)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._next_message_id = 1
        self._serial_connected = False
        self.remote_seen_at = 0.0
        self.remote_config_hash: Optional[bytes] = None
        self.remote_bridge_hash: Optional[bytes] = None
        self.config_match: Optional[bool] = None
        self.last_hello = 0.0
        self.last_expire = 0.0
        self.started_at = time.monotonic()
        self.bytes_tx = 0
        self.bytes_rx = 0
        self.payload_tx = 0
        self.payload_rx = 0
        self.frame_payload_tx = 0
        self.frame_payload_rx = 0
        self.frames_tx = 0
        self.frames_rx = 0
        self.unknown_topics = 0
        self.deserialize_errors = 0
        self.received_messages = 0
        self.sent_messages = 0
        self.control_tx = 0
        self.control_rx = 0
        self.ping_replies: Dict[int, float] = {}
        self.topic_messages_tx = defaultdict(int)
        self.topic_messages_rx = defaultdict(int)
        self.topic_bytes_tx = defaultdict(int)
        self.topic_bytes_rx = defaultdict(int)

    @property
    def bridge_hash(self) -> bytes:
        return hashlib.sha256(self.config.bridge_id.encode()).digest()[:8]

    @property
    def link_state(self) -> str:
        if not self._serial_connected:
            return 'DISCONNECTED'
        age = time.monotonic() - self.remote_seen_at if self.remote_seen_at else 1e30
        if self.config_match is False or age > self.config.link.link_timeout_sec:
            return 'DEGRADED' if self.remote_seen_at else 'CONNECTING'
        return 'CONNECTED'

    @property
    def user_data_allowed(self) -> bool:
        return not self.config.link.refuse_on_config_mismatch or self.config_match is True

    def start(self) -> None:
        self.transport.start(self.receive_bytes, self._transport_state)
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name='rfd-engine', daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        # Stop/cancel serial I/O before waiting for the sender; CTS can
        # otherwise hold a write while the node is trying to join the thread.
        self.transport.close()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)

    def _transport_state(self, connected: bool, detail: str) -> None:
        changed = connected != self._serial_connected
        self._serial_connected = connected
        if changed:
            self.log('info' if connected else 'warn',
                     f'serial {"connected" if connected else "disconnected"}: {detail}')

    def _new_id(self) -> int:
        value = self._next_message_id
        self._next_message_id = (value + 1) & 0xffffffff
        if self._next_message_id == 0:
            self._next_message_id = 1
        return value

    def enqueue(self, topic: TopicConfig, payload: bytes) -> bool:
        if len(payload) > self.config.link.max_message_size:
            self.log('warn', f'dropping topic {topic.ros_topic}: serialized message '
                             f'is {len(payload)} bytes (limit '
                             f'{self.config.link.max_message_size})')
            return False
        reliable = topic.reliability == 'reliable'
        with self._lock:
            message_id = self._new_id()
            frames = fragment_message(topic.topic_id, message_id, payload,
                                      self.config.link.mtu_payload, reliable)
            item = ScheduledMessage(
                topic.topic_id, topic.priority, frames, time.monotonic(),
                ('never-drop' if reliable else
                 'latest' if topic.reliability == 'latest'
                 else topic.drop_policy))
            if reliable and len(self.reliability.pending) >= self.reliability.max_pending:
                return False
            accepted = self.scheduler.enqueue(
                item, topic.queue_depth, topic.replace_started_below)
            if accepted and reliable:
                if not self.reliability.track(frames):
                    return False
            if accepted:
                self.payload_tx += len(payload)
                self.sent_messages += 1
                self.topic_messages_tx[topic.topic_id] += 1
                self.topic_bytes_tx[topic.topic_id] += len(payload)
            return accepted

    def _enqueue_control(self, payload: bytes) -> None:
        frames = fragment_message(CONTROL_TOPIC_ID, self._new_id(), payload,
                                  self.config.link.mtu_payload, False)
        self.scheduler.enqueue(ScheduledMessage(
            CONTROL_TOPIC_ID, -1, frames, time.monotonic(), 'oldest'), 32)
        self.control_tx += 1

    def send_ping(self, token: int) -> None:
        with self._lock:
            self.ping_replies[token] = 0.0
            self._enqueue_control(pack_ping(token))

    def receive_bytes(self, data: bytes) -> None:
        self.bytes_rx += len(data)
        with self._lock:
            for frame in self.parser.feed(data):
                self.frames_rx += 1
                self.frame_payload_rx += len(frame.payload)
                self.remote_seen_at = time.monotonic()
                if frame.topic_id == CONTROL_TOPIC_ID:
                    self._handle_control(frame.payload)
                    continue
                topic = self.topics.get(frame.topic_id)
                if topic is None or not topic.receives:
                    self.unknown_topics += 1
                    continue
                if not self.user_data_allowed:
                    continue
                was_complete = self.reassembler.is_completed(
                    frame.topic_id, frame.message_id)
                payload = self.reassembler.add(frame)
                if frame.flags & FLAG_RELIABLE and was_complete:
                    self._enqueue_control(pack_ack(frame.topic_id, frame.message_id))
                if payload is None:
                    continue
                self.payload_rx += len(payload)
                self.received_messages += 1
                self.topic_messages_rx[frame.topic_id] += 1
                self.topic_bytes_rx[frame.topic_id] += len(payload)
                if frame.flags & FLAG_RELIABLE:
                    self._enqueue_control(pack_ack(frame.topic_id, frame.message_id))
                try:
                    self.on_message(frame.topic_id, payload)
                except Exception as exc:  # keep malformed application data off I/O thread
                    self.deserialize_errors += 1
                    self.log('error', f'cannot publish topic ID {frame.topic_id}: {exc}')

    def _handle_control(self, payload: bytes) -> None:
        if not payload:
            return
        self.control_rx += 1
        kind = payload[0]
        try:
            if kind == CTRL_HELLO:
                version, config_hash, bridge_hash, _ = unpack_hello(payload)
                self.remote_config_hash = config_hash
                self.remote_bridge_hash = bridge_hash
                matched = version == 1 and config_hash == self.config.topic_hash()
                if self.config_match is not matched:
                    self.config_match = matched
                    if matched:
                        self.log('info', 'remote bridge topic map is compatible')
                    else:
                        self.log('error', 'remote topic map does not match local '
                                 f'configuration (local='
                                 f'{self.config.topic_hash().hex()}, remote='
                                 f'{config_hash.hex()}); user data is blocked')
            elif kind in (CTRL_ACK, CTRL_NACK):
                nack, topic, message = unpack_ack(payload)
                if nack:
                    self.reliability.nack(topic, message)
                else:
                    self.reliability.acknowledge(topic, message)
            elif kind in (CTRL_PING, CTRL_PONG):
                pong, token = unpack_ping(payload)
                if pong:
                    self.ping_replies[token] = time.monotonic()
                else:
                    self._enqueue_control(pack_ping(token, pong=True))
            elif kind == CTRL_KEEPALIVE:
                pass
        except (ValueError, TypeError) as exc:
            self.log('warn', f'invalid control frame: {exc}')

    def _run(self) -> None:
        while not self._stop.is_set():
            now = time.monotonic()
            with self._lock:
                if now - self.last_hello >= self.config.link.hello_interval_sec:
                    self._enqueue_control(pack_hello(
                        self.config.topic_hash(), self.bridge_hash))
                    self.last_hello = now
                if now - self.last_expire >= 0.5:
                    for topic_id, message_id in self.reassembler.expire(now):
                        self._enqueue_control(pack_ack(topic_id, message_id, nack=True))
                    self.last_expire = now
                for frames in (self.reliability.due(now)
                               if self.user_data_allowed else []):
                    retransmit = [Frame(
                        f.topic_id, f.message_id, f.fragment_index,
                        f.fragment_count, f.payload, f.flags | 0x02) for f in frames]
                    topic = self.topics[frames[0].topic_id]
                    accepted = self.scheduler.enqueue(ScheduledMessage(
                        topic.topic_id, topic.priority, retransmit, now,
                        'never-drop'), topic.queue_depth + 1)
                    if not accepted:
                        self.reliability.defer(
                            topic.topic_id, frames[0].message_id, now)
                frame = (self.scheduler.pop(now) if self.user_data_allowed
                         else self._pop_control(now))
            if frame is None:
                self._stop.wait(0.002)
                continue
            wire = frame.pack(self.config.link.mtu_payload)
            if self.transport.write(wire):
                self.bytes_tx += len(wire)
                self.frame_payload_tx += len(frame.payload)
                self.frames_tx += 1
                if frame.flags & FLAG_RELIABLE:
                    with self._lock:
                        self.reliability.mark_fragment_sent(frame)
            elif frame.flags & FLAG_RELIABLE:
                with self._lock:
                    self.reliability.fail_attempt(
                        frame.topic_id, frame.message_id)

    def _pop_control(self, now: float) -> Optional[Frame]:
        # A mismatch/unknown peer blocks user bytes, but HELLO/ACK/PING must flow.
        queue = self.scheduler._queues.get(-1)  # internal fast path under lock
        if not queue:
            return None
        # scheduler sees only control by temporarily selecting the same public pop
        user = {p: q for p, q in self.scheduler._queues.items() if p >= 0}
        for p in user:
            self.scheduler._queues[p], user[p] = type(user[p])(), self.scheduler._queues[p]
        try:
            return self.scheduler.pop(now)
        finally:
            for p, q in user.items():
                self.scheduler._queues[p] = q
