"""Whole-message acknowledgement and bounded retry state."""

from __future__ import annotations

from dataclasses import dataclass
import struct
import time
from typing import Dict, List, Optional, Set, Tuple

from .protocol import Frame, PROTOCOL_VERSION

CTRL_HELLO = 1
CTRL_KEEPALIVE = 2
CTRL_ACK = 3
CTRL_NACK = 4
CTRL_PING = 5
CTRL_PONG = 6

_ACK = struct.Struct('>BHI')
_PING = struct.Struct('>BQ')
_HELLO = struct.Struct('>BB16s8sH')


def pack_ack(topic_id: int, message_id: int, nack: bool = False) -> bytes:
    return _ACK.pack(CTRL_NACK if nack else CTRL_ACK, topic_id, message_id)


def unpack_ack(payload: bytes) -> Tuple[bool, int, int]:
    if len(payload) != _ACK.size:
        raise ValueError('invalid ACK size')
    kind, topic, message = _ACK.unpack(payload)
    if kind not in (CTRL_ACK, CTRL_NACK):
        raise ValueError('not an ACK/NACK')
    return kind == CTRL_NACK, topic, message


def pack_hello(config_hash: bytes, bridge_hash: bytes, capabilities: int = 1) -> bytes:
    if len(config_hash) != 16 or len(bridge_hash) != 8:
        raise ValueError('HELLO hash widths are fixed')
    return _HELLO.pack(CTRL_HELLO, PROTOCOL_VERSION, config_hash,
                       bridge_hash, capabilities)


def unpack_hello(payload: bytes):
    if len(payload) != _HELLO.size:
        raise ValueError('invalid HELLO size')
    kind, version, config_hash, bridge_hash, capabilities = _HELLO.unpack(payload)
    if kind != CTRL_HELLO:
        raise ValueError('not a HELLO')
    return version, config_hash, bridge_hash, capabilities


def pack_ping(token: int, pong: bool = False) -> bytes:
    return _PING.pack(CTRL_PONG if pong else CTRL_PING, token)


def unpack_ping(payload: bytes) -> Tuple[bool, int]:
    if len(payload) != _PING.size:
        raise ValueError('invalid PING size')
    kind, token = _PING.unpack(payload)
    if kind not in (CTRL_PING, CTRL_PONG):
        raise ValueError('not PING/PONG')
    return kind == CTRL_PONG, token


@dataclass
class Pending:
    frames: List[Frame]
    sent_at: Optional[float] = None
    retries: int = 0
    sent_fragments: Set[int] = None

    def __post_init__(self):
        if self.sent_fragments is None:
            self.sent_fragments = set()


class ReliabilityManager:
    def __init__(self, ack_timeout_sec: float = 2.0, max_retries: int = 3,
                 max_pending: int = 32):
        if ack_timeout_sec <= 0 or max_retries < 0 or max_pending <= 0:
            raise ValueError('invalid reliability limits')
        self.ack_timeout_sec = ack_timeout_sec
        self.max_retries = max_retries
        self.max_pending = max_pending
        self.pending: Dict[Tuple[int, int], Pending] = {}
        self.retransmissions = 0
        self.exhausted = 0
        self.acks = 0
        self.nacks = 0
        self.duplicate_acks = 0
        self.rtt_sum = 0.0
        self.rtt_count = 0

    def track(self, frames: List[Frame], now: Optional[float] = None) -> bool:
        if not frames:
            return False
        key = (frames[0].topic_id, frames[0].message_id)
        if key not in self.pending and len(self.pending) >= self.max_pending:
            return False
        self.pending[key] = Pending(frames)
        return True

    def mark_fragment_sent(self, frame: Frame,
                           now: Optional[float] = None) -> bool:
        item = self.pending.get((frame.topic_id, frame.message_id))
        if item is None:
            return False
        item.sent_fragments.add(frame.fragment_index)
        if len(item.sent_fragments) == len(item.frames):
            item.sent_at = time.monotonic() if now is None else now
            item.sent_fragments.clear()
            return True
        return False

    def fail_attempt(self, topic_id: int, message_id: int) -> bool:
        item = self.pending.get((topic_id, message_id))
        if item is None:
            return False
        item.sent_fragments.clear()
        item.sent_at = 0.0
        return True

    def defer(self, topic_id: int, message_id: int,
              now: Optional[float] = None) -> bool:
        item = self.pending.get((topic_id, message_id))
        if item is None:
            return False
        item.sent_at = time.monotonic() if now is None else now
        return True

    def acknowledge(self, topic_id: int, message_id: int,
                    now: Optional[float] = None) -> bool:
        item = self.pending.pop((topic_id, message_id), None)
        if item is None:
            self.duplicate_acks += 1
            return False
        now = time.monotonic() if now is None else now
        self.acks += 1
        if item.sent_at is not None:
            self.rtt_sum += max(0.0, now - item.sent_at)
            self.rtt_count += 1
        return True

    def nack(self, topic_id: int, message_id: int) -> bool:
        item = self.pending.get((topic_id, message_id))
        if item is None:
            return False
        item.sent_fragments.clear()
        item.sent_at = 0.0
        self.nacks += 1
        return True

    def due(self, now: Optional[float] = None) -> List[List[Frame]]:
        now = time.monotonic() if now is None else now
        result: List[List[Frame]] = []
        for key, item in list(self.pending.items()):
            if item.sent_at is None:
                continue
            if now - item.sent_at < self.ack_timeout_sec:
                continue
            if item.retries >= self.max_retries:
                del self.pending[key]
                self.exhausted += 1
                continue
            item.retries += 1
            item.sent_at = None
            item.sent_fragments.clear()
            self.retransmissions += 1
            result.append(item.frames)
        return result
