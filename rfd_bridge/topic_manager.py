"""Dynamic ROS publishers/subscribers and loop suppression."""

from __future__ import annotations

from collections import Counter
import hashlib
import threading
import time
from typing import Dict

from rclpy.qos import (
    DurabilityPolicy, qos_profile_sensor_data, QoSProfile, ReliabilityPolicy)
from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

from .config import BridgeConfig, TopicConfig


def qos_from_config(value, depth: int) -> QoSProfile:
    if isinstance(value, str):
        name = value.lower()
        if name == 'sensor_data':
            return qos_profile_sensor_data
        qos = QoSProfile(depth=depth)
        if name == 'best_effort':
            qos.reliability = ReliabilityPolicy.BEST_EFFORT
        elif name == 'reliable':
            qos.reliability = ReliabilityPolicy.RELIABLE
        elif name != 'default':
            raise ValueError(f'unknown QoS preset {value!r}')
        return qos
    if not isinstance(value, dict):
        raise ValueError('qos must be a preset string or mapping')
    qos = QoSProfile(depth=int(value.get('depth', depth)))
    reliability = str(value.get('reliability', 'reliable')).lower()
    qos.reliability = (ReliabilityPolicy.BEST_EFFORT if reliability == 'best_effort'
                       else ReliabilityPolicy.RELIABLE)
    durability = str(value.get('durability', 'volatile')).lower()
    qos.durability = (DurabilityPolicy.TRANSIENT_LOCAL
                      if durability == 'transient_local'
                      else DurabilityPolicy.VOLATILE)
    return qos


class EchoSuppressor:
    """Consumes fingerprints registered immediately before an RF publication."""

    def __init__(self, ttl_sec: float = 2.0):
        self.ttl = ttl_sec
        self._items = Counter()
        self._times = {}
        self._lock = threading.Lock()

    @staticmethod
    def key(payload: bytes) -> bytes:
        return hashlib.blake2s(payload, digest_size=16).digest()

    def register(self, payload: bytes) -> None:
        key = self.key(payload)
        with self._lock:
            self._items[key] += 1
            self._times[key] = time.monotonic()

    def consume(self, payload: bytes) -> bool:
        key = self.key(payload)
        now = time.monotonic()
        with self._lock:
            for old, stamp in list(self._times.items()):
                if now - stamp > self.ttl:
                    self._items.pop(old, None)
                    self._times.pop(old, None)
            if not self._items[key]:
                return False
            self._items[key] -= 1
            if not self._items[key]:
                self._items.pop(key, None)
                self._times.pop(key, None)
            return True


class TopicManager:
    def __init__(self, node, config: BridgeConfig, enqueue):
        self.node = node
        self.config = config
        self.enqueue = enqueue
        self.types = {t.topic_id: get_message(t.type) for t in config.topics}
        self.publishers = {}
        self.subscriptions = []
        self.suppressors: Dict[int, EchoSuppressor] = {}
        self.last_sent: Dict[int, float] = {}
        self.byte_windows: Dict[int, tuple] = {}
        self.rate_drops: Dict[int, int] = Counter()
        self.last_drop_warning: Dict[int, float] = {}
        for topic in config.topics:
            msg_type = self.types[topic.topic_id]
            qos = qos_from_config(topic.qos, topic.queue_depth)
            if topic.receives:
                self.publishers[topic.topic_id] = node.create_publisher(
                    msg_type, topic.remote_topic, qos)
            if topic.transmits:
                suppressor = EchoSuppressor()
                self.suppressors[topic.topic_id] = suppressor
                self.subscriptions.append(node.create_subscription(
                    msg_type, topic.ros_topic,
                    self._callback(topic, suppressor), qos))

    def _callback(self, topic: TopicConfig, suppressor: EchoSuppressor):
        def callback(message):
            payload = bytes(serialize_message(message))
            if topic.direction == 'bidirectional' and suppressor.consume(payload):
                return
            now = time.monotonic()
            last = self.last_sent.get(topic.topic_id, -1e30)
            if topic.max_rate_hz and now - last < 1.0 / topic.max_rate_hz:
                self.rate_drops[topic.topic_id] += 1
                self._warn_drop(topic, len(payload), 'topic rate limit')
                return
            if topic.max_bytes_per_sec:
                start, used = self.byte_windows.get(topic.topic_id, (now, 0))
                if now - start >= 1.0:
                    start, used = now, 0
                if used + len(payload) > topic.max_bytes_per_sec:
                    self.byte_windows[topic.topic_id] = (start, used)
                    self.rate_drops[topic.topic_id] += 1
                    self._warn_drop(topic, len(payload), 'topic byte-rate limit')
                    return
                self.byte_windows[topic.topic_id] = (start, used + len(payload))
            if self.enqueue(topic, payload):
                self.last_sent[topic.topic_id] = now
            else:
                self._warn_drop(topic, len(payload), 'bounded RF queue is full')
        return callback

    def _warn_drop(self, topic: TopicConfig, size: int, reason: str) -> None:
        now = time.monotonic()
        if now - self.last_drop_warning.get(topic.topic_id, -1e30) < 10.0:
            return
        self.last_drop_warning[topic.topic_id] = now
        budget = self.config.link.bandwidth_limit_bps / 8000.0
        self.node.get_logger().warn(
            f'Topic {topic.ros_topic} input is exceeding {reason}; dropping '
            f'according to {topic.drop_policy} policy (last message {size} B, '
            f'configured link budget {budget:.1f} kB/s)')

    def publish(self, topic_id: int, payload: bytes) -> None:
        message = deserialize_message(payload, self.types[topic_id])
        topic = next(t for t in self.config.topics if t.topic_id == topic_id)
        if topic.direction == 'bidirectional' and topic.ros_topic == topic.remote_topic:
            self.suppressors[topic_id].register(payload)
        self.publishers[topic_id].publish(message)
