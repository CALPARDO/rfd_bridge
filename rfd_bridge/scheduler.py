"""Bounded weighted-deficit scheduler and wire-rate token bucket."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import time
from typing import Deque, Dict, Iterable, List, Optional

from .protocol import Frame, FRAME_OVERHEAD


@dataclass
class ScheduledMessage:
    topic_id: int
    priority: int
    frames: List[Frame]
    enqueued_at: float
    drop_policy: str = 'oldest'
    next_index: int = 0

    @property
    def remaining_bytes(self) -> int:
        return sum(FRAME_OVERHEAD + len(f.payload)
                   for f in self.frames[self.next_index:])

    @property
    def progress(self) -> float:
        return self.next_index / len(self.frames)

    def take(self) -> Frame:
        frame = self.frames[self.next_index]
        self.next_index += 1
        return frame

    @property
    def done(self) -> bool:
        return self.next_index >= len(self.frames)


class TokenBucket:
    def __init__(self, rate_bytes_sec: float, burst_bytes: int):
        if rate_bytes_sec <= 0 or burst_bytes <= 0:
            raise ValueError('token bucket rate and burst must be positive')
        self.rate = float(rate_bytes_sec)
        self.capacity = float(burst_bytes)
        self.tokens = float(burst_bytes)
        self.updated = time.monotonic()

    def available(self, amount: int, now: Optional[float] = None) -> bool:
        now = time.monotonic() if now is None else now
        elapsed = max(0.0, now - self.updated)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.updated = now
        if self.tokens < amount:
            return False
        self.tokens -= amount
        return True


class FairScheduler:
    """
    Weighted deficit round robin, rotating after every fragment.

    Higher-priority queues receive more byte credit per round, while every
    non-empty priority receives credit. This bounds starvation without allowing
    a large low-priority message to block control traffic.
    """

    def __init__(self, bandwidth_limit_bps: int, max_queue_bytes: int,
                 quantum_bytes: int = 512, burst_bytes: Optional[int] = None):
        if bandwidth_limit_bps <= 0 or max_queue_bytes <= 0 or quantum_bytes <= 0:
            raise ValueError('scheduler limits must be positive')
        self.max_queue_bytes = max_queue_bytes
        self.quantum = quantum_bytes
        self.bucket = TokenBucket(
            bandwidth_limit_bps / 8.0,
            burst_bytes or max(quantum_bytes * 4, bandwidth_limit_bps // 40))
        self._queues: Dict[int, Deque[ScheduledMessage]] = defaultdict(deque)
        self._deficit: Dict[int, int] = defaultdict(int)
        self._cursor = 0
        self.queued_bytes = 0
        self.dropped_messages = 0
        self.sent_fragments = 0
        self.queue_latency_sum = 0.0
        self.queue_latency_max = 0.0
        self.topic_drops: Dict[int, int] = defaultdict(int)

    @staticmethod
    def _weight(priority: int) -> int:
        # Control priority -1 gets 16 shares. User priorities taper to one.
        return 16 if priority < 0 else max(1, 8 - min(priority, 7))

    def _all_messages(self) -> Iterable[ScheduledMessage]:
        for queue in self._queues.values():
            yield from queue

    def topic_depth(self, topic_id: int) -> int:
        return sum(1 for item in self._all_messages()
                   if item.topic_id == topic_id)

    def _drop_item(self, item: ScheduledMessage) -> None:
        self.queued_bytes -= item.remaining_bytes
        self.dropped_messages += 1
        self.topic_drops[item.topic_id] += 1

    def _remove(self, item: ScheduledMessage) -> None:
        queue = self._queues[item.priority]
        queue.remove(item)
        self._drop_item(item)

    def enqueue(self, item: ScheduledMessage, queue_depth: int = 1,
                replace_started_below: float = 0.0) -> bool:
        if not item.frames or queue_depth <= 0:
            raise ValueError('message must have frames and positive queue depth')
        policy = item.drop_policy.replace('_', '-').lower()
        if policy == 'latest-only':
            policy = 'latest'
        if policy not in ('oldest', 'newest', 'latest', 'never-drop'):
            raise ValueError(f'unknown drop policy {item.drop_policy}')
        existing = [x for x in self._all_messages()
                    if x.topic_id == item.topic_id]
        if policy == 'latest':
            for old in existing:
                if old.next_index == 0 or old.progress <= replace_started_below:
                    self._remove(old)
            existing = [x for x in self._all_messages()
                        if x.topic_id == item.topic_id]
        while len(existing) >= queue_depth:
            if policy in ('newest', 'never-drop'):
                self.dropped_messages += 1
                self.topic_drops[item.topic_id] += 1
                return False
            self._remove(existing.pop(0))
        needed = item.remaining_bytes
        if needed > self.max_queue_bytes:
            self.dropped_messages += 1
            self.topic_drops[item.topic_id] += 1
            return False
        # Shed oldest droppable traffic globally, never allocating unboundedly.
        while self.queued_bytes + needed > self.max_queue_bytes:
            candidates = [x for x in self._all_messages()
                          if x.drop_policy != 'never-drop']
            if not candidates:
                self.dropped_messages += 1
                self.topic_drops[item.topic_id] += 1
                return False
            victim = min(candidates, key=lambda x: x.enqueued_at)
            self._remove(victim)
        self._queues[item.priority].append(item)
        self.queued_bytes += needed
        return True

    def pop(self, now: Optional[float] = None) -> Optional[Frame]:
        now = time.monotonic() if now is None else now
        priorities = sorted(p for p, q in self._queues.items() if q)
        if not priorities:
            return None
        self._cursor %= len(priorities)
        # Up to two complete passes permits newly credited queues to transmit.
        for offset in range(len(priorities) * 2):
            idx = (self._cursor + offset) % len(priorities)
            priority = priorities[idx]
            queue = self._queues[priority]
            if not queue:
                continue
            self._deficit[priority] += self.quantum * self._weight(priority)
            item = queue[0]
            frame = item.frames[item.next_index]
            cost = FRAME_OVERHEAD + len(frame.payload)
            if self._deficit[priority] < cost:
                continue
            if not self.bucket.available(cost, now):
                return None
            self._deficit[priority] -= cost
            queue.popleft()
            frame = item.take()
            self.queued_bytes -= cost
            latency = max(0.0, now - item.enqueued_at)
            self.queue_latency_sum += latency
            self.queue_latency_max = max(self.queue_latency_max, latency)
            self.sent_fragments += 1
            if not item.done:
                queue.append(item)  # fragment-level interleaving
            self._cursor = (idx + 1) % max(1, len(priorities))
            return frame
        return None

    @property
    def message_count(self) -> int:
        return sum(len(q) for q in self._queues.values())
