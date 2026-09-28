"""
RFD Bridge wire protocol v1.

The parser is deliberately allocation-bounded and advances at least one byte
on every rejected candidate, which makes arbitrary serial input safe.
"""

from __future__ import annotations

from dataclasses import dataclass
import struct
import time
from typing import Dict, List, Optional, Tuple
import zlib

MAGIC = b'RB'
PROTOCOL_VERSION = 1
CONTROL_TOPIC_ID = 0
FLAG_RELIABLE = 0x01
FLAG_RETRANSMIT = 0x02
KNOWN_FLAGS = FLAG_RELIABLE | FLAG_RETRANSMIT

# magic, version, flags, topic, message, fragment index/count, payload length
HEADER = struct.Struct('>2sBBHIHHH')
CRC = struct.Struct('>I')
HEADER_SIZE = HEADER.size
CRC_SIZE = CRC.size
FRAME_OVERHEAD = HEADER_SIZE + CRC_SIZE
MAX_WIRE_PAYLOAD = 4096
MAX_FRAGMENTS = 65535


class ProtocolError(ValueError):
    """A frame or message violates protocol limits."""


@dataclass(frozen=True)
class Frame:
    topic_id: int
    message_id: int
    fragment_index: int
    fragment_count: int
    payload: bytes
    flags: int = 0
    version: int = PROTOCOL_VERSION

    def validate(self, max_payload: int = MAX_WIRE_PAYLOAD) -> None:
        if self.version != PROTOCOL_VERSION:
            raise ProtocolError(f'unsupported protocol version {self.version}')
        if self.flags & ~KNOWN_FLAGS:
            raise ProtocolError(f'unknown flag bits 0x{self.flags:02x}')
        if not 0 <= self.topic_id <= 0xffff:
            raise ProtocolError('topic_id is outside uint16')
        if not 0 <= self.message_id <= 0xffffffff:
            raise ProtocolError('message_id is outside uint32')
        if not 1 <= self.fragment_count <= MAX_FRAGMENTS:
            raise ProtocolError('fragment_count must be 1..65535')
        if not 0 <= self.fragment_index < self.fragment_count:
            raise ProtocolError('fragment_index is outside fragment_count')
        if len(self.payload) > max_payload:
            raise ProtocolError('payload exceeds configured frame maximum')

    def pack(self, max_payload: int = MAX_WIRE_PAYLOAD) -> bytes:
        self.validate(max_payload)
        header = HEADER.pack(
            MAGIC, self.version, self.flags, self.topic_id, self.message_id,
            self.fragment_index, self.fragment_count, len(self.payload))
        body = header + self.payload
        return body + CRC.pack(zlib.crc32(body) & 0xffffffff)


def unpack_frame(data: bytes, max_payload: int = MAX_WIRE_PAYLOAD) -> Frame:
    if len(data) < FRAME_OVERHEAD:
        raise ProtocolError('truncated frame')
    values = HEADER.unpack_from(data)
    magic, version, flags, topic, message, index, count, length = values
    if magic != MAGIC:
        raise ProtocolError('bad magic')
    expected = HEADER_SIZE + length + CRC_SIZE
    if length > max_payload or len(data) != expected:
        raise ProtocolError('invalid frame length')
    wanted = CRC.unpack_from(data, expected - CRC_SIZE)[0]
    actual = zlib.crc32(data[:-CRC_SIZE]) & 0xffffffff
    if wanted != actual:
        raise ProtocolError('CRC mismatch')
    frame = Frame(topic, message, index, count,
                  data[HEADER_SIZE:-CRC_SIZE], flags, version)
    frame.validate(max_payload)
    return frame


class StreamParser:
    """Incrementally extracts frames from an arbitrary byte stream."""

    def __init__(self, max_payload: int = MAX_WIRE_PAYLOAD,
                 max_buffer: Optional[int] = None):
        if not 1 <= max_payload <= MAX_WIRE_PAYLOAD:
            raise ValueError(f'max_payload must be 1..{MAX_WIRE_PAYLOAD}')
        self.max_payload = max_payload
        self.max_buffer = max_buffer or max(8192, 4 * (max_payload + FRAME_OVERHEAD))
        self.buffer = bytearray()
        self.crc_failures = 0
        self.resync_count = 0
        self.invalid_headers = 0

    def feed(self, data: bytes) -> List[Frame]:
        if data:
            self.buffer.extend(data)
        if len(self.buffer) > self.max_buffer:
            # Retain enough tail to catch split magic and a current frame.
            del self.buffer[:len(self.buffer) - self.max_buffer]
            self.resync_count += 1
        result: List[Frame] = []
        while True:
            pos = self.buffer.find(MAGIC)
            if pos < 0:
                if self.buffer[-1:] == MAGIC[:1]:
                    if len(self.buffer) > 1:
                        self.resync_count += 1
                    del self.buffer[:-1]
                else:
                    if self.buffer:
                        self.resync_count += 1
                    self.buffer.clear()
                break
            if pos:
                del self.buffer[:pos]
                self.resync_count += 1
            if len(self.buffer) < HEADER_SIZE:
                break
            try:
                values = HEADER.unpack_from(self.buffer)
            except struct.error:
                break
            _, version, flags, _, _, index, count, length = values
            if (version != PROTOCOL_VERSION or flags & ~KNOWN_FLAGS or
                    length > self.max_payload or count == 0 or index >= count):
                del self.buffer[0]
                self.invalid_headers += 1
                self.resync_count += 1
                continue
            total = HEADER_SIZE + length + CRC_SIZE
            if len(self.buffer) < total:
                break
            candidate = bytes(self.buffer[:total])
            try:
                frame = unpack_frame(candidate, self.max_payload)
            except ProtocolError as exc:
                if 'CRC' in str(exc):
                    self.crc_failures += 1
                else:
                    self.invalid_headers += 1
                del self.buffer[0]
                self.resync_count += 1
                continue
            del self.buffer[:total]
            result.append(frame)
        return result


def fragment_message(topic_id: int, message_id: int, payload: bytes,
                     mtu_payload: int, reliable: bool = False) -> List[Frame]:
    if mtu_payload <= 0 or mtu_payload > MAX_WIRE_PAYLOAD:
        raise ValueError(f'mtu_payload must be 1..{MAX_WIRE_PAYLOAD}')
    count = max(1, (len(payload) + mtu_payload - 1) // mtu_payload)
    if count > MAX_FRAGMENTS:
        raise ProtocolError('message needs too many fragments')
    flags = FLAG_RELIABLE if reliable else 0
    return [Frame(topic_id, message_id, i, count,
                  payload[i * mtu_payload:(i + 1) * mtu_payload], flags)
            for i in range(count)]


@dataclass
class _Assembly:
    count: int
    fragments: Dict[int, bytes]
    created: float
    updated: float
    total_bytes: int = 0


class Reassembler:
    """Bounded, duplicate-tolerant and out-of-order message reassembly."""

    def __init__(self, max_message_size: int = 2_000_000,
                 timeout_sec: float = 10.0, max_inflight: int = 32,
                 completed_ttl_sec: float = 30.0):
        if max_message_size <= 0 or timeout_sec <= 0 or max_inflight <= 0:
            raise ValueError('reassembly limits must be positive')
        self.max_message_size = max_message_size
        self.timeout_sec = timeout_sec
        self.max_inflight = max_inflight
        self.completed_ttl_sec = completed_ttl_sec
        self._items: Dict[Tuple[int, int], _Assembly] = {}
        self._completed: Dict[Tuple[int, int], float] = {}
        self.duplicates = 0
        self.timeouts = 0
        self.rejected = 0
        self.reassembly_time_sum = 0.0
        self.reassembly_time_max = 0.0
        self.completed_count = 0

    def add(self, frame: Frame, now: Optional[float] = None) -> Optional[bytes]:
        now = time.monotonic() if now is None else now
        key = (frame.topic_id, frame.message_id)
        if key in self._completed:
            self.duplicates += 1
            return None
        item = self._items.get(key)
        if item is None:
            if len(self._items) >= self.max_inflight:
                oldest = min(self._items, key=lambda k: self._items[k].updated)
                del self._items[oldest]
                self.timeouts += 1
            item = _Assembly(frame.fragment_count, {}, now, now)
            self._items[key] = item
        elif item.count != frame.fragment_count:
            del self._items[key]
            self.rejected += 1
            return None
        if frame.fragment_index in item.fragments:
            self.duplicates += 1
            return None
        if item.total_bytes + len(frame.payload) > self.max_message_size:
            del self._items[key]
            self.rejected += 1
            return None
        item.fragments[frame.fragment_index] = frame.payload
        item.total_bytes += len(frame.payload)
        item.updated = now
        if len(item.fragments) != item.count:
            return None
        payload = b''.join(item.fragments[i] for i in range(item.count))
        del self._items[key]
        self._completed[key] = now
        elapsed = max(0.0, now - item.created)
        self.reassembly_time_sum += elapsed
        self.reassembly_time_max = max(self.reassembly_time_max, elapsed)
        self.completed_count += 1
        return payload

    def missing(self, topic_id: int, message_id: int) -> List[int]:
        item = self._items.get((topic_id, message_id))
        return [] if item is None else [i for i in range(item.count)
                                        if i not in item.fragments]

    def expire(self, now: Optional[float] = None) -> List[Tuple[int, int]]:
        now = time.monotonic() if now is None else now
        expired = [k for k, v in self._items.items()
                   if now - v.updated >= self.timeout_sec]
        for key in expired:
            del self._items[key]
        self.timeouts += len(expired)
        for key, stamp in list(self._completed.items()):
            if now - stamp >= self.completed_ttl_sec:
                del self._completed[key]
        return expired

    @property
    def inflight(self) -> int:
        return len(self._items)

    def is_completed(self, topic_id: int, message_id: int) -> bool:
        return (topic_id, message_id) in self._completed
