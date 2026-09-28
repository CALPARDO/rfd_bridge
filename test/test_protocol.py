import os
import struct

import pytest

from rfd_bridge.protocol import (
    fragment_message, Frame, HEADER_SIZE, ProtocolError, StreamParser)


def test_pack_unpack_through_stream():
    frame = Frame(42, 99, 0, 1, b'\x00binary\xff')
    assert StreamParser(256).feed(frame.pack(256)) == [frame]


def test_partial_magic_and_packet():
    wire = Frame(1, 2, 0, 1, b'abc').pack(256)
    parser = StreamParser(256)
    assert parser.feed(b'garbageR') == []
    assert parser.feed(wire[1:8]) == []
    assert parser.feed(wire[8:]) == [Frame(1, 2, 0, 1, b'abc')]
    assert parser.resync_count >= 1


def test_multiple_packets_one_read():
    frames = [Frame(1, n, 0, 1, bytes([n])) for n in range(4)]
    assert StreamParser(256).feed(b''.join(f.pack(256) for f in frames)) == frames


def test_crc_corruption_recovers_next_frame():
    first = bytearray(Frame(1, 1, 0, 1, b'bad').pack(256))
    first[-1] ^= 0x80
    good = Frame(1, 2, 0, 1, b'good')
    parser = StreamParser(256)
    assert parser.feed(bytes(first) + good.pack(256)) == [good]
    assert parser.crc_failures == 1


@pytest.mark.parametrize('offset', [2, 3, 10, 15])
def test_random_byte_corruption_is_rejected(offset):
    wire = bytearray(Frame(1, 1, 0, 1, os.urandom(40)).pack(256))
    wire[offset] ^= 1
    assert StreamParser(256).feed(wire) == []


def test_oversized_or_corrupt_length_does_not_allocate_or_stall():
    corrupt = bytearray(Frame(1, 1, 0, 1, b'x').pack(256))
    # Payload length is the final uint16 of the header.
    corrupt[HEADER_SIZE - 2:HEADER_SIZE] = struct.pack('>H', 4096)
    good = Frame(1, 2, 0, 1, b'ok')
    parser = StreamParser(256)
    assert parser.feed(corrupt + good.pack(256)) == [good]
    assert parser.invalid_headers >= 1


def test_invalid_version_recovers():
    bad = bytearray(Frame(1, 1, 0, 1, b'x').pack(256))
    bad[2] = 99
    good = Frame(1, 2, 0, 1, b'y')
    parser = StreamParser(256)
    assert parser.feed(bad + good.pack(256)) == [good]


def test_frame_validation():
    with pytest.raises(ProtocolError):
        Frame(1, 1, 2, 2, b'bad').pack(256)
    with pytest.raises(ValueError):
        fragment_message(1, 1, b'x', 0)


def test_empty_message_has_one_fragment():
    frames = fragment_message(1, 2, b'', 256)
    assert len(frames) == 1 and frames[0].payload == b''
