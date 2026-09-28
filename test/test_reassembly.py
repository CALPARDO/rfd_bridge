import os

from rfd_bridge.protocol import fragment_message, Reassembler


def test_tiny_and_exact_fragment():
    for payload in (b'x', b'x' * 256):
        frame = fragment_message(1, 1, payload, 256)[0]
        assert Reassembler().add(frame, now=0.0) == payload


def test_large_out_of_order_and_duplicate():
    payload = os.urandom(50_000)
    frames = fragment_message(4, 77, payload, 256)
    receiver = Reassembler(max_message_size=60_000)
    assert receiver.add(frames[-1], now=0.0) is None
    assert receiver.add(frames[-1], now=0.1) is None
    result = None
    for frame in reversed(frames[:-1]):
        value = receiver.add(frame, now=0.2)
        if value is not None:
            result = value
    assert result == payload
    assert receiver.duplicates == 1


def test_missing_fragment_times_out():
    frames = fragment_message(1, 2, b'x' * 1000, 100)
    receiver = Reassembler(timeout_sec=1.0)
    for frame in frames[:-1]:
        assert receiver.add(frame, now=0.0) is None
    assert receiver.expire(now=1.1) == [(1, 2)]
    assert receiver.timeouts == 1


def test_message_size_and_inflight_are_bounded():
    receiver = Reassembler(max_message_size=10, max_inflight=1)
    frames = fragment_message(1, 1, b'x' * 20, 8)
    receiver.add(frames[0], now=0.0)
    receiver.add(frames[1], now=0.0)
    assert receiver.rejected == 1
    receiver.add(fragment_message(1, 2, b'a' * 2, 8)[0], now=0.0)
    receiver.add(fragment_message(2, 2, b'b' * 2, 8)[0], now=0.0)
    assert receiver.inflight == 0  # both one-fragment messages completed
