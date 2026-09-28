from rfd_bridge.protocol import fragment_message
from rfd_bridge.reliability import (
    pack_ack, pack_hello, pack_ping, ReliabilityManager, unpack_ack,
    unpack_hello, unpack_ping)


def test_control_roundtrips():
    assert unpack_ack(pack_ack(2, 3)) == (False, 2, 3)
    assert unpack_ack(pack_ack(2, 3, True)) == (True, 2, 3)
    hello = unpack_hello(pack_hello(bytes(16), bytes(8)))
    assert hello[0] == 1 and hello[1] == bytes(16)
    assert unpack_ping(pack_ping(44)) == (False, 44)
    assert unpack_ping(pack_ping(44, True)) == (True, 44)


def test_ack_duplicate_and_rtt():
    manager = ReliabilityManager(ack_timeout_sec=1.0)
    frames = fragment_message(1, 2, b'hello', 100, True)
    assert manager.track(frames, now=0.0)
    assert manager.mark_fragment_sent(frames[0], now=0.0)
    assert manager.acknowledge(1, 2, now=0.25)
    assert not manager.acknowledge(1, 2, now=0.5)
    assert manager.acks == 1 and manager.duplicate_acks == 1
    assert manager.rtt_sum == 0.25


def test_retry_nack_and_exhaustion():
    manager = ReliabilityManager(ack_timeout_sec=1.0, max_retries=2)
    frames = fragment_message(1, 2, b'hello', 100, True)
    manager.track(frames, now=0.0)
    assert manager.due(now=10.0) == []  # timer is unarmed while queued
    manager.mark_fragment_sent(frames[0], now=0.0)
    assert manager.due(now=0.9) == []
    assert manager.due(now=1.0) == [frames]
    manager.mark_fragment_sent(frames[0], now=1.0)
    assert manager.nack(1, 2)
    assert manager.due(now=1.1) == [frames]
    manager.mark_fragment_sent(frames[0], now=1.1)
    assert manager.due(now=2.2) == []
    assert manager.exhausted == 1


def test_pending_is_bounded():
    manager = ReliabilityManager(max_pending=1)
    assert manager.track(fragment_message(1, 1, b'a', 10, True))
    assert not manager.track(fragment_message(1, 2, b'b', 10, True))
