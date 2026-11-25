import time

from rfd_bridge.config import BridgeConfig, LinkConfig, SerialConfig, TopicConfig
from rfd_bridge.engine import BridgeEngine
from rfd_bridge.transport import MemoryTransport


def config(bridge_id, direction):
    return BridgeConfig(
        SerialConfig('memory'),
        LinkConfig(bandwidth_limit_bps=1_000_000, hello_interval_sec=0.02,
                   link_timeout_sec=1.0),
        [TopicConfig('test', 1, '/in', '/out', 'std_msgs/msg/String',
                     direction, reliability='reliable', queue_depth=2)],
        bridge_id=bridge_id)


def wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return False


def test_memory_transport_end_to_end_and_ack():
    ta, tb = MemoryTransport(), MemoryTransport()
    ta.peer, tb.peer = tb, ta
    received = []
    a = BridgeEngine(config('a', 'tx'), ta, lambda *_: None)
    b = BridgeEngine(config('b', 'rx'), tb, lambda topic, data: received.append(data))
    a.start()
    b.start()
    try:
        assert wait_for(lambda: a.config_match is True and b.config_match is True)
        assert a.enqueue(a.config.topics[0], b'cdr bytes')
        assert wait_for(lambda: received == [b'cdr bytes'])
        assert wait_for(lambda: not a.reliability.pending)
    finally:
        a.close()
        b.close()


def test_mismatched_map_blocks_user_data():
    ta, tb = MemoryTransport(), MemoryTransport()
    ta.peer, tb.peer = tb, ta
    ca, cb = config('a', 'tx'), config('b', 'rx')
    cb.topics[0].type = 'geometry_msgs/msg/Twist'
    received = []
    a = BridgeEngine(ca, ta, lambda *_: None)
    b = BridgeEngine(cb, tb, lambda _, data: received.append(data))
    a.start()
    b.start()
    try:
        assert wait_for(lambda: a.config_match is False)
        a.enqueue(a.config.topics[0], b'unsafe')
        time.sleep(0.05)
        assert received == []
    finally:
        a.close()
        b.close()


def test_peer_restart_does_not_drop_first_fresh_message(monkeypatch):
    from rfd_bridge.protocol import fragment_message
    starts = iter((1234, 9000, 19000))
    monkeypatch.setattr('secrets.randbelow', lambda limit: next(starts))
    received = []
    receiver = BridgeEngine(config('b', 'rx'), MemoryTransport(),
                            lambda _, data: received.append(data))
    receiver.config_match = True
    for payload in (b'first', b'after restart'):
        sender = BridgeEngine(config('a', 'tx'), MemoryTransport(),
                              lambda *_: None)
        frame, = fragment_message(1, sender._new_id(), payload, 256, True)
        receiver.receive_bytes(frame.pack())
        receiver.receive_bytes(frame.pack())
    assert received == [b'first', b'after restart']
