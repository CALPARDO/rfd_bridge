from rfd_bridge.protocol import Frame
from rfd_bridge.scheduler import FairScheduler, ScheduledMessage


def item(topic, priority, count=1, policy='oldest', stamp=0.0):
    frames = [Frame(topic, topic, i, count, bytes(100)) for i in range(count)]
    return ScheduledMessage(topic, priority, frames, stamp, policy)


def scheduler(limit=10_000_000, queue=1_000_000):
    result = FairScheduler(limit, queue, quantum_bytes=128,
                           burst_bytes=1_000_000)
    result.bucket.updated = 0.0
    result.bucket.tokens = 1_000_000
    return result


def test_high_priority_is_preferred_and_fragments_interleave():
    s = scheduler()
    s.enqueue(item(10, 10, count=4), 2)
    assert s.pop(now=1.0).topic_id == 10
    s.enqueue(item(1, 0), 2)
    assert s.pop(now=1.0).topic_id == 1


def test_low_priority_cannot_starve():
    s = scheduler()
    s.enqueue(item(9, 10, count=10), 2)
    s.enqueue(item(1, 0, count=100), 2)
    seen = [s.pop(now=1.0).topic_id for _ in range(30)]
    assert 9 in seen
    assert seen.count(1) > seen.count(9)


def test_latest_replaces_unsent_and_barely_started():
    s = scheduler()
    old = item(5, 5, count=10, policy='latest')
    s.enqueue(old, 1)
    s.pop(now=1.0)
    assert old.progress == 0.1
    s.enqueue(item(5, 5, policy='latest'), 1, replace_started_below=0.2)
    assert s.topic_depth(5) == 1
    assert s.dropped_messages == 1


def test_newest_and_never_drop_reject_when_full():
    for policy in ('newest', 'never-drop'):
        s = scheduler()
        assert s.enqueue(item(1, 1, policy=policy), 1)
        assert not s.enqueue(item(1, 1, policy=policy), 1)
        assert s.topic_depth(1) == 1


def test_global_queue_limit():
    s = scheduler(queue=200)
    assert s.enqueue(item(1, 1), 1)
    assert s.enqueue(item(2, 1), 1)
    assert s.queued_bytes <= 200
    assert s.dropped_messages == 1


def test_token_bucket_limits_wire_rate():
    s = FairScheduler(800, 1000, quantum_bytes=128, burst_bytes=120)
    s.bucket.updated = 0.0
    s.bucket.tokens = 0.0
    s.enqueue(item(1, 1), 1)
    assert s.pop(now=0.5) is None
    assert s.pop(now=2.0) is not None
