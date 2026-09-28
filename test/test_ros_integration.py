import threading
import time

from geometry_msgs.msg import PoseStamped, Twist
import rclpy
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rfd_bridge.config import BridgeConfig, LinkConfig, SerialConfig, TopicConfig
from rfd_bridge.engine import BridgeEngine
from rfd_bridge.topic_manager import TopicManager
from rfd_bridge.transport import MemoryTransport
from sensor_msgs.msg import CompressedImage
from std_msgs.msg import String


TOPICS = [
    (1, '/it/source_string', '/it/dest_string', 'std_msgs/msg/String', String),
    (2, '/it/source_twist', '/it/dest_twist', 'geometry_msgs/msg/Twist', Twist),
    (3, '/it/source_pose', '/it/dest_pose',
     'geometry_msgs/msg/PoseStamped', PoseStamped),
    (4, '/it/source_image', '/it/dest_image',
     'sensor_msgs/msg/CompressedImage', CompressedImage),
]


def make_config(name, direction):
    topics = [TopicConfig(
        f'topic_{topic_id}', topic_id, source, dest, type_name, direction,
        priority=topic_id, reliability='reliable', queue_depth=2)
        for topic_id, source, dest, type_name, _ in TOPICS]
    link = LinkConfig(
        bandwidth_limit_bps=100_000_000, mtu_payload=256,
        max_message_size=100_000, max_queue_bytes=1_000_000,
        hello_interval_sec=0.02, ack_timeout_sec=0.2)
    return BridgeConfig(SerialConfig('memory'), link, topics, bridge_id=name)


def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_generic_ros_messages_end_to_end():
    rclpy.init()
    node_a = Node('rfd_integration_a')
    node_b = Node('rfd_integration_b')
    transport_a, transport_b = MemoryTransport(), MemoryTransport()
    transport_a.peer, transport_b.peer = transport_b, transport_a
    config_a, config_b = make_config('a', 'tx'), make_config('b', 'rx')
    engine_a = BridgeEngine(config_a, transport_a, lambda *_: None)
    engine_b = BridgeEngine(config_b, transport_b, lambda *_: None)
    TopicManager(node_a, config_a, engine_a.enqueue)
    manager_b = TopicManager(node_b, config_b, engine_b.enqueue)
    engine_b.on_message = manager_b.publish
    executor = MultiThreadedExecutor(num_threads=2)
    executor.add_node(node_a)
    executor.add_node(node_b)
    thread = threading.Thread(target=executor.spin, daemon=True)
    received = {}
    captures = []
    senders = []
    for topic_id, source, dest, _, msg_type in TOPICS:
        senders.append(node_a.create_publisher(msg_type, source, 10))
        captures.append(node_b.create_subscription(
            msg_type, dest,
            lambda msg, key=topic_id: received.__setitem__(key, msg), 10))
    engine_a.start()
    engine_b.start()
    thread.start()
    try:
        assert wait_for(lambda: engine_a.config_match is True)
        messages = [String(data='over RF CDR')]
        twist = Twist()
        twist.linear.x, twist.angular.z = 1.25, -0.5
        messages.append(twist)
        pose = PoseStamped()
        pose.header.frame_id = 'map'
        pose.pose.position.x = 4.5
        messages.append(pose)
        image = CompressedImage()
        image.format = 'jpeg'
        image.data = bytes(range(256)) * 195 + b'end'  # 49,923 bytes
        messages.append(image)
        assert wait_for(lambda: all(p.get_subscription_count() for p in senders))
        for publisher, message in zip(senders, messages):
            publisher.publish(message)
        assert wait_for(lambda: len(received) == 4)
        assert received[1].data == messages[0].data
        assert received[2].linear.x == messages[1].linear.x
        assert received[2].angular.z == messages[1].angular.z
        assert received[3].header.frame_id == 'map'
        assert received[3].pose.position.x == 4.5
        assert received[4].format == 'jpeg'
        assert bytes(received[4].data) == bytes(messages[3].data)
        assert engine_a.reliability.acks >= 4
    finally:
        engine_a.close()
        engine_b.close()
        executor.shutdown(timeout_sec=1.0)
        node_a.destroy_node()
        node_b.destroy_node()
        rclpy.shutdown()
