"""ROS 2 node entry point."""

from __future__ import annotations

import os

import rclpy
from rclpy.node import Node

from .config import ConfigError, load_config
from .diagnostics import DiagnosticsPublisher
from .engine import BridgeEngine
from .topic_manager import TopicManager
from .transport import SerialTransport


class RfdBridgeNode(Node):
    def __init__(self):
        super().__init__('rfd_bridge')
        self.declare_parameter('config_file', '')
        path = os.path.expanduser(
            self.get_parameter('config_file').get_parameter_value().string_value)
        if not path:
            raise ConfigError('parameter config_file is required')
        self.config = load_config(path)
        if not os.path.exists(self.config.serial.port):
            self.get_logger().warn(
                f'Serial device {self.config.serial.port} does not currently '
                'exist; automatic reconnect will keep trying')
        self.transport = SerialTransport(self.config.serial)
        self.topic_manager = None
        self.engine = BridgeEngine(
            self.config, self.transport, self._receive_message, self._engine_log)
        self.topic_manager = TopicManager(self, self.config, self.engine.enqueue)
        self.diagnostics = DiagnosticsPublisher(
            self, self.engine, self.topic_manager, self.config.diagnostics_topic,
            self.config.diagnostics_period_sec)
        self._log_summary()
        self.engine.start()

    def _receive_message(self, topic_id, payload):
        self.topic_manager.publish(topic_id, payload)

    def _engine_log(self, level, message):
        getattr(self.get_logger(), level)(message)

    def _log_summary(self):
        c = self.config
        rows = [
            'RFD Bridge',
            f'Serial: {c.serial.port} @ {c.serial.baud}, '
            f'RTS/CTS={"on" if c.serial.rtscts else "off"}',
            f'Bandwidth budget: {c.link.bandwidth_limit_bps / 8000:.2f} kB/s',
            f'Protocol: v1; topic map: {c.topic_hash().hex()}',
            f'Configured topics: {len(c.topics)}',
            'ID  Direction      Pri  Rate       Type / local -> remote',
        ]
        for t in c.topics:
            rate = 'unlimited' if not t.max_rate_hz else f'{t.max_rate_hz:g} Hz'
            rows.append(f'{t.topic_id:<3} {t.direction:<14} {t.priority:<4} '
                        f'{rate:<10} {t.type} {t.ros_topic} -> {t.remote_topic}')
        self.get_logger().info('\n'.join(rows))

    def destroy_node(self):
        if hasattr(self, 'engine'):
            self.engine.close()
        return super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = None
    try:
        node = RfdBridgeNode()
        rclpy.spin(node)
    except (ConfigError, ValueError) as exc:
        if node:
            node.get_logger().error(str(exc))
        else:
            print(f'rfd_bridge configuration error: {exc}')
        raise SystemExit(2)
    except KeyboardInterrupt:
        pass
    finally:
        if node:
            try:
                node.destroy_node()
            except KeyboardInterrupt:
                # ros2 launch forwards SIGINT to children while teardown may
                # already be in progress. The engine transport has closed.
                pass
        try:
            if rclpy.ok():
                rclpy.shutdown()
        except KeyboardInterrupt:
            pass
