"""Low-overhead ROS diagnostic publication."""

from __future__ import annotations

import time

from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue


class DiagnosticsPublisher:
    def __init__(self, node, engine, topic_manager, topic: str, period: float):
        self.node = node
        self.engine = engine
        self.topic_manager = topic_manager
        self.publisher = node.create_publisher(DiagnosticArray, topic, 10)
        self.last_time = time.monotonic()
        self.last_tx = self.last_rx = 0
        self.timer = node.create_timer(period, self.publish)

    @staticmethod
    def _values(mapping):
        return [KeyValue(key=str(k), value=str(v)) for k, v in mapping.items()]

    def publish(self):
        e = self.engine
        now = time.monotonic()
        elapsed = max(1e-6, now - self.last_time)
        tx_rate = (e.bytes_tx - self.last_tx) / elapsed
        rx_rate = (e.bytes_rx - self.last_rx) / elapsed
        self.last_time, self.last_tx, self.last_rx = now, e.bytes_tx, e.bytes_rx
        state = e.link_state
        level = (DiagnosticStatus.OK if state == 'CONNECTED' else
                 DiagnosticStatus.WARN if state in ('CONNECTING', 'DEGRADED') else
                 DiagnosticStatus.ERROR)
        summary = DiagnosticStatus(
            level=level, name='rfd_bridge/link', hardware_id=e.config.serial.port,
            message=state)
        summary.values = self._values({
            'state': state, 'serial_connected': e.transport.connected,
            'config_match': e.config_match,
            'bytes_tx': e.bytes_tx, 'bytes_rx': e.bytes_rx,
            'payload_tx': e.payload_tx, 'payload_rx': e.payload_rx,
            'wire_overhead_tx': max(0, e.bytes_tx - e.frame_payload_tx),
            'wire_overhead_rx': max(0, e.bytes_rx - e.frame_payload_rx),
            'frames_tx': e.frames_tx, 'frames_rx': e.frames_rx,
            'tx_wire_bytes_per_sec': f'{tx_rate:.1f}',
            'rx_wire_bytes_per_sec': f'{rx_rate:.1f}',
            'crc_failures': e.parser.crc_failures,
            'parser_resyncs': e.parser.resync_count,
            'queue_messages': e.scheduler.message_count,
            'queue_bytes': e.scheduler.queued_bytes,
            'queue_latency_max_sec': f'{e.scheduler.queue_latency_max:.4f}',
            'queue_latency_avg_sec': (
                f'{e.scheduler.queue_latency_sum / e.scheduler.sent_fragments:.4f}'
                if e.scheduler.sent_fragments else '0.0000'),
            'dropped_messages': e.scheduler.dropped_messages,
            'reassembly_inflight': e.reassembler.inflight,
            'reassembly_timeouts': e.reassembler.timeouts,
            'duplicate_fragments': e.reassembler.duplicates,
            'reassembly_time_avg_sec': (
                f'{e.reassembler.reassembly_time_sum / e.reassembler.completed_count:.4f}'
                if e.reassembler.completed_count else '0.0000'),
            'reassembly_time_max_sec': f'{e.reassembler.reassembly_time_max:.4f}',
            'retransmissions': e.reliability.retransmissions,
            'retry_exhausted': e.reliability.exhausted,
            'acks': e.reliability.acks, 'nacks': e.reliability.nacks,
            'unknown_topic_frames': e.unknown_topics,
            'serial_reconnects': getattr(e.transport, 'reconnects', 0),
            'serial_partial_writes': getattr(e.transport, 'partial_writes', 0),
            'cts_block_events': getattr(e.transport, 'cts_block_events', 0),
        })
        statuses = [summary]
        for topic in e.config.topics:
            status = DiagnosticStatus(
                level=DiagnosticStatus.OK,
                name=f'rfd_bridge/topic/{topic.topic_id}',
                hardware_id=e.config.serial.port, message=topic.ros_topic)
            status.values = self._values({
                'type': topic.type, 'direction': topic.direction,
                'priority': topic.priority,
                'queued_messages': e.scheduler.topic_depth(topic.topic_id),
                'scheduler_drops': e.scheduler.topic_drops[topic.topic_id],
                'rate_limit_drops': self.topic_manager.rate_drops[topic.topic_id],
                'messages_tx': e.topic_messages_tx[topic.topic_id],
                'bytes_tx': e.topic_bytes_tx[topic.topic_id],
                'messages_rx': e.topic_messages_rx[topic.topic_id],
                'bytes_rx': e.topic_bytes_rx[topic.topic_id],
            })
            statuses.append(status)
        msg = DiagnosticArray()
        msg.header.stamp = self.node.get_clock().now().to_msg()
        msg.status = statuses
        self.publisher.publish(msg)
