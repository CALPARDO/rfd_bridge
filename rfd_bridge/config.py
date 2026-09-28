"""YAML configuration model and strict startup validation."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List

import yaml


class ConfigError(ValueError):
    pass


@dataclass
class SerialConfig:
    port: str
    baud: int = 460800
    rtscts: bool = True
    reconnect: bool = True
    reconnect_interval_sec: float = 2.0
    read_timeout_sec: float = 0.1
    write_timeout_sec: float = 1.0


@dataclass
class LinkConfig:
    bandwidth_limit_bps: int = 100000
    mtu_payload: int = 256
    max_message_size: int = 2_000_000
    max_queue_bytes: int = 4_000_000
    max_reassembly_messages: int = 32
    reassembly_timeout_sec: float = 15.0
    ack_timeout_sec: float = 2.0
    max_retries: int = 3
    max_reliable_pending: int = 32
    hello_interval_sec: float = 3.0
    link_timeout_sec: float = 10.0
    refuse_on_config_mismatch: bool = True


@dataclass
class TopicConfig:
    name: str
    topic_id: int
    ros_topic: str
    remote_topic: str
    type: str  # noqa: A003 - public YAML field is intentionally named "type"
    direction: str
    priority: int = 3
    max_rate_hz: float = 0.0
    max_bytes_per_sec: float = 0.0
    reliability: str = 'best_effort'
    queue_depth: int = 1
    drop_policy: str = 'oldest'
    qos: Any = 'default'
    replace_started_below: float = 0.0

    @property
    def transmits(self) -> bool:
        return self.direction in ('tx', 'bidirectional')

    @property
    def receives(self) -> bool:
        return self.direction in ('rx', 'bidirectional')


@dataclass
class BridgeConfig:
    serial: SerialConfig
    link: LinkConfig
    topics: List[TopicConfig]
    bridge_id: str = 'rfd_bridge'
    diagnostics_topic: str = '/rfd_bridge/diagnostics'
    diagnostics_period_sec: float = 1.0
    source_path: str = ''

    def topic_hash(self) -> bytes:
        # Only wire compatibility fields belong in the hash. Local names,
        # direction, rates and scheduling policy may intentionally differ.
        mapping = [{'id': t.topic_id, 'type': t.type} for t in self.topics]
        canonical = json.dumps(sorted(mapping, key=lambda x: x['id']),
                               separators=(',', ':'), sort_keys=True)
        return hashlib.sha256(canonical.encode()).digest()[:16]


def _number(data: Dict[str, Any], key: str, default, cast):
    value = data.get(key, default)
    try:
        return cast(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f'{key} must be {cast.__name__}') from exc


def _boolean(data: Dict[str, Any], key: str, default: bool) -> bool:
    value = data.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ('true', 'false'):
        return value.lower() == 'true'
    raise ConfigError(f'{key} must be true or false')


def load_config(path: str, resolve_types: bool = True) -> BridgeConfig:
    p = Path(os.path.expanduser(path))
    if not p.is_file():
        raise ConfigError(f'configuration file does not exist: {p}')
    try:
        raw = yaml.safe_load(p.read_text(encoding='utf-8'))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f'cannot read YAML: {exc}') from exc
    if not isinstance(raw, dict):
        raise ConfigError('configuration root must be a mapping')
    s = raw.get('serial')
    if not isinstance(s, dict) or not str(s.get('port', '')).strip():
        raise ConfigError('serial.port is required')
    serial = SerialConfig(
        port=str(s['port']), baud=_number(s, 'baud', 460800, int),
        rtscts=_boolean(s, 'rtscts', True),
        reconnect=_boolean(s, 'reconnect', True),
        reconnect_interval_sec=_number(s, 'reconnect_interval_sec', 2.0, float),
        read_timeout_sec=_number(s, 'read_timeout_sec', 0.1, float),
        write_timeout_sec=_number(s, 'write_timeout_sec', 1.0, float))
    if serial.baud <= 0 or serial.reconnect_interval_sec <= 0:
        raise ConfigError('serial baud and reconnect interval must be positive')

    link_raw = raw.get('link', {})
    if not isinstance(link_raw, dict):
        raise ConfigError('link must be a mapping')
    link = LinkConfig(**{
        key: (_boolean(link_raw, key, default) if isinstance(default, bool) else
              type(default)(link_raw.get(key, default)))
        for key, default in LinkConfig().__dict__.items()
    })
    if link.bandwidth_limit_bps <= 0:
        raise ConfigError('link.bandwidth_limit_bps must be positive')
    # UART uses at least 10 line bits per octet, so a budget above baud cannot
    # be sustained even before radio throttling.
    if link.bandwidth_limit_bps > serial.baud:
        raise ConfigError('bandwidth_limit_bps cannot exceed serial baud')
    if not 32 <= link.mtu_payload <= 4096:
        raise ConfigError('link.mtu_payload must be 32..4096 bytes')
    if link.max_message_size < link.mtu_payload or link.max_queue_bytes <= 0:
        raise ConfigError('message/queue limits are inconsistent')
    if ((link.max_message_size + link.mtu_payload - 1) // link.mtu_payload
            > 65535):
        raise ConfigError('max_message_size needs more than 65535 fragments')
    if min(link.reassembly_timeout_sec, link.ack_timeout_sec,
           link.hello_interval_sec, link.link_timeout_sec) <= 0:
        raise ConfigError('link timeouts/intervals must be positive')
    if link.max_retries < 0 or link.max_reliable_pending <= 0:
        raise ConfigError('reliability limits are invalid')

    raw_topics = raw.get('topics')
    if isinstance(raw_topics, dict):
        entries = [(str(name), value) for name, value in raw_topics.items()]
    elif isinstance(raw_topics, list):
        entries = [(str(value.get('name') or value.get('topic') or f'topic_{i}'), value)
                   for i, value in enumerate(raw_topics)]
    else:
        raise ConfigError('topics must be a non-empty mapping or list')
    if not entries:
        raise ConfigError('at least one topic is required')
    assigned = {int(v['id']) for _, v in entries
                if isinstance(v, dict) and 'id' in v}
    next_id = 1
    topics: List[TopicConfig] = []
    for name, value in sorted(entries):
        if not isinstance(value, dict):
            raise ConfigError(f'topic {name} must be a mapping')
        while next_id in assigned:
            next_id += 1
        topic_id = int(value.get('id', next_id))
        if 'id' not in value:
            assigned.add(topic_id)
            next_id += 1
        ros_topic = str(value.get('ros_topic', value.get('topic', name)))
        remote_topic = str(value.get('remote_topic', ros_topic))
        type_name = str(value.get('type', ''))
        direction = str(value.get('direction', 'bidirectional')).lower()
        reliability = str(value.get('reliability', 'best_effort')).lower()
        drop = str(value.get('drop_policy',
                             'latest' if reliability == 'latest' else 'oldest')).lower()
        t = TopicConfig(
            name=name, topic_id=topic_id, ros_topic=ros_topic,
            remote_topic=remote_topic, type=type_name, direction=direction,
            priority=int(value.get('priority', 3)),
            max_rate_hz=float(value.get('max_rate_hz', 0.0)),
            max_bytes_per_sec=float(value.get('max_bytes_per_sec', 0.0)),
            reliability=reliability,
            queue_depth=int(value.get('queue_depth', 1)), drop_policy=drop,
            qos=value.get('qos', 'default'),
            replace_started_below=float(value.get('replace_started_below', 0.0)))
        _validate_topic(t)
        topics.append(t)
    ids = [t.topic_id for t in topics]
    if len(ids) != len(set(ids)):
        raise ConfigError('duplicate topic IDs are not allowed')
    if resolve_types:
        try:
            from rosidl_runtime_py.utilities import get_message
        except ImportError as exc:
            raise ConfigError('ROS 2 Python libraries are unavailable') from exc
        for topic in topics:
            try:
                get_message(topic.type)
            except (ImportError, AttributeError, ValueError) as exc:
                raise ConfigError(
                    f'topic {topic.name}: cannot resolve message type '
                    f'{topic.type!r}; install its interface package') from exc
    diagnostics_period = float(raw.get('diagnostics_period_sec', 1.0))
    if diagnostics_period <= 0:
        raise ConfigError('diagnostics_period_sec must be positive')
    return BridgeConfig(
        serial=serial, link=link, topics=sorted(topics, key=lambda x: x.topic_id),
        bridge_id=str(raw.get('bridge_id', 'rfd_bridge')),
        diagnostics_topic=str(raw.get('diagnostics_topic', '/rfd_bridge/diagnostics')),
        diagnostics_period_sec=diagnostics_period,
        source_path=str(p))


def _validate_topic(t: TopicConfig) -> None:
    if not 1 <= t.topic_id <= 65535:
        raise ConfigError(f'topic {t.name}: id must be 1..65535 (0 is control)')
    if t.direction not in ('tx', 'rx', 'bidirectional'):
        raise ConfigError(f'topic {t.name}: invalid direction {t.direction!r}')
    if not 0 <= t.priority <= 255:
        raise ConfigError(f'topic {t.name}: priority must be 0..255')
    if t.reliability not in ('best_effort', 'latest', 'reliable'):
        raise ConfigError(f'topic {t.name}: invalid RF reliability mode')
    if t.drop_policy.replace('_', '-') not in (
            'oldest', 'newest', 'latest', 'latest-only', 'never-drop'):
        raise ConfigError(f'topic {t.name}: invalid drop policy')
    if t.queue_depth <= 0 or t.max_rate_hz < 0 or t.max_bytes_per_sec < 0:
        raise ConfigError(f'topic {t.name}: invalid rate or queue limit')
    if not 0.0 <= t.replace_started_below <= 1.0:
        raise ConfigError(f'topic {t.name}: replace_started_below must be 0..1')
    if not t.type or '/msg/' not in t.type:
        raise ConfigError(f'topic {t.name}: type must look like package/msg/Type')
    if not t.ros_topic.startswith('/') or not t.remote_topic.startswith('/'):
        raise ConfigError(f'topic {t.name}: ROS topic names must be absolute')
