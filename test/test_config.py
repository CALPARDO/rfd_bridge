import pytest

from rfd_bridge.config import ConfigError, load_config


BASE = """
serial: {port: /dev/ttyTEST, baud: 460800, rtscts: true}
link: {bandwidth_limit_bps: 80000, mtu_payload: 256}
topics:
  text:
    id: 1
    ros_topic: /in
    remote_topic: /out
    type: std_msgs/msg/String
    direction: tx
    priority: 1
"""


def write(tmp_path, text):
    path = tmp_path / 'config.yaml'
    path.write_text(text)
    return str(path)


def test_valid_config_and_dynamic_type(tmp_path):
    config = load_config(write(tmp_path, BASE))
    assert config.topics[0].type == 'std_msgs/msg/String'
    assert config.topic_hash() == config.topic_hash()


@pytest.mark.parametrize('replacement', [
    'direction: sideways', 'priority: -1', 'id: 0',
])
def test_invalid_topic_fields(tmp_path, replacement):
    key = replacement.split(':')[0]
    old = {'direction': 'direction: tx', 'priority': 'priority: 1',
           'id': 'id: 1'}[key]
    with pytest.raises(ConfigError):
        load_config(write(tmp_path, BASE.replace(old, replacement)),
                    resolve_types=False)


def test_duplicate_ids(tmp_path):
    extra = """
  other:
    id: 1
    ros_topic: /a
    type: std_msgs/msg/String
    direction: rx
"""
    with pytest.raises(ConfigError, match='duplicate'):
        load_config(write(tmp_path, BASE + extra), resolve_types=False)


def test_missing_message_package(tmp_path):
    broken = BASE.replace('std_msgs/msg/String', 'does_not_exist/msg/Nothing')
    with pytest.raises(ConfigError, match='cannot resolve'):
        load_config(write(tmp_path, broken))


def test_impossible_bandwidth(tmp_path):
    broken = BASE.replace('80000', '999999')
    with pytest.raises(ConfigError, match='serial baud'):
        load_config(write(tmp_path, broken), resolve_types=False)
