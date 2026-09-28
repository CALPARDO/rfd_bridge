from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    config = LaunchConfiguration('config')
    return LaunchDescription([
        DeclareLaunchArgument('config', description='Absolute path to bridge YAML'),
        Node(
            package='rfd_bridge', executable='bridge', name='rfd_bridge',
            output='screen', parameters=[{'config_file': config}]),
    ])
