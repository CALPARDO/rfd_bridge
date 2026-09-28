from glob import glob
import os

from setuptools import find_packages, setup

package_name = 'rfd_bridge'

setup(
    name=package_name,
    version='0.1.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml', 'README.md', 'LICENSE', 'CHANGELOG.md']),
        (os.path.join('share', package_name, 'launch'), glob('launch/*.launch.py')),
        (os.path.join('share', package_name, 'config'), glob('config/*.yaml')),
        (os.path.join('share', package_name, 'docs'), glob('docs/*')),
    ],
    install_requires=['setuptools', 'pyserial', 'PyYAML'],
    tests_require=['pytest'],
    zip_safe=True,
    maintainer='Derin Durak',
    maintainer_email='derin@calpardo.com',
    description='Priority-aware ROS 2 topic transport over RFDesign serial radios',
    license='Apache-2.0',
    entry_points={'console_scripts': [
        'bridge = rfd_bridge.bridge_node:main',
        'rfd_info = rfd_bridge.rfd_modem:info_main',
        'benchmark = rfd_bridge.tools:benchmark_main',
        'ping = rfd_bridge.tools:ping_main',
        'link_test = rfd_bridge.tools:link_test_main',
    ]},
)
