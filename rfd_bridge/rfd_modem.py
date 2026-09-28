"""Explicit, read-only RFDesign AT information utility."""

import time

import rclpy
from rclpy.node import Node
import serial


def query(port_name: str, baud: int, rtscts: bool) -> str:
    """Enter command mode, issue read-only queries, then return to data mode."""
    with serial.Serial(port_name, baud, timeout=0.25, write_timeout=1.0,
                       rtscts=rtscts, exclusive=True) as port:
        port.reset_input_buffer()
        time.sleep(1.1)  # RFDesign command-mode guard time
        port.write(b'+++')
        time.sleep(1.1)
        output = bytearray(port.read(4096))
        for command in (b'ATI\r\n', b'ATI5\r\n'):
            port.write(command)
            time.sleep(0.3)
            output.extend(port.read(8192))
        port.write(b'ATO\r\n')
        time.sleep(0.1)
        output.extend(port.read(1024))
        return output.decode('ascii', errors='replace').strip()


class RfdInfoNode(Node):
    def __init__(self):
        super().__init__('rfd_info')
        self.declare_parameter('port', '/dev/ttyTHS1')
        self.declare_parameter('baud', 460800)
        self.declare_parameter('rtscts', True)

    def run(self):
        port = self.get_parameter('port').value
        self.get_logger().warn(
            'Temporarily entering modem command mode for read-only ATI queries; '
            'do not run while the bridge owns the port')
        print(query(port, int(self.get_parameter('baud').value),
                    bool(self.get_parameter('rtscts').value)))


def info_main(args=None):
    rclpy.init(args=args)
    node = RfdInfoNode()
    try:
        node.run()
    except (serial.SerialException, OSError) as exc:
        node.get_logger().error(str(exc))
        raise SystemExit(1)
    finally:
        node.destroy_node()
        rclpy.shutdown()
