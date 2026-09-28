# rfd_bridge

`rfd_bridge` transports selected ROS 2 topics over RFDesign RFD serial radios
without tunneling the full DDS network. Configure each ROS topic, type,
direction, priority, and rate in YAML; the bridge handles native ROS
serialization, binary framing, fragmentation, fair scheduling, reliability,
and reconstruction.

It targets ROS 2 Jazzy and Python 3 on Linux (including NVIDIA Jetson), and is
transport-structured so another byte-stream transport can be added later.

## Quick start

### Install and build

```bash
mkdir -p ~/rfd_bridge_ws/src
cd ~/rfd_bridge_ws/src
git clone https://github.com/CALPARDO/rfd_bridge.git
cd ..
rosdep install --from-paths src --ignore-src -r -y
colcon build --symlink-install
source install/setup.bash
```

### Minimal configuration

Save this as `bridge.yaml` on both machines. Use `direction: tx` on the sender
and `direction: rx` on the receiver; keep `id` and `type` identical.

```yaml
bridge_id: robot                         # use a different value at each end
serial:
  port: /dev/ttyTHS1                     # receiver might use /dev/serial/by-id/...
  baud: 460800
  rtscts: true
link:
  bandwidth_limit_bps: 80000             # bits/s, configurable for your link
  mtu_payload: 256
topics:
  cmd_vel:
    id: 1
    ros_topic: /cmd_vel
    remote_topic: /cmd_vel
    type: geometry_msgs/msg/Twist
    direction: tx
    priority: 0
    max_rate_hz: 20
    reliability: latest
    queue_depth: 1
```

### Run

```bash
ros2 launch rfd_bridge bridge.launch.py config:=$PWD/bridge.yaml
```

### Test

On the sender:

```bash
ros2 topic pub --once /cmd_vel geometry_msgs/msg/Twist \
  '{linear: {x: 0.2}, angular: {z: 0.1}}'
```

On the receiver:

```bash
ros2 topic echo /cmd_vel geometry_msgs/msg/Twist
```

## What happens

```text
local ROS message -> ROS serialization -> versioned RFD frame
                  -> fragmentation -> priority/fair scheduler -> UART/RFD
                  -> CRC/parser -> reassembly -> ROS deserialization
                  -> remote local ROS topic
```

DDS discovery and RTPS remain local to each ROS machine. They are not sent over
RF. Any installed ROS interface can be resolved at runtime from a name such as
`my_robot_interfaces/msg/VehicleState`; no bridge source change or custom
serializer is needed. Message bytes, including application timestamps, are not
modified.

## Configuration

Copy [`config/example_robot.yaml`](config/example_robot.yaml) and
[`config/example_base_station.yaml`](config/example_base_station.yaml) for a
full pair. Numeric IDs are strongly recommended. If omitted, IDs are assigned
deterministically by sorted YAML key, but an explicit mapping is safer.

`ros_topic` is the local subscription for TX. `remote_topic` is the local
publisher for received data. This permits renaming. Directions are `tx`, `rx`,
and `bidirectional`. On a bidirectional mapping where both names are equal, the
bridge registers a short-lived fingerprint immediately before publishing an RF
message and consumes that exact local subscription callback, preventing an
infinite echo.

### Common topic examples

One-way command:

```yaml
  command:
    id: 1
    ros_topic: /cmd_vel
    remote_topic: /cmd_vel
    type: geometry_msgs/msg/Twist
    direction: tx
    priority: 0
    reliability: reliable
    queue_depth: 5
    drop_policy: never-drop
```

One-way telemetry (use `direction: tx` on the vehicle and `rx` below on the
base):

```yaml
  odometry:
    id: 2
    ros_topic: /odom
    remote_topic: /robot/odom
    type: nav_msgs/msg/Odometry
    direction: rx
    priority: 2
    reliability: latest
    queue_depth: 1
```

Bidirectional and custom interface:

```yaml
  shared_state:
    id: 7
    ros_topic: /vehicle/state_out
    remote_topic: /vehicle/state_in
    type: my_robot_interfaces/msg/VehicleState
    direction: bidirectional
    priority: 3
    reliability: best_effort
    queue_depth: 2
```

Compressed image:

```yaml
  camera:
    id: 10
    ros_topic: /camera/image/compressed
    remote_topic: /camera/image/compressed
    type: sensor_msgs/msg/CompressedImage
    direction: tx
    priority: 10
    max_rate_hz: 0.2
    reliability: latest
    drop_policy: latest
    queue_depth: 1
    replace_started_below: 0.2
    qos: sensor_data
```

Use `sensor_msgs/msg/CompressedImage`, not raw `sensor_msgs/msg/Image`, on a
low-bandwidth link unless raw data is truly required. The protocol remains
generic and will carry either. At an illustrative 12.5 kB/s of usable payload,
25 kB, 50 kB, 60 kB, and 100 kB JPEGs take roughly 2, 4, 5, and 8 seconds when
the link carries no other traffic. These are approximations from one real link,
not RFDesign specifications. Higher-priority control fragments can transmit
between image fragments. `replace_started_below` permits a new latest image to
cancel an older transfer that has made no more than the configured fraction of
progress.

### Queue and reliability behavior

Every topic queue and the global byte queue are bounded. Policies are:

- `oldest`: discard the oldest queued message to accept the new one;
- `newest`: reject the arriving message when full;
- `latest` / `latest-only`: replace unsent stale state (and, optionally, a
  barely-started transfer);
- `never-drop`: reject new input rather than discard an already accepted item.

`best_effort` sends once. `latest` sends once and applies latest-state queue
semantics. `reliable` retains a bounded transfer until a whole-message ACK,
retransmits on timeout/NACK, and gives up after a configured retry count so one
failure cannot block the link forever. Reliability is in-memory and
message-level; v0.1 retransmits all fragments rather than selective missing
fragments. It is not durable exactly-once command execution.

The scheduler uses weighted deficit round robin over priorities and rotates
after each fragment. Lower numeric values receive more service, while every
active priority accrues byte credit, protecting low-priority traffic from
permanent starvation. A token bucket applies `bandwidth_limit_bps` to complete
wire bytes, including the 20-byte frame overhead. `max_rate_hz` and optional
`max_bytes_per_sec` apply at each topic input.

ROS DDS QoS and RF reliability are separate. `qos` accepts `default`,
`sensor_data`, `reliable`, `best_effort`, or a mapping containing `reliability`,
`durability`, and `depth`.

## Handshake and link health

Periodic high-priority HELLO frames contain protocol version, bridge-ID hash,
and the ID/type map hash. User bytes are blocked by default until the remote map
matches, avoiding deserialization under a conflicting ID. Local ROS remains
alive through modem or peer loss; serial reconnect and HELLO recovery are
automatic. Diagnostics report `DISCONNECTED`, `CONNECTING`, `CONNECTED`, or
`DEGRADED` based on serial state, valid remote frames, timeout, and map match.

`/rfd_bridge/diagnostics` uses `diagnostic_msgs/msg/DiagnosticArray` and reports
wire/payload bytes, rates, overhead, CRC/resync counts, queues, drops,
reassembly, ACKs, retries, and per-topic state. Normal packet detail is not
logged at INFO.

## RFD900X2 setup notes

Typical wiring with hardware flow control is:

```text
Jetson TX  -> RFD RX       Jetson RTS -> RFD CTS
Jetson RX  <- RFD TX       Jetson CTS <- RFD RTS
GND        <-> GND
```

- Attach the correct antenna during normal operation and provide an adequate
  power supply.
- Both ends need compatible band/channel settings, air rate, and NETID.
- Generic binary transport normally uses MAVLink mode off.
- Set modem UART baud to the YAML baud and enable modem RTS/CTS (`S13=1` for
  applicable SiK firmware). The bridge passes `rtscts=True` to pyserial; it does
  not busy-wait GPIO lines.
- Prefer `/dev/serial/by-id/...` for USB adapters because `/dev/ttyUSB0` can
  change after reconnect.
- RFD900X2-US certified/legal RF settings, frequency parameters, and power must
  not be casually changed. Consult [official RFDesign documentation](https://files.rfdesign.com.au/docs/).

An example observed setup used UART 460800 baud, AIR_SPEED 224 kbps, NETID 25,
ECC off, MAVLink off, and RTS/CTS on. Only UART/flow-control are package
defaults; NETID, RF power, and radio parameters are not. `rfd_info` performs
explicit read-only ATI queries and is never run by the bridge:

```bash
ros2 run rfd_bridge rfd_info --ros-args \
  -p port:=/dev/ttyTHS1 -p baud:=460800 -p rtscts:=true
```

Do not run it while the bridge owns the port. It temporarily enters AT command
mode and does not write modem settings.

### Four different rates

Do not conflate these values:

```text
UART baud:                       460800 baud (example)
RFD configured AIR_SPEED:        224 kbit/s (example)
measured usable payload:         ~12.5 kB/s (one observed link)
software bandwidth budget:       choose below measured saturation
```

RTS/CTS lets a slower radio deassert CTS and backpressure the higher-speed UART.
The software budget additionally keeps queues and modem buffers controlled.
The RFD duplex RF channel is shared; configuring the same budget on both ends
does not create that much independent capacity in each direction.

## Test and measurement tools

Run the unit suite:

```bash
colcon test --packages-select rfd_bridge
colcon test-result --verbose
```

Two-terminal throughput measurement (receiver first):

```bash
ros2 run rfd_bridge benchmark --port /dev/ttyUSB0 --mode rx --duration 20
ros2 run rfd_bridge benchmark --port /dev/ttyTHS1 --mode tx \
  --rate-bps 128000 --duration 20
```

The output distinguishes requested payload rate from actual writes completed
under CTS throttling, and reports received rate, packet sequence loss, CRC
failures, and observed deasserted CTS.

Ping/pong (responder first):

```bash
ros2 run rfd_bridge ping --port /dev/ttyUSB0 --mode responder
ros2 run rfd_bridge ping --port /dev/ttyTHS1 --mode initiator --count 20
```

Direct two-radio test on one machine, including a 50 kB payload:

```bash
ros2 run rfd_bridge link_test \
  --tx-port /dev/ttyTHS1 --rx-port /dev/ttyUSB0 --large-bytes 50000
```

## Protocol, limits, and security

See [`docs/protocol.md`](docs/protocol.md) for the synchronized binary layout,
CRC coverage, control messages, limits, reliability, and resynchronization
rules. CRC32 detects corruption; it is **not** encryption or authentication.
Any modem encryption is separate. Authenticated application traffic requires a
suitable cryptographic layer.

Known v0.1 limitations are tracked explicitly: ACK/NACK is whole-message rather
than selective-fragment, the bandwidth token bucket controls local TX and
cannot coordinate total two-way RF airtime, and reliable state is not persisted
across process restarts. These choices keep the initial implementation bounded,
auditable, and suitable for extension without claiming functionality it lacks.

## License

Apache-2.0. Contributions are welcome; see [`CONTRIBUTING.md`](CONTRIBUTING.md).
