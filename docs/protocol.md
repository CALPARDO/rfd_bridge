# RFD Bridge wire protocol v1

Protocol v1 transports serialized ROS messages; it does not carry DDS, ROS
topic names, or type strings. All multi-byte integers are unsigned and in
network (big-endian) byte order.

## Data frame

```text
+-------+-----+-------+-------+------------+----------+----------+--------+
| Magic | Ver | Flags | Topic | Message ID | Frag idx | Frag cnt | Length |
| 2 B   | 1 B | 1 B   | 2 B   | 4 B        | 2 B      | 2 B      | 2 B    |
+-------+-----+-------+-------+------------+----------+----------+--------+
| Payload (Length bytes)                                      | CRC32 4 B |
+-------------------------------------------------------------+-----------+
```

| Field | Meaning |
|---|---|
| Magic | ASCII `RB` (`0x52 0x42`) |
| Version | `1` for this document |
| Flags | bit 0 reliable, bit 1 retransmission; other bits must be zero |
| Topic | Configured uint16 topic ID; zero is the control channel |
| Message ID | Sender-local uint32 sequence, wrapping from `0xffffffff` to 1 |
| Fragment index | Zero-based index |
| Fragment count | Total fragments, 1..65535 |
| Length | Payload length in this frame |
| CRC32 | IEEE CRC-32 of the complete 16-byte header and payload |

Frame overhead is 20 bytes. The implementation accepts at most 4096 payload
bytes per frame and additionally enforces the configured (normally 256-byte)
MTU. A message may therefore have at most 65535 fragments, while the smaller
configured `max_message_size` is always authoritative.

An empty serialized message is represented by one zero-length fragment.
Fragments may arrive out of order. Duplicate fragments are ignored. A message
is delivered only after every index has arrived; incomplete assemblies expire.
Completed message IDs are remembered briefly so a retransmission cannot cause a
second ROS publication.

## Stream recovery and malformed data

The receiver searches for `RB`, waits for the fixed header, validates version,
flags, count, index, and length before waiting for payload, and then checks the
CRC. On an invalid candidate it advances by at least one byte and searches
again. Its input buffer, frame payload, number of assemblies, and total message
size are bounded. Magic may be split across serial reads. These rules allow a
valid frame after arbitrary garbage or corruption to resynchronize the parser.

## Reliability

Best-effort and latest messages are sent once. A reliable message sets flag bit
0. After complete reassembly the receiver sends one message-level ACK. If an
assembly times out it sends a NACK. ACK timeout or NACK schedules every fragment
again with the retransmission bit set. Retry count and simultaneous reliable
messages are bounded. Exhausting retries drops that transfer without blocking
other topics. Duplicate reliable data receives another ACK.

This is delivery reliability, not persistent storage or exactly-once execution.
A process restart can forget the recent-message cache.

## Control channel

Topic ID zero is reserved. Control payloads begin with a one-byte kind:

| Kind | Value | Payload after kind |
|---|---:|---|
| HELLO | 1 | protocol `u8`, topic-map hash 16 B, bridge-ID hash 8 B, capabilities `u16` |
| KEEPALIVE | 2 | none (reserved; periodic HELLO currently provides liveness) |
| ACK | 3 | topic `u16`, message ID `u32` |
| NACK | 4 | topic `u16`, message ID `u32` |
| PING | 5 | token `u64` |
| PONG | 6 | token `u64` |

Control frames have the scheduler's highest priority. The topic-map hash is the
first 16 bytes of SHA-256 over canonical JSON entries containing each numeric ID
and ROS type. Local names, directions, rates and priorities are excluded. By
default, user frames remain blocked until a matching HELLO is received. A
version or hash mismatch is reported as an error.

## Compatibility and security

Receivers reject versions other than 1; a future incompatible layout must use a
new protocol version. CRC detects accidental corruption only. It provides no
authentication, confidentiality, replay protection, or tamper resistance.
Modem-layer encryption is separate; applications needing authenticated traffic
must add an appropriate cryptographic layer.
