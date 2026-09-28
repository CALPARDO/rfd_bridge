"""Standalone serial benchmark, ping, and two-port integration utilities."""

from __future__ import annotations

import argparse
import os
import statistics
import struct
import threading
import time

import serial

from .protocol import fragment_message, Frame, StreamParser

TOOL_TOPIC = 65535
_BENCH = struct.Struct('>cIIQ')
_PING = struct.Struct('>cIQ')


def _port(name, baud, rtscts):
    return serial.Serial(name, baud, timeout=0.05, write_timeout=5.0,
                         rtscts=rtscts, exclusive=True)


def benchmark_main(argv=None):
    parser = argparse.ArgumentParser(
        description='Measure requested versus actual framed serial/RF throughput')
    parser.add_argument('--port', required=True)
    parser.add_argument('--mode', choices=('tx', 'rx'), required=True)
    parser.add_argument('--baud', type=int, default=460800)
    parser.add_argument('--rate-bps', type=float, default=100000,
                        help='requested payload bit rate (TX mode)')
    parser.add_argument('--payload-size', type=int, default=240)
    parser.add_argument('--duration', type=float, default=10.0)
    parser.add_argument('--drain', type=float, default=2.0,
                        help='seconds to receive buffered tail data after TX')
    parser.add_argument('--no-rtscts', action='store_true')
    args = parser.parse_args(argv)
    if args.payload_size < _BENCH.size or args.duration <= 0 or args.rate_bps <= 0:
        parser.error('payload size, duration, and rate must be positive')
    port = _port(args.port, args.baud, not args.no_rtscts)
    stream = StreamParser(max(256, args.payload_size))
    start = time.monotonic()
    sent = received = payload_bytes = 0
    sequence = 0
    expected = None
    lost = 0
    declared_total = None
    remote_session = None
    local_session = int.from_bytes(os.urandom(4), 'big')
    cts_blocked = 0
    throttled_writes = 0
    blocked_write_time = 0.0
    max_write_time = 0.0
    first_rx = last_rx = None

    def receive_data(data):
        nonlocal received, payload_bytes, expected, lost, declared_total
        nonlocal remote_session
        nonlocal first_rx, last_rx
        for frame in stream.feed(data):
            if frame.topic_id != TOOL_TOPIC or len(frame.payload) < _BENCH.size:
                continue
            kind, session, seq, _ = _BENCH.unpack_from(frame.payload)
            if kind == b'S':
                remote_session = session
                received = payload_bytes = lost = 0
                expected = declared_total = None
                first_rx = last_rx = None
                continue
            if session != remote_session:
                continue
            if kind == b'E':
                declared_total = seq
                continue
            if kind != b'D':
                continue
            received_at = time.monotonic()
            first_rx = first_rx or received_at
            last_rx = received_at
            if expected is not None and seq > expected:
                lost += seq - expected
            expected = seq + 1
            received += 1
            payload_bytes += len(frame.payload)

    tx_finished = None
    try:
        if args.mode == 'tx':
            start_payload = _BENCH.pack(
                b'S', local_session, 0, time.monotonic_ns())
            start_payload += bytes(args.payload_size - len(start_payload))
            port.write(Frame(TOOL_TOPIC, 0, 0, 1, start_payload).pack(
                max(256, args.payload_size)))
        while time.monotonic() - start < args.duration:
            if args.mode == 'tx':
                target = start + (sent * args.payload_size * 8 / args.rate_bps)
                delay = target - time.monotonic()
                if delay > 0:
                    time.sleep(min(delay, 0.05))
                    continue
                payload = _BENCH.pack(
                    b'D', local_session, sequence,
                    time.monotonic_ns()) + os.urandom(
                        args.payload_size - _BENCH.size)
                frame = Frame(TOOL_TOPIC, sequence, 0, 1, payload)
                if not port.cts and not args.no_rtscts:
                    cts_blocked += 1
                write_started = time.monotonic()
                port.write(frame.pack(max(256, args.payload_size)))
                write_elapsed = time.monotonic() - write_started
                max_write_time = max(max_write_time, write_elapsed)
                uart_time = (args.payload_size + 20) * 10.0 / args.baud
                if write_elapsed > uart_time * 2.0 + 0.001:
                    throttled_writes += 1
                    blocked_write_time += write_elapsed
                sent += 1
                sequence = (sequence + 1) & 0xffffffff
            waiting = port.in_waiting
            if args.mode == 'tx' and not waiting:
                data = b''
            else:
                data = port.read(max(1, waiting or 1))
            receive_data(data)
        if args.mode == 'tx':
            tx_finished = time.monotonic()
            end_payload = _BENCH.pack(
                b'E', local_session, sent, time.monotonic_ns())
            end_payload += bytes(args.payload_size - len(end_payload))
            port.write(Frame(TOOL_TOPIC, sequence, 0, 1, end_payload).pack(
                max(256, args.payload_size)))
            drain_deadline = time.monotonic() + max(0.0, args.drain)
            while time.monotonic() < drain_deadline:
                receive_data(port.read(max(1, port.in_waiting or 1)))
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = max(1e-9, time.monotonic() - start)
        port.close()
    requested = args.rate_bps / 8.0 if args.mode == 'tx' else 0.0
    tx_elapsed = max(1e-9, (tx_finished or (start + elapsed)) - start)
    actual_tx = sent * args.payload_size / tx_elapsed
    active_rx_time = ((last_rx - first_rx) if first_rx is not None and
                      last_rx is not None and last_rx > first_rx else elapsed)
    actual_rx = payload_bytes / max(1e-9, active_rx_time)
    if declared_total is not None:
        lost = max(lost, declared_total - received)
    total_expected = declared_total or (received + lost)
    print(f'duration: {elapsed:.2f} s')
    print(f'local session: 0x{local_session:08x}')
    print(f'requested TX payload rate: {requested:.1f} B/s')
    print(f'actual TX payload rate: {actual_tx:.1f} B/s')
    print(f'actual RX payload rate: {actual_rx:.1f} B/s')
    print(f'packets sent: {sent}; packets received: {received}')
    print(f'CRC failures: {stream.crc_failures}; sequence loss: {lost} '
          f'({100.0 * lost / total_expected if total_expected else 0.0:.2f}%)')
    print(f'CTS observed deasserted before write: {cts_blocked} times')
    print(f'write throttling events: {throttled_writes}; blocked write time: '
          f'{blocked_write_time:.3f} s; maximum write: {max_write_time:.3f} s')


def ping_main(argv=None):
    parser = argparse.ArgumentParser(description='Measure framed RF serial RTT')
    parser.add_argument('--port', required=True)
    parser.add_argument('--mode', choices=('initiator', 'responder'), required=True)
    parser.add_argument('--baud', type=int, default=460800)
    parser.add_argument('--count', type=int, default=20)
    parser.add_argument('--interval', type=float, default=0.5)
    parser.add_argument('--timeout', type=float, default=2.0)
    parser.add_argument('--no-rtscts', action='store_true')
    args = parser.parse_args(argv)
    port = _port(args.port, args.baud, not args.no_rtscts)
    stream = StreamParser(256)
    samples = []
    try:
        if args.mode == 'responder':
            print('ping responder running; Ctrl-C to stop')
            while True:
                for frame in stream.feed(port.read(max(1, port.in_waiting or 1))):
                    if frame.topic_id == TOOL_TOPIC and frame.payload[:1] == b'P':
                        reply = Frame(TOOL_TOPIC, frame.message_id, 0, 1,
                                      b'Q' + frame.payload[1:])
                        port.write(reply.pack(256))
        else:
            for sequence in range(args.count):
                sent_ns = time.monotonic_ns()
                payload = _PING.pack(b'P', sequence, sent_ns)
                port.write(Frame(TOOL_TOPIC, sequence, 0, 1, payload).pack(256))
                deadline = time.monotonic() + args.timeout
                found = False
                while time.monotonic() < deadline and not found:
                    for frame in stream.feed(port.read(max(1, port.in_waiting or 1))):
                        if frame.payload[:1] != b'Q' or len(frame.payload) < _PING.size:
                            continue
                        _, seq, original_ns = _PING.unpack_from(frame.payload)
                        if seq == sequence:
                            rtt = (time.monotonic_ns() - original_ns) / 1e6
                            samples.append(rtt)
                            print(f'{sequence}: {rtt:.2f} ms')
                            found = True
                if not found:
                    print(f'{sequence}: timeout')
                time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        port.close()
    if args.mode == 'initiator':
        loss = args.count - len(samples)
        if samples:
            print(f'RTT min/avg/max: {min(samples):.2f}/'
                  f'{statistics.mean(samples):.2f}/{max(samples):.2f} ms')
        print(f'packet loss: {loss}/{args.count} '
              f'({100.0 * loss / args.count:.1f}%)')


def _receive_exact_message(port, expected_topic, timeout, max_payload=512):
    parser = StreamParser(max_payload)
    fragments = {}
    count = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for frame in parser.feed(port.read(max(1, port.in_waiting or 1))):
            if frame.topic_id != expected_topic:
                continue
            fragments[frame.fragment_index] = frame.payload
            count = frame.fragment_count
            if len(fragments) == count:
                return b''.join(fragments[i] for i in range(count)), parser
    raise TimeoutError(f'timed out with {len(fragments)}/{count or "?"} fragments')


def link_test_main(argv=None):
    parser = argparse.ArgumentParser(
        description='Bidirectional two-radio protocol test using two local ports')
    parser.add_argument('--port-a', '--tx-port', dest='port_a', required=True)
    parser.add_argument('--port-b', '--rx-port', dest='port_b', required=True)
    parser.add_argument('--baud', type=int, default=460800)
    parser.add_argument('--large-bytes', type=int, default=50000)
    parser.add_argument('--timeout', type=float, default=20.0)
    parser.add_argument('--no-rtscts', action='store_true')
    args = parser.parse_args(argv)
    a = _port(args.port_a, args.baud, not args.no_rtscts)
    b = _port(args.port_b, args.baud, not args.no_rtscts)
    cases = [('small A->B', a, b, 100, b'hello rfd_bridge'),
             ('large A->B', a, b, 101, os.urandom(args.large_bytes)),
             ('small B->A', b, a, 102, b'reverse link')]
    try:
        for name, source, dest, topic, payload in cases:
            result = {}
            thread = threading.Thread(
                target=lambda: result.update(data=_receive_exact_message(
                    dest, topic, args.timeout)[0]), daemon=True)
            thread.start()
            for frame in fragment_message(topic, 1, payload, 256):
                source.write(frame.pack(256))
            thread.join(args.timeout + 1)
            if result.get('data') != payload:
                raise RuntimeError(f'{name} failed')
            print(f'PASS {name}: {len(payload)} payload bytes match')
    finally:
        a.close()
        b.close()
