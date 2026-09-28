"""Transport abstraction and reconnecting pyserial implementation."""

from __future__ import annotations

from abc import ABC, abstractmethod
import threading
from typing import Callable, Optional

import serial

from .config import SerialConfig


class Transport(ABC):
    @abstractmethod
    def start(self, receive: Callable[[bytes], None],
              state: Callable[[bool, str], None]) -> None: ...

    @abstractmethod
    def write(self, data: bytes) -> bool: ...

    @abstractmethod
    def close(self) -> None: ...

    @property
    @abstractmethod
    def connected(self) -> bool: ...


class SerialTransport(Transport):
    def __init__(self, config: SerialConfig):
        self.config = config
        self._serial: Optional[serial.Serial] = None
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._receive = lambda data: None
        self._state = lambda connected, detail: None
        self.bytes_read = 0
        self.bytes_written = 0
        self.partial_writes = 0
        self.cts_block_events = 0
        self.reconnects = 0

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._serial is not None and self._serial.is_open

    def start(self, receive, state) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._receive, self._state = receive, state
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name='rfd-serial', daemon=True)
        self._thread.start()

    def _open(self) -> None:
        port = serial.Serial(
            port=self.config.port, baudrate=self.config.baud,
            timeout=self.config.read_timeout_sec,
            write_timeout=self.config.write_timeout_sec,
            rtscts=self.config.rtscts, xonxoff=False, dsrdtr=False,
            exclusive=True)
        with self._lock:
            self._serial = port
        self.reconnects += 1
        self._state(True, self.config.port)

    def _disconnect(self, detail: str) -> None:
        with self._lock:
            port, self._serial = self._serial, None
        if port is not None:
            try:
                port.close()
            except serial.SerialException:
                pass
        self._state(False, detail)

    def _run(self) -> None:
        while not self._stop.is_set():
            if not self.connected:
                try:
                    self._open()
                except (serial.SerialException, OSError) as exc:
                    self._state(False, str(exc))
                    if not self.config.reconnect:
                        return
                    self._stop.wait(self.config.reconnect_interval_sec)
                    continue
            try:
                with self._lock:
                    port = self._serial
                if port is None:
                    continue
                data = port.read(max(1, min(4096, port.in_waiting or 1)))
                if data:
                    self.bytes_read += len(data)
                    self._receive(data)
            except (serial.SerialException, OSError) as exc:
                self._disconnect(str(exc))
        self._disconnect('closed')

    def write(self, data: bytes) -> bool:
        view = memoryview(data)
        offset = 0
        try:
            with self._lock:
                port = self._serial
                if port is None or not port.is_open:
                    return False
                if self.config.rtscts and not port.cts:
                    self.cts_block_events += 1
                while offset < len(view):
                    written = port.write(view[offset:])
                    if not written:
                        raise serial.SerialTimeoutException('zero-byte serial write')
                    if written < len(view) - offset:
                        self.partial_writes += 1
                    offset += written
                self.bytes_written += offset
                return True
        except (serial.SerialException, serial.SerialTimeoutException, OSError) as exc:
            self._disconnect(str(exc))
            return False

    def close(self) -> None:
        self._stop.set()
        # pyserial documents these as callable from another thread. They wake
        # POSIX read/write syscalls immediately instead of waiting for CTS or
        # the timeout during ROS signal shutdown.
        port = self._serial
        if port is not None:
            try:
                port.cancel_read()
                port.cancel_write()
            except (AttributeError, serial.SerialException, OSError):
                pass
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=1.0)
        # The reader normally closes in _run after its short read timeout. If
        # it did not exit, close here only after the bounded join.
        self._disconnect('closed')


class MemoryTransport(Transport):
    """In-memory duplex transport for tests; connect two instances as peers."""

    def __init__(self):
        self.peer: Optional['MemoryTransport'] = None
        self._receive = lambda data: None
        self._connected = False
        self.drop_writes = False

    def start(self, receive, state) -> None:
        self._receive = receive
        self._connected = True
        state(True, 'memory')

    @property
    def connected(self) -> bool:
        return self._connected

    def write(self, data: bytes) -> bool:
        if not self._connected or self.drop_writes or not self.peer:
            return False
        self.peer._receive(bytes(data))
        return True

    def close(self) -> None:
        self._connected = False
