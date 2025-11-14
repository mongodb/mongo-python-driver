# Copyright 2015-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Internal network layer helper methods."""
from __future__ import annotations

import asyncio
import collections
import errno
import socket
import struct
import sys
import time
from asyncio import BaseTransport, BufferedProtocol, Future, Protocol, Transport
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Union,
)

from pymongo import _csot, ssl_support
from pymongo._asyncio_task import create_task
from pymongo.common import MAX_MESSAGE_SIZE
from pymongo.compression_support import decompress
from pymongo.errors import ProtocolError, _OperationCancelled
from pymongo.message import _UNPACK_REPLY, _OpMsg, _OpReply
from pymongo.socket_checker import _errno_from_exception

if TYPE_CHECKING:
    from pymongo.asynchronous.pool import AsyncBaseConnection, AsyncConnection
    from pymongo.pyopenssl_context import _sslConn
    from pymongo.synchronous.pool import BaseConnection, Connection

_UNPACK_HEADER = struct.Struct("<iiii").unpack
_UNPACK_COMPRESSION_HEADER = struct.Struct("<iiB").unpack
_POLL_TIMEOUT = 0.5
_PYPY = "PyPy" in sys.version
_WINDOWS = sys.platform == "win32"

# Errors raised by sockets (and TLS sockets) when in non-blocking mode.
BLOCKING_IO_ERRORS = (
    BlockingIOError,
    *ssl_support.BLOCKING_IO_LOOKUP_ERROR,
    *ssl_support.BLOCKING_IO_ERRORS,
)


def sendall(sock: Union[socket.socket, _sslConn], buf: bytes) -> None:
    sock.sendall(buf)


async def _poll_cancellation(conn: AsyncBaseConnection) -> None:
    while True:
        if conn.cancel_context.cancelled:
            return

        await asyncio.sleep(_POLL_TIMEOUT)


def wait_for_read(conn: BaseConnection, deadline: Optional[float]) -> None:
    """Block until at least one byte is read, or a timeout, or a cancel."""
    sock = conn.conn.sock
    timed_out = False
    # Check if the connection's socket has been manually closed
    if sock.fileno() == -1:
        return
    while True:
        # SSLSocket can have buffered data which won't be caught by select.
        if hasattr(sock, "pending") and sock.pending() > 0:
            readable = True
        else:
            # Wait up to 500ms for the socket to become readable and then
            # check for cancellation.
            if deadline:
                remaining = deadline - time.monotonic()
                # When the timeout has expired perform one final check to
                # see if the socket is readable. This helps avoid spurious
                # timeouts on AWS Lambda and other FaaS environments.
                if remaining <= 0:
                    timed_out = True
                timeout = max(min(remaining, _POLL_TIMEOUT), 0)
            else:
                timeout = _POLL_TIMEOUT
            readable = conn.socket_checker.select(sock, read=True, timeout=timeout)
        if conn.cancel_context.cancelled:
            raise _OperationCancelled("operation cancelled")
        if readable:
            return
        if timed_out:
            raise socket.timeout("timed out")


def receive_data(conn: BaseConnection, length: int, deadline: Optional[float]) -> memoryview:
    buf = bytearray(length)
    mv = memoryview(buf)
    bytes_read = 0
    # To support cancelling a network read, we shorten the socket timeout and
    # check for the cancellation signal after each timeout. Alternatively we
    # could close the socket but that does not reliably cancel recv() calls
    # on all OSes.
    # When the timeout has expired we perform one final non-blocking recv.
    # This helps avoid spurious timeouts when the response is actually already
    # buffered on the client.
    orig_timeout = conn.conn.gettimeout()
    try:
        while bytes_read < length:
            try:
                # Use the legacy wait_for_read cancellation approach on PyPy due to PYTHON-5011.
                # also use it on Windows due to PYTHON-5405
                if _PYPY or _WINDOWS:
                    wait_for_read(conn, deadline)
                    if _csot.get_timeout() and deadline is not None:
                        conn.set_conn_timeout(max(deadline - time.monotonic(), 0))
                else:
                    if deadline is not None:
                        short_timeout = min(max(deadline - time.monotonic(), 0), _POLL_TIMEOUT)
                    else:
                        short_timeout = _POLL_TIMEOUT
                    conn.set_conn_timeout(short_timeout)

                chunk_length = conn.conn.recv_into(mv[bytes_read:])
            except BLOCKING_IO_ERRORS:
                if conn.cancel_context.cancelled:
                    raise _OperationCancelled("operation cancelled") from None
                # We reached the true deadline.
                raise socket.timeout("timed out") from None
            except socket.timeout:
                if conn.cancel_context.cancelled:
                    raise _OperationCancelled("operation cancelled") from None
                if (
                    _PYPY
                    or _WINDOWS
                    or not conn.is_sdam
                    and deadline is not None
                    and deadline - time.monotonic() < 0
                ):
                    # We reached the true deadline.
                    raise
                continue
            except OSError as exc:
                if conn.cancel_context.cancelled:
                    raise _OperationCancelled("operation cancelled") from None
                if _errno_from_exception(exc) == errno.EINTR:
                    continue
                raise
            if chunk_length == 0:
                raise OSError("connection closed")

            bytes_read += chunk_length
    finally:
        conn.set_conn_timeout(orig_timeout)

    return mv


class NetworkingInterfaceBase:
    def __init__(self, conn: Any):
        self.conn = conn

    @property
    def gettimeout(self) -> Any:
        raise NotImplementedError

    def settimeout(self, timeout: float | None) -> None:
        raise NotImplementedError

    def close(self) -> Any:
        raise NotImplementedError

    def is_closing(self) -> bool:
        raise NotImplementedError

    @property
    def get_conn(self) -> Any:
        raise NotImplementedError

    @property
    def sock(self) -> Any:
        raise NotImplementedError


class AsyncNetworkingInterface(NetworkingInterfaceBase):
    def __init__(self, conn: tuple[Transport, PyMongoBaseProtocol]):
        super().__init__(conn)

    @property
    def gettimeout(self) -> float | None:
        return self.conn[1].gettimeout

    def settimeout(self, timeout: float | None) -> None:
        self.conn[1].settimeout(timeout)

    async def close(self) -> None:
        self.conn[1].close()
        await self.conn[1].wait_closed()

    def is_closing(self) -> bool:
        return self.conn[0].is_closing()

    @property
    def get_conn(self) -> PyMongoBaseProtocol:
        return self.conn[1]

    @property
    def sock(self) -> socket.socket:
        return self.conn[0].get_extra_info("socket")


class NetworkingInterface(NetworkingInterfaceBase):
    def __init__(self, conn: Union[socket.socket, _sslConn]):
        super().__init__(conn)

    def gettimeout(self) -> float | None:
        return self.conn.gettimeout()

    def settimeout(self, timeout: float | None) -> None:
        self.conn.settimeout(timeout)

    def close(self) -> None:
        self.conn.close()

    def is_closing(self) -> bool:
        return self.conn.is_closing()

    @property
    def get_conn(self) -> Union[socket.socket, _sslConn]:
        return self.conn

    @property
    def sock(self) -> Union[socket.socket, _sslConn]:
        return self.conn

    def fileno(self) -> int:
        return self.conn.fileno()

    def recv_into(self, buffer: bytes) -> int:
        return self.conn.recv_into(buffer)


class PyMongoBaseProtocol(Protocol):
    def __init__(self, timeout: Optional[float] = None):
        self.transport: Transport = None  # type: ignore[assignment]
        self._timeout = timeout
        self._closed = asyncio.get_running_loop().create_future()
        self._connection_lost = False

    def settimeout(self, timeout: float | None) -> None:
        self._timeout = timeout

    @property
    def gettimeout(self) -> float | None:
        """The configured timeout for the socket that underlies our protocol pair."""
        return self._timeout

    def close(self, exc: Optional[Exception] = None) -> None:
        self.transport.abort()
        self._resolve_pending(exc)
        self._connection_lost = True

    def connection_lost(self, exc: Optional[Exception] = None) -> None:
        self._resolve_pending(exc)
        if not self._closed.done():
            self._closed.set_result(None)

    def _resolve_pending(self, exc: Optional[Exception] = None) -> None:
        pass

    async def wait_closed(self) -> None:
        await self._closed

    async def write(self, message: bytes) -> None:
        """Write a message to this connection's transport."""
        if self.transport.is_closing():
            raise OSError("Connection is closed")
        self.transport.write(message)
        self.transport.resume_reading()

    async def read(self, *args: Any) -> Any:
        raise NotImplementedError


class PyMongoProtocol(PyMongoBaseProtocol, BufferedProtocol):
    def __init__(self, timeout: Optional[float] = None):
        super().__init__(timeout)
        # Each message is reader in 2-3 parts: header, compression header, and message body
        # The message buffer is allocated after the header is read.
        self._header = memoryview(bytearray(16))
        self._header_index = 0
        self._compression_header = memoryview(bytearray(9))
        self._compression_index = 0
        self._message: Optional[memoryview] = None
        self._message_index = 0
        # State. TODO: replace booleans with an enum?
        self._expecting_header = True
        self._expecting_compression = False
        self._message_size = 0
        self._op_code = 0
        self._read_waiter: Optional[Future[Any]] = None
        self._is_compressed = False
        self._compressor_id: Optional[int] = None
        self._max_message_size = MAX_MESSAGE_SIZE
        self._response_to: Optional[int] = None
        self._pending_messages: collections.deque[Future[Any]] = collections.deque()
        self._done_messages: collections.deque[Future[Any]] = collections.deque()

    def connection_made(self, transport: BaseTransport) -> None:
        """Called exactly once when a connection is made.
        The transport argument is the transport representing the write side of the connection.
        """
        self.transport = transport  # type: ignore[assignment]
        self.transport.set_write_buffer_limits(MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE)

    async def read(self, request_id: Optional[int], max_message_size: int) -> tuple[bytes, int]:
        """Read a single MongoDB Wire Protocol message from this connection."""
        if self.transport:
            try:
                self.transport.resume_reading()
            # Known bug in SSL Protocols, fixed in Python 3.11: https://github.com/python/cpython/issues/89322
            except AttributeError:
                raise OSError("connection is already closed") from None
        self._max_message_size = max_message_size
        if self._done_messages:
            message = await self._done_messages.popleft()
        else:
            if self.transport and self.transport.is_closing():
                raise OSError("connection is already closed")
            read_waiter = asyncio.get_running_loop().create_future()
            self._pending_messages.append(read_waiter)
            try:
                message = await read_waiter
            finally:
                if read_waiter in self._done_messages:
                    self._done_messages.remove(read_waiter)
        if message:
            op_code, compressor_id, response_to, data = message
            # No request_id for exhaust cursor "getMore".
            if request_id is not None:
                if request_id != response_to:
                    raise ProtocolError(
                        f"Got response id {response_to!r} but expected {request_id!r}"
                    )
            if compressor_id is not None:
                data = decompress(data, compressor_id)
            return data, op_code
        raise OSError("connection closed")

    def get_buffer(self, sizehint: int) -> memoryview:
        """Called to allocate a new receive buffer.
        The asyncio loop calls this method expecting to receive a non-empty buffer to fill with data.
        If any data does not fit into the returned buffer, this method will be called again until
        either no data remains or an empty buffer is returned.
        """
        # Due to a bug, Python <=3.11 will call get_buffer() even after we raise
        # ProtocolError in buffer_updated() and call connection_lost(). We allocate
        # a temp buffer to drain the waiting data.
        if self._connection_lost:
            if not self._message:
                self._message = memoryview(bytearray(2**14))
            return self._message
        # TODO: optimize this by caching pointers to the buffers.
        # return self._buffer[self._index:]
        if self._expecting_header:
            return self._header[self._header_index :]
        if self._expecting_compression:
            return self._compression_header[self._compression_index :]
        return self._message[self._message_index :]  # type: ignore[index]

    def buffer_updated(self, nbytes: int) -> None:
        """Called when the buffer was updated with the received data"""
        # Wrote 0 bytes into a non-empty buffer, signal connection closed
        if nbytes == 0:
            self.close(OSError("connection closed"))
            return
        if self._connection_lost:
            return
        if self._expecting_header:
            self._header_index += nbytes
            if self._header_index >= 16:
                self._expecting_header = False
                try:
                    (
                        self._message_size,
                        self._op_code,
                        self._response_to,
                        self._expecting_compression,
                    ) = self.process_header()
                except ProtocolError as exc:
                    self.close(exc)
                    return
                self._message = memoryview(bytearray(self._message_size))
            return
        if self._expecting_compression:
            self._compression_index += nbytes
            if self._compression_index >= 9:
                self._expecting_compression = False
                self._op_code, self._compressor_id = self.process_compression_header()
            return

        self._message_index += nbytes
        if self._message_index >= self._message_size:
            self._expecting_header = True
            # Pause reading to avoid storing an arbitrary number of messages in memory.
            self.transport.pause_reading()
            if self._pending_messages:
                result = self._pending_messages.popleft()
            else:
                result = asyncio.get_running_loop().create_future()
            # Future has been cancelled, close this connection
            if result.done():
                self.close(None)
                return
            # Necessary values to reconstruct and verify message
            result.set_result(
                (self._op_code, self._compressor_id, self._response_to, self._message)
            )
            self._done_messages.append(result)
            # Reset internal state to expect a new message
            self._header_index = 0
            self._compression_index = 0
            self._message_index = 0
            self._message_size = 0
            self._message = None
            self._op_code = 0
            self._compressor_id = None
            self._response_to = None

    def process_header(self) -> tuple[int, int, int, bool]:
        """Unpack a MongoDB Wire Protocol header."""
        length, _, response_to, op_code = _UNPACK_HEADER(self._header)
        expecting_compression = False
        if op_code == 2012:  # OP_COMPRESSED
            if length <= 25:
                raise ProtocolError(
                    f"Message length ({length!r}) not longer than standard OP_COMPRESSED message header size (25)"
                )
            expecting_compression = True
            length -= 9
        if length <= 16:
            raise ProtocolError(
                f"Message length ({length!r}) not longer than standard message header size (16)"
            )
        if length > self._max_message_size:
            raise ProtocolError(
                f"Message length ({length!r}) is larger than server max "
                f"message size ({self._max_message_size!r})"
            )

        return length - 16, op_code, response_to, expecting_compression

    def process_compression_header(self) -> tuple[int, int]:
        """Unpack a MongoDB Wire Protocol compression header."""
        op_code, _, compressor_id = _UNPACK_COMPRESSION_HEADER(self._compression_header)
        return op_code, compressor_id

    def _resolve_pending(self, exc: Optional[Exception] = None) -> None:
        pending = list(self._pending_messages)
        for msg in pending:
            if not msg.done():
                if exc is None:
                    msg.set_result(None)
                else:
                    msg.set_exception(exc)
            self._done_messages.append(msg)


class PyMongoKMSProtocol(PyMongoBaseProtocol):
    def __init__(self, timeout: Optional[float] = None):
        super().__init__(timeout)
        self._buffers: collections.deque[memoryview[bytes]] = collections.deque()
        self._bytes_ready = 0
        self._pending_reads: collections.deque[int] = collections.deque()
        self._pending_listeners: collections.deque[Future[Any]] = collections.deque()

    def connection_made(self, transport: BaseTransport) -> None:
        """Called exactly once when a connection is made.
        The transport argument is the transport representing the write side of the connection.
        """
        self.transport = transport  # type: ignore[assignment]

    def data_received(self, data: bytes) -> None:
        if self._connection_lost:
            return

        self._bytes_ready += len(data)
        self._buffers.append(memoryview(data))

        if not len(self._pending_reads):
            return

        bytes_needed = self._pending_reads.popleft()
        data = self._read(bytes_needed)
        waiter = self._pending_listeners.popleft()
        waiter.set_result(data)

    async def read(self, bytes_needed: int) -> bytes:
        """Read up to the requested bytes from this connection."""
        # Note: all reads are "up-to" bytes_needed because we don't know if the kms_context
        # has processed a Content-Length header and is requesting a response or not.
        # Wait for other listeners first.
        if len(self._pending_listeners):
            await asyncio.gather(*self._pending_listeners)
        # If there are bytes ready, then there is no need to wait further.
        if self._bytes_ready > 0:
            return self._read(bytes_needed)
        if self.transport:
            try:
                self.transport.resume_reading()
            # Known bug in SSL Protocols, fixed in Python 3.11: https://github.com/python/cpython/issues/89322
            except AttributeError:
                raise OSError("connection is already closed") from None
        if self.transport and self.transport.is_closing():
            raise OSError("connection is already closed")
        self._pending_reads.append(bytes_needed)
        read_waiter = asyncio.get_running_loop().create_future()
        self._pending_listeners.append(read_waiter)
        return await read_waiter

    def _resolve_pending(self, exc: Optional[Exception] = None) -> None:
        while self._pending_listeners:
            fut = self._pending_listeners.popleft()
            fut.set_result(b"")

    def _read(self, bytes_needed: int) -> memoryview:
        """Read bytes."""
        # Send the bytes to the listener.
        if self._bytes_ready < bytes_needed:
            bytes_needed = self._bytes_ready
        self._bytes_ready -= bytes_needed

        output_buf = bytearray(bytes_needed)
        n_remaining = bytes_needed
        out_index = 0
        while n_remaining > 0:
            buffer = self._buffers.popleft()
            buf_size = len(buffer)
            # if we didn't exhaust the buffer, read the partial data and return the buffer.
            if buf_size > n_remaining:
                output_buf[out_index : n_remaining + out_index] = buffer[:n_remaining]
                buffer = buffer[n_remaining:]
                n_remaining = 0
                self._buffers.appendleft(buffer)
            # otherwise exhaust the buffer.
            else:
                output_buf[out_index : out_index + buf_size] = buffer[:]
                out_index += buf_size
                n_remaining -= buf_size
        return memoryview(output_buf)


async def async_sendall(conn: PyMongoBaseProtocol, buf: bytes) -> None:
    try:
        await asyncio.wait_for(conn.write(buf), timeout=conn.gettimeout)
    except asyncio.TimeoutError as exc:
        # Convert the asyncio.wait_for timeout error to socket.timeout which pool.py understands.
        raise socket.timeout("timed out") from exc


async def async_receive_kms(conn: AsyncBaseConnection, bytes_needed: int) -> bytes:
    """Receive raw bytes from the kms connection."""

    def callback(result: Any) -> bytes:
        return result

    return await _async_receive_data(conn, callback, bytes_needed)


async def _async_receive_data(
    conn: AsyncBaseConnection, callback: Callable[..., Any], *args: Any
) -> Any:
    timeout: Optional[Union[float, int]]
    timeout = conn.conn.gettimeout
    if _csot.get_timeout():
        deadline = _csot.get_deadline()
    else:
        if timeout:
            deadline = time.monotonic() + timeout
        else:
            deadline = None
    if deadline:
        # When the timeout has expired perform one final check to
        # see if the socket is readable. This helps avoid spurious
        # timeouts on AWS Lambda and other FaaS environments.
        timeout = max(deadline - time.monotonic(), 0)

    read_task = create_task(conn.conn.get_conn.read(*args))
    cancellation_task = create_task(_poll_cancellation(conn))
    tasks = [read_task, cancellation_task]
    try:
        done, pending = await asyncio.wait(
            tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending)
        if len(done) == 0:
            raise socket.timeout("timed out")
        if read_task in done:
            return callback(read_task.result())
        raise _OperationCancelled("operation cancelled")
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.wait(tasks)
        raise


async def async_receive_message(
    conn: AsyncConnection,
    request_id: Optional[int],
    max_message_size: int = MAX_MESSAGE_SIZE,
) -> Union[_OpReply, _OpMsg]:
    """Receive a raw BSON message or raise socket.error."""

    def callback(result: Any) -> _OpMsg | _OpReply:
        data, op_code = result
        try:
            unpack_reply = _UNPACK_REPLY[op_code]
        except KeyError:
            raise ProtocolError(
                f"Got opcode {op_code!r} but expected {_UNPACK_REPLY.keys()!r}"
            ) from None
        return unpack_reply(data)

    return await _async_receive_data(conn, callback, request_id, max_message_size)


def receive_kms(conn: BaseConnection, bytes_needed: int) -> bytes:
    """Receive raw bytes from the kms connection."""
    return conn.conn.sock.recv(bytes_needed)


def receive_message(
    conn: Connection, request_id: Optional[int], max_message_size: int = MAX_MESSAGE_SIZE
) -> Union[_OpReply, _OpMsg]:
    """Receive a raw BSON message or raise socket.error."""
    if _csot.get_timeout():
        deadline = _csot.get_deadline()
    else:
        timeout = conn.conn.gettimeout()
        if timeout:
            deadline = time.monotonic() + timeout
        else:
            deadline = None
    # Ignore the response's request id.
    length, _, response_to, op_code = _UNPACK_HEADER(receive_data(conn, 16, deadline))
    # No request_id for exhaust cursor "getMore".
    if request_id is not None:
        if request_id != response_to:
            raise ProtocolError(f"Got response id {response_to!r} but expected {request_id!r}")
    if length <= 16:
        raise ProtocolError(
            f"Message length ({length!r}) not longer than standard message header size (16)"
        )
    if length > max_message_size:
        raise ProtocolError(
            f"Message length ({length!r}) is larger than server max "
            f"message size ({max_message_size!r})"
        )
    if op_code == 2012:
        op_code, _, compressor_id = _UNPACK_COMPRESSION_HEADER(receive_data(conn, 9, deadline))
        data = decompress(receive_data(conn, length - 25, deadline), compressor_id)
    else:
        data = receive_data(conn, length - 16, deadline)

    try:
        unpack_reply = _UNPACK_REPLY[op_code]
    except KeyError:
        raise ProtocolError(
            f"Got opcode {op_code!r} but expected {_UNPACK_REPLY.keys()!r}"
        ) from None
    return unpack_reply(data)
