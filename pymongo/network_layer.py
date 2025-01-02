# Copyright 2015-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
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
import time
import typing
from asyncio import AbstractEventLoop
from typing import (
    TYPE_CHECKING,
    Optional,
    Union,
)

from pymongo import ssl_support
from pymongo._asyncio_task import create_task
from pymongo.common import MAX_MESSAGE_SIZE
from pymongo.errors import _OperationCancelled
from pymongo.socket_checker import _errno_from_exception

try:
    from ssl import SSLError, SSLSocket

    _HAVE_SSL = True
except ImportError:
    _HAVE_SSL = False

try:
    from pymongo.pyopenssl_context import (
        BLOCKING_IO_LOOKUP_ERROR,
        BLOCKING_IO_READ_ERROR,
        BLOCKING_IO_WRITE_ERROR,
        _sslConn,
    )

    _HAVE_PYOPENSSL = True
except ImportError:
    _HAVE_PYOPENSSL = False
    _sslConn = SSLSocket  # type: ignore
    from pymongo.ssl_support import (  # type: ignore[assignment]
        BLOCKING_IO_LOOKUP_ERROR,
        BLOCKING_IO_READ_ERROR,
        BLOCKING_IO_WRITE_ERROR,
    )

if TYPE_CHECKING:
    from pymongo.asynchronous.pool import AsyncConnection, AsyncConnectionStream
    from pymongo.synchronous.pool import Connection

_UNPACK_HEADER = struct.Struct("<iiii").unpack
_UNPACK_COMPRESSION_HEADER = struct.Struct("<iiB").unpack
_POLL_TIMEOUT = 0.5
# Errors raised by sockets (and TLS sockets) when in non-blocking mode.
BLOCKING_IO_ERRORS = (BlockingIOError, BLOCKING_IO_LOOKUP_ERROR, *ssl_support.BLOCKING_IO_ERRORS)


class PyMongoProtocolReadRequest:
    def __init__(self, length: int, future: asyncio.Future):
        self.length = length
        self.future = future


class PyMongoProtocol(asyncio.BufferedProtocol):
    def __init__(self):
        self._buffer_size = MAX_MESSAGE_SIZE
        self.transport = None
        self._buffer = memoryview(bytearray(self._buffer_size))
        self.ready_offset = 0
        self.empty_offset = 0
        self.bytes_available = 0
        self.op_code = None
        self._done = None
        self._connection_lost = False
        self._paused = False
        self._drain_waiter = None
        self._loop = asyncio.get_running_loop()
        self._messages: typing.Deque[PyMongoProtocolReadRequest] = collections.deque()

    def connection_made(self, transport):
        self.transport = transport

    async def write(self, message: bytes):
        self.transport.write(message)
        await self._drain_helper()

    async def read(self, length: int):
        if self.bytes_available >= length:
            start, end = self._calculate_read_offsets(length)
            return self._read_data(start, end)
        else:
            request = PyMongoProtocolReadRequest(length, self._loop.create_future())
            self._messages.append(request)
            try:
                await request.future
            finally:
                self._messages.remove(request)
            if request.future.done():
                start, end = request.future.result()
                return self._read_data(start, end)

    def _calculate_read_offsets(self, length):
        if self.ready_offset < self.empty_offset:
            start, end = self.ready_offset, self.ready_offset + length
            self.ready_offset += length
        # Our offset for writing has wrapped around to the start of the buffer
        else:
            if self.ready_offset + length <= self._buffer_size:
                start, end = self.ready_offset, self.ready_offset + length
                self.ready_offset += length
            else:
                start, end = (self.ready_offset, 0), (self._buffer_size, length - (self._buffer_size - self.ready_offset))
                self.ready_offset = length - (self._buffer_size - self.ready_offset)
        self.bytes_available -= length
        return start, end

    def _read_data(self, start, end):
        if isinstance(start, tuple):
            # print(f"Reading data with start {start} and end {end} on {asyncio.current_task()}")
            return memoryview(
                bytearray(self._buffer[start[0]:end[0]]) + bytearray(self._buffer[start[1]:end[1]]))
        else:
            return self._buffer[start:end]

    def get_buffer(self, sizehint: int):
        # print(f"get_buffer with empty {self.empty_offset} and sizehint {sizehint}, ready {self.ready_offset}")
        if self.empty_offset + sizehint >= self._buffer_size:
            self.empty_offset = 0
        if self.empty_offset < self.ready_offset:
            return self._buffer[self.empty_offset:self.ready_offset]
        else:
            return self._buffer[self.empty_offset:]

    def buffer_updated(self, nbytes: int):
        if nbytes == 0:
            self.connection_lost(OSError("connection closed"))
            self._done.set_result(None)
        self.empty_offset += nbytes
        self.bytes_available += nbytes

        # print(f"Ready: {self.ready_offset} and empty: {self.empty_offset} and available: {self.bytes_available} out of {self._buffer_size}")
        for message in self._messages:
            if not message.future.done() and self.bytes_available >= message.length:
                start, end = self._calculate_read_offsets(message.length)
                message.future.set_result((start, end))
            #     if self.ready_offset < self.empty_offset:
            #         message.future.set_result((self.ready_offset, self.ready_offset + message.length))
            #         self.ready_offset += message.length
            # # Our offset for writing has wrapped around to the start of the buffer
            # else:
            #     # print(f"Ready: {self.ready_offset}, Empty: {self.empty_offset}, expecting: {self.expected_length}")
            #     # print(f"Is linear: {self.ready_offset + self.expected_length <= self._buffer_size}, {self.ready_offset + self.expected_length} vs {self._buffer_size}")
            #     # print(f"Is wrapped: {self._buffer_size - self.ready_offset + self.empty_offset >= self.expected_length}, {self._buffer_size - self.ready_offset + self.empty_offset} vs {self.expected_length}")
            #     if self.ready_offset + message.length <= self._buffer_size:
            #         message.future.set_result((self.ready_offset, self.ready_offset + message.length))
            #         self.ready_offset += message.length
            #     else:
            #         # print(f"{asyncio.current_task()} First chunk: {self._buffer_size - self.ready_offset}, second chunk: {self.expected_length - (self._buffer_size - self.ready_offset)}, total: {self._buffer_size - self.ready_offset + self.expected_length - (self._buffer_size - self.ready_offset)} of {self.expected_length}")
            #         message.future.set_result(((self.ready_offset, 0), (self._buffer_size, message.length - (self._buffer_size - self.ready_offset))))
            #         self.ready_offset = message.length - (self._buffer_size - self.ready_offset)
            #     self.bytes_available -= message.length

    def pause_writing(self):
        assert not self._paused
        self._paused = True

    def resume_writing(self):
        assert self._paused
        self._paused = False

        if self._drain_waiter and not self._drain_waiter.done():
            self._drain_waiter.set_result(None)

    def connection_lost(self, exc):
        self._connection_lost = True
        # Wake up the writer(s) if currently paused.
        if not self._paused:
            return

        if self._drain_waiter and not self._drain_waiter.done():
            if exc is None:
                self._drain_waiter.set_result(None)
            else:
                self._drain_waiter.set_exception(exc)

    async def _drain_helper(self):
        if self._connection_lost:
            raise ConnectionResetError('Connection lost')
        if not self._paused:
            return
        self._drain_waiter = self._loop.create_future()
        await self._drain_waiter

    def data(self):
        return self._buffer


async def async_sendall(stream: AsyncConnectionStream, buf: bytes) -> None:
    try:
        await asyncio.wait_for(stream.conn[1].write(buf), timeout=None)
    except asyncio.TimeoutError as exc:
        # Convert the asyncio.wait_for timeout error to socket.timeout which pool.py understands.
        raise socket.timeout("timed out") from exc


def sendall(sock: Union[socket.socket, _sslConn], buf: bytes) -> None:
    sock.sendall(buf)


async def _poll_cancellation(conn: AsyncConnectionStream) -> None:
    while True:
        if conn.cancel_context.cancelled:
            return

        await asyncio.sleep(_POLL_TIMEOUT)


async def async_receive_data(
    conn: AsyncConnectionStream, length: int, deadline: Optional[float]
) -> memoryview:
    # sock = conn.conn
    # sock_timeout = sock.gettimeout()
    # timeout: Optional[Union[float, int]]
    # if deadline:
    #     # When the timeout has expired perform one final check to
    #     # see if the socket is readable. This helps avoid spurious
    #     # timeouts on AWS Lambda and other FaaS environments.
    #     timeout = max(deadline - time.monotonic(), 0)
    # else:
    #     timeout = sock_timeout

    cancellation_task = create_task(_poll_cancellation(conn))
    try:
        read_task = create_task(conn.conn[1].read(length))
        tasks = [read_task, cancellation_task]
        done, pending = await asyncio.wait(
            tasks, timeout=5, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending)
        if len(done) == 0:
            raise socket.timeout("timed out")
        if read_task in done:
            return read_task.result()
        raise _OperationCancelled("operation cancelled")
    finally:
        pass
        # sock.settimeout(sock_timeout)


def receive_data(conn: Connection, length: int, deadline: Optional[float]) -> memoryview:
    buf = bytearray(length)
    mv = memoryview(buf)
    bytes_read = 0
    # To support cancelling a network read, we shorten the socket timeout and
    # check for the cancellation signal after each timeout. Alternatively we
    # could close the socket but that does not reliably cancel recv() calls
    # on all OSes.
    orig_timeout = conn.conn.gettimeout()
    try:
        while bytes_read < length:
            if deadline is not None:
                # CSOT: Update timeout. When the timeout has expired perform one
                # final non-blocking recv. This helps avoid spurious timeouts when
                # the response is actually already buffered on the client.
                short_timeout = min(max(deadline - time.monotonic(), 0), _POLL_TIMEOUT)
            else:
                short_timeout = _POLL_TIMEOUT
            conn.set_conn_timeout(short_timeout)
            try:
                chunk_length = conn.conn.recv_into(mv[bytes_read:])
            except BLOCKING_IO_ERRORS:
                if conn.cancel_context.cancelled:
                    raise _OperationCancelled("operation cancelled") from None
                # We reached the true deadline.
                raise socket.timeout("timed out") from None
            except socket.timeout:
                if conn.cancel_context.cancelled:
                    raise _OperationCancelled("operation cancelled") from None
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
