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
import os
import random
import socket
import struct
import time
import traceback
import typing
import uuid
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
    from pymongo.asynchronous.pool import AsyncConnection, AsyncConnectionProtocol
    from pymongo.synchronous.pool import Connection

_UNPACK_HEADER = struct.Struct("<iiii").unpack
_UNPACK_COMPRESSION_HEADER = struct.Struct("<iiB").unpack
_POLL_TIMEOUT = 0.5
# Errors raised by sockets (and TLS sockets) when in non-blocking mode.
BLOCKING_IO_ERRORS = (BlockingIOError, BLOCKING_IO_LOOKUP_ERROR, *ssl_support.BLOCKING_IO_ERRORS)


class PyMongoProtocol(asyncio.BufferedProtocol):
    def __init__(self, buffer_size: Optional[int] = 2 ** 14):
        self._buffer_size = buffer_size
        self.transport = None
        self._buffer = memoryview(bytearray(self._buffer_size))
        self._overflow = None
        self._length = 0
        self._overflow_length = 0
        self._body_length = 0
        self._op_code = None
        self._connection_lost = False
        self._paused = False
        self._drain_waiter = None
        self._loop = asyncio.get_running_loop()
        self._read_waiter = None
        self._id = uuid.uuid4()

    def connection_made(self, transport):
        self.transport = transport

    async def write(self, message: bytes):
        self.transport.write(message)
        await self._drain_helper()
        # if "find" in str(message):
        #     print(f"Finished writing find on {self._id}")

    async def read(self):
        # if asyncio.current_task() and 'pymongo' not in asyncio.current_task().get_name():
            # print(f"Read call on {asyncio.current_task().get_name()}, {self._id}, from {traceback.format_stack(limit=5)}")
        # tasks = [(t.get_name(), t.get_coro()) for t in asyncio.all_tasks() if "Task" in t.get_name()]
        # print(f"All pending: {tasks}")
        self._length, self._overflow_length, self._body_length, self._op_code, self._overflow = 0, 0, 0, None, None
        self._read_waiter = self._loop.create_future()
        await self._read_waiter
        if self._read_waiter.done() and self._read_waiter.result() is not None:
            # if asyncio.current_task() and 'pymongo' not in asyncio.current_task().get_name():
                # print(f"Returning body of size {self._body_length} on {asyncio.current_task().get_name()}, {self._id}")
            if self._body_length > self._buffer_size:
                # print(f"Finished reading find on {self._id}")
                return memoryview(bytearray(self._buffer[16:self._length]) + bytearray(self._overflow[:self._overflow_length])), self._op_code
            else:
                return memoryview(self._buffer[16:self._body_length]), self._op_code

    def get_buffer(self, sizehint: int):
        # print(f"Sizehint: {sizehint} for {self._id}")
        # if sizehint > self._buffer_size - self._length:

        if self._overflow is not None:
            # if asyncio.current_task() and 'pymongo' not in asyncio.current_task().get_name():
                # print(f"Overflow offset: {self._overflow_length} on {asyncio.current_task().get_name()}, {self._id}")
            return self._overflow[self._overflow_length:]
        # if asyncio.current_task() and 'pymongo' not in asyncio.current_task().get_name():
        #     print(f"Buffer offset {self._length} on {asyncio.current_task().get_name()}, {self._id}")
        return self._buffer[self._length:]

    def buffer_updated(self, nbytes: int):
        # print(f"Bytes read: {nbytes} for {self._id}, have read {self._length}, {self._overflow_length}")
        if nbytes == 0:
            self._read_waiter.set_result(None)
            self._read_waiter.set_exception(OSError("connection closed"))
        else:
            if self._overflow is not None:
                self._overflow_length += nbytes
                # if asyncio.current_task() and 'pymongo' not in asyncio.current_task().get_name():
                #     print(f"Read {nbytes} into overflow, have {self._length + self._overflow_length} out of {self._body_length} on {asyncio.current_task().get_name()}, {self._id}")
            else:
                if self._length == 0:
                    self._body_length, _, response_to, self._op_code = _UNPACK_HEADER(self._buffer[:16])
                    if self._body_length > self._buffer_size:
                        self._overflow = memoryview(bytearray(self._body_length - (self._buffer_size - nbytes)))
                self._length += nbytes
                # if asyncio.current_task() and 'pymongo' not in asyncio.current_task().get_name():
                #     print(f"Read {nbytes} into buffer, have {self._length + self._overflow_length} out of {self._body_length} on {asyncio.current_task().get_name()}, {self._id}")
            if self._length + self._overflow_length >= self._body_length and self._read_waiter and not self._read_waiter.done():
                self._read_waiter.set_result(True)

    def pause_writing(self):
        assert not self._paused
        self._paused = True

    def resume_writing(self):
        assert self._paused
        self._paused = False

        if self._drain_waiter and not self._drain_waiter.done():
            self._drain_waiter.set_result(None)

    # def eof_received(self):
        # print(f"EOF received on {self._id}")

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


async def async_sendall(stream: AsyncConnectionProtocol, buf: bytes) -> None:
    try:
        await asyncio.wait_for(stream.conn[1].write(buf), timeout=5)
    except Exception as exc:
        # print(f"Got exception writing: {exc} on {asyncio.current_task().get_name() if asyncio.current_task().get_name() else None},")
        raise
        # # Convert the asyncio.wait_for timeout error to socket.timeout which pool.py understands.
        # raise socket.timeout("timed out") from exc


def sendall(sock: Union[socket.socket, _sslConn], buf: bytes) -> None:
    sock.sendall(buf)


async def _poll_cancellation(conn: AsyncConnectionProtocol) -> None:
    while True:
        if conn.cancel_context.cancelled:
            return

        await asyncio.sleep(_POLL_TIMEOUT)


async def async_receive_data(
    conn: AsyncConnectionProtocol, length: int, deadline: Optional[float]
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

    return await conn.conn[1].read()
    # cancellation_task = create_task(_poll_cancellation(conn))
    # try:
    #     read_task = create_task(conn.conn[1].read())
    #     tasks = [read_task, cancellation_task]
    #     done, pending = await asyncio.wait(
    #         tasks, timeout=5, return_when=asyncio.FIRST_COMPLETED
    #     )
    #     for task in pending:
    #         task.cancel()
    #     if pending:
    #         await asyncio.wait(pending)
    #     if len(done) == 0:
    #         raise socket.timeout("timed out")
    #     if read_task in done:
    #         return read_task.result()
    #     raise _OperationCancelled("operation cancelled")
    # finally:
    #     pass
    #     # sock.settimeout(sock_timeout)


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
