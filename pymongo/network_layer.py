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
import errno
import socket
import struct
import sys
import time
from asyncio import AbstractEventLoop, Future
from typing import (
    TYPE_CHECKING,
    Optional,
    Union,
)

from pymongo import _csot, ssl_support
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
    from pymongo.asynchronous.pool import AsyncConnection
    from pymongo.synchronous.pool import Connection

_UNPACK_HEADER = struct.Struct("<iiii").unpack
_UNPACK_COMPRESSION_HEADER = struct.Struct("<iiB").unpack
_POLL_TIMEOUT = 0.5
# Errors raised by sockets (and TLS sockets) when in non-blocking mode.
BLOCKING_IO_ERRORS = (BlockingIOError, BLOCKING_IO_LOOKUP_ERROR, *ssl_support.BLOCKING_IO_ERRORS)


async def async_sendall(sock: Union[socket.socket, _sslConn], buf: bytes) -> None:
    timeout = sock.gettimeout()
    sock.settimeout(0.0)
    loop = asyncio.get_event_loop()
    try:
        if _HAVE_SSL and isinstance(sock, (SSLSocket, _sslConn)):
            await asyncio.wait_for(_async_sendall_ssl(sock, buf, loop), timeout=timeout)
        else:
            await asyncio.wait_for(loop.sock_sendall(sock, buf), timeout=timeout)  # type: ignore[arg-type]
    except asyncio.TimeoutError as exc:
        # Convert the asyncio.wait_for timeout error to socket.timeout which pool.py understands.
        raise socket.timeout("timed out") from exc
    finally:
        sock.settimeout(timeout)


if sys.platform != "win32":

    async def _async_sendall_ssl(
        sock: Union[socket.socket, _sslConn], buf: bytes, loop: AbstractEventLoop
    ) -> None:
        view = memoryview(buf)
        sent = 0

        def _is_ready(fut: Future) -> None:
            if fut.done():
                return
            fut.set_result(None)

        while sent < len(buf):
            try:
                sent += sock.send(view[sent:])
            except BLOCKING_IO_ERRORS as exc:
                fd = sock.fileno()
                # Check for closed socket.
                if fd == -1:
                    raise SSLError("Underlying socket has been closed") from None
                if isinstance(exc, BLOCKING_IO_READ_ERROR):
                    fut = loop.create_future()
                    loop.add_reader(fd, _is_ready, fut)
                    try:
                        await fut
                    finally:
                        loop.remove_reader(fd)
                if isinstance(exc, BLOCKING_IO_WRITE_ERROR):
                    fut = loop.create_future()
                    loop.add_writer(fd, _is_ready, fut)
                    try:
                        await fut
                    finally:
                        loop.remove_writer(fd)
                if _HAVE_PYOPENSSL and isinstance(exc, BLOCKING_IO_LOOKUP_ERROR):
                    fut = loop.create_future()
                    loop.add_reader(fd, _is_ready, fut)
                    try:
                        loop.add_writer(fd, _is_ready, fut)
                        await fut
                    finally:
                        loop.remove_reader(fd)
                        loop.remove_writer(fd)

    async def _async_receive_ssl(
        conn: _sslConn, length: int, loop: AbstractEventLoop
    ) -> memoryview:
        mv = memoryview(bytearray(length))
        total_read = 0

        def _is_ready(fut: Future) -> None:
            if fut.done():
                return
            fut.set_result(None)

        while total_read < length:
            try:
                read = conn.recv_into(mv[total_read:])
                if read == 0:
                    raise OSError("connection closed")
                total_read += read
            except BLOCKING_IO_ERRORS as exc:
                fd = conn.fileno()
                # Check for closed socket.
                if fd == -1:
                    raise SSLError("Underlying socket has been closed") from None
                if isinstance(exc, BLOCKING_IO_READ_ERROR):
                    fut = loop.create_future()
                    loop.add_reader(fd, _is_ready, fut)
                    try:
                        await fut
                    finally:
                        loop.remove_reader(fd)
                if isinstance(exc, BLOCKING_IO_WRITE_ERROR):
                    fut = loop.create_future()
                    loop.add_writer(fd, _is_ready, fut)
                    try:
                        await fut
                    finally:
                        loop.remove_writer(fd)
                if _HAVE_PYOPENSSL and isinstance(exc, BLOCKING_IO_LOOKUP_ERROR):
                    fut = loop.create_future()
                    loop.add_reader(fd, _is_ready, fut)
                    try:
                        loop.add_writer(fd, _is_ready, fut)
                        await fut
                    finally:
                        loop.remove_reader(fd)
                        loop.remove_writer(fd)
        return mv

else:
    # The default Windows asyncio event loop does not support loop.add_reader/add_writer:
    # https://docs.python.org/3/library/asyncio-platforms.html#asyncio-platform-support
    async def _async_sendall_ssl(
        sock: Union[socket.socket, _sslConn], buf: bytes, dummy: AbstractEventLoop
    ) -> None:
        view = memoryview(buf)
        total_length = len(buf)
        total_sent = 0
        while total_sent < total_length:
            try:
                sent = sock.send(view[total_sent:])
            except BLOCKING_IO_ERRORS:
                await asyncio.sleep(0.5)
                sent = 0
            total_sent += sent

    async def _async_receive_ssl(
        conn: _sslConn, length: int, dummy: AbstractEventLoop
    ) -> memoryview:
        mv = memoryview(bytearray(length))
        total_read = 0
        while total_read < length:
            try:
                read = conn.recv_into(mv[total_read:])
            except BLOCKING_IO_ERRORS:
                await asyncio.sleep(0.5)
                read = 0
            total_read += read
        return mv


def sendall(sock: Union[socket.socket, _sslConn], buf: bytes) -> None:
    sock.sendall(buf)


async def _poll_cancellation(conn: AsyncConnection) -> None:
    while True:
        if conn.cancel_context.cancelled:
            return

        await asyncio.sleep(_POLL_TIMEOUT)


async def async_receive_data(
    conn: AsyncConnection, length: int, deadline: Optional[float]
) -> memoryview:
    sock = conn.conn
    sock_timeout = sock.gettimeout()
    timeout: Optional[Union[float, int]]
    if deadline:
        # When the timeout has expired perform one final check to
        # see if the socket is readable. This helps avoid spurious
        # timeouts on AWS Lambda and other FaaS environments.
        timeout = max(deadline - time.monotonic(), 0)
    else:
        timeout = sock_timeout

    sock.settimeout(0.0)
    loop = asyncio.get_event_loop()
    cancellation_task = asyncio.create_task(_poll_cancellation(conn))
    try:
        if _HAVE_SSL and isinstance(sock, (SSLSocket, _sslConn)):
            read_task = asyncio.create_task(_async_receive_ssl(sock, length, loop))  # type: ignore[arg-type]
        else:
            read_task = asyncio.create_task(_async_receive(sock, length, loop))  # type: ignore[arg-type]
        tasks = [read_task, cancellation_task]
        done, pending = await asyncio.wait(
            tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await asyncio.wait(pending)
        if len(done) == 0:
            raise socket.timeout("timed out")
        if read_task in done:
            return read_task.result()
        raise _OperationCancelled("operation cancelled")
    finally:
        sock.settimeout(sock_timeout)


async def _async_receive(conn: socket.socket, length: int, loop: AbstractEventLoop) -> memoryview:
    mv = memoryview(bytearray(length))
    bytes_read = 0
    while bytes_read < length:
        chunk_length = await loop.sock_recv_into(conn, mv[bytes_read:])
        if chunk_length == 0:
            raise OSError("connection closed")
        bytes_read += chunk_length
    return mv


# Sync version:
def wait_for_read(conn: Connection, deadline: Optional[float]) -> None:
    """Block until at least one byte is read, or a timeout, or a cancel."""
    sock = conn.conn
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


def receive_data(conn: Connection, length: int, deadline: Optional[float]) -> memoryview:
    buf = bytearray(length)
    mv = memoryview(buf)
    bytes_read = 0
    while bytes_read < length:
        try:
            wait_for_read(conn, deadline)
            # CSOT: Update timeout. When the timeout has expired perform one
            # final non-blocking recv. This helps avoid spurious timeouts when
            # the response is actually already buffered on the client.
            if _csot.get_timeout() and deadline is not None:
                conn.set_conn_timeout(max(deadline - time.monotonic(), 0))
            chunk_length = conn.conn.recv_into(mv[bytes_read:])
        except BLOCKING_IO_ERRORS:
            raise socket.timeout("timed out") from None
        except OSError as exc:
            if _errno_from_exception(exc) == errno.EINTR:
                continue
            raise
        if chunk_length == 0:
            raise OSError("connection closed")

        bytes_read += chunk_length

    return mv
