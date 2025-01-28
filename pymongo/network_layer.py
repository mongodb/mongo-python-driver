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
from pymongo._asyncio_task import create_task
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
    loop = asyncio.get_running_loop()
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
        conn: _sslConn, length: int, loop: AbstractEventLoop, once: Optional[bool] = False
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
                # KMS responses update their expected size after the first batch, stop reading after one loop
                if once:
                    return mv[:read]
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
    # Note: In PYTHON-4493 we plan to replace this code with asyncio streams.
    async def _async_sendall_ssl(
        sock: Union[socket.socket, _sslConn], buf: bytes, dummy: AbstractEventLoop
    ) -> None:
        view = memoryview(buf)
        total_length = len(buf)
        total_sent = 0
        # Backoff starts at 1ms, doubles on timeout up to 512ms, and halves on success
        # down to 1ms.
        backoff = 0.001
        while total_sent < total_length:
            try:
                sent = sock.send(view[total_sent:])
            except BLOCKING_IO_ERRORS:
                await asyncio.sleep(backoff)
                sent = 0
            if sent > 0:
                backoff = max(backoff / 2, 0.001)
            else:
                backoff = min(backoff * 2, 0.512)
            total_sent += sent

    async def _async_receive_ssl(
        conn: _sslConn, length: int, dummy: AbstractEventLoop, once: Optional[bool] = False
    ) -> memoryview:
        mv = memoryview(bytearray(length))
        total_read = 0
        # Backoff starts at 1ms, doubles on timeout up to 512ms, and halves on success
        # down to 1ms.
        backoff = 0.001
        while total_read < length:
            try:
                read = conn.recv_into(mv[total_read:])
                if read == 0:
                    raise OSError("connection closed")
                # KMS responses update their expected size after the first batch, stop reading after one loop
                if once:
                    return mv[:read]
            except BLOCKING_IO_ERRORS:
                await asyncio.sleep(backoff)
                read = 0
            if read > 0:
                backoff = max(backoff / 2, 0.001)
            else:
                backoff = min(backoff * 2, 0.512)
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
    loop = asyncio.get_running_loop()
    cancellation_task = create_task(_poll_cancellation(conn))
    try:
        if _HAVE_SSL and isinstance(sock, (SSLSocket, _sslConn)):
            read_task = create_task(_async_receive_ssl(sock, length, loop))  # type: ignore[arg-type]
        else:
            read_task = create_task(_async_receive(sock, length, loop))  # type: ignore[arg-type]
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
                return read_task.result()
            raise _OperationCancelled("operation cancelled")
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.wait(tasks)
            raise

    finally:
        sock.settimeout(sock_timeout)


async def async_receive_data_socket(
    sock: Union[socket.socket, _sslConn], length: int
) -> memoryview:
    sock_timeout = sock.gettimeout()
    timeout = sock_timeout

    sock.settimeout(0.0)
    loop = asyncio.get_running_loop()
    try:
        if _HAVE_SSL and isinstance(sock, (SSLSocket, _sslConn)):
            return await asyncio.wait_for(
                _async_receive_ssl(sock, length, loop, once=True),  # type: ignore[arg-type]
                timeout=timeout,
            )
        else:
            return await asyncio.wait_for(_async_receive(sock, length, loop), timeout=timeout)  # type: ignore[arg-type]
    except asyncio.TimeoutError as err:
        raise socket.timeout("timed out") from err
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


_PYPY = "PyPy" in sys.version


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
                if _PYPY:
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
                if _PYPY:
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
