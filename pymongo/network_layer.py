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
import statistics
import struct
import sys
import time
from asyncio import AbstractEventLoop, Future, StreamReader
from typing import (
    TYPE_CHECKING,
    Optional,
    Union,
)

from pymongo import ssl_support
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
    from pymongo.asynchronous.pool import AsyncConnection, AsyncConnectionStream
    from pymongo.synchronous.pool import Connection

_UNPACK_HEADER = struct.Struct("<iiii").unpack
_UNPACK_COMPRESSION_HEADER = struct.Struct("<iiB").unpack
_POLL_TIMEOUT = 0.5
# Errors raised by sockets (and TLS sockets) when in non-blocking mode.
BLOCKING_IO_ERRORS = (BlockingIOError, BLOCKING_IO_LOOKUP_ERROR, *ssl_support.BLOCKING_IO_ERRORS)


async def async_sendall_stream(stream: asyncio.StreamWriter, buf: bytes) -> None:
    try:
        stream.write(buf)
        await asyncio.wait_for(stream.drain(), timeout=None)
    except asyncio.TimeoutError as exc:
        # Convert the asyncio.wait_for timeout error to socket.timeout which pool.py understands.
        raise socket.timeout("timed out") from exc


def sendall(sock: Union[socket.socket, _sslConn], buf: bytes) -> None:
    sock.sendall(buf)


async def _poll_cancellation(conn: AsyncConnection) -> None:
    while True:
        if conn.cancel_context.cancelled:
            return

        await asyncio.sleep(_POLL_TIMEOUT)


async def async_receive_data_stream(
    conn: StreamReader, length: int, deadline: Optional[float]
) -> memoryview:
    # sock = conn.conn
    # sock_timeout = sock.gettimeout()
    timeout: Optional[Union[float, int]]
    # if deadline:
    #     # When the timeout has expired perform one final check to
    #     # see if the socket is readable. This helps avoid spurious
    #     # timeouts on AWS Lambda and other FaaS environments.
    #     timeout = max(deadline - time.monotonic(), 0)
    # else:
    #     timeout = sock_timeout

    try:
        return await asyncio.wait_for(_async_receive_stream(conn, length), timeout=None)
        # read_task = create_task(_async_receive_stream(conn, length))
        # tasks = [read_task, cancellation_task]
        # done, pending = await asyncio.wait(
        #     tasks, timeout=None, return_when=asyncio.FIRST_COMPLETED
        # )
        # print(f"Done: {done}, pending: {pending}")
        # for task in pending:
        #     task.cancel()
        # if pending:
        #     await asyncio.wait(pending)
        # if len(done) == 0:
        #     raise socket.timeout("timed out")
        # if read_task in done:
        #     return read_task.result()
        # # raise _OperationCancelled("operation cancelled")
    finally:
        pass
        # sock.settimeout(sock_timeout)



async def async_receive_data_socket(
    sock: Union[socket.socket, _sslConn], length: int
) -> memoryview:
    sock_timeout = sock.gettimeout()
    timeout = sock_timeout

    sock.settimeout(0.0)
    loop = asyncio.get_event_loop()
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


async def _async_receive_stream(reader: asyncio.StreamReader, length: int) -> memoryview:
    try:
        return memoryview(await reader.readexactly(length))
    except asyncio.IncompleteReadError:
        raise OSError("connection closed")

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
