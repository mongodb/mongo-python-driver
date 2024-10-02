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
import socket
import struct
import sys
from asyncio import AbstractEventLoop, Future
from typing import (
    Union,
)

from pymongo import ssl_support

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
        fd = sock.fileno()
        sent = 0

        def _is_ready(fut: Future) -> None:
            loop.remove_writer(fd)
            loop.remove_reader(fd)
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
                    await fut
                if isinstance(exc, BLOCKING_IO_WRITE_ERROR):
                    fut = loop.create_future()
                    loop.add_writer(fd, _is_ready, fut)
                    await fut
                if _HAVE_PYOPENSSL and isinstance(exc, BLOCKING_IO_LOOKUP_ERROR):
                    fut = loop.create_future()
                    loop.add_reader(fd, _is_ready, fut)
                    loop.add_writer(fd, _is_ready, fut)
                    await fut
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


def sendall(sock: Union[socket.socket, _sslConn], buf: bytes) -> None:
    sock.sendall(buf)
