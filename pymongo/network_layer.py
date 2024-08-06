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
from ssl import SSLError, SSLSocket, SSLWantReadError, SSLWantWriteError
from typing import (
    Union,
)

from pymongo import ssl_support

try:
    from pymongo.pyopenssl_context import _sslConn
except ImportError:
    _sslConn = SSLSocket  # type: ignore

_UNPACK_HEADER = struct.Struct("<iiii").unpack
_UNPACK_COMPRESSION_HEADER = struct.Struct("<iiB").unpack
_POLL_TIMEOUT = 0.5
# Errors raised by sockets (and TLS sockets) when in non-blocking mode.
BLOCKING_IO_ERRORS = (BlockingIOError, *ssl_support.BLOCKING_IO_ERRORS)


async def async_sendall(sock: Union[socket.socket, _sslConn], buf: bytes) -> None:
    timeout = sock.gettimeout()
    sock.settimeout(0.0)
    loop = asyncio.get_event_loop()
    try:
        if isinstance(sock, (SSLSocket, _sslConn)):
            if sys.platform == "win32":
                await asyncio.wait_for(_async_sendall_ssl_windows(sock, buf), timeout=timeout)
            else:
                await asyncio.wait_for(_async_sendall_ssl(sock, buf, loop), timeout=timeout)
        else:
            await asyncio.wait_for(loop.sock_sendall(sock, buf), timeout=timeout)
    finally:
        sock.settimeout(timeout)


async def _async_sendall_ssl(
    sock: Union[socket.socket, _sslConn], buf: bytes, loop: AbstractEventLoop
) -> None:
    fd = sock.fileno()
    sent = 0

    def _is_ready(fut: Future) -> None:
        if fut.done():
            return
        loop.remove_writer(fd)
        loop.remove_reader(fd)
        fut.set_result(None)

    while sent < len(buf):
        try:
            sent += sock.send(buf)
        except BLOCKING_IO_ERRORS as exc:
            fd = sock.fileno()
            # Check for closed socket.
            if fd == -1:
                raise SSLError("Underlying socket has been closed") from None
            if isinstance(exc, SSLWantReadError):
                fut = loop.create_future()
                loop.add_reader(fd, _is_ready, fut)
                await fut
            if isinstance(exc, SSLWantWriteError):
                fut = loop.create_future()
                loop.add_writer(fd, _is_ready, fut)
                await fut


async def _async_sendall_ssl_windows(sock: Union[socket.socket, _sslConn], buf: bytes) -> None:
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
