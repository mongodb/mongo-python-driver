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
from typing import (
    TYPE_CHECKING,
    Union,
)

from pymongo import ssl_support

if TYPE_CHECKING:
    from pymongo.pyopenssl_context import _sslConn

_UNPACK_HEADER = struct.Struct("<iiii").unpack
_UNPACK_COMPRESSION_HEADER = struct.Struct("<iiB").unpack
_POLL_TIMEOUT = 0.5
# Errors raised by sockets (and TLS sockets) when in non-blocking mode.
BLOCKING_IO_ERRORS = (BlockingIOError, *ssl_support.BLOCKING_IO_ERRORS)


async def async_send(socket: Union[socket.socket, _sslConn], buf: bytes, flags: int = 0) -> int:
    socket.setblocking(False)
    timeout = socket.gettimeout()
    socket.settimeout(0.0)
    try:
        return socket.send(buf, flags)
    finally:
        socket.settimeout(timeout)
        socket.setblocking(True)


async def async_sendall(socket: Union[socket.socket, _sslConn], buf: bytes, flags: int = 0) -> None:  # noqa: ARG001
    timeout = socket.gettimeout()
    socket.settimeout(0.0)
    loop = asyncio.get_event_loop()
    try:
        await asyncio.wait_for(loop.sock_sendall(socket, buf), timeout=timeout)  # type: ignore[arg-type]
    finally:
        socket.settimeout(timeout)


def sendall(socket: Union[socket.socket, _sslConn], buf: bytes, flags: int = 0) -> None:  # noqa: ARG001
    socket.sendall(buf)
