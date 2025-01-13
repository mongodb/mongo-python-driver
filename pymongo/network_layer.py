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
import time
import traceback
from typing import (
    TYPE_CHECKING,
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

try:
    from ssl import SSLSocket

    _HAVE_SSL = True
except ImportError:
    _HAVE_SSL = False

try:
    from pymongo.pyopenssl_context import _sslConn

    _HAVE_PYOPENSSL = True
except ImportError:
    _HAVE_PYOPENSSL = False
    _sslConn = SSLSocket  # type: ignore

if TYPE_CHECKING:
    from pymongo.asynchronous.pool import AsyncConnection
    from pymongo.synchronous.pool import Connection

_UNPACK_HEADER = struct.Struct("<iiii").unpack
_UNPACK_COMPRESSION_HEADER = struct.Struct("<iiB").unpack
_POLL_TIMEOUT = 0.5


class NetworkingInterfaceBase:
    def __init__(
        self, conn: Union[socket.socket, _sslConn] | tuple[asyncio.BaseTransport, PyMongoProtocol]
    ):
        self.conn = conn

    def gettimeout(self):
        raise NotImplementedError

    def settimeout(self, timeout: float | None):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def is_closing(self) -> bool:
        raise NotImplementedError

    def get_conn(self):
        raise NotImplementedError

    def sock(self):
        raise NotImplementedError

class AsyncNetworkingInterface(NetworkingInterfaceBase):
    def __init__(self, conn: tuple[asyncio.BaseTransport, PyMongoProtocol]):
        super().__init__(conn)

    @property
    def gettimeout(self):
        return self.conn[1].gettimeout

    def settimeout(self, timeout: float | None):
        self.conn[1].settimeout(timeout)

    async def close(self):
        self.conn[0].abort()
        await self.conn[1].wait_closed()

    def is_closing(self):
        self.conn[0].is_closing()

    @property
    def get_conn(self) -> PyMongoProtocol:
        return self.conn[1]

    @property
    def sock(self):
        return self.conn[0].get_extra_info("socket")


class NetworkingInterface(NetworkingInterfaceBase):
    def __init__(self, conn: Union[socket.socket, _sslConn]):
        super().__init__(conn)

    def gettimeout(self):
        return self.conn.gettimeout()

    def settimeout(self, timeout: float | None):
        self.conn.settimeout(timeout)

    def close(self):
        self.conn.close()

    def is_closing(self):
        self.conn.is_closing()

    @property
    def get_conn(self):
        return self.conn

    @property
    def sock(self):
        return self.conn


class PyMongoProtocol(asyncio.BufferedProtocol):
    def __init__(self, timeout: Optional[float] = None, buffer_size: Optional[int] = 2**14):
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
        self._read_waiter = None
        self._timeout = timeout
        self._is_compressed = False
        self._compressor_id = None
        self._need_compression_header = False
        self._max_message_size = MAX_MESSAGE_SIZE
        self._request_id = None
        self._closed = asyncio.get_running_loop().create_future()
        self._debug = False


    def settimeout(self, timeout: float | None):
        self._timeout = timeout

    @property
    def gettimeout(self) -> float | None:
        """The configured timeout for the socket that underlies our protocol pair."""
        return self._timeout

    def connection_made(self, transport):
        """Called exactly once when a connection is made.
        The transport argument is the transport representing the write side of the connection.
        """
        self.transport = transport

    async def write(self, message: bytes):
        """Write a message to this connection's transport."""
        if self.transport.is_closing():
            raise OSError("Connection is closed")
        self.transport.write(message)
        await self._drain_helper()

    async def read(self, request_id: Optional[int], max_message_size: int, debug: bool = False):
        """Read a single MongoDB Wire Protocol message from this connection."""
        self._debug = debug
        self._max_message_size = max_message_size
        self._request_id = request_id
        self._length, self._overflow_length, self._body_length, self._op_code, self._overflow = (
            0,
            0,
            0,
            None,
            None,
        )
        if self.transport.is_closing():
            print("Connection is closed")
            raise OSError("Connection is closed")
        self._read_waiter = asyncio.get_running_loop().create_future()
        await self._read_waiter
        if self._read_waiter.done() and self._read_waiter.result():
            if self._debug:
                print("Read waiter done")
            header_size = 16
            if self._body_length > self._buffer_size:
                if self._is_compressed:
                    header_size = 25
                    return decompress(
                        memoryview(
                            bytearray(self._buffer[header_size : self._length])
                            + bytearray(self._overflow[: self._overflow_length])
                        ),
                        self._compressor_id,
                    ), self._op_code
                else:
                    return memoryview(
                        bytearray(self._buffer[header_size : self._length])
                        + bytearray(self._overflow[: self._overflow_length])
                    ), self._op_code
            else:
                if self._is_compressed:
                    header_size = 25
                    return decompress(
                        memoryview(self._buffer[header_size : self._body_length]),
                        self._compressor_id,
                    ), self._op_code
                else:
                    return memoryview(self._buffer[header_size : self._body_length]), self._op_code
        raise OSError("connection closed")

    def get_buffer(self, sizehint: int):
        """Called to allocate a new receive buffer."""
        if self._overflow is not None:
            if len(self._overflow[self._overflow_length:]) == 0:
                print(f"Overflow buffer overflow, overflow size of {len(self._overflow)}")
            return self._overflow[self._overflow_length:]
        if len(self._buffer[self._length:]) == 0:
            print(f"Default buffer overflow, overflow size of {len(self._buffer)}")
        return self._buffer[self._length:]

    def buffer_updated(self, nbytes: int):
        """Called when the buffer was updated with the received data"""
        if self._debug:
            print(f"buffer_updated for {nbytes}")
        if nbytes == 0:
            self.connection_lost(OSError("connection closed"))
            return
        else:
            if self._overflow is not None:
                self._overflow_length += nbytes
            else:
                if self._length == 0:
                    try:
                        self._body_length, self._op_code = self.process_header()
                    except ProtocolError as exc:
                        if self._debug:
                            print(f"Protocol error: {exc}")
                        self.connection_lost(exc)
                        return
                    if self._body_length > self._buffer_size:
                        self._overflow = memoryview(
                            bytearray(self._body_length - (self._buffer_size - nbytes) + 1000)
                        )
                self._length += nbytes
            if (
                self._length + self._overflow_length >= self._body_length
                and self._read_waiter
                and not self._read_waiter.done()
            ):
                if self._length > self._body_length:
                    self._body_length = self._length
                if self._length + self._overflow_length > self._body_length:
                    print(f"Done reading with length {self._length + self._overflow_length} out of {self._body_length}")
                self._read_waiter.set_result(True)

    def process_header(self):
        """Unpack a MongoDB Wire Protocol header."""
        length, _, response_to, op_code = _UNPACK_HEADER(self._buffer[:16])
        # No request_id for exhaust cursor "getMore".
        if self._request_id is not None:
            if self._request_id != response_to:
                raise ProtocolError(
                    f"Got response id {response_to!r} but expected {self._request_id!r}"
                )
        if length <= 16:
            raise ProtocolError(
                f"Message length ({length!r}) not longer than standard message header size (16)"
            )
        if length > self._max_message_size:
            raise ProtocolError(
                f"Message length ({length!r}) is larger than server max "
                f"message size ({self._max_message_size!r})"
            )
        if op_code == 2012:
            self._is_compressed = True
            if self._length >= 25:
                op_code, _, self._compressor_id = _UNPACK_COMPRESSION_HEADER(self._buffer[16:25])
            else:
                self._need_compression_header = True

        return length, op_code

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
        if self._read_waiter and not self._read_waiter.done():
            if exc is None:
                self._read_waiter.set_result(None)
            else:
                self._read_waiter.set_exception(exc)

        if not self._closed.done():
            if exc is None:
                self._closed.set_result(None)
            else:
                self._closed.set_exception(exc)

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
            raise ConnectionResetError("Connection lost")
        if not self._paused:
            return
        self._drain_waiter = asyncio.get_running_loop().create_future()
        await self._drain_waiter

    def data(self):
        return self._buffer

    async def wait_closed(self):
        await self._closed


async def async_sendall(conn: PyMongoProtocol, buf: bytes) -> None:
    try:
        await asyncio.wait_for(conn.write(buf), timeout=conn.gettimeout)
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


# Errors raised by sockets (and TLS sockets) when in non-blocking mode.
BLOCKING_IO_ERRORS = (BlockingIOError, *ssl_support.BLOCKING_IO_ERRORS)


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
                chunk_length = conn.conn.get_conn.recv_into(mv[bytes_read:])
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


async def async_receive_message(
    conn: AsyncConnection,
    request_id: Optional[int],
    max_message_size: int = MAX_MESSAGE_SIZE,
    debug: bool = False,
) -> Union[_OpReply, _OpMsg]:
    """Receive a raw BSON message or raise socket.error."""
    timeout: Optional[Union[float, int]]
    if _csot.get_timeout():
        deadline = _csot.get_deadline()
    else:
        timeout = conn.conn.get_conn.gettimeout
        if timeout:
            deadline = time.monotonic() + timeout
        else:
            deadline = None
    if deadline:
        # When the timeout has expired perform one final check to
        # see if the socket is readable. This helps avoid spurious
        # timeouts on AWS Lambda and other FaaS environments.
        timeout = max(deadline - time.monotonic(), 0)

    # if debug:
    #     print(f"async_receive_message with timeout: {timeout}. From csot: {_csot.get_timeout()}, from conn: {conn.conn.get_conn.gettimeout}, deadline: {deadline} ")
    # if timeout is None:
    #     timeout = 5.0


    cancellation_task = create_task(_poll_cancellation(conn))
    read_task = create_task(conn.conn.get_conn.read(request_id, max_message_size, debug))
    tasks = [read_task, cancellation_task]
    done, pending = await asyncio.wait(tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.wait(pending)
    if len(done) == 0:
        raise socket.timeout("timed out")
    if read_task in done:
        data, op_code = read_task.result()

        try:
            unpack_reply = _UNPACK_REPLY[op_code]
        except KeyError:
            raise ProtocolError(
                f"Got opcode {op_code!r} but expected {_UNPACK_REPLY.keys()!r}"
            ) from None
        return unpack_reply(data)
    raise _OperationCancelled("operation cancelled")


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
