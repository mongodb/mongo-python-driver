# Copyright 2026-present MongoDB, Inc.
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

"""Unit tests for network_layer.py."""

from __future__ import annotations

import asyncio
import struct
import sys
from unittest.mock import AsyncMock, MagicMock, patch

sys.path[0:0] = [""]

from test.asynchronous import AsyncUnitTest, unittest

from pymongo.common import MAX_MESSAGE_SIZE
from pymongo.errors import ProtocolError
from pymongo.network_layer import (
    AsyncNetworkingInterface,
    NetworkingInterface,
    NetworkingInterfaceBase,
    PyMongoProtocol,
    _async_socket_receive,
    sendall,
)

_IS_SYNC = False


async def _make_protocol(timeout=None):
    proto = PyMongoProtocol(timeout=timeout)
    mock_transport = MagicMock()
    mock_transport.is_closing.return_value = False
    proto.transport = mock_transport
    return proto


def _make_header(length, request_id, response_to, op_code):
    return struct.pack("<iiii", length, request_id, response_to, op_code)


class TestSendall(AsyncUnitTest):
    def test_delegates_to_sock_sendall(self):
        mock_sock = MagicMock()
        sendall(mock_sock, b"hello")
        mock_sock.sendall.assert_called_once_with(b"hello")


class TestNetworkingInterfaceBase(AsyncUnitTest):
    def setUp(self):
        self.base = NetworkingInterfaceBase(MagicMock())

    def test_gettimeout_raises(self):
        with self.assertRaises(NotImplementedError):
            _ = self.base.gettimeout

    def test_settimeout_raises(self):
        with self.assertRaises(NotImplementedError):
            self.base.settimeout(1.0)

    def test_close_raises(self):
        with self.assertRaises(NotImplementedError):
            self.base.close()

    def test_is_closing_raises(self):
        with self.assertRaises(NotImplementedError):
            self.base.is_closing()

    def test_get_conn_raises(self):
        with self.assertRaises(NotImplementedError):
            _ = self.base.get_conn

    def test_sock_raises(self):
        with self.assertRaises(NotImplementedError):
            _ = self.base.sock


class TestNetworkingInterface(AsyncUnitTest):
    def setUp(self):
        self.mock_sock = MagicMock()
        self.iface = NetworkingInterface(self.mock_sock)

    def test_gettimeout_delegates(self):
        self.mock_sock.gettimeout.return_value = 5.0
        self.assertEqual(self.iface.gettimeout(), 5.0)

    def test_settimeout_delegates(self):
        self.iface.settimeout(3.0)
        self.mock_sock.settimeout.assert_called_once_with(3.0)

    def test_close_delegates(self):
        self.iface.close()
        self.mock_sock.close.assert_called_once()

    def test_is_closing_delegates(self):
        self.mock_sock.is_closing.return_value = True
        self.assertTrue(self.iface.is_closing())

    def test_fileno_delegates(self):
        self.mock_sock.fileno.return_value = 42
        self.assertEqual(self.iface.fileno(), 42)

    def test_recv_into_delegates(self):
        buf = memoryview(bytearray(10))
        self.mock_sock.recv_into.return_value = 7
        result = self.iface.recv_into(buf)
        self.assertEqual(result, 7)
        self.mock_sock.recv_into.assert_called_once_with(buf)

    def test_get_conn_returns_socket(self):
        self.assertIs(self.iface.get_conn, self.mock_sock)

    def test_sock_returns_socket(self):
        self.assertIs(self.iface.sock, self.mock_sock)


if not _IS_SYNC:

    class TestAsyncNetworkingInterface(AsyncUnitTest):
        def _make_iface(self):
            mock_transport = MagicMock()
            mock_protocol = MagicMock()
            mock_protocol.gettimeout = 10.0
            return AsyncNetworkingInterface((mock_transport, mock_protocol))

        def test_gettimeout_returns_protocol_timeout(self):
            iface = self._make_iface()
            self.assertEqual(iface.gettimeout, 10.0)

        def test_settimeout_delegates_to_protocol(self):
            iface = self._make_iface()
            iface.settimeout(7.0)
            iface.conn[1].settimeout.assert_called_once_with(7.0)

        def test_is_closing_delegates_to_transport(self):
            iface = self._make_iface()
            iface.conn[0].is_closing.return_value = False
            self.assertFalse(iface.is_closing())

        def test_get_conn_returns_protocol(self):
            iface = self._make_iface()
            self.assertIs(iface.get_conn, iface.conn[1])

        def test_sock_returns_transport_socket(self):
            iface = self._make_iface()
            sentinel = object()
            iface.conn[0].get_extra_info.return_value = sentinel
            self.assertIs(iface.sock, sentinel)
            iface.conn[0].get_extra_info.assert_called_once_with("socket")

    class TestPyMongoProtocol(AsyncUnitTest):
        async def _make_proto_with_header(self, header_bytes, max_size=MAX_MESSAGE_SIZE):
            proto = await _make_protocol()
            proto._max_message_size = max_size
            proto._header = memoryview(bytearray(header_bytes))
            return proto

        async def test_initial_timeout_from_constructor(self):
            proto = await _make_protocol(timeout=3.0)
            self.assertEqual(proto.gettimeout, 3.0)

        async def test_settimeout_updates_value(self):
            proto = await _make_protocol()
            proto.settimeout(7.5)
            self.assertEqual(proto.gettimeout, 7.5)

        async def test_default_timeout_is_none(self):
            proto = await _make_protocol()
            self.assertIsNone(proto.gettimeout)

        async def test_normal_op_msg(self):
            hdr = _make_header(32, 1, 99, 2013)
            proto = await self._make_proto_with_header(hdr)
            body_len, op_code, response_to, expecting_compression = proto.process_header()
            self.assertEqual(body_len, 16)
            self.assertEqual(op_code, 2013)
            self.assertEqual(response_to, 99)
            self.assertFalse(expecting_compression)

        async def test_op_compressed(self):
            # OP_COMPRESSED=2012, length=35 → adjusted=35-9=26 → body=26-16=10
            hdr = _make_header(35, 1, 0, 2012)
            proto = await self._make_proto_with_header(hdr)
            body_len, op_code, _response_to, expecting_compression = proto.process_header()
            self.assertEqual(body_len, 10)
            self.assertEqual(op_code, 2012)
            self.assertTrue(expecting_compression)

        async def test_op_compressed_length_too_small_raises(self):
            hdr = _make_header(25, 1, 0, 2012)
            proto = await self._make_proto_with_header(hdr)
            with self.assertRaises(ProtocolError):
                proto.process_header()

        async def test_non_compressed_length_too_small_raises(self):
            hdr = _make_header(16, 1, 0, 2013)
            proto = await self._make_proto_with_header(hdr)
            with self.assertRaises(ProtocolError):
                proto.process_header()

        async def test_length_exceeds_max_raises(self):
            hdr = _make_header(MAX_MESSAGE_SIZE + 1, 1, 0, 2013)
            proto = await self._make_proto_with_header(hdr)
            with self.assertRaises(ProtocolError):
                proto.process_header()

        async def test_op_reply_op_code(self):
            hdr = _make_header(20, 0, 0, 1)
            proto = await self._make_proto_with_header(hdr)
            body_len, op_code, _response_to, expecting_compression = proto.process_header()
            self.assertEqual(body_len, 4)
            self.assertEqual(op_code, 1)
            self.assertFalse(expecting_compression)

        async def test_compression_header_returns_op_code_and_compressor_id(self):
            proto = await _make_protocol()
            # op_code=2013, uncompressed_size=0, compressor_id=1 (snappy)
            data = struct.pack("<iiB", 2013, 0, 1)
            proto._compression_header = memoryview(bytearray(data))
            op_code, compressor_id = proto.process_compression_header()
            self.assertEqual(op_code, 2013)
            self.assertEqual(compressor_id, 1)

        async def test_compression_header_zlib_compressor_id(self):
            proto = await _make_protocol()
            data = struct.pack("<iiB", 2013, 0, 2)
            proto._compression_header = memoryview(bytearray(data))
            _, compressor_id = proto.process_compression_header()
            self.assertEqual(compressor_id, 2)

        async def test_message_complete_resolves_pending_future(self):
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = False
            proto._message_size = 10
            proto._message = memoryview(bytearray(10))
            proto._message_index = 0
            proto._op_code = 2013
            proto._compressor_id = None
            proto._response_to = 42

            fut = asyncio.get_running_loop().create_future()
            proto._pending_messages.append(fut)

            proto.buffer_updated(10)
            self.assertTrue(fut.done())
            op_code, compressor_id, response_to, _ = fut.result()
            self.assertEqual(op_code, 2013)
            self.assertIsNone(compressor_id)
            self.assertEqual(response_to, 42)

        async def test_close_aborts_transport(self):
            proto = await _make_protocol()
            proto.close()
            self.assertTrue(proto.transport.abort.called)

        async def test_connection_lost_twice_does_not_raise(self):
            proto = await _make_protocol()
            proto.connection_lost(None)
            proto.connection_lost(None)

        async def test_close_with_exception_propagates_to_pending(self):
            proto = await _make_protocol()
            fut = asyncio.get_running_loop().create_future()
            proto._pending_messages.append(fut)
            exc = OSError("connection reset")
            proto.close(exc)
            with self.assertRaises(OSError) as ctx:
                await fut
            self.assertIn("connection reset", str(ctx.exception))

    class TestAsyncSocketReceive(AsyncUnitTest):
        async def test_reads_full_data_in_one_call(self):
            data = b"hello world!"
            length = len(data)
            mock_sock = MagicMock()
            loop = asyncio.get_running_loop()

            async def fake_recv_into(sock, buf):
                buf[:length] = data
                return length

            with patch.object(loop, "sock_recv_into", new=AsyncMock(side_effect=fake_recv_into)):
                result = await _async_socket_receive(mock_sock, length, loop)
            self.assertEqual(bytes(result), data)

        async def test_reads_data_in_multiple_chunks(self):
            data = b"abcdefgh"
            length = len(data)
            chunk1, chunk2 = data[:4], data[4:]
            mock_sock = MagicMock()
            loop = asyncio.get_running_loop()
            calls = 0

            async def fake_recv_into(sock, buf):
                nonlocal calls
                if calls == 0:
                    buf[: len(chunk1)] = chunk1
                    calls += 1
                    return len(chunk1)
                buf[: len(chunk2)] = chunk2
                calls += 1
                return len(chunk2)

            with patch.object(loop, "sock_recv_into", new=AsyncMock(side_effect=fake_recv_into)):
                result = await _async_socket_receive(mock_sock, length, loop)
            self.assertEqual(bytes(result), data)

        async def test_raises_on_connection_closed(self):
            mock_sock = MagicMock()
            loop = asyncio.get_running_loop()

            async def fake_recv_into(sock, buf):
                return 0

            with patch.object(loop, "sock_recv_into", new=AsyncMock(side_effect=fake_recv_into)):
                with self.assertRaises(OSError) as ctx:
                    await _async_socket_receive(mock_sock, 10, loop)
            self.assertIn("connection closed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
