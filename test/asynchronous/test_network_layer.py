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
    protocol = PyMongoProtocol(timeout=timeout)
    mock_transport = MagicMock()
    mock_transport.is_closing.return_value = False
    protocol.transport = mock_transport
    return protocol


def _make_header(length, request_id, response_to, op_code):
    return struct.pack("<iiii", length, request_id, response_to, op_code)


class TestSendall(AsyncUnitTest):
    def test_delegates_to_sock_sendall(self):
        mock_socket = MagicMock()
        sendall(mock_socket, b"hello")
        mock_socket.sendall.assert_called_once_with(b"hello")


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
        self.mock_socket = MagicMock()
        self.network_interface = NetworkingInterface(self.mock_socket)

    def test_gettimeout_delegates(self):
        self.mock_socket.gettimeout.return_value = 5.0
        self.assertEqual(self.network_interface.gettimeout(), 5.0)

    def test_settimeout_delegates(self):
        self.network_interface.settimeout(3.0)
        self.mock_socket.settimeout.assert_called_once_with(3.0)

    def test_close_delegates(self):
        self.network_interface.close()
        self.mock_socket.close.assert_called_once()

    def test_is_closing_delegates(self):
        self.mock_socket.is_closing.return_value = True
        self.assertTrue(self.network_interface.is_closing())

    def test_fileno_delegates(self):
        self.mock_socket.fileno.return_value = 42
        self.assertEqual(self.network_interface.fileno(), 42)

    def test_recv_into_delegates(self):
        buf = memoryview(bytearray(10))
        self.mock_socket.recv_into.return_value = 7
        result = self.network_interface.recv_into(buf)
        self.assertEqual(result, 7)
        self.mock_socket.recv_into.assert_called_once_with(buf)

    def test_get_conn_returns_socket(self):
        self.assertIs(self.network_interface.get_conn, self.mock_socket)

    def test_sock_returns_socket(self):
        self.assertIs(self.network_interface.sock, self.mock_socket)


if not _IS_SYNC:

    class TestAsyncNetworkingInterface(AsyncUnitTest):
        def _make_network_interface(self):
            mock_transport = MagicMock()
            mock_protocol = MagicMock()
            mock_protocol.gettimeout = 10.0
            return AsyncNetworkingInterface((mock_transport, mock_protocol))

        def test_gettimeout_returns_protocol_timeout(self):
            network_interface = self._make_network_interface()
            self.assertEqual(network_interface.gettimeout, 10.0)

        def test_settimeout_delegates_to_protocol(self):
            network_interface = self._make_network_interface()
            network_interface.settimeout(7.0)
            network_interface.conn[1].settimeout.assert_called_once_with(7.0)

        def test_is_closing_delegates_to_transport(self):
            network_interface = self._make_network_interface()
            network_interface.conn[0].is_closing.return_value = False
            self.assertFalse(network_interface.is_closing())

        def test_get_conn_returns_protocol(self):
            network_interface = self._make_network_interface()
            self.assertIs(network_interface.get_conn, network_interface.conn[1])

        def test_sock_returns_transport_socket(self):
            network_interface = self._make_network_interface()
            sentinel = object()
            network_interface.conn[0].get_extra_info.return_value = sentinel
            self.assertIs(network_interface.sock, sentinel)
            network_interface.conn[0].get_extra_info.assert_called_once_with("socket")

    class TestPyMongoProtocol(AsyncUnitTest):
        async def _make_proto_with_header(self, header_bytes, max_size=MAX_MESSAGE_SIZE):
            protocol = await _make_protocol()
            protocol._max_message_size = max_size
            protocol._header = memoryview(bytearray(header_bytes))
            return protocol

        async def test_initial_timeout_from_constructor(self):
            protocol = await _make_protocol(timeout=3.0)
            self.assertEqual(protocol.gettimeout, 3.0)

        async def test_settimeout_updates_value(self):
            protocol = await _make_protocol()
            protocol.settimeout(7.5)
            self.assertEqual(protocol.gettimeout, 7.5)

        async def test_default_timeout_is_none(self):
            protocol = await _make_protocol()
            self.assertIsNone(protocol.gettimeout)

        async def test_normal_op_msg(self):
            header = _make_header(32, 1, 99, 2013)
            protocol = await self._make_proto_with_header(header)
            body_len, op_code, response_to, expecting_compression = protocol.process_header()
            self.assertEqual(body_len, 16)
            self.assertEqual(op_code, 2013)
            self.assertEqual(response_to, 99)
            self.assertFalse(expecting_compression)

        async def test_op_compressed(self):
            # OP_COMPRESSED=2012, length=35 → adjusted=35-9=26 → body=26-16=10
            header = _make_header(35, 1, 0, 2012)
            protocol = await self._make_proto_with_header(header)
            body_len, op_code, _response_to, expecting_compression = protocol.process_header()
            self.assertEqual(body_len, 10)
            self.assertEqual(op_code, 2012)
            self.assertTrue(expecting_compression)

        async def test_op_compressed_length_too_small_raises(self):
            header = _make_header(25, 1, 0, 2012)
            protocol = await self._make_proto_with_header(header)
            with self.assertRaises(ProtocolError):
                protocol.process_header()

        async def test_non_compressed_length_too_small_raises(self):
            header = _make_header(16, 1, 0, 2013)
            protocol = await self._make_proto_with_header(header)
            with self.assertRaises(ProtocolError):
                protocol.process_header()

        async def test_length_exceeds_max_raises(self):
            header = _make_header(MAX_MESSAGE_SIZE + 1, 1, 0, 2013)
            protocol = await self._make_proto_with_header(header)
            with self.assertRaises(ProtocolError):
                protocol.process_header()

        async def test_op_reply_op_code(self):
            header = _make_header(20, 0, 0, 1)
            protocol = await self._make_proto_with_header(header)
            body_len, op_code, _response_to, expecting_compression = protocol.process_header()
            self.assertEqual(body_len, 4)
            self.assertEqual(op_code, 1)
            self.assertFalse(expecting_compression)

        async def test_compression_header_returns_op_code_and_compressor_id(self):
            protocol = await _make_protocol()
            # op_code=2013, uncompressed_size=0, compressor_id=1 (snappy)
            data = struct.pack("<iiB", 2013, 0, 1)
            protocol._compression_header = memoryview(bytearray(data))
            op_code, compressor_id = protocol.process_compression_header()
            self.assertEqual(op_code, 2013)
            self.assertEqual(compressor_id, 1)

        async def test_compression_header_zlib_compressor_id(self):
            protocol = await _make_protocol()
            data = struct.pack("<iiB", 2013, 0, 2)
            protocol._compression_header = memoryview(bytearray(data))
            _, compressor_id = protocol.process_compression_header()
            self.assertEqual(compressor_id, 2)

        async def test_message_complete_resolves_pending_future(self):
            protocol = await _make_protocol()
            protocol._expecting_header = False
            protocol._expecting_compression = False
            protocol._message_size = 10
            protocol._message = memoryview(bytearray(10))
            protocol._message_index = 0
            protocol._op_code = 2013
            protocol._compressor_id = None
            protocol._response_to = 42

            future = asyncio.get_running_loop().create_future()
            protocol._pending_messages.append(future)

            protocol.buffer_updated(10)
            self.assertTrue(future.done())
            op_code, compressor_id, response_to, _ = future.result()
            self.assertEqual(op_code, 2013)
            self.assertIsNone(compressor_id)
            self.assertEqual(response_to, 42)

        async def test_close_aborts_transport(self):
            protocol = await _make_protocol()
            protocol.close()
            self.assertTrue(protocol.transport.abort.called)

        async def test_connection_lost_twice_does_not_raise(self):
            protocol = await _make_protocol()
            protocol.connection_lost(None)
            protocol.connection_lost(None)

        async def test_close_with_exception_propagates_to_pending(self):
            protocol = await _make_protocol()
            future = asyncio.get_running_loop().create_future()
            protocol._pending_messages.append(future)
            exc = OSError("connection reset")
            protocol.close(exc)
            with self.assertRaises(OSError) as ctx:
                await future
            self.assertIn("connection reset", str(ctx.exception))

    class TestAsyncSocketReceive(AsyncUnitTest):
        async def test_reads_full_data_in_one_call(self):
            data = b"hello world!"
            length = len(data)
            mock_socket = MagicMock()
            loop = asyncio.get_running_loop()

            async def fake_recv_into(sock, buf):
                buf[:length] = data
                return length

            with patch.object(loop, "sock_recv_into", new=AsyncMock(side_effect=fake_recv_into)):
                result = await _async_socket_receive(mock_socket, length, loop)
            self.assertEqual(bytes(result), data)

        async def test_reads_data_in_multiple_chunks(self):
            data = b"abcdefgh"
            length = len(data)
            chunk1, chunk2 = data[:4], data[4:]
            mock_socket = MagicMock()
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
                result = await _async_socket_receive(mock_socket, length, loop)
            self.assertEqual(bytes(result), data)

        async def test_raises_on_connection_closed(self):
            mock_socket = MagicMock()
            loop = asyncio.get_running_loop()

            async def fake_recv_into(sock, buf):
                return 0

            with patch.object(loop, "sock_recv_into", new=AsyncMock(side_effect=fake_recv_into)):
                with self.assertRaises(OSError) as ctx:
                    await _async_socket_receive(mock_socket, 10, loop)
            self.assertIn("connection closed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
