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

        def _make_async_iface(self):
            mock_transport = MagicMock()
            mock_protocol = MagicMock()
            mock_protocol.gettimeout = 10.0
            return AsyncNetworkingInterface((mock_transport, mock_protocol))

        def test_async_gettimeout_returns_protocol_timeout(self):
            iface = self._make_async_iface()
            self.assertEqual(iface.gettimeout, 10.0)

        def test_async_settimeout_delegates_to_protocol(self):
            iface = self._make_async_iface()
            iface.settimeout(7.0)
            iface.conn[1].settimeout.assert_called_once_with(7.0)

        def test_async_is_closing_delegates_to_transport(self):
            iface = self._make_async_iface()
            iface.conn[0].is_closing.return_value = False
            self.assertFalse(iface.is_closing())

        def test_async_get_conn_returns_protocol(self):
            iface = self._make_async_iface()
            self.assertIs(iface.get_conn, iface.conn[1])

        def test_async_sock_returns_transport_socket(self):
            iface = self._make_async_iface()
            sentinel = object()
            iface.conn[0].get_extra_info.return_value = sentinel
            self.assertIs(iface.sock, sentinel)
            iface.conn[0].get_extra_info.assert_called_once_with("socket")


class TestPyMongoProtocolTimeout(AsyncUnitTest):
    if not _IS_SYNC:

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


class TestPyMongoProtocolProcessHeader(AsyncUnitTest):
    if not _IS_SYNC:

        async def _make_proto_with_header(self, header_bytes, max_size=MAX_MESSAGE_SIZE):
            proto = await _make_protocol()
            proto._max_message_size = max_size
            proto._header = memoryview(bytearray(header_bytes))
            return proto

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


class TestPyMongoProtocolProcessCompressionHeader(AsyncUnitTest):
    if not _IS_SYNC:

        async def test_returns_op_code_and_compressor_id(self):
            proto = await _make_protocol()
            # op_code=2013, uncompressed_size=0, compressor_id=1 (snappy)
            data = struct.pack("<iiB", 2013, 0, 1)
            proto._compression_header = memoryview(bytearray(data))
            op_code, compressor_id = proto.process_compression_header()
            self.assertEqual(op_code, 2013)
            self.assertEqual(compressor_id, 1)

        async def test_zlib_compressor_id(self):
            proto = await _make_protocol()
            data = struct.pack("<iiB", 2013, 0, 2)
            proto._compression_header = memoryview(bytearray(data))
            _, compressor_id = proto.process_compression_header()
            self.assertEqual(compressor_id, 2)


class TestPyMongoProtocolGetBuffer(AsyncUnitTest):
    if not _IS_SYNC:

        async def test_expecting_header_returns_full_header_slice(self):
            proto = await _make_protocol()
            proto._expecting_header = True
            proto._header_index = 0
            self.assertEqual(len(proto.get_buffer(0)), 16)

        async def test_expecting_header_partial_returns_remaining(self):
            proto = await _make_protocol()
            proto._expecting_header = True
            proto._header_index = 8
            self.assertEqual(len(proto.get_buffer(0)), 8)

        async def test_expecting_compression_returns_compression_slice(self):
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = True
            proto._compression_index = 0
            self.assertEqual(len(proto.get_buffer(0)), 9)

        async def test_expecting_compression_partial(self):
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = True
            proto._compression_index = 5
            self.assertEqual(len(proto.get_buffer(0)), 4)

        async def test_message_body_returns_remaining_slice(self):
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = False
            proto._message = memoryview(bytearray(100))
            proto._message_index = 0
            self.assertEqual(len(proto.get_buffer(0)), 100)

        async def test_connection_lost_allocates_drain_buffer(self):
            proto = await _make_protocol()
            proto._connection_lost = True
            proto._message = None
            self.assertEqual(len(proto.get_buffer(0)), 2**14)

        async def test_connection_lost_reuses_existing_buffer(self):
            proto = await _make_protocol()
            proto._connection_lost = True
            proto._message = memoryview(bytearray(50))
            self.assertEqual(len(proto.get_buffer(0)), 50)


class TestPyMongoProtocolBufferUpdated(AsyncUnitTest):
    if not _IS_SYNC:

        async def test_zero_bytes_closes_connection(self):
            proto = await _make_protocol()
            proto.buffer_updated(0)
            self.assertTrue(proto._connection_lost)

        async def test_connection_lost_returns_early(self):
            proto = await _make_protocol()
            proto._connection_lost = True
            proto._header_index = 3
            proto.buffer_updated(5)
            self.assertEqual(proto._header_index, 3)

        async def test_partial_header_increments_index(self):
            proto = await _make_protocol()
            proto._expecting_header = True
            proto._header_index = 0
            proto.buffer_updated(8)
            self.assertEqual(proto._header_index, 8)

        async def test_full_header_transitions_to_message(self):
            proto = await _make_protocol()
            proto._expecting_header = True
            hdr = _make_header(32, 1, 0, 2013)
            proto._header = memoryview(bytearray(hdr))
            proto._header_index = 0
            proto.buffer_updated(16)
            self.assertFalse(proto._expecting_header)
            self.assertEqual(proto._message_size, 16)

        async def test_invalid_header_closes_connection(self):
            proto = await _make_protocol()
            proto._expecting_header = True
            # length=16 (not > 16) triggers ProtocolError
            hdr = _make_header(16, 1, 0, 2013)
            proto._header = memoryview(bytearray(hdr))
            proto._header_index = 0
            proto.buffer_updated(16)
            self.assertTrue(proto._connection_lost)

        async def test_compression_header_processing(self):
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = True
            comp_hdr = struct.pack("<iiB", 2013, 0, 2)
            proto._compression_header = memoryview(bytearray(comp_hdr))
            proto._compression_index = 0
            proto.buffer_updated(9)
            self.assertFalse(proto._expecting_compression)
            self.assertEqual(proto._op_code, 2013)
            self.assertEqual(proto._compressor_id, 2)

        async def test_partial_compression_header_increments_index(self):
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = True
            proto._compression_index = 0
            proto.buffer_updated(4)
            self.assertEqual(proto._compression_index, 4)
            self.assertTrue(proto._expecting_compression)

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

        async def test_message_complete_no_pending_creates_new_future(self):
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = False
            proto._message_size = 5
            proto._message = memoryview(bytearray(5))
            proto._message_index = 0
            proto._op_code = 2013
            proto._compressor_id = None
            proto._response_to = 0

            self.assertFalse(proto._pending_messages)
            proto.buffer_updated(5)
            self.assertEqual(len(proto._done_messages), 1)

        async def test_partial_message_increments_index(self):
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = False
            proto._message_size = 20
            proto._message = memoryview(bytearray(20))
            proto._message_index = 0
            proto.buffer_updated(7)
            self.assertEqual(proto._message_index, 7)


class TestPyMongoProtocolClose(AsyncUnitTest):
    if not _IS_SYNC:

        async def test_close_sets_connection_lost_flag(self):
            proto = await _make_protocol()
            proto.close()
            self.assertTrue(proto._connection_lost)

        async def test_close_aborts_transport(self):
            proto = await _make_protocol()
            proto.close()
            self.assertTrue(proto.transport.abort.called)

        async def test_connection_lost_resolves_closed_future(self):
            proto = await _make_protocol()
            self.assertFalse(proto._closed.done())
            proto.connection_lost(None)
            self.assertTrue(proto._closed.done())

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
    if not _IS_SYNC:

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
            calls = [0]

            async def fake_recv_into(sock, buf):
                if calls[0] == 0:
                    buf[: len(chunk1)] = chunk1
                    calls[0] += 1
                    return len(chunk1)
                buf[: len(chunk2)] = chunk2
                calls[0] += 1
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
