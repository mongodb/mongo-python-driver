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

"""Async-only unit tests for network_layer.py."""

from __future__ import annotations

import asyncio
import struct
import sys
from unittest.mock import AsyncMock, MagicMock, patch

sys.path[0:0] = [""]

from pymongo.common import MAX_MESSAGE_SIZE
from pymongo.errors import ProtocolError
from pymongo.network_layer import PyMongoProtocol, _async_socket_receive
from test.asynchronous import AsyncUnitTest, unittest
from test.asynchronous.utils import make_msg_header


def _make_protocol(timeout=None):
    protocol = PyMongoProtocol(timeout=timeout)
    mock_transport = MagicMock()
    mock_transport.is_closing.return_value = False
    protocol.transport = mock_transport
    return protocol


class TestPyMongoProtocol(AsyncUnitTest):
    def _make_proto_with_header(self, header_bytes, max_size=MAX_MESSAGE_SIZE):
        protocol = _make_protocol()
        protocol._max_message_size = max_size
        protocol._header = memoryview(bytearray(header_bytes))
        return protocol

    async def test_normal_op_msg(self):
        header = make_msg_header(length=32, request_id=1, response_to=99, op_code=2013)
        protocol = self._make_proto_with_header(header)
        body_len, op_code, response_to, expecting_compression = protocol.process_header()
        self.assertEqual(body_len, 16)
        self.assertEqual(op_code, 2013)
        self.assertEqual(response_to, 99)
        self.assertFalse(expecting_compression)

    async def test_op_compressed(self):
        # OP_COMPRESSED=2012; process_header strips the 9-byte compression sub-header
        # (op code + uncompressed size + compressor id), then the 16-byte standard header.
        # length=35 → after compression sub-header: 26 → body: 10
        header = make_msg_header(length=35, request_id=1, response_to=0, op_code=2012)
        protocol = self._make_proto_with_header(header)
        body_len, op_code, _response_to, expecting_compression = protocol.process_header()
        self.assertEqual(body_len, 10)
        self.assertEqual(op_code, 2012)
        self.assertTrue(expecting_compression)

    async def test_op_compressed_length_too_small_raises(self):
        header = make_msg_header(length=25, request_id=1, response_to=0, op_code=2012)
        protocol = self._make_proto_with_header(header)
        with self.assertRaisesRegex(ProtocolError, "not longer than standard OP_COMPRESSED"):
            protocol.process_header()

    async def test_non_compressed_length_too_small_raises(self):
        header = make_msg_header(length=16, request_id=1, response_to=0, op_code=2013)
        protocol = self._make_proto_with_header(header)
        with self.assertRaisesRegex(ProtocolError, "not longer than standard message header size"):
            protocol.process_header()

    async def test_length_exceeds_max_raises(self):
        header = make_msg_header(
            length=MAX_MESSAGE_SIZE + 1, request_id=1, response_to=0, op_code=2013
        )
        protocol = self._make_proto_with_header(header)
        with self.assertRaisesRegex(ProtocolError, "larger than server max"):
            protocol.process_header()

    async def test_compression_header_snappy_compressor_id(self):
        protocol = _make_protocol()
        # <iiB: little-endian, i32 op code=2013, i32 uncompressed size=0, u8 compressor id=1 (snappy)
        data = struct.pack("<iiB", 2013, 0, 1)
        protocol._compression_header = memoryview(bytearray(data))
        op_code, compressor_id = protocol.process_compression_header()
        self.assertEqual(op_code, 2013)
        self.assertEqual(compressor_id, 1)

    async def test_close_aborts_transport(self):
        protocol = _make_protocol()
        protocol.close()
        self.assertTrue(protocol.transport.abort.called)

    async def test_close_with_exception_propagates_to_pending(self):
        protocol = _make_protocol()
        future = asyncio.get_running_loop().create_future()
        protocol._pending_messages.append(future)
        exc = OSError("connection reset")
        protocol.close(exc)
        with self.assertRaisesRegex(OSError, "connection reset"):
            await future

    async def test_buffer_updated_completes_pending_future(self):
        protocol = _make_protocol()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        protocol._pending_messages.append(future)

        # Feed a valid 32-byte OP_MSG header (16-byte header + 16-byte body).
        header = make_msg_header(length=32, request_id=1, response_to=99, op_code=2013)
        buf = protocol.get_buffer(16)
        buf[:16] = header
        protocol.buffer_updated(16)

        # Header processed: message buffer allocated, no longer expecting header.
        self.assertFalse(protocol._expecting_header)
        self.assertEqual(protocol._message_size, 16)

        # Feed the 16-byte body.
        buf = protocol.get_buffer(16)
        buf[:16] = b"x" * 16
        protocol.buffer_updated(16)

        # Future resolved with (op_code, compressor_id, response_to, data).
        self.assertTrue(future.done())
        op_code, compressor_id, response_to, _data = future.result()
        self.assertEqual(op_code, 2013)
        self.assertIsNone(compressor_id)
        self.assertEqual(response_to, 99)

    async def test_buffer_updated_zero_bytes_closes(self):
        protocol = _make_protocol()
        protocol.buffer_updated(0)
        self.assertTrue(protocol.transport.abort.called)

    async def test_buffer_updated_protocol_error_closes(self):
        protocol = _make_protocol()
        # length=16 triggers "not longer than standard message header size" in process_header.
        buf = protocol.get_buffer(16)
        buf[:16] = make_msg_header(length=16, request_id=1, response_to=0, op_code=2013)
        protocol.buffer_updated(16)
        self.assertTrue(protocol.transport.abort.called)


class TestAsyncSocketReceive(AsyncUnitTest):
    async def test_raises_on_connection_closed(self):
        # Covers the explicit `raise OSError("connection closed")` branch when
        # sock_recv_into returns 0.
        mock_socket = MagicMock()
        loop = asyncio.get_running_loop()

        with patch.object(loop, "sock_recv_into", new=AsyncMock(return_value=0)):
            with self.assertRaisesRegex(OSError, "connection closed"):
                await _async_socket_receive(mock_socket, 10, loop)


if __name__ == "__main__":
    unittest.main()
