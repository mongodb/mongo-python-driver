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
import sys
from unittest.mock import AsyncMock, MagicMock, patch

sys.path[0:0] = [""]

from pymongo.common import MAX_MESSAGE_SIZE
from pymongo.errors import ProtocolError
from pymongo.network_layer import PyMongoProtocol, _async_socket_receive
from test.asynchronous import AsyncUnitTest, unittest
from test.utils_shared import make_msg_header


def _make_protocol(timeout=None):
    protocol = PyMongoProtocol(timeout=timeout)
    mock_transport = MagicMock()
    mock_transport.is_closing.return_value = False
    protocol.transport = mock_transport
    return protocol


class TestProcessHeader(AsyncUnitTest):
    async def asyncSetUp(self):
        self.protocol = _make_protocol()

    def test_op_msg_returns_body_len_and_op_code(self):
        self.protocol._header = memoryview(
            bytearray(make_msg_header(length=32, request_id=1, response_to=99, op_code=2013))
        )
        body_len, op_code, response_to, expecting_compression = self.protocol.process_header()
        self.assertEqual(body_len, 16)
        self.assertEqual(op_code, 2013)
        self.assertEqual(response_to, 99)
        self.assertFalse(expecting_compression)

    def test_op_compressed_sets_expecting_compression(self):
        # OP_COMPRESSED=2012; process_header strips the 9-byte compression sub-header
        # (op code + uncompressed size + compressor id), then the 16-byte standard header.
        # length=35 → after compression sub-header: 26 → body: 10
        self.protocol._header = memoryview(
            bytearray(make_msg_header(length=35, request_id=1, response_to=0, op_code=2012))
        )
        body_len, op_code, _response_to, expecting_compression = self.protocol.process_header()
        self.assertEqual(body_len, 10)
        self.assertEqual(op_code, 2012)
        self.assertTrue(expecting_compression)

    def test_op_compressed_length_too_small_raises(self):
        self.protocol._header = memoryview(
            bytearray(make_msg_header(length=25, request_id=1, response_to=0, op_code=2012))
        )
        with self.assertRaisesRegex(ProtocolError, "not longer than standard OP_COMPRESSED"):
            self.protocol.process_header()

    def test_non_compressed_length_too_small_raises(self):
        self.protocol._header = memoryview(
            bytearray(make_msg_header(length=16, request_id=1, response_to=0, op_code=2013))
        )
        with self.assertRaisesRegex(ProtocolError, "not longer than standard message header size"):
            self.protocol.process_header()

    def test_length_exceeds_max_raises(self):
        self.protocol._header = memoryview(
            bytearray(
                make_msg_header(
                    length=MAX_MESSAGE_SIZE + 1, request_id=1, response_to=0, op_code=2013
                )
            )
        )
        with self.assertRaisesRegex(ProtocolError, "larger than server max"):
            self.protocol.process_header()


class TestClose(AsyncUnitTest):
    async def asyncSetUp(self):
        self.protocol = _make_protocol()

    def test_close_aborts_transport(self):
        self.protocol.close()
        self.assertTrue(self.protocol.transport.abort.called)

    async def test_close_propagates_exception_to_pending_read(self):
        read_task = asyncio.create_task(
            self.protocol.read(request_id=None, max_message_size=MAX_MESSAGE_SIZE)
        )
        await asyncio.sleep(0)
        self.protocol.close(OSError("connection reset"))
        with self.assertRaisesRegex(OSError, "connection reset"):
            await read_task


class TestBufferUpdated(AsyncUnitTest):
    async def asyncSetUp(self):
        self.protocol = _make_protocol()

    def test_zero_bytes_closes_connection(self):
        self.protocol.buffer_updated(0)
        self.assertTrue(self.protocol.transport.abort.called)

    def test_protocol_error_closes_connection(self):
        buf = self.protocol.get_buffer(16)
        buf[:16] = make_msg_header(length=16, request_id=1, response_to=0, op_code=2013)
        self.protocol.buffer_updated(16)
        self.assertTrue(self.protocol.transport.abort.called)

    async def test_resolves_pending_read(self):
        read_task = asyncio.create_task(
            self.protocol.read(request_id=None, max_message_size=MAX_MESSAGE_SIZE)
        )
        await asyncio.sleep(0)

        # Feed a valid 32-byte OP_MSG header (16-byte header + 16-byte body).
        header = make_msg_header(length=32, request_id=1, response_to=99, op_code=2013)
        buf = self.protocol.get_buffer(16)
        buf[:16] = header
        self.protocol.buffer_updated(16)

        self.assertFalse(self.protocol._expecting_header)
        self.assertEqual(self.protocol._message_size, 16)

        # Feed the 16-byte body.
        buf = self.protocol.get_buffer(16)
        buf[:16] = b"x" * 16
        self.protocol.buffer_updated(16)

        _data, op_code = await read_task
        self.assertEqual(op_code, 2013)


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
