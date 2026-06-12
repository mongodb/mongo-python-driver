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

"""Sync-only unit tests for network_layer.py.

These cover ``receive_message`` and ``receive_data``, which only exist on the
synchronous receive path (the async path uses ``PyMongoProtocol`` instead).
The async-only tests live in ``test/asynchronous/test_async_network_layer.py``.
"""

from __future__ import annotations

import struct
import sys
from unittest.mock import MagicMock, patch

sys.path[0:0] = [""]

from test import UnitTest, unittest

from pymongo import network_layer
from pymongo.common import MAX_MESSAGE_SIZE
from pymongo.errors import ProtocolError


def _make_header(length, request_id, response_to, op_code):
    return struct.pack("<iiii", length, request_id, response_to, op_code)


def _make_compression_header(op_code, uncompressed_size, compressor_id):
    return struct.pack("<iiB", op_code, uncompressed_size, compressor_id)


def _make_conn():
    conn = MagicMock()
    conn.conn.gettimeout.return_value = None
    # PyPy calls wait_for_read() before recv_into(), which checks fileno() == -1
    # as an early-exit. Without this, sock.fileno() returns a MagicMock and the
    # subsequent sock.pending() > 0 comparison raises TypeError on PyPy.
    conn.conn.sock.fileno.return_value = -1
    return conn


class TestReceiveMessage(UnitTest):
    def _patch_receive_data(self, *chunks):
        """Make receive_data return the given byte strings on successive calls."""
        mock = patch.object(network_layer, "receive_data", side_effect=list(chunks))
        self.addCleanup(mock.stop)
        return mock.start()

    def test_request_id_mismatch_raises(self):
        self._patch_receive_data(
            _make_header(length=32, request_id=0, response_to=99, op_code=2013)
        )
        with self.assertRaises(ProtocolError):
            network_layer.receive_message(_make_conn(), request_id=1)

    def test_length_too_small_raises(self):
        self._patch_receive_data(_make_header(length=16, request_id=0, response_to=0, op_code=2013))
        with self.assertRaisesRegex(ProtocolError, "not longer than standard message header"):
            network_layer.receive_message(_make_conn(), request_id=None)

    def test_length_exceeds_max_raises(self):
        self._patch_receive_data(
            _make_header(length=MAX_MESSAGE_SIZE + 1, request_id=0, response_to=0, op_code=2013)
        )
        with self.assertRaisesRegex(ProtocolError, "larger than server max"):
            network_layer.receive_message(_make_conn(), request_id=None)

    def test_normal_op_msg_unpacks(self):
        body = b"x" * 16
        self._patch_receive_data(
            _make_header(length=32, request_id=0, response_to=0, op_code=2013), body
        )
        unpack = MagicMock(return_value="REPLY")
        with patch.object(network_layer, "_UNPACK_REPLY", {2013: unpack}):
            result = network_layer.receive_message(_make_conn(), request_id=None)
        unpack.assert_called_once_with(body)
        self.assertEqual(result, "REPLY")

    def test_op_compressed_decompresses(self):
        # length=35 -> body length = 35 - 25 = 10 (header 16 + compression sub-header 9).
        compressed_body = b"y" * 10
        self._patch_receive_data(
            _make_header(length=35, request_id=0, response_to=0, op_code=2012),
            _make_compression_header(op_code=2013, uncompressed_size=0, compressor_id=1),
            compressed_body,
        )
        unpack = MagicMock(return_value="REPLY")
        with (
            patch.object(network_layer, "decompress", return_value=b"decompressed") as decompress,
            patch.object(network_layer, "_UNPACK_REPLY", {2013: unpack}),
        ):
            result = network_layer.receive_message(_make_conn(), request_id=None)
        decompress.assert_called_once_with(compressed_body, 1)
        unpack.assert_called_once_with(b"decompressed")
        self.assertEqual(result, "REPLY")

    def test_unknown_opcode_raises(self):
        self._patch_receive_data(
            _make_header(length=20, request_id=0, response_to=0, op_code=9999), b"data"
        )
        with patch.object(network_layer, "_UNPACK_REPLY", {2013: MagicMock()}):
            with self.assertRaises(ProtocolError):
                network_layer.receive_message(_make_conn(), request_id=None)


class TestReceiveData(UnitTest):
    def test_raises_on_connection_closed(self):
        # Covers the explicit `raise OSError("connection closed")` branch when
        # recv_into returns 0.
        conn = _make_conn()
        conn.conn.recv_into.return_value = 0
        with self.assertRaisesRegex(OSError, "connection closed"):
            network_layer.receive_data(conn, 10, deadline=None)


if __name__ == "__main__":
    unittest.main()
