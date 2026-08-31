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

import sys
from unittest.mock import MagicMock

sys.path[0:0] = [""]

from pymongo import network_layer
from pymongo.common import MAX_MESSAGE_SIZE
from pymongo.errors import ProtocolError
from test import UnitTest, unittest
from test.utils_shared import pack_msg_header


def _make_conn():
    conn = MagicMock()
    conn.conn.gettimeout.return_value = None
    # On PyPy/Windows, receive_data() calls wait_for_read() before recv_into().
    # wait_for_read() checks fileno() == -1 as an early-exit; without this mock,
    # sock.fileno() returns a MagicMock and sock.pending() > 0 raises TypeError.
    conn.conn.sock.fileno.return_value = -1
    return conn


def _mock_recv_into(conn, *chunks: bytes) -> None:
    # Scope the mock to this conn's recv_into to avoid races
    # with SDAM monitor threads owned by the shared test client
    it = iter(chunks)

    def _recv_into(buf) -> int:
        chunk = next(it)
        buf[: len(chunk)] = chunk
        return len(chunk)

    conn.conn.recv_into.side_effect = _recv_into


class TestReceiveMessage(UnitTest):
    def test_request_id_mismatch_raises(self):
        conn = _make_conn()
        _mock_recv_into(
            conn, pack_msg_header(length=32, request_id=0, response_to=99, op_code=2013)
        )
        with self.assertRaisesRegex(ProtocolError, "Got response id"):
            network_layer.receive_message(conn, request_id=1)

    def test_length_too_small_raises(self):
        conn = _make_conn()
        _mock_recv_into(conn, pack_msg_header(length=16, request_id=0, response_to=0, op_code=2013))
        with self.assertRaisesRegex(ProtocolError, "not longer than standard message header"):
            network_layer.receive_message(conn, request_id=None)

    def test_length_exceeds_max_raises(self):
        conn = _make_conn()
        _mock_recv_into(
            conn,
            pack_msg_header(length=MAX_MESSAGE_SIZE + 1, request_id=0, response_to=0, op_code=2013),
        )
        with self.assertRaisesRegex(ProtocolError, "larger than server max"):
            network_layer.receive_message(conn, request_id=None)

    def test_unknown_opcode_raises(self):
        conn = _make_conn()
        _mock_recv_into(
            conn,
            pack_msg_header(length=20, request_id=0, response_to=0, op_code=9999),
            b"data",
        )
        with self.assertRaisesRegex(ProtocolError, "Got opcode"):
            network_layer.receive_message(conn, request_id=None)


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
