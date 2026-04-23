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

"""Unit tests for network_layer.py.

Tests cover pure-function and state-machine logic that does not require
a live socket or MongoDB server.  Live-endpoint paths are exercised by
integration tests in test_client.py / test_ssl.py.
"""

from __future__ import annotations

import asyncio
import struct
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path[0:0] = [""]

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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(coro):
    """Run a coroutine synchronously for testing."""
    return asyncio.run(coro)


async def _make_protocol(timeout=None):
    """Create a PyMongoProtocol with a stubbed transport."""
    proto = PyMongoProtocol(timeout=timeout)
    mock_transport = MagicMock()
    mock_transport.is_closing.return_value = False
    proto.transport = mock_transport
    return proto


def _make_header(length, request_id, response_to, op_code):
    """Pack a 16-byte MongoDB wire-protocol header."""
    return struct.pack("<iiii", length, request_id, response_to, op_code)


# ---------------------------------------------------------------------------
# sendall
# ---------------------------------------------------------------------------


class TestSendall(unittest.TestCase):
    def test_delegates_to_sock_sendall(self):
        mock_sock = MagicMock()
        sendall(mock_sock, b"hello")
        mock_sock.sendall.assert_called_once_with(b"hello")


# ---------------------------------------------------------------------------
# NetworkingInterfaceBase
# ---------------------------------------------------------------------------


class TestNetworkingInterfaceBase(unittest.TestCase):
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


# ---------------------------------------------------------------------------
# NetworkingInterface
# ---------------------------------------------------------------------------


class TestNetworkingInterface(unittest.TestCase):
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


# ---------------------------------------------------------------------------
# AsyncNetworkingInterface
# ---------------------------------------------------------------------------


class TestAsyncNetworkingInterface(unittest.TestCase):
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


# ---------------------------------------------------------------------------
# PyMongoProtocol — settimeout / gettimeout
# ---------------------------------------------------------------------------


class TestPyMongoProtocolTimeout(unittest.TestCase):
    def test_initial_timeout_from_constructor(self):
        result = _run(_make_protocol(timeout=3.0))
        self.assertEqual(result.gettimeout, 3.0)

    def test_settimeout_updates_value(self):
        async def _test():
            proto = await _make_protocol()
            proto.settimeout(7.5)
            return proto.gettimeout

        self.assertEqual(_run(_test()), 7.5)

    def test_default_timeout_is_none(self):
        result = _run(_make_protocol())
        self.assertIsNone(result.gettimeout)


# ---------------------------------------------------------------------------
# PyMongoProtocol — process_header
# ---------------------------------------------------------------------------


class TestPyMongoProtocolProcessHeader(unittest.TestCase):
    def _run_with_header(self, header_bytes, max_size=MAX_MESSAGE_SIZE):
        async def _test():
            proto = await _make_protocol()
            proto._max_message_size = max_size
            proto._header = memoryview(bytearray(header_bytes))
            return proto.process_header()

        return _run(_test())

    def test_normal_op_msg(self):
        # length=32, op_code=2013 (OP_MSG) → body = 32-16 = 16
        hdr = _make_header(32, 1, 99, 2013)
        body_len, op_code, response_to, expecting_compression = self._run_with_header(hdr)
        self.assertEqual(body_len, 16)
        self.assertEqual(op_code, 2013)
        self.assertEqual(response_to, 99)
        self.assertFalse(expecting_compression)

    def test_op_compressed(self):
        # OP_COMPRESSED=2012, length=35 → adjusted=35-9=26 → body=26-16=10
        hdr = _make_header(35, 1, 0, 2012)
        body_len, op_code, response_to, expecting_compression = self._run_with_header(hdr)
        self.assertEqual(body_len, 10)
        self.assertEqual(op_code, 2012)
        self.assertTrue(expecting_compression)

    def test_op_compressed_length_too_small_raises(self):
        # OP_COMPRESSED with length <= 25
        hdr = _make_header(25, 1, 0, 2012)
        with self.assertRaises(ProtocolError):
            self._run_with_header(hdr)

    def test_non_compressed_length_too_small_raises(self):
        # Non-compressed with length <= 16
        hdr = _make_header(16, 1, 0, 2013)
        with self.assertRaises(ProtocolError):
            self._run_with_header(hdr)

    def test_length_exceeds_max_raises(self):
        hdr = _make_header(MAX_MESSAGE_SIZE + 1, 1, 0, 2013)
        with self.assertRaises(ProtocolError):
            self._run_with_header(hdr)

    def test_op_reply_op_code(self):
        # OP_REPLY=1 is a valid non-compressed opcode
        hdr = _make_header(20, 0, 0, 1)
        body_len, op_code, response_to, expecting_compression = self._run_with_header(hdr)
        self.assertEqual(body_len, 4)
        self.assertEqual(op_code, 1)
        self.assertFalse(expecting_compression)


# ---------------------------------------------------------------------------
# PyMongoProtocol — process_compression_header
# ---------------------------------------------------------------------------


class TestPyMongoProtocolProcessCompressionHeader(unittest.TestCase):
    def test_returns_op_code_and_compressor_id(self):
        async def _test():
            proto = await _make_protocol()
            # op_code=2013, unknown int=0, compressor_id=1 (snappy)
            data = struct.pack("<iiB", 2013, 0, 1)
            proto._compression_header = memoryview(bytearray(data))
            return proto.process_compression_header()

        op_code, compressor_id = _run(_test())
        self.assertEqual(op_code, 2013)
        self.assertEqual(compressor_id, 1)

    def test_zlib_compressor_id(self):
        async def _test():
            proto = await _make_protocol()
            data = struct.pack("<iiB", 2013, 0, 2)
            proto._compression_header = memoryview(bytearray(data))
            return proto.process_compression_header()

        _, compressor_id = _run(_test())
        self.assertEqual(compressor_id, 2)


# ---------------------------------------------------------------------------
# PyMongoProtocol — get_buffer
# ---------------------------------------------------------------------------


class TestPyMongoProtocolGetBuffer(unittest.TestCase):
    def test_expecting_header_returns_full_header_slice(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = True
            proto._header_index = 0
            return len(proto.get_buffer(0))

        self.assertEqual(_run(_test()), 16)

    def test_expecting_header_partial_returns_remaining(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = True
            proto._header_index = 8
            return len(proto.get_buffer(0))

        self.assertEqual(_run(_test()), 8)

    def test_expecting_compression_returns_compression_slice(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = True
            proto._compression_index = 0
            return len(proto.get_buffer(0))

        self.assertEqual(_run(_test()), 9)

    def test_expecting_compression_partial(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = True
            proto._compression_index = 5
            return len(proto.get_buffer(0))

        self.assertEqual(_run(_test()), 4)

    def test_message_body_returns_remaining_slice(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = False
            proto._message = memoryview(bytearray(100))
            proto._message_index = 0
            return len(proto.get_buffer(0))

        self.assertEqual(_run(_test()), 100)

    def test_connection_lost_allocates_drain_buffer(self):
        async def _test():
            proto = await _make_protocol()
            proto._connection_lost = True
            proto._message = None
            return len(proto.get_buffer(0))

        self.assertEqual(_run(_test()), 2**14)

    def test_connection_lost_reuses_existing_buffer(self):
        async def _test():
            proto = await _make_protocol()
            proto._connection_lost = True
            proto._message = memoryview(bytearray(50))
            return len(proto.get_buffer(0))

        self.assertEqual(_run(_test()), 50)


# ---------------------------------------------------------------------------
# PyMongoProtocol — buffer_updated
# ---------------------------------------------------------------------------


class TestPyMongoProtocolBufferUpdated(unittest.TestCase):
    def test_zero_bytes_closes_connection(self):
        async def _test():
            proto = await _make_protocol()
            proto.buffer_updated(0)
            return proto._connection_lost

        self.assertTrue(_run(_test()))

    def test_connection_lost_returns_early(self):
        async def _test():
            proto = await _make_protocol()
            proto._connection_lost = True
            proto._header_index = 3
            proto.buffer_updated(5)
            return proto._header_index  # must remain 3

        self.assertEqual(_run(_test()), 3)

    def test_partial_header_increments_index(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = True
            proto._header_index = 0
            proto.buffer_updated(8)
            return proto._header_index

        self.assertEqual(_run(_test()), 8)

    def test_full_header_transitions_to_message(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = True
            hdr = _make_header(32, 1, 0, 2013)
            proto._header = memoryview(bytearray(hdr))
            proto._header_index = 0
            proto.buffer_updated(16)
            return proto._expecting_header, proto._message_size

        expecting_header, message_size = _run(_test())
        self.assertFalse(expecting_header)
        self.assertEqual(message_size, 16)  # 32 - 16

    def test_invalid_header_closes_connection(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = True
            # length=16 (not > 16) triggers ProtocolError
            hdr = _make_header(16, 1, 0, 2013)
            proto._header = memoryview(bytearray(hdr))
            proto._header_index = 0
            proto.buffer_updated(16)
            return proto._connection_lost

        self.assertTrue(_run(_test()))

    def test_compression_header_processing(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = True
            comp_hdr = struct.pack("<iiB", 2013, 0, 2)
            proto._compression_header = memoryview(bytearray(comp_hdr))
            proto._compression_index = 0
            proto.buffer_updated(9)
            return proto._expecting_compression, proto._op_code, proto._compressor_id

        expecting_compression, op_code, compressor_id = _run(_test())
        self.assertFalse(expecting_compression)
        self.assertEqual(op_code, 2013)
        self.assertEqual(compressor_id, 2)

    def test_partial_compression_header_increments_index(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = True
            proto._compression_index = 0
            proto.buffer_updated(4)
            return proto._compression_index, proto._expecting_compression

        idx, still_expecting = _run(_test())
        self.assertEqual(idx, 4)
        self.assertTrue(still_expecting)

    def test_message_complete_resolves_pending_future(self):
        async def _test():
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
            return fut.done(), fut.result()

        done, result = _run(_test())
        self.assertTrue(done)
        op_code, compressor_id, response_to, _ = result
        self.assertEqual(op_code, 2013)
        self.assertIsNone(compressor_id)
        self.assertEqual(response_to, 42)

    def test_message_complete_no_pending_creates_new_future(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = False
            proto._message_size = 5
            proto._message = memoryview(bytearray(5))
            proto._message_index = 0
            proto._op_code = 2013
            proto._compressor_id = None
            proto._response_to = 0

            # No pending futures; buffer_updated creates one internally
            self.assertFalse(proto._pending_messages)
            proto.buffer_updated(5)
            # The auto-created future should be in _done_messages
            return len(proto._done_messages)

        self.assertEqual(_run(_test()), 1)

    def test_partial_message_increments_index(self):
        async def _test():
            proto = await _make_protocol()
            proto._expecting_header = False
            proto._expecting_compression = False
            proto._message_size = 20
            proto._message = memoryview(bytearray(20))
            proto._message_index = 0
            proto.buffer_updated(7)
            return proto._message_index

        self.assertEqual(_run(_test()), 7)


# ---------------------------------------------------------------------------
# PyMongoProtocol — close / connection_lost
# ---------------------------------------------------------------------------


class TestPyMongoProtocolClose(unittest.TestCase):
    def test_close_sets_connection_lost_flag(self):
        async def _test():
            proto = await _make_protocol()
            proto.close()
            return proto._connection_lost

        self.assertTrue(_run(_test()))

    def test_close_aborts_transport(self):
        async def _test():
            proto = await _make_protocol()
            proto.close()
            return proto.transport.abort.called

        self.assertTrue(_run(_test()))

    def test_connection_lost_resolves_closed_future(self):
        async def _test():
            proto = await _make_protocol()
            self.assertFalse(proto._closed.done())
            proto.connection_lost(None)
            return proto._closed.done()

        self.assertTrue(_run(_test()))

    def test_connection_lost_twice_does_not_raise(self):
        async def _test():
            proto = await _make_protocol()
            proto.connection_lost(None)
            proto.connection_lost(None)  # second call must not raise InvalidStateError

        _run(_test())  # should not raise

    def test_close_with_exception_propagates_to_pending(self):
        async def _test():
            proto = await _make_protocol()
            fut = asyncio.get_running_loop().create_future()
            proto._pending_messages.append(fut)
            exc = OSError("connection reset")
            proto.close(exc)
            try:
                await fut
                return None
            except OSError as e:
                return str(e)

        result = _run(_test())
        self.assertIn("connection reset", result)


# ---------------------------------------------------------------------------
# _async_socket_receive
# ---------------------------------------------------------------------------


class TestAsyncSocketReceive(unittest.TestCase):
    def test_reads_full_data_in_one_call(self):
        data = b"hello world!"
        length = len(data)

        async def _test():
            mock_sock = MagicMock()
            loop = asyncio.get_running_loop()

            async def fake_recv_into(sock, buf):
                buf[:length] = data
                return length

            with patch.object(loop, "sock_recv_into", new=AsyncMock(side_effect=fake_recv_into)):
                result = await _async_socket_receive(mock_sock, length, loop)
            return bytes(result)

        self.assertEqual(_run(_test()), data)

    def test_reads_data_in_multiple_chunks(self):
        data = b"abcdefgh"
        length = len(data)
        chunk1, chunk2 = data[:4], data[4:]

        async def _test():
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
            return bytes(result)

        self.assertEqual(_run(_test()), data)

    def test_raises_on_connection_closed(self):
        async def _test():
            mock_sock = MagicMock()
            loop = asyncio.get_running_loop()

            async def fake_recv_into(sock, buf):
                return 0  # connection closed

            with patch.object(loop, "sock_recv_into", new=AsyncMock(side_effect=fake_recv_into)):
                await _async_socket_receive(mock_sock, 10, loop)

        with self.assertRaises(OSError) as ctx:
            _run(_test())
        self.assertIn("connection closed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
