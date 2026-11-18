# Copyright 2026-present MongoDB, Inc.
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

"""Unit tests for message.py."""

from __future__ import annotations

import struct
import sys
from typing import Any
from unittest.mock import MagicMock

sys.path[0:0] = [""]

from bson import CodecOptions, decode, encode
from bson.objectid import ObjectId
from pymongo.common import MIN_SUPPORTED_WIRE_VERSION, MONGOS_EXHAUST_WIRE_VERSION
from pymongo.compression_support import ZlibContext, _have_zlib
from pymongo.errors import DocumentTooLarge, InvalidOperation, OperationFailure
from pymongo.hello import _get_server_type
from pymongo.message import (
    _check_exhaust_supported,
    _convert_client_bulk_exception,
    _convert_exception,
    _gen_find_command,
    _gen_get_more_command,
    _GetMore,
    _maybe_add_read_preference,
    _op_msg,
    _Query,
    _raise_document_too_large,
)
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference, SecondaryPreferred
from pymongo.server_type import SERVER_TYPE
from test import unittest

try:
    from pymongo import _cmessage
except ImportError:
    _cmessage = None

# OP_MSG header: 4 message length + 4 request id + 4 responseTo + 4 opCode
# + 4 flags + 1 payload-type byte, preceding the type-0 command document.
_OP_MSG_HEADER_SIZE = 21

_OPTS = CodecOptions()


class TestMessage(unittest.TestCase):
    # _gen_get_more_command helper
    def _make_conn(self, max_wire_version=9, is_mongos=False):
        conn = MagicMock()
        conn.max_wire_version = max_wire_version
        conn.is_mongos = is_mongos
        return conn

    # _check_exhaust_supported

    def test_exhaust_allowed_on_mongos_71(self):
        _check_exhaust_supported(self._make_conn(MONGOS_EXHAUST_WIRE_VERSION, is_mongos=True))

    def test_exhaust_refused_on_older_mongos(self):
        with self.assertRaisesRegex(InvalidOperation, "MongoDB 7.1"):
            _check_exhaust_supported(
                self._make_conn(MONGOS_EXHAUST_WIRE_VERSION - 1, is_mongos=True)
            )

    def test_exhaust_not_gated_behind_a_load_balancer(self):
        # A load-balanced hello carries serviceId and is typed LoadBalancer,
        # not Mongos, so these connections take the non-mongos path.
        doc = {"ok": 1, "msg": "isdbgrid", "serviceId": ObjectId(), "maxWireVersion": 21}
        self.assertEqual(_get_server_type(doc), SERVER_TYPE.LoadBalancer)
        _check_exhaust_supported(self._make_conn(MONGOS_EXHAUST_WIRE_VERSION - 1))

    def test_exhaust_allowed_on_any_supported_non_mongos(self):
        # Every other server type has served exhaust since 4.2.
        _check_exhaust_supported(self._make_conn(MIN_SUPPORTED_WIRE_VERSION))

    def _exhaust_query(self):
        return _Query(
            0,
            "db",
            "coll",
            0,
            {},
            None,
            CodecOptions(),
            ReadPreference.PRIMARY,
            0,
            0,
            ReadConcern(),
            None,
            None,
            MagicMock(),
            None,
            True,
        )

    def _exhaust_get_more(self):
        return _GetMore(
            "db",
            "coll",
            0,
            12345,
            CodecOptions(),
            ReadPreference.PRIMARY,
            None,
            MagicMock(),
            None,
            MagicMock(),
            True,
            None,
        )

    # use_command is where the check is consulted.

    def test_find_on_older_mongos_refuses_exhaust(self):
        conn = self._make_conn(MONGOS_EXHAUST_WIRE_VERSION - 1, is_mongos=True)
        with self.assertRaisesRegex(InvalidOperation, "MongoDB 7.1"):
            self._exhaust_query().use_command(conn)

    def test_get_more_on_older_mongos_refuses_exhaust(self):
        conn = self._make_conn(MONGOS_EXHAUST_WIRE_VERSION - 1, is_mongos=True)
        with self.assertRaisesRegex(InvalidOperation, "MongoDB 7.1"):
            self._exhaust_get_more().use_command(conn)

    def test_find_on_71_mongos_allows_exhaust(self):
        conn = self._make_conn(MONGOS_EXHAUST_WIRE_VERSION, is_mongos=True)
        self.assertTrue(self._exhaust_query().use_command(conn))

    def test_get_more_on_71_mongos_allows_exhaust(self):
        conn = self._make_conn(MONGOS_EXHAUST_WIRE_VERSION, is_mongos=True)
        self.assertTrue(self._exhaust_get_more().use_command(conn))

    # _maybe_add_read_preference

    def test_primary_no_read_preference_added(self):
        spec: dict = {"find": "col"}
        result = _maybe_add_read_preference(spec, ReadPreference.PRIMARY)
        self.assertNotIn("$readPreference", result)
        self.assertNotIn("$query", result)

    def test_secondary_adds_read_preference(self):
        spec: dict = {"find": "col"}
        result = _maybe_add_read_preference(spec, ReadPreference.SECONDARY)
        self.assertIn("$readPreference", result)
        self.assertEqual(result["$readPreference"]["mode"], "secondary")
        self.assertIn("$query", result)

    def test_secondary_preferred_no_tags_does_not_add(self):
        spec: dict = {"find": "col"}
        result = _maybe_add_read_preference(spec, ReadPreference.SECONDARY_PREFERRED)
        self.assertNotIn("$readPreference", result)

    def test_secondary_preferred_with_tags_adds_read_preference(self):
        pref = SecondaryPreferred(tag_sets=[{"dc": "east"}])
        spec: dict = {"find": "col"}
        result = _maybe_add_read_preference(spec, pref)
        self.assertIn("$readPreference", result)
        self.assertEqual(result["$readPreference"]["mode"], "secondaryPreferred")
        self.assertIn("$query", result)

    def test_existing_query_wrapper_preserved(self):
        spec: dict = {"$query": {"x": 1}, "other": 2}
        result = _maybe_add_read_preference(spec, ReadPreference.SECONDARY)
        self.assertIn("$readPreference", result)
        self.assertEqual(result["$query"], {"x": 1})

    # _convert_exception / _convert_client_bulk_exception

    def test_basic_exception(self):
        exc = ValueError("bad value")
        doc = _convert_exception(exc)
        self.assertEqual(doc["errmsg"], "bad value")
        self.assertEqual(doc["errtype"], "ValueError")

    def test_client_bulk_exception_includes_code(self):
        exc = OperationFailure("failed", code=11000)
        doc = _convert_client_bulk_exception(exc)
        self.assertEqual(doc["errmsg"], "failed")
        self.assertEqual(doc["code"], 11000)
        self.assertEqual(doc["errtype"], "OperationFailure")

    # _op_msg

    def test_op_msg_max_doc_size_zero_without_docs(self):
        max_doc_size = _op_msg(0, {"ping": 1}, "testdb", None, _OPTS)[3]
        self.assertEqual(max_doc_size, 0)

    def test_op_msg_max_doc_size_matches_largest_encoded_doc(self):
        docs: list[dict[str, Any]] = [{"_id": 1}, {"_id": 2, "data": "a" * 100}]
        cmd: dict = {"insert": "col", "documents": docs}
        max_doc_size = _op_msg(0, cmd, "testdb", None, _OPTS)[3]
        self.assertEqual(max_doc_size, max(len(encode(d)) for d in docs))

    def test_op_msg_read_preference_added_for_non_primary(self):
        cmd: dict = {"find": "col"}
        _op_msg(0, cmd, "testdb", ReadPreference.SECONDARY, _OPTS)
        self.assertIn("$readPreference", cmd)

    def test_op_msg_read_preference_skipped_if_already_present(self):
        cmd: dict = {"find": "col", "$readPreference": {"mode": "nearest"}}
        _op_msg(0, cmd, "testdb", ReadPreference.SECONDARY, _OPTS)
        self.assertEqual(cmd["$readPreference"]["mode"], "nearest")

    def test_op_msg_documents_field_is_restored(self):
        docs = [{"_id": 1}]
        cmd: dict = {"insert": "col", "documents": docs}
        _op_msg(0, cmd, "testdb", None, _OPTS)
        self.assertIn("documents", cmd)
        self.assertEqual(cmd["documents"], docs)

    @unittest.skipUnless(_have_zlib(), "zlib not available")
    def test_op_msg_compressed_zlib_header(self):
        # Verify the compressed path is taken and produces a valid OP_COMPRESSED frame.
        # Header layout (little-endian): [msgLen(4), reqId(4), responseTo(4), opCode(4),
        #   originalOpcode(4), uncompressedSize(4), compressorId(1)]
        ctx = ZlibContext(6)
        _, msg, _, _ = _op_msg(0, {"ping": 1}, "testdb", None, _OPTS, ctx=ctx)
        (opcode,) = struct.unpack_from("<i", msg, 12)
        self.assertEqual(opcode, 2012)  # OP_COMPRESSED
        (original_opcode,) = struct.unpack_from("<i", msg, 16)
        self.assertEqual(original_opcode, 2013)  # OP_MSG
        self.assertEqual(msg[24], ZlibContext.compressor_id)  # compressor_id == 2

    # _raise_document_too_large

    def test_raise_document_too_large_insert_includes_sizes(self):
        with self.assertRaises(DocumentTooLarge) as ctx:
            _raise_document_too_large("insert", 2_000_000, 1_000_000)
        msg = str(ctx.exception)
        self.assertIn("2000000", msg)
        self.assertIn("1000000", msg)

    def test_raise_document_too_large_update_generic_message(self):
        with self.assertRaisesRegex(DocumentTooLarge, "update command document too large"):
            _raise_document_too_large("update", 2_000_000, 1_000_000)

    # _gen_find_command

    def test_find_basic(self):
        cmd = _gen_find_command(
            "col",
            {},
            projection=None,
            skip=0,
            limit=0,
            batch_size=None,
            options=None,
            read_concern=ReadConcern(),
        )
        self.assertEqual(cmd["find"], "col")
        self.assertEqual(cmd["filter"], {})

    def test_find_with_projection(self):
        cmd = _gen_find_command(
            "col",
            {},
            projection={"x": 1},
            skip=0,
            limit=0,
            batch_size=None,
            options=None,
            read_concern=ReadConcern(),
        )
        self.assertEqual(cmd["projection"], {"x": 1})

    def test_find_with_skip(self):
        cmd = _gen_find_command(
            "col",
            {},
            projection=None,
            skip=5,
            limit=0,
            batch_size=None,
            options=None,
            read_concern=ReadConcern(),
        )
        self.assertEqual(cmd["skip"], 5)

    def test_find_with_positive_limit(self):
        cmd = _gen_find_command(
            "col",
            {},
            projection=None,
            skip=0,
            limit=10,
            batch_size=None,
            options=None,
            read_concern=ReadConcern(),
        )
        self.assertEqual(cmd["limit"], 10)
        self.assertNotIn("singleBatch", cmd)

    def test_find_with_negative_limit_sets_single_batch(self):
        cmd = _gen_find_command(
            "col",
            {},
            projection=None,
            skip=0,
            limit=-5,
            batch_size=None,
            options=None,
            read_concern=ReadConcern(),
        )
        self.assertEqual(cmd["limit"], 5)
        self.assertTrue(cmd["singleBatch"])

    def test_find_batch_size_adjusted_when_equal_to_limit(self):
        cmd = _gen_find_command(
            "col",
            {},
            projection=None,
            skip=0,
            limit=10,
            batch_size=10,
            options=None,
            read_concern=ReadConcern(),
        )
        self.assertEqual(cmd["batchSize"], 11)

    def test_find_batch_size_not_adjusted_when_different(self):
        # Covers the False branch of `if limit == batch_size:` — distinct from the True branch above.
        cmd = _gen_find_command(
            "col",
            {},
            projection=None,
            skip=0,
            limit=10,
            batch_size=5,
            options=None,
            read_concern=ReadConcern(),
        )
        self.assertEqual(cmd["batchSize"], 5)

    def test_find_read_concern_level_included(self):
        cmd = _gen_find_command(
            "col",
            {},
            projection=None,
            skip=0,
            limit=0,
            batch_size=None,
            options=None,
            read_concern=ReadConcern("majority"),
        )
        self.assertEqual(cmd["readConcern"], {"level": "majority"})

    def test_find_query_with_dollar_query_modifier(self):
        spec = {"$query": {"x": 1}, "$orderby": {"x": 1}}
        cmd = _gen_find_command(
            "col",
            spec,
            projection=None,
            skip=0,
            limit=0,
            batch_size=None,
            options=None,
            read_concern=ReadConcern(),
        )
        self.assertIn("sort", cmd)
        self.assertNotIn("$orderby", cmd)
        self.assertNotIn("$query", cmd)

    def test_find_allow_disk_use(self):
        cmd = _gen_find_command(
            "col",
            {},
            projection=None,
            skip=0,
            limit=0,
            batch_size=None,
            options=None,
            read_concern=ReadConcern(),
            allow_disk_use=True,
        )
        self.assertTrue(cmd["allowDiskUse"])

    def test_find_collation(self):
        cmd = _gen_find_command(
            "col",
            {},
            projection=None,
            skip=0,
            limit=0,
            batch_size=None,
            options=None,
            read_concern=ReadConcern(),
            collation={"locale": "en"},
        )
        self.assertEqual(cmd["collation"]["locale"], "en")

    def test_find_options_tailable(self):
        cmd = _gen_find_command(
            "col",
            {},
            projection=None,
            skip=0,
            limit=0,
            batch_size=None,
            options=2,
            read_concern=ReadConcern(),
        )
        self.assertTrue(cmd.get("tailable"))

    def test_find_dollar_query_with_explain_removed(self):
        spec = {"$query": {"x": 1}, "$explain": 1}
        cmd = _gen_find_command(
            "col",
            spec,
            projection=None,
            skip=0,
            limit=0,
            batch_size=None,
            options=None,
            read_concern=ReadConcern(),
        )
        self.assertNotIn("$explain", cmd)

    def test_find_dollar_query_with_read_preference_removed(self):
        # Covers the separate `if "$readPreference" in cmd:` branch — not entered by test_dollar_query_with_explain_removed.
        spec = {"$query": {"x": 1}, "$readPreference": {"mode": "secondary"}}
        cmd = _gen_find_command(
            "col",
            spec,
            projection=None,
            skip=0,
            limit=0,
            batch_size=None,
            options=None,
            read_concern=ReadConcern(),
        )
        self.assertNotIn("$readPreference", cmd)

    # _gen_get_more_command

    def test_get_more_basic(self):
        cmd = _gen_get_more_command(
            12345,
            "col",
            batch_size=None,
            max_await_time_ms=None,
            comment=None,
        )
        self.assertEqual(cmd["getMore"], 12345)
        self.assertEqual(cmd["collection"], "col")

    def test_get_more_with_batch_size(self):
        cmd = _gen_get_more_command(1, "col", batch_size=100, max_await_time_ms=None, comment=None)
        self.assertEqual(cmd["batchSize"], 100)

    def test_get_more_with_max_await_time_ms(self):
        cmd = _gen_get_more_command(1, "col", batch_size=None, max_await_time_ms=500, comment=None)
        self.assertEqual(cmd["maxTimeMS"], 500)

    def test_get_more_with_comment(self):
        cmd = _gen_get_more_command(
            1,
            "col",
            batch_size=None,
            max_await_time_ms=None,
            comment="my comment",
        )
        self.assertEqual(cmd["comment"], "my comment")


class _FakeBulkWriteContext:
    """Stub exposing the max sizes read by _cbson_batched_op_msg."""

    max_bson_size = 16 * 1024 * 1024

    def __init__(self, max_write_batch_size=100000, max_message_size=48 * 1024 * 1024):
        self.max_write_batch_size = max_write_batch_size
        self.max_message_size = max_message_size


class TestOpMsgBuilders(unittest.TestCase):
    """Directly drive the C wire-message builders.

    Exercises buffer growth and length back-patching in _cbson_op_msg and
    _cbson_batched_op_msg under sanitizer builds.
    """

    def setUp(self):
        if _cmessage is None:
            self.skipTest("_cmessage not built")

    def _assert_total_length(self, msg):
        """The first 4 bytes of an OP_MSG are its back-patched total length."""
        self.assertEqual(struct.unpack("<i", msg[:4])[0], len(msg))

    def _assert_command_document(self, msg, command):
        """The type-0 command document following the header decodes to `command`.

        Returns the command document's size.
        """
        command_size = struct.unpack("<i", msg[_OP_MSG_HEADER_SIZE : _OP_MSG_HEADER_SIZE + 4])[0]
        self.assertEqual(
            decode(msg[_OP_MSG_HEADER_SIZE : _OP_MSG_HEADER_SIZE + command_size]),
            command,
        )
        return command_size

    def test_op_msg_no_identifier(self):
        _, msg, total_size, max_doc_size = _cmessage._op_msg(0, {"ismaster": 1}, "", None, _OPTS)
        self.assertIsInstance(msg, bytes)
        self._assert_total_length(msg)
        # The header precedes the command document.
        self.assertEqual(total_size, len(msg) - _OP_MSG_HEADER_SIZE)
        self.assertEqual(max_doc_size, 0)
        self.assertEqual(decode(msg[_OP_MSG_HEADER_SIZE:]), {"ismaster": 1})

    def test_op_msg_with_identifier(self):
        docs: list[dict[str, Any]] = [{"b": 1}, {"c": "two"}]
        _, msg, _, max_doc_size = _cmessage._op_msg(0, {"insert": "coll"}, "documents", docs, _OPTS)
        self._assert_total_length(msg)
        self.assertEqual(max_doc_size, max(len(encode(doc)) for doc in docs))
        command_size = self._assert_command_document(msg, {"insert": "coll"})
        # Type-1 payload: type byte, 4-byte length, cstring identifier.
        payload_start = _OP_MSG_HEADER_SIZE + command_size
        self.assertEqual(msg[payload_start], 0x01)
        payload_length = struct.unpack("<i", msg[payload_start + 1 : payload_start + 5])[0]
        self.assertEqual(
            payload_length,
            4 + len(b"documents\x00") + sum(len(encode(doc)) for doc in docs),
        )
        offset = payload_start + 5 + len(b"documents\x00")
        for doc in docs:
            size = len(encode(doc))
            self.assertEqual(decode(msg[offset : offset + size]), doc)
            offset += size
        self.assertEqual(offset, len(msg))

    def test_op_msg_empty_docs(self):
        _, msg, _, max_doc_size = _cmessage._op_msg(0, {"insert": "coll"}, "documents", [], _OPTS)
        self._assert_total_length(msg)
        self.assertEqual(max_doc_size, 0)

    def test_op_msg_multiple_buffer_doublings(self):
        # The C encoder starts with a 256-byte buffer (INITIAL_BUFFER_SIZE in
        # bson/buffer.c) and repeatedly doubles it until the data fits. The
        # 5013-byte first document (plus the 21-byte header, command document,
        # and payload prefix) forces 5 consecutive doublings: 256 -> 8192. The
        # later documents are small, so only the first drives the growth.
        docs = [{"s": "x" * n} for n in (5000, 50, 5)]
        _, msg, _, max_doc_size = _cmessage._op_msg(0, {"insert": "coll"}, "documents", docs, _OPTS)
        self._assert_total_length(msg)
        self.assertEqual(max_doc_size, max(len(encode(doc)) for doc in docs))

    def test_batched_op_msg_insert(self):
        docs = [{"_id": i, "s": "x" * 10} for i in range(3)]
        _, msg, to_publish = _cmessage._batched_op_msg(
            0, {"insert": "coll"}, docs, True, _OPTS, _FakeBulkWriteContext()
        )
        self.assertIsInstance(to_publish, list)
        self._assert_total_length(msg)
        self._assert_command_document(msg, {"insert": "coll"})
        self.assertEqual(len(to_publish), len(docs))

    def test_batched_op_msg_update(self):
        docs = [{"q": {}, "u": {"$set": {"a": 1}}}]
        _, msg, to_publish = _cmessage._batched_op_msg(
            1, {"update": "coll"}, docs, True, _OPTS, _FakeBulkWriteContext()
        )
        self._assert_total_length(msg)
        self.assertIsInstance(to_publish, list)
        self.assertEqual(len(to_publish), len(docs))

    def test_batched_op_msg_split_on_batch_size(self):
        # Exceeding max_write_batch_size ends the batch after exactly that
        # many documents; the remaining documents are left for the caller to
        # batch separately.
        docs = [{"_id": i, "s": "x" * 10} for i in range(5)]
        _, msg, to_publish = _cmessage._batched_op_msg(
            0, {"insert": "coll"}, docs, True, _OPTS, _FakeBulkWriteContext(max_write_batch_size=2)
        )
        self.assertEqual(len(to_publish), 2)
        self._assert_total_length(msg)
        self._assert_command_document(msg, {"insert": "coll"})

    def test_batched_op_msg_split_on_message_size(self):
        # A document that would push the message past max_message_size is
        # rolled back out of the buffer and left for the caller to batch
        # separately.
        docs = [{"_id": i, "s": "x" * 100} for i in range(3)]
        _, msg, to_publish = _cmessage._batched_op_msg(
            0, {"insert": "coll"}, docs, True, _OPTS, _FakeBulkWriteContext(max_message_size=350)
        )
        self.assertEqual(len(to_publish), 2)
        self._assert_total_length(msg)


if __name__ == "__main__":
    unittest.main()
