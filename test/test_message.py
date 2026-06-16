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

from bson import CodecOptions, encode
from pymongo.compression_support import ZlibContext, _have_zlib
from pymongo.errors import DocumentTooLarge, OperationFailure
from pymongo.message import (
    _convert_client_bulk_exception,
    _convert_exception,
    _convert_write_result,
    _gen_find_command,
    _gen_get_more_command,
    _maybe_add_read_preference,
    _op_msg,
    _raise_document_too_large,
)
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference, SecondaryPreferred
from test import unittest

_OPTS = CodecOptions()


class TestMessage(unittest.TestCase):
    # _gen_get_more_command helper
    def _make_conn(self, max_wire_version=9):
        conn = MagicMock()
        conn.max_wire_version = max_wire_version
        return conn

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

    # _convert_write_result
    # In the update command spec, `q` is the query/filter and `u` is the update document.

    def test_insert_basic(self):
        cmd = {"documents": [{"_id": 1}, {"_id": 2}]}
        result = _convert_write_result("insert", cmd, {"n": 0})
        self.assertEqual(result["ok"], 1)
        self.assertEqual(result["n"], 2)

    def test_update_basic(self):
        cmd = {"updates": [{"q": {}, "u": {"$set": {"x": 1}}}]}
        result = _convert_write_result("update", cmd, {"n": 1, "updatedExisting": True})
        self.assertEqual(result["ok"], 1)
        self.assertNotIn("upserted", result)

    def test_update_with_upserted_id(self):
        cmd = {"updates": [{"q": {}, "u": {"_id": 42}}]}
        result = _convert_write_result("update", cmd, {"n": 1, "upserted": 42})
        self.assertIn("upserted", result)
        self.assertEqual(result["upserted"][0]["_id"], 42)

    def test_delete_basic(self):
        cmd = {"deletes": [{"q": {}, "limit": 1}]}
        result = _convert_write_result("delete", cmd, {"n": 1})
        self.assertEqual(result["ok"], 1)
        self.assertEqual(result["n"], 1)

    def test_write_error(self):
        cmd = {"documents": [{"_id": 1}]}
        gle = {"n": 0, "err": "duplicate key error", "code": 11000}
        result = _convert_write_result("insert", cmd, gle)
        self.assertIn("writeErrors", result)
        self.assertEqual(result["writeErrors"][0]["code"], 11000)

    def test_write_concern_timeout(self):
        cmd = {"documents": [{"_id": 1}]}
        gle = {"n": 1, "errmsg": "timeout", "wtimeout": True}
        result = _convert_write_result("insert", cmd, gle)
        self.assertIn("writeConcernError", result)
        self.assertEqual(result["writeConcernError"]["code"], 64)

    def test_write_error_with_err_info(self):
        # Covers the `if "errInfo" in result:` branch, which test_write_error does not enter.
        cmd = {"documents": [{"_id": 1}]}
        gle = {"n": 0, "err": "err", "code": 123, "errInfo": {"detail": "x"}}
        result = _convert_write_result("insert", cmd, gle)
        self.assertIn("errInfo", result["writeErrors"][0])

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
            conn=self._make_conn(),
        )
        self.assertEqual(cmd["getMore"], 12345)
        self.assertEqual(cmd["collection"], "col")

    def test_get_more_with_batch_size(self):
        cmd = _gen_get_more_command(
            1, "col", batch_size=100, max_await_time_ms=None, comment=None, conn=self._make_conn()
        )
        self.assertEqual(cmd["batchSize"], 100)

    def test_get_more_with_max_await_time_ms(self):
        cmd = _gen_get_more_command(
            1, "col", batch_size=None, max_await_time_ms=500, comment=None, conn=self._make_conn()
        )
        self.assertEqual(cmd["maxTimeMS"], 500)

    def test_get_more_comment_added_on_high_wire_version(self):
        cmd = _gen_get_more_command(
            1,
            "col",
            batch_size=None,
            max_await_time_ms=None,
            comment="my comment",
            conn=self._make_conn(9),
        )
        self.assertEqual(cmd["comment"], "my comment")

    def test_get_more_comment_not_added_on_low_wire_version(self):
        cmd = _gen_get_more_command(
            1,
            "col",
            batch_size=None,
            max_await_time_ms=None,
            comment="my comment",
            conn=self._make_conn(8),
        )
        self.assertNotIn("comment", cmd)


if __name__ == "__main__":
    unittest.main()
