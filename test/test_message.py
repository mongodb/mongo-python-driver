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

sys.path[0:0] = [""]

from test import unittest

from bson import CodecOptions
from pymongo.errors import DocumentTooLarge
from pymongo.message import (
    MAX_INT32,
    MIN_INT32,
    _compress,
    _convert_client_bulk_exception,
    _convert_exception,
    _convert_write_result,
    _gen_find_command,
    _gen_get_more_command,
    _get_more,
    _get_more_compressed,
    _get_more_impl,
    _get_more_uncompressed,
    _maybe_add_read_preference,
    _op_msg,
    _op_msg_no_header,
    _op_msg_uncompressed,
    _query,
    _query_compressed,
    _query_impl,
    _query_uncompressed,
    _raise_document_too_large,
    _randint,
)
from pymongo.read_preferences import Primary, ReadPreference, Secondary, SecondaryPreferred

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_OPTS = CodecOptions()


class _MockCtx:
    """Minimal compression context for testing _compress."""

    compressor_id = 2  # zlib id

    def compress(self, data: bytes) -> bytes:
        import zlib

        return zlib.compress(data)


class _MockConn:
    """Mock connection for _gen_get_more_command."""

    max_wire_version = 9


# ---------------------------------------------------------------------------
# _randint
# ---------------------------------------------------------------------------


class TestRandint(unittest.TestCase):
    def test_returns_int(self):
        self.assertIsInstance(_randint(), int)

    def test_in_range(self):
        for _ in range(50):
            r = _randint()
            self.assertGreaterEqual(r, MIN_INT32)
            self.assertLessEqual(r, MAX_INT32)


# ---------------------------------------------------------------------------
# _maybe_add_read_preference
# ---------------------------------------------------------------------------


class TestMaybeAddReadPreference(unittest.TestCase):
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

    def test_nearest_adds_read_preference(self):
        spec: dict = {"find": "col"}
        result = _maybe_add_read_preference(spec, ReadPreference.NEAREST)
        self.assertIn("$readPreference", result)
        self.assertEqual(result["$readPreference"]["mode"], "nearest")

    def test_secondary_preferred_no_tags_does_not_add(self):
        spec: dict = {"find": "col"}
        result = _maybe_add_read_preference(spec, ReadPreference.SECONDARY_PREFERRED)
        # SECONDARY_PREFERRED with no tags uses secondaryOk bit instead.
        self.assertNotIn("$readPreference", result)

    def test_secondary_preferred_with_tags_adds_read_preference(self):
        pref = SecondaryPreferred(tag_sets=[{"dc": "east"}])
        spec: dict = {"find": "col"}
        result = _maybe_add_read_preference(spec, pref)
        self.assertIn("$readPreference", result)

    def test_existing_query_wrapper_preserved(self):
        spec: dict = {"$query": {"x": 1}, "other": 2}
        result = _maybe_add_read_preference(spec, ReadPreference.SECONDARY)
        # $query already present, $readPreference is added directly.
        self.assertIn("$readPreference", result)
        self.assertEqual(result["$query"], {"x": 1})


# ---------------------------------------------------------------------------
# _convert_exception
# ---------------------------------------------------------------------------


class TestConvertException(unittest.TestCase):
    def test_basic_exception(self):
        exc = ValueError("bad value")
        doc = _convert_exception(exc)
        self.assertEqual(doc["errmsg"], "bad value")
        self.assertEqual(doc["errtype"], "ValueError")

    def test_runtime_error(self):
        exc = RuntimeError("oops")
        doc = _convert_exception(exc)
        self.assertEqual(doc["errtype"], "RuntimeError")


# ---------------------------------------------------------------------------
# _convert_client_bulk_exception
# ---------------------------------------------------------------------------


class TestConvertClientBulkException(unittest.TestCase):
    def test_includes_code(self):
        from pymongo.errors import OperationFailure

        exc = OperationFailure("failed", code=11000)
        doc = _convert_client_bulk_exception(exc)
        self.assertEqual(doc["errmsg"], "failed")
        self.assertEqual(doc["code"], 11000)
        self.assertEqual(doc["errtype"], "OperationFailure")


# ---------------------------------------------------------------------------
# _convert_write_result
# ---------------------------------------------------------------------------


class TestConvertWriteResult(unittest.TestCase):
    def test_insert_basic(self):
        cmd = {"documents": [{"_id": 1}, {"_id": 2}]}
        result = _convert_write_result("insert", cmd, {"n": 0})
        self.assertEqual(result["ok"], 1)
        self.assertEqual(result["n"], 2)  # len(documents) overrides n

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

    def test_update_implicit_upsert_from_updatedExisting_false(self):
        cmd = {"updates": [{"q": {"_id": 99}, "u": {"$set": {"x": 1}}}]}
        result = _convert_write_result(
            "update", cmd, {"n": 1, "updatedExisting": False, "upserted": None}
        )
        # upserted is already in result, so it takes the fast path
        self.assertIn("upserted", result)

    def test_update_upsert_no_upserted_id_from_query(self):
        cmd = {"updates": [{"q": {"_id": 77}, "u": {"$set": {"x": 1}}}]}
        result = _convert_write_result("update", cmd, {"n": 1, "updatedExisting": False})
        self.assertIn("upserted", result)
        self.assertEqual(result["upserted"][0]["_id"], 77)

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
        cmd = {"documents": [{"_id": 1}]}
        gle = {"n": 0, "err": "err", "code": 123, "errInfo": {"detail": "x"}}
        result = _convert_write_result("insert", cmd, gle)
        self.assertIn("errInfo", result["writeErrors"][0])


# ---------------------------------------------------------------------------
# _compress
# ---------------------------------------------------------------------------


class TestCompress(unittest.TestCase):
    def test_returns_request_id_and_bytes(self):
        ctx = _MockCtx()
        data = b"hello world"
        request_id, msg = _compress(2013, data, ctx)
        self.assertIsInstance(request_id, int)
        self.assertIsInstance(msg, bytes)

    def test_compressed_message_has_op_compressed_header(self):
        ctx = _MockCtx()
        data = b"hello world"
        _request_id, msg = _compress(2013, data, ctx)
        # Header: 4 bytes msglen, 4 bytes requestId, 4 bytes responseTo, 4 bytes opCode
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2012)  # OP_COMPRESSED


# ---------------------------------------------------------------------------
# _op_msg_no_header
# ---------------------------------------------------------------------------


class TestOpMsgNoHeader(unittest.TestCase):
    def test_basic_command_no_docs(self):
        cmd = {"ping": 1}
        data, total_size, max_doc_size = _op_msg_no_header(0, cmd, "", None, _OPTS)
        self.assertIsInstance(data, bytes)
        self.assertGreater(total_size, 0)
        self.assertEqual(max_doc_size, 0)

    def test_command_with_docs(self):
        cmd = {"insert": "col"}
        docs = [{"_id": 1, "x": 2}, {"_id": 3, "x": 4}]
        data, total_size, max_doc_size = _op_msg_no_header(0, cmd, "documents", docs, _OPTS)
        self.assertIsInstance(data, bytes)
        self.assertGreater(max_doc_size, 0)


# ---------------------------------------------------------------------------
# _op_msg_uncompressed / _op_msg_compressed
# ---------------------------------------------------------------------------


class TestOpMsgUncompressed(unittest.TestCase):
    def test_returns_tuple(self):
        cmd = {"ping": 1}
        rid, msg, total_size, max_doc_size = _op_msg_uncompressed(0, cmd, "", None, _OPTS)
        self.assertIsInstance(rid, int)
        self.assertIsInstance(msg, bytes)

    def test_msg_has_correct_op_code(self):
        cmd = {"ping": 1}
        _rid, msg, _ts, _mds = _op_msg_uncompressed(0, cmd, "", None, _OPTS)
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2013)  # OP_MSG


# ---------------------------------------------------------------------------
# _op_msg
# ---------------------------------------------------------------------------


class TestOpMsg(unittest.TestCase):
    def test_basic_uncompressed(self):
        cmd: dict = {"ping": 1}
        rid, msg, total_size, max_doc_size = _op_msg(0, cmd, "testdb", None, _OPTS)
        self.assertIsInstance(rid, int)
        self.assertIsInstance(msg, bytes)
        self.assertEqual(cmd["$db"], "testdb")

    def test_read_preference_added_for_non_primary(self):
        cmd: dict = {"find": "col"}
        _op_msg(0, cmd, "testdb", ReadPreference.SECONDARY, _OPTS)
        self.assertIn("$readPreference", cmd)

    def test_read_preference_not_added_for_primary(self):
        cmd: dict = {"find": "col"}
        _op_msg(0, cmd, "testdb", ReadPreference.PRIMARY, _OPTS)
        self.assertNotIn("$readPreference", cmd)

    def test_read_preference_skipped_if_already_present(self):
        cmd: dict = {"find": "col", "$readPreference": {"mode": "nearest"}}
        _op_msg(0, cmd, "testdb", ReadPreference.SECONDARY, _OPTS)
        self.assertEqual(cmd["$readPreference"]["mode"], "nearest")

    def test_none_read_preference_skipped(self):
        cmd: dict = {"find": "col"}
        _op_msg(0, cmd, "testdb", None, _OPTS)
        self.assertNotIn("$readPreference", cmd)

    def test_with_compression_context(self):
        ctx = _MockCtx()
        cmd: dict = {"ping": 1}
        rid, msg, total_size, max_doc_size = _op_msg(0, cmd, "testdb", None, _OPTS, ctx)
        self.assertIsInstance(rid, int)
        # Compressed message uses OP_COMPRESSED (2012).
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2012)

    def test_command_with_documents_field_is_restored(self):
        docs = [{"_id": 1}]
        cmd: dict = {"insert": "col", "documents": docs}
        _op_msg(0, cmd, "testdb", None, _OPTS)
        # The documents field should be restored after the call.
        self.assertIn("documents", cmd)
        self.assertEqual(cmd["documents"], docs)


# ---------------------------------------------------------------------------
# _query_impl / _query_uncompressed / _query_compressed / _query
# ---------------------------------------------------------------------------


class TestQueryImpl(unittest.TestCase):
    def test_basic_query(self):
        data, max_bson_size = _query_impl(0, "db.col", 0, 0, {"x": 1}, None, _OPTS)
        self.assertIsInstance(data, bytes)
        self.assertGreater(max_bson_size, 0)

    def test_with_field_selector(self):
        data, max_bson_size = _query_impl(0, "db.col", 0, 0, {"x": 1}, {"_id": 0}, _OPTS)
        self.assertIsInstance(data, bytes)


class TestQueryUncompressed(unittest.TestCase):
    def test_returns_tuple(self):
        rid, msg, max_bson_size = _query_uncompressed(0, "db.col", 0, 0, {}, None, _OPTS)
        self.assertIsInstance(rid, int)
        self.assertIsInstance(msg, bytes)

    def test_op_code(self):
        _rid, msg, _mbs = _query_uncompressed(0, "db.col", 0, 0, {}, None, _OPTS)
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2004)  # OP_QUERY


class TestQueryCompressed(unittest.TestCase):
    def test_returns_compressed(self):
        ctx = _MockCtx()
        rid, msg, max_bson_size = _query_compressed(0, "db.col", 0, 0, {}, None, _OPTS, ctx)
        self.assertIsInstance(rid, int)
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2012)  # OP_COMPRESSED


class TestQuery(unittest.TestCase):
    def test_uncompressed_path(self):
        rid, msg, max_bson_size = _query(0, "db.col", 0, 0, {}, None, _OPTS)
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2004)

    def test_compressed_path(self):
        ctx = _MockCtx()
        rid, msg, max_bson_size = _query(0, "db.col", 0, 0, {}, None, _OPTS, ctx)
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2012)


# ---------------------------------------------------------------------------
# _get_more_impl / _get_more_uncompressed / _get_more_compressed / _get_more
# ---------------------------------------------------------------------------


class TestGetMoreImpl(unittest.TestCase):
    def test_returns_bytes(self):
        data = _get_more_impl("db.col", 10, 12345)
        self.assertIsInstance(data, bytes)


class TestGetMoreUncompressed(unittest.TestCase):
    def test_returns_tuple(self):
        rid, msg = _get_more_uncompressed("db.col", 10, 12345)
        self.assertIsInstance(rid, int)
        self.assertIsInstance(msg, bytes)

    def test_op_code(self):
        _rid, msg = _get_more_uncompressed("db.col", 0, 0)
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2005)  # OP_GET_MORE


class TestGetMoreCompressed(unittest.TestCase):
    def test_returns_compressed(self):
        ctx = _MockCtx()
        rid, msg = _get_more_compressed("db.col", 0, 0, ctx)
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2012)


class TestGetMore(unittest.TestCase):
    def test_uncompressed_path(self):
        rid, msg = _get_more("db.col", 0, 0)
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2005)

    def test_compressed_path(self):
        ctx = _MockCtx()
        rid, msg = _get_more("db.col", 0, 0, ctx)
        op_code = struct.unpack("<i", msg[12:16])[0]
        self.assertEqual(op_code, 2012)


# ---------------------------------------------------------------------------
# _raise_document_too_large
# ---------------------------------------------------------------------------


class TestRaiseDocumentTooLarge(unittest.TestCase):
    def test_insert_includes_sizes(self):
        with self.assertRaises(DocumentTooLarge) as ctx:
            _raise_document_too_large("insert", 2_000_000, 1_000_000)
        msg = str(ctx.exception)
        self.assertIn("2000000", msg)
        self.assertIn("1000000", msg)

    def test_update_generic_message(self):
        with self.assertRaises(DocumentTooLarge) as ctx:
            _raise_document_too_large("update", 2_000_000, 1_000_000)
        self.assertIn("update", str(ctx.exception))

    def test_delete_generic_message(self):
        with self.assertRaises(DocumentTooLarge) as ctx:
            _raise_document_too_large("delete", 2_000_000, 1_000_000)
        self.assertIn("delete", str(ctx.exception))


# ---------------------------------------------------------------------------
# _gen_find_command
# ---------------------------------------------------------------------------


class TestGenFindCommand(unittest.TestCase):
    def _read_concern(self, level=None):
        from pymongo.read_concern import ReadConcern

        return ReadConcern(level=level)

    def test_basic(self):
        cmd = _gen_find_command("col", {}, None, 0, 0, None, None, self._read_concern())
        self.assertEqual(cmd["find"], "col")
        self.assertEqual(cmd["filter"], {})

    def test_with_projection(self):
        cmd = _gen_find_command("col", {}, {"x": 1}, 0, 0, None, None, self._read_concern())
        self.assertIn("projection", cmd)

    def test_with_skip(self):
        cmd = _gen_find_command("col", {}, None, 5, 0, None, None, self._read_concern())
        self.assertEqual(cmd["skip"], 5)

    def test_with_positive_limit(self):
        cmd = _gen_find_command("col", {}, None, 0, 10, None, None, self._read_concern())
        self.assertEqual(cmd["limit"], 10)
        self.assertNotIn("singleBatch", cmd)

    def test_with_negative_limit_sets_single_batch(self):
        cmd = _gen_find_command("col", {}, None, 0, -5, None, None, self._read_concern())
        self.assertEqual(cmd["limit"], 5)
        self.assertTrue(cmd["singleBatch"])

    def test_batch_size_adjusted_when_equal_to_limit(self):
        cmd = _gen_find_command("col", {}, None, 0, 10, 10, None, self._read_concern())
        self.assertEqual(cmd["batchSize"], 11)

    def test_batch_size_not_adjusted_when_different(self):
        cmd = _gen_find_command("col", {}, None, 0, 10, 5, None, self._read_concern())
        self.assertEqual(cmd["batchSize"], 5)

    def test_read_concern_level_included(self):
        cmd = _gen_find_command("col", {}, None, 0, 0, None, None, self._read_concern("majority"))
        self.assertIn("readConcern", cmd)

    def test_query_with_dollar_query_modifier(self):
        spec = {"$query": {"x": 1}, "$orderby": {"x": 1}}
        cmd = _gen_find_command("col", spec, None, 0, 0, None, None, self._read_concern())
        self.assertIn("sort", cmd)
        self.assertNotIn("$orderby", cmd)
        self.assertNotIn("$query", cmd)

    def test_allow_disk_use(self):
        cmd = _gen_find_command(
            "col", {}, None, 0, 0, None, None, self._read_concern(), allow_disk_use=True
        )
        self.assertTrue(cmd["allowDiskUse"])

    def test_collation(self):
        cmd = _gen_find_command(
            "col", {}, None, 0, 0, None, None, self._read_concern(), collation={"locale": "en"}
        )
        self.assertEqual(cmd["collation"]["locale"], "en")

    def test_options_tailable(self):
        cmd = _gen_find_command("col", {}, None, 0, 0, None, 2, self._read_concern())
        self.assertTrue(cmd.get("tailable"))

    def test_dollar_query_with_explain_removed(self):
        spec = {"$query": {"x": 1}, "$explain": 1}
        cmd = _gen_find_command("col", spec, None, 0, 0, None, None, self._read_concern())
        # $explain should be stripped from the find command.
        self.assertNotIn("$explain", cmd)

    def test_dollar_query_with_read_preference_removed(self):
        spec = {"$query": {"x": 1}, "$readPreference": {"mode": "secondary"}}
        cmd = _gen_find_command("col", spec, None, 0, 0, None, None, self._read_concern())
        self.assertNotIn("$readPreference", cmd)


# ---------------------------------------------------------------------------
# _gen_get_more_command
# ---------------------------------------------------------------------------


class TestGenGetMoreCommand(unittest.TestCase):
    def test_basic(self):
        conn = _MockConn()
        cmd = _gen_get_more_command(12345, "col", None, None, None, conn)
        self.assertEqual(cmd["getMore"], 12345)
        self.assertEqual(cmd["collection"], "col")

    def test_with_batch_size(self):
        conn = _MockConn()
        cmd = _gen_get_more_command(1, "col", 100, None, None, conn)
        self.assertEqual(cmd["batchSize"], 100)

    def test_with_max_await_time_ms(self):
        conn = _MockConn()
        cmd = _gen_get_more_command(1, "col", None, 500, None, conn)
        self.assertEqual(cmd["maxTimeMS"], 500)

    def test_comment_added_on_high_wire_version(self):
        conn = _MockConn()
        conn.max_wire_version = 9
        cmd = _gen_get_more_command(1, "col", None, None, "my comment", conn)
        self.assertEqual(cmd["comment"], "my comment")

    def test_comment_not_added_on_low_wire_version(self):
        conn = _MockConn()
        conn.max_wire_version = 8
        cmd = _gen_get_more_command(1, "col", None, None, "my comment", conn)
        self.assertNotIn("comment", cmd)


if __name__ == "__main__":
    unittest.main()
