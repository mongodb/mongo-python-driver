# Copyright 2009-present MongoDB, Inc.
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

"""Test the cursor module."""
import copy
import gc
import itertools
import random
import re
import sys
import threading
import time

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.utils import (
    AllowListEventListener,
    EventListener,
    OvertCommandListener,
    ignore_deprecations,
    rs_or_single_client,
)

from bson import decode_all
from bson.code import Code
from bson.son import SON
from pymongo import ASCENDING, DESCENDING
from pymongo.collation import Collation
from pymongo.cursor import Cursor, CursorType
from pymongo.errors import ExecutionTimeout, InvalidOperation, OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern


class TestCursor(IntegrationTest):
    def test_deepcopy_cursor_littered_with_regexes(self):
        cursor = self.db.test.find(
            {
                "x": re.compile("^hmmm.*"),
                "y": [re.compile("^hmm.*")],
                "z": {"a": [re.compile("^hm.*")]},
                re.compile("^key.*"): {"a": [re.compile("^hm.*")]},
            }
        )

        cursor2 = copy.deepcopy(cursor)
        self.assertEqual(cursor._Cursor__spec, cursor2._Cursor__spec)  # type: ignore

    def test_add_remove_option(self):
        cursor = self.db.test.find()
        self.assertEqual(0, cursor._Cursor__query_flags)
        cursor.add_option(2)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE)
        self.assertEqual(2, cursor2._Cursor__query_flags)
        self.assertEqual(cursor._Cursor__query_flags, cursor2._Cursor__query_flags)
        cursor.add_option(32)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE_AWAIT)
        self.assertEqual(34, cursor2._Cursor__query_flags)
        self.assertEqual(cursor._Cursor__query_flags, cursor2._Cursor__query_flags)
        cursor.add_option(128)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE_AWAIT).add_option(128)
        self.assertEqual(162, cursor2._Cursor__query_flags)
        self.assertEqual(cursor._Cursor__query_flags, cursor2._Cursor__query_flags)

        self.assertEqual(162, cursor._Cursor__query_flags)
        cursor.add_option(128)
        self.assertEqual(162, cursor._Cursor__query_flags)

        cursor.remove_option(128)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE_AWAIT)
        self.assertEqual(34, cursor2._Cursor__query_flags)
        self.assertEqual(cursor._Cursor__query_flags, cursor2._Cursor__query_flags)
        cursor.remove_option(32)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE)
        self.assertEqual(2, cursor2._Cursor__query_flags)
        self.assertEqual(cursor._Cursor__query_flags, cursor2._Cursor__query_flags)

        self.assertEqual(2, cursor._Cursor__query_flags)
        cursor.remove_option(32)
        self.assertEqual(2, cursor._Cursor__query_flags)

        # Timeout
        cursor = self.db.test.find(no_cursor_timeout=True)
        self.assertEqual(16, cursor._Cursor__query_flags)
        cursor2 = self.db.test.find().add_option(16)
        self.assertEqual(cursor._Cursor__query_flags, cursor2._Cursor__query_flags)
        cursor.remove_option(16)
        self.assertEqual(0, cursor._Cursor__query_flags)

        # Tailable / Await data
        cursor = self.db.test.find(cursor_type=CursorType.TAILABLE_AWAIT)
        self.assertEqual(34, cursor._Cursor__query_flags)
        cursor2 = self.db.test.find().add_option(34)
        self.assertEqual(cursor._Cursor__query_flags, cursor2._Cursor__query_flags)
        cursor.remove_option(32)
        self.assertEqual(2, cursor._Cursor__query_flags)

        # Partial
        cursor = self.db.test.find(allow_partial_results=True)
        self.assertEqual(128, cursor._Cursor__query_flags)
        cursor2 = self.db.test.find().add_option(128)
        self.assertEqual(cursor._Cursor__query_flags, cursor2._Cursor__query_flags)
        cursor.remove_option(128)
        self.assertEqual(0, cursor._Cursor__query_flags)

    def test_add_remove_option_exhaust(self):
        # Exhaust - which mongos doesn't support
        if client_context.is_mongos:
            with self.assertRaises(InvalidOperation):
                self.db.test.find(cursor_type=CursorType.EXHAUST)
        else:
            cursor = self.db.test.find(cursor_type=CursorType.EXHAUST)
            self.assertEqual(64, cursor._Cursor__query_flags)
            cursor2 = self.db.test.find().add_option(64)
            self.assertEqual(cursor._Cursor__query_flags, cursor2._Cursor__query_flags)
            self.assertTrue(cursor._Cursor__exhaust)
            cursor.remove_option(64)
            self.assertEqual(0, cursor._Cursor__query_flags)
            self.assertFalse(cursor._Cursor__exhaust)

    def test_allow_disk_use(self):
        db = self.db
        db.pymongo_test.drop()
        coll = db.pymongo_test

        self.assertRaises(TypeError, coll.find().allow_disk_use, "baz")

        cursor = coll.find().allow_disk_use(True)
        self.assertEqual(True, cursor._Cursor__allow_disk_use)  # type: ignore
        cursor = coll.find().allow_disk_use(False)
        self.assertEqual(False, cursor._Cursor__allow_disk_use)  # type: ignore

    def test_max_time_ms(self):
        db = self.db
        db.pymongo_test.drop()
        coll = db.pymongo_test
        self.assertRaises(TypeError, coll.find().max_time_ms, "foo")
        coll.insert_one({"amalia": 1})
        coll.insert_one({"amalia": 2})

        coll.find().max_time_ms(None)
        coll.find().max_time_ms(1)

        cursor = coll.find().max_time_ms(999)
        self.assertEqual(999, cursor._Cursor__max_time_ms)  # type: ignore
        cursor = coll.find().max_time_ms(10).max_time_ms(1000)
        self.assertEqual(1000, cursor._Cursor__max_time_ms)  # type: ignore

        cursor = coll.find().max_time_ms(999)
        c2 = cursor.clone()
        self.assertEqual(999, c2._Cursor__max_time_ms)  # type: ignore
        self.assertTrue("$maxTimeMS" in cursor._Cursor__query_spec())  # type: ignore
        self.assertTrue("$maxTimeMS" in c2._Cursor__query_spec())  # type: ignore

        self.assertTrue(coll.find_one(max_time_ms=1000))

        client = self.client
        if not client_context.is_mongos and client_context.test_commands_enabled:
            # Cursor parses server timeout error in response to initial query.
            client.admin.command("configureFailPoint", "maxTimeAlwaysTimeOut", mode="alwaysOn")
            try:
                cursor = coll.find().max_time_ms(1)
                try:
                    next(cursor)
                except ExecutionTimeout:
                    pass
                else:
                    self.fail("ExecutionTimeout not raised")
                self.assertRaises(ExecutionTimeout, coll.find_one, max_time_ms=1)
            finally:
                client.admin.command("configureFailPoint", "maxTimeAlwaysTimeOut", mode="off")

    def test_max_await_time_ms(self):
        db = self.db
        db.pymongo_test.drop()
        coll = db.create_collection("pymongo_test", capped=True, size=4096)

        self.assertRaises(TypeError, coll.find().max_await_time_ms, "foo")
        coll.insert_one({"amalia": 1})
        coll.insert_one({"amalia": 2})

        coll.find().max_await_time_ms(None)
        coll.find().max_await_time_ms(1)

        # When cursor is not tailable_await
        cursor = coll.find()
        self.assertEqual(None, cursor._Cursor__max_await_time_ms)
        cursor = coll.find().max_await_time_ms(99)
        self.assertEqual(None, cursor._Cursor__max_await_time_ms)

        # If cursor is tailable_await and timeout is unset
        cursor = coll.find(cursor_type=CursorType.TAILABLE_AWAIT)
        self.assertEqual(None, cursor._Cursor__max_await_time_ms)

        # If cursor is tailable_await and timeout is set
        cursor = coll.find(cursor_type=CursorType.TAILABLE_AWAIT).max_await_time_ms(99)
        self.assertEqual(99, cursor._Cursor__max_await_time_ms)

        cursor = (
            coll.find(cursor_type=CursorType.TAILABLE_AWAIT)
            .max_await_time_ms(10)
            .max_await_time_ms(90)
        )
        self.assertEqual(90, cursor._Cursor__max_await_time_ms)

        listener = AllowListEventListener("find", "getMore")
        coll = rs_or_single_client(event_listeners=[listener])[self.db.name].pymongo_test
        results = listener.results

        # Tailable_await defaults.
        list(coll.find(cursor_type=CursorType.TAILABLE_AWAIT))
        # find
        self.assertFalse("maxTimeMS" in results["started"][0].command)
        # getMore
        self.assertFalse("maxTimeMS" in results["started"][1].command)
        results.clear()

        # Tailable_await with max_await_time_ms set.
        list(coll.find(cursor_type=CursorType.TAILABLE_AWAIT).max_await_time_ms(99))
        # find
        self.assertEqual("find", results["started"][0].command_name)
        self.assertFalse("maxTimeMS" in results["started"][0].command)
        # getMore
        self.assertEqual("getMore", results["started"][1].command_name)
        self.assertTrue("maxTimeMS" in results["started"][1].command)
        self.assertEqual(99, results["started"][1].command["maxTimeMS"])
        results.clear()

        # Tailable_await with max_time_ms
        list(coll.find(cursor_type=CursorType.TAILABLE_AWAIT).max_time_ms(99))
        # find
        self.assertEqual("find", results["started"][0].command_name)
        self.assertTrue("maxTimeMS" in results["started"][0].command)
        self.assertEqual(99, results["started"][0].command["maxTimeMS"])
        # getMore
        self.assertEqual("getMore", results["started"][1].command_name)
        self.assertFalse("maxTimeMS" in results["started"][1].command)
        results.clear()

        # Tailable_await with both max_time_ms and max_await_time_ms
        list(coll.find(cursor_type=CursorType.TAILABLE_AWAIT).max_time_ms(99).max_await_time_ms(99))
        # find
        self.assertEqual("find", results["started"][0].command_name)
        self.assertTrue("maxTimeMS" in results["started"][0].command)
        self.assertEqual(99, results["started"][0].command["maxTimeMS"])
        # getMore
        self.assertEqual("getMore", results["started"][1].command_name)
        self.assertTrue("maxTimeMS" in results["started"][1].command)
        self.assertEqual(99, results["started"][1].command["maxTimeMS"])
        results.clear()

        # Non tailable_await with max_await_time_ms
        list(coll.find(batch_size=1).max_await_time_ms(99))
        # find
        self.assertEqual("find", results["started"][0].command_name)
        self.assertFalse("maxTimeMS" in results["started"][0].command)
        # getMore
        self.assertEqual("getMore", results["started"][1].command_name)
        self.assertFalse("maxTimeMS" in results["started"][1].command)
        results.clear()

        # Non tailable_await with max_time_ms
        list(coll.find(batch_size=1).max_time_ms(99))
        # find
        self.assertEqual("find", results["started"][0].command_name)
        self.assertTrue("maxTimeMS" in results["started"][0].command)
        self.assertEqual(99, results["started"][0].command["maxTimeMS"])
        # getMore
        self.assertEqual("getMore", results["started"][1].command_name)
        self.assertFalse("maxTimeMS" in results["started"][1].command)

        # Non tailable_await with both max_time_ms and max_await_time_ms
        list(coll.find(batch_size=1).max_time_ms(99).max_await_time_ms(88))
        # find
        self.assertEqual("find", results["started"][0].command_name)
        self.assertTrue("maxTimeMS" in results["started"][0].command)
        self.assertEqual(99, results["started"][0].command["maxTimeMS"])
        # getMore
        self.assertEqual("getMore", results["started"][1].command_name)
        self.assertFalse("maxTimeMS" in results["started"][1].command)

    @client_context.require_test_commands
    @client_context.require_no_mongos
    def test_max_time_ms_getmore(self):
        # Test that Cursor handles server timeout error in response to getmore.
        coll = self.db.pymongo_test
        coll.insert_many([{} for _ in range(200)])
        cursor = coll.find().max_time_ms(100)

        # Send initial query before turning on failpoint.
        next(cursor)
        self.client.admin.command("configureFailPoint", "maxTimeAlwaysTimeOut", mode="alwaysOn")
        try:
            try:
                # Iterate up to first getmore.
                list(cursor)
            except ExecutionTimeout:
                pass
            else:
                self.fail("ExecutionTimeout not raised")
        finally:
            self.client.admin.command("configureFailPoint", "maxTimeAlwaysTimeOut", mode="off")

    def test_explain(self):
        a = self.db.test.find()
        a.explain()
        for _ in a:
            break
        b = a.explain()
        self.assertIn("executionStats", b)

    def test_explain_with_read_concern(self):
        # Do not add readConcern level to explain.
        listener = AllowListEventListener("explain")
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        coll = client.pymongo_test.test.with_options(read_concern=ReadConcern(level="local"))
        self.assertTrue(coll.find().explain())
        started = listener.results["started"]
        self.assertEqual(len(started), 1)
        self.assertNotIn("readConcern", started[0].command)

    def test_hint(self):
        db = self.db
        self.assertRaises(TypeError, db.test.find().hint, 5.5)
        db.test.drop()

        db.test.insert_many([{"num": i, "foo": i} for i in range(100)])

        self.assertRaises(
            OperationFailure,
            db.test.find({"num": 17, "foo": 17}).hint([("num", ASCENDING)]).explain,
        )
        self.assertRaises(
            OperationFailure,
            db.test.find({"num": 17, "foo": 17}).hint([("foo", ASCENDING)]).explain,
        )

        spec = [("num", DESCENDING)]
        _ = db.test.create_index(spec)

        first = next(db.test.find())
        self.assertEqual(0, first.get("num"))
        first = next(db.test.find().hint(spec))
        self.assertEqual(99, first.get("num"))
        self.assertRaises(
            OperationFailure,
            db.test.find({"num": 17, "foo": 17}).hint([("foo", ASCENDING)]).explain,
        )

        a = db.test.find({"num": 17})
        a.hint(spec)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.hint, spec)

    def test_hint_by_name(self):
        db = self.db
        db.test.drop()

        db.test.insert_many([{"i": i} for i in range(100)])

        db.test.create_index([("i", DESCENDING)], name="fooindex")
        first = next(db.test.find())
        self.assertEqual(0, first.get("i"))
        first = next(db.test.find().hint("fooindex"))
        self.assertEqual(99, first.get("i"))

    def test_limit(self):
        db = self.db

        self.assertRaises(TypeError, db.test.find().limit, None)
        self.assertRaises(TypeError, db.test.find().limit, "hello")
        self.assertRaises(TypeError, db.test.find().limit, 5.5)
        self.assertTrue(db.test.find().limit(5))

        db.test.drop()
        db.test.insert_many([{"x": i} for i in range(100)])

        count = 0
        for _ in db.test.find():
            count += 1
        self.assertEqual(count, 100)

        count = 0
        for _ in db.test.find().limit(20):
            count += 1
        self.assertEqual(count, 20)

        count = 0
        for _ in db.test.find().limit(99):
            count += 1
        self.assertEqual(count, 99)

        count = 0
        for _ in db.test.find().limit(1):
            count += 1
        self.assertEqual(count, 1)

        count = 0
        for _ in db.test.find().limit(0):
            count += 1
        self.assertEqual(count, 100)

        count = 0
        for _ in db.test.find().limit(0).limit(50).limit(10):
            count += 1
        self.assertEqual(count, 10)

        a = db.test.find()
        a.limit(10)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.limit, 5)

    def test_max(self):
        db = self.db
        db.test.drop()
        j_index = [("j", ASCENDING)]
        db.test.create_index(j_index)

        db.test.insert_many([{"j": j, "k": j} for j in range(10)])

        def find(max_spec, expected_index):
            return db.test.find().max(max_spec).hint(expected_index)

        cursor = find([("j", 3)], j_index)
        self.assertEqual(len(list(cursor)), 3)

        # Tuple.
        cursor = find((("j", 3),), j_index)
        self.assertEqual(len(list(cursor)), 3)

        # Compound index.
        index_keys = [("j", ASCENDING), ("k", ASCENDING)]
        db.test.create_index(index_keys)
        cursor = find([("j", 3), ("k", 3)], index_keys)
        self.assertEqual(len(list(cursor)), 3)

        # Wrong order.
        cursor = find([("k", 3), ("j", 3)], index_keys)
        self.assertRaises(OperationFailure, list, cursor)

        # No such index.
        cursor = find([("k", 3)], "k")
        self.assertRaises(OperationFailure, list, cursor)

        self.assertRaises(TypeError, db.test.find().max, 10)
        self.assertRaises(TypeError, db.test.find().max, {"j": 10})

    def test_min(self):
        db = self.db
        db.test.drop()
        j_index = [("j", ASCENDING)]
        db.test.create_index(j_index)

        db.test.insert_many([{"j": j, "k": j} for j in range(10)])

        def find(min_spec, expected_index):
            return db.test.find().min(min_spec).hint(expected_index)

        cursor = find([("j", 3)], j_index)
        self.assertEqual(len(list(cursor)), 7)

        # Tuple.
        cursor = find((("j", 3),), j_index)
        self.assertEqual(len(list(cursor)), 7)

        # Compound index.
        index_keys = [("j", ASCENDING), ("k", ASCENDING)]
        db.test.create_index(index_keys)
        cursor = find([("j", 3), ("k", 3)], index_keys)
        self.assertEqual(len(list(cursor)), 7)

        # Wrong order.
        cursor = find([("k", 3), ("j", 3)], index_keys)
        self.assertRaises(OperationFailure, list, cursor)

        # No such index.
        cursor = find([("k", 3)], "k")
        self.assertRaises(OperationFailure, list, cursor)

        self.assertRaises(TypeError, db.test.find().min, 10)
        self.assertRaises(TypeError, db.test.find().min, {"j": 10})

    def test_min_max_without_hint(self):
        coll = self.db.test
        j_index = [("j", ASCENDING)]
        coll.create_index(j_index)

        with self.assertRaises(InvalidOperation):
            list(coll.find().min([("j", 3)]))
        with self.assertRaises(InvalidOperation):
            list(coll.find().max([("j", 3)]))

    def test_batch_size(self):
        db = self.db
        db.test.drop()
        db.test.insert_many([{"x": x} for x in range(200)])

        self.assertRaises(TypeError, db.test.find().batch_size, None)
        self.assertRaises(TypeError, db.test.find().batch_size, "hello")
        self.assertRaises(TypeError, db.test.find().batch_size, 5.5)
        self.assertRaises(ValueError, db.test.find().batch_size, -1)
        self.assertTrue(db.test.find().batch_size(5))
        a = db.test.find()
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.batch_size, 5)

        def cursor_count(cursor, expected_count):
            count = 0
            for _ in cursor:
                count += 1
            self.assertEqual(expected_count, count)

        cursor_count(db.test.find().batch_size(0), 200)
        cursor_count(db.test.find().batch_size(1), 200)
        cursor_count(db.test.find().batch_size(2), 200)
        cursor_count(db.test.find().batch_size(5), 200)
        cursor_count(db.test.find().batch_size(100), 200)
        cursor_count(db.test.find().batch_size(500), 200)

        cursor_count(db.test.find().batch_size(0).limit(1), 1)
        cursor_count(db.test.find().batch_size(1).limit(1), 1)
        cursor_count(db.test.find().batch_size(2).limit(1), 1)
        cursor_count(db.test.find().batch_size(5).limit(1), 1)
        cursor_count(db.test.find().batch_size(100).limit(1), 1)
        cursor_count(db.test.find().batch_size(500).limit(1), 1)

        cursor_count(db.test.find().batch_size(0).limit(10), 10)
        cursor_count(db.test.find().batch_size(1).limit(10), 10)
        cursor_count(db.test.find().batch_size(2).limit(10), 10)
        cursor_count(db.test.find().batch_size(5).limit(10), 10)
        cursor_count(db.test.find().batch_size(100).limit(10), 10)
        cursor_count(db.test.find().batch_size(500).limit(10), 10)

        cur = db.test.find().batch_size(1)
        next(cur)
        # find command batchSize should be 1
        self.assertEqual(0, len(cur._Cursor__data))
        next(cur)
        self.assertEqual(0, len(cur._Cursor__data))
        next(cur)
        self.assertEqual(0, len(cur._Cursor__data))
        next(cur)
        self.assertEqual(0, len(cur._Cursor__data))

    def test_limit_and_batch_size(self):
        db = self.db
        db.test.drop()
        db.test.insert_many([{"x": x} for x in range(500)])

        curs = db.test.find().limit(0).batch_size(10)
        next(curs)
        self.assertEqual(10, curs._Cursor__retrieved)

        curs = db.test.find(limit=0, batch_size=10)
        next(curs)
        self.assertEqual(10, curs._Cursor__retrieved)

        curs = db.test.find().limit(-2).batch_size(0)
        next(curs)
        self.assertEqual(2, curs._Cursor__retrieved)

        curs = db.test.find(limit=-2, batch_size=0)
        next(curs)
        self.assertEqual(2, curs._Cursor__retrieved)

        curs = db.test.find().limit(-4).batch_size(5)
        next(curs)
        self.assertEqual(4, curs._Cursor__retrieved)

        curs = db.test.find(limit=-4, batch_size=5)
        next(curs)
        self.assertEqual(4, curs._Cursor__retrieved)

        curs = db.test.find().limit(50).batch_size(500)
        next(curs)
        self.assertEqual(50, curs._Cursor__retrieved)

        curs = db.test.find(limit=50, batch_size=500)
        next(curs)
        self.assertEqual(50, curs._Cursor__retrieved)

        curs = db.test.find().batch_size(500)
        next(curs)
        self.assertEqual(500, curs._Cursor__retrieved)

        curs = db.test.find(batch_size=500)
        next(curs)
        self.assertEqual(500, curs._Cursor__retrieved)

        curs = db.test.find().limit(50)
        next(curs)
        self.assertEqual(50, curs._Cursor__retrieved)

        curs = db.test.find(limit=50)
        next(curs)
        self.assertEqual(50, curs._Cursor__retrieved)

        # these two might be shaky, as the default
        # is set by the server. as of 2.0.0-rc0, 101
        # or 1MB (whichever is smaller) is default
        # for queries without ntoreturn
        curs = db.test.find()
        next(curs)
        self.assertEqual(101, curs._Cursor__retrieved)

        curs = db.test.find().limit(0).batch_size(0)
        next(curs)
        self.assertEqual(101, curs._Cursor__retrieved)

        curs = db.test.find(limit=0, batch_size=0)
        next(curs)
        self.assertEqual(101, curs._Cursor__retrieved)

    def test_skip(self):
        db = self.db

        self.assertRaises(TypeError, db.test.find().skip, None)
        self.assertRaises(TypeError, db.test.find().skip, "hello")
        self.assertRaises(TypeError, db.test.find().skip, 5.5)
        self.assertRaises(ValueError, db.test.find().skip, -5)
        self.assertTrue(db.test.find().skip(5))

        db.drop_collection("test")

        db.test.insert_many([{"x": i} for i in range(100)])

        for i in db.test.find():
            self.assertEqual(i["x"], 0)
            break

        for i in db.test.find().skip(20):
            self.assertEqual(i["x"], 20)
            break

        for i in db.test.find().skip(99):
            self.assertEqual(i["x"], 99)
            break

        for i in db.test.find().skip(1):
            self.assertEqual(i["x"], 1)
            break

        for i in db.test.find().skip(0):
            self.assertEqual(i["x"], 0)
            break

        for i in db.test.find().skip(0).skip(50).skip(10):
            self.assertEqual(i["x"], 10)
            break

        for i in db.test.find().skip(1000):
            self.fail()

        a = db.test.find()
        a.skip(10)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.skip, 5)

    def test_sort(self):
        db = self.db

        self.assertRaises(TypeError, db.test.find().sort, 5)
        self.assertRaises(ValueError, db.test.find().sort, [])
        self.assertRaises(TypeError, db.test.find().sort, [], ASCENDING)
        self.assertRaises(TypeError, db.test.find().sort, [("hello", DESCENDING)], DESCENDING)

        db.test.drop()

        unsort = list(range(10))
        random.shuffle(unsort)

        db.test.insert_many([{"x": i} for i in unsort])

        asc = [i["x"] for i in db.test.find().sort("x", ASCENDING)]
        self.assertEqual(asc, list(range(10)))
        asc = [i["x"] for i in db.test.find().sort("x")]
        self.assertEqual(asc, list(range(10)))
        asc = [i["x"] for i in db.test.find().sort([("x", ASCENDING)])]
        self.assertEqual(asc, list(range(10)))

        expect = list(reversed(range(10)))
        desc = [i["x"] for i in db.test.find().sort("x", DESCENDING)]
        self.assertEqual(desc, expect)
        desc = [i["x"] for i in db.test.find().sort([("x", DESCENDING)])]
        self.assertEqual(desc, expect)
        desc = [i["x"] for i in db.test.find().sort("x", ASCENDING).sort("x", DESCENDING)]
        self.assertEqual(desc, expect)

        expected = [(1, 5), (2, 5), (0, 3), (7, 3), (9, 2), (2, 1), (3, 1)]
        shuffled = list(expected)
        random.shuffle(shuffled)

        db.test.drop()
        for (a, b) in shuffled:
            db.test.insert_one({"a": a, "b": b})

        result = [
            (i["a"], i["b"]) for i in db.test.find().sort([("b", DESCENDING), ("a", ASCENDING)])
        ]
        self.assertEqual(result, expected)

        a = db.test.find()
        a.sort("x", ASCENDING)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.sort, "x", ASCENDING)

    def test_where(self):
        db = self.db
        db.test.drop()

        a = db.test.find()
        self.assertRaises(TypeError, a.where, 5)
        self.assertRaises(TypeError, a.where, None)
        self.assertRaises(TypeError, a.where, {})

        db.test.insert_many([{"x": i} for i in range(10)])

        self.assertEqual(3, len(list(db.test.find().where("this.x < 3"))))
        self.assertEqual(3, len(list(db.test.find().where(Code("this.x < 3")))))

        code_with_scope = Code("this.x < i", {"i": 3})
        if client_context.version.at_least(4, 3, 3):
            # MongoDB 4.4 removed support for Code with scope.
            with self.assertRaises(OperationFailure):
                list(db.test.find().where(code_with_scope))

            code_with_empty_scope = Code("this.x < 3", {})
            with self.assertRaises(OperationFailure):
                list(db.test.find().where(code_with_empty_scope))
        else:
            self.assertEqual(3, len(list(db.test.find().where(code_with_scope))))

        self.assertEqual(10, len(list(db.test.find())))
        self.assertEqual([0, 1, 2], [a["x"] for a in db.test.find().where("this.x < 3")])
        self.assertEqual([], [a["x"] for a in db.test.find({"x": 5}).where("this.x < 3")])
        self.assertEqual([5], [a["x"] for a in db.test.find({"x": 5}).where("this.x > 3")])

        cursor = db.test.find().where("this.x < 3").where("this.x > 7")
        self.assertEqual([8, 9], [a["x"] for a in cursor])

        a = db.test.find()
        _ = a.where("this.x > 3")
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.where, "this.x < 3")

    def test_rewind(self):
        self.db.test.insert_many([{"x": i} for i in range(1, 4)])

        cursor = self.db.test.find().limit(2)

        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(0, count)

        cursor.rewind()
        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        cursor.rewind()
        count = 0
        for _ in cursor:
            break
        cursor.rewind()
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        self.assertEqual(cursor, cursor.rewind())

    # oplog_reply, and snapshot are all deprecated.
    @ignore_deprecations
    def test_clone(self):
        self.db.test.insert_many([{"x": i} for i in range(1, 4)])

        cursor = self.db.test.find().limit(2)

        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(0, count)

        cursor = cursor.clone()
        cursor2 = cursor.clone()
        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)
        for _ in cursor2:
            count += 1
        self.assertEqual(4, count)

        cursor.rewind()
        count = 0
        for _ in cursor:
            break
        cursor = cursor.clone()
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        self.assertNotEqual(cursor, cursor.clone())

        # Just test attributes
        cursor = self.db.test.find(
            {"x": re.compile("^hello.*")},
            projection={"_id": False},
            skip=1,
            no_cursor_timeout=True,
            cursor_type=CursorType.TAILABLE_AWAIT,
            sort=[("x", 1)],
            allow_partial_results=True,
            oplog_replay=True,
            batch_size=123,
            collation={"locale": "en_US"},
            hint=[("_id", 1)],
            max_scan=100,
            max_time_ms=1000,
            return_key=True,
            show_record_id=True,
            snapshot=True,
            allow_disk_use=True,
        ).limit(2)
        cursor.min([("a", 1)]).max([("b", 3)])
        cursor.add_option(128)
        cursor.comment("hi!")

        # Every attribute should be the same.
        cursor2 = cursor.clone()
        self.assertEqual(cursor.__dict__, cursor2.__dict__)

        # Shallow copies can so can mutate
        cursor2 = copy.copy(cursor)
        cursor2._Cursor__projection["cursor2"] = False
        self.assertTrue("cursor2" in cursor._Cursor__projection)

        # Deepcopies and shouldn't mutate
        cursor3 = copy.deepcopy(cursor)
        cursor3._Cursor__projection["cursor3"] = False
        self.assertFalse("cursor3" in cursor._Cursor__projection)

        cursor4 = cursor.clone()
        cursor4._Cursor__projection["cursor4"] = False
        self.assertFalse("cursor4" in cursor._Cursor__projection)

        # Test memo when deepcopying queries
        query = {"hello": "world"}
        query["reflexive"] = query
        cursor = self.db.test.find(query)

        cursor2 = copy.deepcopy(cursor)

        self.assertNotEqual(id(cursor._Cursor__spec), id(cursor2._Cursor__spec))
        self.assertEqual(id(cursor2._Cursor__spec["reflexive"]), id(cursor2._Cursor__spec))
        self.assertEqual(len(cursor2._Cursor__spec), 2)

        # Ensure hints are cloned as the correct type
        cursor = self.db.test.find().hint([("z", 1), ("a", 1)])
        cursor2 = copy.deepcopy(cursor)
        self.assertTrue(isinstance(cursor2._Cursor__hint, SON))
        self.assertEqual(cursor._Cursor__hint, cursor2._Cursor__hint)

    def test_clone_empty(self):
        self.db.test.delete_many({})
        self.db.test.insert_many([{"x": i} for i in range(1, 4)])
        cursor = self.db.test.find()[2:2]
        cursor2 = cursor.clone()
        self.assertRaises(StopIteration, cursor.next)
        self.assertRaises(StopIteration, cursor2.next)

    def test_bad_getitem(self):
        self.assertRaises(TypeError, lambda x: self.db.test.find()[x], "hello")
        self.assertRaises(TypeError, lambda x: self.db.test.find()[x], 5.5)
        self.assertRaises(TypeError, lambda x: self.db.test.find()[x], None)

    def test_getitem_slice_index(self):
        self.db.drop_collection("test")
        self.db.test.insert_many([{"i": i} for i in range(100)])

        count = itertools.count

        self.assertRaises(IndexError, lambda: self.db.test.find()[-1:])
        self.assertRaises(IndexError, lambda: self.db.test.find()[1:2:2])

        for a, b in zip(count(0), self.db.test.find()):
            self.assertEqual(a, b["i"])

        self.assertEqual(100, len(list(self.db.test.find()[0:])))
        for a, b in zip(count(0), self.db.test.find()[0:]):
            self.assertEqual(a, b["i"])

        self.assertEqual(80, len(list(self.db.test.find()[20:])))
        for a, b in zip(count(20), self.db.test.find()[20:]):
            self.assertEqual(a, b["i"])

        for a, b in zip(count(99), self.db.test.find()[99:]):
            self.assertEqual(a, b["i"])

        for i in self.db.test.find()[1000:]:
            self.fail()

        self.assertEqual(5, len(list(self.db.test.find()[20:25])))
        self.assertEqual(5, len(list(self.db.test.find()[20:25])))
        for a, b in zip(count(20), self.db.test.find()[20:25]):
            self.assertEqual(a, b["i"])

        self.assertEqual(80, len(list(self.db.test.find()[40:45][20:])))
        for a, b in zip(count(20), self.db.test.find()[40:45][20:]):
            self.assertEqual(a, b["i"])

        self.assertEqual(80, len(list(self.db.test.find()[40:45].limit(0).skip(20))))
        for a, b in zip(count(20), self.db.test.find()[40:45].limit(0).skip(20)):
            self.assertEqual(a, b["i"])

        self.assertEqual(80, len(list(self.db.test.find().limit(10).skip(40)[20:])))
        for a, b in zip(count(20), self.db.test.find().limit(10).skip(40)[20:]):
            self.assertEqual(a, b["i"])

        self.assertEqual(1, len(list(self.db.test.find()[:1])))
        self.assertEqual(5, len(list(self.db.test.find()[:5])))

        self.assertEqual(1, len(list(self.db.test.find()[99:100])))
        self.assertEqual(1, len(list(self.db.test.find()[99:1000])))
        self.assertEqual(0, len(list(self.db.test.find()[10:10])))
        self.assertEqual(0, len(list(self.db.test.find()[:0])))
        self.assertEqual(80, len(list(self.db.test.find()[10:10].limit(0).skip(20))))

        self.assertRaises(IndexError, lambda: self.db.test.find()[10:8])

    def test_getitem_numeric_index(self):
        self.db.drop_collection("test")
        self.db.test.insert_many([{"i": i} for i in range(100)])

        self.assertEqual(0, self.db.test.find()[0]["i"])
        self.assertEqual(50, self.db.test.find()[50]["i"])
        self.assertEqual(50, self.db.test.find().skip(50)[0]["i"])
        self.assertEqual(50, self.db.test.find().skip(49)[1]["i"])
        self.assertEqual(50, self.db.test.find()[50]["i"])
        self.assertEqual(99, self.db.test.find()[99]["i"])

        self.assertRaises(IndexError, lambda x: self.db.test.find()[x], -1)
        self.assertRaises(IndexError, lambda x: self.db.test.find()[x], 100)
        self.assertRaises(IndexError, lambda x: self.db.test.find().skip(50)[x], 50)

    def test_len(self):
        self.assertRaises(TypeError, len, self.db.test.find())

    def test_properties(self):
        self.assertEqual(self.db.test, self.db.test.find().collection)

        def set_coll():
            self.db.test.find().collection = "hello"  # type: ignore

        self.assertRaises(AttributeError, set_coll)

    def test_get_more(self):
        db = self.db
        db.drop_collection("test")
        db.test.insert_many([{"i": i} for i in range(10)])
        self.assertEqual(10, len(list(db.test.find().batch_size(5))))

    def test_tailable(self):
        db = self.db
        db.drop_collection("test")
        db.create_collection("test", capped=True, size=1000, max=3)
        self.addCleanup(db.drop_collection, "test")
        cursor = db.test.find(cursor_type=CursorType.TAILABLE)

        db.test.insert_one({"x": 1})
        count = 0
        for doc in cursor:
            count += 1
            self.assertEqual(1, doc["x"])
        self.assertEqual(1, count)

        db.test.insert_one({"x": 2})
        count = 0
        for doc in cursor:
            count += 1
            self.assertEqual(2, doc["x"])
        self.assertEqual(1, count)

        db.test.insert_one({"x": 3})
        count = 0
        for doc in cursor:
            count += 1
            self.assertEqual(3, doc["x"])
        self.assertEqual(1, count)

        # Capped rollover - the collection can never
        # have more than 3 documents. Just make sure
        # this doesn't raise...
        db.test.insert_many([{"x": i} for i in range(4, 7)])
        self.assertEqual(0, len(list(cursor)))

        # and that the cursor doesn't think it's still alive.
        self.assertFalse(cursor.alive)

        self.assertEqual(3, db.test.count_documents({}))

        # __getitem__(index)
        for cursor in (
            db.test.find(cursor_type=CursorType.TAILABLE),
            db.test.find(cursor_type=CursorType.TAILABLE_AWAIT),
        ):
            self.assertEqual(4, cursor[0]["x"])
            self.assertEqual(5, cursor[1]["x"])
            self.assertEqual(6, cursor[2]["x"])

            cursor.rewind()
            self.assertEqual([4], [doc["x"] for doc in cursor[0:1]])
            cursor.rewind()
            self.assertEqual([5], [doc["x"] for doc in cursor[1:2]])
            cursor.rewind()
            self.assertEqual([6], [doc["x"] for doc in cursor[2:3]])
            cursor.rewind()
            self.assertEqual([4, 5], [doc["x"] for doc in cursor[0:2]])
            cursor.rewind()
            self.assertEqual([5, 6], [doc["x"] for doc in cursor[1:3]])
            cursor.rewind()
            self.assertEqual([4, 5, 6], [doc["x"] for doc in cursor[0:3]])

    def test_concurrent_close(self):
        """Ensure a tailable can be closed from another thread."""
        db = self.db
        db.drop_collection("test")
        db.create_collection("test", capped=True, size=1000, max=3)
        self.addCleanup(db.drop_collection, "test")
        cursor = db.test.find(cursor_type=CursorType.TAILABLE)

        def iterate_cursor():
            while cursor.alive:
                for doc in cursor:
                    pass

        t = threading.Thread(target=iterate_cursor)
        t.start()
        time.sleep(1)
        cursor.close()
        self.assertFalse(cursor.alive)
        t.join(3)
        self.assertFalse(t.is_alive())

    def test_distinct(self):
        self.db.drop_collection("test")

        self.db.test.insert_many([{"a": 1}, {"a": 2}, {"a": 2}, {"a": 2}, {"a": 3}])

        distinct = self.db.test.find({"a": {"$lt": 3}}).distinct("a")
        distinct.sort()

        self.assertEqual([1, 2], distinct)

        self.db.drop_collection("test")

        self.db.test.insert_one({"a": {"b": "a"}, "c": 12})
        self.db.test.insert_one({"a": {"b": "b"}, "c": 8})
        self.db.test.insert_one({"a": {"b": "c"}, "c": 12})
        self.db.test.insert_one({"a": {"b": "c"}, "c": 8})

        distinct = self.db.test.find({"c": 8}).distinct("a.b")
        distinct.sort()

        self.assertEqual(["b", "c"], distinct)

    @client_context.require_version_max(4, 1, 0, -1)
    def test_max_scan(self):
        self.db.drop_collection("test")
        self.db.test.insert_many([{} for _ in range(100)])

        self.assertEqual(100, len(list(self.db.test.find())))
        self.assertEqual(50, len(list(self.db.test.find().max_scan(50))))
        self.assertEqual(50, len(list(self.db.test.find().max_scan(90).max_scan(50))))

    def test_with_statement(self):
        self.db.drop_collection("test")
        self.db.test.insert_many([{} for _ in range(100)])

        c1 = self.db.test.find()
        with self.db.test.find() as c2:
            self.assertTrue(c2.alive)
        self.assertFalse(c2.alive)

        with self.db.test.find() as c2:
            self.assertEqual(100, len(list(c2)))
        self.assertFalse(c2.alive)
        self.assertTrue(c1.alive)

    @client_context.require_no_mongos
    def test_comment(self):
        self.client.drop_database(self.db)
        self.db.command("profile", 2)  # Profile ALL commands.
        try:
            list(self.db.test.find().comment("foo"))
            count = self.db.system.profile.count_documents(
                {"ns": "pymongo_test.test", "op": "query", "command.comment": "foo"}
            )
            self.assertEqual(count, 1)

            self.db.test.find().comment("foo").distinct("type")
            count = self.db.system.profile.count_documents(
                {
                    "ns": "pymongo_test.test",
                    "op": "command",
                    "command.distinct": "test",
                    "command.comment": "foo",
                }
            )
            self.assertEqual(count, 1)
        finally:
            self.db.command("profile", 0)  # Turn off profiling.
            self.db.system.profile.drop()

        self.db.test.insert_many([{}, {}])
        cursor = self.db.test.find()
        next(cursor)
        self.assertRaises(InvalidOperation, cursor.comment, "hello")

    def test_alive(self):
        self.db.test.delete_many({})
        self.db.test.insert_many([{} for _ in range(3)])
        self.addCleanup(self.db.test.delete_many, {})
        cursor = self.db.test.find().batch_size(2)
        n = 0
        while True:
            cursor.next()
            n += 1
            if 3 == n:
                self.assertFalse(cursor.alive)
                break

            self.assertTrue(cursor.alive)

    def test_close_kills_cursor_synchronously(self):
        # Kill any cursors possibly queued up by previous tests.
        gc.collect()
        self.client._process_periodic_tasks()

        listener = AllowListEventListener("killCursors")
        results = listener.results
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        coll = client[self.db.name].test_close_kills_cursors

        # Add some test data.
        docs_inserted = 1000
        coll.insert_many([{"i": i} for i in range(docs_inserted)])

        results.clear()

        # Close a cursor while it's still open on the server.
        cursor = coll.find().batch_size(10)
        self.assertTrue(bool(next(cursor)))
        self.assertLess(cursor.retrieved, docs_inserted)
        cursor.close()

        def assertCursorKilled():
            self.assertEqual(1, len(results["started"]))
            self.assertEqual("killCursors", results["started"][0].command_name)
            self.assertEqual(1, len(results["succeeded"]))
            self.assertEqual("killCursors", results["succeeded"][0].command_name)

        assertCursorKilled()
        results.clear()

        # Close a command cursor while it's still open on the server.
        cursor = coll.aggregate([], batchSize=10)
        self.assertTrue(bool(next(cursor)))
        cursor.close()

        # The cursor should be killed if it had a non-zero id.
        if cursor.cursor_id:
            assertCursorKilled()
        else:
            self.assertEqual(0, len(results["started"]))

    def test_delete_not_initialized(self):
        # Creating a cursor with invalid arguments will not run __init__
        # but will still call __del__, eg test.find(invalidKwarg=1).
        cursor = Cursor.__new__(Cursor)  # Skip calling __init__
        cursor.__del__()  # no error

    def test_getMore_does_not_send_readPreference(self):
        listener = AllowListEventListener("find", "getMore")
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        # We never send primary read preference so override the default.
        coll = client[self.db.name].get_collection(
            "test", read_preference=ReadPreference.PRIMARY_PREFERRED
        )

        coll.delete_many({})
        coll.insert_many([{} for _ in range(5)])
        self.addCleanup(coll.drop)

        list(coll.find(batch_size=3))
        started = listener.results["started"]
        self.assertEqual(2, len(started))
        self.assertEqual("find", started[0].command_name)
        if client_context.is_rs or client_context.is_mongos:
            self.assertIn("$readPreference", started[0].command)
        else:
            self.assertNotIn("$readPreference", started[0].command)
        self.assertEqual("getMore", started[1].command_name)
        self.assertNotIn("$readPreference", started[1].command)


class TestRawBatchCursor(IntegrationTest):
    def test_find_raw(self):
        c = self.db.test
        c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        c.insert_many(docs)
        batches = list(c.find_raw_batches().sort("_id"))
        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

    @client_context.require_transactions
    def test_find_raw_transaction(self):
        c = self.db.test
        c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        c.insert_many(docs)

        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        with client.start_session() as session:
            with session.start_transaction():
                batches = list(
                    client[self.db.name].test.find_raw_batches(session=session).sort("_id")
                )
                cmd = listener.results["started"][0]
                self.assertEqual(cmd.command_name, "find")
                self.assertIn("$clusterTime", cmd.command)
                self.assertEqual(cmd.command["startTransaction"], True)
                self.assertEqual(cmd.command["txnNumber"], 1)
                # Ensure we update $clusterTime from the command response.
                last_cmd = listener.results["succeeded"][-1]
                self.assertEqual(
                    last_cmd.reply["$clusterTime"]["clusterTime"],
                    session.cluster_time["clusterTime"],
                )

        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

    @client_context.require_sessions
    @client_context.require_failCommand_fail_point
    def test_find_raw_retryable_reads(self):
        c = self.db.test
        c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        c.insert_many(docs)

        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener], retryReads=True)
        with self.fail_point(
            {"mode": {"times": 1}, "data": {"failCommands": ["find"], "closeConnection": True}}
        ):
            batches = list(client[self.db.name].test.find_raw_batches().sort("_id"))

        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))
        self.assertEqual(len(listener.results["started"]), 2)
        for cmd in listener.results["started"]:
            self.assertEqual(cmd.command_name, "find")

    @client_context.require_version_min(5, 0, 0)
    @client_context.require_no_standalone
    def test_find_raw_snapshot_reads(self):
        c = self.db.get_collection("test", write_concern=WriteConcern(w="majority"))
        c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        c.insert_many(docs)

        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener], retryReads=True)
        db = client[self.db.name]
        with client.start_session(snapshot=True) as session:
            db.test.distinct("x", {}, session=session)
            batches = list(db.test.find_raw_batches(session=session).sort("_id"))
        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

        find_cmd = listener.results["started"][1].command
        self.assertEqual(find_cmd["readConcern"]["level"], "snapshot")
        self.assertIsNotNone(find_cmd["readConcern"]["atClusterTime"])

    def test_explain(self):
        c = self.db.test
        c.insert_one({})
        explanation = c.find_raw_batches().explain()
        self.assertIsInstance(explanation, dict)

    def test_empty(self):
        self.db.test.drop()
        cursor = self.db.test.find_raw_batches()
        with self.assertRaises(StopIteration):
            next(cursor)

    def test_clone(self):
        self.db.test.insert_one({})
        cursor = self.db.test.find_raw_batches()
        # Copy of a RawBatchCursor is also a RawBatchCursor, not a Cursor.
        self.assertIsInstance(next(cursor.clone()), bytes)
        self.assertIsInstance(next(copy.copy(cursor)), bytes)

    @client_context.require_no_mongos
    def test_exhaust(self):
        c = self.db.test
        c.drop()
        c.insert_many({"_id": i} for i in range(200))
        result = b"".join(c.find_raw_batches(cursor_type=CursorType.EXHAUST))
        self.assertEqual([{"_id": i} for i in range(200)], decode_all(result))

    def test_server_error(self):
        with self.assertRaises(OperationFailure) as exc:
            next(self.db.test.find_raw_batches({"x": {"$bad": 1}}))

        # The server response was decoded, not left raw.
        self.assertIsInstance(exc.exception.details, dict)

    def test_get_item(self):
        with self.assertRaises(InvalidOperation):
            self.db.test.find_raw_batches()[0]

    def test_collation(self):
        next(self.db.test.find_raw_batches(collation=Collation("en_US")))

    @client_context.require_no_mmap  # MMAPv1 does not support read concern
    def test_read_concern(self):
        self.db.get_collection("test", write_concern=WriteConcern(w="majority")).insert_one({})
        c = self.db.get_collection("test", read_concern=ReadConcern("majority"))
        next(c.find_raw_batches())

    def test_monitoring(self):
        listener = EventListener()
        client = rs_or_single_client(event_listeners=[listener])
        c = client.pymongo_test.test
        c.drop()
        c.insert_many([{"_id": i} for i in range(10)])

        listener.results.clear()
        cursor = c.find_raw_batches(batch_size=4)

        # First raw batch of 4 documents.
        next(cursor)

        started = listener.results["started"][0]
        succeeded = listener.results["succeeded"][0]
        self.assertEqual(0, len(listener.results["failed"]))
        self.assertEqual("find", started.command_name)
        self.assertEqual("pymongo_test", started.database_name)
        self.assertEqual("find", succeeded.command_name)
        csr = succeeded.reply["cursor"]
        self.assertEqual(csr["ns"], "pymongo_test.test")

        # The batch is a list of one raw bytes object.
        self.assertEqual(len(csr["firstBatch"]), 1)
        self.assertEqual(decode_all(csr["firstBatch"][0]), [{"_id": i} for i in range(0, 4)])

        listener.results.clear()

        # Next raw batch of 4 documents.
        next(cursor)
        try:
            results = listener.results
            started = results["started"][0]
            succeeded = results["succeeded"][0]
            self.assertEqual(0, len(results["failed"]))
            self.assertEqual("getMore", started.command_name)
            self.assertEqual("pymongo_test", started.database_name)
            self.assertEqual("getMore", succeeded.command_name)
            csr = succeeded.reply["cursor"]
            self.assertEqual(csr["ns"], "pymongo_test.test")
            self.assertEqual(len(csr["nextBatch"]), 1)
            self.assertEqual(decode_all(csr["nextBatch"][0]), [{"_id": i} for i in range(4, 8)])
        finally:
            # Finish the cursor.
            tuple(cursor)


class TestRawBatchCommandCursor(IntegrationTest):
    @classmethod
    def setUpClass(cls):
        super(TestRawBatchCommandCursor, cls).setUpClass()

    def test_aggregate_raw(self):
        c = self.db.test
        c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        c.insert_many(docs)
        batches = list(c.aggregate_raw_batches([{"$sort": {"_id": 1}}]))
        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

    @client_context.require_transactions
    def test_aggregate_raw_transaction(self):
        c = self.db.test
        c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        c.insert_many(docs)

        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        with client.start_session() as session:
            with session.start_transaction():
                batches = list(
                    client[self.db.name].test.aggregate_raw_batches(
                        [{"$sort": {"_id": 1}}], session=session
                    )
                )
                cmd = listener.results["started"][0]
                self.assertEqual(cmd.command_name, "aggregate")
                self.assertIn("$clusterTime", cmd.command)
                self.assertEqual(cmd.command["startTransaction"], True)
                self.assertEqual(cmd.command["txnNumber"], 1)
                # Ensure we update $clusterTime from the command response.
                last_cmd = listener.results["succeeded"][-1]
                self.assertEqual(
                    last_cmd.reply["$clusterTime"]["clusterTime"],
                    session.cluster_time["clusterTime"],
                )
        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

    @client_context.require_sessions
    @client_context.require_failCommand_fail_point
    def test_aggregate_raw_retryable_reads(self):
        c = self.db.test
        c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        c.insert_many(docs)

        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener], retryReads=True)
        with self.fail_point(
            {"mode": {"times": 1}, "data": {"failCommands": ["aggregate"], "closeConnection": True}}
        ):
            batches = list(client[self.db.name].test.aggregate_raw_batches([{"$sort": {"_id": 1}}]))

        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))
        self.assertEqual(len(listener.results["started"]), 3)
        cmds = listener.results["started"]
        self.assertEqual(cmds[0].command_name, "aggregate")
        self.assertEqual(cmds[1].command_name, "aggregate")

    @client_context.require_version_min(5, 0, -1)
    @client_context.require_no_standalone
    def test_aggregate_raw_snapshot_reads(self):
        c = self.db.get_collection("test", write_concern=WriteConcern(w="majority"))
        c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        c.insert_many(docs)

        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener], retryReads=True)
        db = client[self.db.name]
        with client.start_session(snapshot=True) as session:
            db.test.distinct("x", {}, session=session)
            batches = list(db.test.aggregate_raw_batches([{"$sort": {"_id": 1}}], session=session))
        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

        find_cmd = listener.results["started"][1].command
        self.assertEqual(find_cmd["readConcern"]["level"], "snapshot")
        self.assertIsNotNone(find_cmd["readConcern"]["atClusterTime"])

    def test_server_error(self):
        c = self.db.test
        c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        c.insert_many(docs)
        c.insert_one({"_id": 10, "x": "not a number"})

        with self.assertRaises(OperationFailure) as exc:
            list(
                self.db.test.aggregate_raw_batches(
                    [
                        {
                            "$sort": {"_id": 1},
                        },
                        {"$project": {"x": {"$multiply": [2, "$x"]}}},
                    ],
                    batchSize=4,
                )
            )

        # The server response was decoded, not left raw.
        self.assertIsInstance(exc.exception.details, dict)

    def test_get_item(self):
        with self.assertRaises(InvalidOperation):
            self.db.test.aggregate_raw_batches([])[0]

    def test_collation(self):
        next(self.db.test.aggregate_raw_batches([], collation=Collation("en_US")))

    def test_monitoring(self):
        listener = EventListener()
        client = rs_or_single_client(event_listeners=[listener])
        c = client.pymongo_test.test
        c.drop()
        c.insert_many([{"_id": i} for i in range(10)])

        listener.results.clear()
        cursor = c.aggregate_raw_batches([{"$sort": {"_id": 1}}], batchSize=4)

        # Start cursor, no initial batch.
        started = listener.results["started"][0]
        succeeded = listener.results["succeeded"][0]
        self.assertEqual(0, len(listener.results["failed"]))
        self.assertEqual("aggregate", started.command_name)
        self.assertEqual("pymongo_test", started.database_name)
        self.assertEqual("aggregate", succeeded.command_name)
        csr = succeeded.reply["cursor"]
        self.assertEqual(csr["ns"], "pymongo_test.test")

        # First batch is empty.
        self.assertEqual(len(csr["firstBatch"]), 0)
        listener.results.clear()

        # Batches of 4 documents.
        n = 0
        for batch in cursor:
            results = listener.results
            started = results["started"][0]
            succeeded = results["succeeded"][0]
            self.assertEqual(0, len(results["failed"]))
            self.assertEqual("getMore", started.command_name)
            self.assertEqual("pymongo_test", started.database_name)
            self.assertEqual("getMore", succeeded.command_name)
            csr = succeeded.reply["cursor"]
            self.assertEqual(csr["ns"], "pymongo_test.test")
            self.assertEqual(len(csr["nextBatch"]), 1)
            self.assertEqual(csr["nextBatch"][0], batch)
            self.assertEqual(decode_all(batch), [{"_id": i} for i in range(n, min(n + 4, 10))])

            n += 4
            listener.results.clear()


if __name__ == "__main__":
    unittest.main()
