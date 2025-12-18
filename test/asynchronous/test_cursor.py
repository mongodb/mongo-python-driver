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
from __future__ import annotations

import copy
import gc
import itertools
import os
import random
import re
import sys
import threading
import time
from typing import Any

import pymongo

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest
from test.asynchronous.utils import flaky
from test.utils_shared import (
    AllowListEventListener,
    EventListener,
    OvertCommandListener,
    async_wait_until,
    delay,
    ignore_deprecations,
)

from bson import decode_all
from bson.code import Code
from bson.raw_bson import RawBSONDocument
from pymongo import ASCENDING, DESCENDING
from pymongo.asynchronous.cursor import AsyncCursor, CursorType
from pymongo.asynchronous.helpers import anext
from pymongo.collation import Collation
from pymongo.errors import ExecutionTimeout, InvalidOperation, OperationFailure, PyMongoError
from pymongo.operations import _IndexList
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

_IS_SYNC = False


class TestCursor(AsyncIntegrationTest):
    async def test_deepcopy_cursor_littered_with_regexes(self):
        cursor = self.db.test.find(
            {
                "x": re.compile("^hmmm.*"),
                "y": [re.compile("^hmm.*")],
                "z": {"a": [re.compile("^hm.*")]},
                re.compile("^key.*"): {"a": [re.compile("^hm.*")]},
            }
        )

        cursor2 = copy.deepcopy(cursor)
        self.assertEqual(cursor._spec, cursor2._spec)

    async def test_add_remove_option(self):
        cursor = self.db.test.find()
        self.assertEqual(0, cursor._query_flags)
        await cursor.add_option(2)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE)
        self.assertEqual(2, cursor2._query_flags)
        self.assertEqual(cursor._query_flags, cursor2._query_flags)
        await cursor.add_option(32)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE_AWAIT)
        self.assertEqual(34, cursor2._query_flags)
        self.assertEqual(cursor._query_flags, cursor2._query_flags)
        await cursor.add_option(128)
        cursor2 = await self.db.test.find(cursor_type=CursorType.TAILABLE_AWAIT).add_option(128)
        self.assertEqual(162, cursor2._query_flags)
        self.assertEqual(cursor._query_flags, cursor2._query_flags)

        self.assertEqual(162, cursor._query_flags)
        await cursor.add_option(128)
        self.assertEqual(162, cursor._query_flags)

        cursor.remove_option(128)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE_AWAIT)
        self.assertEqual(34, cursor2._query_flags)
        self.assertEqual(cursor._query_flags, cursor2._query_flags)
        cursor.remove_option(32)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE)
        self.assertEqual(2, cursor2._query_flags)
        self.assertEqual(cursor._query_flags, cursor2._query_flags)

        self.assertEqual(2, cursor._query_flags)
        cursor.remove_option(32)
        self.assertEqual(2, cursor._query_flags)

        # Timeout
        cursor = self.db.test.find(no_cursor_timeout=True)
        self.assertEqual(16, cursor._query_flags)
        cursor2 = await self.db.test.find().add_option(16)
        self.assertEqual(cursor._query_flags, cursor2._query_flags)
        cursor.remove_option(16)
        self.assertEqual(0, cursor._query_flags)

        # Tailable / Await data
        cursor = self.db.test.find(cursor_type=CursorType.TAILABLE_AWAIT)
        self.assertEqual(34, cursor._query_flags)
        cursor2 = await self.db.test.find().add_option(34)
        self.assertEqual(cursor._query_flags, cursor2._query_flags)
        cursor.remove_option(32)
        self.assertEqual(2, cursor._query_flags)

        # Partial
        cursor = self.db.test.find(allow_partial_results=True)
        self.assertEqual(128, cursor._query_flags)
        cursor2 = await self.db.test.find().add_option(128)
        self.assertEqual(cursor._query_flags, cursor2._query_flags)
        cursor.remove_option(128)
        self.assertEqual(0, cursor._query_flags)

    async def test_add_remove_option_exhaust(self):
        # Exhaust - which mongos doesn't support
        if async_client_context.is_mongos:
            with self.assertRaises(InvalidOperation):
                await anext(self.db.test.find(cursor_type=CursorType.EXHAUST))
        else:
            cursor = self.db.test.find(cursor_type=CursorType.EXHAUST)
            self.assertEqual(64, cursor._query_flags)
            cursor2 = await self.db.test.find().add_option(64)
            self.assertEqual(cursor._query_flags, cursor2._query_flags)
            self.assertTrue(cursor._exhaust)
            cursor.remove_option(64)
            self.assertEqual(0, cursor._query_flags)
            self.assertFalse(cursor._exhaust)

    async def test_allow_disk_use(self):
        db = self.db
        await db.pymongo_test.drop()
        coll = db.pymongo_test

        with self.assertRaises(TypeError):
            coll.find().allow_disk_use("baz")  # type: ignore[arg-type]

        cursor = coll.find().allow_disk_use(True)
        self.assertEqual(True, cursor._allow_disk_use)
        cursor = coll.find().allow_disk_use(False)
        self.assertEqual(False, cursor._allow_disk_use)

    async def test_max_time_ms(self):
        db = self.db
        await db.pymongo_test.drop()
        coll = db.pymongo_test
        with self.assertRaises(TypeError):
            coll.find().max_time_ms("foo")  # type: ignore[arg-type]
        await coll.insert_one({"amalia": 1})
        await coll.insert_one({"amalia": 2})

        coll.find().max_time_ms(None)
        coll.find().max_time_ms(1)

        cursor = coll.find().max_time_ms(999)
        self.assertEqual(999, cursor._max_time_ms)
        cursor = coll.find().max_time_ms(10).max_time_ms(1000)
        self.assertEqual(1000, cursor._max_time_ms)

        cursor = coll.find().max_time_ms(999)
        c2 = cursor.clone()
        self.assertEqual(999, c2._max_time_ms)
        self.assertIn("$maxTimeMS", cursor._query_spec())
        self.assertIn("$maxTimeMS", c2._query_spec())

        self.assertTrue(await coll.find_one(max_time_ms=1000))

        client = self.client
        if not async_client_context.is_mongos and async_client_context.test_commands_enabled:
            # Cursor parses server timeout error in response to initial query.
            await client.admin.command(
                "configureFailPoint", "maxTimeAlwaysTimeOut", mode="alwaysOn"
            )
            try:
                cursor = coll.find().max_time_ms(1)
                try:
                    await anext(cursor)
                except ExecutionTimeout:
                    pass
                else:
                    self.fail("ExecutionTimeout not raised")
                with self.assertRaises(ExecutionTimeout):
                    await coll.find_one(max_time_ms=1)
            finally:
                await client.admin.command("configureFailPoint", "maxTimeAlwaysTimeOut", mode="off")

    async def test_maxtime_ms_message(self):
        db = self.db
        await db.t.insert_one({"x": 1})
        with self.assertRaises(Exception) as error:
            await db.t.find_one({"$where": delay(2)}, max_time_ms=1)

        self.assertIn("(configured timeouts: connectTimeoutMS: 20000.0ms", str(error.exception))

        client = await self.async_rs_client(document_class=RawBSONDocument)
        await client.db.t.insert_one({"x": 1})
        with self.assertRaises(Exception) as error:
            await client.db.t.find_one({"$where": delay(2)}, max_time_ms=1)

        self.assertIn("(configured timeouts: connectTimeoutMS: 20000.0ms", str(error.exception))

    async def test_max_await_time_ms(self):
        db = self.db
        await db.pymongo_test.drop()
        coll = await db.create_collection("pymongo_test", capped=True, size=4096)

        with self.assertRaises(TypeError):
            coll.find().max_await_time_ms("foo")  # type: ignore[arg-type]
        await coll.insert_one({"amalia": 1})
        await coll.insert_one({"amalia": 2})

        coll.find().max_await_time_ms(None)
        coll.find().max_await_time_ms(1)

        # When cursor is not tailable_await
        cursor = coll.find()
        self.assertEqual(None, cursor._max_await_time_ms)
        cursor = coll.find().max_await_time_ms(99)
        self.assertEqual(None, cursor._max_await_time_ms)

        # If cursor is tailable_await and timeout is unset
        cursor = coll.find(cursor_type=CursorType.TAILABLE_AWAIT)
        self.assertEqual(None, cursor._max_await_time_ms)

        # If cursor is tailable_await and timeout is set
        cursor = coll.find(cursor_type=CursorType.TAILABLE_AWAIT).max_await_time_ms(99)
        self.assertEqual(99, cursor._max_await_time_ms)

        cursor = (
            coll.find(cursor_type=CursorType.TAILABLE_AWAIT)
            .max_await_time_ms(10)
            .max_await_time_ms(90)
        )
        self.assertEqual(90, cursor._max_await_time_ms)

        listener = AllowListEventListener("find", "getMore")
        coll = (await self.async_rs_or_single_client(event_listeners=[listener]))[
            self.db.name
        ].pymongo_test

        # Tailable_await defaults.
        await coll.find(cursor_type=CursorType.TAILABLE_AWAIT).to_list()
        # find
        self.assertNotIn("maxTimeMS", listener.started_events[0].command)
        # getMore
        self.assertNotIn("maxTimeMS", listener.started_events[1].command)
        listener.reset()

        # Tailable_await with max_await_time_ms set.
        await coll.find(cursor_type=CursorType.TAILABLE_AWAIT).max_await_time_ms(99).to_list()
        # find
        self.assertEqual("find", listener.started_events[0].command_name)
        self.assertNotIn("maxTimeMS", listener.started_events[0].command)
        # getMore
        self.assertEqual("getMore", listener.started_events[1].command_name)
        self.assertIn("maxTimeMS", listener.started_events[1].command)
        self.assertEqual(99, listener.started_events[1].command["maxTimeMS"])
        listener.reset()

        # Tailable_await with max_time_ms and make sure list() works on synchronous cursors
        if _IS_SYNC:
            list(coll.find(cursor_type=CursorType.TAILABLE_AWAIT).max_time_ms(99))  # type: ignore[call-overload]
        else:
            await coll.find(cursor_type=CursorType.TAILABLE_AWAIT).max_time_ms(99).to_list()
        # find
        self.assertEqual("find", listener.started_events[0].command_name)
        self.assertIn("maxTimeMS", listener.started_events[0].command)
        self.assertEqual(99, listener.started_events[0].command["maxTimeMS"])
        # getMore
        self.assertEqual("getMore", listener.started_events[1].command_name)
        self.assertNotIn("maxTimeMS", listener.started_events[1].command)
        listener.reset()

        # Tailable_await with both max_time_ms and max_await_time_ms
        await (
            coll.find(cursor_type=CursorType.TAILABLE_AWAIT)
            .max_time_ms(99)
            .max_await_time_ms(99)
            .to_list()
        )
        # find
        self.assertEqual("find", listener.started_events[0].command_name)
        self.assertIn("maxTimeMS", listener.started_events[0].command)
        self.assertEqual(99, listener.started_events[0].command["maxTimeMS"])
        # getMore
        self.assertEqual("getMore", listener.started_events[1].command_name)
        self.assertIn("maxTimeMS", listener.started_events[1].command)
        self.assertEqual(99, listener.started_events[1].command["maxTimeMS"])
        listener.reset()

        # Non tailable_await with max_await_time_ms
        await coll.find(batch_size=1).max_await_time_ms(99).to_list()
        # find
        self.assertEqual("find", listener.started_events[0].command_name)
        self.assertNotIn("maxTimeMS", listener.started_events[0].command)
        # getMore
        self.assertEqual("getMore", listener.started_events[1].command_name)
        self.assertNotIn("maxTimeMS", listener.started_events[1].command)
        listener.reset()

        # Non tailable_await with max_time_ms
        await coll.find(batch_size=1).max_time_ms(99).to_list()
        # find
        self.assertEqual("find", listener.started_events[0].command_name)
        self.assertIn("maxTimeMS", listener.started_events[0].command)
        self.assertEqual(99, listener.started_events[0].command["maxTimeMS"])
        # getMore
        self.assertEqual("getMore", listener.started_events[1].command_name)
        self.assertNotIn("maxTimeMS", listener.started_events[1].command)

        # Non tailable_await with both max_time_ms and max_await_time_ms
        await coll.find(batch_size=1).max_time_ms(99).max_await_time_ms(88).to_list()
        # find
        self.assertEqual("find", listener.started_events[0].command_name)
        self.assertIn("maxTimeMS", listener.started_events[0].command)
        self.assertEqual(99, listener.started_events[0].command["maxTimeMS"])
        # getMore
        self.assertEqual("getMore", listener.started_events[1].command_name)
        self.assertNotIn("maxTimeMS", listener.started_events[1].command)

    @async_client_context.require_test_commands
    @async_client_context.require_no_mongos
    async def test_max_time_ms_getmore(self):
        # Test that Cursor handles server timeout error in response to getmore.
        coll = self.db.pymongo_test
        await coll.insert_many([{} for _ in range(200)])
        cursor = coll.find().max_time_ms(100)

        # Send initial query before turning on failpoint.
        await anext(cursor)
        await self.client.admin.command(
            "configureFailPoint", "maxTimeAlwaysTimeOut", mode="alwaysOn"
        )
        try:
            try:
                # Iterate up to first getmore.
                await cursor.to_list()
            except ExecutionTimeout:
                pass
            else:
                self.fail("ExecutionTimeout not raised")
        finally:
            await self.client.admin.command(
                "configureFailPoint", "maxTimeAlwaysTimeOut", mode="off"
            )

    async def test_explain(self):
        a = self.db.test.find()
        await a.explain()
        async for _ in a:
            break
        b = await a.explain()
        self.assertIn("executionStats", b)

    async def test_explain_with_read_concern(self):
        # Do not add readConcern level to explain.
        listener = AllowListEventListener("explain")
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        coll = client.pymongo_test.test.with_options(read_concern=ReadConcern(level="local"))
        self.assertTrue(await coll.find().explain())
        started = listener.started_events
        self.assertEqual(len(started), 1)
        self.assertNotIn("readConcern", started[0].command)

    # https://github.com/mongodb/specifications/blob/master/source/crud/tests/README.md#14-explain-helpers-allow-users-to-specify-maxtimems
    async def test_explain_csot(self):
        # Create a MongoClient with command monitoring enabled (referred to as client).
        listener = AllowListEventListener("explain")
        client = await self.async_rs_or_single_client(event_listeners=[listener])

        # Create a collection, referred to as collection, with the namespace explain-test.collection.
        # Workaround for SERVER-108463
        names = await client["explain-test"].list_collection_names()
        if "collection" not in names:
            collection = await client["explain-test"].create_collection("collection")
        else:
            collection = client["explain-test"]["collection"]

        # Run an explained find on collection. The find will have the query predicate { name: 'john doe' }. Specify a maxTimeMS value of 2000ms for the explain.
        with pymongo.timeout(2.0):
            self.assertTrue(await collection.find({"name": "john doe"}).explain())

        # Obtain the command started event for the explain. Confirm that the top-level explain command should has a maxTimeMS value of 2000.
        started = listener.started_events
        self.assertEqual(len(started), 1)
        assert 1500 < started[0].command["maxTimeMS"] <= 2000

    async def test_hint(self):
        db = self.db
        with self.assertRaises(TypeError):
            db.test.find().hint(5.5)  # type: ignore[arg-type]
        await db.test.drop()

        await db.test.insert_many([{"num": i, "foo": i} for i in range(100)])

        with self.assertRaises(OperationFailure):
            await db.test.find({"num": 17, "foo": 17}).hint([("num", ASCENDING)]).explain()
        with self.assertRaises(OperationFailure):
            await db.test.find({"num": 17, "foo": 17}).hint([("foo", ASCENDING)]).explain()

        spec: list[Any] = [("num", DESCENDING)]
        _ = await db.test.create_index(spec)

        first = await anext(db.test.find())
        self.assertEqual(0, first.get("num"))
        first = await anext(db.test.find().hint(spec))
        self.assertEqual(99, first.get("num"))
        with self.assertRaises(OperationFailure):
            await db.test.find({"num": 17, "foo": 17}).hint([("foo", ASCENDING)]).explain()

        a = db.test.find({"num": 17})
        a.hint(spec)
        async for _ in a:
            break
        self.assertRaises(InvalidOperation, a.hint, spec)

        await db.test.drop()
        await db.test.insert_many([{"num": i, "foo": i} for i in range(100)])
        spec: _IndexList = ["num", ("foo", DESCENDING)]
        await db.test.create_index(spec)
        first = await anext(db.test.find().hint(spec))
        self.assertEqual(0, first.get("num"))
        self.assertEqual(0, first.get("foo"))

        await db.test.drop()
        await db.test.insert_many([{"num": i, "foo": i} for i in range(100)])
        spec = ["num"]
        await db.test.create_index(spec)
        first = await anext(db.test.find().hint(spec))
        self.assertEqual(0, first.get("num"))

    async def test_hint_by_name(self):
        db = self.db
        await db.test.drop()

        await db.test.insert_many([{"i": i} for i in range(100)])

        await db.test.create_index([("i", DESCENDING)], name="fooindex")
        first = await anext(db.test.find())
        self.assertEqual(0, first.get("i"))
        first = await anext(db.test.find().hint("fooindex"))
        self.assertEqual(99, first.get("i"))

    async def test_limit(self):
        db = self.db

        with self.assertRaises(TypeError):
            db.test.find().limit(None)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            db.test.find().limit("hello")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            db.test.find().limit(5.5)  # type: ignore[arg-type]
        self.assertTrue((db.test.find()).limit(5))

        await db.test.drop()
        await db.test.insert_many([{"x": i} for i in range(100)])

        count = 0
        async for _ in db.test.find():
            count += 1
        self.assertEqual(count, 100)

        count = 0
        async for _ in db.test.find().limit(20):
            count += 1
        self.assertEqual(count, 20)

        count = 0
        async for _ in db.test.find().limit(99):
            count += 1
        self.assertEqual(count, 99)

        count = 0
        async for _ in db.test.find().limit(1):
            count += 1
        self.assertEqual(count, 1)

        count = 0
        async for _ in db.test.find().limit(0):
            count += 1
        self.assertEqual(count, 100)

        count = 0
        async for _ in db.test.find().limit(0).limit(50).limit(10):
            count += 1
        self.assertEqual(count, 10)

        a = db.test.find()
        a.limit(10)
        async for _ in a:
            break
        with self.assertRaises(InvalidOperation):
            a.limit(5)

    async def test_max(self):
        db = self.db
        await db.test.drop()
        j_index = [("j", ASCENDING)]
        await db.test.create_index(j_index)

        await db.test.insert_many([{"j": j, "k": j} for j in range(10)])

        def find(max_spec, expected_index):
            return db.test.find().max(max_spec).hint(expected_index)

        cursor = find([("j", 3)], j_index)
        self.assertEqual(len(await cursor.to_list()), 3)

        # Tuple.
        cursor = find((("j", 3),), j_index)
        self.assertEqual(len(await cursor.to_list()), 3)

        # Compound index.
        index_keys = [("j", ASCENDING), ("k", ASCENDING)]
        await db.test.create_index(index_keys)
        cursor = find([("j", 3), ("k", 3)], index_keys)
        self.assertEqual(len(await cursor.to_list()), 3)

        # Wrong order.
        cursor = find([("k", 3), ("j", 3)], index_keys)
        with self.assertRaises(OperationFailure):
            await cursor.to_list()

        # No such index.
        cursor = find([("k", 3)], "k")
        with self.assertRaises(OperationFailure):
            await cursor.to_list()
        with self.assertRaises(TypeError):
            db.test.find().max(10)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            db.test.find().max({"j": 10})  # type: ignore[arg-type]

    async def test_min(self):
        db = self.db
        await db.test.drop()
        j_index = [("j", ASCENDING)]
        await db.test.create_index(j_index)

        await db.test.insert_many([{"j": j, "k": j} for j in range(10)])

        def find(min_spec, expected_index):
            return db.test.find().min(min_spec).hint(expected_index)

        cursor = find([("j", 3)], j_index)
        self.assertEqual(len(await cursor.to_list()), 7)

        # Tuple.
        cursor = find((("j", 3),), j_index)
        self.assertEqual(len(await cursor.to_list()), 7)

        # Compound index.
        index_keys = [("j", ASCENDING), ("k", ASCENDING)]
        await db.test.create_index(index_keys)
        cursor = find([("j", 3), ("k", 3)], index_keys)
        self.assertEqual(len(await cursor.to_list()), 7)

        # Wrong order.
        cursor = find([("k", 3), ("j", 3)], index_keys)
        with self.assertRaises(OperationFailure):
            await cursor.to_list()

        # No such index.
        cursor = find([("k", 3)], "k")
        with self.assertRaises(OperationFailure):
            await cursor.to_list()

        with self.assertRaises(TypeError):
            db.test.find().min(10)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            db.test.find().min({"j": 10})  # type: ignore[arg-type]

    async def test_min_max_without_hint(self):
        coll = self.db.test
        j_index = [("j", ASCENDING)]
        await coll.create_index(j_index)

        with self.assertRaises(InvalidOperation):
            await coll.find().min([("j", 3)]).to_list()
        with self.assertRaises(InvalidOperation):
            await coll.find().max([("j", 3)]).to_list()

    async def test_batch_size(self):
        db = self.db
        await db.test.drop()
        await db.test.insert_many([{"x": x} for x in range(200)])

        with self.assertRaises(TypeError):
            db.test.find().batch_size(None)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            db.test.find().batch_size("hello")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            db.test.find().batch_size(5.5)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            db.test.find().batch_size(-1)
        self.assertTrue((db.test.find()).batch_size(5))
        a = db.test.find()
        async for _ in a:
            break
        self.assertRaises(InvalidOperation, a.batch_size, 5)

        async def cursor_count(cursor, expected_count):
            count = 0
            async for _ in cursor:
                count += 1
            self.assertEqual(expected_count, count)

        await cursor_count((db.test.find()).batch_size(0), 200)
        await cursor_count((db.test.find()).batch_size(1), 200)
        await cursor_count((db.test.find()).batch_size(2), 200)
        await cursor_count((db.test.find()).batch_size(5), 200)
        await cursor_count((db.test.find()).batch_size(100), 200)
        await cursor_count((db.test.find()).batch_size(500), 200)

        await cursor_count((db.test.find()).batch_size(0).limit(1), 1)
        await cursor_count((db.test.find()).batch_size(1).limit(1), 1)
        await cursor_count((db.test.find()).batch_size(2).limit(1), 1)
        await cursor_count((db.test.find()).batch_size(5).limit(1), 1)
        await cursor_count((db.test.find()).batch_size(100).limit(1), 1)
        await cursor_count((db.test.find()).batch_size(500).limit(1), 1)

        await cursor_count((db.test.find()).batch_size(0).limit(10), 10)
        await cursor_count((db.test.find()).batch_size(1).limit(10), 10)
        await cursor_count((db.test.find()).batch_size(2).limit(10), 10)
        await cursor_count((db.test.find()).batch_size(5).limit(10), 10)
        await cursor_count((db.test.find()).batch_size(100).limit(10), 10)
        await cursor_count((db.test.find()).batch_size(500).limit(10), 10)

        cur = db.test.find().batch_size(1)
        await anext(cur)
        # find command batchSize should be 1
        self.assertEqual(0, len(cur._data))
        await anext(cur)
        self.assertEqual(0, len(cur._data))
        await anext(cur)
        self.assertEqual(0, len(cur._data))
        await anext(cur)
        self.assertEqual(0, len(cur._data))

    async def test_limit_and_batch_size(self):
        db = self.db
        await db.test.drop()
        await db.test.insert_many([{"x": x} for x in range(500)])

        curs = db.test.find().limit(0).batch_size(10)
        await anext(curs)
        self.assertEqual(10, curs._retrieved)

        curs = db.test.find(limit=0, batch_size=10)
        await anext(curs)
        self.assertEqual(10, curs._retrieved)

        curs = db.test.find().limit(-2).batch_size(0)
        await anext(curs)
        self.assertEqual(2, curs._retrieved)

        curs = db.test.find(limit=-2, batch_size=0)
        await anext(curs)
        self.assertEqual(2, curs._retrieved)

        curs = db.test.find().limit(-4).batch_size(5)
        await anext(curs)
        self.assertEqual(4, curs._retrieved)

        curs = db.test.find(limit=-4, batch_size=5)
        await anext(curs)
        self.assertEqual(4, curs._retrieved)

        curs = db.test.find().limit(50).batch_size(500)
        await anext(curs)
        self.assertEqual(50, curs._retrieved)

        curs = db.test.find(limit=50, batch_size=500)
        await anext(curs)
        self.assertEqual(50, curs._retrieved)

        curs = db.test.find().batch_size(500)
        await anext(curs)
        self.assertEqual(500, curs._retrieved)

        curs = db.test.find(batch_size=500)
        await anext(curs)
        self.assertEqual(500, curs._retrieved)

        curs = db.test.find().limit(50)
        await anext(curs)
        self.assertEqual(50, curs._retrieved)

        curs = db.test.find(limit=50)
        await anext(curs)
        self.assertEqual(50, curs._retrieved)

        # these two might be shaky, as the default
        # is set by the server. as of 2.0.0-rc0, 101
        # or 1MB (whichever is smaller) is default
        # for queries without ntoreturn
        curs = db.test.find()
        await anext(curs)
        self.assertEqual(101, curs._retrieved)

        curs = db.test.find().limit(0).batch_size(0)
        await anext(curs)
        self.assertEqual(101, curs._retrieved)

        curs = db.test.find(limit=0, batch_size=0)
        await anext(curs)
        self.assertEqual(101, curs._retrieved)

    async def test_skip(self):
        db = self.db

        with self.assertRaises(TypeError):
            db.test.find().skip(None)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            db.test.find().skip("hello")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            db.test.find().skip(5.5)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            db.test.find().skip(-5)
        self.assertTrue((db.test.find()).skip(5))

        await db.drop_collection("test")

        await db.test.insert_many([{"x": i} for i in range(100)])

        async for i in db.test.find():
            self.assertEqual(i["x"], 0)
            break

        async for i in db.test.find().skip(20):
            self.assertEqual(i["x"], 20)
            break

        async for i in db.test.find().skip(99):
            self.assertEqual(i["x"], 99)
            break

        async for i in db.test.find().skip(1):
            self.assertEqual(i["x"], 1)
            break

        async for i in db.test.find().skip(0):
            self.assertEqual(i["x"], 0)
            break

        async for i in db.test.find().skip(0).skip(50).skip(10):
            self.assertEqual(i["x"], 10)
            break

        async for _ in db.test.find().skip(1000):
            self.fail()

        a = db.test.find()
        a.skip(10)
        async for _ in a:
            break
        self.assertRaises(InvalidOperation, a.skip, 5)

    async def test_sort(self):
        db = self.db

        with self.assertRaises(TypeError):
            db.test.find().sort(5)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            db.test.find().sort([])  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            db.test.find().sort([], ASCENDING)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            db.test.find().sort([("hello", DESCENDING)], DESCENDING)  # type: ignore[arg-type]

        await db.test.drop()

        unsort = list(range(10))
        random.shuffle(unsort)

        await db.test.insert_many([{"x": i} for i in unsort])

        asc = [i["x"] async for i in db.test.find().sort("x", ASCENDING)]
        self.assertEqual(asc, list(range(10)))
        asc = [i["x"] async for i in db.test.find().sort("x")]
        self.assertEqual(asc, list(range(10)))
        asc = [i["x"] async for i in db.test.find().sort([("x", ASCENDING)])]
        self.assertEqual(asc, list(range(10)))

        expect = list(reversed(range(10)))
        desc = [i["x"] async for i in db.test.find().sort("x", DESCENDING)]
        self.assertEqual(desc, expect)
        desc = [i["x"] async for i in db.test.find().sort([("x", DESCENDING)])]
        self.assertEqual(desc, expect)
        desc = [i["x"] async for i in db.test.find().sort("x", ASCENDING).sort("x", DESCENDING)]
        self.assertEqual(desc, expect)

        expected = [(1, 5), (2, 5), (0, 3), (7, 3), (9, 2), (2, 1), (3, 1)]
        shuffled = list(expected)
        random.shuffle(shuffled)

        await db.test.drop()
        for a, b in shuffled:
            await db.test.insert_one({"a": a, "b": b})

        result = [
            (i["a"], i["b"])
            async for i in db.test.find().sort([("b", DESCENDING), ("a", ASCENDING)])
        ]
        self.assertEqual(result, expected)
        result = [(i["a"], i["b"]) async for i in db.test.find().sort([("b", DESCENDING), "a"])]
        self.assertEqual(result, expected)

        a = db.test.find()
        a.sort("x", ASCENDING)
        async for _ in a:
            break
        self.assertRaises(InvalidOperation, a.sort, "x", ASCENDING)

    async def test_where(self):
        db = self.db
        await db.test.drop()

        a = db.test.find()
        with self.assertRaises(TypeError):
            a.where(5)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            a.where(None)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            a.where({})  # type: ignore[arg-type]

        await db.test.insert_many([{"x": i} for i in range(10)])

        self.assertEqual(3, len(await db.test.find().where("this.x < 3").to_list()))
        self.assertEqual(3, len(await db.test.find().where(Code("this.x < 3")).to_list()))

        code_with_scope = Code("this.x < i", {"i": 3})
        if async_client_context.version.at_least(4, 3, 3):
            # MongoDB 4.4 removed support for Code with scope.
            with self.assertRaises(OperationFailure):
                await db.test.find().where(code_with_scope).to_list()

            code_with_empty_scope = Code("this.x < 3", {})
            with self.assertRaises(OperationFailure):
                await db.test.find().where(code_with_empty_scope).to_list()
        else:
            self.assertEqual(3, len(await db.test.find().where(code_with_scope).to_list()))

        self.assertEqual(10, len(await db.test.find().to_list()))
        self.assertEqual([0, 1, 2], [a["x"] async for a in db.test.find().where("this.x < 3")])
        self.assertEqual([], [a["x"] async for a in db.test.find({"x": 5}).where("this.x < 3")])
        self.assertEqual([5], [a["x"] async for a in db.test.find({"x": 5}).where("this.x > 3")])

        cursor = db.test.find().where("this.x < 3").where("this.x > 7")
        self.assertEqual([8, 9], [a["x"] async for a in cursor])

        a = db.test.find()
        _ = a.where("this.x > 3")
        async for _ in a:
            break
        self.assertRaises(InvalidOperation, a.where, "this.x < 3")

    async def test_rewind(self):
        await self.db.test.insert_many([{"x": i} for i in range(1, 4)])

        cursor = self.db.test.find().limit(2)

        count = 0
        async for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        count = 0
        async for _ in cursor:
            count += 1
        self.assertEqual(0, count)

        await cursor.rewind()
        count = 0
        async for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        await cursor.rewind()
        count = 0
        async for _ in cursor:
            break
        await cursor.rewind()
        async for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        self.assertEqual(cursor, await cursor.rewind())

    # oplog_reply, and snapshot are all deprecated.
    @ignore_deprecations
    async def test_clone(self):
        await self.db.test.insert_many([{"x": i} for i in range(1, 4)])

        cursor = self.db.test.find().limit(2)

        count = 0
        async for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        count = 0
        async for _ in cursor:
            count += 1
        self.assertEqual(0, count)

        cursor = cursor.clone()
        cursor2 = cursor.clone()
        count = 0
        async for _ in cursor:
            count += 1
        self.assertEqual(2, count)
        async for _ in cursor2:
            count += 1
        self.assertEqual(4, count)

        await cursor.rewind()
        count = 0
        async for _ in cursor:
            break
        cursor = cursor.clone()
        async for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        self.assertNotEqual(cursor, cursor.clone())

        # Just test attributes
        cursor = (
            self.db.test.find(
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
            )
        ).limit(2)
        cursor.min([("a", 1)]).max([("b", 3)])
        await cursor.add_option(128)
        cursor.comment("hi!")

        # Every attribute should be the same.
        cursor2 = cursor.clone()
        self.assertEqual(cursor.__dict__, cursor2.__dict__)

        # Shallow copies can so can mutate
        cursor2 = copy.copy(cursor)
        cursor2._projection["cursor2"] = False
        self.assertIsNotNone(cursor._projection)
        self.assertIn("cursor2", cursor._projection.keys())

        # Deepcopies and shouldn't mutate
        cursor3 = copy.deepcopy(cursor)
        cursor3._projection["cursor3"] = False
        self.assertIsNotNone(cursor._projection)
        self.assertNotIn("cursor3", cursor._projection.keys())

        cursor4 = cursor.clone()
        cursor4._projection["cursor4"] = False
        self.assertIsNotNone(cursor._projection)
        self.assertNotIn("cursor4", cursor._projection.keys())

        # Test memo when deepcopying queries
        query = {"hello": "world"}
        query["reflexive"] = query
        cursor = self.db.test.find(query)

        cursor2 = copy.deepcopy(cursor)

        self.assertNotEqual(id(cursor._spec), id(cursor2._spec))
        self.assertEqual(id(cursor2._spec["reflexive"]), id(cursor2._spec))
        self.assertEqual(len(cursor2._spec), 2)

        # Ensure hints are cloned as the correct type
        cursor = self.db.test.find().hint([("z", 1), ("a", 1)])
        cursor2 = copy.deepcopy(cursor)
        # Internal types are now dict rather than SON by default
        self.assertIsInstance(cursor2._hint, dict)
        self.assertEqual(cursor._hint, cursor2._hint)

    @async_client_context.require_sync
    def test_clone_empty(self):
        self.db.test.delete_many({})
        self.db.test.insert_many([{"x": i} for i in range(1, 4)])
        cursor = self.db.test.find()[2:2]
        cursor2 = cursor.clone()
        self.assertRaises(StopIteration, cursor.next)
        self.assertRaises(StopIteration, cursor2.next)

    # AsyncCursors don't support slicing
    @async_client_context.require_sync
    def test_bad_getitem(self):
        self.assertRaises(TypeError, lambda x: self.db.test.find()[x], "hello")
        self.assertRaises(TypeError, lambda x: self.db.test.find()[x], 5.5)
        self.assertRaises(TypeError, lambda x: self.db.test.find()[x], None)

    # AsyncCursors don't support slicing
    @async_client_context.require_sync
    def test_getitem_slice_index(self):
        self.db.drop_collection("test")
        self.db.test.insert_many([{"i": i} for i in range(100)])

        count = itertools.count

        self.assertRaises(IndexError, lambda: self.db.test.find()[-1:])
        self.assertRaises(IndexError, lambda: self.db.test.find()[1:2:2])

        for a, b in zip(count(0), self.db.test.find()):  # type: ignore[call-overload]
            self.assertEqual(a, b["i"])

        self.assertEqual(100, len(list(self.db.test.find()[0:])))  # type: ignore[call-overload]
        for a, b in zip(count(0), self.db.test.find()[0:]):  # type: ignore[call-overload]
            self.assertEqual(a, b["i"])

        self.assertEqual(80, len(list(self.db.test.find()[20:])))  # type: ignore[call-overload]
        for a, b in zip(count(20), self.db.test.find()[20:]):  # type: ignore[call-overload]
            self.assertEqual(a, b["i"])

        for a, b in zip(count(99), self.db.test.find()[99:]):  # type: ignore[call-overload]
            self.assertEqual(a, b["i"])

        for _i in self.db.test.find()[1000:]:
            self.fail()

        self.assertEqual(5, len(list(self.db.test.find()[20:25])))  # type: ignore[call-overload]
        self.assertEqual(5, len(list(self.db.test.find()[20:25])))  # type: ignore[call-overload]
        for a, b in zip(count(20), self.db.test.find()[20:25]):  # type: ignore[call-overload]
            self.assertEqual(a, b["i"])

        self.assertEqual(80, len(list(self.db.test.find()[40:45][20:])))  # type: ignore[call-overload]
        for a, b in zip(count(20), self.db.test.find()[40:45][20:]):  # type: ignore[call-overload]
            self.assertEqual(a, b["i"])

        self.assertEqual(80, len(list(self.db.test.find()[40:45].limit(0).skip(20))))  # type: ignore[call-overload]
        for a, b in zip(count(20), self.db.test.find()[40:45].limit(0).skip(20)):  # type: ignore[call-overload]
            self.assertEqual(a, b["i"])

        self.assertEqual(80, len(list(self.db.test.find().limit(10).skip(40)[20:])))  # type: ignore[call-overload]
        for a, b in zip(count(20), self.db.test.find().limit(10).skip(40)[20:]):  # type: ignore[call-overload]
            self.assertEqual(a, b["i"])

        self.assertEqual(1, len(list(self.db.test.find()[:1])))  # type: ignore[call-overload]
        self.assertEqual(5, len(list(self.db.test.find()[:5])))  # type: ignore[call-overload]

        self.assertEqual(1, len(list(self.db.test.find()[99:100])))  # type: ignore[call-overload]
        self.assertEqual(1, len(list(self.db.test.find()[99:1000])))  # type: ignore[call-overload]
        self.assertEqual(0, len(list(self.db.test.find()[10:10])))  # type: ignore[call-overload]
        self.assertEqual(0, len(list(self.db.test.find()[:0])))  # type: ignore[call-overload]
        self.assertEqual(80, len(list(self.db.test.find()[10:10].limit(0).skip(20))))  # type: ignore[call-overload]

        self.assertRaises(IndexError, lambda: self.db.test.find()[10:8])

    # AsyncCursors don't support slicing
    @async_client_context.require_sync
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

    @async_client_context.require_sync
    def test_iteration_with_list(self):
        self.db.drop_collection("test")
        self.db.test.insert_many([{"i": i} for i in range(100)])

        cur = self.db.test.find().batch_size(10)

        self.assertEqual(100, len(list(cur)))  # type: ignore[call-overload]

    def test_len(self):
        with self.assertRaises(TypeError):
            len(self.db.test.find())  # type: ignore[arg-type]

    def test_properties(self):
        self.assertEqual(self.db.test, self.db.test.find().collection)

        with self.assertRaises(AttributeError):
            self.db.test.find().collection = "hello"  # type: ignore

    async def test_get_more(self):
        db = self.db
        await db.drop_collection("test")
        await db.test.insert_many([{"i": i} for i in range(10)])
        self.assertEqual(10, len(await db.test.find().batch_size(5).to_list()))

    async def test_tailable(self):
        db = self.db
        await db.drop_collection("test")
        await db.create_collection("test", capped=True, size=1000, max=3)
        self.addAsyncCleanup(db.drop_collection, "test")
        cursor = db.test.find(cursor_type=CursorType.TAILABLE)

        await db.test.insert_one({"x": 1})
        count = 0
        async for doc in cursor:
            count += 1
            self.assertEqual(1, doc["x"])
        self.assertEqual(1, count)

        await db.test.insert_one({"x": 2})
        count = 0
        async for doc in cursor:
            count += 1
            self.assertEqual(2, doc["x"])
        self.assertEqual(1, count)

        await db.test.insert_one({"x": 3})
        count = 0
        async for doc in cursor:
            count += 1
            self.assertEqual(3, doc["x"])
        self.assertEqual(1, count)

        # Capped rollover - the collection can never
        # have more than 3 documents. Just make sure
        # this doesn't raise...
        await db.test.insert_many([{"x": i} for i in range(4, 7)])
        self.assertEqual(0, len(await cursor.to_list()))

        # and that the cursor doesn't think it's still alive.
        self.assertFalse(cursor.alive)

        self.assertEqual(3, await db.test.count_documents({}))

        # __getitem__(index)
        if _IS_SYNC:
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

    # The Async API does not support threading
    @async_client_context.require_sync
    def test_concurrent_close(self):
        """Ensure a tailable can be closed from another thread."""
        db = self.db
        db.drop_collection("test")
        db.create_collection("test", capped=True, size=1000, max=3)
        self.addCleanup(db.drop_collection, "test")
        cursor = db.test.find(cursor_type=CursorType.TAILABLE)

        def iterate_cursor():
            while cursor.alive:
                try:
                    for _doc in cursor:
                        pass
                except OperationFailure as e:
                    if e.code != 237:  # CursorKilled error code
                        raise

        t = threading.Thread(target=iterate_cursor)
        t.start()
        time.sleep(1)
        cursor.close()
        self.assertFalse(cursor.alive)
        t.join(3)
        self.assertFalse(t.is_alive())

    async def test_distinct(self):
        await self.db.drop_collection("test")

        await self.db.test.insert_many([{"a": 1}, {"a": 2}, {"a": 2}, {"a": 2}, {"a": 3}])

        distinct = await self.db.test.find({"a": {"$lt": 3}}).distinct("a")
        distinct.sort()

        self.assertEqual([1, 2], distinct)

        await self.db.drop_collection("test")

        await self.db.test.insert_one({"a": {"b": "a"}, "c": 12})
        await self.db.test.insert_one({"a": {"b": "b"}, "c": 8})
        await self.db.test.insert_one({"a": {"b": "c"}, "c": 12})
        await self.db.test.insert_one({"a": {"b": "c"}, "c": 8})

        distinct = await self.db.test.find({"c": 8}).distinct("a.b")
        distinct.sort()

        self.assertEqual(["b", "c"], distinct)

    async def test_with_statement(self):
        await self.db.drop_collection("test")
        await self.db.test.insert_many([{} for _ in range(100)])

        c1 = self.db.test.find()
        async with self.db.test.find() as c2:
            self.assertTrue(c2.alive)
        self.assertFalse(c2.alive)

        async with self.db.test.find() as c2:
            self.assertEqual(100, len(await c2.to_list()))
        self.assertFalse(c2.alive)
        self.assertTrue(c1.alive)

    @async_client_context.require_no_mongos
    async def test_comment(self):
        await self.client.drop_database(self.db)
        await self.db.command("profile", 2)  # Profile ALL commands.
        try:
            await self.db.test.find().comment("foo").to_list()
            count = await self.db.system.profile.count_documents(
                {"ns": "pymongo_test.test", "op": "query", "command.comment": "foo"}
            )
            self.assertEqual(count, 1)

            await self.db.test.find().comment("foo").distinct("type")
            count = await self.db.system.profile.count_documents(
                {
                    "ns": "pymongo_test.test",
                    "op": "command",
                    "command.distinct": "test",
                    "command.comment": "foo",
                }
            )
            self.assertEqual(count, 1)
        finally:
            await self.db.command("profile", 0)  # Turn off profiling.
            await self.db.system.profile.drop()

        await self.db.test.insert_many([{}, {}])
        cursor = self.db.test.find()
        await anext(cursor)
        self.assertRaises(InvalidOperation, cursor.comment, "hello")

    async def test_alive(self):
        await self.db.test.delete_many({})
        await self.db.test.insert_many([{} for _ in range(3)])
        self.addAsyncCleanup(self.db.test.delete_many, {})
        cursor = self.db.test.find().batch_size(2)
        n = 0
        while True:
            await cursor.next()
            n += 1
            if n == 3:
                self.assertFalse(cursor.alive)
                break

            self.assertTrue(cursor.alive)

    async def test_close_kills_cursor_synchronously(self):
        # Kill any cursors possibly queued up by previous tests.
        gc.collect()
        await self.client._process_periodic_tasks()

        listener = AllowListEventListener("killCursors")
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        coll = client[self.db.name].test_close_kills_cursors

        # Add some test data.
        docs_inserted = 1000
        await coll.insert_many([{"i": i} for i in range(docs_inserted)])

        listener.reset()

        # Close a cursor while it's still open on the server.
        cursor = coll.find().batch_size(10)
        self.assertTrue(bool(await anext(cursor)))
        self.assertLess(cursor.retrieved, docs_inserted)
        await cursor.close()

        def assertCursorKilled():
            self.assertEqual(1, len(listener.started_events))
            self.assertEqual("killCursors", listener.started_events[0].command_name)
            self.assertEqual(1, len(listener.succeeded_events))
            self.assertEqual("killCursors", listener.succeeded_events[0].command_name)

        assertCursorKilled()
        listener.reset()

        # Close a command cursor while it's still open on the server.
        cursor = await coll.aggregate([], batchSize=10)
        self.assertTrue(bool(await anext(cursor)))
        await cursor.close()

        # The cursor should be killed if it had a non-zero id.
        if cursor.cursor_id:
            assertCursorKilled()
        else:
            self.assertEqual(0, len(listener.started_events))

    @async_client_context.require_failCommand_appName
    async def test_timeout_kills_cursor_asynchronously(self):
        listener = AllowListEventListener("killCursors")
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        coll = client[self.db.name].test_timeout_kills_cursor

        # Add some test data.
        docs_inserted = 10
        await coll.insert_many([{"i": i} for i in range(docs_inserted)])

        listener.reset()

        cursor = coll.find({}, batch_size=1)
        await cursor.next()

        # Mock getMore commands timing out.
        mock_timeout_errors = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {
                "errorCode": 50,
                "failCommands": ["getMore"],
            },
        }

        async with self.fail_point(mock_timeout_errors):
            with self.assertRaises(ExecutionTimeout):
                await cursor.next()

        async def assertCursorKilled():
            await async_wait_until(
                lambda: len(listener.succeeded_events),
                "find successful killCursors command",
            )

            self.assertEqual(1, len(listener.started_events))
            self.assertEqual("killCursors", listener.started_events[0].command_name)
            self.assertEqual(1, len(listener.succeeded_events))
            self.assertEqual("killCursors", listener.succeeded_events[0].command_name)

        await assertCursorKilled()
        listener.reset()

        cursor = await coll.aggregate([], batchSize=1)
        await cursor.next()

        async with self.fail_point(mock_timeout_errors):
            with self.assertRaises(ExecutionTimeout):
                await cursor.next()

        await assertCursorKilled()

    def test_delete_not_initialized(self):
        # Creating a cursor with invalid arguments will not run __init__
        # but will still call __del__, eg test.find(invalidKwarg=1).
        cursor = AsyncCursor.__new__(AsyncCursor)  # Skip calling __init__
        cursor.__del__()  # no error

    async def test_getMore_does_not_send_readPreference(self):
        listener = AllowListEventListener("find", "getMore")
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        # We never send primary read preference so override the default.
        coll = client[self.db.name].get_collection(
            "test", read_preference=ReadPreference.PRIMARY_PREFERRED
        )

        await coll.delete_many({})
        await coll.insert_many([{} for _ in range(5)])
        self.addAsyncCleanup(coll.drop)

        await coll.find(batch_size=3).to_list()
        started = listener.started_events
        self.assertEqual(2, len(started))
        self.assertEqual("find", started[0].command_name)
        if async_client_context.is_rs or async_client_context.is_mongos:
            self.assertIn("$readPreference", started[0].command)
        else:
            self.assertNotIn("$readPreference", started[0].command)
        self.assertEqual("getMore", started[1].command_name)
        self.assertNotIn("$readPreference", started[1].command)

    @async_client_context.require_replica_set
    async def test_to_list_tailable(self):
        oplog = self.client.local.oplog.rs
        last = await oplog.find().sort("$natural", pymongo.DESCENDING).limit(-1).next()
        ts = last["ts"]
        # Set maxAwaitTimeMS=1 to speed up the test and avoid blocking on the noop writer.
        c = oplog.find(
            {"ts": {"$gte": ts}}, cursor_type=pymongo.CursorType.TAILABLE_AWAIT, oplog_replay=True
        ).max_await_time_ms(1)
        self.addAsyncCleanup(c.close)
        # Wait for the change to be read.
        docs = []
        while not docs:
            docs = await c.to_list()
        self.assertGreaterEqual(len(docs), 1)

    async def test_to_list_empty(self):
        c = self.db.does_not_exist.find()
        docs = await c.to_list()
        self.assertEqual([], docs)

    async def test_to_list_length(self):
        coll = self.db.test
        await coll.insert_many([{} for _ in range(5)])
        self.addAsyncCleanup(coll.drop)
        c = coll.find()
        docs = await c.to_list(3)
        self.assertEqual(len(docs), 3)

        c = coll.find(batch_size=2)
        docs = await c.to_list(3)
        self.assertEqual(len(docs), 3)
        docs = await c.to_list(3)
        self.assertEqual(len(docs), 2)

    @flaky(reason="PYTHON-3522")
    async def test_to_list_csot_applied(self):
        client = await self.async_single_client(timeoutMS=500, w=1)
        coll = client.pymongo.test
        # Initialize the client with a larger timeout to help make test less flaky
        with pymongo.timeout(10):
            await coll.insert_many([{} for _ in range(5)])
        cursor = coll.find({"$where": delay(1)})
        with self.assertRaises(PyMongoError) as ctx:
            await cursor.to_list()
        self.assertTrue(ctx.exception.timeout)

    @async_client_context.require_change_streams
    async def test_command_cursor_to_list(self):
        # Set maxAwaitTimeMS=1 to speed up the test.
        c = await self.db.test.aggregate([{"$changeStream": {}}], maxAwaitTimeMS=1)
        self.addAsyncCleanup(c.close)
        docs = await c.to_list()
        self.assertGreaterEqual(len(docs), 0)

    @async_client_context.require_change_streams
    async def test_command_cursor_to_list_empty(self):
        # Set maxAwaitTimeMS=1 to speed up the test.
        c = await self.db.does_not_exist.aggregate([{"$changeStream": {}}], maxAwaitTimeMS=1)
        self.addAsyncCleanup(c.close)
        docs = await c.to_list()
        self.assertEqual([], docs)

    @async_client_context.require_change_streams
    async def test_command_cursor_to_list_length(self):
        db = self.db
        await db.drop_collection("test")
        await db.test.insert_many([{"foo": 1}, {"foo": 2}])

        pipeline = {"$project": {"_id": False, "foo": True}}
        result = await db.test.aggregate([pipeline])
        self.assertEqual(len(await result.to_list()), 2)

        result = await db.test.aggregate([pipeline])
        self.assertEqual(len(await result.to_list(1)), 1)

    @async_client_context.require_failCommand_blockConnection
    @flaky(reason="PYTHON-3522")
    async def test_command_cursor_to_list_csot_applied(self):
        client = await self.async_single_client(timeoutMS=500, w=1)
        coll = client.pymongo.test
        # Initialize the client with a larger timeout to help make test less flaky
        with pymongo.timeout(10):
            await coll.insert_many([{} for _ in range(5)])
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 5},
            "data": {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 1000},
        }
        cursor = await coll.aggregate([], batchSize=1)
        async with self.fail_point(fail_command):
            with self.assertRaises(PyMongoError) as ctx:
                await cursor.to_list()
            self.assertTrue(ctx.exception.timeout)


class TestRawBatchCursor(AsyncIntegrationTest):
    async def test_find_raw(self):
        c = self.db.test
        await c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        await c.insert_many(docs)
        batches = await c.find_raw_batches().sort("_id").to_list()
        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

    @async_client_context.require_transactions
    async def test_find_raw_transaction(self):
        c = self.db.test
        await c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        await c.insert_many(docs)

        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        async with client.start_session() as session:
            async with await session.start_transaction():
                batches = await (
                    client[self.db.name].test.find_raw_batches(session=session).sort("_id")
                ).to_list()
                cmd = listener.started_events[0]
                self.assertEqual(cmd.command_name, "find")
                self.assertIn("$clusterTime", cmd.command)
                self.assertEqual(cmd.command["startTransaction"], True)
                self.assertEqual(cmd.command["txnNumber"], 1)
                # Ensure we update $clusterTime from the command response.
                last_cmd = listener.succeeded_events[-1]
                self.assertEqual(
                    last_cmd.reply["$clusterTime"]["clusterTime"],
                    session.cluster_time["clusterTime"],
                )

        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

    @async_client_context.require_sessions
    @async_client_context.require_failCommand_fail_point
    async def test_find_raw_retryable_reads(self):
        c = self.db.test
        await c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        await c.insert_many(docs)

        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener], retryReads=True)
        async with self.fail_point(
            {"mode": {"times": 1}, "data": {"failCommands": ["find"], "closeConnection": True}}
        ):
            batches = await client[self.db.name].test.find_raw_batches().sort("_id").to_list()

        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))
        self.assertEqual(len(listener.started_events), 2)
        for cmd in listener.started_events:
            self.assertEqual(cmd.command_name, "find")

    @async_client_context.require_version_min(5, 0, 0)
    @async_client_context.require_no_standalone
    async def test_find_raw_snapshot_reads(self):
        c = self.db.get_collection("test", write_concern=WriteConcern(w="majority"))
        await c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        await c.insert_many(docs)

        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener], retryReads=True)
        db = client[self.db.name]
        async with client.start_session(snapshot=True) as session:
            await db.test.distinct("x", {}, session=session)
            batches = await db.test.find_raw_batches(session=session).sort("_id").to_list()
        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

        find_cmd = listener.started_events[1].command
        self.assertEqual(find_cmd["readConcern"]["level"], "snapshot")
        self.assertIsNotNone(find_cmd["readConcern"]["atClusterTime"])

    async def test_explain(self):
        c = self.db.test
        await c.insert_one({})
        explanation = await c.find_raw_batches().explain()
        self.assertIsInstance(explanation, dict)

    async def test_empty(self):
        await self.db.test.drop()
        cursor = self.db.test.find_raw_batches()
        with self.assertRaises(StopAsyncIteration):
            await anext(cursor)

    async def test_clone(self):
        await self.db.test.insert_one({})
        cursor = self.db.test.find_raw_batches()
        # Copy of a RawBatchCursor is also a RawBatchCursor, not a Cursor.
        self.assertIsInstance(await anext(cursor.clone()), bytes)
        self.assertIsInstance(await anext(copy.copy(cursor)), bytes)

    @async_client_context.require_no_mongos
    async def test_exhaust(self):
        c = self.db.test
        await c.drop()
        await c.insert_many({"_id": i} for i in range(200))
        result = b"".join(await c.find_raw_batches(cursor_type=CursorType.EXHAUST).to_list())
        self.assertEqual([{"_id": i} for i in range(200)], decode_all(result))

    async def test_server_error(self):
        with self.assertRaises(OperationFailure) as exc:
            await anext(self.db.test.find_raw_batches({"x": {"$bad": 1}}))

        # The server response was decoded, not left raw.
        self.assertIsInstance(exc.exception.details, dict)

    async def test_get_item(self):
        with self.assertRaises(InvalidOperation):
            self.db.test.find_raw_batches()[0]

    async def test_collation(self):
        await anext(self.db.test.find_raw_batches(collation=Collation("en_US")))

    async def test_read_concern(self):
        await self.db.get_collection("test", write_concern=WriteConcern(w="majority")).insert_one(
            {}
        )
        c = self.db.get_collection("test", read_concern=ReadConcern("majority"))
        await anext(c.find_raw_batches())

    async def test_monitoring(self):
        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        c = client.pymongo_test.test
        await c.drop()
        await c.insert_many([{"_id": i} for i in range(10)])

        listener.reset()
        cursor = c.find_raw_batches(batch_size=4)

        # First raw batch of 4 documents.
        await anext(cursor)

        started = listener.started_events[0]
        succeeded = listener.succeeded_events[0]
        self.assertEqual(0, len(listener.failed_events))
        self.assertEqual("find", started.command_name)
        self.assertEqual("pymongo_test", started.database_name)
        self.assertEqual("find", succeeded.command_name)
        csr = succeeded.reply["cursor"]
        self.assertEqual(csr["ns"], "pymongo_test.test")

        # The batch is a list of one raw bytes object.
        self.assertEqual(len(csr["firstBatch"]), 1)
        self.assertEqual(decode_all(csr["firstBatch"][0]), [{"_id": i} for i in range(4)])

        listener.reset()

        # Next raw batch of 4 documents.
        await anext(cursor)
        try:
            started = listener.started_events[0]
            succeeded = listener.succeeded_events[0]
            self.assertEqual(0, len(listener.failed_events))
            self.assertEqual("getMore", started.command_name)
            self.assertEqual("pymongo_test", started.database_name)
            self.assertEqual("getMore", succeeded.command_name)
            csr = succeeded.reply["cursor"]
            self.assertEqual(csr["ns"], "pymongo_test.test")
            self.assertEqual(len(csr["nextBatch"]), 1)
            self.assertEqual(decode_all(csr["nextBatch"][0]), [{"_id": i} for i in range(4, 8)])
        finally:
            # Finish the cursor.
            await cursor.close()


class TestRawBatchCommandCursor(AsyncIntegrationTest):
    async def test_aggregate_raw(self):
        c = self.db.test
        await c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        await c.insert_many(docs)
        batches = await (await c.aggregate_raw_batches([{"$sort": {"_id": 1}}])).to_list()
        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

    @async_client_context.require_transactions
    async def test_aggregate_raw_transaction(self):
        c = self.db.test
        await c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        await c.insert_many(docs)

        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        async with client.start_session() as session:
            async with await session.start_transaction():
                batches = await (
                    await client[self.db.name].test.aggregate_raw_batches(
                        [{"$sort": {"_id": 1}}], session=session
                    )
                ).to_list()
                cmd = listener.started_events[0]
                self.assertEqual(cmd.command_name, "aggregate")
                self.assertIn("$clusterTime", cmd.command)
                self.assertEqual(cmd.command["startTransaction"], True)
                self.assertEqual(cmd.command["txnNumber"], 1)
                # Ensure we update $clusterTime from the command response.
                last_cmd = listener.succeeded_events[-1]
                self.assertEqual(
                    last_cmd.reply["$clusterTime"]["clusterTime"],
                    session.cluster_time["clusterTime"],
                )
        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

    @async_client_context.require_sessions
    @async_client_context.require_failCommand_fail_point
    async def test_aggregate_raw_retryable_reads(self):
        c = self.db.test
        await c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        await c.insert_many(docs)

        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener], retryReads=True)
        async with self.fail_point(
            {"mode": {"times": 1}, "data": {"failCommands": ["aggregate"], "closeConnection": True}}
        ):
            batches = await (
                await client[self.db.name].test.aggregate_raw_batches([{"$sort": {"_id": 1}}])
            ).to_list()

        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))
        self.assertEqual(len(listener.started_events), 3)
        cmds = listener.started_events
        self.assertEqual(cmds[0].command_name, "aggregate")
        self.assertEqual(cmds[1].command_name, "aggregate")

    @async_client_context.require_version_min(5, 0, -1)
    @async_client_context.require_no_standalone
    async def test_aggregate_raw_snapshot_reads(self):
        c = self.db.get_collection("test", write_concern=WriteConcern(w="majority"))
        await c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        await c.insert_many(docs)

        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener], retryReads=True)
        db = client[self.db.name]
        async with client.start_session(snapshot=True) as session:
            await db.test.distinct("x", {}, session=session)
            batches = await (
                await db.test.aggregate_raw_batches([{"$sort": {"_id": 1}}], session=session)
            ).to_list()
        self.assertEqual(1, len(batches))
        self.assertEqual(docs, decode_all(batches[0]))

        find_cmd = listener.started_events[1].command
        self.assertEqual(find_cmd["readConcern"]["level"], "snapshot")
        self.assertIsNotNone(find_cmd["readConcern"]["atClusterTime"])

    async def test_server_error(self):
        c = self.db.test
        await c.drop()
        docs = [{"_id": i, "x": 3.0 * i} for i in range(10)]
        await c.insert_many(docs)
        await c.insert_one({"_id": 10, "x": "not a number"})

        with self.assertRaises(OperationFailure) as exc:
            await (
                await self.db.test.aggregate_raw_batches(
                    [
                        {
                            "$sort": {"_id": 1},
                        },
                        {"$project": {"x": {"$multiply": [2, "$x"]}}},
                    ],
                    batchSize=4,
                )
            ).to_list()

        # The server response was decoded, not left raw.
        self.assertIsInstance(exc.exception.details, dict)

    async def test_get_item(self):
        with self.assertRaises(InvalidOperation):
            (await self.db.test.aggregate_raw_batches([]))[0]

    async def test_collation(self):
        await anext(await self.db.test.aggregate_raw_batches([], collation=Collation("en_US")))

    async def test_monitoring(self):
        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        c = client.pymongo_test.test
        await c.drop()
        await c.insert_many([{"_id": i} for i in range(10)])

        listener.reset()
        cursor = await c.aggregate_raw_batches([{"$sort": {"_id": 1}}], batchSize=4)

        # Start cursor, no initial batch.
        started = listener.started_events[0]
        succeeded = listener.succeeded_events[0]
        self.assertEqual(0, len(listener.failed_events))
        self.assertEqual("aggregate", started.command_name)
        self.assertEqual("pymongo_test", started.database_name)
        self.assertEqual("aggregate", succeeded.command_name)
        csr = succeeded.reply["cursor"]
        self.assertEqual(csr["ns"], "pymongo_test.test")

        # First batch is empty.
        self.assertEqual(len(csr["firstBatch"]), 0)
        listener.reset()

        # Batches of 4 documents.
        n = 0
        async for batch in cursor:
            started = listener.started_events[0]
            succeeded = listener.succeeded_events[0]
            self.assertEqual(0, len(listener.failed_events))
            self.assertEqual("getMore", started.command_name)
            self.assertEqual("pymongo_test", started.database_name)
            self.assertEqual("getMore", succeeded.command_name)
            csr = succeeded.reply["cursor"]
            self.assertEqual(csr["ns"], "pymongo_test.test")
            self.assertEqual(len(csr["nextBatch"]), 1)
            self.assertEqual(csr["nextBatch"][0], batch)
            self.assertEqual(decode_all(batch), [{"_id": i} for i in range(n, min(n + 4, 10))])

            n += 4
            listener.reset()

    @async_client_context.require_version_min(5, 0, -1)
    @async_client_context.require_no_mongos
    @async_client_context.require_sync
    async def test_exhaust_cursor_db_set(self):
        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        c = client.pymongo_test.test
        await c.delete_many({})
        await c.insert_many([{"_id": i} for i in range(3)])

        listener.reset()

        result = list(await c.find({}, cursor_type=pymongo.CursorType.EXHAUST, batch_size=1))

        self.assertEqual(len(result), 3)

        self.assertEqual(
            listener.started_command_names(), ["find", "getMore", "getMore", "getMore"]
        )
        for cmd in listener.started_events:
            self.assertEqual(cmd.command["$db"], "pymongo_test")


if __name__ == "__main__":
    unittest.main()
