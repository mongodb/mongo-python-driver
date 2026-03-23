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

"""Test the collection module."""
from __future__ import annotations

import asyncio
import contextlib
import re
import sys
from codecs import utf_8_decode
from collections import defaultdict
from test.asynchronous.utils import async_get_pool, async_is_mongos
from typing import Any, Iterable, no_type_check

from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.helpers import anext

sys.path[0:0] = [""]

from test import unittest
from test.asynchronous import (  # TODO: fix sync imports in PYTHON-4528
    AsyncIntegrationTest,
    AsyncUnitTest,
    async_client_context,
)
from test.utils_shared import (
    IMPOSSIBLE_WRITE_CONCERN,
    EventListener,
    OvertCommandListener,
    async_wait_until,
)
from test.version import Version

from bson import encode
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from bson.raw_bson import RawBSONDocument
from bson.regex import Regex
from bson.son import SON
from pymongo import ASCENDING, DESCENDING, GEO2D, GEOSPHERE, HASHED, TEXT
from pymongo.asynchronous.collection import AsyncCollection, ReturnDocument
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.bulk_shared import BulkWriteError
from pymongo.cursor_shared import CursorType
from pymongo.errors import (
    ConfigurationError,
    DocumentTooLarge,
    DuplicateKeyError,
    ExecutionTimeout,
    InvalidDocument,
    InvalidName,
    InvalidOperation,
    OperationFailure,
    WriteConcernError,
)
from pymongo.message import _COMMAND_OVERHEAD, _gen_find_command
from pymongo.operations import *
from pymongo.read_concern import DEFAULT_READ_CONCERN
from pymongo.read_preferences import ReadPreference
from pymongo.results import (
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from pymongo.write_concern import WriteConcern

_IS_SYNC = False


class TestCollectionNoConnect(AsyncUnitTest):
    """Test Collection features on a client that does not connect."""

    db: AsyncDatabase
    client: AsyncMongoClient

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.client = self.simple_client(connect=False)
        self.db = self.client.pymongo_test

    def test_collection(self):
        self.assertRaises(TypeError, AsyncCollection, self.db, 5)

        def make_col(base, name):
            return base[name]

        self.assertRaises(InvalidName, make_col, self.db, "")
        self.assertRaises(InvalidName, make_col, self.db, "te$t")
        self.assertRaises(InvalidName, make_col, self.db, ".test")
        self.assertRaises(InvalidName, make_col, self.db, "test.")
        self.assertRaises(InvalidName, make_col, self.db, "tes..t")
        self.assertRaises(InvalidName, make_col, self.db.test, "")
        self.assertRaises(InvalidName, make_col, self.db.test, "te$t")
        self.assertRaises(InvalidName, make_col, self.db.test, ".test")
        self.assertRaises(InvalidName, make_col, self.db.test, "test.")
        self.assertRaises(InvalidName, make_col, self.db.test, "tes..t")
        self.assertRaises(InvalidName, make_col, self.db.test, "tes\x00t")

    def test_getattr(self):
        coll = self.db.test
        self.assertIsInstance(coll["_does_not_exist"], AsyncCollection)

        with self.assertRaises(AttributeError) as context:
            coll._does_not_exist

        # Message should be:
        # "AttributeError: Collection has no attribute '_does_not_exist'. To
        # access the test._does_not_exist collection, use
        # database['test._does_not_exist']."
        self.assertIn("has no attribute '_does_not_exist'", str(context.exception))

        coll2 = coll.with_options(write_concern=WriteConcern(w=0))
        self.assertEqual(coll2.write_concern, WriteConcern(w=0))
        self.assertNotEqual(coll.write_concern, coll2.write_concern)
        coll3 = coll2.subcoll
        self.assertEqual(coll2.write_concern, coll3.write_concern)
        coll4 = coll2["subcoll"]
        self.assertEqual(coll2.write_concern, coll4.write_concern)

    def test_iteration(self):
        coll = self.db.coll
        msg = "'AsyncCollection' object is not iterable"
        # Iteration fails
        with self.assertRaisesRegex(TypeError, msg):
            for _ in coll:  # type: ignore[misc] # error: "None" not callable  [misc]
                break
        # Non-string indices will start failing in PyMongo 5.
        self.assertEqual(coll[0].name, "coll.0")
        self.assertEqual(coll[{}].name, "coll.{}")
        # next fails
        with self.assertRaisesRegex(TypeError, msg):
            _ = next(coll)
        # .next() fails
        with self.assertRaisesRegex(TypeError, msg):
            _ = coll.next()
        # Do not implement typing.Iterable.
        self.assertNotIsInstance(coll, Iterable)


class AsyncTestCollection(AsyncIntegrationTest):
    w: int

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.w = async_client_context.w  # type: ignore

    async def asyncTearDown(self):
        await self.db.test.drop()
        await self.db.drop_collection("test_large_limit")
        await super().asyncTearDown()

    @contextlib.contextmanager
    def write_concern_collection(self):
        if async_client_context.is_rs:
            with self.assertRaises(WriteConcernError):
                # Unsatisfiable write concern.
                yield AsyncCollection(
                    self.db,
                    "test",
                    write_concern=WriteConcern(w=len(async_client_context.nodes) + 1),
                )
        else:
            yield self.db.test

    async def test_equality(self):
        self.assertIsInstance(self.db.test, AsyncCollection)
        self.assertEqual(self.db.test, self.db["test"])
        self.assertEqual(self.db.test, AsyncCollection(self.db, "test"))
        self.assertEqual(self.db.test.mike, self.db["test.mike"])
        self.assertEqual(self.db.test["mike"], self.db["test.mike"])

    async def test_hashable(self):
        self.assertIn(self.db.test.mike, {self.db["test.mike"]})

    async def test_create(self):
        # No Exception.
        db = async_client_context.client.pymongo_test
        await db.create_test_no_wc.drop()

        async def lambda_test():
            return "create_test_no_wc" not in await db.list_collection_names()

        async def lambda_test_2():
            return "create_test_no_wc" in await db.list_collection_names()

        await async_wait_until(
            lambda_test,
            "drop create_test_no_wc collection",
        )
        await db.create_collection("create_test_no_wc")
        await async_wait_until(
            lambda_test_2,
            "create create_test_no_wc collection",
        )
        # SERVER-33317
        if not async_client_context.is_mongos or not async_client_context.version.at_least(3, 7, 0):
            with self.assertRaises(OperationFailure):
                await db.create_collection("create-test-wc", write_concern=IMPOSSIBLE_WRITE_CONCERN)

    async def test_drop_nonexistent_collection(self):
        await self.db.drop_collection("test")
        self.assertNotIn("test", await self.db.list_collection_names())

        # No exception
        await self.db.drop_collection("test")

    async def test_create_indexes(self):
        db = self.db

        with self.assertRaises(TypeError):
            await db.test.create_indexes("foo")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            await db.test.create_indexes(["foo"])  # type: ignore[list-item]
        self.assertRaises(TypeError, IndexModel, 5)
        self.assertRaises(ValueError, IndexModel, [])

        await db.test.drop_indexes()
        await db.test.insert_one({})
        self.assertEqual(len(await db.test.index_information()), 1)

        await db.test.create_indexes([IndexModel("hello")])
        await db.test.create_indexes([IndexModel([("hello", DESCENDING), ("world", ASCENDING)])])

        # Tuple instead of list.
        await db.test.create_indexes([IndexModel((("world", ASCENDING),))])

        self.assertEqual(len(await db.test.index_information()), 4)

        await db.test.drop_indexes()
        names = await db.test.create_indexes(
            [IndexModel([("hello", DESCENDING), ("world", ASCENDING)], name="hello_world")]
        )
        self.assertEqual(names, ["hello_world"])

        await db.test.drop_indexes()
        self.assertEqual(len(await db.test.index_information()), 1)
        await db.test.create_indexes([IndexModel("hello")])
        self.assertIn("hello_1", await db.test.index_information())

        await db.test.drop_indexes()
        self.assertEqual(len(await db.test.index_information()), 1)
        names = await db.test.create_indexes(
            [IndexModel([("hello", DESCENDING), ("world", ASCENDING)]), IndexModel("hello")]
        )
        info = await db.test.index_information()
        for name in names:
            self.assertIn(name, info)

        await db.test.drop()
        await db.test.insert_one({"a": 1})
        await db.test.insert_one({"a": 1})
        with self.assertRaises(DuplicateKeyError):
            await db.test.create_indexes([IndexModel("a", unique=True)])

        with self.write_concern_collection() as coll:
            await coll.create_indexes([IndexModel("hello")])

    @async_client_context.require_version_max(4, 3, -1)
    async def test_create_indexes_commitQuorum_requires_44(self):
        db = self.db
        with self.assertRaisesRegex(
            ConfigurationError,
            r"Must be connected to MongoDB 4\.4\+ to use the commitQuorum option for createIndexes",
        ):
            await db.coll.create_indexes([IndexModel("a")], commitQuorum="majority")

    @async_client_context.require_no_standalone
    @async_client_context.require_version_min(4, 4, -1)
    async def test_create_indexes_commitQuorum(self):
        await self.db.coll.create_indexes([IndexModel("a")], commitQuorum="majority")

    async def test_create_index(self):
        db = self.db

        with self.assertRaises(TypeError):
            await db.test.create_index(5)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            await db.test.create_index([])

        await db.test.drop_indexes()
        await db.test.insert_one({})
        self.assertEqual(len(await db.test.index_information()), 1)

        await db.test.create_index("hello")
        await db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])

        # Tuple instead of list.
        await db.test.create_index((("world", ASCENDING),))

        self.assertEqual(len(await db.test.index_information()), 4)

        await db.test.drop_indexes()
        ix = await db.test.create_index(
            [("hello", DESCENDING), ("world", ASCENDING)], name="hello_world"
        )
        self.assertEqual(ix, "hello_world")

        await db.test.drop_indexes()
        self.assertEqual(len(await db.test.index_information()), 1)
        await db.test.create_index("hello")
        self.assertIn("hello_1", await db.test.index_information())

        await db.test.drop_indexes()
        self.assertEqual(len(await db.test.index_information()), 1)
        await db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])
        self.assertIn("hello_-1_world_1", await db.test.index_information())

        await db.test.drop_indexes()
        await db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)], name=None)
        self.assertIn("hello_-1_world_1", await db.test.index_information())

        await db.test.drop()
        await db.test.insert_one({"a": 1})
        await db.test.insert_one({"a": 1})
        with self.assertRaises(DuplicateKeyError):
            await db.test.create_index("a", unique=True)

        with self.write_concern_collection() as coll:
            await coll.create_index([("hello", DESCENDING)])

        await db.test.create_index(["hello", "world"])
        await db.test.create_index(["hello", ("world", DESCENDING)])
        await db.test.create_index({"hello": 1}.items())  # type:ignore[arg-type]

    async def test_drop_index(self):
        db = self.db
        await db.test.drop_indexes()
        await db.test.create_index("hello")
        name = await db.test.create_index("goodbye")

        self.assertEqual(len(await db.test.index_information()), 3)
        self.assertEqual(name, "goodbye_1")
        await db.test.drop_index(name)

        # Drop it again.
        if async_client_context.version < Version(8, 3, -1):
            with self.assertRaises(OperationFailure):
                await db.test.drop_index(name)
        else:
            await db.test.drop_index(name)
        self.assertEqual(len(await db.test.index_information()), 2)
        self.assertIn("hello_1", await db.test.index_information())

        await db.test.drop_indexes()
        await db.test.create_index("hello")
        name = await db.test.create_index("goodbye")

        self.assertEqual(len(await db.test.index_information()), 3)
        self.assertEqual(name, "goodbye_1")
        await db.test.drop_index([("goodbye", ASCENDING)])
        self.assertEqual(len(await db.test.index_information()), 2)
        self.assertIn("hello_1", await db.test.index_information())

        with self.write_concern_collection() as coll:
            await coll.drop_index("hello_1")

    @async_client_context.require_no_mongos
    @async_client_context.require_test_commands
    async def test_index_management_max_time_ms(self):
        coll = self.db.test
        await self.client.admin.command(
            "configureFailPoint", "maxTimeAlwaysTimeOut", mode="alwaysOn"
        )
        try:
            with self.assertRaises(ExecutionTimeout):
                await coll.create_index("foo", maxTimeMS=1)
            with self.assertRaises(ExecutionTimeout):
                await coll.create_indexes([IndexModel("foo")], maxTimeMS=1)
            with self.assertRaises(ExecutionTimeout):
                await coll.drop_index("foo", maxTimeMS=1)
            with self.assertRaises(ExecutionTimeout):
                await coll.drop_indexes(maxTimeMS=1)
        finally:
            await self.client.admin.command(
                "configureFailPoint", "maxTimeAlwaysTimeOut", mode="off"
            )

    async def test_list_indexes(self):
        db = self.db
        await db.test.drop()
        await db.test.insert_one({})  # create collection

        def map_indexes(indexes):
            return {index["name"]: index for index in indexes}

        indexes = await (await db.test.list_indexes()).to_list()
        self.assertEqual(len(indexes), 1)
        self.assertIn("_id_", map_indexes(indexes))

        await db.test.create_index("hello")
        indexes = await (await db.test.list_indexes()).to_list()
        self.assertEqual(len(indexes), 2)
        self.assertEqual(map_indexes(indexes)["hello_1"]["key"], SON([("hello", ASCENDING)]))

        await db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)], unique=True)
        indexes = await (await db.test.list_indexes()).to_list()
        self.assertEqual(len(indexes), 3)
        index_map = map_indexes(indexes)
        self.assertEqual(
            index_map["hello_-1_world_1"]["key"], SON([("hello", DESCENDING), ("world", ASCENDING)])
        )
        self.assertEqual(True, index_map["hello_-1_world_1"]["unique"])

        # List indexes on a collection that does not exist.
        indexes = await (await db.does_not_exist.list_indexes()).to_list()
        self.assertEqual(len(indexes), 0)

        # List indexes on a database that does not exist.
        indexes = await (await db.does_not_exist.list_indexes()).to_list()
        self.assertEqual(len(indexes), 0)

    async def test_index_info(self):
        db = self.db
        await db.test.drop()
        await db.test.insert_one({})  # create collection
        self.assertEqual(len(await db.test.index_information()), 1)
        self.assertIn("_id_", await db.test.index_information())

        await db.test.create_index("hello")
        self.assertEqual(len(await db.test.index_information()), 2)
        self.assertEqual(
            (await db.test.index_information())["hello_1"]["key"], [("hello", ASCENDING)]
        )

        await db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)], unique=True)
        self.assertEqual(
            (await db.test.index_information())["hello_1"]["key"], [("hello", ASCENDING)]
        )
        self.assertEqual(len(await db.test.index_information()), 3)
        self.assertEqual(
            [("hello", DESCENDING), ("world", ASCENDING)],
            (await db.test.index_information())["hello_-1_world_1"]["key"],
        )
        self.assertEqual(True, (await db.test.index_information())["hello_-1_world_1"]["unique"])

    async def test_index_geo2d(self):
        db = self.db
        await db.test.drop_indexes()
        self.assertEqual("loc_2d", await db.test.create_index([("loc", GEO2D)]))
        index_info = (await db.test.index_information())["loc_2d"]
        self.assertEqual([("loc", "2d")], index_info["key"])

    # geoSearch was deprecated in 4.4 and removed in 5.0
    @async_client_context.require_version_max(4, 5)
    @async_client_context.require_no_mongos
    async def test_index_haystack(self):
        db = self.db
        await db.test.drop()
        _id = (
            await db.test.insert_one({"pos": {"long": 34.2, "lat": 33.3}, "type": "restaurant"})
        ).inserted_id
        await db.test.insert_one({"pos": {"long": 34.2, "lat": 37.3}, "type": "restaurant"})
        await db.test.insert_one({"pos": {"long": 59.1, "lat": 87.2}, "type": "office"})
        await db.test.create_index([("pos", "geoHaystack"), ("type", ASCENDING)], bucketSize=1)

        results = (
            await db.command(
                SON(
                    [
                        ("geoSearch", "test"),
                        ("near", [33, 33]),
                        ("maxDistance", 6),
                        ("search", {"type": "restaurant"}),
                        ("limit", 30),
                    ]
                )
            )
        )["results"]

        self.assertEqual(2, len(results))
        self.assertEqual(
            {"_id": _id, "pos": {"long": 34.2, "lat": 33.3}, "type": "restaurant"}, results[0]
        )

    @async_client_context.require_no_mongos
    async def test_index_text(self):
        db = self.db
        await db.test.drop_indexes()
        self.assertEqual("t_text", await db.test.create_index([("t", TEXT)]))
        index_info = (await db.test.index_information())["t_text"]
        self.assertIn("weights", index_info)

        await db.test.insert_many(
            [{"t": "spam eggs and spam"}, {"t": "spam"}, {"t": "egg sausage and bacon"}]
        )

        # MongoDB 2.6 text search. Create 'score' field in projection.
        cursor = db.test.find({"$text": {"$search": "spam"}}, {"score": {"$meta": "textScore"}})

        # Sort by 'score' field.
        cursor.sort([("score", {"$meta": "textScore"})])
        results = await cursor.to_list()
        self.assertGreaterEqual(results[0]["score"], results[1]["score"])

        await db.test.drop_indexes()

    async def test_index_2dsphere(self):
        db = self.db
        await db.test.drop_indexes()
        self.assertEqual("geo_2dsphere", await db.test.create_index([("geo", GEOSPHERE)]))

        for dummy, info in (await db.test.index_information()).items():
            field, idx_type = info["key"][0]
            if field == "geo" and idx_type == "2dsphere":
                break
        else:
            self.fail("2dsphere index not found.")

        poly = {"type": "Polygon", "coordinates": [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]]}
        query = {"geo": {"$within": {"$geometry": poly}}}

        # This query will error without a 2dsphere index.
        db.test.find(query)
        await db.test.drop_indexes()

    async def test_index_hashed(self):
        db = self.db
        await db.test.drop_indexes()
        self.assertEqual("a_hashed", await db.test.create_index([("a", HASHED)]))

        for dummy, info in (await db.test.index_information()).items():
            field, idx_type = info["key"][0]
            if field == "a" and idx_type == "hashed":
                break
        else:
            self.fail("hashed index not found.")

        await db.test.drop_indexes()

    async def test_index_sparse(self):
        db = self.db
        await db.test.drop_indexes()
        await db.test.create_index([("key", ASCENDING)], sparse=True)
        self.assertTrue((await db.test.index_information())["key_1"]["sparse"])

    async def test_index_background(self):
        db = self.db
        await db.test.drop_indexes()
        await db.test.create_index([("keya", ASCENDING)])
        await db.test.create_index([("keyb", ASCENDING)], background=False)
        await db.test.create_index([("keyc", ASCENDING)], background=True)
        self.assertNotIn("background", (await db.test.index_information())["keya_1"])
        self.assertFalse((await db.test.index_information())["keyb_1"]["background"])
        self.assertTrue((await db.test.index_information())["keyc_1"]["background"])

    async def _drop_dups_setup(self, db):
        await db.drop_collection("test")
        await db.test.insert_one({"i": 1})
        await db.test.insert_one({"i": 2})
        await db.test.insert_one({"i": 2})  # duplicate
        await db.test.insert_one({"i": 3})

    async def test_index_dont_drop_dups(self):
        # Try *not* dropping duplicates
        db = self.db
        await self._drop_dups_setup(db)

        # There's a duplicate
        async def _test_create():
            await db.test.create_index([("i", ASCENDING)], unique=True, dropDups=False)

        with self.assertRaises(DuplicateKeyError):
            await _test_create()

        # Duplicate wasn't dropped
        self.assertEqual(4, await db.test.count_documents({}))

        # Index wasn't created, only the default index on _id
        self.assertEqual(1, len(await db.test.index_information()))

    # Get the plan dynamically because the explain format will change.
    def get_plan_stage(self, root, stage):
        if root.get("stage") == stage:
            return root
        elif "inputStage" in root:
            return self.get_plan_stage(root["inputStage"], stage)
        elif "inputStages" in root:
            for i in root["inputStages"]:
                stage = self.get_plan_stage(i, stage)
                if stage:
                    return stage
        elif "queryPlan" in root:
            # queryPlan (and slotBasedPlan) are new in 5.0.
            return self.get_plan_stage(root["queryPlan"], stage)
        elif "shards" in root:
            for i in root["shards"]:
                stage = self.get_plan_stage(i["winningPlan"], stage)
                if stage:
                    return stage
        return {}

    async def test_index_filter(self):
        db = self.db
        await db.drop_collection("test")

        # Test bad filter spec on create.
        with self.assertRaises(OperationFailure):
            await db.test.create_index("x", partialFilterExpression=5)
        with self.assertRaises(OperationFailure):
            await db.test.create_index("x", partialFilterExpression={"x": {"$asdasd": 3}})
        with self.assertRaises(OperationFailure):
            await db.test.create_index("x", partialFilterExpression={"$and": 5})

        self.assertEqual(
            "x_1",
            await db.test.create_index(
                [("x", ASCENDING)], partialFilterExpression={"a": {"$lte": 1.5}}
            ),
        )
        await db.test.insert_one({"x": 5, "a": 2})
        await db.test.insert_one({"x": 6, "a": 1})

        # Operations that use the partial index.
        explain = await db.test.find({"x": 6, "a": 1}).explain()
        stage = self.get_plan_stage(explain["queryPlanner"]["winningPlan"], "IXSCAN")
        self.assertEqual("x_1", stage.get("indexName"))
        self.assertTrue(stage.get("isPartial"))

        explain = await db.test.find({"x": {"$gt": 1}, "a": 1}).explain()
        stage = self.get_plan_stage(explain["queryPlanner"]["winningPlan"], "IXSCAN")
        self.assertEqual("x_1", stage.get("indexName"))
        self.assertTrue(stage.get("isPartial"))

        explain = await db.test.find({"x": 6, "a": {"$lte": 1}}).explain()
        stage = self.get_plan_stage(explain["queryPlanner"]["winningPlan"], "IXSCAN")
        self.assertEqual("x_1", stage.get("indexName"))
        self.assertTrue(stage.get("isPartial"))

        # Operations that do not use the partial index.
        explain = await db.test.find({"x": 6, "a": {"$lte": 1.6}}).explain()
        stage = self.get_plan_stage(explain["queryPlanner"]["winningPlan"], "COLLSCAN")
        self.assertNotEqual({}, stage)
        explain = await db.test.find({"x": 6}).explain()
        stage = self.get_plan_stage(explain["queryPlanner"]["winningPlan"], "COLLSCAN")
        self.assertNotEqual({}, stage)

        # Test drop_indexes.
        await db.test.drop_index("x_1")
        explain = await db.test.find({"x": 6, "a": 1}).explain()
        stage = self.get_plan_stage(explain["queryPlanner"]["winningPlan"], "COLLSCAN")
        self.assertNotEqual({}, stage)

    async def test_field_selection(self):
        db = self.db
        await db.drop_collection("test")

        doc = {"a": 1, "b": 5, "c": {"d": 5, "e": 10}}
        await db.test.insert_one(doc)

        # Test field inclusion
        doc = await anext(db.test.find({}, ["_id"]))
        self.assertEqual(list(doc), ["_id"])
        doc = await anext(db.test.find({}, ["a"]))
        l = list(doc)
        l.sort()
        self.assertEqual(l, ["_id", "a"])
        doc = await anext(db.test.find({}, ["b"]))
        l = list(doc)
        l.sort()
        self.assertEqual(l, ["_id", "b"])
        doc = await anext(db.test.find({}, ["c"]))
        l = list(doc)
        l.sort()
        self.assertEqual(l, ["_id", "c"])
        doc = await anext(db.test.find({}, ["a"]))
        self.assertEqual(doc["a"], 1)
        doc = await anext(db.test.find({}, ["b"]))
        self.assertEqual(doc["b"], 5)
        doc = await anext(db.test.find({}, ["c"]))
        self.assertEqual(doc["c"], {"d": 5, "e": 10})

        # Test inclusion of fields with dots
        doc = await anext(db.test.find({}, ["c.d"]))
        self.assertEqual(doc["c"], {"d": 5})
        doc = await anext(db.test.find({}, ["c.e"]))
        self.assertEqual(doc["c"], {"e": 10})
        doc = await anext(db.test.find({}, ["b", "c.e"]))
        self.assertEqual(doc["c"], {"e": 10})

        doc = await anext(db.test.find({}, ["b", "c.e"]))
        l = list(doc)
        l.sort()
        self.assertEqual(l, ["_id", "b", "c"])
        doc = await anext(db.test.find({}, ["b", "c.e"]))
        self.assertEqual(doc["b"], 5)

        # Test field exclusion
        doc = await anext(db.test.find({}, {"a": False, "b": 0}))
        l = list(doc)
        l.sort()
        self.assertEqual(l, ["_id", "c"])

        doc = await anext(db.test.find({}, {"_id": False}))
        l = list(doc)
        self.assertNotIn("_id", l)

    async def test_options(self):
        db = self.db
        await db.drop_collection("test")
        await db.create_collection("test", capped=True, size=4096)
        result = await db.test.options()
        self.assertEqual(result, {"capped": True, "size": 4096})
        await db.drop_collection("test")

    async def test_insert_one(self):
        db = self.db
        await db.test.drop()

        document: dict[str, Any] = {"_id": 1000}
        result = await db.test.insert_one(document)
        self.assertIsInstance(result, InsertOneResult)
        self.assertIsInstance(result.inserted_id, int)
        self.assertEqual(document["_id"], result.inserted_id)
        self.assertTrue(result.acknowledged)
        self.assertIsNotNone(await db.test.find_one({"_id": document["_id"]}))
        self.assertEqual(1, await db.test.count_documents({}))

        document = {"foo": "bar"}
        result = await db.test.insert_one(document)
        self.assertIsInstance(result, InsertOneResult)
        self.assertIsInstance(result.inserted_id, ObjectId)
        self.assertEqual(document["_id"], result.inserted_id)
        self.assertTrue(result.acknowledged)
        self.assertIsNotNone(await db.test.find_one({"_id": document["_id"]}))
        self.assertEqual(2, await db.test.count_documents({}))

        db = db.client.get_database(db.name, write_concern=WriteConcern(w=0))
        result = await db.test.insert_one(document)
        self.assertIsInstance(result, InsertOneResult)
        self.assertIsInstance(result.inserted_id, ObjectId)
        self.assertEqual(document["_id"], result.inserted_id)
        self.assertFalse(result.acknowledged)
        # The insert failed duplicate key...

        async def async_lambda():
            return await db.test.count_documents({}) == 2

        await async_wait_until(async_lambda, "forcing duplicate key error")

        document = RawBSONDocument(encode({"_id": ObjectId(), "foo": "bar"}))
        result = await db.test.insert_one(document)
        self.assertIsInstance(result, InsertOneResult)
        self.assertEqual(result.inserted_id, None)

    async def test_insert_many(self):
        db = self.db
        await db.test.drop()

        docs: list = [{} for _ in range(5)]
        result = await db.test.insert_many(docs)
        self.assertIsInstance(result, InsertManyResult)
        self.assertIsInstance(result.inserted_ids, list)
        self.assertEqual(5, len(result.inserted_ids))
        for doc in docs:
            _id = doc["_id"]
            self.assertIsInstance(_id, ObjectId)
            self.assertIn(_id, result.inserted_ids)
            self.assertEqual(1, await db.test.count_documents({"_id": _id}))
        self.assertTrue(result.acknowledged)

        docs = [{"_id": i} for i in range(5)]
        result = await db.test.insert_many(docs)
        self.assertIsInstance(result, InsertManyResult)
        self.assertIsInstance(result.inserted_ids, list)
        self.assertEqual(5, len(result.inserted_ids))
        for doc in docs:
            _id = doc["_id"]
            self.assertIsInstance(_id, int)
            self.assertIn(_id, result.inserted_ids)
            self.assertEqual(1, await db.test.count_documents({"_id": _id}))
        self.assertTrue(result.acknowledged)

        docs = [RawBSONDocument(encode({"_id": i + 5})) for i in range(5)]
        result = await db.test.insert_many(docs)
        self.assertIsInstance(result, InsertManyResult)
        self.assertIsInstance(result.inserted_ids, list)
        self.assertEqual([], result.inserted_ids)

        db = db.client.get_database(db.name, write_concern=WriteConcern(w=0))
        docs: list = [{} for _ in range(5)]
        result = await db.test.insert_many(docs)
        self.assertIsInstance(result, InsertManyResult)
        self.assertFalse(result.acknowledged)
        self.assertEqual(20, await db.test.count_documents({}))

    async def test_insert_many_generator(self):
        coll = self.db.test
        await coll.delete_many({})

        def gen():
            yield {"a": 1, "b": 1}
            yield {"a": 1, "b": 2}
            yield {"a": 2, "b": 3}
            yield {"a": 3, "b": 5}
            yield {"a": 5, "b": 8}

        result = await coll.insert_many(gen())
        self.assertEqual(5, len(result.inserted_ids))

    async def test_insert_many_invalid(self):
        db = self.db

        with self.assertRaisesRegex(TypeError, "documents must be a non-empty list"):
            await db.test.insert_many({})

        with self.assertRaisesRegex(TypeError, "documents must be a non-empty list"):
            await db.test.insert_many([])

        with self.assertRaisesRegex(TypeError, "documents must be a non-empty list"):
            await db.test.insert_many(1)  # type: ignore[arg-type]

        with self.assertRaisesRegex(TypeError, "documents must be a non-empty list"):
            await db.test.insert_many(RawBSONDocument(encode({"_id": 2})))

    async def test_delete_one(self):
        await self.db.test.drop()

        await self.db.test.insert_one({"x": 1})
        await self.db.test.insert_one({"y": 1})
        await self.db.test.insert_one({"z": 1})

        result = await self.db.test.delete_one({"x": 1})
        self.assertIsInstance(result, DeleteResult)
        self.assertEqual(1, result.deleted_count)
        self.assertTrue(result.acknowledged)
        self.assertEqual(2, await self.db.test.count_documents({}))

        result = await self.db.test.delete_one({"y": 1})
        self.assertIsInstance(result, DeleteResult)
        self.assertEqual(1, result.deleted_count)
        self.assertTrue(result.acknowledged)
        self.assertEqual(1, await self.db.test.count_documents({}))

        db = self.db.client.get_database(self.db.name, write_concern=WriteConcern(w=0))
        result = await db.test.delete_one({"z": 1})
        self.assertIsInstance(result, DeleteResult)
        self.assertRaises(InvalidOperation, lambda: result.deleted_count)
        self.assertFalse(result.acknowledged)

        async def lambda_async():
            return await db.test.count_documents({}) == 0

        await async_wait_until(lambda_async, "delete 1 documents")

    async def test_delete_many(self):
        await self.db.test.drop()

        await self.db.test.insert_one({"x": 1})
        await self.db.test.insert_one({"x": 1})
        await self.db.test.insert_one({"y": 1})
        await self.db.test.insert_one({"y": 1})

        result = await self.db.test.delete_many({"x": 1})
        self.assertIsInstance(result, DeleteResult)
        self.assertEqual(2, result.deleted_count)
        self.assertTrue(result.acknowledged)
        self.assertEqual(0, await self.db.test.count_documents({"x": 1}))

        db = self.db.client.get_database(self.db.name, write_concern=WriteConcern(w=0))
        result = await db.test.delete_many({"y": 1})
        self.assertIsInstance(result, DeleteResult)
        self.assertRaises(InvalidOperation, lambda: result.deleted_count)
        self.assertFalse(result.acknowledged)

        async def lambda_async():
            return await db.test.count_documents({}) == 0

        await async_wait_until(lambda_async, "delete 2 documents")

    async def test_command_document_too_large(self):
        large = "*" * (await async_client_context.max_bson_size + _COMMAND_OVERHEAD)
        coll = self.db.test
        with self.assertRaises(DocumentTooLarge):
            await coll.insert_one({"data": large})
        # update_one and update_many are the same
        with self.assertRaises(DocumentTooLarge):
            await coll.replace_one({}, {"data": large})
        with self.assertRaises(DocumentTooLarge):
            await coll.delete_one({"data": large})

    async def test_write_large_document(self):
        max_size = await async_client_context.max_bson_size
        half_size = int(max_size / 2)
        max_str = "x" * max_size
        half_str = "x" * half_size
        self.assertEqual(max_size, 16777216)

        with self.assertRaises(OperationFailure):
            await self.db.test.insert_one({"foo": max_str})
        with self.assertRaises(OperationFailure):
            await self.db.test.replace_one({}, {"foo": max_str}, upsert=True)
        with self.assertRaises(OperationFailure):
            await self.db.test.insert_many([{"x": 1}, {"foo": max_str}])
        await self.db.test.insert_many([{"foo": half_str}, {"foo": half_str}])

        await self.db.test.insert_one({"bar": "x"})
        # Use w=0 here to test legacy doc size checking in all server versions
        unack_coll = self.db.test.with_options(write_concern=WriteConcern(w=0))
        with self.assertRaises(DocumentTooLarge):
            await unack_coll.replace_one({"bar": "x"}, {"bar": "x" * (max_size - 14)})
        await self.db.test.replace_one({"bar": "x"}, {"bar": "x" * (max_size - 32)})

    async def test_insert_bypass_document_validation(self):
        db = self.db
        await db.test.drop()
        await db.create_collection("test", validator={"a": {"$exists": True}})
        db_w0 = self.db.client.get_database(self.db.name, write_concern=WriteConcern(w=0))

        # Test insert_one
        with self.assertRaises(OperationFailure):
            await db.test.insert_one({"_id": 1, "x": 100})
        result = await db.test.insert_one({"_id": 1, "x": 100}, bypass_document_validation=True)
        self.assertIsInstance(result, InsertOneResult)
        self.assertEqual(1, result.inserted_id)
        result = await db.test.insert_one({"_id": 2, "a": 0})
        self.assertIsInstance(result, InsertOneResult)
        self.assertEqual(2, result.inserted_id)

        await db_w0.test.insert_one({"y": 1}, bypass_document_validation=True)

        async def async_lambda():
            return await db_w0.test.find_one({"y": 1})

        await async_wait_until(async_lambda, "find w:0 inserted document")

        # Test insert_many
        docs = [{"_id": i, "x": 100 - i} for i in range(3, 100)]
        with self.assertRaises(OperationFailure):
            await db.test.insert_many(docs)
        result = await db.test.insert_many(docs, bypass_document_validation=True)
        self.assertIsInstance(result, InsertManyResult)
        self.assertTrue(97, len(result.inserted_ids))
        for doc in docs:
            _id = doc["_id"]
            self.assertIsInstance(_id, int)
            self.assertIn(_id, result.inserted_ids)
            self.assertEqual(1, await db.test.count_documents({"x": doc["x"]}))
        self.assertTrue(result.acknowledged)
        docs = [{"_id": i, "a": 200 - i} for i in range(100, 200)]
        result = await db.test.insert_many(docs)
        self.assertIsInstance(result, InsertManyResult)
        self.assertTrue(97, len(result.inserted_ids))
        for doc in docs:
            _id = doc["_id"]
            self.assertIsInstance(_id, int)
            self.assertIn(_id, result.inserted_ids)
            self.assertEqual(1, await db.test.count_documents({"a": doc["a"]}))
        self.assertTrue(result.acknowledged)

        with self.assertRaises(OperationFailure):
            await db_w0.test.insert_many(
                [{"x": 1}, {"x": 2}],
                bypass_document_validation=True,
            )

    async def test_replace_bypass_document_validation(self):
        db = self.db
        await db.test.drop()
        await db.create_collection("test", validator={"a": {"$exists": True}})
        db_w0 = self.db.client.get_database(self.db.name, write_concern=WriteConcern(w=0))

        # Test replace_one
        await db.test.insert_one({"a": 101})
        with self.assertRaises(OperationFailure):
            await db.test.replace_one({"a": 101}, {"y": 1})
        self.assertEqual(0, await db.test.count_documents({"y": 1}))
        self.assertEqual(1, await db.test.count_documents({"a": 101}))
        await db.test.replace_one({"a": 101}, {"y": 1}, bypass_document_validation=True)
        self.assertEqual(0, await db.test.count_documents({"a": 101}))
        self.assertEqual(1, await db.test.count_documents({"y": 1}))
        await db.test.replace_one({"y": 1}, {"a": 102})
        self.assertEqual(0, await db.test.count_documents({"y": 1}))
        self.assertEqual(0, await db.test.count_documents({"a": 101}))
        self.assertEqual(1, await db.test.count_documents({"a": 102}))

        await db.test.insert_one({"y": 1}, bypass_document_validation=True)
        with self.assertRaises(OperationFailure):
            await db.test.replace_one({"y": 1}, {"x": 101})
        self.assertEqual(0, await db.test.count_documents({"x": 101}))
        self.assertEqual(1, await db.test.count_documents({"y": 1}))
        await db.test.replace_one({"y": 1}, {"x": 101}, bypass_document_validation=True)
        self.assertEqual(0, await db.test.count_documents({"y": 1}))
        self.assertEqual(1, await db.test.count_documents({"x": 101}))
        await db.test.replace_one({"x": 101}, {"a": 103}, bypass_document_validation=False)
        self.assertEqual(0, await db.test.count_documents({"x": 101}))
        self.assertEqual(1, await db.test.count_documents({"a": 103}))

        await db.test.insert_one({"y": 1}, bypass_document_validation=True)
        await db_w0.test.replace_one({"y": 1}, {"x": 1}, bypass_document_validation=True)

        async def predicate():
            return await db_w0.test.find_one({"x": 1})

        await async_wait_until(predicate, "find w:0 replaced document")

    async def test_update_bypass_document_validation(self):
        db = self.db
        await db.test.drop()
        await db.test.insert_one({"z": 5})
        await db.command(SON([("collMod", "test"), ("validator", {"z": {"$gte": 0}})]))
        db_w0 = self.db.client.get_database(self.db.name, write_concern=WriteConcern(w=0))

        # Test update_one
        with self.assertRaises(OperationFailure):
            await db.test.update_one({"z": 5}, {"$inc": {"z": -10}})
        self.assertEqual(0, await db.test.count_documents({"z": -5}))
        self.assertEqual(1, await db.test.count_documents({"z": 5}))
        await db.test.update_one({"z": 5}, {"$inc": {"z": -10}}, bypass_document_validation=True)
        self.assertEqual(0, await db.test.count_documents({"z": 5}))
        self.assertEqual(1, await db.test.count_documents({"z": -5}))
        await db.test.update_one({"z": -5}, {"$inc": {"z": 6}}, bypass_document_validation=False)
        self.assertEqual(1, await db.test.count_documents({"z": 1}))
        self.assertEqual(0, await db.test.count_documents({"z": -5}))

        await db.test.insert_one({"z": -10}, bypass_document_validation=True)
        with self.assertRaises(OperationFailure):
            await db.test.update_one({"z": -10}, {"$inc": {"z": 1}})
        self.assertEqual(0, await db.test.count_documents({"z": -9}))
        self.assertEqual(1, await db.test.count_documents({"z": -10}))
        await db.test.update_one({"z": -10}, {"$inc": {"z": 1}}, bypass_document_validation=True)
        self.assertEqual(1, await db.test.count_documents({"z": -9}))
        self.assertEqual(0, await db.test.count_documents({"z": -10}))
        await db.test.update_one({"z": -9}, {"$inc": {"z": 9}}, bypass_document_validation=False)
        self.assertEqual(0, await db.test.count_documents({"z": -9}))
        self.assertEqual(1, await db.test.count_documents({"z": 0}))

        await db.test.insert_one({"y": 1, "x": 0}, bypass_document_validation=True)
        await db_w0.test.update_one({"y": 1}, {"$inc": {"x": 1}}, bypass_document_validation=True)

        async def async_lambda():
            return await db_w0.test.find_one({"y": 1, "x": 1})

        await async_wait_until(async_lambda, "find w:0 updated document")

        # Test update_many
        await db.test.insert_many([{"z": i} for i in range(3, 101)])
        await db.test.insert_one({"y": 0}, bypass_document_validation=True)
        with self.assertRaises(OperationFailure):
            await db.test.update_many({}, {"$inc": {"z": -100}})
        self.assertEqual(100, await db.test.count_documents({"z": {"$gte": 0}}))
        self.assertEqual(0, await db.test.count_documents({"z": {"$lt": 0}}))
        self.assertEqual(0, await db.test.count_documents({"y": 0, "z": -100}))
        await db.test.update_many(
            {"z": {"$gte": 0}}, {"$inc": {"z": -100}}, bypass_document_validation=True
        )
        self.assertEqual(0, await db.test.count_documents({"z": {"$gt": 0}}))
        self.assertEqual(100, await db.test.count_documents({"z": {"$lte": 0}}))
        await db.test.update_many(
            {"z": {"$gt": -50}}, {"$inc": {"z": 100}}, bypass_document_validation=False
        )
        self.assertEqual(50, await db.test.count_documents({"z": {"$gt": 0}}))
        self.assertEqual(50, await db.test.count_documents({"z": {"$lt": 0}}))

        await db.test.insert_many([{"z": -i} for i in range(50)], bypass_document_validation=True)
        with self.assertRaises(OperationFailure):
            await db.test.update_many({}, {"$inc": {"z": 1}})
        self.assertEqual(100, await db.test.count_documents({"z": {"$lte": 0}}))
        self.assertEqual(50, await db.test.count_documents({"z": {"$gt": 1}}))
        await db.test.update_many(
            {"z": {"$gte": 0}}, {"$inc": {"z": -100}}, bypass_document_validation=True
        )
        self.assertEqual(0, await db.test.count_documents({"z": {"$gt": 0}}))
        self.assertEqual(150, await db.test.count_documents({"z": {"$lte": 0}}))
        await db.test.update_many(
            {"z": {"$lte": 0}}, {"$inc": {"z": 100}}, bypass_document_validation=False
        )
        self.assertEqual(150, await db.test.count_documents({"z": {"$gte": 0}}))
        self.assertEqual(0, await db.test.count_documents({"z": {"$lt": 0}}))

        await db.test.insert_one({"m": 1, "x": 0}, bypass_document_validation=True)
        await db.test.insert_one({"m": 1, "x": 0}, bypass_document_validation=True)
        await db_w0.test.update_many({"m": 1}, {"$inc": {"x": 1}}, bypass_document_validation=True)

        async def async_lambda():
            return await db_w0.test.count_documents({"m": 1, "x": 1}) == 2

        await async_wait_until(async_lambda, "find w:0 updated documents")

    async def test_bypass_document_validation_bulk_write(self):
        db = self.db
        await db.test.drop()
        await db.create_collection("test", validator={"a": {"$gte": 0}})
        db_w0 = self.db.client.get_database(self.db.name, write_concern=WriteConcern(w=0))

        ops: list = [
            InsertOne({"a": -10}),
            InsertOne({"a": -11}),
            InsertOne({"a": -12}),
            UpdateOne({"a": {"$lte": -10}}, {"$inc": {"a": 1}}),
            UpdateMany({"a": {"$lte": -10}}, {"$inc": {"a": 1}}),
            ReplaceOne({"a": {"$lte": -10}}, {"a": -1}),
        ]
        await db.test.bulk_write(ops, bypass_document_validation=True)

        self.assertEqual(3, await db.test.count_documents({}))
        self.assertEqual(1, await db.test.count_documents({"a": -11}))
        self.assertEqual(1, await db.test.count_documents({"a": -1}))
        self.assertEqual(1, await db.test.count_documents({"a": -9}))

        # Assert that the operations would fail without bypass_doc_val
        for op in ops:
            with self.assertRaises(BulkWriteError):
                await db.test.bulk_write([op])

        with self.assertRaises(OperationFailure):
            await db_w0.test.bulk_write(ops, bypass_document_validation=True)

    async def test_find_by_default_dct(self):
        db = self.db
        await db.test.insert_one({"foo": "bar"})
        dct = defaultdict(dict, [("foo", "bar")])  # type: ignore[arg-type]
        self.assertIsNotNone(await db.test.find_one(dct))
        self.assertEqual(dct, defaultdict(dict, [("foo", "bar")]))

    async def test_find_w_fields(self):
        db = self.db
        await db.test.delete_many({})

        await db.test.insert_one(
            {"x": 1, "mike": "awesome", "extra thing": "abcdefghijklmnopqrstuvwxyz"}
        )
        self.assertEqual(1, await db.test.count_documents({}))
        doc = await anext(db.test.find({}))
        self.assertIn("x", doc)
        doc = await anext(db.test.find({}))
        self.assertIn("mike", doc)
        doc = await anext(db.test.find({}))
        self.assertIn("extra thing", doc)
        doc = await anext(db.test.find({}, ["x", "mike"]))
        self.assertIn("x", doc)
        doc = await anext(db.test.find({}, ["x", "mike"]))
        self.assertIn("mike", doc)
        doc = await anext(db.test.find({}, ["x", "mike"]))
        self.assertNotIn("extra thing", doc)
        doc = await anext(db.test.find({}, ["mike"]))
        self.assertNotIn("x", doc)
        doc = await anext(db.test.find({}, ["mike"]))
        self.assertIn("mike", doc)
        doc = await anext(db.test.find({}, ["mike"]))
        self.assertNotIn("extra thing", doc)

    @no_type_check
    async def test_fields_specifier_as_dict(self):
        db = self.db
        await db.test.delete_many({})

        await db.test.insert_one({"x": [1, 2, 3], "mike": "awesome"})

        self.assertEqual([1, 2, 3], (await db.test.find_one())["x"])
        self.assertEqual([2, 3], (await db.test.find_one(projection={"x": {"$slice": -2}}))["x"])
        self.assertNotIn("x", await db.test.find_one(projection={"x": 0}))
        self.assertIn("mike", await db.test.find_one(projection={"x": 0}))

    async def test_find_w_regex(self):
        db = self.db
        await db.test.delete_many({})

        await db.test.insert_one({"x": "hello_world"})
        await db.test.insert_one({"x": "hello_mike"})
        await db.test.insert_one({"x": "hello_mikey"})
        await db.test.insert_one({"x": "hello_test"})

        self.assertEqual(len(await db.test.find().to_list()), 4)
        self.assertEqual(len(await db.test.find({"x": re.compile("^hello.*")}).to_list()), 4)
        self.assertEqual(len(await db.test.find({"x": re.compile("ello")}).to_list()), 4)
        self.assertEqual(len(await db.test.find({"x": re.compile("^hello$")}).to_list()), 0)
        self.assertEqual(len(await db.test.find({"x": re.compile("^hello_mi.*$")}).to_list()), 2)

    async def test_id_can_be_anything(self):
        db = self.db

        await db.test.delete_many({})
        auto_id = {"hello": "world"}
        await db.test.insert_one(auto_id)
        self.assertIsInstance(auto_id["_id"], ObjectId)

        numeric = {"_id": 240, "hello": "world"}
        await db.test.insert_one(numeric)
        self.assertEqual(numeric["_id"], 240)

        obj = {"_id": numeric, "hello": "world"}
        await db.test.insert_one(obj)
        self.assertEqual(obj["_id"], numeric)

        async for x in db.test.find():
            self.assertEqual(x["hello"], "world")
            self.assertIn("_id", x)

    async def test_unique_index(self):
        db = self.db
        await db.drop_collection("test")
        await db.test.create_index("hello")

        # No error.
        await db.test.insert_one({"hello": "world"})
        await db.test.insert_one({"hello": "world"})

        await db.drop_collection("test")
        await db.test.create_index("hello", unique=True)

        with self.assertRaises(DuplicateKeyError):
            await db.test.insert_one({"hello": "world"})
            await db.test.insert_one({"hello": "world"})

    async def test_duplicate_key_error(self):
        db = self.db
        await db.drop_collection("test")

        await db.test.create_index("x", unique=True)

        await db.test.insert_one({"_id": 1, "x": 1})

        with self.assertRaises(DuplicateKeyError) as context:
            await db.test.insert_one({"x": 1})

        self.assertIsNotNone(context.exception.details)

        with self.assertRaises(DuplicateKeyError) as context:
            await db.test.insert_one({"x": 1})

        self.assertIsNotNone(context.exception.details)
        self.assertEqual(1, await db.test.count_documents({}))

    async def test_write_error_text_handling(self):
        db = self.db
        await db.drop_collection("test")

        await db.test.create_index("text", unique=True)

        # Test workaround for SERVER-24007
        data = (
            b"a\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
            b"\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83"
        )

        text = utf_8_decode(data, None, True)
        await db.test.insert_one({"text": text})

        # Should raise DuplicateKeyError, not InvalidBSON
        with self.assertRaises(DuplicateKeyError):
            await db.test.insert_one({"text": text})

        with self.assertRaises(DuplicateKeyError):
            await db.test.replace_one({"_id": ObjectId()}, {"text": text}, upsert=True)

        # Should raise BulkWriteError, not InvalidBSON
        with self.assertRaises(BulkWriteError):
            await db.test.insert_many([{"text": text}])

    async def test_write_error_unicode(self):
        coll = self.db.test
        self.addAsyncCleanup(coll.drop)

        await coll.create_index("a", unique=True)
        await coll.insert_one({"a": "unicode \U0001f40d"})
        with self.assertRaisesRegex(DuplicateKeyError, "E11000 duplicate key error") as ctx:
            await coll.insert_one({"a": "unicode \U0001f40d"})

        # Once more for good measure.
        self.assertIn("E11000 duplicate key error", str(ctx.exception))

    async def test_wtimeout(self):
        # Ensure setting wtimeout doesn't disable write concern altogether.
        # See SERVER-12596.
        collection = self.db.test
        await collection.drop()
        await collection.insert_one({"_id": 1})

        coll = collection.with_options(write_concern=WriteConcern(w=1, wtimeout=1000))
        with self.assertRaises(DuplicateKeyError):
            await coll.insert_one({"_id": 1})

        coll = collection.with_options(write_concern=WriteConcern(wtimeout=1000))
        with self.assertRaises(DuplicateKeyError):
            await coll.insert_one({"_id": 1})

    async def test_error_code(self):
        try:
            await self.db.test.update_many({}, {"$thismodifierdoesntexist": 1})
        except OperationFailure as exc:
            self.assertIn(exc.code, (9, 10147, 16840, 17009))
            # Just check that we set the error document. Fields
            # vary by MongoDB version.
            self.assertIsNotNone(exc.details)
        else:
            self.fail("OperationFailure was not raised")

    async def test_index_on_subfield(self):
        db = self.db
        await db.drop_collection("test")

        await db.test.insert_one({"hello": {"a": 4, "b": 5}})
        await db.test.insert_one({"hello": {"a": 7, "b": 2}})
        await db.test.insert_one({"hello": {"a": 4, "b": 10}})

        await db.drop_collection("test")
        await db.test.create_index("hello.a", unique=True)

        await db.test.insert_one({"hello": {"a": 4, "b": 5}})
        await db.test.insert_one({"hello": {"a": 7, "b": 2}})
        with self.assertRaises(DuplicateKeyError):
            await db.test.insert_one({"hello": {"a": 4, "b": 10}})

    async def test_replace_one(self):
        db = self.db
        await db.drop_collection("test")

        with self.assertRaises(ValueError):
            await db.test.replace_one({}, {"$set": {"x": 1}})

        id1 = (await db.test.insert_one({"x": 1})).inserted_id
        result = await db.test.replace_one({"x": 1}, {"y": 1})
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(1, result.matched_count)
        self.assertIn(result.modified_count, (None, 1))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual(1, await db.test.count_documents({"y": 1}))
        self.assertEqual(0, await db.test.count_documents({"x": 1}))
        self.assertEqual((await db.test.find_one(id1))["y"], 1)  # type: ignore

        replacement = RawBSONDocument(encode({"_id": id1, "z": 1}))
        result = await db.test.replace_one({"y": 1}, replacement, True)
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(1, result.matched_count)
        self.assertIn(result.modified_count, (None, 1))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual(1, await db.test.count_documents({"z": 1}))
        self.assertEqual(0, await db.test.count_documents({"y": 1}))
        self.assertEqual((await db.test.find_one(id1))["z"], 1)  # type: ignore

        result = await db.test.replace_one({"x": 2}, {"y": 2}, True)
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(0, result.matched_count)
        self.assertIn(result.modified_count, (None, 0))
        self.assertIsInstance(result.upserted_id, ObjectId)
        self.assertTrue(result.acknowledged)
        self.assertEqual(1, await db.test.count_documents({"y": 2}))

        db = db.client.get_database(db.name, write_concern=WriteConcern(w=0))
        result = await db.test.replace_one({"x": 0}, {"y": 0})
        self.assertIsInstance(result, UpdateResult)
        self.assertRaises(InvalidOperation, lambda: result.matched_count)
        self.assertRaises(InvalidOperation, lambda: result.modified_count)
        self.assertRaises(InvalidOperation, lambda: result.upserted_id)
        self.assertFalse(result.acknowledged)

    async def test_update_one(self):
        db = self.db
        await db.drop_collection("test")

        with self.assertRaises(ValueError):
            await db.test.update_one({}, {"x": 1})

        id1 = (await db.test.insert_one({"x": 5})).inserted_id
        result = await db.test.update_one({}, {"$inc": {"x": 1}})
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(1, result.matched_count)
        self.assertIn(result.modified_count, (None, 1))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual((await db.test.find_one(id1))["x"], 6)  # type: ignore

        id2 = (await db.test.insert_one({"x": 1})).inserted_id
        result = await db.test.update_one({"x": 6}, {"$inc": {"x": 1}})
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(1, result.matched_count)
        self.assertIn(result.modified_count, (None, 1))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual((await db.test.find_one(id1))["x"], 7)  # type: ignore
        self.assertEqual((await db.test.find_one(id2))["x"], 1)  # type: ignore

        result = await db.test.update_one({"x": 2}, {"$set": {"y": 1}}, True)
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(0, result.matched_count)
        self.assertIn(result.modified_count, (None, 0))
        self.assertIsInstance(result.upserted_id, ObjectId)
        self.assertTrue(result.acknowledged)

        db = db.client.get_database(db.name, write_concern=WriteConcern(w=0))
        result = await db.test.update_one({"x": 0}, {"$inc": {"x": 1}})
        self.assertIsInstance(result, UpdateResult)
        self.assertRaises(InvalidOperation, lambda: result.matched_count)
        self.assertRaises(InvalidOperation, lambda: result.modified_count)
        self.assertRaises(InvalidOperation, lambda: result.upserted_id)
        self.assertFalse(result.acknowledged)

    async def test_update_result(self):
        db = self.db
        await db.drop_collection("test")

        result = await db.test.update_one({"x": 0}, {"$inc": {"x": 1}}, upsert=True)
        self.assertEqual(result.did_upsert, True)

        result = await db.test.update_one({"_id": None, "x": 0}, {"$inc": {"x": 1}}, upsert=True)
        self.assertEqual(result.did_upsert, True)

        result = await db.test.update_one({"_id": None}, {"$inc": {"x": 1}})
        self.assertEqual(result.did_upsert, False)

    async def test_update_many(self):
        db = self.db
        await db.drop_collection("test")

        with self.assertRaises(ValueError):
            await db.test.update_many({}, {"x": 1})

        await db.test.insert_one({"x": 4, "y": 3})
        await db.test.insert_one({"x": 5, "y": 5})
        await db.test.insert_one({"x": 4, "y": 4})

        result = await db.test.update_many({"x": 4}, {"$set": {"y": 5}})
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(2, result.matched_count)
        self.assertIn(result.modified_count, (None, 2))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual(3, await db.test.count_documents({"y": 5}))

        result = await db.test.update_many({"x": 5}, {"$set": {"y": 6}})
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(1, result.matched_count)
        self.assertIn(result.modified_count, (None, 1))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual(1, await db.test.count_documents({"y": 6}))

        result = await db.test.update_many({"x": 2}, {"$set": {"y": 1}}, True)
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(0, result.matched_count)
        self.assertIn(result.modified_count, (None, 0))
        self.assertIsInstance(result.upserted_id, ObjectId)
        self.assertTrue(result.acknowledged)

        db = db.client.get_database(db.name, write_concern=WriteConcern(w=0))
        result = await db.test.update_many({"x": 0}, {"$inc": {"x": 1}})
        self.assertIsInstance(result, UpdateResult)
        self.assertRaises(InvalidOperation, lambda: result.matched_count)
        self.assertRaises(InvalidOperation, lambda: result.modified_count)
        self.assertRaises(InvalidOperation, lambda: result.upserted_id)
        self.assertFalse(result.acknowledged)

    async def test_update_check_keys(self):
        await self.db.drop_collection("test")
        self.assertTrue(await self.db.test.insert_one({"hello": "world"}))

        # Modify shouldn't check keys...
        self.assertTrue(
            await self.db.test.update_one(
                {"hello": "world"}, {"$set": {"foo.bar": "baz"}}, upsert=True
            )
        )

        # I know this seems like testing the server but I'd like to be notified
        # by CI if the server's behavior changes here.
        doc = SON([("$set", {"foo.bar": "bim"}), ("hello", "world")])
        with self.assertRaises(OperationFailure):
            await self.db.test.update_one({"hello": "world"}, doc, upsert=True)

        # This is going to cause keys to be checked and raise InvalidDocument.
        # That's OK assuming the server's behavior in the previous assert
        # doesn't change. If the behavior changes checking the first key for
        # '$' in update won't be good enough anymore.
        doc = SON([("hello", "world"), ("$set", {"foo.bar": "bim"})])
        with self.assertRaises(OperationFailure):
            await self.db.test.replace_one({"hello": "world"}, doc, upsert=True)

        # Replace with empty document
        self.assertNotEqual(
            0, (await self.db.test.replace_one({"hello": "world"}, {})).matched_count
        )

    async def test_acknowledged_delete(self):
        db = self.db
        await db.drop_collection("test")
        await db.test.insert_many([{"x": 1}, {"x": 1}])
        self.assertEqual(2, (await db.test.delete_many({})).deleted_count)
        self.assertEqual(0, (await db.test.delete_many({})).deleted_count)

    @async_client_context.require_version_max(4, 9)
    async def test_manual_last_error(self):
        coll = self.db.get_collection("test", write_concern=WriteConcern(w=0))
        await coll.insert_one({"x": 1})
        await self.db.command("getlasterror", w=1, wtimeout=1)

    async def test_count_documents(self):
        db = self.db
        await db.drop_collection("test")
        self.addAsyncCleanup(db.drop_collection, "test")

        self.assertEqual(await db.test.count_documents({}), 0)
        await db.wrong.insert_many([{}, {}])
        self.assertEqual(await db.test.count_documents({}), 0)
        await db.test.insert_many([{}, {}])
        self.assertEqual(await db.test.count_documents({}), 2)
        await db.test.insert_many([{"foo": "bar"}, {"foo": "baz"}])
        self.assertEqual(await db.test.count_documents({"foo": "bar"}), 1)
        self.assertEqual(await db.test.count_documents({"foo": re.compile(r"ba.*")}), 2)

    async def test_estimated_document_count(self):
        db = self.db
        await db.drop_collection("test")
        self.addAsyncCleanup(db.drop_collection, "test")

        self.assertEqual(await db.test.estimated_document_count(), 0)
        await db.wrong.insert_many([{}, {}])
        self.assertEqual(await db.test.estimated_document_count(), 0)
        await db.test.insert_many([{}, {}])
        self.assertEqual(await db.test.estimated_document_count(), 2)

    async def test_aggregate(self):
        db = self.db
        await db.drop_collection("test")
        await db.test.insert_one({"foo": [1, 2]})

        with self.assertRaises(TypeError):
            await db.test.aggregate("wow")  # type: ignore[arg-type]

        pipeline = {"$project": {"_id": False, "foo": True}}
        result = await db.test.aggregate([pipeline])
        self.assertIsInstance(result, AsyncCommandCursor)
        self.assertEqual([{"foo": [1, 2]}], await result.to_list())

        # Test write concern.
        with self.write_concern_collection() as coll:
            await coll.aggregate([{"$out": "output-collection"}])

    async def test_aggregate_raw_bson(self):
        db = self.db
        await db.drop_collection("test")
        await db.test.insert_one({"foo": [1, 2]})

        with self.assertRaises(TypeError):
            await db.test.aggregate("wow")  # type: ignore[arg-type]

        pipeline = {"$project": {"_id": False, "foo": True}}
        coll = db.get_collection("test", codec_options=CodecOptions(document_class=RawBSONDocument))
        result = await coll.aggregate([pipeline])
        self.assertIsInstance(result, AsyncCommandCursor)
        first_result = await anext(result)
        self.assertIsInstance(first_result, RawBSONDocument)
        self.assertEqual([1, 2], list(first_result["foo"]))

    async def test_aggregation_cursor_validation(self):
        db = self.db
        projection = {"$project": {"_id": "$_id"}}
        cursor = await db.test.aggregate([projection], cursor={})
        self.assertIsInstance(cursor, AsyncCommandCursor)

    async def test_aggregation_cursor(self):
        db = self.db
        if await async_client_context.has_secondaries:
            # Test that getMore messages are sent to the right server.
            db = self.client.get_database(
                db.name,
                read_preference=ReadPreference.SECONDARY,
                write_concern=WriteConcern(w=self.w),
            )

        for collection_size in (10, 1000):
            await db.drop_collection("test")
            await db.test.insert_many([{"_id": i} for i in range(collection_size)])
            expected_sum = sum(range(collection_size))
            # Use batchSize to ensure multiple getMore messages
            cursor = await db.test.aggregate([{"$project": {"_id": "$_id"}}], batchSize=5)

            self.assertEqual(expected_sum, sum(doc["_id"] for doc in await cursor.to_list()))

        # Test that batchSize is handled properly.
        cursor = await db.test.aggregate([], batchSize=5)
        self.assertEqual(5, len(cursor._data))
        # Force a getMore
        cursor._data.clear()
        await anext(cursor)
        # batchSize - 1
        self.assertEqual(4, len(cursor._data))
        # Exhaust the cursor. There shouldn't be any errors.
        async for _doc in cursor:
            pass

    async def test_aggregation_cursor_alive(self):
        await self.db.test.delete_many({})
        await self.db.test.insert_many([{} for _ in range(3)])
        self.addAsyncCleanup(self.db.test.delete_many, {})
        cursor = await self.db.test.aggregate(pipeline=[], cursor={"batchSize": 2})
        n = 0
        while True:
            await cursor.next()
            n += 1
            if n == 3:
                self.assertFalse(cursor.alive)
                break

            self.assertTrue(cursor.alive)

    async def test_invalid_session_parameter(self):
        async def try_invalid_session():
            with await self.db.test.aggregate([], {}):  # type:ignore
                pass

        with self.assertRaisesRegex(ValueError, "must be an AsyncClientSession"):
            await try_invalid_session()

    async def test_large_limit(self):
        db = self.db
        await db.drop_collection("test_large_limit")
        await db.test_large_limit.create_index([("x", 1)])
        my_str = "mongomongo" * 1000

        await db.test_large_limit.insert_many({"x": i, "y": my_str} for i in range(2000))

        i = 0
        y = 0
        async for doc in db.test_large_limit.find(limit=1900).sort([("x", 1)]):
            i += 1
            y += doc["x"]

        self.assertEqual(1900, i)
        self.assertEqual((1900 * 1899) / 2, y)

    async def test_find_kwargs(self):
        db = self.db
        await db.drop_collection("test")
        await db.test.insert_many({"x": i} for i in range(10))

        self.assertEqual(10, await db.test.count_documents({}))

        total = 0
        async for x in db.test.find({}, skip=4, limit=2):
            total += x["x"]

        self.assertEqual(9, total)

    async def test_rename(self):
        db = self.db
        await db.drop_collection("test")
        await db.drop_collection("foo")

        with self.assertRaises(TypeError):
            await db.test.rename(5)  # type: ignore[arg-type]
        with self.assertRaises(InvalidName):
            await db.test.rename("")
        with self.assertRaises(InvalidName):
            await db.test.rename("te$t")
        with self.assertRaises(InvalidName):
            await db.test.rename(".test")
        with self.assertRaises(InvalidName):
            await db.test.rename("test.")
        with self.assertRaises(InvalidName):
            await db.test.rename("tes..t")

        self.assertEqual(0, await db.test.count_documents({}))
        self.assertEqual(0, await db.foo.count_documents({}))

        await db.test.insert_many({"x": i} for i in range(10))

        self.assertEqual(10, await db.test.count_documents({}))

        await db.test.rename("foo")

        self.assertEqual(0, await db.test.count_documents({}))
        self.assertEqual(10, await db.foo.count_documents({}))

        x = 0
        async for doc in db.foo.find():
            self.assertEqual(x, doc["x"])
            x += 1

        await db.test.insert_one({})
        with self.assertRaises(OperationFailure):
            await db.foo.rename("test")
        await db.foo.rename("test", dropTarget=True)

        with self.write_concern_collection() as coll:
            await coll.rename("foo")

    @no_type_check
    async def test_find_one(self):
        db = self.db
        await db.drop_collection("test")

        _id = (await db.test.insert_one({"hello": "world", "foo": "bar"})).inserted_id

        self.assertEqual("world", (await db.test.find_one())["hello"])
        self.assertEqual(await db.test.find_one(_id), await db.test.find_one())
        self.assertEqual(await db.test.find_one(None), await db.test.find_one())
        self.assertEqual(await db.test.find_one({}), await db.test.find_one())
        self.assertEqual(await db.test.find_one({"hello": "world"}), await db.test.find_one())

        self.assertIn("hello", await db.test.find_one(projection=["hello"]))
        self.assertNotIn("hello", await db.test.find_one(projection=["foo"]))

        self.assertIn("hello", await db.test.find_one(projection=("hello",)))
        self.assertNotIn("hello", await db.test.find_one(projection=("foo",)))

        self.assertIn("hello", await db.test.find_one(projection={"hello"}))
        self.assertNotIn("hello", await db.test.find_one(projection={"foo"}))

        self.assertIn("hello", await db.test.find_one(projection=frozenset(["hello"])))
        self.assertNotIn("hello", await db.test.find_one(projection=frozenset(["foo"])))

        self.assertEqual(["_id"], list(await db.test.find_one(projection={"_id": True})))
        self.assertIn("hello", list(await db.test.find_one(projection={})))
        self.assertIn("hello", list(await db.test.find_one(projection=[])))

        self.assertEqual(None, await db.test.find_one({"hello": "foo"}))
        self.assertEqual(None, await db.test.find_one(ObjectId()))

    async def test_find_one_non_objectid(self):
        db = self.db
        await db.drop_collection("test")

        await db.test.insert_one({"_id": 5})

        self.assertTrue(await db.test.find_one(5))
        self.assertFalse(await db.test.find_one(6))

    async def test_find_one_with_find_args(self):
        db = self.db
        await db.drop_collection("test")

        await db.test.insert_many([{"x": i} for i in range(1, 4)])

        self.assertEqual(1, (await db.test.find_one())["x"])
        self.assertEqual(2, (await db.test.find_one(skip=1, limit=2))["x"])

    async def test_find_with_sort(self):
        db = self.db
        await db.drop_collection("test")

        await db.test.insert_many([{"x": 2}, {"x": 1}, {"x": 3}])

        self.assertEqual(2, (await db.test.find_one())["x"])
        self.assertEqual(1, (await db.test.find_one(sort=[("x", 1)]))["x"])
        self.assertEqual(3, (await db.test.find_one(sort=[("x", -1)]))["x"])

        async def to_list(things):
            return [thing["x"] async for thing in things]

        self.assertEqual([2, 1, 3], await to_list(db.test.find()))
        self.assertEqual([1, 2, 3], await to_list(db.test.find(sort=[("x", 1)])))
        self.assertEqual([3, 2, 1], await to_list(db.test.find(sort=[("x", -1)])))

        with self.assertRaises(TypeError):
            await db.test.find(sort=5)
        with self.assertRaises(TypeError):
            await db.test.find(sort="hello")
        with self.assertRaises(TypeError):
            await db.test.find(sort=["hello", 1])

    # TODO doesn't actually test functionality, just that it doesn't blow up
    async def test_cursor_timeout(self):
        await self.db.test.find(no_cursor_timeout=True).to_list()
        await self.db.test.find(no_cursor_timeout=False).to_list()

    async def test_exhaust(self):
        if await async_is_mongos(self.db.client):
            with self.assertRaises(InvalidOperation):
                await anext(self.db.test.find(cursor_type=CursorType.EXHAUST))
            return

        # Limit is incompatible with exhaust.
        with self.assertRaises(InvalidOperation):
            await anext(self.db.test.find(cursor_type=CursorType.EXHAUST, limit=5))
        cur = self.db.test.find(cursor_type=CursorType.EXHAUST)
        with self.assertRaises(InvalidOperation):
            cur.limit(5)
            await cur.next()
        cur = self.db.test.find(limit=5)
        with self.assertRaises(InvalidOperation):
            await cur.add_option(64)
        cur = self.db.test.find()
        await cur.add_option(64)
        with self.assertRaises(InvalidOperation):
            cur.limit(5)

        await self.db.drop_collection("test")
        # Insert enough documents to require more than one batch
        await self.db.test.insert_many([{"i": i} for i in range(150)])

        client = await self.async_rs_or_single_client(maxPoolSize=1)
        pool = await async_get_pool(client)

        # Make sure the socket is returned after exhaustion.
        cur = client[self.db.name].test.find(cursor_type=CursorType.EXHAUST)
        await anext(cur)
        self.assertEqual(0, len(pool.conns))
        async for _ in cur:
            pass
        self.assertEqual(1, len(pool.conns))

        # Same as previous but don't call next()
        async for _ in client[self.db.name].test.find(cursor_type=CursorType.EXHAUST):
            pass
        self.assertEqual(1, len(pool.conns))

        # If the Cursor instance is discarded before being completely iterated
        # and the socket has pending data (more_to_come=True) we have to close
        # and discard the socket.
        cur = client[self.db.name].test.find(cursor_type=CursorType.EXHAUST, batch_size=2)
        if async_client_context.version.at_least(4, 2):
            # On 4.2+ we use OP_MSG which only sets more_to_come=True after the
            # first getMore.
            for _ in range(3):
                await anext(cur)
        else:
            await anext(cur)
        self.assertEqual(0, len(pool.conns))
        # if sys.platform.startswith("java") or "PyPy" in sys.version:
        #     # Don't wait for GC or use gc.collect(), it's unreliable.
        await cur.close()
        cur = None
        # Wait until the background thread returns the socket.
        await async_wait_until(lambda: pool.active_sockets == 0, "return socket")
        # The socket should be discarded.
        self.assertEqual(0, len(pool.conns))

    async def test_distinct(self):
        await self.db.drop_collection("test")

        test = self.db.test
        await test.insert_many([{"a": 1}, {"a": 2}, {"a": 2}, {"a": 2}, {"a": 3}])

        distinct = await test.distinct("a")
        distinct.sort()

        self.assertEqual([1, 2, 3], distinct)

        distinct = await test.find({"a": {"$gt": 1}}).distinct("a")
        distinct.sort()
        self.assertEqual([2, 3], distinct)

        distinct = await test.distinct("a", {"a": {"$gt": 1}})
        distinct.sort()
        self.assertEqual([2, 3], distinct)

        await self.db.drop_collection("test")

        await test.insert_one({"a": {"b": "a"}, "c": 12})
        await test.insert_one({"a": {"b": "b"}, "c": 12})
        await test.insert_one({"a": {"b": "c"}, "c": 12})
        await test.insert_one({"a": {"b": "c"}, "c": 12})

        distinct = await test.distinct("a.b")
        distinct.sort()

        self.assertEqual(["a", "b", "c"], distinct)

    async def test_query_on_query_field(self):
        await self.db.drop_collection("test")
        await self.db.test.insert_one({"query": "foo"})
        await self.db.test.insert_one({"bar": "foo"})

        self.assertEqual(1, await self.db.test.count_documents({"query": {"$ne": None}}))
        self.assertEqual(1, len(await self.db.test.find({"query": {"$ne": None}}).to_list()))

    async def test_min_query(self):
        await self.db.drop_collection("test")
        await self.db.test.insert_many([{"x": 1}, {"x": 2}])
        await self.db.test.create_index("x")

        cursor = self.db.test.find({"$min": {"x": 2}, "$query": {}}, hint="x_1")

        docs = await cursor.to_list()
        self.assertEqual(1, len(docs))
        self.assertEqual(2, docs[0]["x"])

    async def test_numerous_inserts(self):
        # Ensure we don't exceed server's maxWriteBatchSize size limit.
        await self.db.test.drop()
        n_docs = await async_client_context.max_write_batch_size + 100
        await self.db.test.insert_many([{} for _ in range(n_docs)])
        self.assertEqual(n_docs, await self.db.test.count_documents({}))
        await self.db.test.drop()

    async def test_insert_many_large_batch(self):
        # Tests legacy insert.
        db = self.client.test_insert_large_batch
        self.addAsyncCleanup(self.client.drop_database, "test_insert_large_batch")
        max_bson_size = await async_client_context.max_bson_size
        # Write commands are limited to 16MB + 16k per batch
        big_string = "x" * int(max_bson_size / 2)

        # Batch insert that requires 2 batches.
        successful_insert = [
            {"x": big_string},
            {"x": big_string},
            {"x": big_string},
            {"x": big_string},
        ]
        await db.collection_0.insert_many(successful_insert)
        self.assertEqual(4, await db.collection_0.count_documents({}))

        await db.collection_0.drop()

        # Test that inserts fail after first error.
        insert_second_fails = [
            {"_id": "id0", "x": big_string},
            {"_id": "id0", "x": big_string},
            {"_id": "id1", "x": big_string},
            {"_id": "id2", "x": big_string},
        ]

        with self.assertRaises(BulkWriteError):
            await db.collection_1.insert_many(insert_second_fails)

        self.assertEqual(1, await db.collection_1.count_documents({}))

        await db.collection_1.drop()

        # 2 batches, 2nd insert fails, unacknowledged, ordered.
        unack_coll = db.collection_2.with_options(write_concern=WriteConcern(w=0))
        await unack_coll.insert_many(insert_second_fails)

        async def async_lambda():
            return await db.collection_2.count_documents({}) == 1

        await async_wait_until(async_lambda, "insert 1 document", timeout=60)

        await db.collection_2.drop()

        # 2 batches, ids of docs 0 and 1 are dupes, ids of docs 2 and 3 are
        # dupes. Acknowledged, unordered.
        insert_two_failures = [
            {"_id": "id0", "x": big_string},
            {"_id": "id0", "x": big_string},
            {"_id": "id1", "x": big_string},
            {"_id": "id1", "x": big_string},
        ]

        with self.assertRaises(OperationFailure) as context:
            await db.collection_3.insert_many(insert_two_failures, ordered=False)

        self.assertIn("id1", str(context.exception))

        # Only the first and third documents should be inserted.
        self.assertEqual(2, await db.collection_3.count_documents({}))

        await db.collection_3.drop()

        # 2 batches, 2 errors, unacknowledged, unordered.
        unack_coll = db.collection_4.with_options(write_concern=WriteConcern(w=0))
        await unack_coll.insert_many(insert_two_failures, ordered=False)

        async def async_lambda():
            return await db.collection_4.count_documents({}) == 2

        # Only the first and third documents are inserted.
        await async_wait_until(async_lambda, "insert 2 documents", timeout=60)

        await db.collection_4.drop()

    async def test_messages_with_unicode_collection_names(self):
        db = self.db

        await db["Employés"].insert_one({"x": 1})
        await db["Employés"].replace_one({"x": 1}, {"x": 2})
        await db["Employés"].delete_many({})
        await db["Employés"].find_one()
        await db["Employés"].find().to_list()

    async def test_drop_indexes_non_existent(self):
        await self.db.drop_collection("test")
        await self.db.test.drop_indexes()

    # This is really a bson test but easier to just reproduce it here...
    # (Shame on me)
    async def test_bad_encode(self):
        c = self.db.test
        await c.drop()
        with self.assertRaises(InvalidDocument):
            await c.insert_one({"x": c})

        class BadGetAttr(dict):
            def __getattr__(self, name):
                pass

        bad = BadGetAttr([("foo", "bar")])
        await c.insert_one({"bad": bad})
        self.assertEqual("bar", (await c.find_one())["bad"]["foo"])  # type: ignore

    async def test_array_filters_validation(self):
        # array_filters must be a list.
        c = self.db.test
        with self.assertRaises(TypeError):
            await c.update_one({}, {"$set": {"a": 1}}, array_filters={})  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            await c.update_many({}, {"$set": {"a": 1}}, array_filters={})  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            update = {"$set": {"a": 1}}
            await c.find_one_and_update({}, update, array_filters={})  # type: ignore[arg-type]

    async def test_array_filters_unacknowledged(self):
        c_w0 = self.db.test.with_options(write_concern=WriteConcern(w=0))
        with self.assertRaises(ConfigurationError):
            await c_w0.update_one({}, {"$set": {"y.$[i].b": 5}}, array_filters=[{"i.b": 1}])
        with self.assertRaises(ConfigurationError):
            await c_w0.update_many({}, {"$set": {"y.$[i].b": 5}}, array_filters=[{"i.b": 1}])
        with self.assertRaises(ConfigurationError):
            await c_w0.find_one_and_update(
                {}, {"$set": {"y.$[i].b": 5}}, array_filters=[{"i.b": 1}]
            )

    async def test_find_one_and(self):
        c = self.db.test
        await c.drop()
        await c.insert_one({"_id": 1, "i": 1})

        self.assertEqual(
            {"_id": 1, "i": 1}, await c.find_one_and_update({"_id": 1}, {"$inc": {"i": 1}})
        )
        self.assertEqual(
            {"_id": 1, "i": 3},
            await c.find_one_and_update(
                {"_id": 1}, {"$inc": {"i": 1}}, return_document=ReturnDocument.AFTER
            ),
        )

        self.assertEqual({"_id": 1, "i": 3}, await c.find_one_and_delete({"_id": 1}))
        self.assertEqual(None, await c.find_one({"_id": 1}))

        self.assertEqual(None, await c.find_one_and_update({"_id": 1}, {"$inc": {"i": 1}}))
        self.assertEqual(
            {"_id": 1, "i": 1},
            await c.find_one_and_update(
                {"_id": 1}, {"$inc": {"i": 1}}, return_document=ReturnDocument.AFTER, upsert=True
            ),
        )
        self.assertEqual(
            {"_id": 1, "i": 2},
            await c.find_one_and_update(
                {"_id": 1}, {"$inc": {"i": 1}}, return_document=ReturnDocument.AFTER
            ),
        )

        self.assertEqual(
            {"_id": 1, "i": 3},
            await c.find_one_and_replace(
                {"_id": 1}, {"i": 3, "j": 1}, projection=["i"], return_document=ReturnDocument.AFTER
            ),
        )
        self.assertEqual(
            {"i": 4},
            await c.find_one_and_update(
                {"_id": 1},
                {"$inc": {"i": 1}},
                projection={"i": 1, "_id": 0},
                return_document=ReturnDocument.AFTER,
            ),
        )

        await c.drop()
        for j in range(5):
            await c.insert_one({"j": j, "i": 0})

        sort = [("j", DESCENDING)]
        self.assertEqual(4, (await c.find_one_and_update({}, {"$inc": {"i": 1}}, sort=sort))["j"])

    async def test_find_one_and_write_concern(self):
        listener = OvertCommandListener()
        db = (await self.async_single_client(event_listeners=[listener]))[self.db.name]
        # non-default WriteConcern.
        c_w0 = db.get_collection("test", write_concern=WriteConcern(w=0))
        # default WriteConcern.
        c_default = db.get_collection("test", write_concern=WriteConcern())
        # Authenticate the client and throw out auth commands from the listener.
        await db.command("ping")
        listener.reset()
        await c_w0.find_one_and_update({"_id": 1}, {"$set": {"foo": "bar"}})
        self.assertEqual({"w": 0}, listener.started_events[0].command["writeConcern"])
        listener.reset()

        await c_w0.find_one_and_replace({"_id": 1}, {"foo": "bar"})
        self.assertEqual({"w": 0}, listener.started_events[0].command["writeConcern"])
        listener.reset()

        await c_w0.find_one_and_delete({"_id": 1})
        self.assertEqual({"w": 0}, listener.started_events[0].command["writeConcern"])
        listener.reset()

        # Test write concern errors.
        if async_client_context.is_rs:
            c_wc_error = db.get_collection(
                "test", write_concern=WriteConcern(w=len(async_client_context.nodes) + 1)
            )
            with self.assertRaises(WriteConcernError):
                await c_wc_error.find_one_and_update({"_id": 1}, {"$set": {"foo": "bar"}})
            with self.assertRaises(WriteConcernError):
                await c_wc_error.find_one_and_replace(
                    {"w": 0}, listener.started_events[0].command["writeConcern"]
                )
            with self.assertRaises(WriteConcernError):
                await c_wc_error.find_one_and_delete(
                    {"w": 0}, listener.started_events[0].command["writeConcern"]
                )
            listener.reset()

        await c_default.find_one_and_update({"_id": 1}, {"$set": {"foo": "bar"}})
        self.assertNotIn("writeConcern", listener.started_events[0].command)
        listener.reset()

        await c_default.find_one_and_replace({"_id": 1}, {"foo": "bar"})
        self.assertNotIn("writeConcern", listener.started_events[0].command)
        listener.reset()

        await c_default.find_one_and_delete({"_id": 1})
        self.assertNotIn("writeConcern", listener.started_events[0].command)
        listener.reset()

    async def test_find_with_nested(self):
        c = self.db.test
        await c.drop()
        await c.insert_many([{"i": i} for i in range(5)])  # [0, 1, 2, 3, 4]
        self.assertEqual(
            [2],
            [
                i["i"]
                async for i in c.find(
                    {
                        "$and": [
                            {
                                # This clause gives us [1,2,4]
                                "$or": [
                                    {"i": {"$lte": 2}},
                                    {"i": {"$gt": 3}},
                                ],
                            },
                            {
                                # This clause gives us [2,3]
                                "$or": [
                                    {"i": 2},
                                    {"i": 3},
                                ]
                            },
                        ]
                    }
                )
            ],
        )

        self.assertEqual(
            [0, 1, 2],
            [
                i["i"]
                async for i in c.find(
                    {
                        "$or": [
                            {
                                # This clause gives us [2]
                                "$and": [
                                    {"i": {"$gte": 2}},
                                    {"i": {"$lt": 3}},
                                ],
                            },
                            {
                                # This clause gives us [0,1]
                                "$and": [
                                    {"i": {"$gt": -100}},
                                    {"i": {"$lt": 2}},
                                ]
                            },
                        ]
                    }
                )
            ],
        )

    async def test_find_regex(self):
        c = self.db.test
        await c.drop()
        await c.insert_one({"r": re.compile(".*")})

        self.assertIsInstance((await c.find_one())["r"], Regex)  # type: ignore
        async for doc in c.find():
            self.assertIsInstance(doc["r"], Regex)

    def test_find_command_generation(self):
        cmd = _gen_find_command(
            "coll",
            {"$query": {"foo": 1}, "$dumb": 2},
            None,
            0,
            0,
            0,
            None,
            DEFAULT_READ_CONCERN,
            None,
            None,
        )
        self.assertEqual(cmd, {"find": "coll", "$dumb": 2, "filter": {"foo": 1}})

    def test_bool(self):
        with self.assertRaises(NotImplementedError):
            bool(AsyncCollection(self.db, "test"))

    @async_client_context.require_version_min(5, 0, 0)
    async def test_helpers_with_let(self):
        c = self.db.test

        async def afind(*args, **kwargs):
            return c.find(*args, **kwargs)

        helpers = [
            (c.delete_many, ({}, {})),
            (c.delete_one, ({}, {})),
            (afind, ({})),
            (c.update_many, ({}, {"$inc": {"x": 3}})),
            (c.update_one, ({}, {"$inc": {"x": 3}})),
            (c.find_one_and_delete, ({}, {})),
            (c.find_one_and_replace, ({}, {})),
            (c.aggregate, ([],)),
        ]
        for let in [10, "str", [], False]:
            for helper, args in helpers:
                with self.assertRaisesRegex(TypeError, "let must be an instance of dict"):
                    await helper(*args, let=let)  # type: ignore
        for helper, args in helpers:
            await helper(*args, let={})  # type: ignore


if __name__ == "__main__":
    unittest.main()
