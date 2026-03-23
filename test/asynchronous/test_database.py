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

"""Test the database module."""
from __future__ import annotations

import re
import sys
from typing import Any, Iterable, List, Mapping, Union

from pymongo.asynchronous.command_cursor import AsyncCommandCursor

sys.path[0:0] = [""]

from test import unittest
from test.asynchronous import AsyncIntegrationTest, async_client_context
from test.test_custom_types import DECIMAL_CODECOPTS
from test.utils_shared import (
    IMPOSSIBLE_WRITE_CONCERN,
    OvertCommandListener,
    async_wait_until,
)

from bson.codec_options import CodecOptions
from bson.dbref import DBRef
from bson.int64 import Int64
from bson.objectid import ObjectId
from bson.regex import Regex
from bson.son import SON
from pymongo import helpers_shared
from pymongo.asynchronous import auth
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.helpers import anext
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.errors import (
    CollectionInvalid,
    ExecutionTimeout,
    InvalidName,
    InvalidOperation,
    OperationFailure,
    WriteConcernError,
)
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

_IS_SYNC = False


class TestDatabaseNoConnect(unittest.TestCase):
    """Test Database features on a client that does not connect."""

    client: AsyncMongoClient

    @classmethod
    def setUpClass(cls):
        cls.client = AsyncMongoClient(connect=False)

    def test_name(self):
        self.assertRaises(TypeError, AsyncDatabase, self.client, 4)
        self.assertRaises(InvalidName, AsyncDatabase, self.client, "my db")
        self.assertRaises(InvalidName, AsyncDatabase, self.client, 'my"db')
        self.assertRaises(InvalidName, AsyncDatabase, self.client, "my\x00db")
        self.assertRaises(InvalidName, AsyncDatabase, self.client, "my\u0000db")
        self.assertEqual("name", AsyncDatabase(self.client, "name").name)

    def test_get_collection(self):
        codec_options = CodecOptions(tz_aware=True)
        write_concern = WriteConcern(w=2, j=True)
        read_concern = ReadConcern("majority")
        coll = self.client.pymongo_test.get_collection(
            "foo", codec_options, ReadPreference.SECONDARY, write_concern, read_concern
        )
        self.assertEqual("foo", coll.name)
        self.assertEqual(codec_options, coll.codec_options)
        self.assertEqual(ReadPreference.SECONDARY, coll.read_preference)
        self.assertEqual(write_concern, coll.write_concern)
        self.assertEqual(read_concern, coll.read_concern)

    def test_getattr(self):
        db = self.client.pymongo_test
        self.assertIsInstance(db["_does_not_exist"], AsyncCollection)

        with self.assertRaises(AttributeError) as context:
            db._does_not_exist

        # Message should be: "AttributeError: Database has no attribute
        # '_does_not_exist'. To access the _does_not_exist collection,
        # use database['_does_not_exist']".
        self.assertIn("has no attribute '_does_not_exist'", str(context.exception))

    def test_iteration(self):
        db = self.client.pymongo_test
        msg = "'AsyncDatabase' object is not iterable"
        # Iteration fails
        with self.assertRaisesRegex(TypeError, msg):
            for _ in db:  # type: ignore[misc] # error: "None" not callable  [misc]
                break
        # Index fails
        with self.assertRaises(TypeError):
            _ = db[0]
        # next fails
        with self.assertRaisesRegex(TypeError, "'AsyncDatabase' object is not iterable"):
            _ = next(db)
        # .next() fails
        with self.assertRaisesRegex(TypeError, "'AsyncDatabase' object is not iterable"):
            _ = db.next()
        # Do not implement typing.Iterable.
        self.assertNotIsInstance(db, Iterable)


class TestDatabase(AsyncIntegrationTest):
    def test_equality(self):
        self.assertNotEqual(AsyncDatabase(self.client, "test"), AsyncDatabase(self.client, "mike"))
        self.assertEqual(AsyncDatabase(self.client, "test"), AsyncDatabase(self.client, "test"))

        # Explicitly test inequality
        self.assertFalse(AsyncDatabase(self.client, "test") != AsyncDatabase(self.client, "test"))

    def test_hashable(self):
        self.assertIn(self.client.test, {AsyncDatabase(self.client, "test")})

    def test_get_coll(self):
        db = AsyncDatabase(self.client, "pymongo_test")
        self.assertEqual(db.test, db["test"])
        self.assertEqual(db.test, AsyncCollection(db, "test"))
        self.assertNotEqual(db.test, AsyncCollection(db, "mike"))
        self.assertEqual(db.test.mike, db["test.mike"])

    def test_repr(self):
        name = "AsyncDatabase"
        self.assertEqual(
            repr(AsyncDatabase(self.client, "pymongo_test")),
            "{}({!r}, {})".format(name, self.client, repr("pymongo_test")),
        )

    async def test_create_collection(self):
        db = AsyncDatabase(self.client, "pymongo_test")

        await db.test.insert_one({"hello": "world"})
        with self.assertRaises(CollectionInvalid):
            await db.create_collection("test")

        await db.drop_collection("test")

        with self.assertRaises(TypeError):
            await db.create_collection(5)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            await db.create_collection(None)  # type: ignore[arg-type]
        with self.assertRaises(InvalidName):
            await db.create_collection("coll..ection")  # type: ignore[arg-type]

        test = await db.create_collection("test")
        self.assertIn("test", await db.list_collection_names())
        await test.insert_one({"hello": "world"})
        self.assertEqual((await db.test.find_one())["hello"], "world")

        await db.drop_collection("test.foo")
        await db.create_collection("test.foo")
        self.assertIn("test.foo", await db.list_collection_names())
        with self.assertRaises(CollectionInvalid):
            await db.create_collection("test.foo")

    async def test_list_collection_names(self):
        db = AsyncDatabase(self.client, "pymongo_test")
        await db.test.insert_one({"dummy": "object"})
        await db.test.mike.insert_one({"dummy": "object"})

        colls = await db.list_collection_names()
        self.assertIn("test", colls)
        self.assertIn("test.mike", colls)
        for coll in colls:
            self.assertNotIn("$", coll)

        await db.systemcoll.test.insert_one({})
        no_system_collections = await db.list_collection_names(
            filter={"name": {"$regex": r"^(?!system\.)"}}
        )
        for coll in no_system_collections:
            self.assertFalse(coll.startswith("system."))
        self.assertIn("systemcoll.test", no_system_collections)

        # Force more than one batch.
        db = self.client.many_collections
        for i in range(101):
            await db["coll" + str(i)].insert_one({})
        # No Error
        try:
            await db.list_collection_names()
        finally:
            await self.client.drop_database("many_collections")

    async def test_list_collection_names_filter(self):
        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        db = client[self.db.name]
        await db.capped.drop()
        await db.create_collection("capped", capped=True, size=4096)
        await db.capped.insert_one({})
        await db.non_capped.insert_one({})
        self.addAsyncCleanup(client.drop_database, db.name)
        filter: Union[None, Mapping[str, Any]]
        # Should not send nameOnly.
        for filter in ({"options.capped": True}, {"options.capped": True, "name": "capped"}):
            listener.reset()
            names = await db.list_collection_names(filter=filter)
            self.assertEqual(names, ["capped"])
            self.assertNotIn("nameOnly", listener.started_events[0].command)

        # Should send nameOnly (except on 2.6).
        for filter in (None, {}, {"name": {"$in": ["capped", "non_capped"]}}):
            listener.reset()
            names = await db.list_collection_names(filter=filter)
            self.assertIn("capped", names)
            self.assertIn("non_capped", names)
            command = listener.started_events[0].command
            self.assertIn("nameOnly", command)
            self.assertTrue(command["nameOnly"])

    async def test_check_exists(self):
        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        db = client[self.db.name]
        await db.drop_collection("unique")
        await db.create_collection("unique", check_exists=True)
        self.assertIn("listCollections", listener.started_command_names())
        listener.reset()
        await db.drop_collection("unique")
        await db.create_collection("unique", check_exists=False)
        self.assertGreater(len(listener.started_events), 0)
        self.assertNotIn("listCollections", listener.started_command_names())

    async def test_list_collections(self):
        await self.client.drop_database("pymongo_test")
        db = AsyncDatabase(self.client, "pymongo_test")
        await db.test.insert_one({"dummy": "object"})
        await db.test.mike.insert_one({"dummy": "object"})

        results = await db.list_collections()
        colls = [result["name"] async for result in results]

        # All the collections present.
        self.assertIn("test", colls)
        self.assertIn("test.mike", colls)

        # No collection containing a '$'.
        for coll in colls:
            self.assertNotIn("$", coll)

        # Duplicate check.
        coll_cnt: dict = {}
        for coll in colls:
            try:
                # Found duplicate.
                coll_cnt[coll] += 1
                self.fail("Found duplicate")
            except KeyError:
                coll_cnt[coll] = 1
        coll_cnt: dict = {}

        # Check if there are any collections which don't exist.
        self.assertLessEqual(set(colls), {"test", "test.mike", "system.indexes"})

        colls = await (await db.list_collections(filter={"name": {"$regex": "^test$"}})).to_list()
        self.assertEqual(1, len(colls))

        colls = await (
            await db.list_collections(filter={"name": {"$regex": "^test.mike$"}})
        ).to_list()
        self.assertEqual(1, len(colls))

        await db.drop_collection("test")

        await db.create_collection("test", capped=True, size=4096)
        results = await db.list_collections(filter={"options.capped": True})
        colls = [result["name"] async for result in results]

        # Checking only capped collections are present
        self.assertIn("test", colls)
        self.assertNotIn("test.mike", colls)

        # No collection containing a '$'.
        for coll in colls:
            self.assertNotIn("$", coll)

        # Duplicate check.
        coll_cnt = {}
        for coll in colls:
            try:
                # Found duplicate.
                coll_cnt[coll] += 1
                self.fail("Found duplicate")
            except KeyError:
                coll_cnt[coll] = 1
        coll_cnt = {}

        # Check if there are any collections which don't exist.
        self.assertLessEqual(set(colls), {"test", "system.indexes"})

        await self.client.drop_database("pymongo_test")

    async def test_list_collection_names_single_socket(self):
        client = await self.async_rs_or_single_client(maxPoolSize=1)
        await client.drop_database("test_collection_names_single_socket")
        db = client.test_collection_names_single_socket
        for i in range(200):
            await db.create_collection(str(i))

        await db.list_collection_names()  # Must not hang.
        await client.drop_database("test_collection_names_single_socket")

    async def test_drop_collection(self):
        db = AsyncDatabase(self.client, "pymongo_test")

        with self.assertRaises(TypeError):
            await db.drop_collection(5)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            await db.drop_collection(None)  # type: ignore[arg-type]

        await db.test.insert_one({"dummy": "object"})
        self.assertIn("test", await db.list_collection_names())
        await db.drop_collection("test")
        self.assertNotIn("test", await db.list_collection_names())

        await db.test.insert_one({"dummy": "object"})
        self.assertIn("test", await db.list_collection_names())
        await db.drop_collection("test")
        self.assertNotIn("test", await db.list_collection_names())

        await db.test.insert_one({"dummy": "object"})
        self.assertIn("test", await db.list_collection_names())
        await db.drop_collection(db.test)
        self.assertNotIn("test", await db.list_collection_names())

        await db.test.insert_one({"dummy": "object"})
        self.assertIn("test", await db.list_collection_names())
        await db.test.drop()
        self.assertNotIn("test", await db.list_collection_names())
        await db.test.drop()

        await db.drop_collection(db.test.doesnotexist)

        if async_client_context.is_rs:
            db_wc = AsyncDatabase(
                self.client, "pymongo_test", write_concern=IMPOSSIBLE_WRITE_CONCERN
            )
            with self.assertRaises(WriteConcernError):
                await db_wc.drop_collection("test")

    async def test_validate_collection(self):
        db = self.client.pymongo_test

        with self.assertRaises(TypeError):
            await db.validate_collection(5)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            await db.validate_collection(None)  # type: ignore[arg-type]

        await db.test.insert_one({"dummy": "object"})

        with self.assertRaises(OperationFailure):
            await db.validate_collection("test.doesnotexist")
        with self.assertRaises(OperationFailure):
            await db.validate_collection(db.test.doesnotexist)

        self.assertTrue(await db.validate_collection("test"))
        self.assertTrue(await db.validate_collection(db.test))
        self.assertTrue(await db.validate_collection(db.test, full=True))
        self.assertTrue(await db.validate_collection(db.test, scandata=True))
        self.assertTrue(await db.validate_collection(db.test, scandata=True, full=True))
        self.assertTrue(await db.validate_collection(db.test, True, True))

    @async_client_context.require_version_min(4, 3, 3)
    @async_client_context.require_no_standalone
    async def test_validate_collection_background(self):
        db = self.client.pymongo_test.with_options(write_concern=WriteConcern(w="majority"))
        await db.test.insert_one({"dummy": "object"})
        coll = db.test
        self.assertTrue(await db.validate_collection(coll, background=False))
        # The inMemory storage engine does not support background=True.
        if async_client_context.storage_engine != "inMemory":
            # background=True requires the collection exist in a checkpoint.
            await self.client.admin.command("fsync")
            self.assertTrue(await db.validate_collection(coll, background=True))
            self.assertTrue(await db.validate_collection(coll, scandata=True, background=True))
            # The server does not support background=True with full=True.
            # Assert that we actually send the background option by checking
            # that this combination fails.
            with self.assertRaises(OperationFailure):
                await db.validate_collection(coll, full=True, background=True)

    async def test_command(self):
        self.maxDiff = None
        db = self.client.admin
        first = await db.command("buildinfo")
        second = await db.command({"buildinfo": 1})
        third = await db.command("buildinfo", 1)
        self.assertEqualReply(first, second)
        self.assertEqualReply(second, third)

    # We use 'aggregate' as our example command, since it's an easy way to
    # retrieve a BSON regex from a collection using a command.
    async def test_command_with_regex(self):
        db = self.client.pymongo_test
        await db.test.drop()
        await db.test.insert_one({"r": re.compile(".*")})
        await db.test.insert_one({"r": Regex(".*")})

        result = await db.command("aggregate", "test", pipeline=[], cursor={})
        for doc in result["cursor"]["firstBatch"]:
            self.assertIsInstance(doc["r"], Regex)

    async def test_command_bulkWrite(self):
        # Ensure bulk write commands can be run directly via db.command().
        if async_client_context.version.at_least(8, 0):
            await self.client.admin.command(
                {
                    "bulkWrite": 1,
                    "nsInfo": [{"ns": self.db.test.full_name}],
                    "ops": [{"insert": 0, "document": {}}],
                }
            )
        await self.db.command({"insert": "test", "documents": [{}]})
        await self.db.command({"update": "test", "updates": [{"q": {}, "u": {"$set": {"x": 1}}}]})
        await self.db.command({"delete": "test", "deletes": [{"q": {}, "limit": 1}]})
        await self.db.test.drop()

    async def test_cursor_command(self):
        db = self.client.pymongo_test
        await db.test.drop()

        docs = [{"_id": i, "doc": i} for i in range(3)]
        await db.test.insert_many(docs)

        cursor = await db.cursor_command("find", "test")

        self.assertIsInstance(cursor, AsyncCommandCursor)

        result_docs = await cursor.to_list()
        self.assertEqual(docs, result_docs)

    async def test_cursor_command_invalid(self):
        with self.assertRaises(InvalidOperation):
            await self.db.cursor_command("usersInfo", "test")

    @async_client_context.require_no_fips
    def test_password_digest(self):
        with self.assertRaises(TypeError):
            auth._password_digest(5)  # type: ignore[arg-type, call-arg]
        with self.assertRaises(TypeError):
            auth._password_digest(True)  # type: ignore[arg-type, call-arg]
        with self.assertRaises(TypeError):
            auth._password_digest(None)  # type: ignore[arg-type, call-arg]

        self.assertIsInstance(auth._password_digest("mike", "password"), str)
        self.assertEqual(
            auth._password_digest("mike", "password"), "cd7e45b3b2767dc2fa9b6b548457ed00"
        )
        self.assertEqual(
            auth._password_digest("Gustave", "Dor\xe9"), "81e0e2364499209f466e75926a162d73"
        )

    async def test_id_ordering(self):
        # PyMongo attempts to have _id show up first
        # when you iterate key/value pairs in a document.
        # This isn't reliable since python dicts don't
        # guarantee any particular order. This will never
        # work right in any Python or environment
        # with hash randomization enabled (e.g. tox).
        db = self.client.pymongo_test
        await db.test.drop()
        await db.test.insert_one(SON([("hello", "world"), ("_id", 5)]))

        db = self.client.get_database(
            "pymongo_test", codec_options=CodecOptions(document_class=SON[str, Any])
        )
        cursor = db.test.find()
        async for x in cursor:
            for k, _v in x.items():
                self.assertEqual(k, "_id")
                break

    async def test_deref(self):
        db = self.client.pymongo_test
        await db.test.drop()

        with self.assertRaises(TypeError):
            await db.dereference(5)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            await db.dereference("hello")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            await db.dereference(None)  # type: ignore[arg-type]

        self.assertEqual(None, await db.dereference(DBRef("test", ObjectId())))
        obj: dict[str, Any] = {"x": True}
        key = (await db.test.insert_one(obj)).inserted_id
        self.assertEqual(obj, await db.dereference(DBRef("test", key)))
        self.assertEqual(obj, await db.dereference(DBRef("test", key, "pymongo_test")))
        with self.assertRaises(ValueError):
            await db.dereference(DBRef("test", key, "foo"))

        self.assertEqual(None, await db.dereference(DBRef("test", 4)))
        obj = {"_id": 4}
        await db.test.insert_one(obj)
        self.assertEqual(obj, await db.dereference(DBRef("test", 4)))

    async def test_deref_kwargs(self):
        db = self.client.pymongo_test
        await db.test.drop()

        await db.test.insert_one({"_id": 4, "foo": "bar"})
        db = self.client.get_database(
            "pymongo_test", codec_options=CodecOptions(document_class=SON[str, Any])
        )
        self.assertEqual(
            SON([("foo", "bar")]), await db.dereference(DBRef("test", 4), projection={"_id": False})
        )

    # TODO some of these tests belong in the collection level testing.
    async def test_insert_find_one(self):
        db = self.client.pymongo_test
        await db.test.drop()

        a_doc = SON({"hello": "world"})
        a_key = (await db.test.insert_one(a_doc)).inserted_id
        self.assertIsInstance(a_doc["_id"], ObjectId)
        self.assertEqual(a_doc["_id"], a_key)
        self.assertEqual(a_doc, await db.test.find_one({"_id": a_doc["_id"]}))
        self.assertEqual(a_doc, await db.test.find_one(a_key))
        self.assertEqual(None, await db.test.find_one(ObjectId()))
        self.assertEqual(a_doc, await db.test.find_one({"hello": "world"}))
        self.assertEqual(None, await db.test.find_one({"hello": "test"}))

        b = await db.test.find_one()
        assert b is not None
        b["hello"] = "mike"
        await db.test.replace_one({"_id": b["_id"]}, b)

        self.assertNotEqual(a_doc, await db.test.find_one(a_key))
        self.assertEqual(b, await db.test.find_one(a_key))
        self.assertEqual(b, await db.test.find_one())

        count = 0
        async for _ in db.test.find():
            count += 1
        self.assertEqual(count, 1)

    async def test_long(self):
        db = self.client.pymongo_test
        await db.test.drop()
        await db.test.insert_one({"x": 9223372036854775807})
        retrieved = (await db.test.find_one())["x"]
        self.assertEqual(Int64(9223372036854775807), retrieved)
        self.assertIsInstance(retrieved, Int64)
        await db.test.delete_many({})
        await db.test.insert_one({"x": Int64(1)})
        retrieved = (await db.test.find_one())["x"]
        self.assertEqual(Int64(1), retrieved)
        self.assertIsInstance(retrieved, Int64)

    async def test_delete(self):
        db = self.client.pymongo_test
        await db.test.drop()

        await db.test.insert_one({"x": 1})
        await db.test.insert_one({"x": 2})
        await db.test.insert_one({"x": 3})
        length = 0
        async for _ in db.test.find():
            length += 1
        self.assertEqual(length, 3)

        await db.test.delete_one({"x": 1})
        length = 0
        async for _ in db.test.find():
            length += 1
        self.assertEqual(length, 2)

        await db.test.delete_one(await db.test.find_one())  # type: ignore[arg-type]
        await db.test.delete_one(await db.test.find_one())  # type: ignore[arg-type]
        self.assertEqual(await db.test.find_one(), None)

        await db.test.insert_one({"x": 1})
        await db.test.insert_one({"x": 2})
        await db.test.insert_one({"x": 3})

        self.assertTrue(await db.test.find_one({"x": 2}))
        await db.test.delete_one({"x": 2})
        self.assertFalse(await db.test.find_one({"x": 2}))

        self.assertTrue(await db.test.find_one())
        await db.test.delete_many({})
        self.assertFalse(await db.test.find_one())

    def test_command_response_without_ok(self):
        # Sometimes (SERVER-10891) the server's response to a badly-formatted
        # command document will have no 'ok' field. We should raise
        # OperationFailure instead of KeyError.
        with self.assertRaises(OperationFailure):
            helpers_shared._check_command_response({}, None)

        try:
            helpers_shared._check_command_response({"$err": "foo"}, None)
        except OperationFailure as e:
            self.assertEqual(e.args[0], "foo, full error: {'$err': 'foo'}")
        else:
            self.fail("_check_command_response didn't raise OperationFailure")

    def test_mongos_response(self):
        error_document = {
            "ok": 0,
            "errmsg": "outer",
            "raw": {"shard0/host0,host1": {"ok": 0, "errmsg": "inner"}},
        }

        with self.assertRaises(OperationFailure) as context:
            helpers_shared._check_command_response(error_document, None)

        self.assertIn("inner", str(context.exception))

        # If a shard has no primary and you run a command like dbstats, which
        # cannot be run on a secondary, mongos's response includes empty "raw"
        # errors. See SERVER-15428.
        error_document = {"ok": 0, "errmsg": "outer", "raw": {"shard0/host0,host1": {}}}

        with self.assertRaises(OperationFailure) as context:
            helpers_shared._check_command_response(error_document, None)

        self.assertIn("outer", str(context.exception))

        # Raw error has ok: 0 but no errmsg. Not a known case, but test it.
        error_document = {"ok": 0, "errmsg": "outer", "raw": {"shard0/host0,host1": {"ok": 0}}}

        with self.assertRaises(OperationFailure) as context:
            helpers_shared._check_command_response(error_document, None)

        self.assertIn("outer", str(context.exception))

    @async_client_context.require_test_commands
    @async_client_context.require_no_mongos
    async def test_command_max_time_ms(self):
        await self.client.admin.command(
            "configureFailPoint", "maxTimeAlwaysTimeOut", mode="alwaysOn"
        )
        try:
            db = self.client.pymongo_test
            await db.command("count", "test")
            with self.assertRaises(ExecutionTimeout):
                await db.command("count", "test", maxTimeMS=1)
            pipeline = [{"$project": {"name": 1, "count": 1}}]
            # Database command helper.
            await db.command("aggregate", "test", pipeline=pipeline, cursor={})
            with self.assertRaises(ExecutionTimeout):
                await db.command(
                    "aggregate",
                    "test",
                    pipeline=pipeline,
                    cursor={},
                    maxTimeMS=1,
                )
            # Collection helper.
            await db.test.aggregate(pipeline=pipeline)
            with self.assertRaises(ExecutionTimeout):
                await db.test.aggregate(pipeline, maxTimeMS=1)
        finally:
            await self.client.admin.command(
                "configureFailPoint", "maxTimeAlwaysTimeOut", mode="off"
            )

    def test_with_options(self):
        codec_options = DECIMAL_CODECOPTS
        read_preference = ReadPreference.SECONDARY_PREFERRED
        write_concern = WriteConcern(j=True)
        read_concern = ReadConcern(level="majority")

        # List of all options to compare.
        allopts = [
            "name",
            "client",
            "codec_options",
            "read_preference",
            "write_concern",
            "read_concern",
        ]

        db1 = self.client.get_database(
            "with_options_test",
            codec_options=codec_options,
            read_preference=read_preference,
            write_concern=write_concern,
            read_concern=read_concern,
        )

        # Case 1: swap no options
        db2 = db1.with_options()
        for opt in allopts:
            self.assertEqual(getattr(db1, opt), getattr(db2, opt))

        # Case 2: swap all options
        newopts = {
            "codec_options": CodecOptions(),
            "read_preference": ReadPreference.PRIMARY,
            "write_concern": WriteConcern(w=1),
            "read_concern": ReadConcern(level="local"),
        }
        db2 = db1.with_options(**newopts)  # type: ignore[arg-type, call-overload]
        for opt in newopts:
            self.assertEqual(getattr(db2, opt), newopts.get(opt, getattr(db1, opt)))


class TestDatabaseAggregation(AsyncIntegrationTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.pipeline: List[Mapping[str, Any]] = [
            {"$listLocalSessions": {}},
            {"$limit": 1},
            {"$addFields": {"dummy": "dummy field"}},
            {"$project": {"_id": 0, "dummy": 1}},
        ]
        self.result = {"dummy": "dummy field"}
        self.admin = self.client.admin

    async def test_database_aggregation(self):
        async with await self.admin.aggregate(self.pipeline) as cursor:
            result = await anext(cursor)
            self.assertEqual(result, self.result)

    @async_client_context.require_no_mongos
    async def test_database_aggregation_fake_cursor(self):
        coll_name = "test_output"
        write_stage: dict
        if async_client_context.version < (4, 3):
            db_name = "admin"
            write_stage = {"$out": coll_name}
        else:
            # SERVER-43287 disallows writing with $out to the admin db, use
            # $merge instead.
            db_name = "pymongo_test"
            write_stage = {"$merge": {"into": {"db": db_name, "coll": coll_name}}}
        output_coll = self.client[db_name][coll_name]
        await output_coll.drop()
        self.addAsyncCleanup(output_coll.drop)

        admin = self.admin.with_options(write_concern=WriteConcern(w=0))
        pipeline = self.pipeline[:]
        pipeline.append(write_stage)
        async with await admin.aggregate(pipeline) as cursor:
            with self.assertRaises(StopAsyncIteration):
                await anext(cursor)

        async def lambda_fn():
            return await output_coll.find_one()

        result = await async_wait_until(lambda_fn, "read unacknowledged write")
        self.assertEqual(result["dummy"], self.result["dummy"])

    def test_bool(self):
        with self.assertRaises(NotImplementedError):
            bool(AsyncDatabase(self.client, "test"))


if __name__ == "__main__":
    unittest.main()
