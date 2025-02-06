# Copyright 2018-present MongoDB, Inc.
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

"""Run the read and write concern tests."""
from __future__ import annotations

import json
import os
import sys
import warnings
from pathlib import Path

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest
from test.asynchronous.unified_format import generate_test_classes
from test.utils import OvertCommandListener

from pymongo import DESCENDING
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.errors import (
    BulkWriteError,
    ConfigurationError,
    WriteConcernError,
    WriteError,
    WTimeoutError,
)
from pymongo.operations import IndexModel, InsertOne
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

_IS_SYNC = False

# Location of JSON test specifications.
if _IS_SYNC:
    TEST_PATH = os.path.join(Path(__file__).resolve().parent, "read_write_concern")
else:
    TEST_PATH = os.path.join(Path(__file__).resolve().parent.parent, "read_write_concern")


class TestReadWriteConcernSpec(AsyncIntegrationTest):
    async def test_omit_default_read_write_concern(self):
        listener = OvertCommandListener()
        # Client with default readConcern and writeConcern
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        collection = client.pymongo_test.collection
        # Prepare for tests of find() and aggregate().
        await collection.insert_many([{} for _ in range(10)])
        self.addAsyncCleanup(collection.drop)
        self.addAsyncCleanup(client.pymongo_test.collection2.drop)
        # Commands MUST NOT send the default read/write concern to the server.

        async def rename_and_drop():
            # Ensure collection exists.
            await collection.insert_one({})
            await collection.rename("collection2")
            await client.pymongo_test.collection2.drop()

        async def insert_command_default_write_concern():
            await collection.database.command(
                "insert", "collection", documents=[{}], write_concern=WriteConcern()
            )

        async def aggregate_op():
            await (await collection.aggregate([])).to_list()

        ops = [
            ("aggregate", aggregate_op),
            ("find", lambda: collection.find().to_list()),
            ("insert_one", lambda: collection.insert_one({})),
            ("update_one", lambda: collection.update_one({}, {"$set": {"x": 1}})),
            ("update_many", lambda: collection.update_many({}, {"$set": {"x": 1}})),
            ("delete_one", lambda: collection.delete_one({})),
            ("delete_many", lambda: collection.delete_many({})),
            ("bulk_write", lambda: collection.bulk_write([InsertOne({})])),
            ("rename_and_drop", rename_and_drop),
            ("command", insert_command_default_write_concern),
        ]

        for name, f in ops:
            listener.reset()
            await f()

            self.assertGreaterEqual(len(listener.started_events), 1)
            for _i, event in enumerate(listener.started_events):
                self.assertNotIn(
                    "readConcern",
                    event.command,
                    f"{name} sent default readConcern with {event.command_name}",
                )
                self.assertNotIn(
                    "writeConcern",
                    event.command,
                    f"{name} sent default writeConcern with {event.command_name}",
                )

    async def assertWriteOpsRaise(self, write_concern, expected_exception):
        wc = write_concern.document
        # Set socket timeout to avoid indefinite stalls
        client = await self.async_rs_or_single_client(
            w=wc["w"], wTimeoutMS=wc["wtimeout"], socketTimeoutMS=30000
        )
        db = client.get_database("pymongo_test")
        coll = db.test

        async def insert_command():
            await coll.database.command(
                "insert",
                "new_collection",
                documents=[{}],
                writeConcern=write_concern.document,
                parse_write_concern_error=True,
            )

        ops = [
            ("insert_one", lambda: coll.insert_one({})),
            ("insert_many", lambda: coll.insert_many([{}, {}])),
            ("update_one", lambda: coll.update_one({}, {"$set": {"x": 1}})),
            ("update_many", lambda: coll.update_many({}, {"$set": {"x": 1}})),
            ("delete_one", lambda: coll.delete_one({})),
            ("delete_many", lambda: coll.delete_many({})),
            ("bulk_write", lambda: coll.bulk_write([InsertOne({})])),
            ("command", insert_command),
            ("aggregate", lambda: coll.aggregate([{"$out": "out"}])),
            # SERVER-46668 Delete all the documents in the collection to
            # workaround a hang in createIndexes.
            ("delete_many", lambda: coll.delete_many({})),
            ("create_index", lambda: coll.create_index([("a", DESCENDING)])),
            ("create_indexes", lambda: coll.create_indexes([IndexModel("b")])),
            ("drop_index", lambda: coll.drop_index([("a", DESCENDING)])),
            ("create", lambda: db.create_collection("new")),
            ("rename", lambda: coll.rename("new")),
            ("drop", lambda: db.new.drop()),
        ]
        # SERVER-47194: dropDatabase does not respect wtimeout in 3.6.
        if async_client_context.version[:2] != (3, 6):
            ops.append(("drop_database", lambda: client.drop_database(db)))

        for name, f in ops:
            # Ensure insert_many and bulk_write still raise BulkWriteError.
            if name in ("insert_many", "bulk_write"):
                expected = BulkWriteError
            else:
                expected = expected_exception
            with self.assertRaises(expected, msg=name) as cm:
                await f()
            if expected == BulkWriteError:
                bulk_result = cm.exception.details
                assert bulk_result is not None
                wc_errors = bulk_result["writeConcernErrors"]
                self.assertTrue(wc_errors)

    @async_client_context.require_replica_set
    async def test_raise_write_concern_error(self):
        self.addAsyncCleanup(async_client_context.client.drop_database, "pymongo_test")
        assert async_client_context.w is not None
        await self.assertWriteOpsRaise(
            WriteConcern(w=async_client_context.w + 1, wtimeout=1), WriteConcernError
        )

    @async_client_context.require_secondaries_count(1)
    @async_client_context.require_test_commands
    async def test_raise_wtimeout(self):
        self.addAsyncCleanup(async_client_context.client.drop_database, "pymongo_test")
        self.addAsyncCleanup(self.enable_replication, async_client_context.client)
        # Disable replication to guarantee a wtimeout error.
        await self.disable_replication(async_client_context.client)
        await self.assertWriteOpsRaise(
            WriteConcern(w=async_client_context.w, wtimeout=1), WTimeoutError
        )

    @async_client_context.require_failCommand_fail_point
    async def test_error_includes_errInfo(self):
        expected_wce = {
            "code": 100,
            "codeName": "UnsatisfiableWriteConcern",
            "errmsg": "Not enough data-bearing nodes",
            "errInfo": {"writeConcern": {"w": 2, "wtimeout": 0, "provenance": "clientSupplied"}},
        }
        cause_wce = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 2},
            "data": {"failCommands": ["insert"], "writeConcernError": expected_wce},
        }
        async with self.fail_point(cause_wce):
            # Write concern error on insert includes errInfo.
            with self.assertRaises(WriteConcernError) as ctx:
                await self.db.test.insert_one({})
            self.assertEqual(ctx.exception.details, expected_wce)

            # Test bulk_write as well.
            with self.assertRaises(BulkWriteError) as ctx:
                await self.db.test.bulk_write([InsertOne({})])
            expected_details = {
                "writeErrors": [],
                "writeConcernErrors": [expected_wce],
                "nInserted": 1,
                "nUpserted": 0,
                "nMatched": 0,
                "nModified": 0,
                "nRemoved": 0,
                "upserted": [],
            }
            self.assertEqual(ctx.exception.details, expected_details)

    @async_client_context.require_version_min(4, 9)
    async def test_write_error_details_exposes_errinfo(self):
        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(event_listeners=[listener])
        db = client.errinfotest
        self.addAsyncCleanup(client.drop_database, "errinfotest")
        validator = {"x": {"$type": "string"}}
        await db.create_collection("test", validator=validator)
        with self.assertRaises(WriteError) as ctx:
            await db.test.insert_one({"x": 1})
        self.assertEqual(ctx.exception.code, 121)
        self.assertIsNotNone(ctx.exception.details)
        assert ctx.exception.details is not None
        self.assertIsNotNone(ctx.exception.details.get("errInfo"))
        for event in listener.succeeded_events:
            if event.command_name == "insert":
                self.assertEqual(event.reply["writeErrors"][0], ctx.exception.details)
                break
        else:
            self.fail("Couldn't find insert event.")


def normalize_write_concern(concern):
    result = {}
    for key in concern:
        if key.lower() == "wtimeoutms":
            result["wtimeout"] = concern[key]
        elif key == "journal":
            result["j"] = concern[key]
        else:
            result[key] = concern[key]
    return result


def create_connection_string_test(test_case):
    def run_test(self):
        uri = test_case["uri"]
        valid = test_case["valid"]
        warning = test_case["warning"]

        if not valid:
            if warning is False:
                self.assertRaises(
                    (ConfigurationError, ValueError), AsyncMongoClient, uri, connect=False
                )
            else:
                with warnings.catch_warnings():
                    warnings.simplefilter("error", UserWarning)
                    self.assertRaises(UserWarning, AsyncMongoClient, uri, connect=False)
        else:
            client = AsyncMongoClient(uri, connect=False)
            if "writeConcern" in test_case:
                document = client.write_concern.document
                self.assertEqual(document, normalize_write_concern(test_case["writeConcern"]))
            if "readConcern" in test_case:
                document = client.read_concern.document
                self.assertEqual(document, test_case["readConcern"])

    return run_test


def create_document_test(test_case):
    def run_test(self):
        valid = test_case["valid"]

        if "writeConcern" in test_case:
            normalized = normalize_write_concern(test_case["writeConcern"])
            if not valid:
                self.assertRaises((ConfigurationError, ValueError), WriteConcern, **normalized)
            else:
                write_concern = WriteConcern(**normalized)
                self.assertEqual(write_concern.document, test_case["writeConcernDocument"])
                self.assertEqual(write_concern.acknowledged, test_case["isAcknowledged"])
                self.assertEqual(write_concern.is_server_default, test_case["isServerDefault"])
        if "readConcern" in test_case:
            # Any string for 'level' is equally valid
            read_concern = ReadConcern(**test_case["readConcern"])
            self.assertEqual(read_concern.document, test_case["readConcernDocument"])
            self.assertEqual(not bool(read_concern.level), test_case["isServerDefault"])

    return run_test


def create_tests():
    for dirpath, _, filenames in os.walk(TEST_PATH):
        dirname = os.path.split(dirpath)[-1]

        if dirname == "operation":
            # This directory is tested by TestOperations.
            continue
        elif dirname == "connection-string":
            create_test = create_connection_string_test
        else:
            create_test = create_document_test

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as test_stream:
                test_cases = json.load(test_stream)["tests"]

            fname = os.path.splitext(filename)[0]
            for test_case in test_cases:
                new_test = create_test(test_case)
                test_name = "test_{}_{}_{}".format(
                    dirname.replace("-", "_"),
                    fname.replace("-", "_"),
                    str(test_case["description"].lower().replace(" ", "_")),
                )

                new_test.__name__ = test_name
                setattr(TestReadWriteConcernSpec, new_test.__name__, new_test)


create_tests()


# Generate unified tests.
# PyMongo does not support MapReduce.
globals().update(
    generate_test_classes(
        os.path.join(TEST_PATH, "operation"),
        module=__name__,
        expected_failures=["MapReduce .*"],
    )
)


if __name__ == "__main__":
    unittest.main()
