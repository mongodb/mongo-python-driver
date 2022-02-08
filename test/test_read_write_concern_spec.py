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

import json
import os
import sys
import warnings

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.utils import (
    EventListener,
    TestCreator,
    disable_replication,
    enable_replication,
    rs_or_single_client,
)
from test.utils_spec_runner import SpecRunner

from pymongo import DESCENDING
from pymongo.errors import (
    BulkWriteError,
    ConfigurationError,
    WriteConcernError,
    WriteError,
    WTimeoutError,
)
from pymongo.mongo_client import MongoClient
from pymongo.operations import IndexModel, InsertOne
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "read_write_concern")


class TestReadWriteConcernSpec(IntegrationTest):
    def test_omit_default_read_write_concern(self):
        listener = EventListener()
        # Client with default readConcern and writeConcern
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        collection = client.pymongo_test.collection
        # Prepare for tests of find() and aggregate().
        collection.insert_many([{} for _ in range(10)])
        self.addCleanup(collection.drop)
        self.addCleanup(client.pymongo_test.collection2.drop)
        # Commands MUST NOT send the default read/write concern to the server.

        def rename_and_drop():
            # Ensure collection exists.
            collection.insert_one({})
            collection.rename("collection2")
            client.pymongo_test.collection2.drop()

        def insert_command_default_write_concern():
            collection.database.command(
                "insert", "collection", documents=[{}], write_concern=WriteConcern()
            )

        ops = [
            ("aggregate", lambda: list(collection.aggregate([]))),
            ("find", lambda: list(collection.find())),
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
            listener.results.clear()
            f()

            self.assertGreaterEqual(len(listener.results["started"]), 1)
            for i, event in enumerate(listener.results["started"]):
                self.assertNotIn(
                    "readConcern",
                    event.command,
                    "%s sent default readConcern with %s" % (name, event.command_name),
                )
                self.assertNotIn(
                    "writeConcern",
                    event.command,
                    "%s sent default writeConcern with %s" % (name, event.command_name),
                )

    def assertWriteOpsRaise(self, write_concern, expected_exception):
        wc = write_concern.document
        # Set socket timeout to avoid indefinite stalls
        client = rs_or_single_client(w=wc["w"], wTimeoutMS=wc["wtimeout"], socketTimeoutMS=30000)
        db = client.get_database("pymongo_test")
        coll = db.test

        def insert_command():
            coll.database.command(
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
        if client_context.version[:2] != (3, 6):
            ops.append(("drop_database", lambda: client.drop_database(db)))

        for name, f in ops:
            # Ensure insert_many and bulk_write still raise BulkWriteError.
            if name in ("insert_many", "bulk_write"):
                expected = BulkWriteError
            else:
                expected = expected_exception
            with self.assertRaises(expected, msg=name) as cm:
                f()
            if expected == BulkWriteError:
                bulk_result = cm.exception.details
                assert bulk_result is not None
                wc_errors = bulk_result["writeConcernErrors"]
                self.assertTrue(wc_errors)

    @client_context.require_replica_set
    def test_raise_write_concern_error(self):
        self.addCleanup(client_context.client.drop_database, "pymongo_test")
        assert client_context.w is not None
        self.assertWriteOpsRaise(
            WriteConcern(w=client_context.w + 1, wtimeout=1), WriteConcernError
        )

    @client_context.require_secondaries_count(1)
    @client_context.require_test_commands
    def test_raise_wtimeout(self):
        self.addCleanup(client_context.client.drop_database, "pymongo_test")
        self.addCleanup(enable_replication, client_context.client)
        # Disable replication to guarantee a wtimeout error.
        disable_replication(client_context.client)
        self.assertWriteOpsRaise(WriteConcern(w=client_context.w, wtimeout=1), WTimeoutError)

    @client_context.require_failCommand_fail_point
    def test_error_includes_errInfo(self):
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
        with self.fail_point(cause_wce):
            # Write concern error on insert includes errInfo.
            with self.assertRaises(WriteConcernError) as ctx:
                self.db.test.insert_one({})
            self.assertEqual(ctx.exception.details, expected_wce)

            # Test bulk_write as well.
            with self.assertRaises(BulkWriteError) as ctx:
                self.db.test.bulk_write([InsertOne({})])
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

    @client_context.require_version_min(4, 9)
    def test_write_error_details_exposes_errinfo(self):
        listener = EventListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        db = client.errinfotest
        self.addCleanup(client.drop_database, "errinfotest")
        validator = {"x": {"$type": "string"}}
        db.create_collection("test", validator=validator)
        with self.assertRaises(WriteError) as ctx:
            db.test.insert_one({"x": 1})
        self.assertEqual(ctx.exception.code, 121)
        self.assertIsNotNone(ctx.exception.details)
        assert ctx.exception.details is not None
        self.assertIsNotNone(ctx.exception.details.get("errInfo"))
        for event in listener.results["succeeded"]:
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
                self.assertRaises((ConfigurationError, ValueError), MongoClient, uri, connect=False)
            else:
                with warnings.catch_warnings():
                    warnings.simplefilter("error", UserWarning)
                    self.assertRaises(UserWarning, MongoClient, uri, connect=False)
        else:
            client = MongoClient(uri, connect=False)
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
            # Any string for 'level' is equaly valid
            read_concern = ReadConcern(**test_case["readConcern"])
            self.assertEqual(read_concern.document, test_case["readConcernDocument"])
            self.assertEqual(not bool(read_concern.level), test_case["isServerDefault"])

    return run_test


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
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
                test_name = "test_%s_%s_%s" % (
                    dirname.replace("-", "_"),
                    fname.replace("-", "_"),
                    str(test_case["description"].lower().replace(" ", "_")),
                )

                new_test.__name__ = test_name
                setattr(TestReadWriteConcernSpec, new_test.__name__, new_test)


create_tests()


class TestOperation(SpecRunner):
    # Location of JSON test specifications.
    TEST_PATH = os.path.join(_TEST_PATH, "operation")

    def get_outcome_coll_name(self, outcome, collection):
        """Spec says outcome has an optional 'collection.name'."""
        return outcome["collection"].get("name", collection.name)


def create_operation_test(scenario_def, test, name):
    @client_context.require_test_commands
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


test_creator = TestCreator(create_operation_test, TestOperation, TestOperation.TEST_PATH)
test_creator.create_tests()


if __name__ == "__main__":
    unittest.main()
