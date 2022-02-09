# Copyright 2014-present MongoDB, Inc.
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

"""Test the bulk API."""

import sys
import uuid

from pymongo.mongo_client import MongoClient

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.utils import (
    remove_all_users,
    rs_or_single_client_noauth,
    single_client,
    wait_until,
)

from bson.binary import Binary, UuidRepresentation
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.common import partition_node
from pymongo.errors import (
    BulkWriteError,
    ConfigurationError,
    InvalidOperation,
    OperationFailure,
)
from pymongo.operations import *
from pymongo.write_concern import WriteConcern


class BulkTestBase(IntegrationTest):
    coll: Collection
    coll_w0: Collection

    @classmethod
    def setUpClass(cls):
        super(BulkTestBase, cls).setUpClass()
        cls.coll = cls.db.test
        cls.coll_w0 = cls.coll.with_options(write_concern=WriteConcern(w=0))

    def setUp(self):
        super(BulkTestBase, self).setUp()
        self.coll.drop()

    def assertEqualResponse(self, expected, actual):
        """Compare response from bulk.execute() to expected response."""
        for key, value in expected.items():
            if key == "nModified":
                self.assertEqual(value, actual["nModified"])
            elif key == "upserted":
                expected_upserts = value
                actual_upserts = actual["upserted"]
                self.assertEqual(
                    len(expected_upserts),
                    len(actual_upserts),
                    'Expected %d elements in "upserted", got %d'
                    % (len(expected_upserts), len(actual_upserts)),
                )

                for e, a in zip(expected_upserts, actual_upserts):
                    self.assertEqualUpsert(e, a)

            elif key == "writeErrors":
                expected_errors = value
                actual_errors = actual["writeErrors"]
                self.assertEqual(
                    len(expected_errors),
                    len(actual_errors),
                    'Expected %d elements in "writeErrors", got %d'
                    % (len(expected_errors), len(actual_errors)),
                )

                for e, a in zip(expected_errors, actual_errors):
                    self.assertEqualWriteError(e, a)

            else:
                self.assertEqual(
                    actual.get(key),
                    value,
                    "%r value of %r does not match expected %r" % (key, actual.get(key), value),
                )

    def assertEqualUpsert(self, expected, actual):
        """Compare bulk.execute()['upserts'] to expected value.

        Like: {'index': 0, '_id': ObjectId()}
        """
        self.assertEqual(expected["index"], actual["index"])
        if expected["_id"] == "...":
            # Unspecified value.
            self.assertTrue("_id" in actual)
        else:
            self.assertEqual(expected["_id"], actual["_id"])

    def assertEqualWriteError(self, expected, actual):
        """Compare bulk.execute()['writeErrors'] to expected value.

        Like: {'index': 0, 'code': 123, 'errmsg': '...', 'op': { ... }}
        """
        self.assertEqual(expected["index"], actual["index"])
        self.assertEqual(expected["code"], actual["code"])
        if expected["errmsg"] == "...":
            # Unspecified value.
            self.assertTrue("errmsg" in actual)
        else:
            self.assertEqual(expected["errmsg"], actual["errmsg"])

        expected_op = expected["op"].copy()
        actual_op = actual["op"].copy()
        if expected_op.get("_id") == "...":
            # Unspecified _id.
            self.assertTrue("_id" in actual_op)
            actual_op.pop("_id")
            expected_op.pop("_id")

        self.assertEqual(expected_op, actual_op)


class TestBulk(BulkTestBase):
    def test_empty(self):
        self.assertRaises(InvalidOperation, self.coll.bulk_write, [])

    def test_insert(self):
        expected = {
            "nMatched": 0,
            "nModified": 0,
            "nUpserted": 0,
            "nInserted": 1,
            "nRemoved": 0,
            "upserted": [],
            "writeErrors": [],
            "writeConcernErrors": [],
        }

        result = self.coll.bulk_write([InsertOne({})])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(1, result.inserted_count)
        self.assertEqual(1, self.coll.count_documents({}))

    def _test_update_many(self, update):

        expected = {
            "nMatched": 2,
            "nModified": 2,
            "nUpserted": 0,
            "nInserted": 0,
            "nRemoved": 0,
            "upserted": [],
            "writeErrors": [],
            "writeConcernErrors": [],
        }
        self.coll.insert_many([{}, {}])

        result = self.coll.bulk_write([UpdateMany({}, update)])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(2, result.matched_count)
        self.assertTrue(result.modified_count in (2, None))

    def test_update_many(self):
        self._test_update_many({"$set": {"foo": "bar"}})

    @client_context.require_version_min(4, 1, 11)
    def test_update_many_pipeline(self):
        self._test_update_many([{"$set": {"foo": "bar"}}])

    def test_array_filters_validation(self):
        self.assertRaises(TypeError, UpdateMany, {}, {}, array_filters={})
        self.assertRaises(TypeError, UpdateOne, {}, {}, array_filters={})

    def test_array_filters_unacknowledged(self):
        coll = self.coll_w0
        update_one = UpdateOne({}, {"$set": {"y.$[i].b": 5}}, array_filters=[{"i.b": 1}])
        update_many = UpdateMany({}, {"$set": {"y.$[i].b": 5}}, array_filters=[{"i.b": 1}])
        self.assertRaises(ConfigurationError, coll.bulk_write, [update_one])
        self.assertRaises(ConfigurationError, coll.bulk_write, [update_many])

    def _test_update_one(self, update):
        expected = {
            "nMatched": 1,
            "nModified": 1,
            "nUpserted": 0,
            "nInserted": 0,
            "nRemoved": 0,
            "upserted": [],
            "writeErrors": [],
            "writeConcernErrors": [],
        }

        self.coll.insert_many([{}, {}])

        result = self.coll.bulk_write([UpdateOne({}, update)])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(1, result.matched_count)
        self.assertTrue(result.modified_count in (1, None))

    def test_update_one(self):
        self._test_update_one({"$set": {"foo": "bar"}})

    @client_context.require_version_min(4, 1, 11)
    def test_update_one_pipeline(self):
        self._test_update_one([{"$set": {"foo": "bar"}}])

    def test_replace_one(self):

        expected = {
            "nMatched": 1,
            "nModified": 1,
            "nUpserted": 0,
            "nInserted": 0,
            "nRemoved": 0,
            "upserted": [],
            "writeErrors": [],
            "writeConcernErrors": [],
        }

        self.coll.insert_many([{}, {}])

        result = self.coll.bulk_write([ReplaceOne({}, {"foo": "bar"})])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(1, result.matched_count)
        self.assertTrue(result.modified_count in (1, None))

    def test_remove(self):
        # Test removing all documents, ordered.
        expected = {
            "nMatched": 0,
            "nModified": 0,
            "nUpserted": 0,
            "nInserted": 0,
            "nRemoved": 2,
            "upserted": [],
            "writeErrors": [],
            "writeConcernErrors": [],
        }
        self.coll.insert_many([{}, {}])

        result = self.coll.bulk_write([DeleteMany({})])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(2, result.deleted_count)

    def test_remove_one(self):
        # Test removing one document, empty selector.
        self.coll.insert_many([{}, {}])
        expected = {
            "nMatched": 0,
            "nModified": 0,
            "nUpserted": 0,
            "nInserted": 0,
            "nRemoved": 1,
            "upserted": [],
            "writeErrors": [],
            "writeConcernErrors": [],
        }

        result = self.coll.bulk_write([DeleteOne({})])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(1, result.deleted_count)
        self.assertEqual(self.coll.count_documents({}), 1)

    def test_upsert(self):

        expected = {
            "nMatched": 0,
            "nModified": 0,
            "nUpserted": 1,
            "nInserted": 0,
            "nRemoved": 0,
            "upserted": [{"index": 0, "_id": "..."}],
        }

        result = self.coll.bulk_write([ReplaceOne({}, {"foo": "bar"}, upsert=True)])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(1, result.upserted_count)
        assert result.upserted_ids is not None
        self.assertEqual(1, len(result.upserted_ids))
        self.assertTrue(isinstance(result.upserted_ids.get(0), ObjectId))

        self.assertEqual(self.coll.count_documents({"foo": "bar"}), 1)

    def test_numerous_inserts(self):
        # Ensure we don't exceed server's maxWriteBatchSize size limit.
        n_docs = client_context.max_write_batch_size + 100
        requests = [InsertOne({}) for _ in range(n_docs)]
        result = self.coll.bulk_write(requests, ordered=False)
        self.assertEqual(n_docs, result.inserted_count)
        self.assertEqual(n_docs, self.coll.count_documents({}))

        # Same with ordered bulk.
        self.coll.drop()
        result = self.coll.bulk_write(requests)
        self.assertEqual(n_docs, result.inserted_count)
        self.assertEqual(n_docs, self.coll.count_documents({}))

    def test_bulk_max_message_size(self):
        self.coll.delete_many({})
        self.addCleanup(self.coll.delete_many, {})
        _16_MB = 16 * 1000 * 1000
        # Generate a list of documents such that the first batched OP_MSG is
        # as close as possible to the 48MB limit.
        docs = [
            {"_id": 1, "l": "s" * _16_MB},
            {"_id": 2, "l": "s" * _16_MB},
            {"_id": 3, "l": "s" * (_16_MB - 10000)},
        ]
        # Fill in the remaining ~10000 bytes with small documents.
        for i in range(4, 10000):
            docs.append({"_id": i})
        result = self.coll.insert_many(docs)
        self.assertEqual(len(docs), len(result.inserted_ids))

    def test_generator_insert(self):
        def gen():
            yield {"a": 1, "b": 1}
            yield {"a": 1, "b": 2}
            yield {"a": 2, "b": 3}
            yield {"a": 3, "b": 5}
            yield {"a": 5, "b": 8}

        result = self.coll.insert_many(gen())
        self.assertEqual(5, len(result.inserted_ids))

    def test_bulk_write_no_results(self):
        result = self.coll_w0.bulk_write([InsertOne({})])
        self.assertFalse(result.acknowledged)
        self.assertRaises(InvalidOperation, lambda: result.inserted_count)
        self.assertRaises(InvalidOperation, lambda: result.matched_count)
        self.assertRaises(InvalidOperation, lambda: result.modified_count)
        self.assertRaises(InvalidOperation, lambda: result.deleted_count)
        self.assertRaises(InvalidOperation, lambda: result.upserted_count)
        self.assertRaises(InvalidOperation, lambda: result.upserted_ids)

    def test_bulk_write_invalid_arguments(self):
        # The requests argument must be a list.
        generator = (InsertOne({}) for _ in range(10))
        with self.assertRaises(TypeError):
            self.coll.bulk_write(generator)  # type: ignore[arg-type]

        # Document is not wrapped in a bulk write operation.
        with self.assertRaises(TypeError):
            self.coll.bulk_write([{}])  # type: ignore[list-item]

    def test_upsert_large(self):
        big = "a" * (client_context.max_bson_size - 37)
        result = self.coll.bulk_write([UpdateOne({"x": 1}, {"$set": {"s": big}}, upsert=True)])
        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 1,
                "nInserted": 0,
                "nRemoved": 0,
                "upserted": [{"index": 0, "_id": "..."}],
            },
            result.bulk_api_result,
        )

        self.assertEqual(1, self.coll.count_documents({"x": 1}))

    def test_client_generated_upsert_id(self):
        result = self.coll.bulk_write(
            [
                UpdateOne({"_id": 0}, {"$set": {"a": 0}}, upsert=True),
                ReplaceOne({"a": 1}, {"_id": 1}, upsert=True),
                # This is just here to make the counts right in all cases.
                ReplaceOne({"_id": 2}, {"_id": 2}, upsert=True),
            ]
        )
        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 3,
                "nInserted": 0,
                "nRemoved": 0,
                "upserted": [
                    {"index": 0, "_id": 0},
                    {"index": 1, "_id": 1},
                    {"index": 2, "_id": 2},
                ],
            },
            result.bulk_api_result,
        )

    def test_upsert_uuid_standard(self):
        options = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
        coll = self.coll.with_options(codec_options=options)
        uuids = [uuid.uuid4() for _ in range(3)]
        result = coll.bulk_write(
            [
                UpdateOne({"_id": uuids[0]}, {"$set": {"a": 0}}, upsert=True),
                ReplaceOne({"a": 1}, {"_id": uuids[1]}, upsert=True),
                # This is just here to make the counts right in all cases.
                ReplaceOne({"_id": uuids[2]}, {"_id": uuids[2]}, upsert=True),
            ]
        )
        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 3,
                "nInserted": 0,
                "nRemoved": 0,
                "upserted": [
                    {"index": 0, "_id": uuids[0]},
                    {"index": 1, "_id": uuids[1]},
                    {"index": 2, "_id": uuids[2]},
                ],
            },
            result.bulk_api_result,
        )

    def test_upsert_uuid_unspecified(self):
        options = CodecOptions(uuid_representation=UuidRepresentation.UNSPECIFIED)
        coll = self.coll.with_options(codec_options=options)
        uuids = [Binary.from_uuid(uuid.uuid4()) for _ in range(3)]
        result = coll.bulk_write(
            [
                UpdateOne({"_id": uuids[0]}, {"$set": {"a": 0}}, upsert=True),
                ReplaceOne({"a": 1}, {"_id": uuids[1]}, upsert=True),
                # This is just here to make the counts right in all cases.
                ReplaceOne({"_id": uuids[2]}, {"_id": uuids[2]}, upsert=True),
            ]
        )
        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 3,
                "nInserted": 0,
                "nRemoved": 0,
                "upserted": [
                    {"index": 0, "_id": uuids[0]},
                    {"index": 1, "_id": uuids[1]},
                    {"index": 2, "_id": uuids[2]},
                ],
            },
            result.bulk_api_result,
        )

    def test_upsert_uuid_standard_subdocuments(self):
        options = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
        coll = self.coll.with_options(codec_options=options)
        ids: list = [{"f": Binary(bytes(i)), "f2": uuid.uuid4()} for i in range(3)]

        result = coll.bulk_write(
            [
                UpdateOne({"_id": ids[0]}, {"$set": {"a": 0}}, upsert=True),
                ReplaceOne({"a": 1}, {"_id": ids[1]}, upsert=True),
                # This is just here to make the counts right in all cases.
                ReplaceOne({"_id": ids[2]}, {"_id": ids[2]}, upsert=True),
            ]
        )

        # The `Binary` values are returned as `bytes` objects.
        for _id in ids:
            _id["f"] = bytes(_id["f"])

        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 3,
                "nInserted": 0,
                "nRemoved": 0,
                "upserted": [
                    {"index": 0, "_id": ids[0]},
                    {"index": 1, "_id": ids[1]},
                    {"index": 2, "_id": ids[2]},
                ],
            },
            result.bulk_api_result,
        )

    def test_single_ordered_batch(self):
        result = self.coll.bulk_write(
            [
                InsertOne({"a": 1}),
                UpdateOne({"a": 1}, {"$set": {"b": 1}}),
                UpdateOne({"a": 2}, {"$set": {"b": 2}}, upsert=True),
                InsertOne({"a": 3}),
                DeleteOne({"a": 3}),
            ]
        )
        self.assertEqualResponse(
            {
                "nMatched": 1,
                "nModified": 1,
                "nUpserted": 1,
                "nInserted": 2,
                "nRemoved": 1,
                "upserted": [{"index": 2, "_id": "..."}],
            },
            result.bulk_api_result,
        )

    def test_single_error_ordered_batch(self):
        self.coll.create_index("a", unique=True)
        self.addCleanup(self.coll.drop_index, [("a", 1)])
        requests: list = [
            InsertOne({"b": 1, "a": 1}),
            UpdateOne({"b": 2}, {"$set": {"a": 1}}, upsert=True),
            InsertOne({"b": 3, "a": 2}),
        ]
        try:
            self.coll.bulk_write(requests)
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 0,
                "nInserted": 1,
                "nRemoved": 0,
                "upserted": [],
                "writeConcernErrors": [],
                "writeErrors": [
                    {
                        "index": 1,
                        "code": 11000,
                        "errmsg": "...",
                        "op": {
                            "q": {"b": 2},
                            "u": {"$set": {"a": 1}},
                            "multi": False,
                            "upsert": True,
                        },
                    }
                ],
            },
            result,
        )

    def test_multiple_error_ordered_batch(self):
        self.coll.create_index("a", unique=True)
        self.addCleanup(self.coll.drop_index, [("a", 1)])
        requests: list = [
            InsertOne({"b": 1, "a": 1}),
            UpdateOne({"b": 2}, {"$set": {"a": 1}}, upsert=True),
            UpdateOne({"b": 3}, {"$set": {"a": 2}}, upsert=True),
            UpdateOne({"b": 2}, {"$set": {"a": 1}}, upsert=True),
            InsertOne({"b": 4, "a": 3}),
            InsertOne({"b": 5, "a": 1}),
        ]

        try:
            self.coll.bulk_write(requests)
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 0,
                "nInserted": 1,
                "nRemoved": 0,
                "upserted": [],
                "writeConcernErrors": [],
                "writeErrors": [
                    {
                        "index": 1,
                        "code": 11000,
                        "errmsg": "...",
                        "op": {
                            "q": {"b": 2},
                            "u": {"$set": {"a": 1}},
                            "multi": False,
                            "upsert": True,
                        },
                    }
                ],
            },
            result,
        )

    def test_single_unordered_batch(self):
        requests: list = [
            InsertOne({"a": 1}),
            UpdateOne({"a": 1}, {"$set": {"b": 1}}),
            UpdateOne({"a": 2}, {"$set": {"b": 2}}, upsert=True),
            InsertOne({"a": 3}),
            DeleteOne({"a": 3}),
        ]
        result = self.coll.bulk_write(requests, ordered=False)
        self.assertEqualResponse(
            {
                "nMatched": 1,
                "nModified": 1,
                "nUpserted": 1,
                "nInserted": 2,
                "nRemoved": 1,
                "upserted": [{"index": 2, "_id": "..."}],
                "writeErrors": [],
                "writeConcernErrors": [],
            },
            result.bulk_api_result,
        )

    def test_single_error_unordered_batch(self):
        self.coll.create_index("a", unique=True)
        self.addCleanup(self.coll.drop_index, [("a", 1)])
        requests: list = [
            InsertOne({"b": 1, "a": 1}),
            UpdateOne({"b": 2}, {"$set": {"a": 1}}, upsert=True),
            InsertOne({"b": 3, "a": 2}),
        ]

        try:
            self.coll.bulk_write(requests, ordered=False)
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 0,
                "nInserted": 2,
                "nRemoved": 0,
                "upserted": [],
                "writeConcernErrors": [],
                "writeErrors": [
                    {
                        "index": 1,
                        "code": 11000,
                        "errmsg": "...",
                        "op": {
                            "q": {"b": 2},
                            "u": {"$set": {"a": 1}},
                            "multi": False,
                            "upsert": True,
                        },
                    }
                ],
            },
            result,
        )

    def test_multiple_error_unordered_batch(self):
        self.coll.create_index("a", unique=True)
        self.addCleanup(self.coll.drop_index, [("a", 1)])
        requests: list = [
            InsertOne({"b": 1, "a": 1}),
            UpdateOne({"b": 2}, {"$set": {"a": 3}}, upsert=True),
            UpdateOne({"b": 3}, {"$set": {"a": 4}}, upsert=True),
            UpdateOne({"b": 4}, {"$set": {"a": 3}}, upsert=True),
            InsertOne({"b": 5, "a": 2}),
            InsertOne({"b": 6, "a": 1}),
        ]

        try:
            self.coll.bulk_write(requests, ordered=False)
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")
        # Assume the update at index 1 runs before the update at index 3,
        # although the spec does not require it. Same for inserts.
        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 2,
                "nInserted": 2,
                "nRemoved": 0,
                "upserted": [{"index": 1, "_id": "..."}, {"index": 2, "_id": "..."}],
                "writeConcernErrors": [],
                "writeErrors": [
                    {
                        "index": 3,
                        "code": 11000,
                        "errmsg": "...",
                        "op": {
                            "q": {"b": 4},
                            "u": {"$set": {"a": 3}},
                            "multi": False,
                            "upsert": True,
                        },
                    },
                    {
                        "index": 5,
                        "code": 11000,
                        "errmsg": "...",
                        "op": {"_id": "...", "b": 6, "a": 1},
                    },
                ],
            },
            result,
        )

    def test_large_inserts_ordered(self):
        big = "x" * client_context.max_bson_size
        requests = [
            InsertOne({"b": 1, "a": 1}),
            InsertOne({"big": big}),
            InsertOne({"b": 2, "a": 2}),
        ]

        try:
            self.coll.bulk_write(requests)
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqual(1, result["nInserted"])

        self.coll.delete_many({})

        big = "x" * (1024 * 1024 * 4)
        write_result = self.coll.bulk_write(
            [
                InsertOne({"a": 1, "big": big}),
                InsertOne({"a": 2, "big": big}),
                InsertOne({"a": 3, "big": big}),
                InsertOne({"a": 4, "big": big}),
                InsertOne({"a": 5, "big": big}),
                InsertOne({"a": 6, "big": big}),
            ]
        )

        self.assertEqual(6, write_result.inserted_count)
        self.assertEqual(6, self.coll.count_documents({}))

    def test_large_inserts_unordered(self):
        big = "x" * client_context.max_bson_size
        requests = [
            InsertOne({"b": 1, "a": 1}),
            InsertOne({"big": big}),
            InsertOne({"b": 2, "a": 2}),
        ]

        try:
            self.coll.bulk_write(requests, ordered=False)
        except BulkWriteError as exc:
            details = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqual(2, details["nInserted"])

        self.coll.delete_many({})

        big = "x" * (1024 * 1024 * 4)
        result = self.coll.bulk_write(
            [
                InsertOne({"a": 1, "big": big}),
                InsertOne({"a": 2, "big": big}),
                InsertOne({"a": 3, "big": big}),
                InsertOne({"a": 4, "big": big}),
                InsertOne({"a": 5, "big": big}),
                InsertOne({"a": 6, "big": big}),
            ],
            ordered=False,
        )

        self.assertEqual(6, result.inserted_count)
        self.assertEqual(6, self.coll.count_documents({}))


class BulkAuthorizationTestBase(BulkTestBase):
    @classmethod
    @client_context.require_auth
    @client_context.require_no_api_version
    def setUpClass(cls):
        super(BulkAuthorizationTestBase, cls).setUpClass()

    def setUp(self):
        super(BulkAuthorizationTestBase, self).setUp()
        client_context.create_user(self.db.name, "readonly", "pw", ["read"])
        self.db.command(
            "createRole",
            "noremove",
            privileges=[
                {
                    "actions": ["insert", "update", "find"],
                    "resource": {"db": "pymongo_test", "collection": "test"},
                }
            ],
            roles=[],
        )

        client_context.create_user(self.db.name, "noremove", "pw", ["noremove"])

    def tearDown(self):
        self.db.command("dropRole", "noremove")
        remove_all_users(self.db)


class TestBulkUnacknowledged(BulkTestBase):
    def tearDown(self):
        self.coll.delete_many({})

    def test_no_results_ordered_success(self):
        requests: list = [
            InsertOne({"a": 1}),
            UpdateOne({"a": 3}, {"$set": {"b": 1}}, upsert=True),
            InsertOne({"a": 2}),
            DeleteOne({"a": 1}),
        ]
        result = self.coll_w0.bulk_write(requests)
        self.assertFalse(result.acknowledged)
        wait_until(lambda: 2 == self.coll.count_documents({}), "insert 2 documents")
        wait_until(lambda: self.coll.find_one({"_id": 1}) is None, 'removed {"_id": 1}')

    def test_no_results_ordered_failure(self):
        requests: list = [
            InsertOne({"_id": 1}),
            UpdateOne({"_id": 3}, {"$set": {"b": 1}}, upsert=True),
            InsertOne({"_id": 2}),
            # Fails with duplicate key error.
            InsertOne({"_id": 1}),
            # Should not be executed since the batch is ordered.
            DeleteOne({"_id": 1}),
        ]
        result = self.coll_w0.bulk_write(requests)
        self.assertFalse(result.acknowledged)
        wait_until(lambda: 3 == self.coll.count_documents({}), "insert 3 documents")
        self.assertEqual({"_id": 1}, self.coll.find_one({"_id": 1}))

    def test_no_results_unordered_success(self):
        requests: list = [
            InsertOne({"a": 1}),
            UpdateOne({"a": 3}, {"$set": {"b": 1}}, upsert=True),
            InsertOne({"a": 2}),
            DeleteOne({"a": 1}),
        ]
        result = self.coll_w0.bulk_write(requests, ordered=False)
        self.assertFalse(result.acknowledged)
        wait_until(lambda: 2 == self.coll.count_documents({}), "insert 2 documents")
        wait_until(lambda: self.coll.find_one({"_id": 1}) is None, 'removed {"_id": 1}')

    def test_no_results_unordered_failure(self):
        requests: list = [
            InsertOne({"_id": 1}),
            UpdateOne({"_id": 3}, {"$set": {"b": 1}}, upsert=True),
            InsertOne({"_id": 2}),
            # Fails with duplicate key error.
            InsertOne({"_id": 1}),
            # Should be executed since the batch is unordered.
            DeleteOne({"_id": 1}),
        ]
        result = self.coll_w0.bulk_write(requests, ordered=False)
        self.assertFalse(result.acknowledged)
        wait_until(lambda: 2 == self.coll.count_documents({}), "insert 2 documents")
        wait_until(lambda: self.coll.find_one({"_id": 1}) is None, 'removed {"_id": 1}')


class TestBulkAuthorization(BulkAuthorizationTestBase):
    def test_readonly(self):
        # We test that an authorization failure aborts the batch and is raised
        # as OperationFailure.
        cli = rs_or_single_client_noauth(
            username="readonly", password="pw", authSource="pymongo_test"
        )
        coll = cli.pymongo_test.test
        coll.find_one()
        self.assertRaises(OperationFailure, coll.bulk_write, [InsertOne({"x": 1})])

    def test_no_remove(self):
        # We test that an authorization failure aborts the batch and is raised
        # as OperationFailure.
        cli = rs_or_single_client_noauth(
            username="noremove", password="pw", authSource="pymongo_test"
        )
        coll = cli.pymongo_test.test
        coll.find_one()
        requests = [
            InsertOne({"x": 1}),
            ReplaceOne({"x": 2}, {"x": 2}, upsert=True),
            DeleteMany({}),  # Prohibited.
            InsertOne({"x": 3}),  # Never attempted.
        ]
        self.assertRaises(OperationFailure, coll.bulk_write, requests)
        self.assertEqual(set([1, 2]), set(self.coll.distinct("x")))


class TestBulkWriteConcern(BulkTestBase):
    w: Optional[int]
    secondary: MongoClient

    @classmethod
    def setUpClass(cls):
        super(TestBulkWriteConcern, cls).setUpClass()
        cls.w = client_context.w
        cls.secondary = None
        if cls.w is not None and cls.w > 1:
            for member in client_context.hello["hosts"]:
                if member != client_context.hello["primary"]:
                    cls.secondary = single_client(*partition_node(member))
                    break

    @classmethod
    def tearDownClass(cls):
        if cls.secondary:
            cls.secondary.close()

    def cause_wtimeout(self, requests, ordered):
        if not client_context.test_commands_enabled:
            self.skipTest("Test commands must be enabled.")

        # Use the rsSyncApplyStop failpoint to pause replication on a
        # secondary which will cause a wtimeout error.
        self.secondary.admin.command("configureFailPoint", "rsSyncApplyStop", mode="alwaysOn")

        try:
            coll = self.coll.with_options(write_concern=WriteConcern(w=self.w, wtimeout=1))
            return coll.bulk_write(requests, ordered=ordered)
        finally:
            self.secondary.admin.command("configureFailPoint", "rsSyncApplyStop", mode="off")

    @client_context.require_replica_set
    @client_context.require_secondaries_count(1)
    def test_write_concern_failure_ordered(self):
        # Ensure we don't raise on wnote.
        coll_ww = self.coll.with_options(write_concern=WriteConcern(w=self.w))
        result = coll_ww.bulk_write([DeleteOne({"something": "that does no exist"})])
        self.assertTrue(result.acknowledged)

        requests = [InsertOne({"a": 1}), InsertOne({"a": 2})]
        # Replication wtimeout is a 'soft' error.
        # It shouldn't stop batch processing.
        try:
            self.cause_wtimeout(requests, ordered=True)
        except BulkWriteError as exc:
            details = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 0,
                "nInserted": 2,
                "nRemoved": 0,
                "upserted": [],
                "writeErrors": [],
            },
            details,
        )

        # When talking to legacy servers there will be a
        # write concern error for each operation.
        self.assertTrue(len(details["writeConcernErrors"]) > 0)

        failed = details["writeConcernErrors"][0]
        self.assertEqual(64, failed["code"])
        self.assertTrue(isinstance(failed["errmsg"], str))

        self.coll.delete_many({})
        self.coll.create_index("a", unique=True)
        self.addCleanup(self.coll.drop_index, [("a", 1)])

        # Fail due to write concern support as well
        # as duplicate key error on ordered batch.
        requests = [
            InsertOne({"a": 1}),
            ReplaceOne({"a": 3}, {"b": 1}, upsert=True),
            InsertOne({"a": 1}),
            InsertOne({"a": 2}),
        ]
        try:
            self.cause_wtimeout(requests, ordered=True)
        except BulkWriteError as exc:
            details = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 1,
                "nInserted": 1,
                "nRemoved": 0,
                "upserted": [{"index": 1, "_id": "..."}],
                "writeErrors": [
                    {"index": 2, "code": 11000, "errmsg": "...", "op": {"_id": "...", "a": 1}}
                ],
            },
            details,
        )

        self.assertTrue(len(details["writeConcernErrors"]) > 1)
        failed = details["writeErrors"][0]
        self.assertTrue("duplicate" in failed["errmsg"])

    @client_context.require_replica_set
    @client_context.require_secondaries_count(1)
    def test_write_concern_failure_unordered(self):
        # Ensure we don't raise on wnote.
        coll_ww = self.coll.with_options(write_concern=WriteConcern(w=self.w))
        result = coll_ww.bulk_write([DeleteOne({"something": "that does no exist"})], ordered=False)
        self.assertTrue(result.acknowledged)

        requests = [
            InsertOne({"a": 1}),
            UpdateOne({"a": 3}, {"$set": {"a": 3, "b": 1}}, upsert=True),
            InsertOne({"a": 2}),
        ]
        # Replication wtimeout is a 'soft' error.
        # It shouldn't stop batch processing.
        try:
            self.cause_wtimeout(requests, ordered=False)
        except BulkWriteError as exc:
            details = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqual(2, details["nInserted"])
        self.assertEqual(1, details["nUpserted"])
        self.assertEqual(0, len(details["writeErrors"]))
        # When talking to legacy servers there will be a
        # write concern error for each operation.
        self.assertTrue(len(details["writeConcernErrors"]) > 1)

        self.coll.delete_many({})
        self.coll.create_index("a", unique=True)
        self.addCleanup(self.coll.drop_index, [("a", 1)])

        # Fail due to write concern support as well
        # as duplicate key error on unordered batch.
        requests: list = [
            InsertOne({"a": 1}),
            UpdateOne({"a": 3}, {"$set": {"a": 3, "b": 1}}, upsert=True),
            InsertOne({"a": 1}),
            InsertOne({"a": 2}),
        ]
        try:
            self.cause_wtimeout(requests, ordered=False)
        except BulkWriteError as exc:
            details = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqual(2, details["nInserted"])
        self.assertEqual(1, details["nUpserted"])
        self.assertEqual(1, len(details["writeErrors"]))
        # When talking to legacy servers there will be a
        # write concern error for each operation.
        self.assertTrue(len(details["writeConcernErrors"]) > 1)

        failed = details["writeErrors"][0]
        self.assertEqual(2, failed["index"])
        self.assertEqual(11000, failed["code"])
        self.assertTrue(isinstance(failed["errmsg"], str))
        self.assertEqual(1, failed["op"]["a"])

        failed = details["writeConcernErrors"][0]
        self.assertEqual(64, failed["code"])
        self.assertTrue(isinstance(failed["errmsg"], str))

        upserts = details["upserted"]
        self.assertEqual(1, len(upserts))
        self.assertEqual(1, upserts[0]["index"])
        self.assertTrue(upserts[0].get("_id"))


if __name__ == "__main__":
    unittest.main()
