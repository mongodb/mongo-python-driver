# Copyright 2015-present MongoDB, Inc.
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

import json
import os
import re
import sys

sys.path[0:0] = [""]

from bson.py3compat import iteritems
from pymongo import operations, WriteConcern
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.results import _WriteResult, BulkWriteResult
from pymongo.operations import (InsertOne,
                                DeleteOne,
                                DeleteMany,
                                ReplaceOne,
                                UpdateOne,
                                UpdateMany)

from test import unittest, client_context, IntegrationTest
from test.utils import (camel_to_snake, camel_to_upper_camel,
                        camel_to_snake_args, drop_collections, TestCreator)

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'crud', 'v1')


class TestAllScenarios(IntegrationTest):
    pass


def check_result(self, expected_result, result):
    if isinstance(result, _WriteResult):
        for res in expected_result:
            prop = camel_to_snake(res)
            msg = "%s : %r != %r" % (prop, expected_result, result)
            # SPEC-869: Only BulkWriteResult has upserted_count.
            if (prop == "upserted_count"
                    and not isinstance(result, BulkWriteResult)):
                if result.upserted_id is not None:
                    upserted_count = 1
                else:
                    upserted_count = 0
                self.assertEqual(upserted_count, expected_result[res], msg)
            elif prop == "inserted_ids":
                # BulkWriteResult does not have inserted_ids.
                if isinstance(result, BulkWriteResult):
                    self.assertEqual(len(expected_result[res]),
                                     result.inserted_count)
                else:
                    # InsertManyResult may be compared to [id1] from the
                    # crud spec or {"0": id1} from the retryable write spec.
                    ids = expected_result[res]
                    if isinstance(ids, dict):
                        ids = [ids[str(i)] for i in range(len(ids))]
                    self.assertEqual(ids, result.inserted_ids, msg)
            elif prop == "upserted_ids":
                # Convert indexes from strings to integers.
                ids = expected_result[res]
                expected_ids = {}
                for str_index in ids:
                    expected_ids[int(str_index)] = ids[str_index]
                self.assertEqual(expected_ids, result.upserted_ids, msg)
            else:
                self.assertEqual(
                    getattr(result, prop), expected_result[res], msg)

    else:
        self.assertEqual(result, expected_result)


def run_operation(collection, test):
    # Convert command from CamelCase to pymongo.collection method.
    operation = camel_to_snake(test['operation']['name'])
    cmd = getattr(collection, operation)

    # Convert arguments to snake_case and handle special cases.
    arguments = test['operation']['arguments']
    options = arguments.pop("options", {})
    for option_name in options:
        arguments[camel_to_snake(option_name)] = options[option_name]
    if operation == "bulk_write":
        # Parse each request into a bulk write model.
        requests = []
        for request in arguments["requests"]:
            bulk_model = camel_to_upper_camel(request["name"])
            bulk_class = getattr(operations, bulk_model)
            bulk_arguments = camel_to_snake_args(request["arguments"])
            requests.append(bulk_class(**bulk_arguments))
        arguments["requests"] = requests
    else:
        for arg_name in list(arguments):
            c2s = camel_to_snake(arg_name)
            # PyMongo accepts sort as list of tuples.
            if arg_name == "sort":
                sort_dict = arguments[arg_name]
                arguments[arg_name] = list(iteritems(sort_dict))
            # Named "key" instead not fieldName.
            if arg_name == "fieldName":
                arguments["key"] = arguments.pop(arg_name)
            # Aggregate uses "batchSize", while find uses batch_size.
            elif arg_name == "batchSize" and operation == "aggregate":
                continue
            # Requires boolean returnDocument.
            elif arg_name == "returnDocument":
                arguments[c2s] = arguments.pop(arg_name) == "After"
            else:
                arguments[c2s] = arguments.pop(arg_name)

    result = cmd(**arguments)

    if operation == "aggregate":
        if arguments["pipeline"] and "$out" in arguments["pipeline"][-1]:
            out = collection.database[arguments["pipeline"][-1]["$out"]]
            result = out.find()

    if isinstance(result, Cursor) or isinstance(result, CommandCursor):
        return list(result)

    return result


def create_test(scenario_def, test, name):
    def run_scenario(self):
        # Cleanup state and load data (if provided).
        drop_collections(self.db)
        data = scenario_def.get('data')
        if data:
            self.db.test.with_options(
                write_concern=WriteConcern(w="majority")).insert_many(
                scenario_def['data'])

        # Run operations and check results or errors.
        expected_result = test.get('outcome', {}).get('result')
        expected_error = test.get('outcome', {}).get('error')
        if expected_error is True:
            with self.assertRaises(PyMongoError):
                run_operation(self.db.test, test)
        else:
            result = run_operation(self.db.test, test)
            check_result(self, expected_result, result)

        # Assert final state is expected.
        expected_c = test['outcome'].get('collection')
        if expected_c is not None:
            expected_name = expected_c.get('name')
            if expected_name is not None:
                db_coll = self.db[expected_name]
            else:
                db_coll = self.db.test
            db_coll = db_coll.with_options(
                read_concern=ReadConcern(level="local"))
            self.assertEqual(list(db_coll.find()), expected_c['data'])

    return run_scenario


test_creator = TestCreator(create_test, TestAllScenarios, _TEST_PATH)
test_creator.create_tests()


class TestWriteOpsComparison(unittest.TestCase):
    def test_InsertOneEquals(self):
        self.assertEqual(InsertOne({'foo': 42}), InsertOne({'foo': 42}))

    def test_InsertOneNotEquals(self):
        self.assertNotEqual(InsertOne({'foo': 42}), InsertOne({'foo': 23}))

    def test_DeleteOneEquals(self):
        self.assertEqual(DeleteOne({'foo': 42}), DeleteOne({'foo': 42}))

    def test_DeleteOneNotEquals(self):
        self.assertNotEqual(DeleteOne({'foo': 42}), DeleteOne({'foo': 23}))

    def test_DeleteManyEquals(self):
        self.assertEqual(DeleteMany({'foo': 42}), DeleteMany({'foo': 42}))

    def test_DeleteManyNotEquals(self):
        self.assertNotEqual(DeleteMany({'foo': 42}), DeleteMany({'foo': 23}))

    def test_DeleteOneNotEqualsDeleteMany(self):
        self.assertNotEqual(DeleteOne({'foo': 42}), DeleteMany({'foo': 42}))

    def test_ReplaceOneEquals(self):
        self.assertEqual(ReplaceOne({'foo': 42}, {'bar': 42}, upsert=False),
                         ReplaceOne({'foo': 42}, {'bar': 42}, upsert=False))

    def test_ReplaceOneNotEquals(self):
        self.assertNotEqual(ReplaceOne({'foo': 42}, {'bar': 42}, upsert=False),
                            ReplaceOne({'foo': 42}, {'bar': 42}, upsert=True))

    def test_UpdateOneEquals(self):
        self.assertEqual(UpdateOne({'foo': 42}, {'$set': {'bar': 42}}),
                         UpdateOne({'foo': 42}, {'$set': {'bar': 42}}))

    def test_UpdateOneNotEquals(self):
        self.assertNotEqual(UpdateOne({'foo': 42}, {'$set': {'bar': 42}}),
                            UpdateOne({'foo': 42}, {'$set': {'bar': 23}}))

    def test_UpdateManyEquals(self):
        self.assertEqual(UpdateMany({'foo': 42}, {'$set': {'bar': 42}}),
                         UpdateMany({'foo': 42}, {'$set': {'bar': 42}}))

    def test_UpdateManyNotEquals(self):
        self.assertNotEqual(UpdateMany({'foo': 42}, {'$set': {'bar': 42}}),
                            UpdateMany({'foo': 42}, {'$set': {'bar': 23}}))

    def test_UpdateOneNotEqualsUpdateMany(self):
        self.assertNotEqual(UpdateOne({'foo': 42}, {'$set': {'bar': 42}}),
                            UpdateMany({'foo': 42}, {'$set': {'bar': 42}}))

if __name__ == "__main__":
    unittest.main()
