# Copyright 2019-present MongoDB, Inc.
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
import sys

sys.path[0:0] = [""]

from bson.py3compat import iteritems
from pymongo import operations
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.errors import PyMongoError
from pymongo.results import _WriteResult, BulkWriteResult

from test import unittest, client_context, IntegrationTest
from test.utils import (camel_to_snake, camel_to_upper_camel,
                        camel_to_snake_args, drop_collections,
                        parse_collection_options, rs_client,
                        OvertCommandListener, TestCreator)

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'crud', 'v2')

# Default test database and collection names.
TEST_DB = 'testdb'
TEST_COLLECTION = 'testcollection'


class TestAllScenarios(IntegrationTest):
    def run_operation(self, collection, test):
        # Iterate over all operations.
        for opdef in test['operations']:
            # Convert command from CamelCase to pymongo.collection method.
            operation = camel_to_snake(opdef['name'])

            # Get command handle on target entity (collection/database).
            target_object = opdef.get('object', 'collection')
            if target_object == 'database':
                cmd = getattr(collection.database, operation)
            elif target_object == 'collection':
                collection = collection.with_options(**dict(
                    parse_collection_options(opdef.get(
                        'collectionOptions', {}))))
                cmd = getattr(collection, operation)
            else:
                self.fail("Unknown object name %s" % (target_object,))

            # Convert arguments to snake_case and handle special cases.
            arguments = opdef['arguments']
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
                        arguments[c2s] = arguments[arg_name] == "After"
                    else:
                        arguments[c2s] = arguments.pop(arg_name)

            if opdef.get('error') is True:
                with self.assertRaises(PyMongoError):
                    cmd(**arguments)
            else:
                result = cmd(**arguments)
                self.check_result(opdef.get('result'), result)

    def check_result(self, expected_result, result):
        if expected_result is None:
            return True

        if isinstance(result, Cursor) or isinstance(result, CommandCursor):
            return list(result) == expected_result

        elif isinstance(result, _WriteResult):
            for res in expected_result:
                prop = camel_to_snake(res)
                # SPEC-869: Only BulkWriteResult has upserted_count.
                if (prop == "upserted_count" and
                        not isinstance(result, BulkWriteResult)):
                    if result.upserted_id is not None:
                        upserted_count = 1
                    else:
                        upserted_count = 0
                    if upserted_count != expected_result[res]:
                        return False
                elif prop == "inserted_ids":
                    # BulkWriteResult does not have inserted_ids.
                    if isinstance(result, BulkWriteResult):
                        if len(expected_result[res]) != result.inserted_count:
                            return False
                    else:
                        # InsertManyResult may be compared to [id1] from the
                        # crud spec or {"0": id1} from the retryable write spec.
                        ids = expected_result[res]
                        if isinstance(ids, dict):
                            ids = [ids[str(i)] for i in range(len(ids))]
                        if ids != result.inserted_ids:
                            return False
                elif prop == "upserted_ids":
                    # Convert indexes from strings to integers.
                    ids = expected_result[res]
                    expected_ids = {}
                    for str_index in ids:
                        expected_ids[int(str_index)] = ids[str_index]
                    if expected_ids != result.upserted_ids:
                        return False
                elif getattr(result, prop) != expected_result[res]:
                    return False
            return True
        else:
            if not expected_result:
                return result is None
            else:
                return result == expected_result

    def check_events(self, expected_events, listener):
        res = listener.results
        if not len(expected_events):
            return

        # Expectations only have CommandStartedEvents.
        self.assertEqual(len(res['started']), len(expected_events))
        for i, expectation in enumerate(expected_events):
            event_type = next(iter(expectation))
            event = res['started'][i]

            # The tests substitute 42 for any number other than 0.
            if (event.command_name == 'getMore'
                    and event.command['getMore']):
                event.command['getMore'] = 42
            elif event.command_name == 'killCursors':
                event.command['cursors'] = [42]
            # Add upsert and multi fields back into expectations.
            elif event.command_name == 'update':
                updates = expectation[event_type]['command']['updates']
                for update in updates:
                    update.setdefault('upsert', False)
                    update.setdefault('multi', False)

            # Replace afterClusterTime: 42 with actual afterClusterTime.
            expected_cmd = expectation[event_type]['command']
            expected_read_concern = expected_cmd.get('readConcern')
            if expected_read_concern is not None:
                time = expected_read_concern.get('afterClusterTime')
                if time == 42:
                    actual_time = event.command.get(
                        'readConcern', {}).get('afterClusterTime')
                    if actual_time is not None:
                        expected_read_concern['afterClusterTime'] = actual_time

            for attr, expected in expectation[event_type].items():
                actual = getattr(event, attr)
                if isinstance(expected, dict):
                    for key, val in expected.items():
                        if val is None:
                            if key in actual:
                                self.fail("Unexpected key [%s] in %r" % (
                                    key, actual))
                        elif key not in actual:
                            self.fail("Expected key [%s] in %r" % (
                                key, actual))
                        else:
                            self.assertEqual(val, actual[key],
                                             "Key [%s] in %s" % (key, actual))
                else:
                    self.assertEqual(actual, expected)


def create_test(scenario_def, test, name):
    def run_scenario(self):
        listener = OvertCommandListener()
        # New client, to avoid interference from pooled sessions.
        # Convert test['clientOptions'] to dict to avoid a Jython bug using "**"
        # with ScenarioDict.
        client = rs_client(event_listeners=[listener],
                           **dict(test.get('clientOptions', {})))
        # Close the client explicitly to avoid having too many threads open.
        self.addCleanup(client.close)

        # Get database and collection objects.
        database = getattr(
            client, scenario_def.get('database_name', TEST_DB))
        drop_collections(database)
        collection = getattr(
            database, scenario_def.get('collection_name', TEST_COLLECTION))

        # Populate collection with data and run test.
        collection.insert_many(scenario_def.get('data', []))
        listener.results.clear()
        self.run_operation(collection, test)

        # Assert expected events.
        self.check_events(test.get('expectations', {}), listener)

        # Assert final state is expected.
        expected_outcome = test.get('outcome', {}).get('collection')
        if expected_outcome is not None:
            collname = expected_outcome.get('name')
            if collname is not None:
                o_collection = getattr(database, collname)
            else:
                o_collection = collection
            self.assertEqual(list(o_collection.find()),
                             expected_outcome['data'])

    return run_scenario


test_creator = TestCreator(create_test, TestAllScenarios, _TEST_PATH)
test_creator.create_tests()


if __name__ == "__main__":
    unittest.main()