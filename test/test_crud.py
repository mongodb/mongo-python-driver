# Copyright 2015 MongoDB, Inc.
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
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.results import _WriteResult

from test import unittest, client_context, IntegrationTest

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'crud')


class TestAllScenarios(IntegrationTest):
    pass


def camel_to_snake(camel):
    # Regex to convert CamelCase to snake_case.
    snake = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake).lower()


def check_result(expected_result, result):
    if isinstance(result, Cursor) or isinstance(result, CommandCursor):
        return list(result) == expected_result

    elif isinstance(result, _WriteResult):
        for r in expected_result:
            prop = camel_to_snake(r)
            return getattr(result, prop) == expected_result[r]
    else:
        if not expected_result:
            return result is None
        else:
            return result == expected_result


def create_test(scenario_def, test, ignore_result):
    def run_scenario(self):
            # Load data.
            assert scenario_def['data'], "tests must have non-empty data"
            self.db.test.drop()
            self.db.test.insert_many(scenario_def['data'])

            # Convert command from CamelCase to pymongo.collection method.
            operation = camel_to_snake(test['operation']['name'])
            cmd = getattr(self.db.test, operation)

            # Convert arguments to snake_case and handle special cases.
            arguments = test['operation']['arguments']
            for arg_name in list(arguments):
                c2s = camel_to_snake(arg_name)
                # PyMongo accepts sort as list of tuples. Asserting len=1
                # because ordering dicts from JSON in 2.6 is unwieldy.
                if arg_name == "sort":
                    sort_dict = arguments[arg_name]
                    assert len(sort_dict) == 1, 'test can only have 1 sort key'
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

            result = cmd(**arguments)

            # Assert result is expected, excluding the $out aggregation test.
            if not ignore_result:
                check_result(test['outcome'].get('result'), result)

            # Assert final state is expected.
            expected_c = test['outcome'].get('collection')
            if expected_c is not None:
                expected_name = expected_c.get('name')
                if expected_name is not None:
                    db_coll = self.db[expected_name]
                else:
                    db_coll = self.db.test
                self.assertEqual(list(db_coll.find()), expected_c['data'])

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(scenario_stream)

            test_type = os.path.splitext(filename)[0]

            # Construct test from scenario.
            for test in scenario_def['tests']:
                desc = test['description']
                # Special case tests that require specific versions
                if ("without an id specified" in desc or
                        "FindOneAndReplace" in desc and "with upsert" in desc):
                    new_test = client_context.require_version_min(2, 6, 0)(
                        create_test(scenario_def, test, False))
                elif desc == "Aggregate with $out":
                    new_test = client_context.require_version_min(2, 6, 0)(
                        create_test(scenario_def, test, True))
                else:
                    new_test = create_test(scenario_def, test, False)

                test_name = 'test_%s_%s_%s' % (
                    dirname,
                    test_type,
                    str(test['description'].replace(" ", "_")))

                new_test.__name__ = test_name
                setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()
