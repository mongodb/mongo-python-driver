# Copyright 2020-present MongoDB, Inc.
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

"""Test Atlas Data Lake."""

import os
import sys

sys.path[0:0] = [""]

from pymongo.auth import MECHANISMS
from test import client_context, unittest, IntegrationTest
from test.crud_v2_format import TestCrudV2
from test.utils import (
    rs_or_single_client, OvertCommandListener, TestCreator,
    _mongo_client)


# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "data_lake")


class TestDataLakeProse(IntegrationTest):
    # Default test database and collection names.
    TEST_DB = 'test'
    TEST_COLLECTION = 'driverdata'

    @classmethod
    def setUpClass(cls):
        super(TestDataLakeProse, cls).setUpClass()
        if not client_context.is_data_lake:
            raise unittest.SkipTest('Not connected to Atlas Data Lake')

    # Test killCursors
    def test_1(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        cursor = client[self.TEST_DB][self.TEST_COLLECTION].find(
            {}, batch_size=2)
        next(cursor)

        find_cmd = listener.results["succeeded"][-1]
        print(find_cmd.reply)
        self.assertEqual(find_cmd.command_name, "find")
        cursor_id = find_cmd.reply["cursor"]["id"]
        cursor_ns = find_cmd.reply["cursor"]["ns"]

        cursor.close()
        started = listener.results["started"][-1]
        print(started.command)
        succeeded = listener.results["succeeded"][-1]
        print(succeeded.reply)
        self.assertEqual(started.command_name, 'killCursors')
        self.assertEqual(succeeded.command_name, 'killCursors')
        self.assertEqual(started.command["cursor"]["id"], cursor_id)
        self.assertEqual(started.command["cursor"]["ns"], cursor_ns)
        self.assertIn(cursor_id, succeeded.reply["cursorsKilled"])

    # Test no auth
    def test_2(self):
        client = _mongo_client(None, None, authenticate=False)
        client.admin.command('ping')

    # Test with auth
    def test_3(self):
        for mechanism in ['SCRAM-SHA-1', 'SCRAM-SHA-256']:
            client = rs_or_single_client(authMechanism=mechanism)
            client.admin.command('ping')


class DataLakeTestSpec(TestCrudV2):
    # Default test database and collection names.
    TEST_DB = 'test'
    TEST_COLLECTION = 'driverdata'

    @classmethod
    def setUpClass(cls):
        super(DataLakeTestSpec, cls).setUpClass()
        if not client_context.is_data_lake:
            raise unittest.SkipTest('Not connected to Atlas Data Lake')

    def setup_scenario(self, scenario_def):
        # Spec tests MUST NOT insert data/drop collection for
        # data lake testing.
        pass


def create_test(scenario_def, test, name):
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


TestCreator(create_test, DataLakeTestSpec, _TEST_PATH).create_tests()


if __name__ == "__main__":
    unittest.main()
