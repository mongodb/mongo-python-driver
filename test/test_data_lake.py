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

from test import IntegrationTest, client_context, unittest
from test.crud_v2_format import TestCrudV2
from test.utils import (
    OvertCommandListener,
    TestCreator,
    rs_client_noauth,
    rs_or_single_client,
)

# Location of JSON test specifications.
_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data_lake")


class TestDataLakeMustConnect(IntegrationTest):
    def test_connected_to_data_lake(self):
        data_lake = os.environ.get("DATA_LAKE")
        if not data_lake:
            self.skipTest("DATA_LAKE is not set")

        self.assertTrue(
            client_context.is_data_lake,
            "client context.is_data_lake must be True when DATA_LAKE is set",
        )


class TestDataLakeProse(IntegrationTest):
    # Default test database and collection names.
    TEST_DB = "test"
    TEST_COLLECTION = "driverdata"

    @classmethod
    @client_context.require_data_lake
    def setUpClass(cls):
        super(TestDataLakeProse, cls).setUpClass()

    # Test killCursors
    def test_1(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        cursor = client[self.TEST_DB][self.TEST_COLLECTION].find({}, batch_size=2)
        next(cursor)

        # find command assertions
        find_cmd = listener.results["succeeded"][-1]
        self.assertEqual(find_cmd.command_name, "find")
        cursor_id = find_cmd.reply["cursor"]["id"]
        cursor_ns = find_cmd.reply["cursor"]["ns"]

        # killCursors command assertions
        cursor.close()
        started = listener.results["started"][-1]
        self.assertEqual(started.command_name, "killCursors")
        succeeded = listener.results["succeeded"][-1]
        self.assertEqual(succeeded.command_name, "killCursors")

        self.assertIn(cursor_id, started.command["cursors"])
        target_ns = ".".join([started.command["$db"], started.command["killCursors"]])
        self.assertEqual(cursor_ns, target_ns)

        self.assertIn(cursor_id, succeeded.reply["cursorsKilled"])

    # Test no auth
    def test_2(self):
        client = rs_client_noauth()
        client.admin.command("ping")

    # Test with auth
    def test_3(self):
        for mechanism in ["SCRAM-SHA-1", "SCRAM-SHA-256"]:
            client = rs_or_single_client(authMechanism=mechanism)
            client[self.TEST_DB][self.TEST_COLLECTION].find_one()


class DataLakeTestSpec(TestCrudV2):
    # Default test database and collection names.
    TEST_DB = "test"
    TEST_COLLECTION = "driverdata"

    @classmethod
    @client_context.require_data_lake
    def setUpClass(cls):
        super(DataLakeTestSpec, cls).setUpClass()

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
