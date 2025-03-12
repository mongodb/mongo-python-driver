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
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path[0:0] = [""]

from test import IntegrationTest, UnitTest, client_context, unittest
from test.unified_format import generate_test_classes
from test.utils_shared import (
    OvertCommandListener,
)

from pymongo.synchronous.helpers import next

_IS_SYNC = True

pytestmark = pytest.mark.data_lake


class TestDataLakeMustConnect(UnitTest):
    def test_connected_to_data_lake(self):
        self.assertTrue(
            client_context.is_data_lake and client_context.connected,
            "client context must be connected to data lake when DATA_LAKE is set. Failed attempts:\n{}".format(
                client_context.connection_attempt_info()
            ),
        )


class TestDataLakeProse(IntegrationTest):
    # Default test database and collection names.
    TEST_DB = "test"
    TEST_COLLECTION = "driverdata"

    @client_context.require_data_lake
    def setUp(self):
        super().setUp()

    # Test killCursors
    def test_1(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])
        cursor = client[self.TEST_DB][self.TEST_COLLECTION].find({}, batch_size=2)
        next(cursor)

        # find command assertions
        find_cmd = listener.succeeded_events[-1]
        self.assertEqual(find_cmd.command_name, "find")
        cursor_id = find_cmd.reply["cursor"]["id"]
        cursor_ns = find_cmd.reply["cursor"]["ns"]

        # killCursors command assertions
        cursor.close()
        started = listener.started_events[-1]
        self.assertEqual(started.command_name, "killCursors")
        succeeded = listener.succeeded_events[-1]
        self.assertEqual(succeeded.command_name, "killCursors")

        self.assertIn(cursor_id, started.command["cursors"])
        target_ns = ".".join([started.command["$db"], started.command["killCursors"]])
        self.assertEqual(cursor_ns, target_ns)

        self.assertIn(cursor_id, succeeded.reply["cursorsKilled"])

    # Test no auth
    def test_2(self):
        client = self.rs_client_noauth()
        client.admin.command("ping")

    # Test with auth
    def test_3(self):
        for mechanism in ["SCRAM-SHA-1", "SCRAM-SHA-256"]:
            client = self.rs_or_single_client(authMechanism=mechanism)
            client[self.TEST_DB][self.TEST_COLLECTION].find_one()


# Location of JSON test specifications.
if _IS_SYNC:
    TEST_PATH = Path(__file__).parent / "data_lake/unified"
else:
    TEST_PATH = Path(__file__).parent.parent / "data_lake/unified"

# Generate unified tests.
globals().update(generate_test_classes(TEST_PATH, module=__name__))


if __name__ == "__main__":
    unittest.main()
