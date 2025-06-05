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

"""Test compliance with the connections survive primary step down spec."""
from __future__ import annotations

import sys
from test.utils import ensure_all_connected

sys.path[0:0] = [""]

from test import (
    IntegrationTest,
    client_context,
    unittest,
)
from test.helpers import repl_set_step_down
from test.utils_shared import (
    CMAPListener,
)

from bson import SON
from pymongo import monitoring
from pymongo.errors import NotPrimaryError
from pymongo.synchronous.collection import Collection
from pymongo.write_concern import WriteConcern

_IS_SYNC = True


class TestConnectionsSurvivePrimaryStepDown(IntegrationTest):
    listener: CMAPListener
    coll: Collection

    @client_context.require_replica_set
    def setUp(self):
        super().setUp()
        self.listener = CMAPListener()
        self.client = self.rs_or_single_client(
            event_listeners=[self.listener], retryWrites=False, heartbeatFrequencyMS=500
        )

        # Ensure connections to all servers in replica set. This is to test
        # that the is_writable flag is properly updated for connections that
        # survive a replica set election.
        ensure_all_connected(self.client)
        self.db = self.client.get_database("step-down", write_concern=WriteConcern("majority"))
        self.coll = self.db.get_collection("step-down", write_concern=WriteConcern("majority"))
        # Note that all ops use same write-concern as self.db (majority).
        self.db.drop_collection("step-down")
        self.db.create_collection("step-down")
        self.listener.reset()

    def set_fail_point(self, command_args):
        cmd = SON([("configureFailPoint", "failCommand")])
        cmd.update(command_args)
        self.client.admin.command(cmd)

    def verify_pool_cleared(self):
        self.assertEqual(self.listener.event_count(monitoring.PoolClearedEvent), 1)

    def verify_pool_not_cleared(self):
        self.assertEqual(self.listener.event_count(monitoring.PoolClearedEvent), 0)

    @client_context.require_version_min(4, 2, -1)
    def test_get_more_iteration(self):
        # Insert 5 documents with WC majority.
        self.coll.insert_many([{"data": k} for k in range(5)])
        # Start a find operation and retrieve first batch of results.
        batch_size = 2
        cursor = self.coll.find(batch_size=batch_size)
        for _ in range(batch_size):
            cursor.next()
        # Force step-down the primary.
        repl_set_step_down(self.client, replSetStepDown=5, force=True)
        # Get next batch of results.
        for _ in range(batch_size):
            cursor.next()
        # Verify pool not cleared.
        self.verify_pool_not_cleared()
        # Attempt insertion to mark server description as stale and prevent a
        # NotPrimaryError on the subsequent operation.
        try:
            self.coll.insert_one({})
        except NotPrimaryError:
            pass
        # Next insert should succeed on the new primary without clearing pool.
        self.coll.insert_one({})
        self.verify_pool_not_cleared()

    def run_scenario(self, error_code, retry, pool_status_checker):
        # Set fail point.
        self.set_fail_point(
            {"mode": {"times": 1}, "data": {"failCommands": ["insert"], "errorCode": error_code}}
        )
        self.addCleanup(self.set_fail_point, {"mode": "off"})
        # Insert record and verify failure.
        with self.assertRaises(NotPrimaryError) as exc:
            self.coll.insert_one({"test": 1})
        self.assertEqual(exc.exception.details["code"], error_code)  # type: ignore[call-overload]
        # Retry before CMAPListener assertion if retry_before=True.
        if retry:
            self.coll.insert_one({"test": 1})
        # Verify pool cleared/not cleared.
        pool_status_checker()
        # Always retry here to ensure discovery of new primary.
        self.coll.insert_one({"test": 1})

    @client_context.require_version_min(4, 2, -1)
    @client_context.require_test_commands
    def test_not_primary_keep_connection_pool(self):
        self.run_scenario(10107, True, self.verify_pool_not_cleared)

    @client_context.require_version_min(4, 2, 0)
    @client_context.require_test_commands
    def test_shutdown_in_progress(self):
        self.run_scenario(91, False, self.verify_pool_cleared)

    @client_context.require_version_min(4, 2, 0)
    @client_context.require_test_commands
    def test_interrupted_at_shutdown(self):
        self.run_scenario(11600, False, self.verify_pool_cleared)


if __name__ == "__main__":
    unittest.main()
