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

import sys

sys.path[0:0] = [""]

from bson import SON
from pymongo import monitoring
from pymongo.errors import NotMasterError
from pymongo.write_concern import WriteConcern

from test import (client_context,
                  unittest,
                  IntegrationTest)
from test.utils import (CMAPListener,
                        ensure_all_connected,
                        repl_set_step_down,
                        rs_or_single_client)


class TestConnectionsSurvivePrimaryStepDown(IntegrationTest):
    @classmethod
    @client_context.require_replica_set
    def setUpClass(cls):
        super(TestConnectionsSurvivePrimaryStepDown, cls).setUpClass()
        cls.listener = CMAPListener()
        cls.client = rs_or_single_client(event_listeners=[cls.listener],
                                         retryWrites=False,
                                         heartbeatFrequencyMS=500)

        # Ensure connections to all servers in replica set. This is to test
        # that the is_writable flag is properly updated for sockets that
        # survive a replica set election.
        ensure_all_connected(cls.client)
        cls.listener.reset()

        cls.db = cls.client.get_database(
            "step-down", write_concern=WriteConcern("majority"))
        cls.coll = cls.db.get_collection(
            "step-down", write_concern=WriteConcern("majority"))

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        # Note that all ops use same write-concern as self.db (majority).
        self.db.drop_collection("step-down")
        self.db.create_collection("step-down")
        self.listener.reset()

    def set_fail_point(self, command_args):
        cmd = SON([("configureFailPoint", "failCommand")])
        cmd.update(command_args)
        self.client.admin.command(cmd)

    def verify_pool_cleared(self):
        self.assertEqual(
            self.listener.event_count(monitoring.PoolClearedEvent), 1)

    def verify_pool_not_cleared(self):
        self.assertEqual(
            self.listener.event_count(monitoring.PoolClearedEvent), 0)

    @client_context.require_version_min(4, 2, -1)
    def test_get_more_iteration(self):
        # Insert 5 documents with WC majority.
        self.coll.insert_many([{'data': k} for k in range(5)])
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
        # notMaster error on the subsequent operation.
        try:
            self.coll.insert_one({})
        except NotMasterError:
            pass
        # Next insert should succeed on the new primary without clearing pool.
        self.coll.insert_one({})
        self.verify_pool_not_cleared()

    def run_scenario(self, error_code, retry, pool_status_checker):
        # Set fail point.
        self.set_fail_point({"mode": {"times": 1},
                             "data": {"failCommands": ["insert"],
                                      "errorCode": error_code}})
        self.addCleanup(self.set_fail_point, {"mode": "off"})
        # Insert record and verify failure.
        with self.assertRaises(NotMasterError) as exc:
            self.coll.insert_one({"test": 1})
        self.assertEqual(exc.exception.details['code'], error_code)
        # Retry before CMAPListener assertion if retry_before=True.
        if retry:
            self.coll.insert_one({"test": 1})
        # Verify pool cleared/not cleared.
        pool_status_checker()
        # Always retry here to ensure discovery of new primary.
        self.coll.insert_one({"test": 1})

    @client_context.require_version_min(4, 2, -1)
    @client_context.require_test_commands
    def test_not_master_keep_connection_pool(self):
        self.run_scenario(10107, True, self.verify_pool_not_cleared)

    @client_context.require_version_min(4, 0, 0)
    @client_context.require_version_max(4, 1, 0, -1)
    @client_context.require_test_commands
    def test_not_master_reset_connection_pool(self):
        self.run_scenario(10107, False, self.verify_pool_cleared)

    @client_context.require_version_min(4, 0, 0)
    @client_context.require_test_commands
    def test_shutdown_in_progress(self):
        self.run_scenario(91, False, self.verify_pool_cleared)

    @client_context.require_version_min(4, 0, 0)
    @client_context.require_test_commands
    def test_interrupted_at_shutdown(self):
        self.run_scenario(11600, False, self.verify_pool_cleared)


if __name__ == "__main__":
    unittest.main()
