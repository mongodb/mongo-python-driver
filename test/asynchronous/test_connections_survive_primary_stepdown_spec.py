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
from test.asynchronous.utils import async_ensure_all_connected

sys.path[0:0] = [""]

from test.asynchronous import (
    AsyncIntegrationTest,
    async_client_context,
    unittest,
)
from test.asynchronous.helpers import async_repl_set_step_down
from test.utils_shared import (
    CMAPListener,
)

from bson import SON
from pymongo import monitoring
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import NotPrimaryError
from pymongo.write_concern import WriteConcern

_IS_SYNC = False


class TestAsyncConnectionsSurvivePrimaryStepDown(AsyncIntegrationTest):
    listener: CMAPListener
    coll: AsyncCollection

    @async_client_context.require_replica_set
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.listener = CMAPListener()
        self.client = await self.async_rs_or_single_client(
            event_listeners=[self.listener], retryWrites=False, heartbeatFrequencyMS=500
        )

        # Ensure connections to all servers in replica set. This is to test
        # that the is_writable flag is properly updated for connections that
        # survive a replica set election.
        await async_ensure_all_connected(self.client)
        self.db = self.client.get_database("step-down", write_concern=WriteConcern("majority"))
        self.coll = self.db.get_collection("step-down", write_concern=WriteConcern("majority"))
        # Note that all ops use same write-concern as self.db (majority).
        await self.db.drop_collection("step-down")
        await self.db.create_collection("step-down")
        self.listener.reset()

    async def set_fail_point(self, command_args):
        cmd = SON([("configureFailPoint", "failCommand")])
        cmd.update(command_args)
        await self.client.admin.command(cmd)

    def verify_pool_cleared(self):
        self.assertEqual(self.listener.event_count(monitoring.PoolClearedEvent), 1)

    def verify_pool_not_cleared(self):
        self.assertEqual(self.listener.event_count(monitoring.PoolClearedEvent), 0)

    @async_client_context.require_version_min(4, 2, -1)
    async def test_get_more_iteration(self):
        # Insert 5 documents with WC majority.
        await self.coll.insert_many([{"data": k} for k in range(5)])
        # Start a find operation and retrieve first batch of results.
        batch_size = 2
        cursor = self.coll.find(batch_size=batch_size)
        for _ in range(batch_size):
            await cursor.next()
        # Force step-down the primary.
        await async_repl_set_step_down(self.client, replSetStepDown=5, force=True)
        # Get await anext batch of results.
        for _ in range(batch_size):
            await cursor.next()
        # Verify pool not cleared.
        self.verify_pool_not_cleared()
        # Attempt insertion to mark server description as stale and prevent a
        # NotPrimaryError on the subsequent operation.
        try:
            await self.coll.insert_one({})
        except NotPrimaryError:
            pass
        # Next insert should succeed on the new primary without clearing pool.
        await self.coll.insert_one({})
        self.verify_pool_not_cleared()

    async def run_scenario(self, error_code, retry, pool_status_checker):
        # Set fail point.
        await self.set_fail_point(
            {"mode": {"times": 1}, "data": {"failCommands": ["insert"], "errorCode": error_code}}
        )
        self.addAsyncCleanup(self.set_fail_point, {"mode": "off"})
        # Insert record and verify failure.
        with self.assertRaises(NotPrimaryError) as exc:
            await self.coll.insert_one({"test": 1})
        self.assertEqual(exc.exception.details["code"], error_code)  # type: ignore[call-overload]
        # Retry before CMAPListener assertion if retry_before=True.
        if retry:
            await self.coll.insert_one({"test": 1})
        # Verify pool cleared/not cleared.
        pool_status_checker()
        # Always retry here to ensure discovery of new primary.
        await self.coll.insert_one({"test": 1})

    @async_client_context.require_version_min(4, 2, -1)
    @async_client_context.require_test_commands
    async def test_not_primary_keep_connection_pool(self):
        await self.run_scenario(10107, True, self.verify_pool_not_cleared)

    @async_client_context.require_version_min(4, 2, 0)
    @async_client_context.require_test_commands
    async def test_shutdown_in_progress(self):
        await self.run_scenario(91, False, self.verify_pool_cleared)

    @async_client_context.require_version_min(4, 2, 0)
    @async_client_context.require_test_commands
    async def test_interrupted_at_shutdown(self):
        await self.run_scenario(11600, False, self.verify_pool_cleared)


if __name__ == "__main__":
    unittest.main()
