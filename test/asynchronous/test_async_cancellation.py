# Copyright 2025-present MongoDB, Inc.
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

"""Test that async cancellation performed by users raises the expected error."""
from __future__ import annotations

import asyncio
import sys
from test.utils import async_get_pool, get_pool, one

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, connected


class TestAsyncCancellation(AsyncIntegrationTest):
    async def test_async_cancellation_does_not_close_connection(self):
        client = await self.async_rs_or_single_client(maxPoolSize=1, retryReads=False)
        pool = await async_get_pool(client)
        await connected(client)
        conn = one(pool.conns)

        async def task():
            while True:
                await client.db.test.insert_one({"x": 1})
                await asyncio.sleep(0.005)

        task = asyncio.create_task(task())

        # Make sure the task successfully runs a few operations to simulate a long-running user task
        await asyncio.sleep(0.01)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertFalse(conn.closed)
