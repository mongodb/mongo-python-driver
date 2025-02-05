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
import traceback

from test.utils import async_get_pool, get_pool, one, delay

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, connected, async_client_context


class TestAsyncCancellation(AsyncIntegrationTest):
    async def test_async_cancellation_closes_connection(self):
        client = await self.async_rs_or_single_client()
        pool = await async_get_pool(client)
        await connected(client)
        conn = one(pool.conns)

        async def task():
            await client.db.test.find_one({"$where": delay(1.0)})

        task = asyncio.create_task(task())

        await asyncio.sleep(0.1)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertTrue(conn.closed)

    @async_client_context.require_transactions
    async def test_async_cancellation_aborts_transaction(self):
        client = await self.async_rs_or_single_client()
        await connected(client)

        session = client.start_session()

        async def callback(session):
            await client.db.test.find_one({"$where": delay(1.0)})

        async def task():
            await session.with_transaction(callback)

        task = asyncio.create_task(task())

        await asyncio.sleep(0.1)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertFalse(session.in_transaction)

