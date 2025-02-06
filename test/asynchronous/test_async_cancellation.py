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

"""Test that async cancellation performed by users clean up resources correctly."""
from __future__ import annotations

import asyncio
import sys
from test.utils import async_get_pool, delay, one

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, connected


class TestAsyncCancellation(AsyncIntegrationTest):
    async def test_async_cancellation_closes_connection(self):
        client = await self.async_rs_or_single_client(maxPoolSize=1)
        pool = await async_get_pool(client)
        await connected(client)
        conn = one(pool.conns)
        await client.db.test.insert_one({"x": 1})
        self.addAsyncCleanup(client.db.test.drop)

        async def task():
            await client.db.test.find_one({"$where": delay(0.2)})

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
        await client.db.test.insert_one({"x": 1})
        self.addAsyncCleanup(client.db.test.drop)

        session = client.start_session()

        async def callback(session):
            await client.db.test.find_one({"$where": delay(0.2)}, session=session)

        async def task():
            await session.with_transaction(callback)

        task = asyncio.create_task(task())

        await asyncio.sleep(0.1)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertFalse(session.in_transaction)

    @async_client_context.require_failCommand_blockConnection
    async def test_async_cancellation_closes_cursor(self):
        client = await self.async_rs_or_single_client()
        await connected(client)
        for _ in range(2):
            await client.db.test.insert_one({"x": 1})
        self.addAsyncCleanup(client.db.test.drop)

        cursor = client.db.test.find({}, batch_size=1)
        await cursor.next()

        # Make sure getMore commands block
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 200},
        }

        async def task():
            async with self.fail_point(fail_command):
                await cursor.next()

        task = asyncio.create_task(task())

        await asyncio.sleep(0.1)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertTrue(cursor._killed)

    @async_client_context.require_change_streams
    @async_client_context.require_failCommand_blockConnection
    async def test_async_cancellation_closes_change_stream(self):
        client = await self.async_rs_or_single_client()
        await connected(client)
        self.addAsyncCleanup(client.db.test.drop)

        change_stream = await client.db.test.watch(batch_size=2)

        # Make sure getMore commands block
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 200},
        }

        async def task():
            async with self.fail_point(fail_command):
                for _ in range(2):
                    await client.db.test.insert_one({"x": 1})
                await change_stream.next()

        task = asyncio.create_task(task())

        await asyncio.sleep(0.1)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertTrue(change_stream._closed)
