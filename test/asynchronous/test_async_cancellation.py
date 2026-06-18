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
import functools
import socket
import ssl
import sys
from unittest.mock import patch

from test.asynchronous.utils import async_get_pool
from test.utils_shared import delay, one

sys.path[0:0] = [""]

from pymongo import pool_shared
from test.asynchronous import AsyncIntegrationTest, async_client_context, connected


class TestAsyncCancellation(AsyncIntegrationTest):
    async def test_async_cancellation_closes_connection(self):
        pool = await async_get_pool(self.client)
        await self.client.db.test.insert_one({"x": 1})
        self.addAsyncCleanup(self.client.db.test.delete_many, {})

        conn = one(pool.conns)

        async def task():
            await self.client.db.test.find_one({"$where": delay(0.2)})

        task = asyncio.create_task(task())

        await asyncio.sleep(0.1)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertTrue(conn.closed)

    @async_client_context.require_transactions
    async def test_async_cancellation_aborts_transaction(self):
        await self.client.db.test.insert_one({"x": 1})
        self.addAsyncCleanup(self.client.db.test.delete_many, {})

        session = self.client.start_session()

        async def callback(session):
            await self.client.db.test.find_one({"$where": delay(0.2)}, session=session)

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
        await self.client.db.test.insert_many([{"x": 1}, {"x": 2}])
        self.addAsyncCleanup(self.client.db.test.delete_many, {})

        cursor = self.client.db.test.find({}, batch_size=1)
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
        self.addAsyncCleanup(self.client.db.test.delete_many, {})
        change_stream = await self.client.db.test.watch(batch_size=2)
        event = asyncio.Event()

        # Make sure getMore commands block
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 200},
        }

        async def task():
            async with self.fail_point(fail_command):
                await self.client.db.test.insert_many([{"x": 1}, {"x": 2}])
                event.set()
                await change_stream.next()

        task = asyncio.create_task(task())

        await event.wait()

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertTrue(change_stream._closed)

    async def test_cancellation_closes_socket_during_create_connection(self):
        address = (await async_client_context.host, await async_client_context.port)
        options = (await async_get_pool(self.client)).opts

        created_sockets: list[socket.socket] = []
        real_socket_cls = socket.socket
        target_task = None

        def tracking_socket(*args, **kwargs):
            s = real_socket_cls(*args, **kwargs)
            if asyncio.current_task() is target_task:
                created_sockets.append(s)
            return s

        loop = asyncio.get_running_loop()
        real_sock_connect = loop.sock_connect
        started = asyncio.Event()
        block_forever = asyncio.Event()

        async def slow_sock_connect(sock, addr):
            if sock in created_sockets:
                started.set()
                await block_forever.wait()
                return None
            return await real_sock_connect(sock, addr)

        with (
            patch.object(socket, "socket", tracking_socket),
            patch.object(loop, "sock_connect", slow_sock_connect),
        ):
            task = asyncio.create_task(pool_shared._async_create_connection(address, options))
            target_task = task
            await asyncio.wait_for(started.wait(), timeout=5)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(created_sockets, "expected at least one socket to be created")
        for sock in created_sockets:
            self.assertEqual(
                sock.fileno(),
                -1,
                f"socket leaked across cancellation: {sock!r}",
            )

    async def test_cancellation_closes_socket_during_ssl_wrap_socket(self):
        address = (await async_client_context.host, await async_client_context.port)
        options = (await async_get_pool(self.client)).opts
        fake_ssl_context = ssl.create_default_context()

        created_sockets: list[socket.socket] = []
        real_socket_cls = socket.socket
        target_task = None

        def tracking_socket(*args, **kwargs):
            s = real_socket_cls(*args, **kwargs)
            if asyncio.current_task() is target_task:
                created_sockets.append(s)
            return s

        loop = asyncio.get_running_loop()
        real_run_in_executor = loop.run_in_executor
        started = asyncio.Event()

        def slow_run_in_executor(executor, func, *args):
            # Need to unwrap the SNI branch here if present
            inner = func.func if isinstance(func, functools.partial) else func
            # Each `ctx.wrap_socket` access returns a fresh bound-method
            # object, so we check the bound instance (__self__) instead
            if (
                getattr(inner, "__self__", None) is fake_ssl_context
                and asyncio.current_task() is target_task
            ):
                started.set()
                # Return a future that never completes for cancellation.
                return asyncio.get_running_loop().create_future()
            return real_run_in_executor(executor, func, *args)

        with (
            patch.object(socket, "socket", tracking_socket),
            patch.object(loop, "run_in_executor", slow_run_in_executor),
            patch.object(options, "_PoolOptions__ssl_context", fake_ssl_context),
        ):
            task = asyncio.create_task(pool_shared._async_configured_socket(address, options))
            target_task = task
            await asyncio.wait_for(started.wait(), timeout=5)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertTrue(created_sockets, "expected at least one socket to be created")
        for sock in created_sockets:
            self.assertEqual(
                sock.fileno(),
                -1,
                f"socket leaked across cancellation: {sock!r}",
            )
