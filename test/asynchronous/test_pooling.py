# Copyright 2009-present MongoDB, Inc.
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

"""Test built in connection-pooling with threads."""
from __future__ import annotations

import asyncio
import gc
import random
import socket
import sys
import time
from test.asynchronous.utils import async_get_pool, async_joinall, flaky

from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.son import SON
from pymongo import AsyncMongoClient, message, timeout
from pymongo.errors import AutoReconnect, ConnectionFailure, DuplicateKeyError
from pymongo.hello import HelloCompat
from pymongo.lock import _async_create_lock

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest
from test.asynchronous.helpers import ConcurrentRunner
from test.utils_shared import delay

from pymongo.asynchronous.pool import Pool, PoolOptions
from pymongo.socket_checker import SocketChecker

_IS_SYNC = False


N = 10
DB = "pymongo-pooling-tests"


async def gc_collect_until_done(tasks, timeout=60):
    start = time.time()
    running = list(tasks)
    while running:
        assert (time.time() - start) < timeout, "Tasks timed out"
        for t in running:
            await t.join(0.1)
            if not t.is_alive():
                running.remove(t)
        gc.collect()


class MongoTask(ConcurrentRunner):
    """A thread/Task that uses a AsyncMongoClient."""

    def __init__(self, client):
        super().__init__()
        self.daemon = True  # Don't hang whole test if task hangs.
        self.client = client
        self.db = self.client[DB]
        self.passed = False

    async def run(self):
        await self.run_mongo_thread()
        self.passed = True

    async def run_mongo_thread(self):
        raise NotImplementedError


class InsertOneAndFind(MongoTask):
    async def run_mongo_thread(self):
        for _ in range(N):
            rand = random.randint(0, N)
            _id = (await self.db.sf.insert_one({"x": rand})).inserted_id
            assert rand == (await self.db.sf.find_one(_id))["x"]


class Unique(MongoTask):
    async def run_mongo_thread(self):
        for _ in range(N):
            await self.db.unique.insert_one({})  # no error


class NonUnique(MongoTask):
    async def run_mongo_thread(self):
        for _ in range(N):
            try:
                await self.db.unique.insert_one({"_id": "jesse"})
            except DuplicateKeyError:
                pass
            else:
                raise AssertionError("Should have raised DuplicateKeyError")


class SocketGetter(MongoTask):
    """Utility for TestPooling.

    Checks out a socket and holds it forever. Used in
    test_no_wait_queue_timeout.
    """

    def __init__(self, client, pool):
        super().__init__(client)
        self.state = "init"
        self.pool = pool
        self.sock = None

    async def run_mongo_thread(self):
        self.state = "get_socket"

        # Call 'pin_cursor' so we can hold the socket.
        async with self.pool.checkout() as sock:
            sock.pin_cursor()
            self.sock = sock

        self.state = "connection"

    async def release_conn(self):
        if self.sock:
            await self.sock.unpin()
            self.sock = None
            return True
        return False


async def run_cases(client, cases):
    tasks = []
    n_runs = 5

    for case in cases:
        for _i in range(n_runs):
            t = case(client)
            await t.start()
            tasks.append(t)

    for t in tasks:
        await t.join()

    for t in tasks:
        assert t.passed, "%s.run() threw an exception" % repr(t)


class _TestPoolingBase(AsyncIntegrationTest):
    """Base class for all connection-pool tests."""

    @async_client_context.require_connection
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.c = await self.async_rs_or_single_client()
        db = self.c[DB]
        await db.unique.drop()
        await db.test.drop()
        await db.unique.insert_one({"_id": "jesse"})
        await db.test.insert_many([{} for _ in range(10)])

    async def create_pool(self, pair=None, *args, **kwargs):
        if pair is None:
            pair = (await async_client_context.host, await async_client_context.port)
        # Start the pool with the correct ssl options.
        pool_options = async_client_context.client._topology_settings.pool_options
        kwargs["ssl_context"] = pool_options._ssl_context
        kwargs["tls_allow_invalid_hostnames"] = pool_options.tls_allow_invalid_hostnames
        kwargs["server_api"] = pool_options.server_api
        pool = Pool(pair, PoolOptions(*args, **kwargs))
        await pool.ready()
        return pool


class TestPooling(_TestPoolingBase):
    async def test_max_pool_size_validation(self):
        host, port = await async_client_context.host, await async_client_context.port
        self.assertRaises(ValueError, AsyncMongoClient, host=host, port=port, maxPoolSize=-1)

        self.assertRaises(ValueError, AsyncMongoClient, host=host, port=port, maxPoolSize="foo")

        c = AsyncMongoClient(host=host, port=port, maxPoolSize=100, connect=False)
        self.assertEqual(c.options.pool_options.max_pool_size, 100)

    async def test_no_disconnect(self):
        await run_cases(self.c, [NonUnique, Unique, InsertOneAndFind])

    async def test_pool_reuses_open_socket(self):
        # Test Pool's _check_closed() method doesn't close a healthy socket.
        cx_pool = await self.create_pool(max_pool_size=10)
        cx_pool._check_interval_seconds = 0  # Always check.
        async with cx_pool.checkout() as conn:
            pass

        async with cx_pool.checkout() as new_connection:
            self.assertEqual(conn, new_connection)

        self.assertEqual(1, len(cx_pool.conns))

    async def test_get_socket_and_exception(self):
        # get_socket() returns socket after a non-network error.
        cx_pool = await self.create_pool(max_pool_size=1, wait_queue_timeout=1)
        with self.assertRaises(ZeroDivisionError):
            async with cx_pool.checkout() as conn:
                1 / 0

        # Socket was returned, not closed.
        async with cx_pool.checkout() as new_connection:
            self.assertEqual(conn, new_connection)

        self.assertEqual(1, len(cx_pool.conns))

    async def test_pool_removes_closed_socket(self):
        # Test that Pool removes explicitly closed socket.
        cx_pool = await self.create_pool()

        async with cx_pool.checkout() as conn:
            # Use Connection's API to close the socket.
            await conn.close_conn(None)

        self.assertEqual(0, len(cx_pool.conns))

    async def test_pool_removes_dead_socket(self):
        # Test that Pool removes dead socket and the socket doesn't return
        # itself PYTHON-344
        cx_pool = await self.create_pool(max_pool_size=1, wait_queue_timeout=1)
        cx_pool._check_interval_seconds = 0  # Always check.

        async with cx_pool.checkout() as conn:
            # Simulate a closed socket without telling the Connection it's
            # closed.
            await conn.conn.close()
            self.assertTrue(conn.conn_closed())

        async with cx_pool.checkout() as new_connection:
            self.assertEqual(0, len(cx_pool.conns))
            self.assertNotEqual(conn, new_connection)

        self.assertEqual(1, len(cx_pool.conns))

        # Semaphore was released.
        async with cx_pool.checkout():
            pass

    async def test_socket_closed(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((await async_client_context.host, await async_client_context.port))
        socket_checker = SocketChecker()
        self.assertFalse(socket_checker.socket_closed(s))
        s.close()
        self.assertTrue(socket_checker.socket_closed(s))

    async def test_socket_checker(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((await async_client_context.host, await async_client_context.port))
        socket_checker = SocketChecker()
        # Socket has nothing to read.
        self.assertFalse(socket_checker.select(s, read=True))
        self.assertFalse(socket_checker.select(s, read=True, timeout=0))
        self.assertFalse(socket_checker.select(s, read=True, timeout=0.05))
        # Socket is writable.
        self.assertTrue(socket_checker.select(s, write=True, timeout=None))
        self.assertTrue(socket_checker.select(s, write=True))
        self.assertTrue(socket_checker.select(s, write=True, timeout=0))
        self.assertTrue(socket_checker.select(s, write=True, timeout=0.05))
        # Make the socket readable
        _, msg, _ = message._query(
            0, "admin.$cmd", 0, -1, SON([("ping", 1)]), None, DEFAULT_CODEC_OPTIONS
        )
        s.sendall(msg)
        # Block until the socket is readable.
        self.assertTrue(socket_checker.select(s, read=True, timeout=None))
        self.assertTrue(socket_checker.select(s, read=True))
        self.assertTrue(socket_checker.select(s, read=True, timeout=0))
        self.assertTrue(socket_checker.select(s, read=True, timeout=0.05))
        # Socket is still writable.
        self.assertTrue(socket_checker.select(s, write=True, timeout=None))
        self.assertTrue(socket_checker.select(s, write=True))
        self.assertTrue(socket_checker.select(s, write=True, timeout=0))
        self.assertTrue(socket_checker.select(s, write=True, timeout=0.05))
        s.close()
        self.assertTrue(socket_checker.socket_closed(s))

    async def test_return_socket_after_reset(self):
        pool = await self.create_pool()
        async with pool.checkout() as sock:
            self.assertEqual(pool.active_sockets, 1)
            self.assertEqual(pool.operation_count, 1)
            await pool.reset()

        self.assertTrue(sock.closed)
        self.assertEqual(0, len(pool.conns))
        self.assertEqual(pool.active_sockets, 0)
        self.assertEqual(pool.operation_count, 0)

    async def test_pool_check(self):
        # Test that Pool recovers from two connection failures in a row.
        # This exercises code at the end of Pool._check().
        cx_pool = await self.create_pool(max_pool_size=1, connect_timeout=1, wait_queue_timeout=1)
        cx_pool._check_interval_seconds = 0  # Always check.
        self.addAsyncCleanup(cx_pool.close)

        async with cx_pool.checkout() as conn:
            # Simulate a closed socket without telling the Connection it's
            # closed.
            await conn.conn.close()

        # Swap pool's address with a bad one.
        address, cx_pool.address = cx_pool.address, ("foo.com", 1234)
        with self.assertRaises(AutoReconnect):
            async with cx_pool.checkout():
                pass

        # Back to normal, semaphore was correctly released.
        cx_pool.address = address
        async with cx_pool.checkout():
            pass

    async def test_wait_queue_timeout(self):
        wait_queue_timeout = 2  # Seconds
        pool = await self.create_pool(max_pool_size=1, wait_queue_timeout=wait_queue_timeout)
        self.addAsyncCleanup(pool.close)

        async with pool.checkout():
            start = time.time()
            with self.assertRaises(ConnectionFailure):
                async with pool.checkout():
                    pass

        duration = time.time() - start
        self.assertLess(
            abs(wait_queue_timeout - duration),
            1,
            f"Waited {duration:.2f} seconds for a socket, expected {wait_queue_timeout:f}",
        )

    async def test_no_wait_queue_timeout(self):
        # Verify get_socket() with no wait_queue_timeout blocks forever.
        pool = await self.create_pool(max_pool_size=1)
        self.addAsyncCleanup(pool.close)

        # Reach max_size.
        async with pool.checkout() as s1:
            t = SocketGetter(self.c, pool)
            await t.start()
            while t.state != "get_socket":
                await asyncio.sleep(0.1)

            await asyncio.sleep(1)
            self.assertEqual(t.state, "get_socket")

        while t.state != "connection":
            await asyncio.sleep(0.1)

        self.assertEqual(t.state, "connection")
        self.assertEqual(t.sock, s1)
        # Cleanup
        await t.release_conn()
        await t.join()
        await pool.close()

    async def test_checkout_more_than_max_pool_size(self):
        pool = await self.create_pool(max_pool_size=2)

        socks = []
        for _ in range(2):
            # Call 'pin_cursor' so we can hold the socket.
            async with pool.checkout() as sock:
                sock.pin_cursor()
                socks.append(sock)

        tasks = []
        for _ in range(10):
            t = SocketGetter(self.c, pool)
            await t.start()
            tasks.append(t)
        await asyncio.sleep(1)
        for t in tasks:
            self.assertEqual(t.state, "get_socket")
        # Cleanup
        for socket_info in socks:
            await socket_info.unpin()
        while tasks:
            to_remove = []
            for t in tasks:
                if await t.release_conn():
                    to_remove.append(t)
                    await t.join()
            for t in to_remove:
                tasks.remove(t)
            await asyncio.sleep(0.05)
        await pool.close()

    async def test_maxConnecting(self):
        client = await self.async_rs_or_single_client()
        await self.client.test.test.insert_one({})
        self.addAsyncCleanup(self.client.test.test.delete_many, {})
        pool = await async_get_pool(client)
        docs = []

        # Run 50 short running operations
        async def find_one():
            docs.append(await client.test.test.find_one({}))

        tasks = [ConcurrentRunner(target=find_one) for _ in range(50)]
        for task in tasks:
            await task.start()
        for task in tasks:
            await task.join(10)

        self.assertEqual(len(docs), 50)
        self.assertLessEqual(len(pool.conns), 50)
        # TLS and auth make connection establishment more expensive than
        # the query which leads to more threads hitting maxConnecting.
        # The end result is fewer total connections and better latency.
        if async_client_context.tls and async_client_context.auth_enabled:
            self.assertLessEqual(len(pool.conns), 30)
        else:
            self.assertLessEqual(len(pool.conns), 50)
        # MongoDB 4.4.1 with auth + ssl:
        # maxConnecting = 2:         6 connections in ~0.231+ seconds
        # maxConnecting = unbounded: 50 connections in ~0.642+ seconds
        #
        # MongoDB 4.4.1 with no-auth no-ssl Python 3.8:
        # maxConnecting = 2:         15-22 connections in ~0.108+ seconds
        # maxConnecting = unbounded: 30+ connections in ~0.140+ seconds
        print(len(pool.conns))

    @async_client_context.require_failCommand_appName
    async def test_csot_timeout_message(self):
        client = await self.async_rs_or_single_client(appName="connectionTimeoutApp")
        # Mock an operation failing due to pymongo.timeout().
        mock_connection_timeout = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {
                "blockConnection": True,
                "blockTimeMS": 1000,
                "failCommands": ["find"],
                "appName": "connectionTimeoutApp",
            },
        }

        await client.db.t.insert_one({"x": 1})

        async with self.fail_point(mock_connection_timeout):
            with self.assertRaises(Exception) as error:
                with timeout(0.5):
                    await client.db.t.find_one({"$where": delay(2)})

        self.assertIn("(configured timeouts: timeoutMS: 500.0ms", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_socket_timeout_message(self):
        client = await self.async_rs_or_single_client(
            socketTimeoutMS=500, appName="connectionTimeoutApp"
        )
        # Mock an operation failing due to socketTimeoutMS.
        mock_connection_timeout = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {
                "blockConnection": True,
                "blockTimeMS": 1000,
                "failCommands": ["find"],
                "appName": "connectionTimeoutApp",
            },
        }

        await client.db.t.insert_one({"x": 1})

        async with self.fail_point(mock_connection_timeout):
            with self.assertRaises(Exception) as error:
                await client.db.t.find_one({"$where": delay(2)})

        self.assertIn(
            "(configured timeouts: socketTimeoutMS: 500.0ms, connectTimeoutMS: 20000.0ms)",
            str(error.exception),
        )

    @async_client_context.require_failCommand_appName
    async def test_connection_timeout_message(self):
        # Mock a connection creation failing due to timeout.
        mock_connection_timeout = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {
                "blockConnection": True,
                "blockTimeMS": 1000,
                "failCommands": [HelloCompat.LEGACY_CMD, "hello"],
                "appName": "connectionTimeoutApp",
            },
        }

        client = await self.async_rs_or_single_client(
            connectTimeoutMS=500,
            socketTimeoutMS=500,
            appName="connectionTimeoutApp",
            heartbeatFrequencyMS=1000000,
        )
        await client.admin.command("ping")
        pool = await async_get_pool(client)
        await pool.reset_without_pause()
        async with self.fail_point(mock_connection_timeout):
            with self.assertRaises(Exception) as error:
                await client.admin.command("ping")

        self.assertIn(
            "(configured timeouts: socketTimeoutMS: 500.0ms, connectTimeoutMS: 500.0ms)",
            str(error.exception),
        )


class TestPoolMaxSize(_TestPoolingBase):
    async def test_max_pool_size(self):
        max_pool_size = 4
        c = await self.async_rs_or_single_client(maxPoolSize=max_pool_size)
        collection = c[DB].test

        # Need one document.
        await collection.drop()
        await collection.insert_one({})

        # ntasks had better be much larger than max_pool_size to ensure that
        # max_pool_size connections are actually required at some point in this
        # test's execution.
        cx_pool = await async_get_pool(c)
        ntasks = 10
        tasks = []
        lock = _async_create_lock()
        self.n_passed = 0

        async def f():
            for _ in range(5):
                await collection.find_one({"$where": delay(0.1)})
                assert len(cx_pool.conns) <= max_pool_size

            async with lock:
                self.n_passed += 1

        for _i in range(ntasks):
            t = ConcurrentRunner(target=f)
            tasks.append(t)
            await t.start()

        await async_joinall(tasks)
        self.assertEqual(ntasks, self.n_passed)
        self.assertGreater(len(cx_pool.conns), 1)
        self.assertEqual(0, cx_pool.requests)

    async def test_max_pool_size_none(self):
        c = await self.async_rs_or_single_client(maxPoolSize=None)
        collection = c[DB].test

        # Need one document.
        await collection.drop()
        await collection.insert_one({})

        cx_pool = await async_get_pool(c)
        ntasks = 10
        tasks = []
        lock = _async_create_lock()
        self.n_passed = 0

        async def f():
            for _ in range(5):
                await collection.find_one({"$where": delay(0.1)})

            async with lock:
                self.n_passed += 1

        for _i in range(ntasks):
            t = ConcurrentRunner(target=f)
            tasks.append(t)
            await t.start()

        await async_joinall(tasks)
        self.assertEqual(ntasks, self.n_passed)
        self.assertGreater(len(cx_pool.conns), 1)
        self.assertEqual(cx_pool.max_pool_size, float("inf"))

    async def test_max_pool_size_zero(self):
        c = await self.async_rs_or_single_client(maxPoolSize=0)
        pool = await async_get_pool(c)
        self.assertEqual(pool.max_pool_size, float("inf"))

    async def test_max_pool_size_with_connection_failure(self):
        # The pool acquires its semaphore before attempting to connect; ensure
        # it releases the semaphore on connection failure.
        test_pool = Pool(
            ("somedomainthatdoesntexist.org", 27017),
            PoolOptions(max_pool_size=1, connect_timeout=1, socket_timeout=1, wait_queue_timeout=1),
        )
        await test_pool.ready()

        # First call to get_socket fails; if pool doesn't release its semaphore
        # then the second call raises "ConnectionFailure: Timed out waiting for
        # socket from pool" instead of AutoReconnect.
        for _i in range(2):
            with self.assertRaises(AutoReconnect) as context:
                async with test_pool.checkout():
                    pass

            # Testing for AutoReconnect instead of ConnectionFailure, above,
            # is sufficient right *now* to catch a semaphore leak. But that
            # seems error-prone, so check the message too.
            self.assertNotIn("waiting for socket from pool", str(context.exception))


if __name__ == "__main__":
    unittest.main()
