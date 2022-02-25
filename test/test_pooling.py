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

import gc
import random
import socket
import sys
import threading
import time

from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.son import SON
from pymongo import MongoClient, message
from pymongo.errors import AutoReconnect, ConnectionFailure, DuplicateKeyError

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.utils import delay, get_pool, joinall, rs_or_single_client

from pymongo.pool import Pool, PoolOptions
from pymongo.socket_checker import SocketChecker


@client_context.require_connection
def setUpModule():
    pass


N = 10
DB = "pymongo-pooling-tests"


def gc_collect_until_done(threads, timeout=60):
    start = time.time()
    running = list(threads)
    while running:
        assert (time.time() - start) < timeout, "Threads timed out"
        for t in running:
            t.join(0.1)
            if not t.is_alive():
                running.remove(t)
        gc.collect()


class MongoThread(threading.Thread):
    """A thread that uses a MongoClient."""

    def __init__(self, client):
        super(MongoThread, self).__init__()
        self.daemon = True  # Don't hang whole test if thread hangs.
        self.client = client
        self.db = self.client[DB]
        self.passed = False

    def run(self):
        self.run_mongo_thread()
        self.passed = True

    def run_mongo_thread(self):
        raise NotImplementedError


class InsertOneAndFind(MongoThread):
    def run_mongo_thread(self):
        for _ in range(N):
            rand = random.randint(0, N)
            _id = self.db.sf.insert_one({"x": rand}).inserted_id
            assert rand == self.db.sf.find_one(_id)["x"]


class Unique(MongoThread):
    def run_mongo_thread(self):
        for _ in range(N):
            self.db.unique.insert_one({})  # no error


class NonUnique(MongoThread):
    def run_mongo_thread(self):
        for _ in range(N):
            try:
                self.db.unique.insert_one({"_id": "jesse"})
            except DuplicateKeyError:
                pass
            else:
                raise AssertionError("Should have raised DuplicateKeyError")


class SocketGetter(MongoThread):
    """Utility for TestPooling.

    Checks out a socket and holds it forever. Used in
    test_no_wait_queue_timeout.
    """

    def __init__(self, client, pool):
        super(SocketGetter, self).__init__(client)
        self.state = "init"
        self.pool = pool
        self.sock = None

    def run_mongo_thread(self):
        self.state = "get_socket"

        # Call 'pin_cursor' so we can hold the socket.
        with self.pool.get_socket() as sock:
            sock.pin_cursor()
            self.sock = sock

        self.state = "sock"

    def __del__(self):
        if self.sock:
            self.sock.close_socket(None)


def run_cases(client, cases):
    threads = []
    n_runs = 5

    for case in cases:
        for i in range(n_runs):
            t = case(client)
            t.start()
            threads.append(t)

    for t in threads:
        t.join()

    for t in threads:
        assert t.passed, "%s.run() threw an exception" % repr(t)


class _TestPoolingBase(IntegrationTest):
    """Base class for all connection-pool tests."""

    def setUp(self):
        super(_TestPoolingBase, self).setUp()
        self.c = rs_or_single_client()
        db = self.c[DB]
        db.unique.drop()
        db.test.drop()
        db.unique.insert_one({"_id": "jesse"})
        db.test.insert_many([{} for _ in range(10)])

    def tearDown(self):
        self.c.close()
        super(_TestPoolingBase, self).tearDown()

    def create_pool(self, pair=(client_context.host, client_context.port), *args, **kwargs):
        # Start the pool with the correct ssl options.
        pool_options = client_context.client._topology_settings.pool_options
        kwargs["ssl_context"] = pool_options._ssl_context
        kwargs["tls_allow_invalid_hostnames"] = pool_options.tls_allow_invalid_hostnames
        kwargs["server_api"] = pool_options.server_api
        pool = Pool(pair, PoolOptions(*args, **kwargs))
        pool.ready()
        return pool


class TestPooling(_TestPoolingBase):
    def test_max_pool_size_validation(self):
        host, port = client_context.host, client_context.port
        self.assertRaises(ValueError, MongoClient, host=host, port=port, maxPoolSize=-1)

        self.assertRaises(ValueError, MongoClient, host=host, port=port, maxPoolSize="foo")

        c = MongoClient(host=host, port=port, maxPoolSize=100, connect=False)
        self.assertEqual(c.options.pool_options.max_pool_size, 100)

    def test_no_disconnect(self):
        run_cases(self.c, [NonUnique, Unique, InsertOneAndFind])

    def test_pool_reuses_open_socket(self):
        # Test Pool's _check_closed() method doesn't close a healthy socket.
        cx_pool = self.create_pool(max_pool_size=10)
        cx_pool._check_interval_seconds = 0  # Always check.
        with cx_pool.get_socket() as sock_info:
            pass

        with cx_pool.get_socket() as new_sock_info:
            self.assertEqual(sock_info, new_sock_info)

        self.assertEqual(1, len(cx_pool.sockets))

    def test_get_socket_and_exception(self):
        # get_socket() returns socket after a non-network error.
        cx_pool = self.create_pool(max_pool_size=1, wait_queue_timeout=1)
        with self.assertRaises(ZeroDivisionError):
            with cx_pool.get_socket() as sock_info:
                1 / 0

        # Socket was returned, not closed.
        with cx_pool.get_socket() as new_sock_info:
            self.assertEqual(sock_info, new_sock_info)

        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_closed_socket(self):
        # Test that Pool removes explicitly closed socket.
        cx_pool = self.create_pool()

        with cx_pool.get_socket() as sock_info:
            # Use SocketInfo's API to close the socket.
            sock_info.close_socket(None)

        self.assertEqual(0, len(cx_pool.sockets))

    def test_pool_removes_dead_socket(self):
        # Test that Pool removes dead socket and the socket doesn't return
        # itself PYTHON-344
        cx_pool = self.create_pool(max_pool_size=1, wait_queue_timeout=1)
        cx_pool._check_interval_seconds = 0  # Always check.

        with cx_pool.get_socket() as sock_info:
            # Simulate a closed socket without telling the SocketInfo it's
            # closed.
            sock_info.sock.close()
            self.assertTrue(sock_info.socket_closed())

        with cx_pool.get_socket() as new_sock_info:
            self.assertEqual(0, len(cx_pool.sockets))
            self.assertNotEqual(sock_info, new_sock_info)

        self.assertEqual(1, len(cx_pool.sockets))

        # Semaphore was released.
        with cx_pool.get_socket():
            pass

    def test_socket_closed(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((client_context.host, client_context.port))
        socket_checker = SocketChecker()
        self.assertFalse(socket_checker.socket_closed(s))
        s.close()
        self.assertTrue(socket_checker.socket_closed(s))

    def test_socket_checker(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((client_context.host, client_context.port))
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

    def test_return_socket_after_reset(self):
        pool = self.create_pool()
        with pool.get_socket() as sock:
            self.assertEqual(pool.active_sockets, 1)
            self.assertEqual(pool.operation_count, 1)
            pool.reset()

        self.assertTrue(sock.closed)
        self.assertEqual(0, len(pool.sockets))
        self.assertEqual(pool.active_sockets, 0)
        self.assertEqual(pool.operation_count, 0)

    def test_pool_check(self):
        # Test that Pool recovers from two connection failures in a row.
        # This exercises code at the end of Pool._check().
        cx_pool = self.create_pool(max_pool_size=1, connect_timeout=1, wait_queue_timeout=1)
        cx_pool._check_interval_seconds = 0  # Always check.
        self.addCleanup(cx_pool.close)

        with cx_pool.get_socket() as sock_info:
            # Simulate a closed socket without telling the SocketInfo it's
            # closed.
            sock_info.sock.close()

        # Swap pool's address with a bad one.
        address, cx_pool.address = cx_pool.address, ("foo.com", 1234)
        with self.assertRaises(AutoReconnect):
            with cx_pool.get_socket():
                pass

        # Back to normal, semaphore was correctly released.
        cx_pool.address = address
        with cx_pool.get_socket():
            pass

    def test_wait_queue_timeout(self):
        wait_queue_timeout = 2  # Seconds
        pool = self.create_pool(max_pool_size=1, wait_queue_timeout=wait_queue_timeout)
        self.addCleanup(pool.close)

        with pool.get_socket():
            start = time.time()
            with self.assertRaises(ConnectionFailure):
                with pool.get_socket():
                    pass

        duration = time.time() - start
        self.assertTrue(
            abs(wait_queue_timeout - duration) < 1,
            "Waited %.2f seconds for a socket, expected %f" % (duration, wait_queue_timeout),
        )

    def test_no_wait_queue_timeout(self):
        # Verify get_socket() with no wait_queue_timeout blocks forever.
        pool = self.create_pool(max_pool_size=1)
        self.addCleanup(pool.close)

        # Reach max_size.
        with pool.get_socket() as s1:
            t = SocketGetter(self.c, pool)
            t.start()
            while t.state != "get_socket":
                time.sleep(0.1)

            time.sleep(1)
            self.assertEqual(t.state, "get_socket")

        while t.state != "sock":
            time.sleep(0.1)

        self.assertEqual(t.state, "sock")
        self.assertEqual(t.sock, s1)

    def test_checkout_more_than_max_pool_size(self):
        pool = self.create_pool(max_pool_size=2)

        socks = []
        for _ in range(2):
            # Call 'pin_cursor' so we can hold the socket.
            with pool.get_socket() as sock:
                sock.pin_cursor()
                socks.append(sock)

        threads = []
        for _ in range(30):
            t = SocketGetter(self.c, pool)
            t.start()
            threads.append(t)
        time.sleep(1)
        for t in threads:
            self.assertEqual(t.state, "get_socket")

        for socket_info in socks:
            socket_info.close_socket(None)

    def test_maxConnecting(self):
        client = rs_or_single_client()
        self.addCleanup(client.close)
        self.client.test.test.insert_one({})
        self.addCleanup(self.client.test.test.delete_many, {})
        pool = get_pool(client)
        docs = []

        # Run 50 short running operations
        def find_one():
            docs.append(client.test.test.find_one({}))

        threads = [threading.Thread(target=find_one) for _ in range(50)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)

        self.assertEqual(len(docs), 50)
        self.assertLessEqual(len(pool.sockets), 50)
        # TLS and auth make connection establishment more expensive than
        # the query which leads to more threads hitting maxConnecting.
        # The end result is fewer total connections and better latency.
        if client_context.tls and client_context.auth_enabled:
            self.assertLessEqual(len(pool.sockets), 30)
        else:
            self.assertLessEqual(len(pool.sockets), 50)
        # MongoDB 4.4.1 with auth + ssl:
        # maxConnecting = 2:         6 connections in ~0.231+ seconds
        # maxConnecting = unbounded: 50 connections in ~0.642+ seconds
        #
        # MongoDB 4.4.1 with no-auth no-ssl Python 3.8:
        # maxConnecting = 2:         15-22 connections in ~0.108+ seconds
        # maxConnecting = unbounded: 30+ connections in ~0.140+ seconds
        print(len(pool.sockets))


class TestPoolMaxSize(_TestPoolingBase):
    def test_max_pool_size(self):
        max_pool_size = 4
        c = rs_or_single_client(maxPoolSize=max_pool_size)
        self.addCleanup(c.close)
        collection = c[DB].test

        # Need one document.
        collection.drop()
        collection.insert_one({})

        # nthreads had better be much larger than max_pool_size to ensure that
        # max_pool_size sockets are actually required at some point in this
        # test's execution.
        cx_pool = get_pool(c)
        nthreads = 10
        threads = []
        lock = threading.Lock()
        self.n_passed = 0

        def f():
            for _ in range(5):
                collection.find_one({"$where": delay(0.1)})
                assert len(cx_pool.sockets) <= max_pool_size

            with lock:
                self.n_passed += 1

        for i in range(nthreads):
            t = threading.Thread(target=f)
            threads.append(t)
            t.start()

        joinall(threads)
        self.assertEqual(nthreads, self.n_passed)
        self.assertTrue(len(cx_pool.sockets) > 1)
        self.assertEqual(0, cx_pool.requests)

    def test_max_pool_size_none(self):
        c = rs_or_single_client(maxPoolSize=None)
        self.addCleanup(c.close)
        collection = c[DB].test

        # Need one document.
        collection.drop()
        collection.insert_one({})

        cx_pool = get_pool(c)
        nthreads = 10
        threads = []
        lock = threading.Lock()
        self.n_passed = 0

        def f():
            for _ in range(5):
                collection.find_one({"$where": delay(0.1)})

            with lock:
                self.n_passed += 1

        for i in range(nthreads):
            t = threading.Thread(target=f)
            threads.append(t)
            t.start()

        joinall(threads)
        self.assertEqual(nthreads, self.n_passed)
        self.assertTrue(len(cx_pool.sockets) > 1)
        self.assertEqual(cx_pool.max_pool_size, float("inf"))

    def test_max_pool_size_zero(self):
        c = rs_or_single_client(maxPoolSize=0)
        self.addCleanup(c.close)
        pool = get_pool(c)
        self.assertEqual(pool.max_pool_size, float("inf"))

    def test_max_pool_size_with_connection_failure(self):
        # The pool acquires its semaphore before attempting to connect; ensure
        # it releases the semaphore on connection failure.
        test_pool = Pool(
            ("somedomainthatdoesntexist.org", 27017),
            PoolOptions(max_pool_size=1, connect_timeout=1, socket_timeout=1, wait_queue_timeout=1),
        )
        test_pool.ready()

        # First call to get_socket fails; if pool doesn't release its semaphore
        # then the second call raises "ConnectionFailure: Timed out waiting for
        # socket from pool" instead of AutoReconnect.
        for i in range(2):
            with self.assertRaises(AutoReconnect) as context:
                with test_pool.get_socket():
                    pass

            # Testing for AutoReconnect instead of ConnectionFailure, above,
            # is sufficient right *now* to catch a semaphore leak. But that
            # seems error-prone, so check the message too.
            self.assertNotIn("waiting for socket from pool", str(context.exception))


if __name__ == "__main__":
    unittest.main()
