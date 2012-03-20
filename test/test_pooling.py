# Copyright 2009-2010 10gen, Inc.
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

"""Test built in connection-pooling."""

import os
import random
import sys
import threading
import unittest
import thread
import time

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo.connection import Connection
from pymongo.pool import Pool, NO_REQUEST, NO_SOCKET_YET, SocketInfo
from pymongo.errors import ConfigurationError
from test_connection import get_connection
from test.utils import delay, force_reclaim_sockets

N = 50
DB = "pymongo-pooling-tests"
host = os.environ.get("DB_IP", "localhost")
port = int(os.environ.get("DB_PORT", 27017))


def one(s):
    """Get one element of a set"""
    return iter(s).next()


class MongoThread(threading.Thread):

    def __init__(self, test_case):
        threading.Thread.__init__(self)
        self.connection = test_case.c
        self.db = self.connection[DB]
        self.ut = test_case
        self.passed = False

    def run(self):
        self.run_mongo_thread()

        # No exceptions thrown
        self.passed = True

    def run_mongo_thread(self):
        raise NotImplementedError()


class SaveAndFind(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            rand = random.randint(0, N)
            id = self.db.sf.save({"x": rand}, safe=True)
            self.ut.assertEqual(rand, self.db.sf.find_one(id)["x"])


class Unique(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.connection.start_request()
            self.db.unique.insert({})
            self.ut.assertEqual(None, self.db.error())
            self.connection.end_request()


class NonUnique(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.connection.start_request()
            self.db.unique.insert({"_id": "mike"})
            self.ut.assertNotEqual(None, self.db.error())
            self.connection.end_request()


class Disconnect(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.connection.disconnect()


class NoRequest(MongoThread):

    def run_mongo_thread(self):
        self.connection.start_request()
        errors = 0
        for _ in xrange(N):
            self.db.unique.insert({"_id": "mike"})
            if self.db.error() is None:
                errors += 1

        self.connection.end_request()
        self.ut.assertEqual(0, errors)


def run_cases(ut, cases):
    threads = []
    for case in cases:
        for i in range(10):
            t = case(ut)
            t.start()
            threads.append(t)

    for t in threads:
        t.join()

    for t in threads:
        assert t.passed, "%s.run_mongo_thread() threw an exception" % repr(t)


class OneOp(threading.Thread):

    def __init__(self, connection):
        threading.Thread.__init__(self)
        self.c = connection
        self.passed = False

    def run(self):
        pool = self.c._Connection__pool
        assert len(pool.sockets) == 1, "Expected 1 socket, found %d" % (
            len(pool.sockets)
        )

        sock_info = one(pool.sockets)

        self.c.start_request()

        # start_request() hasn't yet moved the socket from the general pool into
        # the request
        assert len(pool.sockets) == 1
        assert one(pool.sockets) == sock_info

        self.c[DB].test.find_one()

        # find_one() causes the socket to be used in the request, so now it's
        # bound to this thread
        assert len(pool.sockets) == 0
        assert pool._get_request_state() == sock_info
        self.c.end_request()

        # The socket is back in the pool
        assert len(pool.sockets) == 1
        assert one(pool.sockets) == sock_info

        self.passed = True


class CreateAndReleaseSocket(threading.Thread):

    def __init__(self, connection, start_request, end_request):
        threading.Thread.__init__(self)
        self.c = connection
        self.start_request = start_request
        self.end_request = end_request
        self.passed = False

    def run(self):
        # Do an operation that requires a socket.
        # test_max_pool_size uses this to spin up lots of threads requiring
        # lots of simultaneous connections, to ensure that Pool obeys its
        # max_size configuration and closes extra sockets as they're returned.
        # We need a delay here to ensure that more than max_size sockets are
        # needed at once.
        for i in range(self.start_request):
            self.c.start_request()

        # A reasonable number of docs here, depending on how many are inserted
        # in TestPooling.setUp()
        list(self.c[DB].test.find())
        for i in range(self.end_request):
            self.c.end_request()
        self.passed = True

class TestPooling(unittest.TestCase):

    def setUp(self):
        self.c = get_connection(auto_start_request=False)

        # reset the db
        db = self.c[DB]
        db.unique.drop()
        db.test.drop()
        db.unique.insert({"_id": "mike"})
        db.test.insert([{} for i in range(1000)])

    def tearDown(self):
        self.c.close()

    def assert_no_request(self):
        self.assertEqual(
            NO_REQUEST, self.c._Connection__pool._get_request_state()
        )

    def assert_request_without_socket(self):
        self.assertEqual(
            NO_SOCKET_YET, self.c._Connection__pool._get_request_state()
        )

    def assert_request_with_socket(self):
        self.assertTrue(isinstance(
            self.c._Connection__pool._get_request_state(), SocketInfo
        ))

    def assert_pool_size(self, pool_size):
        self.assertEqual(
            pool_size, len(self.c._Connection__pool.sockets)
        )

    def test_max_pool_size_validation(self):
        self.assertRaises(
            ConfigurationError, Connection, host=host, port=port,
            max_pool_size=-1
        )

        self.assertRaises(
            ConfigurationError, Connection, host=host, port=port,
            max_pool_size='foo'
        )

        c = Connection(host=host, port=port, max_pool_size=100)
        self.assertEqual(c.max_pool_size, 100)

    def test_no_disconnect(self):
        run_cases(self, [NoRequest, NonUnique, Unique, SaveAndFind])

    def test_simple_disconnect(self):
        # Connection just created, expect 1 free socket
        self.assert_pool_size(1)
        self.assert_no_request()

        self.c.start_request()
        self.assert_request_without_socket()
        cursor = self.c[DB].stuff.find()

        # Cursor hasn't actually caused a request yet, so there's still 1 free
        # socket.
        self.assert_pool_size(1)
        self.assert_request_without_socket()

        # Actually make a request to server, triggering a socket to be
        # allocated to the request
        list(cursor)
        self.assert_pool_size(0)
        self.assert_request_with_socket()

        # Pool returns to its original state
        self.c.end_request()
        self.assert_no_request()
        self.assert_pool_size(1)

        self.c.disconnect()
        self.assert_pool_size(0)
        self.assert_no_request()

    def test_disconnect(self):
        run_cases(self, [SaveAndFind, Disconnect, Unique])

    def test_independent_pools(self):
        p = Pool((host, port), 10, None, None, False)
        self.c.start_request()
        self.assertEqual(set(), p.sockets)
        self.c.end_request()
        self.assertEqual(set(), p.sockets)

    def test_dependent_pools(self):
        self.assert_pool_size(1)
        self.c.start_request()
        self.assert_request_without_socket()
        self.c.test.test.find_one()
        self.assert_request_with_socket()
        self.assert_pool_size(0)
        self.c.end_request()
        self.assert_pool_size(1)

        t = OneOp(self.c)
        t.start()
        t.join()
        self.assertTrue(t.passed, "OneOp.run() threw exception")

        self.assert_pool_size(1)
        self.c.test.test.find_one()
        self.assert_pool_size(1)

    def test_multiple_connections(self):
        a = get_connection(auto_start_request=False)
        b = get_connection(auto_start_request=False)
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))

        a.start_request()
        a.test.test.find_one()
        self.assertEqual(0, len(a._Connection__pool.sockets))
        a.end_request()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))
        a_sock = one(a._Connection__pool.sockets)

        b.end_request()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))

        b.start_request()
        b.test.test.find_one()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(0, len(b._Connection__pool.sockets))

        b.end_request()
        b_sock = one(b._Connection__pool.sockets)
        b.test.test.find_one()
        a.test.test.find_one()

        self.assertEqual(b_sock,
                         b._Connection__pool.get_socket((b.host, b.port)))
        self.assertEqual(a_sock,
                         a._Connection__pool.get_socket((a.host, a.port)))

    def test_pool_with_fork(self):
        # Test that separate Connections have separate Pools, and that the
        # driver can create a new Connection after forking
        if sys.platform == "win32":
            raise SkipTest("Can't test forking on Windows")

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest("No multiprocessing module")

        a = get_connection(auto_start_request=False)
        a.test.test.remove(safe=True)
        a.test.test.insert({'_id':1}, safe=True)
        a.test.test.find_one()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        a_sock = one(a._Connection__pool.sockets)

        def loop(pipe):
            c = get_connection(auto_start_request=False)
            self.assertEqual(1,len(c._Connection__pool.sockets))
            c.test.test.find_one()
            self.assertEqual(1,len(c._Connection__pool.sockets))
            pipe.send(one(c._Connection__pool.sockets).sock.getsockname())

        cp1, cc1 = Pipe()
        cp2, cc2 = Pipe()

        p1 = Process(target=loop, args=(cc1,))
        p2 = Process(target=loop, args=(cc2,))

        p1.start()
        p2.start()

        p1.join(1)
        p2.join(1)

        p1.terminate()
        p2.terminate()

        p1.join()
        p2.join()

        cc1.close()
        cc2.close()

        b_sock = cp1.recv()
        c_sock = cp2.recv()
        self.assertTrue(a_sock.sock.getsockname() != b_sock)
        self.assertTrue(a_sock.sock.getsockname() != c_sock)
        self.assertTrue(b_sock != c_sock)
        self.assertEqual(a_sock,
                         a._Connection__pool.get_socket((a.host, a.port)))

    def test_request_with_fork(self):
        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest("No multiprocessing module")

        coll = self.c.test.test
        coll.remove(safe=True)
        coll.insert({'_id': 1}, safe=True)
        coll.find_one()
        self.assert_pool_size(1)
        self.c.start_request()
        self.assert_pool_size(1)
        coll.find_one()
        self.assert_pool_size(0)
        self.assert_request_with_socket()

        def f(pipe):
            # We can still query server without error
            self.assertEqual({'_id':1}, coll.find_one())

            # Pool has detected that we forked, but resumed the request
            self.assert_request_with_socket()
            self.assert_pool_size(0)
            pipe.send("success")

        parent_conn, child_conn = Pipe()
        p = Process(target=f, args=(child_conn,))
        p.start()
        p.join(1)
        p.terminate()
        child_conn.close()
        self.assertEqual("success", parent_conn.recv())

    def test_request(self):
        # Check that Pool gives two different sockets in two calls to
        # get_socket() -- doesn't automatically put us in a request any more
        cx_pool = Pool(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False
        )

        sock0 = cx_pool.get_socket()
        sock1 = cx_pool.get_socket()

        self.assertNotEqual(sock0, sock1)

        # Now in a request, we'll get the same socket both times
        cx_pool.start_request()

        sock2 = cx_pool.get_socket()
        sock3 = cx_pool.get_socket()
        self.assertEqual(sock2, sock3)

        # Pool didn't keep reference to sock0 or sock1; sock2 and 3 are new
        self.assertNotEqual(sock0, sock2)
        self.assertNotEqual(sock1, sock2)

        # Return the request sock to pool
        cx_pool.end_request()

        sock4 = cx_pool.get_socket()
        sock5 = cx_pool.get_socket()

        # Not in a request any more, we get different sockets
        self.assertNotEqual(sock4, sock5)

        # end_request() returned sock2 to pool
        self.assertEqual(sock4, sock2)

    def test_reset_and_request(self):
        # reset() is called after a fork, or after a socket error. Ensure that
        # a new request is begun if a request was in progress when the reset()
        # occurred, otherwise no request is begun.
        p = Pool((host, port), 10, None, None, False)
        self.assertFalse(p.in_request())
        p.start_request()
        self.assertTrue(p.in_request())
        p.reset()
        self.assertTrue(p.in_request())
        p.end_request()
        self.assertFalse(p.in_request())
        p.reset()
        self.assertFalse(p.in_request())

    def test_primitive_thread(self):
        p = Pool((host, port), 10, None, None, False)

        # Test that start/end_request work with a thread begun from thread
        # module, rather than threading module
        lock = thread.allocate_lock()
        lock.acquire()

        def run_in_request():
            p.start_request()
            p.get_socket()
            p.end_request()
            lock.release()

        thread.start_new_thread(run_in_request, ())

        # Join thread
        acquired = False
        for i in range(10):
            time.sleep(0.1)
            acquired = lock.acquire(0)
            if acquired:
                break

        self.assertTrue(acquired, "Thread is hung")

    def test_socket_reclamation(self):
        # Check that if a thread starts a request and dies without ending
        # the request, that the socket is reclaimed into the pool.
        cx_pool = Pool(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False,
        )

        self.assertEqual(0, len(cx_pool.sockets))

        lock = thread.allocate_lock()
        lock.acquire()

        the_sock = [None]

        def leak_request():
            self.assertEqual(NO_REQUEST, cx_pool.local.sock_info)
            cx_pool.start_request()
            self.assertEqual(NO_SOCKET_YET, cx_pool.local.sock_info)
            sock_info = cx_pool.get_socket()
            self.assertEqual(sock_info, cx_pool.local.sock_info)
            the_sock[0] = id(sock_info.sock)
            lock.release()

        # Start a thread WITHOUT a threading.Thread - important to test that
        # Pool can deal with primitive threads.
        thread.start_new_thread(leak_request, ())

        # Join thread
        acquired = lock.acquire(1)
        self.assertTrue(acquired, "Thread is hung")

        force_reclaim_sockets(cx_pool, 1)

        # Pool reclaimed the socket
        self.assertEqual(1, len(cx_pool.sockets))
        self.assertEqual(the_sock[0], id((iter(cx_pool.sockets).next()).sock))

    def _test_max_pool_size(self, c, start_request, end_request):
        threads = []
        for i in range(40):
            t = CreateAndReleaseSocket(c, start_request, end_request)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        for t in threads:
            self.assertTrue(t.passed)

        # Critical: release refs to threads, so SocketInfo.__del__() executes
        del threads
        t = None

        cx_pool = c._Connection__pool
        force_reclaim_sockets(cx_pool, 4)

        # There's a race condition, so be lenient
        nsock = len(cx_pool.sockets)
        self.assertTrue(
            abs(4 - nsock) < 4,
            "Expected about 4 sockets in the pool, got %d" % nsock
        )

    def test_max_pool_size(self):
        c = get_connection(max_pool_size=4, auto_start_request=False)
        self._test_max_pool_size(c, 0, 0)

    def test_max_pool_size_with_request(self):
        c = get_connection(max_pool_size=4, auto_start_request=False)
        self._test_max_pool_size(c, 1, 1)

    def test_max_pool_size_with_redundant_request(self):
        c = get_connection(max_pool_size=4, auto_start_request=False)
        self._test_max_pool_size(c, 2, 1)
        self._test_max_pool_size(c, 20, 1)

    def test_max_pool_size_with_leaked_request(self):
        # Call start_request() but not end_request() -- when threads die, they
        # should return their request sockets to the pool.
        c = get_connection(max_pool_size=4, auto_start_request=False)
        self._test_max_pool_size(c, 1, 0)

    def test_max_pool_size_with_end_request_only(self):
        # Call end_request() but not start_request()
        c = get_connection(max_pool_size=4, auto_start_request=False)
        self._test_max_pool_size(c, 0, 1)


if __name__ == "__main__":
    unittest.main()
