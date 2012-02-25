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
import socket
import sys
import threading
import unittest

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo.connection import Connection
from pymongo.pool import Pool, NO_REQUEST, NO_SOCKET_YET
from pymongo.errors import ConfigurationError
from test_connection import get_connection
from test.utils import delay

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
            thread = case(ut)
            thread.start()
            threads.append(thread)

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

        sock = one(pool.sockets)

        self.c.start_request()

        # start_request() hasn't yet moved the socket from the general pool into
        # the request
        assert len(pool.sockets) == 1
        assert one(pool.sockets) == sock

        self.c[DB].test.find_one()

        # find_one() causes the socket to be used in the request, so now it's
        # bound to this thread
        assert len(pool.sockets) == 0
        assert pool.thread_local.sock == sock
        self.c.end_request()

        # The socket is back in the pool
        assert len(pool.sockets) == 1
        assert one(pool.sockets) == sock

        self.passed = True


class CreateAndReleaseSocket(threading.Thread):

    def __init__(self, connection, start_request, end_request):
        threading.Thread.__init__(self)
        self.c = connection
        self.start_request = start_request
        self.end_request = end_request
        self.passed = False

    def run(self):
        # Do an operation that requires a socket for .25 seconds.
        # test_max_pool_size uses this to spin up lots of threads requiring
        # lots of simultaneous connections, to ensure that Pool obeys its
        # max_size configuration and closes extra sockets as they're returned.
        # We need a delay here to ensure that more than max_size sockets are
        # needed at once.
        if self.start_request:
            self.c.start_request()

        # A reasonable number of docs here, depending on how many are inserted
        # in TestPooling.setUp()
        list(self.c[DB].test.find())
        if self.end_request:
            self.c.end_request()
        self.passed = True

class TestPooling(unittest.TestCase):

    def setUp(self):
        self.c = get_connection(auto_start_request=False)

        # reset the db
        self.c.drop_database(DB)
        self.c[DB].unique.insert({"_id": "mike"})
        self.c[DB].test.insert([{} for i in range(1000)])

    def tearDown(self):
        self.c = None

    def assert_no_request(self):
        self.assertEqual(
            NO_REQUEST, self.c._Connection__pool._get_request_socket()
        )

    def assert_request_without_socket(self):
        self.assertEqual(
            NO_SOCKET_YET, self.c._Connection__pool._get_request_socket()
        )

    def assert_request_with_socket(self):
        self.assert_(isinstance(
            self.c._Connection__pool._get_request_socket(), socket.socket
        ))

    def assert_pool_size(self, pool_size):
        self.assertEqual(
            pool_size, len(self.c._Connection__pool.sockets)
        )

    def test_max_pool_size_validation(self):
        self.assertRaises(
            ValueError, Connection, host=host, port=port, max_pool_size=-1
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
        self.assert_(t.passed, "OneOp.run() threw exception")

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
                         one(b._Connection__pool.get_socket((b.host, b.port))))
        self.assertEqual(a_sock,
                         one(a._Connection__pool.get_socket((a.host, a.port))))

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
            pipe.send(one(c._Connection__pool.sockets).getsockname())

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
        self.assert_(a_sock.getsockname() != b_sock)
        self.assert_(a_sock.getsockname() != c_sock)
        self.assert_(b_sock != c_sock)
        self.assertEqual(a_sock,
                         one(a._Connection__pool.get_socket((a.host, a.port))))

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
            self.assertEqual({'_id':1}, coll.find_one())

            # Pool has detected that we forked, and it ended the request
            self.assert_no_request()
            pipe.send("success")

        parent_conn, child_conn = Pipe()
        p = Process(target=f, args=(child_conn,))
        p.start()
        p.join(1)
        p.terminate()
        child_conn.close()

        # Child proc ended request
        self.assertEqual("success", parent_conn.recv())

        # This proc's request continues
        self.assert_request_with_socket()

    def _test_max_pool_size(self, c, start_request, end_request):
        threads = []
        for i in range(40):
            t = CreateAndReleaseSocket(c, start_request, end_request)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        for t in threads:
            self.assert_(t.passed)

        # There's a race condition, so be lenient
        nsock = len(c._Connection__pool.sockets)
        self.assert_(
            abs(4 - nsock) < 4,
            "Expected about 4 sockets in the pool, got %d" % nsock
        )

    def test_max_pool_size(self):
        c = get_connection(max_pool_size=4, auto_start_request=False)
        self._test_max_pool_size(c, False, False)

    def test_max_pool_size_with_request(self):
        c = get_connection(max_pool_size=4, auto_start_request=False)
        self._test_max_pool_size(c, True, True)

    def test_max_pool_size_with_leaked_request(self):
        # Call start_request() but not end_request() -- this will leak requests,
        # meaning sockets are closed when the requests end, rather than being
        # returned to the general pool.
        c = get_connection(max_pool_size=4, auto_start_request=False)
        self.assertRaises(
            AssertionError,
            self._test_max_pool_size,
            c,
            start_request=True,
            end_request=False
        )

    def test_max_pool_size_with_end_request_only(self):
        # Call end_request() but not start_request()
        c = get_connection(max_pool_size=4, auto_start_request=False)
        self._test_max_pool_size(c, False, True)

if __name__ == "__main__":
    unittest.main()
