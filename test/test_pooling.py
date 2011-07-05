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
import time
import unittest
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo.connection import Connection, _Pool
from pymongo.errors import ConfigurationError
from test_connection import get_connection

N = 50
DB = "pymongo-pooling-tests"


class MongoThread(threading.Thread):

    def __init__(self, test_case):
        threading.Thread.__init__(self)
        self.connection = test_case.c
        self.db = self.connection[DB]
        self.ut = test_case


class SaveAndFind(MongoThread):

    def run(self):
        for _ in xrange(N):
            rand = random.randint(0, N)
            id = self.db.sf.save({"x": rand}, safe=True)
            self.ut.assertEqual(rand, self.db.sf.find_one(id)["x"])
            self.connection.end_request()


class Unique(MongoThread):

    def run(self):
        for _ in xrange(N):
            self.db.unique.insert({})
            self.ut.assertEqual(None, self.db.error())
            self.connection.end_request()


class NonUnique(MongoThread):

    def run(self):
        for _ in xrange(N):
            self.db.unique.insert({"_id": "mike"})
            self.ut.assertNotEqual(None, self.db.error())
            self.connection.end_request()


class Disconnect(MongoThread):

    def run(self):
        for _ in xrange(N):
            self.connection.disconnect()


class NoRequest(MongoThread):

    def run(self):
        errors = 0
        for _ in xrange(N):
            self.db.unique.insert({"_id": "mike"})
            if self.db.error() is None:
                errors += 1

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


class OneOp(threading.Thread):

    def __init__(self, connection):
        threading.Thread.__init__(self)
        self.c = connection

    def run(self):
        assert len(self.c._Connection__pool.sockets) == 1
        self.c.test.test.find_one()
        assert len(self.c._Connection__pool.sockets) == 0
        self.c.end_request()
        assert len(self.c._Connection__pool.sockets) == 1


class CreateAndReleaseSocket(threading.Thread):

    def __init__(self, connection):
        threading.Thread.__init__(self)
        self.c = connection

    def run(self):
        self.c.test.test.find_one()
        time.sleep(1)
        self.c.end_request()


class TestPooling(unittest.TestCase):

    def setUp(self):
        self.c = get_connection()

        # reset the db
        self.c.drop_database(DB)
        self.c[DB].unique.insert({"_id": "mike"})
        self.c[DB].unique.find_one()

    def tearDown(self):
        self.c = None

    def test_max_pool_size_validation(self):
        self.assertRaises(ValueError, Connection, max_pool_size=-1)
        self.assertRaises(AssertionError, Connection, max_pool_size='foo')
        self.assertRaises(ConfigurationError, Connection,
                          'mongodb://localhost/?maxPoolSize=-1')
        self.assertRaises(ConfigurationError, Connection,
                          'mongodb://localhost/?maxPoolSize=foo')
        self.assertRaises(ConfigurationError, Connection,
                          'mongodb://localhost/?maxPoolSize=5.5')
        c = Connection('mongodb://localhost/?maxPoolSize=5')
        self.assertEqual(c.max_pool_size, 5)
        c = Connection(max_pool_size=100)
        self.assertEqual(c.max_pool_size, 100)

    def test_no_disconnect(self):
        run_cases(self, [NoRequest, NonUnique, Unique, SaveAndFind])

    def test_simple_disconnect(self):
        self.c.test.stuff.find()
        self.assertEqual(0, len(self.c._Connection__pool.sockets))
        if hasattr(self.c._Connection__pool, 'sock'):
          self.assertNotEqual(None, self.c._Connection__pool.sock)
        self.c.end_request()
        self.assertEqual(1, len(self.c._Connection__pool.sockets))
        if hasattr(self.c._Connection__pool, 'sock'):
          self.assertEqual(None, self.c._Connection__pool.sock)
        self.c.disconnect()
        self.assertEqual(0, len(self.c._Connection__pool.sockets))
        if hasattr(self.c._Connection__pool, 'sock'):
          self.assertEqual(None, self.c._Connection__pool.sock)

    def test_disconnect(self):
        run_cases(self, [SaveAndFind, Disconnect, Unique])

    def test_independent_pools(self):
        p = _Pool(10, 0)
        self.assertEqual([], p.sockets)
        self.c.end_request()
        self.assertEqual([], p.sockets)

    def test_dependent_pools(self):
        c = get_connection()
        self.assertEqual(1, len(c._Connection__pool.sockets))
        c.test.test.find_one()
        self.assertEqual(0, len(c._Connection__pool.sockets))
        c.end_request()
        self.assertEqual(1, len(c._Connection__pool.sockets))

        t = OneOp(c)
        t.start()
        t.join()

        self.assertEqual(1, len(c._Connection__pool.sockets))
        c.test.test.find_one()
        self.assertEqual(0, len(c._Connection__pool.sockets))

    def test_multiple_connections(self):
        a = get_connection()
        b = get_connection()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))

        a.test.test.find_one()
        a.end_request()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))
        a_sock = a._Connection__pool.sockets[0]

        b.end_request()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))

        b.test.test.find_one()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(0, len(b._Connection__pool.sockets))

        b.end_request()
        b_sock = b._Connection__pool.sockets[0]
        b.test.test.find_one()
        a.test.test.find_one()
        self.assertEqual(b_sock, b._Connection__pool.get_socket(b.host, b.port))
        self.assertEqual(a_sock, a._Connection__pool.get_socket(a.host, a.port))

    def test_pool_with_fork(self):
        if sys.platform == "win32":
            raise SkipTest()

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest()

        a = get_connection()
        a.test.test.find_one()
        a.end_request()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        a_sock = a._Connection__pool.sockets[0]

        def loop(pipe):
            c = get_connection()
            self.assertEqual(1, len(c._Connection__pool.sockets))
            c.test.test.find_one()
            self.assertEqual(0, len(c._Connection__pool.sockets))
            c.end_request()
            self.assertEqual(1, len(c._Connection__pool.sockets))
            pipe.send(c._Connection__pool.sockets[0].getsockname())

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
        self.assertEqual(a_sock, a._Connection__pool.get_socket(a.host, a.port))

    def test_pool_with_greenlets(self):
        try:
            from greenlet import greenlet
        except ImportError:
            raise SkipTest()

        c = get_connection()
        c.test.test.find_one()
        c.end_request()
        self.assertEqual(1, len(c._Connection__pool.sockets))
        a_sock = c._Connection__pool.sockets[0]

        def loop(name, pipe):
            c.test.test.find_one()
            self.assertEqual(0, len(c._Connection__pool.sockets))
            greenlet.getcurrent().parent.switch()
            c.end_request()
            pipe.append(c._Connection__pool.sockets[-1])

        ga1 = []
        ga2 = []

        g1 = greenlet(loop)
        g2 = greenlet(loop)

        g1.switch('g1', ga1)
        g2.switch('g2', ga2)
        g1.switch()
        g2.switch()

        b_sock = ga1[0]
        c_sock = ga2[0]
        self.assert_(a_sock is b_sock)
        self.assert_(a_sock is not c_sock)
        self.assert_(b_sock is not c_sock)


    def test_max_pool_size(self):
        c = get_connection(max_pool_size=4)

        threads = []
        for i in range(40):
            t = CreateAndReleaseSocket(c)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        # There's a race condition, so be lenient
        self.assert_(abs(4 - len(c._Connection__pool.sockets)) < 4)


if __name__ == "__main__":
    unittest.main()
