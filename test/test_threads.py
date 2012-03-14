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

"""Test that pymongo is thread safe."""

import unittest
import threading
import traceback

from nose.plugins.skip import SkipTest

from test.utils import server_started_with_auth
from test_connection import get_connection
from pymongo.errors import (AutoReconnect,
                            OperationFailure,
                            DuplicateKeyError)


class AutoAuthenticateThreads(threading.Thread):

    def __init__(self, collection, num):
        threading.Thread.__init__(self)
        self.coll = collection
        self.num = num
        self.success = True

    def run(self):
        try:
            for i in xrange(self.num):
                self.coll.insert({'num':i}, safe=True)
                self.coll.find_one({'num':i})
        except Exception:
            traceback.print_exc()
            self.success = False


class SaveAndFind(threading.Thread):

    def __init__(self, collection):
        threading.Thread.__init__(self)
        self.collection = collection

    def run(self):
        sum = 0
        for document in self.collection.find():
            sum += document["x"]
        assert sum == 499500, "sum was %d not 499500" % sum


class Insert(threading.Thread):

    def __init__(self, collection, n, expect_exception):
        threading.Thread.__init__(self)
        self.collection = collection
        self.n = n
        self.expect_exception = expect_exception

    def run(self):
        for _ in xrange(self.n):
            error = True

            try:
                self.collection.insert({"test": "insert"}, safe=True)
                error = False
            except:
                if not self.expect_exception:
                    raise

            if self.expect_exception:
                assert error


class Update(threading.Thread):

    def __init__(self, collection, n, expect_exception):
        threading.Thread.__init__(self)
        self.collection = collection
        self.n = n
        self.expect_exception = expect_exception

    def run(self):
        for _ in xrange(self.n):
            error = True

            try:
                self.collection.update({"test": "unique"},
                                       {"$set": {"test": "update"}}, safe=True)
                error = False
            except:
                if not self.expect_exception:
                    raise

            if self.expect_exception:
                assert error


class IgnoreAutoReconnect(threading.Thread):

    def __init__(self, collection, n):
        threading.Thread.__init__(self)
        self.c = collection
        self.n = n

    def run(self):
        for _ in range(self.n):
            try:
                self.c.find_one()
            except AutoReconnect:
                pass


class BaseTestThreads(object):
    """
    Base test class for TestThreads and TestThreadsReplicaSet. (This is not
    itself a unittest.TestCase, otherwise it'd be run twice -- once when nose
    imports this module, and once when nose imports
    test_threads_replica_set_connection.py, which imports this module.)
    """
    def setUp(self):
        self.db = self._get_connection().pymongo_test

    def tearDown(self):
        pass

    def _get_connection(self):
        """
        Intended for overriding in TestThreadsReplicaSet. This method
        returns a Connection here, and a ReplicaSetConnection in
        test_threads_replica_set_connection.py.
        """
        # Regular test connection
        return get_connection()

    def test_threading(self):
        self.db.drop_collection("test")
        for i in xrange(1000):
            self.db.test.save({"x": i}, safe=True)

        threads = []
        for i in range(10):
            t = SaveAndFind(self.db.test)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    def test_safe_insert(self):
        self.db.drop_collection("test1")
        self.db.test1.insert({"test": "insert"})
        self.db.drop_collection("test2")
        self.db.test2.insert({"test": "insert"})

        self.db.test2.create_index("test", unique=True)
        self.db.test2.find_one()

        okay = Insert(self.db.test1, 2000, False)
        error = Insert(self.db.test2, 2000, True)

        error.start()
        okay.start()

        error.join()
        okay.join()

    def test_safe_update(self):
        self.db.drop_collection("test1")
        self.db.test1.insert({"test": "update"})
        self.db.test1.insert({"test": "unique"})
        self.db.drop_collection("test2")
        self.db.test2.insert({"test": "update"})
        self.db.test2.insert({"test": "unique"})

        self.db.test2.create_index("test", unique=True)
        self.db.test2.find_one()

        okay = Update(self.db.test1, 2000, False)
        error = Update(self.db.test2, 2000, True)

        error.start()
        okay.start()

        error.join()
        okay.join()

    def test_low_network_timeout(self):
        db = None
        i = 0
        n = 10
        while db is None and i < n:
            try:
                db = get_connection(network_timeout=0.0001).pymongo_test
            except AutoReconnect:
                i += 1
        if i == n:
            raise SkipTest()

        threads = []
        for _ in range(4):
            t = IgnoreAutoReconnect(db.test, 100)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()


class BaseTestThreadsAuth(object):
    """
    Base test class for TestThreadsAuth and TestThreadsAuthReplicaSet. (This is
    not itself a unittest.TestCase, otherwise it'd be run twice -- once when
    nose imports this module, and once when nose imports
    test_threads_replica_set_connection.py, which imports this module.)
    """
    def _get_connection(self):
        """
        Intended for overriding in TestThreadsAuthReplicaSet. This method
        returns a Connection here, and a ReplicaSetConnection in
        test_threads_replica_set_connection.py.
        """
        # Regular test connection
        return get_connection()

    def setUp(self):
        self.conn = self._get_connection()
        if not server_started_with_auth(self.conn):
            raise SkipTest("Authentication is not enabled on server")
        self.conn.admin.system.users.remove({})
        self.conn.admin.add_user('admin-user', 'password')
        self.conn.admin.authenticate("admin-user", "password")
        self.conn.auth_test.system.users.remove({})
        self.conn.auth_test.add_user("test-user", "password")

    def tearDown(self):
        # Remove auth users from databases
        self.conn.admin.authenticate("admin-user", "password")
        self.conn.admin.system.users.remove({})
        self.conn.auth_test.system.users.remove({})
        self.conn.drop_database('auth_test')

    def test_auto_auth_login(self):
        conn = self._get_connection()
        self.assertRaises(OperationFailure, conn.auth_test.test.find_one)

        # Admin auth
        conn = self._get_connection()
        conn.admin.authenticate("admin-user", "password")

        nthreads = 10
        threads = []
        for _ in xrange(nthreads):
            t = AutoAuthenticateThreads(conn.auth_test.test, 100)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
            self.assertTrue(t.success)

        # Database-specific auth
        conn = self._get_connection()
        conn.auth_test.authenticate("test-user", "password")

        threads = []
        for _ in xrange(nthreads):
            t = AutoAuthenticateThreads(conn.auth_test.test, 100)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
            self.assertTrue(t.success)

class TestThreads(BaseTestThreads, unittest.TestCase):
    pass

class TestThreadsAuth(BaseTestThreadsAuth, unittest.TestCase):
    pass

if __name__ == "__main__":
    unittest.main()
