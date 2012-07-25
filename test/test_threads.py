# Copyright 2009-2012 10gen, Inc.
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

from test.utils import server_started_with_auth, joinall
from test.test_connection import get_connection
from pymongo.connection import Connection
from pymongo.replica_set_connection import ReplicaSetConnection
from pymongo.pool import SocketInfo, _closed
from pymongo.errors import AutoReconnect, OperationFailure


def get_pool(connection):
    if isinstance(connection, Connection):
        return connection._Connection__pool
    elif isinstance(connection, ReplicaSetConnection):
        writer = connection._ReplicaSetConnection__writer
        pools = connection._ReplicaSetConnection__members
        return pools[writer].pool
    else:
        raise TypeError(str(connection))


class AutoAuthenticateThreads(threading.Thread):

    def __init__(self, collection, num):
        threading.Thread.__init__(self)
        self.coll = collection
        self.num = num
        self.success = True
        self.setDaemon(True)

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
        self.setDaemon(True)

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
        self.setDaemon(True)

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
        self.setDaemon(True)

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
        self.setDaemon(True)

    def run(self):
        for _ in range(self.n):
            try:
                self.c.find_one()
            except AutoReconnect:
                pass


class FindPauseFind(threading.Thread):
    """See test_server_disconnect() for details"""
    @classmethod
    def shared_state(cls, nthreads):
        class SharedState(object):
            pass

        state = SharedState()

        # Number of threads total
        state.nthreads = nthreads

        # Number of threads that have arrived at rendezvous point
        state.arrived_threads = 0
        state.arrived_threads_lock = threading.Lock()

        # set when all threads reach rendezvous
        state.ev_arrived = threading.Event()

        # set from outside FindPauseFind to let threads resume after
        # rendezvous
        state.ev_resume = threading.Event()
        return state

    def __init__(self, collection, state):
        """Params: A collection, an event to signal when all threads have
        done the first find(), an event to signal when threads should resume,
        and the total number of threads
        """
        super(FindPauseFind, self).__init__()
        self.collection = collection
        self.state = state
        self.passed = False

        # If this thread fails to terminate, don't hang the whole program
        self.setDaemon(True)

    def rendezvous(self):
        # pause until all threads arrive here
        s = self.state
        s.arrived_threads_lock.acquire()
        s.arrived_threads += 1
        if s.arrived_threads == s.nthreads:
            s.arrived_threads_lock.release()
            s.ev_arrived.set()
        else:
            s.arrived_threads_lock.release()
            s.ev_arrived.wait()

    def run(self):
        try:
            # acquire a socket
            list(self.collection.find())

            pool = get_pool(self.collection.database.connection)
            socket_info = pool._get_request_state()
            assert isinstance(socket_info, SocketInfo)
            self.request_sock = socket_info.sock
            assert not _closed(self.request_sock)

            # Dereference socket_info so it can potentially return to the pool
            del socket_info
        finally:
            self.rendezvous()

        # all threads have passed the rendezvous, wait for
        # test_server_disconnect() to disconnect the connection
        self.state.ev_resume.wait()

        # test_server_disconnect() has closed this socket, but that's ok
        # because it's not our request socket anymore
        assert _closed(self.request_sock)

        # if disconnect() properly closed all threads' request sockets, then
        # this won't raise AutoReconnect because it will acquire a new socket
        assert self.request_sock == pool._get_request_state().sock
        list(self.collection.find())
        assert self.collection.database.connection.in_request()
        assert self.request_sock != pool._get_request_state().sock
        self.passed = True


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
        # Clear connection reference so that RSC's monitor thread
        # dies.
        self.db = None

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

        joinall(threads)

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

    def test_server_disconnect(self):
        # PYTHON-345, we need to make sure that threads' request sockets are
        # closed by disconnect().
        #
        # 1. Create a connection with auto_start_request=True
        # 2. Start N threads and do a find() in each to get a request socket
        # 3. Pause all threads
        # 4. In the main thread close all sockets, including threads' request
        #       sockets
        # 5. In main thread, do a find(), which raises AutoReconnect and resets
        #       pool
        # 6. Resume all threads, do a find() in them
        #
        # If we've fixed PYTHON-345, then only one AutoReconnect is raised,
        # and all the threads get new request sockets.
        cx = self.db.connection
        self.assertTrue(cx.auto_start_request)
        collection = self.db.pymongo_test

        # acquire a request socket for the main thread
        collection.find_one()
        pool = get_pool(collection.database.connection)
        socket_info = pool._get_request_state()
        assert isinstance(socket_info, SocketInfo)
        request_sock = socket_info.sock

        state = FindPauseFind.shared_state(nthreads=40)

        threads = [
            FindPauseFind(collection, state)
            for _ in range(state.nthreads)
        ]

        # Each thread does a find(), thus acquiring a request socket
        for t in threads:
            t.start()

        # Wait for the threads to reach the rendezvous
        state.ev_arrived.wait(10)
        self.assertTrue(state.ev_arrived.isSet(), "Thread timeout")

        try:
            self.assertEqual(state.nthreads, state.arrived_threads)

            # Simulate an event that closes all sockets, e.g. primary stepdown
            for t in threads:
                t.request_sock.close()

            # Finally, ensure the main thread's socket's last_checkout is
            # updated:
            collection.find_one()

            # ... and close it:
            request_sock.close()

            # Doing an operation on the connection raises an AutoReconnect and
            # resets the pool behind the scenes
            self.assertRaises(AutoReconnect, collection.find_one)

        finally:
            # Let threads do a second find()
            state.ev_resume.set()

        joinall(threads)

        for t in threads:
            self.assertTrue(t.passed, "%s threw exception" % t)


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
        conn = self._get_connection()
        if not server_started_with_auth(conn):
            raise SkipTest("Authentication is not enabled on server")
        self.conn = conn
        self.conn.admin.system.users.remove({})
        try:
            # First admin user add fails gle in MongoDB >= 2.1.2
            # See SERVER-4225 for more information.
            self.conn.admin.add_user('admin-user', 'password')
        except OperationFailure:
            pass
        self.conn.admin.authenticate("admin-user", "password")
        self.conn.auth_test.system.users.remove({})
        self.conn.auth_test.add_user("test-user", "password")

    def tearDown(self):
        # Remove auth users from databases
        self.conn.admin.authenticate("admin-user", "password")
        self.conn.admin.system.users.remove({})
        self.conn.auth_test.system.users.remove({})
        # Clear connection reference so that RSC's monitor thread
        # dies.
        self.conn = None

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

        joinall(threads)

        for t in threads:
            self.assertTrue(t.success)

        # Database-specific auth
        conn = self._get_connection()
        conn.auth_test.authenticate("test-user", "password")

        threads = []
        for _ in xrange(nthreads):
            t = AutoAuthenticateThreads(conn.auth_test.test, 100)
            t.start()
            threads.append(t)

        joinall(threads)

        for t in threads:
            self.assertTrue(t.success)

class TestThreads(BaseTestThreads, unittest.TestCase):
    pass

class TestThreadsAuth(BaseTestThreadsAuth, unittest.TestCase):
    pass


if __name__ == "__main__":
    unittest.main()
