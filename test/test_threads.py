# Copyright 2009-2014 MongoDB, Inc.
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

import threading
import traceback

from test import unittest, client_context, IntegrationTest
from test.utils import (frequent_thread_switches,
                        joinall,
                        remove_all_users,
                        RendezvousThread,
                        rs_or_single_client)
from test.utils import get_pool
from pymongo.pool import SocketInfo, _closed
from pymongo.errors import AutoReconnect, OperationFailure


@client_context.require_connection
def setUpModule():
    pass


class AutoAuthenticateThreads(threading.Thread):

    def __init__(self, collection, num):
        threading.Thread.__init__(self)
        self.coll = collection
        self.num = num
        self.success = True
        self.setDaemon(True)

    def run(self):
        try:
            for i in range(self.num):
                self.coll.insert({'num':i})
                self.coll.find_one({'num':i})
        except Exception:
            traceback.print_exc()
            self.success = False


class SaveAndFind(threading.Thread):

    def __init__(self, collection):
        threading.Thread.__init__(self)
        self.collection = collection
        self.setDaemon(True)
        self.passed = False

    def run(self):
        sum = 0
        for document in self.collection.find():
            sum += document["x"]

        assert sum == 499500, "sum was %d not 499500" % sum
        self.passed = True


class Insert(threading.Thread):

    def __init__(self, collection, n, expect_exception):
        threading.Thread.__init__(self)
        self.collection = collection
        self.n = n
        self.expect_exception = expect_exception
        self.setDaemon(True)

    def run(self):
        for _ in range(self.n):
            error = True

            try:
                self.collection.insert({"test": "insert"})
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
        for _ in range(self.n):
            error = True

            try:
                self.collection.update({"test": "unique"},
                                       {"$set": {"test": "update"}})
                error = False
            except:
                if not self.expect_exception:
                    raise

            if self.expect_exception:
                assert error


class Disconnect(threading.Thread):

    def __init__(self, client, n):
        threading.Thread.__init__(self)
        self.client = client
        self.n = n
        self.passed = False

    def run(self):
        for _ in range(self.n):
            self.client.disconnect()

        self.passed = True


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


class FindPauseFind(RendezvousThread):
    """See test_server_disconnect() for details"""
    def __init__(self, collection, state):
        """Params:
          `collection`: A collection for testing
          `state`: A shared state object from RendezvousThread.shared_state()
        """
        super(FindPauseFind, self).__init__(state)
        self.collection = collection

    def before_rendezvous(self):
        # acquire a socket
        client = self.collection.database.connection
        client.start_request()
        list(self.collection.find())

        pool = get_pool(client)
        socket_info = pool._get_request_state()
        assert isinstance(socket_info, SocketInfo)
        self.request_sock = socket_info.sock
        assert not _closed(self.request_sock)

    def after_rendezvous(self):
        # test_server_disconnect() has closed this socket, but that's ok
        # because it's not our request socket anymore
        assert _closed(self.request_sock)

        # if disconnect() properly replaced the pool, then this won't raise
        # AutoReconnect because it will acquire a new socket
        list(self.collection.find())
        assert self.collection.database.connection.in_request()
        pool = get_pool(self.collection.database.connection)
        assert self.request_sock != pool._get_request_state().sock


class TestThreads(IntegrationTest):
    def setUp(self):
        self.db = client_context.rs_or_standalone_client.pymongo_test

    def test_threading(self):
        self.db.drop_collection("test")
        for i in range(1000):
            self.db.test.save({"x": i})

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
        # 1. Create a client and start a request.
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
        cx = rs_or_single_client()
        cx.start_request()
        collection = cx.db.pymongo_test

        # acquire a request socket for the main thread
        collection.find_one()
        pool = get_pool(collection.database.connection)
        socket_info = pool._get_request_state()
        assert isinstance(socket_info, SocketInfo)
        request_sock = socket_info.sock

        state = FindPauseFind.create_shared_state(nthreads=10)

        threads = [
            FindPauseFind(collection, state)
            for _ in range(state.nthreads)
        ]

        # Each thread does a find(), thus acquiring a request socket
        for t in threads:
            t.start()

        # Wait for the threads to reach the rendezvous
        FindPauseFind.wait_for_rendezvous(state)

        try:
            # Simulate an event that closes all sockets, e.g. primary stepdown
            for t in threads:
                t.request_sock.close()

            # Finally, ensure the main thread's socket's last_checkout is
            # updated:
            collection.find_one()

            # ... and close it:
            request_sock.close()

            # Doing an operation on the client raises an AutoReconnect and
            # resets the pool behind the scenes
            self.assertRaises(AutoReconnect, collection.find_one)

        finally:
            # Let threads do a second find()
            FindPauseFind.resume_after_rendezvous(state)

        joinall(threads)

        for t in threads:
            self.assertTrue(t.passed, "%s threw exception" % t)

    def test_client_disconnect(self):
        self.db.drop_collection("test")
        for i in range(1000):
            self.db.test.save({"x": i})

        # Start 10 threads that execute a query, and 10 threads that call
        # client.disconnect() 10 times in a row.
        threads = [SaveAndFind(self.db.test) for _ in range(10)]
        threads.extend(Disconnect(self.db.connection, 10) for _ in range(10))

        with frequent_thread_switches():
            for t in threads:
                t.start()

            for t in threads:
                t.join(30)

            for t in threads:
                self.assertTrue(t.passed)


class TestThreadsAuth(IntegrationTest):
    @classmethod
    @client_context.require_auth
    def setUpClass(cls):
        super(TestThreadsAuth, cls).setUpClass()

    def setUp(self):
        self.client.admin.add_user('admin-user', 'password',
                                   roles=['clusterAdmin',
                                          'dbAdminAnyDatabase',
                                          'readWriteAnyDatabase',
                                          'userAdminAnyDatabase'])

        # client is already authenticated to get around restricted
        # localhost exception in MongoDB >= 2.7.1.
        self.client.admin.logout()
        self.client.admin.authenticate("admin-user", "password")
        self.client.auth_test.add_user("test-user", "password",
                                       roles=['readWrite'])

    def tearDown(self):
        # Remove auth users from databases
        self.client.admin.authenticate("admin-user", "password")
        remove_all_users(self.client.auth_test)
        self.client.drop_database('auth_test')
        self.client.admin.remove_user('admin-user')
        self.client.admin.logout()

    def test_auto_auth_login(self):
        client = self.client
        client.admin.logout()
        self.assertRaises(OperationFailure, client.auth_test.test.find_one)

        # Admin auth
        client.admin.authenticate("admin-user", "password")

        nthreads = 10
        threads = []
        for _ in range(nthreads):
            t = AutoAuthenticateThreads(client.auth_test.test, 100)
            t.start()
            threads.append(t)

        joinall(threads)

        for t in threads:
            self.assertTrue(t.success)

        # Database-specific auth
        client.admin.logout()
        client.auth_test.authenticate("test-user", "password")

        threads = []
        for _ in range(nthreads):
            t = AutoAuthenticateThreads(client.auth_test.test, 100)
            t.start()
            threads.append(t)

        joinall(threads)

        for t in threads:
            self.assertTrue(t.success)


if __name__ == "__main__":
    unittest.main()
