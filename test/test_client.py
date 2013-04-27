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

"""Test the mongo_client module."""

import datetime
import os
import threading
import socket
import sys
import time
import thread
import unittest


sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.son import SON
from bson.tz_util import utc
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.pool import SocketInfo
from pymongo.errors import (ConfigurationError,
                            ConnectionFailure,
                            InvalidName,
                            OperationFailure)
from test import version, host, port
from test.utils import (assertRaisesExactly,
                        delay,
                        is_mongos,
                        server_is_master_with_slave,
                        server_started_with_auth,
                        TestRequestMixin)

try:
    import greenlet
    import gevent
    have_gevent = True
except ImportError:
    have_gevent = False


def get_client(*args, **kwargs):
    return MongoClient(host, port, *args, **kwargs)


class TestClient(unittest.TestCase, TestRequestMixin):
    def test_types(self):
        self.assertRaises(TypeError, MongoClient, 1)
        self.assertRaises(TypeError, MongoClient, 1.14)
        self.assertRaises(TypeError, MongoClient, "localhost", "27017")
        self.assertRaises(TypeError, MongoClient, "localhost", 1.14)
        self.assertRaises(TypeError, MongoClient, "localhost", [])

        self.assertRaises(ConfigurationError, MongoClient, [])

    def test_constants(self):
        MongoClient.HOST = host
        MongoClient.PORT = port
        self.assertTrue(MongoClient())

        MongoClient.HOST = "somedomainthatdoesntexist.org"
        MongoClient.PORT = 123456789
        assertRaisesExactly(
            ConnectionFailure, MongoClient, connectTimeoutMS=600)
        self.assertTrue(MongoClient(host, port))

        MongoClient.HOST = host
        MongoClient.PORT = port
        self.assertTrue(MongoClient())

    def test_connect(self):
        # Check that the exception is a ConnectionFailure, not a subclass like
        # AutoReconnect
        assertRaisesExactly(
            ConnectionFailure, MongoClient,
            "somedomainthatdoesntexist.org", connectTimeoutMS=600)

        assertRaisesExactly(
            ConnectionFailure, MongoClient, host, 123456789)

        self.assertTrue(MongoClient(host, port))

    def test_equality(self):
        client = MongoClient(host, port)
        self.assertEqual(client, MongoClient(host, port))
        # Explicity test inequality
        self.assertFalse(client != MongoClient(host, port))

    def test_host_w_port(self):
        self.assertTrue(MongoClient("%s:%d" % (host, port)))
        assertRaisesExactly(
            ConnectionFailure, MongoClient, "%s:1234567" % (host,), port)

    def test_repr(self):
        self.assertEqual(repr(MongoClient(host, port)),
                         "MongoClient('%s', %d)" % (host, port))

    def test_getters(self):
        self.assertEqual(MongoClient(host, port).host, host)
        self.assertEqual(MongoClient(host, port).port, port)
        self.assertEqual(set([(host, port)]),
                         MongoClient(host, port).nodes)

    def test_use_greenlets(self):
        self.assertFalse(MongoClient(host, port).use_greenlets)
        if have_gevent:
            self.assertTrue(
                MongoClient(
                    host, port, use_greenlets=True).use_greenlets)

    def test_get_db(self):
        client = MongoClient(host, port)

        def make_db(base, name):
            return base[name]

        self.assertRaises(InvalidName, make_db, client, "")
        self.assertRaises(InvalidName, make_db, client, "te$t")
        self.assertRaises(InvalidName, make_db, client, "te.t")
        self.assertRaises(InvalidName, make_db, client, "te\\t")
        self.assertRaises(InvalidName, make_db, client, "te/t")
        self.assertRaises(InvalidName, make_db, client, "te st")

        self.assertTrue(isinstance(client.test, Database))
        self.assertEqual(client.test, client["test"])
        self.assertEqual(client.test, Database(client, "test"))

    def test_database_names(self):
        client = MongoClient(host, port)

        client.pymongo_test.test.save({"dummy": u"object"})
        client.pymongo_test_mike.test.save({"dummy": u"object"})

        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_mike" in dbs)

    def test_drop_database(self):
        client = MongoClient(host, port)

        self.assertRaises(TypeError, client.drop_database, 5)
        self.assertRaises(TypeError, client.drop_database, None)

        raise SkipTest("This test often fails due to SERVER-2329")

        client.pymongo_test.test.save({"dummy": u"object"})
        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        client.drop_database("pymongo_test")
        dbs = client.database_names()
        self.assertTrue("pymongo_test" not in dbs)

        client.pymongo_test.test.save({"dummy": u"object"})
        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        client.drop_database(client.pymongo_test)
        dbs = client.database_names()
        self.assertTrue("pymongo_test" not in dbs)

    def test_copy_db(self):
        c = MongoClient(host, port)
        # We test copy twice; once starting in a request and once not. In
        # either case the copy should succeed (because it starts a request
        # internally) and should leave us in the same state as before the copy.
        c.start_request()

        self.assertRaises(TypeError, c.copy_database, 4, "foo")
        self.assertRaises(TypeError, c.copy_database, "foo", 4)

        self.assertRaises(InvalidName, c.copy_database, "foo", "$foo")

        c.pymongo_test.test.drop()
        c.drop_database("pymongo_test1")
        c.drop_database("pymongo_test2")

        c.pymongo_test.test.insert({"foo": "bar"})

        # Due to SERVER-2329, databases may not disappear from a master in a
        # master-slave pair
        if not server_is_master_with_slave(c):
            self.assertFalse("pymongo_test1" in c.database_names())
            self.assertFalse("pymongo_test2" in c.database_names())

        c.copy_database("pymongo_test", "pymongo_test1")
        # copy_database() didn't accidentally end the request
        self.assertTrue(c.in_request())

        self.assertTrue("pymongo_test1" in c.database_names())
        self.assertEqual("bar", c.pymongo_test1.test.find_one()["foo"])

        c.end_request()
        self.assertFalse(c.in_request())
        c.copy_database("pymongo_test", "pymongo_test2",
                        "%s:%d" % (host, port))
        # copy_database() didn't accidentally restart the request
        self.assertFalse(c.in_request())

        self.assertTrue("pymongo_test2" in c.database_names())
        self.assertEqual("bar", c.pymongo_test2.test.find_one()["foo"])

        if version.at_least(c, (1, 3, 3, 1)):
            c.drop_database("pymongo_test1")

            c.pymongo_test.add_user("mike", "password")

            self.assertRaises(OperationFailure, c.copy_database,
                              "pymongo_test", "pymongo_test1",
                              username="foo", password="bar")
            if not server_is_master_with_slave(c):
                self.assertFalse("pymongo_test1" in c.database_names())

            self.assertRaises(OperationFailure, c.copy_database,
                              "pymongo_test", "pymongo_test1",
                              username="mike", password="bar")

            if not server_is_master_with_slave(c):
                self.assertFalse("pymongo_test1" in c.database_names())

            if not is_mongos(c):
                # See SERVER-6427
                c.copy_database("pymongo_test", "pymongo_test1",
                                username="mike", password="password")
                self.assertTrue("pymongo_test1" in c.database_names())
                self.assertEqual("bar", c.pymongo_test1.test.find_one()["foo"])

    def test_iteration(self):
        client = MongoClient(host, port)

        def iterate():
            [a for a in client]

        self.assertRaises(TypeError, iterate)

    def test_disconnect(self):
        c = MongoClient(host, port)
        coll = c.pymongo_test.bar

        c.disconnect()
        c.disconnect()

        coll.count()

        c.disconnect()
        c.disconnect()

        coll.count()

    def test_from_uri(self):
        c = MongoClient(host, port)

        self.assertEqual(c, MongoClient("mongodb://%s:%d" % (host, port)))
        self.assertTrue(MongoClient(
            "mongodb://%s:%d" % (host, port), slave_okay=True).slave_okay)
        self.assertTrue(MongoClient(
            "mongodb://%s:%d/?slaveok=true;w=2" % (host, port)).slave_okay)

    def test_auth_from_uri(self):
        c = MongoClient(host, port)
        # Sharded auth not supported before MongoDB 2.0
        if is_mongos(c) and not version.at_least(c, (2, 0, 0)):
            raise SkipTest("Auth with sharding requires MongoDB >= 2.0.0")

        c.admin.system.users.remove({})
        c.pymongo_test.system.users.remove({})
        c.admin.add_user("admin", "pass")
        c.admin.authenticate("admin", "pass")
        c.pymongo_test.add_user("user", "pass")

        self.assertRaises(ConfigurationError, MongoClient,
                          "mongodb://foo:bar@%s:%d" % (host, port))
        self.assertRaises(ConfigurationError, MongoClient,
                          "mongodb://admin:bar@%s:%d" % (host, port))
        self.assertRaises(ConfigurationError, MongoClient,
                          "mongodb://user:pass@%s:%d" % (host, port))
        MongoClient("mongodb://admin:pass@%s:%d" % (host, port))

        self.assertRaises(ConfigurationError, MongoClient,
                          "mongodb://admin:pass@%s:%d/pymongo_test" %
                          (host, port))
        self.assertRaises(ConfigurationError, MongoClient,
                          "mongodb://user:foo@%s:%d/pymongo_test" %
                          (host, port))
        MongoClient("mongodb://user:pass@%s:%d/pymongo_test" %
                   (host, port))

        c.admin.system.users.remove({})
        c.pymongo_test.system.users.remove({})

    def test_unix_socket(self):
        if not hasattr(socket, "AF_UNIX"):
            raise SkipTest("UNIX-sockets are not supported on this system")
        if (sys.platform == 'darwin' and
            server_started_with_auth(MongoClient(host, port))):
            raise SkipTest("SERVER-8492")

        mongodb_socket = '/tmp/mongodb-27017.sock'
        if not os.access(mongodb_socket, os.R_OK):
            raise SkipTest("Socket file is not accessable")

        self.assertTrue(MongoClient("mongodb://%s" % mongodb_socket))

        client = MongoClient("mongodb://%s" % mongodb_socket)
        client.pymongo_test.test.save({"dummy": "object"})

        # Confirm we can read via the socket
        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)

        # Confirm it fails with a missing socket
        self.assertRaises(ConnectionFailure, MongoClient,
                          "mongodb:///tmp/none-existent.sock")

    def test_fork(self):
        # Test using a client before and after a fork.
        if sys.platform == "win32":
            raise SkipTest("Can't fork on windows")

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest("No multiprocessing module")

        db = MongoClient(host, port).pymongo_test

        # Failure occurs if the client is used before the fork
        db.test.find_one()
        db.connection.end_request()

        def loop(pipe):
            while True:
                try:
                    db.test.insert({"a": "b"})
                    for _ in db.test.find():
                        pass
                except:
                    pipe.send(True)
                    os._exit(1)

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

        # recv will only have data if the subprocess failed
        try:
            cp1.recv()
            self.fail()
        except EOFError:
            pass
        try:
            cp2.recv()
            self.fail()
        except EOFError:
            pass

    def test_document_class(self):
        c = MongoClient(host, port)
        db = c.pymongo_test
        db.test.insert({"x": 1})

        self.assertEqual(dict, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

        c.document_class = SON

        self.assertEqual(SON, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))

        c = MongoClient(host, port, document_class=SON)
        db = c.pymongo_test

        self.assertEqual(SON, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))

        c.document_class = dict

        self.assertEqual(dict, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

    def test_timeouts(self):
        client = MongoClient(host, port, connectTimeoutMS=10500)
        self.assertEqual(10.5, client._MongoClient__pool.conn_timeout)
        client = MongoClient(host, port, socketTimeoutMS=10500)
        self.assertEqual(10.5, client._MongoClient__pool.net_timeout)

    def test_network_timeout_validation(self):
        c = get_client(socketTimeoutMS=10 * 1000)
        self.assertEqual(10, c._MongoClient__net_timeout)

        c = get_client(socketTimeoutMS=None)
        self.assertEqual(None, c._MongoClient__net_timeout)

        self.assertRaises(ConfigurationError,
            get_client, socketTimeoutMS=0)

        self.assertRaises(ConfigurationError,
            get_client, socketTimeoutMS=-1)

        self.assertRaises(ConfigurationError,
            get_client, socketTimeoutMS=1e10)

        self.assertRaises(ConfigurationError,
            get_client, socketTimeoutMS='foo')

        # network_timeout is gone from MongoClient, remains in deprecated
        # Connection
        self.assertRaises(ConfigurationError,
            get_client, network_timeout=10)

    def test_network_timeout(self):
        no_timeout = MongoClient(host, port)
        timeout_sec = 1
        timeout = MongoClient(
            host, port, socketTimeoutMS=1000 * timeout_sec)

        no_timeout.pymongo_test.drop_collection("test")
        no_timeout.pymongo_test.test.insert({"x": 1})

        # A $where clause that takes a second longer than the timeout
        where_func = delay(timeout_sec + 1)

        def get_x(db):
            doc = db.test.find().where(where_func).next()
            return doc["x"]
        self.assertEqual(1, get_x(no_timeout.pymongo_test))
        self.assertRaises(ConnectionFailure, get_x, timeout.pymongo_test)

        def get_x_timeout(db, t):
            doc = db.test.find(network_timeout=t).where(where_func).next()
            return doc["x"]
        self.assertEqual(1, get_x_timeout(timeout.pymongo_test, None))
        self.assertRaises(ConnectionFailure, get_x_timeout,
                          no_timeout.pymongo_test, 0.1)

    def test_waitQueueTimeoutMS(self):
        client = MongoClient(host, port, waitQueueTimeoutMS=2000)
        self.assertEqual(client._MongoClient__pool.wait_queue_timeout, 2)

    def test_waitQueueMultiple(self):
        client = MongoClient(host, port, max_pool_size=3, waitQueueMultiple=2)
        pool = client._MongoClient__pool
        self.assertEqual(pool.wait_queue_multiple, 2)
        self.assertEqual(pool._socket_semaphore.waiter_semaphore.counter, 6)

    def test_tz_aware(self):
        self.assertRaises(ConfigurationError, MongoClient, tz_aware='foo')

        aware = MongoClient(host, port, tz_aware=True)
        naive = MongoClient(host, port)
        aware.pymongo_test.drop_collection("test")

        now = datetime.datetime.utcnow()
        aware.pymongo_test.test.insert({"x": now})

        self.assertEqual(None, naive.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(utc, aware.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(
                aware.pymongo_test.test.find_one()["x"].replace(tzinfo=None),
                naive.pymongo_test.test.find_one()["x"])

    def test_ipv6(self):
        try:
            client = MongoClient("[::1]")
        except:
            # Either mongod was started without --ipv6
            # or the OS doesn't support it (or both).
            raise SkipTest("No IPv6")

        # Try a few simple things
        MongoClient("mongodb://[::1]:%d" % (port,))
        MongoClient("mongodb://[::1]:%d/?slaveOk=true" % (port,))
        MongoClient("[::1]:%d,localhost:%d" % (port, port))

        client = MongoClient("localhost:%d,[::1]:%d" % (port, port))
        client.pymongo_test.test.save({"dummy": u"object"})
        client.pymongo_test_bernie.test.save({"dummy": u"object"})

        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_bernie" in dbs)

    def test_fsync_lock_unlock(self):
        c = get_client()
        if is_mongos(c):
            raise SkipTest('fsync/lock not supported by mongos')

        res = c.admin.command('getCmdLineOpts')
        if '--master' in res['argv'] and version.at_least(c, (2, 3, 0)):
            raise SkipTest('SERVER-7714')

        self.assertFalse(c.is_locked)
        # async flushing not supported on windows...
        if sys.platform not in ('cygwin', 'win32'):
            c.fsync(async=True)
            self.assertFalse(c.is_locked)
        c.fsync(lock=True)
        self.assertTrue(c.is_locked)
        locked = True
        c.unlock()
        for _ in xrange(5):
            locked = c.is_locked
            if not locked:
                break
            time.sleep(1)
        self.assertFalse(locked)

    def test_contextlib(self):
        if sys.version_info < (2, 6):
            raise SkipTest("With statement requires Python >= 2.6")

        import contextlib

        client = get_client(auto_start_request=False)
        client.pymongo_test.drop_collection("test")
        client.pymongo_test.test.insert({"foo": "bar"})

        # The socket used for the previous commands has been returned to the
        # pool
        self.assertEqual(1, len(client._MongoClient__pool.sockets))

        # We need exec here because if the Python version is less than 2.6
        # these with-statements won't even compile.
        exec """
with contextlib.closing(client):
    self.assertEqual("bar", client.pymongo_test.test.find_one()["foo"])
self.assertEqual(0, len(client._MongoClient__pool.sockets))
"""

        exec """
with get_client() as client:
    self.assertEqual("bar", client.pymongo_test.test.find_one()["foo"])
# Calling client.close() has reset the pool
self.assertEqual(0, len(client._MongoClient__pool.sockets))
"""

    def test_with_start_request(self):
        client = get_client()
        pool = client._MongoClient__pool

        # No request started
        self.assertNoRequest(pool)
        self.assertDifferentSock(pool)

        # Start a request
        request_context_mgr = client.start_request()
        self.assertTrue(
            isinstance(request_context_mgr, object)
        )

        self.assertNoSocketYet(pool)
        self.assertSameSock(pool)
        self.assertRequestSocket(pool)

        # End request
        request_context_mgr.__exit__(None, None, None)
        self.assertNoRequest(pool)
        self.assertDifferentSock(pool)

        # Test the 'with' statement
        if sys.version_info >= (2, 6):
            # We need exec here because if the Python version is less than 2.6
            # these with-statements won't even compile.
            exec """
with client.start_request() as request:
    self.assertEqual(client, request.connection)
    self.assertNoSocketYet(pool)
    self.assertSameSock(pool)
    self.assertRequestSocket(pool)
"""

            # Request has ended
            self.assertNoRequest(pool)
            self.assertDifferentSock(pool)

    def test_auto_start_request(self):
        for bad_horrible_value in (None, 5, 'hi!'):
            self.assertRaises(
                (TypeError, ConfigurationError),
                lambda: get_client(auto_start_request=bad_horrible_value)
            )

        # auto_start_request should default to False
        client = get_client()
        self.assertFalse(client.auto_start_request)

        client = get_client(auto_start_request=True)
        self.assertTrue(client.auto_start_request)
        self.assertTrue(client.in_request())
        pool = client._MongoClient__pool

        # Request started already, just from MongoClient constructor - it's a
        # bit weird, but MongoClient does some socket stuff when it initializes
        # and it ends up with a request socket
        self.assertRequestSocket(pool)
        self.assertSameSock(pool)

        client.end_request()
        self.assertNoRequest(pool)
        self.assertDifferentSock(pool)

        # Trigger auto_start_request
        client.pymongo_test.test.find_one()
        self.assertRequestSocket(pool)
        self.assertSameSock(pool)

    def test_nested_request(self):
        # auto_start_request is False
        client = get_client()
        pool = client._MongoClient__pool
        self.assertFalse(client.in_request())

        # Start and end request
        client.start_request()
        self.assertInRequestAndSameSock(client, pool)
        client.end_request()
        self.assertNotInRequestAndDifferentSock(client, pool)

        # Double-nesting
        client.start_request()
        client.start_request()
        client.end_request()
        self.assertInRequestAndSameSock(client, pool)
        client.end_request()
        self.assertNotInRequestAndDifferentSock(client, pool)

        # Extra end_request calls have no effect - count stays at zero
        client.end_request()
        self.assertNotInRequestAndDifferentSock(client, pool)

        client.start_request()
        self.assertInRequestAndSameSock(client, pool)
        client.end_request()
        self.assertNotInRequestAndDifferentSock(client, pool)

    def test_request_threads(self):
        client = get_client(auto_start_request=False)
        pool = client._MongoClient__pool
        self.assertNotInRequestAndDifferentSock(client, pool)

        started_request, ended_request = threading.Event(), threading.Event()
        checked_request = threading.Event()
        thread_done = [False]

        # Starting a request in one thread doesn't put the other thread in a
        # request
        def f():
            self.assertNotInRequestAndDifferentSock(client, pool)
            client.start_request()
            self.assertInRequestAndSameSock(client, pool)
            started_request.set()
            checked_request.wait()
            checked_request.clear()
            self.assertInRequestAndSameSock(client, pool)
            client.end_request()
            self.assertNotInRequestAndDifferentSock(client, pool)
            ended_request.set()
            checked_request.wait()
            thread_done[0] = True

        t = threading.Thread(target=f)
        t.setDaemon(True)
        t.start()
        # It doesn't matter in what order the main thread or t initially get
        # to started_request.set() / wait(); by waiting here we ensure that t
        # has called client.start_request() before we assert on the next line.
        started_request.wait()
        self.assertNotInRequestAndDifferentSock(client, pool)
        checked_request.set()
        ended_request.wait()
        self.assertNotInRequestAndDifferentSock(client, pool)
        checked_request.set()
        t.join()
        self.assertNotInRequestAndDifferentSock(client, pool)
        self.assertTrue(thread_done[0], "Thread didn't complete")

    def test_interrupt_signal(self):
        if sys.platform.startswith('java'):
            # We can't figure out how to raise an exception on a thread that's
            # blocked on a socket, whether that's the main thread or a worker,
            # without simply killing the whole thread in Jython. This suggests
            # PYTHON-294 can't actually occur in Jython.
            raise SkipTest("Can't test interrupts in Jython")

        # Test fix for PYTHON-294 -- make sure MongoClient closes its
        # socket if it gets an interrupt while waiting to recv() from it.
        c = get_client()
        db = c.pymongo_test

        # A $where clause which takes 1.5 sec to execute
        where = delay(1.5)

        # Need exactly 1 document so find() will execute its $where clause once
        db.drop_collection('foo')
        db.foo.insert({'_id': 1})

        def interrupter():
            # Raises KeyboardInterrupt in the main thread
            time.sleep(0.25)
            thread.interrupt_main()

        thread.start_new_thread(interrupter, ())

        raised = False
        try:
            # Will be interrupted by a KeyboardInterrupt.
            db.foo.find({'$where': where}).next()
        except KeyboardInterrupt:
            raised = True

        # Can't use self.assertRaises() because it doesn't catch system
        # exceptions
        self.assertTrue(raised, "Didn't raise expected KeyboardInterrupt")

        # Raises AssertionError due to PYTHON-294 -- Mongo's response to the
        # previous find() is still waiting to be read on the socket, so the
        # request id's don't match.
        self.assertEqual(
            {'_id': 1},
            db.foo.find().next()
        )

    def test_operation_failure_without_request(self):
        # Ensure MongoClient doesn't close socket after it gets an error
        # response to getLastError. PYTHON-395.
        c = get_client()
        pool = c._MongoClient__pool
        self.assertEqual(1, len(pool.sockets))
        old_sock_info = iter(pool.sockets).next()
        c.pymongo_test.test.drop()
        c.pymongo_test.test.insert({'_id': 'foo'})
        self.assertRaises(
            OperationFailure,
            c.pymongo_test.test.insert, {'_id': 'foo'})

        self.assertEqual(1, len(pool.sockets))
        new_sock_info = iter(pool.sockets).next()
        self.assertEqual(old_sock_info, new_sock_info)

    def test_operation_failure_with_request(self):
        # Ensure MongoClient doesn't close socket after it gets an error
        # response to getLastError. PYTHON-395.
        c = get_client(auto_start_request=True)
        pool = c._MongoClient__pool

        # MongoClient has reserved a socket for this thread
        self.assertTrue(isinstance(pool._get_request_state(), SocketInfo))

        old_sock_info = pool._get_request_state()
        c.pymongo_test.test.drop()
        c.pymongo_test.test.insert({'_id': 'foo'})
        self.assertRaises(
            OperationFailure,
            c.pymongo_test.test.insert, {'_id': 'foo'})

        # OperationFailure doesn't affect the request socket
        self.assertEqual(old_sock_info, pool._get_request_state())


if __name__ == "__main__":
    unittest.main()
