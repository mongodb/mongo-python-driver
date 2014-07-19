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

"""Test the mongo_client module."""

import datetime
import os
import threading
import socket
import sys
import time
import thread
import unittest
import warnings


sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.son import SON
from bson.tz_util import utc
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.pool import SocketInfo
from pymongo import auth, thread_util, common
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            InvalidName,
                            OperationFailure,
                            PyMongoError)
from test import version, host, port, pair
from test.pymongo_mocks import MockClient
from test.utils import (assertRaisesExactly,
                        catch_warnings,
                        delay,
                        is_mongos,
                        remove_all_users,
                        server_is_master_with_slave,
                        server_started_with_auth,
                        TestRequestMixin,
                        _TestLazyConnectMixin,
                        _TestExhaustCursorMixin,
                        lazy_client_trial,
                        NTHREADS,
                        get_pool,
                        one)


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

    def assertIsInstance(self, obj, cls, msg=None):
        """Backport from Python 2.7."""
        if not isinstance(obj, cls):
            standardMsg = '%r is not an instance of %r' % (obj, cls)
            self.fail(self._formatMessage(msg, standardMsg))

    def test_init_disconnected(self):
        c = MongoClient(host, port, _connect=False)

        self.assertIsInstance(c.is_primary, bool)
        self.assertIsInstance(c.is_mongos, bool)
        self.assertIsInstance(c.max_pool_size, int)
        self.assertIsInstance(c.use_greenlets, bool)
        self.assertIsInstance(c.nodes, frozenset)
        self.assertIsInstance(c.auto_start_request, bool)
        self.assertEqual(dict, c.get_document_class())
        self.assertIsInstance(c.tz_aware, bool)
        self.assertIsInstance(c.max_bson_size, int)
        self.assertIsInstance(c.min_wire_version, int)
        self.assertIsInstance(c.max_wire_version, int)
        self.assertIsInstance(c.max_write_batch_size, int)
        self.assertEqual(None, c.host)
        self.assertEqual(None, c.port)

        c.pymongo_test.test.find_one()  # Auto-connect.
        self.assertEqual(host, c.host)
        self.assertEqual(port, c.port)

        if version.at_least(c, (2, 5, 4, -1)):
            self.assertTrue(c.max_wire_version > 0)
        else:
            self.assertEqual(c.max_wire_version, 0)
        self.assertTrue(c.min_wire_version >= 0)

        bad_host = "somedomainthatdoesntexist.org"
        c = MongoClient(bad_host, port, connectTimeoutMS=1, _connect=False)
        self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

    def test_init_disconnected_with_auth(self):
        uri = "mongodb://user:pass@somedomainthatdoesntexist"
        c = MongoClient(uri, connectTimeoutMS=1, _connect=False)
        self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

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
        # Explicitly test inequality
        self.assertFalse(client != MongoClient(host, port))

    def test_host_w_port(self):
        self.assertTrue(MongoClient("%s:%d" % (host, port)))
        assertRaisesExactly(
            ConnectionFailure, MongoClient, "%s:1234567" % (host,), port)

    def test_repr(self):
        # Making host a str avoids the 'u' prefix in Python 2, so the repr is
        # the same in Python 2 and 3.
        self.assertEqual(repr(MongoClient(str(host), port)),
                         "MongoClient('%s', %d)" % (host, port))

    def test_getters(self):
        self.assertEqual(MongoClient(host, port).host, host)
        self.assertEqual(MongoClient(host, port).port, port)
        self.assertEqual(set([(host, port)]),
                         MongoClient(host, port).nodes)

    def test_use_greenlets(self):
        self.assertFalse(MongoClient(host, port).use_greenlets)
        if thread_util.have_gevent:
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
        # Due to SERVER-2329, databases may not disappear
        # from a master in a master-slave pair.
        if server_is_master_with_slave(c):
            raise SkipTest("SERVER-2329")
        if (not version.at_least(c, (2, 6, 0)) and
                is_mongos(c) and server_started_with_auth(c)):
            raise SkipTest("Need mongos >= 2.6.0 to test with authentication")
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
        self.assertFalse("pymongo_test1" in c.database_names())
        self.assertFalse("pymongo_test2" in c.database_names())

        c.pymongo_test.test.insert({"foo": "bar"})

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

        # See SERVER-6427 for mongos
        if not is_mongos(c) and server_started_with_auth(c):

            c.drop_database("pymongo_test1")

            c.admin.add_user("admin", "password")
            c.admin.authenticate("admin", "password")
            try:
                c.pymongo_test.add_user("mike", "password")

                self.assertRaises(OperationFailure, c.copy_database,
                                  "pymongo_test", "pymongo_test1",
                                  username="foo", password="bar")
                self.assertFalse("pymongo_test1" in c.database_names())

                self.assertRaises(OperationFailure, c.copy_database,
                                  "pymongo_test", "pymongo_test1",
                                  username="mike", password="bar")
                self.assertFalse("pymongo_test1" in c.database_names())

                c.copy_database("pymongo_test", "pymongo_test1",
                                username="mike", password="password")
                self.assertTrue("pymongo_test1" in c.database_names())
                self.assertEqual("bar", c.pymongo_test1.test.find_one()["foo"])
            finally:
                # Cleanup
                remove_all_users(c.pymongo_test)
                c.admin.remove_user("admin")
                c.disconnect()

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

        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            self.assertEqual(c, MongoClient("mongodb://%s:%d" % (host, port)))
            self.assertTrue(MongoClient(
                "mongodb://%s:%d" % (host, port), slave_okay=True).slave_okay)
            self.assertTrue(MongoClient(
                "mongodb://%s:%d/?slaveok=true;w=2" % (host, port)).slave_okay)
        finally:
            ctx.exit()

    def test_get_default_database(self):
        c = MongoClient("mongodb://%s:%d/foo" % (host, port), _connect=False)
        self.assertEqual(Database(c, 'foo'), c.get_default_database())

    def test_get_default_database_error(self):
        # URI with no database.
        c = MongoClient("mongodb://%s:%d/" % (host, port), _connect=False)
        self.assertRaises(ConfigurationError, c.get_default_database)

    def test_get_default_database_with_authsource(self):
        # Ensure we distinguish database name from authSource.
        uri = "mongodb://%s:%d/foo?authSource=src" % (host, port)
        c = MongoClient(uri, _connect=False)
        self.assertEqual(Database(c, 'foo'), c.get_default_database())

    def test_auth_from_uri(self):
        c = MongoClient(host, port)
        # Sharded auth not supported before MongoDB 2.0
        if is_mongos(c) and not version.at_least(c, (2, 0, 0)):
            raise SkipTest("Auth with sharding requires MongoDB >= 2.0.0")
        if not server_started_with_auth(c):
            raise SkipTest('Authentication is not enabled on server')

        c.admin.add_user("admin", "pass")
        c.admin.authenticate("admin", "pass")
        try:
            c.pymongo_test.add_user("user", "pass", roles=['userAdmin', 'readWrite'])

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

            # Auth with lazy connection.
            MongoClient(
                "mongodb://user:pass@%s:%d/pymongo_test" % (host, port),
                _connect=False).pymongo_test.test.find_one()

            # Wrong password.
            bad_client = MongoClient(
                "mongodb://user:wrong@%s:%d/pymongo_test" % (host, port),
                _connect=False)

            self.assertRaises(OperationFailure,
                              bad_client.pymongo_test.test.find_one)

        finally:
            # Clean up.
            remove_all_users(c.pymongo_test)
            remove_all_users(c.admin)

    def test_lazy_auth_raises_operation_failure(self):
        # Check if we have the prerequisites to run this test.
        c = MongoClient(host, port)
        if not server_started_with_auth(c):
            raise SkipTest('Authentication is not enabled on server')

        if is_mongos(c) and not version.at_least(c, (2, 0, 0)):
            raise SkipTest("Auth with sharding requires MongoDB >= 2.0.0")

        lazy_client = MongoClient(
            "mongodb://user:wrong@%s:%d/pymongo_test" % (host, port),
            _connect=False)

        assertRaisesExactly(
            OperationFailure, lazy_client.test.collection.find_one)

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
        self.assertEqual(10.5, get_pool(client).conn_timeout)
        client = MongoClient(host, port, socketTimeoutMS=10500)
        self.assertEqual(10.5, get_pool(client).net_timeout)

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
        self.assertEqual(get_pool(client).wait_queue_timeout, 2)

    def test_waitQueueMultiple(self):
        client = MongoClient(host, port, max_pool_size=3, waitQueueMultiple=2)
        pool = get_pool(client)
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
        MongoClient("mongodb://[::1]:%d/?w=0" % (port,))
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
        if not version.at_least(c, (2, 0)) and server_started_with_auth(c):
            raise SkipTest('Requires server >= 2.0 to test with auth')

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
        self.assertEqual(1, len(get_pool(client).sockets))

        # We need exec here because if the Python version is less than 2.6
        # these with-statements won't even compile.
        exec """
with contextlib.closing(client):
    self.assertEqual("bar", client.pymongo_test.test.find_one()["foo"])
self.assertEqual(None, client._MongoClient__member)
"""

        exec """
with get_client() as client:
    self.assertEqual("bar", client.pymongo_test.test.find_one()["foo"])
self.assertEqual(None, client._MongoClient__member)
"""

    def test_with_start_request(self):
        client = get_client()
        pool = get_pool(client)

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

        # Assure we acquire a request socket.
        client.pymongo_test.test.find_one()
        self.assertTrue(client.in_request())
        pool = get_pool(client)
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
        pool = get_pool(client)
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
        pool = get_pool(client)
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
        pool = get_pool(c)
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
        pool = get_pool(c)

        # Pool reserves a socket for this thread.
        c.pymongo_test.test.find_one()
        self.assertTrue(isinstance(pool._get_request_state(), SocketInfo))

        old_sock_info = pool._get_request_state()
        c.pymongo_test.test.drop()
        c.pymongo_test.test.insert({'_id': 'foo'})
        self.assertRaises(
            OperationFailure,
            c.pymongo_test.test.insert, {'_id': 'foo'})

        # OperationFailure doesn't affect the request socket
        self.assertEqual(old_sock_info, pool._get_request_state())

    def test_alive(self):
        self.assertTrue(get_client().alive())

        client = MongoClient('doesnt exist', _connect=False)
        self.assertFalse(client.alive())

    def test_wire_version(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='b:2',  # Pass a secondary.
            replicaSet='rs',
            _connect=False)

        c.set_wire_version_range('a:1', 1, 5)
        c.db.collection.find_one()  # Connect.
        self.assertEqual(c.min_wire_version, 1)
        self.assertEqual(c.max_wire_version, 5)

        c.set_wire_version_range('a:1', 10, 11)
        c.disconnect()
        self.assertRaises(ConfigurationError, c.db.collection.find_one)

    def test_max_wire_version(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='b:2',  # Pass a secondary.
            replicaSet='rs',
            _connect=False)

        c.set_max_write_batch_size('a:1', 1)
        c.set_max_write_batch_size('b:2', 2)

        # Starts with default max batch size.
        self.assertEqual(1000, c.max_write_batch_size)
        c.db.collection.find_one()  # Connect.
        # Uses primary's max batch size.
        self.assertEqual(c.max_write_batch_size, 1)

        # b becomes primary.
        c.mock_primary = 'b:2'
        c.disconnect()
        self.assertEqual(1000, c.max_write_batch_size)
        c.db.collection.find_one()  # Connect.
        self.assertEqual(c.max_write_batch_size, 2)

    def test_wire_version_mongos_ha(self):
        c = MockClient(
            standalones=[],
            members=[],
            mongoses=['a:1', 'b:2', 'c:3'],
            host='a:1,b:2,c:3',
            _connect=False)

        c.set_wire_version_range('a:1', 2, 5)
        c.set_wire_version_range('b:2', 2, 2)
        c.set_wire_version_range('c:3', 1, 1)
        c.db.collection.find_one()  # Connect.

        # Which member did we use?
        used_host = '%s:%s' % (c.host, c.port)
        expected_min, expected_max = c.mock_wire_versions[used_host]
        self.assertEqual(expected_min, c.min_wire_version)
        self.assertEqual(expected_max, c.max_wire_version)

        c.set_wire_version_range('a:1', 0, 0)
        c.set_wire_version_range('b:2', 0, 0)
        c.set_wire_version_range('c:3', 0, 0)
        c.disconnect()
        c.db.collection.find_one()
        used_host = '%s:%s' % (c.host, c.port)
        expected_min, expected_max = c.mock_wire_versions[used_host]
        self.assertEqual(expected_min, c.min_wire_version)
        self.assertEqual(expected_max, c.max_wire_version)

    def test_replica_set(self):
        client = MongoClient(host, port)
        name = client.pymongo_test.command('ismaster').get('setName')
        if not name:
            raise SkipTest('Not connected to a replica set')

        MongoClient(host, port, replicaSet=name)  # No error.

        self.assertRaises(
            ConfigurationError,
            MongoClient, host, port, replicaSet='bad' + name)

    def test_lazy_connect_w0(self):
        client = get_client(_connect=False)
        client.pymongo_test.test.insert({}, w=0)

        client = get_client(_connect=False)
        client.pymongo_test.test.update({}, {'$set': {'x': 1}}, w=0)

        client = get_client(_connect=False)
        client.pymongo_test.test.remove(w=0)

    def test_auth_network_error(self):
        # Make sure there's no semaphore leak if we get a network error
        # when authenticating a new socket with cached credentials.
        auth_client = get_client()
        if not server_started_with_auth(auth_client):
            raise SkipTest('Authentication is not enabled on server')

        auth_client.admin.add_user('admin', 'password')
        auth_client.admin.authenticate('admin', 'password')
        try:
            # Get a client with one socket so we detect if it's leaked.
            c = get_client(max_pool_size=1, waitQueueTimeoutMS=1)

            # Simulate an authenticate() call on a different socket.
            credentials = auth._build_credentials_tuple(
                'MONGODB-CR', 'admin',
                unicode('admin'), unicode('password'),
                {})

            c._cache_credentials('test', credentials, connect=False)

            # Cause a network error on the actual socket.
            pool = get_pool(c)
            socket_info = one(pool.sockets)
            socket_info.sock.close()

            # In __check_auth, the client authenticates its socket with the
            # new credential, but gets a socket.error. Should be reraised as
            # AutoReconnect.
            self.assertRaises(AutoReconnect, c.test.collection.find_one)

            # No semaphore leak, the pool is allowed to make a new socket.
            c.test.collection.find_one()
        finally:
            remove_all_users(auth_client.admin)


class TestClientLazyConnect(unittest.TestCase, _TestLazyConnectMixin):
    def _get_client(self, **kwargs):
        return get_client(**kwargs)


class TestClientLazyConnectBadSeeds(unittest.TestCase):
    def _get_client(self, **kwargs):
        kwargs.setdefault('connectTimeoutMS', 100)

        # Assume there are no open mongods listening on a.com, b.com, ....
        bad_seeds = ['%s.com' % chr(ord('a') + i) for i in range(10)]
        return MongoClient(bad_seeds, **kwargs)

    def test_connect(self):
        def reset(dummy):
            pass

        def connect(collection, dummy):
            self.assertRaises(AutoReconnect, collection.find_one)

        def test(collection):
            client = collection.database.connection
            self.assertEqual(0, len(client.nodes))

        lazy_client_trial(
            reset, connect, test,
            self._get_client, use_greenlets=False)


class TestClientLazyConnectOneGoodSeed(
        unittest.TestCase,
        _TestLazyConnectMixin):

    def _get_client(self, **kwargs):
        kwargs.setdefault('connectTimeoutMS', 100)

        # Assume there are no open mongods listening on a.com, b.com, ....
        bad_seeds = ['%s.com' % chr(ord('a') + i) for i in range(10)]
        seeds = bad_seeds + [pair]

        # MongoClient puts the seeds in a set before iterating, so order is
        # undefined.
        return MongoClient(seeds, **kwargs)

    def test_insert(self):
        def reset(collection):
            collection.drop()

        def insert(collection, dummy):
            collection.insert({})

        def test(collection):
            self.assertEqual(NTHREADS, collection.count())

        lazy_client_trial(
            reset, insert, test,
            self._get_client, use_greenlets=False)


class TestMongoClientFailover(unittest.TestCase):
    def test_discover_primary(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='b:2',  # Pass a secondary.
            replicaSet='rs')

        self.assertEqual('a', c.host)
        self.assertEqual(1, c.port)
        self.assertEqual(3, len(c.nodes))

        # Fail over.
        c.kill_host('a:1')
        c.mock_primary = 'b:2'

        # Force reconnect.
        c.disconnect()
        c.db.collection.find_one()
        self.assertEqual('b', c.host)
        self.assertEqual(2, c.port)

        # a:1 is still in nodes.
        self.assertEqual(3, len(c.nodes))

    def test_reconnect(self):
        # Verify the node list isn't forgotten during a network failure.
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='b:2',  # Pass a secondary.
            replicaSet='rs')

        # Total failure.
        c.kill_host('a:1')
        c.kill_host('b:2')
        c.kill_host('c:3')

        # MongoClient discovers it's alone.
        self.assertRaises(AutoReconnect, c.db.collection.find_one)

        # But it remembers its node list.
        self.assertEqual(3, len(c.nodes))

        # So it can reconnect.
        c.revive_host('a:1')
        c.db.collection.find_one()


class TestExhaustCursor(_TestExhaustCursorMixin, unittest.TestCase):
    def _get_client(self, **kwargs):
        return get_client(**kwargs)


if __name__ == "__main__":
    unittest.main()
