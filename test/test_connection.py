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

"""Test the connection module."""

import datetime
import os
import sys
import time
import thread
import unittest

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.son import SON
from bson.tz_util import utc
from pymongo.connection import Connection
from pymongo.database import Database
from pymongo.pool import NO_REQUEST, NO_SOCKET_YET, SocketInfo
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            InvalidName,
                            InvalidURI,
                            OperationFailure)
from test import version
from test.utils import is_mongos, server_is_master_with_slave, delay

host = os.environ.get("DB_IP", "localhost")
port = int(os.environ.get("DB_PORT", 27017))


def get_connection(*args, **kwargs):
    return Connection(host, port, *args, **kwargs)


class TestConnection(unittest.TestCase):

    def setUp(self):
        self.host = os.environ.get("DB_IP", "localhost")
        self.port = int(os.environ.get("DB_PORT", 27017))

    def test_types(self):
        self.assertRaises(TypeError, Connection, 1)
        self.assertRaises(TypeError, Connection, 1.14)
        self.assertRaises(TypeError, Connection, "localhost", "27017")
        self.assertRaises(TypeError, Connection, "localhost", 1.14)
        self.assertRaises(TypeError, Connection, "localhost", [])

        self.assertRaises(ConfigurationError, Connection, [])

    def test_constants(self):
        Connection.HOST = self.host
        Connection.PORT = self.port
        self.assertTrue(Connection())

        Connection.HOST = "somedomainthatdoesntexist.org"
        Connection.PORT = 123456789
        self.assertRaises(ConnectionFailure, Connection, connectTimeoutMS=600)
        self.assertTrue(Connection(self.host, self.port))

        Connection.HOST = self.host
        Connection.PORT = self.port
        self.assertTrue(Connection())

    def test_connect(self):
        self.assertRaises(ConnectionFailure, Connection,
                          "somedomainthatdoesntexist.org", connectTimeoutMS=600)
        self.assertRaises(ConnectionFailure, Connection, self.host, 123456789)

        self.assertTrue(Connection(self.host, self.port))

    def test_host_w_port(self):
        self.assertTrue(Connection("%s:%d" % (self.host, self.port)))
        self.assertRaises(ConnectionFailure, Connection,
                          "%s:1234567" % (self.host,), self.port)

    def test_repr(self):
        self.assertEqual(repr(Connection(self.host, self.port)),
                         "Connection('%s', %d)" % (self.host, self.port))

    def test_getters(self):
        self.assertEqual(Connection(self.host, self.port).host, self.host)
        self.assertEqual(Connection(self.host, self.port).port, self.port)
        self.assertEqual(set([(self.host, self.port)]),
                         Connection(self.host, self.port).nodes)

    def test_get_db(self):
        connection = Connection(self.host, self.port)

        def make_db(base, name):
            return base[name]

        self.assertRaises(InvalidName, make_db, connection, "")
        self.assertRaises(InvalidName, make_db, connection, "te$t")
        self.assertRaises(InvalidName, make_db, connection, "te.t")
        self.assertRaises(InvalidName, make_db, connection, "te\\t")
        self.assertRaises(InvalidName, make_db, connection, "te/t")
        self.assertRaises(InvalidName, make_db, connection, "te st")

        self.assertTrue(isinstance(connection.test, Database))
        self.assertEqual(connection.test, connection["test"])
        self.assertEqual(connection.test, Database(connection, "test"))

    def test_database_names(self):
        connection = Connection(self.host, self.port)

        connection.pymongo_test.test.save({"dummy": u"object"})
        connection.pymongo_test_mike.test.save({"dummy": u"object"})

        dbs = connection.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_mike" in dbs)

    def test_drop_database(self):
        connection = Connection(self.host, self.port)

        self.assertRaises(TypeError, connection.drop_database, 5)
        self.assertRaises(TypeError, connection.drop_database, None)

        raise SkipTest("This test often fails due to SERVER-2329")

        connection.pymongo_test.test.save({"dummy": u"object"})
        dbs = connection.database_names()
        self.assertTrue("pymongo_test" in dbs)
        connection.drop_database("pymongo_test")
        dbs = connection.database_names()
        self.assertTrue("pymongo_test" not in dbs)

        connection.pymongo_test.test.save({"dummy": u"object"})
        dbs = connection.database_names()
        self.assertTrue("pymongo_test" in dbs)
        connection.drop_database(connection.pymongo_test)
        dbs = connection.database_names()
        self.assertTrue("pymongo_test" not in dbs)

    def test_copy_db(self):
        c = Connection(self.host, self.port)
        self.assertTrue(c.in_request())

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
        c.copy_database("pymongo_test", "pymongo_test2",
                        "%s:%d" % (self.host, self.port))
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
        connection = Connection(self.host, self.port)

        def iterate():
            [a for a in connection]

        self.assertRaises(TypeError, iterate)

    def test_disconnect(self):
        c = Connection(self.host, self.port)
        coll = c.foo.bar

        c.disconnect()
        c.disconnect()

        coll.count()

        c.disconnect()
        c.disconnect()

        coll.count()

    def test_from_uri(self):
        c = Connection(self.host, self.port)

        self.assertEqual(c, Connection("mongodb://%s:%d" %
                                       (self.host, self.port)))

        c.admin.system.users.remove({})
        c.pymongo_test.system.users.remove({})

        try:
            # First admin user add fails gle in MongoDB >= 2.1.2
            # See SERVER-4225 for more information.
            c.admin.add_user("admin", "pass")
        except OperationFailure:
            pass
        c.admin.authenticate("admin", "pass")
        c.pymongo_test.add_user("user", "pass")

        self.assertRaises(ConfigurationError, Connection,
                          "mongodb://foo:bar@%s:%d" % (self.host, self.port))
        self.assertRaises(ConfigurationError, Connection,
                          "mongodb://admin:bar@%s:%d" % (self.host, self.port))
        self.assertRaises(ConfigurationError, Connection,
                          "mongodb://user:pass@%s:%d" % (self.host, self.port))
        Connection("mongodb://admin:pass@%s:%d" % (self.host, self.port))

        self.assertRaises(ConfigurationError, Connection,
                          "mongodb://admin:pass@%s:%d/pymongo_test" %
                          (self.host, self.port))
        self.assertRaises(ConfigurationError, Connection,
                          "mongodb://user:foo@%s:%d/pymongo_test" %
                          (self.host, self.port))
        Connection("mongodb://user:pass@%s:%d/pymongo_test" %
                   (self.host, self.port))

        self.assertTrue(Connection("mongodb://%s:%d" %
                                (self.host, self.port),
                                slave_okay=True).slave_okay)
        self.assertTrue(Connection("mongodb://%s:%d/?slaveok=true;w=2" %
                                (self.host, self.port)).slave_okay)
        c.admin.system.users.remove({})
        c.pymongo_test.system.users.remove({})

    def test_fork(self):
        # Test using a connection before and after a fork.
        if sys.platform == "win32":
            raise SkipTest("Can't fork on windows")

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest("No multiprocessing module")

        db = Connection(self.host, self.port).pymongo_test

        # Failure occurs if the connection is used before the fork
        db.test.find_one()
        db.connection.end_request()

        def loop(pipe):
            while True:
                try:
                    db.test.insert({"a": "b"}, safe=True)
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
        c = Connection(self.host, self.port)
        db = c.pymongo_test
        db.test.insert({"x": 1})

        self.assertEqual(dict, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

        c.document_class = SON

        self.assertEqual(SON, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))

        c = Connection(self.host, self.port, document_class=SON)
        db = c.pymongo_test

        self.assertEqual(SON, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))

        c.document_class = dict

        self.assertEqual(dict, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

    def test_timeouts(self):
        conn = Connection(self.host, self.port, connectTimeoutMS=10500)
        self.assertEqual(10.5, conn._Connection__pool.conn_timeout)
        conn = Connection(self.host, self.port, socketTimeoutMS=10500)
        self.assertEqual(10.5, conn._Connection__pool.net_timeout)

    def test_network_timeout(self):
        no_timeout = Connection(self.host, self.port)
        timeout_sec = 1
        timeout = Connection(self.host, self.port, network_timeout=timeout_sec)

        no_timeout.pymongo_test.drop_collection("test")
        no_timeout.pymongo_test.test.insert({"x": 1}, safe=True)

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

    def test_tz_aware(self):
        self.assertRaises(ConfigurationError, Connection, tz_aware='foo')

        aware = Connection(self.host, self.port, tz_aware=True)
        naive = Connection(self.host, self.port)
        aware.pymongo_test.drop_collection("test")

        now = datetime.datetime.utcnow()
        aware.pymongo_test.test.insert({"x": now}, safe=True)

        self.assertEqual(None, naive.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(utc, aware.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(
                aware.pymongo_test.test.find_one()["x"].replace(tzinfo=None),
                naive.pymongo_test.test.find_one()["x"])

    def test_ipv6(self):
        try:
            connection = Connection("[::1]")
        except:
            # Either mongod was started without --ipv6
            # or the OS doesn't support it (or both).
            raise SkipTest("No IPv6")

        # Try a few simple things
        connection = Connection("mongodb://[::1]:%d" % (self.port,))
        connection = Connection("mongodb://[::1]:%d/"
                                "?slaveOk=true" % (self.port,))
        connection = Connection("[::1]:%d,"
                                "localhost:%d" % (self.port, self.port))
        connection = Connection("localhost:%d,"
                                "[::1]:%d" % (self.port, self.port))
        connection.pymongo_test.test.save({"dummy": u"object"})
        connection.pymongo_test_bernie.test.save({"dummy": u"object"})

        dbs = connection.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_bernie" in dbs)

    def test_fsync_lock_unlock(self):
        c = get_connection()
        if is_mongos(c):
            raise SkipTest('fsync/lock not supported by mongos')
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

        conn = get_connection(auto_start_request=False)
        conn.pymongo_test.drop_collection("test")
        conn.pymongo_test.test.insert({"foo": "bar"})

        # The socket used for the previous commands has been returned to the
        # pool
        self.assertEqual(1, len(conn._Connection__pool.sockets))

        # We need exec here because if the Python version is less than 2.6
        # these with-statements won't even compile.
        exec """
with contextlib.closing(conn):
    self.assertEqual("bar", conn.pymongo_test.test.find_one()["foo"])
self.assertEqual(0, len(conn._Connection__pool.sockets))
"""

        exec """
with get_connection() as connection:
    self.assertEqual("bar", connection.pymongo_test.test.find_one()["foo"])
    # Calling conn.close() has reset the pool
    self.assertEqual(0, len(connection._Connection__pool.sockets))
"""

    def get_sock(self, pool):
        sock_info = pool.get_socket((self.host, self.port))
        return sock_info

    def assertSameSock(self, pool):
        sock_info0 = self.get_sock(pool)
        sock_info1 = self.get_sock(pool)
        self.assertEqual(sock_info0, sock_info1)
        pool.maybe_return_socket(sock_info0)
        pool.maybe_return_socket(sock_info1)

    def assertDifferentSock(self, pool):
        # We have to hold both SocketInfos at the same time, otherwise the
        # first will send its socket back to the pool as soon as its ref count
        # goes to zero, in which case the second SocketInfo we get will have
        # the same socket as the first.
        sock_info0 = self.get_sock(pool)
        sock_info1 = self.get_sock(pool)
        self.assertNotEqual(sock_info0, sock_info1)
        pool.maybe_return_socket(sock_info0)
        pool.maybe_return_socket(sock_info1)

    def assertNoRequest(self, pool):
        self.assertEqual(NO_REQUEST, pool._get_request_state())

    def assertNoSocketYet(self, pool):
        self.assertEqual(NO_SOCKET_YET, pool._get_request_state())

    def assertRequestSocket(self, pool):
        self.assertTrue(isinstance(pool._get_request_state(), SocketInfo))
        
    def test_with_start_request(self):
        conn = get_connection(auto_start_request=False)
        pool = conn._Connection__pool

        # No request started
        self.assertNoRequest(pool)
        self.assertDifferentSock(pool)

        # Start a request
        request_context_mgr = conn.start_request()
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
with conn.start_request() as request:
    self.assertEqual(conn, request.connection)
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
                lambda: get_connection(auto_start_request=bad_horrible_value)
            )

        # auto_start_request should default to True
        conn = get_connection()
        self.assertTrue(conn.auto_start_request)
        self.assertTrue(conn.in_request())
        pool = conn._Connection__pool

        # Request started already, just from Connection constructor - it's a
        # bit weird, but Connection does some socket stuff when it initializes
        # and it ends up with a request socket
        self.assertRequestSocket(pool)
        self.assertSameSock(pool)

        conn.end_request()
        self.assertNoRequest(pool)
        self.assertDifferentSock(pool)

        # Trigger auto_start_request
        conn.db.test.find_one()
        self.assertRequestSocket(pool)
        self.assertSameSock(pool)

    def test_interrupt_signal(self):
        if sys.platform.startswith('java'):
            # We can't figure out how to raise an exception on a thread that's
            # blocked on a socket, whether that's the main thread or a worker,
            # without simply killing the whole thread in Jython. This suggests
            # PYTHON-294 can't actually occur in Jython.
            raise SkipTest("Can't test interrupts in Jython")

        # Test fix for PYTHON-294 -- make sure Connection closes its
        # socket if it gets an interrupt while waiting to recv() from it.
        c = get_connection()
        db = c.pymongo_test

        # A $where clause which takes 1.5 sec to execute
        where = delay(1.5)

        # Need exactly 1 document so find() will execute its $where clause once
        db.drop_collection('foo')
        db.foo.insert({'_id': 1}, safe=True)

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

if __name__ == "__main__":
    unittest.main()
