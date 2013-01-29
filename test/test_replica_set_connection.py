# Copyright 2011-2013 10gen, Inc.
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

"""Test the replica_set_connection module."""

import copy
import datetime
import os
import signal
import socket
import sys
import time
import thread
import threading
import traceback
import unittest

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.son import SON
from bson.tz_util import utc
from pymongo.connection import Connection
from pymongo.read_preferences import ReadPreference
from pymongo.replica_set_connection import ReplicaSetConnection
from pymongo.mongo_replica_set_client import _partition_node, have_gevent
from pymongo.database import Database
from pymongo.pool import SocketInfo
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            InvalidName,
                            OperationFailure)
from test import version
from test.utils import (
    delay, assertReadFrom, assertReadFromAll, read_from_which_host,
    assertRaisesExactly, TestRequestMixin)


host = os.environ.get("DB_IP", 'localhost')
port = int(os.environ.get("DB_PORT", 27017))
pair = '%s:%d' % (host, port)

class TestReplicaSetConnectionAgainstStandalone(unittest.TestCase):
    """This is a funny beast -- we want to run tests for ReplicaSetConnection
    but only if the database at DB_IP and DB_PORT is a standalone.
    """
    def setUp(self):
        conn = Connection(pair)
        response = conn.admin.command('ismaster')
        if 'setName' in response:
            raise SkipTest("Connected to a replica set, not a standalone mongod")

    def test_connect(self):
        self.assertRaises(ConfigurationError, ReplicaSetConnection,
                          pair, replicaSet='anything',
                          connectTimeoutMS=600)


class TestConnectionReplicaSetBase(unittest.TestCase):
    def setUp(self):
        conn = Connection(pair)
        response = conn.admin.command('ismaster')
        if 'setName' in response:
            self.name = str(response['setName'])
            self.w = len(response['hosts'])
            self.hosts = set([_partition_node(h)
                              for h in response["hosts"]])
            self.arbiters = set([_partition_node(h)
                                 for h in response.get("arbiters", [])])

            repl_set_status = conn.admin.command('replSetGetStatus')
            primary_info = [
                m for m in repl_set_status['members']
                if m['stateStr'] == 'PRIMARY'
            ][0]

            self.primary = _partition_node(primary_info['name'])
            self.secondaries = [
                _partition_node(m['name']) for m in repl_set_status['members']
                if m['stateStr'] == 'SECONDARY'
            ]
        else:
            raise SkipTest("Not connected to a replica set")

    def _get_connection(self, **kwargs):
        return ReplicaSetConnection(pair,
            replicaSet=self.name,
            **kwargs)

class TestConnection(TestConnectionReplicaSetBase, TestRequestMixin):
    def test_connect(self):
        assertRaisesExactly(ConnectionFailure, ReplicaSetConnection,
                          "somedomainthatdoesntexist.org:27017",
                          replicaSet=self.name,
                          connectTimeoutMS=600)
        self.assertRaises(ConfigurationError, ReplicaSetConnection,
                          pair, replicaSet='fdlksjfdslkjfd')
        self.assertTrue(ReplicaSetConnection(pair, replicaSet=self.name))

    def test_repr(self):
        connection = self._get_connection()
        self.assertEqual(repr(connection),
                         "ReplicaSetConnection(%r)" % (["%s:%d" % n
                                                         for n in 
                                                         self.hosts],))

    def test_properties(self):
        c = ReplicaSetConnection(pair, replicaSet=self.name)
        c.admin.command('ping')
        self.assertEqual(c.primary, self.primary)
        self.assertEqual(c.hosts, self.hosts)
        self.assertEqual(c.arbiters, self.arbiters)
        self.assertEqual(c.max_pool_size, 10)
        self.assertEqual(c.document_class, dict)
        self.assertEqual(c.tz_aware, False)

        # Make sure RSC's properties are copied to Database and Collection
        for obj in c, c.pymongo_test, c.pymongo_test.test:
            self.assertEqual(obj.read_preference, ReadPreference.PRIMARY)
            self.assertEqual(obj.tag_sets, [{}])
            self.assertEqual(obj.secondary_acceptable_latency_ms, 15)
            self.assertEqual(obj.slave_okay, False)
            self.assertEqual(obj.safe, False)

        cursor = c.pymongo_test.test.find()
        self.assertEqual(
            ReadPreference.PRIMARY, cursor._Cursor__read_preference)
        self.assertEqual([{}], cursor._Cursor__tag_sets)
        self.assertEqual(15, cursor._Cursor__secondary_acceptable_latency_ms)
        self.assertEqual(False, cursor._Cursor__slave_okay)
        c.close()

        tag_sets = [{'dc': 'la', 'rack': '2'}, {'foo': 'bar'}]
        c = ReplicaSetConnection(pair, replicaSet=self.name, max_pool_size=25,
                                 document_class=SON, tz_aware=True,
                                 slaveOk=False, safe=True,
                                 read_preference=ReadPreference.SECONDARY,
                                 tag_sets=copy.deepcopy(tag_sets),
                                 secondary_acceptable_latency_ms=77)
        c.admin.command('ping')
        self.assertEqual(c.primary, self.primary)
        self.assertEqual(c.hosts, self.hosts)
        self.assertEqual(c.arbiters, self.arbiters)
        self.assertEqual(c.max_pool_size, 25)
        self.assertEqual(c.document_class, SON)
        self.assertEqual(c.tz_aware, True)

        for obj in c, c.pymongo_test, c.pymongo_test.test:
            self.assertEqual(obj.read_preference, ReadPreference.SECONDARY)
            self.assertEqual(obj.tag_sets, tag_sets)
            self.assertEqual(obj.secondary_acceptable_latency_ms, 77)
            self.assertEqual(obj.slave_okay, False)
            self.assertEqual(obj.safe, True)

        cursor = c.pymongo_test.test.find()
        self.assertEqual(
            ReadPreference.SECONDARY, cursor._Cursor__read_preference)
        self.assertEqual(tag_sets, cursor._Cursor__tag_sets)
        self.assertEqual(77, cursor._Cursor__secondary_acceptable_latency_ms)
        self.assertEqual(False, cursor._Cursor__slave_okay)

        cursor = c.pymongo_test.test.find(
            read_preference=ReadPreference.NEAREST,
            tag_sets=[{'dc':'ny'}, {}],
            secondary_acceptable_latency_ms=123)

        self.assertEqual(
            ReadPreference.NEAREST, cursor._Cursor__read_preference)
        self.assertEqual([{'dc':'ny'}, {}], cursor._Cursor__tag_sets)
        self.assertEqual(123, cursor._Cursor__secondary_acceptable_latency_ms)
        self.assertEqual(False, cursor._Cursor__slave_okay)

        if version.at_least(c, (1, 7, 4)):
            self.assertEqual(c.max_bson_size, 16777216)
        else:
            self.assertEqual(c.max_bson_size, 4194304)
        c.close()

    def test_use_greenlets(self):
        self.assertFalse(
            ReplicaSetConnection(pair, replicaSet=self.name).use_greenlets)

        if have_gevent:
            self.assertTrue(ReplicaSetConnection(
                pair, replicaSet=self.name, use_greenlets=True).use_greenlets)

    def test_get_db(self):
        connection = self._get_connection()

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
        connection.close()

    def test_auto_reconnect_exception_when_read_preference_is_secondary(self):
        c = self._get_connection()
        db = c.pymongo_test

        def raise_socket_error(*args, **kwargs):
            raise socket.error

        old_sendall = socket.socket.sendall
        socket.socket.sendall = raise_socket_error

        try:
            cursor = db.test.find(read_preference=ReadPreference.SECONDARY)
            self.assertRaises(AutoReconnect, cursor.next)
        finally:
            socket.socket.sendall = old_sendall

    def test_operations(self):
        c = self._get_connection()

        # Check explicitly for a case we've commonly hit in tests:
        # a replica set is started with a tiny oplog, a previous
        # test does a big insert that leaves the secondaries
        # permanently "RECOVERING", and our insert(w=self.w) hangs
        # forever.
        rs_status = c.admin.command('replSetGetStatus')
        members = rs_status['members']
        self.assertFalse(
            [m for m in members if m['stateStr'] == 'RECOVERING'],
            "Replica set is recovering, try a larger oplogSize next time"
        )

        db = c.pymongo_test
        db.test.remove({}, safe=True)
        self.assertEqual(0, db.test.count())
        db.test.insert({'foo': 'x'}, safe=True, w=self.w, wtimeout=10000)
        self.assertEqual(1, db.test.count())

        cursor = db.test.find()
        doc = cursor.next()
        self.assertEqual('x', doc['foo'])
        # Ensure we read from the primary
        self.assertEqual(c.primary, cursor._Cursor__connection_id)

        cursor = db.test.find(read_preference=ReadPreference.SECONDARY)
        doc = cursor.next()
        self.assertEqual('x', doc['foo'])
        # Ensure we didn't read from the primary
        self.assertTrue(cursor._Cursor__connection_id in c.secondaries)

        self.assertEqual(1, db.test.count())
        db.test.remove({}, safe=True)
        self.assertEqual(0, db.test.count())
        db.test.drop()
        c.close()

    def test_database_names(self):
        connection = self._get_connection()

        connection.pymongo_test.test.save({"dummy": u"object"})
        connection.pymongo_test_mike.test.save({"dummy": u"object"})

        dbs = connection.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_mike" in dbs)
        connection.close()

    def test_drop_database(self):
        connection = self._get_connection()

        self.assertRaises(TypeError, connection.drop_database, 5)
        self.assertRaises(TypeError, connection.drop_database, None)

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
        connection.close()

    def test_copy_db(self):
        c = self._get_connection()
        self.assertTrue(c.in_request())

        self.assertRaises(TypeError, c.copy_database, 4, "foo")
        self.assertRaises(TypeError, c.copy_database, "foo", 4)

        self.assertRaises(InvalidName, c.copy_database, "foo", "$foo")

        c.pymongo_test.test.drop()
        c.drop_database("pymongo_test1")
        c.drop_database("pymongo_test2")

        c.pymongo_test.test.insert({"foo": "bar"})

        self.assertFalse("pymongo_test1" in c.database_names())
        self.assertFalse("pymongo_test2" in c.database_names())

        c.copy_database("pymongo_test", "pymongo_test1")
        # copy_database() didn't accidentally end the request
        self.assertTrue(c.in_request())

        self.assertTrue("pymongo_test1" in c.database_names())
        self.assertEqual("bar", c.pymongo_test1.test.find_one()["foo"])

        c.end_request()

        self.assertFalse(c.in_request())
        c.copy_database("pymongo_test", "pymongo_test2", pair)
        # copy_database() didn't accidentally restart the request
        self.assertFalse(c.in_request())

        time.sleep(1)

        self.assertTrue("pymongo_test2" in c.database_names())
        self.assertEqual("bar", c.pymongo_test2.test.find_one()["foo"])

        if version.at_least(c, (1, 3, 3, 1)):
            c.drop_database("pymongo_test1")

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
            time.sleep(2)
            self.assertEqual("bar", c.pymongo_test1.test.find_one()["foo"])
        c.close()

    def test_iteration(self):
        connection = self._get_connection()

        def iterate():
            [a for a in connection]

        self.assertRaises(TypeError, iterate)
        connection.close()

    def test_disconnect(self):
        c = self._get_connection()
        coll = c.pymongo_test.bar

        c.disconnect()
        c.disconnect()

        coll.count()

        c.disconnect()
        c.disconnect()

        coll.count()

    def test_fork(self):
        """Test using a connection before and after a fork.
        """
        if sys.platform == "win32":
            raise SkipTest("Can't fork on Windows")

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest("No multiprocessing module")

        db = self._get_connection().pymongo_test

        # Failure occurs if the connection is used before the fork
        db.test.find_one()
        #db.connection.end_request()

        def loop(pipe):
            while True:
                try:
                    db.test.insert({"a": "b"}, safe=True)
                    for _ in db.test.find():
                        pass
                except:
                    traceback.print_exc()
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

        db.connection.close()

    def test_document_class(self):
        c = self._get_connection()
        db = c.pymongo_test
        db.test.insert({"x": 1})

        self.assertEqual(dict, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

        c.document_class = SON

        self.assertEqual(SON, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))
        c.close()

        c = self._get_connection(document_class=SON)
        db = c.pymongo_test

        self.assertEqual(SON, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))

        c.document_class = dict

        self.assertEqual(dict, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))
        c.close()

    def test_network_timeout_validation(self):
        c = self._get_connection(network_timeout=10)
        self.assertEqual(10, c._MongoReplicaSetClient__net_timeout)

        c = self._get_connection(network_timeout=None)
        self.assertEqual(None, c._MongoReplicaSetClient__net_timeout)

        self.assertRaises(ConfigurationError,
            self._get_connection, network_timeout=0)

        self.assertRaises(ConfigurationError,
            self._get_connection, network_timeout=-1)

        self.assertRaises(ConfigurationError,
            self._get_connection, network_timeout=1e10)

        self.assertRaises(ConfigurationError,
            self._get_connection, network_timeout='foo')

    def test_network_timeout(self):
        no_timeout = self._get_connection()
        timeout_sec = 1
        timeout = self._get_connection(socketTimeoutMS=timeout_sec*1000)

        no_timeout.pymongo_test.drop_collection("test")
        no_timeout.pymongo_test.test.insert({"x": 1}, safe=True)

        # A $where clause that takes a second longer than the timeout
        where_func = delay(1 + timeout_sec)

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
        no_timeout.close()
        timeout.close()

    def test_tz_aware(self):
        self.assertRaises(ConfigurationError, ReplicaSetConnection,
                          tz_aware='foo', replicaSet=self.name)

        aware = self._get_connection(tz_aware=True)
        naive = self._get_connection()
        aware.pymongo_test.drop_collection("test")

        now = datetime.datetime.utcnow()
        aware.pymongo_test.test.insert({"x": now}, safe=True)
        time.sleep(1)

        self.assertEqual(None, naive.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(utc, aware.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(
                aware.pymongo_test.test.find_one()["x"].replace(tzinfo=None),
                naive.pymongo_test.test.find_one()["x"])

    def test_ipv6(self):
        try:
            connection = ReplicaSetConnection("[::1]:%d" % (port,),
                                              replicaSet=self.name)
        except:
            # Either mongod was started without --ipv6
            # or the OS doesn't support it (or both).
            raise SkipTest("No IPv6")

        # Try a few simple things
        connection = ReplicaSetConnection("mongodb://[::1]:%d" % (port,),
                                          replicaSet=self.name)
        connection = ReplicaSetConnection("mongodb://[::1]:%d/?safe=true;"
                                          "replicaSet=%s" % (port, self.name))
        connection = ReplicaSetConnection("[::1]:%d,localhost:"
                                          "%d" % (port, port),
                                          replicaSet=self.name)
        connection = ReplicaSetConnection("localhost:%d,[::1]:"
                                          "%d" % (port, port),
                                          replicaSet=self.name)
        connection.pymongo_test.test.save({"dummy": u"object"})
        connection.pymongo_test_bernie.test.save({"dummy": u"object"})

        dbs = connection.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_bernie" in dbs)
        connection.close()

    def _test_kill_cursor_explicit(self, read_pref):
        c = self._get_connection(read_preference=read_pref)
        db = c.pymongo_test
        db.drop_collection("test")

        test = db.test
        test.insert([{"i": i} for i in range(20)], w=1 + len(c.secondaries))

        # Partially evaluate cursor so it's left alive, then kill it
        cursor = test.find().batch_size(10)
        cursor.next()
        self.assertNotEqual(0, cursor.cursor_id)

        connection_id = cursor._Cursor__connection_id
        writer = c._MongoReplicaSetClient__writer
        if read_pref == ReadPreference.PRIMARY:
            msg = "Expected cursor's connection_id to be %s, got %s" % (
                writer, connection_id)

            self.assertEqual(connection_id, writer, msg)
        else:
            self.assertNotEqual(connection_id, writer,
                "Expected cursor's connection_id not to be primary")

        cursor_id = cursor.cursor_id

        # Cursor dead on server - trigger a getMore on the same cursor_id and
        # check that the server returns an error.
        cursor2 = cursor.clone()
        cursor2._Cursor__id = cursor_id

        if (sys.platform.startswith('java') or
            'PyPy' in sys.version):
            # Explicitly kill cursor.
            cursor.close()
        else:
            # Implicitly kill it in CPython.
            del cursor

        self.assertRaises(OperationFailure, lambda: list(cursor2))

    def test_kill_cursor_explicit_primary(self):
        self._test_kill_cursor_explicit(ReadPreference.PRIMARY)

    def test_kill_cursor_explicit_secondary(self):
        self._test_kill_cursor_explicit(ReadPreference.SECONDARY)

    def test_interrupt_signal(self):
        if sys.platform.startswith('java'):
            raise SkipTest("Can't test interrupts in Jython")

        # Test fix for PYTHON-294 -- make sure Connection closes its
        # socket if it gets an interrupt while waiting to recv() from it.
        c = self._get_connection()
        db = c.pymongo_test

        # A $where clause which takes 1.5 sec to execute
        where = delay(1.5)

        # Need exactly 1 document so find() will execute its $where clause once
        db.drop_collection('foo')
        db.foo.insert({'_id': 1}, safe=True)

        old_signal_handler = None

        try:
            # Platform-specific hacks for raising a KeyboardInterrupt on the main
            # thread while find() is in-progress: On Windows, SIGALRM is unavailable
            # so we use second thread. In our Bamboo setup on Linux, the thread
            # technique causes an error in the test at sock.recv():
            #    TypeError: 'int' object is not callable
            # We don't know what causes this in Bamboo, so we hack around it.
            if sys.platform == 'win32':
                def interrupter():
                    time.sleep(0.25)

                    # Raises KeyboardInterrupt in the main thread
                    thread.interrupt_main()

                thread.start_new_thread(interrupter, ())
            else:
                # Convert SIGALRM to SIGINT -- it's hard to schedule a SIGINT for one
                # second in the future, but easy to schedule SIGALRM.
                def sigalarm(num, frame):
                    raise KeyboardInterrupt

                old_signal_handler = signal.signal(signal.SIGALRM, sigalarm)
                signal.alarm(1)

            raised = False
            try:
                # Will be interrupted by a KeyboardInterrupt.
                db.foo.find({'$where': where}).next()
            except KeyboardInterrupt:
                raised = True

            # Can't use self.assertRaises() because it doesn't catch system
            # exceptions
            self.assertTrue(raised, "Didn't raise expected ConnectionFailure")

            # Raises AssertionError due to PYTHON-294 -- Mongo's response to the
            # previous find() is still waiting to be read on the socket, so the
            # request id's don't match.
            self.assertEqual(
                {'_id': 1},
                db.foo.find().next()
            )
        finally:
            if old_signal_handler:
                signal.signal(signal.SIGALRM, old_signal_handler)

    def test_operation_failure_without_request(self):
        # Ensure ReplicaSetConnection doesn't close socket after it gets an
        # error response to getLastError. PYTHON-395.
        c = self._get_connection(auto_start_request=False)
        pool = c._MongoReplicaSetClient__members[self.primary].pool
        self.assertEqual(1, len(pool.sockets))
        old_sock_info = iter(pool.sockets).next()
        c.pymongo_test.test.drop()
        c.pymongo_test.test.insert({'_id': 'foo'}, safe=True)
        self.assertRaises(
            OperationFailure,
            c.pymongo_test.test.insert, {'_id': 'foo'}, safe=True)

        self.assertEqual(1, len(pool.sockets))
        new_sock_info = iter(pool.sockets).next()

        self.assertEqual(old_sock_info, new_sock_info)
        c.close()

    def test_operation_failure_with_request(self):
        # Ensure ReplicaSetConnection doesn't close socket after it gets an
        # error response to getLastError. PYTHON-395.
        c = self._get_connection(auto_start_request=True)
        c.pymongo_test.test.find_one()
        pool = c._MongoReplicaSetClient__members[self.primary].pool

        # Connection has reserved a socket for this thread
        self.assertTrue(isinstance(pool._get_request_state(), SocketInfo))

        old_sock_info = pool._get_request_state()
        c.pymongo_test.test.drop()
        c.pymongo_test.test.insert({'_id': 'foo'}, safe=True)
        self.assertRaises(
            OperationFailure,
            c.pymongo_test.test.insert, {'_id': 'foo'}, safe=True)

        # OperationFailure doesn't affect the request socket
        self.assertEqual(old_sock_info, pool._get_request_state())
        c.close()

    def test_auto_start_request(self):
        for bad_horrible_value in (None, 5, 'hi!'):
            self.assertRaises(
                (TypeError, ConfigurationError),
                lambda: self._get_connection(auto_start_request=bad_horrible_value)
            )

        conn = self._get_connection()
        self.assertTrue(conn.auto_start_request)
        pools = [mongo.pool for mongo in
                 conn._MongoReplicaSetClient__members.values()]

        primary_pool = conn._MongoReplicaSetClient__members[conn.primary].pool
        self.assertTrue(conn.auto_start_request)
        self.assertTrue(conn.in_request())

        # Trigger the RSC to actually start a request on primary pool
        conn.pymongo_test.test.find_one()
        self.assertTrue(primary_pool.in_request())

        # Trigger the RSC to actually start a request on secondary pool
        cursor = conn.pymongo_test.test.find(
                read_preference=ReadPreference.SECONDARY)
        try:
            cursor.next()
        except StopIteration:
            # No results, no problem
            pass

        secondary = cursor._Cursor__connection_id
        secondary_pool = conn._MongoReplicaSetClient__members[secondary].pool
        self.assertTrue(secondary_pool.in_request())

        conn.end_request()
        self.assertFalse(conn.in_request())
        for pool in pools:
            self.assertFalse(pool.in_request())
        conn.start_request()
        self.assertTrue(conn.in_request())
        conn.close()

        conn = self._get_connection(auto_start_request=False)
        self.assertFalse(conn.in_request())
        conn.start_request()
        self.assertTrue(conn.in_request())
        conn.end_request()
        self.assertFalse(conn.in_request())
        conn.close()

    def test_nested_request(self):
        # auto_start_request is True
        conn = self._get_connection()
        try:
            pools = [member.pool for member in
                conn._MongoReplicaSetClient__members.values()]
            self.assertTrue(conn.in_request())

            # Start and end request - we're still in "outer" original request
            conn.start_request()
            self.assertInRequestAndSameSock(conn, pools)
            conn.end_request()
            self.assertInRequestAndSameSock(conn, pools)

            # Double-nesting
            conn.start_request()
            conn.start_request()
            self.assertEqual(
                3, conn._MongoReplicaSetClient__request_counter.get())

            for pool in pools:
                # MRSC only called start_request() once per pool, although its
                # own counter is 2.
                self.assertEqual(1, pool._request_counter.get())

            conn.end_request()
            conn.end_request()
            self.assertInRequestAndSameSock(conn, pools)

            self.assertEqual(
                1, conn._MongoReplicaSetClient__request_counter.get())

            for pool in pools:
                self.assertEqual(1, pool._request_counter.get())

            # Finally, end original request
            conn.end_request()
            for pool in pools:
                self.assertFalse(pool.in_request())

            self.assertNotInRequestAndDifferentSock(conn, pools)
        finally:
            conn.close()

    def test_request_threads(self):
        conn = self._get_connection(auto_start_request=False)
        try:
            pools = [member.pool for member in
                conn._MongoReplicaSetClient__members.values()]
            self.assertNotInRequestAndDifferentSock(conn, pools)

            started_request, ended_request = threading.Event(), threading.Event()
            checked_request = threading.Event()
            thread_done = [False]

            # Starting a request in one thread doesn't put the other thread in a
            # request
            def f():
                self.assertNotInRequestAndDifferentSock(conn, pools)
                conn.start_request()
                self.assertInRequestAndSameSock(conn, pools)
                started_request.set()
                checked_request.wait()
                checked_request.clear()
                self.assertInRequestAndSameSock(conn, pools)
                conn.end_request()
                self.assertNotInRequestAndDifferentSock(conn, pools)
                ended_request.set()
                checked_request.wait()
                thread_done[0] = True

            t = threading.Thread(target=f)
            t.setDaemon(True)
            t.start()
            started_request.wait()
            self.assertNotInRequestAndDifferentSock(conn, pools)
            checked_request.set()
            ended_request.wait()
            self.assertNotInRequestAndDifferentSock(conn, pools)
            checked_request.set()
            t.join()
            self.assertNotInRequestAndDifferentSock(conn, pools)
            self.assertTrue(thread_done[0], "Thread didn't complete")
        finally:
            conn.close()

    def test_schedule_refresh(self):
        # Monitor thread starts waiting for _refresh_interval, 30 seconds
        conn = self._get_connection()

        # Reconnect if necessary
        conn.pymongo_test.test.find_one()

        secondaries = conn.secondaries
        for secondary in secondaries:
            conn._MongoReplicaSetClient__members[secondary].up = False

        conn._MongoReplicaSetClient__members[conn.primary].up = False

        # Wake up monitor thread
        conn._MongoReplicaSetClient__schedule_refresh()

        # Refresh interval is 30 seconds; scheduling a refresh tells the
        # monitor thread / greenlet to start a refresh now. We still need to
        # sleep a few seconds for it to complete.
        time.sleep(5)
        for secondary in secondaries:
            self.assertTrue(conn._MongoReplicaSetClient__members[secondary].up,
                "ReplicaSetConnection didn't detect secondary is up")

        self.assertTrue(conn._MongoReplicaSetClient__members[conn.primary].up,
            "ReplicaSetConnection didn't detect primary is up")

        conn.close()

    def test_pinned_member(self):
        latency = 1000 * 1000
        conn = self._get_connection(
            auto_start_request=False, secondary_acceptable_latency_ms=latency)

        host = read_from_which_host(conn, ReadPreference.SECONDARY)
        self.assertTrue(host in conn.secondaries)

        # No pinning since we're not in a request
        assertReadFromAll(
            self, conn, conn.secondaries,
            ReadPreference.SECONDARY, None, latency)

        assertReadFromAll(
            self, conn, list(conn.secondaries) + [conn.primary],
            ReadPreference.NEAREST, None, latency)

        conn.start_request()
        host = read_from_which_host(conn, ReadPreference.SECONDARY)
        self.assertTrue(host in conn.secondaries)
        assertReadFrom(self, conn, host, ReadPreference.SECONDARY)

        # Oddly, we expect PRIMARY_PREFERRED to keep reading from secondary,
        # since the secondary is pinned and "matches" the preference.
        assertReadFrom(self, conn, host, ReadPreference.PRIMARY_PREFERRED)

        # Repin
        primary = read_from_which_host(conn, ReadPreference.PRIMARY)
        self.assertEqual(conn.primary, primary)
        assertReadFrom(self, conn, primary, ReadPreference.NEAREST)
        assertReadFrom(self, conn, primary, ReadPreference.PRIMARY_PREFERRED)

        # Since the we're pinned to primary we still use it
        assertReadFrom(self, conn, primary, ReadPreference.SECONDARY_PREFERRED)

        # Repin again
        host = read_from_which_host(conn, ReadPreference.SECONDARY)
        self.assertTrue(host in conn.secondaries)
        assertReadFrom(self, conn, host, ReadPreference.SECONDARY)

        # Unpin
        conn.end_request()
        assertReadFromAll(
            self, conn, list(conn.secondaries) + [conn.primary],
            ReadPreference.NEAREST, None, latency)


if __name__ == "__main__":
    unittest.main()
