# Copyright 2011-2014 MongoDB, Inc.
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

"""Test the mongo_replica_set_client module."""

# TODO: anywhere we wait for refresh in tests, consider just refreshing w/ sync

import copy
import datetime
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
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import ReadPreference
from pymongo.member import PRIMARY, SECONDARY, OTHER
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.mongo_replica_set_client import _partition_node, have_gevent
from pymongo.database import Database
from pymongo.pool import SocketInfo
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            InvalidName,
                            OperationFailure, InvalidOperation)
from pymongo import auth
from test import version, port, pair
from test.pymongo_mocks import MockReplicaSetClient
from test.utils import (
    delay, assertReadFrom, assertReadFromAll, read_from_which_host,
    remove_all_users, assertRaisesExactly, TestRequestMixin, one,
    server_started_with_auth, pools_from_rs_client, get_pool,
    _TestLazyConnectMixin, _TestExhaustCursorMixin)


class TestReplicaSetClientAgainstStandalone(unittest.TestCase):
    """This is a funny beast -- we want to run tests for MongoReplicaSetClient
    but only if the database at DB_IP and DB_PORT is a standalone.
    """
    def setUp(self):
        client = MongoClient(pair)
        response = client.admin.command('ismaster')
        if 'setName' in response:
            raise SkipTest("Connected to a replica set, not a standalone mongod")

    def test_connect(self):
        self.assertRaises(ConfigurationError, MongoReplicaSetClient,
                          pair, replicaSet='anything',
                          connectTimeoutMS=600)


class TestReplicaSetClientBase(unittest.TestCase):
    def setUp(self):
        client = MongoClient(pair)
        response = client.admin.command('ismaster')
        if 'setName' in response:
            self.name = str(response['setName'])
            self.w = len(response['hosts'])
            self.hosts = set([_partition_node(h)
                              for h in response["hosts"]])
            self.arbiters = set([_partition_node(h)
                                 for h in response.get("arbiters", [])])

            repl_set_status = client.admin.command('replSetGetStatus')
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

        super(TestReplicaSetClientBase, self).setUp()

    def _get_client(self, **kwargs):
        return MongoReplicaSetClient(pair,
            replicaSet=self.name,
            **kwargs)


class TestReplicaSetClient(TestReplicaSetClientBase, TestRequestMixin):
    def assertSoon(self, fn, msg=None):
        start = time.time()
        while time.time() - start < 10:
            if fn():
                return

            time.sleep(0.1)

        self.fail(msg)

    def assertIsInstance(self, obj, cls, msg=None):
        """Backport from Python 2.7."""
        if not isinstance(obj, cls):
            standardMsg = '%r is not an instance of %r' % (obj, cls)
            self.fail(self._formatMessage(msg, standardMsg))

    def test_init_disconnected(self):
        c = self._get_client(_connect=False)

        self.assertIsInstance(c.is_mongos, bool)
        self.assertIsInstance(c.max_pool_size, int)
        self.assertIsInstance(c.use_greenlets, bool)
        self.assertIsInstance(c.auto_start_request, bool)
        self.assertIsInstance(c.tz_aware, bool)
        self.assertIsInstance(c.max_bson_size, int)
        self.assertIsInstance(c.min_wire_version, int)
        self.assertIsInstance(c.max_wire_version, int)
        self.assertIsInstance(c.seeds, set)
        self.assertIsInstance(c.hosts, frozenset)
        self.assertIsInstance(c.arbiters, frozenset)
        self.assertEqual(dict, c.get_document_class())
        self.assertFalse(c.primary)
        self.assertFalse(c.secondaries)

        c.pymongo_test.test.find_one()  # Auto-connect for read.
        self.assertTrue(c.primary)
        self.assertTrue(c.secondaries)

        if version.at_least(c, (2, 5, 4, -1)):
            self.assertTrue(c.max_wire_version > 0)
        else:
            self.assertEqual(c.max_wire_version, 0)
        self.assertTrue(c.min_wire_version >= 0)

        c = self._get_client(_connect=False)
        c.pymongo_test.test.update({}, {})  # Auto-connect for write.
        self.assertTrue(c.primary)

        c = self._get_client(_connect=False)
        c.pymongo_test.test.insert({})  # Auto-connect for write.
        self.assertTrue(c.primary)

        c = self._get_client(_connect=False)
        c.pymongo_test.test.remove({})  # Auto-connect for write.
        self.assertTrue(c.primary)

        c = MongoReplicaSetClient(
            "somedomainthatdoesntexist.org", replicaSet="rs",
            connectTimeoutMS=1, _connect=False)

        self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

    def test_init_disconnected_with_auth_failure(self):
        c = MongoReplicaSetClient(
            "mongodb://user:pass@somedomainthatdoesntexist", replicaSet="rs",
            connectTimeoutMS=1, _connect=False)

        self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

    def test_init_disconnected_with_auth(self):
        c = self._get_client()
        if not server_started_with_auth(c):
            raise SkipTest('Authentication is not enabled on server')

        c.admin.add_user("admin", "pass")
        c.admin.authenticate("admin", "pass")
        try:
            c.pymongo_test.add_user("user", "pass", roles=['readWrite', 'userAdmin'])

            # Auth with lazy connection.
            host = one(self.hosts)
            uri = "mongodb://user:pass@%s:%d/pymongo_test?replicaSet=%s" % (
                host[0], host[1], self.name)

            authenticated_client = MongoReplicaSetClient(uri, _connect=False)
            authenticated_client.pymongo_test.test.find_one()

            # Wrong password.
            bad_uri = "mongodb://user:wrong@%s:%d/pymongo_test?replicaSet=%s" % (
                host[0], host[1], self.name)

            bad_client = MongoReplicaSetClient(bad_uri, _connect=False)
            self.assertRaises(
                OperationFailure, bad_client.pymongo_test.test.find_one)

        finally:
            # Clean up.
            remove_all_users(c.pymongo_test)
            remove_all_users(c.admin)

    def test_connect(self):
        assertRaisesExactly(ConnectionFailure, MongoReplicaSetClient,
                          "somedomainthatdoesntexist.org:27017",
                          replicaSet=self.name,
                          connectTimeoutMS=600)
        self.assertRaises(ConfigurationError, MongoReplicaSetClient,
                          pair, replicaSet='fdlksjfdslkjfd')
        self.assertTrue(MongoReplicaSetClient(pair, replicaSet=self.name))

    def test_repr(self):
        client = self._get_client()

        # Quirk: the RS client makes a frozenset of hosts from a dict's keys,
        # so we must do the same to achieve the same order.
        host_dict = dict([(host, 1) for host in self.hosts])
        hosts_set = frozenset(host_dict)
        hosts_repr = ', '.join([
            repr(unicode('%s:%s' % host)) for host in hosts_set])

        self.assertEqual(repr(client),
                         "MongoReplicaSetClient([%s])" % hosts_repr)

    def test_properties(self):
        c = MongoReplicaSetClient(pair, replicaSet=self.name)
        c.admin.command('ping')
        self.assertEqual(c.primary, self.primary)
        self.assertEqual(c.hosts, self.hosts)
        self.assertEqual(c.arbiters, self.arbiters)
        self.assertEqual(c.max_pool_size, 100)
        self.assertEqual(c.document_class, dict)
        self.assertEqual(c.tz_aware, False)

        # Make sure MRSC's properties are copied to Database and Collection
        for obj in c, c.pymongo_test, c.pymongo_test.test:
            self.assertEqual(obj.read_preference, ReadPreference.PRIMARY)
            self.assertEqual(obj.tag_sets, [{}])
            self.assertEqual(obj.secondary_acceptable_latency_ms, 15)
            self.assertEqual(obj.slave_okay, False)
            self.assertEqual(obj.write_concern, {})

        cursor = c.pymongo_test.test.find()
        self.assertEqual(
            ReadPreference.PRIMARY, cursor._Cursor__read_preference)
        self.assertEqual([{}], cursor._Cursor__tag_sets)
        self.assertEqual(15, cursor._Cursor__secondary_acceptable_latency_ms)
        self.assertEqual(False, cursor._Cursor__slave_okay)
        c.close()

        tag_sets = [{'dc': 'la', 'rack': '2'}, {'foo': 'bar'}]
        c = MongoReplicaSetClient(pair, replicaSet=self.name, max_pool_size=25,
                                 document_class=SON, tz_aware=True,
                                 slaveOk=False,
                                 read_preference=ReadPreference.SECONDARY,
                                 tag_sets=copy.deepcopy(tag_sets),
                                 secondary_acceptable_latency_ms=77)
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
            MongoReplicaSetClient(pair, replicaSet=self.name).use_greenlets)

        if have_gevent:
            self.assertTrue(MongoReplicaSetClient(
                pair, replicaSet=self.name, use_greenlets=True).use_greenlets)

    def test_get_db(self):
        client = self._get_client()

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
        client.close()

    def test_auto_reconnect_exception_when_read_preference_is_secondary(self):
        c = self._get_client()
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

    def test_lazy_auth_raises_operation_failure(self):
        # Check if we have the prerequisites to run this test.
        c = self._get_client()
        if not server_started_with_auth(c):
            raise SkipTest('Authentication is not enabled on server')

        lazy_client = MongoReplicaSetClient(
            "mongodb://user:wrong@%s/pymongo_test" % pair,
            replicaSet=self.name,
            _connect=False)

        assertRaisesExactly(
            OperationFailure, lazy_client.test.collection.find_one)

    def test_operations(self):
        c = self._get_client()

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
        db.test.remove({})
        self.assertEqual(0, db.test.count())
        db.test.insert({'foo': 'x'}, w=self.w, wtimeout=10000)
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
        db.test.remove({})
        self.assertEqual(0, db.test.count())
        db.test.drop()
        c.close()

    def test_database_names(self):
        client = self._get_client()

        client.pymongo_test.test.save({"dummy": u"object"})
        client.pymongo_test_mike.test.save({"dummy": u"object"})

        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_mike" in dbs)
        client.close()

    def test_drop_database(self):
        client = self._get_client()

        self.assertRaises(TypeError, client.drop_database, 5)
        self.assertRaises(TypeError, client.drop_database, None)

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
        client.close()

    def test_copy_db(self):
        c = self._get_client()
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

        if version.at_least(c, (1, 3, 3, 1)) and server_started_with_auth(c):
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
                res = c.pymongo_test1.test.find_one(_must_use_master=True)
                self.assertEqual("bar", res["foo"])
            finally:
                # Cleanup
                remove_all_users(c.pymongo_test)
                c.admin.remove_user("admin")
        c.close()

    def test_get_default_database(self):
        host = one(self.hosts)
        uri = "mongodb://%s:%d/foo?replicaSet=%s" % (
            host[0], host[1], self.name)

        c = MongoReplicaSetClient(uri, _connect=False)
        self.assertEqual(Database(c, 'foo'), c.get_default_database())

    def test_get_default_database_error(self):
        host = one(self.hosts)
        # URI with no database.
        uri = "mongodb://%s:%d/?replicaSet=%s" % (
            host[0], host[1], self.name)

        c = MongoReplicaSetClient(uri, _connect=False)
        self.assertRaises(ConfigurationError, c.get_default_database)

    def test_get_default_database_with_authsource(self):
        # Ensure we distinguish database name from authSource.
        host = one(self.hosts)
        uri = "mongodb://%s:%d/foo?replicaSet=%s&authSource=src" % (
            host[0], host[1], self.name)

        c = MongoReplicaSetClient(uri, _connect=False)
        self.assertEqual(Database(c, 'foo'), c.get_default_database())

    def test_iteration(self):
        client = self._get_client()

        def iterate():
            [a for a in client]

        self.assertRaises(TypeError, iterate)
        client.close()

    def test_disconnect(self):
        c = self._get_client()
        coll = c.pymongo_test.bar

        c.disconnect()
        c.disconnect()

        coll.count()

        c.disconnect()
        c.disconnect()

        coll.count()

    def test_close(self):
        # Multiple threads can call close() at once without error, and all
        # operations raise InvalidOperation afterward.
        c = self._get_client()
        nthreads = 10
        outcomes = []

        def close():
            c.close()
            outcomes.append(True)

        threads = [threading.Thread(target=close) for _ in range(nthreads)]
        for t in threads:
            t.start()

        for t in threads:
            t.join(10)

        self.assertEqual(nthreads, len(outcomes))
        self.assertRaises(InvalidOperation, c.db.collection.find_one)
        self.assertRaises(InvalidOperation, c.db.collection.insert, {})

    def test_fork(self):
        # After a fork the monitor thread is gone.
        # Verify that schedule_refresh restarts it.
        if sys.platform == "win32":
            raise SkipTest("Can't fork on Windows")

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest("No multiprocessing module")

        client = self._get_client()

        def f(pipe):
            try:
                # Trigger a refresh.
                self.assertFalse(
                    client._MongoReplicaSetClient__monitor.isAlive())

                client.disconnect()
                self.assertSoon(
                    lambda: client._MongoReplicaSetClient__monitor.isAlive())

                client.db.collection.find_one()  # No error.
            except:
                traceback.print_exc()
                pipe.send(True)

        cp, cc = Pipe()
        p = Process(target=f, args=(cc,))
        p.start()
        p.join(10)
        cc.close()

        # recv will only have data if the subprocess failed
        try:
            cp.recv()
            self.fail()
        except EOFError:
            pass

    def test_document_class(self):
        c = self._get_client()
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

        c = self._get_client(document_class=SON)
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
        c = self._get_client(socketTimeoutMS=10 * 1000)
        self.assertEqual(10, c._MongoReplicaSetClient__net_timeout)

        c = self._get_client(socketTimeoutMS=None)
        self.assertEqual(None, c._MongoReplicaSetClient__net_timeout)

        self.assertRaises(ConfigurationError,
            self._get_client, socketTimeoutMS=0)

        self.assertRaises(ConfigurationError,
            self._get_client, socketTimeoutMS=-1)

        self.assertRaises(ConfigurationError,
            self._get_client, socketTimeoutMS=1e10)

        self.assertRaises(ConfigurationError,
            self._get_client, socketTimeoutMS='foo')

        # network_timeout is gone from MongoReplicaSetClient, remains in
        # deprecated ReplicaSetConnection
        self.assertRaises(ConfigurationError,
            self._get_client, network_timeout=10)

    def test_network_timeout(self):
        no_timeout = self._get_client()
        timeout_sec = 1
        timeout = self._get_client(socketTimeoutMS=timeout_sec*1000)

        no_timeout.pymongo_test.drop_collection("test")
        no_timeout.pymongo_test.test.insert({"x": 1})

        # A $where clause that takes a second longer than the timeout.
        query = {'$where': delay(1 + timeout_sec)}
        no_timeout.pymongo_test.test.find_one(query)  # No error.

        try:
            timeout.pymongo_test.test.find_one(query)
        except AutoReconnect, e:
            self.assertTrue('%d: timed out' % (port,) in e.args[0])
        else:
            self.fail('RS client should have raised timeout error')

        timeout.pymongo_test.test.find_one(query, network_timeout=None)

        try:
            no_timeout.pymongo_test.test.find_one(query, network_timeout=0.1)
        except AutoReconnect, e:
            self.assertTrue('%d: timed out' % (port,) in e.args[0])
        else:
            self.fail('RS client should have raised timeout error')

        try:
            timeout.pymongo_test.test.find_one(
                query,
                read_preference=ReadPreference.SECONDARY)
        except AutoReconnect, e:
            # Like 'No replica set secondary available for query with
            # ReadPreference SECONDARY. host:27018: timed out,
            # host:27019: timed out'.
            self.assertTrue(
                str(e).startswith('No replica set secondary available'))

            self.assertTrue('timed out' in str(e))
        else:
            self.fail('RS client should have raised timeout error')

        no_timeout.close()
        timeout.close()

    def test_timeout_does_not_mark_member_down(self):
        # If a query times out, the RS client shouldn't mark the member "down".
        c = self._get_client(socketTimeoutMS=3000)
        collection = c.pymongo_test.test
        collection.insert({}, w=self.w)

        # Query the primary.
        self.assertRaises(
            ConnectionFailure,
            collection.find_one,
            {'$where': delay(5)})

        # primary_member returns None if primary is marked "down".
        rs_state = c._MongoReplicaSetClient__rs_state
        self.assertTrue(rs_state.primary_member)

        collection.find_one()  # No error.

        # Query the secondary.
        self.assertRaises(
            ConnectionFailure,
            collection.find_one,
            {'$where': delay(5)},
            read_preference=SECONDARY)

        rs_state = c._MongoReplicaSetClient__rs_state
        secondary_host = one(rs_state.secondaries)
        self.assertTrue(rs_state.get(secondary_host))
        collection.find_one(read_preference=SECONDARY)  # No error.

    def test_waitQueueTimeoutMS(self):
        client = self._get_client(waitQueueTimeoutMS=2000)
        pool = get_pool(client)
        self.assertEqual(pool.wait_queue_timeout, 2)

    def test_waitQueueMultiple(self):
        client = self._get_client(max_pool_size=3, waitQueueMultiple=2)
        pool = get_pool(client)
        self.assertEqual(pool.wait_queue_multiple, 2)
        self.assertEqual(pool._socket_semaphore.waiter_semaphore.counter, 6)

    def test_tz_aware(self):
        self.assertRaises(ConfigurationError, MongoReplicaSetClient,
                          tz_aware='foo', replicaSet=self.name)

        aware = self._get_client(tz_aware=True)
        naive = self._get_client()
        aware.pymongo_test.drop_collection("test")

        now = datetime.datetime.utcnow()
        aware.pymongo_test.test.insert({"x": now})
        time.sleep(1)

        self.assertEqual(None, naive.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(utc, aware.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(
                aware.pymongo_test.test.find_one()["x"].replace(tzinfo=None),
                naive.pymongo_test.test.find_one()["x"])

    def test_ipv6(self):
        try:
            client = MongoReplicaSetClient("[::1]:%d" % (port,),
                                              replicaSet=self.name)
        except:
            # Either mongod was started without --ipv6
            # or the OS doesn't support it (or both).
            raise SkipTest("No IPv6")

        # Try a few simple things
        client = MongoReplicaSetClient("mongodb://[::1]:%d" % (port,),
                                          replicaSet=self.name)
        client = MongoReplicaSetClient("mongodb://[::1]:%d/?w=0;"
                                          "replicaSet=%s" % (port, self.name))
        client = MongoReplicaSetClient("[::1]:%d,localhost:"
                                          "%d" % (port, port),
                                          replicaSet=self.name)
        client = MongoReplicaSetClient("localhost:%d,[::1]:"
                                          "%d" % (port, port),
                                          replicaSet=self.name)
        client.pymongo_test.test.save({"dummy": u"object"})
        client.pymongo_test_bernie.test.save({"dummy": u"object"})

        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_bernie" in dbs)
        client.close()

    def _test_kill_cursor_explicit(self, read_pref):
        c = self._get_client(read_preference=read_pref)
        db = c.pymongo_test
        db.drop_collection("test")

        test = db.test
        test.insert([{"i": i} for i in range(20)], w=1 + len(c.secondaries))

        # Partially evaluate cursor so it's left alive, then kill it
        cursor = test.find().batch_size(10)
        cursor.next()
        self.assertNotEqual(0, cursor.cursor_id)

        connection_id = cursor._Cursor__connection_id
        writer = c._MongoReplicaSetClient__rs_state.writer
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

        # Test fix for PYTHON-294 -- make sure client closes its socket if it
        # gets an interrupt while waiting to recv() from it.
        c = self._get_client()
        db = c.pymongo_test

        # A $where clause which takes 1.5 sec to execute
        where = delay(1.5)

        # Need exactly 1 document so find() will execute its $where clause once
        db.drop_collection('foo')
        db.foo.insert({'_id': 1})

        old_signal_handler = None

        try:
            def interrupter():
                time.sleep(0.25)

                # Raises KeyboardInterrupt in the main thread
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
        # Ensure MongoReplicaSetClient doesn't close socket after it gets an
        # error response to getLastError. PYTHON-395.
        c = self._get_client(auto_start_request=False)
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
        c.close()

    def test_operation_failure_with_request(self):
        # Ensure MongoReplicaSetClient doesn't close socket after it gets an
        # error response to getLastError. PYTHON-395.
        c = self._get_client(auto_start_request=True)
        c.pymongo_test.test.find_one()
        pool = get_pool(c)

        # Client reserved a socket for this thread
        self.assertTrue(isinstance(pool._get_request_state(), SocketInfo))

        old_sock_info = pool._get_request_state()
        c.pymongo_test.test.drop()
        c.pymongo_test.test.insert({'_id': 'foo'})
        self.assertRaises(
            OperationFailure,
            c.pymongo_test.test.insert, {'_id': 'foo'})

        # OperationFailure doesn't affect the request socket
        self.assertEqual(old_sock_info, pool._get_request_state())
        c.close()

    def test_auto_start_request(self):
        for bad_horrible_value in (None, 5, 'hi!'):
            self.assertRaises(
                (TypeError, ConfigurationError),
                lambda: self._get_client(auto_start_request=bad_horrible_value)
            )

        client = self._get_client(auto_start_request=True)
        self.assertTrue(client.auto_start_request)
        pools = pools_from_rs_client(client)
        self.assertInRequestAndSameSock(client, pools)

        primary_pool = get_pool(client)

        # Trigger the RSC to actually start a request on primary pool
        client.pymongo_test.test.find_one()
        self.assertTrue(primary_pool.in_request())

        # Trigger the RSC to actually start a request on secondary pool
        cursor = client.pymongo_test.test.find(
                read_preference=ReadPreference.SECONDARY)
        try:
            cursor.next()
        except StopIteration:
            # No results, no problem
            pass

        secondary = cursor._Cursor__connection_id
        rs_state = client._MongoReplicaSetClient__rs_state
        secondary_pool = rs_state.get(secondary).pool
        self.assertTrue(secondary_pool.in_request())

        client.end_request()
        self.assertNotInRequestAndDifferentSock(client, pools)
        for pool in pools:
            self.assertFalse(pool.in_request())
        client.start_request()
        self.assertInRequestAndSameSock(client, pools)
        client.close()

        client = self._get_client()
        pools = pools_from_rs_client(client)
        self.assertNotInRequestAndDifferentSock(client, pools)
        client.start_request()
        self.assertInRequestAndSameSock(client, pools)
        client.end_request()
        self.assertNotInRequestAndDifferentSock(client, pools)
        client.close()

    def test_nested_request(self):
        client = self._get_client(auto_start_request=True)
        try:
            pools = pools_from_rs_client(client)
            self.assertTrue(client.in_request())

            # Start and end request - we're still in "outer" original request
            client.start_request()
            self.assertInRequestAndSameSock(client, pools)
            client.end_request()
            self.assertInRequestAndSameSock(client, pools)

            # Double-nesting
            client.start_request()
            client.start_request()
            self.assertEqual(
                3, client._MongoReplicaSetClient__request_counter.get())

            for pool in pools:
                # MRSC only called start_request() once per pool, although its
                # own counter is 2.
                self.assertEqual(1, pool._request_counter.get())

            client.end_request()
            client.end_request()
            self.assertInRequestAndSameSock(client, pools)

            self.assertEqual(
                1, client._MongoReplicaSetClient__request_counter.get())

            for pool in pools:
                self.assertEqual(1, pool._request_counter.get())

            # Finally, end original request
            client.end_request()
            for pool in pools:
                self.assertFalse(pool.in_request())

            self.assertNotInRequestAndDifferentSock(client, pools)
        finally:
            client.close()

    def test_request_threads(self):
        client = self._get_client()
        try:
            pools = pools_from_rs_client(client)
            self.assertNotInRequestAndDifferentSock(client, pools)

            started_request, ended_request = threading.Event(), threading.Event()
            checked_request = threading.Event()
            thread_done = [False]

            # Starting a request in one thread doesn't put the other thread in a
            # request
            def f():
                self.assertNotInRequestAndDifferentSock(client, pools)
                client.start_request()
                self.assertInRequestAndSameSock(client, pools)
                started_request.set()
                checked_request.wait()
                checked_request.clear()
                self.assertInRequestAndSameSock(client, pools)
                client.end_request()
                self.assertNotInRequestAndDifferentSock(client, pools)
                ended_request.set()
                checked_request.wait()
                thread_done[0] = True

            t = threading.Thread(target=f)
            t.setDaemon(True)
            t.start()
            started_request.wait()
            self.assertNotInRequestAndDifferentSock(client, pools)
            checked_request.set()
            ended_request.wait()
            self.assertNotInRequestAndDifferentSock(client, pools)
            checked_request.set()
            t.join()
            self.assertNotInRequestAndDifferentSock(client, pools)
            self.assertTrue(thread_done[0], "Thread didn't complete")
        finally:
            client.close()

    def test_schedule_refresh(self):
        client = self._get_client()
        new_rs_state = rs_state = client._MongoReplicaSetClient__rs_state
        for host in rs_state.hosts:
            new_rs_state = new_rs_state.clone_with_host_down(host, 'error!')

        client._MongoReplicaSetClient__rs_state = new_rs_state
        client._MongoReplicaSetClient__schedule_refresh(sync=True)
        rs_state = client._MongoReplicaSetClient__rs_state
        self.assertEqual(
            self.w, len(rs_state.members),
            "MongoReplicaSetClient didn't detect members are up")

        client.close()

    def test_pinned_member(self):
        latency = 1000 * 1000
        client = self._get_client(secondary_acceptable_latency_ms=latency)

        host = read_from_which_host(client, ReadPreference.SECONDARY)
        self.assertTrue(host in client.secondaries)

        # No pinning since we're not in a request
        assertReadFromAll(
            self, client, client.secondaries,
            ReadPreference.SECONDARY, None, latency)

        assertReadFromAll(
            self, client, list(client.secondaries) + [client.primary],
            ReadPreference.NEAREST, None, latency)

        client.start_request()
        host = read_from_which_host(client, ReadPreference.SECONDARY)
        self.assertTrue(host in client.secondaries)
        assertReadFrom(self, client, host, ReadPreference.SECONDARY)

        # Changing any part of read preference (mode, tag_sets, latency)
        # unpins the current host and pins to a new one
        primary = client.primary
        assertReadFrom(self, client, primary, ReadPreference.PRIMARY_PREFERRED)

        host = read_from_which_host(client, ReadPreference.NEAREST)
        assertReadFrom(self, client, host, ReadPreference.NEAREST)

        assertReadFrom(self, client, primary, ReadPreference.PRIMARY_PREFERRED)

        host = read_from_which_host(client, ReadPreference.SECONDARY_PREFERRED)
        self.assertTrue(host in client.secondaries)
        assertReadFrom(self, client, host, ReadPreference.SECONDARY_PREFERRED)

        # Unpin
        client.end_request()
        assertReadFromAll(
            self, client, list(client.secondaries) + [client.primary],
            ReadPreference.NEAREST, None, latency)

    def test_alive(self):
        client = self._get_client()
        self.assertTrue(client.alive())

        client = MongoReplicaSetClient(
            'doesnt exist', replicaSet='rs', _connect=False)

        self.assertFalse(client.alive())

    def test_auth_network_error(self):
        # Make sure there's no semaphore leak if we get a network error
        # when authenticating a new socket with cached credentials.
        auth_client = self._get_client()
        if not server_started_with_auth(auth_client):
            raise SkipTest('Authentication is not enabled on server')

        auth_client.admin.add_user('admin', 'password')
        auth_client.admin.authenticate('admin', 'password')
        try:
            # Get a client with one socket so we detect if it's leaked.
            c = self._get_client(max_pool_size=1, waitQueueTimeoutMS=1)

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


class TestReplicaSetWireVersion(unittest.TestCase):
    def test_wire_version(self):
        c = MockReplicaSetClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='a:1',
            replicaSet='rs',
            _connect=False)

        c.set_wire_version_range('a:1', 1, 5)
        c.set_wire_version_range('b:2', 0, 1)
        c.set_wire_version_range('c:3', 1, 2)
        c.db.collection.find_one()  # Connect.
        self.assertEqual(c.min_wire_version, 1)
        self.assertEqual(c.max_wire_version, 5)

        c.set_wire_version_range('a:1', 2, 2)
        c.refresh()
        self.assertEqual(c.min_wire_version, 2)
        self.assertEqual(c.max_wire_version, 2)

        # A secondary doesn't overlap with us.
        c.set_wire_version_range('b:2', 5, 6)

        # refresh() raises, as do all following operations.
        self.assertRaises(ConfigurationError, c.refresh)
        self.assertRaises(ConfigurationError, c.db.collection.find_one)
        self.assertRaises(ConfigurationError, c.db.collection.insert, {})


# Test concurrent access to a lazily-connecting RS client.
class TestReplicaSetClientLazyConnect(
        TestReplicaSetClientBase,
        _TestLazyConnectMixin):

    def test_read_mode_secondary(self):
        client = MongoReplicaSetClient(
            pair, replicaSet=self.name, _connect=False,
            read_preference=ReadPreference.SECONDARY)

        # No error.
        client.pymongo_test.test_collection.find_one()


# Test concurrent access to a lazily-connecting RS client, with Gevent.
class TestReplicaSetClientLazyConnectGevent(
        TestReplicaSetClientBase,
        _TestLazyConnectMixin):
    use_greenlets = True


class TestReplicaSetClientLazyConnectBadSeeds(
        TestReplicaSetClientBase,
        _TestLazyConnectMixin):

    def _get_client(self, **kwargs):
        kwargs.setdefault('connectTimeoutMS', 500)

        # Assume there are no open mongods listening on a.com, b.com, ....
        bad_seeds = ['%s.com' % chr(ord('a') + i) for i in range(5)]
        seeds = ','.join(bad_seeds + [pair])
        client = MongoReplicaSetClient(seeds, replicaSet=self.name, **kwargs)

        # In case of a slow test machine.
        client._refresh_timeout_sec = 30
        return client


class TestReplicaSetClientInternalIPs(unittest.TestCase):
    def test_connect_with_internal_ips(self):
        # Client is passed an IP it can reach, 'a:1', but the RS config
        # only contains unreachable IPs like 'internal-ip'. PYTHON-608.
        assertRaisesExactly(
            ConnectionFailure,
            MockReplicaSetClient,
            standalones=[],
            members=['a:1'],
            mongoses=[],
            ismaster_hosts=['internal-ip:27017'],
            host='a:1',
            replicaSet='rs')


class TestReplicaSetClientMaxWriteBatchSize(unittest.TestCase):
    def test_max_write_batch_size(self):
        c = MockReplicaSetClient(
            standalones=[],
            members=['a:1', 'b:2'],
            mongoses=[],
            host='a:1',
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
        c.refresh()
        self.assertEqual(c.max_write_batch_size, 2)


class TestReplicaSetClientExhaustCursor(
        _TestExhaustCursorMixin,
        TestReplicaSetClientBase):

    # Base class implements _get_client already.
    pass

if __name__ == "__main__":
    unittest.main()
