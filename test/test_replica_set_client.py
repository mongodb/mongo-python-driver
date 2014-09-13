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

import datetime
import signal
import socket
import sys
import time
import threading
import warnings

sys.path[0:0] = [""]

from bson.py3compat import thread, u, _unicode
from bson.son import SON
from bson.tz_util import utc
from pymongo import auth
from pymongo.database import Database
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            InvalidName,
                            OperationFailure,
                            NetworkTimeout)
from pymongo.mongo_client import _partition_node
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.pool import SocketInfo
from pymongo.read_preferences import ReadPreference, Secondary, Nearest
from test import (client_context,
                  client_knobs,
                  connection_string,
                  pair,
                  port,
                  SkipTest,
                  unittest,
                  db_pwd,
                  db_user,
                  MockClientTest)
from test.pymongo_mocks import MockClient
from test.utils import (
    delay, assertReadFrom, assertReadFromAll, read_from_which_host,
    remove_all_users, assertRaisesExactly, TestRequestMixin, one,
    server_started_with_auth, pools_from_rs_client, get_pool,
    get_rs_client, _TestLazyConnectMixin, connected, wait_until)
from test.version import Version


class TestReplicaSetClientAgainstStandalone(unittest.TestCase):
    """This is a funny beast -- we want to run tests for MongoReplicaSetClient
    but only if the database at DB_IP and DB_PORT is a standalone.
    """
    @client_context.require_connection
    def setUp(self):
        if client_context.setname:
            raise SkipTest("Connected to a replica set, not a standalone mongod")

    def test_connect(self):
        with client_knobs(server_wait_time=0.1):
            client = MongoReplicaSetClient(pair, replicaSet='anything')

            with self.assertRaises(AutoReconnect):
                client.test.test.find_one()


class TestReplicaSetClientBase(unittest.TestCase):

    @classmethod
    @client_context.require_replica_set
    def setUpClass(cls):
        cls.name = client_context.setname
        ismaster = client_context.ismaster
        cls.w = client_context.w
        cls.hosts = set(_partition_node(h) for h in ismaster['hosts'])
        cls.arbiters = set(_partition_node(h)
                           for h in ismaster.get("arbiters", []))

        repl_set_status = client_context.client.admin.command(
            'replSetGetStatus')
        primary_info = [
            m for m in repl_set_status['members']
            if m['stateStr'] == 'PRIMARY'
        ][0]

        cls.primary = _partition_node(primary_info['name'])
        cls.secondaries = set(
            _partition_node(m['name']) for m in repl_set_status['members']
            if m['stateStr'] == 'SECONDARY')

    def _get_client(self, **kwargs):
        return get_rs_client(connection_string(),
                             replicaSet=self.name, **kwargs)


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

    def test_deprecated(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            with self.assertRaises(DeprecationWarning):
                MongoReplicaSetClient()

    def test_init_disconnected(self):
        c = self._get_client(connect=False)

        self.assertIsInstance(c.is_mongos, bool)
        self.assertIsInstance(c.max_pool_size, int)
        self.assertIsInstance(c.tz_aware, bool)
        self.assertIsInstance(c.max_bson_size, int)
        self.assertIsInstance(c.min_wire_version, int)
        self.assertIsInstance(c.max_wire_version, int)
        self.assertIsInstance(c.arbiters, set)
        self.assertEqual(dict, c.get_document_class())
        self.assertFalse(c.primary)
        self.assertFalse(c.secondaries)

        connected(c)
        self.assertTrue(c.primary)
        wait_until(lambda: c.secondaries, "found secondaries")

        if Version.from_client(c).at_least(2, 5, 4, -1):
            self.assertTrue(c.max_wire_version > 0)
        else:
            self.assertEqual(c.max_wire_version, 0)
        self.assertTrue(c.min_wire_version >= 0)

        c = self._get_client(connect=False)
        c.pymongo_test.test.update({}, {})  # Auto-connect for write.
        self.assertTrue(c.primary)

        c = self._get_client(connect=False)
        c.pymongo_test.test.insert({})  # Auto-connect for write.
        self.assertTrue(c.primary)

        c = self._get_client(connect=False)
        c.pymongo_test.test.remove({})  # Auto-connect for write.
        self.assertTrue(c.primary)

        with client_knobs(server_wait_time=0.1):
            c = MongoReplicaSetClient(
                "somedomainthatdoesntexist.org", replicaSet="rs",
                connectTimeoutMS=1, connect=False)

            self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

    def test_init_disconnected_with_auth_failure(self):
        with client_knobs(server_wait_time=0.1):
            c = MongoReplicaSetClient(
                "mongodb://user:pass@somedomainthatdoesntexist",
                replicaSet="rs", connectTimeoutMS=1, connect=False)

            self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

    @client_context.require_auth
    def test_init_disconnected_with_auth(self):
        c = client_context.rs_client
        try:
            c.pymongo_test.add_user("user", "pass",
                                    roles=['readWrite', 'userAdmin'])

            # Auth with lazy connection.
            host = one(self.hosts)
            uri = "mongodb://user:pass@%s:%d/pymongo_test?replicaSet=%s" % (
                host[0], host[1], self.name)

            authenticated_client = MongoReplicaSetClient(uri, connect=False)
            authenticated_client.pymongo_test.test.find_one()

            # Wrong password.
            bad_uri = "mongodb://user:wrong@%s:%d/pymongo_test?replicaSet=%s" % (
                host[0], host[1], self.name)

            bad_client = MongoReplicaSetClient(bad_uri, connect=False)
            self.assertRaises(
                OperationFailure, bad_client.pymongo_test.test.find_one)

        finally:
            # Clean up.
            remove_all_users(c.pymongo_test)

    def test_connect(self):
        with client_knobs(server_wait_time=0.1):
            client = MongoReplicaSetClient(
                "somedomainthatdoesntexist.org:27017",
                replicaSet=self.name)

            with self.assertRaises(AutoReconnect):
                client.test.test.find_one()

            client = MongoReplicaSetClient(pair, replicaSet='fdlksjfdslkjfd')

            with self.assertRaises(ConnectionFailure):
                client.test.test.find_one()

    def test_repr(self):
        client = client_context.rs_client

        # Quirk: the RS client makes a frozenset of hosts from a dict's keys,
        # so we must do the same to achieve the same order.
        host_dict = dict([(host, 1) for host in self.hosts])
        hosts_set = frozenset(host_dict)
        hosts_repr = ', '.join([
            repr(_unicode('%s:%s' % host)) for host in hosts_set])

        self.assertEqual(repr(client),
                         "MongoReplicaSetClient([%s])" % hosts_repr)

    def test_properties(self):
        c = client_context.rs_client
        c.admin.command('ping')
        self.assertEqual(c.primary, self.primary)
        self.assertEqual(c.secondaries, self.secondaries)
        self.assertEqual(c.arbiters, self.arbiters)
        self.assertEqual(c.max_pool_size, 100)
        self.assertEqual(c.document_class, dict)
        self.assertEqual(c.tz_aware, False)

        # Make sure MRSC's properties are copied to Database and Collection
        for obj in c, c.pymongo_test, c.pymongo_test.test:
            self.assertEqual(obj.read_preference, ReadPreference.PRIMARY)
            self.assertEqual(obj.write_concern, {})

        cursor = c.pymongo_test.test.find()
        self.assertEqual(
            ReadPreference.PRIMARY, cursor._Cursor__read_preference)

        tag_sets = [{'dc': 'la', 'rack': '2'}, {'foo': 'bar'}]
        secondary = Secondary(tag_sets=tag_sets)
        c = MongoReplicaSetClient(
            pair, replicaSet=self.name, max_pool_size=25,
            document_class=SON, tz_aware=True,
            read_preference=secondary,
            secondaryacceptablelatencyms=77)

        wait_until(lambda: c.primary == self.primary, "discover primary")
        wait_until(lambda: c.arbiters == self.arbiters, "discover arbiters")
        wait_until(lambda: c.secondaries == self.secondaries,
                   "discover secondaries")

        self.assertEqual(c.max_pool_size, 25)
        self.assertEqual(c.document_class, SON)
        self.assertEqual(c.tz_aware, True)

        for obj in c, c.pymongo_test, c.pymongo_test.test:
            self.assertEqual(obj.read_preference, secondary)

        cursor = c.pymongo_test.test.find()
        self.assertEqual(
            secondary, cursor._Cursor__read_preference)

        nearest = Nearest(tag_sets=[{'dc': 'ny'}, {}])
        cursor = c.pymongo_test.test.find(read_preference=nearest)

        self.assertEqual(
            nearest, cursor._Cursor__read_preference)

        if Version.from_client(c).at_least(1, 7, 4):
            self.assertEqual(c.max_bson_size, 16777216)
        else:
            self.assertEqual(c.max_bson_size, 4194304)
        c.close()

    def test_get_db(self):
        client = client_context.rs_client

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

    def test_auto_reconnect_exception_when_read_preference_is_secondary(self):
        with client_knobs(server_wait_time=0.1):
            c = MongoReplicaSetClient(pair, replicaSet=self.name)
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

    @client_context.require_auth
    def test_lazy_auth_raises_operation_failure(self):
        # Check if we have the prerequisites to run this test.
        lazy_client = MongoReplicaSetClient(
            "mongodb://user:wrong@%s/pymongo_test" % pair,
            replicaSet=self.name,
            connect=False)

        assertRaisesExactly(
            OperationFailure, lazy_client.test.collection.find_one)

    def test_operations(self):
        c = client_context.rs_client

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
        doc = next(cursor)
        self.assertEqual('x', doc['foo'])
        # Ensure we read from the primary
        self.assertEqual(c.primary, cursor._Cursor__connection_id)

        cursor = db.test.find(read_preference=ReadPreference.SECONDARY)
        doc = next(cursor)
        self.assertEqual('x', doc['foo'])
        # Ensure we didn't read from the primary
        self.assertTrue(cursor._Cursor__connection_id in c.secondaries)

        self.assertEqual(1, db.test.count())
        db.test.remove({})
        self.assertEqual(0, db.test.count())
        db.test.drop()

    def test_database_names(self):
        client = client_context.rs_client

        client.pymongo_test.test.save({"dummy": u("object")})
        client.pymongo_test_mike.test.save({"dummy": u("object")})

        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_mike" in dbs)

    def test_drop_database(self):
        client = client_context.rs_client

        self.assertRaises(TypeError, client.drop_database, 5)
        self.assertRaises(TypeError, client.drop_database, None)

        client.pymongo_test.test.save({"dummy": u("object")})
        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        client.drop_database("pymongo_test")
        dbs = client.database_names()
        self.assertTrue("pymongo_test" not in dbs)

        client.pymongo_test.test.save({"dummy": u("object")})
        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        client.drop_database(client.pymongo_test)
        dbs = client.database_names()
        self.assertTrue("pymongo_test" not in dbs)

    def test_copy_db(self):
        c = client_context.rs_client
        # We test copy twice; once starting in a request and once not. In
        # either case the copy should succeed (because it starts a request
        # internally) and should leave us in the same state as before the copy.
        with c.start_request():
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

        self.assertFalse(c.in_request())

        c.copy_database("pymongo_test", "pymongo_test2")
        # copy_database() didn't accidentally restart the request
        self.assertFalse(c.in_request())

        time.sleep(1)

        self.assertTrue("pymongo_test2" in c.database_names())
        self.assertEqual("bar", c.pymongo_test2.test.find_one()["foo"])

        if (Version.from_client(c).at_least(1, 3, 3, 1) and
                server_started_with_auth(c)):
            c.drop_database("pymongo_test1")

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
                res = c.pymongo_test1.test.find_one(
                    read_preference=ReadPreference.PRIMARY)
                self.assertEqual("bar", res["foo"])
            finally:
                # Cleanup
                remove_all_users(c.pymongo_test)

    def test_get_default_database(self):
        host = one(self.hosts)
        uri = "mongodb://%s:%d/foo?replicaSet=%s" % (
            host[0], host[1], self.name)

        c = MongoReplicaSetClient(uri, connect=False)
        self.assertEqual(Database(c, 'foo'), c.get_default_database())

    def test_get_default_database_error(self):
        host = one(self.hosts)
        # URI with no database.
        uri = "mongodb://%s:%d/?replicaSet=%s" % (
            host[0], host[1], self.name)

        c = MongoReplicaSetClient(uri, connect=False)
        self.assertRaises(ConfigurationError, c.get_default_database)

    def test_get_default_database_with_authsource(self):
        # Ensure we distinguish database name from authSource.
        host = one(self.hosts)
        uri = "mongodb://%s:%d/foo?replicaSet=%s&authSource=src" % (
            host[0], host[1], self.name)

        c = MongoReplicaSetClient(uri, connect=False)
        self.assertEqual(Database(c, 'foo'), c.get_default_database())

    def test_iteration(self):
        client = client_context.rs_client

        def iterate():
            [a for a in client]

        self.assertRaises(TypeError, iterate)

    def test_disconnect(self):
        c = client_context.rs_client
        coll = c.pymongo_test.bar

        c.disconnect()
        c.disconnect()

        coll.count()

        c.disconnect()
        c.disconnect()

        coll.count()

    def test_close(self):
        # Multiple threads can call close() at once without error.
        # Subsequent operations reopen the client.
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

        # No error.
        c.db.collection.insert({})
        c.db.collection.find_one()

    def test_socket_timeout_ms_validation(self):
        c = self._get_client(socketTimeoutMS=10 * 1000)
        self.assertEqual(10,
                         c._MongoClient__options.pool_options.socket_timeout)

        c = self._get_client(socketTimeoutMS=None)
        self.assertEqual(None,
                         c._MongoClient__options.pool_options.socket_timeout)

        self.assertRaises(ConfigurationError,
            self._get_client, socketTimeoutMS=0)

        self.assertRaises(ConfigurationError,
            self._get_client, socketTimeoutMS=-1)

        self.assertRaises(ConfigurationError,
            self._get_client, socketTimeoutMS=1e10)

        self.assertRaises(ConfigurationError,
            self._get_client, socketTimeoutMS='foo')

    def test_socket_timeout(self):
        no_timeout = client_context.rs_client
        timeout_sec = 1
        timeout = self._get_client(socketTimeoutMS=timeout_sec*1000)

        no_timeout.pymongo_test.drop_collection("test")
        no_timeout.pymongo_test.test.insert({"x": 1})

        # A $where clause that takes a second longer than the timeout.
        query = {'$where': delay(1 + timeout_sec)}
        no_timeout.pymongo_test.test.find_one(query)  # No error.

        try:
            timeout.pymongo_test.test.find_one(query)
        except AutoReconnect as e:
            self.assertTrue('%d: timed out' % (port,) in e.args[0])
        else:
            self.fail('RS client should have raised timeout error')

        try:
            timeout.pymongo_test.test.find_one(
                query,
                read_preference=ReadPreference.SECONDARY)
        except NetworkTimeout as e:
            self.assertTrue(
                any('%d: timed out' % address[1] in e.args[0]
                    for address in self.secondaries),
                "%r does not mention any secondary's port" % e.args[0])
        else:
            self.fail('RS client should have raised timeout error')

    def test_timeout_does_not_mark_member_down(self):
        # If a query times out, the RS client shouldn't mark the member "down".

        # Disable background refresh.
        with client_knobs(heartbeat_frequency=999999):
            c = self._get_client(socketTimeoutMS=3000)
            collection = c.pymongo_test.test
            collection.insert({}, w=self.w)

            # Query the primary.
            self.assertRaises(
                NetworkTimeout,
                collection.find_one,
                {'$where': delay(5)})

            self.assertTrue(c.primary)
            collection.find_one()  # No error.

            # Query the secondary.
            self.assertRaises(
                NetworkTimeout,
                collection.find_one,
                {'$where': delay(5)},
                read_preference=ReadPreference.SECONDARY)

            self.assertTrue(c.secondaries)

            # No error.
            collection.find_one(read_preference=ReadPreference.SECONDARY)

    def test_waitQueueTimeoutMS(self):
        client = self._get_client(waitQueueTimeoutMS=2000)
        pool = get_pool(client)
        self.assertEqual(pool.opts.wait_queue_timeout, 2)

    def test_waitQueueMultiple(self):
        client = self._get_client(max_pool_size=3, waitQueueMultiple=2)
        pool = get_pool(client)
        self.assertEqual(pool.opts.wait_queue_multiple, 2)
        self.assertEqual(pool._socket_semaphore.waiter_semaphore.counter, 6)

    def test_tz_aware(self):
        self.assertRaises(ConfigurationError, MongoReplicaSetClient,
                          tz_aware='foo', replicaSet=self.name)

        aware = self._get_client(tz_aware=True)
        naive = client_context.rs_client
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
            MongoReplicaSetClient("[::1]:%d" % (port,), replicaSet=self.name)
        except:
            # Either mongod was started without --ipv6
            # or the OS doesn't support it (or both).
            raise SkipTest("No IPv6")

        # Try a few simple things
        MongoReplicaSetClient("mongodb://[::1]:%d" % (port,),
                              replicaSet=self.name)
        MongoReplicaSetClient("mongodb://[::1]:%d/?w=0;"
                              "replicaSet=%s" % (port, self.name))
        MongoReplicaSetClient("[::1]:%d,localhost:"
                              "%d" % (port, port),
                              replicaSet=self.name)

        if client_context.auth_enabled:
            auth_str = "%s:%s@" % (db_user, db_pwd)
        else:
            auth_str = ""

        uri = "mongodb://%slocalhost:%d,[::1]:%d" % (auth_str, port, port)
        client = MongoReplicaSetClient(uri, replicaSet=self.name)
        client.pymongo_test.test.save({"dummy": u("object")})
        client.pymongo_test_bernie.test.save({"dummy": u("object")})

        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_bernie" in dbs)
        client.close()

    def _test_kill_cursor_explicit(self, read_pref):
        c = self._get_client(read_preference=read_pref)
        db = c.pymongo_test
        db.drop_collection("test")

        test = db.test
        test.insert([{"i": i} for i in range(20)], w=self.w)

        # Partially evaluate cursor so it's left alive, then kill it
        cursor = test.find().batch_size(10)
        next(cursor)
        self.assertNotEqual(0, cursor.cursor_id)

        connection_id = cursor._Cursor__connection_id
        if read_pref == ReadPreference.PRIMARY:
            msg = "Expected cursor's connection_id to be %s, got %s" % (
                c.primary, connection_id)

            self.assertEqual(connection_id, c.primary, msg)
        else:
            self.assertNotEqual(
                connection_id, c.primary,
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
        c = client_context.rs_client
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
                next(db.foo.find({'$where': where}))
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
                next(db.foo.find())
            )
        finally:
            if old_signal_handler:
                signal.signal(signal.SIGALRM, old_signal_handler)

    def test_operation_failure_without_request(self):
        # Ensure MongoReplicaSetClient doesn't close socket after it gets an
        # error response to getLastError. PYTHON-395.
        c = self._get_client()
        connected(c)
        pool = get_pool(c)
        self.assertEqual(1, len(pool.sockets))
        old_sock_info = next(iter(pool.sockets))
        c.pymongo_test.test.drop()
        c.pymongo_test.test.insert({'_id': 'foo'})
        self.assertRaises(
            OperationFailure,
            c.pymongo_test.test.insert, {'_id': 'foo'})

        self.assertEqual(1, len(pool.sockets))
        new_sock_info = next(iter(pool.sockets))

        self.assertEqual(old_sock_info, new_sock_info)
        c.close()

    def test_operation_failure_with_request(self):
        # Ensure MongoReplicaSetClient doesn't close socket after it gets an
        # error response to getLastError. PYTHON-395.
        c = self._get_client()
        c.start_request()
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

    def test_nested_request(self):
        client = self._get_client()
        connected(client)
        client.start_request()
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

            for pool in pools:
                # MRSC only called start_request() once per pool, although its
                # own counter is 3.
                self.assertEqual(1, pool._request_counter.get())

            client.end_request()
            client.end_request()
            self.assertInRequestAndSameSock(client, pools)

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
        client = client_context.rs_client

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

    def test_pinned_member(self):
        raise SkipTest("Secondary pinning not implemented in PyMongo 3")

        latency = 1000 * 1000
        client = self._get_client(secondaryacceptablelatencyms=latency)

        host = read_from_which_host(client, ReadPreference.SECONDARY)
        self.assertTrue(host in client.secondaries)

        # No pinning since we're not in a request
        assertReadFromAll(
            self, client, client.secondaries,
            ReadPreference.SECONDARY, None)

        assertReadFromAll(
            self, client, list(client.secondaries) + [client.primary],
            ReadPreference.NEAREST, None)

        client.start_request()
        host = read_from_which_host(client, ReadPreference.SECONDARY)
        self.assertTrue(host in client.secondaries)
        assertReadFrom(self, client, host, ReadPreference.SECONDARY)

        # Changing any part of read preference (mode, tag_sets)
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
            ReadPreference.NEAREST, None)

    def test_alive(self):
        client = client_context.rs_client
        self.assertTrue(client.alive())

        client = MongoReplicaSetClient(
            'doesnt exist', replicaSet='rs', connect=False)

        self.assertFalse(client.alive())

    @client_context.require_auth
    def test_auth_network_error(self):
        # Make sure there's no semaphore leak if we get a network error
        # when authenticating a new socket with cached credentials.

        # Get a client with one socket so we detect if it's leaked.
        c = self._get_client(max_pool_size=1, waitQueueTimeoutMS=1)

        # Simulate an authenticate() call on a different socket.
        credentials = auth._build_credentials_tuple(
            'MONGODB-CR', 'admin', db_user, db_pwd, {})

        c._cache_credentials('test', credentials, connect=False)

        # Cause a network error on the actual socket.
        connected(c)
        pool = get_pool(c)
        socket_info = one(pool.sockets)
        socket_info.sock.close()

        # In __check_auth, the client authenticates its socket with the
        # new credential, but gets a socket.error. Should be reraised as
        # AutoReconnect.
        self.assertRaises(AutoReconnect, c.test.collection.find_one)

        # No semaphore leak, the pool is allowed to make a new socket.
        c.test.collection.find_one()


class TestReplicaSetWireVersion(MockClientTest):

    @client_context.require_connection
    @client_context.require_no_auth
    def test_wire_version(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='a:1',
            replicaSet='rs',
            connect=False)

        c.set_wire_version_range('a:1', 1, 5)
        c.set_wire_version_range('b:2', 0, 1)
        c.set_wire_version_range('c:3', 1, 2)
        c.db.command('ismaster')  # Connect.
        self.assertEqual(c.min_wire_version, 1)
        self.assertEqual(c.max_wire_version, 5)

        c.set_wire_version_range('a:1', 2, 2)
        wait_until(lambda: c.min_wire_version == 2, 'update min_wire_version')
        self.assertEqual(c.max_wire_version, 2)

        # A secondary doesn't overlap with us.
        c.set_wire_version_range('b:2', 5, 6)

        def raises_configuration_error():
            try:
                c.db.collection.find_one()
                return False
            except ConfigurationError:
                return True

        wait_until(raises_configuration_error,
                   'notice we are incompatible with server')

        self.assertRaises(ConfigurationError, c.db.collection.insert, {})


# Test concurrent access to a lazily-connecting RS client.
class TestReplicaSetClientLazyConnect(
        TestReplicaSetClientBase,
        _TestLazyConnectMixin):

    @classmethod
    def setUpClass(cls):
        TestReplicaSetClientBase.setUpClass()

    def test_read_mode_secondary(self):
        client = MongoReplicaSetClient(
            connection_string(), replicaSet=self.name, connect=False,
            read_preference=ReadPreference.SECONDARY)

        # No error.
        client.pymongo_test.test_collection.find_one()


class TestReplicaSetClientLazyConnectBadSeeds(
        TestReplicaSetClientBase,
        _TestLazyConnectMixin):

    @classmethod
    def setUpClass(cls):
        TestReplicaSetClientBase.setUpClass()

    def _get_client(self, **kwargs):
        kwargs.setdefault('connectTimeoutMS', 500)

        # Assume there are no open mongods listening on a.com, b.com, ....
        bad_seeds = ['%s.com' % chr(ord('a') + i) for i in range(5)]
        client = MongoReplicaSetClient(
            connection_string(seeds=(bad_seeds + [pair])),
            replicaSet=self.name, **kwargs)

        # In case of a slow test machine.
        client._refresh_timeout_sec = 30
        return client


class TestReplicaSetClientInternalIPs(MockClientTest):

    @client_context.require_connection
    def test_connect_with_internal_ips(self):
        # Client is passed an IP it can reach, 'a:1', but the RS config
        # only contains unreachable IPs like 'internal-ip'. PYTHON-608.
        assertRaisesExactly(
            AutoReconnect,
            connected,
            MockClient(
                standalones=[],
                members=['a:1'],
                mongoses=[],
                ismaster_hosts=['internal-ip:27017'],
                host='a:1',
                replicaSet='rs'))


class TestReplicaSetClientMaxWriteBatchSize(MockClientTest):

    @client_context.require_connection
    def test_max_write_batch_size(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2'],
            mongoses=[],
            host='a:1',
            replicaSet='rs',
            connect=False)

        c.set_max_write_batch_size('a:1', 1)
        c.set_max_write_batch_size('b:2', 2)

        # Starts with default max batch size.
        self.assertEqual(1000, c.max_write_batch_size)
        c.db.command('ismaster')  # Connect.

        # Uses primary's max batch size.
        self.assertEqual(c.max_write_batch_size, 1)

        # b becomes primary.
        c.mock_primary = 'b:2'
        wait_until(lambda: c.max_write_batch_size == 2,
                   'update max_write_batch_size')


if __name__ == "__main__":
    unittest.main()
