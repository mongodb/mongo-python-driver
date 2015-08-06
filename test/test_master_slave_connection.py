# Copyright 2009-2015 MongoDB, Inc.
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

"""Test for master slave connections."""

import datetime
import sys
import threading
import time
import unittest
import warnings

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.son import SON
from bson.tz_util import utc
from pymongo import ReadPreference, thread_util
from pymongo.errors import ConnectionFailure, InvalidName
from pymongo.errors import CollectionInvalid, OperationFailure
from pymongo.errors import AutoReconnect
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection
from pymongo.master_slave_connection import MasterSlaveConnection
from test import (host, port,
                  host2, port2,
                  host3, port3,
                  skip_restricted_localhost)
from test.utils import TestRequestMixin, catch_warnings, get_pool


setUpModule = skip_restricted_localhost


class TestMasterSlaveConnection(unittest.TestCase, TestRequestMixin):

    def setUp(self):
        self.master = MongoClient(host, port)

        self.slaves = []
        try:
            self.slaves.append(MongoClient(
                host2, port2, read_preference=ReadPreference.SECONDARY))
        except ConnectionFailure:
            pass

        try:
            self.slaves.append(MongoClient(
                host3, port3, read_preference=ReadPreference.SECONDARY))
        except ConnectionFailure:
            pass

        if not self.slaves:
            raise SkipTest("Not connected to master-slave set")

        self.ctx = catch_warnings()
        warnings.simplefilter("ignore", DeprecationWarning)
        self.client = MasterSlaveConnection(self.master, self.slaves)
        self.db = self.client.pymongo_test

    def tearDown(self):
        self.ctx.exit()
        try:
            self.db.test.drop_indexes()
        except Exception:
            # Tests like test_disconnect can monkey with the client in ways
            # that make this fail
            pass

        self.master = self.slaves = self.db = self.client = None
        super(TestMasterSlaveConnection, self).tearDown()

    def test_types(self):
        self.assertRaises(TypeError, MasterSlaveConnection, 1)
        self.assertRaises(TypeError, MasterSlaveConnection, self.master, 1)
        self.assertRaises(TypeError, MasterSlaveConnection, self.master, [1])

    def test_use_greenlets(self):
        self.assertFalse(self.client.use_greenlets)

        if thread_util.have_gevent:
            master = MongoClient(host, port, use_greenlets=True)
            slaves = [
                MongoClient(slave.host, slave.port, use_greenlets=True)
                for slave in self.slaves]

            self.assertTrue(
                MasterSlaveConnection(master, slaves).use_greenlets)

    def test_repr(self):
        self.assertEqual(repr(self.client),
                         "MasterSlaveConnection(%r, %r)" %
                         (self.master, self.slaves))

    def test_disconnect(self):
        class MongoClient(object):
            def __init__(self):
                self._disconnects = 0

            def disconnect(self):
                self._disconnects += 1

        self.client._MasterSlaveConnection__master = MongoClient()
        self.client._MasterSlaveConnection__slaves = [MongoClient(),
                                                      MongoClient()]

        self.client.disconnect()
        self.assertEqual(1,
            self.client._MasterSlaveConnection__master._disconnects)
        self.assertEqual(1,
            self.client._MasterSlaveConnection__slaves[0]._disconnects)
        self.assertEqual(1,
            self.client._MasterSlaveConnection__slaves[1]._disconnects)

    def test_continue_until_slave_works(self):
        class Slave(object):
            calls = 0

            def __init__(self, fail):
                self._fail = fail

            def _send_message_with_response(self, *args, **kwargs):
                Slave.calls += 1
                if self._fail:
                    raise AutoReconnect()
                return (None, 'sent')

        class NotRandomList(object):
            last_idx = -1

            def __init__(self):
                self._items = [Slave(True), Slave(True),
                               Slave(False), Slave(True)]

            def __len__(self):
                return len(self._items)

            def __getitem__(self, idx):
                NotRandomList.last_idx = idx
                return self._items.pop(0)

        self.client._MasterSlaveConnection__slaves = NotRandomList()

        response = self.client._send_message_with_response('message')
        self.assertEqual((NotRandomList.last_idx, 'sent'), response)
        self.assertNotEqual(-1, NotRandomList.last_idx)
        self.assertEqual(3, Slave.calls)

    def test_raise_autoreconnect_if_all_slaves_fail(self):
        class Slave(object):
            calls = 0

            def __init__(self, fail):
                self._fail = fail

            def _send_message_with_response(self, *args, **kwargs):
                Slave.calls += 1
                if self._fail:
                    raise AutoReconnect()
                return 'sent'

        class NotRandomList(object):
            def __init__(self):
                self._items = [Slave(True), Slave(True),
                               Slave(True), Slave(True)]

            def __len__(self):
                return len(self._items)

            def __getitem__(self, idx):
                return self._items.pop(0)

        self.client._MasterSlaveConnection__slaves = NotRandomList()

        self.assertRaises(AutoReconnect,
            self.client._send_message_with_response, 'message')
        self.assertEqual(4, Slave.calls)

    def test_get_db(self):

        def make_db(base, name):
            return base[name]

        self.assertRaises(InvalidName, make_db, self.client, "")
        self.assertRaises(InvalidName, make_db, self.client, "te$t")
        self.assertRaises(InvalidName, make_db, self.client, "te.t")
        self.assertRaises(InvalidName, make_db, self.client, "te\\t")
        self.assertRaises(InvalidName, make_db, self.client, "te/t")
        self.assertRaises(InvalidName, make_db, self.client, "te st")

        self.assertTrue(isinstance(self.client.test, Database))
        self.assertEqual(self.client.test, self.client["test"])
        self.assertEqual(self.client.test, Database(self.client,
                                                        "test"))

    def test_database_names(self):
        self.client.pymongo_test.test.save({"dummy": u"object"})
        self.client.pymongo_test_mike.test.save({"dummy": u"object"})

        dbs = self.client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_mike" in dbs)

    def test_drop_database(self):
        self.assertRaises(TypeError, self.client.drop_database, 5)
        self.assertRaises(TypeError, self.client.drop_database, None)

        raise SkipTest("This test often fails due to SERVER-2329")

        self.client.pymongo_test.test.save({"dummy": u"object"})
        dbs = self.client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.client.drop_database("pymongo_test")
        dbs = self.client.database_names()
        self.assertTrue("pymongo_test" not in dbs)

        self.client.pymongo_test.test.save({"dummy": u"object"})
        dbs = self.client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.client.drop_database(self.client.pymongo_test)
        dbs = self.client.database_names()
        self.assertTrue("pymongo_test" not in dbs)

    def test_iteration(self):

        def iterate():
            [a for a in self.client]

        self.assertRaises(TypeError, iterate)

    def test_insert_find_one_in_request(self):
        count = 0
        for i in range(100):
            self.client.start_request()
            self.db.test.remove({})
            self.db.test.insert({"x": i})
            try:
                if i != self.db.test.find_one()["x"]:
                    count += 1
            except:
                count += 1
            self.client.end_request()
        self.assertFalse(count)

    def test_nested_request(self):
        client = self.client

        def assertRequest(in_request):
            self.assertEqual(in_request, client.in_request())
            self.assertEqual(in_request, client.master.in_request())

        # MasterSlaveConnection is special, alas - it has no auto_start_request
        # and it begins *not* in a request. When it's in a request, it sends
        # all queries to primary.
        self.assertFalse(client.in_request())
        self.assertFalse(client.master.in_request())

        # Start and end request
        client.start_request()
        assertRequest(True)
        client.end_request()
        assertRequest(False)

        # Double-nesting
        client.start_request()
        client.start_request()
        client.end_request()
        assertRequest(True)
        client.end_request()
        assertRequest(False)

    def test_request_threads(self):
        client = self.client

        # In a request, all ops go through master
        pool = get_pool(client.master)
        client.master.end_request()
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
        started_request.wait()
        self.assertNotInRequestAndDifferentSock(client, pool)
        checked_request.set()
        ended_request.wait()
        self.assertNotInRequestAndDifferentSock(client, pool)
        checked_request.set()
        t.join()
        self.assertNotInRequestAndDifferentSock(client, pool)
        self.assertTrue(thread_done[0], "Thread didn't complete")

    # This was failing because commands were being sent to the slaves
    def test_create_collection(self):
        self.client.pymongo_test.test.drop()

        collection = self.db.create_collection('test')
        self.assertTrue(isinstance(collection, Collection))

        self.assertRaises(CollectionInvalid, self.db.create_collection, 'test')

    # Believe this was failing for the same reason...
    def test_unique_index(self):
        self.client.pymongo_test.test.drop()
        self.db.test.create_index('username', unique=True)

        self.db.test.save({'username': 'mike'})
        self.assertRaises(OperationFailure,
                          self.db.test.save, {'username': 'mike'})

    def test_insert(self):
        w = len(self.slaves) + 1
        self.db.test.remove(w=w)
        self.assertEqual(0, self.db.test.count())
        doc = {}
        self.db.test.insert(doc, w=w)
        self.assertEqual(doc, self.db.test.find_one())

    def test_kill_cursor_explicit(self):
        c = self.client
        c.read_preference = ReadPreference.SECONDARY_PREFERRED
        db = c.pymongo_test

        test = db.master_slave_test_kill_cursor_explicit
        test.drop()

        for i in range(20):
            test.insert({"i": i}, w=1 + len(self.slaves))

        st = time.time()
        while time.time() - st < 120:
            # Wait for replication -- the 'w' parameter should obviate this
            # loop but it's not working reliably in Jenkins right now
            if list(test.find({"i": 19})):
                break
            time.sleep(0.5)
        else:
            self.fail("Replication timeout, test coll has %s records" % (
                len(list(test.find()))
            ))

        # Partially evaluate cursor so it's left alive, then kill it
        cursor = test.find().batch_size(10)
        self.assertNotEqual(
            cursor._Cursor__connection_id,
            -1,
            "Expected cursor connected to a slave, not master")

        self.assertTrue(cursor.next())
        self.assertNotEqual(0, cursor.cursor_id)

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

    def test_base_object(self):
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            c = self.client
            self.assertFalse(c.slave_okay)
            self.assertTrue(bool(c.read_preference))
            self.assertTrue(c.safe)
            self.assertEqual({}, c.get_lasterror_options())
            db = c.pymongo_test
            self.assertFalse(db.slave_okay)
            self.assertTrue(bool(c.read_preference))
            self.assertTrue(db.safe)
            self.assertEqual({}, db.get_lasterror_options())
            coll = db.test
            coll.drop()
            self.assertFalse(coll.slave_okay)
            self.assertTrue(bool(c.read_preference))
            self.assertTrue(coll.safe)
            self.assertEqual({}, coll.get_lasterror_options())
            cursor = coll.find()
            self.assertFalse(cursor._Cursor__slave_okay)
            self.assertTrue(bool(cursor._Cursor__read_preference))

            w = 1 + len(self.slaves)
            wtimeout=10000 # Wait 10 seconds for replication to complete
            c.set_lasterror_options(w=w, wtimeout=wtimeout)
            self.assertFalse(c.slave_okay)
            self.assertTrue(bool(c.read_preference))
            self.assertTrue(c.safe)
            self.assertEqual({'w': w, 'wtimeout': wtimeout}, c.get_lasterror_options())
            db = c.pymongo_test
            self.assertFalse(db.slave_okay)
            self.assertTrue(bool(c.read_preference))
            self.assertTrue(db.safe)
            self.assertEqual({'w': w, 'wtimeout': wtimeout}, db.get_lasterror_options())
            coll = db.test
            self.assertFalse(coll.slave_okay)
            self.assertTrue(bool(c.read_preference))
            self.assertTrue(coll.safe)
            self.assertEqual({'w': w, 'wtimeout': wtimeout},
                             coll.get_lasterror_options())
            cursor = coll.find()
            self.assertFalse(cursor._Cursor__slave_okay)
            self.assertTrue(bool(cursor._Cursor__read_preference))

            coll.insert({'foo': 'bar'})
            self.assertEqual(1, coll.find({'foo': 'bar'}).count())
            self.assertTrue(coll.find({'foo': 'bar'}))
            coll.remove({'foo': 'bar'})
            self.assertEqual(0, coll.find({'foo': 'bar'}).count())

            c.safe = False
            c.unset_lasterror_options()
            self.assertFalse(self.client.slave_okay)
            self.assertTrue(bool(self.client.read_preference))
            self.assertFalse(self.client.safe)
            self.assertEqual({}, self.client.get_lasterror_options())
        finally:
            ctx.exit()

    def test_document_class(self):
        c = MasterSlaveConnection(self.master, self.slaves)
        db = c.pymongo_test
        w = 1 + len(self.slaves)
        db.test.insert({"x": 1}, w=w)

        self.assertEqual(dict, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

        c.document_class = SON
        db = c.pymongo_test

        self.assertEqual(SON, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))

        c = MasterSlaveConnection(self.master, self.slaves, document_class=SON)
        db = c.pymongo_test

        self.assertEqual(SON, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))

        c.document_class = dict
        db = c.pymongo_test

        self.assertEqual(dict, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

    def test_tz_aware(self):
        dt = datetime.datetime.utcnow()
        client = MasterSlaveConnection(self.master, self.slaves)
        self.assertEqual(False, client.tz_aware)
        db = client.pymongo_test
        w = 1 + len(self.slaves)
        db.tztest.insert({'dt': dt}, w=w)
        self.assertEqual(None, db.tztest.find_one()['dt'].tzinfo)

        client = MasterSlaveConnection(self.master, self.slaves, tz_aware=True)
        self.assertEqual(True, client.tz_aware)
        db = client.pymongo_test
        db.tztest.insert({'dt': dt}, w=w)
        self.assertEqual(utc, db.tztest.find_one()['dt'].tzinfo)

        client = MasterSlaveConnection(self.master, self.slaves, tz_aware=False)
        self.assertEqual(False, client.tz_aware)
        db = client.pymongo_test
        db.tztest.insert({'dt': dt}, w=w)
        self.assertEqual(None, db.tztest.find_one()['dt'].tzinfo)


if __name__ == "__main__":
    unittest.main()
