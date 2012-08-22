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

"""Test for master slave connections."""

import datetime
import os
import sys
import time
import unittest
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.son import SON
from bson.tz_util import utc
from pymongo import ReadPreference
from pymongo.errors import ConnectionFailure, InvalidName
from pymongo.errors import CollectionInvalid, OperationFailure
from pymongo.errors import AutoReconnect
from pymongo.database import Database
from pymongo.connection import Connection
from pymongo.collection import Collection
from pymongo.master_slave_connection import MasterSlaveConnection


class TestMasterSlaveConnection(unittest.TestCase):

    def setUp(self):
        host = os.environ.get("DB_IP", "localhost")
        self.master = Connection(host, int(os.environ.get("DB_PORT", 27017)))

        self.slaves = []
        try:
            self.slaves.append(Connection(os.environ.get("DB_IP2", host),
                               int(os.environ.get("DB_PORT2", 27018)),
                               read_preference=ReadPreference.SECONDARY))
        except ConnectionFailure:
            pass

        try:
            self.slaves.append(Connection(os.environ.get("DB_IP3", host),
                               int(os.environ.get("DB_PORT3", 27019)),
                               read_preference=ReadPreference.SECONDARY))
        except ConnectionFailure:
            pass

        if not self.slaves:
            raise SkipTest("Not connected to master-slave set")

        self.connection = MasterSlaveConnection(self.master, self.slaves)
        self.db = self.connection.pymongo_test

    def tearDown(self):
        try:
            self.db.test.drop_indexes()
        except Exception:
            # Tests like test_disconnect can monkey with the connection in ways
            # that make this fail
            pass

        super(TestMasterSlaveConnection, self).tearDown()

    def test_types(self):
        self.assertRaises(TypeError, MasterSlaveConnection, 1)
        self.assertRaises(TypeError, MasterSlaveConnection, self.master, 1)
        self.assertRaises(TypeError, MasterSlaveConnection, self.master, [1])

    def test_repr(self):
        self.assertEqual(repr(self.connection),
                         "MasterSlaveConnection(%r, %r)" %
                         (self.master, self.slaves))

    def test_disconnect(self):
        class Connection(object):
            def __init__(self):
                self._disconnects = 0

            def disconnect(self):
                self._disconnects += 1

        self.connection._MasterSlaveConnection__master = Connection()
        self.connection._MasterSlaveConnection__slaves = [Connection(),
                                                          Connection()]

        self.connection.disconnect()
        self.assertEqual(1,
            self.connection._MasterSlaveConnection__master._disconnects)
        self.assertEqual(1,
            self.connection._MasterSlaveConnection__slaves[0]._disconnects)
        self.assertEqual(1,
            self.connection._MasterSlaveConnection__slaves[1]._disconnects)

    def test_continue_until_slave_works(self):
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
            last_idx = -1

            def __init__(self):
                self._items = [Slave(True), Slave(True),
                               Slave(False), Slave(True)]

            def __len__(self):
                return len(self._items)

            def __getitem__(self, idx):
                NotRandomList.last_idx = idx
                return self._items.pop(0)

        self.connection._MasterSlaveConnection__slaves = NotRandomList()

        response = self.connection._send_message_with_response('message')
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

        self.connection._MasterSlaveConnection__slaves = NotRandomList()

        self.assertRaises(AutoReconnect,
            self.connection._send_message_with_response, 'message')
        self.assertEqual(4, Slave.calls)

    def test_get_db(self):

        def make_db(base, name):
            return base[name]

        self.assertRaises(InvalidName, make_db, self.connection, "")
        self.assertRaises(InvalidName, make_db, self.connection, "te$t")
        self.assertRaises(InvalidName, make_db, self.connection, "te.t")
        self.assertRaises(InvalidName, make_db, self.connection, "te\\t")
        self.assertRaises(InvalidName, make_db, self.connection, "te/t")
        self.assertRaises(InvalidName, make_db, self.connection, "te st")

        self.assertTrue(isinstance(self.connection.test, Database))
        self.assertEqual(self.connection.test, self.connection["test"])
        self.assertEqual(self.connection.test, Database(self.connection,
                                                        "test"))

    def test_database_names(self):
        self.connection.pymongo_test.test.save({"dummy": u"object"})
        self.connection.pymongo_test_mike.test.save({"dummy": u"object"})

        dbs = self.connection.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_mike" in dbs)

    def test_drop_database(self):
        self.assertRaises(TypeError, self.connection.drop_database, 5)
        self.assertRaises(TypeError, self.connection.drop_database, None)

        raise SkipTest("This test often fails due to SERVER-2329")
        
        self.connection.pymongo_test.test.save({"dummy": u"object"}, safe=True)
        dbs = self.connection.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.connection.drop_database("pymongo_test")
        dbs = self.connection.database_names()
        self.assertTrue("pymongo_test" not in dbs)

        self.connection.pymongo_test.test.save({"dummy": u"object"})
        dbs = self.connection.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.connection.drop_database(self.connection.pymongo_test)
        dbs = self.connection.database_names()
        self.assertTrue("pymongo_test" not in dbs)

    def test_iteration(self):

        def iterate():
            [a for a in self.connection]

        self.assertRaises(TypeError, iterate)

    def test_insert_find_one_in_request(self):
        count = 0
        for i in range(100):
            self.connection.start_request()
            self.db.test.remove({})
            self.db.test.insert({"x": i})
            try:
                if i != self.db.test.find_one()["x"]:
                    count += 1
            except:
                count += 1
            self.connection.end_request()
        self.assertFalse(count)

    # This was failing because commands were being sent to the slaves
    def test_create_collection(self):
        self.connection.pymongo_test.test.drop()

        collection = self.db.create_collection('test')
        self.assertTrue(isinstance(collection, Collection))

        self.assertRaises(CollectionInvalid, self.db.create_collection, 'test')

    # Believe this was failing for the same reason...
    def test_unique_index(self):
        self.connection.pymongo_test.test.drop()
        self.db.test.create_index('username', unique=True)

        self.db.test.save({'username': 'mike'}, safe=True)
        self.assertRaises(OperationFailure,
                          self.db.test.save, {'username': 'mike'}, safe=True)

    # NOTE this test is non-deterministic, but I expect
    # some failures unless the db is pulling instantaneously...
    def test_insert_find_one_with_slaves(self):
        count = 0
        for i in range(100):
            self.db.test.remove({})
            self.db.test.insert({"x": i})
            try:
                if i != self.db.test.find_one()["x"]:
                    count += 1
            except:
                count += 1
        self.assertTrue(count)

    # NOTE this test is non-deterministic, but hopefully we pause long enough
    # for the slaves to pull...
    def test_insert_find_one_with_pause(self):
        count = 0

        self.db.test.remove({})
        self.db.test.insert({"x": 5586})
        time.sleep(11)
        for _ in range(10):
            try:
                if 5586 != self.db.test.find_one()["x"]:
                    count += 1
            except:
                count += 1
        self.assertFalse(count)

    def test_kill_cursor_explicit(self):
        c = self.connection
        c.slave_okay = True
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
        c = self.connection
        self.assertFalse(c.slave_okay)
        self.assertTrue(bool(c.read_preference))
        self.assertFalse(c.safe)
        self.assertEqual({}, c.get_lasterror_options())
        db = c.test
        self.assertFalse(db.slave_okay)
        self.assertTrue(bool(c.read_preference))
        self.assertFalse(db.safe)
        self.assertEqual({}, db.get_lasterror_options())
        coll = db.test
        coll.drop()
        self.assertFalse(coll.slave_okay)
        self.assertTrue(bool(c.read_preference))
        self.assertFalse(coll.safe)
        self.assertEqual({}, coll.get_lasterror_options())
        cursor = coll.find()
        self.assertFalse(cursor._Cursor__slave_okay)
        self.assertTrue(bool(cursor._Cursor__read_preference))

        c.safe = True
        w = 1 + len(self.slaves)
        wtimeout=10000 # Wait 10 seconds for replication to complete
        c.set_lasterror_options(w=w, wtimeout=wtimeout)
        self.assertFalse(c.slave_okay)
        self.assertTrue(bool(c.read_preference))
        self.assertTrue(c.safe)
        self.assertEqual({'w': w, 'wtimeout': wtimeout}, c.get_lasterror_options())
        db = c.test
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

        # Set self.connection back to defaults
        c.safe = False
        c.unset_lasterror_options()
        self.assertFalse(self.connection.slave_okay)
        self.assertTrue(bool(self.connection.read_preference))
        self.assertFalse(self.connection.safe)
        self.assertEqual({}, self.connection.get_lasterror_options())

    def test_document_class(self):
        c = MasterSlaveConnection(self.master, self.slaves)
        db = c.pymongo_test
        w = 1 + len(self.slaves)
        db.test.insert({"x": 1}, safe=True, w=w)

        self.assertEqual(dict, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

        c.document_class = SON

        self.assertEqual(SON, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))

        c = MasterSlaveConnection(self.master, self.slaves, document_class=SON)
        db = c.pymongo_test

        self.assertEqual(SON, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))

        c.document_class = dict

        self.assertEqual(dict, c.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

    def test_tz_aware(self):
        dt = datetime.datetime.utcnow()
        conn = MasterSlaveConnection(self.master, self.slaves)
        self.assertEqual(False, conn.tz_aware)
        db = conn.pymongo_test
        w = 1 + len(self.slaves)
        db.tztest.insert({'dt': dt}, safe=True, w=w)
        self.assertEqual(None, db.tztest.find_one()['dt'].tzinfo)

        conn = MasterSlaveConnection(self.master, self.slaves, tz_aware=True)
        self.assertEqual(True, conn.tz_aware)
        db = conn.pymongo_test
        db.tztest.insert({'dt': dt}, safe=True, w=w)
        self.assertEqual(utc, db.tztest.find_one()['dt'].tzinfo)

        conn = MasterSlaveConnection(self.master, self.slaves, tz_aware=False)
        self.assertEqual(False, conn.tz_aware)
        db = conn.pymongo_test
        db.tztest.insert({'dt': dt}, safe=True, w=w)
        self.assertEqual(None, db.tztest.find_one()['dt'].tzinfo)


if __name__ == "__main__":
    unittest.main()
