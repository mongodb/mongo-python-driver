# Copyright 2009-2010 10gen, Inc.
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

import unittest
import os
import time
import sys
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo.errors import ConnectionFailure, InvalidName
from pymongo.errors import CollectionInvalid, OperationFailure
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
                                          int(os.environ.get("DB_PORT2",
                                                             27018)),
                                          slave_okay=True))
        except ConnectionFailure:
            pass

        try:
            self.slaves.append(Connection(os.environ.get("DB_IP3", host),
                                          int(os.environ.get("DB_PORT3",
                                                             27019)),
                                          slave_okay=True))
        except ConnectionFailure:
            pass

        if not self.slaves:
            raise SkipTest()

        self.connection = MasterSlaveConnection(self.master, self.slaves)
        self.db = self.connection.pymongo_test

    def test_types(self):
        self.assertRaises(TypeError, MasterSlaveConnection, 1)
        self.assertRaises(TypeError, MasterSlaveConnection, self.master, 1)
        self.assertRaises(TypeError, MasterSlaveConnection, self.master, [1])

    def test_repr(self):
        self.assertEqual(repr(self.connection),
                         "MasterSlaveConnection(%r, %r)" %
                         (self.master, self.slaves))

    def test_get_db(self):

        def make_db(base, name):
            return base[name]

        self.assertRaises(InvalidName, make_db, self.connection, "")
        self.assertRaises(InvalidName, make_db, self.connection, "te$t")
        self.assertRaises(InvalidName, make_db, self.connection, "te.t")
        self.assertRaises(InvalidName, make_db, self.connection, "te\\t")
        self.assertRaises(InvalidName, make_db, self.connection, "te/t")
        self.assertRaises(InvalidName, make_db, self.connection, "te st")

        self.assert_(isinstance(self.connection.test, Database))
        self.assertEqual(self.connection.test, self.connection["test"])
        self.assertEqual(self.connection.test, Database(self.connection,
                                                        "test"))

    def test_database_names(self):
        self.connection.pymongo_test.test.save({"dummy": u"object"})
        self.connection.pymongo_test_mike.test.save({"dummy": u"object"})

        dbs = self.connection.database_names()
        self.assert_("pymongo_test" in dbs)
        self.assert_("pymongo_test_mike" in dbs)

    def test_drop_database(self):
        self.assertRaises(TypeError, self.connection.drop_database, 5)
        self.assertRaises(TypeError, self.connection.drop_database, None)

        self.connection.pymongo_test.test.save({"dummy": u"object"})
        dbs = self.connection.database_names()
        self.assert_("pymongo_test" in dbs)
        self.connection.drop_database("pymongo_test")
        dbs = self.connection.database_names()
        self.assert_("pymongo_test" not in dbs)

        self.connection.pymongo_test.test.save({"dummy": u"object"})
        dbs = self.connection.database_names()
        self.assert_("pymongo_test" in dbs)
        self.connection.drop_database(self.connection.pymongo_test)
        dbs = self.connection.database_names()
        self.assert_("pymongo_test" not in dbs)

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
        self.failIf(count)

    # This was failing because commands were being sent to the slaves
    def test_create_collection(self):
        self.connection.drop_database('pymongo_test')

        collection = self.db.create_collection('test')
        self.assert_(isinstance(collection, Collection))

        self.assertRaises(CollectionInvalid, self.db.create_collection, 'test')

    # Believe this was failing for the same reason...
    def test_unique_index(self):
        self.connection.drop_database('pymongo_test')
        self.db.test.create_index('username', unique=True)

        self.db.test.save({'username': 'mike'}, safe=True)
        self.assertRaises(OperationFailure, self.db.test.save, {'username': 'mike'}, safe=True)

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
        self.assert_(count)

    # NOTE this test is non-deterministic, but hopefully we pause long enough
    # for the slaves to pull...
    def test_insert_find_one_with_pause(self):
        count = 0

        self.db.test.remove({})
        self.db.test.insert({"x": 5586})
        time.sleep(7)
        for _ in range(10):
            try:
                if 5586 != self.db.test.find_one()["x"]:
                    count += 1
            except:
                count += 1
        self.failIf(count)

    def test_kill_cursors(self):

        def cursor_count():
            count = 0
            res = self.connection.master.test_pymongo.command("cursorInfo")
            count += res["clientCursors_size"]
            for slave in self.connection.slaves:
                res = slave.test_pymongo.command("cursorInfo")
                count += res["clientCursors_size"]
            return count

        self.connection.test_pymongo.drop_collection("test")
        db = self.db

        before = cursor_count()

        for i in range(10000):
            db.test.insert({"i": i})
        time.sleep(6) # need to sleep to be sure this gets pulled...

        self.assertEqual(before, cursor_count())

        for _ in range(10):
            db.test.find_one()

        self.assertEqual(before, cursor_count())

        for _ in range(10):
            for x in db.test.find():
                break

        self.assertEqual(before, cursor_count())

        a = db.test.find()
        for x in a:
            break

        self.assertNotEqual(before, cursor_count())

        del a

        self.assertEqual(before, cursor_count())

        a = db.test.find().limit(10)
        for x in a:
            break

        self.assertEqual(before, cursor_count())

if __name__ == "__main__":
    unittest.main()
