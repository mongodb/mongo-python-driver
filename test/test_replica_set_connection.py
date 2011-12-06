# Copyright 2009-2011 10gen, Inc.
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

import datetime
import os
import sys
import socket
import time
import unittest
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.son import SON
from bson.tz_util import utc
from pymongo import ReadPreference
from pymongo.connection import Connection
from pymongo.replica_set_connection import ReplicaSetConnection
from pymongo.replica_set_connection import _partition_node
from pymongo.database import Database
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            InvalidName,
                            OperationFailure)
from test import version

host = os.environ.get("DB_IP", socket.gethostname())
port = os.environ.get("DB_PORT", 27017)
pair = '%s:%d' % (host, port)

class TestConnectionReplicaSetBase(unittest.TestCase):
    def setUp(self):
        conn = Connection(pair)
        response = conn.admin.command('ismaster')
        if 'setName' in response:
            self.name = response['setName']
            self.w = len(response['hosts'])
            self.hosts = set([_partition_node(h)
                              for h in response["hosts"]])
            self.arbiters = set([_partition_node(h)
                                 for h in response.get("arbiters", [])])
            self.primary = _partition_node(pair)
        else:
            raise SkipTest()

    def _get_connection(self, **kwargs):
        return ReplicaSetConnection(pair,
            replicaSet=self.name,
            **kwargs)

class TestConnection(TestConnectionReplicaSetBase):
    def test_connect(self):
        self.assertRaises(ConnectionFailure, ReplicaSetConnection,
                          "somedomainthatdoesntexist.org:27017",
                          replicaSet=self.name,
                          connectTimeoutMS=600)
        self.assertRaises(ConfigurationError, ReplicaSetConnection,
                          pair, replicaSet='fdlksjfdslkjfd')
        self.assert_(ReplicaSetConnection(pair, replicaSet=self.name))

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
        self.assertEqual(c.read_preference, ReadPreference.PRIMARY)
        self.assertEqual(c.max_pool_size, 10)
        self.assertEqual(c.document_class, dict)
        self.assertEqual(c.tz_aware, False)
        self.assertEqual(c.slave_okay, False)
        self.assertEqual(c.safe, False)
        c.close()

        c = ReplicaSetConnection(pair, replicaSet=self.name, max_pool_size=25,
                                 document_class=SON, tz_aware=True,
                                 slaveOk=False, safe=True,
                                 read_preference=ReadPreference.SECONDARY)
        c.admin.command('ping')
        self.assertEqual(c.primary, self.primary)
        self.assertEqual(c.hosts, self.hosts)
        self.assertEqual(c.arbiters, self.arbiters)
        self.assertEqual(c.read_preference, ReadPreference.SECONDARY)
        self.assertEqual(c.max_pool_size, 25)
        self.assertEqual(c.document_class, SON)
        self.assertEqual(c.tz_aware, True)
        self.assertEqual(c.slave_okay, False)
        self.assertEqual(c.safe, True)

        if version.at_least(c, (1, 7, 4)):
            self.assertEqual(c.max_bson_size, 16777216)
        else:
            self.assertEqual(c.max_bson_size, 4194304)
        c.close()

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

        self.assert_(isinstance(connection.test, Database))
        self.assertEqual(connection.test, connection["test"])
        self.assertEqual(connection.test, Database(connection, "test"))
        connection.close()

    def test_operations(self):
        c = self._get_connection()

        db = c.pymongo_test
        db.test.remove({})
        self.assertEqual(0, db.test.count())
        db.test.insert({'foo': 'x'}, safe=True)
        self.assertEqual(1, db.test.count())

        cursor = db.test.find()
        self.assertEqual('x', cursor.next()['foo'])
        # Ensure we read from the primary
        self.assertEqual(c.primary, cursor._Cursor__connection_id)
        time.sleep(1)
        cursor = db.test.find(read_preference=ReadPreference.SECONDARY)
        self.assertEqual('x', cursor.next()['foo'])
        # Ensure we didn't read from the primary
        self.assertTrue(cursor._Cursor__connection_id in c.secondaries)

        self.assertEqual(1, db.test.count())
        db.test.remove({})
        self.assertEqual(0, db.test.count())
        c.drop_database(db)
        c.close()

    def test_database_names(self):
        connection = self._get_connection()

        connection.pymongo_test.test.save({"dummy": u"object"})
        connection.pymongo_test_mike.test.save({"dummy": u"object"})

        dbs = connection.database_names()
        self.assert_("pymongo_test" in dbs)
        self.assert_("pymongo_test_mike" in dbs)
        connection.close()

    def test_drop_database(self):
        connection = self._get_connection()

        self.assertRaises(TypeError, connection.drop_database, 5)
        self.assertRaises(TypeError, connection.drop_database, None)

        connection.pymongo_test.test.save({"dummy": u"object"})
        dbs = connection.database_names()
        self.assert_("pymongo_test" in dbs)
        connection.drop_database("pymongo_test")
        dbs = connection.database_names()
        self.assert_("pymongo_test" not in dbs)

        connection.pymongo_test.test.save({"dummy": u"object"})
        dbs = connection.database_names()
        self.assert_("pymongo_test" in dbs)
        connection.drop_database(connection.pymongo_test)
        dbs = connection.database_names()
        self.assert_("pymongo_test" not in dbs)
        connection.close()

    def test_copy_db(self):
        c = self._get_connection()

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

        self.assert_("pymongo_test1" in c.database_names())
        self.assertEqual("bar", c.pymongo_test1.test.find_one()["foo"])

        c.copy_database("pymongo_test", "pymongo_test2", pair)
        time.sleep(1)

        self.assert_("pymongo_test2" in c.database_names())
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
            self.assert_("pymongo_test1" in c.database_names())
            time.sleep(2)
            self.assertEqual("bar", c.pymongo_test1.test.find_one()["foo"])
        c.close()

    def test_iteration(self):
        connection = self._get_connection()

        def iterate():
            [a for a in connection]

        self.assertRaises(TypeError, iterate)
        connection.close()

    # TODO this test is probably very dependent on the machine its running on
    # due to timing issues, but I want to get something in here.
    def test_low_network_timeout(self):
        c = None
        i = 0
        n = 10
        while c is None and i < n:
            try:
                c = self._get_connection(socketTimeoutMS=0.1)
            except AutoReconnect:
                i += 1
        if i == n:
            raise SkipTest()

        coll = c.pymongo_test.test

        for _ in xrange(1000):
            try:
                coll.find_one()
            except AutoReconnect:
                pass
            except AssertionError:
                self.fail()
        c.close()

    def test_close(self):
        c = self._get_connection()
        coll = c.foo.bar

        c.close()
        c.close()

        coll.count()

        c.close()
        c.close()

        coll.count()

    def test_fork(self):
        """Test using a connection before and after a fork.
        """
        if sys.platform == "win32":
            raise SkipTest()

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest()

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
        self.assert_(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

        c.document_class = SON

        self.assertEqual(SON, c.document_class)
        self.assert_(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))
        c.close()

        c = self._get_connection(document_class=SON)
        db = c.pymongo_test

        self.assertEqual(SON, c.document_class)
        self.assert_(isinstance(db.test.find_one(), SON))
        self.assertFalse(isinstance(db.test.find_one(as_class=dict), SON))

        c.document_class = dict

        self.assertEqual(dict, c.document_class)
        self.assert_(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))
        c.close()

    def test_network_timeout(self):
        no_timeout = self._get_connection()
        timeout = self._get_connection(socketTimeoutMS=100)

        no_timeout.pymongo_test.drop_collection("test")
        no_timeout.pymongo_test.test.insert({"x": 1}, safe=True)
        time.sleep(1)

        where_func = """function (doc) {
  var d = new Date().getTime() + 200;
  var x = new Date().getTime();
  while (x < d) {
    x = new Date().getTime();
  }
  return true;
}"""

        def get_x(db):
            return db.test.find().where(where_func).next()["x"]
        self.assertEqual(1, get_x(no_timeout.pymongo_test))
        self.assertRaises(ConnectionFailure, get_x, timeout.pymongo_test)

        def get_x_timeout(db, t):
            return db.test.find(
                        network_timeout=t).where(where_func).next()["x"]
        self.assertEqual(1, get_x_timeout(timeout.pymongo_test, None))
        self.assertRaises(ConnectionFailure, get_x_timeout,
                          no_timeout.pymongo_test, 0.1)
        no_timeout.close()
        timeout.close()

    def test_tz_aware(self):
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
            connection = ReplicaSetConnection("[::1]:27017", replicaSet=self.name)
        except:
            # Either mongod was started without --ipv6
            # or the OS doesn't support it (or both).
            raise SkipTest()

        # Try a few simple things
        connection = ReplicaSetConnection("mongodb://[::1]:27017",
                                          replicaSet=self.name)
        uri = "mongodb://[::1]:27017/?slaveOk=true;replicaSet=" + self.name
        connection = ReplicaSetConnection(uri)
        connection = ReplicaSetConnection("[::1]:27017,localhost:27017",
                                          replicaSet=self.name)
        connection = ReplicaSetConnection("localhost:27017,[::1]:27017",
                                          replicaSet=self.name)
        connection.pymongo_test.test.save({"dummy": u"object"})
        connection.pymongo_test_bernie.test.save({"dummy": u"object"})

        dbs = connection.database_names()
        self.assert_("pymongo_test" in dbs)
        self.assert_("pymongo_test_bernie" in dbs)
        connection.close()

    def test_kill_cursors_explicit(self):
        c = self._get_connection()
        db = c.pymongo_test
        db.drop_collection("test")

        def get_cursor_counts():
            counts = {}
            conn = Connection(*c.primary)
            db = conn.pymongo_test
            counts[c.primary] = db.command("cursorInfo")["clientCursors_size"]
            for member in c.secondaries:
                conn = Connection(*member)
                db = conn.pymongo_test
                counts[member] = db.command("cursorInfo")["clientCursors_size"]
            return counts

        start = get_cursor_counts()

        test = db.test
        for i in range(10000):
            test.insert({"i": i}) 
        self.assertEqual(start, get_cursor_counts())

        # Automatically closed by the server (limit == -1).
        for _ in range(10):
            db.test.find_one()
        self.assertEqual(start, get_cursor_counts())

        a = db.test.find()
        for x in a:
            break
        self.assertNotEqual(start, get_cursor_counts())

        # Explicitly close (should work with all interpreter implementations).
        a.close()
        self.assertEqual(start, get_cursor_counts())

        # Automatically closed by the server since the entire
        # result was returned.
        a = db.test.find().limit(10)
        for x in a:
            break
        self.assertEqual(start, get_cursor_counts())

if __name__ == "__main__":
    unittest.main()
