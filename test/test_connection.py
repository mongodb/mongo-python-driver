# Copyright 2009 10gen, Inc.
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

import unittest
import os
import sys
sys.path[0:0] = [""]

from pymongo.errors import ConnectionFailure, InvalidName
from pymongo.database import Database
from pymongo.connection import Connection

def get_connection():
    host = os.environ.get("db_ip", "localhost")
    port = int(os.environ.get("db_port", 27017))
    return Connection(host, port)

class TestConnection(unittest.TestCase):
    def setUp(self):
        self.host = os.environ.get("db_ip", "localhost")
        self.port = int(os.environ.get("db_port", 27017))

    def test_types(self):
        self.assertRaises(TypeError, Connection, 1)
        self.assertRaises(TypeError, Connection, 1.14)
        self.assertRaises(TypeError, Connection, None)
        self.assertRaises(TypeError, Connection, [])
        self.assertRaises(TypeError, Connection, "localhost", "27017")
        self.assertRaises(TypeError, Connection, "localhost", 1.14)
        self.assertRaises(TypeError, Connection, "localhost", None)
        self.assertRaises(TypeError, Connection, "localhost", [])

    def test_connect(self):
        self.assertRaises(ConnectionFailure, Connection, "somedomainthatdoesntexist.org")
        self.assertRaises(ConnectionFailure, Connection, self.host, 123456789)

        self.assertTrue(Connection(self.host, self.port))

    def test_repr(self):
        self.assertEqual(repr(Connection(self.host, self.port)),
                         "Connection('%s', %s)" % (self.host, self.port))
    def test_getters(self):
        self.assertEqual(Connection(self.host, self.port).host(), self.host)
        self.assertEqual(Connection(self.host, self.port).port(), self.port)

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

        connection.test.test.save({"dummy": u"object"})
        connection.test_mike.test.save({"dummy": u"object"})

        dbs = connection.database_names()
        self.assertTrue("test" in dbs)
        self.assertTrue("test_mike" in dbs)

    def test_drop_database(self):
        connection = Connection(self.host, self.port)

        self.assertRaises(TypeError, connection.drop_database, 5)
        self.assertRaises(TypeError, connection.drop_database, None)

        connection.test.test.save({"dummy": u"object"})
        dbs = connection.database_names()
        self.assertTrue("test" in dbs)
        connection.drop_database("test")
        dbs = connection.database_names()
        self.assertTrue("test" not in dbs)

        connection.test.test.save({"dummy": u"object"})
        dbs = connection.database_names()
        self.assertTrue("test" in dbs)
        connection.drop_database(connection.test)
        dbs = connection.database_names()
        self.assertTrue("test" not in dbs)

    def test_iteration(self):
        connection = Connection(self.host, self.port)

        def iterate():
            [a for a in connection]

        self.assertRaises(TypeError, iterate)

    def test_master(self):
        connection = Connection(self.host, self.port)
        # NOTE: this test assumes that we're connecting to master...
        self.assertEqual(connection._master(), True)

if __name__ == "__main__":
    unittest.main()
