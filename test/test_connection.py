"""Test the connection module."""

import unittest
import os

from errors import ConnectionFailure, InvalidName
from database import Database
from connection import Connection

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

        connection.test.test.save({"dummy": "object"})
        connection.test_mike.test.save({"dummy": "object"})

        dbs = connection.database_names()
        self.assertTrue("test" in dbs)
        self.assertTrue("test_mike" in dbs)

if __name__ == "__main__":
    unittest.main()
