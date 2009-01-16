"""Test the database module."""

import unittest

from errors import InvalidName
from database import Database
from connection import Connection
from collection import Collection
from test_connection import get_connection

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.connection = get_connection()

    def test_name(self):
        self.assertRaises(TypeError, Database, self.connection, 4)
        self.assertRaises(InvalidName, Database, self.connection, "my db")
        self.assertEqual("name", Database(self.connection, "name").name())

    def test_cmp(self):
        self.assertNotEqual(Database(self.connection, "test"), Database(self.connection, "mike"))
        self.assertEqual(Database(self.connection, "test"), Database(self.connection, "test"))

    def test_repr(self):
        self.assertEqual(repr(Database(self.connection, "test")),
                         "Database(%r, u'test')" % self.connection)

    def test_get_coll(self):
        db = Database(self.connection, "test")
        self.assertEqual(db.test, db["test"])
        self.assertEqual(db.test, Collection(db, "test"))
        self.assertNotEqual(db.test, Collection(db, "mike"))
        self.assertEqual(db.test.mike, db["test.mike"])

if __name__ == "__main__":
    unittest.main()
