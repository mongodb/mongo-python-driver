"""Test the collection module."""
import unittest

from collection import Collection
from test_connection import get_connection
from errors import InvalidName
from database import ASCENDING, DESCENDING
from son import SON

class TestCollection(unittest.TestCase):
    def setUp(self):
        self.db = get_connection().test

    def test_collection(self):
        self.assertRaises(TypeError, Collection, self.db, 5)

        def make_col(base, name):
            return base[name]

        self.assertRaises(InvalidName, make_col, self.db, "")
        self.assertRaises(InvalidName, make_col, self.db, "te$t")
        self.assertRaises(InvalidName, make_col, self.db, ".test")
        self.assertRaises(InvalidName, make_col, self.db, "test.")
        self.assertRaises(InvalidName, make_col, self.db, "tes..t")
        self.assertRaises(InvalidName, make_col, self.db.test, "")
        self.assertRaises(InvalidName, make_col, self.db.test, "te$t")
        self.assertRaises(InvalidName, make_col, self.db.test, ".test")
        self.assertRaises(InvalidName, make_col, self.db.test, "test.")
        self.assertRaises(InvalidName, make_col, self.db.test, "tes..t")

        self.assertTrue(isinstance(self.db.test, Collection))
        self.assertEqual(self.db.test, self.db["test"])
        self.assertEqual(self.db.test, Collection(self.db, "test"))
        self.assertEqual(self.db.test.mike, self.db["test.mike"])
        self.assertEqual(self.db.test["mike"], self.db["test.mike"])

    def test_create_index(self):
        db = self.db

        self.assertRaises(TypeError, db.test.create_index, 5)
        self.assertRaises(TypeError, db.test.create_index, "hello")
        self.assertRaises(ValueError, db.test.create_index, [])
        self.assertRaises(TypeError, db.test.create_index, [], ASCENDING)
        self.assertRaises(TypeError, db.test.create_index, [("hello", DESCENDING)], DESCENDING)
        self.assertRaises(TypeError, db.test.create_index, "hello", "world")

        db.test.drop_indexes()
        self.assertFalse(db.system.indexes.find_one({"ns": u"test.test"}))

        db.test.create_index("hello", ASCENDING)
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])

        count = 0
        for _ in db.system.indexes.find({"ns": u"test.test"}):
            count += 1
        self.assertEqual(count, 2)

        db.test.drop_indexes()
        self.assertFalse(db.system.indexes.find_one({"ns": u"test.test"}))
        db.test.create_index("hello", ASCENDING)
        self.assertEqual(db.system.indexes.find_one({"ns": u"test.test"}),
                         SON([(u"name", u"hello_1"),
                              (u"ns", u"test.test"),
                              (u"key", SON([(u"hello", 1)]))]))

        db.test.drop_indexes()
        self.assertFalse(db.system.indexes.find_one({"ns": u"test.test"}))
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])
        self.assertEqual(db.system.indexes.find_one({"ns": u"test.test"}),
                         SON([(u"name", u"hello_-1_world_1"),
                              (u"ns", u"test.test"),
                              (u"key", SON([(u"hello", -1),
                                            (u"world", 1)]))]))

    def test_drop_index(self):
        db = self.db
        db.test.drop_indexes()
        db.test.create_index("hello", ASCENDING)
        name = db.test.create_index("goodbye", DESCENDING)

        self.assertEqual(db.system.indexes.find({"ns": u"test.test"}).count(), 2)
        self.assertEqual(name, "goodbye_-1")
        db.test.drop_index(name)
        self.assertEqual(db.system.indexes.find({"ns": u"test.test"}).count(), 1)
        self.assertEqual(db.system.indexes.find_one({"ns": u"test.test"}),
                         SON([(u"name", u"hello_1"),
                              (u"ns", u"test.test"),
                              (u"key", SON([(u"hello", 1)]))]))

if __name__ == "__main__":
    unittest.main()
