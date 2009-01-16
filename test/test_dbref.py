"""Tests for the dbref module."""

import unittest
import types

from objectid import ObjectId
from dbref import DBRef

class TestDBRef(unittest.TestCase):
    def setUp(self):
        pass

    def test_creation(self):
        a = ObjectId()
        self.assertRaises(TypeError, DBRef)
        self.assertRaises(TypeError, DBRef, "coll")
        self.assertRaises(TypeError, DBRef, "coll", 4)
        self.assertRaises(TypeError, DBRef, "coll", 175.0)
        self.assertRaises(TypeError, DBRef, 4, a)
        self.assertRaises(TypeError, DBRef, 1.5, a)
        self.assertRaises(TypeError, DBRef, a, a)
        self.assertRaises(TypeError, DBRef, None, a)
        self.assertTrue(DBRef("coll", a))
        self.assertTrue(DBRef(u"coll", a))

    def test_repr(self):
        self.assertEqual(repr(DBRef("coll", ObjectId("123456789012"))), "DBRef(u'coll', ObjectId('123456789012'))")
        self.assertEqual(repr(DBRef(u"coll", ObjectId("123456789012"))), "DBRef(u'coll', ObjectId('123456789012'))")

    def test_cmp(self):
        self.assertEqual(DBRef("coll", ObjectId("123456789012")), DBRef(u"coll", ObjectId("123456789012")))
        self.assertNotEqual(DBRef("coll", ObjectId("123456789012")), DBRef("col", ObjectId("123456789012")))
        self.assertNotEqual(DBRef("coll", ObjectId("123456789012")), DBRef("coll", ObjectId("123456789011")))
        self.assertNotEqual(DBRef("coll", ObjectId("123456789012")), 4)

if __name__ == "__main__":
    unittest.main()
