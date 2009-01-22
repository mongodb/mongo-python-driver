"""Tests for the objectid module."""

import unittest

from pymongo.objectid import ObjectId
from pymongo.errors import InvalidId

class TestObjectId(unittest.TestCase):
    def setUp(self):
        pass

    def test_creation(self):
        self.assertRaises(TypeError, ObjectId, 4)
        self.assertRaises(TypeError, ObjectId, u"hello")
        self.assertRaises(TypeError, ObjectId, 175.0)
        self.assertRaises(TypeError, ObjectId, {"test": 4})
        self.assertRaises(TypeError, ObjectId, ["something"])
        self.assertRaises(InvalidId, ObjectId, "")
        self.assertRaises(InvalidId, ObjectId, "12345678901")
        self.assertRaises(InvalidId, ObjectId, "1234567890123")
        self.assertTrue(ObjectId())
        self.assertTrue(ObjectId("123456789012"))
        a = ObjectId()
        self.assertTrue(ObjectId(a))

    def test_repr_str(self):
        self.assertEqual(repr(ObjectId("123456789012")), "ObjectId('123456789012')")
        self.assertEqual(str(ObjectId("123456789012")), "123456789012")

    def test_cmp(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(a))
        self.assertEqual(ObjectId("123456789012"), ObjectId("123456789012"))
        self.assertNotEqual(ObjectId(), ObjectId())
        self.assertNotEqual(ObjectId("123456789012"), "123456789012")

    def test_new(self):
        a = ObjectId()
        b = ObjectId("123456789012")
        self.assertTrue(a.is_new())
        self.assertFalse(b.is_new())
        a._use()
        b._use()
        self.assertFalse(a.is_new())
        self.assertFalse(b.is_new())

if __name__ == "__main__":
    unittest.main()
