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

"""Tests for the dbref module."""

import unittest
import sys
sys.path[0:0] = [""]

from pymongo.objectid import ObjectId
from pymongo.dbref import DBRef


class TestDBRef(unittest.TestCase):

    def setUp(self):
        pass

    def test_creation(self):
        a = ObjectId()
        self.assertRaises(TypeError, DBRef)
        self.assertRaises(TypeError, DBRef, "coll")
        self.assertRaises(TypeError, DBRef, 4, a)
        self.assertRaises(TypeError, DBRef, 1.5, a)
        self.assertRaises(TypeError, DBRef, a, a)
        self.assertRaises(TypeError, DBRef, None, a)
        self.assertRaises(TypeError, DBRef, "coll", a, 5)
        self.assert_(DBRef("coll", a))
        self.assert_(DBRef(u"coll", a))
        self.assert_(DBRef(u"coll", 5))
        self.assert_(DBRef(u"coll", 5, "database"))

    def test_read_only(self):
        a = DBRef("coll", ObjectId())

        def foo():
            a.collection = "blah"

        def bar():
            a.id = "aoeu"

        self.assertEqual("coll", a.collection)
        a.id
        self.assertEqual(None, a.database)
        self.assertRaises(AttributeError, foo)
        self.assertRaises(AttributeError, bar)

    def test_repr(self):
        self.assertEqual(repr(DBRef("coll", ObjectId("1234567890abcdef12345678"))),
                         "DBRef('coll', ObjectId('1234567890abcdef12345678'))")
        self.assertEqual(repr(DBRef(u"coll", ObjectId("1234567890abcdef12345678"))),
                         "DBRef(u'coll', ObjectId('1234567890abcdef12345678'))")
        self.assertEqual(repr(DBRef("coll", ObjectId("1234567890abcdef12345678"), "foo")),
                         "DBRef('coll', ObjectId('1234567890abcdef12345678'), 'foo')")

    def test_cmp(self):
        self.assertEqual(DBRef("coll", ObjectId("1234567890abcdef12345678")),
                         DBRef(u"coll", ObjectId("1234567890abcdef12345678")))
        self.assertNotEqual(DBRef("coll", ObjectId("1234567890abcdef12345678")),
                            DBRef(u"coll", ObjectId("1234567890abcdef12345678"), "foo"))
        self.assertNotEqual(DBRef("coll", ObjectId("1234567890abcdef12345678")),
                            DBRef("col", ObjectId("1234567890abcdef12345678")))
        self.assertNotEqual(DBRef("coll", ObjectId("1234567890abcdef12345678")),
                            DBRef("coll", ObjectId("123456789011")))
        self.assertNotEqual(DBRef("coll", ObjectId("1234567890abcdef12345678")), 4)
        self.assertEqual(DBRef("coll", ObjectId("1234567890abcdef12345678"), "foo"),
                         DBRef(u"coll", ObjectId("1234567890abcdef12345678"), "foo"))
        self.assertNotEqual(DBRef("coll", ObjectId("1234567890abcdef12345678"), "foo"),
                            DBRef(u"coll", ObjectId("1234567890abcdef12345678"), "bar"))


if __name__ == "__main__":
    unittest.main()
