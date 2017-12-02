# Copyright 2009-present MongoDB, Inc.
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

import pickle
import sys
sys.path[0:0] = [""]

from bson.dbref import DBRef
from bson.objectid import ObjectId
from test import unittest

from copy import deepcopy


class TestDBRef(unittest.TestCase):
    def test_creation(self):
        a = ObjectId()
        self.assertRaises(TypeError, DBRef)
        self.assertRaises(TypeError, DBRef, "coll")
        self.assertRaises(TypeError, DBRef, 4, a)
        self.assertRaises(TypeError, DBRef, 1.5, a)
        self.assertRaises(TypeError, DBRef, a, a)
        self.assertRaises(TypeError, DBRef, None, a)
        self.assertRaises(TypeError, DBRef, "coll", a, 5)
        self.assertTrue(DBRef("coll", a))
        self.assertTrue(DBRef(u"coll", a))
        self.assertTrue(DBRef(u"coll", 5))
        self.assertTrue(DBRef(u"coll", 5, "database"))

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
        self.assertEqual(repr(DBRef("coll",
                                    ObjectId("1234567890abcdef12345678"))),
                         "DBRef('coll', ObjectId('1234567890abcdef12345678'))")
        self.assertEqual(repr(DBRef(u"coll",
                              ObjectId("1234567890abcdef12345678"))),
                         "DBRef(%s, ObjectId('1234567890abcdef12345678'))"
                         % (repr(u'coll'),)
                        )
        self.assertEqual(repr(DBRef("coll", 5, foo="bar")),
                         "DBRef('coll', 5, foo='bar')")
        self.assertEqual(repr(DBRef("coll",
                              ObjectId("1234567890abcdef12345678"), "foo")),
                         "DBRef('coll', ObjectId('1234567890abcdef12345678'), "
                         "'foo')")

    def test_equality(self):
        obj_id = ObjectId("1234567890abcdef12345678")

        self.assertEqual(DBRef('foo', 5), DBRef('foo', 5))
        self.assertEqual(DBRef("coll", obj_id), DBRef(u"coll", obj_id))
        self.assertNotEqual(DBRef("coll", obj_id),
                            DBRef(u"coll", obj_id, "foo"))
        self.assertNotEqual(DBRef("coll", obj_id), DBRef("col", obj_id))
        self.assertNotEqual(DBRef("coll", obj_id),
                            DBRef("coll", ObjectId(b"123456789011")))
        self.assertNotEqual(DBRef("coll", obj_id), 4)
        self.assertEqual(DBRef("coll", obj_id, "foo"),
                         DBRef(u"coll", obj_id, "foo"))
        self.assertNotEqual(DBRef("coll", obj_id, "foo"),
                            DBRef(u"coll", obj_id, "bar"))

        # Explicitly test inequality
        self.assertFalse(DBRef('foo', 5) != DBRef('foo', 5))
        self.assertFalse(DBRef("coll", obj_id) != DBRef(u"coll", obj_id))
        self.assertFalse(DBRef("coll", obj_id, "foo") !=
                         DBRef(u"coll", obj_id, "foo"))

    def test_kwargs(self):
        self.assertEqual(DBRef("coll", 5, foo="bar"),
                         DBRef("coll", 5, foo="bar"))
        self.assertNotEqual(DBRef("coll", 5, foo="bar"), DBRef("coll", 5))
        self.assertNotEqual(DBRef("coll", 5, foo="bar"),
                            DBRef("coll", 5, foo="baz"))
        self.assertEqual("bar", DBRef("coll", 5, foo="bar").foo)
        self.assertRaises(AttributeError, getattr,
                          DBRef("coll", 5, foo="bar"), "bar")

    def test_deepcopy(self):
        a = DBRef('coll', 'asdf', 'db', x=[1])
        b = deepcopy(a)

        self.assertEqual(a, b)
        self.assertNotEqual(id(a), id(b.x))
        self.assertEqual(a.x, b.x)
        self.assertNotEqual(id(a.x), id(b.x))

        b.x[0] = 2
        self.assertEqual(a.x, [1])
        self.assertEqual(b.x, [2])

    def test_pickling(self):
        dbr = DBRef('coll', 5, foo='bar')
        for protocol in [0, 1, 2, -1]:
            pkl = pickle.dumps(dbr, protocol=protocol)
            dbr2 = pickle.loads(pkl)
            self.assertEqual(dbr, dbr2)

    def test_dbref_hash(self):
        dbref_1a = DBRef('collection', 'id', 'database')
        dbref_1b = DBRef('collection', 'id', 'database')
        self.assertEqual(hash(dbref_1a), hash(dbref_1b))

        dbref_2a = DBRef('collection', 'id', 'database', custom='custom')
        dbref_2b = DBRef('collection', 'id', 'database', custom='custom')
        self.assertEqual(hash(dbref_2a), hash(dbref_2b))

        self.assertNotEqual(hash(dbref_1a), hash(dbref_2a))

if __name__ == "__main__":
    unittest.main()
