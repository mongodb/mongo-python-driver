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

"""Test the database module."""

import unittest
import types
import random
import datetime

from test_connection import get_connection
from pymongo.errors import InvalidName, InvalidOperation, CollectionInvalid, OperationFailure
from pymongo.son import SON
from pymongo.objectid import ObjectId
from pymongo.database import Database, ASCENDING, DESCENDING, OFF, SLOW_ONLY, ALL
from pymongo.connection import Connection
from pymongo.collection import Collection

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

    def test_create_collection(self):
        db = Database(self.connection, "test")

        db.test.insert({"hello": "world"})
        self.assertRaises(CollectionInvalid, db.create_collection, "test")

        db.drop_collection("test")

        self.assertRaises(TypeError, db.create_collection, 5)
        self.assertRaises(TypeError, db.create_collection, None)
        self.assertRaises(InvalidName, db.create_collection, "coll..ection")
        self.assertRaises(TypeError, db.create_collection, "test", 5)
        self.assertRaises(TypeError, db.create_collection, "test", None)

        test = db.create_collection("test")
        test.save({"hello": u"world"})
        self.assertEqual(db.test.find_one()["hello"], "world")
        self.assertTrue(u"test" in db.collection_names())

        db.drop_collection("test.foo")
        db.create_collection("test.foo")
        self.assertFalse(u"test.foo" in db.collection_names())
        db.create_collection("test.foo", {"capped": True})
        self.assertTrue(u"test.foo" in db.collection_names())

    def test_collection_names(self):
        db = Database(self.connection, "test")
        db.test.save({"dummy": u"object"})
        db.test.mike.save({"dummy": u"object"})

        colls = db.collection_names()
        self.assertTrue("test" in colls)
        self.assertTrue("test.mike" in colls)
        for coll in colls:
            self.assertTrue("$" not in coll)

    def test_drop_collection(self):
        db = Database(self.connection, "test")

        self.assertRaises(TypeError, db.drop_collection, 5)
        self.assertRaises(TypeError, db.drop_collection, None)

        db.test.save({"dummy": u"object"})
        self.assertTrue("test" in db.collection_names())
        db.drop_collection("test")
        self.assertFalse("test" in db.collection_names())

        db.test.save({"dummy": u"object"})
        self.assertTrue("test" in db.collection_names())
        db.drop_collection(u"test")
        self.assertFalse("test" in db.collection_names())

        db.test.save({"dummy": u"object"})
        self.assertTrue("test" in db.collection_names())
        db.drop_collection(db.test)
        self.assertFalse("test" in db.collection_names())

        db.drop_collection(db.test.doesnotexist)

    def test_validate_collection(self):
        db = self.connection.test

        self.assertRaises(TypeError, db.validate_collection, 5)
        self.assertRaises(TypeError, db.validate_collection, None)

        db.test.save({"dummy": u"object"})

        self.assertRaises(OperationFailure, db.validate_collection, "test.doesnotexist")
        self.assertRaises(OperationFailure, db.validate_collection, db.test.doesnotexist)

        self.assertTrue(db.validate_collection("test"))
        self.assertTrue(db.validate_collection(db.test))

    def test_profiling_levels(self):
        db = self.connection.test
        self.assertEqual(db.profiling_level(), OFF) #default

        self.assertRaises(ValueError, db.set_profiling_level, 5.5)
        self.assertRaises(ValueError, db.set_profiling_level, None)
        self.assertRaises(ValueError, db.set_profiling_level, -1)

        db.set_profiling_level(SLOW_ONLY)
        self.assertEqual(db.profiling_level(), SLOW_ONLY)

        db.set_profiling_level(ALL)
        self.assertEqual(db.profiling_level(), ALL)

        db.set_profiling_level(OFF)
        self.assertEqual(db.profiling_level(), OFF)

    def test_profiling_info(self):
        db = self.connection.test

        db.set_profiling_level(ALL)
        db.test.find()
        db.set_profiling_level(OFF)

        info = db.profiling_info()
        self.assertTrue(isinstance(info, types.ListType))
        self.assertTrue(len(info) >= 1)
        self.assertTrue(isinstance(info[0]["info"], types.StringTypes))
        self.assertTrue(isinstance(info[0]["ts"], datetime.datetime))
        self.assertTrue(isinstance(info[0]["millis"], types.FloatType))

    def test_iteration(self):
        db = self.connection.test

        def iterate():
            [a for a in db]

        self.assertRaises(TypeError, iterate)

    def test_save_find_one(self):
        db = Database(self.connection, "test")
        db.test.remove({})

        a_doc = SON({"hello": u"world"})
        a_key = db.test.save(a_doc)
        self.assertTrue(isinstance(a_doc["_id"], ObjectId))
        self.assertEqual(a_doc["_id"], a_key)
        self.assertEqual(a_doc, db.test.find_one({"_id": a_doc["_id"]}))
        self.assertEqual(a_doc, db.test.find_one(a_key))
        self.assertEqual(None, db.test.find_one(ObjectId()))
        self.assertEqual(a_doc, db.test.find_one({"hello": u"world"}))
        self.assertEqual(None, db.test.find_one({"hello": u"test"}))

        b = db.test.find_one()
        self.assertFalse(b["_id"].is_new())
        b["hello"] = u"mike"
        db.test.save(b)

        self.assertNotEqual(a_doc, db.test.find_one(a_key))
        self.assertEqual(b, db.test.find_one(a_key))
        self.assertEqual(b, db.test.find_one())

        count = 0
        for _ in db.test.find():
            count += 1
        self.assertEqual(count, 1)

    def test_remove(self):
        db = self.connection.test
        db.test.remove({})

        self.assertRaises(TypeError, db.test.remove, 5)
        self.assertRaises(TypeError, db.test.remove, "test")
        self.assertRaises(TypeError, db.test.remove, [])

        one = db.test.save({"x": 1})
        db.test.save({"x": 2})
        db.test.save({"x": 3})
        length = 0
        for _ in db.test.find():
            length += 1
        self.assertEqual(length, 3)

        db.test.remove(one)
        length = 0
        for _ in db.test.find():
            length += 1
        self.assertEqual(length, 2)

        db.test.remove(db.test.find_one())
        db.test.remove(db.test.find_one())
        self.assertEqual(db.test.find_one(), None)

        one = db.test.save({"x": 1})
        db.test.save({"x": 2})
        db.test.save({"x": 3})

        self.assertTrue(db.test.find_one({"x": 2}))
        db.test.remove({"x": 2})
        self.assertFalse(db.test.find_one({"x": 2}))

        self.assertTrue(db.test.find_one())
        db.test.remove({})
        self.assertFalse(db.test.find_one())

    def test_save_a_bunch(self):
        db = self.connection.test
        db.test.remove({})

        for i in xrange(1000):
            db.test.save({"x": i})

        count = 0
        for _ in db.test.find():
            count += 1

        self.assertEqual(1000, count)

        # test that kill cursors doesn't assert or anything
        for _ in xrange(62):
            for _ in db.test.find():
                break

if __name__ == "__main__":
    unittest.main()
