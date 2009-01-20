"""Test the database module."""

import unittest
import types
import random
import datetime

from errors import InvalidName, InvalidOperation, CollectionInvalid, OperationFailure
from son import SON
from objectid import ObjectId
from database import Database, ASCENDING, DESCENDING, OFF, SLOW_ONLY, ALL
from connection import Connection
from collection import Collection, SYSTEM_INDEX_COLLECTION
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
        two = db.test.save({"x": 2})
        three = db.test.save({"x": 3})
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
        two = db.test.save({"x": 2})
        three = db.test.save({"x": 3})

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

    def test_create_index(self):
        db = self.connection.test

        self.assertRaises(TypeError, db.test.create_index, 5)
        self.assertRaises(TypeError, db.test.create_index, "hello")
        self.assertRaises(ValueError, db.test.create_index, [])
        self.assertRaises(TypeError, db.test.create_index, [], ASCENDING)
        self.assertRaises(TypeError, db.test.create_index, [("hello", DESCENDING)], DESCENDING)
        self.assertRaises(TypeError, db.test.create_index, "hello", "world")

        db.test.drop_indexes()
        self.assertFalse(db[SYSTEM_INDEX_COLLECTION].find_one({"ns": u"test.test"}))

        db.test.create_index("hello", ASCENDING)
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])

        count = 0
        for _ in db[SYSTEM_INDEX_COLLECTION].find({"ns": u"test.test"}):
            count += 1
        self.assertEqual(count, 2)

        db.test.drop_indexes()
        self.assertFalse(db[SYSTEM_INDEX_COLLECTION].find_one({"ns": u"test.test"}))
        db.test.create_index("hello", ASCENDING)
        self.assertEqual(db[SYSTEM_INDEX_COLLECTION].find_one({"ns": u"test.test"}),
                         SON([(u"name", u"hello_1"),
                              (u"ns", u"test.test"),
                              (u"key", SON([(u"hello", 1)]))]))

        db.test.drop_indexes()
        self.assertFalse(db[SYSTEM_INDEX_COLLECTION].find_one({"ns": u"test.test"}))
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])
        self.assertEqual(db[SYSTEM_INDEX_COLLECTION].find_one({"ns": u"test.test"}),
                         SON([(u"name", u"hello_-1_world_1"),
                              (u"ns", u"test.test"),
                              (u"key", SON([(u"hello", -1),
                                            (u"world", 1)]))]))

    def test_limit(self):
        db = self.connection.test

        self.assertRaises(TypeError, db.test.find().limit, None)
        self.assertRaises(TypeError, db.test.find().limit, "hello")
        self.assertRaises(TypeError, db.test.find().limit, 5.5)

        db.test.remove({})
        for i in range(100):
            db.test.save({"x": i})

        count = 0
        for _ in db.test.find():
            count += 1
        self.assertEqual(count, 100)

        count = 0
        for _ in db.test.find().limit(20):
            count += 1
        self.assertEqual(count, 20)

        count = 0
        for _ in db.test.find().limit(99):
            count += 1
        self.assertEqual(count, 99)

        count = 0
        for _ in db.test.find().limit(1):
            count += 1
        self.assertEqual(count, 1)

        count = 0
        for _ in db.test.find().limit(0):
            count += 1
        self.assertEqual(count, 100)

        count = 0
        for _ in db.test.find().limit(0).limit(50).limit(10):
            count += 1
        self.assertEqual(count, 10)

        a = db.test.find()
        a.limit(10)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.limit, 5)

    def test_skip(self):
        db = self.connection.test

        self.assertRaises(TypeError, db.test.find().skip, None)
        self.assertRaises(TypeError, db.test.find().skip, "hello")
        self.assertRaises(TypeError, db.test.find().skip, 5.5)

        db.test.remove({})
        for i in range(100):
            db.test.save({"x": i})

        for i in db.test.find():
            self.assertEqual(i["x"], 0)
            break

        for i in db.test.find().skip(20):
            self.assertEqual(i["x"], 20)
            break

        for i in db.test.find().skip(99):
            self.assertEqual(i["x"], 99)
            break

        for i in db.test.find().skip(1):
            self.assertEqual(i["x"], 1)
            break

        for i in db.test.find().skip(0):
            self.assertEqual(i["x"], 0)
            break

        for i in db.test.find().skip(0).skip(50).skip(10):
            self.assertEqual(i["x"], 10)
            break

        for i in db.test.find().skip(1000):
            self.fail()

        a = db.test.find()
        a.skip(10)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.skip, 5)

    def test_sort(self):
        db = self.connection.test

        self.assertRaises(TypeError, db.test.find().sort, 5)
        self.assertRaises(TypeError, db.test.find().sort, "hello")
        self.assertRaises(ValueError, db.test.find().sort, [])
        self.assertRaises(TypeError, db.test.find().sort, [], ASCENDING)
        self.assertRaises(TypeError, db.test.find().sort, [("hello", DESCENDING)], DESCENDING)
        self.assertRaises(TypeError, db.test.find().sort, "hello", "world")

        db.test.remove({})

        unsort = range(10)
        random.shuffle(unsort)

        for i in unsort:
            db.test.save({"x": i})

        asc = [i["x"] for i in db.test.find().sort("x", ASCENDING)]
        self.assertEqual(asc, range(10))
        asc = [i["x"] for i in db.test.find().sort([("x", ASCENDING)])]
        self.assertEqual(asc, range(10))

        expect = range(10)
        expect.reverse()
        desc = [i["x"] for i in db.test.find().sort("x", DESCENDING)]
        self.assertEqual(desc, expect)
        desc = [i["x"] for i in db.test.find().sort([("x", DESCENDING)])]
        self.assertEqual(desc, expect)
        desc = [i["x"] for i in db.test.find().sort("x", ASCENDING).sort("x", DESCENDING)]
        self.assertEqual(desc, expect)

        expected = [(1, 5), (2, 5), (0, 3), (7, 3), (9, 2), (2, 1), (3, 1)]
        shuffled = list(expected)
        random.shuffle(shuffled)

        db.test.remove({})
        for (a, b) in shuffled:
            db.test.save({"a": a, "b": b})

        result = [(i["a"], i["b"]) for i in db.test.find().sort([("b", DESCENDING),
                                                                 ("a", ASCENDING)])]
        self.assertEqual(result, expected)

        a = db.test.find()
        a.sort("x", ASCENDING)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.sort, "x", ASCENDING)

    def test_count(self):
        db = self.connection.test
        db.test.remove({})

        self.assertEqual(0, db.test.find().count())

        for i in range(10):
            db.test.save({"x": i})

        self.assertEqual(10, db.test.find().count())
        self.assertTrue(isinstance(db.test.find().count(), types.IntType))
        self.assertEqual(10, db.test.find().limit(5).count())
        self.assertEqual(10, db.test.find().skip(5).count())

        self.assertEqual(1, db.test.find({"x": 1}).count())
        self.assertEqual(5, db.test.find({"x": {"$lt": 5}}).count())

        a = db.test.find()
        b = a.count()
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.count)

        self.assertEqual(0, db.test.acollectionthatdoesntexist.find().count())

if __name__ == "__main__":
    unittest.main()
