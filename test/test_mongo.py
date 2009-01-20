"""Test for the level 1 Mongo driver."""

import unittest
import random
import types
import os

from objectid import ObjectId
from dbref import DBRef
from son import SON
from errors import InvalidOperation, ConnectionFailure
from mongo import Mongo, ASCENDING, DESCENDING

class TestMongo(unittest.TestCase):
    def setUp(self):
        self.host = os.environ.get("db_ip", "localhost")
        self.port = int(os.environ.get("db_port", 27017))

    def test_connection(self):
        self.assertRaises(TypeError, Mongo, 1)
        self.assertRaises(TypeError, Mongo, 1.14)
        self.assertRaises(TypeError, Mongo, None)
        self.assertRaises(TypeError, Mongo, [])
        self.assertRaises(TypeError, Mongo, "test", 1)
        self.assertRaises(TypeError, Mongo, "test", 1.14)
        self.assertRaises(TypeError, Mongo, "test", None)
        self.assertRaises(TypeError, Mongo, "test", [])
        self.assertRaises(TypeError, Mongo, "test", "localhost", "27017")
        self.assertRaises(TypeError, Mongo, "test", "localhost", 1.14)
        self.assertRaises(TypeError, Mongo, "test", "localhost", None)
        self.assertRaises(TypeError, Mongo, "test", "localhost", [])
        self.assertRaises(TypeError, Mongo, "test", "localhost", 27017, "settings")
        self.assertRaises(TypeError, Mongo, "test", "localhost", 27017, None)

        self.assertRaises(ConnectionFailure, Mongo, "test", "somedomainthatdoesntexist.org")
        self.assertRaises(ConnectionFailure, Mongo, "test", self.host, 123456789)

        self.assertTrue(Mongo("test", self.host, self.port))

    def test_repr(self):
        self.assertEqual(repr(Mongo("test", self.host, self.port)),
                         "Mongo(u'test', '%s', %s)" % (self.host, self.port))
        self.assertEqual(repr(Mongo("test", self.host, self.port).test),
                         "Collection(Mongo(u'test', '%s', %s), u'test')" % (self.host, self.port))

    def test_save_find_one(self):
        db = Mongo("test", self.host, self.port)
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
        db = Mongo("test", self.host, self.port)
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
        db = Mongo("test", self.host, self.port)
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
        db = Mongo("test", self.host, self.port)

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

    def test_limit(self):
        db = Mongo("test", self.host, self.port)

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
        db = Mongo("test", self.host, self.port)

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
        db = Mongo("test", self.host, self.port)

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
        db = Mongo("test", self.host, self.port)
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

    def test_deref(self):
        db = Mongo("test", self.host, self.port)
        db.test.remove({})

        self.assertRaises(TypeError, db.dereference, 5)
        self.assertRaises(TypeError, db.dereference, "hello")
        self.assertRaises(TypeError, db.dereference, None)

        self.assertEqual(None, db.dereference(DBRef("test", ObjectId())))

        obj = {"x": True}
        key = db.test.save(obj)
        self.assertEqual(obj, db.dereference(DBRef("test", key)))

    def test_auto_deref(self):
        db = Mongo("test", self.host, self.port)
        db.test.a.remove({})
        db.test.b.remove({})
        db.test.remove({})

        a = {"hello": u"world"}
        key = db.test.b.save(a)
        dbref = DBRef("test.b", key)

        self.assertEqual(db.dereference(dbref), a)

        b = {"b_obj": dbref}
        db.test.a.save(b)
        self.assertEqual(dbref, db.test.a.find_one()["b_obj"])
        self.assertEqual(a, db.dereference(db.test.a.find_one()["b_obj"]))

        db = Mongo("test", self.host, self.port, {"auto_dereference": False})
        self.assertEqual(dbref, db.test.a.find_one()["b_obj"])

        db = Mongo("test", self.host, self.port, {"auto_dereference": True})
        self.assertNotEqual(dbref, db.test.a.find_one()["b_obj"])
        self.assertEqual(a, db.test.a.find_one()["b_obj"])

        key2 = db.test.a.save({"x": [dbref]})
        self.assertEqual(a, db.test.a.find_one(key2)["x"][0])

        dbref2 = DBRef("test.a", key2)
        key3 = db.test.b.save({"x": dbref2})
        self.assertEqual(a, db.test.b.find_one(key3)["x"]["x"][0])

        dbref = DBRef("test.c", ObjectId())
        key = db.test.save({"x": dbref})
        self.assertEqual(dbref, db.test.find_one(key)["x"])

    def test_auto_ref(self):
        db = Mongo("test", self.host, self.port)
        db.test.a.remove({})
        db.test.b.remove({})

        a = SON({u"hello": u"world"})
        db.test.a.save(a)
        self.assertEqual(a["_ns"], "test.a")

        b = SON({"ref?": a})
        key = db.test.b.save(b)
        self.assertEqual(b["_ns"], "test.b")
        self.assertEqual(b["ref?"], a)
        self.assertEqual(db.test.b.find_one(key)["ref?"], a)

        db = Mongo("test", self.host, self.port, {"auto_reference": False})
        key = db.test.b.save(b)
        self.assertEqual(b["_ns"], "test.b")
        self.assertEqual(b["ref?"], a)
        self.assertEqual(db.test.b.find_one(key)["ref?"], a)

        db = Mongo("test", self.host, self.port, {"auto_reference": True})
        key = db.test.b.save(b)
        self.assertEqual(b["_ns"], "test.b")
        self.assertEqual(b["ref?"], a)
        self.assertNotEqual(db.test.b.find_one(key)["ref?"], a)
        self.assertEqual(db.dereference(db.test.b.find_one(key)["ref?"]), a)

    def test_auto_ref_and_deref(self):
        db = Mongo("test", self.host, self.port, {"auto_reference": True, "auto_dereference": True})
        db.test.a.remove({})
        db.test.b.remove({})
        db.test.c.remove({})

        a = SON({"hello": u"world"})
        b = SON({"test": a})
        c = SON({"another test": b})

        db.test.a.save(a)
        db.test.b.save(b)
        db.test.c.save(c)

        self.assertEqual(db.test.a.find_one(), a)
        self.assertEqual(db.test.b.find_one()["test"], a)
        self.assertEqual(db.test.c.find_one()["another test"]["test"], a)
        self.assertEqual(db.test.b.find_one(), b)
        self.assertEqual(db.test.c.find_one()["another test"], b)
        self.assertEqual(db.test.c.find_one(), c)

if __name__ == "__main__":
    unittest.main()
