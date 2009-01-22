"""Test the cursor module."""
import unittest
import types
import random

from test_connection import get_connection
from pymongo.errors import InvalidOperation
from pymongo.cursor import Cursor
from pymongo.database import Database, ASCENDING, DESCENDING

class TestCursor(unittest.TestCase):
    def setUp(self):
        self.db = Database(get_connection(), "test")

    def test_explain(self):
        a = self.db.test.find()
        explanation = a.explain()
        for _ in a:
            break
        self.assertEqual(a.explain(), explanation)
        self.assertTrue("cursor" in explanation)

    def test_hint(self):
        db = self.db
        self.assertRaises(TypeError, db.test.find().hint, 5.5)
        db.test.remove({})
        db.test.drop_indexes()

        for i in range(100):
            db.test.insert({"num": i, "foo": i})

        self.assertEqual(db.test.find({"num": 17, "foo": 17}
                                      ).hint([("num", ASCENDING)]
                                             ).explain()["nscanned"], 100)
        self.assertEqual(db.test.find({"num": 17, "foo": 17}
                                      ).hint([("foo", ASCENDING)]
                                             ).explain()["nscanned"], 100)

        index = db.test.create_index("num", ASCENDING)

        self.assertEqual(db.test.find({"num": 17}).explain()["nscanned"], 1)
        self.assertEqual(db.test.find({"num": 17, "foo": 17}
                                      ).explain()["nscanned"], 100)
        self.assertEqual(db.test.find({"num": 17, "foo": 17}
                                      ).hint(index).explain()["nscanned"], 1)
        self.assertEqual(db.test.find({"num": 17, "foo": 17}
                                      ).hint(index
                                             ).hint(None
                                                    ).explain()["nscanned"], 100)
        self.assertEqual(db.test.find({"num": 17, "foo": 17}
                                      ).hint([("num", ASCENDING)]
                                             ).explain()["nscanned"], 1)
        self.assertEqual(db.test.find({"num": 17, "foo": 17}
                                      ).hint([("foo", ASCENDING)]
                                             ).explain()["nscanned"], 100)

        a = db.test.find({"num": 17})
        a.hint(index)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.hint, index)

    def test_limit(self):
        db = self.db

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
        db = self.db

        self.assertRaises(TypeError, db.test.find().skip, None)
        self.assertRaises(TypeError, db.test.find().skip, "hello")
        self.assertRaises(TypeError, db.test.find().skip, 5.5)

        db.drop_collection("test")

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
        db = self.db

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
        db = self.db
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
