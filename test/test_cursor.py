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

"""Test the cursor module."""
import unittest
import random
import warnings
import sys
import itertools
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.code import Code
from pymongo import (ASCENDING,
                     DESCENDING)
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.errors import (InvalidOperation,
                            OperationFailure)
from test_connection import get_connection
import version


class TestCursor(unittest.TestCase):

    def setUp(self):
        self.db = Database(get_connection(), "pymongo_test")

    def test_explain(self):
        a = self.db.test.find()
        b = a.explain()
        for _ in a:
            break
        c = a.explain()
        del b["millis"]
        b.pop("oldPlan", None)
        del c["millis"]
        c.pop("oldPlan", None)
        self.assertEqual(b, c)
        self.assert_("cursor" in b)

    def test_hint(self):
        db = self.db
        self.assertRaises(TypeError, db.test.find().hint, 5.5)
        db.test.drop()

        for i in range(100):
            db.test.insert({"num": i, "foo": i})

        self.assertRaises(OperationFailure,
                          db.test.find({"num": 17, "foo": 17})
                          .hint([("num", ASCENDING)]).explain)
        self.assertRaises(OperationFailure,
                          db.test.find({"num": 17, "foo": 17})
                          .hint([("foo", ASCENDING)]).explain)

        index = db.test.create_index("num")

        spec = [("num", ASCENDING)]
        self.assertEqual(db.test.find({}).explain()["cursor"], "BasicCursor")
        self.assertEqual(db.test.find({}).hint(spec).explain()["cursor"],
                         "BtreeCursor %s" % index)
        self.assertEqual(db.test.find({}).hint(spec).hint(None)
                         .explain()["cursor"],
                         "BasicCursor")
        self.assertRaises(OperationFailure,
                          db.test.find({"num": 17, "foo": 17})
                          .hint([("foo", ASCENDING)]).explain)

        a = db.test.find({"num": 17})
        a.hint(spec)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.hint, spec)

        self.assertRaises(TypeError, db.test.find().hint, index)

    def test_limit(self):
        db = self.db

        self.assertRaises(TypeError, db.test.find().limit, None)
        self.assertRaises(TypeError, db.test.find().limit, "hello")
        self.assertRaises(TypeError, db.test.find().limit, 5.5)

        db.test.drop()
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

    def test_batch_size(self):
        db = self.db
        db.test.drop()
        for x in range(200):
            db.test.save({"x": x})

        self.assertRaises(TypeError, db.test.find().batch_size, None)
        self.assertRaises(TypeError, db.test.find().batch_size, "hello")
        self.assertRaises(TypeError, db.test.find().batch_size, 5.5)
        self.assertRaises(ValueError, db.test.find().batch_size, -1)
        a = db.test.find()
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.batch_size, 5)

        def cursor_count(cursor, expected_count):
            count = 0
            for _ in cursor:
                count += 1
            self.assertEqual(expected_count, count)

        cursor_count(db.test.find().batch_size(0), 200)
        cursor_count(db.test.find().batch_size(1), 200)
        cursor_count(db.test.find().batch_size(2), 200)
        cursor_count(db.test.find().batch_size(5), 200)
        cursor_count(db.test.find().batch_size(100), 200)
        cursor_count(db.test.find().batch_size(500), 200)

        cursor_count(db.test.find().batch_size(0).limit(1), 1)
        cursor_count(db.test.find().batch_size(1).limit(1), 1)
        cursor_count(db.test.find().batch_size(2).limit(1), 1)
        cursor_count(db.test.find().batch_size(5).limit(1), 1)
        cursor_count(db.test.find().batch_size(100).limit(1), 1)
        cursor_count(db.test.find().batch_size(500).limit(1), 1)

        cursor_count(db.test.find().batch_size(0).limit(10), 10)
        cursor_count(db.test.find().batch_size(1).limit(10), 10)
        cursor_count(db.test.find().batch_size(2).limit(10), 10)
        cursor_count(db.test.find().batch_size(5).limit(10), 10)
        cursor_count(db.test.find().batch_size(100).limit(10), 10)
        cursor_count(db.test.find().batch_size(500).limit(10), 10)

    def test_limit_and_batch_size(self):
        db = self.db
        db.test.drop()
        for x in range(500):
            db.test.save({"x": x})

        curs = db.test.find().limit(0).batch_size(10)
        curs.next()
        self.assertEquals(10, curs._Cursor__retrieved)

        curs = db.test.find().limit(-2).batch_size(0)
        curs.next()
        self.assertEquals(2, curs._Cursor__retrieved)

        curs = db.test.find().limit(-4).batch_size(5)
        curs.next()
        self.assertEquals(4, curs._Cursor__retrieved)

        curs = db.test.find().limit(50).batch_size(500)
        curs.next()
        self.assertEquals(50, curs._Cursor__retrieved)

        curs = db.test.find().batch_size(500)
        curs.next()
        self.assertEquals(500, curs._Cursor__retrieved)

        curs = db.test.find().limit(50)
        curs.next()
        self.assertEquals(50, curs._Cursor__retrieved)

        # these two might be shaky, as the default
        # is set by the server. as of 2.0.0-rc0, 101
        # or 1MB (whichever is smaller) is default
        # for queries without ntoreturn
        curs = db.test.find()
        curs.next()
        self.assertEquals(101, curs._Cursor__retrieved)

        curs = db.test.find().limit(0).batch_size(0)
        curs.next()
        self.assertEquals(101, curs._Cursor__retrieved)

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
        self.assertRaises(ValueError, db.test.find().sort, [])
        self.assertRaises(TypeError, db.test.find().sort, [], ASCENDING)
        self.assertRaises(TypeError, db.test.find().sort,
                          [("hello", DESCENDING)], DESCENDING)
        self.assertRaises(TypeError, db.test.find().sort, "hello", "world")

        db.test.drop()

        unsort = range(10)
        random.shuffle(unsort)

        for i in unsort:
            db.test.save({"x": i})

        asc = [i["x"] for i in db.test.find().sort("x", ASCENDING)]
        self.assertEqual(asc, range(10))
        asc = [i["x"] for i in db.test.find().sort("x")]
        self.assertEqual(asc, range(10))
        asc = [i["x"] for i in db.test.find().sort([("x", ASCENDING)])]
        self.assertEqual(asc, range(10))

        expect = range(10)
        expect.reverse()
        desc = [i["x"] for i in db.test.find().sort("x", DESCENDING)]
        self.assertEqual(desc, expect)
        desc = [i["x"] for i in db.test.find().sort([("x", DESCENDING)])]
        self.assertEqual(desc, expect)
        desc = [i["x"] for i in
                db.test.find().sort("x", ASCENDING).sort("x", DESCENDING)]
        self.assertEqual(desc, expect)

        expected = [(1, 5), (2, 5), (0, 3), (7, 3), (9, 2), (2, 1), (3, 1)]
        shuffled = list(expected)
        random.shuffle(shuffled)

        db.test.drop()
        for (a, b) in shuffled:
            db.test.save({"a": a, "b": b})

        result = [(i["a"], i["b"]) for i in
                  db.test.find().sort([("b", DESCENDING),
                                       ("a", ASCENDING)])]
        self.assertEqual(result, expected)

        a = db.test.find()
        a.sort("x", ASCENDING)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.sort, "x", ASCENDING)

    def test_count(self):
        db = self.db
        db.test.drop()

        self.assertEqual(0, db.test.find().count())

        for i in range(10):
            db.test.save({"x": i})

        self.assertEqual(10, db.test.find().count())
        self.assert_(isinstance(db.test.find().count(), int))
        self.assertEqual(10, db.test.find().limit(5).count())
        self.assertEqual(10, db.test.find().skip(5).count())

        self.assertEqual(1, db.test.find({"x": 1}).count())
        self.assertEqual(5, db.test.find({"x": {"$lt": 5}}).count())

        a = db.test.find()
        b = a.count()
        for _ in a:
            break
        self.assertEqual(b, a.count())

        self.assertEqual(0, db.test.acollectionthatdoesntexist.find().count())

    def test_where(self):
        db = self.db
        db.test.drop()

        a = db.test.find()
        self.assertRaises(TypeError, a.where, 5)
        self.assertRaises(TypeError, a.where, None)
        self.assertRaises(TypeError, a.where, {})

        for i in range(10):
            db.test.save({"x": i})

        self.assertEqual(3, len(list(db.test.find().where('this.x < 3'))))
        self.assertEqual(3,
                         len(list(db.test.find().where(Code('this.x < 3')))))
        self.assertEqual(3, len(list(db.test.find().where(Code('this.x < i',
                                                               {"i": 3})))))
        self.assertEqual(10, len(list(db.test.find())))

        self.assertEqual(3, db.test.find().where('this.x < 3').count())
        self.assertEqual(10, db.test.find().count())
        self.assertEqual(3, db.test.find().where(u'this.x < 3').count())
        self.assertEqual([0, 1, 2],
                         [a["x"] for a in
                          db.test.find().where('this.x < 3')])
        self.assertEqual([],
                         [a["x"] for a in
                          db.test.find({"x": 5}).where('this.x < 3')])
        self.assertEqual([5],
                         [a["x"] for a in
                          db.test.find({"x": 5}).where('this.x > 3')])

        cursor = db.test.find().where('this.x < 3').where('this.x > 7')
        self.assertEqual([8, 9], [a["x"] for a in cursor])

        a = db.test.find()
        b = a.where('this.x > 3')
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.where, 'this.x < 3')

    def test_kill_cursors_implicit(self):
        # Only CPython does reference counting garbage collection.
        if (sys.platform.startswith('java') or
            sys.platform == 'cli' or
            'PyPy' in sys.version):
            raise SkipTest()

        db = self.db
        db.drop_collection("test")

        c = db.command("cursorInfo")["clientCursors_size"]

        test = db.test
        for i in range(10000):
            test.insert({"i": i})
        self.assertEqual(c, db.command("cursorInfo")["clientCursors_size"])

        # Automatically closed by the server (limit == -1).
        for _ in range(10):
            db.test.find_one()
        self.assertEqual(c, db.command("cursorInfo")["clientCursors_size"])

        for _ in range(10):
            for x in db.test.find():
                break
        self.assertEqual(c, db.command("cursorInfo")["clientCursors_size"])

        a = db.test.find()
        for x in a:
            break
        self.assertNotEqual(c, db.command("cursorInfo")["clientCursors_size"])

        # Explicitly close (won't work with PyPy and Jython).
        del a
        self.assertEqual(c, db.command("cursorInfo")["clientCursors_size"])

        # Automatically closed by the server since the entire
        # result was returned.
        a = db.test.find().limit(10)
        for x in a:
            break
        self.assertEqual(c, db.command("cursorInfo")["clientCursors_size"])

    def test_kill_cursors_explicit(self):
        db = self.db
        db.drop_collection("test")

        c = db.command("cursorInfo")["clientCursors_size"]

        test = db.test
        for i in range(10000):
            test.insert({"i": i})
        self.assertEqual(c, db.command("cursorInfo")["clientCursors_size"])

        # Automatically closed by the server (limit == -1).
        for _ in range(10):
            db.test.find_one()
        self.assertEqual(c, db.command("cursorInfo")["clientCursors_size"])

        a = db.test.find()
        for x in a:
            break
        self.assertNotEqual(c, db.command("cursorInfo")["clientCursors_size"])

        # Explicitly close (should work with all interpreter implementations).
        a.close()
        self.assertEqual(c, db.command("cursorInfo")["clientCursors_size"])

        # Automatically closed by the server since the entire
        # result was returned.
        a = db.test.find().limit(10)
        for x in a:
            break
        self.assertEqual(c, db.command("cursorInfo")["clientCursors_size"])

    def test_rewind(self):
        self.db.test.save({"x": 1})
        self.db.test.save({"x": 2})
        self.db.test.save({"x": 3})

        cursor = self.db.test.find().limit(2)

        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(0, count)

        cursor.rewind()
        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        cursor.rewind()
        count = 0
        for _ in cursor:
            break
        cursor.rewind()
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        self.assertEqual(cursor, cursor.rewind())

    def test_clone(self):
        self.db.test.save({"x": 1})
        self.db.test.save({"x": 2})
        self.db.test.save({"x": 3})

        cursor = self.db.test.find().limit(2)

        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(0, count)

        cursor = cursor.clone()
        cursor2 = cursor.clone()
        count = 0
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)
        for _ in cursor2:
            count += 1
        self.assertEqual(4, count)

        cursor.rewind()
        count = 0
        for _ in cursor:
            break
        cursor = cursor.clone()
        for _ in cursor:
            count += 1
        self.assertEqual(2, count)

        self.assertNotEqual(cursor, cursor.clone())

        class MyClass(dict):
            pass

        cursor = self.db.test.find(as_class=MyClass)
        for e in cursor:
            self.assertEqual(type(MyClass()), type(e))
        cursor = self.db.test.find(as_class=MyClass)
        self.assertEqual(type(MyClass()), type(cursor[0]))

        # Just test attributes
        cursor = self.db.test.find(skip=1,
                                   timeout=False,
                                   snapshot=True,
                                   tailable=True,
                                   as_class=MyClass,
                                   slave_okay=True,
                                   await_data=True,
                                   partial=True,
                                   manipulate=False).limit(2)
        cursor.add_option(64)

        cursor2 = cursor.clone()
        self.assertEqual(cursor._Cursor__skip, cursor2._Cursor__skip)
        self.assertEqual(cursor._Cursor__limit, cursor2._Cursor__limit)
        self.assertEqual(cursor._Cursor__timeout, cursor2._Cursor__timeout)
        self.assertEqual(cursor._Cursor__snapshot, cursor2._Cursor__snapshot)
        self.assertEqual(cursor._Cursor__tailable, cursor2._Cursor__tailable)
        self.assertEqual(type(cursor._Cursor__as_class),
                         type(cursor2._Cursor__as_class))
        self.assertEqual(cursor._Cursor__slave_okay,
                         cursor2._Cursor__slave_okay)
        self.assertEqual(cursor._Cursor__await_data,
                         cursor2._Cursor__await_data)
        self.assertEqual(cursor._Cursor__partial, cursor2._Cursor__partial)
        self.assertEqual(cursor._Cursor__manipulate,
                         cursor2._Cursor__manipulate)
        self.assertEqual(cursor._Cursor__query_flags,
                         cursor2._Cursor__query_flags)

    def test_add_remove_option(self):
        cursor = self.db.test.find()
        self.assertEqual(0, cursor._Cursor__query_options())
        cursor.add_option(2)
        cursor2 = self.db.test.find(tailable=True)
        self.assertEqual(2, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.add_option(32)
        cursor2 = self.db.test.find(tailable=True, await_data=True)
        self.assertEqual(34, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.add_option(128)
        cursor2 = self.db.test.find(tailable=True,
                                    await_data=True).add_option(128)
        self.assertEqual(162, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())

        self.assertEqual(162, cursor._Cursor__query_options())
        cursor.add_option(128)
        self.assertEqual(162, cursor._Cursor__query_options())

        cursor.remove_option(128)
        cursor2 = self.db.test.find(tailable=True, await_data=True)
        self.assertEqual(34, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.remove_option(32)
        cursor2 = self.db.test.find(tailable=True)
        self.assertEqual(2, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())

        self.assertEqual(2, cursor._Cursor__query_options())
        cursor.remove_option(32)
        self.assertEqual(2, cursor._Cursor__query_options())

    def test_count_with_fields(self):
        self.db.test.drop()
        self.db.test.save({"x": 1})

        if not version.at_least(self.db.connection, (1, 1, 3, -1)):
            for _ in self.db.test.find({}, ["a"]):
                self.fail()

            self.assertEqual(0, self.db.test.find({}, ["a"]).count())
        else:
            self.assertEqual(1, self.db.test.find({}, ["a"]).count())

    def test_bad_getitem(self):
        self.assertRaises(TypeError, lambda x: self.db.test.find()[x], "hello")
        self.assertRaises(TypeError, lambda x: self.db.test.find()[x], 5.5)
        self.assertRaises(TypeError, lambda x: self.db.test.find()[x], None)

    def test_getitem_slice_index(self):
        self.db.drop_collection("test")
        for i in range(100):
            self.db.test.save({"i": i})

        izip = itertools.izip
        count = itertools.count

        self.assertRaises(IndexError, lambda: self.db.test.find()[-1:])
        self.assertRaises(IndexError, lambda: self.db.test.find()[1:2:2])

        for a, b in izip(count(0), self.db.test.find()):
            self.assertEqual(a, b['i'])

        self.assertEqual(100, len(list(self.db.test.find()[0:])))
        for a, b in izip(count(0), self.db.test.find()[0:]):
            self.assertEqual(a, b['i'])

        self.assertEqual(80, len(list(self.db.test.find()[20:])))
        for a, b in izip(count(20), self.db.test.find()[20:]):
            self.assertEqual(a, b['i'])

        for a, b in izip(count(99), self.db.test.find()[99:]):
            self.assertEqual(a, b['i'])

        for i in self.db.test.find()[1000:]:
            self.fail()

        self.assertEqual(5, len(list(self.db.test.find()[20:25])))
        self.assertEqual(5, len(list(self.db.test.find()[20L:25L])))
        for a, b in izip(count(20), self.db.test.find()[20:25]):
            self.assertEqual(a, b['i'])

        self.assertEqual(80, len(list(self.db.test.find()[40:45][20:])))
        for a, b in izip(count(20), self.db.test.find()[40:45][20:]):
            self.assertEqual(a, b['i'])

        self.assertEqual(80,
                         len(list(self.db.test.find()[40:45].limit(0).skip(20))
                            )
                        )
        for a, b in izip(count(20),
                         self.db.test.find()[40:45].limit(0).skip(20)):
            self.assertEqual(a, b['i'])

        self.assertEqual(80,
                         len(list(self.db.test.find().limit(10).skip(40)[20:]))
                        )
        for a, b in izip(count(20),
                         self.db.test.find().limit(10).skip(40)[20:]):
            self.assertEqual(a, b['i'])

        self.assertEqual(1, len(list(self.db.test.find()[:1])))
        self.assertEqual(5, len(list(self.db.test.find()[:5])))

        self.assertEqual(1, len(list(self.db.test.find()[99:100])))
        self.assertEqual(1, len(list(self.db.test.find()[99:1000])))
        self.assertEqual(0, len(list(self.db.test.find()[10:10])))
        self.assertEqual(0, len(list(self.db.test.find()[:0])))
        self.assertEqual(80,
                         len(list(self.db.test.find()[10:10].limit(0).skip(20))
                            )
                        )

        self.assertRaises(IndexError, lambda: self.db.test.find()[10:8])

    def test_getitem_numeric_index(self):
        self.db.drop_collection("test")
        for i in range(100):
            self.db.test.save({"i": i})

        self.assertEqual(0, self.db.test.find()[0]['i'])
        self.assertEqual(50, self.db.test.find()[50]['i'])
        self.assertEqual(50, self.db.test.find().skip(50)[0]['i'])
        self.assertEqual(50, self.db.test.find().skip(49)[1]['i'])
        self.assertEqual(50, self.db.test.find()[50L]['i'])
        self.assertEqual(99, self.db.test.find()[99]['i'])

        self.assertRaises(IndexError, lambda x: self.db.test.find()[x], -1)
        self.assertRaises(IndexError, lambda x: self.db.test.find()[x], 100)
        self.assertRaises(IndexError,
                          lambda x: self.db.test.find().skip(50)[x], 50)

    def test_count_with_limit_and_skip(self):
        if not version.at_least(self.db.connection, (1, 1, 4, -1)):
            raise SkipTest()

        def check_len(cursor, length):
            self.assertEqual(len(list(cursor)), cursor.count(True))
            self.assertEqual(length, cursor.count(True))

        self.db.drop_collection("test")
        for i in range(100):
            self.db.test.save({"i": i})

        check_len(self.db.test.find(), 100)

        check_len(self.db.test.find().limit(10), 10)
        check_len(self.db.test.find().limit(110), 100)

        check_len(self.db.test.find().skip(10), 90)
        check_len(self.db.test.find().skip(110), 0)

        check_len(self.db.test.find().limit(10).skip(10), 10)
        check_len(self.db.test.find()[10:20], 10)
        check_len(self.db.test.find().limit(10).skip(95), 5)
        check_len(self.db.test.find()[95:105], 5)

    def test_len(self):
        self.assertRaises(TypeError, len, self.db.test.find())

    def test_properties(self):
        self.assertEqual(self.db.test, self.db.test.find().collection)

        def set_coll():
            self.db.test.find().collection = "hello"

        self.assertRaises(AttributeError, set_coll)

    def test_get_more(self):
        db = self.db
        db.drop_collection("test")
        db.test.insert([{'i': i} for i in range(10)])
        self.assertEqual(10, len(list(db.test.find().batch_size(5))))

    def test_tailable(self):
        db = self.db
        db.drop_collection("test")
        db.create_collection("test", capped=True, size=1000)

        cursor = db.test.find(tailable=True)

        db.test.insert({"x": 1})
        count = 0
        for doc in cursor:
            count += 1
            self.assertEqual(1, doc["x"])
        self.assertEqual(1, count)

        db.test.insert({"x": 2})
        count = 0
        for doc in cursor:
            count += 1
            self.assertEqual(2, doc["x"])
        self.assertEqual(1, count)

        db.test.insert({"x": 3})
        count = 0
        for doc in cursor:
            count += 1
            self.assertEqual(3, doc["x"])
        self.assertEqual(1, count)

        self.assertEqual(3, db.test.count())
        db.drop_collection("test")

    def test_distinct(self):
        if not version.at_least(self.db.connection, (1, 1, 3, 1)):
            raise SkipTest()

        self.db.drop_collection("test")

        self.db.test.save({"a": 1})
        self.db.test.save({"a": 2})
        self.db.test.save({"a": 2})
        self.db.test.save({"a": 2})
        self.db.test.save({"a": 3})

        distinct = self.db.test.find({"a": {"$lt": 3}}).distinct("a")
        distinct.sort()

        self.assertEqual([1, 2], distinct)

        self.db.drop_collection("test")

        self.db.test.save({"a": {"b": "a"}, "c": 12})
        self.db.test.save({"a": {"b": "b"}, "c": 8})
        self.db.test.save({"a": {"b": "c"}, "c": 12})
        self.db.test.save({"a": {"b": "c"}, "c": 8})

        distinct = self.db.test.find({"c": 8}).distinct("a.b")
        distinct.sort()

        self.assertEqual(["b", "c"], distinct)

    def test_max_scan(self):
        if not version.at_least(self.db.connection, (1, 5, 1)):
            raise SkipTest()

        self.db.drop_collection("test")
        for _ in range(100):
            self.db.test.insert({})

        self.assertEqual(100, len(list(self.db.test.find())))
        self.assertEqual(50, len(list(self.db.test.find(max_scan=50))))
        self.assertEqual(50, len(list(self.db.test.find()
                                      .max_scan(90).max_scan(50))))

    def test_with_statement(self):
        if sys.version_info < (2, 6):
            raise SkipTest()

        self.db.drop_collection("test")
        for _ in range(100):
            self.db.test.insert({})

        c1 = self.db.test.find()
        exec """
with self.db.test.find() as c2:
    self.assertTrue(c2.alive)
self.assertFalse(c2.alive)

with self.db.test.find() as c2:
    self.assertEqual(100, len(list(c2)))
self.assertFalse(c2.alive)
"""
        self.assertTrue(c1.alive)

if __name__ == "__main__":
    unittest.main()
