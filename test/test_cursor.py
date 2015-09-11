# Copyright 2009-2015 MongoDB, Inc.
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
import copy
import itertools
import random
import re
import sys
import unittest
import warnings

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.code import Code
from bson.son import SON
from pymongo import (ASCENDING,
                     DESCENDING,
                     ALL,
                     OFF)
from pymongo.command_cursor import CommandCursor
from pymongo.cursor_manager import CursorManager
from pymongo.database import Database
from pymongo.errors import (InvalidOperation,
                            OperationFailure,
                            ExecutionTimeout)
from pymongo.cursor import CursorType

from test import version, skip_restricted_localhost
from test.test_client import get_client
from test.utils import (catch_warnings, is_mongos,
                        get_command_line, server_started_with_auth)


setUpModule = skip_restricted_localhost


class TestCursor(unittest.TestCase):

    def setUp(self):
        self.client = get_client()
        self.db = Database(self.client, "pymongo_test")

    def tearDown(self):
        self.db = None

    def test_max_time_ms(self):
        if not version.at_least(self.db.connection, (2, 5, 3, -1)):
            raise SkipTest("MaxTimeMS requires MongoDB >= 2.5.3")

        db = self.db
        db.pymongo_test.drop()
        coll = db.pymongo_test
        self.assertRaises(TypeError, coll.find().max_time_ms, 'foo')
        coll.insert({"amalia": 1})
        coll.insert({"amalia": 2})

        coll.find().max_time_ms(None)
        coll.find().max_time_ms(1L)

        cursor = coll.find().max_time_ms(999)
        self.assertEqual(999, cursor._Cursor__max_time_ms)
        cursor = coll.find().max_time_ms(10).max_time_ms(1000)
        self.assertEqual(1000, cursor._Cursor__max_time_ms)

        cursor = coll.find().max_time_ms(999)
        c2 = cursor.clone()
        self.assertEqual(999, c2._Cursor__max_time_ms)
        self.assertTrue("$maxTimeMS" in cursor._Cursor__query_spec())
        self.assertTrue("$maxTimeMS" in c2._Cursor__query_spec())

        self.assertTrue(coll.find_one(max_time_ms=1000))

        if "enableTestCommands=1" in get_command_line(self.client)["argv"]:
            # Cursor parses server timeout error in response to initial query.
            self.client.admin.command("configureFailPoint",
                                      "maxTimeAlwaysTimeOut",
                                      mode="alwaysOn")
            try:
                cursor = coll.find().max_time_ms(1)
                try:
                    cursor.next()
                except ExecutionTimeout:
                    pass
                else:
                    self.fail("ExecutionTimeout not raised")
                self.assertRaises(ExecutionTimeout,
                                  coll.find_one, max_time_ms=1)
            finally:
                self.client.admin.command("configureFailPoint",
                                          "maxTimeAlwaysTimeOut",
                                          mode="off")

    def test_max_time_ms_getmore(self):
        # Test that Cursor handles server timeout error in response to getmore.
        if "enableTestCommands=1" not in get_command_line(self.client)["argv"]:
            raise SkipTest("Need test commands enabled")

        if not version.at_least(self.db.connection, (2, 5, 3, -1)):
            raise SkipTest("MaxTimeMS requires MongoDB >= 2.5.3")

        coll = self.db.pymongo_test
        coll.insert({} for _ in range(200))
        cursor = coll.find().max_time_ms(100)

        # Send initial query before turning on failpoint.
        cursor.next()
        self.client.admin.command("configureFailPoint",
                                  "maxTimeAlwaysTimeOut",
                                  mode="alwaysOn")
        try:
            try:
                # Iterate up to first getmore.
                list(cursor)
            except ExecutionTimeout:
                pass
            else:
                self.fail("ExecutionTimeout not raised")
        finally:
            self.client.admin.command("configureFailPoint",
                                      "maxTimeAlwaysTimeOut",
                                      mode="off")

    def test_explain(self):
        a = self.db.test.find()
        a.explain()
        for _ in a:
            break
        b = a.explain()
        # "cursor" pre MongoDB 2.7.6, "executionStats" post
        self.assertTrue("cursor" in b or "executionStats" in b)

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

        spec = [("num", DESCENDING)]
        index = db.test.create_index(spec)

        first = db.test.find().next()
        self.assertEqual(0, first.get('num'))
        first = db.test.find().hint(spec).next()
        self.assertEqual(99, first.get('num'))
        self.assertRaises(OperationFailure,
                          db.test.find({"num": 17, "foo": 17})
                          .hint([("foo", ASCENDING)]).explain)

        a = db.test.find({"num": 17})
        a.hint(spec)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.hint, spec)

    def test_hint_by_name(self):
        db = self.db
        db.test.drop()

        for i in range(100):
            db.test.insert({'i': i})

        db.test.create_index([('i', DESCENDING)], name='fooindex')
        first = db.test.find().next()
        self.assertEqual(0, first.get('i'))
        first = db.test.find().hint('fooindex').next()
        self.assertEqual(99, first.get('i'))

    def test_limit(self):
        db = self.db

        self.assertRaises(TypeError, db.test.find().limit, None)
        self.assertRaises(TypeError, db.test.find().limit, "hello")
        self.assertRaises(TypeError, db.test.find().limit, 5.5)
        self.assertTrue(db.test.find().limit(5L))

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

    def test_max(self):
        db = self.db
        db.test.drop()
        db.test.ensure_index([("j", ASCENDING)])

        for j in range(10):
            db.test.insert({"j": j, "k": j})

        cursor = db.test.find().max([("j", 3)])
        self.assertEqual(len(list(cursor)), 3)

        # Tuple.
        cursor = db.test.find().max((("j", 3), ))
        self.assertEqual(len(list(cursor)), 3)

        # Compound index.
        db.test.ensure_index([("j", ASCENDING), ("k", ASCENDING)])
        cursor = db.test.find().max([("j", 3), ("k", 3)])
        self.assertEqual(len(list(cursor)), 3)

        # Wrong order.
        cursor = db.test.find().max([("k", 3), ("j", 3)])
        self.assertRaises(OperationFailure, list, cursor)

        # No such index.
        cursor = db.test.find().max([("k", 3)])
        self.assertRaises(OperationFailure, list, cursor)

        self.assertRaises(TypeError, db.test.find().max, 10)
        self.assertRaises(TypeError, db.test.find().max, {"j": 10})

    def test_min(self):
        db = self.db
        db.test.drop()
        db.test.ensure_index([("j", ASCENDING)])

        for j in range(10):
            db.test.insert({"j": j, "k": j})

        cursor = db.test.find().min([("j", 3)])
        self.assertEqual(len(list(cursor)), 7)

        # Tuple.
        cursor = db.test.find().min((("j", 3), ))
        self.assertEqual(len(list(cursor)), 7)

        # Compound index.
        db.test.ensure_index([("j", ASCENDING), ("k", ASCENDING)])
        cursor = db.test.find().min([("j", 3), ("k", 3)])
        self.assertEqual(len(list(cursor)), 7)

        # Wrong order.
        cursor = db.test.find().min([("k", 3), ("j", 3)])
        self.assertRaises(OperationFailure, list, cursor)

        # No such index.
        cursor = db.test.find().min([("k", 3)])
        self.assertRaises(OperationFailure, list, cursor)

        self.assertRaises(TypeError, db.test.find().min, 10)
        self.assertRaises(TypeError, db.test.find().min, {"j": 10})

    def test_batch_size(self):
        db = self.db
        db.test.drop()
        for x in range(200):
            db.test.save({"x": x})

        self.assertRaises(TypeError, db.test.find().batch_size, None)
        self.assertRaises(TypeError, db.test.find().batch_size, "hello")
        self.assertRaises(TypeError, db.test.find().batch_size, 5.5)
        self.assertRaises(ValueError, db.test.find().batch_size, -1)
        self.assertTrue(db.test.find().batch_size(5L))
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
        self.assertEqual(10, curs._Cursor__retrieved)

        curs = db.test.find().limit(-2).batch_size(0)
        curs.next()
        self.assertEqual(2, curs._Cursor__retrieved)

        curs = db.test.find().limit(-4).batch_size(5)
        curs.next()
        self.assertEqual(4, curs._Cursor__retrieved)

        curs = db.test.find().limit(50).batch_size(500)
        curs.next()
        self.assertEqual(50, curs._Cursor__retrieved)

        curs = db.test.find().batch_size(500)
        curs.next()
        self.assertEqual(500, curs._Cursor__retrieved)

        curs = db.test.find().limit(50)
        curs.next()
        self.assertEqual(50, curs._Cursor__retrieved)

        # these two might be shaky, as the default
        # is set by the server. as of 2.0.0-rc0, 101
        # or 1MB (whichever is smaller) is default
        # for queries without ntoreturn
        curs = db.test.find()
        curs.next()
        self.assertEqual(101, curs._Cursor__retrieved)

        curs = db.test.find().limit(0).batch_size(0)
        curs.next()
        self.assertEqual(101, curs._Cursor__retrieved)

    def test_skip(self):
        db = self.db

        self.assertRaises(TypeError, db.test.find().skip, None)
        self.assertRaises(TypeError, db.test.find().skip, "hello")
        self.assertRaises(TypeError, db.test.find().skip, 5.5)
        self.assertRaises(ValueError, db.test.find().skip, -5)
        self.assertTrue(db.test.find().skip(5L))

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
        self.assertTrue(isinstance(db.test.find().count(), int))
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

    def test_count_with_hint(self):
        collection = self.db.test
        collection.drop()

        collection.save({'i': 1})
        collection.save({'i': 2})
        self.assertEqual(2, collection.find().count())

        collection.create_index([('i', 1)])

        self.assertEqual(1, collection.find({'i': 1}).hint("_id_").count())
        self.assertEqual(2, collection.find().hint("_id_").count())

        if version.at_least(self.client, (2, 6, 0)):
            # Count supports hint
            self.assertRaises(OperationFailure,
                              collection.find({'i': 1}).hint("BAD HINT").count)
        else:
            # Hint is ignored
            self.assertEqual(
                1, collection.find({'i': 1}).hint("BAD HINT").count())

        # Create a sparse index which should have no entries.
        collection.create_index([('x', 1)], sparse=True)

        if version.at_least(self.client, (2, 6, 0)):
            # Count supports hint
            self.assertEqual(0, collection.find({'i': 1}).hint("x_1").count())
        else:
            # Hint is ignored
            self.assertEqual(1, collection.find({'i': 1}).hint("x_1").count())

        self.assertEqual(2, collection.find().hint("x_1").count())

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
        cursor = self.db.test.find({"x": re.compile("^hello.*")},
                                   skip=1,
                                   timeout=False,
                                   snapshot=True,
                                   tailable=True,
                                   as_class=MyClass,
                                   slave_okay=True,
                                   await_data=True,
                                   partial=True,
                                   manipulate=False,
                                   compile_re=False,
                                   modifiers={"$maxTimeMS": 500},
                                   fields={'_id': False}).limit(2)
        cursor.min([('a', 1)]).max([('b', 3)])
        cursor.add_option(128)
        cursor.comment('hi!')

        cursor2 = cursor.clone()
        self.assertEqual(cursor._Cursor__skip, cursor2._Cursor__skip)
        self.assertEqual(cursor._Cursor__limit, cursor2._Cursor__limit)
        self.assertEqual(cursor._Cursor__snapshot, cursor2._Cursor__snapshot)
        self.assertEqual(type(cursor._Cursor__codec_options),
                         type(cursor2._Cursor__codec_options))
        self.assertEqual(cursor._Cursor__slave_okay,
                         cursor2._Cursor__slave_okay)
        self.assertEqual(cursor._Cursor__manipulate,
                         cursor2._Cursor__manipulate)
        self.assertEqual(cursor._Cursor__compile_re,
                         cursor2._Cursor__compile_re)
        self.assertEqual(cursor._Cursor__query_flags,
                         cursor2._Cursor__query_flags)
        self.assertEqual(cursor._Cursor__comment,
                         cursor2._Cursor__comment)
        self.assertEqual(cursor._Cursor__min,
                         cursor2._Cursor__min)
        self.assertEqual(cursor._Cursor__max,
                         cursor2._Cursor__max)
        self.assertEqual(cursor._Cursor__modifiers,
                         cursor2._Cursor__modifiers)

        # Shallow copies can so can mutate
        cursor2 = copy.copy(cursor)
        cursor2._Cursor__fields['cursor2'] = False
        self.assertTrue('cursor2' in cursor._Cursor__fields)

        # Deepcopies and shouldn't mutate
        cursor3 = copy.deepcopy(cursor)
        cursor3._Cursor__fields['cursor3'] = False
        self.assertFalse('cursor3' in cursor._Cursor__fields)

        cursor4 = cursor.clone()
        cursor4._Cursor__fields['cursor4'] = False
        self.assertFalse('cursor4' in cursor._Cursor__fields)

        # Test memo when deepcopying queries
        query = {"hello": "world"}
        query["reflexive"] = query
        cursor = self.db.test.find(query)

        cursor2 = copy.deepcopy(cursor)

        self.assertNotEqual(id(cursor._Cursor__spec),
                            id(cursor2._Cursor__spec))
        self.assertEqual(id(cursor2._Cursor__spec['reflexive']),
                         id(cursor2._Cursor__spec))
        self.assertEqual(len(cursor2._Cursor__spec), 2)

        # Ensure hints are cloned as the correct type
        cursor = self.db.test.find().hint([('z', 1), ("a", 1)])
        cursor2 = copy.deepcopy(cursor)
        self.assertTrue(isinstance(cursor2._Cursor__hint, SON))
        self.assertEqual(cursor._Cursor__hint, cursor2._Cursor__hint)

    def test_deepcopy_cursor_littered_with_regexes(self):

        cursor = self.db.test.find({"x": re.compile("^hmmm.*"),
                                    "y": [re.compile("^hmm.*")],
                                    "z": {"a": [re.compile("^hm.*")]},
                                    re.compile("^key.*"): {"a": [re.compile("^hm.*")]}})

        cursor2 = copy.deepcopy(cursor)
        self.assertEqual(cursor._Cursor__spec, cursor2._Cursor__spec)

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

        # Slave OK
        cursor = self.db.test.find(slave_okay=True)
        self.assertEqual(4, cursor._Cursor__query_options())
        cursor2 = self.db.test.find().add_option(4)
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        self.assertTrue(cursor._Cursor__slave_okay)
        cursor.remove_option(4)
        self.assertEqual(0, cursor._Cursor__query_options())
        self.assertFalse(cursor._Cursor__slave_okay)

        # Timeout
        cursor = self.db.test.find(timeout=False)
        self.assertEqual(16, cursor._Cursor__query_options())
        cursor2 = self.db.test.find().add_option(16)
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.remove_option(16)
        self.assertEqual(0, cursor._Cursor__query_options())

        # Tailable / Await data
        cursor = self.db.test.find(tailable=True, await_data=True)
        self.assertEqual(34, cursor._Cursor__query_options())
        cursor2 = self.db.test.find().add_option(34)
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.remove_option(32)
        self.assertEqual(2, cursor._Cursor__query_options())

        # Exhaust - which mongos doesn't support
        if not is_mongos(self.db.connection):
            cursor = self.db.test.find(exhaust=True)
            self.assertEqual(64, cursor._Cursor__query_options())
            cursor2 = self.db.test.find().add_option(64)
            self.assertEqual(cursor._Cursor__query_options(),
                             cursor2._Cursor__query_options())
            self.assertTrue(cursor._Cursor__exhaust)
            cursor.remove_option(64)
            self.assertEqual(0, cursor._Cursor__query_options())
            self.assertFalse(cursor._Cursor__exhaust)

        # Partial
        cursor = self.db.test.find(partial=True)
        self.assertEqual(128, cursor._Cursor__query_options())
        cursor2 = self.db.test.find().add_option(128)
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.remove_option(128)
        self.assertEqual(0, cursor._Cursor__query_options())

    def test_backport_no_cursor_timeout(self):
        # Test backport of "no_cursor_timeout".
        cursor = self.db.test.find(no_cursor_timeout=True)
        self.assertEqual(16, cursor._Cursor__query_options())
        cursor2 = self.db.test.find().add_option(16)
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.remove_option(16)
        self.assertEqual(0, cursor._Cursor__query_options())

        cursor = self.db.test.find(no_cursor_timeout=True, timeout=True)
        self.assertEqual(16, cursor._Cursor__query_options())
        cursor2 = self.db.test.find().add_option(16)
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.remove_option(16)
        self.assertEqual(0, cursor._Cursor__query_options())

    def test_backport_allow_partial_results(self):
        # Test backport of "allow_partial_results"
        cursor = self.db.test.find(allow_partial_results=True)
        self.assertEqual(128, cursor._Cursor__query_options())
        cursor2 = self.db.test.find().add_option(128)
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.remove_option(128)
        self.assertEqual(0, cursor._Cursor__query_options())

        cursor = self.db.test.find(allow_partial_results=True, partial=False)
        self.assertEqual(128, cursor._Cursor__query_options())
        cursor2 = self.db.test.find().add_option(128)
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.remove_option(128)
        self.assertEqual(0, cursor._Cursor__query_options())

    def test_backport_cursor_type(self):
        # Test cursor_types works for NON_TAILABLE
        cursor = self.db.test.find()
        self.assertEqual(0, cursor._Cursor__query_options())
        cursor2 = self.db.test.find(cursor_type=CursorType.NON_TAILABLE)
        self.assertEqual(0, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())

        # Test cursor_types has precedence over "tailable".
        cursor = self.db.test.find()
        self.assertEqual(0, cursor._Cursor__query_options())
        cursor2 = self.db.test.find(cursor_type=CursorType.NON_TAILABLE,
                                    tailable=True)
        self.assertEqual(0, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())

        # Test cursor_types works for TAILABLE and TAILABLE_AWAIT.
        cursor = self.db.test.find()
        self.assertEqual(0, cursor._Cursor__query_options())
        cursor.add_option(2)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE)
        self.assertEqual(2, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.add_option(32)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE_AWAIT)
        self.assertEqual(34, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())

        # Test cursor_types has precedence over "tailable" and "await_data".
        cursor = self.db.test.find()
        self.assertEqual(0, cursor._Cursor__query_options())
        cursor.add_option(2)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE,
                                    tailable=False)
        self.assertEqual(2, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())
        cursor.add_option(32)
        cursor2 = self.db.test.find(cursor_type=CursorType.TAILABLE_AWAIT,
                                    tailable=False, await_data=False)
        self.assertEqual(34, cursor2._Cursor__query_options())
        self.assertEqual(cursor._Cursor__query_options(),
                         cursor2._Cursor__query_options())

        # Test cursor_types works for EXHAUST - which mongos doesn't support
        if not is_mongos(self.db.connection):
            cursor = self.db.test.find(cursor_type=CursorType.EXHAUST)
            self.assertEqual(64, cursor._Cursor__query_options())
            cursor2 = self.db.test.find().add_option(64)
            self.assertEqual(cursor._Cursor__query_options(),
                             cursor2._Cursor__query_options())
            self.assertTrue(cursor._Cursor__exhaust)
            cursor.remove_option(64)
            self.assertEqual(0, cursor._Cursor__query_options())
            self.assertFalse(cursor._Cursor__exhaust)

            # Test cursor_type has precedence over "exhaust"
            cursor = self.db.test.find(cursor_type=CursorType.EXHAUST,
                                       exhaust=False)
            self.assertEqual(64, cursor._Cursor__query_options())
            cursor2 = self.db.test.find().add_option(64)
            self.assertEqual(cursor._Cursor__query_options(),
                             cursor2._Cursor__query_options())
            self.assertTrue(cursor._Cursor__exhaust)
            cursor.remove_option(64)
            self.assertEqual(0, cursor._Cursor__query_options())
            self.assertFalse(cursor._Cursor__exhaust)

    def test_backport_modifiers(self):
        cur = self.db.test.find()
        self.assertTrue('$query' not in cur._Cursor__query_spec())
        cur = self.db.test.find().comment("testing").max_time_ms(500)
        self.assertTrue('$query' in cur._Cursor__query_spec())
        self.assertEqual(cur._Cursor__query_spec()["$comment"], "testing")
        self.assertEqual(cur._Cursor__query_spec()["$maxTimeMS"], 500)
        cur = self.db.test.find(
            modifiers={"$maxTimeMS": 500, "$comment": "testing"})
        self.assertTrue('$query' in cur._Cursor__query_spec())
        self.assertEqual(cur._Cursor__query_spec()["$comment"], "testing")
        self.assertEqual(cur._Cursor__query_spec()["$maxTimeMS"], 500)

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

        count = itertools.count

        self.assertRaises(IndexError, lambda: self.db.test.find()[-1:])
        self.assertRaises(IndexError, lambda: self.db.test.find()[1:2:2])

        for a, b in zip(count(0), self.db.test.find()):
            self.assertEqual(a, b['i'])

        self.assertEqual(100, len(list(self.db.test.find()[0:])))
        for a, b in zip(count(0), self.db.test.find()[0:]):
            self.assertEqual(a, b['i'])

        self.assertEqual(80, len(list(self.db.test.find()[20:])))
        for a, b in zip(count(20), self.db.test.find()[20:]):
            self.assertEqual(a, b['i'])

        for a, b in zip(count(99), self.db.test.find()[99:]):
            self.assertEqual(a, b['i'])

        for i in self.db.test.find()[1000:]:
            self.fail()

        self.assertEqual(5, len(list(self.db.test.find()[20:25])))
        self.assertEqual(5, len(list(self.db.test.find()[20L:25L])))
        for a, b in zip(count(20), self.db.test.find()[20:25]):
            self.assertEqual(a, b['i'])

        self.assertEqual(80, len(list(self.db.test.find()[40:45][20:])))
        for a, b in zip(count(20), self.db.test.find()[40:45][20:]):
            self.assertEqual(a, b['i'])

        self.assertEqual(80,
                         len(list(self.db.test.find()[40:45].limit(0).skip(20))
                            )
                        )
        for a, b in zip(count(20),
                         self.db.test.find()[40:45].limit(0).skip(20)):
            self.assertEqual(a, b['i'])

        self.assertEqual(80,
                         len(list(self.db.test.find().limit(10).skip(40)[20:]))
                        )
        for a, b in zip(count(20),
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
            raise SkipTest("count with limit / skip requires MongoDB >= 1.1.4")

        self.assertRaises(TypeError, self.db.test.find().count, "foo")

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
        db.create_collection("test", capped=True, size=1000, max=3)

        try:
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

            # Capped rollover - the collection can never
            # have more than 3 documents. Just make sure
            # this doesn't raise...
            db.test.insert(({"x": i} for i in xrange(4, 7)))
            self.assertEqual(0, len(list(cursor)))

            # and that the cursor doesn't think it's still alive.
            self.assertFalse(cursor.alive)

            self.assertEqual(3, db.test.count())
        finally:
            db.drop_collection("test")

    def test_distinct(self):
        if not version.at_least(self.db.connection, (1, 1, 3, 1)):
            raise SkipTest("distinct with query requires MongoDB >= 1.1.3")

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
            raise SkipTest("maxScan requires MongoDB >= 1.5.1")

        self.db.drop_collection("test")
        for _ in range(100):
            self.db.test.insert({})

        self.assertEqual(100, len(list(self.db.test.find())))
        self.assertEqual(50, len(list(self.db.test.find(max_scan=50))))
        self.assertEqual(50, len(list(self.db.test.find()
                                      .max_scan(90).max_scan(50))))

    def test_with_statement(self):
        if sys.version_info < (2, 6):
            raise SkipTest("With statement requires Python >= 2.6")

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

    def test_comment(self):
        if is_mongos(self.client):
            raise SkipTest("profile is not supported by mongos")
        if not version.at_least(self.db.connection, (2, 0)):
            raise SkipTest("Requires server >= 2.0")
        if server_started_with_auth(self.db.connection):
            raise SkipTest("SERVER-4754 - This test uses profiling.")

        # MongoDB 3.1.5 changed the ns for commands.
        regex = {'$regex': 'pymongo_test.(\$cmd|test)'}

        if version.at_least(self.db.connection, (3, 1, 8, -1)):
            query_key = "query.comment"
        else:
            query_key = "query.$comment"

        self.client.drop_database(self.db)
        self.db.set_profiling_level(ALL)
        try:
            list(self.db.test.find().comment('foo'))
            op = self.db.system.profile.find({'ns': 'pymongo_test.test',
                                              'op': 'query',
                                              query_key: 'foo'})
            self.assertEqual(op.count(), 1)

            self.db.test.find().comment('foo').count()
            op = self.db.system.profile.find({'ns': regex,
                                              'op': 'command',
                                              'command.count': 'test',
                                              'command.$comment': 'foo'})
            self.assertEqual(op.count(), 1)

            self.db.test.find().comment('foo').distinct('type')
            op = self.db.system.profile.find({'ns': regex,
                                              'op': 'command',
                                              'command.distinct': 'test',
                                              'command.$comment': 'foo'})
            self.assertEqual(op.count(), 1)
        finally:
            self.db.set_profiling_level(OFF)
            self.db.system.profile.drop()

        self.db.test.insert([{}, {}])
        cursor = self.db.test.find()
        cursor.next()
        self.assertRaises(InvalidOperation, cursor.comment, 'hello')

    def test_cursor_transfer(self):

        # This is just a test, don't try this at home...
        self.db.test.remove({})
        self.db.test.insert({'_id': i} for i in xrange(200))

        class CManager(CursorManager):
            def __init__(self, connection):
                super(CManager, self).__init__(connection)

            def close(self, dummy):
                # Do absolutely nothing...
                pass

        client = self.db.connection
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            client.set_cursor_manager(CManager)

            docs = []
            cursor = self.db.test.find().batch_size(10)
            docs.append(cursor.next())
            cursor.close()
            docs.extend(cursor)
            self.assertEqual(len(docs), 10)
            cmd_cursor = {'id': cursor.cursor_id, 'firstBatch': []}
            ccursor = CommandCursor(cursor.collection, cmd_cursor,
                                    cursor.conn_id, retrieved=cursor.retrieved)
            docs.extend(ccursor)
            self.assertEqual(len(docs), 200)
        finally:
            client.set_cursor_manager(CursorManager)
            ctx.exit()

    def test_alive(self):
        self.db.test.remove()
        self.db.test.insert([{} for _ in range(3)])
        cursor = self.db.test.find().batch_size(2)
        n = 0
        while True:
            cursor.next()
            n += 1
            if 3 == n:
                self.assertFalse(cursor.alive)
                break

            self.assertTrue(cursor.alive)

if __name__ == "__main__":
    unittest.main()
