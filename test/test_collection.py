# -*- coding: utf-8 -*-

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

"""Test the collection module."""

import itertools
import re
import sys
import time
import unittest
import warnings

from nose.plugins.skip import SkipTest

sys.path[0:0] = [""]

from pymongo import ASCENDING, DESCENDING
from pymongo.binary import Binary
from pymongo.code import Code
from pymongo.collection import Collection
from pymongo.errors import (DuplicateKeyError,
                            InvalidDocument,
                            InvalidName,
                            OperationFailure)
from pymongo.objectid import ObjectId
from pymongo.son import SON
from test.test_connection import get_connection
from test import (qcheck,
                  version)


class TestCollection(unittest.TestCase):

    def setUp(self):
        self.connection = get_connection()
        self.db = self.connection.pymongo_test

    def test_collection(self):
        self.assertRaises(TypeError, Collection, self.db, 5)

        def make_col(base, name):
            return base[name]

        self.assertRaises(InvalidName, make_col, self.db, "")
        self.assertRaises(InvalidName, make_col, self.db, "te$t")
        self.assertRaises(InvalidName, make_col, self.db, ".test")
        self.assertRaises(InvalidName, make_col, self.db, "test.")
        self.assertRaises(InvalidName, make_col, self.db, "tes..t")
        self.assertRaises(InvalidName, make_col, self.db.test, "")
        self.assertRaises(InvalidName, make_col, self.db.test, "te$t")
        self.assertRaises(InvalidName, make_col, self.db.test, ".test")
        self.assertRaises(InvalidName, make_col, self.db.test, "test.")
        self.assertRaises(InvalidName, make_col, self.db.test, "tes..t")
        self.assertRaises(InvalidName, make_col, self.db.test, "tes\x00t")

        self.assert_(isinstance(self.db.test, Collection))
        self.assertEqual(self.db.test, self.db["test"])
        self.assertEqual(self.db.test, Collection(self.db, "test"))
        self.assertEqual(self.db.test.mike, self.db["test.mike"])
        self.assertEqual(self.db.test["mike"], self.db["test.mike"])

    def test_create_index(self):
        db = self.db

        self.assertRaises(TypeError, db.test.create_index, 5)
        self.assertRaises(TypeError, db.test.create_index, {"hello": 1})
        self.assertRaises(ValueError, db.test.create_index, [])

        db.test.drop_indexes()
        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 1)

        db.test.create_index("hello")
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])

        count = 0
        for _ in db.system.indexes.find({"ns": u"pymongo_test.test"}):
            count += 1
        self.assertEqual(count, 3)

        db.test.drop_indexes()
        ix = db.test.create_index([("hello", DESCENDING),
                                   ("world", ASCENDING)], name="hello_world")
        self.assertEquals(ix, "hello_world")

        db.test.drop_indexes()
        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 1)
        db.test.create_index("hello")
        self.assert_(SON([(u"name", u"hello_1"),
                          (u"ns", u"pymongo_test.test"),
                          (u"key", SON([(u"hello", 1)]))]) in
                     list(db.system.indexes
                          .find({"ns": u"pymongo_test.test"})))

        db.test.drop_indexes()
        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 1)
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])
        self.assert_(SON([(u"name", u"hello_-1_world_1"),
                          (u"ns", u"pymongo_test.test"),
                          (u"key", SON([(u"hello", -1),
                                        (u"world", 1)]))]) in
                     list(db.system.indexes
                          .find({"ns": u"pymongo_test.test"})))

    def test_ensure_index(self):
        db = self.db

        self.assertRaises(TypeError, db.test.ensure_index, {"hello": 1})

        db.test.drop_indexes()
        self.assertEqual("hello_1", db.test.create_index("hello"))
        self.assertEqual("hello_1", db.test.create_index("hello"))

        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

        db.test.drop_indexes()
        self.assertEqual("foo",
                         db.test.ensure_index("goodbye", name="foo"))
        self.assertEqual(None, db.test.ensure_index("goodbye", name="foo"))

        db.test.drop_indexes()
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

        db.drop_collection("test")
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

        db_name = self.db.name
        self.connection.drop_database(self.db.name)
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.create_index("goodbye"))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye", ttl=1))
        time.sleep(1.1)
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.create_index("goodbye", ttl=1))
        time.sleep(1.1)
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))

    def test_index_on_binary(self):
        db = self.db
        db.drop_collection("test")
        db.test.save({"bin": Binary("def")})
        db.test.save({"bin": Binary("abc")})
        db.test.save({"bin": Binary("ghi")})

        self.assertEqual(db.test.find({"bin": Binary("abc")})
                         .explain()["nscanned"], 3)

        db.test.create_index("bin")
        self.assertEqual(db.test.find({"bin": Binary("abc")})
                         .explain()["nscanned"], 1)

    def test_drop_index(self):
        db = self.db
        db.test.drop_indexes()
        db.test.create_index("hello")
        name = db.test.create_index("goodbye")

        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 3)
        self.assertEqual(name, "goodbye_1")
        db.test.drop_index(name)
        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 2)
        self.assert_(SON([(u"name", u"hello_1"),
                          (u"ns", u"pymongo_test.test"),
                          (u"key", SON([(u"hello", 1)]))]) in
                     list(db.system.indexes
                          .find({"ns": u"pymongo_test.test"})))

        db.test.drop_indexes()
        db.test.create_index("hello")
        name = db.test.create_index("goodbye")

        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 3)
        self.assertEqual(name, "goodbye_1")
        db.test.drop_index([("goodbye", ASCENDING)])
        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 2)
        self.assert_(SON([(u"name", u"hello_1"),
                          (u"ns", u"pymongo_test.test"),
                          (u"key", SON([(u"hello", 1)]))]) in
                     list(db.system.indexes
                          .find({"ns": u"pymongo_test.test"})))

    def test_index_info(self):
        db = self.db
        db.test.drop_indexes()
        self.assertEqual(len(db.test.index_information()), 1)
        self.assert_("_id_" in db.test.index_information())

        db.test.create_index("hello")
        self.assertEqual(len(db.test.index_information()), 2)
        self.assertEqual(db.test.index_information()["hello_1"],
                         [("hello", ASCENDING)])

        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])
        self.assertEqual(db.test.index_information()["hello_1"],
                         [("hello", ASCENDING)])
        self.assertEqual(len(db.test.index_information()), 3)
        self.assert_(("hello", DESCENDING) in
                     db.test.index_information()["hello_-1_world_1"])
        self.assert_(("world", ASCENDING) in
                     db.test.index_information()["hello_-1_world_1"])
        self.assert_(len(db.test.index_information()["hello_-1_world_1"]) == 2)

    def test_fields_list_to_dict(self):
        f = self.db.test._fields_list_to_dict

        self.assertEqual(f(["a", "b"]), {"a": 1, "b": 1})
        self.assertEqual(f(["a.b.c", "d", "a.c"]),
                         {"a.b.c": 1, "d": 1, "a.c": 1})

    def test_field_selection(self):
        db = self.db
        db.drop_collection("test")

        doc = {"a": 1, "b": 5, "c": {"d": 5, "e": 10}}
        db.test.insert(doc)

        self.assertEqual(db.test.find({}, ["_id"]).next().keys(), ["_id"])
        l = db.test.find({}, ["a"]).next().keys()
        l.sort()
        self.assertEqual(l, ["_id", "a"])
        l = db.test.find({}, ["b"]).next().keys()
        l.sort()
        self.assertEqual(l, ["_id", "b"])
        l = db.test.find({}, ["c"]).next().keys()
        l.sort()
        self.assertEqual(l, ["_id", "c"])
        self.assertEqual(db.test.find({}, ["a"]).next()["a"], 1)
        self.assertEqual(db.test.find({}, ["b"]).next()["b"], 5)
        self.assertEqual(db.test.find({}, ["c"]).next()["c"],
                         {"d": 5, "e": 10})

        self.assertEqual(db.test.find({}, ["c.d"]).next()["c"], {"d": 5})
        self.assertEqual(db.test.find({}, ["c.e"]).next()["c"], {"e": 10})
        self.assertEqual(db.test.find({}, ["b", "c.e"]).next()["c"],
                         {"e": 10})

        l = db.test.find({}, ["b", "c.e"]).next().keys()
        l.sort()
        self.assertEqual(l, ["_id", "b", "c"])
        self.assertEqual(db.test.find({}, ["b", "c.e"]).next()["b"], 5)

    def test_options(self):
        db = self.db
        db.drop_collection("test")
        db.test.save({})
        self.assertEqual(db.test.options(), {})
        self.assertEqual(db.test.doesnotexist.options(), {})

        db.drop_collection("test")
        db.create_collection("test", capped=True)
        self.assertEqual(db.test.options(), {"capped": True})
        db.drop_collection("test")

    def test_insert_find_one(self):
        db = self.db
        db.test.remove({})
        self.assertEqual(db.test.find().count(), 0)
        doc = {"hello": u"world"}
        id = db.test.insert(doc)
        self.assertEqual(db.test.find().count(), 1)
        self.assertEqual(doc, db.test.find_one())
        self.assertEqual(doc["_id"], id)
        self.assert_(isinstance(id, ObjectId))

        def remove_insert_find_one(dict):
            db.test.remove({})
            db.test.insert(dict)
            return db.test.find_one() == dict

        qcheck.check_unittest(self, remove_insert_find_one,
                              qcheck.gen_mongo_dict(3))

    def test_remove_all(self):
        self.db.test.remove()
        self.assertEqual(0, self.db.test.count())

        self.db.test.insert({"x": 1})
        self.db.test.insert({"y": 1})
        self.assertEqual(2, self.db.test.count())

        self.db.test.remove()
        self.assertEqual(0, self.db.test.count())

    def test_find_w_fields(self):
        db = self.db
        db.test.remove({})

        db.test.insert({"x": 1, "mike": "awesome",
                        "extra thing": "abcdefghijklmnopqrstuvwxyz"})
        self.assertEqual(1, db.test.count())
        self.assert_("x" in db.test.find({}).next())
        self.assert_("mike" in db.test.find({}).next())
        self.assert_("extra thing" in db.test.find({}).next())
        self.assert_("x" in db.test.find({}, ["x", "mike"]).next())
        self.assert_("mike" in db.test.find({}, ["x", "mike"]).next())
        self.failIf("extra thing" in db.test.find({}, ["x", "mike"]).next())
        self.failIf("x" in db.test.find({}, ["mike"]).next())
        self.assert_("mike" in db.test.find({}, ["mike"]).next())
        self.failIf("extra thing" in db.test.find({}, ["mike"]).next())

    def test_find_w_regex(self):
        db = self.db
        db.test.remove({})

        db.test.insert({"x": "hello_world"})
        db.test.insert({"x": "hello_mike"})
        db.test.insert({"x": "hello_mikey"})
        db.test.insert({"x": "hello_test"})

        self.assertEqual(db.test.find().count(), 4)
        self.assertEqual(db.test.find({"x":
                                       re.compile("^hello.*")}).count(), 4)
        self.assertEqual(db.test.find({"x":
                                       re.compile("ello")}).count(), 4)
        self.assertEqual(db.test.find({"x":
                                       re.compile("^hello$")}).count(), 0)
        self.assertEqual(db.test.find({"x":
                                       re.compile("^hello_mi.*$")}).count(), 2)

    def test_id_can_be_anything(self):
        db = self.db

        db.test.remove({})
        auto_id = {"hello": "world"}
        db.test.insert(auto_id)
        self.assert_(isinstance(auto_id["_id"], ObjectId))

        numeric = {"_id": 240, "hello": "world"}
        db.test.insert(numeric)
        self.assertEqual(numeric["_id"], 240)

        object = {"_id": numeric, "hello": "world"}
        db.test.insert(object)
        self.assertEqual(object["_id"], numeric)

        for x in db.test.find():
            self.assertEqual(x["hello"], u"world")
            self.assert_("_id" in x)

    def test_iteration(self):
        db = self.db

        def iterate():
            [a for a in db.test]

        self.assertRaises(TypeError, iterate)

    def test_invalid_key_names(self):
        db = self.db
        db.test.remove({})

        db.test.insert({"hello": "world"})
        db.test.insert({"hello": {"hello": "world"}})

        self.assertRaises(InvalidName, db.test.insert, {"$hello": "world"})
        self.assertRaises(InvalidName, db.test.insert,
                          {"hello": {"$hello": "world"}})

        db.test.insert({"he$llo": "world"})
        db.test.insert({"hello": {"hello$": "world"}})

        self.assertRaises(InvalidName, db.test.insert,
                          {".hello": "world"})
        self.assertRaises(InvalidName, db.test.insert,
                          {"hello": {".hello": "world"}})
        self.assertRaises(InvalidName, db.test.insert,
                          {"hello.": "world"})
        self.assertRaises(InvalidName, db.test.insert,
                          {"hello": {"hello.": "world"}})
        self.assertRaises(InvalidName, db.test.insert,
                          {"hel.lo": "world"})
        self.assertRaises(InvalidName, db.test.insert,
                          {"hello": {"hel.lo": "world"}})

        db.test.update({"hello": "world"}, {"$inc": "hello"})

    def test_insert_multiple(self):
        db = self.db
        db.drop_collection("test")
        doc1 = {"hello": u"world"}
        doc2 = {"hello": u"mike"}
        self.assertEqual(db.test.find().count(), 0)
        ids = db.test.insert([doc1, doc2])
        self.assertEqual(db.test.find().count(), 2)
        self.assertEqual(doc1, db.test.find_one({"hello": u"world"}))
        self.assertEqual(doc2, db.test.find_one({"hello": u"mike"}))

        self.assertEqual(2, len(ids))
        self.assertEqual(doc1["_id"], ids[0])
        self.assertEqual(doc2["_id"], ids[1])

        id = db.test.insert([{"hello": 1}])
        self.assert_(isinstance(id, list))
        self.assertEqual(1, len(id))

    def test_insert_iterables(self):
        db = self.db

        self.assertRaises(TypeError, db.test.insert, 4)
        self.assertRaises(TypeError, db.test.insert, None)
        self.assertRaises(TypeError, db.test.insert, True)

        db.drop_collection("test")
        self.assertEqual(db.test.find().count(), 0)
        ids = db.test.insert(({"hello": u"world"}, {"hello": u"world"}))
        self.assertEqual(db.test.find().count(), 2)

        db.drop_collection("test")
        self.assertEqual(db.test.find().count(), 0)
        ids = db.test.insert(itertools.imap(lambda x: {"hello": "world"}, itertools.repeat(None, 10)))
        self.assertEqual(db.test.find().count(), 10)

    def test_save(self):
        self.db.drop_collection("test")
        id = self.db.test.save({"hello": "world"})
        self.assertEqual(self.db.test.find_one()["_id"], id)
        self.assert_(isinstance(id, ObjectId))

    def test_unique_index(self):
        db = self.db

        db.drop_collection("test")
        db.test.create_index("hello")

        db.test.save({"hello": "world"})
        db.test.save({"hello": "mike"})
        db.test.save({"hello": "world"})
        self.failIf(db.error())

        db.drop_collection("test")
        db.test.create_index("hello", unique=True)

        db.test.save({"hello": "world"})
        db.test.save({"hello": "mike"})
        db.test.save({"hello": "world"})
        self.assert_(db.error())

    def test_duplicate_key_error(self):
        db = self.db
        db.drop_collection("test")

        db.test.create_index("x", unique=True)

        db.test.insert({"_id": 1, "x": 1}, safe=True)
        db.test.insert({"_id": 2, "x": 2}, safe=True)

        expected_error = OperationFailure
        if version.at_least(db.connection, (1, 3)):
            expected_error = DuplicateKeyError

        self.assertRaises(expected_error,
                          db.test.insert, {"_id": 1}, safe=True)
        self.assertRaises(expected_error,
                          db.test.insert, {"x": 1}, safe=True)

        self.assertRaises(expected_error,
                          db.test.save, {"x": 2}, safe=True)
        self.assertRaises(expected_error,
                          db.test.update, {"x": 1}, {"$inc": {"x": 1}}, safe=True)


    def test_index_on_subfield(self):
        db = self.db
        db.drop_collection("test")

        db.test.insert({"hello": {"a": 4, "b": 5}})
        db.test.insert({"hello": {"a": 7, "b": 2}})
        db.test.insert({"hello": {"a": 4, "b": 10}})
        self.failIf(db.error())

        db.drop_collection("test")
        db.test.create_index("hello.a", unique=True)

        db.test.insert({"hello": {"a": 4, "b": 5}})
        db.test.insert({"hello": {"a": 7, "b": 2}})
        db.test.insert({"hello": {"a": 4, "b": 10}})
        self.assert_(db.error())

    def test_safe_insert(self):
        db = self.db
        db.drop_collection("test")

        a = {"hello": "world"}
        db.test.insert(a)
        db.test.insert(a)
        self.assert_("E11000" in db.error()["err"])

        self.assertRaises(OperationFailure, db.test.insert, a, safe=True)

    def test_update(self):
        db = self.db
        db.drop_collection("test")

        id1 = db.test.save({"x": 5})
        db.test.update({}, {"$inc": {"x": 1}})
        self.assertEqual(db.test.find_one(id1)["x"], 6)

        id2 = db.test.save({"x": 1})
        db.test.update({"x": 6}, {"$inc": {"x": 1}})
        self.assertEqual(db.test.find_one(id1)["x"], 7)
        self.assertEqual(db.test.find_one(id2)["x"], 1)

    def test_multi_update(self):
        db = self.db
        if not version.at_least(db.connection, (1, 1, 3, -1)):
            raise SkipTest()

        db.drop_collection("test")

        db.test.save({"x": 4, "y": 3})
        db.test.save({"x": 5, "y": 5})
        db.test.save({"x": 4, "y": 4})

        db.test.update({"x": 4}, {"$set": {"y": 5}}, multi=True)

        self.assertEqual(3, db.test.count())
        for doc in db.test.find():
            self.assertEqual(5, doc["y"])

        self.assertEqual(2, db.test.update({"x": 4}, {"$set": {"y": 6}},
                                           multi=True, safe=True)["n"])

    def test_upsert(self):
        db = self.db
        db.drop_collection("test")

        db.test.update({"page": "/"}, {"$inc": {"count": 1}}, upsert=True)
        db.test.update({"page": "/"}, {"$inc": {"count": 1}}, upsert=True)

        self.assertEqual(1, db.test.count())
        self.assertEqual(2, db.test.find_one()["count"])

    def test_safe_update(self):
        db = self.db
        v113minus = version.at_least(db.connection, (1, 1, 3, -1))

        db.drop_collection("test")
        db.test.create_index("x", unique=True)

        db.test.insert({"x": 5})
        id = db.test.insert({"x": 4})

        self.assertEqual(None, db.test.update({"_id": id}, {"$inc": {"x": 1}}))
        if v113minus:
            self.assert_(db.error()["err"].startswith("E11001"))
        else:
            self.assert_(db.error()["err"].startswith("E12011"))

        self.assertRaises(OperationFailure, db.test.update,
                          {"_id": id}, {"$inc": {"x": 1}}, safe=True)

        self.assertEqual(1, db.test.update({"_id": id}, {"$inc": {"x": 2}}, safe=True)["n"])

        self.assertEqual(0, db.test.update({"_id": "foo"}, {"$inc": {"x": 2}}, safe=True)["n"])

    def test_safe_save(self):
        db = self.db
        db.drop_collection("test")
        db.test.create_index("hello", unique=True)

        db.test.save({"hello": "world"})
        db.test.save({"hello": "world"})
        self.assert_("E11000" in db.error()["err"])

        self.assertRaises(OperationFailure, db.test.save, {"hello": "world"}, safe=True)

    def test_safe_remove(self):
        db = self.db
        db.drop_collection("test")
        db.create_collection("test", capped=True, size=1000)

        db.test.insert({"x": 1})
        self.assertEqual(1, db.test.count())

        self.assertEqual(None, db.test.remove({"x": 1}))
        self.assertEqual(1, db.test.count())

        if version.at_least(db.connection, (1, 1, 3, -1)):
            self.assertRaises(OperationFailure, db.test.remove, {"x": 1}, safe=True)
        else: # Just test that it doesn't blow up
            db.test.remove({"x": 1}, safe=True)

        db.drop_collection("test")
        db.test.insert({"x": 1})
        db.test.insert({"x": 1})
        self.assertEqual(2, db.test.remove({}, safe=True)["n"])
        self.assertEqual(0, db.test.remove({}, safe=True)["n"])

    def test_count(self):
        db = self.db
        db.drop_collection("test")

        self.assertEqual(db.test.count(), 0)
        db.test.save({})
        db.test.save({})
        self.assertEqual(db.test.count(), 2)

    def test_group(self):
        db = self.db
        db.drop_collection("test")

        def group_checker(args, expected):
            eval = db.test.group(*args)
            self.assertEqual(eval, expected)


        self.assertEqual([], db.test.group([], {}, {"count": 0},
                                           "function (obj, prev) { prev.count++; }"))

        db.test.save({"a": 2})
        db.test.save({"b": 5})
        db.test.save({"a": 1})

        self.assertEqual([{"count": 3}],
                         db.test.group([], {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"))

        self.assertEqual([{"count": 1}],
                         db.test.group([], {"a": {"$gt": 1}}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"))

        db.test.save({"a": 2, "b": 3})

        self.assertEqual([{"a": 2, "count": 2},
                          {"a": None, "count": 1},
                          {"a": 1, "count": 1}],
                         db.test.group(["a"], {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"))

        # modifying finalize
        self.assertEqual([{"a": 2, "count": 3},
                          {"a": None, "count": 2},
                          {"a": 1, "count": 2}],
                         db.test.group(["a"], {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }",
                                       "function (obj) { obj.count++; }"))

        # returning finalize
        self.assertEqual([2, 1, 1],
                         db.test.group(["a"], {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }",
                                       "function (obj) { return obj.count; }"))

        # keyf
        self.assertEqual([2, 2],
                         db.test.group("function (obj) { if (obj.a == 2) { return {a: true} }; "
                                       "return {b: true}; }", {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }",
                                       "function (obj) { return obj.count; }"))

        # no key
        self.assertEqual([{"count": 4}],
                         db.test.group(None, {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"))

        warnings.simplefilter("error")
        self.assertRaises(DeprecationWarning,
                          db.test.group, [], {}, {"count": 0},
                          "function (obj, prev) { prev.count++; }",
                          command=False)
        warnings.simplefilter("default")

        self.assertRaises(OperationFailure, db.test.group, [], {}, {}, "5 ++ 5")

    def test_group_with_scope(self):
        db = self.db
        db.drop_collection("test")
        db.test.save({"a": 1})
        db.test.save({"b": 1})

        reduce_function = "function (obj, prev) { prev.count += inc_value; }"

        self.assertEqual(2, db.test.group([], {}, {"count": 0},
                                          Code(reduce_function,
                                               {"inc_value": 1}))[0]['count'])
        self.assertEqual(4, db.test.group([], {}, {"count": 0},
                                          Code(reduce_function,
                                               {"inc_value": 2}))[0]['count'])

        self.assertEqual(1, db.test.group([], {}, {"count": 0},
                                          Code(reduce_function,
                                               {"inc_value": 0.5}))[0]['count'])

        if version.at_least(db.connection, (1, 1)):
            self.assertEqual(2, db.test.group([], {}, {"count": 0},
                                              Code(reduce_function,
                                                   {"inc_value": 1}),
                                              command=True)[0]['count'])

            self.assertEqual(4, db.test.group([], {}, {"count": 0},
                                              Code(reduce_function,
                                                   {"inc_value": 2}),
                                              command=True)[0]['count'])

            self.assertEqual(1, db.test.group([], {}, {"count": 0},
                                              Code(reduce_function,
                                                   {"inc_value": 0.5}),
                                              command=True)[0]['count'])

    def test_large_limit(self):
        db = self.db
        db.drop_collection("test")

        for i in range(2000):
            db.test.insert({"x": i, "y": "mongomongo" * 1000})

        self.assertEqual(2000, db.test.count())

        i = 0
        y = 0
        for doc in db.test.find(limit=1900):
            i += 1
            y += doc["x"]

        self.assertEqual(1900, i)
        self.assertEqual(1804050, y)

    def test_find_kwargs(self):
        db = self.db
        db.drop_collection("test")

        for i in range(10):
            db.test.insert({"x": i})

        self.assertEqual(10, db.test.count())

        sum = 0
        for x in db.test.find({}, skip=4, limit=2):
            sum += x["x"]

        self.assertEqual(9, sum)

    def test_rename(self):
        db = self.db
        db.drop_collection("test")
        db.drop_collection("foo")

        self.assertRaises(TypeError, db.test.rename, 5)
        self.assertRaises(InvalidName, db.test.rename, "")
        self.assertRaises(InvalidName, db.test.rename, "te$t")
        self.assertRaises(InvalidName, db.test.rename, ".test")
        self.assertRaises(InvalidName, db.test.rename, "test.")
        self.assertRaises(InvalidName, db.test.rename, "tes..t")

        self.assertEqual(0, db.test.count())
        self.assertEqual(0, db.foo.count())

        for i in range(10):
            db.test.insert({"x": i})

        self.assertEqual(10, db.test.count())

        db.test.rename("foo")

        self.assertEqual(0, db.test.count())
        self.assertEqual(10, db.foo.count())

        x = 0
        for doc in db.foo.find():
            self.assertEqual(x, doc["x"])
            x += 1

    # doesn't really test functionality, just that the option is set correctly
    def test_snapshot(self):
        db = self.db

        self.assertRaises(TypeError, db.test.find, snapshot=5)

        list(db.test.find(snapshot=True))
        self.assertRaises(OperationFailure, list, db.test.find(snapshot=True).sort("foo", 1))

    def test_find_one(self):
        db = self.db
        db.drop_collection("test")

        id = db.test.save({"hello": "world", "foo": "bar"})

        self.assertEqual("world", db.test.find_one()["hello"])
        self.assertEqual(db.test.find_one(id), db.test.find_one())
        self.assertEqual(db.test.find_one(None), db.test.find_one())
        self.assertEqual(db.test.find_one({}), db.test.find_one())
        self.assertEqual(db.test.find_one({"hello": "world"}), db.test.find_one())

        self.assert_("hello" in db.test.find_one(fields=["hello"]))
        self.assert_("hello" not in db.test.find_one(fields=["foo"]))
        self.assertEqual(["_id"], db.test.find_one(fields=[]).keys())

        self.assertEqual(None, db.test.find_one({"hello": "foo"}))
        self.assertEqual(None, db.test.find_one(ObjectId()))

        self.assertRaises(TypeError, db.test.find_one, 6)

    def test_insert_adds_id(self):
        doc = {"hello": "world"}
        self.db.test.insert(doc)
        self.assert_("_id" in doc)

        docs = [{"hello": "world"}, {"hello": "world"}]
        self.db.test.insert(docs)
        for doc in docs:
            self.assert_("_id" in doc)

    def test_save_adds_id(self):
        doc = {"hello": "world"}
        self.db.test.save(doc)
        self.assert_("_id" in doc)

    # TODO doesn't actually test functionality, just that it doesn't blow up
    def test_cursor_timeout(self):
        list(self.db.test.find(timeout=False))
        list(self.db.test.find(timeout=True))

    def test_distinct(self):
        if not version.at_least(self.db.connection, (1, 1)):
            raise SkipTest()

        self.db.drop_collection("test")

        self.db.test.save({"a": 1})
        self.db.test.save({"a": 2})
        self.db.test.save({"a": 2})
        self.db.test.save({"a": 2})
        self.db.test.save({"a": 3})

        distinct = self.db.test.distinct("a")
        distinct.sort()

        self.assertEqual([1, 2, 3], distinct)

        self.db.drop_collection("test")

        self.db.test.save({"a": {"b": "a"}, "c": 12})
        self.db.test.save({"a": {"b": "b"}, "c": 12})
        self.db.test.save({"a": {"b": "c"}, "c": 12})
        self.db.test.save({"a": {"b": "c"}, "c": 12})

        distinct = self.db.test.distinct("a.b")
        distinct.sort()

        self.assertEqual(["a", "b", "c"], distinct)

    def test_query_on_query_field(self):
        self.db.drop_collection("test")
        self.db.test.save({"query": "foo"})
        self.db.test.save({"bar": "foo"})

        self.assertEqual(1, self.db.test.find({"query": {"$ne": None}}).count())
        self.assertEqual(1, len(list(self.db.test.find({"query": {"$ne": None}}))))

    def test_min_query(self):
        self.db.drop_collection("test")
        self.db.test.save({"x": 1})
        self.db.test.save({"x": 2})
        self.db.test.create_index("x")

        self.assertEqual(1, len(list(self.db.test.find({"$min": {"x": 2},
                                                        "$query": {}}))))
        self.assertEqual(2, self.db.test.find({"$min": {"x": 2},
                                               "$query": {}})[0]["x"])

    def test_insert_large_document(self):
        self.assertRaises(InvalidDocument, self.db.test.insert,
                          {"foo": "x" * 4 * 1024 * 1024})
        self.assertRaises(InvalidDocument, self.db.test.insert,
                          [{"x": 1}, {"foo": "x" * 4 * 1024 * 1024}])
        self.db.test.insert([{"foo": "x" * 2 * 1024 * 1024},
                             {"foo": "x" * 2 * 1024 * 1024}], safe=True)

    def test_map_reduce(self):
        if not version.at_least(self.db.connection, (1, 1, 1)):
            raise SkipTest()

        db = self.db
        db.drop_collection("test")

        db.test.insert({"id": 1, "tags": ["dog", "cat"]})
        db.test.insert({"id": 2, "tags": ["cat"]})
        db.test.insert({"id": 3, "tags": ["mouse", "cat", "dog"]})
        db.test.insert({"id": 4, "tags": []})

        map = Code("function () {"
                   "  this.tags.forEach(function(z) {"
                   "    emit(z, 1);"
                   "  });"
                   "}")
        reduce = Code("function (key, values) {"
                      "  var total = 0;"
                      "  for (var i = 0; i < values.length; i++) {"
                      "    total += values[i];"
                      "  }"
                      "  return total;"
                      "}")
        result = db.test.map_reduce(map, reduce)
        self.assertEqual(3, result.find_one({"_id": "cat"})["value"])
        self.assertEqual(2, result.find_one({"_id": "dog"})["value"])
        self.assertEqual(1, result.find_one({"_id": "mouse"})["value"])

        full_result = db.test.map_reduce(map, reduce, full_response=True)
        self.assertEqual(6, full_result["counts"]["emit"])

        result = db.test.map_reduce(map, reduce, limit=2)
        self.assertEqual(2, result.find_one({"_id": "cat"})["value"])
        self.assertEqual(1, result.find_one({"_id": "dog"})["value"])
        self.assertEqual(None, result.find_one({"_id": "mouse"}))

    def test_messages_with_unicode_collection_names(self):
        db = self.db

        db[u"Employés"].insert({"x": 1})
        db[u"Employés"].update({"x": 1}, {"x": 2})
        db[u"Employés"].remove({})
        db[u"Employés"].find_one()
        list(db[u"Employés"].find())

if __name__ == "__main__":
    unittest.main()
