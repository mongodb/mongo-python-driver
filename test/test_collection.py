# -*- coding: utf-8 -*-

# Copyright 2009-2014 MongoDB, Inc.
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
import threading
import time
import warnings

sys.path[0:0] = [""]

from bson.regex import Regex
from bson.code import Code
from bson.dbref import DBRef
from bson.objectid import ObjectId
from bson.py3compat import u, itervalues
from bson.son import SON
from pymongo import (ASCENDING, DESCENDING, GEO2D,
                     GEOHAYSTACK, GEOSPHERE, HASHED, TEXT)
from pymongo import MongoClient
from pymongo.codec_options import CodecOptions
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import EXHAUST
from pymongo.errors import (DocumentTooLarge,
                            DuplicateKeyError,
                            InvalidDocument,
                            InvalidName,
                            InvalidOperation,
                            OperationFailure,
                            WTimeoutError)
from pymongo.read_preferences import ReadPreference
from pymongo.son_manipulator import SONManipulator
from pymongo.write_concern import WriteConcern
from test.test_client import IntegrationTest
from test.utils import (is_mongos, joinall, enable_text_search, get_pool,
                        oid_generated_on_client, ignore_deprecations,
                        rs_or_single_client, wait_until)
from test import client_context, host, port, qcheck, unittest


class TestCollectionNoConnect(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        client = MongoClient(host, port, connect=False)
        cls.db = client.pymongo_test

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

        self.assertTrue(isinstance(self.db.test, Collection))
        self.assertEqual(self.db.test, self.db["test"])
        self.assertEqual(self.db.test, Collection(self.db, "test"))
        self.assertEqual(self.db.test.mike, self.db["test.mike"])
        self.assertEqual(self.db.test["mike"], self.db["test.mike"])

    def test_getattr(self):
        coll = self.db.test
        self.assertTrue(isinstance(coll['_does_not_exist'], Collection))

        with self.assertRaises(AttributeError) as context:
            coll._does_not_exist

        # Message should be:
        # "AttributeError: Collection has no attribute '_does_not_exist'. To
        # access the test._does_not_exist collection, use
        # database['test._does_not_exist']."
        self.assertIn("has no attribute '_does_not_exist'",
                      str(context.exception))

    def test_iteration(self):
        self.assertRaises(TypeError, next, self.db)


class TestCollection(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        super(TestCollection, cls).setUpClass()
        cls.w = client_context.w

    @classmethod
    def tearDownClass(cls):
        cls.db.drop_collection("test_large_limit")

    def test_drop_nonexistent_collection(self):
        self.db.drop_collection('test')
        self.assertFalse('test' in self.db.collection_names())

        # No exception
        self.db.drop_collection('test')

    def test_create_index(self):
        db = self.db

        self.assertRaises(TypeError, db.test.create_index, 5)
        self.assertRaises(TypeError, db.test.create_index, {"hello": 1})
        self.assertRaises(TypeError,
                          db.test.ensure_index, {"hello": 1}, cache_for='foo')
        self.assertRaises(TypeError,
                          db.test.ensure_index, {"hello": 1}, ttl='foo')
        self.assertRaises(ValueError, db.test.create_index, [])

        db.test.drop_indexes()
        db.test.insert({})
        self.assertEqual(len(db.test.index_information()), 1)

        db.test.create_index("hello")
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])

        # Tuple instead of list.
        db.test.create_index((("world", ASCENDING),))

        self.assertEqual(len(db.test.index_information()), 4)

        db.test.drop_indexes()
        ix = db.test.create_index([("hello", DESCENDING),
                                   ("world", ASCENDING)], name="hello_world")
        self.assertEqual(ix, "hello_world")

        db.test.drop_indexes()
        self.assertEqual(len(db.test.index_information()), 1)
        db.test.create_index("hello")
        self.assertTrue("hello_1" in db.test.index_information())

        db.test.drop_indexes()
        self.assertEqual(len(db.test.index_information()), 1)
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])
        self.assertTrue("hello_-1_world_1" in db.test.index_information())

        db.test.drop()
        db.test.insert({'a': 1})
        db.test.insert({'a': 1})
        self.assertRaises(DuplicateKeyError, db.test.create_index,
                                                    'a', unique=True)

    def test_ensure_index(self):
        db = self.db

        self.assertRaises(TypeError, db.test.ensure_index, {"hello": 1})
        self.assertRaises(TypeError,
                          db.test.ensure_index, {"hello": 1}, cache_for='foo')
        self.assertRaises(TypeError,
                          db.test.ensure_index, {"hello": 1}, ttl='foo')

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

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.create_index("goodbye"))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye", cache_for=1))
        time.sleep(1.2)
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.create_index("goodbye", cache_for=1))
        time.sleep(1.2)
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))
        # Make sure the expiration time is updated.
        self.assertEqual(None,
                         db.test.ensure_index("goodbye"))

        # Clean up indexes for later tests
        db.test.drop_indexes()

    def test_deprecated_ttl_index_kwarg(self):
        db = self.db

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            self.assertRaises(DeprecationWarning, lambda:
                              db.test.ensure_index("goodbye", ttl=10))

        with ignore_deprecations():
            self.assertEqual("goodbye_1",
                             db.test.ensure_index("goodbye", ttl=10))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

    def test_ensure_unique_index_threaded(self):
        coll = self.db.test_unique_threaded
        coll.drop()
        coll.insert(({'foo': i} for i in range(10000)))

        class Indexer(threading.Thread):
            def run(self):
                try:
                    coll.ensure_index('foo', unique=True)
                    coll.insert({'foo': 'bar'})
                    coll.insert({'foo': 'bar'})
                except OperationFailure:
                    pass

        threads = []
        for _ in range(10):
            t = Indexer()
            t.setDaemon(True)
            threads.append(t)

        for i in range(10):
            threads[i].start()

        joinall(threads)

        self.assertEqual(10001, coll.count())
        coll.drop()

    def test_drop_index(self):
        db = self.db
        db.test.drop_indexes()
        db.test.create_index("hello")
        name = db.test.create_index("goodbye")

        self.assertEqual(len(db.test.index_information()), 3)
        self.assertEqual(name, "goodbye_1")
        db.test.drop_index(name)

        # Drop it again.
        with self.assertRaises(OperationFailure):
            db.test.drop_index(name)
        self.assertEqual(len(db.test.index_information()), 2)
        self.assertTrue("hello_1" in db.test.index_information())

        db.test.drop_indexes()
        db.test.create_index("hello")
        name = db.test.create_index("goodbye")

        self.assertEqual(len(db.test.index_information()), 3)
        self.assertEqual(name, "goodbye_1")
        db.test.drop_index([("goodbye", ASCENDING)])
        self.assertEqual(len(db.test.index_information()), 2)
        self.assertTrue("hello_1" in db.test.index_information())

    def test_reindex(self):
        db = self.db
        db.drop_collection("test")
        db.test.insert({"foo": "bar", "who": "what", "when": "how"})
        db.test.create_index("foo")
        db.test.create_index("who")
        db.test.create_index("when")
        info = db.test.index_information()

        def check_result(result):
            self.assertEqual(4, result['nIndexes'])
            indexes = result['indexes']
            names = [idx['name'] for idx in indexes]
            for name in names:
                self.assertTrue(name in info)
            for key in info:
                self.assertTrue(key in names)

        reindexed = db.test.reindex()
        if 'raw' in reindexed:
            # mongos
            for result in itervalues(reindexed['raw']):
                check_result(result)
        else:
            check_result(reindexed)

    def test_index_info(self):
        db = self.db
        db.test.drop_indexes()
        db.test.remove({})
        db.test.save({})  # create collection
        self.assertEqual(len(db.test.index_information()), 1)
        self.assertTrue("_id_" in db.test.index_information())

        db.test.create_index("hello")
        self.assertEqual(len(db.test.index_information()), 2)
        self.assertEqual(db.test.index_information()["hello_1"]["key"],
                         [("hello", ASCENDING)])

        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)],
                             unique=True)
        self.assertEqual(db.test.index_information()["hello_1"]["key"],
                         [("hello", ASCENDING)])
        self.assertEqual(len(db.test.index_information()), 3)
        self.assertEqual([("hello", DESCENDING), ("world", ASCENDING)],
                         db.test.index_information()["hello_-1_world_1"]["key"]
                        )
        self.assertEqual(True,
                     db.test.index_information()["hello_-1_world_1"]["unique"])

    def test_index_geo2d(self):
        db = self.db
        db.test.drop_indexes()
        self.assertEqual('loc_2d', db.test.create_index([("loc", GEO2D)]))
        index_info = db.test.index_information()['loc_2d']
        self.assertEqual([('loc', '2d')], index_info['key'])

    @client_context.require_no_mongos
    def test_index_haystack(self):
        db = self.db
        db.test.drop_indexes()
        db.test.remove()
        _id = db.test.insert({
            "pos": {"long": 34.2, "lat": 33.3},
            "type": "restaurant"
        })
        db.test.insert({
            "pos": {"long": 34.2, "lat": 37.3}, "type": "restaurant"
        })
        db.test.insert({
            "pos": {"long": 59.1, "lat": 87.2}, "type": "office"
        })
        db.test.create_index(
            [("pos", GEOHAYSTACK), ("type", ASCENDING)],
            bucket_size=1
        )

        results = db.command(SON([
            ("geoSearch", "test"),
            ("near", [33, 33]),
            ("maxDistance", 6),
            ("search", {"type": "restaurant"}),
            ("limit", 30),
        ]))['results']

        self.assertEqual(2, len(results))
        self.assertEqual({
            "_id": _id,
            "pos": {"long": 34.2, "lat": 33.3},
            "type": "restaurant"
        }, results[0])

    @client_context.require_version_min(2, 3, 2)
    @client_context.require_no_mongos
    def test_index_text(self):
        enable_text_search(self.client)

        db = self.db
        db.test.drop_indexes()
        self.assertEqual("t_text", db.test.create_index([("t", TEXT)]))
        index_info = db.test.index_information()["t_text"]
        self.assertTrue("weights" in index_info)

        if client_context.version.at_least(2, 5, 5):
            db.test.insert([
                {'t': 'spam eggs and spam'},
                {'t': 'spam'},
                {'t': 'egg sausage and bacon'}])

            # MongoDB 2.6 text search. Create 'score' field in projection.
            cursor = db.test.find(
                {'$text': {'$search': 'spam'}},
                {'score': {'$meta': 'textScore'}})

            # Sort by 'score' field.
            cursor.sort([('score', {'$meta': 'textScore'})])
            results = list(cursor)
            self.assertTrue(results[0]['score'] >= results[1]['score'])

        db.test.drop_indexes()

    @client_context.require_version_min(2, 3, 2)
    def test_index_2dsphere(self):
        db = self.db
        db.test.drop_indexes()
        self.assertEqual("geo_2dsphere",
                         db.test.create_index([("geo", GEOSPHERE)]))

        for name, info in db.test.index_information().items():
            field, idx_type = info['key'][0]
            if field == 'geo' and idx_type == '2dsphere':
                break
        else:
            self.fail("2dsphere index not found.")

        poly = {"type": "Polygon",
                "coordinates": [[[40,5], [40,6], [41,6], [41,5], [40,5]]]}
        query = {"geo": {"$within": {"$geometry": poly}}}

        # This query will error without a 2dsphere index.
        db.test.find(query)
        db.test.drop_indexes()

    @client_context.require_version_min(2, 3, 2)
    def test_index_hashed(self):
        db = self.db
        db.test.drop_indexes()
        self.assertEqual("a_hashed",
                         db.test.create_index([("a", HASHED)]))

        for name, info in db.test.index_information().items():
            field, idx_type = info['key'][0]
            if field == 'a' and idx_type == 'hashed':
                break
        else:
            self.fail("hashed index not found.")

        db.test.drop_indexes()

    def test_index_sparse(self):
        db = self.db
        db.test.drop_indexes()
        db.test.create_index([('key', ASCENDING)], sparse=True)
        self.assertTrue(db.test.index_information()['key_1']['sparse'])

    def test_index_background(self):
        db = self.db
        db.test.drop_indexes()
        db.test.create_index([('keya', ASCENDING)])
        db.test.create_index([('keyb', ASCENDING)], background=False)
        db.test.create_index([('keyc', ASCENDING)], background=True)
        self.assertFalse('background' in db.test.index_information()['keya_1'])
        self.assertFalse(db.test.index_information()['keyb_1']['background'])
        self.assertTrue(db.test.index_information()['keyc_1']['background'])

    def _drop_dups_setup(self, db):
        db.drop_collection('test')
        db.test.insert({'i': 1})
        db.test.insert({'i': 2})
        db.test.insert({'i': 2})  # duplicate
        db.test.insert({'i': 3})

    @client_context.require_version_max(2, 6)
    def test_index_drop_dups(self):
        # Try dropping duplicates
        db = self.db
        self._drop_dups_setup(db)

        if client_context.version.at_least(1, 9, 2):
            # No error, just drop the duplicate
            db.test.create_index(
                [('i', ASCENDING)],
                unique=True,
                drop_dups=True
            )
        else:
            # https://jira.mongodb.org/browse/SERVER-2054 "Creating an index
            # with dropDups shouldn't assert". On Mongo < 1.9.2, the duplicate
            # is dropped & the index created, but an error is thrown.
            def test_create():
                db.test.create_index(
                    [('i', ASCENDING)],
                    unique=True,
                    drop_dups=True
                )
            self.assertRaises(DuplicateKeyError, test_create)

        # Duplicate was dropped
        self.assertEqual(3, db.test.count())

        # Index was created, plus the index on _id
        self.assertEqual(2, len(db.test.index_information()))

    def test_index_dont_drop_dups(self):
        # Try *not* dropping duplicates
        db = self.db
        self._drop_dups_setup(db)

        # There's a duplicate
        def test_create():
            db.test.create_index(
                [('i', ASCENDING)],
                unique=True,
                drop_dups=False
            )
        self.assertRaises(DuplicateKeyError, test_create)

        # Duplicate wasn't dropped
        self.assertEqual(4, db.test.count())

        # Index wasn't created, only the default index on _id
        self.assertEqual(1, len(db.test.index_information()))

    def test_field_selection(self):
        db = self.db
        db.drop_collection("test")

        doc = {"a": 1, "b": 5, "c": {"d": 5, "e": 10}}
        db.test.insert(doc)

        # Test field inclusion
        doc = next(db.test.find({}, ["_id"]))
        self.assertEqual(list(doc), ["_id"])
        doc = next(db.test.find({}, ["a"]))
        l = list(doc)
        l.sort()
        self.assertEqual(l, ["_id", "a"])
        doc = next(db.test.find({}, ["b"]))
        l = list(doc)
        l.sort()
        self.assertEqual(l, ["_id", "b"])
        doc = next(db.test.find({}, ["c"]))
        l = list(doc)
        l.sort()
        self.assertEqual(l, ["_id", "c"])
        doc = next(db.test.find({}, ["a"]))
        self.assertEqual(doc["a"], 1)
        doc = next(db.test.find({}, ["b"]))
        self.assertEqual(doc["b"], 5)
        doc = next(db.test.find({}, ["c"]))
        self.assertEqual(doc["c"], {"d": 5, "e": 10})

        # Test inclusion of fields with dots
        doc = next(db.test.find({}, ["c.d"]))
        self.assertEqual(doc["c"], {"d": 5})
        doc = next(db.test.find({}, ["c.e"]))
        self.assertEqual(doc["c"], {"e": 10})
        doc = next(db.test.find({}, ["b", "c.e"]))
        self.assertEqual(doc["c"], {"e": 10})

        doc = next(db.test.find({}, ["b", "c.e"]))
        l = list(doc)
        l.sort()
        self.assertEqual(l, ["_id", "b", "c"])
        doc = next(db.test.find({}, ["b", "c.e"]))
        self.assertEqual(doc["b"], 5)

        # Test field exclusion
        doc = next(db.test.find({}, {"a": False, "b": 0}))
        l = list(doc)
        l.sort()
        self.assertEqual(l, ["_id", "c"])

        doc = next(db.test.find({}, {"_id": False}))
        l = list(doc)
        self.assertFalse("_id" in l)

    def test_options(self):
        db = self.db
        db.drop_collection("test")
        if client_context.version.at_least(1, 9):
            db.create_collection("test", capped=True, size=4096)
            result = db.test.options()
            # mongos 2.2.x adds an $auth field when auth is enabled.
            result.pop('$auth', None)
            self.assertEqual(result, {"capped": True, 'size': 4096})
        else:
            db.create_collection("test", capped=True)
            self.assertEqual(db.test.options(), {"capped": True})
        db.drop_collection("test")

    def test_insert_find_one(self):
        db = self.db
        db.test.remove({})
        self.assertEqual(0, len(list(db.test.find())))
        doc = {"hello": u("world")}
        id = db.test.insert(doc)
        self.assertEqual(1, len(list(db.test.find())))
        self.assertEqual(doc, db.test.find_one())
        self.assertEqual(doc["_id"], id)
        self.assertTrue(isinstance(id, ObjectId))

        doc_class = dict
        # Work around http://bugs.jython.org/issue1728
        if (sys.platform.startswith('java') and
            sys.version_info[:3] >= (2, 5, 2)):
            doc_class = SON

        db = self.client.get_database(
            db.name, codec_options=CodecOptions(as_class=doc_class))

        def remove_insert_find_one(doc):
            db.test.remove({})
            db.test.insert(doc)
            # SON equality is order sensitive.
            return db.test.find_one() == doc.to_dict()

        qcheck.check_unittest(self, remove_insert_find_one,
                              qcheck.gen_mongo_dict(3))

    def test_generator_insert(self):
        db = self.db
        db.test.remove({})
        self.assertEqual(db.test.find().count(), 0)
        db.test.insert(({'a': i} for i in range(5)), manipulate=False)
        self.assertEqual(5, db.test.count())
        db.test.remove({})

        db.test.insert(({'a': i} for i in range(5)), manipulate=True)
        self.assertEqual(5, db.test.count())
        db.test.remove({})

    def test_remove_one(self):
        self.db.test.remove()
        self.assertEqual(0, self.db.test.count())

        self.db.test.insert({"x": 1})
        self.db.test.insert({"y": 1})
        self.db.test.insert({"z": 1})
        self.assertEqual(3, self.db.test.count())

        self.db.test.remove(multi=False)
        self.assertEqual(2, self.db.test.count())
        self.db.test.remove()
        self.assertEqual(0, self.db.test.count())

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
        doc = next(db.test.find({}))
        self.assertTrue("x" in doc)
        doc = next(db.test.find({}))
        self.assertTrue("mike" in doc)
        doc = next(db.test.find({}))
        self.assertTrue("extra thing" in doc)
        doc = next(db.test.find({}, ["x", "mike"]))
        self.assertTrue("x" in doc)
        doc = next(db.test.find({}, ["x", "mike"]))
        self.assertTrue("mike" in doc)
        doc = next(db.test.find({}, ["x", "mike"]))
        self.assertFalse("extra thing" in doc)
        doc = next(db.test.find({}, ["mike"]))
        self.assertFalse("x" in doc)
        doc = next(db.test.find({}, ["mike"]))
        self.assertTrue("mike" in doc)
        doc = next(db.test.find({}, ["mike"]))
        self.assertFalse("extra thing" in doc)

    def test_fields_specifier_as_dict(self):
        db = self.db
        db.test.remove({})

        db.test.insert({"x": [1, 2, 3], "mike": "awesome"})

        self.assertEqual([1, 2, 3], db.test.find_one()["x"])
        if client_context.version.at_least(1, 5, 1):
            self.assertEqual([2, 3],
                             db.test.find_one(
                                 projection={"x": {"$slice": -2}})["x"])
        self.assertTrue("x" not in db.test.find_one(projection={"x": 0}))
        self.assertTrue("mike" in db.test.find_one(projection={"x": 0}))

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
        self.assertTrue(isinstance(auto_id["_id"], ObjectId))

        numeric = {"_id": 240, "hello": "world"}
        db.test.insert(numeric)
        self.assertEqual(numeric["_id"], 240)

        object = {"_id": numeric, "hello": "world"}
        db.test.insert(object)
        self.assertEqual(object["_id"], numeric)

        for x in db.test.find():
            self.assertEqual(x["hello"], u("world"))
            self.assertTrue("_id" in x)

    def test_invalid_key_names(self):
        db = self.db
        db.test.drop()

        db.test.insert({"hello": "world"})
        db.test.insert({"hello": {"hello": "world"}})

        self.assertRaises(InvalidDocument, db.test.insert, {"$hello": "world"})
        self.assertRaises(InvalidDocument, db.test.insert,
                          {"hello": {"$hello": "world"}})

        db.test.insert({"he$llo": "world"})
        db.test.insert({"hello": {"hello$": "world"}})

        self.assertRaises(InvalidDocument, db.test.insert,
                          {".hello": "world"})
        self.assertRaises(InvalidDocument, db.test.insert,
                          {"hello": {".hello": "world"}})
        self.assertRaises(InvalidDocument, db.test.insert,
                          {"hello.": "world"})
        self.assertRaises(InvalidDocument, db.test.insert,
                          {"hello": {"hello.": "world"}})
        self.assertRaises(InvalidDocument, db.test.insert,
                          {"hel.lo": "world"})
        self.assertRaises(InvalidDocument, db.test.insert,
                          {"hello": {"hel.lo": "world"}})

    def test_insert_multiple(self):
        db = self.db
        db.drop_collection("test")
        doc1 = {"hello": u("world")}
        doc2 = {"hello": u("mike")}
        self.assertEqual(db.test.find().count(), 0)
        ids = db.test.insert([doc1, doc2])
        self.assertEqual(db.test.find().count(), 2)
        self.assertEqual(doc1, db.test.find_one({"hello": u("world")}))
        self.assertEqual(doc2, db.test.find_one({"hello": u("mike")}))

        self.assertEqual(2, len(ids))
        self.assertEqual(doc1["_id"], ids[0])
        self.assertEqual(doc2["_id"], ids[1])

        id = db.test.insert([{"hello": 1}])
        self.assertTrue(isinstance(id, list))
        self.assertEqual(1, len(id))

        self.assertRaises(InvalidOperation, db.test.insert, [])

        # Generator that raises StopIteration on first call to next().
        self.assertRaises(InvalidOperation, db.test.insert, (i for i in []))

    def test_insert_multiple_with_duplicate(self):
        db = self.db
        db.drop_collection("test")
        db.test.ensure_index([('i', ASCENDING)], unique=True)

        # No error
        db.test.insert([{'i': i} for i in range(5, 10)], w=0)
        wait_until(lambda: 5 == db.test.count(), 'insert 5 documents')

        db.test.remove()

        # No error
        db.test.insert([{'i': 1}] * 2, w=0)
        wait_until(lambda: 1 == db.test.count(), 'insert 1 document')

        self.assertRaises(
            DuplicateKeyError,
            lambda: db.test.insert([{'i': 2}] * 2),
        )

        db.drop_collection("test")
        db = self.client.get_database(
            db.name, write_concern=WriteConcern(w=0))

        db.test.ensure_index([('i', ASCENDING)], unique=True)

        # No error.
        db.test.insert([{'i': 1}] * 2)
        wait_until(lambda: 1 == db.test.count(), 'insert 1 document')

        # Implied acknowledged.
        self.assertRaises(
            DuplicateKeyError,
            lambda: db.test.insert([{'i': 2}] * 2, fsync=True),
        )

        # Explicit acknowledged.
        self.assertRaises(
            DuplicateKeyError,
            lambda: db.test.insert([{'i': 2}] * 2, w=1),
        )

    def test_insert_iterables(self):
        db = self.db

        self.assertRaises(TypeError, db.test.insert, 4)
        self.assertRaises(TypeError, db.test.insert, None)
        self.assertRaises(TypeError, db.test.insert, True)

        db.drop_collection("test")
        self.assertEqual(db.test.find().count(), 0)
        ids = db.test.insert(({"hello": u("world")}, {"hello": u("world")}))
        self.assertEqual(db.test.find().count(), 2)

        db.drop_collection("test")
        self.assertEqual(db.test.find().count(), 0)
        ids = db.test.insert(map(lambda x: {"hello": "world"},
                                 itertools.repeat(None, 10)))
        self.assertEqual(db.test.find().count(), 10)

    @client_context.require_version_min(2, 0)
    def test_insert_manipulate_false(self):
        # Test three aspects of insert with manipulate=False:
        #   1. The return value is None or [None] as appropriate.
        #   2. _id is not set on the passed-in document object.
        #   3. _id is not sent to server.
        collection = self.db.test_insert_manipulate_false
        collection.drop()
        oid = ObjectId()
        doc = {'a': oid}

        # The return value is None.
        self.assertTrue(collection.insert(doc, manipulate=False) is None)
        # insert() shouldn't set _id on the passed-in document object.
        self.assertEqual({'a': oid}, doc)
        server_doc = collection.find_one()

        # _id is not sent to server, so it's generated server-side.
        self.assertFalse(oid_generated_on_client(server_doc['_id']))

        # Bulk insert. The return value is a list of None.
        self.assertEqual([None], collection.insert([{}], manipulate=False))

        ids = collection.insert([{}, {}], manipulate=False)
        self.assertEqual([None, None], ids)
        collection.drop()

    def test_save(self):
        self.db.drop_collection("test")

        # Save a doc with autogenerated id
        id = self.db.test.save({"hello": "world"})
        self.assertEqual(self.db.test.find_one()["_id"], id)
        self.assertTrue(isinstance(id, ObjectId))

        # Save a doc with explicit id
        self.db.test.save({"_id": "explicit_id", "hello": "bar"})
        doc = self.db.test.find_one({"_id": "explicit_id"})
        self.assertEqual(doc['_id'], 'explicit_id')
        self.assertEqual(doc['hello'], 'bar')

        # Save docs with _id field already present (shouldn't create new docs)
        self.assertEqual(2, self.db.test.count())
        self.db.test.save({'_id': id, 'hello': 'world'})
        self.assertEqual(2, self.db.test.count())
        self.db.test.save({'_id': 'explicit_id', 'hello': 'baz'})
        self.assertEqual(2, self.db.test.count())
        self.assertEqual(
            'baz',
            self.db.test.find_one({'_id': 'explicit_id'})['hello']
        )

        # Acknowledged mode.
        self.db.test.create_index("hello", unique=True)
        # No exception, even though we duplicate the first doc's "hello" value
        self.db.test.save({'_id': 'explicit_id', 'hello': 'world'}, w=0)

        self.assertRaises(
            DuplicateKeyError,
            self.db.test.save,
            {'_id': 'explicit_id', 'hello': 'world'})

    def test_save_with_invalid_key(self):
        self.db.drop_collection("test")
        self.assertTrue(self.db.test.insert({"hello": "world"}))
        doc = self.db.test.find_one()
        doc['a.b'] = 'c'
        expected = InvalidDocument
        if client_context.version.at_least(2, 5, 4, -1):
            expected = OperationFailure
        self.assertRaises(expected, self.db.test.save, doc)

    def test_unique_index(self):
        db = self.db
        db.drop_collection("test")
        db.test.create_index("hello")

        # No error.
        db.test.save({"hello": "world"})
        db.test.save({"hello": "world"})

        db.drop_collection("test")
        db.test.create_index("hello", unique=True)

        with self.assertRaises(DuplicateKeyError):
            db.test.save({"hello": "world"})
            db.test.save({"hello": "world"})

    def test_duplicate_key_error(self):
        db = self.db
        db.drop_collection("test")

        db.test.create_index("x", unique=True)

        db.test.insert({"_id": 1, "x": 1})

        with self.assertRaises(DuplicateKeyError) as context:
            db.test.insert({"x": 1})

        self.assertIsNotNone(context.exception.details)

        with self.assertRaises(DuplicateKeyError) as context:
            db.test.save({"x": 1})

        self.assertIsNotNone(context.exception.details)
        self.assertEqual(1, db.test.count())

    def test_wtimeout(self):
        # Ensure setting wtimeout doesn't disable write concern altogether.
        # See SERVER-12596.
        collection = self.db.test
        collection.remove()
        collection.insert({'_id': 1})

        coll = collection.with_options(
            write_concern=WriteConcern(w=1, wtimeout=1000))
        self.assertRaises(DuplicateKeyError, coll.insert, {'_id': 1})

        coll = collection.with_options(
            write_concern=WriteConcern(wtimeout=1000))
        self.assertRaises(DuplicateKeyError, coll.insert, {'_id': 1})

    @client_context.require_version_min(1, 9, 1)
    def test_continue_on_error(self):
        db = self.db
        db.drop_collection("test")
        oid = db.test.insert({"one": 1})
        self.assertEqual(1, db.test.count())

        docs = []
        docs.append({"_id": oid, "two": 2})  # Duplicate _id.
        docs.append({"three": 3})
        docs.append({"four": 4})
        docs.append({"five": 5})

        with self.assertRaises(DuplicateKeyError):
            db.test.insert(docs, manipulate=False)

        self.assertEqual(1, db.test.count())

        with self.assertRaises(DuplicateKeyError):
            db.test.insert(docs, manipulate=False, continue_on_error=True)

        self.assertEqual(4, db.test.count())

        db.drop_collection("test")
        oid = db.test.insert({"_id": oid, "one": 1}, w=0)
        self.assertEqual(1, db.test.count())
        docs[0].pop("_id")
        docs[2]["_id"] = oid

        with self.assertRaises(DuplicateKeyError):
            db.test.insert(docs, manipulate=False)

        self.assertEqual(3, db.test.count())
        db.test.insert(docs, manipulate=False, continue_on_error=True, w=0)

        wait_until(lambda: 6 == db.test.count(),
                   'insert 3 documents')

    def test_error_code(self):
        try:
            self.db.test.update({}, {"$thismodifierdoesntexist": 1})
        except OperationFailure as exc:
            if client_context.version.at_least(1, 3):
                self.assertTrue(exc.code in (9, 10147, 16840, 17009))
                # Just check that we set the error document. Fields
                # vary by MongoDB version.
                self.assertTrue(exc.details is not None)
        else:
            self.fail("OperationFailure was not raised")

    def test_index_on_subfield(self):
        db = self.db
        db.drop_collection("test")

        db.test.insert({"hello": {"a": 4, "b": 5}})
        db.test.insert({"hello": {"a": 7, "b": 2}})
        db.test.insert({"hello": {"a": 4, "b": 10}})

        db.drop_collection("test")
        db.test.create_index("hello.a", unique=True)

        db.test.insert({"hello": {"a": 4, "b": 5}})
        db.test.insert({"hello": {"a": 7, "b": 2}})
        self.assertRaises(DuplicateKeyError,
            db.test.insert, {"hello": {"a": 4, "b": 10}})

    def test_acknowledged_insert(self):
        db = self.db
        db.drop_collection("test")

        a = {"hello": "world"}
        db.test.insert(a)
        db.test.insert(a, w=0)
        self.assertRaises(OperationFailure, db.test.insert, a)

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

    def test_update_manipulate(self):
        db = self.db
        db.drop_collection("test")
        db.test.insert({'_id': 1})
        db.test.update({'_id': 1}, {'a': 1}, manipulate=True)
        self.assertEqual(
            {'_id': 1, 'a': 1},
            db.test.find_one())

        class AddField(SONManipulator):
            def transform_incoming(self, son, collection):
                son['field'] = 'value'
                return son

        db.add_son_manipulator(AddField())
        db.test.update({'_id': 1}, {'a': 2}, manipulate=False)
        self.assertEqual(
            {'_id': 1, 'a': 2},
            db.test.find_one())

        db.test.update({'_id': 1}, {'a': 3}, manipulate=True)
        self.assertEqual(
            {'_id': 1, 'a': 3, 'field': 'value'},
            db.test.find_one())

    def test_update_nmodified(self):
        db = self.db
        db.drop_collection("test")
        used_write_commands = (self.client.max_wire_version > 1)

        db.test.insert({'_id': 1})
        result = db.test.update({'_id': 1}, {'$set': {'x': 1}})
        if used_write_commands:
            self.assertEqual(1, result['nModified'])
        else:
            self.assertFalse('nModified' in result)

        # x is already 1.
        result = db.test.update({'_id': 1}, {'$set': {'x': 1}})
        if used_write_commands:
            self.assertEqual(0, result['nModified'])
        else:
            self.assertFalse('nModified' in result)

    @client_context.require_version_min(1, 1, 3, -1)
    def test_multi_update(self):
        db = self.db
        db.drop_collection("test")

        db.test.save({"x": 4, "y": 3})
        db.test.save({"x": 5, "y": 5})
        db.test.save({"x": 4, "y": 4})

        db.test.update({"x": 4}, {"$set": {"y": 5}}, multi=True)

        self.assertEqual(3, db.test.count())
        for doc in db.test.find():
            self.assertEqual(5, doc["y"])

        self.assertEqual(2, db.test.update({"x": 4}, {"$set": {"y": 6}},
                                           multi=True)["n"])

    def test_upsert(self):
        db = self.db
        db.drop_collection("test")

        db.test.update({"page": "/"}, {"$inc": {"count": 1}}, upsert=True)
        db.test.update({"page": "/"}, {"$inc": {"count": 1}}, upsert=True)

        self.assertEqual(1, db.test.count())
        self.assertEqual(2, db.test.find_one()["count"])

    def test_acknowledged_update(self):
        db = self.db
        db.drop_collection("test")
        db.test.create_index("x", unique=True)

        db.test.insert({"x": 5})
        id = db.test.insert({"x": 4})

        self.assertEqual(
            None, db.test.update({"_id": id}, {"$inc": {"x": 1}}, w=0))

        self.assertRaises(DuplicateKeyError, db.test.update,
                          {"_id": id}, {"$inc": {"x": 1}})

        self.assertEqual(1, db.test.update({"_id": id},
                                           {"$inc": {"x": 2}})["n"])

        self.assertEqual(0, db.test.update({"_id": "foo"},
                                           {"$inc": {"x": 2}})["n"])

    def test_update_with_invalid_keys(self):
        self.db.drop_collection("test")
        self.assertTrue(self.db.test.insert({"hello": "world"}))
        doc = self.db.test.find_one()
        doc['a.b'] = 'c'

        expected = InvalidDocument
        if client_context.version.at_least(2, 5, 4, -1):
            expected = OperationFailure

        # Replace
        self.assertRaises(expected,
                          self.db.test.update, {"hello": "world"}, doc)
        # Upsert
        self.assertRaises(expected,
                          self.db.test.update, {"foo": "bar"}, doc, upsert=True)

        # Check that the last two ops didn't actually modify anything
        self.assertTrue('a.b' not in self.db.test.find_one())

        # Modify shouldn't check keys...
        self.assertTrue(self.db.test.update({"hello": "world"},
                                            {"$set": {"foo.bar": "baz"}},
                                            upsert=True))

        # I know this seems like testing the server but I'd like to be notified
        # by CI if the server's behavior changes here.
        doc = SON([("$set", {"foo.bar": "bim"}), ("hello", "world")])
        self.assertRaises(OperationFailure, self.db.test.update,
                          {"hello": "world"}, doc, upsert=True)

        # This is going to cause keys to be checked and raise InvalidDocument.
        # That's OK assuming the server's behavior in the previous assert
        # doesn't change. If the behavior changes checking the first key for
        # '$' in update won't be good enough anymore.
        doc = SON([("hello", "world"), ("$set", {"foo.bar": "bim"})])
        self.assertRaises(expected, self.db.test.update,
                          {"hello": "world"}, doc, upsert=True)

        # Replace with empty document
        self.assertNotEqual(0, self.db.test.update({"hello": "world"},
                            {})['n'])

    def test_acknowledged_save(self):
        db = self.db
        db.drop_collection("test")
        db.test.create_index("hello", unique=True)

        db.test.save({"hello": "world"})
        db.test.save({"hello": "world"}, w=0)
        self.assertRaises(DuplicateKeyError, db.test.save,
                          {"hello": "world"})

    def test_acknowledged_remove(self):
        db = self.db
        db.drop_collection("test")
        db.create_collection("test", capped=True, size=1000)

        db.test.insert({"x": 1})
        self.assertEqual(1, db.test.count())

        # Can't remove from capped collection.
        self.assertRaises(OperationFailure, db.test.remove, {"x": 1})
        db.drop_collection("test")
        db.test.insert({"x": 1})
        db.test.insert({"x": 1})
        self.assertEqual(2, db.test.remove({})["n"])
        self.assertEqual(0, db.test.remove({})["n"])

    @client_context.require_version_min(1, 5, 1)
    def test_last_error_options(self):
        self.db.test.save({"x": 1}, w=1, wtimeout=1)
        self.db.test.insert({"x": 1}, w=1, wtimeout=1)
        self.db.test.remove({"x": 1}, w=1, wtimeout=1)
        self.db.test.update({"x": 1}, {"y": 2}, w=1, wtimeout=1)

        if client_context.replica_set_name:
            # client_context.w is the number of hosts in the replica set
            w = client_context.w + 1

            # MongoDB 2.8+ raises error code 100, CannotSatisfyWriteConcern,
            # if w > number of members. Older versions just time out after 1 ms
            # as if they had enough secondaries but some are lagging. They
            # return an error with 'wtimeout': True and no code.
            def wtimeout_err(f, *args, **kwargs):
                try:
                    f(*args, **kwargs)
                except WTimeoutError as exc:
                    self.assertIsNotNone(exc.details)
                except OperationFailure as exc:
                    self.assertIsNotNone(exc.details)
                    self.assertEqual(100, exc.code,
                                     "Unexpected error: %r" % exc)
                else:
                    self.fail("%s should have failed" % f)

            coll = self.db.test
            wtimeout_err(coll.save, {"x": 1}, w=w, wtimeout=1)
            wtimeout_err(coll.insert, {"x": 1}, w=w, wtimeout=1)
            wtimeout_err(coll.update, {"x": 1}, {"y": 2}, w=w, wtimeout=1)
            wtimeout_err(coll.remove, {"x": 1}, w=w, wtimeout=1)

        # can't use fsync and j options together
        if client_context.version.at_least(1, 8, 2):
            self.assertRaises(OperationFailure, self.db.test.insert,
                              {"_id": 1}, j=True, fsync=True)

    def test_manual_last_error(self):
        self.db.test.save({"x": 1}, w=0)
        self.db.command("getlasterror", w=1, wtimeout=1)

    def test_count(self):
        db = self.db
        db.drop_collection("test")

        self.assertEqual(db.test.count(), 0)
        db.test.save({})
        db.test.save({})
        self.assertEqual(db.test.count(), 2)
        db.test.save({'foo': 'bar'})
        db.test.save({'foo': 'baz'})
        self.assertEqual(db.test.find({'foo': 'bar'}).count(), 1)
        self.assertEqual(db.test.count({'foo': 'bar'}), 1)
        self.assertEqual(db.test.find({'foo': re.compile(r'ba.*')}).count(), 2)
        self.assertEqual(
            db.test.count({'foo': re.compile(r'ba.*')}), 2)

    @client_context.require_version_min(2, 1, 0)
    def test_aggregate(self):
        db = self.db
        db.drop_collection("test")
        db.test.save({'foo': [1, 2]})

        self.assertRaises(TypeError, db.test.aggregate, "wow")

        pipeline = {"$project": {"_id": False, "foo": True}}
        result = db.test.aggregate([pipeline], useCursor=False)

        self.assertTrue(isinstance(result, CommandCursor))
        self.assertEqual([{'foo': [1, 2]}], list(result))

    @client_context.require_version_min(2, 5, 1)
    def test_aggregation_cursor_validation(self):
        db = self.db
        projection = {'$project': {'_id': '$_id'}}
        cursor = db.test.aggregate([projection], cursor={})
        self.assertTrue(isinstance(cursor, CommandCursor))

        cursor = db.test.aggregate([projection], useCursor=True)
        self.assertTrue(isinstance(cursor, CommandCursor))

    @client_context.require_version_min(2, 5, 1)
    def test_aggregation_cursor(self):
        db = self.db
        if client_context.replica_set_name:
            # Test that getMore messages are sent to the right server.
            db = self.client.get_database(
                db.name, read_preference=ReadPreference.SECONDARY)

        for collection_size in (10, 1000):
            db.drop_collection("test")
            db.test.insert([{'_id': i} for i in range(collection_size)],
                           w=self.w)
            expected_sum = sum(range(collection_size))
            # Use batchSize to ensure multiple getMore messages
            cursor = db.test.aggregate(
                [{'$project': {'_id': '$_id'}}],
                batchSize=5)

            self.assertEqual(
                expected_sum,
                sum(doc['_id'] for doc in cursor))

    @client_context.require_version_min(2, 5, 5)
    @client_context.require_no_mongos
    def test_parallel_scan(self):
        db = self.db
        db.drop_collection("test")
        if client_context.replica_set_name:
            # Test that getMore messages are sent to the right server.
            db = self.client.get_database(
                db.name, read_preference=ReadPreference.SECONDARY)

        coll = db.test
        coll.insert(({'_id': i} for i in range(8000)), w=self.w)
        docs = []
        threads = [threading.Thread(target=docs.extend, args=(cursor,))
                   for cursor in coll.parallel_scan(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(
            set(range(8000)),
            set(doc['_id'] for doc in docs))

    def test_group(self):
        db = self.db
        db.drop_collection("test")

        self.assertEqual([],
                         db.test.group([], {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"
                                       ))

        db.test.save({"a": 2})
        db.test.save({"b": 5})
        db.test.save({"a": 1})

        self.assertEqual([{"count": 3}],
                         db.test.group([], {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"
                                      ))

        self.assertEqual([{"count": 1}],
                         db.test.group([], {"a": {"$gt": 1}}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"
                                      ))

        db.test.save({"a": 2, "b": 3})

        self.assertEqual([{"a": 2, "count": 2},
                          {"a": None, "count": 1},
                          {"a": 1, "count": 1}],
                         db.test.group(["a"], {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"
                                      ))

        # modifying finalize
        self.assertEqual([{"a": 2, "count": 3},
                          {"a": None, "count": 2},
                          {"a": 1, "count": 2}],
                         db.test.group(["a"], {}, {"count": 0},
                                       "function (obj, prev) "
                                       "{ prev.count++; }",
                                       "function (obj) { obj.count++; }"))

        # returning finalize
        self.assertEqual([2, 1, 1],
                         db.test.group(["a"], {}, {"count": 0},
                                       "function (obj, prev) "
                                       "{ prev.count++; }",
                                       "function (obj) { return obj.count; }"))

        # keyf
        self.assertEqual([2, 2],
                         db.test.group("function (obj) { if (obj.a == 2) "
                                       "{ return {a: true} }; "
                                       "return {b: true}; }", {}, {"count": 0},
                                       "function (obj, prev) "
                                       "{ prev.count++; }",
                                       "function (obj) { return obj.count; }"))

        # no key
        self.assertEqual([{"count": 4}],
                         db.test.group(None, {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"
                                      ))

        self.assertRaises(OperationFailure, db.test.group,
                          [], {}, {}, "5 ++ 5")

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

        self.assertEqual(1,
                         db.test.group([], {}, {"count": 0},
                                       Code(reduce_function,
                                            {"inc_value": 0.5}))[0]['count'])

        if client_context.version.at_least(1, 1):
            self.assertEqual(2, db.test.group([], {}, {"count": 0},
                                              Code(reduce_function,
                                                   {"inc_value": 1}),
                                             )[0]['count'])

            self.assertEqual(4, db.test.group([], {}, {"count": 0},
                                              Code(reduce_function,
                                                   {"inc_value": 2}),
                                             )[0]['count'])

            self.assertEqual(1, db.test.group([], {}, {"count": 0},
                                              Code(reduce_function,
                                                   {"inc_value": 0.5}),
                                             )[0]['count'])

    def test_large_limit(self):
        db = self.db
        db.drop_collection("test_large_limit")
        db.test_large_limit.create_index([('x', 1)])
        my_str = "mongomongo" * 1000

        for i in range(2000):
            doc = {"x": i, "y": my_str}
            db.test_large_limit.insert(doc)

        i = 0
        y = 0
        for doc in db.test_large_limit.find(limit=1900).sort([('x', 1)]):
            i += 1
            y += doc["x"]

        self.assertEqual(1900, i)
        self.assertEqual((1900 * 1899) / 2, y)

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

        db.test.insert({})
        self.assertRaises(OperationFailure, db.foo.rename, "test")
        db.foo.rename("test", dropTarget=True)

    def test_find_one(self):
        db = self.db
        db.drop_collection("test")

        id = db.test.save({"hello": "world", "foo": "bar"})

        self.assertEqual("world", db.test.find_one()["hello"])
        self.assertEqual(db.test.find_one(id), db.test.find_one())
        self.assertEqual(db.test.find_one(None), db.test.find_one())
        self.assertEqual(db.test.find_one({}), db.test.find_one())
        self.assertEqual(db.test.find_one({"hello": "world"}),
                                          db.test.find_one())

        self.assertTrue("hello" in db.test.find_one(projection=["hello"]))
        self.assertTrue("hello" not in db.test.find_one(projection=["foo"]))
        self.assertEqual(["_id"], list(db.test.find_one(projection=[])))

        self.assertEqual(None, db.test.find_one({"hello": "foo"}))
        self.assertEqual(None, db.test.find_one(ObjectId()))

    def test_find_one_non_objectid(self):
        db = self.db
        db.drop_collection("test")

        db.test.save({"_id": 5})

        self.assertTrue(db.test.find_one(5))
        self.assertFalse(db.test.find_one(6))

    def test_remove_non_objectid(self):
        db = self.db
        db.drop_collection("test")

        db.test.save({"_id": 5})

        self.assertEqual(1, db.test.count())
        db.test.remove(5)
        self.assertEqual(0, db.test.count())

    def test_find_one_with_find_args(self):
        db = self.db
        db.drop_collection("test")

        db.test.save({"x": 1})
        db.test.save({"x": 2})
        db.test.save({"x": 3})

        self.assertEqual(1, db.test.find_one()["x"])
        self.assertEqual(2, db.test.find_one(skip=1, limit=2)["x"])

    def test_find_with_sort(self):
        db = self.db
        db.drop_collection("test")

        db.test.save({"x": 2})
        db.test.save({"x": 1})
        db.test.save({"x": 3})

        self.assertEqual(2, db.test.find_one()["x"])
        self.assertEqual(1, db.test.find_one(sort=[("x", 1)])["x"])
        self.assertEqual(3, db.test.find_one(sort=[("x", -1)])["x"])

        def to_list(foo):
            return [bar["x"] for bar in foo]

        self.assertEqual([2, 1, 3], to_list(db.test.find()))
        self.assertEqual([1, 2, 3], to_list(db.test.find(sort=[("x", 1)])))
        self.assertEqual([3, 2, 1], to_list(db.test.find(sort=[("x", -1)])))

        self.assertRaises(TypeError, db.test.find, sort=5)
        self.assertRaises(TypeError, db.test.find, sort="hello")
        self.assertRaises(ValueError, db.test.find, sort=["hello", 1])

    def test_insert_adds_id(self):
        doc = {"hello": "world"}
        self.db.test.insert(doc)
        self.assertTrue("_id" in doc)

        docs = [{"hello": "world"}, {"hello": "world"}]
        self.db.test.insert(docs)
        for doc in docs:
            self.assertTrue("_id" in doc)

    def test_save_adds_id(self):
        doc = {"hello": "jesse"}
        self.db.test.save(doc)
        self.assertTrue("_id" in doc)

    # TODO doesn't actually test functionality, just that it doesn't blow up
    def test_cursor_timeout(self):
        list(self.db.test.find(no_cursor_timeout=True))
        list(self.db.test.find(no_cursor_timeout=False))

    def test_exhaust(self):
        if is_mongos(self.db.connection):
            self.assertRaises(InvalidOperation,
                              self.db.test.find, cursor_type=EXHAUST)
            return

        # Limit is incompatible with exhaust.
        self.assertRaises(InvalidOperation,
                          self.db.test.find, cursor_type=EXHAUST, limit=5)
        cur = self.db.test.find(cursor_type=EXHAUST)
        self.assertRaises(InvalidOperation, cur.limit, 5)
        cur = self.db.test.find(limit=5)
        self.assertRaises(InvalidOperation, cur.add_option, 64)
        cur = self.db.test.find()
        cur.add_option(64)
        self.assertRaises(InvalidOperation, cur.limit, 5)

        self.db.drop_collection("test")
        # Insert enough documents to require more than one batch
        self.db.test.insert([{'i': i} for i in range(150)])

        client = rs_or_single_client(max_pool_size=1)
        socks = get_pool(client).sockets

        # Make sure the socket is returned after exhaustion.
        cur = client[self.db.name].test.find(cursor_type=EXHAUST)
        next(cur)
        self.assertEqual(0, len(socks))
        for doc in cur:
            pass
        self.assertEqual(1, len(socks))

        # Same as previous but don't call next()
        for doc in client[self.db.name].test.find(cursor_type=EXHAUST):
            pass
        self.assertEqual(1, len(socks))

        # If the Cursor instance is discarded before being
        # completely iterated we have to close and
        # discard the socket.
        cur = client[self.db.name].test.find(cursor_type=EXHAUST)
        next(cur)
        self.assertEqual(0, len(socks))
        if sys.platform.startswith('java') or 'PyPy' in sys.version:
            # Don't wait for GC or use gc.collect(), it's unreliable.
            cur.close()
        cur = None
        # The socket should be discarded.
        self.assertEqual(0, len(socks))

    @client_context.require_version_min(1, 1)
    def test_distinct(self):
        self.db.drop_collection("test")

        test = self.db.test
        test.save({"a": 1})
        test.save({"a": 2})
        test.save({"a": 2})
        test.save({"a": 2})
        test.save({"a": 3})

        distinct = test.distinct("a")
        distinct.sort()

        self.assertEqual([1, 2, 3], distinct)

        distinct = test.find({'a': {'$gt': 1}}).distinct("a")
        distinct.sort()
        self.assertEqual([2, 3], distinct)

        distinct = test.distinct('a', {'a': {'$gt': 1}})
        distinct.sort()
        self.assertEqual([2, 3], distinct)

        self.db.drop_collection("test")

        test.save({"a": {"b": "a"}, "c": 12})
        test.save({"a": {"b": "b"}, "c": 12})
        test.save({"a": {"b": "c"}, "c": 12})
        test.save({"a": {"b": "c"}, "c": 12})

        distinct = test.distinct("a.b")
        distinct.sort()

        self.assertEqual(["a", "b", "c"], distinct)

    def test_query_on_query_field(self):
        self.db.drop_collection("test")
        self.db.test.save({"query": "foo"})
        self.db.test.save({"bar": "foo"})

        self.assertEqual(1,
                         self.db.test.find({"query": {"$ne": None}}).count())
        self.assertEqual(1,
                         len(list(self.db.test.find({"query": {"$ne": None}})))
                        )

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
        max_size = self.db.connection.max_bson_size
        half_size = int(max_size / 2)
        if client_context.version.at_least(1, 7, 4):
            self.assertEqual(max_size, 16777216)

        expected = DocumentTooLarge
        if client_context.version.at_least(2, 5, 4, -1):
            # Document too large handled by the server
            expected = OperationFailure
        self.assertRaises(expected, self.db.test.insert,
                          {"foo": "x" * max_size})
        self.assertRaises(expected, self.db.test.save,
                          {"foo": "x" * max_size})
        self.assertRaises(expected, self.db.test.insert,
                          [{"x": 1}, {"foo": "x" * max_size}])
        self.db.test.insert([{"foo": "x" * half_size},
                             {"foo": "x" * half_size}])

        self.db.test.insert({"bar": "x"})
        # Use w=0 here to test legacy doc size checking in all server versions
        self.assertRaises(DocumentTooLarge, self.db.test.update,
                          {"bar": "x"}, {"bar": "x" * (max_size - 14)}, w=0)
        # This will pass with OP_UPDATE or the update command.
        self.db.test.update({"bar": "x"}, {"bar": "x" * (max_size - 32)})

    def test_insert_large_batch(self):
        db = self.client.test_insert_large_batch
        self.addCleanup(self.client.drop_database, 'test_insert_large_batch')
        max_bson_size = self.client.max_bson_size
        if client_context.version.at_least(2, 5, 4, -1):
            # Write commands are limited to 16MB + 16k per batch
            big_string = 'x' * int(max_bson_size / 2)
        else:
            big_string = 'x' * (max_bson_size - 100)

        # Batch insert that requires 2 batches.
        successful_insert = [{'x': big_string}, {'x': big_string},
                             {'x': big_string}, {'x': big_string}]
        db.collection_0.insert(successful_insert, w=1)
        self.assertEqual(4, db.collection_0.count())

        # Test that inserts fail after first error.
        insert_second_fails = [{'_id': 'id0', 'x': big_string},
                               {'_id': 'id0', 'x': big_string},
                               {'_id': 'id1', 'x': big_string},
                               {'_id': 'id2', 'x': big_string}]

        with self.assertRaises(DuplicateKeyError):
            db.collection_1.insert(insert_second_fails)

        self.assertEqual(1, db.collection_1.count())

        # 2 batches, 2nd insert fails, don't continue on error.
        self.assertTrue(db.collection_2.insert(insert_second_fails, w=0))
        wait_until(lambda: 1 == db.collection_2.count(),
                   'insert 1 document')

        # 2 batches, ids of docs 0 and 1 are dupes, ids of docs 2 and 3 are
        # dupes. Acknowledged, continue on error.
        insert_two_failures = [{'_id': 'id0', 'x': big_string},
                               {'_id': 'id0', 'x': big_string},
                               {'_id': 'id1', 'x': big_string},
                               {'_id': 'id1', 'x': big_string}]

        with self.assertRaises(OperationFailure) as context:
            db.collection_3.insert(insert_two_failures,
                                   continue_on_error=True, w=1)

        self.assertIn('id1', str(context.exception))

        # Only the first and third documents should be inserted.
        self.assertEqual(2, db.collection_3.count())

        # 2 batches, 2 errors, unacknowledged, continue on error.
        db.collection_4.insert(insert_two_failures, continue_on_error=True, w=0)

        # Only the first and third documents are inserted.
        wait_until(lambda: 2 == db.collection_4.count(),
                   'insert 2 documents')

    def test_numerous_inserts(self):
        # Ensure we don't exceed server's 1000-document batch size limit.
        self.db.test.remove()
        n_docs = 2100
        self.db.test.insert({} for _ in range(n_docs))
        self.assertEqual(n_docs, self.db.test.count())
        self.db.test.remove()

    @client_context.require_version_min(1, 1, 1)
    def test_map_reduce(self):
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
        result = db.test.map_reduce(map, reduce, out='mrunittests')
        self.assertEqual(3, result.find_one({"_id": "cat"})["value"])
        self.assertEqual(2, result.find_one({"_id": "dog"})["value"])
        self.assertEqual(1, result.find_one({"_id": "mouse"})["value"])

        if client_context.version.at_least(1, 7, 4):
            db.test.insert({"id": 5, "tags": ["hampster"]})
            result = db.test.map_reduce(map, reduce, out='mrunittests')
            self.assertEqual(1, result.find_one({"_id": "hampster"})["value"])
            db.test.remove({"id": 5})

            result = db.test.map_reduce(map, reduce,
                                        out={'merge': 'mrunittests'})
            self.assertEqual(3, result.find_one({"_id": "cat"})["value"])
            self.assertEqual(1, result.find_one({"_id": "hampster"})["value"])

            result = db.test.map_reduce(map, reduce,
                                        out={'reduce': 'mrunittests'})

            self.assertEqual(6, result.find_one({"_id": "cat"})["value"])
            self.assertEqual(4, result.find_one({"_id": "dog"})["value"])
            self.assertEqual(2, result.find_one({"_id": "mouse"})["value"])
            self.assertEqual(1, result.find_one({"_id": "hampster"})["value"])

            result = db.test.map_reduce(
                map,
                reduce,
                out={'replace': 'mrunittests'}
            )
            self.assertEqual(3, result.find_one({"_id": "cat"})["value"])
            self.assertEqual(2, result.find_one({"_id": "dog"})["value"])
            self.assertEqual(1, result.find_one({"_id": "mouse"})["value"])

            if (client_context.is_mongos and
                    not client_context.version.at_least(2, 1, 2)):
                pass
            else:
                result = db.test.map_reduce(map, reduce,
                                            out=SON([('replace', 'mrunittests'),
                                                     ('db', 'mrtestdb')
                                                    ]))
                self.assertEqual(3, result.find_one({"_id": "cat"})["value"])
                self.assertEqual(2, result.find_one({"_id": "dog"})["value"])
                self.assertEqual(1, result.find_one({"_id": "mouse"})["value"])
                self.client.drop_database('mrtestdb')

        full_result = db.test.map_reduce(map, reduce,
                                         out='mrunittests', full_response=True)
        self.assertEqual(6, full_result["counts"]["emit"])

        result = db.test.map_reduce(map, reduce, out='mrunittests', limit=2)
        self.assertEqual(2, result.find_one({"_id": "cat"})["value"])
        self.assertEqual(1, result.find_one({"_id": "dog"})["value"])
        self.assertEqual(None, result.find_one({"_id": "mouse"}))

        if client_context.version.at_least(1, 7, 4):
            result = db.test.map_reduce(map, reduce, out={'inline': 1})
            self.assertTrue(isinstance(result, dict))
            self.assertTrue('results' in result)
            self.assertTrue(result['results'][1]["_id"] in ("cat",
                                                            "dog",
                                                            "mouse"))

            result = db.test.inline_map_reduce(map, reduce)
            self.assertTrue(isinstance(result, list))
            self.assertEqual(3, len(result))
            self.assertTrue(result[1]["_id"] in ("cat", "dog", "mouse"))

            full_result = db.test.inline_map_reduce(map, reduce,
                                                    full_response=True)
            self.assertEqual(6, full_result["counts"]["emit"])

    def test_messages_with_unicode_collection_names(self):
        db = self.db

        db[u("Employés")].insert({"x": 1})
        db[u("Employés")].update({"x": 1}, {"x": 2})
        db[u("Employés")].remove({})
        db[u("Employés")].find_one()
        list(db[u("Employés")].find())

    def test_drop_indexes_non_existent(self):
        self.db.drop_collection("test")
        self.db.test.drop_indexes()

    # This is really a bson test but easier to just reproduce it here...
    # (Shame on me)
    def test_bad_encode(self):
        c = self.db.test
        c.drop()
        self.assertRaises(InvalidDocument, c.save, {"x": c})

        class BadGetAttr(dict):
            def __getattr__(self, name):
                pass

        bad = BadGetAttr([('foo', 'bar')])
        c.insert({'bad': bad})
        self.assertEqual('bar', c.find_one()['bad']['foo'])

    def test_bad_dbref(self):
        c = self.db.test
        c.drop()

        # Incomplete DBRefs.
        self.assertRaises(
            InvalidDocument,
            c.insert, {'ref': {'$ref': 'collection'}})

        self.assertRaises(
            InvalidDocument,
            c.insert, {'ref': {'$id': ObjectId()}})

        ref_only = {'ref': {'$ref': 'collection'}}
        id_only = {'ref': {'$id': ObjectId()}}

        # Starting with MongoDB 2.5.2 this is no longer possible
        # from insert, update, or findAndModify.
        if not client_context.version.at_least(2, 5, 2):
            # Force insert of ref without $id.
            c.insert(ref_only, check_keys=False)
            self.assertEqual(DBRef('collection', id=None),
                             c.find_one()['ref'])

            c.drop()

            # DBRef without $ref is decoded as normal subdocument.
            c.insert(id_only, check_keys=False)
            self.assertEqual(id_only, c.find_one())

    def test_find_and_modify(self):
        c = self.db.test
        c.drop()
        c.insert({'_id': 1, 'i': 1})

        # Test that we raise DuplicateKeyError when appropriate.
        # MongoDB doesn't have a code field for DuplicateKeyError
        # from commands before 2.2.
        if client_context.version.at_least(2, 2):
            c.ensure_index('i', unique=True)
            self.assertRaises(DuplicateKeyError,
                              c.find_and_modify, query={'i': 1, 'j': 1},
                              update={'$set': {'k': 1}}, upsert=True)
            c.drop_indexes()

        # Test correct findAndModify
        self.assertEqual({'_id': 1, 'i': 1},
                         c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}}))
        self.assertEqual({'_id': 1, 'i': 3},
                         c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                           new=True))

        self.assertEqual({'_id': 1, 'i': 3},
                         c.find_and_modify({'_id': 1}, remove=True))

        self.assertEqual(None, c.find_one({'_id': 1}))

        self.assertEqual(None,
                         c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}}))
        # The return value changed in 2.1.2. See SERVER-6226.
        if client_context.version.at_least(2, 1, 2):
            self.assertEqual(None, c.find_and_modify({'_id': 1},
                                                     {'$inc': {'i': 1}},
                                                     upsert=True))
        else:
            self.assertEqual({}, c.find_and_modify({'_id': 1},
                                                   {'$inc': {'i': 1}},
                                                   upsert=True))
        self.assertEqual({'_id': 1, 'i': 2},
                         c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                           upsert=True, new=True))

        self.assertEqual({'_id': 1, 'i': 2},
                         c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                           fields=['i']))
        self.assertEqual({'_id': 1, 'i': 4},
                         c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                           new=True, fields={'i': 1}))

        # Test with full_response=True
        # No lastErrorObject from mongos until 2.0
        if (not client_context.is_mongos and
                client_context.version.at_least(2, 0)):
            result = c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                               new=True, upsert=True,
                                               full_response=True,
                                               fields={'i': 1})
            self.assertEqual({'_id': 1, 'i': 5}, result["value"])
            self.assertEqual(True, result["lastErrorObject"]["updatedExisting"])

            result = c.find_and_modify({'_id': 2}, {'$inc': {'i': 1}},
                                               new=True, upsert=True,
                                               full_response=True,
                                               fields={'i': 1})
            self.assertEqual({'_id': 2, 'i': 1}, result["value"])
            self.assertEqual(False, result["lastErrorObject"]["updatedExisting"])

        class ExtendedDict(dict):
            pass

        result = c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                    new=True, fields={'i': 1})
        self.assertFalse(isinstance(result, ExtendedDict))
        c = self.db.get_collection(
            "test", codec_options=CodecOptions(as_class=ExtendedDict))
        result = c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                    new=True, fields={'i': 1})
        self.assertTrue(isinstance(result, ExtendedDict))

    def test_update_backward_compat(self):
        # MongoDB versions >= 2.6.0 don't return the updatedExisting field
        # and return upsert _id in an array subdocument. This test should
        # pass regardless of server version or type (mongod/s).
        c = self.db.test
        c.drop()
        oid = ObjectId()
        res = c.update({'_id': oid}, {'$set': {'a': 'a'}}, upsert=True)
        self.assertFalse(res.get('updatedExisting'))
        self.assertEqual(oid, res.get('upserted'))

        res = c.update({'_id': oid}, {'$set': {'b': 'b'}})
        self.assertTrue(res.get('updatedExisting'))

    def test_find_and_modify_with_sort(self):
        c = self.db.test
        c.drop()
        for j in range(5):
            c.insert({'j': j, 'i': 0})

        with ignore_deprecations():
            sort={'j': DESCENDING}
            self.assertEqual(4, c.find_and_modify({},
                                                  {'$inc': {'i': 1}},
                                                  sort=sort)['j'])
            sort={'j': ASCENDING}
            self.assertEqual(0, c.find_and_modify({},
                                                  {'$inc': {'i': 1}},
                                                  sort=sort)['j'])
            sort=[('j', DESCENDING)]
            self.assertEqual(4, c.find_and_modify({},
                                                  {'$inc': {'i': 1}},
                                                  sort=sort)['j'])
            sort=[('j', ASCENDING)]
            self.assertEqual(0, c.find_and_modify({},
                                                  {'$inc': {'i': 1}},
                                                  sort=sort)['j'])
            sort=SON([('j', DESCENDING)])
            self.assertEqual(4, c.find_and_modify({},
                                                  {'$inc': {'i': 1}},
                                                  sort=sort)['j'])
            sort=SON([('j', ASCENDING)])
            self.assertEqual(0, c.find_and_modify({},
                                                  {'$inc': {'i': 1}},
                                                  sort=sort)['j'])

            try:
                from collections import OrderedDict
                sort=OrderedDict([('j', DESCENDING)])
                self.assertEqual(4, c.find_and_modify({},
                                                      {'$inc': {'i': 1}},
                                                      sort=sort)['j'])
                sort=OrderedDict([('j', ASCENDING)])
                self.assertEqual(0, c.find_and_modify({},
                                                      {'$inc': {'i': 1}},
                                                      sort=sort)['j'])
            except ImportError:
                pass
            # Test that a standard dict with two keys is rejected.
            sort={'j': DESCENDING, 'foo': DESCENDING}
            self.assertRaises(TypeError, c.find_and_modify,
                              {}, {'$inc': {'i': 1}}, sort=sort)

    @client_context.require_version_min(2, 0, 0)
    def test_find_with_nested(self):
        c = self.db.test
        c.drop()
        c.insert([{'i': i} for i in range(5)])  # [0, 1, 2, 3, 4]
        self.assertEqual(
            [2],
            [i['i'] for i in c.find({
                '$and': [
                    {
                        # This clause gives us [1,2,4]
                        '$or': [
                            {'i': {'$lte': 2}},
                            {'i': {'$gt': 3}},
                        ],
                    },
                    {
                        # This clause gives us [2,3]
                        '$or': [
                            {'i': 2},
                            {'i': 3},
                        ]
                    },
                ]
            })]
        )

        self.assertEqual(
            [0, 1, 2],
            [i['i'] for i in c.find({
                '$or': [
                    {
                        # This clause gives us [2]
                        '$and': [
                            {'i': {'$gte': 2}},
                            {'i': {'$lt': 3}},
                        ],
                    },
                    {
                        # This clause gives us [0,1]
                        '$and': [
                            {'i': {'$gt': -100}},
                            {'i': {'$lt': 2}},
                        ]
                    },
                ]
            })]
        )

    def test_disabling_manipulators(self):

        class IncByTwo(SONManipulator):
            def transform_outgoing(self, son, collection):
                if 'foo' in son:
                    son['foo'] += 2
                return son

        db = self.client.pymongo_test
        db.add_son_manipulator(IncByTwo())
        c = db.test
        c.drop()
        c.insert({'foo': 0})
        self.assertEqual(2, c.find_one()['foo'])
        self.assertEqual(0, c.find_one(manipulate=False)['foo'])

        self.assertEqual(2, c.find_one(manipulate=True)['foo'])
        c.remove({})

    def test_find_regex(self):
        c = self.db.test
        c.drop()
        c.insert({'r': re.compile('.*')})

        self.assertTrue(isinstance(c.find_one()['r'], Regex))
        for doc in c.find():
            self.assertTrue(isinstance(doc['r'], Regex))

    def test_find_and_modify_with_manipulator(self):
        class AddCollectionNameManipulator(SONManipulator):
            def will_copy(self):
                return True

            def transform_incoming(self, son, collection):
                copy = SON(son)
                if 'collection' in copy:
                    del copy['collection']
                return copy

            def transform_outgoing(self, son, collection):
                copy = SON(son)
                copy['collection'] = collection.name
                return copy

        db = self.client.pymongo_test
        db.add_son_manipulator(AddCollectionNameManipulator())

        c = db.test
        c.drop()
        c.insert({'_id': 1, 'i': 1})

        # Test correct findAndModify
        # With manipulators
        self.assertEqual({'_id': 1, 'i': 1, 'collection': 'test'},
                         c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                           manipulate=True))
        self.assertEqual({'_id': 1, 'i': 3, 'collection': 'test'},
                         c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                           new=True, manipulate=True))
        # With out manipulators
        self.assertEqual({'_id': 1, 'i': 3},
                         c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}}))
        self.assertEqual({'_id': 1, 'i': 5},
                         c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                           new=True))


if __name__ == "__main__":
    unittest.main()
