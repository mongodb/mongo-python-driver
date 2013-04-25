# -*- coding: utf-8 -*-

# Copyright 2009-2012 10gen, Inc.
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
import unittest
import warnings

from nose.plugins.skip import SkipTest

sys.path[0:0] = [""]

from bson.binary import Binary, UUIDLegacy, OLD_UUID_SUBTYPE, UUID_SUBTYPE
from bson.code import Code
from bson.objectid import ObjectId
from bson.py3compat import b
from bson.son import SON
from pymongo import (ASCENDING, DESCENDING, GEO2D,
                     GEOHAYSTACK, GEOSPHERE, HASHED)
from pymongo.collection import Collection
from pymongo.son_manipulator import SONManipulator
from pymongo.errors import (ConfigurationError,
                            DuplicateKeyError,
                            InvalidDocument,
                            InvalidName,
                            InvalidOperation,
                            OperationFailure,
                            TimeoutError)
from test.test_client import get_client
from test.utils import is_mongos, joinall
from test import (qcheck,
                  version)

have_uuid = True
try:
    import uuid
except ImportError:
    have_uuid = False


class TestCollection(unittest.TestCase):

    def setUp(self):
        self.client = get_client()
        self.db = self.client.pymongo_test

    def tearDown(self):
        self.db.drop_collection("test_large_limit")
        self.db = None
        self.client = None

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

        self.db.drop_collection('test')
        self.assertFalse('test' in self.db.collection_names())

        # No exception
        self.db.drop_collection('test')

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
        self.assertEqual(ix, "hello_world")

        db.test.drop_indexes()
        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 1)
        db.test.create_index("hello")
        self.assertTrue(u"hello_1" in
                        [a["name"] for a in db.system.indexes
                         .find({"ns": u"pymongo_test.test"})])

        db.test.drop_indexes()
        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 1)
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])
        self.assertTrue(u"hello_-1_world_1" in
                        [a["name"] for a in db.system.indexes
                         .find({"ns": u"pymongo_test.test"})])

        db.test.drop()
        db.test.insert({'a': 1})
        db.test.insert({'a': 1})
        self.assertRaises(DuplicateKeyError, db.test.create_index,
                                                    'a', unique=True)

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

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.create_index("goodbye"))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye", cache_for=1))
        time.sleep(1.1)
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.create_index("goodbye", cache_for=1))
        time.sleep(1.1)
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))
        # Make sure the expiration time is updated.
        self.assertEqual(None,
                         db.test.ensure_index("goodbye"))

        # Clean up indexes for later tests
        db.test.drop_indexes()

    def test_deprecated_ttl_index_kwarg(self):
        db = self.db

        # In Python 2.6+ we could use the catch_warnings context
        # manager to test this warning nicely. As we can't do that
        # we must test raising errors before the ignore filter is applied.
        warnings.simplefilter("error", DeprecationWarning)
        self.assertRaises(DeprecationWarning, lambda:
                        db.test.ensure_index("goodbye", ttl=10))
        warnings.resetwarnings()
        warnings.simplefilter("ignore")

        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye", ttl=10))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

    def test_ensure_unique_index_threaded(self):
        coll = self.db.test_unique_threaded
        coll.drop()
        coll.insert(({'foo': i} for i in xrange(10000)))

        class Indexer(threading.Thread):
            def run(self):
                try:
                    coll.ensure_index('foo', unique=True)
                    coll.insert({'foo': 'bar'})
                    coll.insert({'foo': 'bar'})
                except OperationFailure:
                    pass

        threads = []
        for _ in xrange(10):
            t = Indexer()
            t.setDaemon(True)
            threads.append(t)

        for i in xrange(10):
            threads[i].start()

        joinall(threads)

        self.assertEqual(10001, coll.count())
        coll.drop()

    def test_index_on_binary(self):
        db = self.db
        db.drop_collection("test")
        db.test.save({"bin": Binary(b("def"))})
        db.test.save({"bin": Binary(b("abc"))})
        db.test.save({"bin": Binary(b("ghi"))})

        self.assertEqual(db.test.find({"bin": Binary(b("abc"))})
                         .explain()["nscanned"], 3)

        db.test.create_index("bin")
        self.assertEqual(db.test.find({"bin": Binary(b("abc"))})
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
        self.assertTrue(u"hello_1" in
                        [a["name"] for a in db.system.indexes
                         .find({"ns": u"pymongo_test.test"})])

        db.test.drop_indexes()
        db.test.create_index("hello")
        name = db.test.create_index("goodbye")

        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 3)
        self.assertEqual(name, "goodbye_1")
        db.test.drop_index([("goodbye", ASCENDING)])
        self.assertEqual(db.system.indexes.find({"ns": u"pymongo_test.test"})
                         .count(), 2)
        self.assertTrue(u"hello_1" in
                        [a["name"] for a in db.system.indexes
                         .find({"ns": u"pymongo_test.test"})])

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
            self.assertEqual(4, result['nIndexesWas'])
            indexes = result['indexes']
            names = [idx['name'] for idx in indexes]
            for name in names:
                self.assertTrue(name in info)
            for key in info:
                self.assertTrue(key in names)

        reindexed = db.test.reindex()
        if 'raw' in reindexed:
            # mongos
            for result in reindexed['raw'].itervalues():
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

    def test_index_haystack(self):
        if is_mongos(self.db.connection):
            raise SkipTest("geoSearch is not supported by mongos")
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

    def test_index_text(self):
        if not version.at_least(self.client, (2, 3, 2)):
            raise SkipTest("Text search requires server >=2.3.2.")

        if is_mongos(self.client):
            raise SkipTest("setParameter does not work through mongos")

        self.client.admin.command('setParameter', '*',
                                  textSearchEnabled=True)

        db = self.db
        db.test.drop_indexes()
        self.assertEqual("t_text", db.test.create_index([("t", "text")]))
        index_info = db.test.index_information()["t_text"]
        self.assertTrue("weights" in index_info)
        db.test.drop_indexes()

        self.client.admin.command('setParameter', '*',
                                  textSearchEnabled=False)

    def test_index_2dsphere(self):
        if not version.at_least(self.client, (2, 3, 2)):
            raise SkipTest("2dsphere indexing requires server >=2.3.2.")

        db = self.db
        db.test.drop_indexes()
        self.assertEqual("geo_2dsphere",
                         db.test.create_index([("geo", GEOSPHERE)]))

        poly = {"type": "Polygon",
                "coordinates": [[[40,5], [40,6], [41,6], [41,5], [40,5]]]}
        query = {"geo": {"$within": {"$geometry": poly}}}

        self.assertTrue(
            db.test.find(query).explain()['cursor'].startswith('S2Cursor'))

        db.test.drop_indexes()

    def test_index_hashed(self):
        if not version.at_least(self.client, (2, 3, 2)):
            raise SkipTest("hashed indexing requires server >=2.3.2.")

        db = self.db
        db.test.drop_indexes()
        self.assertEqual("a_hashed",
                         db.test.create_index([("a", HASHED)]))

        self.assertEqual("BtreeCursor a_hashed",
                db.test.find({'a': 1}).explain()['cursor'])
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

    def test_index_drop_dups(self):
        # Try dropping duplicates
        db = self.db
        self._drop_dups_setup(db)

        if version.at_least(db.connection, (1, 9, 2)):
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
        doc = db.test.find({}, ["_id"]).next()
        self.assertEqual(doc.keys(), ["_id"])
        doc = db.test.find({}, ["a"]).next()
        l = doc.keys()
        l.sort()
        self.assertEqual(l, ["_id", "a"])
        doc = db.test.find({}, ["b"]).next()
        l = doc.keys()
        l.sort()
        self.assertEqual(l, ["_id", "b"])
        doc = db.test.find({}, ["c"]).next()
        l = doc.keys()
        l.sort()
        self.assertEqual(l, ["_id", "c"])
        doc = db.test.find({}, ["a"]).next()
        self.assertEqual(doc["a"], 1)
        doc = db.test.find({}, ["b"]).next()
        self.assertEqual(doc["b"], 5)
        doc = db.test.find({}, ["c"]).next()
        self.assertEqual(doc["c"], {"d": 5, "e": 10})

        # Test inclusion of fields with dots
        doc = db.test.find({}, ["c.d"]).next()
        self.assertEqual(doc["c"], {"d": 5})
        doc = db.test.find({}, ["c.e"]).next()
        self.assertEqual(doc["c"], {"e": 10})
        doc = db.test.find({}, ["b", "c.e"]).next()
        self.assertEqual(doc["c"], {"e": 10})

        doc = db.test.find({}, ["b", "c.e"]).next()
        l = doc.keys()
        l.sort()
        self.assertEqual(l, ["_id", "b", "c"])
        doc = db.test.find({}, ["b", "c.e"]).next()
        self.assertEqual(doc["b"], 5)

        # Test field exclusion
        doc = db.test.find({}, {"a": False, "b": 0}).next()
        l = doc.keys()
        l.sort()
        self.assertEqual(l, ["_id", "c"])

        doc = db.test.find({}, {"_id": False}).next()
        l = doc.keys()
        self.assertFalse("_id" in l)

    def test_options(self):
        db = self.db
        db.drop_collection("test")
        db.test.save({})
        self.assertEqual(db.test.options(), {})
        self.assertEqual(db.test.doesnotexist.options(), {})

        db.drop_collection("test")
        if version.at_least(db.connection, (1, 9)):
            db.create_collection("test", capped=True, size=1000)
            self.assertEqual(db.test.options(), {"capped": True, 'size': 1000})
        else:
            db.create_collection("test", capped=True)
            self.assertEqual(db.test.options(), {"capped": True})
        db.drop_collection("test")

    def test_insert_find_one(self):
        db = self.db
        db.test.remove({})
        self.assertEqual(0, len(list(db.test.find())))
        doc = {"hello": u"world"}
        id = db.test.insert(doc)
        self.assertEqual(1, len(list(db.test.find())))
        self.assertEqual(doc, db.test.find_one())
        self.assertEqual(doc["_id"], id)
        self.assertTrue(isinstance(id, ObjectId))

        doc_class = None
        # Work around http://bugs.jython.org/issue1728
        if (sys.platform.startswith('java') and
            sys.version_info[:3] >= (2, 5, 2)):
            doc_class = SON

        def remove_insert_find_one(doc):
            db.test.remove({})
            db.test.insert(doc)
            # SON equality is order sensitive.
            return db.test.find_one(as_class=doc_class) == doc.to_dict()

        qcheck.check_unittest(self, remove_insert_find_one,
                              qcheck.gen_mongo_dict(3))

    def test_generator_insert(self):
        db = self.db
        db.test.remove({})
        self.assertEqual(db.test.find().count(), 0)
        db.test.insert(({'a': i} for i in xrange(5)), manipulate=False)
        self.assertEqual(5, db.test.count())
        db.test.remove({})

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
        doc = db.test.find({}).next()
        self.assertTrue("x" in doc)
        doc = db.test.find({}).next()
        self.assertTrue("mike" in doc)
        doc = db.test.find({}).next()
        self.assertTrue("extra thing" in doc)
        doc = db.test.find({}, ["x", "mike"]).next()
        self.assertTrue("x" in doc)
        doc = db.test.find({}, ["x", "mike"]).next()
        self.assertTrue("mike" in doc)
        doc = db.test.find({}, ["x", "mike"]).next()
        self.assertFalse("extra thing" in doc)
        doc = db.test.find({}, ["mike"]).next()
        self.assertFalse("x" in doc)
        doc = db.test.find({}, ["mike"]).next()
        self.assertTrue("mike" in doc)
        doc = db.test.find({}, ["mike"]).next()
        self.assertFalse("extra thing" in doc)

    def test_fields_specifier_as_dict(self):
        db = self.db
        db.test.remove({})

        db.test.insert({"x": [1, 2, 3], "mike": "awesome"})

        self.assertEqual([1, 2, 3], db.test.find_one()["x"])
        if version.at_least(db.connection, (1, 5, 1)):
            self.assertEqual([2, 3],
                             db.test.find_one(fields={"x": {"$slice":
                                                            -2}})["x"])
        self.assertTrue("x" not in db.test.find_one(fields={"x": 0}))
        self.assertTrue("mike" in db.test.find_one(fields={"x": 0}))

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
            self.assertEqual(x["hello"], u"world")
            self.assertTrue("_id" in x)

    def test_iteration(self):
        db = self.db

        def iterate():
            [a for a in db.test]

        self.assertRaises(TypeError, iterate)

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
        self.assertTrue(isinstance(id, list))
        self.assertEqual(1, len(id))

        self.assertRaises(InvalidOperation, db.test.insert, [])

    def test_insert_multiple_with_duplicate(self):
        db = self.db
        db.drop_collection("test")
        db.test.ensure_index([('i', ASCENDING)], unique=True)

        # No error
        db.test.insert([{'i': i} for i in range(5, 10)], w=0)
        db.test.remove()

        # No error
        db.test.insert([{'i': 1}] * 2, w=0)
        self.assertEqual(1, db.test.count())

        self.assertRaises(
            DuplicateKeyError,
            lambda: db.test.insert([{'i': 2}] * 2),
        )

        db.drop_collection("test")
        db.write_concern['w'] = 0
        db.test.ensure_index([('i', ASCENDING)], unique=True)

        # No error
        db.test.insert([{'i': 1}] * 2)
        self.assertEqual(1, db.test.count())

        # Implied safe
        self.assertRaises(
            DuplicateKeyError,
            lambda: db.test.insert([{'i': 2}] * 2, j=True),
        )

        # Explicit safe
        self.assertRaises(
            DuplicateKeyError,
            lambda: db.test.insert([{'i': 2}] * 2, w=1),
        )

        # Misconfigured value for safe
        self.assertRaises(
            TypeError,
            lambda: db.test.insert([{'i': 2}] * 2, safe=1),
        )

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
        ids = db.test.insert(itertools.imap(lambda x: {"hello": "world"},
                                            itertools.repeat(None, 10)))
        self.assertEqual(db.test.find().count(), 10)

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

        # Safe mode
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
        self.assertRaises(InvalidDocument, self.db.test.save, doc)

    def test_unique_index(self):
        db = self.db

        db.drop_collection("test")
        db.test.create_index("hello")

        db.test.save({"hello": "world"})
        db.test.save({"hello": "mike"})
        db.test.save({"hello": "world"})
        self.assertFalse(db.error())

        db.drop_collection("test")
        db.test.create_index("hello", unique=True)

        db.test.save({"hello": "world"})
        db.test.save({"hello": "mike"})
        db.test.save({"hello": "world"}, w=0)
        self.assertTrue(db.error())

    def test_duplicate_key_error(self):
        db = self.db
        db.drop_collection("test")

        db.test.create_index("x", unique=True)

        db.test.insert({"_id": 1, "x": 1})
        db.test.insert({"_id": 2, "x": 2})

        # No error
        db.test.insert({"_id": 1, "x": 1}, safe=False)
        db.test.save({"_id": 1, "x": 1}, safe=False)
        db.test.insert({"_id": 2, "x": 2}, safe=False)
        db.test.save({"_id": 2, "x": 2}, safe=False)
        db.test.insert({"_id": 1, "x": 1}, w=0)
        db.test.save({"_id": 1, "x": 1}, w=0)
        db.test.insert({"_id": 2, "x": 2}, w=0)
        db.test.save({"_id": 2, "x": 2}, w=0)

        # But all those statements didn't do anything
        self.assertEqual(2, db.test.count())

        expected_error = OperationFailure
        if version.at_least(db.connection, (1, 3)):
            expected_error = DuplicateKeyError

        self.assertRaises(expected_error,
                          db.test.insert, {"_id": 1})
        self.assertRaises(expected_error,
                          db.test.insert, {"x": 1})

        self.assertRaises(expected_error,
                          db.test.save, {"x": 2})
        self.assertRaises(expected_error,
                          db.test.update, {"x": 1},
                          {"$inc": {"x": 1}})

    def test_continue_on_error(self):
        db = self.db
        if not version.at_least(db.connection, (1, 9, 1)):
            raise SkipTest("continue_on_error requires MongoDB >= 1.9.1")

        db.drop_collection("test")
        oid = db.test.insert({"one": 1})
        self.assertEqual(1, db.test.count())

        docs = []
        docs.append({"_id": oid, "two": 2})
        docs.append({"three": 3})
        docs.append({"four": 4})
        docs.append({"five": 5})

        db.test.insert(docs, manipulate=False, w=0)
        self.assertEqual(11000, db.error()['code'])
        self.assertEqual(1, db.test.count())

        db.test.insert(docs, manipulate=False, continue_on_error=True, w=0)
        self.assertEqual(11000, db.error()['code'])
        self.assertEqual(4, db.test.count())

        db.drop_collection("test")
        oid = db.test.insert({"_id": oid, "one": 1}, w=0)
        self.assertEqual(1, db.test.count())
        docs[0].pop("_id")
        docs[2]["_id"] = oid

        db.test.insert(docs, manipulate=False, w=0)
        self.assertEqual(11000, db.error()['code'])
        self.assertEqual(3, db.test.count())

        db.test.insert(docs, manipulate=False, continue_on_error=True, w=0)
        self.assertEqual(11000, db.error()['code'])
        self.assertEqual(6, db.test.count())

    def test_error_code(self):
        try:
            self.db.test.update({}, {"$thismodifierdoesntexist": 1})
            self.fail()
        except OperationFailure, e:
            if version.at_least(self.db.connection, (1, 3)):
                self.assertEqual(10147, e.code)

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

    def test_safe_insert(self):
        db = self.db
        db.drop_collection("test")

        a = {"hello": "world"}
        db.test.insert(a)
        db.test.insert(a, w=0)
        self.assertTrue("E11000" in db.error()["err"])

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

    def test_multi_update(self):
        db = self.db
        if not version.at_least(db.connection, (1, 1, 3, -1)):
            raise SkipTest("multi-update requires MongoDB >= 1.1.3")

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

    def test_safe_update(self):
        db = self.db
        v113minus = version.at_least(db.connection, (1, 1, 3, -1))
        v19 = version.at_least(db.connection, (1, 9))

        db.drop_collection("test")
        db.test.create_index("x", unique=True)

        db.test.insert({"x": 5})
        id = db.test.insert({"x": 4})

        self.assertEqual(
            None, db.test.update({"_id": id}, {"$inc": {"x": 1}}, w=0))

        if v19:
            self.assertTrue("E11000" in db.error()["err"])
        elif v113minus:
            self.assertTrue(db.error()["err"].startswith("E11001"))
        else:
            self.assertTrue(db.error()["err"].startswith("E12011"))

        self.assertRaises(OperationFailure, db.test.update,
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

        # Replace
        self.assertRaises(InvalidDocument,
                          self.db.test.update, {"hello": "world"}, doc)
        # Upsert
        self.assertRaises(InvalidDocument,
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
        self.assertRaises(InvalidDocument, self.db.test.update,
                          {"hello": "world"}, doc, upsert=True)

        # Replace with empty document
        self.assertNotEqual(0, self.db.test.update({"hello": "world"},
                            {})['n'])

    def test_safe_save(self):
        db = self.db
        db.drop_collection("test")
        db.test.create_index("hello", unique=True)

        db.test.save({"hello": "world"})
        db.test.save({"hello": "world"}, w=0)
        self.assertTrue("E11000" in db.error()["err"])

        self.assertRaises(OperationFailure, db.test.save,
                          {"hello": "world"})

    def test_safe_remove(self):
        db = self.db
        db.drop_collection("test")
        db.create_collection("test", capped=True, size=1000)

        db.test.insert({"x": 1})
        self.assertEqual(1, db.test.count())

        self.assertEqual(None, db.test.remove({"x": 1}, w=0))
        self.assertEqual(1, db.test.count())

        if version.at_least(db.connection, (1, 1, 3, -1)):
            self.assertRaises(OperationFailure, db.test.remove,
                              {"x": 1})
        else:  # Just test that it doesn't blow up
            db.test.remove({"x": 1})

        db.drop_collection("test")
        db.test.insert({"x": 1})
        db.test.insert({"x": 1})
        self.assertEqual(2, db.test.remove({})["n"])
        self.assertEqual(0, db.test.remove({})["n"])

    def test_last_error_options(self):
        if not version.at_least(self.client, (1, 5, 1)):
            raise SkipTest("getLastError options require MongoDB >= 1.5.1")

        # XXX: Fix this if we ever have a replica set unittest env.
        # mongo >=1.7.6 errors with 'norepl' when w=2+
        # and we aren't replicated.
        if not version.at_least(self.client, (1, 7, 6)):
            self.assertRaises(TimeoutError, self.db.test.save,
                              {"x": 1}, w=2, wtimeout=1)
            self.assertRaises(TimeoutError, self.db.test.insert,
                              {"x": 1}, w=2, wtimeout=1)
            self.assertRaises(TimeoutError, self.db.test.update,
                              {"x": 1}, {"y": 2}, w=2, wtimeout=1)
            self.assertRaises(TimeoutError, self.db.test.remove,
                              {"x": 1}, w=2, wtimeout=1)

        self.db.test.save({"x": 1}, w=1, wtimeout=1)
        self.db.test.insert({"x": 1}, w=1, wtimeout=1)
        self.db.test.remove({"x": 1}, w=1, wtimeout=1)
        self.db.test.update({"x": 1}, {"y": 2}, w=1, wtimeout=1)

    def test_manual_last_error(self):
        self.db.test.save({"x": 1}, w=0)
        # XXX: Fix this if we ever have a replica set unittest env.
        # mongo >=1.7.6 errors with 'norepl' when w=2+
        # and we aren't replicated
        if not version.at_least(self.client, (1, 7, 6)):
            self.assertRaises(TimeoutError, self.db.command,
                              "getlasterror", w=2, wtimeout=1)
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
        self.assertEqual(db.test.find({'foo': re.compile(r'ba.*')}).count(), 2)

    def test_aggregate(self):
        if not version.at_least(self.db.connection, (2, 1, 0)):
            raise SkipTest("The aggregate command requires MongoDB >= 2.1.0")
        db = self.db
        db.drop_collection("test")
        db.test.save({'foo': [1, 2]})

        self.assertRaises(TypeError, db.test.aggregate, "wow")

        pipeline = {"$project": {"_id": False, "foo": True}}
        expected = {'ok': 1.0, 'result': [{'foo': [1, 2]}]}
        self.assertEqual(expected, db.test.aggregate(pipeline))
        self.assertEqual(expected, db.test.aggregate([pipeline]))
        self.assertEqual(expected, db.test.aggregate((pipeline,)))

    def test_group(self):
        db = self.db
        db.drop_collection("test")

        def group_checker(args, expected):
            eval = db.test.group(*args)
            self.assertEqual(eval, expected)

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

        if version.at_least(db.connection, (1, 1)):
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

        for i in range(2000):
            doc = {"x": i, "y": "mongomongo" * 1000}
            db.test_large_limit.insert(doc)

        # Wait for insert to complete; often mysteriously failing in Jenkins
        st = time.time()
        while (
            len(list(db.test_large_limit.find())) < 2000
            and time.time() - st < 30
        ):
            time.sleep(1)

        self.assertEqual(2000, len(list(db.test_large_limit.find())))

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

    # doesn't really test functionality, just that the option is set correctly
    def test_snapshot(self):
        db = self.db

        self.assertRaises(TypeError, db.test.find, snapshot=5)

        list(db.test.find(snapshot=True))
        self.assertRaises(OperationFailure, list,
                          db.test.find(snapshot=True).sort("foo", 1))

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

        self.assertTrue("hello" in db.test.find_one(fields=["hello"]))
        self.assertTrue("hello" not in db.test.find_one(fields=["foo"]))
        self.assertEqual(["_id"], db.test.find_one(fields=[]).keys())

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
        list(self.db.test.find(timeout=False))
        list(self.db.test.find(timeout=True))

    def test_distinct(self):
        if not version.at_least(self.db.connection, (1, 1)):
            raise SkipTest("distinct command requires MongoDB >= 1.1")

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
        if version.at_least(self.db.connection, (1, 7, 4)):
            self.assertEqual(max_size, 16777216)
        self.assertRaises(InvalidDocument, self.db.test.insert,
                          {"foo": "x" * max_size})
        self.assertRaises(InvalidDocument, self.db.test.save,
                          {"foo": "x" * max_size})
        self.assertRaises(InvalidDocument, self.db.test.insert,
                          [{"x": 1}, {"foo": "x" * max_size}])
        self.db.test.insert([{"foo": "x" * half_size},
                             {"foo": "x" * half_size}])

        self.db.test.insert({"bar": "x"})
        self.assertRaises(InvalidDocument, self.db.test.update,
                          {"bar": "x"}, {"bar": "x" * (max_size - 14)})
        self.db.test.update({"bar": "x"}, {"bar": "x" * (max_size - 15)})

    def test_map_reduce(self):
        if not version.at_least(self.db.connection, (1, 1, 1)):
            raise SkipTest("mapReduce command requires MongoDB >= 1.1.1")

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

        if version.at_least(self.db.connection, (1, 7, 4)):
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

            if (is_mongos(self.db.connection)
                and not version.at_least(self.db.connection, (2, 1, 2))):
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

        if version.at_least(self.db.connection, (1, 7, 4)):
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

        db[u"Employés"].insert({"x": 1})
        db[u"Employés"].update({"x": 1}, {"x": 2})
        db[u"Employés"].remove({})
        db[u"Employés"].find_one()
        list(db[u"Employés"].find())

    def test_drop_indexes_non_existant(self):
        self.db.drop_collection("test")
        self.db.test.drop_indexes()

    # This is really a bson test but easier to just reproduce it here...
    # (Shame on me)
    def test_bad_encode(self):
        c = self.db.test
        warnings.simplefilter("ignore")
        self.assertRaises(InvalidDocument, c.save, {"x": c})
        warnings.simplefilter("default")

    def test_as_class(self):
        c = self.db.test
        c.drop()
        c.insert({"x": 1})

        doc = c.find().next()
        self.assertTrue(isinstance(doc, dict))
        doc = c.find().next()
        self.assertFalse(isinstance(doc, SON))
        doc = c.find(as_class=SON).next()
        self.assertTrue(isinstance(doc, SON))

        self.assertTrue(isinstance(c.find_one(), dict))
        self.assertFalse(isinstance(c.find_one(), SON))
        self.assertTrue(isinstance(c.find_one(as_class=SON), SON))

        self.assertEqual(1, c.find_one(as_class=SON)["x"])
        doc = c.find(as_class=SON).next()
        self.assertEqual(1, doc["x"])

    def test_find_and_modify(self):
        c = self.db.test
        c.drop()
        c.insert({'_id': 1, 'i': 1})

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
        if version.at_least(self.db.connection, (2, 1, 2)):
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

        # Test with full_response=True (version > 2.4.2)
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
        result = c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                    new=True, fields={'i': 1},
                                    as_class=ExtendedDict)
        self.assertTrue(isinstance(result, ExtendedDict))

    def test_find_and_modify_with_sort(self):
        c = self.db.test
        c.drop()
        for j in xrange(5):
            c.insert({'j': j, 'i': 0})

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
        self.assertRaises(TypeError, c.find_and_modify, {},
                                                         {'$inc': {'i': 1}},
                                                         sort=sort)

    def test_find_with_nested(self):
        if not version.at_least(self.db.connection, (2, 0, 0)):
            raise SkipTest("nested $and and $or requires MongoDB >= 2.0")
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

    def test_uuid_subtype(self):
        if not have_uuid:
            raise SkipTest("No uuid module")

        coll = self.client.pymongo_test.uuid
        coll.drop()

        def change_subtype(collection, subtype):
            collection.uuid_subtype = subtype

        # Test property
        self.assertEqual(OLD_UUID_SUBTYPE, coll.uuid_subtype)
        self.assertRaises(ConfigurationError, change_subtype, coll, 7)
        self.assertRaises(ConfigurationError, change_subtype, coll, 2)

        # Test basic query
        uu = uuid.uuid4()
        # Insert as binary subtype 3
        coll.insert({'uu': uu})
        self.assertEqual(uu, coll.find_one({'uu': uu})['uu'])
        coll.uuid_subtype = UUID_SUBTYPE
        self.assertEqual(UUID_SUBTYPE, coll.uuid_subtype)
        self.assertEqual(None, coll.find_one({'uu': uu}))
        self.assertEqual(uu, coll.find_one({'uu': UUIDLegacy(uu)})['uu'])

        # Test Cursor.count
        self.assertEqual(0, coll.find({'uu': uu}).count())
        coll.uuid_subtype = OLD_UUID_SUBTYPE
        self.assertEqual(1, coll.find({'uu': uu}).count())

        # Test remove
        coll.uuid_subtype = UUID_SUBTYPE
        coll.remove({'uu': uu})
        self.assertEqual(1, coll.count())
        coll.uuid_subtype = OLD_UUID_SUBTYPE
        coll.remove({'uu': uu})
        self.assertEqual(0, coll.count())

        # Test save
        coll.insert({'_id': uu, 'i': 0})
        self.assertEqual(1, coll.count())
        self.assertEqual(1, coll.find({'_id': uu}).count())
        self.assertEqual(0, coll.find_one({'_id': uu})['i'])
        doc = coll.find_one({'_id': uu})
        doc['i'] = 1
        coll.save(doc)
        self.assertEqual(1, coll.find_one({'_id': uu})['i'])

        # Test update
        coll.uuid_subtype = UUID_SUBTYPE
        coll.update({'_id': uu}, {'$set': {'i': 2}})
        coll.uuid_subtype = OLD_UUID_SUBTYPE
        self.assertEqual(1, coll.find_one({'_id': uu})['i'])
        coll.update({'_id': uu}, {'$set': {'i': 2}})
        self.assertEqual(2, coll.find_one({'_id': uu})['i'])

        # Test Cursor.distinct
        self.assertEqual([2], coll.find({'_id': uu}).distinct('i'))
        coll.uuid_subtype = UUID_SUBTYPE
        self.assertEqual([], coll.find({'_id': uu}).distinct('i'))

        # Test find_and_modify
        self.assertEqual(None, coll.find_and_modify({'_id': uu},
                                                     {'$set': {'i': 5}}))
        coll.uuid_subtype = OLD_UUID_SUBTYPE
        self.assertEqual(2, coll.find_and_modify({'_id': uu},
                                                  {'$set': {'i': 5}})['i'])
        self.assertEqual(5, coll.find_one({'_id': uu})['i'])

        # Test command
        db = self.client.pymongo_test
        no_obj_error = "No matching object found"
        result = db.command('findAndModify', 'uuid',
                            allowable_errors=[no_obj_error],
                            uuid_subtype=UUID_SUBTYPE,
                            query={'_id': uu},
                            update={'$set': {'i': 6}})
        self.assertEqual(None, result.get('value'))
        self.assertEqual(5, db.command('findAndModify', 'uuid',
                                       update={'$set': {'i': 6}},
                                       query={'_id': uu})['value']['i'])
        self.assertEqual(6, db.command('findAndModify', 'uuid',
                                       update={'$set': {'i': 7}},
                                       query={'_id': UUIDLegacy(uu)}
                                      )['value']['i'])

        # Test (inline)_map_reduce
        coll.drop()
        coll.insert({"_id": uu, "x": 1, "tags": ["dog", "cat"]})
        coll.insert({"_id": uuid.uuid4(), "x": 3,
                     "tags": ["mouse", "cat", "dog"]})

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

        coll.uuid_subtype = UUID_SUBTYPE
        q = {"_id": uu}
        if version.at_least(self.db.connection, (1, 7, 4)):
            result = coll.inline_map_reduce(map, reduce, query=q)
            self.assertEqual([], result)

        result = coll.map_reduce(map, reduce, "results", query=q)
        self.assertEqual(0, db.results.count())

        coll.uuid_subtype = OLD_UUID_SUBTYPE
        q = {"_id": uu}
        if version.at_least(self.db.connection, (1, 7, 4)):
            result = coll.inline_map_reduce(map, reduce, query=q)
            self.assertEqual(2, len(result))

        result = coll.map_reduce(map, reduce, "results", query=q)
        self.assertEqual(2, db.results.count())

        db.drop_collection("result")
        coll.drop()

        # Test group
        coll.insert({"_id": uu, "a": 2})
        coll.insert({"_id": uuid.uuid4(), "a": 1})

        reduce = "function (obj, prev) { prev.count++; }"
        coll.uuid_subtype = UUID_SUBTYPE
        self.assertEqual([],
                         coll.group([], {"_id": uu},
                                     {"count": 0}, reduce))
        coll.uuid_subtype = OLD_UUID_SUBTYPE
        self.assertEqual([{"count": 1}],
                         coll.group([], {"_id": uu},
                                    {"count": 0}, reduce))


if __name__ == "__main__":
    unittest.main()
