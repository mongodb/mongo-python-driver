# -*- coding: utf-8 -*-

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

"""Test the collection module."""

import contextlib
import re
import sys
import threading

from codecs import utf_8_decode
from collections import defaultdict

sys.path[0:0] = [""]

from bson import encode
from bson.raw_bson import RawBSONDocument
from bson.regex import Regex
from bson.code import Code
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from bson.py3compat import itervalues
from bson.son import SON
from pymongo import (ASCENDING, DESCENDING, GEO2D,
                     GEOHAYSTACK, GEOSPHERE, HASHED, TEXT)
from pymongo.bulk import BulkWriteError
from pymongo.collection import Collection, ReturnDocument
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import CursorType
from pymongo.errors import (ConfigurationError,
                            DocumentTooLarge,
                            DuplicateKeyError,
                            ExecutionTimeout,
                            InvalidDocument,
                            InvalidName,
                            InvalidOperation,
                            OperationFailure,
                            WriteConcernError)
from pymongo.message import _COMMAND_OVERHEAD, _gen_find_command
from pymongo.mongo_client import MongoClient
from pymongo.operations import *
from pymongo.read_concern import DEFAULT_READ_CONCERN
from pymongo.read_preferences import ReadPreference
from pymongo.results import (InsertOneResult,
                             InsertManyResult,
                             UpdateResult,
                             DeleteResult)
from pymongo.write_concern import WriteConcern
from test import client_context, unittest
from test.test_client import IntegrationTest
from test.utils import (get_pool, ignore_deprecations, is_mongos,
                        rs_or_single_client, single_client,
                        wait_until, EventListener,
                        IMPOSSIBLE_WRITE_CONCERN)


class TestCollectionNoConnect(unittest.TestCase):
    """Test Collection features on a client that does not connect.
    """

    @classmethod
    def setUpClass(cls):
        cls.db = MongoClient(connect=False).pymongo_test

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

        coll2 = coll.with_options(write_concern=WriteConcern(w=0))
        self.assertEqual(coll2.write_concern, WriteConcern(w=0))
        self.assertNotEqual(coll.write_concern, coll2.write_concern)
        coll3 = coll2.subcoll
        self.assertEqual(coll2.write_concern, coll3.write_concern)
        coll4 = coll2["subcoll"]
        self.assertEqual(coll2.write_concern, coll4.write_concern)

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

    def setUp(self):
        self.db.test.drop()

    def tearDown(self):
        self.db.test.drop()

    @contextlib.contextmanager
    def write_concern_collection(self):
        if client_context.version.at_least(3, 3, 9) and client_context.is_rs:
            with self.assertRaises(WriteConcernError):
                # Unsatisfiable write concern.
                yield Collection(
                    self.db, 'test',
                    write_concern=WriteConcern(w=len(client_context.nodes) + 1))
        else:
            yield self.db.test

    def test_equality(self):
        self.assertTrue(isinstance(self.db.test, Collection))
        self.assertEqual(self.db.test, self.db["test"])
        self.assertEqual(self.db.test, Collection(self.db, "test"))
        self.assertEqual(self.db.test.mike, self.db["test.mike"])
        self.assertEqual(self.db.test["mike"], self.db["test.mike"])

    @client_context.require_version_min(3, 3, 9)
    def test_create(self):
        # No Exception.
        db = client_context.client.pymongo_test
        db.create_test_no_wc.drop()
        wait_until(
            lambda: 'create_test_no_wc' not in db.list_collection_names(),
            'drop create_test_no_wc collection')
        Collection(db, name='create_test_no_wc', create=True)
        wait_until(
            lambda: 'create_test_no_wc' in db.list_collection_names(),
            'create create_test_no_wc collection')
        # SERVER-33317
        if (not client_context.is_mongos or not
                client_context.version.at_least(3, 7, 0)):
            with self.assertRaises(OperationFailure):
                Collection(
                    db, name='create-test-wc',
                    write_concern=IMPOSSIBLE_WRITE_CONCERN,
                    create=True)

    def test_drop_nonexistent_collection(self):
        self.db.drop_collection('test')
        self.assertFalse('test' in self.db.list_collection_names())

        # No exception
        self.db.drop_collection('test')

    def test_create_indexes(self):
        db = self.db

        self.assertRaises(TypeError, db.test.create_indexes, 'foo')
        self.assertRaises(TypeError, db.test.create_indexes, ['foo'])
        self.assertRaises(TypeError, IndexModel, 5)
        self.assertRaises(ValueError, IndexModel, [])

        db.test.drop_indexes()
        db.test.insert_one({})
        self.assertEqual(len(db.test.index_information()), 1)

        db.test.create_indexes([IndexModel("hello")])
        db.test.create_indexes([IndexModel([("hello", DESCENDING),
                                            ("world", ASCENDING)])])

        # Tuple instead of list.
        db.test.create_indexes([IndexModel((("world", ASCENDING),))])

        self.assertEqual(len(db.test.index_information()), 4)

        db.test.drop_indexes()
        names = db.test.create_indexes([IndexModel([("hello", DESCENDING),
                                                    ("world", ASCENDING)],
                                                   name="hello_world")])
        self.assertEqual(names, ["hello_world"])

        db.test.drop_indexes()
        self.assertEqual(len(db.test.index_information()), 1)
        db.test.create_indexes([IndexModel("hello")])
        self.assertTrue("hello_1" in db.test.index_information())

        db.test.drop_indexes()
        self.assertEqual(len(db.test.index_information()), 1)
        names = db.test.create_indexes([IndexModel([("hello", DESCENDING),
                                                    ("world", ASCENDING)]),
                                        IndexModel("hello")])
        info = db.test.index_information()
        for name in names:
            self.assertTrue(name in info)

        db.test.drop()
        db.test.insert_one({'a': 1})
        db.test.insert_one({'a': 1})
        self.assertRaises(
            DuplicateKeyError,
            db.test.create_indexes,
            [IndexModel('a', unique=True)])

        with self.write_concern_collection() as coll:
            coll.create_indexes([IndexModel('hello')])

    @client_context.require_version_max(4, 3, -1)
    def test_create_indexes_commitQuorum_requires_44(self):
        db = self.db
        with self.assertRaisesRegex(
                ConfigurationError,
                'Must be connected to MongoDB 4\.4\+ to use the commitQuorum '
                'option for createIndexes'):
            db.coll.create_indexes([IndexModel('a')], commitQuorum="majority")

    @client_context.require_no_standalone
    @client_context.require_version_min(4, 4, -1)
    def test_create_indexes_commitQuorum(self):
        self.db.coll.create_indexes([IndexModel('a')], commitQuorum="majority")

    def test_create_index(self):
        db = self.db

        self.assertRaises(TypeError, db.test.create_index, 5)
        self.assertRaises(TypeError, db.test.create_index, {"hello": 1})
        self.assertRaises(ValueError, db.test.create_index, [])

        db.test.drop_indexes()
        db.test.insert_one({})
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
        db.test.insert_one({'a': 1})
        db.test.insert_one({'a': 1})
        self.assertRaises(
            DuplicateKeyError, db.test.create_index, 'a', unique=True)

        with self.write_concern_collection() as coll:
            coll.create_index([('hello', DESCENDING)])

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

        with self.write_concern_collection() as coll:
            coll.drop_index('hello_1')

    @client_context.require_no_mongos
    @client_context.require_test_commands
    def test_index_management_max_time_ms(self):
        if (client_context.version[:2] == (3, 4) and
                client_context.version[2] < 4):
            raise unittest.SkipTest("SERVER-27711")
        coll = self.db.test
        self.client.admin.command("configureFailPoint",
                                  "maxTimeAlwaysTimeOut",
                                  mode="alwaysOn")
        try:
            self.assertRaises(
                ExecutionTimeout, coll.create_index, "foo", maxTimeMS=1)
            self.assertRaises(
                ExecutionTimeout,
                coll.create_indexes,
                [IndexModel("foo")],
                maxTimeMS=1)
            self.assertRaises(
                ExecutionTimeout, coll.drop_index, "foo", maxTimeMS=1)
            self.assertRaises(
                ExecutionTimeout, coll.drop_indexes, maxTimeMS=1)
            self.assertRaises(
                ExecutionTimeout, coll.reindex, maxTimeMS=1)
        finally:
            self.client.admin.command("configureFailPoint",
                                      "maxTimeAlwaysTimeOut",
                                      mode="off")

    def test_reindex(self):
        if not client_context.supports_reindex:
            raise unittest.SkipTest(
                "reindex is no longer supported by mongos 4.1+")
        db = self.db
        db.drop_collection("test")
        db.test.insert_one({"foo": "bar", "who": "what", "when": "how"})
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

        coll = Collection(
            self.db,
            'test',
            write_concern=WriteConcern(w=100))
        # No error since writeConcern is not sent.
        coll.reindex()

    def test_list_indexes(self):
        db = self.db
        db.test.drop()
        db.test.insert_one({})  # create collection

        def map_indexes(indexes):
            return dict([(index["name"], index) for index in indexes])

        indexes = list(db.test.list_indexes())
        self.assertEqual(len(indexes), 1)
        self.assertTrue("_id_" in map_indexes(indexes))

        db.test.create_index("hello")
        indexes = list(db.test.list_indexes())
        self.assertEqual(len(indexes), 2)
        self.assertEqual(map_indexes(indexes)["hello_1"]["key"],
                         SON([("hello", ASCENDING)]))

        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)],
                             unique=True)
        indexes = list(db.test.list_indexes())
        self.assertEqual(len(indexes), 3)
        index_map = map_indexes(indexes)
        self.assertEqual(index_map["hello_-1_world_1"]["key"],
                         SON([("hello", DESCENDING), ("world", ASCENDING)]))
        self.assertEqual(True, index_map["hello_-1_world_1"]["unique"])

        # List indexes on a collection that does not exist.
        indexes = list(db.does_not_exist.list_indexes())
        self.assertEqual(len(indexes), 0)

        # List indexes on a database that does not exist.
        indexes = list(self.client.db_does_not_exist.coll.list_indexes())
        self.assertEqual(len(indexes), 0)

    def test_index_info(self):
        db = self.db
        db.test.drop()
        db.test.insert_one({})  # create collection
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
        self.assertEqual(
            True, db.test.index_information()["hello_-1_world_1"]["unique"])

    def test_index_geo2d(self):
        db = self.db
        db.test.drop_indexes()
        self.assertEqual('loc_2d', db.test.create_index([("loc", GEO2D)]))
        index_info = db.test.index_information()['loc_2d']
        self.assertEqual([('loc', '2d')], index_info['key'])

    # geoSearch was deprecated in 4.4 and removed in 5.0
    @client_context.require_version_max(4, 5)
    @client_context.require_no_mongos
    def test_index_haystack(self):
        db = self.db
        db.test.drop()
        _id = db.test.insert_one({
            "pos": {"long": 34.2, "lat": 33.3},
            "type": "restaurant"
        }).inserted_id
        db.test.insert_one({
            "pos": {"long": 34.2, "lat": 37.3}, "type": "restaurant"
        })
        db.test.insert_one({
            "pos": {"long": 59.1, "lat": 87.2}, "type": "office"
        })
        db.test.create_index(
            [("pos", GEOHAYSTACK), ("type", ASCENDING)],
            bucketSize=1
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

    @client_context.require_no_mongos
    def test_index_text(self):
        db = self.db
        db.test.drop_indexes()
        self.assertEqual("t_text", db.test.create_index([("t", TEXT)]))
        index_info = db.test.index_information()["t_text"]
        self.assertTrue("weights" in index_info)

        db.test.insert_many([
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

    def test_index_2dsphere(self):
        db = self.db
        db.test.drop_indexes()
        self.assertEqual("geo_2dsphere",
                         db.test.create_index([("geo", GEOSPHERE)]))

        for dummy, info in db.test.index_information().items():
            field, idx_type = info['key'][0]
            if field == 'geo' and idx_type == '2dsphere':
                break
        else:
            self.fail("2dsphere index not found.")

        poly = {"type": "Polygon",
                "coordinates": [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]]}
        query = {"geo": {"$within": {"$geometry": poly}}}

        # This query will error without a 2dsphere index.
        db.test.find(query)
        db.test.drop_indexes()

    def test_index_hashed(self):
        db = self.db
        db.test.drop_indexes()
        self.assertEqual("a_hashed",
                         db.test.create_index([("a", HASHED)]))

        for dummy, info in db.test.index_information().items():
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
        db.test.insert_one({'i': 1})
        db.test.insert_one({'i': 2})
        db.test.insert_one({'i': 2})  # duplicate
        db.test.insert_one({'i': 3})

    @client_context.require_version_max(2, 6)
    def test_index_drop_dups(self):
        # Try dropping duplicates
        db = self.db
        self._drop_dups_setup(db)

        # No error, just drop the duplicate
        db.test.create_index([('i', ASCENDING)], unique=True, dropDups=True)

        # Duplicate was dropped
        self.assertEqual(3, db.test.count_documents({}))

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
                dropDups=False
            )
        self.assertRaises(DuplicateKeyError, test_create)

        # Duplicate wasn't dropped
        self.assertEqual(4, db.test.count_documents({}))

        # Index wasn't created, only the default index on _id
        self.assertEqual(1, len(db.test.index_information()))

    # Get the plan dynamically because the explain format will change.
    def get_plan_stage(self, root, stage):
        if root.get('stage') == stage:
            return root
        elif "inputStage" in root:
            return self.get_plan_stage(root['inputStage'], stage)
        elif "inputStages" in root:
            for i in root['inputStages']:
                stage = self.get_plan_stage(i, stage)
                if stage:
                    return stage
        elif "shards" in root:
            for i in root['shards']:
                stage = self.get_plan_stage(i['winningPlan'], stage)
                if stage:
                    return stage
        return {}

    @client_context.require_version_min(3, 1, 9, -1)
    def test_index_filter(self):
        db = self.db
        db.drop_collection("test")

        # Test bad filter spec on create.
        self.assertRaises(OperationFailure, db.test.create_index, "x",
                          partialFilterExpression=5)
        self.assertRaises(OperationFailure, db.test.create_index, "x",
                          partialFilterExpression={"x": {"$asdasd": 3}})
        self.assertRaises(OperationFailure, db.test.create_index, "x",
                          partialFilterExpression={"$and": 5})

        self.assertEqual("x_1", db.test.create_index(
            [('x', ASCENDING)], partialFilterExpression={"a": {"$lte": 1.5}}))
        db.test.insert_one({"x": 5, "a": 2})
        db.test.insert_one({"x": 6, "a": 1})

        # Operations that use the partial index.
        explain = db.test.find({"x": 6, "a": 1}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'],
                                    'IXSCAN')
        self.assertEqual("x_1", stage.get('indexName'))
        self.assertTrue(stage.get('isPartial'))

        explain = db.test.find({"x": {"$gt": 1}, "a": 1}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'],
                                    'IXSCAN')
        self.assertEqual("x_1", stage.get('indexName'))
        self.assertTrue(stage.get('isPartial'))

        explain = db.test.find({"x": 6, "a": {"$lte": 1}}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'],
                                    'IXSCAN')
        self.assertEqual("x_1", stage.get('indexName'))
        self.assertTrue(stage.get('isPartial'))

        # Operations that do not use the partial index.
        explain = db.test.find({"x": 6, "a": {"$lte": 1.6}}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'],
                                    'COLLSCAN')
        self.assertNotEqual({}, stage)
        explain = db.test.find({"x": 6}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'],
                                    'COLLSCAN')
        self.assertNotEqual({}, stage)

        # Test drop_indexes.
        db.test.drop_index("x_1")
        explain = db.test.find({"x": 6, "a": 1}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'],
                                    'COLLSCAN')
        self.assertNotEqual({}, stage)

    def test_field_selection(self):
        db = self.db
        db.drop_collection("test")

        doc = {"a": 1, "b": 5, "c": {"d": 5, "e": 10}}
        db.test.insert_one(doc)

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
        db.create_collection("test", capped=True, size=4096)
        result = db.test.options()
        # mongos 2.2.x adds an $auth field when auth is enabled.
        result.pop('$auth', None)
        self.assertEqual(result, {"capped": True, 'size': 4096})
        db.drop_collection("test")

    def test_insert_one(self):
        db = self.db
        db.test.drop()

        document = {"_id": 1000}
        result = db.test.insert_one(document)
        self.assertTrue(isinstance(result, InsertOneResult))
        self.assertTrue(isinstance(result.inserted_id, int))
        self.assertEqual(document["_id"], result.inserted_id)
        self.assertTrue(result.acknowledged)
        self.assertIsNotNone(db.test.find_one({"_id": document["_id"]}))
        self.assertEqual(1, db.test.count_documents({}))

        document = {"foo": "bar"}
        result = db.test.insert_one(document)
        self.assertTrue(isinstance(result, InsertOneResult))
        self.assertTrue(isinstance(result.inserted_id, ObjectId))
        self.assertEqual(document["_id"], result.inserted_id)
        self.assertTrue(result.acknowledged)
        self.assertIsNotNone(db.test.find_one({"_id": document["_id"]}))
        self.assertEqual(2, db.test.count_documents({}))

        db = db.client.get_database(db.name,
                                    write_concern=WriteConcern(w=0))
        result = db.test.insert_one(document)
        self.assertTrue(isinstance(result, InsertOneResult))
        self.assertTrue(isinstance(result.inserted_id, ObjectId))
        self.assertEqual(document["_id"], result.inserted_id)
        self.assertFalse(result.acknowledged)
        # The insert failed duplicate key...
        wait_until(lambda: 2 == db.test.count_documents({}),
                   'forcing duplicate key error')

        document = RawBSONDocument(
            encode({'_id': ObjectId(), 'foo': 'bar'}))
        result = db.test.insert_one(document)
        self.assertTrue(isinstance(result, InsertOneResult))
        self.assertEqual(result.inserted_id, None)

    def test_insert_many(self):
        db = self.db
        db.test.drop()

        docs = [{} for _ in range(5)]
        result = db.test.insert_many(docs)
        self.assertTrue(isinstance(result, InsertManyResult))
        self.assertTrue(isinstance(result.inserted_ids, list))
        self.assertEqual(5, len(result.inserted_ids))
        for doc in docs:
            _id = doc["_id"]
            self.assertTrue(isinstance(_id, ObjectId))
            self.assertTrue(_id in result.inserted_ids)
            self.assertEqual(1, db.test.count_documents({'_id': _id}))
        self.assertTrue(result.acknowledged)

        docs = [{"_id": i} for i in range(5)]
        result = db.test.insert_many(docs)
        self.assertTrue(isinstance(result, InsertManyResult))
        self.assertTrue(isinstance(result.inserted_ids, list))
        self.assertEqual(5, len(result.inserted_ids))
        for doc in docs:
            _id = doc["_id"]
            self.assertTrue(isinstance(_id, int))
            self.assertTrue(_id in result.inserted_ids)
            self.assertEqual(1, db.test.count_documents({"_id": _id}))
        self.assertTrue(result.acknowledged)

        docs = [RawBSONDocument(encode({"_id": i + 5}))
                for i in range(5)]
        result = db.test.insert_many(docs)
        self.assertTrue(isinstance(result, InsertManyResult))
        self.assertTrue(isinstance(result.inserted_ids, list))
        self.assertEqual([], result.inserted_ids)

        db = db.client.get_database(db.name,
                                    write_concern=WriteConcern(w=0))
        docs = [{} for _ in range(5)]
        result = db.test.insert_many(docs)
        self.assertTrue(isinstance(result, InsertManyResult))
        self.assertFalse(result.acknowledged)
        self.assertEqual(20, db.test.count_documents({}))

    def test_delete_one(self):
        self.db.test.drop()

        self.db.test.insert_one({"x": 1})
        self.db.test.insert_one({"y": 1})
        self.db.test.insert_one({"z": 1})

        result = self.db.test.delete_one({"x": 1})
        self.assertTrue(isinstance(result, DeleteResult))
        self.assertEqual(1, result.deleted_count)
        self.assertTrue(result.acknowledged)
        self.assertEqual(2, self.db.test.count_documents({}))

        result = self.db.test.delete_one({"y": 1})
        self.assertTrue(isinstance(result, DeleteResult))
        self.assertEqual(1, result.deleted_count)
        self.assertTrue(result.acknowledged)
        self.assertEqual(1, self.db.test.count_documents({}))

        db = self.db.client.get_database(self.db.name,
                                         write_concern=WriteConcern(w=0))
        result = db.test.delete_one({"z": 1})
        self.assertTrue(isinstance(result, DeleteResult))
        self.assertRaises(InvalidOperation, lambda: result.deleted_count)
        self.assertFalse(result.acknowledged)
        wait_until(lambda: 0 == db.test.count_documents({}), 'delete 1 documents')

    def test_delete_many(self):
        self.db.test.drop()

        self.db.test.insert_one({"x": 1})
        self.db.test.insert_one({"x": 1})
        self.db.test.insert_one({"y": 1})
        self.db.test.insert_one({"y": 1})

        result = self.db.test.delete_many({"x": 1})
        self.assertTrue(isinstance(result, DeleteResult))
        self.assertEqual(2, result.deleted_count)
        self.assertTrue(result.acknowledged)
        self.assertEqual(0, self.db.test.count_documents({"x": 1}))

        db = self.db.client.get_database(self.db.name,
                                         write_concern=WriteConcern(w=0))
        result = db.test.delete_many({"y": 1})
        self.assertTrue(isinstance(result, DeleteResult))
        self.assertRaises(InvalidOperation, lambda: result.deleted_count)
        self.assertFalse(result.acknowledged)
        wait_until(
            lambda: 0 == db.test.count_documents({}), 'delete 2 documents')

    def test_command_document_too_large(self):
        large = '*' * (self.client.max_bson_size + _COMMAND_OVERHEAD)
        coll = self.db.test
        self.assertRaises(
            DocumentTooLarge, coll.insert_one, {'data': large})
        # update_one and update_many are the same
        self.assertRaises(
            DocumentTooLarge, coll.replace_one, {}, {'data': large})
        self.assertRaises(
            DocumentTooLarge, coll.delete_one, {'data': large})

    @client_context.require_version_min(3, 1, 9, -1)
    def test_insert_bypass_document_validation(self):
        db = self.db
        db.test.drop()
        db.create_collection("test", validator={"a": {"$exists": True}})
        db_w0 = self.db.client.get_database(
            self.db.name, write_concern=WriteConcern(w=0))

        # Test insert_one
        self.assertRaises(OperationFailure, db.test.insert_one,
                          {"_id": 1, "x": 100})
        result = db.test.insert_one({"_id": 1, "x": 100},
                                    bypass_document_validation=True)
        self.assertTrue(isinstance(result, InsertOneResult))
        self.assertEqual(1, result.inserted_id)
        result = db.test.insert_one({"_id":2, "a":0})
        self.assertTrue(isinstance(result, InsertOneResult))
        self.assertEqual(2, result.inserted_id)

        if client_context.version < (3, 6):
            # Uses OP_INSERT which does not support bypass_document_validation.
            self.assertRaises(OperationFailure, db_w0.test.insert_one,
                              {"y": 1}, bypass_document_validation=True)
        else:
            db_w0.test.insert_one({"y": 1}, bypass_document_validation=True)
            wait_until(lambda: db_w0.test.find_one({"y": 1}),
                       "find w:0 inserted document")

        # Test insert_many
        docs = [{"_id": i, "x": 100 - i} for i in range(3, 100)]
        self.assertRaises(OperationFailure, db.test.insert_many, docs)
        result = db.test.insert_many(docs, bypass_document_validation=True)
        self.assertTrue(isinstance(result, InsertManyResult))
        self.assertTrue(97, len(result.inserted_ids))
        for doc in docs:
            _id = doc["_id"]
            self.assertTrue(isinstance(_id, int))
            self.assertTrue(_id in result.inserted_ids)
            self.assertEqual(1, db.test.count_documents({"x": doc["x"]}))
        self.assertTrue(result.acknowledged)
        docs = [{"_id": i, "a": 200 - i} for i in range(100, 200)]
        result = db.test.insert_many(docs)
        self.assertTrue(isinstance(result, InsertManyResult))
        self.assertTrue(97, len(result.inserted_ids))
        for doc in docs:
            _id = doc["_id"]
            self.assertTrue(isinstance(_id, int))
            self.assertTrue(_id in result.inserted_ids)
            self.assertEqual(1, db.test.count_documents({"a": doc["a"]}))
        self.assertTrue(result.acknowledged)

        self.assertRaises(OperationFailure, db_w0.test.insert_many,
                          [{"x": 1}, {"x": 2}],
                          bypass_document_validation=True)

    @client_context.require_version_min(3, 1, 9, -1)
    def test_replace_bypass_document_validation(self):
        db = self.db
        db.test.drop()
        db.create_collection("test", validator={"a": {"$exists": True}})
        db_w0 = self.db.client.get_database(
            self.db.name, write_concern=WriteConcern(w=0))

        # Test replace_one
        db.test.insert_one({"a": 101})
        self.assertRaises(OperationFailure, db.test.replace_one,
                          {"a": 101}, {"y": 1})
        self.assertEqual(0, db.test.count_documents({"y": 1}))
        self.assertEqual(1, db.test.count_documents({"a": 101}))
        db.test.replace_one({"a": 101}, {"y": 1},
                            bypass_document_validation=True)
        self.assertEqual(0, db.test.count_documents({"a": 101}))
        self.assertEqual(1, db.test.count_documents({"y": 1}))
        db.test.replace_one({"y": 1}, {"a": 102})
        self.assertEqual(0, db.test.count_documents({"y": 1}))
        self.assertEqual(0, db.test.count_documents({"a": 101}))
        self.assertEqual(1, db.test.count_documents({"a": 102}))

        db.test.insert_one({"y": 1}, bypass_document_validation=True)
        self.assertRaises(OperationFailure, db.test.replace_one,
                          {"y": 1}, {"x": 101})
        self.assertEqual(0, db.test.count_documents({"x": 101}))
        self.assertEqual(1, db.test.count_documents({"y": 1}))
        db.test.replace_one({"y": 1}, {"x": 101},
                            bypass_document_validation=True)
        self.assertEqual(0, db.test.count_documents({"y": 1}))
        self.assertEqual(1, db.test.count_documents({"x": 101}))
        db.test.replace_one({"x": 101}, {"a": 103},
                            bypass_document_validation=False)
        self.assertEqual(0, db.test.count_documents({"x": 101}))
        self.assertEqual(1, db.test.count_documents({"a": 103}))

        db.test.insert_one({"y": 1}, bypass_document_validation=True)
        if client_context.version < (3, 6):
            # Uses OP_UPDATE which does not support bypass_document_validation.
            self.assertRaises(OperationFailure, db_w0.test.replace_one,
                              {"y": 1}, {"x": 1},
                              bypass_document_validation=True)
        else:
            db_w0.test.replace_one({"y": 1}, {"x": 1},
                                   bypass_document_validation=True)
            wait_until(lambda: db_w0.test.find_one({"x": 1}),
                       "find w:0 replaced document")

    @client_context.require_version_min(3, 1, 9, -1)
    def test_update_bypass_document_validation(self):
        db = self.db
        db.test.drop()
        db.test.insert_one({"z": 5})
        db.command(SON([("collMod", "test"),
                        ("validator", {"z": {"$gte": 0}})]))
        db_w0 = self.db.client.get_database(
            self.db.name, write_concern=WriteConcern(w=0))

        # Test update_one
        self.assertRaises(OperationFailure, db.test.update_one,
                          {"z": 5}, {"$inc": {"z": -10}})
        self.assertEqual(0, db.test.count_documents({"z": -5}))
        self.assertEqual(1, db.test.count_documents({"z": 5}))
        db.test.update_one({"z": 5}, {"$inc": {"z": -10}},
                           bypass_document_validation=True)
        self.assertEqual(0, db.test.count_documents({"z": 5}))
        self.assertEqual(1, db.test.count_documents({"z": -5}))
        db.test.update_one({"z": -5}, {"$inc": {"z": 6}},
                           bypass_document_validation=False)
        self.assertEqual(1, db.test.count_documents({"z": 1}))
        self.assertEqual(0, db.test.count_documents({"z": -5}))

        db.test.insert_one({"z": -10},
                           bypass_document_validation=True)
        self.assertRaises(OperationFailure, db.test.update_one,
                          {"z": -10}, {"$inc": {"z": 1}})
        self.assertEqual(0, db.test.count_documents({"z": -9}))
        self.assertEqual(1, db.test.count_documents({"z": -10}))
        db.test.update_one({"z": -10}, {"$inc": {"z": 1}},
                           bypass_document_validation=True)
        self.assertEqual(1, db.test.count_documents({"z": -9}))
        self.assertEqual(0, db.test.count_documents({"z": -10}))
        db.test.update_one({"z": -9}, {"$inc": {"z": 9}},
                           bypass_document_validation=False)
        self.assertEqual(0, db.test.count_documents({"z": -9}))
        self.assertEqual(1, db.test.count_documents({"z": 0}))

        db.test.insert_one({"y": 1, "x": 0}, bypass_document_validation=True)
        if client_context.version < (3, 6):
            # Uses OP_UPDATE which does not support bypass_document_validation.
            self.assertRaises(OperationFailure, db_w0.test.update_one,
                              {"y": 1}, {"$inc": {"x": 1}},
                              bypass_document_validation=True)
        else:
            db_w0.test.update_one({"y": 1}, {"$inc": {"x": 1}},
                                  bypass_document_validation=True)
            wait_until(lambda: db_w0.test.find_one({"y": 1, "x": 1}),
                       "find w:0 updated document")

        # Test update_many
        db.test.insert_many([{"z": i} for i in range(3, 101)])
        db.test.insert_one({"y": 0},
                           bypass_document_validation=True)
        self.assertRaises(OperationFailure, db.test.update_many, {},
                          {"$inc": {"z": -100}})
        self.assertEqual(100, db.test.count_documents({"z": {"$gte": 0}}))
        self.assertEqual(0, db.test.count_documents({"z": {"$lt": 0}}))
        self.assertEqual(0, db.test.count_documents({"y": 0, "z": -100}))
        db.test.update_many({"z": {"$gte": 0}}, {"$inc": {"z": -100}},
                            bypass_document_validation=True)
        self.assertEqual(0, db.test.count_documents({"z": {"$gt": 0}}))
        self.assertEqual(100, db.test.count_documents({"z": {"$lte": 0}}))
        db.test.update_many({"z": {"$gt": -50}}, {"$inc": {"z": 100}},
                            bypass_document_validation=False)
        self.assertEqual(50, db.test.count_documents({"z": {"$gt": 0}}))
        self.assertEqual(50, db.test.count_documents({"z": {"$lt": 0}}))

        db.test.insert_many([{"z": -i} for i in range(50)],
                            bypass_document_validation=True)
        self.assertRaises(OperationFailure, db.test.update_many,
            {}, {"$inc": {"z": 1}})
        self.assertEqual(100, db.test.count_documents({"z": {"$lte": 0}}))
        self.assertEqual(50, db.test.count_documents({"z": {"$gt": 1}}))
        db.test.update_many({"z": {"$gte": 0}}, {"$inc": {"z": -100}},
                            bypass_document_validation=True)
        self.assertEqual(0, db.test.count_documents({"z": {"$gt": 0}}))
        self.assertEqual(150, db.test.count_documents({"z": {"$lte": 0}}))
        db.test.update_many({"z": {"$lte": 0}}, {"$inc": {"z": 100}},
                            bypass_document_validation=False)
        self.assertEqual(150, db.test.count_documents({"z": {"$gte": 0}}))
        self.assertEqual(0, db.test.count_documents({"z": {"$lt": 0}}))

        db.test.insert_one({"m": 1, "x": 0}, bypass_document_validation=True)
        db.test.insert_one({"m": 1, "x": 0}, bypass_document_validation=True)
        if client_context.version < (3, 6):
            # Uses OP_UPDATE which does not support bypass_document_validation.
            self.assertRaises(OperationFailure, db_w0.test.update_many,
                              {"m": 1}, {"$inc": {"x": 1}},
                              bypass_document_validation=True)
        else:
            db_w0.test.update_many({"m": 1}, {"$inc": {"x": 1}},
                                   bypass_document_validation=True)
            wait_until(
                lambda: db_w0.test.count_documents({"m": 1, "x": 1}) == 2,
                "find w:0 updated documents")

    @client_context.require_version_min(3, 1, 9, -1)
    def test_bypass_document_validation_bulk_write(self):
        db = self.db
        db.test.drop()
        db.create_collection("test", validator={"a": {"$gte": 0}})
        db_w0 = self.db.client.get_database(
            self.db.name, write_concern=WriteConcern(w=0))

        ops = [InsertOne({"a": -10}),
               InsertOne({"a": -11}),
               InsertOne({"a": -12}),
               UpdateOne({"a": {"$lte": -10}}, {"$inc": {"a": 1}}),
               UpdateMany({"a": {"$lte": -10}}, {"$inc": {"a": 1}}),
               ReplaceOne({"a": {"$lte": -10}}, {"a": -1})]
        db.test.bulk_write(ops, bypass_document_validation=True)

        self.assertEqual(3, db.test.count_documents({}))
        self.assertEqual(1, db.test.count_documents({"a": -11}))
        self.assertEqual(1, db.test.count_documents({"a": -1}))
        self.assertEqual(1, db.test.count_documents({"a": -9}))

        # Assert that the operations would fail without bypass_doc_val
        for op in ops:
            self.assertRaises(BulkWriteError, db.test.bulk_write, [op])

        self.assertRaises(OperationFailure, db_w0.test.bulk_write, ops,
                          bypass_document_validation=True)

    def test_find_by_default_dct(self):
        db = self.db
        db.test.insert_one({'foo': 'bar'})
        dct = defaultdict(dict, [('foo', 'bar')])
        self.assertIsNotNone(db.test.find_one(dct))
        self.assertEqual(dct, defaultdict(dict, [('foo', 'bar')]))

    def test_find_w_fields(self):
        db = self.db
        db.test.delete_many({})

        db.test.insert_one({"x": 1, "mike": "awesome",
                            "extra thing": "abcdefghijklmnopqrstuvwxyz"})
        self.assertEqual(1, db.test.count_documents({}))
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
        db.test.delete_many({})

        db.test.insert_one({"x": [1, 2, 3], "mike": "awesome"})

        self.assertEqual([1, 2, 3], db.test.find_one()["x"])
        self.assertEqual([2, 3],
                         db.test.find_one(
                             projection={"x": {"$slice": -2}})["x"])
        self.assertTrue("x" not in db.test.find_one(projection={"x": 0}))
        self.assertTrue("mike" in db.test.find_one(projection={"x": 0}))

    def test_find_w_regex(self):
        db = self.db
        db.test.delete_many({})

        db.test.insert_one({"x": "hello_world"})
        db.test.insert_one({"x": "hello_mike"})
        db.test.insert_one({"x": "hello_mikey"})
        db.test.insert_one({"x": "hello_test"})

        self.assertEqual(len(list(db.test.find())), 4)
        self.assertEqual(len(list(db.test.find({"x":
                                       re.compile("^hello.*")}))), 4)
        self.assertEqual(len(list(db.test.find({"x":
                                       re.compile("ello")}))), 4)
        self.assertEqual(len(list(db.test.find({"x":
                                       re.compile("^hello$")}))), 0)
        self.assertEqual(len(list(db.test.find({"x":
                                       re.compile("^hello_mi.*$")}))), 2)

    def test_id_can_be_anything(self):
        db = self.db

        db.test.delete_many({})
        auto_id = {"hello": "world"}
        db.test.insert_one(auto_id)
        self.assertTrue(isinstance(auto_id["_id"], ObjectId))

        numeric = {"_id": 240, "hello": "world"}
        db.test.insert_one(numeric)
        self.assertEqual(numeric["_id"], 240)

        obj = {"_id": numeric, "hello": "world"}
        db.test.insert_one(obj)
        self.assertEqual(obj["_id"], numeric)

        for x in db.test.find():
            self.assertEqual(x["hello"], u"world")
            self.assertTrue("_id" in x)

    def test_invalid_key_names(self):
        db = self.db
        db.test.drop()

        db.test.insert_one({"hello": "world"})
        db.test.insert_one({"hello": {"hello": "world"}})

        self.assertRaises(InvalidDocument, db.test.insert_one,
                          {"$hello": "world"})
        self.assertRaises(InvalidDocument, db.test.insert_one,
                          {"hello": {"$hello": "world"}})

        db.test.insert_one({"he$llo": "world"})
        db.test.insert_one({"hello": {"hello$": "world"}})

        self.assertRaises(InvalidDocument, db.test.insert_one,
                          {".hello": "world"})
        self.assertRaises(InvalidDocument, db.test.insert_one,
                          {"hello": {".hello": "world"}})
        self.assertRaises(InvalidDocument, db.test.insert_one,
                          {"hello.": "world"})
        self.assertRaises(InvalidDocument, db.test.insert_one,
                          {"hello": {"hello.": "world"}})
        self.assertRaises(InvalidDocument, db.test.insert_one,
                          {"hel.lo": "world"})
        self.assertRaises(InvalidDocument, db.test.insert_one,
                          {"hello": {"hel.lo": "world"}})

    def test_unique_index(self):
        db = self.db
        db.drop_collection("test")
        db.test.create_index("hello")

        # No error.
        db.test.insert_one({"hello": "world"})
        db.test.insert_one({"hello": "world"})

        db.drop_collection("test")
        db.test.create_index("hello", unique=True)

        with self.assertRaises(DuplicateKeyError):
            db.test.insert_one({"hello": "world"})
            db.test.insert_one({"hello": "world"})

    def test_duplicate_key_error(self):
        db = self.db
        db.drop_collection("test")

        db.test.create_index("x", unique=True)

        db.test.insert_one({"_id": 1, "x": 1})

        with self.assertRaises(DuplicateKeyError) as context:
            db.test.insert_one({"x": 1})

        self.assertIsNotNone(context.exception.details)

        with self.assertRaises(DuplicateKeyError) as context:
            db.test.insert_one({"x": 1})

        self.assertIsNotNone(context.exception.details)
        self.assertEqual(1, db.test.count_documents({}))

    def test_write_error_text_handling(self):
        db = self.db
        db.drop_collection("test")

        db.test.create_index("text", unique=True)

        # Test workaround for SERVER-24007
        data = (b'a\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83')

        text = utf_8_decode(data, None, True)
        db.test.insert_one({"text": text})

        # Should raise DuplicateKeyError, not InvalidBSON
        self.assertRaises(DuplicateKeyError,
                          db.test.insert_one,
                          {"text": text})

        self.assertRaises(DuplicateKeyError,
                          db.test.insert,
                          {"text": text})

        self.assertRaises(DuplicateKeyError,
                          db.test.insert,
                          [{"text": text}])

        self.assertRaises(DuplicateKeyError,
                          db.test.replace_one,
                          {"_id": ObjectId()},
                          {"text": text},
                          upsert=True)

        self.assertRaises(DuplicateKeyError,
                          db.test.update,
                          {"_id": ObjectId()},
                          {"text": text},
                          upsert=True)

        # Should raise BulkWriteError, not InvalidBSON
        self.assertRaises(BulkWriteError,
                          db.test.insert_many,
                          [{"text": text}])

    def test_write_error_unicode(self):
        coll = self.db.test
        self.addCleanup(coll.drop)

        coll.create_index('a', unique=True)
        coll.insert_one({'a': u'unicode \U0001f40d'})
        with self.assertRaisesRegex(
                DuplicateKeyError,
                'E11000 duplicate key error') as ctx:
            coll.insert_one({'a': u'unicode \U0001f40d'})

        # Once more for good measure.
        self.assertIn('E11000 duplicate key error',
                      str(ctx.exception))

        if sys.version_info[0] == 2:
            # Test unicode(error) conversion.
            self.assertIn('E11000 duplicate key error',
                          unicode(ctx.exception))


    def test_wtimeout(self):
        # Ensure setting wtimeout doesn't disable write concern altogether.
        # See SERVER-12596.
        collection = self.db.test
        collection.drop()
        collection.insert_one({'_id': 1})

        coll = collection.with_options(
            write_concern=WriteConcern(w=1, wtimeout=1000))
        self.assertRaises(DuplicateKeyError, coll.insert_one, {'_id': 1})

        coll = collection.with_options(
            write_concern=WriteConcern(wtimeout=1000))
        self.assertRaises(DuplicateKeyError, coll.insert_one, {'_id': 1})

    def test_error_code(self):
        try:
            self.db.test.update_many({}, {"$thismodifierdoesntexist": 1})
        except OperationFailure as exc:
            self.assertTrue(exc.code in (9, 10147, 16840, 17009))
            # Just check that we set the error document. Fields
            # vary by MongoDB version.
            self.assertTrue(exc.details is not None)
        else:
            self.fail("OperationFailure was not raised")

    def test_index_on_subfield(self):
        db = self.db
        db.drop_collection("test")

        db.test.insert_one({"hello": {"a": 4, "b": 5}})
        db.test.insert_one({"hello": {"a": 7, "b": 2}})
        db.test.insert_one({"hello": {"a": 4, "b": 10}})

        db.drop_collection("test")
        db.test.create_index("hello.a", unique=True)

        db.test.insert_one({"hello": {"a": 4, "b": 5}})
        db.test.insert_one({"hello": {"a": 7, "b": 2}})
        self.assertRaises(DuplicateKeyError,
                          db.test.insert_one,
                          {"hello": {"a": 4, "b": 10}})

    def test_replace_one(self):
        db = self.db
        db.drop_collection("test")

        self.assertRaises(ValueError,
                          lambda: db.test.replace_one({}, {"$set": {"x": 1}}))

        id1 = db.test.insert_one({"x": 1}).inserted_id
        result = db.test.replace_one({"x": 1}, {"y": 1})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(1, result.matched_count)
        self.assertTrue(result.modified_count in (None, 1))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual(1, db.test.count_documents({"y": 1}))
        self.assertEqual(0, db.test.count_documents({"x": 1}))
        self.assertEqual(db.test.find_one(id1)["y"], 1)

        replacement = RawBSONDocument(encode({"_id": id1, "z": 1}))
        result = db.test.replace_one({"y": 1}, replacement, True)
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(1, result.matched_count)
        self.assertTrue(result.modified_count in (None, 1))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual(1, db.test.count_documents({"z": 1}))
        self.assertEqual(0, db.test.count_documents({"y": 1}))
        self.assertEqual(db.test.find_one(id1)["z"], 1)

        result = db.test.replace_one({"x": 2}, {"y": 2}, True)
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(0, result.matched_count)
        self.assertTrue(result.modified_count in (None, 0))
        self.assertTrue(isinstance(result.upserted_id, ObjectId))
        self.assertTrue(result.acknowledged)
        self.assertEqual(1, db.test.count_documents({"y": 2}))

        db = db.client.get_database(db.name,
                                    write_concern=WriteConcern(w=0))
        result = db.test.replace_one({"x": 0}, {"y": 0})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertRaises(InvalidOperation, lambda: result.matched_count)
        self.assertRaises(InvalidOperation, lambda: result.modified_count)
        self.assertRaises(InvalidOperation, lambda: result.upserted_id)
        self.assertFalse(result.acknowledged)

    def test_update_one(self):
        db = self.db
        db.drop_collection("test")

        self.assertRaises(ValueError,
                          lambda: db.test.update_one({}, {"x": 1}))

        id1 = db.test.insert_one({"x": 5}).inserted_id
        result = db.test.update_one({}, {"$inc": {"x": 1}})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(1, result.matched_count)
        self.assertTrue(result.modified_count in (None, 1))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual(db.test.find_one(id1)["x"], 6)

        id2 = db.test.insert_one({"x": 1}).inserted_id
        result = db.test.update_one({"x": 6}, {"$inc": {"x": 1}})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(1, result.matched_count)
        self.assertTrue(result.modified_count in (None, 1))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual(db.test.find_one(id1)["x"], 7)
        self.assertEqual(db.test.find_one(id2)["x"], 1)

        result = db.test.update_one({"x": 2}, {"$set": {"y": 1}}, True)
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(0, result.matched_count)
        self.assertTrue(result.modified_count in (None, 0))
        self.assertTrue(isinstance(result.upserted_id, ObjectId))
        self.assertTrue(result.acknowledged)

        db = db.client.get_database(db.name,
                                    write_concern=WriteConcern(w=0))
        result = db.test.update_one({"x": 0}, {"$inc": {"x": 1}})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertRaises(InvalidOperation, lambda: result.matched_count)
        self.assertRaises(InvalidOperation, lambda: result.modified_count)
        self.assertRaises(InvalidOperation, lambda: result.upserted_id)
        self.assertFalse(result.acknowledged)

    def test_update_many(self):
        db = self.db
        db.drop_collection("test")

        self.assertRaises(ValueError,
                          lambda: db.test.update_many({}, {"x": 1}))

        db.test.insert_one({"x": 4, "y": 3})
        db.test.insert_one({"x": 5, "y": 5})
        db.test.insert_one({"x": 4, "y": 4})

        result = db.test.update_many({"x": 4}, {"$set": {"y": 5}})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(2, result.matched_count)
        self.assertTrue(result.modified_count in (None, 2))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual(3, db.test.count_documents({"y": 5}))

        result = db.test.update_many({"x": 5}, {"$set": {"y": 6}})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(1, result.matched_count)
        self.assertTrue(result.modified_count in (None, 1))
        self.assertIsNone(result.upserted_id)
        self.assertTrue(result.acknowledged)
        self.assertEqual(1, db.test.count_documents({"y": 6}))

        result = db.test.update_many({"x": 2}, {"$set": {"y": 1}}, True)
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(0, result.matched_count)
        self.assertTrue(result.modified_count in (None, 0))
        self.assertTrue(isinstance(result.upserted_id, ObjectId))
        self.assertTrue(result.acknowledged)

        db = db.client.get_database(db.name,
                                    write_concern=WriteConcern(w=0))
        result = db.test.update_many({"x": 0}, {"$inc": {"x": 1}})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertRaises(InvalidOperation, lambda: result.matched_count)
        self.assertRaises(InvalidOperation, lambda: result.modified_count)
        self.assertRaises(InvalidOperation, lambda: result.upserted_id)
        self.assertFalse(result.acknowledged)

    # MongoDB >= 3.5.8 allows dotted fields in updates
    @client_context.require_version_max(3, 5, 7)
    def test_update_with_invalid_keys(self):
        self.db.drop_collection("test")
        self.assertTrue(self.db.test.insert_one({"hello": "world"}))
        doc = self.db.test.find_one()
        doc['a.b'] = 'c'

        # Replace
        self.assertRaises(OperationFailure, self.db.test.replace_one,
                          {"hello": "world"}, doc)
        # Upsert
        self.assertRaises(OperationFailure, self.db.test.replace_one,
                          {"foo": "bar"}, doc, upsert=True)

        # Check that the last two ops didn't actually modify anything
        self.assertTrue('a.b' not in self.db.test.find_one())

    def test_update_check_keys(self):
        self.db.drop_collection("test")
        self.assertTrue(self.db.test.insert_one({"hello": "world"}))

        # Modify shouldn't check keys...
        self.assertTrue(self.db.test.update_one({"hello": "world"},
                                                {"$set": {"foo.bar": "baz"}},
                                                upsert=True))

        # I know this seems like testing the server but I'd like to be notified
        # by CI if the server's behavior changes here.
        doc = SON([("$set", {"foo.bar": "bim"}), ("hello", "world")])
        self.assertRaises(OperationFailure, self.db.test.update_one,
                          {"hello": "world"}, doc, upsert=True)

        # This is going to cause keys to be checked and raise InvalidDocument.
        # That's OK assuming the server's behavior in the previous assert
        # doesn't change. If the behavior changes checking the first key for
        # '$' in update won't be good enough anymore.
        doc = SON([("hello", "world"), ("$set", {"foo.bar": "bim"})])
        self.assertRaises(OperationFailure, self.db.test.replace_one,
                          {"hello": "world"}, doc, upsert=True)

        # Replace with empty document
        self.assertNotEqual(0,
                            self.db.test.replace_one(
                                {"hello": "world"}, {}).matched_count)

    def test_acknowledged_delete(self):
        db = self.db
        db.drop_collection("test")
        db.create_collection("test", capped=True, size=1000)

        db.test.insert_one({"x": 1})
        self.assertEqual(1, db.test.count_documents({}))

        # Can't remove from capped collection.
        self.assertRaises(OperationFailure, db.test.delete_one, {"x": 1})
        db.drop_collection("test")
        db.test.insert_one({"x": 1})
        db.test.insert_one({"x": 1})
        self.assertEqual(2, db.test.delete_many({}).deleted_count)
        self.assertEqual(0, db.test.delete_many({}).deleted_count)

    def test_manual_last_error(self):
        coll = self.db.get_collection("test", write_concern=WriteConcern(w=0))
        coll.insert_one({"x": 1})
        self.db.command("getlasterror", w=1, wtimeout=1)

    @ignore_deprecations
    def test_count(self):
        db = self.db
        db.drop_collection("test")

        self.assertEqual(db.test.count(), 0)
        db.test.insert_many([{}, {}])
        self.assertEqual(db.test.count(), 2)
        db.test.insert_many([{'foo': 'bar'}, {'foo': 'baz'}])
        self.assertEqual(db.test.find({'foo': 'bar'}).count(), 1)
        self.assertEqual(db.test.count({'foo': 'bar'}), 1)
        self.assertEqual(db.test.find({'foo': re.compile(r'ba.*')}).count(), 2)
        self.assertEqual(
            db.test.count({'foo': re.compile(r'ba.*')}), 2)

    def test_count_documents(self):
        db = self.db
        db.drop_collection("test")
        self.addCleanup(db.drop_collection, "test")

        self.assertEqual(db.test.count_documents({}), 0)
        db.wrong.insert_many([{}, {}])
        self.assertEqual(db.test.count_documents({}), 0)
        db.test.insert_many([{}, {}])
        self.assertEqual(db.test.count_documents({}), 2)
        db.test.insert_many([{'foo': 'bar'}, {'foo': 'baz'}])
        self.assertEqual(db.test.count_documents({'foo': 'bar'}), 1)
        self.assertEqual(
            db.test.count_documents({'foo': re.compile(r'ba.*')}), 2)

    def test_estimated_document_count(self):
        db = self.db
        db.drop_collection("test")
        self.addCleanup(db.drop_collection, "test")

        self.assertEqual(db.test.estimated_document_count(), 0)
        db.wrong.insert_many([{}, {}])
        self.assertEqual(db.test.estimated_document_count(), 0)
        db.test.insert_many([{}, {}])
        self.assertEqual(db.test.estimated_document_count(), 2)

    def test_aggregate(self):
        db = self.db
        db.drop_collection("test")
        db.test.insert_one({'foo': [1, 2]})

        self.assertRaises(TypeError, db.test.aggregate, "wow")

        pipeline = {"$project": {"_id": False, "foo": True}}
        # MongoDB 3.5.1+ requires either the 'cursor' or 'explain' options.
        if client_context.version.at_least(3, 5, 1):
            result = db.test.aggregate([pipeline])
        else:
            result = db.test.aggregate([pipeline], useCursor=False)

        self.assertTrue(isinstance(result, CommandCursor))
        self.assertEqual([{'foo': [1, 2]}], list(result))

        # Test write concern.
        with self.write_concern_collection() as coll:
            coll.aggregate([{'$out': 'output-collection'}])

    def test_aggregate_raw_bson(self):
        db = self.db
        db.drop_collection("test")
        db.test.insert_one({'foo': [1, 2]})

        self.assertRaises(TypeError, db.test.aggregate, "wow")

        pipeline = {"$project": {"_id": False, "foo": True}}
        coll = db.get_collection(
            'test',
            codec_options=CodecOptions(document_class=RawBSONDocument))
        # MongoDB 3.5.1+ requires either the 'cursor' or 'explain' options.
        if client_context.version.at_least(3, 5, 1):
            result = coll.aggregate([pipeline])
        else:
            result = coll.aggregate([pipeline], useCursor=False)
        self.assertTrue(isinstance(result, CommandCursor))
        first_result = next(result)
        self.assertIsInstance(first_result, RawBSONDocument)
        self.assertEqual([1, 2], list(first_result['foo']))

    def test_aggregation_cursor_validation(self):
        db = self.db
        projection = {'$project': {'_id': '$_id'}}
        cursor = db.test.aggregate([projection], cursor={})
        self.assertTrue(isinstance(cursor, CommandCursor))

        cursor = db.test.aggregate([projection], useCursor=True)
        self.assertTrue(isinstance(cursor, CommandCursor))

    def test_aggregation_cursor(self):
        db = self.db
        if client_context.has_secondaries:
            # Test that getMore messages are sent to the right server.
            db = self.client.get_database(
                db.name,
                read_preference=ReadPreference.SECONDARY,
                write_concern=WriteConcern(w=self.w))

        for collection_size in (10, 1000):
            db.drop_collection("test")
            db.test.insert_many([{'_id': i} for i in range(collection_size)])
            expected_sum = sum(range(collection_size))
            # Use batchSize to ensure multiple getMore messages
            cursor = db.test.aggregate(
                [{'$project': {'_id': '$_id'}}],
                batchSize=5)

            self.assertEqual(
                expected_sum,
                sum(doc['_id'] for doc in cursor))

        # Test that batchSize is handled properly.
        cursor = db.test.aggregate([], batchSize=5)
        self.assertEqual(5, len(cursor._CommandCursor__data))
        # Force a getMore
        cursor._CommandCursor__data.clear()
        next(cursor)
        # batchSize - 1
        self.assertEqual(4, len(cursor._CommandCursor__data))
        # Exhaust the cursor. There shouldn't be any errors.
        for doc in cursor:
            pass

    def test_aggregation_cursor_alive(self):
        self.db.test.delete_many({})
        self.db.test.insert_many([{} for _ in range(3)])
        self.addCleanup(self.db.test.delete_many, {})
        cursor = self.db.test.aggregate(pipeline=[], cursor={'batchSize': 2})
        n = 0
        while True:
            cursor.next()
            n += 1
            if 3 == n:
                self.assertFalse(cursor.alive)
                break

            self.assertTrue(cursor.alive)

    @client_context.require_no_mongos
    @client_context.require_version_max(4, 1, 0)
    @ignore_deprecations
    def test_parallel_scan(self):
        db = self.db
        db.drop_collection("test")
        if client_context.has_secondaries:
            # Test that getMore messages are sent to the right server.
            db = self.client.get_database(
                db.name,
                read_preference=ReadPreference.SECONDARY,
                write_concern=WriteConcern(w=self.w))

        coll = db.test
        coll.insert_many([{'_id': i} for i in range(8000)])
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

    @client_context.require_no_mongos
    @client_context.require_version_min(3, 3, 10)
    @client_context.require_version_max(4, 1, 0)
    @client_context.require_test_commands
    @ignore_deprecations
    def test_parallel_scan_max_time_ms(self):
            self.client.admin.command("configureFailPoint",
                                      "maxTimeAlwaysTimeOut",
                                      mode="alwaysOn")
            try:
                self.assertRaises(ExecutionTimeout,
                                  self.db.test.parallel_scan,
                                  3,
                                  maxTimeMS=1)
            finally:
                self.client.admin.command("configureFailPoint",
                                          "maxTimeAlwaysTimeOut",
                                          mode="off")

    def test_large_limit(self):
        db = self.db
        db.drop_collection("test_large_limit")
        db.test_large_limit.create_index([('x', 1)])
        my_str = "mongomongo" * 1000

        db.test_large_limit.insert_many(
            {"x": i, "y": my_str} for i in range(2000))

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
        db.test.insert_many({"x": i} for i in range(10))

        self.assertEqual(10, db.test.count_documents({}))

        total = 0
        for x in db.test.find({}, skip=4, limit=2):
            total += x["x"]

        self.assertEqual(9, total)

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

        self.assertEqual(0, db.test.count_documents({}))
        self.assertEqual(0, db.foo.count_documents({}))

        db.test.insert_many({"x": i} for i in range(10))

        self.assertEqual(10, db.test.count_documents({}))

        db.test.rename("foo")

        self.assertEqual(0, db.test.count_documents({}))
        self.assertEqual(10, db.foo.count_documents({}))

        x = 0
        for doc in db.foo.find():
            self.assertEqual(x, doc["x"])
            x += 1

        db.test.insert_one({})
        self.assertRaises(OperationFailure, db.foo.rename, "test")
        db.foo.rename("test", dropTarget=True)

        with self.write_concern_collection() as coll:
            coll.rename('foo')

    def test_find_one(self):
        db = self.db
        db.drop_collection("test")

        _id = db.test.insert_one({"hello": "world", "foo": "bar"}).inserted_id

        self.assertEqual("world", db.test.find_one()["hello"])
        self.assertEqual(db.test.find_one(_id), db.test.find_one())
        self.assertEqual(db.test.find_one(None), db.test.find_one())
        self.assertEqual(db.test.find_one({}), db.test.find_one())
        self.assertEqual(db.test.find_one({"hello": "world"}),
                         db.test.find_one())

        self.assertTrue("hello" in db.test.find_one(projection=["hello"]))
        self.assertTrue("hello" not in db.test.find_one(projection=["foo"]))

        self.assertTrue("hello" in db.test.find_one(projection=("hello",)))
        self.assertTrue("hello" not in db.test.find_one(projection=("foo",)))

        self.assertTrue("hello" in db.test.find_one(projection=set(["hello"])))
        self.assertTrue("hello" not in db.test.find_one(projection=set(["foo"])))

        self.assertTrue("hello" in db.test.find_one(projection=frozenset(["hello"])))
        self.assertTrue("hello" not in db.test.find_one(projection=frozenset(["foo"])))

        self.assertEqual(["_id"], list(db.test.find_one(projection=[])))

        self.assertEqual(None, db.test.find_one({"hello": "foo"}))
        self.assertEqual(None, db.test.find_one(ObjectId()))

    def test_find_one_non_objectid(self):
        db = self.db
        db.drop_collection("test")

        db.test.insert_one({"_id": 5})

        self.assertTrue(db.test.find_one(5))
        self.assertFalse(db.test.find_one(6))

    def test_find_one_with_find_args(self):
        db = self.db
        db.drop_collection("test")

        db.test.insert_many([{"x": i} for i in range(1, 4)])

        self.assertEqual(1, db.test.find_one()["x"])
        self.assertEqual(2, db.test.find_one(skip=1, limit=2)["x"])

    def test_find_with_sort(self):
        db = self.db
        db.drop_collection("test")

        db.test.insert_many([{"x": 2}, {"x": 1}, {"x": 3}])

        self.assertEqual(2, db.test.find_one()["x"])
        self.assertEqual(1, db.test.find_one(sort=[("x", 1)])["x"])
        self.assertEqual(3, db.test.find_one(sort=[("x", -1)])["x"])

        def to_list(things):
            return [thing["x"] for thing in things]

        self.assertEqual([2, 1, 3], to_list(db.test.find()))
        self.assertEqual([1, 2, 3], to_list(db.test.find(sort=[("x", 1)])))
        self.assertEqual([3, 2, 1], to_list(db.test.find(sort=[("x", -1)])))

        self.assertRaises(TypeError, db.test.find, sort=5)
        self.assertRaises(TypeError, db.test.find, sort="hello")
        self.assertRaises(ValueError, db.test.find, sort=["hello", 1])

    # TODO doesn't actually test functionality, just that it doesn't blow up
    def test_cursor_timeout(self):
        list(self.db.test.find(no_cursor_timeout=True))
        list(self.db.test.find(no_cursor_timeout=False))

    def test_exhaust(self):
        if is_mongos(self.db.client):
            self.assertRaises(InvalidOperation,
                              self.db.test.find,
                              cursor_type=CursorType.EXHAUST)
            return

        # Limit is incompatible with exhaust.
        self.assertRaises(InvalidOperation,
                          self.db.test.find,
                          cursor_type=CursorType.EXHAUST,
                          limit=5)
        cur = self.db.test.find(cursor_type=CursorType.EXHAUST)
        self.assertRaises(InvalidOperation, cur.limit, 5)
        cur = self.db.test.find(limit=5)
        self.assertRaises(InvalidOperation, cur.add_option, 64)
        cur = self.db.test.find()
        cur.add_option(64)
        self.assertRaises(InvalidOperation, cur.limit, 5)

        self.db.drop_collection("test")
        # Insert enough documents to require more than one batch
        self.db.test.insert_many([{'i': i} for i in range(150)])

        client = rs_or_single_client(maxPoolSize=1)
        socks = get_pool(client).sockets

        # Make sure the socket is returned after exhaustion.
        cur = client[self.db.name].test.find(cursor_type=CursorType.EXHAUST)
        next(cur)
        self.assertEqual(0, len(socks))
        for _ in cur:
            pass
        self.assertEqual(1, len(socks))

        # Same as previous but don't call next()
        for _ in client[self.db.name].test.find(cursor_type=CursorType.EXHAUST):
            pass
        self.assertEqual(1, len(socks))

        # If the Cursor instance is discarded before being
        # completely iterated we have to close and
        # discard the socket.
        cur = client[self.db.name].test.find(cursor_type=CursorType.EXHAUST)
        next(cur)
        self.assertEqual(0, len(socks))
        if sys.platform.startswith('java') or 'PyPy' in sys.version:
            # Don't wait for GC or use gc.collect(), it's unreliable.
            cur.close()
        cur = None
        # The socket should be discarded.
        self.assertEqual(0, len(socks))

    def test_distinct(self):
        self.db.drop_collection("test")

        test = self.db.test
        test.insert_many([{"a": 1}, {"a": 2}, {"a": 2}, {"a": 2}, {"a": 3}])

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

        test.insert_one({"a": {"b": "a"}, "c": 12})
        test.insert_one({"a": {"b": "b"}, "c": 12})
        test.insert_one({"a": {"b": "c"}, "c": 12})
        test.insert_one({"a": {"b": "c"}, "c": 12})

        distinct = test.distinct("a.b")
        distinct.sort()

        self.assertEqual(["a", "b", "c"], distinct)

    def test_query_on_query_field(self):
        self.db.drop_collection("test")
        self.db.test.insert_one({"query": "foo"})
        self.db.test.insert_one({"bar": "foo"})

        self.assertEqual(1,
                         self.db.test.count_documents({"query": {"$ne": None}}))
        self.assertEqual(1,
                         len(list(self.db.test.find({"query": {"$ne": None}})))
                        )

    def test_min_query(self):
        self.db.drop_collection("test")
        self.db.test.insert_many([{"x": 1}, {"x": 2}])
        self.db.test.create_index("x")

        cursor = self.db.test.find({"$min": {"x": 2}, "$query": {}})
        if client_context.requires_hint_with_min_max_queries:
            cursor = cursor.hint("x_1")

        docs = list(cursor)
        self.assertEqual(1, len(docs))
        self.assertEqual(2, docs[0]["x"])

    def test_numerous_inserts(self):
        # Ensure we don't exceed server's 1000-document batch size limit.
        self.db.test.drop()
        n_docs = 2100
        self.db.test.insert_many([{} for _ in range(n_docs)])
        self.assertEqual(n_docs, self.db.test.count_documents({}))
        self.db.test.drop()

    def test_map_reduce(self):
        db = self.db
        db.drop_collection("test")

        db.test.insert_one({"id": 1, "tags": ["dog", "cat"]})
        db.test.insert_one({"id": 2, "tags": ["cat"]})
        db.test.insert_one({"id": 3, "tags": ["mouse", "cat", "dog"]})
        db.test.insert_one({"id": 4, "tags": []})

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

        db.test.insert_one({"id": 5, "tags": ["hampster"]})
        result = db.test.map_reduce(map, reduce, out='mrunittests')
        self.assertEqual(1, result.find_one({"_id": "hampster"})["value"])
        db.test.delete_one({"id": 5})

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

        # Create the output database.
        db.client.mrtestdb.mrunittests.insert_one({})
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
        self.assertEqual('mrunittests', full_result["result"])
        if client_context.version < (4, 3):
            self.assertEqual(6, full_result["counts"]["emit"])

        result = db.test.map_reduce(map, reduce, out='mrunittests', limit=2)
        self.assertEqual(2, result.find_one({"_id": "cat"})["value"])
        self.assertEqual(1, result.find_one({"_id": "dog"})["value"])
        self.assertEqual(None, result.find_one({"_id": "mouse"}))

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
        self.assertEqual(3, len(full_result["results"]))
        if client_context.version < (4, 3):
            self.assertEqual(6, full_result["counts"]["emit"])

        with self.write_concern_collection() as coll:
            coll.map_reduce(map, reduce, 'output')

    def test_messages_with_unicode_collection_names(self):
        db = self.db

        db[u"Employés"].insert_one({"x": 1})
        db[u"Employés"].replace_one({"x": 1}, {"x": 2})
        db[u"Employés"].delete_many({})
        db[u"Employés"].find_one()
        list(db[u"Employés"].find())

    def test_drop_indexes_non_existent(self):
        self.db.drop_collection("test")
        self.db.test.drop_indexes()

    # This is really a bson test but easier to just reproduce it here...
    # (Shame on me)
    def test_bad_encode(self):
        c = self.db.test
        c.drop()
        self.assertRaises(InvalidDocument, c.insert_one, {"x": c})

        class BadGetAttr(dict):
            def __getattr__(self, name):
                pass

        bad = BadGetAttr([('foo', 'bar')])
        c.insert_one({'bad': bad})
        self.assertEqual('bar', c.find_one()['bad']['foo'])

    @client_context.require_version_max(3, 5, 5)
    def test_array_filters_unsupported(self):
        c = self.db.test
        with self.assertRaises(ConfigurationError):
            c.update_one(
                {}, {'$set': {'y.$[i].b': 5}}, array_filters=[{'i.b': 1}])
        with self.assertRaises(ConfigurationError):
            c.update_many(
                {}, {'$set': {'y.$[i].b': 5}}, array_filters=[{'i.b': 1}])
        with self.assertRaises(ConfigurationError):
            c.find_one_and_update(
                {}, {'$set': {'y.$[i].b': 5}}, array_filters=[{'i.b': 1}])

    def test_array_filters_validation(self):
        # array_filters must be a list.
        c = self.db.test
        with self.assertRaises(TypeError):
            c.update_one({}, {'$set': {'a': 1}}, array_filters={})
        with self.assertRaises(TypeError):
            c.update_many({}, {'$set': {'a': 1}}, array_filters={})
        with self.assertRaises(TypeError):
            c.find_one_and_update({}, {'$set': {'a': 1}}, array_filters={})

    def test_array_filters_unacknowledged(self):
        c_w0 = self.db.test.with_options(write_concern=WriteConcern(w=0))
        with self.assertRaises(ConfigurationError):
            c_w0.update_one({}, {'$set': {'y.$[i].b': 5}},
                            array_filters=[{'i.b': 1}])
        with self.assertRaises(ConfigurationError):
            c_w0.update_many({}, {'$set': {'y.$[i].b': 5}},
                             array_filters=[{'i.b': 1}])
        with self.assertRaises(ConfigurationError):
            c_w0.find_one_and_update({}, {'$set': {'y.$[i].b': 5}},
                                     array_filters=[{'i.b': 1}])

    def test_find_one_and(self):
        c = self.db.test
        c.drop()
        c.insert_one({'_id': 1, 'i': 1})

        self.assertEqual({'_id': 1, 'i': 1},
                         c.find_one_and_update({'_id': 1}, {'$inc': {'i': 1}}))
        self.assertEqual({'_id': 1, 'i': 3},
                         c.find_one_and_update(
                             {'_id': 1}, {'$inc': {'i': 1}},
                             return_document=ReturnDocument.AFTER))

        self.assertEqual({'_id': 1, 'i': 3},
                         c.find_one_and_delete({'_id': 1}))
        self.assertEqual(None, c.find_one({'_id': 1}))

        self.assertEqual(None,
                         c.find_one_and_update({'_id': 1}, {'$inc': {'i': 1}}))
        self.assertEqual({'_id': 1, 'i': 1},
                         c.find_one_and_update(
                             {'_id': 1}, {'$inc': {'i': 1}},
                             return_document=ReturnDocument.AFTER,
                             upsert=True))
        self.assertEqual({'_id': 1, 'i': 2},
                         c.find_one_and_update(
                             {'_id': 1}, {'$inc': {'i': 1}},
                             return_document=ReturnDocument.AFTER))

        self.assertEqual({'_id': 1, 'i': 3},
                         c.find_one_and_replace(
                             {'_id': 1}, {'i': 3, 'j': 1},
                             projection=['i'],
                             return_document=ReturnDocument.AFTER))
        self.assertEqual({'i': 4},
                         c.find_one_and_update(
                             {'_id': 1}, {'$inc': {'i': 1}},
                             projection={'i': 1, '_id': 0},
                             return_document=ReturnDocument.AFTER))

        c.drop()
        for j in range(5):
            c.insert_one({'j': j, 'i': 0})

        sort = [('j', DESCENDING)]
        self.assertEqual(4, c.find_one_and_update({},
                                                  {'$inc': {'i': 1}},
                                                  sort=sort)['j'])

    def test_find_one_and_write_concern(self):
        listener = EventListener()
        db = single_client(event_listeners=[listener])[self.db.name]
        # non-default WriteConcern.
        c_w0 = db.get_collection(
            'test', write_concern=WriteConcern(w=0))
        # default WriteConcern.
        c_default = db.get_collection('test', write_concern=WriteConcern())
        results = listener.results
        # Authenticate the client and throw out auth commands from the listener.
        db.command('ismaster')
        results.clear()
        if client_context.version.at_least(3, 1, 9, -1):
            c_w0.find_and_modify(
                {'_id': 1}, {'$set': {'foo': 'bar'}})
            self.assertEqual(
                {'w': 0}, results['started'][0].command['writeConcern'])
            results.clear()

            c_w0.find_one_and_update(
                {'_id': 1}, {'$set': {'foo': 'bar'}})
            self.assertEqual(
                {'w': 0}, results['started'][0].command['writeConcern'])
            results.clear()

            c_w0.find_one_and_replace({'_id': 1}, {'foo': 'bar'})
            self.assertEqual(
                {'w': 0}, results['started'][0].command['writeConcern'])
            results.clear()

            c_w0.find_one_and_delete({'_id': 1})
            self.assertEqual(
                {'w': 0}, results['started'][0].command['writeConcern'])
            results.clear()

            # Test write concern errors.
            if client_context.is_rs:
                c_wc_error = db.get_collection(
                    'test',
                    write_concern=WriteConcern(
                        w=len(client_context.nodes) + 1))
                self.assertRaises(
                    WriteConcernError,
                    c_wc_error.find_and_modify,
                    {'_id': 1}, {'$set': {'foo': 'bar'}})
                self.assertRaises(
                    WriteConcernError,
                    c_wc_error.find_one_and_update,
                    {'_id': 1}, {'$set': {'foo': 'bar'}})
                self.assertRaises(
                    WriteConcernError,
                    c_wc_error.find_one_and_replace,
                    {'w': 0}, results['started'][0].command['writeConcern'])
                self.assertRaises(
                    WriteConcernError,
                    c_wc_error.find_one_and_delete,
                    {'w': 0}, results['started'][0].command['writeConcern'])
                results.clear()
        else:
            c_w0.find_and_modify(
                {'_id': 1}, {'$set': {'foo': 'bar'}})
            self.assertNotIn('writeConcern', results['started'][0].command)
            results.clear()

            c_w0.find_one_and_update(
                {'_id': 1}, {'$set': {'foo': 'bar'}})
            self.assertNotIn('writeConcern', results['started'][0].command)
            results.clear()

            c_w0.find_one_and_replace({'_id': 1}, {'foo': 'bar'})
            self.assertNotIn('writeConcern', results['started'][0].command)
            results.clear()

            c_w0.find_one_and_delete({'_id': 1})
            self.assertNotIn('writeConcern', results['started'][0].command)
            results.clear()

        c_default.find_and_modify({'_id': 1}, {'$set': {'foo': 'bar'}})
        self.assertNotIn('writeConcern', results['started'][0].command)
        results.clear()

        c_default.find_one_and_update({'_id': 1}, {'$set': {'foo': 'bar'}})
        self.assertNotIn('writeConcern', results['started'][0].command)
        results.clear()

        c_default.find_one_and_replace({'_id': 1}, {'foo': 'bar'})
        self.assertNotIn('writeConcern', results['started'][0].command)
        results.clear()

        c_default.find_one_and_delete({'_id': 1})
        self.assertNotIn('writeConcern', results['started'][0].command)
        results.clear()

    def test_find_with_nested(self):
        c = self.db.test
        c.drop()
        c.insert_many([{'i': i} for i in range(5)])  # [0, 1, 2, 3, 4]
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

    def test_find_regex(self):
        c = self.db.test
        c.drop()
        c.insert_one({'r': re.compile('.*')})

        self.assertTrue(isinstance(c.find_one()['r'], Regex))
        for doc in c.find():
            self.assertTrue(isinstance(doc['r'], Regex))

    def test_find_command_generation(self):
        cmd = _gen_find_command('coll', {'$query': {'foo': 1}, '$dumb': 2},
                                None, 0, 0, 0, None, DEFAULT_READ_CONCERN,
                                None, None)
        self.assertEqual(
            cmd.to_dict(),
            SON([('find', 'coll'),
                 ('$dumb', 2),
                 ('filter', {'foo': 1})]).to_dict())


if __name__ == "__main__":
    unittest.main()
