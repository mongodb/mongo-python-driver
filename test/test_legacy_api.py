# Copyright 2015-present MongoDB, Inc.
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

"""Test various legacy / deprecated API features."""

import itertools
import sys
import threading
import time
import uuid
import warnings

sys.path[0:0] = [""]

from bson.binary import PYTHON_LEGACY, STANDARD
from bson.code import Code
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from bson.py3compat import string_type
from bson.son import SON
from pymongo import ASCENDING, DESCENDING, GEOHAYSTACK
from pymongo.database import Database
from pymongo.common import partition_node
from pymongo.errors import (BulkWriteError,
                            ConfigurationError,
                            CursorNotFound,
                            DocumentTooLarge,
                            DuplicateKeyError,
                            InvalidDocument,
                            InvalidOperation,
                            OperationFailure,
                            WriteConcernError,
                            WTimeoutError)
from pymongo.message import _CursorAddress
from pymongo.operations import IndexModel
from pymongo.son_manipulator import (AutoReference,
                                     NamespaceInjector,
                                     ObjectIdShuffler,
                                     SONManipulator)
from pymongo.write_concern import WriteConcern
from test import client_context, qcheck, unittest, SkipTest
from test.test_client import IntegrationTest
from test.test_bulk import BulkTestBase, BulkAuthorizationTestBase
from test.utils import (DeprecationFilter,
                        joinall,
                        oid_generated_on_process,
                        rs_or_single_client,
                        rs_or_single_client_noauth,
                        single_client,
                        wait_until)


class TestDeprecations(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        super(TestDeprecations, cls).setUpClass()
        cls.deprecation_filter = DeprecationFilter("error")

    @classmethod
    def tearDownClass(cls):
        cls.deprecation_filter.stop()

    def test_save_deprecation(self):
        self.assertRaises(
            DeprecationWarning, lambda: self.db.test.save({}))

    def test_insert_deprecation(self):
        self.assertRaises(
            DeprecationWarning, lambda: self.db.test.insert({}))

    def test_update_deprecation(self):
        self.assertRaises(
            DeprecationWarning, lambda: self.db.test.update({}, {}))

    def test_remove_deprecation(self):
        self.assertRaises(
            DeprecationWarning, lambda: self.db.test.remove({}))

    def test_find_and_modify_deprecation(self):
        self.assertRaises(
            DeprecationWarning,
            lambda: self.db.test.find_and_modify({'i': 5}, {}))

    def test_add_son_manipulator_deprecation(self):
        db = self.client.pymongo_test
        self.assertRaises(DeprecationWarning,
                          lambda: db.add_son_manipulator(AutoReference(db)))

    def test_ensure_index_deprecation(self):
        try:
            self.assertRaises(
                DeprecationWarning,
                lambda: self.db.test.ensure_index('i'))
        finally:
            self.db.test.drop()

    def test_reindex_deprecation(self):
        self.assertRaises(DeprecationWarning, lambda: self.db.test.reindex())

    def test_geoHaystack_deprecation(self):
        self.addCleanup(self.db.test.drop)
        keys = [("pos", GEOHAYSTACK), ("type", ASCENDING)]
        self.assertRaises(
            DeprecationWarning, self.db.test.create_index, keys, bucketSize=1)
        indexes = [IndexModel(keys, bucketSize=1)]
        self.assertRaises(
            DeprecationWarning, self.db.test.create_indexes, indexes)


class TestLegacy(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        super(TestLegacy, cls).setUpClass()
        cls.w = client_context.w
        cls.deprecation_filter = DeprecationFilter()

    @classmethod
    def tearDownClass(cls):
        cls.deprecation_filter.stop()

    def test_insert_find_one(self):
        # Tests legacy insert.
        db = self.db
        db.test.drop()
        self.assertEqual(0, len(list(db.test.find())))
        doc = {"hello": u"world"}
        _id = db.test.insert(doc)
        self.assertEqual(1, len(list(db.test.find())))
        self.assertEqual(doc, db.test.find_one())
        self.assertEqual(doc["_id"], _id)
        self.assertTrue(isinstance(_id, ObjectId))

        doc_class = dict
        # Work around http://bugs.jython.org/issue1728
        if (sys.platform.startswith('java') and
                sys.version_info[:3] >= (2, 5, 2)):
            doc_class = SON

        db = self.client.get_database(
            db.name, codec_options=CodecOptions(document_class=doc_class))

        def remove_insert_find_one(doc):
            db.test.remove({})
            db.test.insert(doc)
            # SON equality is order sensitive.
            return db.test.find_one() == doc.to_dict()

        qcheck.check_unittest(self, remove_insert_find_one,
                              qcheck.gen_mongo_dict(3))

    def test_generator_insert(self):
        # Only legacy insert currently supports insert from a generator.
        db = self.db
        db.test.remove({})
        self.assertEqual(db.test.find().count(), 0)
        db.test.insert(({'a': i} for i in range(5)), manipulate=False)
        self.assertEqual(5, db.test.count())
        db.test.remove({})

        db.test.insert(({'a': i} for i in range(5)), manipulate=True)
        self.assertEqual(5, db.test.count())
        db.test.remove({})

    def test_insert_multiple(self):
        # Tests legacy insert.
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

        ids = db.test.insert([{"hello": 1}])
        self.assertTrue(isinstance(ids, list))
        self.assertEqual(1, len(ids))

        self.assertRaises(InvalidOperation, db.test.insert, [])

        # Generator that raises StopIteration on first call to next().
        self.assertRaises(InvalidOperation, db.test.insert, (i for i in []))

    def test_insert_multiple_with_duplicate(self):
        # Tests legacy insert.
        db = self.db
        db.drop_collection("test_insert_multiple_with_duplicate")
        collection = db.test_insert_multiple_with_duplicate
        collection.create_index([('i', ASCENDING)], unique=True)

        # No error
        collection.insert([{'i': i} for i in range(5, 10)], w=0)
        wait_until(lambda: 5 == collection.count(), 'insert 5 documents')

        db.drop_collection("test_insert_multiple_with_duplicate")
        collection.create_index([('i', ASCENDING)], unique=True)

        # No error
        collection.insert([{'i': 1}] * 2, w=0)
        wait_until(lambda: 1 == collection.count(), 'insert 1 document')

        self.assertRaises(
            DuplicateKeyError,
            lambda: collection.insert([{'i': 2}] * 2),
        )

        db.drop_collection("test_insert_multiple_with_duplicate")
        db = self.client.get_database(
            db.name, write_concern=WriteConcern(w=0))

        collection = db.test_insert_multiple_with_duplicate
        collection.create_index([('i', ASCENDING)], unique=True)

        # No error.
        collection.insert([{'i': 1}] * 2)
        wait_until(lambda: 1 == collection.count(), 'insert 1 document')

        # Implied acknowledged.
        self.assertRaises(
            DuplicateKeyError,
            lambda: collection.insert([{'i': 2}] * 2, fsync=True),
        )

        # Explicit acknowledged.
        self.assertRaises(
            DuplicateKeyError,
            lambda: collection.insert([{'i': 2}] * 2, w=1))

        db.drop_collection("test_insert_multiple_with_duplicate")

    @client_context.require_replica_set
    def test_insert_prefers_write_errors(self):
        # Tests legacy insert.
        collection = self.db.test_insert_prefers_write_errors
        self.db.drop_collection(collection.name)
        collection.insert_one({'_id': 1})
        large = 's' * 1024 * 1024 * 15
        with self.assertRaises(DuplicateKeyError):
            collection.insert(
                [{'_id': 1, 's': large}, {'_id': 2, 's': large}])
        self.assertEqual(1, collection.count())

        with self.assertRaises(DuplicateKeyError):
            collection.insert(
                [{'_id': 1, 's': large}, {'_id': 2, 's': large}],
                continue_on_error=True)
        self.assertEqual(2, collection.count())
        collection.delete_one({'_id': 2})

        # A writeError followed by a writeConcernError should prefer to raise
        # the writeError.
        with self.assertRaises(DuplicateKeyError):
            collection.insert(
                [{'_id': 1, 's': large}, {'_id': 2, 's': large}],
                continue_on_error=True,
                w=len(client_context.nodes) + 10, wtimeout=1)
        self.assertEqual(2, collection.count())
        collection.delete_many({})

        with self.assertRaises(WriteConcernError):
            collection.insert(
                [{'_id': 1, 's': large}, {'_id': 2, 's': large}],
                continue_on_error=True,
                w=len(client_context.nodes) + 10, wtimeout=1)
        self.assertEqual(2, collection.count())

    def test_insert_iterables(self):
        # Tests legacy insert.
        db = self.db

        self.assertRaises(TypeError, db.test.insert, 4)
        self.assertRaises(TypeError, db.test.insert, None)
        self.assertRaises(TypeError, db.test.insert, True)

        db.drop_collection("test")
        self.assertEqual(db.test.find().count(), 0)
        db.test.insert(({"hello": u"world"}, {"hello": u"world"}))
        self.assertEqual(db.test.find().count(), 2)

        db.drop_collection("test")
        self.assertEqual(db.test.find().count(), 0)
        db.test.insert(map(lambda x: {"hello": "world"},
                           itertools.repeat(None, 10)))
        self.assertEqual(db.test.find().count(), 10)

    def test_insert_manipulate_false(self):
        # Test two aspects of legacy insert with manipulate=False:
        #   1. The return value is None or [None] as appropriate.
        #   2. _id is not set on the passed-in document object.
        collection = self.db.test_insert_manipulate_false
        collection.drop()
        oid = ObjectId()
        doc = {'a': oid}

        try:
            # The return value is None.
            self.assertTrue(collection.insert(doc, manipulate=False) is None)
            # insert() shouldn't set _id on the passed-in document object.
            self.assertEqual({'a': oid}, doc)

            # Bulk insert. The return value is a list of None.
            self.assertEqual([None], collection.insert([{}], manipulate=False))

            docs = [{}, {}]
            ids = collection.insert(docs, manipulate=False)
            self.assertEqual([None, None], ids)
            self.assertEqual([{}, {}], docs)
        finally:
            collection.drop()

    def test_continue_on_error(self):
        # Tests legacy insert.
        db = self.db
        db.drop_collection("test_continue_on_error")
        collection = db.test_continue_on_error
        oid = collection.insert({"one": 1})
        self.assertEqual(1, collection.count())

        docs = []
        docs.append({"_id": oid, "two": 2})  # Duplicate _id.
        docs.append({"three": 3})
        docs.append({"four": 4})
        docs.append({"five": 5})

        with self.assertRaises(DuplicateKeyError):
            collection.insert(docs, manipulate=False)

        self.assertEqual(1, collection.count())

        with self.assertRaises(DuplicateKeyError):
            collection.insert(docs, manipulate=False, continue_on_error=True)

        self.assertEqual(4, collection.count())

        collection.remove({}, w=client_context.w)

        oid = collection.insert({"_id": oid, "one": 1}, w=0)
        wait_until(lambda: 1 == collection.count(), 'insert 1 document')

        docs[0].pop("_id")
        docs[2]["_id"] = oid

        with self.assertRaises(DuplicateKeyError):
            collection.insert(docs, manipulate=False)

        self.assertEqual(3, collection.count())
        collection.insert(docs, manipulate=False, continue_on_error=True, w=0)
        wait_until(lambda: 6 == collection.count(), 'insert 3 documents')

    def test_acknowledged_insert(self):
        # Tests legacy insert.
        db = self.db
        db.drop_collection("test_acknowledged_insert")
        collection = db.test_acknowledged_insert

        a = {"hello": "world"}
        collection.insert(a)
        collection.insert(a, w=0)
        self.assertRaises(OperationFailure,
                          collection.insert, a)

    def test_insert_adds_id(self):
        # Tests legacy insert.
        doc = {"hello": "world"}
        self.db.test.insert(doc)
        self.assertTrue("_id" in doc)

        docs = [{"hello": "world"}, {"hello": "world"}]
        self.db.test.insert(docs)
        for doc in docs:
            self.assertTrue("_id" in doc)

    def test_insert_large_batch(self):
        # Tests legacy insert.
        db = self.client.test_insert_large_batch
        self.addCleanup(self.client.drop_database, 'test_insert_large_batch')
        max_bson_size = self.client.max_bson_size
        # Write commands are limited to 16MB + 16k per batch
        big_string = 'x' * int(max_bson_size / 2)

        # Batch insert that requires 2 batches.
        successful_insert = [{'x': big_string}, {'x': big_string},
                             {'x': big_string}, {'x': big_string}]
        db.collection_0.insert(successful_insert, w=1)
        self.assertEqual(4, db.collection_0.count())

        db.collection_0.drop()

        # Test that inserts fail after first error.
        insert_second_fails = [{'_id': 'id0', 'x': big_string},
                               {'_id': 'id0', 'x': big_string},
                               {'_id': 'id1', 'x': big_string},
                               {'_id': 'id2', 'x': big_string}]

        with self.assertRaises(DuplicateKeyError):
            db.collection_1.insert(insert_second_fails)

        self.assertEqual(1, db.collection_1.count())

        db.collection_1.drop()

        # 2 batches, 2nd insert fails, don't continue on error.
        self.assertTrue(db.collection_2.insert(insert_second_fails, w=0))
        wait_until(lambda: 1 == db.collection_2.count(),
                   'insert 1 document', timeout=60)

        db.collection_2.drop()

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

        db.collection_3.drop()

        # 2 batches, 2 errors, unacknowledged, continue on error.
        db.collection_4.insert(insert_two_failures, continue_on_error=True, w=0)

        # Only the first and third documents are inserted.
        wait_until(lambda: 2 == db.collection_4.count(),
                   'insert 2 documents', timeout=60)

        db.collection_4.drop()

    def test_bad_dbref(self):
        # Requires the legacy API to test.
        c = self.db.test
        c.drop()

        # Incomplete DBRefs.
        self.assertRaises(
            InvalidDocument,
            c.insert_one, {'ref': {'$ref': 'collection'}})

        self.assertRaises(
            InvalidDocument,
            c.insert_one, {'ref': {'$id': ObjectId()}})

        ref_only = {'ref': {'$ref': 'collection'}}
        id_only = {'ref': {'$id': ObjectId()}}


    def test_update(self):
        # Tests legacy update.
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
        # Tests legacy update.
        db = self.db
        db.drop_collection("test")
        db.test.insert({'_id': 1})
        db.test.update({'_id': 1}, {'a': 1}, manipulate=True)
        self.assertEqual(
            {'_id': 1, 'a': 1},
            db.test.find_one())

        class AddField(SONManipulator):
            def transform_incoming(self, son, dummy):
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
        # Tests legacy update.
        db = self.db
        db.drop_collection("test")
        ismaster = self.client.admin.command('ismaster')
        used_write_commands = (ismaster.get("maxWireVersion", 0) > 1)

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

    def test_multi_update(self):
        # Tests legacy update.
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
        # Tests legacy update.
        db = self.db
        db.drop_collection("test")

        db.test.update({"page": "/"}, {"$inc": {"count": 1}}, upsert=True)
        db.test.update({"page": "/"}, {"$inc": {"count": 1}}, upsert=True)

        self.assertEqual(1, db.test.count())
        self.assertEqual(2, db.test.find_one()["count"])

    def test_acknowledged_update(self):
        # Tests legacy update.
        db = self.db
        db.drop_collection("test_acknowledged_update")
        collection = db.test_acknowledged_update
        collection.create_index("x", unique=True)

        collection.insert({"x": 5})
        _id = collection.insert({"x": 4})

        self.assertEqual(
            None, collection.update({"_id": _id}, {"$inc": {"x": 1}}, w=0))

        self.assertRaises(DuplicateKeyError, collection.update,
                          {"_id": _id}, {"$inc": {"x": 1}})

        self.assertEqual(1, collection.update({"_id": _id},
                                              {"$inc": {"x": 2}})["n"])

        self.assertEqual(0, collection.update({"_id": "foo"},
                                              {"$inc": {"x": 2}})["n"])
        db.drop_collection("test_acknowledged_update")

    def test_update_backward_compat(self):
        # MongoDB versions >= 2.6.0 don't return the updatedExisting field
        # and return upsert _id in an array subdocument. This test should
        # pass regardless of server version or type (mongod/s).
        # Tests legacy update.
        c = self.db.test
        c.drop()
        oid = ObjectId()
        res = c.update({'_id': oid}, {'$set': {'a': 'a'}}, upsert=True)
        self.assertFalse(res.get('updatedExisting'))
        self.assertEqual(oid, res.get('upserted'))

        res = c.update({'_id': oid}, {'$set': {'b': 'b'}})
        self.assertTrue(res.get('updatedExisting'))

    def test_save(self):
        # Tests legacy save.
        self.db.drop_collection("test_save")
        collection = self.db.test_save

        # Save a doc with autogenerated id
        _id = collection.save({"hello": "world"})
        self.assertEqual(collection.find_one()["_id"], _id)
        self.assertTrue(isinstance(_id, ObjectId))

        # Save a doc with explicit id
        collection.save({"_id": "explicit_id", "hello": "bar"})
        doc = collection.find_one({"_id": "explicit_id"})
        self.assertEqual(doc['_id'], 'explicit_id')
        self.assertEqual(doc['hello'], 'bar')

        # Save docs with _id field already present (shouldn't create new docs)
        self.assertEqual(2, collection.count())
        collection.save({'_id': _id, 'hello': 'world'})
        self.assertEqual(2, collection.count())
        collection.save({'_id': 'explicit_id', 'hello': 'baz'})
        self.assertEqual(2, collection.count())
        self.assertEqual(
            'baz',
            collection.find_one({'_id': 'explicit_id'})['hello']
        )

        # Acknowledged mode.
        collection.create_index("hello", unique=True)
        # No exception, even though we duplicate the first doc's "hello" value
        collection.save({'_id': 'explicit_id', 'hello': 'world'}, w=0)

        self.assertRaises(
            DuplicateKeyError,
            collection.save,
            {'_id': 'explicit_id', 'hello': 'world'})
        self.db.drop_collection("test")

    def test_save_with_invalid_key(self):
        if client_context.version.at_least(3, 5, 8):
            raise SkipTest("MongoDB >= 3.5.8 allows dotted fields in updates")
        # Tests legacy save.
        self.db.drop_collection("test")
        self.assertTrue(self.db.test.insert({"hello": "world"}))
        doc = self.db.test.find_one()
        doc['a.b'] = 'c'
        self.assertRaises(OperationFailure, self.db.test.save, doc)

    def test_acknowledged_save(self):
        # Tests legacy save.
        db = self.db
        db.drop_collection("test_acknowledged_save")
        collection = db.test_acknowledged_save
        collection.create_index("hello", unique=True)

        collection.save({"hello": "world"})
        collection.save({"hello": "world"}, w=0)
        self.assertRaises(DuplicateKeyError, collection.save,
                          {"hello": "world"})
        db.drop_collection("test_acknowledged_save")

    def test_save_adds_id(self):
        # Tests legacy save.
        doc = {"hello": "jesse"}
        self.db.test.save(doc)
        self.assertTrue("_id" in doc)

    def test_save_returns_id(self):
        doc = {"hello": "jesse"}
        _id = self.db.test.save(doc)
        self.assertTrue(isinstance(_id, ObjectId))
        self.assertEqual(_id, doc["_id"])
        doc["hi"] = "bernie"
        _id = self.db.test.save(doc)
        self.assertTrue(isinstance(_id, ObjectId))
        self.assertEqual(_id, doc["_id"])

    def test_remove_one(self):
        # Tests legacy remove.
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
        # Tests legacy remove.
        self.db.test.remove()
        self.assertEqual(0, self.db.test.count())

        self.db.test.insert({"x": 1})
        self.db.test.insert({"y": 1})
        self.assertEqual(2, self.db.test.count())

        self.db.test.remove()
        self.assertEqual(0, self.db.test.count())

    def test_remove_non_objectid(self):
        # Tests legacy remove.
        db = self.db
        db.drop_collection("test")

        db.test.insert_one({"_id": 5})

        self.assertEqual(1, db.test.count())
        db.test.remove(5)
        self.assertEqual(0, db.test.count())

    def test_write_large_document(self):
        # Tests legacy insert, save, and update.
        max_size = self.db.client.max_bson_size
        half_size = int(max_size / 2)
        self.assertEqual(max_size, 16777216)

        self.assertRaises(OperationFailure, self.db.test.insert,
                          {"foo": "x" * max_size})
        self.assertRaises(OperationFailure, self.db.test.save,
                          {"foo": "x" * max_size})
        self.assertRaises(OperationFailure, self.db.test.insert,
                          [{"x": 1}, {"foo": "x" * max_size}])
        self.db.test.insert([{"foo": "x" * half_size},
                             {"foo": "x" * half_size}])

        self.db.test.insert({"bar": "x"})
        # Use w=0 here to test legacy doc size checking in all server versions
        self.assertRaises(DocumentTooLarge, self.db.test.update,
                          {"bar": "x"}, {"bar": "x" * (max_size - 14)}, w=0)
        # This will pass with OP_UPDATE or the update command.
        self.db.test.update({"bar": "x"}, {"bar": "x" * (max_size - 32)})

    def test_last_error_options(self):
        # Tests legacy write methods.
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
        self.assertRaises(ConfigurationError, self.db.test.insert,
                          {"_id": 1}, j=True, fsync=True)

    def test_find_and_modify(self):
        c = self.db.test
        c.drop()
        c.insert({'_id': 1, 'i': 1})

        # Test that we raise DuplicateKeyError when appropriate.
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
        self.assertEqual(None, c.find_and_modify({'_id': 1},
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

        # Test with full_response=True.
        result = c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                   new=True, upsert=True,
                                   full_response=True,
                                   fields={'i': 1})
        self.assertEqual({'_id': 1, 'i': 5}, result["value"])
        self.assertEqual(True,
                         result["lastErrorObject"]["updatedExisting"])

        result = c.find_and_modify({'_id': 2}, {'$inc': {'i': 1}},
                                   new=True, upsert=True,
                                   full_response=True,
                                   fields={'i': 1})
        self.assertEqual({'_id': 2, 'i': 1}, result["value"])
        self.assertEqual(False,
                         result["lastErrorObject"]["updatedExisting"])

        class ExtendedDict(dict):
            pass

        result = c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                   new=True, fields={'i': 1})
        self.assertFalse(isinstance(result, ExtendedDict))
        c = self.db.get_collection(
            "test", codec_options=CodecOptions(document_class=ExtendedDict))
        result = c.find_and_modify({'_id': 1}, {'$inc': {'i': 1}},
                                   new=True, fields={'i': 1})
        self.assertTrue(isinstance(result, ExtendedDict))

    def test_find_and_modify_with_sort(self):
        c = self.db.test
        c.drop()
        for j in range(5):
            c.insert({'j': j, 'i': 0})

        sort = {'j': DESCENDING}
        self.assertEqual(4, c.find_and_modify({},
                                              {'$inc': {'i': 1}},
                                              sort=sort)['j'])
        sort = {'j': ASCENDING}
        self.assertEqual(0, c.find_and_modify({},
                                              {'$inc': {'i': 1}},
                                              sort=sort)['j'])
        sort = [('j', DESCENDING)]
        self.assertEqual(4, c.find_and_modify({},
                                              {'$inc': {'i': 1}},
                                              sort=sort)['j'])
        sort = [('j', ASCENDING)]
        self.assertEqual(0, c.find_and_modify({},
                                              {'$inc': {'i': 1}},
                                              sort=sort)['j'])
        sort = SON([('j', DESCENDING)])
        self.assertEqual(4, c.find_and_modify({},
                                              {'$inc': {'i': 1}},
                                              sort=sort)['j'])
        sort = SON([('j', ASCENDING)])
        self.assertEqual(0, c.find_and_modify({},
                                              {'$inc': {'i': 1}},
                                              sort=sort)['j'])

        try:
            from collections import OrderedDict
            sort = OrderedDict([('j', DESCENDING)])
            self.assertEqual(4, c.find_and_modify({},
                                                  {'$inc': {'i': 1}},
                                                  sort=sort)['j'])
            sort = OrderedDict([('j', ASCENDING)])
            self.assertEqual(0, c.find_and_modify({},
                                                  {'$inc': {'i': 1}},
                                                  sort=sort)['j'])
        except ImportError:
            pass
        # Test that a standard dict with two keys is rejected.
        sort = {'j': DESCENDING, 'foo': DESCENDING}
        self.assertRaises(TypeError, c.find_and_modify,
                          {}, {'$inc': {'i': 1}}, sort=sort)

    def test_find_and_modify_with_manipulator(self):
        class AddCollectionNameManipulator(SONManipulator):
            def will_copy(self):
                return True

            def transform_incoming(self, son, dummy):
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

    @client_context.require_version_max(4, 1, 0, -1)
    def test_group(self):
        db = self.db
        db.drop_collection("test")

        self.assertEqual([],
                         db.test.group([], {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"
                                      ))

        db.test.insert_many([{"a": 2}, {"b": 5}, {"a": 1}])

        self.assertEqual([{"count": 3}],
                         db.test.group([], {}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"
                                      ))

        self.assertEqual([{"count": 1}],
                         db.test.group([], {"a": {"$gt": 1}}, {"count": 0},
                                       "function (obj, prev) { prev.count++; }"
                                      ))

        db.test.insert_one({"a": 2, "b": 3})

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

    @client_context.require_version_max(4, 1, 0, -1)
    def test_group_with_scope(self):
        db = self.db
        db.drop_collection("test")
        db.test.insert_many([{"a": 1}, {"b": 1}])

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

        self.assertEqual(2, db.test.group(
            [], {}, {"count": 0},
            Code(reduce_function, {"inc_value": 1}))[0]['count'])

        self.assertEqual(4, db.test.group(
            [], {}, {"count": 0},
            Code(reduce_function, {"inc_value": 2}))[0]['count'])

        self.assertEqual(1, db.test.group(
            [], {}, {"count": 0},
            Code(reduce_function, {"inc_value": 0.5}))[0]['count'])

    @client_context.require_version_max(4, 1, 0, -1)
    def test_group_uuid_representation(self):
        db = self.db
        coll = db.uuid
        coll.drop()
        uu = uuid.uuid4()
        coll.insert_one({"_id": uu, "a": 2})
        coll.insert_one({"_id": uuid.uuid4(), "a": 1})

        reduce = "function (obj, prev) { prev.count++; }"
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=STANDARD))
        self.assertEqual([],
                         coll.group([], {"_id": uu},
                                    {"count": 0}, reduce))
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual([{"count": 1}],
                         coll.group([], {"_id": uu},
                                    {"count": 0}, reduce))

    def test_last_status(self):
        # Tests many legacy API elements.
        # We must call getlasterror on same socket as the last operation.
        db = rs_or_single_client(maxPoolSize=1).pymongo_test
        collection = db.test_last_status
        collection.remove({})
        collection.save({"i": 1})

        collection.update({"i": 1}, {"$set": {"i": 2}}, w=0)
        # updatedExisting is always false on mongos after an OP_MSG
        # unacknowledged write.
        if not (client_context.version >= (3, 6) and client_context.is_mongos):
            self.assertTrue(db.last_status()["updatedExisting"])
        wait_until(lambda: collection.find_one({"i": 2}),
                   "found updated w=0 doc")

        collection.update({"i": 1}, {"$set": {"i": 500}}, w=0)
        self.assertFalse(db.last_status()["updatedExisting"])

    def test_auto_ref_and_deref(self):
        # Legacy API.
        db = self.client.pymongo_test
        db.add_son_manipulator(AutoReference(db))
        db.add_son_manipulator(NamespaceInjector())

        db.test.a.remove({})
        db.test.b.remove({})
        db.test.c.remove({})

        a = {"hello": u"world"}
        db.test.a.save(a)

        b = {"test": a}
        db.test.b.save(b)

        c = {"another test": b}
        db.test.c.save(c)

        a["hello"] = "mike"
        db.test.a.save(a)

        self.assertEqual(db.test.a.find_one(), a)
        self.assertEqual(db.test.b.find_one()["test"], a)
        self.assertEqual(db.test.c.find_one()["another test"]["test"], a)
        self.assertEqual(db.test.b.find_one(), b)
        self.assertEqual(db.test.c.find_one()["another test"], b)
        self.assertEqual(db.test.c.find_one(), c)

    def test_auto_ref_and_deref_list(self):
        # Legacy API.
        db = self.client.pymongo_test
        db.add_son_manipulator(AutoReference(db))
        db.add_son_manipulator(NamespaceInjector())

        db.drop_collection("users")
        db.drop_collection("messages")

        message_1 = {"title": "foo"}
        db.messages.save(message_1)
        message_2 = {"title": "bar"}
        db.messages.save(message_2)

        user = {"messages": [message_1, message_2]}
        db.users.save(user)
        db.messages.update(message_1, {"title": "buzz"})

        self.assertEqual("buzz", db.users.find_one()["messages"][0]["title"])
        self.assertEqual("bar", db.users.find_one()["messages"][1]["title"])

    def test_object_to_dict_transformer(self):
        # PYTHON-709: Some users rely on their custom SONManipulators to run
        # before any other checks, so they can insert non-dict objects and
        # have them dictified before the _id is inserted or any other
        # processing.
        # Tests legacy API elements.
        class Thing(object):
            def __init__(self, value):
                self.value = value

        class ThingTransformer(SONManipulator):
            def transform_incoming(self, thing, dummy):
                return {'value': thing.value}

        db = self.client.foo
        db.add_son_manipulator(ThingTransformer())
        t = Thing('value')

        db.test.remove()
        db.test.insert([t])
        out = db.test.find_one()
        self.assertEqual('value', out.get('value'))

    def test_son_manipulator_outgoing(self):
        class Thing(object):
            def __init__(self, value):
                self.value = value

        class ThingTransformer(SONManipulator):
            def transform_outgoing(self, doc, collection):
                # We don't want this applied to the command return
                # value in pymongo.cursor.Cursor.
                if 'value' in doc:
                    return Thing(doc['value'])
                return doc

        db = self.client.foo
        db.add_son_manipulator(ThingTransformer())

        db.test.delete_many({})
        db.test.insert_one({'value': 'value'})
        out = db.test.find_one()
        self.assertTrue(isinstance(out, Thing))
        self.assertEqual('value', out.value)

        out = next(db.test.aggregate([], cursor={}))
        self.assertTrue(isinstance(out, Thing))
        self.assertEqual('value', out.value)

    def test_son_manipulator_inheritance(self):
        # Tests legacy API elements.
        class Thing(object):
            def __init__(self, value):
                self.value = value

        class ThingTransformer(SONManipulator):
            def transform_incoming(self, thing, dummy):
                return {'value': thing.value}

            def transform_outgoing(self, son, dummy):
                return Thing(son['value'])

        class Child(ThingTransformer):
            pass

        db = self.client.foo
        db.add_son_manipulator(Child())
        t = Thing('value')

        db.test.remove()
        db.test.insert([t])
        out = db.test.find_one()
        self.assertTrue(isinstance(out, Thing))
        self.assertEqual('value', out.value)

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
        c.drop()

    def test_manipulator_properties(self):
        db = self.client.foo
        self.assertEqual([], db.incoming_manipulators)
        self.assertEqual([], db.incoming_copying_manipulators)
        self.assertEqual([], db.outgoing_manipulators)
        self.assertEqual([], db.outgoing_copying_manipulators)
        db.add_son_manipulator(AutoReference(db))
        db.add_son_manipulator(NamespaceInjector())
        db.add_son_manipulator(ObjectIdShuffler())
        self.assertEqual(1, len(db.incoming_manipulators))
        self.assertEqual(db.incoming_manipulators, ['NamespaceInjector'])
        self.assertEqual(2, len(db.incoming_copying_manipulators))
        for name in db.incoming_copying_manipulators:
            self.assertTrue(name in ('ObjectIdShuffler', 'AutoReference'))
        self.assertEqual([], db.outgoing_manipulators)
        self.assertEqual(['AutoReference'],
                         db.outgoing_copying_manipulators)

    def test_ensure_index(self):
        db = self.db

        self.assertRaises(TypeError, db.test.ensure_index, {"hello": 1})
        self.assertRaises(TypeError,
                          db.test.ensure_index, {"hello": 1}, cache_for='foo')

        db.test.drop_indexes()

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
                         db.test.ensure_index("goodbye"))
        self.assertEqual(None, db.test.ensure_index("goodbye"))

        db.test.drop_index("goodbye_1")
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye", cache_for=1))
        time.sleep(1.2)
        self.assertEqual("goodbye_1",
                         db.test.ensure_index("goodbye"))
        # Make sure the expiration time is updated.
        self.assertEqual(None,
                         db.test.ensure_index("goodbye"))

        # Clean up indexes for later tests
        db.test.drop_indexes()

    @client_context.require_version_max(4, 1)  # PYTHON-1734
    def test_ensure_index_threaded(self):
        coll = self.db.threaded_index_creation
        index_docs = []

        class Indexer(threading.Thread):
            def run(self):
                coll.ensure_index('foo0')
                coll.ensure_index('foo1')
                coll.ensure_index('foo2')
                index_docs.append(coll.index_information())

        try:
            threads = []
            for _ in range(10):
                t = Indexer()
                t.setDaemon(True)
                threads.append(t)

            for thread in threads:
                thread.start()

            joinall(threads)

            first = index_docs[0]
            for index_doc in index_docs[1:]:
                self.assertEqual(index_doc, first)
        finally:
            coll.drop()

    def test_ensure_purge_index_threaded(self):
        coll = self.db.threaded_index_creation

        class Indexer(threading.Thread):
            def run(self):
                coll.ensure_index('foo')
                try:
                    coll.drop_index('foo')
                except OperationFailure:
                    # The index may have already been dropped.
                    pass
                coll.ensure_index('foo')
                coll.drop_indexes()
                coll.create_index('foo')

        try:
            threads = []
            for _ in range(10):
                t = Indexer()
                t.setDaemon(True)
                threads.append(t)

            for thread in threads:
                thread.start()

            joinall(threads)

            self.assertTrue('foo_1' in coll.index_information())
        finally:
            coll.drop()

    @client_context.require_version_max(4, 1)  # PYTHON-1734
    def test_ensure_unique_index_threaded(self):
        coll = self.db.test_unique_threaded
        coll.drop()
        coll.insert_many([{'foo': i} for i in range(10000)])

        class Indexer(threading.Thread):
            def run(self):
                try:
                    coll.ensure_index('foo', unique=True)
                    coll.insert_one({'foo': 'bar'})
                    coll.insert_one({'foo': 'bar'})
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

    def test_kill_cursors_with_cursoraddress(self):
        coll = self.client.pymongo_test.test
        coll.drop()

        coll.insert_many([{'_id': i} for i in range(200)])
        cursor = coll.find().batch_size(1)
        next(cursor)
        self.client.kill_cursors(
            [cursor.cursor_id],
            _CursorAddress(self.client.address, coll.full_name))

        # Prevent killcursors from reaching the server while a getmore is in
        # progress -- the server logs "Assertion: 16089:Cannot kill active
        # cursor."
        time.sleep(2)

        def raises_cursor_not_found():
            try:
                next(cursor)
                return False
            except CursorNotFound:
                return True

        wait_until(raises_cursor_not_found, 'close cursor')

    def test_kill_cursors_with_tuple(self):
        # Some evergreen distros (Debian 7.1) still test against 3.6.5 where
        # OP_KILL_CURSORS does not work.
        if (client_context.is_mongos and client_context.auth_enabled and
                (3, 6, 0) <= client_context.version < (3, 6, 6)):
            raise SkipTest("SERVER-33553 This server version does not support "
                           "OP_KILL_CURSORS")

        coll = self.client.pymongo_test.test
        coll.drop()

        coll.insert_many([{'_id': i} for i in range(200)])
        cursor = coll.find().batch_size(1)
        next(cursor)
        self.client.kill_cursors(
            [cursor.cursor_id],
            self.client.address)

        # Prevent killcursors from reaching the server while a getmore is in
        # progress -- the server logs "Assertion: 16089:Cannot kill active
        # cursor."
        time.sleep(2)

        def raises_cursor_not_found():
            try:
                next(cursor)
                return False
            except CursorNotFound:
                return True

        wait_until(raises_cursor_not_found, 'close cursor')


class TestLegacyBulk(BulkTestBase):

    @classmethod
    def setUpClass(cls):
        super(TestLegacyBulk, cls).setUpClass()
        cls.deprecation_filter = DeprecationFilter()

    @classmethod
    def tearDownClass(cls):
        cls.deprecation_filter.stop()

    def test_empty(self):
        bulk = self.coll.initialize_ordered_bulk_op()
        self.assertRaises(InvalidOperation, bulk.execute)

    def test_find(self):
        # find() requires a selector.
        bulk = self.coll.initialize_ordered_bulk_op()
        self.assertRaises(TypeError, bulk.find)
        self.assertRaises(TypeError, bulk.find, 'foo')
        # No error.
        bulk.find({})

    @client_context.require_version_min(3, 1, 9, -1)
    def test_bypass_document_validation_bulk_op(self):

        # Test insert
        self.coll.insert_one({"z": 0})
        self.db.command(SON([("collMod", "test"),
                             ("validator", {"z": {"$gte": 0}})]))
        bulk = self.coll.initialize_ordered_bulk_op(
            bypass_document_validation=False)
        bulk.insert({"z": -1}) # error
        self.assertRaises(BulkWriteError, bulk.execute)
        self.assertEqual(0, self.coll.count({"z": -1}))

        bulk = self.coll.initialize_ordered_bulk_op(
            bypass_document_validation=True)
        bulk.insert({"z": -1})
        bulk.execute()
        self.assertEqual(1, self.coll.count({"z": -1}))

        self.coll.insert_one({"z": 0})
        self.db.command(SON([("collMod", "test"),
                             ("validator", {"z": {"$gte": 0}})]))
        bulk = self.coll.initialize_unordered_bulk_op(
            bypass_document_validation=False)
        bulk.insert({"z": -1}) # error
        self.assertRaises(BulkWriteError, bulk.execute)
        self.assertEqual(1, self.coll.count({"z": -1}))

        bulk = self.coll.initialize_unordered_bulk_op(
            bypass_document_validation=True)
        bulk.insert({"z": -1})
        bulk.execute()
        self.assertEqual(2, self.coll.count({"z": -1}))
        self.coll.drop()

    def test_insert(self):
        expected = {
            'nMatched': 0,
            'nModified': 0,
            'nUpserted': 0,
            'nInserted': 1,
            'nRemoved': 0,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }

        bulk = self.coll.initialize_ordered_bulk_op()
        self.assertRaises(TypeError, bulk.insert, 1)

        # find() before insert() is prohibited.
        self.assertRaises(AttributeError, lambda: bulk.find({}).insert({}))

        # We don't allow multiple documents per call.
        self.assertRaises(TypeError, bulk.insert, [{}, {}])
        self.assertRaises(TypeError, bulk.insert, ({} for _ in range(2)))

        bulk.insert({})
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual(1, self.coll.count())
        doc = self.coll.find_one()
        self.assertTrue(oid_generated_on_process(doc['_id']))

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.insert({})
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual(2, self.coll.count())

    def test_insert_check_keys(self):
        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.insert({'$dollar': 1})
        self.assertRaises(InvalidDocument, bulk.execute)

        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.insert({'a.b': 1})
        self.assertRaises(InvalidDocument, bulk.execute)

    def test_update(self):

        expected = {
            'nMatched': 2,
            'nModified': 2,
            'nUpserted': 0,
            'nInserted': 0,
            'nRemoved': 0,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }
        self.coll.insert_many([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()

        # update() requires find() first.
        self.assertRaises(
            AttributeError,
            lambda: bulk.update({'$set': {'x': 1}}))

        self.assertRaises(TypeError, bulk.find({}).update, 1)
        self.assertRaises(ValueError, bulk.find({}).update, {})

        # All fields must be $-operators.
        self.assertRaises(ValueError, bulk.find({}).update, {'foo': 'bar'})
        bulk.find({}).update({'$set': {'foo': 'bar'}})
        result = bulk.execute()
        self.assertEqualResponse(expected, result)
        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 2)

        # All fields must be $-operators -- validated server-side.
        bulk = self.coll.initialize_ordered_bulk_op()
        updates = SON([('$set', {'x': 1}), ('y', 1)])
        bulk.find({}).update(updates)
        self.assertRaises(BulkWriteError, bulk.execute)

        self.coll.delete_many({})
        self.coll.insert_many([{}, {}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({}).update({'$set': {'bim': 'baz'}})
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 2,
             'nModified': 2,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual(self.coll.find({'bim': 'baz'}).count(), 2)

        self.coll.insert_one({'x': 1})
        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({'x': 1}).update({'$set': {'x': 42}})
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 1,
             'nModified': 1,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual(1, self.coll.find({'x': 42}).count())

        # Second time, x is already 42 so nModified is 0.
        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({'x': 42}).update({'$set': {'x': 42}})
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 1,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

    def test_update_one(self):

        expected = {
            'nMatched': 1,
            'nModified': 1,
            'nUpserted': 0,
            'nInserted': 0,
            'nRemoved': 0,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }

        self.coll.insert_many([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()

        # update_one() requires find() first.
        self.assertRaises(
            AttributeError,
            lambda: bulk.update_one({'$set': {'x': 1}}))

        self.assertRaises(TypeError, bulk.find({}).update_one, 1)
        self.assertRaises(ValueError, bulk.find({}).update_one, {})
        self.assertRaises(ValueError, bulk.find({}).update_one, {'foo': 'bar'})
        bulk.find({}).update_one({'$set': {'foo': 'bar'}})
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 1)

        self.coll.delete_many({})
        self.coll.insert_many([{}, {}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({}).update_one({'$set': {'bim': 'baz'}})
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual(self.coll.find({'bim': 'baz'}).count(), 1)

        # All fields must be $-operators -- validated server-side.
        bulk = self.coll.initialize_ordered_bulk_op()
        updates = SON([('$set', {'x': 1}), ('y', 1)])
        bulk.find({}).update_one(updates)
        self.assertRaises(BulkWriteError, bulk.execute)

    def test_replace_one(self):

        expected = {
            'nMatched': 1,
            'nModified': 1,
            'nUpserted': 0,
            'nInserted': 0,
            'nRemoved': 0,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }

        self.coll.insert_many([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()
        self.assertRaises(TypeError, bulk.find({}).replace_one, 1)
        self.assertRaises(ValueError,
                          bulk.find({}).replace_one, {'$set': {'foo': 'bar'}})
        bulk.find({}).replace_one({'foo': 'bar'})
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 1)

        self.coll.delete_many({})
        self.coll.insert_many([{}, {}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({}).replace_one({'bim': 'baz'})
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual(self.coll.find({'bim': 'baz'}).count(), 1)

    def test_remove(self):
        # Test removing all documents, ordered.
        expected = {
            'nMatched': 0,
            'nModified': 0,
            'nUpserted': 0,
            'nInserted': 0,
            'nRemoved': 2,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }
        self.coll.insert_many([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()

        # remove() must be preceded by find().
        self.assertRaises(AttributeError, lambda: bulk.remove())
        bulk.find({}).remove()
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual(self.coll.count(), 0)

        # Test removing some documents, ordered.
        self.coll.insert_many([{}, {'x': 1}, {}, {'x': 1}])

        bulk = self.coll.initialize_ordered_bulk_op()

        bulk.find({'x': 1}).remove()
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 2,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual(self.coll.count(), 2)
        self.coll.delete_many({})

        # Test removing all documents, unordered.
        self.coll.insert_many([{}, {}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({}).remove()
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 2,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        # Test removing some documents, unordered.
        self.assertEqual(self.coll.count(), 0)

        self.coll.insert_many([{}, {'x': 1}, {}, {'x': 1}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({'x': 1}).remove()
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 2,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual(self.coll.count(), 2)
        self.coll.delete_many({})

    def test_remove_one(self):

        bulk = self.coll.initialize_ordered_bulk_op()

        # remove_one() must be preceded by find().
        self.assertRaises(AttributeError, lambda: bulk.remove_one())

        # Test removing one document, empty selector.
        # First ordered, then unordered.
        self.coll.insert_many([{}, {}])
        expected = {
            'nMatched': 0,
            'nModified': 0,
            'nUpserted': 0,
            'nInserted': 0,
            'nRemoved': 1,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }

        bulk.find({}).remove_one()
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual(self.coll.count(), 1)

        self.coll.insert_one({})

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({}).remove_one()
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual(self.coll.count(), 1)

        # Test removing one document, with a selector.
        # First ordered, then unordered.
        self.coll.insert_one({'x': 1})

        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.find({'x': 1}).remove_one()
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual([{}], list(self.coll.find({}, {'_id': False})))
        self.coll.insert_one({'x': 1})

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({'x': 1}).remove_one()
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        self.assertEqual([{}], list(self.coll.find({}, {'_id': False})))

    def test_upsert(self):
        bulk = self.coll.initialize_ordered_bulk_op()

        # upsert() requires find() first.
        self.assertRaises(
            AttributeError,
            lambda: bulk.upsert())

        expected = {
            'nMatched': 0,
            'nModified': 0,
            'nUpserted': 1,
            'nInserted': 0,
            'nRemoved': 0,
            'upserted': [{'index': 0, '_id': '...'}]
        }

        bulk.find({}).upsert().replace_one({'foo': 'bar'})
        result = bulk.execute()
        self.assertEqualResponse(expected, result)

        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.find({}).upsert().update_one({'$set': {'bim': 'baz'}})
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 1,
             'nModified': 1,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual(self.coll.find({'bim': 'baz'}).count(), 1)

        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.find({}).upsert().update({'$set': {'bim': 'bop'}})
        # Non-upsert, no matches.
        bulk.find({'x': 1}).update({'$set': {'x': 2}})
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 1,
             'nModified': 1,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual(self.coll.find({'bim': 'bop'}).count(), 1)
        self.assertEqual(self.coll.find({'x': 2}).count(), 0)

    def test_upsert_large(self):
        big = 'a' * (client_context.client.max_bson_size - 37)
        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.find({'x': 1}).upsert().update({'$set': {'s': big}})
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 1,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [{'index': 0, '_id': '...'}]},
            result)

        self.assertEqual(1, self.coll.find({'x': 1}).count())

    def test_client_generated_upsert_id(self):
        batch = self.coll.initialize_ordered_bulk_op()
        batch.find({'_id': 0}).upsert().update_one({'$set': {'a': 0}})
        batch.find({'a': 1}).upsert().replace_one({'_id': 1})
        # This is just here to make the counts right in all cases.
        batch.find({'_id': 2}).upsert().replace_one({'_id': 2})
        result = batch.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 3,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [{'index': 0, '_id': 0},
                          {'index': 1, '_id': 1},
                          {'index': 2, '_id': 2}]},
            result)

    def test_single_ordered_batch(self):
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1})
        batch.find({'a': 1}).update_one({'$set': {'b': 1}})
        batch.find({'a': 2}).upsert().update_one({'$set': {'b': 2}})
        batch.insert({'a': 3})
        batch.find({'a': 3}).remove()
        result = batch.execute()
        self.assertEqualResponse(
            {'nMatched': 1,
             'nModified': 1,
             'nUpserted': 1,
             'nInserted': 2,
             'nRemoved': 1,
             'upserted': [{'index': 2, '_id': '...'}]},
            result)

    def test_single_error_ordered_batch(self):
        self.coll.create_index('a', unique=True)
        self.addCleanup(self.coll.drop_index, [('a', 1)])
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
        batch.insert({'b': 3, 'a': 2})

        try:
            batch.execute()
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 1,
             'nRemoved': 0,
             'upserted': [],
             'writeConcernErrors': [],
             'writeErrors': [
                 {'index': 1,
                  'code': 11000,
                  'errmsg': '...',
                  'op': {'q': {'b': 2},
                         'u': {'$set': {'a': 1}},
                         'multi': False,
                         'upsert': True}}]},
            result)

    def test_multiple_error_ordered_batch(self):
        self.coll.create_index('a', unique=True)
        self.addCleanup(self.coll.drop_index, [('a', 1)])
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
        batch.find({'b': 3}).upsert().update_one({'$set': {'a': 2}})
        batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
        batch.insert({'b': 4, 'a': 3})
        batch.insert({'b': 5, 'a': 1})

        try:
            batch.execute()
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 1,
             'nRemoved': 0,
             'upserted': [],
             'writeConcernErrors': [],
             'writeErrors': [
                 {'index': 1,
                  'code': 11000,
                  'errmsg': '...',
                  'op': {'q': {'b': 2},
                         'u': {'$set': {'a': 1}},
                         'multi': False,
                         'upsert': True}}]},
            result)

    def test_single_unordered_batch(self):
        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'a': 1})
        batch.find({'a': 1}).update_one({'$set': {'b': 1}})
        batch.find({'a': 2}).upsert().update_one({'$set': {'b': 2}})
        batch.insert({'a': 3})
        batch.find({'a': 3}).remove()
        result = batch.execute()
        self.assertEqualResponse(
            {'nMatched': 1,
             'nModified': 1,
             'nUpserted': 1,
             'nInserted': 2,
             'nRemoved': 1,
             'upserted': [{'index': 2, '_id': '...'}],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

    def test_single_error_unordered_batch(self):
        self.coll.create_index('a', unique=True)
        self.addCleanup(self.coll.drop_index, [('a', 1)])
        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
        batch.insert({'b': 3, 'a': 2})

        try:
            batch.execute()
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 2,
             'nRemoved': 0,
             'upserted': [],
             'writeConcernErrors': [],
             'writeErrors': [
                 {'index': 1,
                  'code': 11000,
                  'errmsg': '...',
                  'op': {'q': {'b': 2},
                         'u': {'$set': {'a': 1}},
                         'multi': False,
                         'upsert': True}}]},
            result)

    def test_multiple_error_unordered_batch(self):
        self.coll.create_index('a', unique=True)
        self.addCleanup(self.coll.drop_index, [('a', 1)])
        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.find({'b': 2}).upsert().update_one({'$set': {'a': 3}})
        batch.find({'b': 3}).upsert().update_one({'$set': {'a': 4}})
        batch.find({'b': 4}).upsert().update_one({'$set': {'a': 3}})
        batch.insert({'b': 5, 'a': 2})
        batch.insert({'b': 6, 'a': 1})

        try:
            batch.execute()
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")
        # Assume the update at index 1 runs before the update at index 3,
        # although the spec does not require it. Same for inserts.
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 2,
             'nInserted': 2,
             'nRemoved': 0,
             'upserted': [
                 {'index': 1, '_id': '...'},
                 {'index': 2, '_id': '...'}],
             'writeConcernErrors': [],
             'writeErrors': [
                 {'index': 3,
                  'code': 11000,
                  'errmsg': '...',
                  'op': {'q': {'b': 4},
                         'u': {'$set': {'a': 3}},
                         'multi': False,
                         'upsert': True}},
                 {'index': 5,
                  'code': 11000,
                  'errmsg': '...',
                  'op': {'_id': '...', 'b': 6, 'a': 1}}]},
            result)

    def test_large_inserts_ordered(self):
        big = 'x' * self.coll.database.client.max_bson_size
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.insert({'big': big})
        batch.insert({'b': 2, 'a': 2})

        try:
            batch.execute()
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqual(1, result['nInserted'])

        self.coll.delete_many({})

        big = 'x' * (1024 * 1024 * 4)
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1, 'big': big})
        batch.insert({'a': 2, 'big': big})
        batch.insert({'a': 3, 'big': big})
        batch.insert({'a': 4, 'big': big})
        batch.insert({'a': 5, 'big': big})
        batch.insert({'a': 6, 'big': big})
        result = batch.execute()

        self.assertEqual(6, result['nInserted'])
        self.assertEqual(6, self.coll.count())

    def test_large_inserts_unordered(self):
        big = 'x' * self.coll.database.client.max_bson_size
        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.insert({'big': big})
        batch.insert({'b': 2, 'a': 2})

        try:
            batch.execute()
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqual(2, result['nInserted'])

        self.coll.delete_many({})

        big = 'x' * (1024 * 1024 * 4)
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1, 'big': big})
        batch.insert({'a': 2, 'big': big})
        batch.insert({'a': 3, 'big': big})
        batch.insert({'a': 4, 'big': big})
        batch.insert({'a': 5, 'big': big})
        batch.insert({'a': 6, 'big': big})
        result = batch.execute()

        self.assertEqual(6, result['nInserted'])
        self.assertEqual(6, self.coll.count())

    def test_numerous_inserts(self):
        # Ensure we don't exceed server's 1000-document batch size limit.
        n_docs = 2100
        batch = self.coll.initialize_unordered_bulk_op()
        for _ in range(n_docs):
            batch.insert({})

        result = batch.execute()
        self.assertEqual(n_docs, result['nInserted'])
        self.assertEqual(n_docs, self.coll.count())

        # Same with ordered bulk.
        self.coll.delete_many({})
        batch = self.coll.initialize_ordered_bulk_op()
        for _ in range(n_docs):
            batch.insert({})

        result = batch.execute()
        self.assertEqual(n_docs, result['nInserted'])
        self.assertEqual(n_docs, self.coll.count())

    def test_multiple_execution(self):
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({})
        batch.execute()
        self.assertRaises(InvalidOperation, batch.execute)

    def test_generator_insert(self):
        def gen():
            yield {'a': 1, 'b': 1}
            yield {'a': 1, 'b': 2}
            yield {'a': 2, 'b': 3}
            yield {'a': 3, 'b': 5}
            yield {'a': 5, 'b': 8}

        result = self.coll.insert_many(gen())
        self.assertEqual(5, len(result.inserted_ids))


class TestLegacyBulkNoResults(BulkTestBase):

    @classmethod
    def setUpClass(cls):
        super(TestLegacyBulkNoResults, cls).setUpClass()
        cls.deprecation_filter = DeprecationFilter()

    @classmethod
    def tearDownClass(cls):
        cls.deprecation_filter.stop()

    def tearDown(self):
        self.coll.delete_many({})

    def test_no_results_ordered_success(self):
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'_id': 1})
        batch.find({'_id': 3}).upsert().update_one({'$set': {'b': 1}})
        batch.insert({'_id': 2})
        batch.find({'_id': 1}).remove_one()
        self.assertTrue(batch.execute({'w': 0}) is None)
        wait_until(lambda: 2 == self.coll.count(),
                   'insert 2 documents')
        wait_until(lambda: self.coll.find_one({'_id': 1}) is None,
                   'removed {"_id": 1}')

    def test_no_results_ordered_failure(self):
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'_id': 1})
        batch.find({'_id': 3}).upsert().update_one({'$set': {'b': 1}})
        batch.insert({'_id': 2})
        # Fails with duplicate key error.
        batch.insert({'_id': 1})
        # Should not be executed since the batch is ordered.
        batch.find({'_id': 1}).remove_one()
        self.assertTrue(batch.execute({'w': 0}) is None)
        wait_until(lambda: 3 == self.coll.count(),
                   'insert 3 documents')
        self.assertEqual({'_id': 1}, self.coll.find_one({'_id': 1}))

    def test_no_results_unordered_success(self):
        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'_id': 1})
        batch.find({'_id': 3}).upsert().update_one({'$set': {'b': 1}})
        batch.insert({'_id': 2})
        batch.find({'_id': 1}).remove_one()
        self.assertTrue(batch.execute({'w': 0}) is None)
        wait_until(lambda: 2 == self.coll.count(),
                   'insert 2 documents')
        wait_until(lambda: self.coll.find_one({'_id': 1}) is None,
                   'removed {"_id": 1}')

    def test_no_results_unordered_failure(self):
        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'_id': 1})
        batch.find({'_id': 3}).upsert().update_one({'$set': {'b': 1}})
        batch.insert({'_id': 2})
        # Fails with duplicate key error.
        batch.insert({'_id': 1})
        # Should be executed since the batch is unordered.
        batch.find({'_id': 1}).remove_one()
        self.assertTrue(batch.execute({'w': 0}) is None)
        wait_until(lambda: 2 == self.coll.count(),
                   'insert 2 documents')
        wait_until(lambda: self.coll.find_one({'_id': 1}) is None,
                   'removed {"_id": 1}')


class TestLegacyBulkWriteConcern(BulkTestBase):

    @classmethod
    def setUpClass(cls):
        super(TestLegacyBulkWriteConcern, cls).setUpClass()
        cls.w = client_context.w
        cls.secondary = None
        if cls.w > 1:
            for member in client_context.ismaster['hosts']:
                if member != client_context.ismaster['primary']:
                    cls.secondary = single_client(*partition_node(member))
                    break

        # We tested wtimeout errors by specifying a write concern greater than
        # the number of members, but in MongoDB 2.7.8+ this causes a different
        # sort of error, "Not enough data-bearing nodes". In recent servers we
        # use a failpoint to pause replication on a secondary.
        cls.need_replication_stopped = client_context.version.at_least(2, 7, 8)
        cls.deprecation_filter = DeprecationFilter()

    @classmethod
    def tearDownClass(cls):
        cls.deprecation_filter.stop()
        if cls.secondary:
            cls.secondary.close()

    def cause_wtimeout(self, batch):
        if self.need_replication_stopped:
            if not client_context.test_commands_enabled:
                raise SkipTest("Test commands must be enabled.")

            self.secondary.admin.command('configureFailPoint',
                                         'rsSyncApplyStop',
                                         mode='alwaysOn')

            try:
                return batch.execute({'w': self.w, 'wtimeout': 1})
            finally:
                self.secondary.admin.command('configureFailPoint',
                                             'rsSyncApplyStop',
                                             mode='off')
        else:
            return batch.execute({'w': self.w + 1, 'wtimeout': 1})

    def test_fsync_and_j(self):
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1})
        self.assertRaises(
            ConfigurationError,
            batch.execute, {'fsync': True, 'j': True})

    @client_context.require_replica_set
    def test_write_concern_failure_ordered(self):
        # Ensure we don't raise on wnote.
        batch = self.coll.initialize_ordered_bulk_op()
        batch.find({"something": "that does no exist"}).remove()
        self.assertTrue(batch.execute({"w": self.w}))

        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1})
        batch.insert({'a': 2})

        # Replication wtimeout is a 'soft' error.
        # It shouldn't stop batch processing.
        try:
            self.cause_wtimeout(batch)
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 2,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': []},
            result)

        # When talking to legacy servers there will be a
        # write concern error for each operation.
        self.assertTrue(len(result['writeConcernErrors']) > 0)

        failed = result['writeConcernErrors'][0]
        self.assertEqual(64, failed['code'])
        self.assertTrue(isinstance(failed['errmsg'], string_type))

        self.coll.delete_many({})
        self.coll.create_index('a', unique=True)
        self.addCleanup(self.coll.drop_index, [('a', 1)])

        # Fail due to write concern support as well
        # as duplicate key error on ordered batch.
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1})
        batch.find({'a': 3}).upsert().replace_one({'b': 1})
        batch.insert({'a': 1})
        batch.insert({'a': 2})
        try:
            self.cause_wtimeout(batch)
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 1,
             'nInserted': 1,
             'nRemoved': 0,
             'upserted': [{'index': 1, '_id': '...'}],
             'writeErrors': [
                 {'index': 2,
                  'code': 11000,
                  'errmsg': '...',
                  'op': {'_id': '...', 'a': 1}}]},
            result)

        self.assertTrue(len(result['writeConcernErrors']) > 1)
        failed = result['writeErrors'][0]
        self.assertTrue("duplicate" in failed['errmsg'])

    @client_context.require_replica_set
    def test_write_concern_failure_unordered(self):
        # Ensure we don't raise on wnote.
        batch = self.coll.initialize_unordered_bulk_op()
        batch.find({"something": "that does no exist"}).remove()
        self.assertTrue(batch.execute({"w": self.w}))

        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'a': 1})
        batch.find({'a': 3}).upsert().update_one({'$set': {'a': 3, 'b': 1}})
        batch.insert({'a': 2})

        # Replication wtimeout is a 'soft' error.
        # It shouldn't stop batch processing.
        try:
            self.cause_wtimeout(batch)
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqual(2, result['nInserted'])
        self.assertEqual(1, result['nUpserted'])
        self.assertEqual(0, len(result['writeErrors']))
        # When talking to legacy servers there will be a
        # write concern error for each operation.
        self.assertTrue(len(result['writeConcernErrors']) > 1)

        self.coll.delete_many({})
        self.coll.create_index('a', unique=True)
        self.addCleanup(self.coll.drop_index, [('a', 1)])

        # Fail due to write concern support as well
        # as duplicate key error on unordered batch.
        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'a': 1})
        batch.find({'a': 3}).upsert().update_one({'$set': {'a': 3,
                                                           'b': 1}})
        batch.insert({'a': 1})
        batch.insert({'a': 2})
        try:
            self.cause_wtimeout(batch)
        except BulkWriteError as exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqual(2, result['nInserted'])
        self.assertEqual(1, result['nUpserted'])
        self.assertEqual(1, len(result['writeErrors']))
        # When talking to legacy servers there will be a
        # write concern error for each operation.
        self.assertTrue(len(result['writeConcernErrors']) > 1)

        failed = result['writeErrors'][0]
        self.assertEqual(2, failed['index'])
        self.assertEqual(11000, failed['code'])
        self.assertTrue(isinstance(failed['errmsg'], string_type))
        self.assertEqual(1, failed['op']['a'])

        failed = result['writeConcernErrors'][0]
        self.assertEqual(64, failed['code'])
        self.assertTrue(isinstance(failed['errmsg'], string_type))

        upserts = result['upserted']
        self.assertEqual(1, len(upserts))
        self.assertEqual(1, upserts[0]['index'])
        self.assertTrue(upserts[0].get('_id'))


class TestLegacyBulkAuthorization(BulkAuthorizationTestBase):

    @classmethod
    def setUpClass(cls):
        super(TestLegacyBulkAuthorization, cls).setUpClass()
        cls.deprecation_filter = DeprecationFilter()

    @classmethod
    def tearDownClass(cls):
        cls.deprecation_filter.stop()

    def test_readonly(self):
        # We test that an authorization failure aborts the batch and is raised
        # as OperationFailure.
        cli = rs_or_single_client_noauth()
        db = cli.pymongo_test
        coll = db.test
        db.authenticate('readonly', 'pw')
        bulk = coll.initialize_ordered_bulk_op()
        bulk.insert({'x': 1})
        self.assertRaises(OperationFailure, bulk.execute)

    def test_no_remove(self):
        # We test that an authorization failure aborts the batch and is raised
        # as OperationFailure.
        cli = rs_or_single_client_noauth()
        db = cli.pymongo_test
        coll = db.test
        db.authenticate('noremove', 'pw')
        bulk = coll.initialize_ordered_bulk_op()
        bulk.insert({'x': 1})
        bulk.find({'x': 2}).upsert().replace_one({'x': 2})
        bulk.find({}).remove()  # Prohibited.
        bulk.insert({'x': 3})   # Never attempted.
        self.assertRaises(OperationFailure, bulk.execute)
        self.assertEqual(set([1, 2]), set(self.coll.distinct('x')))

if __name__ == "__main__":
    unittest.main()
