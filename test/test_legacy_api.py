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

sys.path[0:0] = [""]

from bson.binary import PYTHON_LEGACY, STANDARD
from bson.code import Code
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from bson.son import SON
from pymongo import ASCENDING, DESCENDING, GEOHAYSTACK
from pymongo.common import partition_node
from pymongo.errors import (BulkWriteError,
                            ConfigurationError,
                            DuplicateKeyError,
                            InvalidDocument,
                            InvalidOperation,
                            OperationFailure,
                            WriteConcernError,
                            WTimeoutError)
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
                        oid_generated_on_process,
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

    def test_add_son_manipulator_deprecation(self):
        db = self.client.pymongo_test
        self.assertRaises(DeprecationWarning,
                          lambda: db.add_son_manipulator(AutoReference(db)))

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

    @client_context.require_version_max(4, 8)  # PYTHON-2436
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
        self.assertTrue(isinstance(failed['errmsg'], str))

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
        self.assertTrue(isinstance(failed['errmsg'], str))
        self.assertEqual(1, failed['op']['a'])

        failed = result['writeConcernErrors'][0]
        self.assertEqual(64, failed['code'])
        self.assertTrue(isinstance(failed['errmsg'], str))

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
