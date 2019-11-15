# Copyright 2014-present MongoDB, Inc.
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

"""Test the bulk API."""

import sys

sys.path[0:0] = [""]

from bson.objectid import ObjectId
from pymongo.operations import *
from pymongo.errors import (ConfigurationError,
                            InvalidOperation,
                            OperationFailure)
from pymongo.write_concern import WriteConcern
from test import (client_context,
                  unittest,
                  IntegrationTest)
from test.utils import (remove_all_users,
                        rs_or_single_client_noauth)


class BulkTestBase(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        super(BulkTestBase, cls).setUpClass()
        cls.coll = cls.db.test
        ismaster = client_context.client.admin.command('ismaster')
        cls.has_write_commands = (ismaster.get("maxWireVersion", 0) > 1)

    def setUp(self):
        super(BulkTestBase, self).setUp()
        self.coll.drop()

    def assertEqualResponse(self, expected, actual):
        """Compare response from bulk.execute() to expected response."""
        for key, value in expected.items():
            if key == 'nModified':
                if self.has_write_commands:
                    self.assertEqual(value, actual['nModified'])
                else:
                    # Legacy servers don't include nModified in the response.
                    self.assertFalse('nModified' in actual)
            elif key == 'upserted':
                expected_upserts = value
                actual_upserts = actual['upserted']
                self.assertEqual(
                    len(expected_upserts), len(actual_upserts),
                    'Expected %d elements in "upserted", got %d' % (
                        len(expected_upserts), len(actual_upserts)))

                for e, a in zip(expected_upserts, actual_upserts):
                    self.assertEqualUpsert(e, a)

            elif key == 'writeErrors':
                expected_errors = value
                actual_errors = actual['writeErrors']
                self.assertEqual(
                    len(expected_errors), len(actual_errors),
                    'Expected %d elements in "writeErrors", got %d' % (
                        len(expected_errors), len(actual_errors)))

                for e, a in zip(expected_errors, actual_errors):
                    self.assertEqualWriteError(e, a)

            else:
                self.assertEqual(
                    actual.get(key), value,
                    '%r value of %r does not match expected %r' %
                    (key, actual.get(key), value))

    def assertEqualUpsert(self, expected, actual):
        """Compare bulk.execute()['upserts'] to expected value.

        Like: {'index': 0, '_id': ObjectId()}
        """
        self.assertEqual(expected['index'], actual['index'])
        if expected['_id'] == '...':
            # Unspecified value.
            self.assertTrue('_id' in actual)
        else:
            self.assertEqual(expected['_id'], actual['_id'])

    def assertEqualWriteError(self, expected, actual):
        """Compare bulk.execute()['writeErrors'] to expected value.

        Like: {'index': 0, 'code': 123, 'errmsg': '...', 'op': { ... }}
        """
        self.assertEqual(expected['index'], actual['index'])
        self.assertEqual(expected['code'], actual['code'])
        if expected['errmsg'] == '...':
            # Unspecified value.
            self.assertTrue('errmsg' in actual)
        else:
            self.assertEqual(expected['errmsg'], actual['errmsg'])

        expected_op = expected['op'].copy()
        actual_op = actual['op'].copy()
        if expected_op.get('_id') == '...':
            # Unspecified _id.
            self.assertTrue('_id' in actual_op)
            actual_op.pop('_id')
            expected_op.pop('_id')

        self.assertEqual(expected_op, actual_op)


class TestBulk(BulkTestBase):

    def test_empty(self):
        self.assertRaises(InvalidOperation, self.coll.bulk_write, [])

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

        result = self.coll.bulk_write([InsertOne({})])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(1, result.inserted_count)
        self.assertEqual(1, self.coll.count_documents({}))

    def _test_update_many(self, update):

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

        result = self.coll.bulk_write([UpdateMany({}, update)])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(2, result.matched_count)
        self.assertTrue(result.modified_count in (2, None))

    def test_update_many(self):
        self._test_update_many({'$set': {'foo': 'bar'}})

    @client_context.require_version_min(4, 1, 11)
    def test_update_many_pipeline(self):
        self._test_update_many([{'$set': {'foo': 'bar'}}])

    @client_context.require_version_max(3, 5, 5)
    def test_array_filters_unsupported(self):
        requests = [
            UpdateMany(
                {}, {'$set': {'y.$[i].b': 5}}, array_filters=[{'i.b': 1}]),
            UpdateOne(
                {}, {'$set': {"y.$[i].b": 2}}, array_filters=[{'i.b': 3}])
        ]
        for bulk_op in requests:
            self.assertRaises(
                ConfigurationError, self.coll.bulk_write, [bulk_op])

    def test_array_filters_validation(self):
        self.assertRaises(TypeError, UpdateMany, {}, {}, array_filters={})
        self.assertRaises(TypeError, UpdateOne, {}, {}, array_filters={})

    def test_array_filters_unacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        update_one = UpdateOne(
            {}, {'$set': {'y.$[i].b': 5}}, array_filters=[{'i.b': 1}])
        update_many = UpdateMany(
            {}, {'$set': {'y.$[i].b': 5}}, array_filters=[{'i.b': 1}])
        self.assertRaises(ConfigurationError, coll.bulk_write, [update_one])
        self.assertRaises(ConfigurationError, coll.bulk_write, [update_many])

    def _test_update_one(self, update):
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

        result = self.coll.bulk_write([UpdateOne({}, update)])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(1, result.matched_count)
        self.assertTrue(result.modified_count in (1, None))

    def test_update_one(self):
        self._test_update_one({'$set': {'foo': 'bar'}})

    @client_context.require_version_min(4, 1, 11)
    def test_update_one_pipeline(self):
        self._test_update_one([{'$set': {'foo': 'bar'}}])

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

        result = self.coll.bulk_write([ReplaceOne({}, {'foo': 'bar'})])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(1, result.matched_count)
        self.assertTrue(result.modified_count in (1, None))

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

        result = self.coll.bulk_write([DeleteMany({})])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(2, result.deleted_count)

    def test_remove_one(self):
        # Test removing one document, empty selector.
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

        result = self.coll.bulk_write([DeleteOne({})])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(1, result.deleted_count)
        self.assertEqual(self.coll.count_documents({}), 1)

    def test_upsert(self):

        expected = {
            'nMatched': 0,
            'nModified': 0,
            'nUpserted': 1,
            'nInserted': 0,
            'nRemoved': 0,
            'upserted': [{'index': 0, '_id': '...'}]
        }

        result = self.coll.bulk_write([ReplaceOne({},
                                                  {'foo': 'bar'},
                                                  upsert=True)])
        self.assertEqualResponse(expected, result.bulk_api_result)
        self.assertEqual(1, result.upserted_count)
        self.assertEqual(1, len(result.upserted_ids))
        self.assertTrue(isinstance(result.upserted_ids.get(0), ObjectId))

        self.assertEqual(self.coll.count_documents({'foo': 'bar'}), 1)

    def test_numerous_inserts(self):
        # Ensure we don't exceed server's 1000-document batch size limit.
        n_docs = 2100
        requests = [InsertOne({}) for _ in range(n_docs)]
        result = self.coll.bulk_write(requests, ordered=False)
        self.assertEqual(n_docs, result.inserted_count)
        self.assertEqual(n_docs, self.coll.count_documents({}))

        # Same with ordered bulk.
        self.coll.drop()
        result = self.coll.bulk_write(requests)
        self.assertEqual(n_docs, result.inserted_count)
        self.assertEqual(n_docs, self.coll.count_documents({}))

    @client_context.require_version_min(3, 6)
    def test_bulk_max_message_size(self):
        self.coll.delete_many({})
        self.addCleanup(self.coll.delete_many, {})
        _16_MB = 16 * 1000 * 1000
        # Generate a list of documents such that the first batched OP_MSG is
        # as close as possible to the 48MB limit.
        docs = [
            {'_id': 1, 'l': 's' * _16_MB},
            {'_id': 2, 'l': 's' * _16_MB},
            {'_id': 3, 'l': 's' * (_16_MB - 10000)},
        ]
        # Fill in the remaining ~10000 bytes with small documents.
        for i in range(4, 10000):
            docs.append({'_id': i})
        result = self.coll.insert_many(docs)
        self.assertEqual(len(docs), len(result.inserted_ids))

    def test_generator_insert(self):
        def gen():
            yield {'a': 1, 'b': 1}
            yield {'a': 1, 'b': 2}
            yield {'a': 2, 'b': 3}
            yield {'a': 3, 'b': 5}
            yield {'a': 5, 'b': 8}

        result = self.coll.insert_many(gen())
        self.assertEqual(5, len(result.inserted_ids))

    def test_bulk_write_no_results(self):

        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = coll.bulk_write([InsertOne({})])
        self.assertFalse(result.acknowledged)
        self.assertRaises(InvalidOperation, lambda: result.inserted_count)
        self.assertRaises(InvalidOperation, lambda: result.matched_count)
        self.assertRaises(InvalidOperation, lambda: result.modified_count)
        self.assertRaises(InvalidOperation, lambda: result.deleted_count)
        self.assertRaises(InvalidOperation, lambda: result.upserted_count)
        self.assertRaises(InvalidOperation, lambda: result.upserted_ids)

    def test_bulk_write_invalid_arguments(self):
        # The requests argument must be a list.
        generator = (InsertOne({}) for _ in range(10))
        with self.assertRaises(TypeError):
            self.coll.bulk_write(generator)

        # Document is not wrapped in a bulk write operation.
        with self.assertRaises(TypeError):
            self.coll.bulk_write([{}])


class BulkAuthorizationTestBase(BulkTestBase):

    @classmethod
    @client_context.require_auth
    def setUpClass(cls):
        super(BulkAuthorizationTestBase, cls).setUpClass()

    def setUp(self):
        super(BulkAuthorizationTestBase, self).setUp()
        client_context.create_user(
            self.db.name, 'readonly', 'pw', ['read'])
        self.db.command(
            'createRole', 'noremove',
            privileges=[{
                'actions': ['insert', 'update', 'find'],
                'resource': {'db': 'pymongo_test', 'collection': 'test'}
            }],
            roles=[])

        client_context.create_user(self.db.name, 'noremove', 'pw', ['noremove'])

    def tearDown(self):
        self.db.command('dropRole', 'noremove')
        remove_all_users(self.db)


class TestBulkAuthorization(BulkAuthorizationTestBase):

    def test_readonly(self):
        # We test that an authorization failure aborts the batch and is raised
        # as OperationFailure.
        cli = rs_or_single_client_noauth(username='readonly', password='pw',
                                         authSource='pymongo_test')
        coll = cli.pymongo_test.test
        coll.find_one()
        self.assertRaises(OperationFailure, coll.bulk_write,
                          [InsertOne({'x': 1})])

    def test_no_remove(self):
        # We test that an authorization failure aborts the batch and is raised
        # as OperationFailure.
        cli = rs_or_single_client_noauth(username='noremove', password='pw',
                                         authSource='pymongo_test')
        coll = cli.pymongo_test.test
        coll.find_one()
        requests = [
            InsertOne({'x': 1}),
            ReplaceOne({'x': 2}, {'x': 2}, upsert=True),
            DeleteMany({}),       # Prohibited.
            InsertOne({'x': 3}),  # Never attempted.
        ]
        self.assertRaises(OperationFailure, coll.bulk_write, requests)
        self.assertEqual(set([1, 2]), set(self.coll.distinct('x')))

if __name__ == "__main__":
    unittest.main()
