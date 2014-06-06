# Copyright 2014-2014 MongoDB, Inc.
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
import unittest

from nose.plugins.skip import SkipTest

sys.path[0:0] = [""]

from bson import InvalidDocument, SON
from pymongo.errors import BulkWriteError, InvalidOperation, OperationFailure
from test import version
from test.test_client import get_client
from test.utils import (oid_generated_on_client,
                        remove_all_users,
                        server_started_with_auth,
                        server_started_with_nojournal)

class BulkTestBase(unittest.TestCase):

    def setUp(self):
        client = get_client()
        self.has_write_commands = (client.max_wire_version > 1)

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

    def setUp(self):
        super(TestBulk, self).setUp()
        self.coll = get_client().pymongo_test.test
        self.coll.remove()

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
        bulk = self.coll.initialize_ordered_bulk_op()
        self.assertRaises(TypeError, bulk.insert, 1)

        # find() before insert() is prohibited.
        self.assertRaises(AttributeError, lambda: bulk.find({}).insert({}))

        # We don't allow multiple documents per call.
        self.assertRaises(TypeError, bulk.insert, [{}, {}])
        self.assertRaises(TypeError, bulk.insert, ({} for _ in range(2)))

        bulk.insert({})
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 1,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual(1, self.coll.count())
        doc = self.coll.find_one()
        self.assertTrue(oid_generated_on_client(doc))

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.insert({})
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 1,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual(2, self.coll.count())

    def test_insert_check_keys(self):
        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.insert({'$dollar': 1})
        self.assertRaises(InvalidDocument, bulk.execute)

        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.insert({'a.b': 1})
        self.assertRaises(InvalidDocument, bulk.execute)

    def test_update(self):
        self.coll.insert([{}, {}])

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

        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 2)

        # All fields must be $-operators -- validated server-side.
        bulk = self.coll.initialize_ordered_bulk_op()
        updates = SON([('$set', {'x': 1}), ('y', 1)])
        bulk.find({}).update(updates)
        self.assertRaises(BulkWriteError, bulk.execute)

        self.coll.remove()
        self.coll.insert([{}, {}])

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

        self.coll.insert({'x': 1})
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
        self.coll.insert([{}, {}])

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

        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 1)

        self.coll.remove()
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({}).update_one({'$set': {'bim': 'baz'}})
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

        # All fields must be $-operators -- validated server-side.
        bulk = self.coll.initialize_ordered_bulk_op()
        updates = SON([('$set', {'x': 1}), ('y', 1)])
        bulk.find({}).update_one(updates)
        self.assertRaises(BulkWriteError, bulk.execute)

    def test_replace_one(self):
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()
        self.assertRaises(TypeError, bulk.find({}).replace_one, 1)
        self.assertRaises(ValueError,
                          bulk.find({}).replace_one, {'$set': {'foo': 'bar'}})
        bulk.find({}).replace_one({'foo': 'bar'})
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

        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 1)

        self.coll.remove()
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({}).replace_one({'bim': 'baz'})
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

    def test_remove(self):
        # Test removing all documents, ordered.
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()

        # remove() must be preceded by find().
        self.assertRaises(AttributeError, lambda: bulk.remove())
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

        self.assertEqual(self.coll.count(), 0)

        # Test removing some documents, ordered.
        self.coll.insert([{}, {'x': 1}, {}, {'x': 1}])

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
        self.coll.remove()

        # Test removing all documents, unordered.
        self.coll.insert([{}, {}])

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

        self.coll.insert([{}, {'x': 1}, {}, {'x': 1}])

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
        self.coll.remove()

    def test_remove_one(self):

        bulk = self.coll.initialize_ordered_bulk_op()

        # remove_one() must be preceded by find().
        self.assertRaises(AttributeError, lambda: bulk.remove_one())

        # Test removing one document, empty selector.
        # First ordered, then unordered.
        self.coll.insert([{}, {}])
        bulk.find({}).remove_one()
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 1,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual(self.coll.count(), 1)

        self.coll.insert({})

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({}).remove_one()
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 1,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual(self.coll.count(), 1)

        # Test removing one document, with a selector.
        # First ordered, then unordered.
        self.coll.insert([{'x': 1}])

        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.find({'x': 1}).remove_one()
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 1,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual([{}], list(self.coll.find({}, {'_id': False})))
        self.coll.insert({'x': 1})

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find({'x': 1}).remove_one()
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 1,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        self.assertEqual([{}], list(self.coll.find({}, {'_id': False})))

    def test_upsert(self):
        bulk = self.coll.initialize_ordered_bulk_op()

        # upsert() requires find() first.
        self.assertRaises(
            AttributeError,
            lambda: bulk.upsert())

        # Note, in MongoDB 2.4 the server won't return the
        # "upserted" field unless _id is an ObjectId
        bulk.find({}).upsert().replace_one({'foo': 'bar'})
        result = bulk.execute()
        self.assertEqualResponse(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 1,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [{'index': 0, '_id': '...'}]},
            result)

        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 1)

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
        client = self.coll.database.connection
        big = 'a' * (client.max_bson_size - 37)
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
        if not version.at_least(self.coll.database.connection, (2, 6, 0)):
            # This case is only possible in MongoDB versions before 2.6.
            batch.find({'_id': 3}).upsert().replace_one({'_id': 2})
        else:
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
        self.coll.ensure_index('a', unique=True)
        try:
            batch = self.coll.initialize_ordered_bulk_op()
            batch.insert({'b': 1, 'a': 1})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
            batch.insert({'b': 3, 'a': 2})

            try:
                batch.execute()
            except BulkWriteError, exc:
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
        finally:
            self.coll.drop_index([('a', 1)])

    def test_multiple_error_ordered_batch(self):
        self.coll.ensure_index('a', unique=True)
        try:
            batch = self.coll.initialize_ordered_bulk_op()
            batch.insert({'b': 1, 'a': 1})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
            batch.find({'b': 3}).upsert().update_one({'$set': {'a': 2}})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
            batch.insert({'b': 4, 'a': 3})
            batch.insert({'b': 5, 'a': 1})

            try:
                batch.execute()
            except BulkWriteError, exc:
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
        finally:
            self.coll.drop_index([('a', 1)])

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
        self.coll.ensure_index('a', unique=True)
        try:
            batch = self.coll.initialize_unordered_bulk_op()
            batch.insert({'b': 1, 'a': 1})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
            batch.insert({'b': 3, 'a': 2})

            try:
                batch.execute()
            except BulkWriteError, exc:
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
        finally:
            self.coll.drop_index([('a', 1)])

    def test_multiple_error_unordered_batch(self):
        self.coll.ensure_index('a', unique=True)
        try:
            batch = self.coll.initialize_unordered_bulk_op()
            batch.insert({'b': 1, 'a': 1})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 3}})
            batch.find({'b': 3}).upsert().update_one({'$set': {'a': 4}})
            batch.find({'b': 4}).upsert().update_one({'$set': {'a': 3}})
            batch.insert({'b': 5, 'a': 2})
            batch.insert({'b': 6, 'a': 1})

            try:
                batch.execute()
            except BulkWriteError, exc:
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
        finally:
            self.coll.drop_index([('a', 1)])

    def test_large_inserts_ordered(self):
        big = 'x' * self.coll.database.connection.max_bson_size
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.insert({'big': big})
        batch.insert({'b': 2, 'a': 2})

        try:
            batch.execute()
        except BulkWriteError, exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqual(1, result['nInserted'])

        self.coll.remove()

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
        big = 'x' * self.coll.database.connection.max_bson_size
        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.insert({'big': big})
        batch.insert({'b': 2, 'a': 2})

        try:
            batch.execute()
        except BulkWriteError, exc:
            result = exc.details
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

        self.assertEqual(2, result['nInserted'])

        self.coll.remove()

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
        self.coll.remove()
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


class TestBulkWriteConcern(BulkTestBase):

    def setUp(self):
        super(TestBulkWriteConcern, self).setUp()
        client = get_client()
        ismaster = client.test.command('ismaster')
        self.is_repl = bool(ismaster.get('setName'))
        self.w = len(ismaster.get("hosts", []))
        self.client = client
        self.coll = client.pymongo_test.test
        self.coll.remove()

    def test_fsync_and_j(self):
        if not version.at_least(self.client, (1, 8, 2)):
            raise SkipTest("Need at least MongoDB 1.8.2")
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1})
        self.assertRaises(
            OperationFailure,
            batch.execute, {'fsync': True, 'j': True})

    def test_write_concern_failure_ordered(self):
        if not self.is_repl:
            raise SkipTest("Need a replica set to test.")

        # Ensure we don't raise on wnote.
        batch = self.coll.initialize_ordered_bulk_op()
        batch.find({"something": "that does not exist"}).remove()
        self.assertTrue(batch.execute({"w": self.w}))

        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1})
        batch.insert({'a': 2})

        # Replication wtimeout is a 'soft' error.
        # It shouldn't stop batch processing.
        try:
            batch.execute({'w': self.w + 1, 'wtimeout': 1})
        except BulkWriteError, exc:
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
        self.assertTrue(isinstance(failed['errmsg'], basestring))

        self.coll.remove()
        self.coll.ensure_index('a', unique=True)

        # Fail due to write concern support as well
        # as duplicate key error on ordered batch.
        try:
            batch = self.coll.initialize_ordered_bulk_op()
            batch.insert({'a': 1})
            batch.find({'a': 3}).upsert().replace_one({'b': 1})
            batch.insert({'a': 1})
            batch.insert({'a': 2})
            try:
                batch.execute({'w': self.w + 1, 'wtimeout': 1})
            except BulkWriteError, exc:
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

            self.assertEqual(2, len(result['writeConcernErrors']))
            failed = result['writeErrors'][0]
            self.assertTrue("duplicate" in failed['errmsg'])
        finally:
            self.coll.drop_index([('a', 1)])

    def test_write_concern_failure_unordered(self):
        if not self.is_repl:
            raise SkipTest("Need a replica set to test.")

        # Ensure we don't raise on wnote.
        batch = self.coll.initialize_unordered_bulk_op()
        batch.find({"something": "that does not exist"}).remove()
        self.assertTrue(batch.execute({"w": self.w}))

        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'a': 1})
        batch.find({'a': 3}).upsert().update_one({'$set': {'a': 3, 'b': 1}})
        batch.insert({'a': 2})

        # Replication wtimeout is a 'soft' error.
        # It shouldn't stop batch processing.
        try:
            batch.execute({'w': self.w + 1, 'wtimeout': 1})
        except BulkWriteError, exc:
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

        self.coll.remove()
        self.coll.ensure_index('a', unique=True)

        # Fail due to write concern support as well
        # as duplicate key error on unordered batch.
        try:
            batch = self.coll.initialize_unordered_bulk_op()
            batch.insert({'a': 1})
            batch.find({'a': 3}).upsert().update_one({'$set': {'a': 3,
                                                               'b': 1}})
            batch.insert({'a': 1})
            batch.insert({'a': 2})
            try:
                batch.execute({'w': self.w + 1, 'wtimeout': 1})
            except BulkWriteError, exc:
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
            self.assertTrue(isinstance(failed['errmsg'], basestring))
            self.assertEqual(1, failed['op']['a'])

            failed = result['writeConcernErrors'][0]
            self.assertEqual(64, failed['code'])
            self.assertTrue(isinstance(failed['errmsg'], basestring))

            upserts = result['upserted']
            self.assertEqual(1, len(upserts))
            self.assertEqual(1, upserts[0]['index'])
            self.assertTrue(upserts[0].get('_id'))
        finally:
            self.coll.drop_index([('a', 1)])


class TestBulkNoResults(BulkTestBase):

    def setUp(self):
        super(TestBulkNoResults, self).setUp()
        self.coll = get_client().pymongo_test.test
        self.coll.remove()

    def test_no_results_ordered_success(self):

        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'_id': 1})
        batch.find({'_id': 3}).upsert().update_one({'$set': {'b': 1}})
        batch.insert({'_id': 2})
        batch.find({'_id': 1}).remove_one()
        self.assertTrue(batch.execute({'w': 0}) is None)
        self.assertEqual(2, self.coll.count())

    def test_no_results_ordered_failure(self):

        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'_id': 1})
        batch.find({'_id': 3}).upsert().update_one({'$set': {'b': 1}})
        batch.insert({'_id': 2})
        batch.insert({'_id': 1})
        batch.find({'_id': 1}).remove_one()
        self.assertTrue(batch.execute({'w': 0}) is None)
        self.assertEqual(3, self.coll.count())

    def test_no_results_unordered_success(self):

        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'_id': 1})
        batch.find({'_id': 3}).upsert().update_one({'$set': {'b': 1}})
        batch.insert({'_id': 2})
        batch.find({'_id': 1}).remove_one()
        self.assertTrue(batch.execute({'w': 0}) is None)
        self.assertEqual(2, self.coll.count())

    def test_no_results_unordered_failure(self):

        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'_id': 1})
        batch.find({'_id': 3}).upsert().update_one({'$set': {'b': 1}})
        batch.insert({'_id': 2})
        batch.insert({'_id': 1})
        batch.find({'_id': 1}).remove_one()
        self.assertTrue(batch.execute({'w': 0}) is None)
        self.assertEqual(2, self.coll.count())
        self.assertTrue(self.coll.find_one({'_id': 1}) is None)


class TestBulkAuthorization(BulkTestBase):

    def setUp(self):
        super(TestBulkAuthorization, self).setUp()
        self.client = client = get_client()
        if (not server_started_with_auth(client)
                or not version.at_least(client, (2, 5, 3))):
            raise SkipTest('Need at least MongoDB 2.5.3 with auth')

        db = client.pymongo_test
        self.coll = db.test
        self.coll.remove()

        db.add_user('dbOwner', 'pw', roles=['dbOwner'])
        db.authenticate('dbOwner', 'pw')
        db.add_user('readonly', 'pw', roles=['read'])
        db.command(
            'createRole', 'noremove',
            privileges=[{
                'actions': ['insert', 'update', 'find'],
                'resource': {'db': 'pymongo_test', 'collection': 'test'}
            }],
            roles=[])

        db.add_user('noremove', 'pw', roles=['noremove'])
        db.logout()

    def test_readonly(self):
        # We test that an authorization failure aborts the batch and is raised
        # as OperationFailure.
        db = self.client.pymongo_test
        db.authenticate('readonly', 'pw')
        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.insert({'x': 1})
        self.assertRaises(OperationFailure, bulk.execute)

    def test_no_remove(self):
        # We test that an authorization failure aborts the batch and is raised
        # as OperationFailure.
        db = self.client.pymongo_test
        db.authenticate('noremove', 'pw')
        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.insert({'x': 1})
        bulk.find({'x': 2}).upsert().replace_one({'x': 2})
        bulk.find({}).remove()  # Prohibited.
        bulk.insert({'x': 3})   # Never attempted.
        self.assertRaises(OperationFailure, bulk.execute)
        self.assertEqual(set([1, 2]), set(self.coll.distinct('x')))

    def tearDown(self):
        db = self.client.pymongo_test
        db.logout()
        db.authenticate('dbOwner', 'pw')
        db.command('dropRole', 'noremove')
        remove_all_users(db)
        db.logout()


if __name__ == "__main__":
    unittest.main()
