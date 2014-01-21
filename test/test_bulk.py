# Copyright 2014 MongoDB, Inc.
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

from pymongo.errors import BulkWriteError, OperationFailure
from test.test_client import get_client
from test.utils import server_started_with_option
from test import version


class TestBulk(unittest.TestCase):

    def setUp(self):
        self.coll = get_client().pymongo_test.test
        self.coll.remove()

    def test_insert(self):
        bulk = self.coll.initialize_ordered_bulk_op()
        self.assertRaises(TypeError, bulk.insert, 1)
        bulk.insert({})
        result = bulk.execute()
        self.assertEqual(1, result['nInserted'])
        self.assertEqual(1, self.coll.count())

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.insert({})
        result = bulk.execute()
        self.assertEqual(1, result['nInserted'])
        self.assertEqual(2, self.coll.count())

    def test_update(self):
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()
        self.assertRaises(TypeError, bulk.find({}).update, 1)
        self.assertRaises(ValueError, bulk.find({}).update, {})
        self.assertRaises(ValueError, bulk.find({}).update, {'foo': 'bar'})
        bulk.find().update({'$set': {'foo': 'bar'}})
        result = bulk.execute()
        self.assertEqual(2, result['nUpdated'])
        self.assertEqual(0, result['nUpserted'])
        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 2)

        self.coll.remove()
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find().update({'$set': {'bim': 'baz'}})
        bulk.execute()
        self.assertEqual(2, result['nUpdated'])
        self.assertEqual(0, result['nUpserted'])
        self.assertEqual(self.coll.find({'bim': 'baz'}).count(), 2)

    def test_update_one(self):
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()
        self.assertRaises(TypeError, bulk.find({}).update_one, 1)
        self.assertRaises(ValueError, bulk.find({}).update_one, {})
        self.assertRaises(ValueError, bulk.find({}).update_one, {'foo': 'bar'})
        bulk.find().update_one({'$set': {'foo': 'bar'}})
        result = bulk.execute()
        self.assertEqual(1, result['nUpdated'])
        self.assertEqual(0, result['nUpserted'])
        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 1)

        self.coll.remove()
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find().update_one({'$set': {'bim': 'baz'}})
        result = bulk.execute()
        self.assertEqual(1, result['nUpdated'])
        self.assertEqual(0, result['nUpserted'])
        self.assertEqual(self.coll.find({'bim': 'baz'}).count(), 1)

    def test_replace_one(self):
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()
        self.assertRaises(TypeError, bulk.find({}).replace_one, 1)
        self.assertRaises(ValueError,
                          bulk.find({}).replace_one, {'$set': {'foo': 'bar'}})
        bulk.find().replace_one({'foo': 'bar'})
        result = bulk.execute()
        self.assertEqual(1, result['nUpdated'])
        self.assertEqual(0, result['nUpserted'])
        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 1)

        self.coll.remove()
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find().replace_one({'bim': 'baz'})
        result = bulk.execute()
        self.assertEqual(1, result['nUpdated'])
        self.assertEqual(0, result['nUpserted'])
        self.assertEqual(self.coll.find({'bim': 'baz'}).count(), 1)

    def test_remove(self):
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.find().remove()
        result = bulk.execute()
        self.assertEqual(2, result['nRemoved'])
        self.assertEqual(self.coll.count(), 0)

        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find().remove()
        result = bulk.execute()
        self.assertEqual(2, result['nRemoved'])
        self.assertEqual(self.coll.count(), 0)

    def test_remove_one(self):
        self.coll.insert([{}, {}])

        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.find().remove_one()
        result = bulk.execute()
        self.assertEqual(1, result['nRemoved'])
        self.assertEqual(self.coll.count(), 1)

        self.coll.insert({})

        bulk = self.coll.initialize_unordered_bulk_op()
        bulk.find().remove_one()
        result = bulk.execute()
        self.assertEqual(1, result['nRemoved'])
        self.assertEqual(self.coll.count(), 1)

    def test_upsert(self):
        # Note, in MongoDB 2.4 the server won't return the
        # "upserted" field unless _id is an ObjectId
        self.assertFalse(self.coll.count())
        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.find().upsert().replace_one({'foo': 'bar'})
        result = bulk.execute()
        self.assertEqual(0, result['nUpdated'])
        self.assertEqual(1, result['nUpserted'])
        self.assertEqual(self.coll.find({'foo': 'bar'}).count(), 1)

        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.find().upsert().update_one({'$set': {'bim': 'baz'}})
        bulk.execute()
        self.assertEqual(0, result['nUpdated'])
        self.assertEqual(1, result['nUpserted'])
        self.assertEqual(self.coll.find({'bim': 'baz'}).count(), 1)

        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.find().upsert().update({'$set': {'bim': 'bop'}})
        bulk.execute()
        self.assertEqual(0, result['nUpdated'])
        self.assertEqual(1, result['nUpserted'])
        self.assertEqual(self.coll.find({'bim': 'bop'}).count(), 1)

    def test_single_ordered_batch(self):
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1})
        batch.find({'a': 1}).update_one({'$set': {'b': 1}})
        batch.find({'a': 2}).upsert().update_one({'$set': {'b' :2}})
        batch.insert({'a': 3})
        batch.find({'a': 3}).remove()
        result = batch.execute()
        self.assertEqual(2, result['nInserted'])
        self.assertEqual(1, result['nUpserted'])
        self.assertEqual(1, result['nUpdated'])
        self.assertEqual(1, result['nRemoved'])
        upserts = result['upserted']
        self.assertEqual(1, len(upserts))
        self.assertEqual(2, upserts[0]['index'])
        self.assertTrue(upserts[0].get('_id'))

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
                result = exc.error_document
                self.assertEqual(exc.code, 65)
            else:
                self.fail("Error not raised")

            self.assertEqual(1, result['nInserted'])
            self.assertEqual(1, len(result['writeErrors']))

            error = result['writeErrors'][0]
            self.assertEqual(1, error['index'])
            self.assertEqual(11000, error['code'])
            self.assertTrue(isinstance(error['errmsg'], basestring))

            failed = error['op']
            self.assertEqual(2, failed['q']['b'])
            self.assertEqual(1, failed['u']['$set']['a'])
            self.assertFalse(failed['multi'])
            self.assertTrue(failed['upsert'])
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
                result = exc.error_document
                self.assertEqual(exc.code, 65)
            else:
                self.fail("Error not raised")

            self.assertEqual(1, result['nInserted'])
            self.assertEqual(1, len(result['writeErrors']))

            error = result['writeErrors'][0]
            self.assertEqual(1, error['index'])
            self.assertEqual(11000, error['code'])
            self.assertTrue(isinstance(error['errmsg'], basestring))

            failed = error['op']
            self.assertEqual(2, failed['q']['b'])
            self.assertEqual(1, failed['u']['$set']['a'])
            self.assertFalse(failed['multi'])
            self.assertTrue(failed['upsert'])
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
        self.assertEqual(0, len(result['writeErrors']))
        upserts = result['upserted']
        self.assertEqual(1, len(upserts))
        self.assertEqual(2, upserts[0]['index'])
        self.assertTrue(upserts[0].get('_id'))

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
                result = exc.error_document
                self.assertEqual(exc.code, 65)
            else:
                self.fail("Error not raised")

            self.assertEqual(2, result['nInserted'])
            self.assertEqual(1, len(result['writeErrors']))

            error = result['writeErrors'][0]
            self.assertEqual(1, error['index'])
            self.assertEqual(11000, error['code'])
            self.assertTrue(isinstance(error['errmsg'], basestring))

            failed = error['op']
            self.assertEqual(2, failed['q']['b'])
            self.assertEqual(1, failed['u']['$set']['a'])
            self.assertFalse(failed['multi'])
            self.assertTrue(failed['upsert'])
        finally:
            self.coll.drop_index([('a', 1)])

    def test_multiple_error_unordered_batch(self):
        self.coll.ensure_index('a', unique=True)
        try:
            batch = self.coll.initialize_unordered_bulk_op()
            batch.insert({'b': 1, 'a': 1})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
            batch.find({'b': 3}).upsert().update_one({'$set': {'a': 2}})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
            batch.insert({'b': 4, 'a': 3})
            batch.insert({'b': 5, 'a': 1})

            try:
                batch.execute()
            except BulkWriteError, exc:
                result = exc.error_document
                self.assertEqual(exc.code, 65)
            else:
                self.fail("Error not raised")

            self.assertEqual(2, result['nInserted'])
            self.assertEqual(3, len(result['writeErrors']))

            error = result['writeErrors'][0]
            self.assertEqual(1, error['index'])
            self.assertEqual(11000, error['code'])
            self.assertTrue(isinstance(error['errmsg'], basestring))
            failed = error['op']
            self.assertEqual(2, failed['q']['b'])
            self.assertEqual(1, failed['u']['$set']['a'])
            self.assertFalse(failed['multi'])
            self.assertTrue(failed['upsert'])

            error = result['writeErrors'][1]
            self.assertEqual(3, error['index'])
            self.assertEqual(11000, error['code'])
            self.assertTrue(isinstance(error['errmsg'], basestring))
            failed = error['op']
            self.assertEqual(2, failed['q']['b'])
            self.assertEqual(1, failed['u']['$set']['a'])
            self.assertFalse(failed['multi'])
            self.assertTrue(failed['upsert'])

            error = result['writeErrors'][2]
            self.assertEqual(5, error['index'])
            self.assertEqual(11000, error['code'])
            self.assertTrue(isinstance(error['errmsg'], basestring))
            failed = error['op']
            self.assertEqual(5, failed['b'])
            self.assertEqual(1, failed['a'])
        finally:
            self.coll.drop_index([('a', 1)])

    def test_large_inserts_ordered(self):
        client = self.coll.database.connection
        if not version.at_least(client, (2, 5, 4)):
            raise SkipTest('Legacy server...')
        big = 'x' * self.coll.database.connection.max_bson_size
        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.insert({'big': big})

        try:
            batch.execute()
        except BulkWriteError, exc:
            result = exc.error_document
            self.assertEqual(exc.code, 65)
        else:
            self.fail("Error not raised")

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


class TestBulkWriteConcern(unittest.TestCase):

    def setUp(self):
        client = get_client()
        ismaster = client.test.command('ismaster')
        self.is_repl = bool(ismaster.get('setName'))
        self.w = len(ismaster.get("hosts", []))
        self.coll = client.pymongo_test.test
        self.coll.remove()

    def test_write_concern_failure_ordered(self):

        batch = self.coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1})
        batch.insert({'a': 2})

        client = self.coll.database.connection
        # Using j=True without journaling is a hard failure.
        if server_started_with_option(client, '--nojournal', 'nojournal'):
            self.assertRaises(OperationFailure, batch.execute, {'j': True})
        # So is using w > 1 with no replication.
        elif not self.is_repl:
            self.assertRaises(OperationFailure,
                              batch.execute, {'w': 5, 'wtimeout': 1})
        # Replication wtimeout is a 'soft' error.
        # It shouldn't stop batch processing.
        else:
            try:
                batch.execute({'w': self.w + 1, 'wtimeout': 1})
            except BulkWriteError, exc:
                result = exc.error_document
                self.assertEqual(exc.code, 65)
            else:
                self.fail("Error not raised")

            self.assertEqual(2, result['nInserted'])
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
                    result = exc.error_document
                    self.assertEqual(exc.code, 65)
                else:
                    self.fail("Error not raised")

                self.assertEqual(1, result['nInserted'])
                self.assertEqual(1, result['nUpserted'])
                self.assertEqual(1, len(result['writeErrors']))
                self.assertEqual(2, len(result['writeConcernErrors']))

                failed = result['writeErrors'][0]
                self.assertEqual(2, failed['index'])
                self.assertEqual(11000, failed['code'])
                self.assertTrue("duplicate" in failed['errmsg'])
                self.assertEqual(1, failed['op']['a'])
            finally:
                self.coll.drop_index([('a', 1)])

    def test_write_concern_failure_unordered(self):

        batch = self.coll.initialize_unordered_bulk_op()
        batch.insert({'a': 1})
        batch.find({'a': 3}).upsert().update_one({'$set': {'a': 3, 'b': 1}})
        batch.insert({'a': 2})

        client = self.coll.database.connection
        # Using j=True without journaling is a hard failure.
        if server_started_with_option(client, '--nojournal', 'nojournal'):
            self.assertRaises(OperationFailure, batch.execute, {'j': True})
        # So is using w > 1 with no replication.
        elif not self.is_repl:
            self.assertRaises(OperationFailure,
                              batch.execute, {'w': 5, 'wtimeout': 1})
        # Replication wtimeout is a 'soft' error.
        # It shouldn't stop batch processing.
        else:
            try:
                batch.execute({'w': self.w + 1, 'wtimeout': 1})
            except BulkWriteError, exc:
                result = exc.error_document
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
                    result = exc.error_document
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


class TestBulkNoResults(unittest.TestCase):

    def setUp(self):
        self.coll = get_client().pymongo_test.test
        self.coll.remove()
        self.coll.database.connection.start_request()

    def tearDown(self):
        self.coll.database.connection.end_request()

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

if __name__ == "__main__":
    unittest.main()
