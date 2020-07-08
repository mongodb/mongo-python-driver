# Copyright 2018-present MongoDB, Inc.
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

"""Execute Transactions Spec tests."""

import os
import sys

sys.path[0:0] = [""]

from bson.py3compat import StringIO

from pymongo import client_session, WriteConcern
from pymongo.client_session import TransactionOptions
from pymongo.errors import (CollectionInvalid,
                            ConfigurationError,
                            ConnectionFailure,
                            InvalidOperation,
                            OperationFailure)
from pymongo.operations import IndexModel, InsertOne
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference

from gridfs import GridFS, GridFSBucket

from test import unittest, client_context
from test.utils import (rs_client, single_client,
                        wait_until, OvertCommandListener,
                        TestCreator)
from test.utils_spec_runner import SpecRunner

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'transactions')

_TXN_TESTS_DEBUG = os.environ.get('TRANSACTION_TESTS_DEBUG')

# Max number of operations to perform after a transaction to prove unpinning
# occurs. Chosen so that there's a low false positive rate. With 2 mongoses,
# 50 attempts yields a one in a quadrillion chance of a false positive
# (1/(0.5^50)).
UNPIN_TEST_MAX_ATTEMPTS = 50


class TransactionsBase(SpecRunner):
    @classmethod
    def setUpClass(cls):
        super(TransactionsBase, cls).setUpClass()
        if client_context.supports_transactions():
            for address in client_context.mongoses:
                cls.mongos_clients.append(single_client('%s:%s' % address))

    @classmethod
    def tearDownClass(cls):
        for client in cls.mongos_clients:
            client.close()
        super(TransactionsBase, cls).tearDownClass()

    def maybe_skip_scenario(self, test):
        super(TransactionsBase, self).maybe_skip_scenario(test)
        if ('secondary' in self.id() and
                not client_context.is_mongos and
                not client_context.has_secondaries):
            raise unittest.SkipTest('No secondaries')


class TestTransactions(TransactionsBase):
    @client_context.require_transactions
    def test_transaction_options_validation(self):
        default_options = TransactionOptions()
        self.assertIsNone(default_options.read_concern)
        self.assertIsNone(default_options.write_concern)
        self.assertIsNone(default_options.read_preference)
        self.assertIsNone(default_options.max_commit_time_ms)
        # No error when valid options are provided.
        TransactionOptions(read_concern=ReadConcern(),
                           write_concern=WriteConcern(),
                           read_preference=ReadPreference.PRIMARY,
                           max_commit_time_ms=10000)
        with self.assertRaisesRegex(TypeError, "read_concern must be "):
            TransactionOptions(read_concern={})
        with self.assertRaisesRegex(TypeError, "write_concern must be "):
            TransactionOptions(write_concern={})
        with self.assertRaisesRegex(
                ConfigurationError,
                "transactions do not support unacknowledged write concern"):
            TransactionOptions(write_concern=WriteConcern(w=0))
        with self.assertRaisesRegex(
                TypeError, "is not valid for read_preference"):
            TransactionOptions(read_preference={})
        with self.assertRaisesRegex(
                TypeError, "max_commit_time_ms must be an integer or None"):
            TransactionOptions(max_commit_time_ms="10000")

    @client_context.require_transactions
    def test_transaction_write_concern_override(self):
        """Test txn overrides Client/Database/Collection write_concern."""
        client = rs_client(w=0)
        self.addCleanup(client.close)
        db = client.test
        coll = db.test
        coll.insert_one({})
        with client.start_session() as s:
            with s.start_transaction(write_concern=WriteConcern(w=1)):
                self.assertTrue(coll.insert_one({}, session=s).acknowledged)
                self.assertTrue(coll.insert_many(
                    [{}, {}], session=s).acknowledged)
                self.assertTrue(coll.bulk_write(
                    [InsertOne({})], session=s).acknowledged)
                self.assertTrue(coll.replace_one(
                    {}, {}, session=s).acknowledged)
                self.assertTrue(coll.update_one(
                    {}, {"$set": {"a": 1}}, session=s).acknowledged)
                self.assertTrue(coll.update_many(
                    {}, {"$set": {"a": 1}}, session=s).acknowledged)
                self.assertTrue(coll.delete_one({}, session=s).acknowledged)
                self.assertTrue(coll.delete_many({}, session=s).acknowledged)
                coll.find_one_and_delete({}, session=s)
                coll.find_one_and_replace({}, {}, session=s)
                coll.find_one_and_update({}, {"$set": {"a": 1}}, session=s)

        unsupported_txn_writes = [
            (client.drop_database, [db.name], {}),
            (db.drop_collection, ['collection'], {}),
            (coll.drop, [], {}),
            (coll.map_reduce,
             ['function() {}', 'function() {}', 'output'], {}),
            (coll.rename, ['collection2'], {}),
            # Drop collection2 between tests of "rename", above.
            (coll.database.drop_collection, ['collection2'], {}),
            (coll.create_indexes, [[IndexModel('a')]], {}),
            (coll.create_index, ['a'], {}),
            (coll.drop_index, ['a_1'], {}),
            (coll.drop_indexes, [], {}),
            (coll.aggregate, [[{"$out": "aggout"}]], {}),
        ]
        # Creating a collection in a transaction requires MongoDB 4.4+.
        if client_context.version < (4, 3, 4):
            unsupported_txn_writes.extend([
                (db.create_collection, ['collection'], {}),
            ])

        for op in unsupported_txn_writes:
            op, args, kwargs = op
            with client.start_session() as s:
                kwargs['session'] = s
                s.start_transaction(write_concern=WriteConcern(w=1))
                with self.assertRaises(OperationFailure):
                    op(*args, **kwargs)
                s.abort_transaction()

    @client_context.require_transactions
    @client_context.require_multiple_mongoses
    def test_unpin_for_next_transaction(self):
        # Increase localThresholdMS and wait until both nodes are discovered
        # to avoid false positives.
        client = rs_client(client_context.mongos_seeds(),
                           localThresholdMS=1000)
        wait_until(lambda: len(client.nodes) > 1, "discover both mongoses")
        coll = client.test.test
        # Create the collection.
        coll.insert_one({})
        self.addCleanup(client.close)
        with client.start_session() as s:
            # Session is pinned to Mongos.
            with s.start_transaction():
                coll.insert_one({}, session=s)

            addresses = set()
            for _ in range(UNPIN_TEST_MAX_ATTEMPTS):
                with s.start_transaction():
                    cursor = coll.find({}, session=s)
                    self.assertTrue(next(cursor))
                    addresses.add(cursor.address)
                # Break early if we can.
                if len(addresses) > 1:
                    break

            self.assertGreater(len(addresses), 1)

    @client_context.require_transactions
    @client_context.require_multiple_mongoses
    def test_unpin_for_non_transaction_operation(self):
        # Increase localThresholdMS and wait until both nodes are discovered
        # to avoid false positives.
        client = rs_client(client_context.mongos_seeds(),
                           localThresholdMS=1000)
        wait_until(lambda: len(client.nodes) > 1, "discover both mongoses")
        coll = client.test.test
        # Create the collection.
        coll.insert_one({})
        self.addCleanup(client.close)
        with client.start_session() as s:
            # Session is pinned to Mongos.
            with s.start_transaction():
                coll.insert_one({}, session=s)

            addresses = set()
            for _ in range(UNPIN_TEST_MAX_ATTEMPTS):
                cursor = coll.find({}, session=s)
                self.assertTrue(next(cursor))
                addresses.add(cursor.address)
                # Break early if we can.
                if len(addresses) > 1:
                    break

            self.assertGreater(len(addresses), 1)

    @client_context.require_transactions
    @client_context.require_version_min(4, 3, 4)
    def test_create_collection(self):
        client = client_context.client
        db = client.pymongo_test
        coll = db.test_create_collection
        self.addCleanup(coll.drop)

        # Use with_transaction to avoid StaleConfig errors on sharded clusters.
        def create_and_insert(session):
            coll2 = db.create_collection(coll.name, session=session)
            self.assertEqual(coll, coll2)
            coll.insert_one({}, session=session)

        with client.start_session() as s:
            s.with_transaction(create_and_insert)

        # Outside a transaction we raise CollectionInvalid on existing colls.
        with self.assertRaises(CollectionInvalid):
            db.create_collection(coll.name)

        # Inside a transaction we raise the OperationFailure from create.
        with client.start_session() as s:
            s.start_transaction()
            with self.assertRaises(OperationFailure) as ctx:
                db.create_collection(coll.name, session=s)
            self.assertEqual(ctx.exception.code, 48)  # NamespaceExists

    @client_context.require_transactions
    def test_gridfs_does_not_support_transactions(self):
        client = client_context.client
        db = client.pymongo_test
        gfs = GridFS(db)
        bucket = GridFSBucket(db)

        def gridfs_find(*args, **kwargs):
            return gfs.find(*args, **kwargs).next()

        def gridfs_open_upload_stream(*args, **kwargs):
            bucket.open_upload_stream(*args, **kwargs).write(b'1')

        gridfs_ops = [
            (gfs.put, (b'123',)),
            (gfs.get, (1,)),
            (gfs.get_version, ('name',)),
            (gfs.get_last_version, ('name',)),
            (gfs.delete, (1, )),
            (gfs.list, ()),
            (gfs.find_one, ()),
            (gridfs_find, ()),
            (gfs.exists, ()),
            (gridfs_open_upload_stream, ('name',)),
            (bucket.upload_from_stream, ('name', b'data',)),
            (bucket.download_to_stream, (1, StringIO(),)),
            (bucket.download_to_stream_by_name, ('name', StringIO(),)),
            (bucket.delete, (1,)),
            (bucket.find, ()),
            (bucket.open_download_stream, (1,)),
            (bucket.open_download_stream_by_name, ('name',)),
            (bucket.rename, (1, 'new-name',)),
        ]

        with client.start_session() as s, s.start_transaction():
            for op, args in gridfs_ops:
                with self.assertRaisesRegex(
                        InvalidOperation,
                        'GridFS does not support multi-document transactions',
                ):
                    op(*args, session=s)


class PatchSessionTimeout(object):
    """Patches the client_session's with_transaction timeout for testing."""
    def __init__(self, mock_timeout):
        self.real_timeout = client_session._WITH_TRANSACTION_RETRY_TIME_LIMIT
        self.mock_timeout = mock_timeout

    def __enter__(self):
        client_session._WITH_TRANSACTION_RETRY_TIME_LIMIT = self.mock_timeout
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        client_session._WITH_TRANSACTION_RETRY_TIME_LIMIT = self.real_timeout


class TestTransactionsConvenientAPI(TransactionsBase):
    TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             'transactions-convenient-api')

    @client_context.require_transactions
    def test_callback_raises_custom_error(self):
        class _MyException(Exception):pass

        def raise_error(_):
            raise _MyException()
        with self.client.start_session() as s:
            with self.assertRaises(_MyException):
                s.with_transaction(raise_error)

    @client_context.require_transactions
    def test_callback_returns_value(self):
        def callback(_):
            return 'Foo'
        with self.client.start_session() as s:
            self.assertEqual(s.with_transaction(callback), 'Foo')

        self.db.test.insert_one({})

        def callback(session):
            self.db.test.insert_one({}, session=session)
            return 'Foo'
        with self.client.start_session() as s:
            self.assertEqual(s.with_transaction(callback), 'Foo')

    @client_context.require_transactions
    def test_callback_not_retried_after_timeout(self):
        listener = OvertCommandListener()
        client = rs_client(event_listeners=[listener])
        coll = client[self.db.name].test

        def callback(session):
            coll.insert_one({}, session=session)
            err = {
                'ok': 0,
                'errmsg': 'Transaction 7819 has been aborted.',
                'code': 251,
                'codeName': 'NoSuchTransaction',
                'errorLabels': ['TransientTransactionError'],
            }
            raise OperationFailure(err['errmsg'], err['code'], err)

        # Create the collection.
        coll.insert_one({})
        listener.results.clear()
        with client.start_session() as s:
            with PatchSessionTimeout(0):
                with self.assertRaises(OperationFailure):
                    s.with_transaction(callback)

        self.assertEqual(listener.started_command_names(),
                         ['insert', 'abortTransaction'])

    @client_context.require_test_commands
    @client_context.require_transactions
    def test_callback_not_retried_after_commit_timeout(self):
        listener = OvertCommandListener()
        client = rs_client(event_listeners=[listener])
        coll = client[self.db.name].test

        def callback(session):
            coll.insert_one({}, session=session)

        # Create the collection.
        coll.insert_one({})
        self.set_fail_point({
            'configureFailPoint': 'failCommand', 'mode': {'times': 1},
            'data': {
                'failCommands': ['commitTransaction'],
                'errorCode': 251,  # NoSuchTransaction
            }})
        self.addCleanup(self.set_fail_point, {
            'configureFailPoint': 'failCommand', 'mode': 'off'})
        listener.results.clear()

        with client.start_session() as s:
            with PatchSessionTimeout(0):
                with self.assertRaises(OperationFailure):
                    s.with_transaction(callback)

        self.assertEqual(listener.started_command_names(),
                         ['insert', 'commitTransaction'])

    @client_context.require_test_commands
    @client_context.require_transactions
    def test_commit_not_retried_after_timeout(self):
        listener = OvertCommandListener()
        client = rs_client(event_listeners=[listener])
        coll = client[self.db.name].test

        def callback(session):
            coll.insert_one({}, session=session)

        # Create the collection.
        coll.insert_one({})
        self.set_fail_point({
            'configureFailPoint': 'failCommand', 'mode': {'times': 2},
            'data': {
                'failCommands': ['commitTransaction'],
                'closeConnection': True}})
        self.addCleanup(self.set_fail_point, {
            'configureFailPoint': 'failCommand', 'mode': 'off'})
        listener.results.clear()

        with client.start_session() as s:
            with PatchSessionTimeout(0):
                with self.assertRaises(ConnectionFailure):
                    s.with_transaction(callback)

        # One insert for the callback and two commits (includes the automatic
        # retry).
        self.assertEqual(listener.started_command_names(),
                         ['insert', 'commitTransaction', 'commitTransaction'])

    # Tested here because this supports Motor's convenient transactions API.
    @client_context.require_transactions
    def test_in_transaction_property(self):
        client = client_context.client
        coll = client.test.testcollection
        coll.insert_one({})
        self.addCleanup(coll.drop)

        with client.start_session() as s:
            self.assertFalse(s.in_transaction)
            s.start_transaction()
            self.assertTrue(s.in_transaction)
            coll.insert_one({}, session=s)
            self.assertTrue(s.in_transaction)
            s.commit_transaction()
            self.assertFalse(s.in_transaction)

        with client.start_session() as s:
            s.start_transaction()
            # commit empty transaction
            s.commit_transaction()
            self.assertFalse(s.in_transaction)

        with client.start_session() as s:
            s.start_transaction()
            s.abort_transaction()
            self.assertFalse(s.in_transaction)

        # Using a callback
        def callback(session):
            self.assertTrue(session.in_transaction)
        with client.start_session() as s:
            self.assertFalse(s.in_transaction)
            s.with_transaction(callback)
            self.assertFalse(s.in_transaction)


def create_test(scenario_def, test, name):
    @client_context.require_test_commands
    @client_context.require_transactions
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


test_creator = TestCreator(create_test, TestTransactions, _TEST_PATH)
test_creator.create_tests()


TestCreator(create_test, TestTransactionsConvenientAPI,
            TestTransactionsConvenientAPI.TEST_PATH).create_tests()


if __name__ == "__main__":
    unittest.main()
