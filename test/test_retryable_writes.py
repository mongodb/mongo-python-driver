# Copyright 2017 MongoDB, Inc.
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

"""Test retryable writes."""

import copy
import json
import os
import sys

sys.path[0:0] = [""]

from bson.int64 import Int64
from bson.objectid import ObjectId
from bson.son import SON


from pymongo.errors import (ConnectionFailure,
                            OperationFailure,
                            ServerSelectionTimeoutError)
from pymongo.mongo_client import MongoClient
from pymongo.operations import (InsertOne,
                                DeleteMany,
                                DeleteOne,
                                ReplaceOne,
                                UpdateMany,
                                UpdateOne)
from pymongo.results import BulkWriteResult
from pymongo.write_concern import WriteConcern

from test import unittest, client_context, IntegrationTest, SkipTest, client_knobs
from test.utils import (rs_or_single_client,
                        DeprecationFilter,
                        OvertCommandListener)
from test.test_crud_v1 import check_result, run_operation

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'retryable_writes')


class TestAllScenarios(IntegrationTest):

    @classmethod
    @client_context.require_version_min(3, 5)
    @client_context.require_replica_set
    @client_context.require_test_commands
    def setUpClass(cls):
        super(TestAllScenarios, cls).setUpClass()
        # Speed up the tests by decreasing the heartbeat frequency.
        cls.knobs = client_knobs(heartbeat_frequency=0.1,
                                 min_heartbeat_interval=0.1)
        cls.knobs.enable()

    @classmethod
    def tearDownClass(cls):
        cls.knobs.disable()
        super(TestAllScenarios, cls).tearDownClass()

    def tearDown(self):
        client_context.client.admin.command(SON([
            ('configureFailPoint', 'onPrimaryTransactionalWrite'),
            ('mode', 'off')]))

    def set_fail_point(self, command_args):
        cmd = SON([('configureFailPoint', 'onPrimaryTransactionalWrite')])
        cmd.update(command_args)
        client_context.client.admin.command(cmd)


def create_test(scenario_def, test):
    def run_scenario(self):
        # Load data.
        assert scenario_def['data'], "tests must have non-empty data"
        client_context.client.pymongo_test.test.drop()
        client_context.client.pymongo_test.test.insert_many(scenario_def['data'])

        # Set the failPoint
        self.set_fail_point(test['failPoint'])
        self.addCleanup(self.set_fail_point, {
            'configureFailPoint': test['failPoint']['configureFailPoint'],
            'mode': 'off'})

        test_outcome = test['outcome']
        should_fail = test_outcome.get('error')
        result = None
        error = None

        db = rs_or_single_client(**test.get('clientOptions', {})).pymongo_test
        # Close the client explicitly to avoid having too many threads open.
        self.addCleanup(db.client.close)
        try:
            result = run_operation(db.test, test)
        except (ConnectionFailure, OperationFailure) as exc:
            error = exc

        if should_fail:
            self.assertIsNotNone(error, 'should have raised an error')
        else:
            self.assertIsNone(error)

        # Assert final state is expected.
        expected_c = test_outcome.get('collection')
        if expected_c is not None:
            expected_name = expected_c.get('name')
            if expected_name is not None:
                db_coll = db[expected_name]
            else:
                db_coll = db.test
            self.assertEqual(list(db_coll.find()), expected_c['data'])
        expected_result = test_outcome.get('result')
        # We can't test the expected result when the test should fail because
        # the BulkWriteResult is not reported when raising a network error.
        if not should_fail:
            self.assertTrue(check_result(expected_result, result),
                            "%r != %r" % (expected_result, result))

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(scenario_stream)

            test_type = os.path.splitext(filename)[0]
            min_ver, max_ver = None, None
            if 'minServerVersion' in scenario_def:
                min_ver = tuple(
                    int(elt) for
                    elt in scenario_def['minServerVersion'].split('.'))
            if 'maxServerVersion' in scenario_def:
                max_ver = tuple(
                    int(elt) for
                    elt in scenario_def['maxServerVersion'].split('.'))

            # Construct test from scenario.
            for test in scenario_def['tests']:
                new_test = create_test(scenario_def, test)
                if min_ver is not None:
                    new_test = client_context.require_version_min(*min_ver)(
                        new_test)
                if max_ver is not None:
                    new_test = client_context.require_version_max(*max_ver)(
                        new_test)

                test_name = 'test_%s_%s_%s' % (
                    dirname,
                    test_type,
                    str(test['description'].replace(" ", "_")))

                new_test.__name__ = test_name
                setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()


def _retryable_single_statement_ops(coll):
    return [
        (coll.bulk_write, [[InsertOne({}), InsertOne({})]], {}),
        (coll.bulk_write, [[InsertOne({}),
                            InsertOne({})]], {'ordered': False}),
        (coll.bulk_write, [[ReplaceOne({}, {})]], {}),
        (coll.bulk_write, [[ReplaceOne({}, {}), ReplaceOne({}, {})]], {}),
        (coll.bulk_write, [[UpdateOne({}, {'$set': {'a': 1}}),
                            UpdateOne({}, {'$set': {'a': 1}})]], {}),
        (coll.bulk_write, [[DeleteOne({})]], {}),
        (coll.bulk_write, [[DeleteOne({}), DeleteOne({})]], {}),
        (coll.insert_one, [{}], {}),
        (coll.insert_many, [[{}, {}]], {}),
        (coll.replace_one, [{}, {}], {}),
        (coll.update_one, [{}, {'$set': {'a': 1}}], {}),
        (coll.delete_one, [{}], {}),
        (coll.find_one_and_replace, [{}, {'a': 3}], {}),
        (coll.find_one_and_update, [{}, {'$set': {'a': 1}}], {}),
        (coll.find_one_and_delete, [{}, {}], {}),
    ]


def retryable_single_statement_ops(coll):
    return _retryable_single_statement_ops(coll) + [
        # Deprecated methods.
        # Insert with single or multiple documents.
        (coll.insert, [{}], {}),
        (coll.insert, [[{}]], {}),
        (coll.insert, [[{}, {}]], {}),
        # Save with and without an _id.
        (coll.save, [{}], {}),
        (coll.save, [{'_id': ObjectId()}], {}),
        # Non-multi update.
        (coll.update, [{}, {'$set': {'a': 1}}], {}),
        # Non-multi remove.
        (coll.remove, [{}], {'multi': False}),
        # Replace.
        (coll.find_and_modify, [{}, {'a': 3}], {}),
        # Update.
        (coll.find_and_modify, [{}, {'$set': {'a': 1}}], {}),
        # Delete.
        (coll.find_and_modify, [{}, {}], {'remove': True}),
    ]


def non_retryable_single_statement_ops(coll):
    return [
        (coll.bulk_write, [[UpdateOne({}, {'$set': {'a': 1}}),
                            UpdateMany({}, {'$set': {'a': 1}})]], {}),
        (coll.bulk_write, [[DeleteOne({}), DeleteMany({})]], {}),
        (coll.update_many, [{}, {'$set': {'a': 1}}], {}),
        (coll.delete_many, [{}], {}),
        # Deprecated methods.
        # Multi remove.
        (coll.remove, [{}], {}),
        # Multi update.
        (coll.update, [{}, {'$set': {'a': 1}}], {'multi': True}),
        # Unacknowledged deprecated methods.
        (coll.insert, [{}], {'w': 0}),
        # Unacknowledged Non-multi update.
        (coll.update, [{}, {'$set': {'a': 1}}], {'w': 0}),
        # Unacknowledged Non-multi remove.
        (coll.remove, [{}], {'multi': False, 'w': 0}),
        # Unacknowledged Replace.
        (coll.find_and_modify, [{}, {'a': 3}], {'writeConcern': {'w': 0}}),
        # Unacknowledged Update.
        (coll.find_and_modify, [{}, {'$set': {'a': 1}}],
         {'writeConcern': {'w': 0}}),
        # Unacknowledged Delete.
        (coll.find_and_modify, [{}, {}],
         {'remove': True, 'writeConcern': {'w': 0}}),
    ]


class IgnoreDeprecationsTest(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        super(IgnoreDeprecationsTest, cls).setUpClass()
        cls.deprecation_filter = DeprecationFilter()

    @classmethod
    def tearDownClass(cls):
        cls.deprecation_filter.stop()
        super(IgnoreDeprecationsTest, cls).tearDownClass()


class TestRetryableWrites(IgnoreDeprecationsTest):

    @classmethod
    def setUpClass(cls):
        super(TestRetryableWrites, cls).setUpClass()
        # Speed up the tests by decreasing the heartbeat frequency.
        cls.knobs = client_knobs(heartbeat_frequency=0.1,
                                 min_heartbeat_interval=0.1)
        cls.knobs.enable()
        cls.listener = OvertCommandListener()
        cls.client = rs_or_single_client(
            retryWrites=True, event_listeners=[cls.listener])
        cls.db = cls.client.pymongo_test

    @classmethod
    def tearDownClass(cls):
        cls.knobs.disable()
        super(TestRetryableWrites, cls).tearDownClass()

    def setUp(self):
        if (client_context.version.at_least(3, 5) and client_context.is_rs
                and client_context.test_commands_enabled):
            self.client.admin.command(SON([
                ('configureFailPoint', 'onPrimaryTransactionalWrite'),
                ('mode', 'alwaysOn')]))

    def tearDown(self):
        if (client_context.version.at_least(3, 5) and client_context.is_rs
                and client_context.test_commands_enabled):
            self.client.admin.command(SON([
                ('configureFailPoint', 'onPrimaryTransactionalWrite'),
                ('mode', 'off')]))

    def test_supported_single_statement_no_retry(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(
            retryWrites=False, event_listeners=[listener])
        self.addCleanup(client.close)
        for method, args, kwargs in retryable_single_statement_ops(
                client.db.retryable_write_test):
            msg = '%s(*%r, **%r)' % (method.__name__, args, kwargs)
            listener.results.clear()
            method(*args, **kwargs)
            for event in listener.results['started']:
                self.assertNotIn(
                    'txnNumber', event.command,
                    '%s sent txnNumber with %s' % (msg, event.command_name))

    @client_context.require_version_min(3, 5)
    @client_context.require_no_standalone
    def test_supported_single_statement_supported_cluster(self):
        for method, args, kwargs in retryable_single_statement_ops(
                self.db.retryable_write_test):
            msg = '%s(*%r, **%r)' % (method.__name__, args, kwargs)
            self.listener.results.clear()
            method(*args, **kwargs)
            commands_started = self.listener.results['started']
            self.assertEqual(len(self.listener.results['succeeded']), 1, msg)
            first_attempt = commands_started[0]
            self.assertIn(
                'lsid', first_attempt.command,
                '%s sent no lsid with %s' % (msg, first_attempt.command_name))
            initial_session_id = first_attempt.command['lsid']
            self.assertIn(
                'txnNumber', first_attempt.command,
                '%s sent no txnNumber with %s' % (
                    msg, first_attempt.command_name))

            # There should be no retry when the failpoint is not active.
            if (client_context.is_mongos or
                    not client_context.test_commands_enabled):
                self.assertEqual(len(commands_started), 1)
                continue

            initial_transaction_id = first_attempt.command['txnNumber']
            retry_attempt = commands_started[1]
            self.assertIn(
                'lsid', retry_attempt.command,
                '%s sent no lsid with %s' % (msg, first_attempt.command_name))
            self.assertEqual(
                retry_attempt.command['lsid'], initial_session_id, msg)
            self.assertIn(
                'txnNumber', retry_attempt.command,
                '%s sent no txnNumber with %s' % (
                    msg, first_attempt.command_name))
            self.assertEqual(retry_attempt.command['txnNumber'],
                             initial_transaction_id, msg)

    def test_supported_single_statement_unsupported_cluster(self):
        if client_context.version.at_least(3, 5) and (
                    client_context.is_rs or client_context.is_mongos):
            raise SkipTest('This cluster supports retryable writes')

        for method, args, kwargs in retryable_single_statement_ops(
                self.db.retryable_write_test):
            msg = '%s(*%r, **%r)' % (method.__name__, args, kwargs)
            self.listener.results.clear()
            method(*args, **kwargs)

            for event in self.listener.results['started']:
                self.assertNotIn(
                    'txnNumber', event.command,
                    '%s sent txnNumber with %s' % (msg, event.command_name))

    def test_unsupported_single_statement(self):
        coll = self.db.retryable_write_test
        coll.insert_many([{}, {}])
        coll_w0 = coll.with_options(write_concern=WriteConcern(w=0))
        for method, args, kwargs in (non_retryable_single_statement_ops(coll) +
                                     retryable_single_statement_ops(coll_w0)):
            msg = '%s(*%r, **%r)' % (method.__name__, args, kwargs)
            self.listener.results.clear()
            method(*args, **kwargs)
            started_events = self.listener.results['started']
            self.assertEqual(len(self.listener.results['succeeded']),
                             len(started_events), msg)
            self.assertEqual(len(self.listener.results['failed']), 0, msg)
            for event in started_events:
                self.assertNotIn(
                    'txnNumber', event.command,
                    '%s sent txnNumber with %s' % (msg, event.command_name))

    def test_server_selection_timeout_not_retried(self):
        """A ServerSelectionTimeoutError is not retried."""
        listener = OvertCommandListener()
        client = MongoClient(
            'somedomainthatdoesntexist.org',
            serverSelectionTimeoutMS=1,
            retryWrites=True, event_listeners=[listener])
        for method, args, kwargs in retryable_single_statement_ops(
                client.db.retryable_write_test):
            msg = '%s(*%r, **%r)' % (method.__name__, args, kwargs)
            listener.results.clear()
            with self.assertRaises(ServerSelectionTimeoutError, msg=msg):
                method(*args, **kwargs)
            self.assertEqual(len(listener.results['started']), 0, msg)

    @client_context.require_version_min(3, 5)
    @client_context.require_replica_set
    @client_context.require_test_commands
    def test_retry_timeout_raises_original_error(self):
        """A ServerSelectionTimeoutError on the retry attempt raises the
        original error.
        """
        listener = OvertCommandListener()
        client = rs_or_single_client(
            retryWrites=True, event_listeners=[listener])
        self.addCleanup(client.close)
        topology = client._topology
        select_server = topology.select_server

        def mock_select_server(*args, **kwargs):
            server = select_server(*args, **kwargs)

            def raise_error(*args, **kwargs):
                raise ServerSelectionTimeoutError(
                    'No primary available for writes')
            # Raise ServerSelectionTimeout on the retry attempt.
            topology.select_server = raise_error
            return server

        for method, args, kwargs in retryable_single_statement_ops(
                client.db.retryable_write_test):
            msg = '%s(*%r, **%r)' % (method.__name__, args, kwargs)
            listener.results.clear()
            topology.select_server = mock_select_server
            with self.assertRaises(ConnectionFailure, msg=msg):
                method(*args, **kwargs)
            self.assertEqual(len(listener.results['started']), 1, msg)

    @client_context.require_version_min(3, 5)
    @client_context.require_replica_set
    @client_context.require_test_commands
    def test_batch_splitting(self):
        """Test retry succeeds after failures during batch splitting."""
        large = 's' * 1024 * 1024 * 15
        coll = self.db.retryable_write_test
        coll.delete_many({})
        self.listener.results.clear()
        bulk_result = coll.bulk_write([
            InsertOne({'_id': 1, 'l': large}),
            InsertOne({'_id': 2, 'l': large}),
            InsertOne({'_id': 3, 'l': large}),
            UpdateOne({'_id': 1, 'l': large},
                      {'$unset': {'l': 1}, '$inc': {'count': 1}}),
            UpdateOne({'_id': 2, 'l': large}, {'$set': {'foo': 'bar'}}),
            DeleteOne({'l': large}),
            DeleteOne({'l': large})])
        # Each command should fail and be retried.
        # With OP_MSG 3 inserts are one batch. 2 updates another.
        # 2 deletes a third.
        self.assertEqual(len(self.listener.results['started']), 6)
        self.assertEqual(coll.find_one(), {'_id': 1, 'count': 1})
        # Assert the final result
        expected_result = {
            "writeErrors": [],
            "writeConcernErrors": [],
            "nInserted": 3,
            "nUpserted": 0,
            "nMatched": 2,
            "nModified": 2,
            "nRemoved": 2,
            "upserted": [],
        }
        self.assertEqual(bulk_result.bulk_api_result, expected_result)

    @client_context.require_version_min(3, 5)
    @client_context.require_replica_set
    @client_context.require_test_commands
    def test_batch_splitting_retry_fails(self):
        """Test retry fails during batch splitting."""
        large = 's' * 1024 * 1024 * 15
        coll = self.db.retryable_write_test
        coll.delete_many({})
        self.client.admin.command(SON([
            ('configureFailPoint', 'onPrimaryTransactionalWrite'),
            ('mode', {'skip': 3}),  # The number of _documents_ to skip.
            ('data', {'failBeforeCommitExceptionCode': 1})]))
        self.listener.results.clear()
        with self.client.start_session() as session:
            initial_txn = session._server_session._transaction_id
            try:
                coll.bulk_write([InsertOne({'_id': 1, 'l': large}),
                                 InsertOne({'_id': 2, 'l': large}),
                                 InsertOne({'_id': 3, 'l': large}),
                                 InsertOne({'_id': 4, 'l': large})],
                                session=session)
            except ConnectionFailure:
                pass
            else:
                self.fail("bulk_write should have failed")

            started = self.listener.results['started']
            self.assertEqual(len(started), 3)
            self.assertEqual(len(self.listener.results['succeeded']), 1)
            expected_txn = Int64(initial_txn + 1)
            self.assertEqual(started[0].command['txnNumber'], expected_txn)
            self.assertEqual(started[0].command['lsid'], session.session_id)
            expected_txn = Int64(initial_txn + 2)
            self.assertEqual(started[1].command['txnNumber'], expected_txn)
            self.assertEqual(started[1].command['lsid'], session.session_id)
            started[1].command.pop('$clusterTime')
            started[2].command.pop('$clusterTime')
            self.assertEqual(started[1].command, started[2].command)
            final_txn = session._server_session._transaction_id
            self.assertEqual(final_txn, expected_txn)
        self.assertEqual(coll.find_one(projection={'_id': True}), {'_id': 1})


# TODO: Make this a real integration test where we stepdown the primary.
class TestRetryableWritesTxnNumber(IgnoreDeprecationsTest):
    @client_context.require_version_min(3, 6)
    @client_context.require_replica_set
    def test_increment_transaction_id_without_sending_command(self):
        """Test that the txnNumber field is properly incremented, even when
        the first attempt fails before sending the command.
        """
        listener = OvertCommandListener()
        client = rs_or_single_client(
            retryWrites=True, event_listeners=[listener])
        topology = client._topology
        select_server = topology.select_server

        def raise_connection_err_select_server(*args, **kwargs):
            # Raise ConnectionFailure on the first attempt and perform
            # normal selection on the retry attempt.
            topology.select_server = select_server
            raise ConnectionFailure('Connection refused')

        for method, args, kwargs in _retryable_single_statement_ops(
                client.db.retryable_write_test):
            listener.results.clear()
            topology.select_server = raise_connection_err_select_server
            with client.start_session() as session:
                kwargs = copy.deepcopy(kwargs)
                kwargs['session'] = session
                msg = '%s(*%r, **%r)' % (method.__name__, args, kwargs)
                initial_txn_id = session._server_session.transaction_id

                # Each operation should fail on the first attempt and succeed
                # on the second.
                method(*args, **kwargs)
                self.assertEqual(len(listener.results['started']), 1, msg)
                retry_cmd = listener.results['started'][0].command
                sent_txn_id = retry_cmd['txnNumber']
                final_txn_id = session._server_session.transaction_id
                self.assertEqual(Int64(initial_txn_id + 1), sent_txn_id, msg)
                self.assertEqual(sent_txn_id, final_txn_id, msg)


if __name__ == '__main__':
    unittest.main()
