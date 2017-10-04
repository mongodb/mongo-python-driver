# Copyright 2017-present MongoDB, Inc.
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

import json
import os
import re
import sys

sys.path[0:0] = [""]

from bson import SON

from pymongo.errors import (ConfigurationError,
                            ConnectionFailure,
                            OperationFailure,
                            ServerSelectionTimeoutError)
from pymongo.monitoring import _SENSITIVE_COMMANDS
from pymongo.mongo_client import MongoClient
from pymongo.write_concern import WriteConcern

from test import unittest, client_context, IntegrationTest
from test.utils import rs_or_single_client, EventListener, DeprecationFilter
from test.test_crud import run_operation

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'retryable_writes')


class CommandListener(EventListener):
    def started(self, event):
        if event.command_name.lower() in _SENSITIVE_COMMANDS:
            return
        super(CommandListener, self).started(event)

    def succeeded(self, event):
        if event.command_name.lower() in _SENSITIVE_COMMANDS:
            return
        super(CommandListener, self).succeeded(event)

    def failed(self, event):
        if event.command_name.lower() in _SENSITIVE_COMMANDS:
            return
        super(CommandListener, self).failed(event)


class TestAllScenarios(IntegrationTest):

    @classmethod
    @client_context.require_version_min(3, 5)
    @client_context.require_replica_set
    def setUpClass(cls):
        super(TestAllScenarios, cls).setUpClass()
        cls.client = rs_or_single_client(retryWrites=True)
        cls.db = cls.client.pymongo_test

    def tearDown(self):
        self.client.admin.command(SON([
            ('configureFailPoint', 'onPrimaryTransactionalWrite'),
            ('mode', 'off')]))

    def set_fail_point(self, command_args):
        cmd = SON([('configureFailPoint', 'onPrimaryTransactionalWrite')])
        cmd.update(command_args)
        self.client.admin.command(cmd)


def create_test(scenario_def, test):
    def run_scenario(self):
        # Load data.
        assert scenario_def['data'], "tests must have non-empty data"
        self.db.test.drop()
        self.db.test.insert_many(scenario_def['data'])

        # Set the failPoint
        self.set_fail_point(test['failPoint'])

        test_outcome = test['outcome']
        should_fail = test_outcome.get('error')
        error = None
        try:
            result = run_operation(self.db.test, test)
        except ConnectionFailure as exc:
            error = exc

        if should_fail:
            self.assertIsNotNone(error, 'should have raised an error')
        else:
            self.assertIsNone(error, 'should not have raised an error')

        # Assert final state is expected.
        expected_c = test_outcome.get('collection')
        if expected_c is not None:
            expected_name = expected_c.get('name')
            if expected_name is not None:
                db_coll = self.db[expected_name]
            else:
                db_coll = self.db.test
            self.assertEqual(list(db_coll.find()), expected_c['data'])
        expected_result = test_outcome.get('result')
        if expected_result is not None:
            self.assertTrue(result, expected_result)

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(scenario_stream)

            test_type = os.path.splitext(filename)[0]

            # Construct test from scenario.
            for test in scenario_def['tests']:
                new_test = create_test(scenario_def, test)
                test_name = 'test_%s_%s_%s' % (
                    dirname,
                    test_type,
                    str(test['description'].replace(" ", "_")))

                new_test.__name__ = test_name
                setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()


def retryable_single_statement_ops(coll):
    return [
        (coll.insert_one, [{}], {}),
        (coll.replace_one, [{}, {}], {}),
        (coll.update_one, [{}, {'$set': {'a': 1}}], {}),
        (coll.delete_one, [{}], {}),
        # Insert document for find_one_and_*.
        (coll.insert_one, [{}], {}),
        (coll.find_one_and_replace, [{}, {'a': 3}], {}),
        (coll.find_one_and_update, [{}, {'$set': {'a': 1}}], {}),
        (coll.find_one_and_delete, [{}, {}], {}),
        # Deprecated methods.
        # Insert document for update.
        (coll.insert_one, [{}], {}),
        # Non-multi update.
        (coll.update, [{}, {'$set': {'a': 1}}], {}),
        # Non-multi remove.
        (coll.remove, [{}], {'multi': False}),
        # Insert document for find_and_modify.
        (coll.insert_one, [{}], {}),
        # Replace.
        (coll.find_and_modify, [{}, {'a': 3}], {}),
        # Update.
        (coll.find_and_modify, [{}, {'$set': {'a': 1}}], {}),
        # Delete.
        (coll.find_and_modify, [{}, {}], {'remove': True}),
    ]


def non_retryable_single_statement_ops(coll):
    return [
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
    @client_context.require_version_min(3, 5)
    @client_context.require_no_standalone
    def setUpClass(cls):
        super(TestRetryableWrites, cls).setUpClass()
        cls.listener = CommandListener()
        cls.client = rs_or_single_client(
            retryWrites=True, event_listeners=[cls.listener])
        cls.db = cls.client.pymongo_test

    def setUp(self):
        if client_context.is_rs:
            self.client.admin.command(SON([
                ('configureFailPoint', 'onPrimaryTransactionalWrite'),
                ('mode', 'alwaysOn')]))

    def tearDown(self):
        if client_context.is_rs:
            self.client.admin.command(SON([
                ('configureFailPoint', 'onPrimaryTransactionalWrite'),
                ('mode', 'off')]))

    def test_supported_single_statement_no_retry(self):
        listener = CommandListener()
        client = rs_or_single_client(
            retryWrites=False, event_listeners=[listener])
        for method, args, kwargs in retryable_single_statement_ops(
                client.db.retryable_write_test):
            listener.results.clear()
            method(*args, **kwargs)
            for event in listener.results['started']:
                self.assertNotIn(
                    'txnNumber', event.command,
                    '%s sent txnNumber with %s' % (
                         method.__name__, event.command_name))

    def test_supported_single_statement(self):

        for method, args, kwargs in retryable_single_statement_ops(
                self.db.retryable_write_test):
            self.listener.results.clear()
            method(*args, **kwargs)
            commands_started = self.listener.results['started']
            self.assertEqual(len(self.listener.results['succeeded']), 1,
                             method.__name__)
            first_attempt = commands_started[0]
            self.assertIn(
                'lsid', first_attempt.command,
                '%s sent no lsid with %s' % (
                    method.__name__, first_attempt.command_name))
            initial_session_id = first_attempt.command['lsid']
            self.assertIn(
                'txnNumber', first_attempt.command,
                '%s sent no txnNumber with %s' % (
                    method.__name__, first_attempt.command_name))

            # The failpoint is only enabled on a replica set.
            if client_context.is_rs:
                self.assertEqual(len(self.listener.results['failed']), 1,
                                 method.__name__)
                initial_transaction_id = first_attempt.command['txnNumber']
                retry_attempt = commands_started[1]
                self.assertIn(
                    'lsid', retry_attempt.command,
                    '%s sent no lsid with %s' % (
                        method.__name__, first_attempt.command_name))
                self.assertEqual(
                    retry_attempt.command['lsid'], initial_session_id)
                self.assertIn(
                    'txnNumber', retry_attempt.command,
                    '%s sent no txnNumber with %s' % (
                        method.__name__, first_attempt.command_name))
                self.assertEqual(retry_attempt.command['txnNumber'],
                                 initial_transaction_id)

    def test_unsupported_single_statement(self):
        coll = self.db.retryable_write_test
        coll.insert_many([{}, {}])
        coll_w0 = coll.with_options(write_concern=WriteConcern(w=0))
        for method, args, kwargs in (non_retryable_single_statement_ops(coll) +
                                     retryable_single_statement_ops(coll_w0)):
            self.listener.results.clear()
            method(*args, **kwargs)
            started_events = self.listener.results['started']
            self.assertEqual(len(self.listener.results['succeeded']), 1,
                             method.__name__)
            self.assertEqual(len(started_events), 1, method.__name__)
            for event in started_events:
                self.assertNotIn(
                    'txnNumber', event.command,
                    '%s sent txnNumber with %s' % (
                        method.__name__, event.command_name))

    def test_server_selection_timeout_not_retried(self):
        """A ServerSelectionTimeoutError is not retried."""
        listener = CommandListener()
        client = MongoClient(
            'somedomainthatdoesntexist.org',
            serverSelectionTimeoutMS=10,
            retryWrites=True, event_listeners=[listener])
        for method, args, kwargs in retryable_single_statement_ops(
                client.db.retryable_write_test):
            listener.results.clear()
            with self.assertRaises(ServerSelectionTimeoutError):
                method(*args, **kwargs)
            self.assertEqual(len(listener.results['started']), 0)

    @client_context.require_replica_set
    def test_retry_timeout_raises_original_error(self):
        """A ServerSelectionTimeoutError on the retry attempt raises the
        original error.
        """
        listener = CommandListener()
        client = rs_or_single_client(
            retryWrites=True, event_listeners=[listener])
        socket_for_writes = client._socket_for_writes

        def mock_socket_for_writes(*args, **kwargs):
            sock_info = socket_for_writes(*args, **kwargs)

            def raise_error():
                raise ServerSelectionTimeoutError(
                    'No primary available for writes')
            # Raise ServerSelectionTimeout on the retry attempt.
            client._socket_for_writes = raise_error
            return sock_info

        for method, args, kwargs in retryable_single_statement_ops(
                client.db.retryable_write_test):
            listener.results.clear()
            client._socket_for_writes = mock_socket_for_writes
            with self.assertRaises(ConnectionFailure):
                method(*args, **kwargs)
            self.assertEqual(len(listener.results['started']), 1)


class TestRetryableWritesNotSupported(IgnoreDeprecationsTest):

    @client_context.require_version_max(3, 5, 0, -1)
    def test_raises_error(self):
        client = rs_or_single_client(retryWrites=True)
        coll = client.pymongo_test.test
        # No error running non-retryable operations.
        client.admin.command('isMaster')
        for method, args, kwargs in non_retryable_single_statement_ops(coll):
            method(*args, **kwargs)

        for method, args, kwargs in retryable_single_statement_ops(coll):
            with self.assertRaisesRegex(
                    ConfigurationError,
                    'Retryable writes are not supported by this MongoDB '
                    'deployment'):
                method(*args, **kwargs)

    @client_context.require_version_min(3, 5)
    @client_context.require_standalone
    def test_standalone_raises_error(self):
        client = rs_or_single_client(retryWrites=True)
        coll = client.pymongo_test.test
        # No error running non-retryable operations.
        client.admin.command('isMaster')
        for method, args, kwargs in non_retryable_single_statement_ops(coll):
            method(*args, **kwargs)

        for method, args, kwargs in retryable_single_statement_ops(coll):
            with self.assertRaisesRegex(
                    OperationFailure,
                    'Transaction numbers are only allowed on a replica set '
                    'member or mongos'):
                method(*args, **kwargs)


if __name__ == '__main__':
    unittest.main()
