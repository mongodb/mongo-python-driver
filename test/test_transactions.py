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

import collections
from functools import partial
import os
import re
import sys

sys.path[0:0] = [""]

from bson import json_util, py3compat
from bson.py3compat import iteritems
from bson.son import SON
from pymongo import client_session, operations, WriteConcern
from pymongo.client_session import TransactionOptions
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.errors import (ConfigurationError,
                            OperationFailure,
                            PyMongoError)
from pymongo.operations import IndexModel, InsertOne
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.results import _WriteResult, BulkWriteResult

from test import unittest, client_context, IntegrationTest
from test.utils import (OvertCommandListener,
                        rs_client,
                        single_client,
                        wait_until)
from test.utils_selection_tests import parse_read_preference

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'transactions')

_TXN_TESTS_DEBUG = os.environ.get('TRANSACTION_TESTS_DEBUG')

# Max number of operations to perform after a transaction to prove unpinning
# occurs. Chosen so that there's a low false positive rate. With 2 mongoses,
# 50 attempts yields a one in a quadrillion chance of a false positive
# (1/(0.5^50)).
UNPIN_TEST_MAX_ATTEMPTS = 50


# TODO: factor the following functions with CRUD v2 test runner.
def camel_to_snake(camel):
    # Regex to convert CamelCase to snake_case.
    snake = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake).lower()


def camel_to_upper_camel(camel):
    return camel[0].upper() + camel[1:]


def camel_to_snake_args(arguments):
    for arg_name in list(arguments):
        c2s = camel_to_snake(arg_name)
        arguments[c2s] = arguments.pop(arg_name)
    return arguments


class TestTransactions(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        super(TestTransactions, cls).setUpClass()
        cls.mongos_clients = []
        if client_context.supports_transactions():
            for address in client_context.mongoses:
                cls.mongos_clients.append(single_client('%s:%s' % address))

    def transaction_test_debug(self, msg):
        if _TXN_TESTS_DEBUG:
            print(msg)

    def assertErrorLabelsContain(self, exc, expected_labels):
        labels = [l for l in expected_labels if exc.has_error_label(l)]
        self.assertEqual(labels, expected_labels)

    def assertErrorLabelsOmit(self, exc, omit_labels):
        for label in omit_labels:
            self.assertFalse(
                exc.has_error_label(label),
                msg='error labels should not contain %s' % (label,))

    def set_fail_point(self, command_args):
        cmd = SON([('configureFailPoint', 'failCommand')])
        cmd.update(command_args)
        clients = self.mongos_clients if self.mongos_clients else [self.client]
        for client in clients:
            client.admin.command(cmd)

    def kill_all_sessions(self):
        clients = self.mongos_clients if self.mongos_clients else [self.client]
        for client in clients:
            # Run killAllSessions without an implicit session to work
            # around SERVER-38335.
            with client._socket_for_writes(None) as sock_info:
                spec = SON([('killAllSessions', [])])
                sock_info.command('admin', spec, client=client)

    @client_context.require_transactions
    def test_transaction_options_validation(self):
        default_options = TransactionOptions()
        self.assertIsNone(default_options.read_concern)
        self.assertIsNone(default_options.write_concern)
        self.assertIsNone(default_options.read_preference)
        TransactionOptions(read_concern=ReadConcern(),
                           write_concern=WriteConcern(),
                           read_preference=ReadPreference.PRIMARY)
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
            (db.create_collection, ['collection'], {}),
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

    def check_command_result(self, expected_result, result):
        # Only compare the keys in the expected result.
        filtered_result = {}
        for key in expected_result:
            try:
                filtered_result[key] = result[key]
            except KeyError:
                pass
        self.assertEqual(filtered_result, expected_result)

    # TODO: factor the following function with CRUD v2 test runner.
    def check_result(self, expected_result, result):
        if isinstance(result, _WriteResult):
            for res in expected_result:
                prop = camel_to_snake(res)
                # SPEC-869: Only BulkWriteResult has upserted_count.
                if (prop == "upserted_count"
                        and not isinstance(result, BulkWriteResult)):
                    if result.upserted_id is not None:
                        upserted_count = 1
                    else:
                        upserted_count = 0
                    self.assertEqual(upserted_count, expected_result[res], prop)
                elif prop == "inserted_ids":
                    # BulkWriteResult does not have inserted_ids.
                    if isinstance(result, BulkWriteResult):
                        self.assertEqual(len(expected_result[res]),
                                         result.inserted_count)
                    else:
                        # InsertManyResult may be compared to [id1] from the
                        # crud spec or {"0": id1} from the retryable write spec.
                        ids = expected_result[res]
                        if isinstance(ids, dict):
                            ids = [ids[str(i)] for i in range(len(ids))]
                        self.assertEqual(ids, result.inserted_ids, prop)
                elif prop == "upserted_ids":
                    # Convert indexes from strings to integers.
                    ids = expected_result[res]
                    expected_ids = {}
                    for str_index in ids:
                        expected_ids[int(str_index)] = ids[str_index]
                    self.assertEqual(expected_ids, result.upserted_ids, prop)
                else:
                    self.assertEqual(
                        getattr(result, prop), expected_result[res], prop)

            return True
        else:
            self.assertEqual(result, expected_result)

    def run_operation(self, sessions, collection, operation):
        name = camel_to_snake(operation['name'])
        if name == 'run_command':
            name = 'command'
        self.transaction_test_debug(name)

        def parse_options(opts):
            if 'readPreference' in opts:
                opts['read_preference'] = parse_read_preference(
                    opts.pop('readPreference'))

            if 'writeConcern' in opts:
                opts['write_concern'] = WriteConcern(
                    **dict(opts.pop('writeConcern')))

            if 'readConcern' in opts:
                opts['read_concern'] = ReadConcern(
                    **dict(opts.pop('readConcern')))
            return opts

        database = collection.database
        collection = database.get_collection(collection.name)
        if 'collectionOptions' in operation:
            collection = collection.with_options(
                **dict(parse_options(operation['collectionOptions'])))

        objects = {'database': database, 'collection': collection}
        objects.update(sessions)
        obj = objects[operation['object']]

        # Combine arguments with options and handle special cases.
        arguments = operation.get('arguments', {})
        arguments.update(arguments.pop("options", {}))
        parse_options(arguments)

        cmd = getattr(obj, name)

        for arg_name in list(arguments):
            c2s = camel_to_snake(arg_name)
            # PyMongo accepts sort as list of tuples.
            if arg_name == "sort":
                sort_dict = arguments[arg_name]
                arguments[arg_name] = list(iteritems(sort_dict))
            # Named "key" instead not fieldName.
            if arg_name == "fieldName":
                arguments["key"] = arguments.pop(arg_name)
            # Aggregate uses "batchSize", while find uses batch_size.
            elif arg_name == "batchSize" and name == "aggregate":
                continue
            # Requires boolean returnDocument.
            elif arg_name == "returnDocument":
                arguments[c2s] = arguments[arg_name] == "After"
            elif c2s == "requests":
                # Parse each request into a bulk write model.
                requests = []
                for request in arguments["requests"]:
                    bulk_model = camel_to_upper_camel(request["name"])
                    bulk_class = getattr(operations, bulk_model)
                    bulk_arguments = camel_to_snake_args(request["arguments"])
                    requests.append(bulk_class(**dict(bulk_arguments)))
                arguments["requests"] = requests
            elif arg_name == "session":
                arguments['session'] = sessions[arguments['session']]
            elif name == 'command' and arg_name == 'command':
                # Ensure the first key is the command name.
                ordered_command = SON([(operation['command_name'], 1)])
                ordered_command.update(arguments['command'])
                arguments['command'] = ordered_command
            else:
                arguments[c2s] = arguments.pop(arg_name)

        result = cmd(**dict(arguments))

        if name == "aggregate":
            if arguments["pipeline"] and "$out" in arguments["pipeline"][-1]:
                # Read from the primary to ensure causal consistency.
                out = collection.database.get_collection(
                    arguments["pipeline"][-1]["$out"],
                    read_preference=ReadPreference.PRIMARY)
                return out.find()

        if isinstance(result, Cursor) or isinstance(result, CommandCursor):
            return list(result)

        return result

    # TODO: factor with test_command_monitoring.py
    def check_events(self, test, listener, session_ids):
        res = listener.results
        if not len(test['expectations']):
            return

        self.assertEqual(len(res['started']), len(test['expectations']))
        for i, expectation in enumerate(test['expectations']):
            event_type = next(iter(expectation))
            event = res['started'][i]

            # The tests substitute 42 for any number other than 0.
            if (event.command_name == 'getMore'
                    and event.command['getMore']):
                event.command['getMore'] = 42
            elif event.command_name == 'killCursors':
                event.command['cursors'] = [42]

            # Replace afterClusterTime: 42 with actual afterClusterTime.
            expected_cmd = expectation[event_type]['command']
            expected_read_concern = expected_cmd.get('readConcern')
            if expected_read_concern is not None:
                time = expected_read_concern.get('afterClusterTime')
                if time == 42:
                    actual_time = event.command.get(
                        'readConcern', {}).get('afterClusterTime')
                    if actual_time is not None:
                        expected_read_concern['afterClusterTime'] = actual_time

            # Replace lsid with a name like "session0" to match test.
            if 'lsid' in event.command:
                for name, lsid in session_ids.items():
                    if event.command['lsid'] == lsid:
                        event.command['lsid'] = name
                        break

            for attr, expected in expectation[event_type].items():
                actual = getattr(event, attr)
                if isinstance(expected, dict):
                    for key, val in expected.items():
                        if val is None:
                            if key in actual:
                                self.fail("Unexpected key [%s] in %r" % (
                                    key, actual))
                        elif key not in actual:
                            self.fail("Expected key [%s] in %r" % (
                                key, actual))
                        else:
                            self.assertEqual(val, actual[key],
                                             "Key [%s] in %s" % (key, actual))
                else:
                    self.assertEqual(actual, expected)


def expect_error_message(expected_result):
    if isinstance(expected_result, dict):
        return expected_result['errorContains']

    return False


def expect_error_code(expected_result):
    if isinstance(expected_result, dict):
        return expected_result['errorCodeName']

    return False


def expect_error_labels_contain(expected_result):
    if isinstance(expected_result, dict):
        return expected_result['errorLabelsContain']

    return False


def expect_error_labels_omit(expected_result):
    if isinstance(expected_result, dict):
        return expected_result['errorLabelsOmit']

    return False


def expect_error(expected_result):
    return (expect_error_message(expected_result)
            or expect_error_code(expected_result)
            or expect_error_labels_contain(expected_result)
            or expect_error_labels_omit(expected_result))


def end_sessions(sessions):
    for s in sessions.values():
        # Aborts the transaction if it's open.
        s.end_session()


def create_test(scenario_def, test):
    def run_scenario(self):
        if test.get('skipReason'):
            raise unittest.SkipTest(test.get('skipReason'))

        listener = OvertCommandListener()
        # Create a new client, to avoid interference from pooled sessions.
        # Convert test['clientOptions'] to dict to avoid a Jython bug using
        # "**" with ScenarioDict.
        client_options = dict(test['clientOptions'])
        if client_context.is_mongos:
            client = rs_client(client_context.mongos_seeds(),
                               event_listeners=[listener], **client_options)
        else:
            client = rs_client(event_listeners=[listener], **client_options)
        # Close the client explicitly to avoid having too many threads open.
        self.addCleanup(client.close)

        # Kill all sessions before and after each test to prevent an open
        # transaction (from a test failure) from blocking collection/database
        # operations during test set up and tear down.
        self.kill_all_sessions()
        self.addCleanup(self.kill_all_sessions)

        database_name = scenario_def['database_name']
        collection_name = scenario_def['collection_name']
        write_concern_db = client.get_database(
            database_name, write_concern=WriteConcern(w='majority'))
        write_concern_coll = write_concern_db[collection_name]
        write_concern_coll.drop()
        write_concern_db.create_collection(collection_name)
        if scenario_def['data']:
            # Load data.
            write_concern_coll.insert_many(scenario_def['data'])

        # Create session0 and session1.
        sessions = {}
        session_ids = {}
        for i in range(2):
            session_name = 'session%d' % i
            opts = camel_to_snake_args(test['sessionOptions'][session_name])
            if 'default_transaction_options' in opts:
                txn_opts = opts['default_transaction_options']
                if 'readConcern' in txn_opts:
                    read_concern = ReadConcern(
                        **dict(txn_opts['readConcern']))
                else:
                    read_concern = None
                if 'writeConcern' in txn_opts:
                    write_concern = WriteConcern(
                        **dict(txn_opts['writeConcern']))
                else:
                    write_concern = None

                if 'readPreference' in txn_opts:
                    read_pref = parse_read_preference(
                        txn_opts['readPreference'])
                else:
                    read_pref = None

                txn_opts = client_session.TransactionOptions(
                    read_concern=read_concern,
                    write_concern=write_concern,
                    read_preference=read_pref,
                )
                opts['default_transaction_options'] = txn_opts

            s = client.start_session(**dict(opts))

            sessions[session_name] = s
            # Store lsid so we can access it after end_session, in check_events.
            session_ids[session_name] = s.session_id

        self.addCleanup(end_sessions, sessions)

        if 'failPoint' in test:
            self.set_fail_point(test['failPoint'])
            self.addCleanup(self.set_fail_point, {
                'configureFailPoint': 'failCommand', 'mode': 'off'})

        listener.results.clear()
        collection = client[database_name][collection_name]

        for op in test['operations']:
            expected_result = op.get('result')
            if expect_error(expected_result):
                with self.assertRaises(PyMongoError,
                                       msg=op['name']) as context:
                    self.run_operation(sessions, collection, op.copy())

                if expect_error_message(expected_result):
                    self.assertIn(expected_result['errorContains'].lower(),
                                  str(context.exception).lower())
                if expect_error_code(expected_result):
                    self.assertEqual(expected_result['errorCodeName'],
                                     context.exception.details.get('codeName'))
                if expect_error_labels_contain(expected_result):
                    self.assertErrorLabelsContain(
                        context.exception,
                        expected_result['errorLabelsContain'])
                if expect_error_labels_omit(expected_result):
                    self.assertErrorLabelsOmit(
                        context.exception,
                        expected_result['errorLabelsOmit'])
            else:
                result = self.run_operation(sessions, collection, op.copy())
                if 'result' in op:
                    if op['name'] == 'runCommand':
                        self.check_command_result(expected_result, result)
                    else:
                        self.check_result(expected_result, result)

        for s in sessions.values():
            s.end_session()

        self.check_events(test, listener, session_ids)

        # Disable fail points.
        if 'failPoint' in test:
            self.set_fail_point({
                'configureFailPoint': 'failCommand', 'mode': 'off'})

        # Assert final state is expected.
        expected_c = test['outcome'].get('collection')
        if expected_c is not None:
            # Read from the primary with local read concern to ensure causal
            # consistency.
            primary_coll = collection.with_options(
                read_preference=ReadPreference.PRIMARY,
                read_concern=ReadConcern('local'))
            self.assertEqual(list(primary_coll.find()), expected_c['data'])

    return run_scenario


class ScenarioDict(dict):
    """Dict that returns {} for any unknown key, recursively."""
    def __init__(self, data):
        def convert(v):
            if isinstance(v, collections.Mapping):
                return ScenarioDict(v)
            if isinstance(v, py3compat.string_type):
                return v
            if isinstance(v, collections.Sequence):
                return [convert(item) for item in v]
            return v

        dict.__init__(self, [(k, convert(v)) for k, v in data.items()])

    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            # Unlike a defaultdict, don't set the key, just return a dict.
            return ScenarioDict({})


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:
            test_type, ext = os.path.splitext(filename)
            if ext != '.json':
                continue

            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = ScenarioDict(
                    json_util.loads(scenario_stream.read()))

            # Construct test from scenario.
            for test in scenario_def['tests']:
                test_name = 'test_%s_%s_%s' % (
                    dirname,
                    test_type.replace("-", "_"),
                    str(test['description'].replace(" ", "_")))

                new_test = create_test(scenario_def, test)
                new_test = client_context.require_transactions(new_test)

                if 'secondary' in test_name:
                    new_test = client_context._require(
                        lambda: client_context.has_secondaries,
                        'No secondaries',
                        new_test)

                new_test.__name__ = test_name
                setattr(TestTransactions, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()
