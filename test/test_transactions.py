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
import os
import re
import sys

from bson import json_util, py3compat
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import (make_read_preference,
                                      read_pref_mode_from_name)

sys.path[0:0] = [""]

from bson.py3compat import iteritems
from pymongo import operations, WriteConcern
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.results import _WriteResult, BulkWriteResult

from test import unittest, client_context, IntegrationTest
from test.utils import EventListener, rs_client

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'transactions')

_TXN_TESTS_DEBUG = os.environ.get('TRANSACTION_TESTS_DEBUG')


# TODO: factor the following functions with test_crud.py.
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
    def transaction_test_debug(self, msg):
        if _TXN_TESTS_DEBUG:
            print(msg)

    # TODO: factor the following function with test_crud.py.
    def check_result(self, expected_result, result):
        if isinstance(result, Cursor) or isinstance(result, CommandCursor):
            self.assertEqual(list(result), expected_result)

        elif isinstance(result, _WriteResult):
            for res in expected_result:
                prop = camel_to_snake(res)
                # SPEC-869: Only BulkWriteResult has upserted_count.
                if (prop == "upserted_count"
                        and not isinstance(result, BulkWriteResult)):
                    if result.upserted_id is not None:
                        upserted_count = 1
                    else:
                        upserted_count = 0
                    self.assertEqual(upserted_count, expected_result[res])
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
                        self.assertEqual(ids,
                                         result.inserted_ids)
                elif prop == "upserted_ids":
                    # Convert indexes from strings to integers.
                    ids = expected_result[res]
                    expected_ids = {}
                    for str_index in ids:
                        expected_ids[int(str_index)] = ids[str_index]
                    self.assertEqual(expected_ids,
                                     result.upserted_ids)
                else:
                    self.assertEqual(getattr(result, prop),
                                     expected_result[res])
            return True
        else:
            self.assertEqual(result, expected_result)

    def run_operation(self, sessions, collection, operation):
        session = None
        name = camel_to_snake(operation['name'])
        self.transaction_test_debug(name)
        session_name = operation['arguments'].pop('session', None)
        if session_name:
            session = sessions[session_name]

        # Convert arguments to snake_case and handle special cases.
        arguments = operation['arguments']
        options = arguments.pop("options", {})
        for option_name in options:
            arguments[camel_to_snake(option_name)] = options[option_name]

        pref = arguments.pop('readPreference', None)
        if pref:
            mode = read_pref_mode_from_name(pref['mode'])
            collection = collection.with_options(
                read_preference=make_read_preference(mode, None))

        if name.endswith('_transaction'):
            cmd = getattr(session, name)
        else:
            cmd = getattr(collection, name)
            arguments['session'] = session

        if operation == "bulk_write":
            # Parse each request into a bulk write model.
            requests = []
            for request in arguments["requests"]:
                bulk_model = camel_to_upper_camel(request["name"])
                bulk_class = getattr(operations, bulk_model)
                bulk_arguments = camel_to_snake_args(request["arguments"])
                requests.append(bulk_class(**bulk_arguments))
            arguments["requests"] = requests
        else:
            for arg_name in list(arguments):
                c2s = camel_to_snake(arg_name)
                # PyMongo accepts sort as list of tuples. Asserting len=1
                # because ordering dicts from JSON in 2.6 is unwieldy.
                if arg_name == "sort":
                    sort_dict = arguments[arg_name]
                    assert len(sort_dict) == 1, 'test can only have 1 sort key'
                    arguments[arg_name] = list(iteritems(sort_dict))
                # Named "key" instead not fieldName.
                if arg_name == "fieldName":
                    arguments["key"] = arguments.pop(arg_name)
                # Aggregate uses "batchSize", while find uses batch_size.
                elif arg_name == "batchSize" and operation == "aggregate":
                    continue
                # Requires boolean returnDocument.
                elif arg_name == "returnDocument":
                    arguments[c2s] = arguments[arg_name] == "After"
                elif arg_name == "readConcern":
                    arguments[c2s] = ReadConcern(**arguments.pop(arg_name))
                elif arg_name == "writeConcern":
                    arguments[c2s] = WriteConcern(**arguments.pop(arg_name))
                else:
                    arguments[c2s] = arguments.pop(arg_name)

        result = cmd(**arguments)

        if operation == "aggregate":
            if arguments["pipeline"] and "$out" in arguments["pipeline"][-1]:
                out = collection.database[arguments["pipeline"][-1]["$out"]]
                return out.find()

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

            # Replace lsid with a name like "session0" to match test.
            if 'lsid' in event.command:
                for name, lsid in session_ids.items():
                    if event.command['lsid'] == lsid:
                        event.command['lsid'] = name
                        break

            # TODO: Allow stmtId for find/getMore, SERVER-33213.
            if event.command_name in ('find', 'getMore'):
                expectation[event_type]['command'].pop('stmtId', None)

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


def expect_error(expected_result):
    if isinstance(expected_result, dict):
        return expected_result['errorContains']

    return False


def end_sessions(sessions):
    for s in sessions.values():
        try:
            s.commit_transaction()
        except Exception:
            # Ignore errors from committing without an open transaction.
            pass

    for s in sessions.values():
        s.end_session()


def create_test(scenario_def, test):
    def run_scenario(self):
        listener = EventListener()
        # New client to avoid interference from pooled sessions.
        client = rs_client(event_listeners=[listener])
        write_concern_db = client.get_database(
            'transaction-tests', write_concern=WriteConcern(w='majority'))

        write_concern_db.test.drop()
        write_concern_db.create_collection('test')
        if scenario_def['data']:
            # Load data.
            write_concern_db.test.insert_many(scenario_def['data'])

        # Create session0 and session1.
        sessions = {}
        session_ids = {}
        for i in range(2):
            session_name = 'session%d' % i
            s = client.start_session(
                **camel_to_snake_args(test['transactionOptions'][session_name]))

            sessions[session_name] = s
            # Store lsid so we can access it after end_session, in check_events.
            session_ids[session_name] = s.session_id

        self.addCleanup(end_sessions, sessions)

        listener.results.clear()
        collection = client['transaction-tests'].test

        if _TXN_TESTS_DEBUG:
            self.transaction_test_debug("")

        for op in test['operations']:
            expected_result = op.get('result')
            if expect_error(expected_result):
                with self.assertRaises(PyMongoError) as context:
                    self.run_operation(sessions, collection, op.copy())

                self.assertIn(expected_result['errorContains'].lower(),
                              str(context.exception).lower())

            else:
                result = self.run_operation(sessions, collection, op.copy())
                self.check_result(expected_result, result)

        for s in sessions.values():
            s.end_session()

        self.check_events(test, listener, session_ids)

        # Assert final state is expected.
        expected_c = test['outcome'].get('collection')
        if expected_c is not None:
            self.assertEqual(list(collection.find()), expected_c['data'])

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
                new_test = client_context.require_version_min(3, 7)(new_test)
                new_test = client_context.require_replica_set(new_test)
                new_test = client_context._require(
                    not test.get('skipReason'),
                    test.get('skipReason'),
                    new_test)

                if 'secondary' in test_name:
                    new_test = client_context._require(
                        client_context.has_secondaries,
                        'No secondaries',
                        new_test)

                new_test.__name__ = test_name
                setattr(TestTransactions, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()
