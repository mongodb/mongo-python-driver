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

"""Run the command monitoring spec tests."""

import os
import re
import sys

sys.path[0:0] = [""]

import pymongo

from bson import json_util
from pymongo.errors import OperationFailure
from pymongo.write_concern import WriteConcern
from test import unittest, client_context
from test.utils import single_client, wait_until, EventListener, parse_read_preference

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'command_monitoring')


def camel_to_snake(camel):
    # Regex to convert CamelCase to snake_case.
    snake = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake).lower()


class TestAllScenarios(unittest.TestCase):

    @classmethod
    @client_context.require_connection
    def setUpClass(cls):
        cls.listener = EventListener()
        cls.client = single_client(event_listeners=[cls.listener])

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def tearDown(self):
        self.listener.results.clear()


def format_actual_results(results):
    started = results['started']
    succeeded = results['succeeded']
    failed = results['failed']
    msg = "\nStarted:   %r" % (started[0].command if len(started) else None,)
    msg += "\nSucceeded: %r" % (succeeded[0].reply if len(succeeded) else None,)
    msg += "\nFailed:    %r" % (failed[0].failure if len(failed) else None,)
    return msg


def create_test(scenario_def, test):
    def run_scenario(self):
        dbname = scenario_def['database_name']
        collname = scenario_def['collection_name']

        coll = self.client[dbname][collname]
        coll.drop()
        coll.insert_many(scenario_def['data'])
        self.listener.results.clear()
        name = camel_to_snake(test['operation']['name'])
        if 'read_preference' in test['operation']:
            coll = coll.with_options(read_preference=parse_read_preference(
                test['operation']['read_preference']))
        if 'collectionOptions' in test['operation']:
            colloptions = test['operation']['collectionOptions']
            if 'writeConcern' in colloptions:
                concern = colloptions['writeConcern']
                coll = coll.with_options(
                    write_concern=WriteConcern(**concern))

        test_args = test['operation']['arguments']
        if 'options' in test_args:
            options = test_args.pop('options')
            test_args.update(options)
        args = {}
        for arg in test_args:
            args[camel_to_snake(arg)] = test_args[arg]

        if name == 'bulk_write':
            bulk_args = []
            for request in args['requests']:
                opname = request['name']
                klass = opname[0:1].upper() + opname[1:]
                arg = getattr(pymongo, klass)(**request['arguments'])
                bulk_args.append(arg)
            try:
                coll.bulk_write(bulk_args, args.get('ordered', True))
            except OperationFailure:
                pass
        elif name == 'find':
            if 'sort' in args:
                args['sort'] = list(args['sort'].items())
            for arg in 'skip', 'limit':
                if arg in args:
                    args[arg] = int(args[arg])
            try:
                # Iterate the cursor.
                tuple(coll.find(**args))
            except OperationFailure:
                pass
            # Wait for the killCursors thread to run if necessary.
            if 'limit' in args and client_context.version[:2] < (3, 1):
                self.client._kill_cursors_executor.wake()
                started = self.listener.results['started']
                succeeded = self.listener.results['succeeded']
                wait_until(
                    lambda: started[-1].command_name == 'killCursors',
                    "publish a start event for killCursors.")
                wait_until(
                    lambda: succeeded[-1].command_name == 'killCursors',
                    "publish a succeeded event for killCursors.")
        else:
            try:
                getattr(coll, name)(**args)
            except OperationFailure:
                pass

        res = self.listener.results
        for expectation in test['expectations']:
            event_type = next(iter(expectation))
            if event_type == "command_started_event":
                event = res['started'][0] if len(res['started']) else None
                if event is not None:
                    # The tests substitute 42 for any number other than 0.
                    if (event.command_name == 'getMore'
                            and event.command['getMore']):
                        event.command['getMore'] = 42
                    elif event.command_name == 'killCursors':
                        event.command['cursors'] = [42]
            elif event_type == "command_succeeded_event":
                event = (
                    res['succeeded'].pop(0) if len(res['succeeded']) else None)
                if event is not None:
                    reply = event.reply
                    # The tests substitute 42 for any number other than 0,
                    # and "" for any error message.
                    if 'writeErrors' in reply:
                        for doc in reply['writeErrors']:
                            # Remove any new fields the server adds. The tests
                            # only have index, code, and errmsg.
                            diff = set(doc) - set(['index', 'code', 'errmsg'])
                            for field in diff:
                                doc.pop(field)
                            doc['code'] = 42
                            doc['errmsg'] = ""
                    elif 'cursor' in reply:
                        if reply['cursor']['id']:
                            reply['cursor']['id'] = 42
                    elif event.command_name == 'killCursors':
                        # Make the tests continue to pass when the killCursors
                        # command is actually in use.
                        if 'cursorsKilled' in reply:
                            reply.pop('cursorsKilled')
                        reply['cursorsUnknown'] = [42]
                    # Found succeeded event. Pop related started event.
                    res['started'].pop(0)
            elif event_type == "command_failed_event":
                event = res['failed'].pop(0) if len(res['failed']) else None
                if event is not None:
                    # Found failed event. Pop related started event.
                    res['started'].pop(0)
            else:
                self.fail("Unknown event type")

            if event is None:
                event_name = event_type.split('_')[1]
                self.fail(
                    "Expected %s event for %s command. Actual "
                    "results:%s" % (
                        event_name,
                        expectation[event_type]['command_name'],
                        format_actual_results(res)))

            for attr, expected in expectation[event_type].items():
                if 'options' in expected:
                    options = expected.pop('options')
                    expected.update(options)
                actual = getattr(event, attr)
                if isinstance(expected, dict):
                    for key, val in expected.items():
                        self.assertEqual(val, actual[key])
                else:
                    self.assertEqual(actual, expected)

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)[-1]
        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json_util.loads(scenario_stream.read())
            assert bool(scenario_def.get('tests')), "tests cannot be empty"
            # Construct test from scenario.
            for test in scenario_def['tests']:
                new_test = create_test(scenario_def, test)
                if "ignore_if_server_version_greater_than" in test:
                    version = test["ignore_if_server_version_greater_than"]
                    ver = tuple(int(elt) for elt in version.split('.'))
                    new_test = client_context.require_version_max(*ver)(
                        new_test)
                if "ignore_if_server_version_less_than" in test:
                    version = test["ignore_if_server_version_less_than"]
                    ver = tuple(int(elt) for elt in version.split('.'))
                    new_test = client_context.require_version_min(*ver)(
                        new_test)
                if "ignore_if_topology_type" in test:
                    types = set(test["ignore_if_topology_type"])
                    if "sharded" in types:
                        new_test = client_context.require_no_mongos(None)(
                            new_test)

                test_name = 'test_%s_%s_%s' % (
                    dirname,
                    os.path.splitext(filename)[0],
                    str(test['description'].replace(" ", "_")))
                new_test.__name__ = test_name
                setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()
