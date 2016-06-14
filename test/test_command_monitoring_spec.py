# Copyright 2015 MongoDB, Inc.
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

import json
import os
import re
import sys

sys.path[0:0] = [""]

import pymongo

from bson.json_util import object_hook
from pymongo import monitoring
from pymongo.errors import OperationFailure
from pymongo.read_preferences import (make_read_preference,
                                      read_pref_mode_from_name)
from pymongo.write_concern import WriteConcern
from test import unittest, client_context
from test.utils import single_client, wait_until, EventListener

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
        cls.saved_listeners = monitoring._LISTENERS
        monitoring._LISTENERS = monitoring._Listeners([], [], [], [])
        cls.client = single_client(event_listeners=[cls.listener])

    @classmethod
    def tearDownClass(cls):
        monitoring._LISTENERS = cls.saved_listeners

    def tearDown(self):
        self.listener.results.clear()


def create_test(scenario_def):
    def run_scenario(self):
        self.assertTrue(scenario_def['tests'], "tests cannot be empty")
        dbname = scenario_def['database_name']
        collname = scenario_def['collection_name']

        for test in scenario_def['tests']:
            ver = client_context.version[:2]
            if "ignore_if_server_version_greater_than" in test:
                version = test["ignore_if_server_version_greater_than"]
                if ver > tuple(map(int, version.split("."))):
                    continue
            if "ignore_if_server_version_less_than" in test:
                version = test["ignore_if_server_version_less_than"]
                if ver < tuple(map(int, version.split("."))):
                    continue
            if "ignore_if_topology_type" in test:
                types = set(test["ignore_if_topology_type"])
                if "sharded" in types and client_context.is_mongos:
                    continue

            coll = self.client[dbname][collname]
            coll.drop()
            coll.insert_many(scenario_def['data'])
            self.listener.results.clear()
            name = camel_to_snake(test['operation']['name'])
            # Don't send $readPreference to mongos before 2.4.
            if (client_context.version.at_least(2, 4, 0)
                    and 'read_preference' in test['operation']):
                mode = read_pref_mode_from_name(
                    test['operation']['read_preference']['mode'])
                coll = coll.with_options(
                    read_preference=make_read_preference(mode, None))

            test_args = test['operation']['arguments']
            if 'writeConcern' in test_args:
                concern = test_args.pop('writeConcern')
                coll = coll.with_options(
                    write_concern=WriteConcern(**concern))
            args = {}
            for arg in test_args:
                args[camel_to_snake(arg)] = test_args[arg]

            if name == 'bulk_write':
                bulk_args = []
                for request in args['requests']:
                    opname = next(iter(request))
                    klass = opname[0:1].upper() + opname[1:]
                    arg = getattr(pymongo, klass)(**request[opname])
                    bulk_args.append(arg)
                try:
                    coll.bulk_write(bulk_args, args.get('ordered', True))
                except OperationFailure:
                    pass
            elif name == 'find':
                if 'sort' in args:
                    args['sort'] = list(args['sort'].items())
                try:
                    # Iterate the cursor.
                    tuple(coll.find(**args))
                except OperationFailure:
                    pass
                # Wait for the killCursors thread to run if necessary.
                if 'limit' in args and client_context.version[:2] < (3, 1):
                    self.client._kill_cursors_executor.wake()
                    started = self.listener.results['started']
                    wait_until(
                        lambda: started[-1].command_name == 'killCursors',
                        "publish a start event for killCursors.")
            else:
                try:
                    getattr(coll, name)(**args)
                except OperationFailure:
                    pass

            for expectation in test['expectations']:
                event_type = next(iter(expectation))
                if event_type == "command_started_event":
                    event = self.listener.results['started'].pop(0)
                    # The tests substitute 42 for any number other than 0.
                    if (event.command_name == 'getMore'
                            and event.command['getMore']):
                        event.command['getMore'] = 42
                    elif event.command_name == 'killCursors':
                        event.command['cursors'] = [42]
                elif event_type == "command_succeeded_event":
                    event = self.listener.results['succeeded'].pop(0)
                    reply = event.reply
                    # The tests substitute 42 for any number other than 0,
                    # and "" for any error message.
                    if 'writeErrors' in reply:
                        for doc in reply['writeErrors']:
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
                elif event_type == "command_failed_event":
                    event = self.listener.results['failed'].pop(0)
                else:
                    self.fail("Unknown event type")
                for attr, expected in expectation[event_type].items():
                    actual = getattr(event, attr)
                    if isinstance(expected, dict):
                        for key, val in expected.items():
                            self.assertEqual(val, actual[key])
                    else:
                        self.assertEqual(actual, expected)

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(
                    scenario_stream, object_hook=object_hook)
            # Construct test from scenario.
            new_test = create_test(scenario_def)
            test_name = 'test_%s' % (os.path.splitext(filename)[0],)
            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()
