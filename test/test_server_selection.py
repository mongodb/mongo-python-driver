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

"""Test the topology module's Server Selection Spec implementation."""

import os
import sys

from pymongo import MongoClient
from pymongo import ReadPreference

sys.path[0:0] = [""]

from test import unittest, IntegrationTest
from test.utils import rs_or_single_client, EventListener
from test.utils_selection_tests import create_selection_tests


# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('server_selection', 'server_selection'))


class TestAllScenarios(create_selection_tests(_TEST_PATH)):
    pass


class TestCustomServerSelectorFunction(IntegrationTest):
    def test_functional_select_max_port_number_host(self):
        # Selector that returns server with highest port number.
        def custom_selector(selection):
            if not selection:
                return selection.with_server_descriptions([])
            ports = [
                s.address[1] for s in selection.server_descriptions
            ]
            idx = ports.index(max(ports))
            return selection.with_server_descriptions(
                [selection.server_descriptions[idx]]
            )

        # Initialize client with appropriate listeners.
        listener = EventListener()
        client = rs_or_single_client(
            serverSelector=custom_selector,
            event_listeners=[listener]
        )
        self.addCleanup(client.close)
        coll = client.get_database(
            'testdb', read_preference=ReadPreference.NEAREST).coll
        self.addCleanup(client.drop_database, 'testdb')

        # Hack to wait long enough for server to populate node list.
        from time import sleep
        sleep(1)
        expected_port = max([n[1] for n in client.nodes])

        # Insert 1 record and access it 10 times.
        coll.insert_one({'name': 'John Doe'})
        for _ in range(10):
            coll.find_one({'name': 'John Doe'})

        # Confirm all find commands are run against appropriate host.
        for command in listener.results['started']:
            if command.command_name == 'find':
                self.assertEqual(
                    command.connection_id[1], expected_port)

    def test_invalid_server_selector(self):
        _bad_server_selector = 1
        with self.assertRaisesRegex(ValueError, "must be a callable"):
            _ = MongoClient(
                connect=False, serverSelector=list()
            )

    def test_selector_called(self):
        # Special selector that keeps track of how many times it is called.
        class _Selector(object):
            def __init__(self):
                self.call_count = 0

            def __call__(self, selection):
                self.call_count += 1
                return selection
        _selector = _Selector()

        # Client setup.
        mongo_client = rs_or_single_client(serverSelector=_selector)
        test_collection = mongo_client.testdb.test_collection
        self.addCleanup(mongo_client.drop_database, 'testdb')
        self.addCleanup(mongo_client.close)

        # Do N operations and test selector is called all N times.
        test_collection.insert_one({'age': 20, 'name': 'John'})
        test_collection.insert_one({'age': 31, 'name': 'Jane'})
        test_collection.update_one({'name': 'Jane'}, {'$set': {'age': 21}})
        test_collection.find_one({'name': 'Roe'})
        self.assertEqual(_selector.call_count, 4)


if __name__ == "__main__":
    unittest.main()
