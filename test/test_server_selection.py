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
from pymongo.topology import Topology
from pymongo.settings import TopologySettings

sys.path[0:0] = [""]

from test import client_context, unittest, IntegrationTest
from test.utils import rs_or_single_client, wait_until, EventListener
from test.utils_selection_tests import (
    create_selection_tests, get_addresses, get_topology_settings_dict,
    make_server_description)


# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('server_selection', 'server_selection'))


class TestAllScenarios(create_selection_tests(_TEST_PATH)):
    pass


class TestCustomServerSelectorFunction(IntegrationTest):
    @client_context.require_replica_set
    def test_functional_select_max_port_number_host(self):
        # Selector that returns server with highest port number.
        def custom_selector(servers):
            ports = [s.address[1] for s in servers]
            idx = ports.index(max(ports))
            return [servers[idx]]

        # Initialize client with appropriate listeners.
        listener = EventListener()
        client = rs_or_single_client(
            serverSelector=custom_selector, event_listeners=[listener])
        self.addCleanup(client.close)
        coll = client.get_database(
            'testdb', read_preference=ReadPreference.NEAREST).coll
        self.addCleanup(client.drop_database, 'testdb')

        # Wait the node list to be fully populated.
        def all_hosts_started():
            return (len(client.admin.command('isMaster')['hosts']) ==
                    len(client._topology._description.readable_servers))

        wait_until(all_hosts_started, 'receive heartbeat from all hosts')
        expected_port = max([
            n.address[1]
            for n in client._topology._description.readable_servers])

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
        # Test appropriate validation of serverSelector kwarg.
        for selector_candidate in [list(), 10, 'string', {}]:
            with self.assertRaisesRegex(ValueError, "must be a callable"):
                MongoClient(connect=False, serverSelector=selector_candidate)

    @client_context.require_replica_set
    def test_selector_called(self):
        # No-op selector that keeps track of how many times it is called.
        class _Selector(object):
            def __init__(self):
                self.call_count = 0

            def __call__(self, servers):
                self.call_count += 1
                return servers

        _selector = _Selector()

        # Client setup.
        mongo_client = rs_or_single_client(serverSelector=_selector)
        test_collection = mongo_client.testdb.test_collection
        self.addCleanup(mongo_client.drop_database, 'testdb')
        self.addCleanup(mongo_client.close)

        # Do N operations and test selector is called at least N times.
        test_collection.insert_one({'age': 20, 'name': 'John'})
        test_collection.insert_one({'age': 31, 'name': 'Jane'})
        test_collection.update_one({'name': 'Jane'}, {'$set': {'age': 21}})
        test_collection.find_one({'name': 'Roe'})
        self.assertGreaterEqual(_selector.call_count, 4)

    @client_context.require_replica_set
    def test_latency_threshold_application(self):
        # No-op selector that keeps track of what was passed to it.
        class _Selector(object):
            def __init__(self):
                self.selection = None

            def __call__(self, selection):
                self.selection = selection
                return selection

        _selector = _Selector()

        scenario_def = {
            'topology_description': {
                'type': 'ReplicaSetWithPrimary', 'servers': [
                    {'address': 'b:27017',
                     'avg_rtt_ms': 10000,
                     'type': 'RSSecondary',
                     'tag': {}},
                    {'address': 'c:27017',
                     'avg_rtt_ms': 20000,
                     'type': 'RSSecondary',
                     'tag': {}},
                    {'address': 'a:27017',
                     'avg_rtt_ms': 30000,
                     'type': 'RSPrimary',
                     'tag': {}},
                ]}}

        # Create & populate Topology such that all but one server is too slow.
        rtt_times = [srv['avg_rtt_ms'] for srv in
                     scenario_def['topology_description']['servers']]
        min_rtt_idx = rtt_times.index(min(rtt_times))
        seeds, hosts = get_addresses(
            scenario_def["topology_description"]["servers"])
        settings = get_topology_settings_dict(
            heartbeat_frequency=1, local_threshold_ms=1, seeds=seeds,
            server_selector=_selector)
        topology = Topology(TopologySettings(**settings))
        topology.open()
        for server in scenario_def['topology_description']['servers']:
            server_description = make_server_description(server, hosts)
            topology.on_change(server_description)

        # Invoke server selection and assert no filtering based on latency
        # prior to custom server selection logic kicking in.
        server = topology.select_server(ReadPreference.NEAREST)
        self.assertEqual(
            len(_selector.selection),
            len(topology.description.server_descriptions()))

        # Ensure proper filtering based on latency after custom selection.
        self.assertEqual(
            server.description.address, seeds[min_rtt_idx])


if __name__ == "__main__":
    unittest.main()
