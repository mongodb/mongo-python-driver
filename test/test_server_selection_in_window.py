# Copyright 2020-present MongoDB, Inc.
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
from pymongo.common import clean_node, HEARTBEAT_FREQUENCY
from pymongo.pool import PoolOptions
from pymongo.read_preferences import ReadPreference
from pymongo.settings import TopologySettings
from pymongo.topology import Topology
from test import unittest
from test.utils_selection_tests import (
    get_addresses,
    get_topology_settings_dict,
    make_server_description)
from test.utils import TestCreator


# Location of JSON test specifications.
TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('server_selection', 'in_window'))

# Number of times to repeat server selection
REPEAT = 2000


class TestAllScenarios(unittest.TestCase):
    def run_scenario(self, scenario_def):
        # Initialize topologies.
        if 'heartbeatFrequencyMS' in scenario_def:
            frequency = int(scenario_def['heartbeatFrequencyMS']) / 1000.0
        else:
            frequency = HEARTBEAT_FREQUENCY

        seeds, hosts = get_addresses(
            scenario_def['topology_description']['servers'])

        settings = get_topology_settings_dict(
            heartbeat_frequency=frequency,
            seeds=seeds
        )
        # "In latency window" is defined in the server selection
        # spec as the subset of suitable_servers that falls within the
        # allowable latency window.
        settings['local_threshold_ms'] = 1000000
        topology = Topology(TopologySettings(**settings))
        topology.open()

        # Update topologies with server descriptions.
        for server in scenario_def['topology_description']['servers']:
            server_description = make_server_description(server, hosts)
            topology.on_change(server_description)

        # Update mock operation_count state:
        for in_window in scenario_def['in_window']:
            address = clean_node(in_window['address'])
            server = topology.get_server_by_address(address)
            server._operation_count = in_window['operation_count']

        pref = ReadPreference.NEAREST
        counts = dict((address, 0) for address in
                      topology._description.server_descriptions())

        for _ in range(REPEAT):
            server = topology.select_server(pref, server_selection_timeout=0)
            counts[server.description.address] += 1

        # Verify expected_frequencies
        expected_frequencies = scenario_def['expected_frequencies']
        for host_str, freq in expected_frequencies.items():
            address = clean_node(host_str)
            actual_freq = float(counts[address])/REPEAT
            if freq == 0:
                # Should be exactly 0.
                self.assertEqual(actual_freq, 0)
            else:
                # Should be within ~5%
                self.assertAlmostEqual(actual_freq, freq, delta=0.05)


def create_test(scenario_def, test, name):
    def run_scenario(self):
        self.run_scenario(scenario_def)

    return run_scenario


class CustomTestCreator(TestCreator):
    def tests(self, scenario_def):
        """Extract the tests from a spec file.

        Server selection in_window tests do not have a 'tests' field.
        The whole file represents a single test case.
        """
        return [scenario_def]


CustomTestCreator(create_test, TestAllScenarios, TEST_PATH).create_tests()

if __name__ == "__main__":
    unittest.main()
