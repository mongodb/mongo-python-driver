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
import threading
from test import IntegrationTest, client_context, unittest
from test.utils import (
    OvertCommandListener,
    TestCreator,
    get_pool,
    rs_client,
    wait_until,
)
from test.utils_selection_tests import create_topology

from pymongo.common import clean_node
from pymongo.read_preferences import ReadPreference

# Location of JSON test specifications.
TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), os.path.join("server_selection", "in_window")
)


class TestAllScenarios(unittest.TestCase):
    def run_scenario(self, scenario_def):
        topology = create_topology(scenario_def)

        # Update mock operation_count state:
        for mock in scenario_def["mocked_topology_state"]:
            address = clean_node(mock["address"])
            server = topology.get_server_by_address(address)
            server.pool.operation_count = mock["operation_count"]

        pref = ReadPreference.NEAREST
        counts = dict((address, 0) for address in topology._description.server_descriptions())

        # Number of times to repeat server selection
        iterations = scenario_def["iterations"]
        for _ in range(iterations):
            server = topology.select_server(pref, server_selection_timeout=0)
            counts[server.description.address] += 1

        # Verify expected_frequencies
        outcome = scenario_def["outcome"]
        tolerance = outcome["tolerance"]
        expected_frequencies = outcome["expected_frequencies"]
        for host_str, freq in expected_frequencies.items():
            address = clean_node(host_str)
            actual_freq = float(counts[address]) / iterations
            if freq == 0:
                # Should be exactly 0.
                self.assertEqual(actual_freq, 0)
            else:
                # Should be within 'tolerance'.
                self.assertAlmostEqual(actual_freq, freq, delta=tolerance)


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


class FinderThread(threading.Thread):
    def __init__(self, collection, iterations):
        super(FinderThread, self).__init__()
        self.daemon = True
        self.collection = collection
        self.iterations = iterations
        self.passed = False

    def run(self):
        for _ in range(self.iterations):
            self.collection.find_one({})
        self.passed = True


class TestProse(IntegrationTest):
    def frequencies(self, client, listener, n_finds=10):
        coll = client.test.test
        N_THREADS = 10
        threads = [FinderThread(coll, n_finds) for _ in range(N_THREADS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        for thread in threads:
            self.assertTrue(thread.passed)

        events = listener.results["started"]
        self.assertEqual(len(events), n_finds * N_THREADS)
        nodes = client.nodes
        self.assertEqual(len(nodes), 2)
        freqs = {address: 0.0 for address in nodes}
        for event in events:
            freqs[event.connection_id] += 1
        for address in freqs:
            freqs[address] = freqs[address] / float(len(events))
        return freqs

    @client_context.require_failCommand_appName
    @client_context.require_multiple_mongoses
    def test_load_balancing(self):
        listener = OvertCommandListener()
        # PYTHON-2584: Use a large localThresholdMS to avoid the impact of
        # varying RTTs.
        client = rs_client(
            client_context.mongos_seeds(),
            appName="loadBalancingTest",
            event_listeners=[listener],
            localThresholdMS=30000,
            minPoolSize=10,
        )
        self.addCleanup(client.close)
        wait_until(lambda: len(client.nodes) == 2, "discover both nodes")
        wait_until(lambda: len(get_pool(client).sockets) >= 10, "create 10 connections")
        # Delay find commands on
        delay_finds = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 10000},
            "data": {
                "failCommands": ["find"],
                "blockConnection": True,
                "blockTimeMS": 500,
                "appName": "loadBalancingTest",
            },
        }
        with self.fail_point(delay_finds):
            nodes = client_context.client.nodes
            self.assertEqual(len(nodes), 1)
            delayed_server = next(iter(nodes))
            freqs = self.frequencies(client, listener)
            self.assertLessEqual(freqs[delayed_server], 0.25)
        listener.reset()
        freqs = self.frequencies(client, listener, n_finds=100)
        self.assertAlmostEqual(freqs[delayed_server], 0.50, delta=0.15)


if __name__ == "__main__":
    unittest.main()
