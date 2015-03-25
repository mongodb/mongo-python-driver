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

"""Test the topology module."""

import json
import os
import sys

sys.path[0:0] = [""]

from pymongo import read_preferences
from pymongo.common import clean_node
from pymongo.errors import AutoReconnect
from pymongo.ismaster import IsMaster
from pymongo.server_description import ServerDescription
from pymongo.settings import TopologySettings
from pymongo.server_selectors import writable_server_selector
from pymongo.topology import Topology
from test import unittest


# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('server_selection', 'server_selection'))


class MockSocketInfo(object):
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockPool(object):
    def __init__(self, *args, **kwargs):
        pass

    def reset(self):
        pass


class MockMonitor(object):
    def __init__(self, server_description, topology, pool, topology_settings):
        pass

    def open(self):
        pass

    def request_check(self):
        pass

    def close(self):
        pass


def get_addresses(server_list):
    seeds = []
    hosts = []
    for server in server_list:
        seeds.append(clean_node(server['address']))
        hosts.append(server['address'])
    return seeds, hosts


def make_server_description(server, hosts):
    """Make ServerDescription from server info from JSON file."""
    ismaster_response = {}
    ismaster_response['tags'] = server['tags']
    ismaster_response['ok'] = True
    ismaster_response['hosts'] = hosts

    server_type = server['type']

    if server_type != "Standalone" and server_type != "Mongos":
        ismaster_response['setName'] = True
    if server_type == "RSPrimary":
        ismaster_response['ismaster'] = True
    elif server_type == "RSSecondary":
        ismaster_response['secondary'] = True
    elif server_type == "Mongos":
        ismaster_response['msg'] = 'isdbgrid'

    return ServerDescription(clean_node(server['address']),
                             IsMaster(ismaster_response),
                             round_trip_time=server['avg_rtt_ms'])


class TestAllScenarios(unittest.TestCase):
    pass


def create_test(scenario_def):
    def run_scenario(self):

        # Initialize topologies.
        seeds, hosts = get_addresses(
            scenario_def['topology_description']['servers'])

        # "Eligible servers" is defined in the server selection spec as
        # the set of servers matching both the ReadPreference's mode
        # and tag sets.
        top_latency = Topology(
            TopologySettings(seeds=seeds, monitor_class=MockMonitor,
                             pool_class=MockPool))
        # "In latency window" is defined in the server selection
        # spec as the subset of suitable_servers that falls within the
        # allowable latency window.
        top_suitable = Topology(
            TopologySettings(seeds=seeds, local_threshold_ms=1000000,
                             monitor_class=MockMonitor,
                             pool_class=MockPool))

        # Update topologies with server descriptions.
        for server in scenario_def['topology_description']['servers']:
            server_description = make_server_description(server, hosts)
            top_suitable.on_change(server_description)
            top_latency.on_change(server_description)

        # Create server selector.
        if scenario_def["operation"] == "write":
            instance = writable_server_selector
        else:
            # Make first letter lowercase to match read_pref's modes.
            mode_string = scenario_def['read_preference']['mode']
            if mode_string:
                mode_string = mode_string[:1].lower() + mode_string[1:]

            mode = read_preferences.read_pref_mode_from_name(mode_string)
            tag_sets = None
            if scenario_def['read_preference']['tag_sets'][0]:
                tag_sets = scenario_def['read_preference']['tag_sets']
            instance = read_preferences.make_read_preference(mode, tag_sets)

        # Select servers.
        if not scenario_def['suitable_servers']:
            self.assertRaises(AutoReconnect, top_suitable.select_server,
                              instance,
                              server_selection_timeout=0)
            return

        if not scenario_def['in_latency_window']:
            self.assertRaises(AutoReconnect, top_latency.select_server,
                              instance,
                              server_selection_timeout=0)
            return

        actual_suitable_s = top_suitable.select_servers(instance,
                                                    server_selection_timeout=0)
        actual_latency_s = top_latency.select_servers(instance,
                                                    server_selection_timeout=0)

        expected_suitable_servers = {}
        for server in scenario_def['suitable_servers']:
            server_description = make_server_description(server, hosts)
            expected_suitable_servers[server['address']] = server_description

        actual_suitable_servers = {}
        for s in actual_suitable_s:
            actual_suitable_servers["%s:%d" % (s.description.address[0],
                                    s.description.address[1])] = s.description

        self.assertEqual(len(actual_suitable_servers),
                         len(expected_suitable_servers))
        for k, actual in actual_suitable_servers.items():
            expected = expected_suitable_servers[k]
            self.assertEqual(expected.address, actual.address)
            self.assertEqual(expected.server_type, actual.server_type)
            self.assertEqual(expected.round_trip_time, actual.round_trip_time)
            self.assertEqual(expected.tags, actual.tags)
            self.assertEqual(expected.all_hosts, actual.all_hosts)

        expected_latency_servers = {}
        for server in scenario_def['in_latency_window']:
            server_description = make_server_description(server, hosts)
            expected_latency_servers[server['address']] = server_description

        actual_latency_servers = {}
        for s in actual_latency_s:
            actual_latency_servers["%s:%d" %
                                   (s.description.address[0],
                                    s.description.address[1])] = s.description

        self.assertEqual(len(actual_latency_servers),
                         len(expected_latency_servers))
        for k, actual in actual_latency_servers.items():
            expected = expected_latency_servers[k]
            self.assertEqual(expected.address, actual.address)
            self.assertEqual(expected.server_type, actual.server_type)
            self.assertEqual(expected.round_trip_time, actual.round_trip_time)
            self.assertEqual(expected.tags, actual.tags)
            self.assertEqual(expected.all_hosts, actual.all_hosts)

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)
        dirname = os.path.split(dirname[-2])[-1] + '_' + dirname[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(scenario_stream)

            # Construct test from scenario.
            new_test = create_test(scenario_def)
            test_name = 'test_%s_%s' % (
                dirname, os.path.splitext(filename)[0])

            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()
