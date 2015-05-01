# Copyright 2014-2015 MongoDB, Inc.
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

import os
import sys
import threading

sys.path[0:0] = [""]

from bson import json_util
from pymongo import common
from pymongo.topology import Topology
from pymongo.topology_description import TOPOLOGY_TYPE
from pymongo.ismaster import IsMaster
from pymongo.server_description import ServerDescription, SERVER_TYPE
from pymongo.settings import TopologySettings
from pymongo.uri_parser import parse_uri
from test import unittest


# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'discovery_and_monitoring')


class MockSocketInfo(object):
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockPool(object):
    def __init__(self, *args, **kwargs):
        self.pool_id = 0
        self._lock = threading.Lock()

    def reset(self):
        with self._lock:
            self.pool_id += 1


class MockMonitor(object):
    def __init__(self, server_description, topology, pool, topology_settings):
        self._server_description = server_description
        self._topology = topology

    def open(self):
        pass

    def request_check(self):
        pass

    def close(self):
        pass


def create_mock_topology(uri, monitor_class=MockMonitor):
    # Some tests in the spec include URIs like mongodb://A/?connect=direct,
    # but PyMongo considers any single-seed URI with no setName to be "direct".
    parsed_uri = parse_uri(uri.replace('connect=direct', ''))
    replica_set_name = None
    if 'replicaset' in parsed_uri['options']:
        replica_set_name = parsed_uri['options']['replicaset']

    topology_settings = TopologySettings(
        parsed_uri['nodelist'],
        replica_set_name=replica_set_name,
        pool_class=MockPool,
        monitor_class=monitor_class)

    c = Topology(topology_settings)
    c.open()
    return c


def got_ismaster(topology, server_address, ismaster_response):
    server_description = ServerDescription(
        server_address, IsMaster(ismaster_response), 0)

    topology.on_change(server_description)


def get_type(topology, hostname):
    description = topology.get_server_by_address((hostname, 27017)).description
    return description.server_type


class TestAllScenarios(unittest.TestCase):
    pass


def topology_type_name(topology_type):
    return TOPOLOGY_TYPE._fields[topology_type]


def server_type_name(server_type):
    return SERVER_TYPE._fields[server_type]


def check_outcome(self, topology, outcome):
    expected_servers = outcome['servers']

    # Check weak equality before proceeding.
    self.assertEqual(
        len(topology.description.server_descriptions()),
        len(expected_servers))

    # Since lengths are equal, every actual server must have a corresponding
    # expected server.
    for expected_server_address, expected_server in expected_servers.items():
        node = common.partition_node(expected_server_address)
        self.assertTrue(topology.has_server(node))
        actual_server = topology.get_server_by_address(node)
        actual_server_description = actual_server.description

        if expected_server['type'] == 'PossiblePrimary':
            # Special case, some tests in the spec include the PossiblePrimary
            # type, but only single-threaded drivers need that type. We call
            # possible primaries Unknown.
            expected_server_type = SERVER_TYPE.Unknown
        else:
            expected_server_type = getattr(
                SERVER_TYPE, expected_server['type'])

        self.assertEqual(
            server_type_name(expected_server_type),
            server_type_name(actual_server_description.server_type))

        self.assertEqual(
            expected_server['setName'],
            actual_server_description.replica_set_name)

    self.assertEqual(outcome['setName'], topology.description.replica_set_name)
    expected_topology_type = getattr(TOPOLOGY_TYPE, outcome['topologyType'])
    self.assertEqual(topology_type_name(expected_topology_type),
                     topology_type_name(topology.description.topology_type))


def create_test(scenario_def):
    def run_scenario(self):
        c = create_mock_topology(scenario_def['uri'])
        
        for phase in scenario_def['phases']:
            for response in phase['responses']:
                got_ismaster(c,
                             common.partition_node(response[0]),
                             response[1])

            check_outcome(self, c, phase['outcome'])

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json_util.loads(scenario_stream.read())

            # Construct test from scenario.
            new_test = create_test(scenario_def)
            test_name = 'test_%s_%s' % (
                dirname, os.path.splitext(filename)[0])

            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()
