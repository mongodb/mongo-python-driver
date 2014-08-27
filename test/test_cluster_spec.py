# Copyright 2009-2014 MongoDB, Inc.
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

"""Test the cluster module."""

import os
import sys
import yaml

sys.path[0:0] = [""]

import socket
import threading

from bson.py3compat import imap
from pymongo import common
from pymongo.cluster import Cluster
from pymongo.cluster_description import CLUSTER_TYPE
from pymongo.errors import (ConfigurationError,
                            ConnectionFailure,
                            InvalidOperation)
from pymongo.ismaster import IsMaster
from pymongo.monitor import Monitor
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription, SERVER_TYPE
from pymongo.server_selectors import (any_server_selector,
                                      writable_server_selector)
from pymongo.settings import ClusterSettings
from pymongo.uri_parser import parse_uri
from test import unittest


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

    def get_socket(self):
        return MockSocketInfo()

    def maybe_return_socket(self, _):
        pass

    def reset(self):
        with self._lock:
            self.pool_id += 1


class MockMonitor(object):
    def __init__(self, server_description, cluster, pool, cluster_settings):
        self._server_description = server_description
        self._cluster = cluster

    def start(self):
        pass

    def request_check(self):
        pass

    def close(self):
        pass


def create_mock_cluster(uri, monitor_class=MockMonitor):

    parsed_uri = parse_uri(uri)
    set_name = None
    if 'replicaset' in parsed_uri['options']:
        set_name = parsed_uri['options']['replicaset']

    cluster_settings = ClusterSettings(
        parsed_uri['nodelist'],
        set_name=set_name,
        pool_class=MockPool,
        monitor_class=monitor_class,
        heartbeat_frequency=99999999)

    c = Cluster(cluster_settings)
    c.open()
    return c


def got_ismaster(cluster, server_address, ismaster_response):
    server_description = ServerDescription(
        server_address,
        IsMaster(ismaster_response),
        MovingAverage([0]))

    cluster.on_change(server_description)


def get_type(cluster, hostname):
    description = cluster.get_server_by_address((hostname, 27017)).description
    return description.server_type


class TestAllScenarios(unittest.TestCase):
    pass


def check_outcome(self, cluster, outcome):
    expected_servers = outcome['servers']

    # Check weak equality before proceeding.
    self.assertTrue(len(cluster.description.server_descriptions()) == len(expected_servers))

    # Since lengths are equal, every actual server must
    # have a corresponding expected server
    for expected_server_address, expected_server in expected_servers.iteritems():
        node = common.partition_node(expected_server_address)
        self.assertTrue(cluster.has_server(node))
        actual_server_description = cluster.get_server_by_address(node).description

        expected_server_type = getattr(SERVER_TYPE, expected_server['type'])
        self.assertEqual(expected_server_type, actual_server_description.server_type)
        self.assertEqual(expected_server['setName'], actual_server_description.set_name)

    self.assertEqual(outcome['setName'], cluster.description.set_name)
    expected_cluster_type = getattr(CLUSTER_TYPE, outcome['clusterType'])
    self.assertEqual(expected_cluster_type, cluster.description.cluster_type)


def create_test(scenario_def):
    def run_scenario(self):

        c = create_mock_cluster(scenario_def['uri'])
        
        for phase in scenario_def['phases']:

            for response in phase['responses']:
                address = response[0].split(':')
                got_ismaster(c, common.partition_node(response[0]), response[1])

            check_outcome(self, c, phase['outcome'])

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk('./test/cluster'):

        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:

            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = yaml.load(scenario_stream)

            # Construct test from scenario
            new_test = create_test(scenario_def)
            test_name = 'test_{0}_{1}'.format(dirname, os.path.splitext(filename)[0])
            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()
