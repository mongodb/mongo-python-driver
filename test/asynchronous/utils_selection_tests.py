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

"""Utilities for testing Server Selection and Max Staleness."""
from __future__ import annotations

import datetime
import os
import sys
from test.asynchronous import AsyncPyMongoTestCase
from test.asynchronous.utils import AsyncMockPool

sys.path[0:0] = [""]

from test import unittest
from test.pymongo_mocks import DummyMonitor
from test.utils_selection_tests_shared import (
    get_addresses,
    get_topology_type_name,
    make_server_description,
)
from test.utils_shared import parse_read_preference

from bson import json_util
from pymongo.asynchronous.settings import TopologySettings
from pymongo.asynchronous.topology import Topology
from pymongo.common import HEARTBEAT_FREQUENCY, clean_node
from pymongo.errors import AutoReconnect, ConfigurationError
from pymongo.operations import _Op
from pymongo.server_selectors import writable_server_selector

_IS_SYNC = False


def get_topology_settings_dict(**kwargs):
    settings = {
        "monitor_class": DummyMonitor,
        "heartbeat_frequency": HEARTBEAT_FREQUENCY,
        "pool_class": AsyncMockPool,
    }
    settings.update(kwargs)
    return settings


async def create_topology(scenario_def, **kwargs):
    # Initialize topologies.
    if "heartbeatFrequencyMS" in scenario_def:
        frequency = int(scenario_def["heartbeatFrequencyMS"]) / 1000.0
    else:
        frequency = HEARTBEAT_FREQUENCY

    seeds, hosts = get_addresses(scenario_def["topology_description"]["servers"])

    topology_type = get_topology_type_name(scenario_def)
    if topology_type == "LoadBalanced":
        kwargs.setdefault("load_balanced", True)
    # Force topology description to ReplicaSet
    elif topology_type in ["ReplicaSetNoPrimary", "ReplicaSetWithPrimary"]:
        kwargs.setdefault("replica_set_name", "rs")
    settings = get_topology_settings_dict(heartbeat_frequency=frequency, seeds=seeds, **kwargs)

    # "Eligible servers" is defined in the server selection spec as
    # the set of servers matching both the ReadPreference's mode
    # and tag sets.
    topology = Topology(TopologySettings(**settings))
    await topology.open()

    # Update topologies with server descriptions.
    for server in scenario_def["topology_description"]["servers"]:
        server_description = make_server_description(server, hosts)
        await topology.on_change(server_description)

    # Assert that descriptions match
    assert (
        scenario_def["topology_description"]["type"] == topology.description.topology_type_name
    ), topology.description.topology_type_name

    return topology


def create_test(scenario_def):
    async def run_scenario(self):
        _, hosts = get_addresses(scenario_def["topology_description"]["servers"])
        # "Eligible servers" is defined in the server selection spec as
        # the set of servers matching both the ReadPreference's mode
        # and tag sets.
        top_suitable = await create_topology(scenario_def, local_threshold_ms=1000000)

        # "In latency window" is defined in the server selection
        # spec as the subset of suitable_servers that falls within the
        # allowable latency window.
        top_latency = await create_topology(scenario_def)

        top_suitable_deprioritized_servers = [
            top_suitable.get_server_by_address(clean_node(server["address"]))
            for server in scenario_def.get("deprioritized_servers", [])
        ]
        top_latency_deprioritized_servers = [
            top_latency.get_server_by_address(clean_node(server["address"]))
            for server in scenario_def.get("deprioritized_servers", [])
        ]

        # Create server selector.
        if scenario_def.get("operation") == "write":
            pref = writable_server_selector
        else:
            # Make first letter lowercase to match read_pref's modes.
            pref_def = scenario_def["read_preference"]
            if scenario_def.get("error"):
                with self.assertRaises((ConfigurationError, ValueError)):
                    # Error can be raised when making Read Pref or selecting.
                    pref = parse_read_preference(pref_def)
                    await top_latency.select_server(pref, _Op.TEST)
                return

            pref = parse_read_preference(pref_def)

        # Select servers.
        if not scenario_def.get("suitable_servers"):
            with self.assertRaises(AutoReconnect):
                await top_suitable.select_server(
                    pref,
                    _Op.TEST,
                    server_selection_timeout=0,
                    deprioritized_servers=top_suitable_deprioritized_servers,
                )

            return

        if not scenario_def["in_latency_window"]:
            with self.assertRaises(AutoReconnect):
                await top_latency.select_server(
                    pref,
                    _Op.TEST,
                    server_selection_timeout=0,
                    deprioritized_servers=top_latency_deprioritized_servers,
                )

            return

        actual_suitable_s = await top_suitable.select_servers(
            pref,
            _Op.TEST,
            server_selection_timeout=0,
            deprioritized_servers=top_suitable_deprioritized_servers,
        )
        actual_latency_s = await top_latency.select_servers(
            pref,
            _Op.TEST,
            server_selection_timeout=0,
            deprioritized_servers=top_latency_deprioritized_servers,
        )

        expected_suitable_servers = {}
        for server in scenario_def["suitable_servers"]:
            server_description = make_server_description(server, hosts)
            expected_suitable_servers[server["address"]] = server_description

        actual_suitable_servers = {}
        for s in actual_suitable_s:
            actual_suitable_servers[
                "%s:%d" % (s.description.address[0], s.description.address[1])
            ] = s.description

        self.assertEqual(len(actual_suitable_servers), len(expected_suitable_servers))
        for k, actual in actual_suitable_servers.items():
            expected = expected_suitable_servers[k]
            self.assertEqual(expected.address, actual.address)
            self.assertEqual(expected.server_type, actual.server_type)
            self.assertEqual(expected.round_trip_time, actual.round_trip_time)
            self.assertEqual(expected.tags, actual.tags)
            self.assertEqual(expected.all_hosts, actual.all_hosts)

        expected_latency_servers = {}
        for server in scenario_def["in_latency_window"]:
            server_description = make_server_description(server, hosts)
            expected_latency_servers[server["address"]] = server_description

        actual_latency_servers = {}
        for s in actual_latency_s:
            actual_latency_servers[
                "%s:%d" % (s.description.address[0], s.description.address[1])
            ] = s.description

        self.assertEqual(len(actual_latency_servers), len(expected_latency_servers))
        for k, actual in actual_latency_servers.items():
            expected = expected_latency_servers[k]
            self.assertEqual(expected.address, actual.address)
            self.assertEqual(expected.server_type, actual.server_type)
            self.assertEqual(expected.round_trip_time, actual.round_trip_time)
            self.assertEqual(expected.tags, actual.tags)
            self.assertEqual(expected.all_hosts, actual.all_hosts)

    return run_scenario


def create_selection_tests(test_dir):
    class TestAllScenarios(AsyncPyMongoTestCase):
        pass

    for dirpath, _, filenames in os.walk(test_dir):
        dirname = os.path.split(dirpath)
        dirname = os.path.split(dirname[-2])[-1] + "_" + dirname[-1]

        for filename in filenames:
            if os.path.splitext(filename)[1] != ".json":
                continue
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json_util.loads(scenario_stream.read())

            # Construct test from scenario.
            new_test = create_test(scenario_def)
            test_name = f"test_{dirname}_{os.path.splitext(filename)[0]}"

            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)

    return TestAllScenarios
