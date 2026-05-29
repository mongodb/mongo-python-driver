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
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from pymongo import MongoClient, ReadPreference, monitoring
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.hello import HelloCompat
from pymongo.operations import _Op
from pymongo.server_selectors import writable_server_selector
from pymongo.synchronous.settings import TopologySettings
from pymongo.synchronous.topology import Topology
from pymongo.typings import strip_optional

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, client_knobs, unittest
from test.utils import wait_until
from test.utils_selection_tests import (
    create_selection_tests,
    get_topology_settings_dict,
)
from test.utils_selection_tests_shared import (
    get_addresses,
    make_server_description,
)
from test.utils_shared import (
    FunctionCallRecorder,
    HeartbeatEventListener,
    OvertCommandListener,
)

_IS_SYNC = True

# Location of JSON test specifications.
if _IS_SYNC:
    TEST_PATH = os.path.join(
        Path(__file__).resolve().parent, "server_selection", "server_selection"
    )
else:
    TEST_PATH = os.path.join(
        Path(__file__).resolve().parent.parent, "server_selection", "server_selection"
    )


class SelectionStoreSelector:
    """No-op selector that keeps track of what was passed to it."""

    def __init__(self):
        self.selection = None

    def __call__(self, selection):
        self.selection = selection
        return selection


class TestAllScenarios(create_selection_tests(TEST_PATH)):  # type: ignore
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
        listener = OvertCommandListener()
        client = self.rs_or_single_client(
            server_selector=custom_selector, event_listeners=[listener]
        )
        coll = client.get_database("testdb", read_preference=ReadPreference.NEAREST).coll
        self.addCleanup(client.drop_database, "testdb")

        # Wait the node list to be fully populated.
        def all_hosts_started():
            return len((client.admin.command(HelloCompat.LEGACY_CMD))["hosts"]) == len(
                client._topology._description.readable_servers
            )

        wait_until(all_hosts_started, "receive heartbeat from all hosts")

        expected_port = max(
            [strip_optional(n.address[1]) for n in client._topology._description.readable_servers]
        )

        # Insert 1 record and access it 10 times.
        coll.insert_one({"name": "John Doe"})
        for _ in range(10):
            coll.find_one({"name": "John Doe"})

        # Confirm all find commands are run against appropriate host.
        for command in listener.started_events:
            if command.command_name == "find":
                self.assertEqual(command.connection_id[1], expected_port)

    def test_invalid_server_selector(self):
        # Client initialization must fail if server_selector is not callable.
        for selector_candidate in [[], 10, "string", {}]:
            with self.assertRaisesRegex(ValueError, "must be a callable"):
                MongoClient(connect=False, server_selector=selector_candidate)

        # None value for server_selector is OK.
        MongoClient(connect=False, server_selector=None)

    @client_context.require_replica_set
    def test_selector_called(self):
        selector = FunctionCallRecorder(lambda x: x)

        # Client setup.
        mongo_client = self.rs_or_single_client(server_selector=selector)
        test_collection = mongo_client.testdb.test_collection
        self.addCleanup(mongo_client.drop_database, "testdb")

        # Do N operations and test selector is called at least N-1 times due to fast path.
        test_collection.insert_one({"age": 20, "name": "John"})
        test_collection.insert_one({"age": 31, "name": "Jane"})
        test_collection.update_one({"name": "Jane"}, {"$set": {"age": 21}})
        test_collection.find_one({"name": "Roe"})
        self.assertGreaterEqual(selector.call_count, 3)

    @client_context.require_replica_set
    def test_latency_threshold_application(self):
        selector = SelectionStoreSelector()

        scenario_def: dict = {
            "topology_description": {
                "type": "ReplicaSetWithPrimary",
                "servers": [
                    {"address": "b:27017", "avg_rtt_ms": 10000, "type": "RSSecondary", "tag": {}},
                    {"address": "c:27017", "avg_rtt_ms": 20000, "type": "RSSecondary", "tag": {}},
                    {"address": "a:27017", "avg_rtt_ms": 30000, "type": "RSPrimary", "tag": {}},
                ],
            }
        }

        # Create & populate Topology such that all but one server is too slow.
        rtt_times = [srv["avg_rtt_ms"] for srv in scenario_def["topology_description"]["servers"]]
        min_rtt_idx = rtt_times.index(min(rtt_times))
        seeds, hosts = get_addresses(scenario_def["topology_description"]["servers"])
        settings = get_topology_settings_dict(
            heartbeat_frequency=1, local_threshold_ms=1, seeds=seeds, server_selector=selector
        )
        topology = Topology(TopologySettings(**settings))
        topology.open()
        for server in scenario_def["topology_description"]["servers"]:
            server_description = make_server_description(server, hosts)
            topology.on_change(server_description)

        # Invoke server selection and assert no filtering based on latency
        # prior to custom server selection logic kicking in.
        server = topology.select_server(ReadPreference.NEAREST, _Op.TEST)
        assert selector.selection is not None
        self.assertEqual(len(selector.selection), len(topology.description.server_descriptions()))

        # Ensure proper filtering based on latency after custom selection.
        self.assertEqual(server.description.address, seeds[min_rtt_idx])

    @client_context.require_replica_set
    def test_server_selector_bypassed(self):
        selector = FunctionCallRecorder(lambda x: x)

        scenario_def = {
            "topology_description": {
                "type": "ReplicaSetNoPrimary",
                "servers": [
                    {"address": "b:27017", "avg_rtt_ms": 10000, "type": "RSSecondary", "tag": {}},
                    {"address": "c:27017", "avg_rtt_ms": 20000, "type": "RSSecondary", "tag": {}},
                    {"address": "a:27017", "avg_rtt_ms": 30000, "type": "RSSecondary", "tag": {}},
                ],
            }
        }

        # Create & populate Topology such that no server is writeable.
        seeds, hosts = get_addresses(scenario_def["topology_description"]["servers"])
        settings = get_topology_settings_dict(
            heartbeat_frequency=1, local_threshold_ms=1, seeds=seeds, server_selector=selector
        )
        topology = Topology(TopologySettings(**settings))
        topology.open()
        for server in scenario_def["topology_description"]["servers"]:
            server_description = make_server_description(server, hosts)
            topology.on_change(server_description)

        # Invoke server selection and assert no calls to our custom selector.
        with self.assertRaisesRegex(ServerSelectionTimeoutError, "No primary available for writes"):
            topology.select_server(writable_server_selector, _Op.TEST, server_selection_timeout=0.1)
        self.assertEqual(selector.call_count, 0)

    @client_context.require_replica_set
    @client_context.require_failCommand_appName
    def test_server_selection_getMore_blocks(self):
        hb_listener = HeartbeatEventListener()
        client = self.rs_client(
            event_listeners=[hb_listener], heartbeatFrequencyMS=500, appName="heartbeatFailedClient"
        )
        coll = client.db.test
        coll.drop()
        docs = [{"x": 1} for _ in range(5)]
        coll.insert_many(docs)

        fail_heartbeat = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 4},
            "data": {
                "failCommands": [HelloCompat.LEGACY_CMD, "hello"],
                "closeConnection": True,
                "appName": "heartbeatFailedClient",
            },
        }

        def hb_failed(event):
            return isinstance(event, monitoring.ServerHeartbeatFailedEvent)

        cursor = coll.find({}, batch_size=1)
        cursor.next()  # force initial query that will pin the address for the getMore

        with self.fail_point(fail_heartbeat):
            wait_until(lambda: hb_listener.matching(hb_failed), "published failed event")
        self.assertEqual(len(cursor.to_list()), 4)


if __name__ == "__main__":
    unittest.main()
