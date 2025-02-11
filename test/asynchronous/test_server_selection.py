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
from pathlib import Path

from pymongo import AsyncMongoClient, ReadPreference
from pymongo.asynchronous.settings import TopologySettings
from pymongo.asynchronous.topology import Topology
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.hello import HelloCompat
from pymongo.operations import _Op
from pymongo.server_selectors import writable_server_selector
from pymongo.typings import strip_optional

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest
from test.asynchronous.utils_selection_tests import (
    create_selection_tests,
    get_addresses,
    get_topology_settings_dict,
    make_server_description,
)
from test.utils import (
    EventListener,
    FunctionCallRecorder,
    OvertCommandListener,
    async_wait_until,
)

_IS_SYNC = False

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


class TestCustomServerSelectorFunction(AsyncIntegrationTest):
    @async_client_context.require_replica_set
    async def test_functional_select_max_port_number_host(self):
        # Selector that returns server with highest port number.
        def custom_selector(servers):
            ports = [s.address[1] for s in servers]
            idx = ports.index(max(ports))
            return [servers[idx]]

        # Initialize client with appropriate listeners.
        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(
            server_selector=custom_selector, event_listeners=[listener]
        )
        coll = client.get_database("testdb", read_preference=ReadPreference.NEAREST).coll
        self.addAsyncCleanup(client.drop_database, "testdb")

        # Wait the node list to be fully populated.
        async def all_hosts_started():
            return len((await client.admin.command(HelloCompat.LEGACY_CMD))["hosts"]) == len(
                client._topology._description.readable_servers
            )

        await async_wait_until(all_hosts_started, "receive heartbeat from all hosts")

        expected_port = max(
            [strip_optional(n.address[1]) for n in client._topology._description.readable_servers]
        )

        # Insert 1 record and access it 10 times.
        await coll.insert_one({"name": "John Doe"})
        for _ in range(10):
            await coll.find_one({"name": "John Doe"})

        # Confirm all find commands are run against appropriate host.
        for command in listener.started_events:
            if command.command_name == "find":
                self.assertEqual(command.connection_id[1], expected_port)

    async def test_invalid_server_selector(self):
        # Client initialization must fail if server_selector is not callable.
        for selector_candidate in [[], 10, "string", {}]:
            with self.assertRaisesRegex(ValueError, "must be a callable"):
                AsyncMongoClient(connect=False, server_selector=selector_candidate)

        # None value for server_selector is OK.
        AsyncMongoClient(connect=False, server_selector=None)

    @async_client_context.require_replica_set
    async def test_selector_called(self):
        selector = FunctionCallRecorder(lambda x: x)

        # Client setup.
        mongo_client = await self.async_rs_or_single_client(server_selector=selector)
        test_collection = mongo_client.testdb.test_collection
        self.addAsyncCleanup(mongo_client.drop_database, "testdb")

        # Do N operations and test selector is called at least N times.
        await test_collection.insert_one({"age": 20, "name": "John"})
        await test_collection.insert_one({"age": 31, "name": "Jane"})
        await test_collection.update_one({"name": "Jane"}, {"$set": {"age": 21}})
        await test_collection.find_one({"name": "Roe"})
        self.assertGreaterEqual(selector.call_count, 4)

    @async_client_context.require_replica_set
    async def test_latency_threshold_application(self):
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
        await topology.open()
        for server in scenario_def["topology_description"]["servers"]:
            server_description = make_server_description(server, hosts)
            await topology.on_change(server_description)

        # Invoke server selection and assert no filtering based on latency
        # prior to custom server selection logic kicking in.
        server = await topology.select_server(ReadPreference.NEAREST, _Op.TEST)
        assert selector.selection is not None
        self.assertEqual(len(selector.selection), len(topology.description.server_descriptions()))

        # Ensure proper filtering based on latency after custom selection.
        self.assertEqual(server.description.address, seeds[min_rtt_idx])

    @async_client_context.require_replica_set
    async def test_server_selector_bypassed(self):
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
        await topology.open()
        for server in scenario_def["topology_description"]["servers"]:
            server_description = make_server_description(server, hosts)
            await topology.on_change(server_description)

        # Invoke server selection and assert no calls to our custom selector.
        with self.assertRaisesRegex(ServerSelectionTimeoutError, "No primary available for writes"):
            await topology.select_server(
                writable_server_selector, _Op.TEST, server_selection_timeout=0.1
            )
        self.assertEqual(selector.call_count, 0)


if __name__ == "__main__":
    unittest.main()
