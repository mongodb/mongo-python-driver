# Copyright 2014-present MongoDB, Inc.
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
from __future__ import annotations

import asyncio
import os
import socketserver
import sys
import threading
import time
from asyncio import StreamReader, StreamWriter
from pathlib import Path
from test.asynchronous.helpers import ConcurrentRunner
from test.asynchronous.utils import flaky

from pymongo.asynchronous.pool import AsyncConnection
from pymongo.operations import _Op
from pymongo.server_selectors import writable_server_selector

sys.path[0:0] = [""]

from test.asynchronous import (
    AsyncIntegrationTest,
    AsyncPyMongoTestCase,
    AsyncUnitTest,
    async_client_context,
    unittest,
)
from test.asynchronous.pymongo_mocks import DummyMonitor
from test.asynchronous.unified_format import generate_test_classes, get_test_path
from test.asynchronous.utils import (
    async_get_pool,
)
from test.utils_shared import (
    CMAPListener,
    HeartbeatEventListener,
    HeartbeatEventsListListener,
    assertion_context,
    async_barrier_wait,
    async_create_barrier,
    async_wait_until,
    server_name_to_type,
)
from unittest.mock import patch

from bson import Timestamp, json_util
from pymongo import common, monitoring
from pymongo.asynchronous.settings import TopologySettings
from pymongo.asynchronous.topology import Topology, _ErrorContext
from pymongo.asynchronous.uri_parser import parse_uri
from pymongo.errors import (
    AutoReconnect,
    ConfigurationError,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
)
from pymongo.hello import Hello, HelloCompat
from pymongo.helpers_shared import _check_command_response, _check_write_command_response
from pymongo.monitoring import ServerHeartbeatFailedEvent, ServerHeartbeatStartedEvent
from pymongo.server_description import SERVER_TYPE, ServerDescription
from pymongo.topology_description import TOPOLOGY_TYPE

_IS_SYNC = False

SDAM_PATH = get_test_path("discovery_and_monitoring")


async def create_mock_topology(uri, monitor_class=DummyMonitor):
    parsed_uri = await parse_uri(uri)
    replica_set_name = None
    direct_connection = None
    load_balanced = None
    if "replicaSet" in parsed_uri["options"]:
        replica_set_name = parsed_uri["options"]["replicaSet"]
    if "directConnection" in parsed_uri["options"]:
        direct_connection = parsed_uri["options"]["directConnection"]
    if "loadBalanced" in parsed_uri["options"]:
        load_balanced = parsed_uri["options"]["loadBalanced"]

    topology_settings = TopologySettings(
        parsed_uri["nodelist"],
        replica_set_name=replica_set_name,
        monitor_class=monitor_class,
        direct_connection=direct_connection,
        load_balanced=load_balanced,
    )

    c = Topology(topology_settings)
    await c.open()
    return c


async def got_hello(topology, server_address, hello_response):
    server_description = ServerDescription(server_address, Hello(hello_response), 0)
    await topology.on_change(server_description)


async def got_app_error(topology, app_error):
    server_address = common.partition_node(app_error["address"])
    server = topology.get_server_by_address(server_address)
    error_type = app_error["type"]
    generation = app_error.get("generation", server.pool.gen.get_overall())
    when = app_error["when"]
    max_wire_version = app_error["maxWireVersion"]
    # XXX: We could get better test coverage by mocking the errors on the
    # Pool/AsyncConnection.
    try:
        if error_type == "command":
            _check_command_response(app_error["response"], max_wire_version)
            _check_write_command_response(app_error["response"])
        elif error_type == "network":
            raise AutoReconnect("mock non-timeout network error")
        elif error_type == "timeout":
            raise NetworkTimeout("mock network timeout error")
        else:
            raise AssertionError(f"unknown error type: {error_type}")
        raise AssertionError
    except (AutoReconnect, NotPrimaryError, OperationFailure) as e:
        if when == "beforeHandshakeCompletes":
            completed_handshake = False
        elif when == "afterHandshakeCompletes":
            completed_handshake = True
        else:
            raise AssertionError(f"Unknown when field {when}")

        await topology.handle_error(
            server_address,
            _ErrorContext(e, max_wire_version, generation, completed_handshake, None),
        )


def get_type(topology, hostname):
    description = topology.get_server_by_address((hostname, 27017)).description
    return description.server_type


class TestAllScenarios(AsyncUnitTest):
    pass


def topology_type_name(topology_type):
    return TOPOLOGY_TYPE._fields[topology_type]


def server_type_name(server_type):
    return SERVER_TYPE._fields[server_type]


def check_outcome(self, topology, outcome):
    expected_servers = outcome["servers"]

    # Check weak equality before proceeding.
    self.assertEqual(len(topology.description.server_descriptions()), len(expected_servers))

    if outcome.get("compatible") is False:
        with self.assertRaises(ConfigurationError):
            topology.description.check_compatible()
    else:
        # No error.
        topology.description.check_compatible()

    # Since lengths are equal, every actual server must have a corresponding
    # expected server.
    for expected_server_address, expected_server in expected_servers.items():
        node = common.partition_node(expected_server_address)
        self.assertTrue(topology.has_server(node))
        actual_server = topology.get_server_by_address(node)
        actual_server_description = actual_server.description
        expected_server_type = server_name_to_type(expected_server["type"])

        self.assertEqual(
            server_type_name(expected_server_type),
            server_type_name(actual_server_description.server_type),
        )
        expected_error = expected_server.get("error")
        if expected_error:
            self.assertIn(expected_error, str(actual_server_description.error))

        self.assertEqual(expected_server.get("setName"), actual_server_description.replica_set_name)

        self.assertEqual(expected_server.get("setVersion"), actual_server_description.set_version)

        self.assertEqual(expected_server.get("electionId"), actual_server_description.election_id)

        self.assertEqual(
            expected_server.get("topologyVersion"), actual_server_description.topology_version
        )

        expected_pool = expected_server.get("pool")
        if expected_pool:
            self.assertEqual(expected_pool.get("generation"), actual_server.pool.gen.get_overall())

    self.assertEqual(outcome["setName"], topology.description.replica_set_name)
    self.assertEqual(
        outcome.get("logicalSessionTimeoutMinutes"),
        topology.description.logical_session_timeout_minutes,
    )

    expected_topology_type = getattr(TOPOLOGY_TYPE, outcome["topologyType"])
    self.assertEqual(
        topology_type_name(expected_topology_type),
        topology_type_name(topology.description.topology_type),
    )

    self.assertEqual(outcome.get("maxSetVersion"), topology.description.max_set_version)
    self.assertEqual(outcome.get("maxElectionId"), topology.description.max_election_id)


def create_test(scenario_def):
    async def run_scenario(self):
        c = await create_mock_topology(scenario_def["uri"])

        for i, phase in enumerate(scenario_def["phases"]):
            # Including the phase description makes failures easier to debug.
            description = phase.get("description", str(i))
            with assertion_context(f"phase: {description}"):
                for response in phase.get("responses", []):
                    await got_hello(c, common.partition_node(response[0]), response[1])

                for app_error in phase.get("applicationErrors", []):
                    await got_app_error(c, app_error)

                check_outcome(self, c, phase["outcome"])

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(SDAM_PATH):
        dirname = os.path.split(dirpath)[-1]
        # SDAM unified tests are handled separately.
        if dirname == "unified":
            continue

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


create_tests()


class TestClusterTimeComparison(AsyncPyMongoTestCase):
    async def test_cluster_time_comparison(self):
        t = await create_mock_topology("mongodb://host")

        async def send_cluster_time(time, inc):
            old = t.max_cluster_time()
            new = {"clusterTime": Timestamp(time, inc)}
            await got_hello(
                t,
                ("host", 27017),
                {
                    "ok": 1,
                    "minWireVersion": 0,
                    "maxWireVersion": common.MIN_SUPPORTED_WIRE_VERSION,
                    "$clusterTime": new,
                },
            )

            actual = t.max_cluster_time()
            # We never update $clusterTime from monitoring connections.
            self.assertEqual(actual, old)

        await send_cluster_time(0, 1)
        await send_cluster_time(2, 2)
        await send_cluster_time(2, 1)
        await send_cluster_time(1, 3)
        await send_cluster_time(2, 3)


class TestIgnoreStaleErrors(AsyncIntegrationTest):
    async def test_ignore_stale_connection_errors(self):
        if not _IS_SYNC and sys.version_info < (3, 11):
            self.skipTest("Test requires asyncio.Barrier (added in Python 3.11)")
        N_TASKS = 5
        barrier = async_create_barrier(N_TASKS)
        client = await self.async_rs_or_single_client(minPoolSize=N_TASKS)

        # Wait for initial discovery.
        await client.admin.command("ping")
        pool = await async_get_pool(client)
        starting_generation = pool.gen.get_overall()
        await async_wait_until(lambda: len(pool.conns) == N_TASKS, "created conns")

        async def mock_command(*args, **kwargs):
            # Synchronize all tasks to ensure they use the same generation.
            await async_barrier_wait(barrier, timeout=30)
            raise AutoReconnect("mock AsyncConnection.command error")

        for conn in pool.conns:
            conn.command = mock_command

        async def insert_command(i):
            try:
                await client.test.command("insert", "test", documents=[{"i": i}])
            except AutoReconnect:
                pass

        tasks = []
        for i in range(N_TASKS):
            tasks.append(ConcurrentRunner(target=insert_command, args=(i,)))
        for t in tasks:
            await t.start()
        for t in tasks:
            await t.join()

        # Expect a single pool reset for the network error
        self.assertEqual(starting_generation + 1, pool.gen.get_overall())

        # Server should be selectable.
        await client.admin.command("ping")


class CMAPHeartbeatListener(HeartbeatEventListener, CMAPListener):
    pass


class TestPoolManagement(AsyncIntegrationTest):
    @async_client_context.require_failCommand_appName
    async def test_pool_unpause(self):
        # This test implements the prose test "AsyncConnection Pool Management"
        listener = CMAPHeartbeatListener()
        _ = await self.async_single_client(
            appName="SDAMPoolManagementTest", heartbeatFrequencyMS=500, event_listeners=[listener]
        )
        # Assert that AsyncConnectionPoolReadyEvent occurs after the first
        # ServerHeartbeatSucceededEvent.
        await listener.async_wait_for_event(monitoring.PoolReadyEvent, 1)
        pool_ready = listener.events_by_type(monitoring.PoolReadyEvent)[0]
        hb_succeeded = listener.events_by_type(monitoring.ServerHeartbeatSucceededEvent)[0]
        self.assertGreater(listener.events.index(pool_ready), listener.events.index(hb_succeeded))

        listener.reset()
        fail_hello = {
            "mode": {"times": 2},
            "data": {
                "failCommands": [HelloCompat.LEGACY_CMD, "hello"],
                "errorCode": 1234,
                "appName": "SDAMPoolManagementTest",
            },
        }
        async with self.fail_point(fail_hello):
            await listener.async_wait_for_event(monitoring.ServerHeartbeatFailedEvent, 1)
            await listener.async_wait_for_event(monitoring.PoolClearedEvent, 1)
            await listener.async_wait_for_event(monitoring.ServerHeartbeatSucceededEvent, 1)
            await listener.async_wait_for_event(monitoring.PoolReadyEvent, 1)

    @async_client_context.require_failCommand_appName
    @async_client_context.require_test_commands
    @async_client_context.require_async
    @flaky(reason="PYTHON-5428")
    async def test_connection_close_does_not_block_other_operations(self):
        listener = CMAPHeartbeatListener()
        client = await self.async_single_client(
            appName="SDAMConnectionCloseTest",
            event_listeners=[listener],
            heartbeatFrequencyMS=500,
            minPoolSize=10,
        )
        server = await (await client._get_topology()).select_server(
            writable_server_selector, _Op.TEST
        )
        await async_wait_until(
            lambda: len(server._pool.conns) == 10,
            "pool initialized with 10 connections",
        )

        await client.db.test.insert_one({"x": 1})
        close_delay = 0.1
        latencies = []
        should_exit = []

        async def run_task():
            while True:
                start_time = time.monotonic()
                await client.db.test.find_one({})
                elapsed = time.monotonic() - start_time
                latencies.append(elapsed)
                if should_exit:
                    break
                await asyncio.sleep(0.001)

        task = ConcurrentRunner(target=run_task)
        await task.start()
        original_close = AsyncConnection.close_conn
        try:
            # Artificially delay the close operation to simulate a slow close
            async def mock_close(self, reason):
                await asyncio.sleep(close_delay)
                await original_close(self, reason)

            AsyncConnection.close_conn = mock_close

            fail_hello = {
                "mode": {"times": 4},
                "data": {
                    "failCommands": [HelloCompat.LEGACY_CMD, "hello"],
                    "errorCode": 91,
                    "appName": "SDAMConnectionCloseTest",
                },
            }
            async with self.fail_point(fail_hello):
                # Wait for server heartbeat to fail
                await listener.async_wait_for_event(monitoring.ServerHeartbeatFailedEvent, 1)
            # Wait until all idle connections are closed to simulate real-world conditions
            await listener.async_wait_for_event(monitoring.ConnectionClosedEvent, 10)
            # Wait for one more find to complete after the pool has been reset, then shutdown the task
            n = len(latencies)
            await async_wait_until(lambda: len(latencies) >= n + 1, "run one more find")
            should_exit.append(True)
            await task.join()
            # No operation latency should not significantly exceed close_delay
            self.assertLessEqual(max(latencies), close_delay * 5.0)
        finally:
            AsyncConnection.close_conn = original_close


class TestServerMonitoringMode(AsyncIntegrationTest):
    @async_client_context.require_no_load_balancer
    async def asyncSetUp(self):
        await super().asyncSetUp()

    async def test_rtt_connection_is_enabled_stream(self):
        client = await self.async_rs_or_single_client(serverMonitoringMode="stream")
        await client.admin.command("ping")

        def predicate():
            for _, server in client._topology._servers.items():
                monitor = server._monitor
                if not monitor._stream:
                    return False
                if async_client_context.version >= (4, 4):
                    if _IS_SYNC:
                        if monitor._rtt_monitor._executor._thread is None:
                            return False
                    else:
                        if monitor._rtt_monitor._executor._task is None:
                            return False
                else:
                    if _IS_SYNC:
                        if monitor._rtt_monitor._executor._thread is not None:
                            return False
                    else:
                        if monitor._rtt_monitor._executor._task is not None:
                            return False
            return True

        await async_wait_until(predicate, "find all RTT monitors")

    async def test_rtt_connection_is_disabled_poll(self):
        client = await self.async_rs_or_single_client(serverMonitoringMode="poll")

        await self.assert_rtt_connection_is_disabled(client)

    async def test_rtt_connection_is_disabled_auto(self):
        envs = [
            {"AWS_EXECUTION_ENV": "AWS_Lambda_python3.10"},
            {"FUNCTIONS_WORKER_RUNTIME": "python"},
            {"K_SERVICE": "gcpservicename"},
            {"FUNCTION_NAME": "gcpfunctionname"},
            {"VERCEL": "1"},
        ]
        for env in envs:
            with patch.dict("os.environ", env):
                client = await self.async_rs_or_single_client(serverMonitoringMode="auto")
                await self.assert_rtt_connection_is_disabled(client)

    async def assert_rtt_connection_is_disabled(self, client):
        await client.admin.command("ping")
        for _, server in client._topology._servers.items():
            monitor = server._monitor
            self.assertFalse(monitor._stream)
            if _IS_SYNC:
                self.assertIsNone(monitor._rtt_monitor._executor._thread)
            else:
                self.assertIsNone(monitor._rtt_monitor._executor._task)


class MockTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.server.events.append("client connected")
        if self.request.recv(1024).strip():
            self.server.events.append("client hello received")
        self.request.close()


class TCPServer(socketserver.TCPServer):
    allow_reuse_address = True

    def handle_request_and_shutdown(self):
        self.handle_request()
        self.server_close()


class TestHeartbeatStartOrdering(AsyncPyMongoTestCase):
    async def test_heartbeat_start_ordering(self):
        events = []
        listener = HeartbeatEventsListListener(events)

        if _IS_SYNC:
            server = TCPServer(("localhost", 9999), MockTCPHandler)
            server.events = events
            server_thread = ConcurrentRunner(target=server.handle_request_and_shutdown)
            await server_thread.start()
            _c = await self.simple_client(
                "mongodb://localhost:9999",
                serverSelectionTimeoutMS=500,
                event_listeners=(listener,),
            )
            await server_thread.join()
            listener.wait_for_event(ServerHeartbeatStartedEvent, 1)
            listener.wait_for_event(ServerHeartbeatFailedEvent, 1)

        else:

            async def handle_client(reader: StreamReader, writer: StreamWriter):
                events.append("client connected")
                if (await reader.read(1024)).strip():
                    events.append("client hello received")
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_server(handle_client, "localhost", 9999)
            server.events = events
            await server.start_serving()
            _c = self.simple_client(
                "mongodb://localhost:9999",
                serverSelectionTimeoutMS=500,
                event_listeners=(listener,),
            )
            await _c.aconnect()

            await listener.async_wait_for_event(ServerHeartbeatStartedEvent, 1)
            await listener.async_wait_for_event(ServerHeartbeatFailedEvent, 1)

            server.close()
            await server.wait_closed()
            await _c.close()

        self.assertEqual(
            events,
            [
                "serverHeartbeatStartedEvent",
                "client connected",
                "client hello received",
                "serverHeartbeatFailedEvent",
            ],
        )


# Generate unified tests.
globals().update(generate_test_classes(os.path.join(SDAM_PATH, "unified"), module=__name__))


if __name__ == "__main__":
    unittest.main()
