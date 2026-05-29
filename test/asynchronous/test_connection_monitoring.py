# Copyright 2019-present MongoDB, Inc.
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

"""Execute Transactions Spec tests."""
from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path
from test.asynchronous.utils import async_get_pool, async_get_pools

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, client_knobs, unittest
from test.asynchronous.pymongo_mocks import DummyMonitor
from test.asynchronous.utils_spec_runner import AsyncSpecTestCreator, SpecRunnerTask
from test.utils_shared import (
    CMAPListener,
    async_wait_until,
    camel_to_snake,
)

from bson.objectid import ObjectId
from bson.son import SON
from pymongo.asynchronous.pool import PoolState, _PoolClosedError
from pymongo.errors import (
    ConnectionFailure,
    OperationFailure,
    PyMongoError,
    WaitQueueTimeoutError,
)
from pymongo.monitoring import (
    ConnectionCheckedInEvent,
    ConnectionCheckedOutEvent,
    ConnectionCheckOutFailedEvent,
    ConnectionCheckOutFailedReason,
    ConnectionCheckOutStartedEvent,
    ConnectionClosedEvent,
    ConnectionClosedReason,
    ConnectionCreatedEvent,
    ConnectionReadyEvent,
    PoolClearedEvent,
    PoolClosedEvent,
    PoolCreatedEvent,
    PoolReadyEvent,
)
from pymongo.read_preferences import ReadPreference
from pymongo.topology_description import updated_topology_description

_IS_SYNC = False

OBJECT_TYPES = {
    # Event types.
    "ConnectionCheckedIn": ConnectionCheckedInEvent,
    "ConnectionCheckedOut": ConnectionCheckedOutEvent,
    "ConnectionCheckOutFailed": ConnectionCheckOutFailedEvent,
    "ConnectionClosed": ConnectionClosedEvent,
    "ConnectionCreated": ConnectionCreatedEvent,
    "ConnectionReady": ConnectionReadyEvent,
    "ConnectionCheckOutStarted": ConnectionCheckOutStartedEvent,
    "ConnectionPoolCreated": PoolCreatedEvent,
    "ConnectionPoolReady": PoolReadyEvent,
    "ConnectionPoolCleared": PoolClearedEvent,
    "ConnectionPoolClosed": PoolClosedEvent,
    # Error types.
    "PoolClosedError": _PoolClosedError,
    "WaitQueueTimeoutError": WaitQueueTimeoutError,
}


class AsyncTestCMAP(AsyncIntegrationTest):
    # Location of JSON test specifications.
    if _IS_SYNC:
        TEST_PATH = os.path.join(Path(__file__).resolve().parent, "connection_monitoring")
    else:
        TEST_PATH = os.path.join(Path(__file__).resolve().parent.parent, "connection_monitoring")

    # Test operations:

    async def start(self, op):
        """Run the 'start' thread operation."""
        target = op["target"]
        thread = SpecRunnerTask(target)
        await thread.start()
        self.targets[target] = thread

    async def wait(self, op):
        """Run the 'wait' operation."""
        await asyncio.sleep(op["ms"] / 1000.0)

    async def wait_for_thread(self, op):
        """Run the 'waitForThread' operation."""
        target = op["target"]
        thread = self.targets[target]
        await thread.stop()
        await thread.join()
        if thread.exc:
            raise thread.exc
        self.assertFalse(thread.ops)

    async def wait_for_event(self, op):
        """Run the 'waitForEvent' operation."""
        event = OBJECT_TYPES[op["event"]]
        count = op["count"]
        timeout = op.get("timeout", 10000) / 1000.0
        await async_wait_until(
            lambda: self.listener.event_count(event) >= count,
            f"find {count} {event} event(s)",
            timeout=timeout,
        )

    async def check_out(self, op):
        """Run the 'checkOut' operation."""
        label = op["label"]
        async with self.pool.checkout() as conn:
            # Call 'pin_cursor' so we can hold the socket.
            conn.pin_cursor()
            if label:
                self.labels[label] = conn
            else:
                self.addAsyncCleanup(conn.close_conn, None)

    async def check_in(self, op):
        """Run the 'checkIn' operation."""
        label = op["connection"]
        conn = self.labels[label]
        await self.pool.checkin(conn)

    async def ready(self, op):
        """Run the 'ready' operation."""
        await self.pool.ready()

    async def clear(self, op):
        """Run the 'clear' operation."""
        if "interruptInUseConnections" in op:
            await self.pool.reset(interrupt_connections=op["interruptInUseConnections"])
        else:
            await self.pool.reset()

    async def close(self, op):
        """Run the 'close' operation."""
        await self.pool.close()

    async def run_operation(self, op):
        """Run a single operation in a test."""
        op_name = camel_to_snake(op["name"])
        thread = op["thread"]
        meth = getattr(self, op_name)
        if thread:
            await self.targets[thread].schedule(lambda: meth(op))
        else:
            await meth(op)

    async def run_operations(self, ops):
        """Run a test's operations."""
        for op in ops:
            self._ops.append(op)
            await self.run_operation(op)

    def check_object(self, actual, expected):
        """Assert that the actual object matches the expected object."""
        self.assertEqual(type(actual), OBJECT_TYPES[expected["type"]])
        for attr, expected_val in expected.items():
            if attr == "type":
                continue
            c2s = camel_to_snake(attr)
            if c2s == "interrupt_in_use_connections":
                c2s = "interrupt_connections"
            actual_val = getattr(actual, c2s)
            if expected_val == 42:
                self.assertIsNotNone(actual_val)
            else:
                self.assertEqual(actual_val, expected_val)

    def check_event(self, actual, expected):
        """Assert that the actual event matches the expected event."""
        self.check_object(actual, expected)

    def actual_events(self, ignore):
        """Return all the non-ignored events."""
        ignore = tuple(OBJECT_TYPES[name] for name in ignore)
        return [event for event in self.listener.events if not isinstance(event, ignore)]

    def check_events(self, events, ignore):
        """Check the events of a test."""
        actual_events = self.actual_events(ignore)
        for actual, expected in zip(actual_events, events):
            self.logs.append(f"Checking event actual: {actual!r} vs expected: {expected!r}")
            self.check_event(actual, expected)

        if len(events) > len(actual_events):
            self.fail(f"missing events: {events[len(actual_events) :]!r}")

    def check_error(self, actual, expected):
        message = expected.pop("message")
        self.check_object(actual, expected)
        self.assertIn(message, str(actual))

    async def set_fail_point(self, command_args):
        if not async_client_context.supports_failCommand_fail_point:
            self.skipTest("failCommand fail point must be supported")
        await self.configure_fail_point(self.client, command_args)

    async def run_scenario(self, scenario_def, test):
        """Run a CMAP spec test."""
        self.logs: list = []
        self.assertEqual(scenario_def["version"], 1)
        self.assertIn(scenario_def["style"], ["unit", "integration"])
        self.listener = CMAPListener()
        self._ops: list = []

        # Configure the fail point before creating the client.
        if "failPoint" in test:
            fp = test["failPoint"]
            await self.set_fail_point(fp)
            self.addAsyncCleanup(
                self.set_fail_point, {"configureFailPoint": fp["configureFailPoint"], "mode": "off"}
            )

        opts = test["poolOptions"].copy()
        opts["event_listeners"] = [self.listener]
        opts["_monitor_class"] = DummyMonitor
        opts["connect"] = False
        # Support backgroundThreadIntervalMS, default to 50ms.
        interval = opts.pop("backgroundThreadIntervalMS", 50)
        if interval < 0:
            kill_cursor_frequency = 99999999
        else:
            kill_cursor_frequency = interval / 1000.0
        with client_knobs(kill_cursor_frequency=kill_cursor_frequency, min_heartbeat_interval=0.05):
            client = await self.async_single_client(**opts)
            # Update the SD to a known type because the DummyMonitor will not.
            # Note we cannot simply call topology.on_change because that would
            # internally call pool.ready() which introduces unexpected
            # PoolReadyEvents. Instead, update the initial state before
            # opening the Topology.
            td = async_client_context.client._topology.description
            sd = td.server_descriptions()[
                (await async_client_context.host, await async_client_context.port)
            ]
            client._topology._description = updated_topology_description(
                client._topology._description, sd
            )
            # When backgroundThreadIntervalMS is negative we do not start the
            # background thread to ensure it never runs.
            if interval < 0:
                await client._topology.open()
            else:
                await client._get_topology()
        self.pool = list(client._topology._servers.values())[0].pool

        # Map of target names to Thread objects.
        self.targets: dict = {}
        # Map of label names to AsyncConnection objects
        self.labels: dict = {}

        async def cleanup():
            for t in self.targets.values():
                await t.stop()
            for t in self.targets.values():
                await t.join(5)
            for conn in self.labels.values():
                await conn.close_conn(None)

        self.addAsyncCleanup(cleanup)

        try:
            if test["error"]:
                with self.assertRaises(PyMongoError) as ctx:
                    await self.run_operations(test["operations"])
                self.check_error(ctx.exception, test["error"])
            else:
                await self.run_operations(test["operations"])

            self.check_events(test["events"], test["ignore"])
        except Exception:
            # Print the events after a test failure.
            print("\nFailed test: {!r}".format(test["description"]))
            print("Operations:")
            for op in self._ops:
                print(op)
            print("Threads:")
            print(self.targets)
            print("AsyncConnections:")
            print(self.labels)
            print("Events:")
            for event in self.listener.events:
                print(event)
            print("Log:")
            for log in self.logs:
                print(log)
            raise

    POOL_OPTIONS = {
        "maxPoolSize": 50,
        "minPoolSize": 1,
        "maxIdleTimeMS": 10000,
        "waitQueueTimeoutMS": 10000,
    }

    #
    # Prose tests. Numbers correspond to the prose test number in the spec.
    #
    async def test_1_client_connection_pool_options(self):
        client = await self.async_rs_or_single_client(**self.POOL_OPTIONS)
        pool_opts = (await async_get_pool(client)).opts
        self.assertEqual(pool_opts.non_default_options, self.POOL_OPTIONS)

    async def test_2_all_client_pools_have_same_options(self):
        client = await self.async_rs_or_single_client(**self.POOL_OPTIONS)
        await client.admin.command("ping")
        # Discover at least one secondary.
        if await async_client_context.has_secondaries:
            await client.admin.command("ping", read_preference=ReadPreference.SECONDARY)
        pools = await async_get_pools(client)
        pool_opts = pools[0].opts

        self.assertEqual(pool_opts.non_default_options, self.POOL_OPTIONS)
        for pool in pools[1:]:
            self.assertEqual(pool.opts, pool_opts)

    async def test_3_uri_connection_pool_options(self):
        opts = "&".join([f"{k}={v}" for k, v in self.POOL_OPTIONS.items()])
        uri = f"mongodb://{await async_client_context.pair}/?{opts}"
        client = await self.async_rs_or_single_client(uri)
        pool_opts = (await async_get_pool(client)).opts
        self.assertEqual(pool_opts.non_default_options, self.POOL_OPTIONS)

    async def test_4_subscribe_to_events(self):
        listener = CMAPListener()
        client = await self.async_single_client(event_listeners=[listener])
        self.assertEqual(listener.event_count(PoolCreatedEvent), 1)

        # Creates a new connection.
        await client.admin.command("ping")
        self.assertEqual(listener.event_count(ConnectionCheckOutStartedEvent), 1)
        self.assertEqual(listener.event_count(ConnectionCreatedEvent), 1)
        self.assertEqual(listener.event_count(ConnectionReadyEvent), 1)
        self.assertEqual(listener.event_count(ConnectionCheckedOutEvent), 1)
        self.assertEqual(listener.event_count(ConnectionCheckedInEvent), 1)

        # Uses the existing connection.
        await client.admin.command("ping")
        self.assertEqual(listener.event_count(ConnectionCheckOutStartedEvent), 2)
        self.assertEqual(listener.event_count(ConnectionCheckedOutEvent), 2)
        self.assertEqual(listener.event_count(ConnectionCheckedInEvent), 2)

        await client.close()
        self.assertEqual(listener.event_count(PoolClosedEvent), 1)
        self.assertEqual(listener.event_count(ConnectionClosedEvent), 1)

    async def test_5_check_out_fails_connection_error(self):
        listener = CMAPListener()
        client = await self.async_single_client(event_listeners=[listener])
        pool = await async_get_pool(client)

        def mock_connect(*args, **kwargs):
            raise ConnectionFailure("connect failed")

        pool.connect = mock_connect
        # Un-patch Pool.connect to break the cyclic reference.
        self.addCleanup(delattr, pool, "connect")

        # Attempt to create a new connection.
        with self.assertRaisesRegex(ConnectionFailure, "connect failed"):
            await client.admin.command("ping")

        self.assertIsInstance(listener.events[0], PoolCreatedEvent)
        self.assertIsInstance(listener.events[1], PoolReadyEvent)
        self.assertIsInstance(listener.events[2], ConnectionCheckOutStartedEvent)
        self.assertIsInstance(listener.events[3], ConnectionCheckOutFailedEvent)
        self.assertIsInstance(listener.events[4], PoolClearedEvent)

        failed_event = listener.events[3]
        self.assertEqual(failed_event.reason, ConnectionCheckOutFailedReason.CONN_ERROR)

    @async_client_context.require_no_fips
    async def test_5_check_out_fails_auth_error(self):
        listener = CMAPListener()
        client = await self.async_single_client_noauth(
            username="notauser", password="fail", event_listeners=[listener]
        )

        # Attempt to create a new connection.
        with self.assertRaisesRegex(OperationFailure, "failed"):
            await client.admin.command("ping")

        self.assertIsInstance(listener.events[0], PoolCreatedEvent)
        self.assertIsInstance(listener.events[1], PoolReadyEvent)
        self.assertIsInstance(listener.events[2], ConnectionCheckOutStartedEvent)
        self.assertIsInstance(listener.events[3], ConnectionCreatedEvent)
        # Error happens here.
        self.assertIsInstance(listener.events[4], ConnectionClosedEvent)
        self.assertIsInstance(listener.events[5], ConnectionCheckOutFailedEvent)
        self.assertEqual(listener.events[5].reason, ConnectionCheckOutFailedReason.CONN_ERROR)

    #
    # Extra non-spec tests
    #
    def assertRepr(self, obj):
        new_obj = eval(repr(obj))
        self.assertEqual(type(new_obj), type(obj))
        self.assertEqual(repr(new_obj), repr(obj))

    async def test_events_repr(self):
        host = ("localhost", 27017)
        self.assertRepr(ConnectionCheckedInEvent(host, 1))
        self.assertRepr(ConnectionCheckedOutEvent(host, 1, time.monotonic()))
        self.assertRepr(
            ConnectionCheckOutFailedEvent(
                host, ConnectionCheckOutFailedReason.POOL_CLOSED, time.monotonic()
            )
        )
        self.assertRepr(ConnectionClosedEvent(host, 1, ConnectionClosedReason.POOL_CLOSED))
        self.assertRepr(ConnectionCreatedEvent(host, 1))
        self.assertRepr(ConnectionReadyEvent(host, 1, time.monotonic()))
        self.assertRepr(ConnectionCheckOutStartedEvent(host))
        self.assertRepr(PoolCreatedEvent(host, {}))
        self.assertRepr(PoolClearedEvent(host))
        self.assertRepr(PoolClearedEvent(host, service_id=ObjectId()))
        self.assertRepr(PoolClosedEvent(host))

    async def test_close_leaves_pool_unpaused(self):
        listener = CMAPListener()
        client = await self.async_single_client(event_listeners=[listener])
        await client.admin.command("ping")
        pool = await async_get_pool(client)
        await client.close()
        self.assertEqual(1, listener.event_count(PoolClosedEvent))
        self.assertEqual(PoolState.CLOSED, pool.state)
        # Checking out a connection should fail
        with self.assertRaises(_PoolClosedError):
            async with pool.checkout():
                pass


def create_test(scenario_def, test, name):
    async def run_scenario(self):
        await self.run_scenario(scenario_def, test)

    return run_scenario


class CMAPSpecTestCreator(AsyncSpecTestCreator):
    def tests(self, scenario_def):
        """Extract the tests from a spec file.

        CMAP tests do not have a 'tests' field. The whole file represents
        a single test case.
        """
        return [scenario_def]


test_creator = CMAPSpecTestCreator(create_test, AsyncTestCMAP, AsyncTestCMAP.TEST_PATH)
test_creator.create_tests()


if __name__ == "__main__":
    unittest.main()
