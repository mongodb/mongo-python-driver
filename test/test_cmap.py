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

import os
import sys
import time

sys.path[0:0] = [""]

from test import IntegrationTest, client_knobs, unittest
from test.pymongo_mocks import DummyMonitor
from test.utils import (
    CMAPListener,
    TestCreator,
    camel_to_snake,
    client_context,
    get_pool,
    get_pools,
    rs_or_single_client,
    single_client,
    single_client_noauth,
    wait_until,
)
from test.utils_spec_runner import SpecRunnerThread

from bson.objectid import ObjectId
from bson.son import SON
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
from pymongo.pool import PoolState, _PoolClosedError
from pymongo.read_preferences import ReadPreference
from pymongo.topology_description import updated_topology_description

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


class TestCMAP(IntegrationTest):
    # Location of JSON test specifications.
    TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "cmap")

    # Test operations:

    def start(self, op):
        """Run the 'start' thread operation."""
        target = op["target"]
        thread = SpecRunnerThread(target)
        thread.start()
        self.targets[target] = thread

    def wait(self, op):
        """Run the 'wait' operation."""
        time.sleep(op["ms"] / 1000.0)

    def wait_for_thread(self, op):
        """Run the 'waitForThread' operation."""
        target = op["target"]
        thread = self.targets[target]
        thread.stop()
        thread.join()
        if thread.exc:
            raise thread.exc
        self.assertFalse(thread.ops)

    def wait_for_event(self, op):
        """Run the 'waitForEvent' operation."""
        event = OBJECT_TYPES[op["event"]]
        count = op["count"]
        timeout = op.get("timeout", 10000) / 1000.0
        wait_until(
            lambda: self.listener.event_count(event) >= count,
            "find %s %s event(s)" % (count, event),
            timeout=timeout,
        )

    def check_out(self, op):
        """Run the 'checkOut' operation."""
        label = op["label"]
        with self.pool.get_socket() as sock_info:
            # Call 'pin_cursor' so we can hold the socket.
            sock_info.pin_cursor()
            if label:
                self.labels[label] = sock_info
            else:
                self.addCleanup(sock_info.close_socket, None)

    def check_in(self, op):
        """Run the 'checkIn' operation."""
        label = op["connection"]
        sock_info = self.labels[label]
        self.pool.return_socket(sock_info)

    def ready(self, op):
        """Run the 'ready' operation."""
        self.pool.ready()

    def clear(self, op):
        """Run the 'clear' operation."""
        self.pool.reset()

    def close(self, op):
        """Run the 'close' operation."""
        self.pool.close()

    def run_operation(self, op):
        """Run a single operation in a test."""
        op_name = camel_to_snake(op["name"])
        thread = op["thread"]
        meth = getattr(self, op_name)
        if thread:
            self.targets[thread].schedule(lambda: meth(op))
        else:
            meth(op)

    def run_operations(self, ops):
        """Run a test's operations."""
        for op in ops:
            self._ops.append(op)
            self.run_operation(op)

    def check_object(self, actual, expected):
        """Assert that the actual object matches the expected object."""
        self.assertEqual(type(actual), OBJECT_TYPES[expected["type"]])
        for attr, expected_val in expected.items():
            if attr == "type":
                continue
            c2s = camel_to_snake(attr)
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
            self.logs.append("Checking event actual: %r vs expected: %r" % (actual, expected))
            self.check_event(actual, expected)

        if len(events) > len(actual_events):
            self.fail("missing events: %r" % (events[len(actual_events) :],))

    def check_error(self, actual, expected):
        message = expected.pop("message")
        self.check_object(actual, expected)
        self.assertIn(message, str(actual))

    def _set_fail_point(self, client, command_args):
        cmd = SON([("configureFailPoint", "failCommand")])
        cmd.update(command_args)
        client.admin.command(cmd)

    def set_fail_point(self, command_args):
        if not client_context.supports_failCommand_fail_point:
            self.skipTest("failCommand fail point must be supported")
        self._set_fail_point(self.client, command_args)

    def run_scenario(self, scenario_def, test):
        """Run a CMAP spec test."""
        self.logs: list = []
        self.assertEqual(scenario_def["version"], 1)
        self.assertIn(scenario_def["style"], ["unit", "integration"])
        self.listener = CMAPListener()
        self._ops: list = []

        # Configure the fail point before creating the client.
        if "failPoint" in test:
            fp = test["failPoint"]
            self.set_fail_point(fp)
            self.addCleanup(
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
            client = single_client(**opts)
            # Update the SD to a known type because the DummyMonitor will not.
            # Note we cannot simply call topology.on_change because that would
            # internally call pool.ready() which introduces unexpected
            # PoolReadyEvents. Instead, update the initial state before
            # opening the Topology.
            td = client_context.client._topology.description
            sd = td.server_descriptions()[(client_context.host, client_context.port)]
            client._topology._description = updated_topology_description(
                client._topology._description, sd
            )
            # When backgroundThreadIntervalMS is negative we do not start the
            # background thread to ensure it never runs.
            if interval < 0:
                client._topology.open()
            else:
                client._get_topology()
        self.addCleanup(client.close)
        self.pool = list(client._topology._servers.values())[0].pool

        # Map of target names to Thread objects.
        self.targets: dict = dict()
        # Map of label names to Connection objects
        self.labels: dict = dict()

        def cleanup():
            for t in self.targets.values():
                t.stop()
            for t in self.targets.values():
                t.join(5)
            for conn in self.labels.values():
                conn.close_socket(None)

        self.addCleanup(cleanup)

        try:
            if test["error"]:
                with self.assertRaises(PyMongoError) as ctx:
                    self.run_operations(test["operations"])
                self.check_error(ctx.exception, test["error"])
            else:
                self.run_operations(test["operations"])

            self.check_events(test["events"], test["ignore"])
        except Exception:
            # Print the events after a test failure.
            print("\nFailed test: %r" % (test["description"],))
            print("Operations:")
            for op in self._ops:
                print(op)
            print("Threads:")
            print(self.targets)
            print("Connections:")
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
    def test_1_client_connection_pool_options(self):
        client = rs_or_single_client(**self.POOL_OPTIONS)
        self.addCleanup(client.close)
        pool_opts = get_pool(client).opts
        self.assertEqual(pool_opts.non_default_options, self.POOL_OPTIONS)

    def test_2_all_client_pools_have_same_options(self):
        client = rs_or_single_client(**self.POOL_OPTIONS)
        self.addCleanup(client.close)
        client.admin.command("ping")
        # Discover at least one secondary.
        if client_context.has_secondaries:
            client.admin.command("ping", read_preference=ReadPreference.SECONDARY)
        pools = get_pools(client)
        pool_opts = pools[0].opts

        self.assertEqual(pool_opts.non_default_options, self.POOL_OPTIONS)
        for pool in pools[1:]:
            self.assertEqual(pool.opts, pool_opts)

    def test_3_uri_connection_pool_options(self):
        opts = "&".join(["%s=%s" % (k, v) for k, v in self.POOL_OPTIONS.items()])
        uri = "mongodb://%s/?%s" % (client_context.pair, opts)
        client = rs_or_single_client(uri)
        self.addCleanup(client.close)
        pool_opts = get_pool(client).opts
        self.assertEqual(pool_opts.non_default_options, self.POOL_OPTIONS)

    def test_4_subscribe_to_events(self):
        listener = CMAPListener()
        client = single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        self.assertEqual(listener.event_count(PoolCreatedEvent), 1)

        # Creates a new connection.
        client.admin.command("ping")
        self.assertEqual(listener.event_count(ConnectionCheckOutStartedEvent), 1)
        self.assertEqual(listener.event_count(ConnectionCreatedEvent), 1)
        self.assertEqual(listener.event_count(ConnectionReadyEvent), 1)
        self.assertEqual(listener.event_count(ConnectionCheckedOutEvent), 1)
        self.assertEqual(listener.event_count(ConnectionCheckedInEvent), 1)

        # Uses the existing connection.
        client.admin.command("ping")
        self.assertEqual(listener.event_count(ConnectionCheckOutStartedEvent), 2)
        self.assertEqual(listener.event_count(ConnectionCheckedOutEvent), 2)
        self.assertEqual(listener.event_count(ConnectionCheckedInEvent), 2)

        client.close()
        self.assertEqual(listener.event_count(PoolClearedEvent), 1)
        self.assertEqual(listener.event_count(ConnectionClosedEvent), 1)

    def test_5_check_out_fails_connection_error(self):
        listener = CMAPListener()
        client = single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        pool = get_pool(client)

        def mock_connect(*args, **kwargs):
            raise ConnectionFailure("connect failed")

        pool.connect = mock_connect
        # Un-patch Pool.connect to break the cyclic reference.
        self.addCleanup(delattr, pool, "connect")

        # Attempt to create a new connection.
        with self.assertRaisesRegex(ConnectionFailure, "connect failed"):
            client.admin.command("ping")

        self.assertIsInstance(listener.events[0], PoolCreatedEvent)
        self.assertIsInstance(listener.events[1], PoolReadyEvent)
        self.assertIsInstance(listener.events[2], ConnectionCheckOutStartedEvent)
        self.assertIsInstance(listener.events[3], ConnectionCheckOutFailedEvent)
        self.assertIsInstance(listener.events[4], PoolClearedEvent)

        failed_event = listener.events[3]
        self.assertEqual(failed_event.reason, ConnectionCheckOutFailedReason.CONN_ERROR)

    def test_5_check_out_fails_auth_error(self):
        listener = CMAPListener()
        client = single_client_noauth(
            username="notauser", password="fail", event_listeners=[listener]
        )
        self.addCleanup(client.close)

        # Attempt to create a new connection.
        with self.assertRaisesRegex(OperationFailure, "failed"):
            client.admin.command("ping")

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

    def test_events_repr(self):
        host = ("localhost", 27017)
        self.assertRepr(ConnectionCheckedInEvent(host, 1))
        self.assertRepr(ConnectionCheckedOutEvent(host, 1))
        self.assertRepr(
            ConnectionCheckOutFailedEvent(host, ConnectionCheckOutFailedReason.POOL_CLOSED)
        )
        self.assertRepr(ConnectionClosedEvent(host, 1, ConnectionClosedReason.POOL_CLOSED))
        self.assertRepr(ConnectionCreatedEvent(host, 1))
        self.assertRepr(ConnectionReadyEvent(host, 1))
        self.assertRepr(ConnectionCheckOutStartedEvent(host))
        self.assertRepr(PoolCreatedEvent(host, {}))
        self.assertRepr(PoolClearedEvent(host))
        self.assertRepr(PoolClearedEvent(host, service_id=ObjectId()))
        self.assertRepr(PoolClosedEvent(host))

    def test_close_leaves_pool_unpaused(self):
        # Needed until we implement PYTHON-2463. This test is related to
        # test_threads.TestThreads.test_client_disconnect
        listener = CMAPListener()
        client = single_client(event_listeners=[listener])
        client.admin.command("ping")
        pool = get_pool(client)
        client.close()
        self.assertEqual(1, listener.event_count(PoolClearedEvent))
        self.assertEqual(PoolState.READY, pool.state)
        # Checking out a connection should succeed
        with pool.get_socket():
            pass


def create_test(scenario_def, test, name):
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


class CMAPTestCreator(TestCreator):
    def tests(self, scenario_def):
        """Extract the tests from a spec file.

        CMAP tests do not have a 'tests' field. The whole file represents
        a single test case.
        """
        return [scenario_def]


test_creator = CMAPTestCreator(create_test, TestCMAP, TestCMAP.TEST_PATH)
test_creator.create_tests()


if __name__ == "__main__":
    unittest.main()
