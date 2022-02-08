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

import sys

sys.path[0:0] = [""]

from test import client_knobs, unittest
from test.pymongo_mocks import DummyMonitor
from test.utils import MockPool, wait_until

from bson.objectid import ObjectId
from pymongo import common
from pymongo.errors import AutoReconnect, ConfigurationError, ConnectionFailure
from pymongo.hello import Hello, HelloCompat
from pymongo.monitor import Monitor
from pymongo.pool import PoolOptions
from pymongo.read_preferences import ReadPreference, Secondary
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import any_server_selector, writable_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.settings import TopologySettings
from pymongo.topology import Topology, _ErrorContext
from pymongo.topology_description import TOPOLOGY_TYPE


class SetNameDiscoverySettings(TopologySettings):
    def get_topology_type(self):
        return TOPOLOGY_TYPE.ReplicaSetNoPrimary


address = ("a", 27017)


def create_mock_topology(
    seeds=None, replica_set_name=None, monitor_class=DummyMonitor, direct_connection=False
):
    partitioned_seeds = list(map(common.partition_node, seeds or ["a"]))
    topology_settings = TopologySettings(
        partitioned_seeds,
        replica_set_name=replica_set_name,
        pool_class=MockPool,
        monitor_class=monitor_class,
        direct_connection=direct_connection,
    )

    t = Topology(topology_settings)
    t.open()
    return t


def got_hello(topology, server_address, hello_response):
    server_description = ServerDescription(server_address, Hello(hello_response), 0)

    topology.on_change(server_description)


def disconnected(topology, server_address):
    # Create new description of server type Unknown.
    topology.on_change(ServerDescription(server_address))


def get_server(topology, hostname):
    return topology.get_server_by_address((hostname, 27017))


def get_type(topology, hostname):
    return get_server(topology, hostname).description.server_type


def get_monitor(topology, hostname):
    return get_server(topology, hostname)._monitor


class TopologyTest(unittest.TestCase):
    """Disables periodic monitoring, to make tests deterministic."""

    def setUp(self):
        super(TopologyTest, self).setUp()
        self.client_knobs = client_knobs(heartbeat_frequency=999999)
        self.client_knobs.enable()
        self.addCleanup(self.client_knobs.disable)


class TestTopologyConfiguration(TopologyTest):
    def test_timeout_configuration(self):
        pool_options = PoolOptions(connect_timeout=1, socket_timeout=2)
        topology_settings = TopologySettings(pool_options=pool_options)
        t = Topology(topology_settings=topology_settings)
        t.open()

        # Get the default server.
        server = t.get_server_by_address(("localhost", 27017))

        # The pool for application operations obeys our settings.
        self.assertEqual(1, server._pool.opts.connect_timeout)
        self.assertEqual(2, server._pool.opts.socket_timeout)

        # The pool for monitoring operations uses our connect_timeout as both
        # its connect_timeout and its socket_timeout.
        monitor = server._monitor
        self.assertEqual(1, monitor._pool.opts.connect_timeout)
        self.assertEqual(1, monitor._pool.opts.socket_timeout)

        # The monitor, not its pool, is responsible for calling hello.
        self.assertFalse(monitor._pool.handshake)


class TestSingleServerTopology(TopologyTest):
    def test_direct_connection(self):
        for server_type, hello_response in [
            (
                SERVER_TYPE.RSPrimary,
                {
                    "ok": 1,
                    HelloCompat.LEGACY_CMD: True,
                    "hosts": ["a"],
                    "setName": "rs",
                    "maxWireVersion": 6,
                },
            ),
            (
                SERVER_TYPE.RSSecondary,
                {
                    "ok": 1,
                    HelloCompat.LEGACY_CMD: False,
                    "secondary": True,
                    "hosts": ["a"],
                    "setName": "rs",
                    "maxWireVersion": 6,
                },
            ),
            (
                SERVER_TYPE.Mongos,
                {"ok": 1, HelloCompat.LEGACY_CMD: True, "msg": "isdbgrid", "maxWireVersion": 6},
            ),
            (
                SERVER_TYPE.RSArbiter,
                {
                    "ok": 1,
                    HelloCompat.LEGACY_CMD: False,
                    "arbiterOnly": True,
                    "hosts": ["a"],
                    "setName": "rs",
                    "maxWireVersion": 6,
                },
            ),
            (SERVER_TYPE.Standalone, {"ok": 1, HelloCompat.LEGACY_CMD: True, "maxWireVersion": 6}),
            # A "slave" in a master-slave deployment.
            # This replication type was removed in MongoDB
            # 4.0.
            (SERVER_TYPE.Standalone, {"ok": 1, HelloCompat.LEGACY_CMD: False, "maxWireVersion": 6}),
        ]:
            t = create_mock_topology(direct_connection=True)

            # Can't select a server while the only server is of type Unknown.
            with self.assertRaisesRegex(ConnectionFailure, "No servers found yet"):
                t.select_servers(any_server_selector, server_selection_timeout=0)

            got_hello(t, address, hello_response)

            # Topology type never changes.
            self.assertEqual(TOPOLOGY_TYPE.Single, t.description.topology_type)

            # No matter whether the server is writable,
            # select_servers() returns it.
            s = t.select_server(writable_server_selector)
            self.assertEqual(server_type, s.description.server_type)

            # Topology type single is always readable and writable regardless
            # of server type or state.
            self.assertEqual(t.description.topology_type_name, "Single")
            self.assertTrue(t.description.has_writable_server())
            self.assertTrue(t.description.has_readable_server())
            self.assertTrue(t.description.has_readable_server(Secondary()))
            self.assertTrue(
                t.description.has_readable_server(Secondary(tag_sets=[{"tag": "does-not-exist"}]))
            )

    def test_reopen(self):
        t = create_mock_topology()

        # Additional calls are permitted.
        t.open()
        t.open()

    def test_unavailable_seed(self):
        t = create_mock_topology()
        disconnected(t, address)
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, "a"))

    def test_round_trip_time(self):
        round_trip_time = 125
        available = True

        class TestMonitor(Monitor):
            def _check_with_socket(self, *args, **kwargs):
                if available:
                    return (Hello({"ok": 1, "maxWireVersion": 6}), round_trip_time)
                else:
                    raise AutoReconnect("mock monitor error")

        t = create_mock_topology(monitor_class=TestMonitor)
        self.addCleanup(t.close)
        s = t.select_server(writable_server_selector)
        self.assertEqual(125, s.description.round_trip_time)

        round_trip_time = 25
        t.request_check_all()

        # Exponential weighted average: .8 * 125 + .2 * 25 = 105.
        self.assertAlmostEqual(105, s.description.round_trip_time)

        # The server is temporarily down.
        available = False
        t.request_check_all()

        def raises_err():
            try:
                t.select_server(writable_server_selector, server_selection_timeout=0.1)
            except ConnectionFailure:
                return True
            else:
                return False

        wait_until(raises_err, "discover server is down")
        self.assertIsNone(s.description.round_trip_time)

        # Bring it back, RTT is now 20 milliseconds.
        available = True
        round_trip_time = 20

        def new_average():
            # We reset the average to the most recent measurement.
            description = s.description
            return (
                description.round_trip_time is not None
                and round(abs(20 - description.round_trip_time), 7) == 0
            )

        tries = 0
        while not new_average():
            t.request_check_all()
            tries += 1
            if tries > 10:
                self.fail("Didn't ever calculate correct new average")


class TestMultiServerTopology(TopologyTest):
    def test_readable_writable(self):
        t = create_mock_topology(replica_set_name="rs")
        got_hello(
            t,
            ("a", 27017),
            {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a", "b"]},
        )

        got_hello(
            t,
            ("b", 27017),
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: False,
                "secondary": True,
                "setName": "rs",
                "hosts": ["a", "b"],
            },
        )

        self.assertEqual(t.description.topology_type_name, "ReplicaSetWithPrimary")
        self.assertTrue(t.description.has_writable_server())
        self.assertTrue(t.description.has_readable_server())
        self.assertTrue(t.description.has_readable_server(Secondary()))
        self.assertFalse(t.description.has_readable_server(Secondary(tag_sets=[{"tag": "exists"}])))

        t = create_mock_topology(replica_set_name="rs")
        got_hello(
            t,
            ("a", 27017),
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: False,
                "secondary": False,
                "setName": "rs",
                "hosts": ["a", "b"],
            },
        )

        got_hello(
            t,
            ("b", 27017),
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: False,
                "secondary": True,
                "setName": "rs",
                "hosts": ["a", "b"],
            },
        )

        self.assertEqual(t.description.topology_type_name, "ReplicaSetNoPrimary")
        self.assertFalse(t.description.has_writable_server())
        self.assertFalse(t.description.has_readable_server())
        self.assertTrue(t.description.has_readable_server(Secondary()))
        self.assertFalse(t.description.has_readable_server(Secondary(tag_sets=[{"tag": "exists"}])))

        t = create_mock_topology(replica_set_name="rs")
        got_hello(
            t,
            ("a", 27017),
            {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a", "b"]},
        )

        got_hello(
            t,
            ("b", 27017),
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: False,
                "secondary": True,
                "setName": "rs",
                "hosts": ["a", "b"],
                "tags": {"tag": "exists"},
            },
        )

        self.assertEqual(t.description.topology_type_name, "ReplicaSetWithPrimary")
        self.assertTrue(t.description.has_writable_server())
        self.assertTrue(t.description.has_readable_server())
        self.assertTrue(t.description.has_readable_server(Secondary()))
        self.assertTrue(t.description.has_readable_server(Secondary(tag_sets=[{"tag": "exists"}])))

    def test_close(self):
        t = create_mock_topology(replica_set_name="rs")
        got_hello(
            t,
            ("a", 27017),
            {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a", "b"]},
        )

        got_hello(
            t,
            ("b", 27017),
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: False,
                "secondary": True,
                "setName": "rs",
                "hosts": ["a", "b"],
            },
        )

        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(t, "a"))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(t, "b"))
        self.assertTrue(get_monitor(t, "a").opened)
        self.assertTrue(get_monitor(t, "b").opened)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary, t.description.topology_type)

        t.close()
        self.assertEqual(2, len(t.description.server_descriptions()))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, "a"))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, "b"))
        self.assertFalse(get_monitor(t, "a").opened)
        self.assertFalse(get_monitor(t, "b").opened)
        self.assertEqual("rs", t.description.replica_set_name)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary, t.description.topology_type)

        # A closed topology should not be updated when receiving a hello.
        got_hello(
            t,
            ("a", 27017),
            {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a", "b", "c"]},
        )

        self.assertEqual(2, len(t.description.server_descriptions()))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, "a"))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, "b"))
        self.assertFalse(get_monitor(t, "a").opened)
        self.assertFalse(get_monitor(t, "b").opened)
        # Server c should not have been added.
        self.assertEqual(None, get_server(t, "c"))
        self.assertEqual("rs", t.description.replica_set_name)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary, t.description.topology_type)

    def test_handle_error(self):
        t = create_mock_topology(replica_set_name="rs")
        got_hello(
            t,
            ("a", 27017),
            {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a", "b"]},
        )

        got_hello(
            t,
            ("b", 27017),
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: False,
                "secondary": True,
                "setName": "rs",
                "hosts": ["a", "b"],
            },
        )

        errctx = _ErrorContext(AutoReconnect("mock"), 0, 0, True, None)
        t.handle_error(("a", 27017), errctx)
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, "a"))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(t, "b"))
        self.assertEqual("rs", t.description.replica_set_name)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary, t.description.topology_type)

        got_hello(
            t,
            ("a", 27017),
            {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a", "b"]},
        )

        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(t, "a"))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary, t.description.topology_type)

        t.handle_error(("b", 27017), errctx)
        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(t, "a"))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, "b"))
        self.assertEqual("rs", t.description.replica_set_name)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary, t.description.topology_type)

    def test_handle_error_removed_server(self):
        t = create_mock_topology(replica_set_name="rs")

        # No error resetting a server not in the TopologyDescription.
        errctx = _ErrorContext(AutoReconnect("mock"), 0, 0, True, None)
        t.handle_error(("b", 27017), errctx)

        # Server was *not* added as type Unknown.
        self.assertFalse(t.has_server(("b", 27017)))

    def test_discover_set_name_from_primary(self):
        # Discovering a replica set without the setName supplied by the user
        # is not yet supported by MongoClient, but Topology can do it.
        topology_settings = SetNameDiscoverySettings(
            seeds=[address], pool_class=MockPool, monitor_class=DummyMonitor
        )

        t = Topology(topology_settings)
        self.assertEqual(t.description.replica_set_name, None)
        self.assertEqual(t.description.topology_type, TOPOLOGY_TYPE.ReplicaSetNoPrimary)
        t.open()
        got_hello(
            t, address, {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a"]}
        )

        self.assertEqual(t.description.replica_set_name, "rs")
        self.assertEqual(t.description.topology_type, TOPOLOGY_TYPE.ReplicaSetWithPrimary)

        # Another response from the primary. Tests the code that processes
        # primary response when topology type is already ReplicaSetWithPrimary.
        got_hello(
            t, address, {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a"]}
        )

        # No change.
        self.assertEqual(t.description.replica_set_name, "rs")
        self.assertEqual(t.description.topology_type, TOPOLOGY_TYPE.ReplicaSetWithPrimary)

    def test_discover_set_name_from_secondary(self):
        # Discovering a replica set without the setName supplied by the user
        # is not yet supported by MongoClient, but Topology can do it.
        topology_settings = SetNameDiscoverySettings(
            seeds=[address], pool_class=MockPool, monitor_class=DummyMonitor
        )

        t = Topology(topology_settings)
        self.assertEqual(t.description.replica_set_name, None)
        self.assertEqual(t.description.topology_type, TOPOLOGY_TYPE.ReplicaSetNoPrimary)
        t.open()
        got_hello(
            t,
            address,
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: False,
                "secondary": True,
                "setName": "rs",
                "hosts": ["a"],
            },
        )

        self.assertEqual(t.description.replica_set_name, "rs")
        self.assertEqual(t.description.topology_type, TOPOLOGY_TYPE.ReplicaSetNoPrimary)

    def test_wire_version(self):
        t = create_mock_topology(replica_set_name="rs")
        t.description.check_compatible()  # No error.

        got_hello(
            t, address, {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a"]}
        )

        # Use defaults.
        server = t.get_server_by_address(address)
        self.assertEqual(server.description.min_wire_version, 0)
        self.assertEqual(server.description.max_wire_version, 0)

        got_hello(
            t,
            address,
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: True,
                "setName": "rs",
                "hosts": ["a"],
                "minWireVersion": 1,
                "maxWireVersion": 6,
            },
        )

        self.assertEqual(server.description.min_wire_version, 1)
        self.assertEqual(server.description.max_wire_version, 6)
        t.select_servers(any_server_selector)

        # Incompatible.
        got_hello(
            t,
            address,
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: True,
                "setName": "rs",
                "hosts": ["a"],
                "minWireVersion": 21,
                "maxWireVersion": 22,
            },
        )

        try:
            t.select_servers(any_server_selector)
        except ConfigurationError as e:
            # Error message should say which server failed and why.
            self.assertEqual(
                str(e),
                "Server at a:27017 requires wire version 21, but this version "
                "of PyMongo only supports up to %d." % (common.MAX_SUPPORTED_WIRE_VERSION,),
            )
        else:
            self.fail("No error with incompatible wire version")

        # Incompatible.
        got_hello(
            t,
            address,
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: True,
                "setName": "rs",
                "hosts": ["a"],
                "minWireVersion": 0,
                "maxWireVersion": 0,
            },
        )

        try:
            t.select_servers(any_server_selector)
        except ConfigurationError as e:
            # Error message should say which server failed and why.
            self.assertEqual(
                str(e),
                "Server at a:27017 reports wire version 0, but this version "
                "of PyMongo requires at least %d (MongoDB %s)."
                % (common.MIN_SUPPORTED_WIRE_VERSION, common.MIN_SUPPORTED_SERVER_VERSION),
            )
        else:
            self.fail("No error with incompatible wire version")

    def test_max_write_batch_size(self):
        t = create_mock_topology(seeds=["a", "b"], replica_set_name="rs")

        def write_batch_size():
            s = t.select_server(writable_server_selector)
            return s.description.max_write_batch_size

        got_hello(
            t,
            ("a", 27017),
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: True,
                "setName": "rs",
                "hosts": ["a", "b"],
                "maxWireVersion": 6,
                "maxWriteBatchSize": 1,
            },
        )

        got_hello(
            t,
            ("b", 27017),
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: False,
                "secondary": True,
                "setName": "rs",
                "hosts": ["a", "b"],
                "maxWireVersion": 6,
                "maxWriteBatchSize": 2,
            },
        )

        # Uses primary's max batch size.
        self.assertEqual(1, write_batch_size())

        # b becomes primary.
        got_hello(
            t,
            ("b", 27017),
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: True,
                "setName": "rs",
                "hosts": ["a", "b"],
                "maxWireVersion": 6,
                "maxWriteBatchSize": 2,
            },
        )

        self.assertEqual(2, write_batch_size())

    def test_topology_repr(self):
        t = create_mock_topology(replica_set_name="rs")
        self.addCleanup(t.close)
        got_hello(
            t,
            ("a", 27017),
            {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a", "c", "b"]},
        )
        self.assertEqual(
            repr(t.description),
            "<TopologyDescription id: %s, "
            "topology_type: ReplicaSetWithPrimary, servers: ["
            "<ServerDescription ('a', 27017) server_type: RSPrimary, rtt: 0>, "
            "<ServerDescription ('b', 27017) server_type: Unknown,"
            " rtt: None>, "
            "<ServerDescription ('c', 27017) server_type: Unknown,"
            " rtt: None>]>" % (t._topology_id,),
        )

    def test_unexpected_load_balancer(self):
        # Note: This behavior should not be reachable in practice but we
        # should handle it gracefully nonetheless. See PYTHON-2791.
        # Load balancers are included in topology with a single seed.
        t = create_mock_topology(seeds=["a"])
        mock_lb_response = {
            "ok": 1,
            "msg": "isdbgrid",
            "serviceId": ObjectId(),
            "maxWireVersion": 13,
        }
        got_hello(t, ("a", 27017), mock_lb_response)
        sds = t.description.server_descriptions()
        self.assertIn(("a", 27017), sds)
        self.assertEqual(sds[("a", 27017)].server_type_name, "LoadBalancer")
        self.assertEqual(t.description.topology_type_name, "Single")
        self.assertTrue(t.description.has_writable_server())

        # Load balancers are removed from a topology with multiple seeds.
        t = create_mock_topology(seeds=["a", "b"])
        got_hello(t, ("a", 27017), mock_lb_response)
        self.assertNotIn(("a", 27017), t.description.server_descriptions())
        self.assertEqual(t.description.topology_type_name, "Unknown")


def wait_for_primary(topology):
    """Wait for a Topology to discover a writable server.

    If the monitor is currently calling hello, a blocking call to
    select_server from this thread can trigger a spurious wake of the monitor
    thread. In applications this is harmless but it would break some tests,
    so we pass server_selection_timeout=0 and poll instead.
    """

    def get_primary():
        try:
            return topology.select_server(writable_server_selector, 0)
        except ConnectionFailure:
            return None

    return wait_until(get_primary, "find primary")


class TestTopologyErrors(TopologyTest):
    # Errors when calling hello.

    def test_pool_reset(self):
        # hello succeeds at first, then always raises socket error.
        hello_count = [0]

        class TestMonitor(Monitor):
            def _check_with_socket(self, *args, **kwargs):
                hello_count[0] += 1
                if hello_count[0] == 1:
                    return Hello({"ok": 1, "maxWireVersion": 6}), 0
                else:
                    raise AutoReconnect("mock monitor error")

        t = create_mock_topology(monitor_class=TestMonitor)
        self.addCleanup(t.close)
        server = wait_for_primary(t)
        self.assertEqual(1, hello_count[0])
        generation = server.pool.gen.get_overall()

        # Pool is reset by hello failure.
        t.request_check_all()
        self.assertNotEqual(generation, server.pool.gen.get_overall())

    def test_hello_retry(self):
        # hello succeeds at first, then raises socket error, then succeeds.
        hello_count = [0]

        class TestMonitor(Monitor):
            def _check_with_socket(self, *args, **kwargs):
                hello_count[0] += 1
                if hello_count[0] in (1, 3):
                    return Hello({"ok": 1, "maxWireVersion": 6}), 0
                else:
                    raise AutoReconnect("mock monitor error #%s" % (hello_count[0],))

        t = create_mock_topology(monitor_class=TestMonitor)
        self.addCleanup(t.close)
        server = wait_for_primary(t)
        self.assertEqual(1, hello_count[0])
        self.assertEqual(SERVER_TYPE.Standalone, server.description.server_type)

        # Second hello call, server is marked Unknown, then the monitor
        # immediately runs a retry (third hello).
        t.request_check_all()
        # The third hello call (the immediate retry) happens sometime soon
        # after the failed check triggered by request_check_all. Wait until
        # the server becomes known again.
        server = t.select_server(writable_server_selector, 0.250)
        self.assertEqual(SERVER_TYPE.Standalone, server.description.server_type)
        self.assertEqual(3, hello_count[0])

    def test_internal_monitor_error(self):
        exception = AssertionError("internal error")

        class TestMonitor(Monitor):
            def _check_with_socket(self, *args, **kwargs):
                raise exception

        t = create_mock_topology(monitor_class=TestMonitor)
        self.addCleanup(t.close)
        with self.assertRaisesRegex(ConnectionFailure, "internal error"):
            t.select_server(any_server_selector, server_selection_timeout=0.5)


class TestServerSelectionErrors(TopologyTest):
    def assertMessage(self, message, topology, selector=any_server_selector):
        with self.assertRaises(ConnectionFailure) as context:
            topology.select_server(selector, server_selection_timeout=0)

        self.assertIn(message, str(context.exception))

    def test_no_primary(self):
        t = create_mock_topology(replica_set_name="rs")
        got_hello(
            t,
            address,
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: False,
                "secondary": True,
                "setName": "rs",
                "hosts": ["a"],
            },
        )

        self.assertMessage(
            'No replica set members match selector "Primary()"', t, ReadPreference.PRIMARY
        )

        self.assertMessage("No primary available for writes", t, writable_server_selector)

    def test_no_secondary(self):
        t = create_mock_topology(replica_set_name="rs")
        got_hello(
            t, address, {"ok": 1, HelloCompat.LEGACY_CMD: True, "setName": "rs", "hosts": ["a"]}
        )

        self.assertMessage(
            "No replica set members match selector"
            ' "Secondary(tag_sets=None, max_staleness=-1, hedge=None)"',
            t,
            ReadPreference.SECONDARY,
        )

        self.assertMessage(
            "No replica set members match selector"
            " \"Secondary(tag_sets=[{'dc': 'ny'}], max_staleness=-1, "
            'hedge=None)"',
            t,
            Secondary(tag_sets=[{"dc": "ny"}]),
        )

    def test_bad_replica_set_name(self):
        t = create_mock_topology(replica_set_name="rs")
        got_hello(
            t,
            address,
            {
                "ok": 1,
                HelloCompat.LEGACY_CMD: False,
                "secondary": True,
                "setName": "wrong",
                "hosts": ["a"],
            },
        )

        self.assertMessage('No replica set members available for replica set name "rs"', t)

    def test_multiple_standalones(self):
        # Standalones are removed from a topology with multiple seeds.
        t = create_mock_topology(seeds=["a", "b"])
        got_hello(t, ("a", 27017), {"ok": 1})
        got_hello(t, ("b", 27017), {"ok": 1})
        self.assertMessage("No servers available", t)

    def test_no_mongoses(self):
        # Standalones are removed from a topology with multiple seeds.
        t = create_mock_topology(seeds=["a", "b"])

        # Discover a mongos and change topology type to Sharded.
        got_hello(t, ("a", 27017), {"ok": 1, "msg": "isdbgrid"})

        # Oops, both servers are standalone now. Remove them.
        got_hello(t, ("a", 27017), {"ok": 1})
        got_hello(t, ("b", 27017), {"ok": 1})
        self.assertMessage("No mongoses available", t)


if __name__ == "__main__":
    unittest.main()
