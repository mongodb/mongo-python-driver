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

import sys

sys.path[0:0] = [""]

import threading

from bson.py3compat import imap
from pymongo import common
from pymongo.read_preferences import ReadPreference, Secondary
from pymongo.server_type import SERVER_TYPE
from pymongo.topology import Topology
from pymongo.topology_description import TOPOLOGY_TYPE
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure)
from pymongo.ismaster import IsMaster
from pymongo.monitor import Monitor
from pymongo.pool import PoolOptions
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import (any_server_selector,
                                      writable_server_selector)
from pymongo.settings import TopologySettings
from test import client_knobs, unittest
from test.utils import wait_until


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

    def get_socket(self, all_credentials):
        return MockSocketInfo()

    def return_socket(self, _):
        pass

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


class SetNameDiscoverySettings(TopologySettings):
    def get_topology_type(self):
        return TOPOLOGY_TYPE.ReplicaSetNoPrimary


address = ('a', 27017)


def create_mock_topology(
        seeds=None,
        replica_set_name=None,
        monitor_class=MockMonitor):
    partitioned_seeds = list(imap(common.partition_node, seeds or ['a']))
    topology_settings = TopologySettings(
        partitioned_seeds,
        replica_set_name=replica_set_name,
        pool_class=MockPool,
        monitor_class=monitor_class)

    t = Topology(topology_settings)
    t.open()
    return t


def got_ismaster(topology, server_address, ismaster_response):
    server_description = ServerDescription(
        server_address, IsMaster(ismaster_response), 0)

    topology.on_change(server_description)


def disconnected(topology, server_address):
    # Create new description of server type Unknown.
    topology.on_change(ServerDescription(server_address))


def get_type(topology, hostname):
    description = topology.get_server_by_address((hostname, 27017)).description
    return description.server_type


class TopologyTest(unittest.TestCase):
    """Disables periodic monitoring, to make tests deterministic."""

    def setUp(self):
        super(TopologyTest, self).setUp()
        self.client_knobs = client_knobs(heartbeat_frequency=999999)
        self.client_knobs.enable()
        self.addCleanup(self.client_knobs.disable)


# Use assertRaisesRegex if available, otherwise use Python 2.7's
# deprecated assertRaisesRegexp, with a 'p'.
if not hasattr(unittest.TestCase, 'assertRaisesRegex'):
    TopologyTest.assertRaisesRegex = unittest.TestCase.assertRaisesRegexp


class TestTopologyConfiguration(TopologyTest):
    def test_timeout_configuration(self):
        pool_options = PoolOptions(connect_timeout=1, socket_timeout=2)
        topology_settings = TopologySettings(pool_options=pool_options)
        t = Topology(topology_settings=topology_settings)
        t.open()

        # Get the default server.
        server = t.get_server_by_address(('localhost', 27017))

        # The pool for application operations obeys our settings.
        self.assertEqual(1, server._pool.opts.connect_timeout)
        self.assertEqual(2, server._pool.opts.socket_timeout)

        # The pool for monitoring operations uses our connect_timeout as both
        # its connect_timeout and its socket_timeout.
        monitor = server._monitor
        self.assertEqual(1, monitor._pool.opts.connect_timeout)
        self.assertEqual(1, monitor._pool.opts.socket_timeout)

        # The monitor, not its pool, is responsible for calling ismaster.
        self.assertFalse(monitor._pool.handshake)


class TestSingleServerTopology(TopologyTest):
    def test_direct_connection(self):
        for server_type, ismaster_response in [
            (SERVER_TYPE.RSPrimary, {
                'ok': 1,
                'ismaster': True,
                'hosts': ['a'],
                'setName': 'rs'}),

            (SERVER_TYPE.RSSecondary, {
                'ok': 1,
                'ismaster': False,
                'secondary': True,
                'hosts': ['a'],
                'setName': 'rs'}),

            (SERVER_TYPE.Mongos, {
                'ok': 1,
                'ismaster': True,
                'msg': 'isdbgrid'}),

            (SERVER_TYPE.RSArbiter, {
                'ok': 1,
                'ismaster': False,
                'arbiterOnly': True,
                'hosts': ['a'],
                'setName': 'rs'}),

            (SERVER_TYPE.Standalone, {
                'ok': 1,
                'ismaster': True}),

            # Slave.
            (SERVER_TYPE.Standalone, {
                'ok': 1,
                'ismaster': False}),
        ]:
            t = create_mock_topology()

            # Can't select a server while the only server is of type Unknown.
            with self.assertRaisesRegex(ConnectionFailure,
                                        'No servers found yet'):
                t.select_servers(any_server_selector,
                                 server_selection_timeout=0)

            got_ismaster(t, address, ismaster_response)

            # Topology type never changes.
            self.assertEqual(TOPOLOGY_TYPE.Single, t.description.topology_type)

            # No matter whether the server is writable,
            # select_servers() returns it.
            s = t.select_server(writable_server_selector)
            self.assertEqual(server_type, s.description.server_type)

    def test_reopen(self):
        t = create_mock_topology()

        # Additional calls are permitted.
        t.open()
        t.open()

    def test_unavailable_seed(self):
        t = create_mock_topology()
        disconnected(t, address)
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, 'a'))

    def test_round_trip_time(self):
        round_trip_time = 125
        available = True

        class TestMonitor(Monitor):
            def _check_with_socket(self, sock_info):
                if available:
                    return IsMaster({'ok': 1}), round_trip_time
                else:
                    raise AutoReconnect('mock monitor error')

        t = create_mock_topology(monitor_class=TestMonitor)
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
                t.select_server(writable_server_selector,
                                server_selection_timeout=0.1)
            except ConnectionFailure:
                return True
            else:
                return False

        wait_until(raises_err, 'discover server is down')
        self.assertIsNone(s.description.round_trip_time)

        # Bring it back, RTT is now 20 milliseconds.
        available = True
        round_trip_time = 20

        def new_average():
            # We reset the average to the most recent measurement.
            description = s.description
            return (description.round_trip_time is not None
                    and round(abs(20 - description.round_trip_time), 7) == 0)

        tries = 0
        while not new_average():
            t.request_check_all()
            tries += 1
            if tries > 10:
                self.fail("Didn't ever calculate correct new average")


class TestMultiServerTopology(TopologyTest):
    def test_close(self):
        t = create_mock_topology(replica_set_name='rs')
        got_ismaster(t, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        got_ismaster(t, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(t, 'a'))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(t, 'b'))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         t.description.topology_type)

        t.close()
        self.assertEqual(2, len(t.description.server_descriptions()))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, 'a'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, 'b'))
        self.assertEqual('rs', t.description.replica_set_name)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         t.description.topology_type)

        got_ismaster(t, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(t, 'a'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, 'b'))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         t.description.topology_type)

    def test_reset_server(self):
        t = create_mock_topology(replica_set_name='rs')
        got_ismaster(t, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        got_ismaster(t, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        t.reset_server(('a', 27017))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, 'a'))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(t, 'b'))
        self.assertEqual('rs', t.description.replica_set_name)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         t.description.topology_type)

        got_ismaster(t, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(t, 'a'))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         t.description.topology_type)

        t.reset_server(('b', 27017))
        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(t, 'a'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(t, 'b'))
        self.assertEqual('rs', t.description.replica_set_name)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         t.description.topology_type)

    def test_reset_removed_server(self):
        t = create_mock_topology(replica_set_name='rs')

        # No error resetting a server not in the TopologyDescription.
        t.reset_server(('b', 27017))

        # Server was *not* added as type Unknown.
        self.assertFalse(t.has_server(('b', 27017)))

    def test_discover_set_name_from_primary(self):
        # Discovering a replica set without the setName supplied by the user
        # is not yet supported by MongoClient, but Topology can do it.
        topology_settings = SetNameDiscoverySettings(
            seeds=[address],
            pool_class=MockPool,
            monitor_class=MockMonitor)

        t = Topology(topology_settings)
        self.assertEqual(t.description.replica_set_name, None)
        self.assertEqual(t.description.topology_type,
                         TOPOLOGY_TYPE.ReplicaSetNoPrimary)

        got_ismaster(t, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a']})

        self.assertEqual(t.description.replica_set_name, 'rs')
        self.assertEqual(t.description.topology_type,
                         TOPOLOGY_TYPE.ReplicaSetWithPrimary)

        # Another response from the primary. Tests the code that processes
        # primary response when topology type is already ReplicaSetWithPrimary.
        got_ismaster(t, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a']})

        # No change.
        self.assertEqual(t.description.replica_set_name, 'rs')
        self.assertEqual(t.description.topology_type,
                         TOPOLOGY_TYPE.ReplicaSetWithPrimary)

    def test_discover_set_name_from_secondary(self):
        # Discovering a replica set without the setName supplied by the user
        # is not yet supported by MongoClient, but Topology can do it.
        topology_settings = SetNameDiscoverySettings(
            seeds=[address],
            pool_class=MockPool,
            monitor_class=MockMonitor)

        t = Topology(topology_settings)
        self.assertEqual(t.description.replica_set_name, None)
        self.assertEqual(t.description.topology_type,
                         TOPOLOGY_TYPE.ReplicaSetNoPrimary)

        got_ismaster(t, address, {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a']})

        self.assertEqual(t.description.replica_set_name, 'rs')
        self.assertEqual(t.description.topology_type,
                         TOPOLOGY_TYPE.ReplicaSetNoPrimary)

    def test_wire_version(self):
        t = create_mock_topology(replica_set_name='rs')
        t.description.check_compatible()  # No error.

        got_ismaster(t, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a']})

        # Use defaults.
        server = t.get_server_by_address(address)
        self.assertEqual(server.description.min_wire_version, 0)
        self.assertEqual(server.description.max_wire_version, 0)

        got_ismaster(t, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a'],
            'minWireVersion': 1,
            'maxWireVersion': 5})

        self.assertEqual(server.description.min_wire_version, 1)
        self.assertEqual(server.description.max_wire_version, 5)

        # Incompatible.
        got_ismaster(t, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a'],
            'minWireVersion': 11,
            'maxWireVersion': 12})

        try:
            t.select_servers(any_server_selector)
        except ConfigurationError as e:
            # Error message should say which server failed and why.
            self.assertTrue('a:27017' in str(e))
            self.assertTrue('wire protocol versions 11 through 12' in str(e))
        else:
            self.fail('No error with incompatible wire version')

    def test_max_write_batch_size(self):
        t = create_mock_topology(seeds=['a', 'b'], replica_set_name='rs')

        def write_batch_size():
            s = t.select_server(writable_server_selector)
            return s.description.max_write_batch_size

        got_ismaster(t, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b'],
            'maxWriteBatchSize': 1})

        got_ismaster(t, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b'],
            'maxWriteBatchSize': 2})

        # Uses primary's max batch size.
        self.assertEqual(1, write_batch_size())

        # b becomes primary.
        got_ismaster(t, ('b', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b'],
            'maxWriteBatchSize': 2})

        self.assertEqual(2, write_batch_size())


def wait_for_master(topology):
    """Wait for a Topology to discover a writable server.

    If the monitor is currently calling ismaster, a blocking call to
    select_server from this thread can trigger a spurious wake of the monitor
    thread. In applications this is harmless but it would break some tests,
    so we pass server_selection_timeout=0 and poll instead.
    """
    def get_master():
        try:
            return topology.select_server(writable_server_selector, 0)
        except ConnectionFailure:
            return None

    return wait_until(get_master, 'find master')


class TestTopologyErrors(TopologyTest):
    # Errors when calling ismaster.

    def test_pool_reset(self):
        # ismaster succeeds at first, then always raises socket error.
        ismaster_count = [0]

        class TestMonitor(Monitor):
            def _check_with_socket(self, sock_info):
                ismaster_count[0] += 1
                if ismaster_count[0] == 1:
                    return IsMaster({'ok': 1}), 0
                else:
                    raise AutoReconnect('mock monitor error')

        t = create_mock_topology(monitor_class=TestMonitor)
        server = wait_for_master(t)
        self.assertEqual(1, ismaster_count[0])
        pool_id = server.pool.pool_id

        # Pool is reset by ismaster failure.
        t.request_check_all()
        self.assertNotEqual(pool_id, server.pool.pool_id)

    def test_ismaster_retry(self):
        # ismaster succeeds at first, then raises socket error, then succeeds.
        ismaster_count = [0]

        class TestMonitor(Monitor):
            def _check_with_socket(self, sock_info):
                ismaster_count[0] += 1
                if ismaster_count[0] in (1, 3):
                    return IsMaster({'ok': 1}), 0
                else:
                    raise AutoReconnect('mock monitor error')

        t = create_mock_topology(monitor_class=TestMonitor)
        server = wait_for_master(t)
        self.assertEqual(1, ismaster_count[0])
        self.assertEqual(SERVER_TYPE.Standalone,
                         server.description.server_type)

        # Second ismaster call, then immediately the third.
        t.request_check_all()
        self.assertEqual(3, ismaster_count[0])
        self.assertEqual(SERVER_TYPE.Standalone, get_type(t, 'a'))

    def test_internal_monitor_error(self):
        exception = AssertionError('internal error')

        class TestMonitor(Monitor):
            def _check_with_socket(self, sock_info):
                raise exception

        t = create_mock_topology(monitor_class=TestMonitor)
        with self.assertRaisesRegex(ConnectionFailure, 'internal error'):
            t.select_server(any_server_selector,
                            server_selection_timeout=0.5)


class TestServerSelectionErrors(TopologyTest):
    def assertMessage(self, message, topology, selector=any_server_selector):
        with self.assertRaises(ConnectionFailure) as context:
            topology.select_server(selector, server_selection_timeout=0)

        self.assertEqual(message, str(context.exception))

    def test_no_primary(self):
        t = create_mock_topology(replica_set_name='rs')
        got_ismaster(t, address, {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a']})

        self.assertMessage('No replica set members match selector "Primary()"',
                           t, ReadPreference.PRIMARY)

        self.assertMessage('No primary available for writes',
                           t, writable_server_selector)

    def test_no_secondary(self):
        t = create_mock_topology(replica_set_name='rs')
        got_ismaster(t, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a']})

        self.assertMessage(
            'No replica set members match selector'
            ' "Secondary(tag_sets=None)"',
            t, ReadPreference.SECONDARY)

        self.assertMessage(
            "No replica set members match selector"
            " \"Secondary(tag_sets=[{'dc': 'ny'}])\"",
            t, Secondary(tag_sets=[{'dc': 'ny'}]))

    def test_bad_replica_set_name(self):
        t = create_mock_topology(replica_set_name='rs')
        got_ismaster(t, address, {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'wrong',
            'hosts': ['a']})

        self.assertMessage(
            'No replica set members available for replica set name "rs"', t)

    def test_multiple_standalones(self):
        # Standalones are removed from a topology with multiple seeds.
        t = create_mock_topology(seeds=['a', 'b'])
        got_ismaster(t, ('a', 27017), {'ok': 1})
        got_ismaster(t, ('b', 27017), {'ok': 1})
        self.assertMessage('No servers available', t)

    def test_no_mongoses(self):
        # Standalones are removed from a topology with multiple seeds.
        t = create_mock_topology(seeds=['a', 'b'])

        # Discover a mongos and change topology type to Sharded.
        got_ismaster(t, ('a', 27017), {'ok': 1, 'msg': 'isdbgrid'})

        # Oops, both servers are standalone now. Remove them.
        got_ismaster(t, ('a', 27017), {'ok': 1})
        got_ismaster(t, ('b', 27017), {'ok': 1})
        self.assertMessage('No mongoses available', t)


if __name__ == "__main__":
    unittest.main()
