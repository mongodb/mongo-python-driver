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

"""Test the topology module."""

import sys

sys.path[0:0] = [""]

import socket
import threading

from bson.py3compat import imap
from pymongo import common
from pymongo.server_type import SERVER_TYPE
from pymongo.topology import Topology
from pymongo.topology_description import TOPOLOGY_TYPE
from pymongo.errors import (ConfigurationError,
                            ConnectionFailure)
from pymongo.ismaster import IsMaster
from pymongo.monitor import Monitor
from pymongo.pool import PoolOptions
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import (any_server_selector,
                                      writable_server_selector)
from pymongo.settings import TopologySettings
from test import unittest, client_knobs


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

    def maybe_return_socket(self, _):
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


def create_mock_topology(seeds=None, set_name=None, monitor_class=MockMonitor):
    partitioned_seeds = list(imap(common.partition_node, seeds or ['a']))
    topology_settings = TopologySettings(
        partitioned_seeds,
        set_name=set_name,
        pool_class=MockPool,
        monitor_class=monitor_class)

    c = Topology(topology_settings)
    c.open()
    return c


def got_ismaster(topology, server_address, ismaster_response):
    server_description = ServerDescription(
        server_address,
        IsMaster(ismaster_response),
        MovingAverage([0]))

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

    def tearDown(self):
        self.client_knobs.disable()
        super(TopologyTest, self).tearDown()


class TestTopologyConfiguration(TopologyTest):
    def test_timeout_configuration(self):
        pool_options = PoolOptions(connect_timeout=1, socket_timeout=2)
        topology_settings = TopologySettings(pool_options=pool_options)
        t = Topology(topology_settings=topology_settings)
        server = t.select_server(any_server_selector)

        # The pool for application operations obeys our settings.
        self.assertEqual(1, server._pool.opts.connect_timeout)
        self.assertEqual(2, server._pool.opts.socket_timeout)

        # The pool for monitoring operations uses our connect_timeout as both
        # its connect_timeout and its socket_timeout.
        monitor = server._monitor
        self.assertEqual(1, monitor._pool.opts.connect_timeout)
        self.assertEqual(1, monitor._pool.opts.socket_timeout)


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
            c = create_mock_topology()

            # Can't select a server while the only server is of type Unknown.
            self.assertRaises(
                ConnectionFailure,
                c.select_servers, any_server_selector, server_wait_time=0)

            got_ismaster(c, address, ismaster_response)

            # Topology type never changes.
            self.assertEqual(TOPOLOGY_TYPE.Single, c.description.topology_type)

            # No matter whether the server is writable,
            # select_servers() returns it.
            s = c.select_server(writable_server_selector)
            self.assertEqual(server_type, s.description.server_type)

    def test_reopen(self):
        c = create_mock_topology()

        # Additional calls are permitted.
        c.open()
        c.open()

    def test_unavailable_seed(self):
        c = create_mock_topology()
        disconnected(c, address)
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'a'))

    def test_round_trip_time(self):
        round_trip_time = 1

        class TestMonitor(Monitor):
            def _check_with_socket(self, sock_info):
                return IsMaster({'ok': 1}), round_trip_time

        c = create_mock_topology(monitor_class=TestMonitor)
        s = c.select_server(writable_server_selector)
        self.assertEqual(1, s.description.round_trip_time)

        round_trip_time = 3
        c.request_check_all()

        # Average of 1 and 3.
        self.assertEqual(2, s.description.round_trip_time)


class TestMultiServerTopology(TopologyTest):
    def test_unexpected_host(self):
        # Received ismaster response from host not in topology.
        # E.g., a race where the host is removed before it responds.
        c = create_mock_topology(['a', 'b'], set_name='rs')

        # 'b' is not in the set.
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'hosts': ['a'],
            'setName': 'rs'})

        self.assertFalse(c.has_server(('b', 27017)))

        # 'b' still thinks it's in the set.
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'hosts': ['a', 'b'],
            'setName': 'rs'})

        # We don't add it.
        self.assertFalse(c.has_server(('b', 27017)))

    def test_ghost_seed(self):
        c = create_mock_topology(['a', 'b'])
        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': False,
            'isreplicaset': True})

        self.assertEqual(SERVER_TYPE.RSGhost, get_type(c, 'a'))
        self.assertEqual(TOPOLOGY_TYPE.Unknown, c.description.topology_type)

    def test_standalone_removed(self):
        c = create_mock_topology(['a', 'b'])
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True})

        self.assertEqual(1, len(c.description.server_descriptions()))
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False})

        self.assertEqual(0, len(c.description.server_descriptions()))

    def test_mongos_ha(self):
        c = create_mock_topology(['a', 'b'])
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'msg': 'isdbgrid'})

        self.assertEqual(TOPOLOGY_TYPE.Sharded, c.description.topology_type)
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': True,
            'msg': 'isdbgrid'})

        self.assertEqual(SERVER_TYPE.Mongos, get_type(c, 'a'))
        self.assertEqual(SERVER_TYPE.Mongos, get_type(c, 'b'))

    def test_non_mongos_server(self):
        c = create_mock_topology(['a', 'b'])
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'msg': 'isdbgrid'})

        # Standalone is removed from sharded cluster description.
        got_ismaster(c, ('b', 27017), {'ok': 1})
        self.assertFalse(c.has_server(('b', 27017)))

    def test_rs_discovery(self):
        c = create_mock_topology(set_name='rs')

        # At first, A, B, and C are secondaries.
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b', 'c']})

        self.assertEqual(3, len(c.description.server_descriptions()))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(c, 'a'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'b'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'c'))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         c.description.topology_type)

        # Admin removes A, adds a high-priority member D which becomes primary.
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'primary': 'd',
            'hosts': ['b', 'c', 'd']})

        self.assertEqual(4, len(c.description.server_descriptions()))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(c, 'a'))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(c, 'b'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'c'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'd'))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         c.description.topology_type)

        # Primary responds.
        got_ismaster(c, ('d', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['b', 'c', 'd', 'e']})

        self.assertEqual(4, len(c.description.server_descriptions()))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(c, 'b'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'c'))
        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(c, 'd'))

        # E is new.
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'e'))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         c.description.topology_type)

        # Stale response from C.
        got_ismaster(c, ('c', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b', 'c']})

        # We don't add A back.
        self.assertEqual(4, len(c.description.server_descriptions()))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(c, 'b'))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(c, 'c'))
        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(c, 'd'))

        # We don't remove E.
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'e'))

    def test_reset(self):
        c = create_mock_topology(set_name='rs')
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(c, 'a'))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(c, 'b'))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         c.description.topology_type)

        c.reset()
        self.assertEqual(2, len(c.description.server_descriptions()))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'a'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'b'))
        self.assertEqual('rs', c.description.set_name)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         c.description.topology_type)

        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(c, 'a'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'b'))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         c.description.topology_type)

    def test_reset_server(self):
        c = create_mock_topology(set_name='rs')
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        c.reset_server(('a', 27017))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'a'))
        self.assertEqual(SERVER_TYPE.RSSecondary, get_type(c, 'b'))
        self.assertEqual('rs', c.description.set_name)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         c.description.topology_type)

        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(c, 'a'))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         c.description.topology_type)

        c.reset_server(('b', 27017))
        self.assertEqual(SERVER_TYPE.RSPrimary, get_type(c, 'a'))
        self.assertEqual(SERVER_TYPE.Unknown, get_type(c, 'b'))
        self.assertEqual('rs', c.description.set_name)
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         c.description.topology_type)

    def test_reset_removed_server(self):
        c = create_mock_topology(set_name='rs')

        # No error resetting a server not in the TopologyDescription.
        c.reset_server(('b', 27017))

        # Server was *not* added as type Unknown.
        self.assertFalse(c.has_server(('b', 27017)))

    def test_discover_set_name_from_primary(self):
        # Discovering a replica set without the setName supplied by the user
        # is not yet supported by MongoClient, but Topology can do it.
        topology_settings = SetNameDiscoverySettings(
            seeds=[address],
            pool_class=MockPool,
            monitor_class=MockMonitor)

        c = Topology(topology_settings)
        self.assertEqual(c.description.set_name, None)
        self.assertEqual(c.description.topology_type,
                         TOPOLOGY_TYPE.ReplicaSetNoPrimary)

        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a']})

        self.assertEqual(c.description.set_name, 'rs')
        self.assertEqual(c.description.topology_type,
                         TOPOLOGY_TYPE.ReplicaSetWithPrimary)

        # Another response from the primary. Tests the code that processes
        # primary response when topology type is already ReplicaSetWithPrimary.
        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a']})

        # No change.
        self.assertEqual(c.description.set_name, 'rs')
        self.assertEqual(c.description.topology_type,
                         TOPOLOGY_TYPE.ReplicaSetWithPrimary)

    def test_discover_set_name_from_secondary(self):
        # Discovering a replica set without the setName supplied by the user
        # is not yet supported by MongoClient, but Topology can do it.
        topology_settings = SetNameDiscoverySettings(
            seeds=[address],
            pool_class=MockPool,
            monitor_class=MockMonitor)

        c = Topology(topology_settings)
        self.assertEqual(c.description.set_name, None)
        self.assertEqual(c.description.topology_type,
                         TOPOLOGY_TYPE.ReplicaSetNoPrimary)

        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a']})

        self.assertEqual(c.description.set_name, 'rs')
        self.assertEqual(c.description.topology_type,
                         TOPOLOGY_TYPE.ReplicaSetNoPrimary)

    def test_primary_disconnect(self):
        c = create_mock_topology(set_name='rs')
        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a']})

        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         c.description.topology_type)

        disconnected(c, address)
        self.assertTrue(c.has_server(address))  # Not removed.
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         c.description.topology_type)

    def test_primary_becomes_standalone(self):
        c = create_mock_topology(set_name='rs')
        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a']})

        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         c.description.topology_type)

        # An administrator restarts primary as standalone.
        got_ismaster(c, address, {'ok': 1})
        self.assertFalse(c.has_server(address))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         c.description.topology_type)

    def test_primary_wrong_set_name(self):
        c = create_mock_topology(set_name='rs')
        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'wrong',
            'hosts': ['a']})

        self.assertFalse(c.has_server(address))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         c.description.topology_type)

    def test_secondary_wrong_set_name(self):
        c = create_mock_topology(set_name='rs')
        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'wrong',
            'hosts': ['a']})

        self.assertFalse(c.has_server(address))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         c.description.topology_type)

    def test_secondary_wrong_set_name_with_primary(self):
        c = create_mock_topology(['a', 'b'], set_name='rs')

        # Find the primary normally.
        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b']})

        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         c.description.topology_type)

        self.assertTrue(c.has_server(('b', 27017)))
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'wrong',
            'hosts': ['a', 'b']})

        # Secondary removed.
        self.assertFalse(c.has_server(('b', 27017)))
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         c.description.topology_type)

    def test_non_rs_member(self):
        c = create_mock_topology(['a', 'b'], set_name='rs')
        self.assertTrue(c.has_server(('b', 27017)))
        got_ismaster(c, ('b', 27017), {'ok': 1})  # Standalone is removed.
        self.assertFalse(c.has_server(('b', 27017)))

    def test_wire_version(self):
        c = create_mock_topology(set_name='rs')
        c.description.check_compatible()  # No error.

        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a']})

        # Use defaults.
        server = c.get_server_by_address(address)
        self.assertEqual(server.description.min_wire_version, 0)
        self.assertEqual(server.description.max_wire_version, 0)

        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a'],
            'minWireVersion': 1,
            'maxWireVersion': 5})

        self.assertEqual(server.description.min_wire_version, 1)
        self.assertEqual(server.description.max_wire_version, 5)

        # Incompatible.
        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a'],
            'minWireVersion': 11,
            'maxWireVersion': 12})

        try:
            c.select_servers(any_server_selector)
        except ConfigurationError as e:
            # Error message should say which server failed and why.
            self.assertTrue('a:27017' in str(e))
            self.assertTrue('wire protocol versions 11 through 12' in str(e))
        else:
            self.fail('No error with incompatible wire version')

    def test_max_write_batch_size(self):
        c = create_mock_topology(seeds=['a', 'b'], set_name='rs')

        def write_batch_size():
            s = c.select_server(writable_server_selector)
            return s.description.max_write_batch_size

        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b'],
            'maxWriteBatchSize': 1})

        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b'],
            'maxWriteBatchSize': 2})

        # Uses primary's max batch size.
        self.assertEqual(1, write_batch_size())

        # b becomes primary.
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b'],
            'maxWriteBatchSize': 2})

        self.assertEqual(2, write_batch_size())


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
                    raise socket.error()

        c = create_mock_topology(monitor_class=TestMonitor)
        # Await first ismaster call.
        s = c.select_server(writable_server_selector)
        self.assertEqual(1, ismaster_count[0])
        pool_id = s.pool.pool_id

        # Pool is reset by ismaster failure.
        c.request_check_all()
        self.assertNotEqual(pool_id, s.pool.pool_id)

    def test_ismaster_retry(self):
        # ismaster succeeds at first, then raises socket error, then succeeds.
        ismaster_count = [0]

        class TestMonitor(Monitor):
            def _check_with_socket(self, sock_info):
                ismaster_count[0] += 1
                if ismaster_count[0] in (1, 3):
                    return IsMaster({'ok': 1}), 0
                else:
                    raise socket.error()

        c = create_mock_topology(monitor_class=TestMonitor)

        # Await first ismaster call.
        s = c.select_server(writable_server_selector)
        self.assertEqual(1, ismaster_count[0])
        self.assertEqual(SERVER_TYPE.Standalone, s.description.server_type)

        # Second ismaster call, then immediately the third.
        c.request_check_all()
        self.assertEqual(3, ismaster_count[0])
        self.assertEqual(SERVER_TYPE.Standalone, get_type(c, 'a'))

    def test_selection_failure(self):
        # While ismaster fails, ensure it's called about every 10 ms.
        ismaster_count = [0]

        class TestMonitor(Monitor):
            def _check_with_socket(self, sock_info):
                ismaster_count[0] += 1
                raise socket.error()

        c = create_mock_topology(monitor_class=TestMonitor)

        self.assertRaises(
            ConnectionFailure,
            c.select_servers, any_server_selector, server_wait_time=0.5)

        self.assertTrue(
            25 <= ismaster_count[0] <= 100,
            "Expected ismaster to be attempted about 50 times, not %d" %
            ismaster_count[0])


if __name__ == "__main__":
    unittest.main()
