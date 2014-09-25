# Copyright 2013-2014 MongoDB, Inc.
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

"""Tools for mocking parts of PyMongo to test other parts."""

import socket
from functools import partial

from pymongo import common
from pymongo import MongoClient
from pymongo.ismaster import IsMaster
from pymongo.monitor import Monitor
from pymongo.pool import Pool, PoolOptions
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription

from test import host as default_host, port as default_port


class MockPool(Pool):
    def __init__(self, client, pair, *args, **kwargs):
        # MockPool gets a 'client' arg, regular pools don't.
        self.client = client
        self.mock_host, self.mock_port = pair

        # Actually connect to the default server.
        Pool.__init__(self,
            (default_host, default_port), PoolOptions(connect_timeout=20))

    def get_socket(self, all_credentials):
        client = self.client
        host_and_port = '%s:%s' % (self.mock_host, self.mock_port)
        if host_and_port in client.mock_down_hosts:
            raise socket.error('mock error')

        assert host_and_port in (
            client.mock_standalones
            + client.mock_members
            + client.mock_mongoses), "bad host: %s" % host_and_port

        sock_info = Pool.get_socket(self, all_credentials)
        sock_info.mock_host = self.mock_host
        sock_info.mock_port = self.mock_port
        return sock_info


class MockMonitor(Monitor):
    def __init__(
            self,
            client,
            server_description,
            topology,
            pool,
            topology_settings):
        # MockMonitor gets a 'client' arg, regular monitors don't.
        self.client = client
        self.mock_address = server_description.address

        # Actually connect to the default server.
        Monitor.__init__(
            self,
            ServerDescription((default_host, default_port)),
            topology,
            pool,
            topology_settings)

    def _check_once(self):
        try:
            response = self.client.mock_is_master('%s:%d' % self.mock_address)
            return ServerDescription(
                self.mock_address,
                IsMaster(response),
                MovingAverage([0]))
        except socket.error:
            return None


class MockClient(MongoClient):
    def __init__(
            self, standalones, members, mongoses, ismaster_hosts=None,
            *args, **kwargs):
        """A MongoClient connected to the default server, with a mock topology.

        standalones, members, mongoses determine the configuration of the
        topology. They are formatted like ['a:1', 'b:2']. ismaster_hosts
        provides an alternative host list for the server's mocked ismaster
        response; see test_connect_with_internal_ips.
        """
        self.mock_standalones = standalones[:]
        self.mock_members = members[:]

        if self.mock_members:
            self.mock_primary = self.mock_members[0]
        else:
            self.mock_primary = None

        if ismaster_hosts is not None:
            self.mock_ismaster_hosts = ismaster_hosts
        else:
            self.mock_ismaster_hosts = members[:]

        self.mock_mongoses = mongoses[:]

        # Hosts that should raise socket errors.
        self.mock_down_hosts = []

        # Hostname -> (min wire version, max wire version)
        self.mock_wire_versions = {}

        # Hostname -> max write batch size
        self.mock_max_write_batch_sizes = {}

        kwargs['_pool_class'] = partial(MockPool, self)
        kwargs['_monitor_class'] = partial(MockMonitor, self)

        super(MockClient, self).__init__(*args, **kwargs)

    def kill_host(self, host):
        """Host is like 'a:1'."""
        self.mock_down_hosts.append(host)

    def revive_host(self, host):
        """Host is like 'a:1'."""
        self.mock_down_hosts.remove(host)

    def set_wire_version_range(self, host, min_version, max_version):
        self.mock_wire_versions[host] = (min_version, max_version)

    def set_max_write_batch_size(self, host, size):
        self.mock_max_write_batch_sizes[host] = size

    def mock_is_master(self, host):
        min_wire_version, max_wire_version = self.mock_wire_versions.get(
            host,
            (common.MIN_WIRE_VERSION, common.MAX_WIRE_VERSION))

        max_write_batch_size = self.mock_max_write_batch_sizes.get(
            host, common.MAX_WRITE_BATCH_SIZE)

        # host is like 'a:1'.
        if host in self.mock_down_hosts:
            raise socket.timeout('mock timeout')

        if host in self.mock_standalones:
            return {
                'ok': 1,
                'ismaster': True,
                'minWireVersion': min_wire_version,
                'maxWireVersion': max_wire_version,
                'maxWriteBatchSize': max_write_batch_size}

        if host in self.mock_members:
            ismaster = (host == self.mock_primary)

            # Simulate a replica set member.
            response = {
                'ok': 1,
                'ismaster': ismaster,
                'secondary': not ismaster,
                'setName': 'rs',
                'hosts': self.mock_ismaster_hosts,
                'minWireVersion': min_wire_version,
                'maxWireVersion': max_wire_version,
                'maxWriteBatchSize': max_write_batch_size}

            if self.mock_primary:
                response['primary'] = self.mock_primary

            return response

        if host in self.mock_mongoses:
            return {
                'ok': 1,
                'ismaster': True,
                'minWireVersion': min_wire_version,
                'maxWireVersion': max_wire_version,
                'msg': 'isdbgrid',
                'maxWriteBatchSize': max_write_batch_size}

        # In test_internal_ips(), we try to connect to a host listed
        # in ismaster['hosts'] but not publicly accessible.
        raise socket.error('Unknown host: %s' % host)
