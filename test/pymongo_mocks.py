# Copyright 2013-2015 MongoDB, Inc.
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

from pymongo import common
from pymongo import MongoClient, MongoReplicaSetClient
from pymongo.pool import Pool

from test import host as default_host, port as default_port
from test.utils import my_partial


class MockPool(Pool):
    def __init__(self, client, pair, *args, **kwargs):
        # MockPool gets a 'client' arg, regular pools don't.
        self.client = client
        self.mock_host, self.mock_port = pair

        # Actually connect to the default server.
        Pool.__init__(
            self,
            pair=(default_host, default_port),
            max_size=None,
            net_timeout=None,
            conn_timeout=20,
            use_ssl=False,
            use_greenlets=False)

    def get_socket(self, force=False):
        client = self.client
        host_and_port = '%s:%s' % (self.mock_host, self.mock_port)
        if host_and_port in client.mock_down_hosts:
            raise socket.error('mock error')

        assert host_and_port in (
            client.mock_standalones
            + client.mock_members
            + client.mock_mongoses), "bad host: %s" % host_and_port

        sock_info = Pool.get_socket(self, force)
        sock_info.mock_host = self.mock_host
        sock_info.mock_port = self.mock_port
        return sock_info


class MockClientBase(object):
    def __init__(self, standalones, members, mongoses, config):
        """standalones, etc., are like ['a:1', 'b:2']"""
        self.mock_standalones = standalones[:]
        self.mock_members = members[:]

        if self.mock_members:
            self.mock_primary = self.mock_members[0]
        else:
            self.mock_primary = None

        if config is not None:
            self.mock_ismaster_hosts = config
        else:
            self.mock_ismaster_hosts = members[:]

        self.mock_mongoses = mongoses[:]

        # Hosts that should raise socket errors.
        self.mock_down_hosts = []

        # Hostname -> (min wire version, max wire version)
        self.mock_wire_versions = {}

        # Hostname -> max write batch size
        self.mock_max_write_batch_sizes = {}

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
                'ismaster': True,
                'minWireVersion': min_wire_version,
                'maxWireVersion': max_wire_version,
                'maxWriteBatchSize': max_write_batch_size}

        if host in self.mock_members:
            ismaster = (host == self.mock_primary)

            # Simulate a replica set member.
            response = {
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
                'ismaster': True,
                'minWireVersion': min_wire_version,
                'maxWireVersion': max_wire_version,
                'msg': 'isdbgrid',
                'maxWriteBatchSize': max_write_batch_size}

        # In test_internal_ips(), we try to connect to a host listed
        # in ismaster['hosts'] but not publicly accessible.
        raise socket.error('Unknown host: %s' % host)

    def simple_command(self, sock_info, dbname, spec):
        # __simple_command is also used for authentication, but in this
        # test it's only used for ismaster.
        assert spec == {'ismaster': 1}
        response = self.mock_is_master(
            '%s:%s' % (sock_info.mock_host, sock_info.mock_port))

        ping_time = 10
        return response, ping_time


class MockClient(MockClientBase, MongoClient):
    def __init__(
        self, standalones, members, mongoses, ismaster_hosts=None,
        *args, **kwargs
    ):
        MockClientBase.__init__(
            self, standalones, members, mongoses, ismaster_hosts)

        kwargs['_pool_class'] = my_partial(MockPool, self)
        MongoClient.__init__(self, *args, **kwargs)

    def _MongoClient__simple_command(self, sock_info, dbname, spec):
        return self.simple_command(sock_info, dbname, spec)


class MockReplicaSetClient(MockClientBase, MongoReplicaSetClient):
    def __init__(
        self, standalones, members, mongoses, ismaster_hosts=None,
        *args, **kwargs
    ):
        MockClientBase.__init__(
            self, standalones, members, mongoses, ismaster_hosts)

        kwargs['_pool_class'] = my_partial(MockPool, self)
        MongoReplicaSetClient.__init__(self, *args, **kwargs)

    def _MongoReplicaSetClient__is_master(self, host):
        response = self.mock_is_master('%s:%s' % host)
        connection_pool = MockPool(self, host)
        ping_time = 10
        return response, connection_pool, ping_time

    def _MongoReplicaSetClient__simple_command(self, sock_info, dbname, spec):
        return self.simple_command(sock_info, dbname, spec)
