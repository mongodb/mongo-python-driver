# Copyright 2013 MongoDB, Inc.
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

"""Test MongoReplicaSetClients and replica set configuration changes."""

import socket
import sys
import unittest

sys.path[0:0] = [""]

from pymongo import MongoClient
from pymongo.errors import AutoReconnect
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.pool import Pool
from test import host as default_host, port as default_port


class MockPool(Pool):
    def __init__(self, pair, *args, **kwargs):
        if pair:
            # RS client passes 'pair' to Pool's constructor.
            self.mock_host, self.mock_port = pair
        else:
            # MongoClient passes pair to get_socket() instead.
            self.mock_host, self.mock_port = None, None

        Pool.__init__(
            self,
            pair=(default_host, default_port),
            max_size=None,
            net_timeout=None,
            conn_timeout=20,
            use_ssl=False,
            use_greenlets=False)

    def get_socket(self, pair=None, force=False):
        sock_info = Pool.get_socket(self, (default_host, default_port), force)
        sock_info.host = self.mock_host or pair[0]
        return sock_info


MOCK_HOSTS = ['a:27017', 'b:27017', 'c:27017']
MOCK_PRIMARY = MOCK_HOSTS[0]
MOCK_RS_NAME = 'rs'


class MockClientBase(object):
    def __init__(self):
        self.mock_hosts = MOCK_HOSTS

        # Hosts that should raise socket errors.
        self.mock_down_hosts = []

        # Hosts that should respond to ismaster as if they're standalone.
        self.mock_standalone_hosts = []

    def mock_is_master(self, host):
        if host in self.mock_down_hosts:
            raise socket.timeout('timed out')

        if host in self.mock_standalone_hosts:
            return {'ismaster': True}

        if host not in self.mock_hosts:
            # Host removed from set by a reconfig.
            return {'ismaster': False, 'secondary': False}

        ismaster = host == MOCK_PRIMARY

        # Simulate a replica set member.
        return {
            'ismaster': ismaster,
            'secondary': not ismaster,
            'setName': MOCK_RS_NAME,
            'hosts': self.mock_hosts}

    def simple_command(self, sock_info, dbname, spec):
        # __simple_command is also used for authentication, but in this
        # test it's only used for ismaster.
        assert spec == {'ismaster': 1}
        response = self.mock_is_master('%s:%s' % (sock_info.host, 27017))
        ping_time = 10
        return response, ping_time


class MockClient(MockClientBase, MongoClient):
    def __init__(self, hosts):
        MockClientBase.__init__(self)
        MongoClient.__init__(
            self,
            hosts,
            replicaSet=MOCK_RS_NAME,
            _pool_class=MockPool)

    def _MongoClient__simple_command(self, sock_info, dbname, spec):
        return self.simple_command(sock_info, dbname, spec)


class MockReplicaSetClient(MockClientBase, MongoReplicaSetClient):
    def __init__(self, hosts):
        MockClientBase.__init__(self)
        MongoReplicaSetClient.__init__(
            self,
            hosts,
            replicaSet=MOCK_RS_NAME)

    def _MongoReplicaSetClient__is_master(self, host):
        response = self.mock_is_master('%s:%s' % host)
        connection_pool = MockPool(host)
        ping_time = 10
        return response, connection_pool, ping_time

    def _MongoReplicaSetClient__simple_command(self, sock_info, dbname, spec):
        return self.simple_command(sock_info, dbname, spec)


class TestSecondaryBecomesStandalone(unittest.TestCase):
    # An administrator removes a secondary from a 3-node set and
    # brings it back up as standalone, without updating the other
    # members' config. Verify we don't continue using it.
    def test_client(self):
        c = MockClient(','.join(MOCK_HOSTS))

        # MongoClient connects to primary by default.
        self.assertEqual('a', c.host)
        self.assertEqual(27017, c.port)

        # C is brought up as a standalone.
        c.mock_standalone_hosts.append('c:27017')

        # Fail over.
        c.mock_down_hosts = ['a:27017', 'b:27017']

        # Force reconnect.
        c.disconnect()

        try:
            c.db.collection.find_one()
        except AutoReconnect, e:
            self.assertTrue('not a member of replica set' in str(e))
        else:
            self.fail("MongoClient didn't raise AutoReconnect")

        self.assertEqual(None, c.host)
        self.assertEqual(None, c.port)

    def test_replica_set_client(self):
        c = MockReplicaSetClient(','.join(MOCK_HOSTS))
        self.assertTrue(('c', 27017) in c.secondaries)

        # C is brought up as a standalone.
        c.mock_standalone_hosts.append('c:27017')
        c.refresh()

        self.assertEqual(('a', 27017), c.primary)
        self.assertEqual(set([('b', 27017)]), c.secondaries)


class TestSecondaryRemoved(unittest.TestCase):
    # An administrator removes a secondary from a 3-node set *without*
    # restarting it as standalone.
    def test_replica_set_client(self):
        c = MockReplicaSetClient(','.join(MOCK_HOSTS))
        self.assertTrue(('c', 27017) in c.secondaries)

        # C is removed.
        c.mock_hosts.remove('c:27017')
        c.refresh()

        self.assertEqual(('a', 27017), c.primary)
        self.assertEqual(set([('b', 27017)]), c.secondaries)


if __name__ == "__main__":
    unittest.main()
