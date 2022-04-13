# Copyright 2013-present MongoDB, Inc.
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

import contextlib
import weakref
from functools import partial
from test import client_context

from pymongo import MongoClient, common
from pymongo.errors import AutoReconnect, NetworkTimeout
from pymongo.hello import Hello, HelloCompat
from pymongo.monitor import Monitor
from pymongo.pool import Pool
from pymongo.server_description import ServerDescription


class MockPool(Pool):
    def __init__(self, client, pair, *args, **kwargs):
        # MockPool gets a 'client' arg, regular pools don't. Weakref it to
        # avoid cycle with __del__, causing ResourceWarnings in Python 3.3.
        self.client = weakref.proxy(client)
        self.mock_host, self.mock_port = pair

        # Actually connect to the default server.
        Pool.__init__(self, (client_context.host, client_context.port), *args, **kwargs)

    @contextlib.contextmanager
    def get_socket(self, handler=None):
        client = self.client
        host_and_port = "%s:%s" % (self.mock_host, self.mock_port)
        if host_and_port in client.mock_down_hosts:
            raise AutoReconnect("mock error")

        assert host_and_port in (
            client.mock_standalones + client.mock_members + client.mock_mongoses
        ), ("bad host: %s" % host_and_port)

        with Pool.get_socket(self, handler) as sock_info:
            sock_info.mock_host = self.mock_host
            sock_info.mock_port = self.mock_port
            yield sock_info


class DummyMonitor(object):
    def __init__(self, server_description, topology, pool, topology_settings):
        self._server_description = server_description
        self.opened = False

    def cancel_check(self):
        pass

    def join(self):
        pass

    def open(self):
        self.opened = True

    def request_check(self):
        pass

    def close(self):
        self.opened = False


class MockMonitor(Monitor):
    def __init__(self, client, server_description, topology, pool, topology_settings):
        # MockMonitor gets a 'client' arg, regular monitors don't. Weakref it
        # to avoid cycles.
        self.client = weakref.proxy(client)
        Monitor.__init__(self, server_description, topology, pool, topology_settings)

    def _check_once(self):
        client = self.client
        address = self._server_description.address
        response, rtt = client.mock_hello("%s:%d" % address)
        return ServerDescription(address, Hello(response), rtt)


class MockClient(MongoClient):
    def __init__(
        self,
        standalones,
        members,
        mongoses,
        hello_hosts=None,
        arbiters=None,
        down_hosts=None,
        *args,
        **kwargs
    ):
        """A MongoClient connected to the default server, with a mock topology.

        standalones, members, mongoses, arbiters, and down_hosts determine the
        configuration of the topology. They are formatted like ['a:1', 'b:2'].
        hello_hosts provides an alternative host list for the server's
        mocked hello response; see test_connect_with_internal_ips.
        """
        self.mock_standalones = standalones[:]
        self.mock_members = members[:]

        if self.mock_members:
            self.mock_primary = self.mock_members[0]
        else:
            self.mock_primary = None

        # Hosts that should be considered an arbiter.
        self.mock_arbiters = arbiters[:] if arbiters else []

        if hello_hosts is not None:
            self.mock_hello_hosts = hello_hosts
        else:
            self.mock_hello_hosts = members[:]

        self.mock_mongoses = mongoses[:]

        # Hosts that should raise socket errors.
        self.mock_down_hosts = down_hosts[:] if down_hosts else []

        # Hostname -> (min wire version, max wire version)
        self.mock_wire_versions = {}

        # Hostname -> max write batch size
        self.mock_max_write_batch_sizes = {}

        # Hostname -> round trip time
        self.mock_rtts = {}

        kwargs["_pool_class"] = partial(MockPool, self)
        kwargs["_monitor_class"] = partial(MockMonitor, self)

        client_options = client_context.default_client_options.copy()
        client_options.update(kwargs)

        super(MockClient, self).__init__(*args, **client_options)

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

    def mock_hello(self, host):
        """Return mock hello response (a dict) and round trip time."""
        if host in self.mock_wire_versions:
            min_wire_version, max_wire_version = self.mock_wire_versions[host]
        else:
            min_wire_version = common.MIN_SUPPORTED_WIRE_VERSION
            max_wire_version = common.MAX_SUPPORTED_WIRE_VERSION

        max_write_batch_size = self.mock_max_write_batch_sizes.get(
            host, common.MAX_WRITE_BATCH_SIZE
        )

        rtt = self.mock_rtts.get(host, 0)

        # host is like 'a:1'.
        if host in self.mock_down_hosts:
            raise NetworkTimeout("mock timeout")

        elif host in self.mock_standalones:
            response = {
                "ok": 1,
                HelloCompat.LEGACY_CMD: True,
                "minWireVersion": min_wire_version,
                "maxWireVersion": max_wire_version,
                "maxWriteBatchSize": max_write_batch_size,
            }
        elif host in self.mock_members:
            primary = host == self.mock_primary

            # Simulate a replica set member.
            response = {
                "ok": 1,
                HelloCompat.LEGACY_CMD: primary,
                "secondary": not primary,
                "setName": "rs",
                "hosts": self.mock_hello_hosts,
                "minWireVersion": min_wire_version,
                "maxWireVersion": max_wire_version,
                "maxWriteBatchSize": max_write_batch_size,
            }

            if self.mock_primary:
                response["primary"] = self.mock_primary

            if host in self.mock_arbiters:
                response["arbiterOnly"] = True
                response["secondary"] = False
        elif host in self.mock_mongoses:
            response = {
                "ok": 1,
                HelloCompat.LEGACY_CMD: True,
                "minWireVersion": min_wire_version,
                "maxWireVersion": max_wire_version,
                "msg": "isdbgrid",
                "maxWriteBatchSize": max_write_batch_size,
            }
        else:
            # In test_internal_ips(), we try to connect to a host listed
            # in hello['hosts'] but not publicly accessible.
            raise AutoReconnect("Unknown host: %s" % host)

        return response, rtt

    def _process_periodic_tasks(self):
        # Avoid the background thread causing races, e.g. a surprising
        # reconnect while we're trying to test a disconnected client.
        pass
