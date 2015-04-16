# Copyright 2014-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Internal class to monitor a topology of one or more servers."""

import random
import threading

from bson.py3compat import itervalues
from pymongo import common
from pymongo.pool import PoolOptions
from pymongo.topology_description import (updated_topology_description,
                                          TOPOLOGY_TYPE,
                                          TopologyDescription)
from pymongo.errors import ServerSelectionTimeoutError, InvalidOperation
from pymongo.monotonic import time as _time
from pymongo.server import Server
from pymongo.server_selectors import (any_server_selector,
                                      apply_local_threshold,
                                      arbiter_server_selector,
                                      secondary_server_selector,
                                      writable_server_selector)


class Topology(object):
    """Monitor a topology of one or more servers."""
    def __init__(self, topology_settings):
        self._settings = topology_settings
        topology_description = TopologyDescription(
            topology_settings.get_topology_type(),
            topology_settings.get_server_descriptions(),
            topology_settings.replica_set_name)

        self._description = topology_description
        # Store the seed list to help diagnose errors in _error_message().
        self._seed_addresses = list(topology_description.server_descriptions())
        self._opened = False
        self._lock = threading.Lock()
        self._condition = self._settings.condition_class(self._lock)
        self._servers = {}

    def open(self):
        """Start monitoring, or restart after a fork.

        No effect if called multiple times.
        """
        with self._lock:
            self._ensure_opened()

    def select_servers(self,
                       selector,
                       server_selection_timeout=None,
                       address=None):
        """Return a list of Servers matching selector, or time out.

        :Parameters:
          - `selector`: function that takes a list of Servers and returns
            a subset of them.
          - `server_selection_timeout` (optional): maximum seconds to wait.
            If not provided, the default value common.SERVER_SELECTION_TIMEOUT
            is used.
          - `address`: optional server address to select.

        Calls self.open() if needed.

        Raises exc:`ServerSelectionTimeoutError` after
        `server_selection_timeout` if no matching servers are found.
        """
        if server_selection_timeout is None:
            server_timeout = self._settings.server_selection_timeout
        else:
            server_timeout = server_selection_timeout

        with self._lock:
            self._description.check_compatible()

            now = _time()
            end_time = now + server_timeout
            server_descriptions = self._apply_selector(selector, address)

            while not server_descriptions:
                # No suitable servers.
                if server_timeout == 0 or now > end_time:
                    raise ServerSelectionTimeoutError(
                        self._error_message(selector))

                self._ensure_opened()
                self._request_check_all()

                # Release the lock and wait for the topology description to
                # change, or for a timeout. We won't miss any changes that
                # came after our most recent _apply_selector call, since we've
                # held the lock until now.
                self._condition.wait(common.MIN_HEARTBEAT_INTERVAL)
                self._description.check_compatible()
                now = _time()
                server_descriptions = self._apply_selector(selector, address)

            return [self.get_server_by_address(sd.address)
                    for sd in server_descriptions]

    def select_server(self,
                      selector,
                      server_selection_timeout=None,
                      address=None):
        """Like select_servers, but choose a random server if several match."""
        return random.choice(self.select_servers(selector,
                                                 server_selection_timeout,
                                                 address))

    def select_server_by_address(self, address,
                                 server_selection_timeout=None):
        """Return a Server for "address", reconnecting if necessary.

        If the server's type is not known, request an immediate check of all
        servers. Time out after "server_selection_timeout" if the server
        cannot be reached.

        :Parameters:
          - `address`: A (host, port) pair.
          - `server_selection_timeout` (optional): maximum seconds to wait.
            If not provided, the default value
            common.SERVER_SELECTION_TIMEOUT is used.

        Calls self.open() if needed.

        Raises exc:`ServerSelectionTimeoutError` after
        `server_selection_timeout` if no matching servers are found.
        """
        return self.select_server(any_server_selector,
                                  server_selection_timeout,
                                  address)

    def on_change(self, server_description):
        """Process a new ServerDescription after an ismaster call completes."""
        # We do no I/O holding the lock.
        with self._lock:
            # Any monitored server was definitely in the topology description
            # once. Check if it's still in the description or if some state-
            # change removed it. E.g., we got a host list from the primary
            # that didn't include this server.
            if self._description.has_server(server_description.address):
                self._description = updated_topology_description(
                    self._description, server_description)

                self._update_servers()

                # Wake waiters in select_servers().
                self._condition.notify_all()

    def get_server_by_address(self, address):
        """Get a Server or None.

        Returns the current version of the server immediately, even if it's
        Unknown or absent from the topology. Only use this in unittests.
        In driver code, use select_server_by_address, since then you're
        assured a recent view of the server's type and wire protocol version.
        """
        return self._servers.get(address)

    def has_server(self, address):
        return address in self._servers

    def get_primary(self):
        """Return primary's address or None."""
        # Implemented here in Topology instead of MongoClient, so it can lock.
        with self._lock:
            topology_type = self._description.topology_type
            if topology_type != TOPOLOGY_TYPE.ReplicaSetWithPrimary:
                return None

            description = writable_server_selector(
                self._description.known_servers)[0]

            return description.address

    def _get_replica_set_members(self, selector):
        """Return set of replica set member addresses."""
        # Implemented here in Topology instead of MongoClient, so it can lock.
        with self._lock:
            topology_type = self._description.topology_type
            if topology_type not in (TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                                     TOPOLOGY_TYPE.ReplicaSetNoPrimary):
                return set()

            descriptions = selector(self._description.known_servers)
            return set([d.address for d in descriptions])

    def get_direct_or_primary(self):
        """Return the address of a connected primary or standalone, or None.

        Raise InvalidOperation for Sharded topologies.
        """
        # Implemented here in Topology instead of MongoClient, so it can lock.
        with self._lock:
            topology_type = self._description.topology_type
            if topology_type == TOPOLOGY_TYPE.Sharded:
                raise InvalidOperation()
            if topology_type not in (TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                                     TOPOLOGY_TYPE.Single):
                return None
            descriptions = writable_server_selector(
                self._description.known_servers)
            return descriptions[0].address if descriptions else None

    def get_secondaries(self):
        """Return set of secondary addresses."""
        return self._get_replica_set_members(secondary_server_selector)

    def get_arbiters(self):
        """Return set of arbiter addresses."""
        return self._get_replica_set_members(arbiter_server_selector)

    def request_check_all(self, wait_time=5):
        """Wake all monitors, wait for at least one to check its server."""
        with self._lock:
            self._request_check_all()
            self._condition.wait(wait_time)

    def reset_pool(self, address):
        with self._lock:
            server = self._servers.get(address)
            if server:
                server.pool.reset()

    def reset_server(self, address):
        """Clear our pool for a server and mark it Unknown.

        Do *not* request an immediate check.
        """
        with self._lock:
            self._reset_server(address)

    def reset_server_and_request_check(self, address):
        """Clear our pool for a server, mark it Unknown, and check it soon."""
        with self._lock:
            self._reset_server(address)
            self._request_check(address)

    def close(self):
        """Clear pools and terminate monitors. Topology reopens on demand."""
        with self._lock:
            for server in self._servers.values():
                server.close()

            # Mark all servers Unknown.
            self._description = self._description.reset()
            self._update_servers()

    @property
    def description(self):
        return self._description

    def _ensure_opened(self):
        """Start monitors, or restart after a fork.

        Hold the lock when calling this.
        """
        if not self._opened:
            self._opened = True
            self._update_servers()
        else:
            # Restart monitors if we forked since previous call.
            for server in itervalues(self._servers):
                server.open()

    def _reset_server(self, address):
        """Clear our pool for a server and mark it Unknown.

        Hold the lock when calling this. Does *not* request an immediate check.
        """
        server = self._servers.get(address)

        # "server" is None if another thread removed it from the topology.
        if server:
            server.reset()

            # Mark this server Unknown.
            self._description = self._description.reset_server(address)
            self._update_servers()

    def _request_check(self, address):
        """Wake one monitor. Hold the lock when calling this."""
        server = self._servers.get(address)

        # "server" is None if another thread removed it from the topology.
        if server:
            server.request_check()

    def _request_check_all(self):
        """Wake all monitors. Hold the lock when calling this."""
        for server in self._servers.values():
            server.request_check()

    def _apply_selector(self, selector, address):
        if self._description.topology_type == TOPOLOGY_TYPE.Single:
            # Ignore the selector.
            return self._description.known_servers
        elif address:
            sd = self._description.server_descriptions().get(address)
            return [sd] if sd else []
        elif self._description.topology_type == TOPOLOGY_TYPE.Sharded:
            return apply_local_threshold(self._settings.local_threshold_ms,
                                         self._description.known_servers)
        else:
            sds = selector(self._description.known_servers)
            return apply_local_threshold(
                self._settings.local_threshold_ms, sds)

    def _update_servers(self):
        """Sync our Servers from TopologyDescription.server_descriptions.

        Hold the lock while calling this.
        """
        for address, sd in self._description.server_descriptions().items():
            if address not in self._servers:
                monitor = self._settings.monitor_class(
                    server_description=sd,
                    topology=self,
                    pool=self._create_pool_for_monitor(address),
                    topology_settings=self._settings)

                server = Server(
                    server_description=sd,
                    pool=self._create_pool_for_server(address),
                    monitor=monitor)

                self._servers[address] = server
                server.open()
            else:
                self._servers[address].description = sd

        for address, server in list(self._servers.items()):
            if not self._description.has_server(address):
                server.close()
                self._servers.pop(address)

    def _create_pool_for_server(self, address):
        return self._settings.pool_class(address, self._settings.pool_options)

    def _create_pool_for_monitor(self, address):
        options = self._settings.pool_options

        # According to the Server Discovery And Monitoring Spec, monitors use
        # connect_timeout for both connect_timeout and socket_timeout. The
        # pool only has one socket so maxPoolSize and so on aren't needed.
        monitor_pool_options = PoolOptions(
            connect_timeout=options.connect_timeout,
            socket_timeout=options.connect_timeout,
            ssl_context=options.ssl_context,
            ssl_match_hostname=options.ssl_match_hostname,
            socket_keepalive=True)

        return self._settings.pool_class(address, monitor_pool_options,
                                         handshake=False)

    def _error_message(self, selector):
        """Format an error message if server selection fails.

        Hold the lock when calling this.
        """
        is_replica_set = self._description.topology_type in (
            TOPOLOGY_TYPE.ReplicaSetWithPrimary,
            TOPOLOGY_TYPE.ReplicaSetNoPrimary)

        if is_replica_set:
            server_plural = 'replica set members'
        elif self._description.topology_type == TOPOLOGY_TYPE.Sharded:
            server_plural = 'mongoses'
        else:
            server_plural = 'servers'

        if self._description.known_servers:
            # We've connected, but no servers match the selector.
            if selector is writable_server_selector:
                if is_replica_set:
                    return 'No primary available for writes'
                else:
                    return 'No %s available for writes' % server_plural
            else:
                return 'No %s match selector "%s"' % (server_plural, selector)
        else:
            addresses = list(self._description.server_descriptions())
            servers = list(self._description.server_descriptions().values())
            if not servers:
                if is_replica_set:
                    # We removed all servers because of the wrong setName?
                    return 'No %s available for replica set name "%s"' % (
                        server_plural, self._settings.replica_set_name)
                else:
                    return 'No %s available' % server_plural

            # 1 or more servers, all Unknown. Are they unknown for one reason?
            error = servers[0].error
            same = all(server.error == error for server in servers[1:])
            if same:
                if error is None:
                    # We're still discovering.
                    return 'No %s found yet' % server_plural

                if (is_replica_set and not
                        set(addresses).intersection(self._seed_addresses)):
                    # We replaced our seeds with new hosts but can't reach any.
                    return (
                        'Could not reach any servers in %s. Replica set is'
                        ' configured with internal hostnames or IPs?' %
                        addresses)

                return str(error)
            else:
                return ','.join(str(server.error) for server in servers
                                if server.error)
