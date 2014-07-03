# Copyright 2009-2014 MongoDB, Inc.
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

"""Internal classes to monitor clusters of one or more servers."""

import threading
import time
from pymongo import common

from pymongo.cluster_description import (updated_cluster_description,
                                         CLUSTER_TYPE,
                                         ClusterDescription)
from pymongo.errors import InvalidOperation, ConnectionFailure
from pymongo.pool import PoolOptions
from pymongo.server import Server


class Cluster(object):
    """Monitor a cluster of one or more servers."""
    def __init__(self, cluster_settings):
        self._settings = cluster_settings
        cluster_description = ClusterDescription(
            cluster_settings.get_cluster_type(),
            cluster_settings.get_server_descriptions(),
            cluster_settings.set_name)

        self._description = cluster_description
        self._opened = False
        self._lock = threading.Lock()
        self._condition = self._settings.condition_class(self._lock)
        self._servers = {}

    def open(self):
        """Start monitoring."""
        with self._lock:
            if self._opened:
                raise InvalidOperation('Cluster already opened')

            self._opened = True
            self._update_servers()

    def select_servers(self, selector, server_wait_time=5):
        """Return a list of Servers matching selector, or time out.

        :Parameters:
          - `selector`: function that takes a list of Servers and returns
            a subset of them.
          - `server_wait_time` (optional): maximum seconds to wait.

        Raises exc:`ConnectionFailure` after `server_wait_time` if no
        matching servers are found.
        """
        with self._lock:
            self._description.check_compatible()

            # TODO: use settings.server_wait_time.
            # TODO: use monotonic time if available.
            now = time.time()
            end_time = now + server_wait_time
            server_descriptions = self._apply_selector(selector)

            while not server_descriptions:
                # No suitable servers.
                if now > end_time:
                    # TODO: more error diagnostics. E.g., if state is
                    # ReplicaSet but every server is Unknown, and the host list
                    # is non-empty, and doesn't intersect with settings.seeds,
                    # the set is probably configured with internal hostnames or
                    # IPs and we're connecting from outside. Or if state is
                    # ReplicaSet and clusterDescription.server_descriptions is
                    # empty, we have the wrong set_name. Include
                    # ClusterDescription's stringification in exception msg.
                    raise ConnectionFailure("No suitable servers available")

                self._request_check_all()

                # Release the lock and wait for the cluster description to
                # change, or for a timeout. We won't miss any changes that
                # came after our most recent selector() call, since we've
                # held the lock until now.
                self._condition.wait(common.MIN_HEARTBEAT_INTERVAL)
                now = time.time()
                server_descriptions = self._apply_selector(selector)

            return [self.get_server_by_address(sd.address)
                    for sd in server_descriptions]

    def on_change(self, server_description):
        """Process a new ServerDescription after an ismaster call completes."""
        # We do no I/O holding the lock.
        with self._lock:
            # Any monitored server was definitely in the cluster description
            # once. Check if it's still in the description or if some state-
            # change removed it. E.g., we got a host list from the primary
            # that didn't include this server.
            if self._description.has_server(server_description.address):
                self._description = updated_cluster_description(
                    self._description, server_description)

                self._update_servers()

                # Wake waiters in select_servers().
                self._condition.notify_all()

    def get_server_by_address(self, address):
        """Get a Server or None."""
        return self._servers.get(address)

    def has_server(self, address):
        return address in self._servers

    def close(self):
        # TODO.
        raise NotImplementedError

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

    @property
    def description(self):
        return self._description

    def _request_check_all(self):
        """Wake all monitors. Hold the lock when calling this."""
        for server in self._servers.values():
            server.request_check()

    def _apply_selector(self, selector):
        if self._description.cluster_type == CLUSTER_TYPE.Single:
            # Ignore the selector.
            return self._description.known_servers
        else:
            return selector(self._description.known_servers)

    def _update_servers(self):
        """Sync our set of Servers from ClusterDescription.server_descriptions.

        Hold the lock while calling this.
        """
        for address, sd in self._description.server_descriptions().items():
            if address not in self._servers:
                m = self._settings.monitor_class(
                    sd, self, self._create_pool(address), self._settings)

                s = Server(sd, self._create_pool(address), m)
                self._servers[address] = s
                s.open()
            else:
                self._servers[address].description = sd

        for address, server in list(self._servers.items()):
            if not self._description.has_server(address):
                server.close()
                self._servers.pop(address)

    def _create_pool(self, address):
        # TODO: Need PoolSettings, SocketSettings, and SSLContext classes.
        return self._settings.pool_class(
            address,
            PoolOptions(
                max_pool_size=100,
                connect_timeout=20)
            )
