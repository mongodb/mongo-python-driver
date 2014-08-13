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

import random
import threading
import time

from pymongo import common
from pymongo.cluster_description import (updated_cluster_description,
                                         CLUSTER_TYPE,
                                         ClusterDescription)
from pymongo.errors import AutoReconnect
from pymongo.server import Server
from pymongo.server_selectors import (secondary_server_selector,
                                      writable_server_selector)


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
        """Start monitoring.

        No effect if called multiple times.
        """
        with self._lock:
            self._ensure_opened()

    def select_servers(self, selector,
                       server_wait_time=common.SERVER_WAIT_TIME):
        """Return a list of Servers matching selector, or time out.

        :Parameters:
          - `selector`: function that takes a list of Servers and returns
            a subset of them.
          - `server_wait_time` (optional): maximum seconds to wait. If not
            provided, the default value common.SERVER_WAIT_TIME is used.

        Raises exc:`AutoReconnect` after `server_wait_time` if no
        matching servers are found.
        """
        with self._lock:
            self._description.check_compatible()

            # TODO: use monotonic time if available.
            now = time.time()
            end_time = now + server_wait_time
            server_descriptions = self._apply_selector(selector)

            while not server_descriptions:
                # No suitable servers.
                if server_wait_time == 0 or now > end_time:
                    # TODO: more error diagnostics. E.g., if state is
                    # ReplicaSet but every server is Unknown, and the host list
                    # is non-empty, and doesn't intersect with settings.seeds,
                    # the set is probably configured with internal hostnames or
                    # IPs and we're connecting from outside. Or if state is
                    # ReplicaSet and clusterDescription.server_descriptions is
                    # empty, we have the wrong set_name. Include
                    # ClusterDescription's stringification in exception msg.
                    raise AutoReconnect("No suitable servers available")

                self._ensure_opened()
                self._request_check_all()

                # Release the lock and wait for the cluster description to
                # change, or for a timeout. We won't miss any changes that
                # came after our most recent selector() call, since we've
                # held the lock until now.
                self._condition.wait(common.MIN_HEARTBEAT_INTERVAL)
                self._description.check_compatible()
                now = time.time()
                server_descriptions = self._apply_selector(selector)

            return [self.get_server_by_address(sd.address)
                    for sd in server_descriptions]

    def select_server(self, selector, server_wait_time=common.SERVER_WAIT_TIME):
        """Like select_servers, but choose a random server if several match."""
        return random.choice(self.select_servers(selector, server_wait_time))

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

    def get_primary(self):
        """Return primary's address or None."""
        # Implemented here in Cluster instead of MongoClient, so it can lock.
        with self._lock:
            cluster_type = self._description.cluster_type
            if cluster_type != CLUSTER_TYPE.ReplicaSetWithPrimary:
                return None

            description = writable_server_selector(
                self._description.known_servers)[0]

            return description.address

    def get_secondaries(self):
        """Return set of secondary addresses."""
        # Implemented here in Cluster instead of MongoClient, so it can lock.
        with self._lock:
            cluster_type = self._description.cluster_type
            if cluster_type not in (CLUSTER_TYPE.ReplicaSetWithPrimary,
                                    CLUSTER_TYPE.ReplicaSetNoPrimary):
                return []

            descriptions = secondary_server_selector(
                self._description.known_servers)

            return set([d.address for d in descriptions])

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

    def reset_server(self, address):
        with self._lock:
            server = self._servers.get(address)
            if server:
                server.pool.reset()

            # Mark this server Unknown.
            self._description = self._description.reset_server(address)
            self._update_servers()

    def reset(self):
        """Reset all pools and disconnect from all servers.

        The cluster reconnects on demand, or after common.HEARTBEAT_FREQUENCY
        seconds.
        """
        with self._lock:
            for server in self._servers.values():
                server.pool.reset()

            # Mark all servers Unknown.
            self._description = self._description.reset()
            self._update_servers()

    @property
    def description(self):
        return self._description

    def _ensure_opened(self):
        """Start monitors. Hold the lock when calling this."""
        if not self._opened:
            self._opened = True
            self._update_servers()

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
        return self._settings.pool_class(address, self._settings.pool_options)
