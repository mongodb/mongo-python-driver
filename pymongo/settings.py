# Copyright 2014 MongoDB, Inc.
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

"""Represent MongoClient's configuration."""

import threading

from pymongo import monitor, pool
from pymongo.common import SERVER_SELECTION_TIMEOUT
from pymongo.topology_description import TOPOLOGY_TYPE
from pymongo.monotonic import time as _time
from pymongo.pool import PoolOptions
from pymongo.server_description import ServerDescription


class TopologySettings(object):
    def __init__(
        self,
        seeds=None,
        replica_set_name=None,
        pool_class=None,
        pool_options=None,
        monitor_class=None,
        condition_class=None,
        local_threshold_ms=15,
        server_selection_timeout=SERVER_SELECTION_TIMEOUT,
        timer=None,
    ):
        """Represent MongoClient's configuration.

        Take a list of (host, port) pairs and optional replica set name.
        """
        self._seeds = seeds or [('localhost', 27017)]
        self._replica_set_name = replica_set_name
        self._pool_class = pool_class or pool.Pool
        self._pool_options = pool_options or PoolOptions()
        self._monitor_class = monitor_class or monitor.Monitor
        self._condition_class = condition_class or threading.Condition
        self._local_threshold_ms = local_threshold_ms
        self._server_selection_timeout = server_selection_timeout
        self._timer = timer or _time
        self._direct = (len(self._seeds) == 1 and not replica_set_name)

    @property
    def seeds(self):
        """List of server addresses."""
        return self._seeds

    @property
    def replica_set_name(self):
        return self._replica_set_name

    @property
    def pool_class(self):
        return self._pool_class

    @property
    def pool_options(self):
        return self._pool_options

    @property
    def monitor_class(self):
        return self._monitor_class

    @property
    def condition_class(self):
        return self._condition_class

    @property
    def local_threshold_ms(self):
        return self._local_threshold_ms

    @property
    def server_selection_timeout(self):
        return self._server_selection_timeout

    @property
    def timer(self):
        """A function like time.time()."""
        return self._timer

    @property
    def direct(self):
        """Connect directly to a single server, or use a set of servers?

        True if there is one seed and no replica_set_name.
        """
        return self._direct

    def get_topology_type(self):
        if self.direct:
            return TOPOLOGY_TYPE.Single
        elif self.replica_set_name is not None:
            return TOPOLOGY_TYPE.ReplicaSetNoPrimary
        else:
            return TOPOLOGY_TYPE.Unknown

    def get_server_descriptions(self):
        """Initial dict of (address, ServerDescription) for all seeds."""
        return dict([
            (address, ServerDescription(address))
            for address in self.seeds])
