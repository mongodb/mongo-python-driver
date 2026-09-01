# Copyright 2014-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Represent MongoClient's configuration."""

from __future__ import annotations

import threading
from collections.abc import Collection
from typing import Optional

from bson.objectid import ObjectId
from pymongo import common
from pymongo.asynchronous import monitor, pool
from pymongo.asynchronous.pool import Pool
from pymongo.common import LOCAL_THRESHOLD_MS, SERVER_SELECTION_TIMEOUT
from pymongo.pool_options import PoolOptions
from pymongo.settings_shared import _BaseTopologySettings
from pymongo.topology_description import _ServerSelector

_IS_SYNC = False


class TopologySettings(_BaseTopologySettings[type[Pool], type[monitor.Monitor]]):
    def __init__(
        self,
        seeds: Optional[Collection[tuple[str, int]]] = None,
        replica_set_name: Optional[str] = None,
        pool_class: Optional[type[Pool]] = None,
        pool_options: Optional[PoolOptions] = None,
        monitor_class: Optional[type[monitor.Monitor]] = None,
        condition_class: Optional[type[threading.Condition]] = None,
        local_threshold_ms: int = LOCAL_THRESHOLD_MS,
        server_selection_timeout: int = SERVER_SELECTION_TIMEOUT,
        heartbeat_frequency: int = common.HEARTBEAT_FREQUENCY,
        server_selector: Optional[_ServerSelector] = None,
        fqdn: Optional[str] = None,
        direct_connection: Optional[bool] = False,
        load_balanced: Optional[bool] = None,
        srv_service_name: str = common.SRV_SERVICE_NAME,
        srv_max_hosts: int = 0,
        server_monitoring_mode: str = common.SERVER_MONITORING_MODE,
        topology_id: Optional[ObjectId] = None,
    ):
        """Represent MongoClient's configuration.

        Take a list of (host, port) pairs and optional replica set name.
        """
        pool_class = pool_class or pool.Pool
        monitor_class = monitor_class or monitor.Monitor
        super().__init__(
            pool_class,
            monitor_class,
            seeds,
            replica_set_name,
            pool_options,
            condition_class,
            local_threshold_ms,
            server_selection_timeout,
            heartbeat_frequency,
            server_selector,
            fqdn,
            direct_connection,
            load_balanced,
            srv_service_name,
            srv_max_hosts,
            server_monitoring_mode,
            topology_id,
        )
