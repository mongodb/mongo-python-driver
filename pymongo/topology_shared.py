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

"""Topology utilities and shared helper methods."""

from __future__ import annotations

import queue
import weakref
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Callable, Generic, Optional, TypeVar

from pymongo import _csot
from pymongo._telemetry import _SdamTelemetry
from pymongo.client_session_shared import _ServerSession, _ServerSessionPool
from pymongo.pool_options import PoolOptions
from pymongo.server_selectors import Selection, writable_server_selector
from pymongo.topology_description import TOPOLOGY_TYPE, TopologyDescription

if TYPE_CHECKING:
    from bson import ObjectId
    from pymongo.server_description import ServerDescription
    from pymongo.settings_shared import _BaseTopologySettings
    from pymongo.typings import ClusterTime, _Address, _AgnosticPool, _AgnosticServer

_PoolT = TypeVar("_PoolT", bound="_AgnosticPool")
_ServerT = TypeVar("_ServerT", bound="_AgnosticServer")


def process_events_queue(queue_ref: weakref.ReferenceType[queue.Queue]) -> bool:  # type: ignore[type-arg]
    q = queue_ref()
    if not q:
        return False  # Cancel PeriodicExecutor.

    while True:
        try:
            event = q.get_nowait()
        except queue.Empty:
            break
        else:
            fn, args = event
            fn(*args)

    return True  # Continue PeriodicExecutor.


class _ErrorContext:
    """An error with context for SDAM error handling."""

    def __init__(
        self,
        error: BaseException,
        max_wire_version: int,
        sock_generation: int,
        completed_handshake: bool,
        service_id: Optional[ObjectId],
    ):
        self.error = error
        self.max_wire_version = max_wire_version
        self.sock_generation = sock_generation
        self.completed_handshake = completed_handshake
        self.service_id = service_id


def _is_stale_error_topology_version(
    current_tv: Optional[Mapping[str, Any]], error_tv: Optional[Mapping[str, Any]]
) -> bool:
    """Return True if the error's topologyVersion is <= current."""
    if current_tv is None or error_tv is None:
        return False
    if current_tv["processId"] != error_tv["processId"]:
        return False
    return current_tv["counter"] >= error_tv["counter"]


def _is_stale_server_description(current_sd: ServerDescription, new_sd: ServerDescription) -> bool:
    """Return True if the new topologyVersion is < current."""
    current_tv, new_tv = current_sd.topology_version, new_sd.topology_version
    if current_tv is None or new_tv is None:
        return False
    if current_tv["processId"] != new_tv["processId"]:
        return False
    return current_tv["counter"] > new_tv["counter"]


class _AgnosticTopologyBase(Generic[_PoolT, _ServerT]):
    """Monitor a topology of one or more servers."""

    def __init__(self, topology_settings: _BaseTopologySettings[Any, Any]) -> None:
        self._topology_id = topology_settings._topology_id
        self._listeners = topology_settings._pool_options._event_listeners

        # Create events queue if there are publishers.
        self._events: queue.Queue[Any] | None = None
        self._events_executor: Any = None

        publish_server = self._listeners is not None and self._listeners.enabled_for_server
        publish_tp = self._listeners is not None and self._listeners.enabled_for_topology
        if publish_server or publish_tp:
            self._events = queue.Queue(maxsize=100)

        self._sdam = _SdamTelemetry(self._topology_id, self._listeners, self._events)
        self._sdam.topology_opened()

        self._settings = topology_settings
        topology_description = TopologyDescription(
            topology_settings.get_topology_type(),
            topology_settings.get_server_descriptions(),
            topology_settings.replica_set_name,
            None,
            None,
            topology_settings,
        )

        self._description = topology_description
        initial_td = TopologyDescription(
            TOPOLOGY_TYPE.Unknown, {}, None, None, None, self._settings
        )
        self._sdam.topology_description_changed(initial_td, self._description)

        for seed in topology_settings.seeds:
            self._sdam.server_opened(seed)

        # Store the seed list to help diagnose errors in _error_message().
        self._seed_addresses = list(topology_description.server_descriptions())
        self._opened = False
        self._closed = False
        self._servers: dict[_Address, _ServerT] = {}
        self._pid: Optional[int] = None
        self._max_cluster_time: Optional[ClusterTime] = None
        self._session_pool = _ServerSessionPool()
        # The lock, condition, events executor, SRV monitor, and monitor tasks
        # are IO-specific and are created by the async/sync subclasses.
        self._srv_monitor: Any = None
        # Stores all monitor tasks that need to be joined on close or server selection
        self._monitor_tasks: list[Any] = []

    def get_server_selection_timeout(self) -> float:
        # CSOT: use remaining timeout when set.
        timeout = _csot.remaining()
        if timeout is None:
            return self._settings.server_selection_timeout
        return timeout

    def get_server_by_address(self, address: _Address) -> Optional[_ServerT]:
        """Get a Server or None.

        Returns the current version of the server immediately, even if it's
        Unknown or absent from the topology. Only use this in unittests.
        In driver code, use select_server_by_address, since then you're
        assured a recent view of the server's type and wire protocol version.
        """
        return self._servers.get(address)

    def has_server(self, address: _Address) -> bool:
        return address in self._servers

    def max_cluster_time(self) -> Optional[ClusterTime]:
        """Return a document, the highest seen $clusterTime."""
        return self._max_cluster_time

    def _receive_cluster_time_no_lock(self, cluster_time: Optional[Mapping[str, Any]]) -> None:
        # Driver Sessions Spec: "Whenever a driver receives a cluster time from
        # a server it MUST compare it to the current highest seen cluster time
        # for the deployment. If the new cluster time is higher than the
        # highest seen cluster time it MUST become the new highest seen cluster
        # time. Two cluster times are compared using only the BsonTimestamp
        # value of the clusterTime embedded field."
        if cluster_time:
            # ">" uses bson.timestamp.Timestamp's comparison operator.
            if (
                not self._max_cluster_time
                or cluster_time["clusterTime"] > self._max_cluster_time["clusterTime"]
            ):
                self._max_cluster_time = cluster_time

    def data_bearing_servers(self) -> list[ServerDescription]:
        """Return a list of all data-bearing servers.

        This includes any server that might be selected for an operation.
        """
        if self._description.topology_type == TOPOLOGY_TYPE.Single:
            return self._description.known_servers
        return self._description.readable_servers

    @property
    def description(self) -> TopologyDescription:
        return self._description

    def pop_all_sessions(self) -> list[_ServerSession]:
        """Pop all session ids from the pool."""
        return self._session_pool.pop_all()

    def get_server_session(self, session_timeout_minutes: Optional[int]) -> _ServerSession:
        """Start or resume a server session, or raise ConfigurationError."""
        return self._session_pool.get_server_session(session_timeout_minutes)

    def return_server_session(self, server_session: _ServerSession) -> None:
        self._session_pool.return_server_session(server_session)

    def _new_selection(self) -> Selection:
        """A Selection object, initially including all known servers.

        Hold the lock when calling this.
        """
        return Selection.from_topology_description(self._description)

    def _is_stale_error(self, address: _Address, err_ctx: _ErrorContext) -> bool:
        server = self._servers.get(address)
        if server is None:
            # Another thread removed this server from the topology.
            return True

        if server._pool.stale_generation(err_ctx.sock_generation, err_ctx.service_id):
            # This is an outdated error from a previous pool version.
            return True

        # topologyVersion check, ignore error when cur_tv >= error_tv:
        cur_tv = server.description.topology_version
        error = err_ctx.error
        error_tv = None
        if error and hasattr(error, "details"):
            if isinstance(error.details, dict):
                error_tv = error.details.get("topologyVersion")

        return _is_stale_error_topology_version(cur_tv, error_tv)

    def _request_check_all(self) -> None:
        """Wake all monitors. Hold the lock when calling this."""
        for server in self._servers.values():
            server.request_check()

    def _create_pool_for_server(self, address: _Address) -> _PoolT:
        return self._settings.pool_class(
            address, self._settings.pool_options, client_id=self._topology_id
        )

    def _create_pool_for_monitor(self, address: _Address) -> _PoolT:
        options = self._settings.pool_options

        # According to the Server Discovery And Monitoring Spec, monitors use
        # connect_timeout for both connect_timeout and socket_timeout. The
        # pool only has one socket so maxPoolSize and so on aren't needed.
        monitor_pool_options = PoolOptions(
            connect_timeout=options.connect_timeout,
            socket_timeout=options.connect_timeout,
            ssl_context=options._ssl_context,
            tls_allow_invalid_hostnames=options.tls_allow_invalid_hostnames,
            event_listeners=options._event_listeners,
            appname=options.appname,
            driver=options.driver,
            pause_enabled=False,
            server_api=options.server_api,
        )

        return self._settings.pool_class(
            address, monitor_pool_options, is_sdam=True, client_id=self._topology_id
        )

    def _error_message(self, selector: Callable[[Selection], Selection]) -> str:
        """Format an error message if server selection fails.

        Hold the lock when calling this.
        """
        is_replica_set = self._description.topology_type in (
            TOPOLOGY_TYPE.ReplicaSetWithPrimary,
            TOPOLOGY_TYPE.ReplicaSetNoPrimary,
        )

        if is_replica_set:
            server_plural = "replica set members"
        elif self._description.topology_type == TOPOLOGY_TYPE.Sharded:
            server_plural = "mongoses"
        else:
            server_plural = "servers"

        if self._description.known_servers:
            # We've connected, but no servers match the selector.
            if selector is writable_server_selector:
                if is_replica_set:
                    return "No primary available for writes"
                else:
                    return f"No {server_plural} available for writes"
            else:
                return f'No {server_plural} match selector "{selector}"'
        else:
            addresses = list(self._description.server_descriptions())
            servers = list(self._description.server_descriptions().values())
            if not servers:
                if is_replica_set:
                    # We removed all servers because of the wrong setName?
                    return f'No {server_plural} available for replica set name "{self._settings.replica_set_name}"'
                else:
                    return f"No {server_plural} available"

            # 1 or more servers, all Unknown. Are they unknown for one reason?
            error = servers[0].error
            same = all(server.error == error for server in servers[1:])
            if same:
                if error is None:
                    # We're still discovering.
                    return f"No {server_plural} found yet"

                if is_replica_set and not set(addresses).intersection(self._seed_addresses):
                    # We replaced our seeds with new hosts but can't reach any.
                    return (
                        f"Could not reach any servers in {addresses}. Replica set is"
                        " configured with internal hostnames or IPs?"
                    )

                return str(error)
            else:
                return ",".join(str(server.error) for server in servers if server.error)

    def __repr__(self) -> str:
        msg = ""
        if not self._opened:
            msg = "CLOSED "
        return f"<{self.__class__.__name__} {msg}{self._description!r}>"

    def eq_props(self) -> tuple[tuple[_Address, ...], Optional[str], Optional[str], str]:
        """The properties to use for MongoClient/AsyncMongoClient and Topology
        equality checks.
        """
        ts = self._settings
        return (tuple(sorted(ts.seeds)), ts.replica_set_name, ts.fqdn, ts.srv_service_name)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return self.eq_props() == other.eq_props()
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.eq_props())
