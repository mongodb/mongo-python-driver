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

"""Internal class to monitor a topology of one or more servers."""

from __future__ import annotations

import asyncio
import os
import queue
import random
import sys
import time
import warnings
import weakref
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional, cast

from pymongo import _csot, common, helpers_shared, periodic_executor
from pymongo._telemetry import (
    _ServerSelectionTelemetry,
    log_server_selection_succeeded,
)
from pymongo.asynchronous.monitor import SrvMonitor
from pymongo.asynchronous.pool import Pool
from pymongo.asynchronous.server import Server
from pymongo.errors import (
    ConnectionFailure,
    InvalidOperation,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
    PyMongoError,
    ServerSelectionTimeoutError,
    WaitQueueTimeoutError,
    WriteError,
)
from pymongo.hello import Hello
from pymongo.lock import _async_cond_wait, _async_create_condition, _async_create_lock
from pymongo.logger import _SERVER_SELECTION_LOGGER, _is_debug_enabled
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import (
    Selection,
    any_server_selector,
    arbiter_server_selector,
    secondary_server_selector,
    writable_server_selector,
)
from pymongo.topology_description import (
    SRV_POLLING_TOPOLOGIES,
    TOPOLOGY_TYPE,
    TopologyDescription,
    _updated_topology_description_srv_polling,
    updated_topology_description,
)
from pymongo.topology_shared import (
    _AgnosticTopologyBase,
    _ErrorContext,
    _is_stale_server_description,
    process_events_queue,
)

if TYPE_CHECKING:
    from pymongo.asynchronous.settings import TopologySettings
    from pymongo.typings import _Address

_IS_SYNC = False

_pymongo_dir = str(Path(__file__).parent)


class Topology(_AgnosticTopologyBase[Pool, Server]):
    """Monitor a topology of one or more servers."""

    _settings: TopologySettings

    def __init__(self, topology_settings: TopologySettings):
        super().__init__(topology_settings)
        self._lock = _async_create_lock()
        self._condition = _async_create_condition(
            self._lock, self._settings.condition_class if _IS_SYNC else None
        )

        if self._sdam._publish_server or self._sdam._publish_tp:
            assert self._events is not None
            weak: weakref.ReferenceType[queue.Queue[Any]]

            async def target() -> bool:
                return process_events_queue(weak)

            executor = periodic_executor.AsyncPeriodicExecutor(
                interval=common.EVENTS_QUEUE_FREQUENCY,
                min_interval=common.MIN_HEARTBEAT_INTERVAL,
                target=target,
                name="pymongo_events_thread",
            )

            # We strongly reference the executor and it weakly references
            # the queue via this closure. When the topology is freed, stop
            # the executor soon.
            weak = weakref.ref(self._events, executor.close)
            self._events_executor = executor
            executor.open()

        if self._settings.fqdn is not None and not self._settings.load_balanced:
            self._srv_monitor = SrvMonitor(self, self._settings)

    async def open(self) -> None:
        """Start monitoring, or restart after a fork.

        No effect if called multiple times.

        .. warning:: Topology is shared among multiple threads and is protected
          by mutual exclusion. Using Topology from a process other than the one
          that initialized it will emit a warning and may result in deadlock. To
          prevent this from happening, AsyncMongoClient must be created after any
          forking.

        """
        pid = os.getpid()
        if self._pid is None:
            self._pid = pid
        elif pid != self._pid:
            self._pid = pid
            if sys.version_info[:2] >= (3, 12):
                kwargs = {"skip_file_prefixes": (_pymongo_dir,)}
            else:
                kwargs = {"stacklevel": 6}
            warnings.warn(  # type: ignore[call-overload]
                "AsyncMongoClient opened before fork. May not be entirely fork-safe, "
                "proceed with caution. See PyMongo's documentation for details: "
                "https://dochub.mongodb.org/core/pymongo-fork-deadlock",
                **kwargs,
            )
            async with self._lock:
                # Close servers and clear the pools.
                for server in self._servers.values():
                    await server.close()
                # Reset the session pool to avoid duplicate sessions in
                # the child process.
                self._session_pool.reset()

        async with self._lock:
            await self._ensure_opened()

    async def select_servers(
        self,
        selector: Callable[[Selection], Selection],
        operation: str,
        server_selection_timeout: Optional[float] = None,
        address: Optional[_Address] = None,
        operation_id: Optional[int] = None,
        deprioritized_servers: Optional[list[Server]] = None,
    ) -> list[Server]:
        """Return a list of Servers matching selector, or time out.

        :param selector: function that takes a list of Servers and returns
            a subset of them.
        :param operation: The name of the operation that the server is being selected for.
        :param server_selection_timeout: maximum seconds to wait.
            If not provided, the default value common.SERVER_SELECTION_TIMEOUT
            is used.
        :param address: optional server address to select.

        Calls self.open() if needed.

        Raises exc:`ServerSelectionTimeoutError` after
        `server_selection_timeout` if no matching servers are found.
        """
        if server_selection_timeout is None:
            server_timeout = self.get_server_selection_timeout()
        else:
            server_timeout = server_selection_timeout

        # Cleanup any completed monitor tasks safely
        if not _IS_SYNC and self._monitor_tasks:
            await self.cleanup_monitors()

        async with self._lock:
            server_descriptions = await self._select_servers_loop(
                selector,
                server_timeout,
                operation,
                operation_id,
                address,
                deprioritized_servers=deprioritized_servers,
            )

            return [
                cast(Server, self.get_server_by_address(sd.address)) for sd in server_descriptions
            ]

    async def _select_servers_loop(
        self,
        selector: Callable[[Selection], Selection],
        timeout: float,
        operation: str,
        operation_id: Optional[int],
        address: Optional[_Address],
        deprioritized_servers: Optional[list[Server]] = None,
    ) -> list[ServerDescription]:
        """select_servers() guts. Hold the lock when calling this."""
        now = time.monotonic()
        end_time = now + timeout
        logged_waiting = False
        # Server selection does not have APM events, gate only on logging
        ss: Optional[_ServerSelectionTelemetry] = None
        if _is_debug_enabled(_SERVER_SELECTION_LOGGER):
            ss = _ServerSelectionTelemetry(
                self._topology_id, selector, operation, operation_id, self.description
            )
            ss.started()

        server_descriptions = self._description.apply_selector(
            selector,
            address,
            custom_selector=self._settings.server_selector,
            deprioritized_servers=[server.description for server in deprioritized_servers]
            if deprioritized_servers
            else None,
        )

        while not server_descriptions:
            # No suitable servers.
            if timeout == 0 or now > end_time:
                if ss is not None:
                    ss.failed(self._error_message(selector), self.description)
                raise ServerSelectionTimeoutError(
                    f"{self._error_message(selector)}, Timeout: {timeout}s, Topology Description: {self.description!r}"
                )

            if ss is not None and not logged_waiting:
                ss.waiting(int(1000 * (end_time - time.monotonic())))
                logged_waiting = True

            await self._ensure_opened()
            self._request_check_all()

            # Release the lock and wait for the topology description to
            # change, or for a timeout. We won't miss any changes that
            # came after our most recent apply_selector call, since we've
            # held the lock until now.
            await _async_cond_wait(self._condition, common.MIN_HEARTBEAT_INTERVAL)
            self._description.check_compatible()
            now = time.monotonic()
            server_descriptions = self._description.apply_selector(
                selector, address, custom_selector=self._settings.server_selector
            )

        self._description.check_compatible()
        return server_descriptions

    async def _select_server(
        self,
        selector: Callable[[Selection], Selection],
        operation: str,
        server_selection_timeout: Optional[float] = None,
        address: Optional[_Address] = None,
        deprioritized_servers: Optional[list[Server]] = None,
        operation_id: Optional[int] = None,
    ) -> Server:
        servers = await self.select_servers(
            selector,
            operation,
            server_selection_timeout,
            address,
            operation_id,
            deprioritized_servers,
        )
        if len(servers) == 1:
            return servers[0]
        server1, server2 = random.sample(servers, 2)
        if server1.pool.operation_count <= server2.pool.operation_count:
            return server1
        else:
            return server2

    async def select_server(
        self,
        selector: Callable[[Selection], Selection],
        operation: str,
        server_selection_timeout: Optional[float] = None,
        address: Optional[_Address] = None,
        deprioritized_servers: Optional[list[Server]] = None,
        operation_id: Optional[int] = None,
    ) -> Server:
        """Like select_servers, but choose a random server if several match."""
        server = await self._select_server(
            selector,
            operation,
            server_selection_timeout,
            address,
            deprioritized_servers,
            operation_id=operation_id,
        )
        if _csot.get_timeout():
            _csot.set_rtt(server.description.min_round_trip_time)
        if _is_debug_enabled(_SERVER_SELECTION_LOGGER):
            log_server_selection_succeeded(
                self._topology_id,
                selector,
                operation,
                operation_id,
                self.description,
                server.description.address[0],
                server.description.address[1],
            )
        return server

    async def select_server_by_address(
        self,
        address: _Address,
        operation: str,
        server_selection_timeout: Optional[int] = None,
        operation_id: Optional[int] = None,
    ) -> Server:
        """Return a Server for "address", reconnecting if necessary.

        If the server's type is not known, request an immediate check of all
        servers. Time out after "server_selection_timeout" if the server
        cannot be reached.

        :param address: A (host, port) pair.
        :param operation: The name of the operation that the server is being selected for.
        :param server_selection_timeout: maximum seconds to wait.
            If not provided, the default value
            common.SERVER_SELECTION_TIMEOUT is used.
        :param operation_id: The unique id of the current operation being performed. Defaults to None if not provided.

        Calls self.open() if needed.

        Raises exc:`ServerSelectionTimeoutError` after
        `server_selection_timeout` if no matching servers are found.
        """
        return await self.select_server(
            any_server_selector,
            operation,
            server_selection_timeout,
            address,
            operation_id=operation_id,
        )

    async def _process_change(
        self,
        server_description: ServerDescription,
        reset_pool: bool = False,
        interrupt_connections: bool = False,
    ) -> None:
        """Process a new ServerDescription on an opened topology.

        Hold the lock when calling this.
        """
        td_old = self._description
        sd_old = td_old._server_descriptions[server_description.address]
        if _is_stale_server_description(sd_old, server_description):
            # This is a stale hello response. Ignore it.
            return

        new_td = updated_topology_description(self._description, server_description)
        # CMAP: Ensure the pool is "ready" when the server is selectable.
        if server_description.is_readable or (
            server_description.is_server_type_known and new_td.topology_type == TOPOLOGY_TYPE.Single
        ):
            server = self._servers.get(server_description.address)
            if server:
                await server.pool.ready()

        suppress_event = sd_old == server_description
        if not suppress_event:
            self._sdam.server_description_changed(
                sd_old, server_description, server_description.address
            )

        self._description = new_td
        await self._update_servers()

        if not suppress_event:
            self._sdam.topology_description_changed(td_old, self._description)

        # Shutdown SRV polling for unsupported cluster types.
        # This is only applicable if the old topology was Unknown, and the
        # new one is something other than Unknown or Sharded.
        if self._srv_monitor and (
            td_old.topology_type == TOPOLOGY_TYPE.Unknown
            and self._description.topology_type not in SRV_POLLING_TOPOLOGIES
        ):
            await self._srv_monitor.close()
            if not _IS_SYNC:
                self._monitor_tasks.append(self._srv_monitor)

        # Wake anything waiting in select_servers().
        self._condition.notify_all()

    async def on_change(
        self,
        server_description: ServerDescription,
        reset_pool: bool = False,
        interrupt_connections: bool = False,
    ) -> None:
        """Process a new ServerDescription after an hello call completes."""
        # We do no I/O holding the lock.
        async with self._lock:
            # Monitors may continue working on hello calls for some time
            # after a call to Topology.close, so this method may be called at
            # any time. Ensure the topology is open before processing the
            # change.
            # Any monitored server was definitely in the topology description
            # once. Check if it's still in the description or if some state-
            # change removed it. E.g., we got a host list from the primary
            # that didn't include this server.
            if self._opened and self._description.has_server(server_description.address):
                await self._process_change(server_description, reset_pool, interrupt_connections)
        # Clear the pool from a failed heartbeat, done outside the lock to avoid blocking on connection close.
        if reset_pool:
            server = self._servers.get(server_description.address)
            if server:
                await server.pool.reset(interrupt_connections=interrupt_connections)

    async def _process_srv_update(self, seedlist: list[tuple[str, Any]]) -> None:
        """Process a new seedlist on an opened topology.
        Hold the lock when calling this.
        """
        td_old = self._description
        if td_old.topology_type not in SRV_POLLING_TOPOLOGIES:
            return
        self._description = _updated_topology_description_srv_polling(self._description, seedlist)

        await self._update_servers()
        self._sdam.topology_description_changed(td_old, self._description)

    async def on_srv_update(self, seedlist: list[tuple[str, Any]]) -> None:
        """Process a new list of nodes obtained from scanning SRV records."""
        # We do no I/O holding the lock.
        async with self._lock:
            if self._opened:
                await self._process_srv_update(seedlist)

    async def get_primary(self) -> Optional[_Address]:
        """Return primary's address or None."""
        # Implemented here in Topology instead of AsyncMongoClient, so it can lock.
        async with self._lock:
            topology_type = self._description.topology_type
            if topology_type != TOPOLOGY_TYPE.ReplicaSetWithPrimary:
                return None

            return writable_server_selector(self._new_selection())[0].address

    async def _get_replica_set_members(
        self, selector: Callable[[Selection], Selection]
    ) -> set[_Address]:
        """Return set of replica set member addresses."""
        # Implemented here in Topology instead of AsyncMongoClient, so it can lock.
        async with self._lock:
            topology_type = self._description.topology_type
            if topology_type not in (
                TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                TOPOLOGY_TYPE.ReplicaSetNoPrimary,
            ):
                return set()

            return {sd.address for sd in iter(selector(self._new_selection()))}

    async def get_secondaries(self) -> set[_Address]:
        """Return set of secondary addresses."""
        return await self._get_replica_set_members(secondary_server_selector)

    async def get_arbiters(self) -> set[_Address]:
        """Return set of arbiter addresses."""
        return await self._get_replica_set_members(arbiter_server_selector)

    async def receive_cluster_time(self, cluster_time: Optional[Mapping[str, Any]]) -> None:
        async with self._lock:
            self._receive_cluster_time_no_lock(cluster_time)

    async def request_check_all(self, wait_time: int = 5) -> None:
        """Wake all monitors, wait for at least one to check its server."""
        async with self._lock:
            self._request_check_all()
            await _async_cond_wait(self._condition, wait_time)

    async def update_pool(self) -> None:
        # Remove any stale sockets and add new sockets if pool is too small.
        servers = []
        async with self._lock:
            # Only update pools for data-bearing servers.
            for sd in self.data_bearing_servers():
                server = self._servers[sd.address]
                servers.append((server, server.pool.gen.get_overall()))

        for server, generation in servers:
            try:
                await server.pool.remove_stale_sockets(generation)
            except PyMongoError as exc:
                ctx = _ErrorContext(exc, 0, generation, False, None)
                await self.handle_error(server.description.address, ctx)
                raise

    async def close(self) -> None:
        """Clear pools and terminate monitors. Topology does not reopen on
        demand. Any further operations will raise
        :exc:`~.errors.InvalidOperation`.
        """
        async with self._lock:
            old_td = self._description
            for server in self._servers.values():
                await server.close()
                if not _IS_SYNC:
                    self._monitor_tasks.append(server._monitor)

            # Mark all servers Unknown.
            self._description = self._description.reset()
            for address, sd in self._description.server_descriptions().items():
                if address in self._servers:
                    self._servers[address].description = sd

            # Stop SRV polling thread.
            if self._srv_monitor:
                await self._srv_monitor.close()
                if not _IS_SYNC:
                    self._monitor_tasks.append(self._srv_monitor)

            self._opened = False
            self._closed = True

        # Publish only after releasing the lock.
        if self._sdam._publish_tp:
            self._description = TopologyDescription(
                TOPOLOGY_TYPE.Unknown,
                {},
                self._description.replica_set_name,
                self._description.max_set_version,
                self._description.max_election_id,
                self._description._topology_settings,
            )
        self._sdam.topology_closed(old_td, self._description)

        if self._sdam._publish_server or self._sdam._publish_tp:
            # Make sure the events executor thread is fully closed before publishing the remaining events
            self._events_executor.close()
            await self._events_executor.join(1)
            process_events_queue(weakref.ref(self._events))  # type: ignore[arg-type]

    async def _ensure_opened(self) -> None:
        """Start monitors, or restart after a fork.

        Hold the lock when calling this.
        """
        if self._closed:
            raise InvalidOperation("Cannot use AsyncMongoClient after close")

        if not self._opened:
            self._opened = True
            await self._update_servers()

            # Start or restart the events publishing thread.
            if self._sdam._publish_tp or self._sdam._publish_server:
                self._events_executor.open()

            # Start the SRV polling thread.
            if self._srv_monitor and (self.description.topology_type in SRV_POLLING_TOPOLOGIES):
                self._srv_monitor.open()

            if self._settings.load_balanced:
                # Emit initial SDAM events for load balancer mode.
                await self._process_change(
                    ServerDescription(
                        self._seed_addresses[0],
                        Hello({"ok": 1, "serviceId": self._topology_id, "maxWireVersion": 13}),
                    )
                )

        # Ensure that the monitors are open.
        for server in self._servers.values():
            await server.open()

    async def _handle_error(self, address: _Address, err_ctx: _ErrorContext) -> None:
        if self._is_stale_error(address, err_ctx):
            return

        server = self._servers[address]
        error = err_ctx.error
        service_id = err_ctx.service_id

        # Ignore a handshake error if the server is behind a load balancer but
        # the service ID is unknown. This indicates that the error happened
        # when dialing the connection or during the MongoDB  handshake, so we
        # don't know the service ID to use for clearing the pool.
        if self._settings.load_balanced and not service_id and not err_ctx.completed_handshake:
            return

        if isinstance(error, NetworkTimeout) and err_ctx.completed_handshake:
            # The socket has been closed. Don't reset the server.
            # Server Discovery And Monitoring Spec: "When an application
            # operation fails because of any network error besides a socket
            # timeout...."
            return
        elif isinstance(error, WriteError):
            # Ignore writeErrors.
            return
        elif isinstance(error, (NotPrimaryError, OperationFailure)):
            # As per the SDAM spec if:
            #   - the server sees a "not primary" error, and
            #   - the server is not shutting down, then
            # we keep the existing connection pool, but mark the server type
            # as Unknown and request an immediate check of the server.
            # Otherwise, we clear the connection pool, mark the server as
            # Unknown and request an immediate check of the server.
            if hasattr(error, "code"):
                err_code = error.code
            else:
                # Default error code if one does not exist.
                default = 10107 if isinstance(error, NotPrimaryError) else None
                err_code = error.details.get("code", default)  # type: ignore[union-attr]
            if err_code in helpers_shared._NOT_PRIMARY_CODES:
                is_shutting_down = err_code in helpers_shared._SHUTDOWN_CODES
                if not self._settings.load_balanced:
                    await self._process_change(ServerDescription(address, error=error))
                if is_shutting_down:
                    # Clear the pool.
                    await server.reset(service_id)
                server.request_check()
            elif not err_ctx.completed_handshake:
                # Unknown command error during the connection handshake.
                if not self._settings.load_balanced:
                    await self._process_change(ServerDescription(address, error=error))
                # Clear the pool.
                await server.reset(service_id)
        elif isinstance(error, ConnectionFailure):
            if isinstance(error, WaitQueueTimeoutError) or (
                error.has_error_label("SystemOverloadedError")
            ):
                return
            # "Client MUST replace the server's description with type Unknown
            # ... MUST NOT request an immediate check of the server."
            if not self._settings.load_balanced:
                await self._process_change(ServerDescription(address, error=error))
            # Clear the pool.
            await server.reset(service_id)
            # "When a client marks a server Unknown from `Network error when
            # reading or writing`_, clients MUST cancel the hello check on
            # that server and close the current monitoring connection."
            server._monitor.cancel_check()

    async def handle_error(self, address: _Address, err_ctx: _ErrorContext) -> None:
        """Handle an application error.

        May reset the server to Unknown, clear the pool, and request an
        immediate check depending on the error and the context.
        """
        async with self._lock:
            await self._handle_error(address, err_ctx)

    async def _update_servers(self) -> None:
        """Sync our Servers from TopologyDescription.server_descriptions.

        Hold the lock while calling this.
        """
        for address, sd in self._description.server_descriptions().items():
            if address not in self._servers:
                monitor = self._settings.monitor_class(
                    server_description=sd,
                    topology=self,
                    pool=self._create_pool_for_monitor(address),
                    topology_settings=self._settings,
                )

                weak = None
                if self._sdam._publish_server and self._events is not None:
                    weak = weakref.ref(self._events)
                server = Server(
                    server_description=sd,
                    pool=self._create_pool_for_server(address),
                    monitor=monitor,
                    topology_id=self._topology_id,
                    listeners=self._listeners,
                    events=weak,
                )

                self._servers[address] = server
                await server.open()
            else:
                # Cache old is_writable value.
                was_writable = self._servers[address].description.is_writable
                # Update server description.
                self._servers[address].description = sd
                # Update is_writable value of the pool, if it changed.
                if was_writable != sd.is_writable:
                    await self._servers[address].pool.update_is_writable(sd.is_writable)

        for address, server in list(self._servers.items()):
            if not self._description.has_server(address):
                await server.close()
                if not _IS_SYNC:
                    self._monitor_tasks.append(server._monitor)
                self._servers.pop(address)

    async def cleanup_monitors(self) -> None:
        tasks = []
        try:
            while self._monitor_tasks:
                tasks.append(self._monitor_tasks.pop())
        except IndexError:
            pass
        await asyncio.gather(*[t.join() for t in tasks], return_exceptions=True)  # type: ignore[func-returns-value]
