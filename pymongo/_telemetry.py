# Copyright 2026-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Internal helpers combining structured logging with APM event publishing."""

from __future__ import annotations

import datetime
import logging
import queue
import time
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Optional

from pymongo.logger import (
    _COMMAND_LOGGER,
    _CONNECTION_LOGGER,
    _SDAM_LOGGER,
    _SERVER_SELECTION_LOGGER,
    _CommandStatusMessage,
    _ConnectionStatusMessage,
    _debug_log,
    _SDAMStatusMessage,
    _ServerSelectionStatusMessage,
    _verbose_connection_error_reason,
)
from pymongo.pool_shared import _ConnectionTelemetryInfo

if TYPE_CHECKING:
    from bson.objectid import ObjectId
    from pymongo.hello import Hello
    from pymongo.monitoring import _EventListeners
    from pymongo.typings import _Address, _DocumentOut


class _CommandTelemetry:
    """Combines structured logging and APM event publishing for a single command.

    Construct once per command, call :meth:`started` before the network send,
    then call :meth:`succeeded` or :meth:`failed` when the outcome is known.
    Duration is measured from the :meth:`started` call.
    """

    __slots__ = (
        "_active",
        "_cmd",
        "_conn",
        "_dbname",
        "_duration",
        "_listeners",
        "_name",
        "_op_id",
        "_publish",
        "_request_id",
        "_should_log",
        "_start",
        "_topology_id",
    )

    def __init__(
        self,
        topology_id: Optional[ObjectId],
        conn: _ConnectionTelemetryInfo,
        listeners: Optional[_EventListeners],
        cmd: MutableMapping[str, Any],
        dbname: str,
        request_id: int,
        op_id: Optional[int],
    ) -> None:
        self._topology_id = topology_id
        self._should_log = topology_id is not None and _COMMAND_LOGGER.isEnabledFor(logging.DEBUG)
        self._publish = listeners is not None and listeners.enabled_for_commands
        self._active = self._should_log or self._publish
        self._listeners = listeners
        self._conn = conn
        self._cmd = cmd
        self._name = next(iter(cmd))
        self._dbname = dbname
        self._request_id = request_id
        self._op_id = op_id
        self._start: datetime.datetime
        self._duration: datetime.timedelta

    def _emit_log(self, message: _CommandStatusMessage, **extra: Any) -> None:
        _debug_log(
            _COMMAND_LOGGER,
            message=message,
            clientId=self._topology_id,
            commandName=self._name,
            databaseName=self._dbname,
            requestId=self._request_id,
            operationId=self._request_id,
            driverConnectionId=self._conn.id,
            serverConnectionId=self._conn.server_connection_id,
            serverHost=self._conn.address[0],
            serverPort=self._conn.address[1],
            serviceId=self._conn.service_id,
            **extra,
        )

    def started(self, orig: MutableMapping[str, Any], ensure_db: bool) -> None:
        """Emit the STARTED log entry and APM event, and start the duration clock."""
        self._start = datetime.datetime.now()
        if not self._active:
            return
        if self._should_log:
            self._emit_log(_CommandStatusMessage.STARTED, command=self._cmd)
        if self._publish:
            assert self._listeners is not None
            if ensure_db and "$db" not in orig:
                orig["$db"] = self._dbname
            self._listeners.publish_command_start(
                orig,
                self._dbname,
                self._request_id,
                self._conn.address,
                self._conn.server_connection_id,
                self._op_id,
                service_id=self._conn.service_id,
            )

    @property
    def duration(self) -> datetime.timedelta:
        """Duration from :meth:`started` to :meth:`succeeded` or :meth:`failed`."""
        return self._duration

    def succeeded(
        self,
        reply: _DocumentOut,
        command_name: str,
        speculative_hello: bool,
    ) -> None:
        """Emit the SUCCEEDED log entry and APM event."""
        self._duration = datetime.datetime.now() - self._start
        if not self._active:
            return
        if self._should_log:
            self._emit_log(
                _CommandStatusMessage.SUCCEEDED,
                durationMS=self._duration,
                reply=reply,
                speculative_authenticate=speculative_hello,
            )
        if self._publish:
            assert self._listeners is not None
            self._listeners.publish_command_success(
                self._duration,
                reply,
                command_name,
                self._request_id,
                self._conn.address,
                self._conn.server_connection_id,
                self._op_id,
                service_id=self._conn.service_id,
                speculative_hello=speculative_hello,
                database_name=self._dbname,
            )

    def failed(
        self,
        failure: _DocumentOut,
        command_name: str,
        is_server_side_error: bool,
    ) -> None:
        """Emit the FAILED log entry and APM event."""
        self._duration = datetime.datetime.now() - self._start
        if not self._active:
            return
        if self._should_log:
            self._emit_log(
                _CommandStatusMessage.FAILED,
                durationMS=self._duration,
                failure=failure,
                isServerSideError=is_server_side_error,
            )
        if self._publish:
            assert self._listeners is not None
            self._listeners.publish_command_failure(
                self._duration,
                failure,
                command_name,
                self._request_id,
                self._conn.address,
                self._conn.server_connection_id,
                self._op_id,
                service_id=self._conn.service_id,
                database_name=self._dbname,
            )


class _CmapTelemetry:
    """Combines CMAP structured logging and APM event publishing for pool and connection events."""

    __slots__ = (
        "_address",
        "_client_id",
        "_listeners",
        "_log",
        "_publish",
    )

    def __init__(
        self,
        client_id: Optional[ObjectId],
        address: _Address,
        listeners: Optional[_EventListeners],
        publish: bool,
        log: bool,
    ) -> None:
        self._client_id = client_id
        self._address = address
        self._listeners = listeners
        self._publish = publish
        self._log = log

    @property
    def _should_publish(self) -> bool:
        """Computed per-call because listener registration can change while the pool is open."""
        return self._publish and self._listeners is not None and self._listeners.enabled_for_cmap

    @property
    def _should_log(self) -> bool:
        """Computed per-call because logging level can be reconfigured at runtime."""
        return self._log and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG)

    def _emit_log(self, message: _ConnectionStatusMessage, **extra: Any) -> None:
        _debug_log(
            _CONNECTION_LOGGER,
            message=message,
            clientId=self._client_id,
            serverHost=self._address[0],
            serverPort=self._address[1],
            **extra,
        )

    def pool_created(self, non_default_options: dict[str, Any]) -> None:
        """Emit the pool created log entry and APM event."""
        # Log before publishing to prevent potential listener preemption in tests.
        if self._should_log:
            self._emit_log(_ConnectionStatusMessage.POOL_CREATED, **non_default_options)
        if self._should_publish:
            assert self._listeners is not None
            self._listeners.publish_pool_created(self._address, non_default_options)

    def pool_ready(self) -> None:
        """Emit the pool ready log entry and APM event."""
        # Log before publishing to prevent potential listener preemption in tests.
        if self._should_log:
            self._emit_log(_ConnectionStatusMessage.POOL_READY)
        if self._should_publish:
            assert self._listeners is not None
            self._listeners.publish_pool_ready(self._address)

    def pool_cleared(self, service_id: Optional[ObjectId], interrupt_connections: bool) -> None:
        """Emit the pool cleared log entry and APM event."""
        # Log before publishing to prevent potential listener preemption in tests.
        if self._should_log:
            self._emit_log(_ConnectionStatusMessage.POOL_CLEARED, serviceId=service_id)
        if self._should_publish:
            assert self._listeners is not None
            self._listeners.publish_pool_cleared(
                self._address,
                service_id=service_id,
                interrupt_connections=interrupt_connections,
            )

    def pool_closed(self) -> None:
        """Emit the pool closed log entry and APM event."""
        # Log before publishing to prevent potential listener preemption in tests.
        if self._should_log:
            self._emit_log(_ConnectionStatusMessage.POOL_CLOSED)
        if self._should_publish:
            assert self._listeners is not None
            self._listeners.publish_pool_closed(self._address)

    def connection_created(self, conn_id: int) -> None:
        """Emit the connection created log entry and APM event."""
        # Log before publishing to prevent potential listener preemption in tests.
        if self._should_log:
            self._emit_log(_ConnectionStatusMessage.CONN_CREATED, driverConnectionId=conn_id)
        if self._should_publish:
            assert self._listeners is not None
            self._listeners.publish_connection_created(self._address, conn_id)

    def connection_ready(self, conn_id: int, creation_time: float) -> None:
        """Emit the connection ready log entry and APM event."""
        should_log = self._should_log
        should_publish = self._should_publish
        if not should_log and not should_publish:
            return
        duration = max(0.0, time.monotonic() - creation_time)
        # Log before publishing to prevent potential listener preemption in tests.
        if should_log:
            self._emit_log(
                _ConnectionStatusMessage.CONN_READY,
                driverConnectionId=conn_id,
                durationMS=duration * 1000,
            )
        if should_publish:
            assert self._listeners is not None
            self._listeners.publish_connection_ready(self._address, conn_id, duration)

    def connection_closed(self, conn_id: int, reason: str) -> None:
        """Emit the connection closed log entry and APM event."""
        should_log = self._should_log
        should_publish = self._should_publish
        if should_publish:
            assert self._listeners is not None
            self._listeners.publish_connection_closed(self._address, conn_id, reason)
        if should_log:
            self._emit_log(
                _ConnectionStatusMessage.CONN_CLOSED,
                driverConnectionId=conn_id,
                reason=_verbose_connection_error_reason(reason),
                error=reason,
            )

    def checkout_started(self) -> float:
        """Emit the checkout started event/log and return the start time for duration tracking."""
        start = time.monotonic()
        if self._should_publish:
            assert self._listeners is not None
            self._listeners.publish_connection_check_out_started(self._address)
        if self._should_log:
            self._emit_log(_ConnectionStatusMessage.CHECKOUT_STARTED)
        return start

    def checkout_succeeded(self, conn_id: int, start: float) -> None:
        """Emit the checkout succeeded log entry and APM event."""
        should_log = self._should_log
        should_publish = self._should_publish
        if not should_log and not should_publish:
            return
        duration = max(0.0, time.monotonic() - start)
        if should_publish:
            assert self._listeners is not None
            self._listeners.publish_connection_checked_out(self._address, conn_id, duration)
        if should_log:
            self._emit_log(
                _ConnectionStatusMessage.CHECKOUT_SUCCEEDED,
                driverConnectionId=conn_id,
                durationMS=duration * 1000,
            )

    def checkout_failed(self, reason: str, error: str, start: float) -> None:
        """Emit the checkout failed log entry and APM event."""
        should_log = self._should_log
        should_publish = self._should_publish
        if not should_log and not should_publish:
            return
        duration = max(0.0, time.monotonic() - start)
        if should_publish:
            assert self._listeners is not None
            self._listeners.publish_connection_check_out_failed(self._address, error, duration)
        if should_log:
            self._emit_log(
                _ConnectionStatusMessage.CHECKOUT_FAILED,
                reason=reason,
                error=error,
                durationMS=duration * 1000,
            )

    def checked_in(self, conn_id: int) -> None:
        """Emit the connection checked-in log entry and APM event."""
        if self._should_publish:
            assert self._listeners is not None
            self._listeners.publish_connection_checked_in(self._address, conn_id)
        if self._should_log:
            self._emit_log(_ConnectionStatusMessage.CHECKEDIN, driverConnectionId=conn_id)


class _HeartbeatTelemetry:
    """Combines SDAM structured logging and APM event publishing for server heartbeats.

    The APM started event is published before connection checkout (no conn_id yet);
    the log entry for started is emitted after checkout once the conn_id is known.
    Call :meth:`started` first, then :meth:`emit_started_log` inside the checkout
    context, then :meth:`succeeded` or :meth:`failed` when the outcome is known.
    """

    __slots__ = (
        "_address",
        "_awaited",
        "_listeners",
        "_should_log",
        "_should_publish",
        "_start",
        "_topology_id",
    )

    def __init__(
        self,
        topology_id: ObjectId,
        address: _Address,
        listeners: Optional[_EventListeners],
        awaited: bool,
    ) -> None:
        self._topology_id = topology_id
        self._address = address
        self._listeners = listeners
        self._awaited = awaited
        # Cached at construction: this object is short-lived (one heartbeat check) so
        # listener registration and logging level are stable for its lifetime.
        self._should_publish = listeners is not None and listeners.enabled_for_server_heartbeat
        self._should_log = _SDAM_LOGGER.isEnabledFor(logging.DEBUG)

    def _emit_log(self, message: _SDAMStatusMessage, **extra: Any) -> None:
        _debug_log(
            _SDAM_LOGGER,
            message=message,
            topologyId=self._topology_id,
            serverHost=self._address[0],
            serverPort=self._address[1],
            awaited=self._awaited,
            **extra,
        )

    def started(self) -> None:
        """Publish the APM heartbeat-started event (before connection checkout)."""
        if self._should_publish or self._should_log:
            self._start = time.monotonic()
        if self._should_publish:
            assert self._listeners is not None
            self._listeners.publish_server_heartbeat_started(self._address, self._awaited)

    def emit_started_log(self, conn_id: int, server_conn_id: Optional[int]) -> None:
        """Emit the log entry for heartbeat started (after connection checkout)."""
        if self._should_log:
            self._emit_log(
                _SDAMStatusMessage.HEARTBEAT_START,
                driverConnectionId=conn_id,
                serverConnectionId=server_conn_id,
            )

    def succeeded(
        self,
        round_trip_time: float,
        response: Hello[Any],
        conn_id: int,
        server_conn_id: Optional[int],
    ) -> None:
        """Emit the SUCCEEDED log entry and APM event."""
        if self._should_publish:
            assert self._listeners is not None
            self._listeners.publish_server_heartbeat_succeeded(
                self._address, round_trip_time, response, response.awaitable
            )
        if self._should_log:
            self._emit_log(
                _SDAMStatusMessage.HEARTBEAT_SUCCESS,
                driverConnectionId=conn_id,
                serverConnectionId=server_conn_id,
                durationMS=round_trip_time * 1000,
                reply=response.document,
            )

    def failed(self, error: Exception, conn_id: Optional[int]) -> None:
        """Emit the FAILED log entry and APM event."""
        should_publish = self._should_publish
        should_log = self._should_log
        if not should_publish and not should_log:
            return
        duration = max(0.0, time.monotonic() - self._start)
        if should_publish:
            assert self._listeners is not None
            self._listeners.publish_server_heartbeat_failed(
                self._address, duration, error, self._awaited
            )
        if should_log:
            self._emit_log(
                _SDAMStatusMessage.HEARTBEAT_FAIL,
                durationMS=duration * 1000,
                failure=error,
                driverConnectionId=conn_id,
            )


class _SdamTelemetry:
    """Combines SDAM structured logging and APM event publishing for topology and server events.

    Topology events are queued for asynchronous delivery; log entries are emitted inline.
    """

    __slots__ = ("_events", "_listeners", "_topology_id")

    def __init__(
        self,
        topology_id: ObjectId,
        listeners: Optional[_EventListeners],
        events: Optional[queue.Queue[Any]],
    ) -> None:
        self._topology_id = topology_id
        self._listeners = listeners
        self._events = events

    @property
    def _publish_server(self) -> bool:
        """Computed per-call because listener registration can change while the topology is open."""
        return self._listeners is not None and self._listeners.enabled_for_server

    @property
    def _publish_tp(self) -> bool:
        """Computed per-call because listener registration can change while the topology is open."""
        return self._listeners is not None and self._listeners.enabled_for_topology

    @property
    def _should_log(self) -> bool:
        """Computed per-call because logging level can be reconfigured at runtime."""
        return _SDAM_LOGGER.isEnabledFor(logging.DEBUG)

    def _enqueue(self, fn: Any, args: tuple[Any, ...]) -> None:
        if self._events is not None:
            self._events.put((fn, args))

    def _emit_log(self, message: _SDAMStatusMessage, **extra: Any) -> None:
        _debug_log(
            _SDAM_LOGGER,
            message=message,
            topologyId=self._topology_id,
            **extra,
        )

    def topology_opened(self) -> None:
        """Emit the topology opened log entry and APM event."""
        if self._should_log:
            self._emit_log(_SDAMStatusMessage.START_TOPOLOGY)
        if self._publish_tp:
            assert self._listeners is not None
            self._enqueue(self._listeners.publish_topology_opened, (self._topology_id,))

    def topology_description_changed(self, old_td: Any, new_td: Any) -> None:
        """Emit the topology description changed APM event and log entry."""
        if self._publish_tp:
            assert self._listeners is not None
            self._enqueue(
                self._listeners.publish_topology_description_changed,
                (old_td, new_td, self._topology_id),
            )
        if self._should_log:
            self._emit_log(
                _SDAMStatusMessage.TOPOLOGY_CHANGE,
                previousDescription=repr(old_td),
                newDescription=repr(new_td),
            )

    def topology_closed(self, old_td: Any, new_td: Any) -> None:
        """Emit APM and log events for topology description change + topology closed."""
        if self._publish_tp:
            assert self._listeners is not None
            self._enqueue(
                self._listeners.publish_topology_description_changed,
                (old_td, new_td, self._topology_id),
            )
            self._enqueue(self._listeners.publish_topology_closed, (self._topology_id,))
        if self._should_log:
            self._emit_log(
                _SDAMStatusMessage.TOPOLOGY_CHANGE,
                previousDescription=repr(old_td),
                newDescription=repr(new_td),
            )
            self._emit_log(_SDAMStatusMessage.STOP_TOPOLOGY)

    def server_opened(self, address: _Address) -> None:
        """Emit the server opened log entry and APM event."""
        if self._publish_server:
            assert self._listeners is not None
            self._enqueue(self._listeners.publish_server_opened, (address, self._topology_id))
        if self._should_log:
            self._emit_log(
                _SDAMStatusMessage.START_SERVER,
                serverHost=address[0],
                serverPort=address[1],
            )

    def server_description_changed(self, sd_old: Any, sd_new: Any, address: _Address) -> None:
        """Emit the server description changed APM event."""
        if self._publish_server:
            assert self._listeners is not None
            self._enqueue(
                self._listeners.publish_server_description_changed,
                (sd_old, sd_new, address, self._topology_id),
            )

    def server_closed(self, address: _Address) -> None:
        """Emit the server closed log entry and APM event."""
        if self._publish_server:
            assert self._listeners is not None
            self._enqueue(self._listeners.publish_server_closed, (address, self._topology_id))
        if self._should_log:
            self._emit_log(
                _SDAMStatusMessage.STOP_SERVER,
                serverHost=address[0],
                serverPort=address[1],
            )


class _ServerSelectionTelemetry:
    """Structured logging for server selection events.

    The server selection spec defines only log entries, not APM events, so this
    class has no publish methods.

    Construct once per :meth:`select_server` call.
    """

    __slots__ = (
        "_operation",
        "_operation_id",
        "_selector",
        "_should_log",
        "_topology_description",
        "_topology_id",
    )

    def __init__(
        self,
        topology_id: Any,
        selector: Any,
        operation: str,
        operation_id: Optional[int],
        topology_description: Any,
    ) -> None:
        self._topology_id = topology_id
        self._selector = selector
        self._operation = operation
        self._operation_id = operation_id
        self._topology_description = topology_description
        # Cached at construction: this object is short-lived (one select_server call) so
        # logging level is stable for its lifetime.
        self._should_log = _SERVER_SELECTION_LOGGER.isEnabledFor(logging.DEBUG)

    def _emit_log(
        self, message: _ServerSelectionStatusMessage, topology_description: Any, **extra: Any
    ) -> None:
        _debug_log(
            _SERVER_SELECTION_LOGGER,
            message=message,
            clientId=self._topology_id,
            selector=self._selector,
            operation=self._operation,
            operationId=self._operation_id,
            topologyDescription=topology_description,
            **extra,
        )

    def started(self) -> None:
        """Emit the server selection STARTED log entry."""
        if self._should_log:
            self._emit_log(_ServerSelectionStatusMessage.STARTED, self._topology_description)

    def waiting(self, remaining_time_ms: int) -> None:
        """Emit the server selection WAITING log entry."""
        if self._should_log:
            self._emit_log(
                _ServerSelectionStatusMessage.WAITING,
                self._topology_description,
                remainingTimeMS=remaining_time_ms,
            )

    def failed(self, failure: str, topology_description: Any) -> None:
        """Emit the server selection FAILED log entry with the current topology description."""
        if self._should_log:
            self._emit_log(
                _ServerSelectionStatusMessage.FAILED,
                topology_description,
                failure=failure,
            )

    def succeeded(self, server_host: str, server_port: Optional[int]) -> None:
        """Emit the server selection SUCCEEDED log entry."""
        if self._should_log:
            self._emit_log(
                _ServerSelectionStatusMessage.SUCCEEDED,
                self._topology_description,
                serverHost=server_host,
                serverPort=server_port,
            )


def log_srv_monitor_failure(failure: Exception) -> None:
    """Emit a log entry when the SRV monitor fails to poll DNS records."""
    if _SDAM_LOGGER.isEnabledFor(logging.DEBUG):
        _debug_log(_SDAM_LOGGER, message="SRV monitor check failed", failure=repr(failure))


def log_command_retry(
    topology_id: Any,
    command_name: str,
    operation_id: Optional[int],
    attempt_number: int,
    is_write: bool,
) -> None:
    """Emit a command-retry log entry."""
    if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
        op = "write" if is_write else "read"
        _debug_log(
            _COMMAND_LOGGER,
            message=f"Retrying {op} attempt number {attempt_number}",
            clientId=topology_id,
            commandName=command_name,
            operationId=operation_id,
        )
