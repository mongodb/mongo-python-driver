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
import queue
import time
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Optional

from pymongo import _op_id, _otel
from pymongo.errors import OperationFailure
from pymongo.logger import (
    _COMMAND_LOGGER,
    _CONNECTION_LOGGER,
    _SDAM_LOGGER,
    _SERVER_SELECTION_LOGGER,
    _CommandStatusMessage,
    _ConnectionStatusMessage,
    _debug_log,
    _is_debug_enabled,
    _SDAMStatusMessage,
    _ServerSelectionStatusMessage,
    _verbose_connection_error_reason,
)
from pymongo.message import _randint
from pymongo.pool_shared import _ConnectionTelemetryInfo

if TYPE_CHECKING:
    from bson.objectid import ObjectId
    from pymongo.hello import Hello
    from pymongo.monitoring import _EventListeners
    from pymongo.server_description import ServerDescription
    from pymongo.topology_description import TopologyDescription
    from pymongo.typings import _Address, _DocumentOut


def _monotonic_duration(start: float) -> float:
    """Return the duration since the given start time.

    Accounts for buggy platforms where time.monotonic() is not monotonic.
    See PYTHON-4600.
    """
    return max(0.0, time.monotonic() - start)


def _generate_op_id_or_none(listeners: Optional[_EventListeners]) -> Optional[int]:
    """Return a random operation id if it would be consumed by APM events or logging, else None."""
    return (
        _randint()
        if (
            (listeners is not None and listeners.enabled_for_commands)
            or _is_debug_enabled(_COMMAND_LOGGER)
            or _is_debug_enabled(_SERVER_SELECTION_LOGGER)
        )
        else None
    )


class _CommandTelemetry:
    """Combines structured logging and APM event publishing for a single command.

    Construct up to once per command, call :meth:`started` before the network send,
    then call :meth:`succeeded` or :meth:`failed` when the outcome is known.
    Duration is measured from the :meth:`started` call.

    This sits on the hot path of every command: when both APM events and command
    logging are disabled, only the gate flags and the monotonic duration clock
    are maintained.
    """

    __slots__ = (
        "_active",
        "_cmd",
        "_conn",
        "_dbname",
        "_duration_s",
        "_listeners",
        "_name",
        "_op_id",
        "_publish",
        "_request_id",
        "_should_log",
        "_span",
        "_speculative_hello",
        "_start",
        "_topology_id",
        "_tracing_enabled",
        "_tracing_options",
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
        tracing_options: Optional[_otel.TracingOptions] = None,
        speculative_hello: bool = False,
        name: Optional[str] = None,
    ) -> None:
        # NOTE: the _run_command fast path in command_runner.py inline this gate for performance
        # They must be kept in sync with any gating changes
        self._should_log = topology_id is not None and _is_debug_enabled(_COMMAND_LOGGER)
        self._publish = listeners is not None and listeners.enabled_for_commands
        self._tracing_options = tracing_options
        self._tracing_enabled = _otel._is_tracing_enabled(tracing_options)
        self._span: Optional[Any] = None
        self._active = self._should_log or self._publish or self._tracing_enabled
        self._start = 0.0
        self._duration_s = 0.0
        if not self._active:
            return
        self._topology_id = topology_id
        self._listeners = listeners
        self._conn = conn
        self._cmd = cmd
        self._name = name if name is not None else next(iter(cmd))
        self._dbname = dbname
        self._request_id = request_id
        self._op_id = op_id if op_id is not None else _op_id.OP_ID.get()
        self._speculative_hello = speculative_hello

    def _emit_log(self, message: _CommandStatusMessage, **extra: Any) -> None:
        _debug_log(
            _COMMAND_LOGGER,
            message=message,
            clientId=self._topology_id,
            commandName=self._name,
            databaseName=self._dbname,
            requestId=self._request_id,
            operationId=self._op_id if self._op_id is not None else self._request_id,
            driverConnectionId=self._conn.id,
            serverConnectionId=self._conn.server_connection_id,
            serverHost=self._conn.address[0],
            serverPort=self._conn.address[1],
            serviceId=self._conn.service_id,
            **extra,
        )

    def started(self, orig: MutableMapping[str, Any], ensure_db: bool) -> None:
        """Emit the STARTED log entry and APM event, start the span, and start the duration clock."""
        self._start = time.monotonic()
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
        if self._tracing_enabled:
            self._span = _otel.start_command_span(
                self._tracing_options,
                self._conn,
                self._cmd,
                self._dbname,
                self._name,
                self._speculative_hello,
            )

    @property
    def duration_s(self) -> float:
        """Duration in seconds from :meth:`started` to :meth:`succeeded` or :meth:`failed`."""
        return self._duration_s

    def succeeded(
        self,
        reply: _DocumentOut,
        command_name: str,
        speculative_hello: bool,
    ) -> None:
        """Emit the SUCCEEDED log entry and APM event, and end the span."""
        self._duration_s = _monotonic_duration(self._start)
        if not self._active:
            return
        duration = datetime.timedelta(seconds=self._duration_s)
        if self._should_log:
            self._emit_log(
                _CommandStatusMessage.SUCCEEDED,
                durationMS=duration,
                reply=reply,
                speculative_authenticate=speculative_hello,
            )
        if self._publish:
            assert self._listeners is not None
            self._listeners.publish_command_success(
                duration,
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
        if self._span is not None:
            _otel.end_command_span_success(self._span, reply)

    def failed(
        self,
        failure: _DocumentOut,
        command_name: str,
        exc: BaseException,
    ) -> None:
        """Emit the FAILED log entry and APM event, and end the span."""
        self._duration_s = _monotonic_duration(self._start)
        if not self._active:
            return
        duration = datetime.timedelta(seconds=self._duration_s)
        if self._should_log:
            self._emit_log(
                _CommandStatusMessage.FAILED,
                durationMS=duration,
                failure=failure,
                isServerSideError=isinstance(exc, OperationFailure),
            )
        if self._publish:
            assert self._listeners is not None
            self._listeners.publish_command_failure(
                duration,
                failure,
                command_name,
                self._request_id,
                self._conn.address,
                self._conn.server_connection_id,
                self._op_id,
                service_id=self._conn.service_id,
                database_name=self._dbname,
            )
        if self._span is not None:
            _otel.end_command_span_failure(self._span, failure, exc)


class _OperationTelemetry:
    """One span-scoped context per logical operation (spanning all retry attempts).

    Construct once per call to ``_retry_internal``; call :meth:`succeeded` or
    :meth:`failed` exactly once when the operation's outcome is known, or use
    it as a context manager to do so automatically. A no-op throughout when
    tracing is disabled.

    This span is shared by every attempt, while each attempt gets a command
    span of its own underneath it. Retries are therefore visible as sibling
    command spans rather than being collapsed into one.

    With ``set_current=False`` the span is not made current at construction.
    That suits a span started outside the ``_retry_internal`` call it covers,
    such as a cursor-creating command's, whose span has to exist before the
    cursor does; that call makes it current with :meth:`use`.
    """

    __slots__ = ("handle",)

    def __init__(
        self,
        tracing_options: Optional[_otel.TracingOptions],
        operation: str,
        session: Optional[Any],
        is_run_command: bool = False,
        dbname: Optional[str] = None,
        collection: Optional[str] = None,
        set_current: bool = True,
    ) -> None:
        parent_span = None
        if session is not None and session.in_transaction:
            parent_span = session._transaction.span
        self.handle = _otel.start_operation_span(
            tracing_options,
            _otel._build_operation_name(operation, is_run_command),
            parent_span,
            dbname=dbname,
            collection=collection,
            set_current=set_current,
        )

    def use(self) -> Any:
        """Make this operation's span current for the duration of a block."""
        return _otel.use_operation_span(self.handle)

    def succeeded(self) -> None:
        _otel.end_operation_span_success(self.handle)

    def failed(self, exc: BaseException) -> None:
        _otel.end_operation_span_failure(self.handle, exc)

    def __enter__(self) -> _OperationTelemetry:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_val is None:
            self.succeeded()
        else:
            self.failed(exc_val)


def _operation_telemetry_or_none(
    tracing_options: Optional[_otel.TracingOptions],
    operation: str,
    session: Optional[Any],
    is_run_command: bool = False,
    dbname: Optional[str] = None,
    collection: Optional[str] = None,
    set_current: bool = True,
) -> Optional[_OperationTelemetry]:
    """Return an :class:`_OperationTelemetry`, or None if tracing is disabled.

    Every operation goes through here, so follow _CommandTelemetry's fast path
    and skip the object rather than build one whose methods all do nothing.
    """
    if not _otel._is_tracing_enabled(tracing_options):
        return None
    return _OperationTelemetry(
        tracing_options,
        operation,
        session,
        is_run_command=is_run_command,
        dbname=dbname,
        collection=collection,
        set_current=set_current,
    )


class _CmapTelemetry:
    """Combines CMAP structured logging and APM event publishing for pool and connection events."""

    __slots__ = (
        "_address",
        "_client_id",
        "_listeners",
        "_log",
        "_should_publish",
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
        # The CMAP listener set is fixed once the client is constructed
        # (_EventListeners copies the global listeners at __init__), so this
        # gate is static for the life of the pool.
        # NOTE: the checkout/checkin fast paths in pool.py inline this gate for performance
        # They must be kept in sync with any gating changes
        self._should_publish = publish and listeners is not None and listeners.enabled_for_cmap
        self._log = log

    @property
    def _should_log(self) -> bool:
        """Computed per-call because logging level can be reconfigured at runtime."""
        return self._log and _is_debug_enabled(_CONNECTION_LOGGER)

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
        duration = _monotonic_duration(creation_time)
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
        duration = _monotonic_duration(start)
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
        duration = _monotonic_duration(start)
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
    ) -> None:
        self._topology_id = topology_id
        self._address = address
        self._listeners = listeners
        # Cached at construction: this object is short-lived (one heartbeat check) so
        # listener registration and logging level are stable for its lifetime.
        self._should_publish = listeners is not None and listeners.enabled_for_server_heartbeat
        self._should_log = _is_debug_enabled(_SDAM_LOGGER)
        self._start: float = 0.0

    def _emit_log(self, message: _SDAMStatusMessage, awaited: bool, **extra: Any) -> None:
        _debug_log(
            _SDAM_LOGGER,
            message=message,
            topologyId=self._topology_id,
            serverHost=self._address[0],
            serverPort=self._address[1],
            awaited=awaited,
            **extra,
        )

    def started(self, awaited: bool) -> None:
        """Publish the APM heartbeat-started event (before connection checkout)."""
        if self._should_publish or self._should_log:
            self._start = time.monotonic()
        if self._should_publish:
            assert self._listeners is not None
            self._listeners.publish_server_heartbeat_started(self._address, awaited)

    def emit_started_log(self, conn_id: int, server_conn_id: Optional[int], awaited: bool) -> None:
        """Emit the log entry for heartbeat started (after connection checkout)."""
        if self._should_log:
            self._emit_log(
                _SDAMStatusMessage.HEARTBEAT_START,
                awaited=awaited,
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
                awaited=response.awaitable,
                driverConnectionId=conn_id,
                serverConnectionId=server_conn_id,
                durationMS=round_trip_time * 1000,
                reply=response.document,
            )

    def failed(self, error: Exception, conn_id: Optional[int], awaited: bool) -> None:
        """Emit the FAILED log entry and APM event."""
        should_publish = self._should_publish
        should_log = self._should_log
        if not should_publish and not should_log:
            return
        duration = _monotonic_duration(self._start)
        if should_publish:
            assert self._listeners is not None
            self._listeners.publish_server_heartbeat_failed(self._address, duration, error, awaited)
        if should_log:
            self._emit_log(
                _SDAMStatusMessage.HEARTBEAT_FAIL,
                awaited=awaited,
                durationMS=duration * 1000,
                failure=error,
                driverConnectionId=conn_id,
            )


class _SdamTelemetry:
    """Combines SDAM structured logging and APM event publishing for topology and server events.

    Topology events are queued for asynchronous delivery; log entries are emitted inline.
    """

    __slots__ = ("_events", "_listeners", "_publish_server", "_publish_tp", "_topology_id")

    def __init__(
        self,
        topology_id: ObjectId,
        listeners: Optional[_EventListeners],
        events: Optional[queue.Queue[Any]],
    ) -> None:
        self._topology_id = topology_id
        self._listeners = listeners
        self._events = events
        # The SDAM listener set is fixed once the client is constructed
        # (_EventListeners copies the global listeners at __init__), so these
        # gates are static for the life of the client.
        self._publish_server = self._listeners is not None and self._listeners.enabled_for_server
        self._publish_tp = self._listeners is not None and self._listeners.enabled_for_topology

    @property
    def _should_log(self) -> bool:
        """Computed per-call because logging level can be reconfigured at runtime."""
        return _is_debug_enabled(_SDAM_LOGGER)

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

    def topology_description_changed(
        self, old_td: TopologyDescription, new_td: TopologyDescription
    ) -> None:
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

    def topology_closed(self, old_td: TopologyDescription, new_td: TopologyDescription) -> None:
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

    def server_description_changed(
        self, sd_old: ServerDescription, sd_new: ServerDescription, address: _Address
    ) -> None:
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
        topology_id: ObjectId,
        selector: Any,
        operation: str,
        operation_id: Optional[int],
        topology_description: TopologyDescription,
    ) -> None:
        self._topology_id = topology_id
        self._selector = selector
        self._operation = operation
        self._operation_id = operation_id
        self._topology_description = topology_description
        # Cached at construction: this object is short-lived (one select_server call) so
        # logging level is stable for its lifetime.
        self._should_log = _is_debug_enabled(_SERVER_SELECTION_LOGGER)

    def _emit_log(
        self,
        message: _ServerSelectionStatusMessage,
        topology_description: TopologyDescription,
        **extra: Any,
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

    def failed(self, failure: str, topology_description: TopologyDescription) -> None:
        """Emit the server selection FAILED log entry with the current topology description."""
        if self._should_log:
            self._emit_log(
                _ServerSelectionStatusMessage.FAILED,
                topology_description,
                failure=failure,
            )


def log_server_selection_succeeded(
    topology_id: ObjectId,
    selector: Any,
    operation: str,
    operation_id: Optional[int],
    topology_description: TopologyDescription,
    server_host: str,
    server_port: Optional[int],
) -> None:
    """Emit the server selection SUCCEEDED log entry."""
    if _is_debug_enabled(_SERVER_SELECTION_LOGGER):
        _debug_log(
            _SERVER_SELECTION_LOGGER,
            message=_ServerSelectionStatusMessage.SUCCEEDED,
            clientId=topology_id,
            selector=selector,
            operation=operation,
            operationId=operation_id,
            topologyDescription=topology_description,
            serverHost=server_host,
            serverPort=server_port,
        )


def log_srv_monitor_failure(failure: Exception) -> None:
    """Emit a log entry when the SRV monitor fails to poll DNS records."""
    if _is_debug_enabled(_SDAM_LOGGER):
        _debug_log(_SDAM_LOGGER, message="SRV monitor check failed", failure=repr(failure))


def log_command_retry(
    topology_id: Optional[ObjectId],
    command_name: str,
    operation_id: Optional[int],
    attempt_number: int,
    is_write: bool,
) -> None:
    """Emit a command-retry log entry."""
    if _is_debug_enabled(_COMMAND_LOGGER):
        op = "write" if is_write else "read"
        _debug_log(
            _COMMAND_LOGGER,
            message=f"Retrying {op} attempt number {attempt_number}",
            clientId=topology_id,
            commandName=command_name,
            operationId=operation_id,
        )
