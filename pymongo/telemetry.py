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

"""Unified telemetry support for PyMongo.

Supports telemetry through standardized logging, event publishing, and OpenTelemetry.

To enable OpenTelemetry logging, set the environment variable:
    OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED=true

.. versionadded:: 4.x
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping, Optional

from pymongo import message
from pymongo.errors import NotPrimaryError, OperationFailure
from pymongo.logger import _COMMAND_LOGGER, _SENSITIVE_COMMANDS, _CommandStatusMessage, _debug_log
from pymongo.monitoring import _EventListeners

try:
    from opentelemetry import trace  # type: ignore[import-not-found]
    from opentelemetry.trace import (  # type: ignore[import-not-found]
        Span,
        SpanKind,
        Status,
        StatusCode,
        Tracer,
    )

    _HAS_OPENTELEMETRY = True
except ImportError:
    _HAS_OPENTELEMETRY = False
    trace = None
    Span = None
    SpanKind = None
    Status = None
    StatusCode = None
    Tracer = None

if TYPE_CHECKING:
    from pymongo.typings import _Address, _AgnosticMongoClient, _DocumentOut


# Environment variable names
_OTEL_ENABLED_ENV = "OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED"


def _is_tracing_enabled() -> bool:
    """Check if tracing is enabled via environment variable."""
    if not _HAS_OPENTELEMETRY:
        return False
    value = os.environ.get(_OTEL_ENABLED_ENV, "").lower()
    return value in ("1", "true")


def _get_tracer() -> Optional[Tracer]:
    """Get the PyMongo tracer instance."""
    if not _HAS_OPENTELEMETRY or not _is_tracing_enabled():
        return None
    from pymongo._version import __version__

    return trace.get_tracer("PyMongo", __version__)


def _is_sensitive_command(command_name: str) -> bool:
    """Check if a command is sensitive and should not be traced."""
    return command_name.lower() in _SENSITIVE_COMMANDS


def _build_query_summary(
    command_name: str,
    database_name: str,
    collection_name: Optional[str],
) -> str:
    """Build the db.query.summary attribute value."""
    if collection_name:
        return f"{command_name} {database_name}.{collection_name}"
    return f"{command_name} {database_name}"


def _extract_collection_name(spec: Mapping[str, Any]) -> Optional[str]:
    """Extract collection name from command spec if applicable."""
    if not spec:
        return None
    cmd_name = next(iter(spec)).lower()
    # Commands where the first value is the collection name
    if cmd_name in (
        "insert",
        "update",
        "delete",
        "find",
        "aggregate",
        "findandmodify",
        "count",
        "distinct",
        "create",
        "drop",
        "createindexes",
        "dropindexes",
        "listindexes",
    ):
        value = spec.get(next(iter(spec)))
        if isinstance(value, str):
            return value
    return None


class _CommandTelemetry:
    """Manages telemetry for MongoDB commands, including logging, event publishing, and OpenTelemetry spans.

    This class is a context manager that handles the full lifecycle of command telemetry:
    - On entry: sets up OpenTelemetry span and publishes the started event
    - On exit: cleans up the span context (caller handles success/failure publishing)
    """

    __slots__ = (
        "_command_name",
        "_database_name",
        "_spec",
        "_driver_connection_id",
        "_server_connection_id",
        "_publish_event",
        "_start_time",
        "_address",
        "_listeners",
        "_client",
        "_request_id",
        "_operation_id",
        "_service_id",
        "_span",
        "_span_context",
    )

    def __init__(
        self,
        command_name: str,
        database_name: str,
        spec: Mapping[str, Any],
        driver_connection_id: int,
        server_connection_id: Optional[int],
        publish_event: bool,
        start_time: datetime,
        address: _Address,
        listeners: Optional[_EventListeners],
        client: Optional[_AgnosticMongoClient],
        request_id: int,
        service_id: Optional[Any],
        operation_id: Optional[int] = None,
    ):
        self._command_name = command_name
        self._database_name = database_name
        self._spec = spec
        self._driver_connection_id = driver_connection_id
        self._server_connection_id = server_connection_id
        self._publish_event = publish_event
        self._start_time = start_time
        self._address = address
        self._listeners = listeners
        self._client = client
        self._request_id = request_id
        self._operation_id = operation_id if operation_id is not None else request_id
        self._service_id = service_id
        self._span: Optional[Span] = None
        self._span_context: Optional[Any] = None

    def __enter__(self) -> _CommandTelemetry:
        """Enter the telemetry context: set up span and publish started event."""
        self._setup_span()
        self.publish_started()
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Exit the telemetry context: clean up span context."""
        if self._span_context is not None:
            self._span_context.__exit__(exc_type, exc_val, exc_tb)

    def _setup_span(self) -> None:
        """Set up OpenTelemetry span if tracing is enabled and command is not sensitive."""
        tracer = _get_tracer()

        if tracer is None or _is_sensitive_command(self._command_name):
            return

        collection_name = _extract_collection_name(self._spec)
        query_summary = _build_query_summary(
            self._command_name, self._database_name, collection_name
        )

        self._span_context = tracer.start_as_current_span(
            name=self._command_name,
            kind=SpanKind.CLIENT,
        )
        self._span = self._span_context.__enter__()

        # Set span attributes
        self._span.set_attribute("db.system", "mongodb")
        self._span.set_attribute("db.namespace", self._database_name)
        self._span.set_attribute("db.command.name", self._command_name)
        self._span.set_attribute("db.query.summary", query_summary)
        if self._address:
            self._span.set_attribute("server.address", self._address[0])
            self._span.set_attribute("server.port", self._address[1])
        self._span.set_attribute("network.transport", "tcp")
        self._span.set_attribute("db.mongodb.driver_connection_id", self._driver_connection_id)

        if collection_name:
            self._span.set_attribute("db.collection.name", collection_name)
        if self._server_connection_id is not None:
            self._span.set_attribute("db.mongodb.server_connection_id", self._server_connection_id)

    @property
    def span(self) -> Optional[Span]:
        """Return the OpenTelemetry span, or None if tracing is disabled."""
        return self._span

    def publish_started(self) -> None:
        """Publish command started event and log."""
        if self._client is not None:
            if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _COMMAND_LOGGER,
                    message=_CommandStatusMessage.STARTED,
                    clientId=self._client._topology_settings._topology_id,
                    command=self._spec,
                    commandName=next(iter(self._spec)),
                    databaseName=self._database_name,
                    requestId=self._request_id,
                    operationId=self._operation_id,
                    driverConnectionId=self._driver_connection_id,
                    serverConnectionId=self._server_connection_id,
                    serverHost=self._address[0] if self._address else None,
                    serverPort=self._address[1] if self._address else None,
                    serviceId=self._service_id,
                )
        if self._publish_event:
            assert self._listeners is not None
            assert self._address is not None
            self._listeners.publish_command_start(
                self._spec,  # type: ignore[arg-type]
                self._database_name,
                self._request_id,
                self._address,
                self._server_connection_id,
                op_id=self._operation_id,
                service_id=self._service_id,
            )

    def publish_succeeded(
        self,
        reply: _DocumentOut,
        speculative_hello: bool = False,
        speculative_authenticate: bool = False,
    ) -> None:
        """Publish command succeeded event and log."""
        duration = datetime.now() - self._start_time

        # Add cursor_id to span if present in response
        if self._span is not None and isinstance(reply, dict):
            cursor_info = reply.get("cursor")
            if cursor_info and isinstance(cursor_info, dict):
                cursor_id = cursor_info.get("id", 0)
                if cursor_id:
                    self._span.set_attribute("db.mongodb.cursor_id", cursor_id)

        if self._client is not None:
            if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _COMMAND_LOGGER,
                    message=_CommandStatusMessage.SUCCEEDED,
                    clientId=self._client._topology_settings._topology_id,
                    durationMS=duration,
                    reply=reply,
                    commandName=next(iter(self._spec)),
                    databaseName=self._database_name,
                    requestId=self._request_id,
                    operationId=self._operation_id,
                    driverConnectionId=self._driver_connection_id,
                    serverConnectionId=self._server_connection_id,
                    serverHost=self._address[0] if self._address else None,
                    serverPort=self._address[1] if self._address else None,
                    serviceId=self._service_id,
                    speculative_authenticate=speculative_authenticate,
                )
        if self._publish_event:
            assert self._listeners is not None
            assert self._address is not None
            self._listeners.publish_command_success(
                duration,
                reply,
                self._command_name,
                self._request_id,
                self._address,
                self._server_connection_id,
                op_id=self._operation_id,
                service_id=self._service_id,
                speculative_hello=speculative_hello,
                database_name=self._database_name,
            )

    def publish_failed(self, exc: Exception) -> None:
        """Publish command failed event and log."""
        duration = datetime.now() - self._start_time
        if isinstance(exc, (NotPrimaryError, OperationFailure)):
            failure: _DocumentOut = exc.details  # type: ignore[assignment]
        else:
            failure = message._convert_exception(exc)

        if self._span is not None:
            error_code = getattr(exc, "code", None)
            self._span.record_exception(exc)
            self._span.set_status(Status(StatusCode.ERROR, str(exc)))

            if error_code is not None:
                self._span.set_attribute("db.response.status_code", str(error_code))
        if self._client is not None:
            if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _COMMAND_LOGGER,
                    message=_CommandStatusMessage.FAILED,
                    clientId=self._client._topology_settings._topology_id,
                    durationMS=duration,
                    failure=failure,
                    commandName=next(iter(self._spec)),
                    databaseName=self._database_name,
                    requestId=self._request_id,
                    operationId=self._operation_id,
                    driverConnectionId=self._driver_connection_id,
                    serverConnectionId=self._server_connection_id,
                    serverHost=self._address[0] if self._address else None,
                    serverPort=self._address[1] if self._address else None,
                    serviceId=self._service_id,
                    isServerSideError=isinstance(exc, OperationFailure),
                )
        if self._publish_event:
            assert self._listeners is not None
            assert self._address is not None
            self._listeners.publish_command_failure(
                duration,
                failure,
                self._command_name,
                self._request_id,
                self._address,
                self._server_connection_id,
                op_id=self._operation_id,
                service_id=self._service_id,
                database_name=self._database_name,
            )


def command_telemetry(
    command_name: str,
    database_name: str,
    spec: Mapping[str, Any],
    driver_connection_id: int,
    server_connection_id: Optional[int],
    publish_event: bool,
    start_time: datetime,
    request_id: int,
    address: _Address,
    listeners: Optional[_EventListeners] = None,
    client: Optional[_AgnosticMongoClient] = None,
    service_id: Optional[Any] = None,
    operation_id: Optional[int] = None,
) -> _CommandTelemetry:
    """Create a _CommandTelemetry context manager for command telemetry.

    Returns a _CommandTelemetry instance that should be used as a context manager.
    The context manager automatically:
    - Sets up OpenTelemetry span if tracing is enabled and command is not sensitive
    - Publishes the started event on entry
    - Cleans up the span context on exit

    The caller is responsible for calling publish_succeeded() on successful completion
    and publish_failed() if an exception occurs.

    Example usage::

        with command_telemetry(...) as telemetry:
            try:
                # execute command
                result = execute_command()
            except Exception as exc:
                telemetry.publish_failed(exc)
                raise
            telemetry.publish_succeeded(result)
    """
    return _CommandTelemetry(
        command_name=command_name,
        database_name=database_name,
        spec=spec,
        driver_connection_id=driver_connection_id,
        server_connection_id=server_connection_id,
        publish_event=publish_event,
        start_time=start_time,
        address=address,
        listeners=listeners,
        client=client,
        request_id=request_id,
        service_id=service_id,
        operation_id=operation_id,
    )
