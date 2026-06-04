# Copyright 2025-present MongoDB, Inc.
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

"""Unified per-command telemetry: logging, monitoring, and OpenTelemetry.

Currently wires the command logging channel; APM event publishing and
OpenTelemetry spans are layered on top of :class:`_CommandTelemetry`.
"""
from __future__ import annotations

import datetime
import logging
from typing import Any, Mapping, Optional

from pymongo.errors import NotPrimaryError, OperationFailure
from pymongo.logger import _COMMAND_LOGGER, _CommandStatusMessage, _debug_log
from pymongo.message import _convert_exception


class _CommandTelemetry:
    """Context manager for per-command telemetry.

    Currently wires the command *logging* channel only; the name reflects the
    intended scope as the APM and OpenTelemetry channels are added on top.

    Logs the ``STARTED`` event on entry, then ``SUCCEEDED`` or ``FAILED`` once
    the outcome is known. Call :meth:`handle_succeeded` with the server reply
    on success or :meth:`handle_failed` with the raised exception on error; if
    an exception propagates out of the ``with`` block without either being
    called, the ``FAILED`` event is logged automatically from ``__exit__``.

    This consolidates command *logging* only -- APM event publishing remains
    at the call site. The context manager owns the duration clock (from the
    ``start`` time passed in) and exposes it via :attr:`duration`, and stores
    the computed failure document on :attr:`failure`, so callers can reuse both
    for APM events. A future change can extend this class to publish monitoring
    (and OpenTelemetry) events alongside logging.

    Usage::

        with _CommandTelemetry(client, conn, cmd, dbname, request_id, start) as cmd_telemetry:
            reply = do_network_call()
            duration = cmd_telemetry.handle_succeeded(reply)
        # Failures are logged automatically in __exit__.
    """

    __slots__ = (
        "_client",
        "_conn",
        "_spec",
        "_dbname",
        "_request_id",
        "_operation_id",
        "_start",
        "duration",
        "failure",
        "_handled",
    )

    def __init__(
        self,
        client: Any,
        conn: Any,
        spec: Mapping[str, Any],
        dbname: str,
        request_id: int,
        start: datetime.datetime,
        operation_id: Optional[int] = None,
    ) -> None:
        self._client = client
        self._conn = conn
        self._spec = spec
        self._dbname = dbname
        self._request_id = request_id
        self._operation_id = request_id if operation_id is None else operation_id
        self._start = start
        self.duration: Optional[datetime.timedelta] = None
        self.failure: Any = None
        self._handled = False

    def _enabled(self) -> bool:
        return self._client is not None and _COMMAND_LOGGER.isEnabledFor(logging.DEBUG)

    def __enter__(self) -> _CommandTelemetry:
        if self._enabled():
            _debug_log(
                _COMMAND_LOGGER,
                message=_CommandStatusMessage.STARTED,
                clientId=self._client._topology_settings._topology_id,
                command=self._spec,
                commandName=next(iter(self._spec)),
                databaseName=self._dbname,
                requestId=self._request_id,
                operationId=self._operation_id,
                driverConnectionId=self._conn.id,
                serverConnectionId=self._conn.server_connection_id,
                serverHost=self._conn.address[0],
                serverPort=self._conn.address[1],
                serviceId=self._conn.service_id,
            )
        return self

    def handle_succeeded(
        self,
        reply: Any,
        speculative_hello: bool = False,
    ) -> datetime.timedelta:
        """Log the ``SUCCEEDED`` event and return the elapsed duration."""
        self.duration = datetime.datetime.now() - self._start
        if self._enabled():
            _debug_log(
                _COMMAND_LOGGER,
                message=_CommandStatusMessage.SUCCEEDED,
                clientId=self._client._topology_settings._topology_id,
                durationMS=self.duration,
                reply=reply,
                commandName=next(iter(self._spec)),
                databaseName=self._dbname,
                requestId=self._request_id,
                operationId=self._operation_id,
                driverConnectionId=self._conn.id,
                serverConnectionId=self._conn.server_connection_id,
                serverHost=self._conn.address[0],
                serverPort=self._conn.address[1],
                serviceId=self._conn.service_id,
                speculative_authenticate=speculative_hello,
            )
        self._handled = True
        return self.duration

    def handle_failed(
        self,
        exc: BaseException,
        failure: Optional[Any] = None,
        is_server_side_error: Optional[bool] = None,
    ) -> datetime.timedelta:
        """Log the ``FAILED`` event and return the elapsed duration.

        The failure document and server-side-error flag are derived from *exc*
        for the common case. Callers that must transform the failure document
        (e.g. unacknowledged bulk writes) pass *failure* explicitly. The
        computed failure is stored on :attr:`failure` for reuse by APM events.
        """
        self.duration = datetime.datetime.now() - self._start
        if failure is None:
            if isinstance(exc, (NotPrimaryError, OperationFailure)):
                failure = exc.details
            else:
                failure = _convert_exception(exc)  # type: ignore[arg-type]
        if is_server_side_error is None:
            is_server_side_error = isinstance(exc, OperationFailure)
        self.failure = failure
        if self._enabled():
            _debug_log(
                _COMMAND_LOGGER,
                message=_CommandStatusMessage.FAILED,
                clientId=self._client._topology_settings._topology_id,
                durationMS=self.duration,
                failure=failure,
                commandName=next(iter(self._spec)),
                databaseName=self._dbname,
                requestId=self._request_id,
                operationId=self._operation_id,
                driverConnectionId=self._conn.id,
                serverConnectionId=self._conn.server_connection_id,
                serverHost=self._conn.address[0],
                serverPort=self._conn.address[1],
                serviceId=self._conn.service_id,
                isServerSideError=is_server_side_error,
            )
        self._handled = True
        return self.duration

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        # Safety net: log a failure if an exception propagates without the
        # outcome having been recorded explicitly by the caller.
        if exc_val is not None and not self._handled:
            self.handle_failed(exc_val)
