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

"""Internal telemetry helpers for unified logging and APM event publishing."""
from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING, Any, Optional

from pymongo.errors import NotPrimaryError, OperationFailure
from pymongo.logger import (
    _COMMAND_LOGGER,
    _CommandStatusMessage,
    _debug_log,
)
from pymongo.message import _convert_exception

if TYPE_CHECKING:
    from bson import ObjectId
    from pymongo.monitoring import _EventListeners
    from pymongo.typings import _Address, _DocumentOut


class _CommandTelemetry:
    """Unified context manager for command-level telemetry.

    Consolidates debug logging and APM event publishing into a single
    context manager, eliminating duplicated telemetry code at each call site.

    On entry, publishes the command-started event to all enabled channels.
    Call :meth:`handle_succeeded` with the server reply on success.
    On exit, if an exception is propagating and the outcome has not already
    been recorded, the command-failed event is published automatically.

    Usage::

        with _CommandTelemetry(
            topology_id=client._topology_id,
            command_name=name,
            database_name=dbname,
            spec=spec,
            orig=orig,
            driver_connection_id=conn.id,
            server_connection_id=conn.server_connection_id,
            service_id=conn.service_id,
            address=address,
            listeners=listeners if publish else None,
            request_id=request_id,
        ) as cmd_telemetry:
            reply = do_network_call()
            cmd_telemetry.handle_succeeded(reply)
        # Failures are published automatically in __exit__.
    """

    __slots__ = (
        "_topology_id",
        "_command_name",
        "_database_name",
        "_spec",
        "_orig",
        "_driver_connection_id",
        "_server_connection_id",
        "_service_id",
        "_address",
        "_listeners",
        "_request_id",
        "_operation_id",
        "_start_time",
        "_handled",
    )

    def __init__(
        self,
        topology_id: Optional[ObjectId],
        command_name: str,
        database_name: str,
        spec: Any,
        orig: Any,
        driver_connection_id: int,
        server_connection_id: Optional[int],
        service_id: Optional[ObjectId],
        address: Optional[_Address],
        listeners: Optional[_EventListeners],
        request_id: int,
        operation_id: Optional[int] = None,
    ) -> None:
        self._topology_id = topology_id
        self._command_name = command_name
        self._database_name = database_name
        self._spec = spec
        self._orig = orig
        self._driver_connection_id = driver_connection_id
        self._server_connection_id = server_connection_id
        self._service_id = service_id
        self._address = address
        self._listeners = listeners
        self._request_id = request_id
        self._operation_id = operation_id
        self._start_time: Optional[datetime.datetime] = None
        self._handled = False

    def __enter__(self) -> _CommandTelemetry:
        self._start_time = datetime.datetime.now()
        if self._topology_id is not None and _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _COMMAND_LOGGER,
                message=_CommandStatusMessage.STARTED,
                clientId=self._topology_id,
                command=self._spec,
                commandName=next(iter(self._spec)),
                databaseName=self._database_name,
                requestId=self._request_id,
                operationId=self._request_id,
                driverConnectionId=self._driver_connection_id,
                serverConnectionId=self._server_connection_id,
                serverHost=self._address[0] if self._address else None,
                serverPort=self._address[1] if self._address else None,
                serviceId=self._service_id,
            )
        if self._listeners is not None:
            assert self._address is not None
            self._listeners.publish_command_start(
                self._orig,
                self._database_name,
                self._request_id,
                self._address,
                self._server_connection_id,
                op_id=self._operation_id,
                service_id=self._service_id,
            )
        return self

    def handle_succeeded(
        self,
        reply: Any,
        speculative_hello: bool = False,
    ) -> None:
        """Publish command-succeeded telemetry.

        Must be called explicitly by the caller when the command succeeds.
        Sets the internal *handled* flag so that ``__exit__`` does not also
        publish a failure event when the ``with`` block exits normally.
        """
        assert self._start_time is not None
        duration = datetime.datetime.now() - self._start_time
        if self._topology_id is not None and _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _COMMAND_LOGGER,
                message=_CommandStatusMessage.SUCCEEDED,
                clientId=self._topology_id,
                durationMS=duration,
                reply=reply,
                commandName=next(iter(self._spec)),
                databaseName=self._database_name,
                requestId=self._request_id,
                operationId=self._request_id,
                driverConnectionId=self._driver_connection_id,
                serverConnectionId=self._server_connection_id,
                serverHost=self._address[0] if self._address else None,
                serverPort=self._address[1] if self._address else None,
                serviceId=self._service_id,
                speculative_authenticate="speculativeAuthenticate" in self._orig,
            )
        if self._listeners is not None:
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
        self._handled = True

    def handle_failed(self, exc: BaseException) -> None:
        """Publish command-failed telemetry.

        Called automatically by ``__exit__`` when an exception propagates
        out of the ``with`` block.  May also be called explicitly in cases
        where the exception is caught inside the ``with`` block (e.g. when
        the caller must not re-raise).
        """
        assert self._start_time is not None
        duration = datetime.datetime.now() - self._start_time
        if isinstance(exc, (NotPrimaryError, OperationFailure)):
            failure: _DocumentOut = exc.details  # type: ignore[assignment]
        else:
            failure = _convert_exception(exc)  # type: ignore[arg-type]
        if self._topology_id is not None and _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _COMMAND_LOGGER,
                message=_CommandStatusMessage.FAILED,
                clientId=self._topology_id,
                durationMS=duration,
                failure=failure,
                commandName=next(iter(self._spec)),
                databaseName=self._database_name,
                requestId=self._request_id,
                operationId=self._request_id,
                driverConnectionId=self._driver_connection_id,
                serverConnectionId=self._server_connection_id,
                serverHost=self._address[0] if self._address else None,
                serverPort=self._address[1] if self._address else None,
                serviceId=self._service_id,
                isServerSideError=isinstance(exc, OperationFailure),
            )
        if self._listeners is not None:
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
        self._handled = True

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        if exc_val is not None and not self._handled:
            self.handle_failed(exc_val)
