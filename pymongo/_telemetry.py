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
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Optional

from pymongo.logger import _COMMAND_LOGGER, _CommandStatusMessage, _debug_log
from pymongo.pool_shared import _ConnectionTelemetryInfo

if TYPE_CHECKING:
    from bson.objectid import ObjectId
    from pymongo.monitoring import _EventListeners
    from pymongo.typings import _DocumentOut


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
