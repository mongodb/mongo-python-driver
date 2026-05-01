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

"""Shared helpers for command monitoring and logging."""
from __future__ import annotations

import datetime
import logging
from typing import Any, Mapping

from pymongo.logger import _COMMAND_LOGGER, _CommandStatusMessage, _debug_log


def _log_command_started(
    client: Any,
    conn: Any,
    cmd: Mapping[str, Any],
    dbname: str,
    request_id: int,
    operation_id: int,
) -> None:
    if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
        _debug_log(
            _COMMAND_LOGGER,
            message=_CommandStatusMessage.STARTED,
            clientId=client._topology_settings._topology_id,
            command=cmd,
            commandName=next(iter(cmd)),
            databaseName=dbname,
            requestId=request_id,
            operationId=operation_id,
            driverConnectionId=conn.id,
            serverConnectionId=conn.server_connection_id,
            serverHost=conn.address[0],
            serverPort=conn.address[1],
            serviceId=conn.service_id,
        )


def _log_command_succeeded(
    client: Any,
    conn: Any,
    cmd: Mapping[str, Any],
    dbname: str,
    request_id: int,
    operation_id: int,
    reply: Any,
    duration: datetime.timedelta,
    speculative_authenticate: bool = False,
) -> None:
    if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
        _debug_log(
            _COMMAND_LOGGER,
            message=_CommandStatusMessage.SUCCEEDED,
            clientId=client._topology_settings._topology_id,
            durationMS=duration,
            reply=reply,
            commandName=next(iter(cmd)),
            databaseName=dbname,
            requestId=request_id,
            operationId=operation_id,
            driverConnectionId=conn.id,
            serverConnectionId=conn.server_connection_id,
            serverHost=conn.address[0],
            serverPort=conn.address[1],
            serviceId=conn.service_id,
            speculative_authenticate=speculative_authenticate,
        )


def _log_command_failed(
    client: Any,
    conn: Any,
    cmd: Mapping[str, Any],
    dbname: str,
    request_id: int,
    operation_id: int,
    failure: Any,
    duration: datetime.timedelta,
    is_server_side_error: bool,
) -> None:
    if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
        _debug_log(
            _COMMAND_LOGGER,
            message=_CommandStatusMessage.FAILED,
            clientId=client._topology_settings._topology_id,
            durationMS=duration,
            failure=failure,
            commandName=next(iter(cmd)),
            databaseName=dbname,
            requestId=request_id,
            operationId=operation_id,
            driverConnectionId=conn.id,
            serverConnectionId=conn.server_connection_id,
            serverHost=conn.address[0],
            serverPort=conn.address[1],
            serviceId=conn.service_id,
            isServerSideError=is_server_side_error,
        )
