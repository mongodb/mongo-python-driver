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

"""The single code path for executing a command over a connection.

Every database operation -- standard commands, cursor ``find``/``getMore``
operations, and (collection-level and client-level) bulk writes -- runs its
network round trip through :func:`run_command`. The function owns the entire
shared skeleton: command logging, APM event publishing, ``send``/``receive``,
``$clusterTime`` gossip, ``_process_response``, ``_check_command_response``,
failure conversion, and auto-encryption decryption. Callers supply only the
parts that vary (the encoded message and a handful of transport/output hooks).
"""
from __future__ import annotations

import datetime
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
    cast,
)

from bson import _decode_all_selective
from pymongo import helpers_shared
from pymongo.errors import NotPrimaryError, OperationFailure
from pymongo.logger import _COMMAND_LOGGER, _CommandStatusMessage, _debug_log
from pymongo.message import _convert_exception
from pymongo.network_layer import receive_message, sendall

if TYPE_CHECKING:
    from bson import CodecOptions
    from pymongo.message import _OpMsg, _OpReply
    from pymongo.monitoring import _EventListeners
    from pymongo.synchronous.client_session import ClientSession
    from pymongo.synchronous.mongo_client import MongoClient
    from pymongo.synchronous.pool import Connection
    from pymongo.typings import _Address, _DocumentOut, _DocumentType

_IS_SYNC = True


def run_command(
    conn: Connection,
    cmd: MutableMapping[str, Any],
    dbname: str,
    request_id: int,
    msg: bytes,
    *,
    client: Optional[MongoClient[Any]],
    session: Optional[ClientSession],
    listeners: Optional[_EventListeners],
    address: Optional[_Address],
    start: datetime.datetime,
    codec_options: CodecOptions[_DocumentType],
    user_fields: Optional[Mapping[str, Any]] = None,
    orig: Optional[MutableMapping[str, Any]] = None,
    op_id: Optional[int] = None,
    check: bool = True,
    allowable_errors: Optional[Sequence[Union[str, int]]] = None,
    parse_write_concern_error: bool = False,
    unacknowledged: bool = False,
    speculative_hello: bool = False,
) -> tuple[list[dict[str, Any]], Optional[Union[_OpReply, _OpMsg]], datetime.timedelta]:
    """Send ``msg`` over ``conn`` and return ``(docs, reply, duration)``.

    This is the single code path for command execution. It publishes the
    ``STARTED``/``SUCCEEDED``/``FAILED`` command log and APM events, performs
    the network round trip, gossips ``$clusterTime``, runs
    ``client._process_response`` and ``_check_command_response``, and decrypts
    the reply when auto-encryption is enabled.

    :param conn: The Connection to send on.
    :param cmd: The command document, used for the ``STARTED`` log event.
    :param dbname: The database the command runs against.
    :param request_id: The request id of the encoded message.
    :param msg: The encoded OP_MSG bytes to send.
    :param client: The MongoClient, for ``$clusterTime`` gossip, logging,
        and decryption. ``None`` disables those steps (e.g. during handshake).
    :param session: The session to update from the response.
    :param listeners: The event listeners, or ``None`` to disable APM.
    :param address: The (host, port) of ``conn`` for APM events.
    :param start: The ``datetime`` the operation began, for duration timing.
    :param codec_options: The CodecOptions used to decode the reply.
    :param user_fields: Response fields decoded with the codec's TypeDecoders.
    :param orig: The command document published in the ``STARTED`` APM event;
        defaults to ``cmd`` (differs only when the wire command was mutated,
        e.g. with a read preference or after encryption).
    :param op_id: The APM operation id; defaults to ``request_id``.
    :param check: Raise OperationFailure on a command error.
    :param allowable_errors: Errors to ignore when ``check`` is True.
    :param parse_write_concern_error: Parse the ``writeConcernError`` field.
    :param unacknowledged: True for an unacknowledged write: send only and fake
        an ``{"ok": 1}`` reply.
    :param speculative_hello: True if the command carried speculative auth, for
        APM redaction.
    """
    name = next(iter(cmd))
    if orig is None:
        orig = cmd
    publish = listeners is not None and listeners.enabled_for_commands

    if client is not None and _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
        _debug_log(
            _COMMAND_LOGGER,
            message=_CommandStatusMessage.STARTED,
            clientId=client._topology_settings._topology_id,
            command=cmd,
            commandName=name,
            databaseName=dbname,
            requestId=request_id,
            operationId=request_id,
            driverConnectionId=conn.id,
            serverConnectionId=conn.server_connection_id,
            serverHost=conn.address[0],
            serverPort=conn.address[1],
            serviceId=conn.service_id,
        )
    if publish:
        assert listeners is not None
        assert address is not None
        listeners.publish_command_start(
            orig,
            dbname,
            request_id,
            address,
            conn.server_connection_id,
            op_id,
            service_id=conn.service_id,
        )

    try:
        sendall(conn.conn.get_conn, msg)
        if unacknowledged:
            # Unacknowledged, fake a successful command response.
            reply = None
            docs: list[dict[str, Any]] = [{"ok": 1}]
        else:
            reply = receive_message(conn, request_id)
            conn.more_to_come = reply.more_to_come
            docs = reply.unpack_response(codec_options=codec_options, user_fields=user_fields)
            response_doc = docs[0]
            if not conn.ready:
                cluster_time = response_doc.get("$clusterTime")
                if cluster_time:
                    conn._cluster_time = cluster_time
            if client:
                client._process_response(response_doc, session)
            if check:
                helpers_shared._check_command_response(
                    response_doc,
                    conn.max_wire_version,
                    allowable_errors,
                    parse_write_concern_error=parse_write_concern_error,
                )
    except Exception as exc:
        duration = datetime.datetime.now() - start
        if isinstance(exc, (NotPrimaryError, OperationFailure)):
            failure: _DocumentOut = exc.details  # type: ignore[assignment]
        else:
            failure = _convert_exception(exc)
        if client is not None and _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _COMMAND_LOGGER,
                message=_CommandStatusMessage.FAILED,
                clientId=client._topology_settings._topology_id,
                durationMS=duration,
                failure=failure,
                commandName=name,
                databaseName=dbname,
                requestId=request_id,
                operationId=request_id,
                driverConnectionId=conn.id,
                serverConnectionId=conn.server_connection_id,
                serverHost=conn.address[0],
                serverPort=conn.address[1],
                serviceId=conn.service_id,
                isServerSideError=isinstance(exc, OperationFailure),
            )
        if publish:
            assert listeners is not None
            assert address is not None
            listeners.publish_command_failure(
                duration,
                failure,
                name,
                request_id,
                address,
                conn.server_connection_id,
                op_id,
                service_id=conn.service_id,
                database_name=dbname,
            )
        raise

    duration = datetime.datetime.now() - start
    response_doc = docs[0]
    if client is not None and _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
        _debug_log(
            _COMMAND_LOGGER,
            message=_CommandStatusMessage.SUCCEEDED,
            clientId=client._topology_settings._topology_id,
            durationMS=duration,
            reply=response_doc,
            commandName=name,
            databaseName=dbname,
            requestId=request_id,
            operationId=request_id,
            driverConnectionId=conn.id,
            serverConnectionId=conn.server_connection_id,
            serverHost=conn.address[0],
            serverPort=conn.address[1],
            serviceId=conn.service_id,
            speculative_authenticate="speculativeAuthenticate" in orig,
        )
    if publish:
        assert listeners is not None
        assert address is not None
        listeners.publish_command_success(
            duration,
            response_doc,
            name,
            request_id,
            address,
            conn.server_connection_id,
            op_id,
            service_id=conn.service_id,
            speculative_hello=speculative_hello,
            database_name=dbname,
        )

    if client and client._encrypter and reply:
        decrypted = client._encrypter.decrypt(reply.raw_command_response())
        docs = cast(
            "list[dict[str, Any]]", _decode_all_selective(decrypted, codec_options, user_fields)
        )

    return docs, reply, duration
