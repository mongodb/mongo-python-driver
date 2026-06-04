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

Every database operation -- standard commands and cursor ``find``/``getMore``
operations -- runs its network round trip through :func:`run_command`. The
function owns the entire shared skeleton: command logging, APM event
publishing, ``send``/``receive``, ``$clusterTime`` gossip,
``_process_response``, ``_check_command_response``, failure conversion, and
auto-encryption decryption. Callers supply only the parts that vary (the
encoded message and a handful of transport/output hooks).
"""
from __future__ import annotations

import datetime
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
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
from pymongo.network_layer import async_receive_message, async_sendall

if TYPE_CHECKING:
    from bson import CodecOptions
    from pymongo.asynchronous.client_session import AsyncClientSession
    from pymongo.asynchronous.mongo_client import AsyncMongoClient
    from pymongo.asynchronous.pool import AsyncConnection
    from pymongo.message import _OpMsg, _OpReply
    from pymongo.monitoring import _EventListeners
    from pymongo.pool_options import PoolOptions
    from pymongo.typings import _Address, _DocumentOut, _DocumentType

_IS_SYNC = False


async def run_command(
    conn: AsyncConnection,
    cmd: MutableMapping[str, Any],
    dbname: str,
    request_id: int,
    msg: bytes,
    *,
    client: Optional[AsyncMongoClient[Any]],
    session: Optional[AsyncClientSession],
    listeners: Optional[_EventListeners],
    address: Optional[_Address],
    start: datetime.datetime,
    codec_options: CodecOptions[_DocumentType],
    user_fields: Optional[Mapping[str, Any]] = None,
    orig: Optional[MutableMapping[str, Any]] = None,
    op_id: Optional[int] = None,
    command_name: Optional[str] = None,
    check: bool = True,
    allowable_errors: Optional[Sequence[Union[str, int]]] = None,
    parse_write_concern_error: bool = False,
    pool_opts: Optional[PoolOptions] = None,
    unacknowledged: bool = False,
    speculative_hello: bool = False,
    ensure_db: bool = False,
    process_response: bool = True,
    decrypt_reply: bool = True,
    use_conn_transport: bool = False,
    max_doc_size: int = 0,
    more_to_come: bool = False,
    set_conn_more_to_come: bool = True,
    is_command_response: bool = True,
    unpack_res: Optional[Callable[..., Any]] = None,
    cursor_id: Optional[int] = None,
    reply_doc_builder: Optional[
        Callable[[list[dict[str, Any]], Optional[Union[_OpReply, _OpMsg]]], _DocumentOut]
    ] = None,
) -> tuple[list[dict[str, Any]], Optional[Union[_OpReply, _OpMsg]], datetime.timedelta]:
    """Send ``msg`` over ``conn`` and return ``(docs, reply, duration)``.

    This is the single code path for command execution. It publishes the
    ``STARTED``/``SUCCEEDED``/``FAILED`` command log and APM events, performs
    the network round trip, gossips ``$clusterTime``, runs
    ``client._process_response`` and ``_check_command_response``, and decrypts
    the reply when auto-encryption is enabled.

    :param conn: The AsyncConnection to send on.
    :param cmd: The command document, used for the ``STARTED`` log/APM event.
    :param dbname: The database the command runs against.
    :param request_id: The request id of the encoded message (``0`` when
        ``more_to_come`` and no message is sent).
    :param msg: The encoded bytes to send (ignored when ``more_to_come``).
    :param client: The AsyncMongoClient, for ``$clusterTime`` gossip, logging,
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
    :param command_name: The command name for the ``SUCCEEDED``/``FAILED`` APM
        events; defaults to the first key of ``cmd``.
    :param check: Raise OperationFailure on a command error.
    :param allowable_errors: Errors to ignore when ``check`` is True.
    :param parse_write_concern_error: Parse the ``writeConcernError`` field.
    :param pool_opts: PoolOptions forwarded to ``_check_command_response`` (the
        cursor path uses this in place of ``allowable_errors``).
    :param unacknowledged: True for an unacknowledged write: send only and fake
        an ``{"ok": 1}`` reply.
    :param speculative_hello: True if the command carried speculative auth, for
        APM redaction.
    :param ensure_db: Add ``$db`` to the published command if missing (cursor
        path), after the ``STARTED`` log has been emitted.
    :param process_response: Run ``client._process_response`` on success here;
        the bulk paths pass False and process the reply at the call site to
        keep their check -> APM-succeed -> process ordering.
    :param decrypt_reply: Decrypt the reply when auto-encryption is enabled;
        the bulk paths pass False (their commands are encrypted up front).
    :param use_conn_transport: Send/receive via ``conn.send_message`` /
        ``conn.receive_message`` (cursor path) or ``conn.unack_write`` (bulk
        unacknowledged) instead of the raw ``async_sendall`` /
        ``async_receive_message`` (network path).
    :param max_doc_size: The largest document size, for ``conn.send_message``.
    :param more_to_come: Receive only, without sending (exhaust ``getMore``).
    :param set_conn_more_to_come: Store ``reply.more_to_come`` on ``conn`` (the
        network/streaming-monitor path); the cursor path manages exhaust
        separately and must leave ``conn.more_to_come`` untouched.
    :param is_command_response: True if the reply is an OP_MSG command response
        (``_process_response``/``_check_command_response``/decryption apply);
        False for a legacy OP_QUERY cursor response.
    :param unpack_res: A callable decoding the wire response (cursor path); when
        ``None`` the reply's own ``unpack_response`` is used.
    :param cursor_id: The cursor id passed to ``unpack_res``.
    :param reply_doc_builder: Builds the reply document published in the
        ``SUCCEEDED`` event from ``(docs, reply)`` (cursor find/getMore format);
        when ``None`` the first decoded document is published.
    """
    name = next(iter(cmd))
    if command_name is None:
        command_name = name
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
        if ensure_db and "$db" not in orig:
            orig["$db"] = dbname
        listeners.publish_command_start(
            orig,
            dbname,
            request_id,
            address,
            conn.server_connection_id,
            op_id,
            service_id=conn.service_id,
        )

    reply: Optional[Union[_OpReply, _OpMsg]]
    try:
        if more_to_come:
            reply = await conn.receive_message(None)
        elif unacknowledged:
            if use_conn_transport:
                await conn.unack_write(msg, max_doc_size)
            else:
                await async_sendall(conn.conn.get_conn, msg)
            # Unacknowledged, fake a successful command response.
            reply = None
            docs: list[dict[str, Any]] = [{"ok": 1}]
        elif use_conn_transport:
            if session is not None and session._starting_transaction:
                session._transaction.set_in_progress()
            await conn.send_message(msg, max_doc_size)
            reply = await conn.receive_message(request_id)
        else:
            await async_sendall(conn.conn.get_conn, msg)
            reply = await async_receive_message(conn, request_id)

        if reply is not None:
            if set_conn_more_to_come:
                conn.more_to_come = reply.more_to_come
            if unpack_res is not None:
                docs = unpack_res(
                    reply,
                    cursor_id,
                    codec_options,
                    legacy_response=not is_command_response,
                    user_fields=user_fields,
                )
            else:
                docs = reply.unpack_response(codec_options=codec_options, user_fields=user_fields)
            if is_command_response:
                response_doc = docs[0]
                if not conn.ready:
                    cluster_time = response_doc.get("$clusterTime")
                    if cluster_time:
                        conn._cluster_time = cluster_time
                if process_response and client:
                    await client._process_response(response_doc, session)
                if check:
                    helpers_shared._check_command_response(
                        response_doc,
                        conn.max_wire_version,
                        allowable_errors,
                        parse_write_concern_error=parse_write_concern_error,
                        pool_opts=pool_opts,
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
                command_name,
                request_id,
                address,
                conn.server_connection_id,
                op_id,
                service_id=conn.service_id,
                database_name=dbname,
            )
        raise

    duration = datetime.datetime.now() - start
    published_reply: _DocumentOut
    if reply_doc_builder is not None:
        published_reply = reply_doc_builder(docs, reply)
    else:
        published_reply = docs[0]
    if client is not None and _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
        _debug_log(
            _COMMAND_LOGGER,
            message=_CommandStatusMessage.SUCCEEDED,
            clientId=client._topology_settings._topology_id,
            durationMS=duration,
            reply=published_reply,
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
            published_reply,
            command_name,
            request_id,
            address,
            conn.server_connection_id,
            op_id,
            service_id=conn.service_id,
            speculative_hello=speculative_hello,
            database_name=dbname,
        )

    if client and client._encrypter and reply and is_command_response and decrypt_reply:
        decrypted = await client._encrypter.decrypt(reply.raw_command_response())
        docs = cast(
            "list[dict[str, Any]]", _decode_all_selective(decrypted, codec_options, user_fields)
        )

    return docs, reply, duration
