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

"""Encoding and execution of commands over a connection.

The public :func:`command` entry point applies read preference, read concern,
collation, ``$clusterTime``, auto-encryption, and CSOT to a command spec,
encodes it as an OP_MSG message, and then delegates to :func:`run_command`.

Three public entry points each wrap the private :func:`_run_command`:

- :func:`run_command` — standard network-transport commands (acknowledged or
  unacknowledged). Called by :func:`command`.
- :func:`run_bulk_write_command` — collection-level and client-level bulk write
  batches (connection transport; pre-encrypted so decryption is skipped).
  Callers: ``bulk.py``, ``client_bulk.py``.
- :func:`run_cursor_command` — cursor ``find``/``getMore`` operations
  (connection transport, exhaust-cursor handling). Caller: ``server.py``.

:func:`_run_command` owns the entire shared skeleton: command logging, APM
event publishing, ``send``/``receive``, ``$clusterTime`` gossip,
``_process_response``, ``_check_command_response``, failure conversion, and
auto-encryption decryption. Each public wrapper hardcodes the transport and
response-shaping flags for its command type so callers only pass what varies.
"""

from __future__ import annotations

import datetime
import logging
from collections.abc import Mapping, MutableMapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Protocol,
    Union,
    cast,
)

from bson import _decode_all_selective
from pymongo import _csot, helpers_shared, message
from pymongo.compression_support import _NO_COMPRESSION
from pymongo.errors import NotPrimaryError, OperationFailure
from pymongo.logger import _COMMAND_LOGGER, _CommandStatusMessage, _debug_log
from pymongo.message import _convert_exception, _OpMsg
from pymongo.monitoring import _is_speculative_authenticate
from pymongo.network_layer import async_receive_message, async_sendall

if TYPE_CHECKING:
    from bson import CodecOptions
    from pymongo.asynchronous.client_session import AsyncClientSession
    from pymongo.asynchronous.mongo_client import AsyncMongoClient
    from pymongo.asynchronous.pool import AsyncConnection
    from pymongo.compression_support import SnappyContext, ZlibContext, ZstdContext
    from pymongo.monitoring import _EventListeners
    from pymongo.pool_options import PoolOptions
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import _ServerMode
    from pymongo.typings import _Address, _CollationIn, _DocumentOut, _DocumentType
    from pymongo.write_concern import WriteConcern

_IS_SYNC = False


class _BulkWriteContextProto(Protocol):
    """Structural interface for bulk write context objects passed to :func:`run_bulk_write_command`."""

    conn: AsyncConnection
    db_name: str
    session: Optional[AsyncClientSession]
    listeners: Optional[_EventListeners]
    start_time: datetime.datetime
    codec: CodecOptions[Any]
    op_id: int
    name: str


async def _run_command(
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
    unpack_res: Optional[Callable[..., Any]] = None,
    cursor_id: Optional[int] = None,
) -> tuple[list[dict[str, Any]], Optional[_OpMsg], datetime.timedelta]:
    """Send ``msg`` over ``conn`` and return ``(docs, reply, duration)``.

    Private shared implementation. Use :func:`run_command`,
    :func:`run_bulk_write_command`, or :func:`run_cursor_command` instead.

    It publishes the
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
    :param process_response: Run ``client._process_response`` on the response
        document before ``_check_command_response`` and APM/log events.
    :param decrypt_reply: Decrypt the reply when auto-encryption is enabled;
        the bulk paths pass False (their commands are encrypted up front).
    :param use_conn_transport: Send/receive via ``conn.send_message`` /
        ``conn.receive_message`` instead of the raw ``async_sendall`` /
        ``async_receive_message`` (network path).
    :param max_doc_size: The largest document size, for ``conn.send_message``.
    :param more_to_come: Receive only, without sending (exhaust ``getMore``).
    :param set_conn_more_to_come: Store ``reply.more_to_come`` on ``conn`` (the
        network/streaming-monitor path); the cursor path manages exhaust
        separately and must leave ``conn.more_to_come`` untouched.
    :param unpack_res: A callable decoding the wire response (cursor path); when
        ``None`` the reply's own ``unpack_response`` is used.
    :param cursor_id: The cursor id passed to ``unpack_res``.
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

    reply: Optional[_OpMsg]
    try:
        if more_to_come:
            reply = await conn.receive_message(None)
        elif unacknowledged:
            if use_conn_transport:
                conn._raise_if_not_writable()
                await conn.send_message(msg, max_doc_size)
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
                    user_fields=user_fields,
                )
            else:
                docs = reply.unpack_response(codec_options=codec_options, user_fields=user_fields)
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

    if client and client._encrypter and reply and decrypt_reply:
        decrypted = await client._encrypter.decrypt(reply.raw_command_response())
        docs = cast(
            "list[dict[str, Any]]", _decode_all_selective(decrypted, codec_options, user_fields)
        )

    return docs, reply, duration


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
    unacknowledged: bool = False,
    speculative_hello: bool = False,
) -> tuple[list[dict[str, Any]], Optional[_OpMsg], datetime.timedelta]:
    """Send a standard network-transport command and return ``(docs, reply, duration)``.

    Encodes as an OP_MSG message sent via ``async_sendall`` /
    ``async_receive_message``. For the bulk write path use
    :func:`run_bulk_write_command`; for cursors use :func:`run_cursor_command`.
    """
    return await _run_command(
        conn,
        cmd,
        dbname,
        request_id,
        msg,
        client=client,
        session=session,
        listeners=listeners,
        address=address,
        start=start,
        codec_options=codec_options,
        user_fields=user_fields,
        orig=orig,
        op_id=op_id,
        command_name=command_name,
        check=check,
        allowable_errors=allowable_errors,
        parse_write_concern_error=parse_write_concern_error,
        unacknowledged=unacknowledged,
        speculative_hello=speculative_hello,
        process_response=not unacknowledged,
        decrypt_reply=not unacknowledged,
    )


async def run_bulk_write_command(
    bwc: _BulkWriteContextProto,
    cmd: MutableMapping[str, Any],
    request_id: int,
    msg: bytes,
    *,
    client: Optional[AsyncMongoClient[Any]],
    orig: Optional[MutableMapping[str, Any]] = None,
    max_doc_size: int = 0,
    unacknowledged: bool = False,
) -> tuple[list[dict[str, Any]], Optional[_OpMsg], datetime.timedelta]:
    """Send a bulk write batch over the connection transport and return ``(docs, reply, duration)``.

    Uses ``conn.send_message`` / ``conn.receive_message`` (the bulk write
    path). Commands are encrypted up front so ``decrypt_reply`` is ``False``;
    bulk write replies never set ``more_to_come`` so ``set_conn_more_to_come``
    is ``False``.

    :param bwc: A bulk write context supplying the connection, session, listeners, etc.
    :param max_doc_size: The largest document size; passed to ``conn.send_message``.
    :param unacknowledged: When ``True``, send only and fake an ``{"ok": 1}`` reply.
    """
    return await _run_command(
        bwc.conn,
        cmd,
        bwc.db_name,
        request_id,
        msg,
        client=client,
        session=bwc.session,
        listeners=bwc.listeners,
        address=bwc.conn.address,
        start=bwc.start_time,
        codec_options=bwc.codec,
        op_id=bwc.op_id,
        command_name=bwc.name,
        orig=orig,
        max_doc_size=max_doc_size,
        unacknowledged=unacknowledged,
        use_conn_transport=True,
        decrypt_reply=False,
        set_conn_more_to_come=False,
        process_response=not unacknowledged,
    )


async def run_cursor_command(
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
    command_name: str,
    user_fields: Optional[Mapping[str, Any]] = None,
    pool_opts: Optional[PoolOptions] = None,
    max_doc_size: int = 0,
    more_to_come: bool = False,
    unpack_res: Optional[Callable[..., Any]] = None,
    cursor_id: Optional[int] = None,
) -> tuple[list[dict[str, Any]], Optional[_OpMsg], datetime.timedelta]:
    """Run a cursor ``find``/``getMore`` operation over ``conn``.

    Uses the connection transport, leaves ``conn.more_to_come`` untouched (the
    cursor path manages exhaust separately), and shapes the published reply in
    the find/getMore command response format.

    :param more_to_come: Receive only, without sending (exhaust ``getMore``).
    :param unpack_res: A callable decoding the wire response.
    :param cursor_id: The cursor id passed to ``unpack_res``.

    See :func:`_run_command` for the remaining parameters.
    """
    return await _run_command(
        conn,
        cmd,
        dbname,
        request_id,
        msg,
        client=client,
        session=session,
        listeners=listeners,
        address=address,
        start=start,
        codec_options=codec_options,
        user_fields=user_fields,
        command_name=command_name,
        pool_opts=pool_opts,
        ensure_db=True,
        use_conn_transport=True,
        max_doc_size=max_doc_size,
        more_to_come=more_to_come,
        set_conn_more_to_come=False,
        unpack_res=unpack_res,
        cursor_id=cursor_id,
    )


async def command(
    conn: AsyncConnection,
    dbname: str,
    spec: MutableMapping[str, Any],
    is_mongos: bool,  # noqa: ARG001
    read_preference: Optional[_ServerMode],
    codec_options: CodecOptions[_DocumentType],
    session: Optional[AsyncClientSession],
    client: Optional[AsyncMongoClient[Any]],
    check: bool = True,
    allowable_errors: Optional[Sequence[Union[str, int]]] = None,
    address: Optional[_Address] = None,
    listeners: Optional[_EventListeners] = None,
    max_bson_size: Optional[int] = None,
    read_concern: Optional[ReadConcern] = None,
    parse_write_concern_error: bool = False,
    collation: Optional[_CollationIn] = None,
    compression_ctx: Union[SnappyContext, ZlibContext, ZstdContext, None] = None,
    unacknowledged: bool = False,
    user_fields: Optional[Mapping[str, Any]] = None,
    exhaust_allowed: bool = False,
    write_concern: Optional[WriteConcern] = None,
) -> _DocumentType:
    """Encode and execute a command over ``conn``, or raise socket.error.

    Applies read preference, read concern, collation, ``$clusterTime``,
    auto-encryption, and CSOT to ``spec``, encodes it as an OP_MSG message,
    and then delegates the network round trip and response processing to
    :func:`run_command`.

    :param conn: a AsyncConnection instance
    :param dbname: name of the database on which to run the command
    :param spec: a command document as an ordered dict type, eg SON.
    :param is_mongos: are we connected to a mongos?
    :param read_preference: a read preference
    :param codec_options: a CodecOptions instance
    :param session: optional AsyncClientSession instance.
    :param client: optional AsyncMongoClient instance for updating $clusterTime.
    :param check: raise OperationFailure if there are errors
    :param allowable_errors: errors to ignore if `check` is True
    :param address: the (host, port) of `conn`
    :param listeners: An instance of :class:`~pymongo.monitoring.EventListeners`
    :param max_bson_size: The maximum encoded bson size for this server
    :param read_concern: The read concern for this command.
    :param parse_write_concern_error: Whether to parse the ``writeConcernError``
        field in the command response.
    :param collation: The collation for this command.
    :param compression_ctx: optional compression Context.
    :param unacknowledged: True if this is an unacknowledged command.
    :param user_fields: Response fields that should be decoded
        using the TypeDecoders from codec_options, passed to
        bson._decode_all_selective.
    :param exhaust_allowed: True if we should enable OP_MSG exhaustAllowed.
    """
    name = next(iter(spec))
    speculative_hello = False

    # Publish the original command document, perhaps with lsid and $clusterTime.
    orig = spec
    if read_concern and not (session and session.in_transaction):
        if read_concern.level:
            spec["readConcern"] = read_concern.document
        if session:
            session._update_read_concern(spec, conn)
    if collation is not None:
        spec["collation"] = collation

    publish = listeners is not None and listeners.enabled_for_commands
    start = datetime.datetime.now()
    if publish:
        speculative_hello = _is_speculative_authenticate(name, spec)

    if compression_ctx and name.lower() in _NO_COMPRESSION:
        compression_ctx = None

    if client and client._encrypter and not client._encrypter._bypass_auto_encryption:
        spec = orig = await client._encrypter.encrypt(dbname, spec, codec_options)

    # Support CSOT
    if client:
        conn.apply_timeout(client, spec)
    _csot.apply_write_concern(spec, write_concern)

    flags = _OpMsg.MORE_TO_COME if unacknowledged else 0
    flags |= _OpMsg.EXHAUST_ALLOWED if exhaust_allowed else 0
    request_id, msg, size, max_doc_size = message._op_msg(
        flags, spec, dbname, read_preference, codec_options, ctx=compression_ctx
    )
    # If this is an unacknowledged write then make sure the encoded doc(s)
    # are small enough, otherwise rely on the server to return an error.
    if unacknowledged and max_bson_size is not None and max_doc_size > max_bson_size:
        message._raise_document_too_large(name, size, max_bson_size)

    if max_bson_size is not None and size > max_bson_size + message._COMMAND_OVERHEAD:
        message._raise_document_too_large(name, size, max_bson_size + message._COMMAND_OVERHEAD)
    docs, _, _ = await run_command(
        conn,
        spec,
        dbname,
        request_id,
        msg,
        client=client,
        session=session,
        listeners=listeners,
        address=address,
        start=start,
        codec_options=codec_options,
        user_fields=user_fields,
        orig=orig,
        check=check,
        allowable_errors=allowable_errors,
        parse_write_concern_error=parse_write_concern_error,
        speculative_hello=speculative_hello,
        unacknowledged=unacknowledged,
    )
    return docs[0]  # type: ignore[return-value]
