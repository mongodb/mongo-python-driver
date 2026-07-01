# Copyright 2015-present MongoDB, Inc.
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

"""Encoding and execution of commands over a connection (previously ``network.py``).

Three public entry points each wrap the private :func:`_run_command`:

- :func:`run_command` — the standard entry point used by ``pool.py``. Applies
  read concern, collation, ``$clusterTime``, compression, auto-encryption, and
  CSOT to a raw spec dict, encodes it as OP_MSG, then delegates to
  :func:`_run_command`.
- :func:`run_bulk_write_command` — collection-level and client-level bulk write
  batches. Pre-encrypted, so decryption is skipped. Callers: ``bulk.py``,
  ``client_bulk.py``.
- :func:`run_cursor_command` — cursor ``find``/``getMore`` operations with
  exhaust-cursor handling. Caller: ``server.py``.

:func:`_run_command` owns the entire shared skeleton: command logging, APM
event publishing, ``send``/``receive``, ``$clusterTime`` gossip,
``_process_response``, ``_check_command_response``, failure conversion, and
auto-encryption decryption. Each public wrapper hardcodes the flags for its
command type so callers only pass what varies.
"""

from __future__ import annotations

import datetime
from collections.abc import Mapping, MutableMapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Union,
    cast,
)

from bson import _decode_all_selective
from pymongo import _csot, helpers_shared, message
from pymongo._telemetry import _CommandTelemetry
from pymongo.compression_support import _NO_COMPRESSION
from pymongo.errors import NotPrimaryError, OperationFailure
from pymongo.message import _BulkWriteContextBase, _convert_exception, _OpMsg
from pymongo.monitoring import _is_speculative_authenticate

if TYPE_CHECKING:
    from bson import CodecOptions
    from bson.objectid import ObjectId
    from pymongo.compression_support import SnappyContext, ZlibContext, ZstdContext
    from pymongo.monitoring import _EventListeners
    from pymongo.pool_options import PoolOptions
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import _ServerMode
    from pymongo.synchronous.client_session import ClientSession
    from pymongo.synchronous.mongo_client import MongoClient
    from pymongo.synchronous.pool import Connection
    from pymongo.typings import _CollationIn, _DocumentOut, _DocumentType
    from pymongo.write_concern import WriteConcern

_IS_SYNC = True


def _run_command(
    conn: Connection,
    cmd: MutableMapping[str, Any],
    dbname: str,
    request_id: int,
    msg: bytes,
    *,
    client: Optional[MongoClient[Any]],
    session: Optional[ClientSession],
    listeners: Optional[_EventListeners],
    topology_id: Optional[ObjectId],
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
    decrypt_reply: bool = True,
    max_doc_size: int = 0,
    more_to_come: bool = False,
    set_conn_more_to_come: bool = False,
    unpack_res: Optional[Callable[..., Any]] = None,
    cursor_id: Optional[int] = None,
) -> tuple[list[dict[str, Any]], Optional[_OpMsg], datetime.timedelta]:
    """Send ``msg`` over ``conn`` and return ``(docs, reply, duration)``.

    Private shared implementation. Should not be called directly outside this module. Use :func:`run_command`, :func:`run_bulk_write_command`, or :func:`run_cursor_command` instead.

    Publishes the
    ``STARTED``/``SUCCEEDED``/``FAILED`` command log and APM events, performs
    the network round trip, gossips ``$clusterTime``, runs
    ``client._process_response`` and ``_check_command_response``, and decrypts
    the reply when auto-encryption is enabled.

    :param conn: The Connection to send on.
    :param cmd: The command document, used for the ``STARTED`` log/APM event.
    :param dbname: The database the command runs against.
    :param request_id: The request id of the encoded message (``0`` when
        ``more_to_come`` and no message is sent).
    :param msg: The encoded bytes to send (ignored when ``more_to_come``).
    :param client: The MongoClient, for ``$clusterTime`` gossip and
        decryption. ``None`` disables those steps (e.g. during handshake).
    :param session: The session to update from the response.
    :param listeners: The event listeners, or ``None`` to disable APM.
    :param topology_id: The client topology id for structured logging, or
        ``None`` to disable command logging.
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
    :param decrypt_reply: Decrypt the reply when auto-encryption is enabled;
        the bulk paths pass False (their commands are encrypted up front).
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

    telemetry = _CommandTelemetry(topology_id, conn, listeners, cmd, dbname, request_id, op_id)
    telemetry.started(orig, ensure_db)

    reply: Optional[_OpMsg] = None
    docs: list[dict[str, Any]] = [{"ok": 1}]
    try:
        if more_to_come:
            reply = conn.receive_message(None)
        else:
            if unacknowledged:
                conn._raise_if_not_writable()
            elif session is not None and session._starting_transaction:
                session._transaction.set_in_progress()
            conn.send_message(msg, max_doc_size)
            if not unacknowledged:
                reply = conn.receive_message(request_id)

        if reply:
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
            if client:
                client._process_response(response_doc, session)
            if check:
                helpers_shared._check_command_response(
                    response_doc,
                    conn.max_wire_version,
                    allowable_errors,
                    parse_write_concern_error=parse_write_concern_error,
                    pool_opts=pool_opts,
                )
    except Exception as exc:
        if isinstance(exc, (NotPrimaryError, OperationFailure)):
            failure: _DocumentOut = exc.details  # type: ignore[assignment]
        else:
            failure = _convert_exception(exc)
        telemetry.failed(failure, command_name, isinstance(exc, OperationFailure))
        raise

    telemetry.succeeded(docs[0], command_name, speculative_hello)

    if client and client._encrypter and reply and decrypt_reply:
        decrypted = client._encrypter.decrypt(reply.raw_command_response())
        docs = cast(
            "list[dict[str, Any]]", _decode_all_selective(decrypted, codec_options, user_fields)
        )

    return docs, reply, telemetry.duration


def run_bulk_write_command(
    bwc: _BulkWriteContextBase,
    cmd: MutableMapping[str, Any],
    request_id: int,
    msg: bytes,
    *,
    client: Optional[MongoClient[Any]],
    orig: Optional[MutableMapping[str, Any]] = None,
    max_doc_size: int = 0,
    unacknowledged: bool = False,
) -> tuple[list[dict[str, Any]], Optional[_OpMsg], datetime.timedelta]:
    """Send a bulk write batch and return ``(docs, reply, duration)``.

    :param bwc: Bulk write context supplying the connection, session, listeners, etc.
    :param cmd: The encoded command document.
    :param request_id: The request id of the encoded message.
    :param msg: The encoded bytes to send.
    :param client: The MongoClient, for ``$clusterTime`` gossip and logging.
    :param orig: The original command document for the APM ``STARTED`` event;
        defaults to ``cmd``.
    :param max_doc_size: The largest document size in the batch, passed to ``conn.send_message``.
    :param unacknowledged: When ``True``, send only and fake an ``{"ok": 1}`` reply.
    """
    topology_id = client._topology_id if client is not None else None
    return _run_command(
        bwc.conn,  # type: ignore[arg-type]
        cmd,
        bwc.db_name,
        request_id,
        msg,
        client=client,
        session=bwc.session,  # type: ignore[arg-type]
        listeners=bwc.listeners,
        topology_id=topology_id,
        codec_options=bwc.codec,
        op_id=bwc.op_id,
        command_name=bwc.name,
        orig=orig,
        max_doc_size=max_doc_size,
        unacknowledged=unacknowledged,
        decrypt_reply=False,
    )


def run_cursor_command(
    conn: Connection,
    cmd: MutableMapping[str, Any],
    dbname: str,
    request_id: int,
    msg: bytes,
    *,
    client: Optional[MongoClient[Any]],
    session: Optional[ClientSession],
    listeners: Optional[_EventListeners],
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

    :param conn: The Connection to send on.
    :param cmd: The command document, used for the ``STARTED`` log/APM event.
    :param dbname: The database the command runs against.
    :param request_id: The request id of the encoded message.
    :param msg: The encoded bytes to send (ignored when ``more_to_come``).
    :param client: The MongoClient, for ``$clusterTime`` gossip and logging.
    :param session: The session to update from the response.
    :param listeners: The event listeners, or ``None`` to disable APM.
    :param codec_options: The CodecOptions used to decode the reply.
    :param command_name: The command name for APM events.
    :param user_fields: Response fields decoded with the codec's TypeDecoders.
    :param pool_opts: PoolOptions forwarded to ``_check_command_response``.
    :param max_doc_size: The largest document size, for ``conn.send_message``.
    :param more_to_come: Receive only, without sending. Used for ``getMore`` on exhaust cursors.
    :param unpack_res: A callable decoding the wire response; when ``None`` the
        reply's own ``unpack_response`` is used.
    :param cursor_id: The cursor id passed to ``unpack_res``.
    """
    topology_id = client._topology_id if client is not None else None
    return _run_command(
        conn,
        cmd,
        dbname,
        request_id,
        msg,
        client=client,
        session=session,
        listeners=listeners,
        topology_id=topology_id,
        codec_options=codec_options,
        user_fields=user_fields,
        command_name=command_name,
        pool_opts=pool_opts,
        ensure_db=True,
        max_doc_size=max_doc_size,
        more_to_come=more_to_come,
        unpack_res=unpack_res,
        cursor_id=cursor_id,
    )


def run_command(
    conn: Connection,
    dbname: str,
    spec: MutableMapping[str, Any],
    read_preference: Optional[_ServerMode],
    codec_options: CodecOptions[_DocumentType],
    session: Optional[ClientSession],
    client: Optional[MongoClient[Any]],
    check: bool = True,
    allowable_errors: Optional[Sequence[Union[str, int]]] = None,
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
    then performs the network round trip and response processing.

    :param conn: The Connection to send on.
    :param dbname: The database the command runs against.
    :param spec: A command document as an ordered dict type, e.g. SON.
    :param read_preference: The read preference for this command.
    :param codec_options: The CodecOptions used to decode the reply.
    :param session: The ClientSession for this command.
    :param client: The MongoClient, for ``$clusterTime`` gossip and logging.
    :param check: Raise OperationFailure if there are errors.
    :param allowable_errors: Errors to ignore when ``check`` is True.
    :param listeners: The event listeners, or ``None`` to disable APM.
    :param max_bson_size: The maximum encoded BSON size for this server.
    :param read_concern: The read concern for this command.
    :param parse_write_concern_error: Whether to parse the ``writeConcernError``
        field in the command response.
    :param collation: The collation for this command.
    :param compression_ctx: Optional compression context.
    :param unacknowledged: True if this is an unacknowledged command.
    :param user_fields: Response fields decoded with the codec's TypeDecoders,
        passed to ``bson._decode_all_selective``.
    :param exhaust_allowed: True if we should enable OP_MSG exhaustAllowed.
    :param write_concern: The write concern for this command. Applied via CSOT.
    """
    name = next(iter(spec))

    # Publish the original command document, perhaps with lsid and $clusterTime.
    orig = spec
    if read_concern and not (session and session.in_transaction):
        if read_concern.level:
            spec["readConcern"] = read_concern.document
        if session:
            session._update_read_concern(spec, conn)
    if collation is not None:
        spec["collation"] = collation

    topology_id = client._topology_id if client is not None else None
    speculative_hello = _is_speculative_authenticate(name, spec)

    if compression_ctx and name.lower() in _NO_COMPRESSION:
        compression_ctx = None

    if client and client._encrypter and not client._encrypter._bypass_auto_encryption:
        spec = orig = client._encrypter.encrypt(dbname, spec, codec_options)

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
    docs, _, _ = _run_command(
        conn,
        spec,
        dbname,
        request_id,
        msg,
        client=client,
        session=session,
        listeners=listeners,
        topology_id=topology_id,
        codec_options=codec_options,
        user_fields=user_fields,
        orig=orig,
        check=check,
        allowable_errors=allowable_errors,
        parse_write_concern_error=parse_write_concern_error,
        speculative_hello=speculative_hello,
        unacknowledged=unacknowledged,
        set_conn_more_to_come=True,
    )
    return docs[0]  # type: ignore[return-value]
