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

"""Internal network layer helper methods."""
from __future__ import annotations

import datetime
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
from pymongo import _csot, helpers_shared, message
from pymongo.command_helpers import (
    _log_command_failed,
    _log_command_started,
    _log_command_succeeded,
)
from pymongo.compression_support import _NO_COMPRESSION
from pymongo.errors import (
    NotPrimaryError,
    OperationFailure,
)
from pymongo.message import _OpMsg
from pymongo.monitoring import _is_speculative_authenticate

if TYPE_CHECKING:
    from bson import CodecOptions
    from pymongo.asynchronous.client_session import AsyncClientSession
    from pymongo.asynchronous.mongo_client import AsyncMongoClient
    from pymongo.asynchronous.pool import AsyncConnection
    from pymongo.compression_support import SnappyContext, ZlibContext, ZstdContext
    from pymongo.monitoring import _EventListeners
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import _ServerMode
    from pymongo.typings import _Address, _CollationIn, _DocumentOut, _DocumentType
    from pymongo.write_concern import WriteConcern

_IS_SYNC = False

_CURSOR_DOC_FIELDS = {"cursor": {"firstBatch": 1, "nextBatch": 1}}


async def _network_command_core(
    conn: AsyncConnection,
    dbname: str,
    spec: MutableMapping[str, Any],
    request_id: int,
    msg: Optional[bytes],
    max_doc_size: int,
    codec_options: CodecOptions[_DocumentType],
    session: Optional[AsyncClientSession],
    client: Optional[AsyncMongoClient[Any]],
    listeners: Optional[_EventListeners],
    address: Optional[_Address],
    start: datetime.datetime,
    check: bool = True,
    allowable_errors: Optional[Sequence[Union[str, int]]] = None,
    parse_write_concern_error: bool = False,
    user_fields: Optional[Mapping[str, Any]] = None,
    unacknowledged: bool = False,
    more_to_come: bool = False,
    unpack_res: Optional[Callable[..., list[_DocumentOut]]] = None,
    cursor_id: Optional[int] = None,
    orig: Optional[MutableMapping[str, Any]] = None,
    speculative_hello: bool = False,
) -> tuple[list[_DocumentOut], Optional[_OpMsg], datetime.timedelta]:
    """Send/receive a command and return (docs, raw_reply, duration).

    Handles APM logging, send/receive, unpacking, response processing,
    and decryption. Both the standard command path and the cursor
    (find/getMore) path go through this function.
    """
    publish = listeners is not None and listeners.enabled_for_commands
    name = next(iter(spec))
    reply: Optional[_OpMsg] = None
    docs: list[_DocumentOut] = []

    if client is not None:
        _log_command_started(client, conn, spec, dbname, request_id, request_id)
    if publish:
        assert listeners is not None
        assert address is not None
        listeners.publish_command_start(
            orig if orig is not None else spec,
            dbname,
            request_id,
            address,
            conn.server_connection_id,
            service_id=conn.service_id,
        )

    try:
        if more_to_come:
            reply = await conn.receive_message(None)
        else:
            assert msg is not None
            await conn.send_message(msg, max_doc_size)
            if unacknowledged:
                # Unacknowledged write: fake a successful command response.
                docs = [{"ok": 1}]  # type: ignore[list-item]
            else:
                reply = await conn.receive_message(request_id)

        if reply is not None:
            conn.more_to_come = reply.more_to_come
            if unpack_res is not None:
                docs = unpack_res(
                    reply,
                    cursor_id,
                    codec_options,
                    legacy_response=False,
                    user_fields=_CURSOR_DOC_FIELDS,
                )
            else:
                docs = list(
                    reply.unpack_response(codec_options=codec_options, user_fields=user_fields)
                )
            response_doc = docs[0]
            if not conn.ready:
                cluster_time = response_doc.get("$clusterTime")
                if cluster_time:
                    conn._cluster_time = cluster_time
            if client:
                await client._process_response(response_doc, session)
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
            failure = message._convert_exception(exc)
        if client is not None:
            _log_command_failed(
                client,
                conn,
                spec,
                dbname,
                request_id,
                request_id,
                failure,
                duration,
                isinstance(exc, OperationFailure),
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
                service_id=conn.service_id,
                database_name=dbname,
            )
        raise
    duration = datetime.datetime.now() - start
    if client is not None:
        _log_command_succeeded(
            client,
            conn,
            spec,
            dbname,
            request_id,
            request_id,
            docs[0],
            duration,
            speculative_hello,
        )
    if publish:
        assert listeners is not None
        assert address is not None
        listeners.publish_command_success(
            duration,
            docs[0],
            name,
            request_id,
            address,
            conn.server_connection_id,
            service_id=conn.service_id,
            speculative_hello=speculative_hello,
            database_name=dbname,
        )

    # Decrypt response.
    if client and client._encrypter and reply is not None:
        decrypted = await client._encrypter.decrypt(reply.raw_command_response())
        decrypt_fields = _CURSOR_DOC_FIELDS if unpack_res is not None else user_fields
        docs = list(_decode_all_selective(decrypted, codec_options, decrypt_fields))

    return docs, reply, duration


async def command(
    conn: AsyncConnection,
    dbname: str,
    spec: MutableMapping[str, Any],
    is_mongos: bool,
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
    use_op_msg: bool = False,
    unacknowledged: bool = False,
    user_fields: Optional[Mapping[str, Any]] = None,
    exhaust_allowed: bool = False,
    write_concern: Optional[WriteConcern] = None,
) -> _DocumentType:
    """Execute a command over the socket, or raise socket.error.

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
    :param use_op_msg: True if we should use OP_MSG.
    :param unacknowledged: True if this is an unacknowledged command.
    :param user_fields: Response fields that should be decoded
        using the TypeDecoders from codec_options, passed to
        bson._decode_all_selective.
    :param exhaust_allowed: True if we should enable OP_MSG exhaustAllowed.
    """
    name = next(iter(spec))
    ns = dbname + ".$cmd"
    speculative_hello = False

    # Publish the original command document, perhaps with lsid and $clusterTime.
    orig = spec
    if is_mongos and not use_op_msg:
        assert read_preference is not None
        spec = message._maybe_add_read_preference(spec, read_preference)
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

    if use_op_msg:
        flags = _OpMsg.MORE_TO_COME if unacknowledged else 0
        flags |= _OpMsg.EXHAUST_ALLOWED if exhaust_allowed else 0
        request_id, msg, size, max_doc_size = message._op_msg(
            flags, spec, dbname, read_preference, codec_options, ctx=compression_ctx
        )
        # If this is an unacknowledged write then make sure the encoded doc(s)
        # are small enough, otherwise rely on the server to return an error.
        if unacknowledged and max_bson_size is not None and max_doc_size > max_bson_size:
            message._raise_document_too_large(name, size, max_bson_size)
    else:
        request_id, msg, size = message._query(
            0, ns, 0, -1, spec, None, codec_options, compression_ctx
        )
        max_doc_size = 0

    if max_bson_size is not None and size > max_bson_size + message._COMMAND_OVERHEAD:
        message._raise_document_too_large(name, size, max_bson_size + message._COMMAND_OVERHEAD)

    docs, _reply, _duration = await _network_command_core(
        conn=conn,
        dbname=dbname,
        spec=spec,
        request_id=request_id,
        msg=msg,
        max_doc_size=max_doc_size,
        codec_options=codec_options,
        session=session,
        client=client,
        listeners=listeners,
        address=address,
        start=start,
        check=check,
        allowable_errors=allowable_errors,
        parse_write_concern_error=parse_write_concern_error,
        user_fields=user_fields,
        unacknowledged=unacknowledged,
        orig=orig,
        speculative_hello=speculative_hello,
    )
    return cast("_DocumentType", docs[0])
