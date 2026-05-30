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

from datetime import timedelta
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
from pymongo._telemetry import _CommandTelemetry
from pymongo.compression_support import _NO_COMPRESSION
from pymongo.message import _OpMsg
from pymongo.monitoring import _is_speculative_authenticate
from pymongo.network_layer import (
    receive_message,
    sendall,
)

if TYPE_CHECKING:
    from bson import CodecOptions
    from pymongo.message import _GetMore, _OpReply, _Query
    from pymongo.monitoring import _EventListeners
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import _ServerMode
    from pymongo.synchronous.client_session import ClientSession
    from pymongo.synchronous.mongo_client import MongoClient
    from pymongo.synchronous.pool import Connection
    from pymongo.typings import _CollationIn, _DocumentOut, _DocumentType
    from pymongo.write_concern import WriteConcern

_IS_SYNC = True

_CURSOR_DOC_FIELDS = {"cursor": {"firstBatch": 1, "nextBatch": 1}}


def bulk_write_command(
    conn: Connection,
    request_id: int,
    msg: bytes,
    codec_options: CodecOptions[Mapping[str, Any]],
    command_name: str,
    database_name: str,
    spec: Any,
    orig: Any,
    *,
    acknowledged: bool = True,
    max_doc_size: int = 0,
    publish_events: bool = True,
    operation_id: Optional[int] = None,
) -> dict[str, Any]:
    """Send a bulk write command, returning the server response as a dict.

    When acknowledged=False, sends the message without waiting for a
    response and returns a synthetic {"ok": 1}.

    Can raise ConnectionFailure or OperationFailure.
    """
    conn._raise_if_not_writable(not acknowledged)
    with _CommandTelemetry(
        conn=conn,
        command_name=command_name,
        database_name=database_name,
        spec=spec,
        orig=orig,
        request_id=request_id,
        publish_events=publish_events,
        operation_id=operation_id,
    ) as cmd_telemetry:
        conn.send_message(msg, max_doc_size)
        if not acknowledged:
            result: dict[str, Any] = {"ok": 1}
        else:
            reply = conn.receive_message(request_id)
            result = reply.command_response(codec_options)
            # Raises NotPrimaryError or OperationFailure.
            helpers_shared._check_command_response(result, conn.max_wire_version)
        cmd_telemetry.handle_succeeded(result)
    return result


def cursor_operation(
    conn: Connection,
    data: bytes,
    max_doc_size: int,
    more_to_come: bool,
    request_id: int,
    unpack_res: Callable[..., list[_DocumentOut]],
    operation: Union[_Query, _GetMore],
    use_cmd: bool,
    command_name: str,
    database_name: str,
    spec: Any,
    publish_events: bool,
) -> tuple[Union[_OpReply, _OpMsg], list[_DocumentOut], timedelta]:
    """Send a find/getMore operation, manage telemetry, and return the result.

    Can raise ConnectionFailure, OperationFailure, etc.
    """
    with _CommandTelemetry(
        conn=conn,
        command_name=command_name,
        database_name=database_name,
        spec=spec,
        orig=spec,
        request_id=request_id,
        publish_events=publish_events,
    ) as cmd_telemetry:
        if more_to_come:
            reply = conn.receive_message(None)
        else:
            conn.send_message(data, max_doc_size)
            reply = conn.receive_message(request_id)

        # Unpack and check for command errors.
        user_fields: Optional[Mapping[str, Any]]
        if use_cmd:
            user_fields = _CURSOR_DOC_FIELDS
            legacy_response = False
        else:
            user_fields = None
            legacy_response = True
        docs = unpack_res(
            reply,
            operation.cursor_id,
            operation.codec_options,
            legacy_response=legacy_response,
            user_fields=user_fields,
        )
        if use_cmd:
            first = docs[0]
            operation.client._process_response(first, operation.session)  # type: ignore[misc, arg-type]
            helpers_shared._check_command_response(
                first, conn.max_wire_version, pool_opts=conn.opts
            )  # type: ignore[has-type]

        # Must publish in find / getMore / explain command response format.
        if use_cmd:
            res = docs[0]
        elif operation.name == "explain":
            res = docs[0] if docs else {}
        else:
            res = {"cursor": {"id": reply.cursor_id, "ns": operation.namespace()}, "ok": 1}  # type: ignore[union-attr]
            if operation.name == "find":
                res["cursor"]["firstBatch"] = docs
            else:
                res["cursor"]["nextBatch"] = docs
        duration = cmd_telemetry.handle_succeeded(res)
    return reply, docs, duration


def command(
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
    read_concern: Optional[ReadConcern] = None,
    parse_write_concern_error: bool = False,
    collation: Optional[_CollationIn] = None,
    user_fields: Optional[Mapping[str, Any]] = None,
    exhaust_allowed: bool = False,
    write_concern: Optional[WriteConcern] = None,
) -> _DocumentType:
    """Execute a command over the socket, or raise socket.error.

    :param conn: a Connection instance
    :param dbname: name of the database on which to run the command
    :param spec: a command document as an ordered dict type, eg SON.
    :param read_preference: a read preference
    :param codec_options: a CodecOptions instance
    :param session: optional ClientSession instance.
    :param client: optional MongoClient instance for updating $clusterTime.
    :param check: raise OperationFailure if there are errors
    :param allowable_errors: errors to ignore if `check` is True
    :param listeners: An instance of :class:`~pymongo.monitoring.EventListeners`
    :param read_concern: The read concern for this command.
    :param parse_write_concern_error: Whether to parse the ``writeConcernError``
        field in the command response.
    :param collation: The collation for this command.
    :param user_fields: Response fields that should be decoded
        using the TypeDecoders from codec_options, passed to
        bson._decode_all_selective.
    :param exhaust_allowed: True if we should enable OP_MSG exhaustAllowed.
    """
    is_mongos = conn.is_mongos
    use_op_msg = conn.op_msg_enabled
    max_bson_size = conn.max_bson_size
    unacknowledged = bool(write_concern and not write_concern.acknowledged)
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
    speculative_hello = _is_speculative_authenticate(name, spec) if publish else False

    compression_ctx = conn.compression_context
    if compression_ctx and name.lower() in _NO_COMPRESSION:
        compression_ctx = None

    if client and client._encrypter and not client._encrypter._bypass_auto_encryption:
        spec = orig = client._encrypter.encrypt(dbname, spec, codec_options)

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

    if max_bson_size is not None and size > max_bson_size + message._COMMAND_OVERHEAD:
        message._raise_document_too_large(name, size, max_bson_size + message._COMMAND_OVERHEAD)

    with _CommandTelemetry(
        conn=conn,
        command_name=name,
        database_name=dbname,
        spec=spec,
        orig=orig,
        request_id=request_id,
        publish_events=publish,
        log_events=client is not None,
    ) as cmd_telemetry:
        sendall(conn.conn.get_conn, msg)
        if use_op_msg and unacknowledged:
            # Unacknowledged, fake a successful command response.
            reply = None
            response_doc: _DocumentOut = {"ok": 1}
        else:
            reply = receive_message(conn, request_id)
            conn.more_to_come = reply.more_to_come
            unpacked_docs = reply.unpack_response(
                codec_options=codec_options, user_fields=user_fields
            )

            response_doc = unpacked_docs[0]
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
        cmd_telemetry.handle_succeeded(response_doc, speculative_hello=speculative_hello)

    if client and client._encrypter and reply:
        decrypted = client._encrypter.decrypt(reply.raw_command_response())
        response_doc = cast(
            "_DocumentOut", _decode_all_selective(decrypted, codec_options, user_fields)[0]
        )

    return response_doc  # type: ignore[return-value]
