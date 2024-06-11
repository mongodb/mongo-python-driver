# Copyright 2009-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tools for creating `messages
<https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/>`_ to be sent to
MongoDB.

.. note:: This module is for internal use and is generally not needed by
   application developers.
"""
from __future__ import annotations

import datetime
import logging
from io import BytesIO as _BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    MutableMapping,
    Optional,
)

from bson import CodecOptions, _dict_to_bson, encode
from bson.raw_bson import (
    DEFAULT_RAW_BSON_OPTIONS,
    _inflate_bson,
)
from pymongo.asynchronous.helpers import _handle_reauth
from pymongo.errors import (
    InvalidOperation,
    NotPrimaryError,
    OperationFailure,
)
from pymongo.logger import (
    _COMMAND_LOGGER,
    _CommandStatusMessage,
    _debug_log,
)
from pymongo.message import (
    _BSONOBJ,
    _COMMAND_OVERHEAD,
    _FIELD_MAP,
    _OP_MAP,
    _OP_MSG_MAP,
    _SKIPLIM,
    _ZERO_8,
    _ZERO_16,
    _ZERO_32,
    _ZERO_64,
    _compress,
    _convert_exception,
    _convert_write_result,
    _pack_int,
    _raise_document_too_large,
    _randint,
)
from pymongo.write_concern import WriteConcern

try:
    from pymongo import _cmessage  # type: ignore[attr-defined]

    _use_c = True
except ImportError:
    _use_c = False

if TYPE_CHECKING:
    from datetime import timedelta

    from pymongo.asynchronous.client_session import AsyncClientSession
    from pymongo.asynchronous.mongo_client import AsyncMongoClient
    from pymongo.asynchronous.pool import AsyncConnection
    from pymongo.monitoring import _EventListeners
    from pymongo.typings import _DocumentOut


_IS_SYNC = False


class _BulkWriteContext:
    """A wrapper around AsyncConnection for use with write splitting functions."""

    __slots__ = (
        "db_name",
        "conn",
        "op_id",
        "name",
        "field",
        "publish",
        "start_time",
        "listeners",
        "session",
        "compress",
        "op_type",
        "codec",
    )

    def __init__(
        self,
        database_name: str,
        cmd_name: str,
        conn: AsyncConnection,
        operation_id: int,
        listeners: _EventListeners,
        session: AsyncClientSession,
        op_type: int,
        codec: CodecOptions,
    ):
        self.db_name = database_name
        self.conn = conn
        self.op_id = operation_id
        self.listeners = listeners
        self.publish = listeners.enabled_for_commands
        self.name = cmd_name
        self.field = _FIELD_MAP[self.name]
        self.start_time = datetime.datetime.now()
        self.session = session
        self.compress = bool(conn.compression_context)
        self.op_type = op_type
        self.codec = codec

    def __batch_command(
        self, cmd: MutableMapping[str, Any], docs: list[Mapping[str, Any]]
    ) -> tuple[int, bytes, list[Mapping[str, Any]]]:
        namespace = self.db_name + ".$cmd"
        request_id, msg, to_send = _do_batched_op_msg(
            namespace, self.op_type, cmd, docs, self.codec, self
        )
        if not to_send:
            raise InvalidOperation("cannot do an empty bulk write")
        return request_id, msg, to_send

    async def execute(
        self, cmd: MutableMapping[str, Any], docs: list[Mapping[str, Any]], client: AsyncMongoClient
    ) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
        request_id, msg, to_send = self.__batch_command(cmd, docs)
        result = await self.write_command(cmd, request_id, msg, to_send, client)
        await client._process_response(result, self.session)
        return result, to_send

    async def execute_unack(
        self, cmd: MutableMapping[str, Any], docs: list[Mapping[str, Any]], client: AsyncMongoClient
    ) -> list[Mapping[str, Any]]:
        request_id, msg, to_send = self.__batch_command(cmd, docs)
        # Though this isn't strictly a "legacy" write, the helper
        # handles publishing commands and sending our message
        # without receiving a result. Send 0 for max_doc_size
        # to disable size checking. Size checking is handled while
        # the documents are encoded to BSON.
        await self.unack_write(cmd, request_id, msg, 0, to_send, client)
        return to_send

    @property
    def max_bson_size(self) -> int:
        """A proxy for SockInfo.max_bson_size."""
        return self.conn.max_bson_size

    @property
    def max_message_size(self) -> int:
        """A proxy for SockInfo.max_message_size."""
        if self.compress:
            # Subtract 16 bytes for the message header.
            return self.conn.max_message_size - 16
        return self.conn.max_message_size

    @property
    def max_write_batch_size(self) -> int:
        """A proxy for SockInfo.max_write_batch_size."""
        return self.conn.max_write_batch_size

    @property
    def max_split_size(self) -> int:
        """The maximum size of a BSON command before batch splitting."""
        return self.max_bson_size

    async def unack_write(
        self,
        cmd: MutableMapping[str, Any],
        request_id: int,
        msg: bytes,
        max_doc_size: int,
        docs: list[Mapping[str, Any]],
        client: AsyncMongoClient,
    ) -> Optional[Mapping[str, Any]]:
        """A proxy for AsyncConnection.unack_write that handles event publishing."""
        if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _COMMAND_LOGGER,
                clientId=client._topology_settings._topology_id,
                message=_CommandStatusMessage.STARTED,
                command=cmd,
                commandName=next(iter(cmd)),
                databaseName=self.db_name,
                requestId=request_id,
                operationId=request_id,
                driverConnectionId=self.conn.id,
                serverConnectionId=self.conn.server_connection_id,
                serverHost=self.conn.address[0],
                serverPort=self.conn.address[1],
                serviceId=self.conn.service_id,
            )
        if self.publish:
            cmd = self._start(cmd, request_id, docs)
        try:
            result = await self.conn.unack_write(msg, max_doc_size)  # type: ignore[func-returns-value]
            duration = datetime.datetime.now() - self.start_time
            if result is not None:
                reply = _convert_write_result(self.name, cmd, result)
            else:
                # Comply with APM spec.
                reply = {"ok": 1}
                if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
                    _debug_log(
                        _COMMAND_LOGGER,
                        clientId=client._topology_settings._topology_id,
                        message=_CommandStatusMessage.SUCCEEDED,
                        durationMS=duration,
                        reply=reply,
                        commandName=next(iter(cmd)),
                        databaseName=self.db_name,
                        requestId=request_id,
                        operationId=request_id,
                        driverConnectionId=self.conn.id,
                        serverConnectionId=self.conn.server_connection_id,
                        serverHost=self.conn.address[0],
                        serverPort=self.conn.address[1],
                        serviceId=self.conn.service_id,
                    )
            if self.publish:
                self._succeed(request_id, reply, duration)
        except Exception as exc:
            duration = datetime.datetime.now() - self.start_time
            if isinstance(exc, OperationFailure):
                failure: _DocumentOut = _convert_write_result(self.name, cmd, exc.details)  # type: ignore[arg-type]
            elif isinstance(exc, NotPrimaryError):
                failure = exc.details  # type: ignore[assignment]
            else:
                failure = _convert_exception(exc)
            if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _COMMAND_LOGGER,
                    clientId=client._topology_settings._topology_id,
                    message=_CommandStatusMessage.FAILED,
                    durationMS=duration,
                    failure=failure,
                    commandName=next(iter(cmd)),
                    databaseName=self.db_name,
                    requestId=request_id,
                    operationId=request_id,
                    driverConnectionId=self.conn.id,
                    serverConnectionId=self.conn.server_connection_id,
                    serverHost=self.conn.address[0],
                    serverPort=self.conn.address[1],
                    serviceId=self.conn.service_id,
                    isServerSideError=isinstance(exc, OperationFailure),
                )
            if self.publish:
                assert self.start_time is not None
                self._fail(request_id, failure, duration)
            raise
        finally:
            self.start_time = datetime.datetime.now()
        return result

    @_handle_reauth
    async def write_command(
        self,
        cmd: MutableMapping[str, Any],
        request_id: int,
        msg: bytes,
        docs: list[Mapping[str, Any]],
        client: AsyncMongoClient,
    ) -> dict[str, Any]:
        """A proxy for SocketInfo.write_command that handles event publishing."""
        cmd[self.field] = docs
        if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _COMMAND_LOGGER,
                clientId=client._topology_settings._topology_id,
                message=_CommandStatusMessage.STARTED,
                command=cmd,
                commandName=next(iter(cmd)),
                databaseName=self.db_name,
                requestId=request_id,
                operationId=request_id,
                driverConnectionId=self.conn.id,
                serverConnectionId=self.conn.server_connection_id,
                serverHost=self.conn.address[0],
                serverPort=self.conn.address[1],
                serviceId=self.conn.service_id,
            )
        if self.publish:
            self._start(cmd, request_id, docs)
        try:
            reply = await self.conn.write_command(request_id, msg, self.codec)
            duration = datetime.datetime.now() - self.start_time
            if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _COMMAND_LOGGER,
                    clientId=client._topology_settings._topology_id,
                    message=_CommandStatusMessage.SUCCEEDED,
                    durationMS=duration,
                    reply=reply,
                    commandName=next(iter(cmd)),
                    databaseName=self.db_name,
                    requestId=request_id,
                    operationId=request_id,
                    driverConnectionId=self.conn.id,
                    serverConnectionId=self.conn.server_connection_id,
                    serverHost=self.conn.address[0],
                    serverPort=self.conn.address[1],
                    serviceId=self.conn.service_id,
                )
            if self.publish:
                self._succeed(request_id, reply, duration)
        except Exception as exc:
            duration = datetime.datetime.now() - self.start_time
            if isinstance(exc, (NotPrimaryError, OperationFailure)):
                failure: _DocumentOut = exc.details  # type: ignore[assignment]
            else:
                failure = _convert_exception(exc)
            if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _COMMAND_LOGGER,
                    clientId=client._topology_settings._topology_id,
                    message=_CommandStatusMessage.FAILED,
                    durationMS=duration,
                    failure=failure,
                    commandName=next(iter(cmd)),
                    databaseName=self.db_name,
                    requestId=request_id,
                    operationId=request_id,
                    driverConnectionId=self.conn.id,
                    serverConnectionId=self.conn.server_connection_id,
                    serverHost=self.conn.address[0],
                    serverPort=self.conn.address[1],
                    serviceId=self.conn.service_id,
                    isServerSideError=isinstance(exc, OperationFailure),
                )

            if self.publish:
                self._fail(request_id, failure, duration)
            raise
        finally:
            self.start_time = datetime.datetime.now()
        return reply

    def _start(
        self, cmd: MutableMapping[str, Any], request_id: int, docs: list[Mapping[str, Any]]
    ) -> MutableMapping[str, Any]:
        """Publish a CommandStartedEvent."""
        cmd[self.field] = docs
        self.listeners.publish_command_start(
            cmd,
            self.db_name,
            request_id,
            self.conn.address,
            self.conn.server_connection_id,
            self.op_id,
            self.conn.service_id,
        )
        return cmd

    def _succeed(self, request_id: int, reply: _DocumentOut, duration: timedelta) -> None:
        """Publish a CommandSucceededEvent."""
        self.listeners.publish_command_success(
            duration,
            reply,
            self.name,
            request_id,
            self.conn.address,
            self.conn.server_connection_id,
            self.op_id,
            self.conn.service_id,
            database_name=self.db_name,
        )

    def _fail(self, request_id: int, failure: _DocumentOut, duration: timedelta) -> None:
        """Publish a CommandFailedEvent."""
        self.listeners.publish_command_failure(
            duration,
            failure,
            self.name,
            request_id,
            self.conn.address,
            self.conn.server_connection_id,
            self.op_id,
            self.conn.service_id,
            database_name=self.db_name,
        )


# From the Client Side Encryption spec:
# Because automatic encryption increases the size of commands, the driver
# MUST split bulk writes at a reduced size limit before undergoing automatic
# encryption. The write payload MUST be split at 2MiB (2097152).
_MAX_SPLIT_SIZE_ENC = 2097152


class _EncryptedBulkWriteContext(_BulkWriteContext):
    __slots__ = ()

    def __batch_command(
        self, cmd: MutableMapping[str, Any], docs: list[Mapping[str, Any]]
    ) -> tuple[dict[str, Any], list[Mapping[str, Any]]]:
        namespace = self.db_name + ".$cmd"
        msg, to_send = _encode_batched_write_command(
            namespace, self.op_type, cmd, docs, self.codec, self
        )
        if not to_send:
            raise InvalidOperation("cannot do an empty bulk write")

        # Chop off the OP_QUERY header to get a properly batched write command.
        cmd_start = msg.index(b"\x00", 4) + 9
        outgoing = _inflate_bson(memoryview(msg)[cmd_start:], DEFAULT_RAW_BSON_OPTIONS)
        return outgoing, to_send

    async def execute(
        self, cmd: MutableMapping[str, Any], docs: list[Mapping[str, Any]], client: AsyncMongoClient
    ) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
        batched_cmd, to_send = self.__batch_command(cmd, docs)
        result: Mapping[str, Any] = await self.conn.command(
            self.db_name, batched_cmd, codec_options=self.codec, session=self.session, client=client
        )
        return result, to_send

    async def execute_unack(
        self, cmd: MutableMapping[str, Any], docs: list[Mapping[str, Any]], client: AsyncMongoClient
    ) -> list[Mapping[str, Any]]:
        batched_cmd, to_send = self.__batch_command(cmd, docs)
        await self.conn.command(
            self.db_name,
            batched_cmd,
            write_concern=WriteConcern(w=0),
            session=self.session,
            client=client,
        )
        return to_send

    @property
    def max_split_size(self) -> int:
        """Reduce the batch splitting size."""
        return _MAX_SPLIT_SIZE_ENC


def _batched_op_msg_impl(
    operation: int,
    command: Mapping[str, Any],
    docs: list[Mapping[str, Any]],
    ack: bool,
    opts: CodecOptions,
    ctx: _BulkWriteContext,
    buf: _BytesIO,
) -> tuple[list[Mapping[str, Any]], int]:
    """Create a batched OP_MSG write."""
    max_bson_size = ctx.max_bson_size
    max_write_batch_size = ctx.max_write_batch_size
    max_message_size = ctx.max_message_size

    flags = b"\x00\x00\x00\x00" if ack else b"\x02\x00\x00\x00"
    # Flags
    buf.write(flags)

    # Type 0 Section
    buf.write(b"\x00")
    buf.write(_dict_to_bson(command, False, opts))

    # Type 1 Section
    buf.write(b"\x01")
    size_location = buf.tell()
    # Save space for size
    buf.write(b"\x00\x00\x00\x00")
    try:
        buf.write(_OP_MSG_MAP[operation])
    except KeyError:
        raise InvalidOperation("Unknown command") from None

    to_send = []
    idx = 0
    for doc in docs:
        # Encode the current operation
        value = _dict_to_bson(doc, False, opts)
        doc_length = len(value)
        new_message_size = buf.tell() + doc_length
        # Does first document exceed max_message_size?
        doc_too_large = idx == 0 and (new_message_size > max_message_size)
        # When OP_MSG is used unacknowledged we have to check
        # document size client side or applications won't be notified.
        # Otherwise we let the server deal with documents that are too large
        # since ordered=False causes those documents to be skipped instead of
        # halting the bulk write operation.
        unacked_doc_too_large = not ack and (doc_length > max_bson_size)
        if doc_too_large or unacked_doc_too_large:
            write_op = list(_FIELD_MAP.keys())[operation]
            _raise_document_too_large(write_op, len(value), max_bson_size)
        # We have enough data, return this batch.
        if new_message_size > max_message_size:
            break
        buf.write(value)
        to_send.append(doc)
        idx += 1
        # We have enough documents, return this batch.
        if idx == max_write_batch_size:
            break

    # Write type 1 section size
    length = buf.tell()
    buf.seek(size_location)
    buf.write(_pack_int(length - size_location))

    return to_send, length


def _encode_batched_op_msg(
    operation: int,
    command: Mapping[str, Any],
    docs: list[Mapping[str, Any]],
    ack: bool,
    opts: CodecOptions,
    ctx: _BulkWriteContext,
) -> tuple[bytes, list[Mapping[str, Any]]]:
    """Encode the next batched insert, update, or delete operation
    as OP_MSG.
    """
    buf = _BytesIO()

    to_send, _ = _batched_op_msg_impl(operation, command, docs, ack, opts, ctx, buf)
    return buf.getvalue(), to_send


if _use_c:
    _encode_batched_op_msg = _cmessage._encode_batched_op_msg


def _batched_op_msg_compressed(
    operation: int,
    command: Mapping[str, Any],
    docs: list[Mapping[str, Any]],
    ack: bool,
    opts: CodecOptions,
    ctx: _BulkWriteContext,
) -> tuple[int, bytes, list[Mapping[str, Any]]]:
    """Create the next batched insert, update, or delete operation
    with OP_MSG, compressed.
    """
    data, to_send = _encode_batched_op_msg(operation, command, docs, ack, opts, ctx)

    assert ctx.conn.compression_context is not None
    request_id, msg = _compress(2013, data, ctx.conn.compression_context)
    return request_id, msg, to_send


def _batched_op_msg(
    operation: int,
    command: Mapping[str, Any],
    docs: list[Mapping[str, Any]],
    ack: bool,
    opts: CodecOptions,
    ctx: _BulkWriteContext,
) -> tuple[int, bytes, list[Mapping[str, Any]]]:
    """OP_MSG implementation entry point."""
    buf = _BytesIO()

    # Save space for message length and request id
    buf.write(_ZERO_64)
    # responseTo, opCode
    buf.write(b"\x00\x00\x00\x00\xdd\x07\x00\x00")

    to_send, length = _batched_op_msg_impl(operation, command, docs, ack, opts, ctx, buf)

    # Header - request id and message length
    buf.seek(4)
    request_id = _randint()
    buf.write(_pack_int(request_id))
    buf.seek(0)
    buf.write(_pack_int(length))

    return request_id, buf.getvalue(), to_send


if _use_c:
    _batched_op_msg = _cmessage._batched_op_msg


def _do_batched_op_msg(
    namespace: str,
    operation: int,
    command: MutableMapping[str, Any],
    docs: list[Mapping[str, Any]],
    opts: CodecOptions,
    ctx: _BulkWriteContext,
) -> tuple[int, bytes, list[Mapping[str, Any]]]:
    """Create the next batched insert, update, or delete operation
    using OP_MSG.
    """
    command["$db"] = namespace.split(".", 1)[0]
    if "writeConcern" in command:
        ack = bool(command["writeConcern"].get("w", 1))
    else:
        ack = True
    if ctx.conn.compression_context:
        return _batched_op_msg_compressed(operation, command, docs, ack, opts, ctx)
    return _batched_op_msg(operation, command, docs, ack, opts, ctx)


# End OP_MSG -----------------------------------------------------


def _encode_batched_write_command(
    namespace: str,
    operation: int,
    command: MutableMapping[str, Any],
    docs: list[Mapping[str, Any]],
    opts: CodecOptions,
    ctx: _BulkWriteContext,
) -> tuple[bytes, list[Mapping[str, Any]]]:
    """Encode the next batched insert, update, or delete command."""
    buf = _BytesIO()

    to_send, _ = _batched_write_command_impl(namespace, operation, command, docs, opts, ctx, buf)
    return buf.getvalue(), to_send


if _use_c:
    _encode_batched_write_command = _cmessage._encode_batched_write_command


def _batched_write_command_impl(
    namespace: str,
    operation: int,
    command: MutableMapping[str, Any],
    docs: list[Mapping[str, Any]],
    opts: CodecOptions,
    ctx: _BulkWriteContext,
    buf: _BytesIO,
) -> tuple[list[Mapping[str, Any]], int]:
    """Create a batched OP_QUERY write command."""
    max_bson_size = ctx.max_bson_size
    max_write_batch_size = ctx.max_write_batch_size
    # Max BSON object size + 16k - 2 bytes for ending NUL bytes.
    # Server guarantees there is enough room: SERVER-10643.
    max_cmd_size = max_bson_size + _COMMAND_OVERHEAD
    max_split_size = ctx.max_split_size

    # No options
    buf.write(_ZERO_32)
    # Namespace as C string
    buf.write(namespace.encode("utf8"))
    buf.write(_ZERO_8)
    # Skip: 0, Limit: -1
    buf.write(_SKIPLIM)

    # Where to write command document length
    command_start = buf.tell()
    buf.write(encode(command))

    # Start of payload
    buf.seek(-1, 2)
    # Work around some Jython weirdness.
    buf.truncate()
    try:
        buf.write(_OP_MAP[operation])
    except KeyError:
        raise InvalidOperation("Unknown command") from None

    # Where to write list document length
    list_start = buf.tell() - 4
    to_send = []
    idx = 0
    for doc in docs:
        # Encode the current operation
        key = str(idx).encode("utf8")
        value = _dict_to_bson(doc, False, opts)
        # Is there enough room to add this document? max_cmd_size accounts for
        # the two trailing null bytes.
        doc_too_large = len(value) > max_cmd_size
        if doc_too_large:
            write_op = list(_FIELD_MAP.keys())[operation]
            _raise_document_too_large(write_op, len(value), max_bson_size)
        enough_data = idx >= 1 and (buf.tell() + len(key) + len(value)) >= max_split_size
        enough_documents = idx >= max_write_batch_size
        if enough_data or enough_documents:
            break
        buf.write(_BSONOBJ)
        buf.write(key)
        buf.write(_ZERO_8)
        buf.write(value)
        to_send.append(doc)
        idx += 1

    # Finalize the current OP_QUERY message.
    # Close list and command documents
    buf.write(_ZERO_16)

    # Write document lengths and request id
    length = buf.tell()
    buf.seek(list_start)
    buf.write(_pack_int(length - list_start - 1))
    buf.seek(command_start)
    buf.write(_pack_int(length - command_start))

    return to_send, length
