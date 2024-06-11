# Copyright 2024-present MongoDB, Inc.
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

import random
import struct
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Union,
)

import bson
from bson import CodecOptions, _dict_to_bson
from bson.int64 import Int64
from bson.raw_bson import (
    _RAW_ARRAY_BSON_OPTIONS,
    RawBSONDocument,
)
from pymongo.hello_compat import HelloCompat

try:
    from pymongo import _cmessage  # type: ignore[attr-defined]

    _use_c = True
except ImportError:
    _use_c = False
from pymongo.errors import (
    ConfigurationError,
    CursorNotFound,
    DocumentTooLarge,
    ExecutionTimeout,
    NotPrimaryError,
    OperationFailure,
    ProtocolError,
)
from pymongo.read_preferences import ReadPreference, _ServerMode

if TYPE_CHECKING:
    from pymongo.asynchronous.client_session import AsyncClientSession
    from pymongo.asynchronous.mongo_client import AsyncMongoClient
    from pymongo.asynchronous.pool import AsyncConnection
    from pymongo.compression_support import SnappyContext, ZlibContext, ZstdContext
    from pymongo.read_concern import ReadConcern
    from pymongo.typings import _Address, _AgnosticClientSession, _AgnosticConnection

MAX_INT32 = 2147483647
MIN_INT32 = -2147483648

# Overhead allowed for encoded command documents.
_COMMAND_OVERHEAD = 16382

_INSERT = 0
_UPDATE = 1
_DELETE = 2

_EMPTY = b""
_BSONOBJ = b"\x03"
_ZERO_8 = b"\x00"
_ZERO_16 = b"\x00\x00"
_ZERO_32 = b"\x00\x00\x00\x00"
_ZERO_64 = b"\x00\x00\x00\x00\x00\x00\x00\x00"
_SKIPLIM = b"\x00\x00\x00\x00\xff\xff\xff\xff"
_OP_MAP = {
    _INSERT: b"\x04documents\x00\x00\x00\x00\x00",
    _UPDATE: b"\x04updates\x00\x00\x00\x00\x00",
    _DELETE: b"\x04deletes\x00\x00\x00\x00\x00",
}
_FIELD_MAP = {"insert": "documents", "update": "updates", "delete": "deletes"}

_UNICODE_REPLACE_CODEC_OPTIONS: CodecOptions[Mapping[str, Any]] = CodecOptions(
    unicode_decode_error_handler="replace"
)


def _randint() -> int:
    """Generate a pseudo random 32 bit integer."""
    return random.randint(MIN_INT32, MAX_INT32)  # noqa: S311


def _maybe_add_read_preference(
    spec: MutableMapping[str, Any], read_preference: _ServerMode
) -> MutableMapping[str, Any]:
    """Add $readPreference to spec when appropriate."""
    mode = read_preference.mode
    document = read_preference.document
    # Only add $readPreference if it's something other than primary to avoid
    # problems with mongos versions that don't support read preferences. Also,
    # for maximum backwards compatibility, don't add $readPreference for
    # secondaryPreferred unless tags or maxStalenessSeconds are in use (setting
    # the secondaryOkay bit has the same effect).
    if mode and (mode != ReadPreference.SECONDARY_PREFERRED.mode or len(document) > 1):
        if "$query" not in spec:
            spec = {"$query": spec}
        spec["$readPreference"] = document
    return spec


def _convert_exception(exception: Exception) -> dict[str, Any]:
    """Convert an Exception into a failure document for publishing."""
    return {"errmsg": str(exception), "errtype": exception.__class__.__name__}


def _convert_write_result(
    operation: str, command: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    """Convert a legacy write result to write command format."""
    # Based on _merge_legacy from bulk.py
    affected = result.get("n", 0)
    res = {"ok": 1, "n": affected}
    errmsg = result.get("errmsg", result.get("err", ""))
    if errmsg:
        # The write was successful on at least the primary so don't return.
        if result.get("wtimeout"):
            res["writeConcernError"] = {"errmsg": errmsg, "code": 64, "errInfo": {"wtimeout": True}}
        else:
            # The write failed.
            error = {"index": 0, "code": result.get("code", 8), "errmsg": errmsg}
            if "errInfo" in result:
                error["errInfo"] = result["errInfo"]
            res["writeErrors"] = [error]
            return res
    if operation == "insert":
        # GLE result for insert is always 0 in most MongoDB versions.
        res["n"] = len(command["documents"])
    elif operation == "update":
        if "upserted" in result:
            res["upserted"] = [{"index": 0, "_id": result["upserted"]}]
        # Versions of MongoDB before 2.6 don't return the _id for an
        # upsert if _id is not an ObjectId.
        elif result.get("updatedExisting") is False and affected == 1:
            # If _id is in both the update document *and* the query spec
            # the update document _id takes precedence.
            update = command["updates"][0]
            _id = update["u"].get("_id", update["q"].get("_id"))
            res["upserted"] = [{"index": 0, "_id": _id}]
    return res


_OPTIONS = {
    "tailable": 2,
    "oplogReplay": 8,
    "noCursorTimeout": 16,
    "awaitData": 32,
    "allowPartialResults": 128,
}


_MODIFIERS = {
    "$query": "filter",
    "$orderby": "sort",
    "$hint": "hint",
    "$comment": "comment",
    "$maxScan": "maxScan",
    "$maxTimeMS": "maxTimeMS",
    "$max": "max",
    "$min": "min",
    "$returnKey": "returnKey",
    "$showRecordId": "showRecordId",
    "$showDiskLoc": "showRecordId",  # <= MongoDb 3.0
    "$snapshot": "snapshot",
}


def _gen_find_command(
    coll: str,
    spec: Mapping[str, Any],
    projection: Optional[Union[Mapping[str, Any], Iterable[str]]],
    skip: int,
    limit: int,
    batch_size: Optional[int],
    options: Optional[int],
    read_concern: ReadConcern,
    collation: Optional[Mapping[str, Any]] = None,
    session: Optional[_AgnosticClientSession] = None,
    allow_disk_use: Optional[bool] = None,
) -> dict[str, Any]:
    """Generate a find command document."""
    cmd: dict[str, Any] = {"find": coll}
    if "$query" in spec:
        cmd.update(
            [
                (_MODIFIERS[key], val) if key in _MODIFIERS else (key, val)
                for key, val in spec.items()
            ]
        )
        if "$explain" in cmd:
            cmd.pop("$explain")
        if "$readPreference" in cmd:
            cmd.pop("$readPreference")
    else:
        cmd["filter"] = spec

    if projection:
        cmd["projection"] = projection
    if skip:
        cmd["skip"] = skip
    if limit:
        cmd["limit"] = abs(limit)
        if limit < 0:
            cmd["singleBatch"] = True
    if batch_size:
        cmd["batchSize"] = batch_size
    if read_concern.level and not (session and session.in_transaction):
        cmd["readConcern"] = read_concern.document
    if collation:
        cmd["collation"] = collation
    if allow_disk_use is not None:
        cmd["allowDiskUse"] = allow_disk_use
    if options:
        cmd.update([(opt, True) for opt, val in _OPTIONS.items() if options & val])

    return cmd


def _gen_get_more_command(
    cursor_id: Optional[int],
    coll: str,
    batch_size: Optional[int],
    max_await_time_ms: Optional[int],
    comment: Optional[Any],
    conn: _AgnosticConnection,
) -> dict[str, Any]:
    """Generate a getMore command document."""
    cmd: dict[str, Any] = {"getMore": cursor_id, "collection": coll}
    if batch_size:
        cmd["batchSize"] = batch_size
    if max_await_time_ms is not None:
        cmd["maxTimeMS"] = max_await_time_ms
    if comment is not None and conn.max_wire_version >= 9:
        cmd["comment"] = comment
    return cmd


class _OpReply:
    """A MongoDB OP_REPLY response message."""

    __slots__ = ("flags", "cursor_id", "number_returned", "documents")

    UNPACK_FROM = struct.Struct("<iqii").unpack_from
    OP_CODE = 1

    def __init__(self, flags: int, cursor_id: int, number_returned: int, documents: bytes):
        self.flags = flags
        self.cursor_id = Int64(cursor_id)
        self.number_returned = number_returned
        self.documents = documents

    def raw_response(
        self, cursor_id: Optional[int] = None, user_fields: Optional[Mapping[str, Any]] = None
    ) -> list[bytes]:
        """Check the response header from the database, without decoding BSON.

        Check the response for errors and unpack.

        Can raise CursorNotFound, NotPrimaryError, ExecutionTimeout, or
        OperationFailure.

        :param cursor_id: cursor_id we sent to get this response -
            used for raising an informative exception when we get cursor id not
            valid at server response.
        """
        if self.flags & 1:
            # Shouldn't get this response if we aren't doing a getMore
            if cursor_id is None:
                raise ProtocolError("No cursor id for getMore operation")

            # Fake a getMore command response. OP_GET_MORE provides no
            # document.
            msg = "Cursor not found, cursor id: %d" % (cursor_id,)
            errobj = {"ok": 0, "errmsg": msg, "code": 43}
            raise CursorNotFound(msg, 43, errobj)
        elif self.flags & 2:
            error_object: dict = bson.BSON(self.documents).decode()
            # Fake the ok field if it doesn't exist.
            error_object.setdefault("ok", 0)
            if error_object["$err"].startswith(HelloCompat.LEGACY_ERROR):
                raise NotPrimaryError(error_object["$err"], error_object)
            elif error_object.get("code") == 50:
                default_msg = "operation exceeded time limit"
                raise ExecutionTimeout(
                    error_object.get("$err", default_msg), error_object.get("code"), error_object
                )
            raise OperationFailure(
                "database error: %s" % error_object.get("$err"),
                error_object.get("code"),
                error_object,
            )
        if self.documents:
            return [self.documents]
        return []

    def unpack_response(
        self,
        cursor_id: Optional[int] = None,
        codec_options: CodecOptions = _UNICODE_REPLACE_CODEC_OPTIONS,
        user_fields: Optional[Mapping[str, Any]] = None,
        legacy_response: bool = False,
    ) -> list[dict[str, Any]]:
        """Unpack a response from the database and decode the BSON document(s).

        Check the response for errors and unpack, returning a dictionary
        containing the response data.

        Can raise CursorNotFound, NotPrimaryError, ExecutionTimeout, or
        OperationFailure.

        :param cursor_id: cursor_id we sent to get this response -
            used for raising an informative exception when we get cursor id not
            valid at server response
        :param codec_options: an instance of
            :class:`~bson.codec_options.CodecOptions`
        :param user_fields: Response fields that should be decoded
            using the TypeDecoders from codec_options, passed to
            bson._decode_all_selective.
        """
        self.raw_response(cursor_id)
        if legacy_response:
            return bson.decode_all(self.documents, codec_options)
        return bson._decode_all_selective(self.documents, codec_options, user_fields)

    def command_response(self, codec_options: CodecOptions) -> dict[str, Any]:
        """Unpack a command response."""
        docs = self.unpack_response(codec_options=codec_options)
        assert self.number_returned == 1
        return docs[0]

    def raw_command_response(self) -> NoReturn:
        """Return the bytes of the command response."""
        # This should never be called on _OpReply.
        raise NotImplementedError

    @property
    def more_to_come(self) -> bool:
        """Is the moreToCome bit set on this response?"""
        return False

    @classmethod
    def unpack(cls, msg: bytes) -> _OpReply:
        """Construct an _OpReply from raw bytes."""
        # PYTHON-945: ignore starting_from field.
        flags, cursor_id, _, number_returned = cls.UNPACK_FROM(msg)

        documents = msg[20:]
        return cls(flags, cursor_id, number_returned, documents)


class _OpMsg:
    """A MongoDB OP_MSG response message."""

    __slots__ = ("flags", "cursor_id", "number_returned", "payload_document")

    UNPACK_FROM = struct.Struct("<IBi").unpack_from
    OP_CODE = 2013

    # Flag bits.
    CHECKSUM_PRESENT = 1
    MORE_TO_COME = 1 << 1
    EXHAUST_ALLOWED = 1 << 16  # Only present on requests.

    def __init__(self, flags: int, payload_document: bytes):
        self.flags = flags
        self.payload_document = payload_document

    def raw_response(
        self,
        cursor_id: Optional[int] = None,
        user_fields: Optional[Mapping[str, Any]] = {},
    ) -> list[Mapping[str, Any]]:
        """
        cursor_id is ignored
        user_fields is used to determine which fields must not be decoded
        """
        inflated_response = bson._decode_selective(
            RawBSONDocument(self.payload_document), user_fields, _RAW_ARRAY_BSON_OPTIONS
        )
        return [inflated_response]

    def unpack_response(
        self,
        cursor_id: Optional[int] = None,
        codec_options: CodecOptions = _UNICODE_REPLACE_CODEC_OPTIONS,
        user_fields: Optional[Mapping[str, Any]] = None,
        legacy_response: bool = False,
    ) -> list[dict[str, Any]]:
        """Unpack a OP_MSG command response.

        :param cursor_id: Ignored, for compatibility with _OpReply.
        :param codec_options: an instance of
            :class:`~bson.codec_options.CodecOptions`
        :param user_fields: Response fields that should be decoded
            using the TypeDecoders from codec_options, passed to
            bson._decode_all_selective.
        """
        # If _OpMsg is in-use, this cannot be a legacy response.
        assert not legacy_response
        return bson._decode_all_selective(self.payload_document, codec_options, user_fields)

    def command_response(self, codec_options: CodecOptions) -> dict[str, Any]:
        """Unpack a command response."""
        return self.unpack_response(codec_options=codec_options)[0]

    def raw_command_response(self) -> bytes:
        """Return the bytes of the command response."""
        return self.payload_document

    @property
    def more_to_come(self) -> bool:
        """Is the moreToCome bit set on this response?"""
        return bool(self.flags & self.MORE_TO_COME)

    @classmethod
    def unpack(cls, msg: bytes) -> _OpMsg:
        """Construct an _OpMsg from raw bytes."""
        flags, first_payload_type, first_payload_size = cls.UNPACK_FROM(msg)
        if flags != 0:
            if flags & cls.CHECKSUM_PRESENT:
                raise ProtocolError(f"Unsupported OP_MSG flag checksumPresent: 0x{flags:x}")

            if flags ^ cls.MORE_TO_COME:
                raise ProtocolError(f"Unsupported OP_MSG flags: 0x{flags:x}")
        if first_payload_type != 0:
            raise ProtocolError(f"Unsupported OP_MSG payload type: 0x{first_payload_type:x}")

        if len(msg) != first_payload_size + 5:
            raise ProtocolError("Unsupported OP_MSG reply: >1 section")

        payload_document = msg[5:]
        return cls(flags, payload_document)


_UNPACK_REPLY: dict[int, Callable[[bytes], Union[_OpReply, _OpMsg]]] = {
    _OpReply.OP_CODE: _OpReply.unpack,
    _OpMsg.OP_CODE: _OpMsg.unpack,
}


class _Query:
    """A query operation."""

    __slots__ = (
        "flags",
        "db",
        "coll",
        "ntoskip",
        "spec",
        "fields",
        "codec_options",
        "read_preference",
        "limit",
        "batch_size",
        "name",
        "read_concern",
        "collation",
        "session",
        "client",
        "allow_disk_use",
        "_as_command",
        "exhaust",
    )

    # For compatibility with the _GetMore class.
    conn_mgr = None
    cursor_id = None

    def __init__(
        self,
        flags: int,
        db: str,
        coll: str,
        ntoskip: int,
        spec: Mapping[str, Any],
        fields: Optional[Mapping[str, Any]],
        codec_options: CodecOptions,
        read_preference: _ServerMode,
        limit: int,
        batch_size: int,
        read_concern: ReadConcern,
        collation: Optional[Mapping[str, Any]],
        session: Optional[AsyncClientSession],
        client: AsyncMongoClient,
        allow_disk_use: Optional[bool],
        exhaust: bool,
    ):
        self.flags = flags
        self.db = db
        self.coll = coll
        self.ntoskip = ntoskip
        self.spec = spec
        self.fields = fields
        self.codec_options = codec_options
        self.read_preference = read_preference
        self.read_concern = read_concern
        self.limit = limit
        self.batch_size = batch_size
        self.collation = collation
        self.session = session
        self.client = client
        self.allow_disk_use = allow_disk_use
        self.name = "find"
        self._as_command: Optional[tuple[dict[str, Any], str]] = None
        self.exhaust = exhaust

    def reset(self) -> None:
        self._as_command = None

    def namespace(self) -> str:
        return f"{self.db}.{self.coll}"

    def use_command(self, conn: AsyncConnection) -> bool:
        use_find_cmd = False
        if not self.exhaust:
            use_find_cmd = True
        elif conn.max_wire_version >= 8:
            # OP_MSG supports exhaust on MongoDB 4.2+
            use_find_cmd = True
        elif not self.read_concern.ok_for_legacy:
            raise ConfigurationError(
                "read concern level of %s is not valid "
                "with a max wire version of %d." % (self.read_concern.level, conn.max_wire_version)
            )

        conn.validate_session(self.client, self.session)
        return use_find_cmd

    def as_command(self, dummy0: Optional[Any] = None) -> tuple[dict[str, Any], str]:
        """Return a find command document for this query."""
        # We use the command twice: on the wire and for command monitoring.
        # Generate it once, for speed and to avoid repeating side-effects.
        if self._as_command is not None:
            return self._as_command

        explain = "$explain" in self.spec
        cmd: dict[str, Any] = _gen_find_command(
            self.coll,
            self.spec,
            self.fields,
            self.ntoskip,
            self.limit,
            self.batch_size,
            self.flags,
            self.read_concern,
            self.collation,
            self.session,
            self.allow_disk_use,
        )
        if explain:
            self.name = "explain"
            cmd = {"explain": cmd}
        self._as_command = cmd, self.db
        return self._as_command

    def get_message(
        self, read_preference: _ServerMode, conn: AsyncConnection, use_cmd: bool = False
    ) -> tuple[int, bytes, int]:
        """Get a query message, possibly setting the secondaryOk bit."""
        # Use the read_preference decided by _socket_from_server.
        self.read_preference = read_preference
        if read_preference.mode:
            # Set the secondaryOk bit.
            flags = self.flags | 4
        else:
            flags = self.flags

        ns = self.namespace()
        spec = self.spec

        if use_cmd:
            spec = self.as_command(None)[0]
            request_id, msg, size, _ = _op_msg(
                0,
                spec,
                self.db,
                read_preference,
                self.codec_options,
                ctx=conn.compression_context,
            )
            return request_id, msg, size

        # OP_QUERY treats ntoreturn of -1 and 1 the same, return
        # one document and close the cursor. We have to use 2 for
        # batch size if 1 is specified.
        ntoreturn = self.batch_size == 1 and 2 or self.batch_size
        if self.limit:
            if ntoreturn:
                ntoreturn = min(self.limit, ntoreturn)
            else:
                ntoreturn = self.limit

        if conn.is_mongos:
            assert isinstance(spec, MutableMapping)
            spec = _maybe_add_read_preference(spec, read_preference)

        return _query(
            flags,
            ns,
            self.ntoskip,
            ntoreturn,
            spec,
            None if use_cmd else self.fields,
            self.codec_options,
            ctx=conn.compression_context,
        )


class _GetMore:
    """A getmore operation."""

    __slots__ = (
        "db",
        "coll",
        "ntoreturn",
        "cursor_id",
        "max_await_time_ms",
        "codec_options",
        "read_preference",
        "session",
        "client",
        "conn_mgr",
        "_as_command",
        "exhaust",
        "comment",
    )

    name = "getMore"

    def __init__(
        self,
        db: str,
        coll: str,
        ntoreturn: int,
        cursor_id: int,
        codec_options: CodecOptions,
        read_preference: _ServerMode,
        session: Optional[AsyncClientSession],
        client: AsyncMongoClient,
        max_await_time_ms: Optional[int],
        conn_mgr: Any,
        exhaust: bool,
        comment: Any,
    ):
        self.db = db
        self.coll = coll
        self.ntoreturn = ntoreturn
        self.cursor_id = cursor_id
        self.codec_options = codec_options
        self.read_preference = read_preference
        self.session = session
        self.client = client
        self.max_await_time_ms = max_await_time_ms
        self.conn_mgr = conn_mgr
        self._as_command: Optional[tuple[dict[str, Any], str]] = None
        self.exhaust = exhaust
        self.comment = comment

    def reset(self) -> None:
        self._as_command = None

    def namespace(self) -> str:
        return f"{self.db}.{self.coll}"

    def use_command(self, conn: AsyncConnection) -> bool:
        use_cmd = False
        if not self.exhaust:
            use_cmd = True
        elif conn.max_wire_version >= 8:
            # OP_MSG supports exhaust on MongoDB 4.2+
            use_cmd = True

        conn.validate_session(self.client, self.session)
        return use_cmd

    def as_command(self, conn: AsyncConnection) -> tuple[dict[str, Any], str]:
        """Return a getMore command document for this query."""
        # See _Query.as_command for an explanation of this caching.
        if self._as_command is not None:
            return self._as_command

        cmd: dict[str, Any] = _gen_get_more_command(
            self.cursor_id,
            self.coll,
            self.ntoreturn,
            self.max_await_time_ms,
            self.comment,
            conn,
        )
        self._as_command = cmd, self.db
        return self._as_command

    def get_message(
        self, dummy0: Any, conn: AsyncConnection, use_cmd: bool = False
    ) -> Union[tuple[int, bytes, int], tuple[int, bytes]]:
        """Get a getmore message."""
        ns = self.namespace()
        ctx = conn.compression_context

        if use_cmd:
            spec = self.as_command(conn)[0]
            if self.conn_mgr and self.exhaust:
                flags = _OpMsg.EXHAUST_ALLOWED
            else:
                flags = 0
            request_id, msg, size, _ = _op_msg(
                flags, spec, self.db, None, self.codec_options, ctx=conn.compression_context
            )
            return request_id, msg, size

        return _get_more(ns, self.ntoreturn, self.cursor_id, ctx)


class _RawBatchQuery(_Query):
    def use_command(self, conn: AsyncConnection) -> bool:
        # Compatibility checks.
        super().use_command(conn)
        if conn.max_wire_version >= 8:
            # MongoDB 4.2+ supports exhaust over OP_MSG
            return True
        elif not self.exhaust:
            return True
        return False


class _RawBatchGetMore(_GetMore):
    def use_command(self, conn: AsyncConnection) -> bool:
        # Compatibility checks.
        super().use_command(conn)
        if conn.max_wire_version >= 8:
            # MongoDB 4.2+ supports exhaust over OP_MSG
            return True
        elif not self.exhaust:
            return True
        return False


class _CursorAddress(tuple):
    """The server address (host, port) of a cursor, with namespace property."""

    __namespace: Any

    def __new__(cls, address: _Address, namespace: str) -> _CursorAddress:
        self = tuple.__new__(cls, address)
        self.__namespace = namespace
        return self

    @property
    def namespace(self) -> str:
        """The namespace this cursor."""
        return self.__namespace

    def __hash__(self) -> int:
        # Two _CursorAddress instances with different namespaces
        # must not hash the same.
        return ((*self, self.__namespace)).__hash__()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _CursorAddress):
            return tuple(self) == tuple(other) and self.namespace == other.namespace
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        return not self == other


_pack_compression_header = struct.Struct("<iiiiiiB").pack
_COMPRESSION_HEADER_SIZE = 25


def _compress(
    operation: int, data: bytes, ctx: Union[SnappyContext, ZlibContext, ZstdContext]
) -> tuple[int, bytes]:
    """Takes message data, compresses it, and adds an OP_COMPRESSED header."""
    compressed = ctx.compress(data)
    request_id = _randint()

    header = _pack_compression_header(
        _COMPRESSION_HEADER_SIZE + len(compressed),  # Total message length
        request_id,  # Request id
        0,  # responseTo
        2012,  # operation id
        operation,  # original operation id
        len(data),  # uncompressed message length
        ctx.compressor_id,
    )  # compressor id
    return request_id, header + compressed


_pack_header = struct.Struct("<iiii").pack


def __pack_message(operation: int, data: bytes) -> tuple[int, bytes]:
    """Takes message data and adds a message header based on the operation.

    Returns the resultant message string.
    """
    rid = _randint()
    message = _pack_header(16 + len(data), rid, 0, operation)
    return rid, message + data


_pack_int = struct.Struct("<i").pack
_pack_op_msg_flags_type = struct.Struct("<IB").pack
_pack_byte = struct.Struct("<B").pack


def _op_msg_no_header(
    flags: int,
    command: Mapping[str, Any],
    identifier: str,
    docs: Optional[list[Mapping[str, Any]]],
    opts: CodecOptions,
) -> tuple[bytes, int, int]:
    """Get a OP_MSG message.

    Note: this method handles multiple documents in a type one payload but
    it does not perform batch splitting and the total message size is
    only checked *after* generating the entire message.
    """
    # Encode the command document in payload 0 without checking keys.
    encoded = _dict_to_bson(command, False, opts)
    flags_type = _pack_op_msg_flags_type(flags, 0)
    total_size = len(encoded)
    max_doc_size = 0
    if identifier and docs is not None:
        type_one = _pack_byte(1)
        cstring = bson._make_c_string(identifier)
        encoded_docs = [_dict_to_bson(doc, False, opts) for doc in docs]
        size = len(cstring) + sum(len(doc) for doc in encoded_docs) + 4
        encoded_size = _pack_int(size)
        total_size += size
        max_doc_size = max(len(doc) for doc in encoded_docs)
        data = [flags_type, encoded, type_one, encoded_size, cstring, *encoded_docs]
    else:
        data = [flags_type, encoded]
    return b"".join(data), total_size, max_doc_size


def _op_msg_compressed(
    flags: int,
    command: Mapping[str, Any],
    identifier: str,
    docs: Optional[list[Mapping[str, Any]]],
    opts: CodecOptions,
    ctx: Union[SnappyContext, ZlibContext, ZstdContext],
) -> tuple[int, bytes, int, int]:
    """Internal OP_MSG message helper."""
    msg, total_size, max_bson_size = _op_msg_no_header(flags, command, identifier, docs, opts)
    rid, msg = _compress(2013, msg, ctx)
    return rid, msg, total_size, max_bson_size


def _op_msg_uncompressed(
    flags: int,
    command: Mapping[str, Any],
    identifier: str,
    docs: Optional[list[Mapping[str, Any]]],
    opts: CodecOptions,
) -> tuple[int, bytes, int, int]:
    """Internal compressed OP_MSG message helper."""
    data, total_size, max_bson_size = _op_msg_no_header(flags, command, identifier, docs, opts)
    request_id, op_message = __pack_message(2013, data)
    return request_id, op_message, total_size, max_bson_size


if _use_c:
    _op_msg_uncompressed = _cmessage._op_msg


def _op_msg(
    flags: int,
    command: MutableMapping[str, Any],
    dbname: str,
    read_preference: Optional[_ServerMode],
    opts: CodecOptions,
    ctx: Union[SnappyContext, ZlibContext, ZstdContext, None] = None,
) -> tuple[int, bytes, int, int]:
    """Get a OP_MSG message."""
    command["$db"] = dbname
    # getMore commands do not send $readPreference.
    if read_preference is not None and "$readPreference" not in command:
        # Only send $readPreference if it's not primary (the default).
        if read_preference.mode:
            command["$readPreference"] = read_preference.document
    name = next(iter(command))
    try:
        identifier = _FIELD_MAP[name]
        docs = command.pop(identifier)
    except KeyError:
        identifier = ""
        docs = None
    try:
        if ctx:
            return _op_msg_compressed(flags, command, identifier, docs, opts, ctx)
        return _op_msg_uncompressed(flags, command, identifier, docs, opts)
    finally:
        # Add the field back to the command.
        if identifier:
            command[identifier] = docs


def _query_impl(
    options: int,
    collection_name: str,
    num_to_skip: int,
    num_to_return: int,
    query: Mapping[str, Any],
    field_selector: Optional[Mapping[str, Any]],
    opts: CodecOptions,
) -> tuple[bytes, int]:
    """Get an OP_QUERY message."""
    encoded = _dict_to_bson(query, False, opts)
    if field_selector:
        efs = _dict_to_bson(field_selector, False, opts)
    else:
        efs = b""
    max_bson_size = max(len(encoded), len(efs))
    return (
        b"".join(
            [
                _pack_int(options),
                bson._make_c_string(collection_name),
                _pack_int(num_to_skip),
                _pack_int(num_to_return),
                encoded,
                efs,
            ]
        ),
        max_bson_size,
    )


def _query_compressed(
    options: int,
    collection_name: str,
    num_to_skip: int,
    num_to_return: int,
    query: Mapping[str, Any],
    field_selector: Optional[Mapping[str, Any]],
    opts: CodecOptions,
    ctx: Union[SnappyContext, ZlibContext, ZstdContext],
) -> tuple[int, bytes, int]:
    """Internal compressed query message helper."""
    op_query, max_bson_size = _query_impl(
        options, collection_name, num_to_skip, num_to_return, query, field_selector, opts
    )
    rid, msg = _compress(2004, op_query, ctx)
    return rid, msg, max_bson_size


def _query_uncompressed(
    options: int,
    collection_name: str,
    num_to_skip: int,
    num_to_return: int,
    query: Mapping[str, Any],
    field_selector: Optional[Mapping[str, Any]],
    opts: CodecOptions,
) -> tuple[int, bytes, int]:
    """Internal query message helper."""
    op_query, max_bson_size = _query_impl(
        options, collection_name, num_to_skip, num_to_return, query, field_selector, opts
    )
    rid, msg = __pack_message(2004, op_query)
    return rid, msg, max_bson_size


if _use_c:
    _query_uncompressed = _cmessage._query_message


def _query(
    options: int,
    collection_name: str,
    num_to_skip: int,
    num_to_return: int,
    query: Mapping[str, Any],
    field_selector: Optional[Mapping[str, Any]],
    opts: CodecOptions,
    ctx: Union[SnappyContext, ZlibContext, ZstdContext, None] = None,
) -> tuple[int, bytes, int]:
    """Get a **query** message."""
    if ctx:
        return _query_compressed(
            options, collection_name, num_to_skip, num_to_return, query, field_selector, opts, ctx
        )
    return _query_uncompressed(
        options, collection_name, num_to_skip, num_to_return, query, field_selector, opts
    )


_pack_long_long = struct.Struct("<q").pack


def _get_more_impl(collection_name: str, num_to_return: int, cursor_id: int) -> bytes:
    """Get an OP_GET_MORE message."""
    return b"".join(
        [
            _ZERO_32,
            bson._make_c_string(collection_name),
            _pack_int(num_to_return),
            _pack_long_long(cursor_id),
        ]
    )


def _get_more_compressed(
    collection_name: str,
    num_to_return: int,
    cursor_id: int,
    ctx: Union[SnappyContext, ZlibContext, ZstdContext],
) -> tuple[int, bytes]:
    """Internal compressed getMore message helper."""
    return _compress(2005, _get_more_impl(collection_name, num_to_return, cursor_id), ctx)


def _get_more_uncompressed(
    collection_name: str, num_to_return: int, cursor_id: int
) -> tuple[int, bytes]:
    """Internal getMore message helper."""
    return __pack_message(2005, _get_more_impl(collection_name, num_to_return, cursor_id))


if _use_c:
    _get_more_uncompressed = _cmessage._get_more_message


def _get_more(
    collection_name: str,
    num_to_return: int,
    cursor_id: int,
    ctx: Union[SnappyContext, ZlibContext, ZstdContext, None] = None,
) -> tuple[int, bytes]:
    """Get a **getMore** message."""
    if ctx:
        return _get_more_compressed(collection_name, num_to_return, cursor_id, ctx)
    return _get_more_uncompressed(collection_name, num_to_return, cursor_id)


def _raise_document_too_large(operation: str, doc_size: int, max_size: int) -> NoReturn:
    """Internal helper for raising DocumentTooLarge."""
    if operation == "insert":
        raise DocumentTooLarge(
            "BSON document too large (%d bytes)"
            " - the connected server supports"
            " BSON document sizes up to %d"
            " bytes." % (doc_size, max_size)
        )
    else:
        # There's nothing intelligent we can say
        # about size for update and delete
        raise DocumentTooLarge(f"{operation!r} command document too large")


# OP_MSG -------------------------------------------------------------


_OP_MSG_MAP = {
    _INSERT: b"documents\x00",
    _UPDATE: b"updates\x00",
    _DELETE: b"deletes\x00",
}
