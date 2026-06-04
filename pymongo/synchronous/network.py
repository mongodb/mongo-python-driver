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
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
)

from pymongo import _csot, message
from pymongo.compression_support import _NO_COMPRESSION
from pymongo.message import _OpMsg
from pymongo.monitoring import _is_speculative_authenticate
from pymongo.synchronous.command_runner import run_command

if TYPE_CHECKING:
    from bson import CodecOptions
    from pymongo.compression_support import SnappyContext, ZlibContext, ZstdContext
    from pymongo.monitoring import _EventListeners
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import _ServerMode
    from pymongo.synchronous.client_session import ClientSession
    from pymongo.synchronous.mongo_client import MongoClient
    from pymongo.synchronous.pool import Connection
    from pymongo.typings import _Address, _CollationIn, _DocumentType
    from pymongo.write_concern import WriteConcern

_IS_SYNC = True


def command(
    conn: Connection,
    dbname: str,
    spec: MutableMapping[str, Any],
    is_mongos: bool,  # noqa: ARG001
    read_preference: Optional[_ServerMode],
    codec_options: CodecOptions[_DocumentType],
    session: Optional[ClientSession],
    client: Optional[MongoClient[Any]],
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
    """Execute a command over the socket, or raise socket.error.

    :param conn: a Connection instance
    :param dbname: name of the database on which to run the command
    :param spec: a command document as an ordered dict type, eg SON.
    :param is_mongos: are we connected to a mongos?
    :param read_preference: a read preference
    :param codec_options: a CodecOptions instance
    :param session: optional ClientSession instance.
    :param client: optional MongoClient instance for updating $clusterTime.
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
    docs, _, _ = run_command(
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
        unacknowledged=unacknowledged,
        speculative_hello=speculative_hello,
    )
    return docs[0]  # type: ignore[return-value]
>>>>>>> 0d7dedb0 (PYTHON-5676 Add command_runner.run_command; route network.command() through it)
