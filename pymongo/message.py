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
<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol>`_ to be sent to
MongoDB.

.. note:: This module is for internal use and is generally not needed by
   application developers.
"""

import datetime
import random
import struct

import bson
from bson import CodecOptions
from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.py3compat import b, StringIO
from bson.son import SON

try:
    from pymongo import _cmessage
    _use_c = True
except ImportError:
    _use_c = False
from pymongo.errors import (ConfigurationError,
                            CursorNotFound,
                            DocumentTooLarge,
                            ExecutionTimeout,
                            InvalidOperation,
                            NotMasterError,
                            OperationFailure,
                            ProtocolError)
from pymongo.read_concern import DEFAULT_READ_CONCERN
from pymongo.read_preferences import ReadPreference


MAX_INT32 = 2147483647
MIN_INT32 = -2147483648

# Overhead allowed for encoded command documents.
_COMMAND_OVERHEAD = 16382

_INSERT = 0
_UPDATE = 1
_DELETE = 2

_EMPTY   = b''
_BSONOBJ = b'\x03'
_ZERO_8  = b'\x00'
_ZERO_16 = b'\x00\x00'
_ZERO_32 = b'\x00\x00\x00\x00'
_ZERO_64 = b'\x00\x00\x00\x00\x00\x00\x00\x00'
_SKIPLIM = b'\x00\x00\x00\x00\xff\xff\xff\xff'
_OP_MAP = {
    _INSERT: b'\x04documents\x00\x00\x00\x00\x00',
    _UPDATE: b'\x04updates\x00\x00\x00\x00\x00',
    _DELETE: b'\x04deletes\x00\x00\x00\x00\x00',
}

_UJOIN = u"%s.%s"

_UNICODE_REPLACE_CODEC_OPTIONS = CodecOptions(
    unicode_decode_error_handler='replace')


def _randint():
    """Generate a pseudo random 32 bit integer."""
    return random.randint(MIN_INT32, MAX_INT32)


def _maybe_add_read_preference(spec, read_preference):
    """Add $readPreference to spec when appropriate."""
    mode = read_preference.mode
    tag_sets = read_preference.tag_sets
    max_staleness = read_preference.max_staleness
    # Only add $readPreference if it's something other than primary to avoid
    # problems with mongos versions that don't support read preferences. Also,
    # for maximum backwards compatibility, don't add $readPreference for
    # secondaryPreferred unless tags or maxStalenessSeconds are in use (setting
    # the slaveOkay bit has the same effect).
    if mode and (
        mode != ReadPreference.SECONDARY_PREFERRED.mode
        or tag_sets != [{}]
        or max_staleness != -1):

        if "$query" not in spec:
            spec = SON([("$query", spec)])
        spec["$readPreference"] = read_preference.document
    return spec


def _convert_exception(exception):
    """Convert an Exception into a failure document for publishing."""
    return {'errmsg': str(exception),
            'errtype': exception.__class__.__name__}


def _convert_write_result(operation, command, result):
    """Convert a legacy write result to write commmand format."""

    # Based on _merge_legacy from bulk.py
    affected = result.get("n", 0)
    res = {"ok": 1, "n": affected}
    errmsg = result.get("errmsg", result.get("err", ""))
    if errmsg:
        # The write was successful on at least the primary so don't return.
        if result.get("wtimeout"):
            res["writeConcernError"] = {"errmsg": errmsg,
                                        "code": 64,
                                        "errInfo": {"wtimeout": True}}
        else:
            # The write failed.
            error = {"index": 0,
                     "code": result.get("code", 8),
                     "errmsg": errmsg}
            if "errInfo" in result:
                error["errInfo"] = result["errInfo"]
            res["writeErrors"] = [error]
            return res
    if operation == "insert":
        # GLE result for insert is always 0 in most MongoDB versions.
        res["n"] = len(command['documents'])
    elif operation == "update":
        if "upserted" in result:
            res["upserted"] = [{"index": 0, "_id": result["upserted"]}]
        # Versions of MongoDB before 2.6 don't return the _id for an
        # upsert if _id is not an ObjectId.
        elif result.get("updatedExisting") is False and affected == 1:
            # If _id is in both the update document *and* the query spec
            # the update document _id takes precedence.
            update = command['updates'][0]
            _id = update["u"].get("_id", update["q"].get("_id"))
            res["upserted"] = [{"index": 0, "_id": _id}]
    return res


_OPTIONS = SON([
    ('tailable', 2),
    ('oplogReplay', 8),
    ('noCursorTimeout', 16),
    ('awaitData', 32),
    ('allowPartialResults', 128)])


_MODIFIERS = SON([
    ('$query', 'filter'),
    ('$orderby', 'sort'),
    ('$hint', 'hint'),
    ('$comment', 'comment'),
    ('$maxScan', 'maxScan'),
    ('$maxTimeMS', 'maxTimeMS'),
    ('$max', 'max'),
    ('$min', 'min'),
    ('$returnKey', 'returnKey'),
    ('$showRecordId', 'showRecordId'),
    ('$showDiskLoc', 'showRecordId'),  # <= MongoDb 3.0
    ('$snapshot', 'snapshot')])


def _gen_find_command(coll, spec, projection, skip, limit, batch_size, options,
                      read_concern, collation=None):
    """Generate a find command document."""
    cmd = SON([('find', coll)])
    if '$query' in spec:
        cmd.update([(_MODIFIERS[key], val) if key in _MODIFIERS else (key, val)
                    for key, val in spec.items()])
        if '$explain' in cmd:
            cmd.pop('$explain')
        if '$readPreference' in cmd:
            cmd.pop('$readPreference')
    else:
        cmd['filter'] = spec

    if projection:
        cmd['projection'] = projection
    if skip:
        cmd['skip'] = skip
    if limit:
        cmd['limit'] = abs(limit)
        if limit < 0:
            cmd['singleBatch'] = True
    if batch_size:
        cmd['batchSize'] = batch_size
    if read_concern.level:
        cmd['readConcern'] = read_concern.document
    if collation:
        cmd['collation'] = collation
    if options:
        cmd.update([(opt, True)
                    for opt, val in _OPTIONS.items()
                    if options & val])
    return cmd


def _gen_get_more_command(cursor_id, coll, batch_size, max_await_time_ms):
    """Generate a getMore command document."""
    cmd = SON([('getMore', cursor_id),
               ('collection', coll)])
    if batch_size:
        cmd['batchSize'] = batch_size
    if max_await_time_ms is not None:
        cmd['maxTimeMS'] = max_await_time_ms
    return cmd


class _Query(object):
    """A query operation."""

    __slots__ = ('flags', 'db', 'coll', 'ntoskip', 'spec',
                 'fields', 'codec_options', 'read_preference', 'limit',
                 'batch_size', 'name', 'read_concern', 'collation',
                 'session', 'client')

    def __init__(self, flags, db, coll, ntoskip, spec, fields,
                 codec_options, read_preference, limit,
                 batch_size, read_concern, collation, session, client):
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
        self.name = 'find'

    def use_command(self, sock_info, exhaust):
        use_find_cmd = False
        if sock_info.max_wire_version >= 4:
            if not exhaust:
                use_find_cmd = True
        elif not self.read_concern.ok_for_legacy:
            raise ConfigurationError(
                'read concern level of %s is not valid '
                'with a max wire version of %d.'
                % (self.read_concern.level,
                   sock_info.max_wire_version))

        if sock_info.max_wire_version < 5 and self.collation is not None:
            raise ConfigurationError(
                'Specifying a collation is unsupported with a max wire '
                'version of %d.' % (sock_info.max_wire_version,))

        sock_info.validate_session(self.client, self.session)

        return use_find_cmd

    def as_command(self, sock_info):
        """Return a find command document for this query.

        Should be called *after* get_message.
        """
        explain = '$explain' in self.spec
        cmd = _gen_find_command(
            self.coll, self.spec, self.fields, self.ntoskip,
            self.limit, self.batch_size, self.flags, self.read_concern,
            self.collation)
        if explain:
            self.name = 'explain'
            cmd = SON([('explain', cmd)])
        session = self.session
        if session:
            cmd['lsid'] = session._use_lsid()
            # Explain does not support readConcern.
            if (not explain and session.options.causal_consistency
                    and session.operation_time is not None):
                cmd.setdefault(
                    'readConcern', {})[
                    'afterClusterTime'] = session.operation_time
        sock_info.send_cluster_time(cmd, session, self.client)
        return cmd, self.db

    def get_message(self, set_slave_ok, sock_info, use_cmd=False):
        """Get a query message, possibly setting the slaveOk bit."""
        if set_slave_ok:
            # Set the slaveOk bit.
            flags = self.flags | 4
        else:
            flags = self.flags

        ns = _UJOIN % (self.db, self.coll)
        spec = self.spec

        if use_cmd:
            ns = _UJOIN % (self.db, "$cmd")
            spec = self.as_command(sock_info)[0]
            ntoreturn = -1  # All DB commands return 1 document
        else:
            # OP_QUERY treats ntoreturn of -1 and 1 the same, return
            # one document and close the cursor. We have to use 2 for
            # batch size if 1 is specified.
            ntoreturn = self.batch_size == 1 and 2 or self.batch_size
            if self.limit:
                if ntoreturn:
                    ntoreturn = min(self.limit, ntoreturn)
                else:
                    ntoreturn = self.limit

        if sock_info.is_mongos:
            spec = _maybe_add_read_preference(spec,
                                              self.read_preference)

        return query(flags, ns, self.ntoskip, ntoreturn,
                     spec, None if use_cmd else self.fields, self.codec_options)


class _GetMore(object):
    """A getmore operation."""

    __slots__ = ('db', 'coll', 'ntoreturn', 'cursor_id', 'max_await_time_ms',
                 'codec_options', 'session', 'client')

    name = 'getMore'

    def __init__(self, db, coll, ntoreturn, cursor_id, codec_options, session,
                 client, max_await_time_ms=None):
        self.db = db
        self.coll = coll
        self.ntoreturn = ntoreturn
        self.cursor_id = cursor_id
        self.codec_options = codec_options
        self.session = session
        self.client = client
        self.max_await_time_ms = max_await_time_ms

    def use_command(self, sock_info, exhaust):
        sock_info.validate_session(self.client, self.session)
        return sock_info.max_wire_version >= 4 and not exhaust

    def as_command(self, sock_info):
        """Return a getMore command document for this query."""
        cmd = _gen_get_more_command(self.cursor_id, self.coll,
                                    self.ntoreturn,
                                    self.max_await_time_ms)

        if self.session:
            cmd['lsid'] = self.session._use_lsid()
        sock_info.send_cluster_time(cmd, self.session, self.client)
        return cmd, self.db

    def get_message(self, dummy0, sock_info, use_cmd=False):
        """Get a getmore message."""

        ns = _UJOIN % (self.db, self.coll)

        if use_cmd:
            ns = _UJOIN % (self.db, "$cmd")
            spec = self.as_command(sock_info)[0]

            return query(0, ns, 0, -1, spec, None, self.codec_options)

        return get_more(ns, self.ntoreturn, self.cursor_id)


class _RawBatchQuery(_Query):
    def use_command(self, socket_info, exhaust):
        # Compatibility checks.
        super(_RawBatchQuery, self).use_command(socket_info, exhaust)

        return False

    def get_message(self, set_slave_ok, sock_info, use_cmd=False):
        # Always pass False for use_cmd.
        return super(_RawBatchQuery, self).get_message(
            set_slave_ok, sock_info, False)


class _RawBatchGetMore(_GetMore):
    def use_command(self, socket_info, exhaust):
        return False

    def get_message(self, set_slave_ok, sock_info, use_cmd=False):
        # Always pass False for use_cmd.
        return super(_RawBatchGetMore, self).get_message(
            set_slave_ok, sock_info, False)


class _CursorAddress(tuple):
    """The server address (host, port) of a cursor, with namespace property."""

    def __new__(cls, address, namespace):
        self = tuple.__new__(cls, address)
        self.__namespace = namespace
        return self

    @property
    def namespace(self):
        """The namespace this cursor."""
        return self.__namespace

    def __hash__(self):
        # Two _CursorAddress instances with different namespaces
        # must not hash the same.
        return (self + (self.__namespace,)).__hash__()

    def __eq__(self, other):
        if isinstance(other, _CursorAddress):
            return (tuple(self) == tuple(other)
                    and self.namespace == other.namespace)
        return NotImplemented

    def __ne__(self, other):
        return not self == other


def __last_error(namespace, args):
    """Data to send to do a lastError.
    """
    cmd = SON([("getlasterror", 1)])
    cmd.update(args)
    splitns = namespace.split('.', 1)
    return query(0, splitns[0] + '.$cmd', 0, -1, cmd,
                 None, DEFAULT_CODEC_OPTIONS)


def __pack_message(operation, data):
    """Takes message data and adds a message header based on the operation.

    Returns the resultant message string.
    """
    request_id = _randint()
    message = struct.pack("<i", 16 + len(data))
    message += struct.pack("<i", request_id)
    message += _ZERO_32  # responseTo
    message += struct.pack("<i", operation)
    return (request_id, message + data)


def insert(collection_name, docs, check_keys,
           safe, last_error_args, continue_on_error, opts):
    """Get an **insert** message."""
    options = 0
    if continue_on_error:
        options += 1
    data = struct.pack("<i", options)
    data += bson._make_c_string(collection_name)
    encoded = [bson.BSON.encode(doc, check_keys, opts) for doc in docs]
    if not encoded:
        raise InvalidOperation("cannot do an empty bulk insert")
    max_bson_size = max(map(len, encoded))
    data += _EMPTY.join(encoded)
    if safe:
        (_, insert_message) = __pack_message(2002, data)
        (request_id, error_message, _) = __last_error(collection_name,
                                                      last_error_args)
        return (request_id, insert_message + error_message, max_bson_size)
    else:
        (request_id, insert_message) = __pack_message(2002, data)
        return (request_id, insert_message, max_bson_size)
if _use_c:
    insert = _cmessage._insert_message


def update(collection_name, upsert, multi,
           spec, doc, safe, last_error_args, check_keys, opts):
    """Get an **update** message.
    """
    options = 0
    if upsert:
        options += 1
    if multi:
        options += 2

    data = _ZERO_32
    data += bson._make_c_string(collection_name)
    data += struct.pack("<i", options)
    data += bson.BSON.encode(spec, False, opts)
    encoded = bson.BSON.encode(doc, check_keys, opts)
    data += encoded
    if safe:
        (_, update_message) = __pack_message(2001, data)
        (request_id, error_message, _) = __last_error(collection_name,
                                                      last_error_args)
        return (request_id, update_message + error_message, len(encoded))
    else:
        (request_id, update_message) = __pack_message(2001, data)
        return (request_id, update_message, len(encoded))
if _use_c:
    update = _cmessage._update_message


def query(options, collection_name, num_to_skip,
          num_to_return, query, field_selector, opts, check_keys=False):
    """Get a **query** message.
    """
    data = struct.pack("<I", options)
    data += bson._make_c_string(collection_name)
    data += struct.pack("<i", num_to_skip)
    data += struct.pack("<i", num_to_return)
    if check_keys:
        # Temporarily remove $clusterTime to avoid an error from the $-prefix.
        cluster_time = query.pop('$clusterTime', None)
        encoded = bson.BSON.encode(query, True, opts)
        if cluster_time is not None:
            extra = bson._name_value_to_bson(
                b"$clusterTime\x00", cluster_time, False, opts)
            encoded = (
                bson._PACK_INT(len(encoded) + len(extra))
                + encoded[4:-1] + extra + b'\x00')
            query['$clusterTime'] = cluster_time
    else:
        encoded = bson.BSON.encode(query, False, opts)
    data += encoded
    max_bson_size = len(encoded)
    if field_selector is not None:
        encoded = bson.BSON.encode(field_selector, False, opts)
        data += encoded
        max_bson_size = max(len(encoded), max_bson_size)
    (request_id, query_message) = __pack_message(2004, data)
    return (request_id, query_message, max_bson_size)
if _use_c:
    query = _cmessage._query_message


def get_more(collection_name, num_to_return, cursor_id):
    """Get a **getMore** message.
    """
    data = _ZERO_32
    data += bson._make_c_string(collection_name)
    data += struct.pack("<i", num_to_return)
    data += struct.pack("<q", cursor_id)
    return __pack_message(2005, data)
if _use_c:
    get_more = _cmessage._get_more_message


def delete(collection_name, spec, safe,
           last_error_args, opts, flags=0):
    """Get a **delete** message.

    `opts` is a CodecOptions. `flags` is a bit vector that may contain
    the SingleRemove flag or not:

    http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-delete
    """
    data = _ZERO_32
    data += bson._make_c_string(collection_name)
    data += struct.pack("<I", flags)
    encoded = bson.BSON.encode(spec, False, opts)
    data += encoded
    if safe:
        (_, remove_message) = __pack_message(2006, data)
        (request_id, error_message, _) = __last_error(collection_name,
                                                      last_error_args)
        return (request_id, remove_message + error_message, len(encoded))
    else:
        (request_id, remove_message) = __pack_message(2006, data)
        return (request_id, remove_message, len(encoded))


def kill_cursors(cursor_ids):
    """Get a **killCursors** message.
    """
    data = _ZERO_32
    data += struct.pack("<i", len(cursor_ids))
    for cursor_id in cursor_ids:
        data += struct.pack("<q", cursor_id)
    return __pack_message(2007, data)


_FIELD_MAP = {
    'insert': 'documents',
    'update': 'updates',
    'delete': 'deletes'
}


class _BulkWriteContext(object):
    """A wrapper around SocketInfo for use with write splitting functions."""

    __slots__ = ('db_name', 'command', 'sock_info', 'op_id',
                 'name', 'field', 'publish', 'start_time', 'listeners',
                 'session')

    def __init__(self, database_name, command, sock_info, operation_id,
                 listeners, session):
        self.db_name = database_name
        self.command = command
        self.sock_info = sock_info
        self.op_id = operation_id
        self.listeners = listeners
        self.publish = listeners.enabled_for_commands
        self.name = next(iter(command))
        self.field = _FIELD_MAP[self.name]
        self.start_time = datetime.datetime.now() if self.publish else None
        self.session = session

    @property
    def max_bson_size(self):
        """A proxy for SockInfo.max_bson_size."""
        return self.sock_info.max_bson_size

    @property
    def max_message_size(self):
        """A proxy for SockInfo.max_message_size."""
        return self.sock_info.max_message_size

    @property
    def max_write_batch_size(self):
        """A proxy for SockInfo.max_write_batch_size."""
        return self.sock_info.max_write_batch_size

    def legacy_write(self, request_id, msg, max_doc_size, acknowledged, docs):
        """A proxy for SocketInfo.legacy_write that handles event publishing.
        """
        if self.publish:
            duration = datetime.datetime.now() - self.start_time
            cmd = self._start(request_id, docs)
            start = datetime.datetime.now()
        try:
            result = self.sock_info.legacy_write(
                request_id, msg, max_doc_size, acknowledged)
            if self.publish:
                duration = (datetime.datetime.now() - start) + duration
                if result is not None:
                    reply = _convert_write_result(self.name, cmd, result)
                else:
                    # Comply with APM spec.
                    reply = {'ok': 1}
                self._succeed(request_id, reply, duration)
        except OperationFailure as exc:
            if self.publish:
                duration = (datetime.datetime.now() - start) + duration
                self._fail(
                    request_id,
                    _convert_write_result(
                        self.name, cmd, exc.details),
                    duration)
            raise
        finally:
            self.start_time = datetime.datetime.now()
        return result

    def write_command(self, request_id, msg, docs):
        """A proxy for SocketInfo.write_command that handles event publishing.
        """
        if self.session:
            # Update last_use time.
            self.session._use_lsid()
        if self.publish:
            duration = datetime.datetime.now() - self.start_time
            self._start(request_id, docs)
            start = datetime.datetime.now()
        try:
            reply = self.sock_info.write_command(request_id, msg)
            if self.publish:
                duration = (datetime.datetime.now() - start) + duration
                self._succeed(request_id, reply, duration)
        except OperationFailure as exc:
            if self.publish:
                duration = (datetime.datetime.now() - start) + duration
                self._fail(request_id, exc.details, duration)
            raise
        finally:
            self.start_time = datetime.datetime.now()
        return reply

    def _start(self, request_id, docs):
        """Publish a CommandStartedEvent."""
        cmd = self.command.copy()
        cmd[self.field] = docs
        self.listeners.publish_command_start(
            cmd, self.db_name,
            request_id, self.sock_info.address, self.op_id)
        return cmd

    def _succeed(self, request_id, reply, duration):
        """Publish a CommandSucceededEvent."""
        self.listeners.publish_command_success(
            duration, reply, self.name,
            request_id, self.sock_info.address, self.op_id)

    def _fail(self, request_id, failure, duration):
        """Publish a CommandFailedEvent."""
        self.listeners.publish_command_failure(
            duration, failure, self.name,
            request_id, self.sock_info.address, self.op_id)


def _raise_document_too_large(operation, doc_size, max_size):
    """Internal helper for raising DocumentTooLarge."""
    if operation == "insert":
        raise DocumentTooLarge("BSON document too large (%d bytes)"
                               " - the connected server supports"
                               " BSON document sizes up to %d"
                               " bytes." % (doc_size, max_size))
    else:
        # There's nothing intelligent we can say
        # about size for update and remove
        raise DocumentTooLarge("command document too large")


def _do_batched_insert(collection_name, docs, check_keys,
                       safe, last_error_args, continue_on_error, opts,
                       ctx):
    """Insert `docs` using multiple batches.
    """
    def _insert_message(insert_message, send_safe):
        """Build the insert message with header and GLE.
        """
        request_id, final_message = __pack_message(2002, insert_message)
        if send_safe:
            request_id, error_message, _ = __last_error(collection_name,
                                                        last_error_args)
            final_message += error_message
        return request_id, final_message

    send_safe = safe or not continue_on_error
    last_error = None
    data = StringIO()
    data.write(struct.pack("<i", int(continue_on_error)))
    data.write(bson._make_c_string(collection_name))
    message_length = begin_loc = data.tell()
    has_docs = False
    to_send = []
    for doc in docs:
        encoded = bson.BSON.encode(doc, check_keys, opts)
        encoded_length = len(encoded)
        too_large = (encoded_length > ctx.max_bson_size)

        message_length += encoded_length
        if message_length < ctx.max_message_size and not too_large:
            data.write(encoded)
            to_send.append(doc)
            has_docs = True
            continue

        if has_docs:
            # We have enough data, send this message.
            try:
                request_id, msg = _insert_message(data.getvalue(), send_safe)
                ctx.legacy_write(request_id, msg, 0, send_safe, to_send)
            # Exception type could be OperationFailure or a subtype
            # (e.g. DuplicateKeyError)
            except OperationFailure as exc:
                # Like it says, continue on error...
                if continue_on_error:
                    # Store exception details to re-raise after the final batch.
                    last_error = exc
                # With unacknowledged writes just return at the first error.
                elif not safe:
                    return
                # With acknowledged writes raise immediately.
                else:
                    raise

        if too_large:
            _raise_document_too_large(
                "insert", encoded_length, ctx.max_bson_size)

        message_length = begin_loc + encoded_length
        data.seek(begin_loc)
        data.truncate()
        data.write(encoded)
        to_send = [doc]

    if not has_docs:
        raise InvalidOperation("cannot do an empty bulk insert")

    request_id, msg = _insert_message(data.getvalue(), safe)
    ctx.legacy_write(request_id, msg, 0, safe, to_send)

    # Re-raise any exception stored due to continue_on_error
    if last_error is not None:
        raise last_error
if _use_c:
    _do_batched_insert = _cmessage._do_batched_insert


def _do_batched_write_command(namespace, operation, command,
                              docs, check_keys, opts, ctx):
    """Create the next batched insert, update, or delete command.
    """
    max_bson_size = ctx.max_bson_size
    max_write_batch_size = ctx.max_write_batch_size
    # Max BSON object size + 16k - 2 bytes for ending NUL bytes.
    # Server guarantees there is enough room: SERVER-10643.
    max_cmd_size = max_bson_size + _COMMAND_OVERHEAD

    buf = StringIO()
    # Save space for message length and request id
    buf.write(_ZERO_64)
    # responseTo, opCode
    buf.write(b"\x00\x00\x00\x00\xd4\x07\x00\x00")
    # No options
    buf.write(_ZERO_32)
    # Namespace as C string
    buf.write(b(namespace))
    buf.write(_ZERO_8)
    # Skip: 0, Limit: -1
    buf.write(_SKIPLIM)

    # Where to write command document length
    command_start = buf.tell()
    buf.write(bson.BSON.encode(command))

    # Start of payload
    buf.seek(-1, 2)
    # Work around some Jython weirdness.
    buf.truncate()
    try:
        buf.write(_OP_MAP[operation])
    except KeyError:
        raise InvalidOperation('Unknown command')

    if operation in (_UPDATE, _DELETE):
        check_keys = False

    # Where to write list document length
    list_start = buf.tell() - 4
    to_send = []
    idx = 0
    for doc in docs:
        # Encode the current operation
        key = b(str(idx))
        value = bson.BSON.encode(doc, check_keys, opts)
        # Is there enough room to add this document? max_cmd_size accounts for
        # the two trailing null bytes.
        enough_data = (buf.tell() + len(key) + len(value)) >= max_cmd_size
        enough_documents = (idx >= max_write_batch_size)
        if enough_data or enough_documents:
            if not idx:
                write_op = "insert" if operation == _INSERT else None
                _raise_document_too_large(
                    write_op, len(value), max_bson_size)
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
    buf.write(struct.pack('<i', length - list_start - 1))
    buf.seek(command_start)
    buf.write(struct.pack('<i', length - command_start))
    buf.seek(4)
    request_id = _randint()
    buf.write(struct.pack('<i', request_id))
    buf.seek(0)
    buf.write(struct.pack('<i', length))

    return request_id, buf.getvalue(), to_send
if _use_c:
    _do_batched_write_command = _cmessage._do_batched_write_command


class _OpReply(object):
    """A MongoDB OP_REPLY response message."""

    __slots__ = ("flags", "cursor_id", "number_returned", "documents")

    UNPACK = struct.Struct("<iqii").unpack
    OP_CODE = 1

    def __init__(self, flags, cursor_id, number_returned, documents):
        self.flags = flags
        self.cursor_id = cursor_id
        self.number_returned = number_returned
        self.documents = documents

    def raw_response(self, cursor_id=None):
        """Check the response header from the database, without decoding BSON.

        Check the response for errors and unpack.

        Can raise CursorNotFound, NotMasterError, ExecutionTimeout, or
        OperationFailure.

        :Parameters:
          - `cursor_id` (optional): cursor_id we sent to get this response -
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
            error_object = bson.BSON(self.documents).decode()
            # Fake the ok field if it doesn't exist.
            error_object.setdefault("ok", 0)
            if error_object["$err"].startswith("not master"):
                raise NotMasterError(error_object["$err"], error_object)
            elif error_object.get("code") == 50:
                raise ExecutionTimeout(error_object.get("$err"),
                                       error_object.get("code"),
                                       error_object)
            raise OperationFailure("database error: %s" %
                                   error_object.get("$err"),
                                   error_object.get("code"),
                                   error_object)
        return [self.documents]

    def unpack_response(self, cursor_id=None,
                        codec_options=_UNICODE_REPLACE_CODEC_OPTIONS):
        """Unpack a response from the database and decode the BSON document(s).

        Check the response for errors and unpack, returning a dictionary
        containing the response data.

        Can raise CursorNotFound, NotMasterError, ExecutionTimeout, or
        OperationFailure.

        :Parameters:
          - `cursor_id` (optional): cursor_id we sent to get this response -
            used for raising an informative exception when we get cursor id not
            valid at server response
          - `codec_options` (optional): an instance of
            :class:`~bson.codec_options.CodecOptions`
        """
        self.raw_response(cursor_id)
        return bson.decode_all(self.documents, codec_options)

    def command_response(self):
        """Unpack a command response."""
        docs = self.unpack_response()
        assert self.number_returned == 1
        return docs[0]

    @classmethod
    def unpack(cls, msg):
        """Construct an _OpReply from raw bytes."""
        # PYTHON-945: ignore starting_from field.
        flags, cursor_id, _, number_returned = cls.UNPACK(msg[:20])
        return cls(flags, cursor_id, number_returned, msg[20:])


def _first_batch(sock_info, db, coll, query, ntoreturn,
                 slave_ok, codec_options, read_preference, cmd, listeners):
    """Simple query helper for retrieving a first (and possibly only) batch."""
    query = _Query(
        0, db, coll, 0, query, None, codec_options,
        read_preference, ntoreturn, 0, DEFAULT_READ_CONCERN, None, None,
        None)

    name = next(iter(cmd))
    publish = listeners.enabled_for_commands
    if publish:
        start = datetime.datetime.now()

    request_id, msg, max_doc_size = query.get_message(slave_ok, sock_info)

    if publish:
        encoding_duration = datetime.datetime.now() - start
        listeners.publish_command_start(
            cmd, db, request_id, sock_info.address)
        start = datetime.datetime.now()

    sock_info.send_message(msg, max_doc_size)
    reply = sock_info.receive_message(request_id)
    try:
        docs = reply.unpack_response(None, codec_options)
    except Exception as exc:
        if publish:
            duration = (datetime.datetime.now() - start) + encoding_duration
            if isinstance(exc, (NotMasterError, OperationFailure)):
                failure = exc.details
            else:
                failure = _convert_exception(exc)
            listeners.publish_command_failure(
                duration, failure, name, request_id, sock_info.address)
        raise
    # listIndexes
    if 'cursor' in cmd:
        result = {
            u'cursor': {
                u'firstBatch': docs,
                u'id': reply.cursor_id,
                u'ns': u'%s.%s' % (db, coll)
            },
            u'ok': 1.0
        }
    # fsyncUnlock, currentOp
    else:
        result = docs[0] if docs else {}
        result[u'ok'] = 1.0
    if publish:
        duration = (datetime.datetime.now() - start) + encoding_duration
        listeners.publish_command_success(
            duration, result, name, request_id, sock_info.address)

    return result
