# Copyright 2009-2014 MongoDB, Inc.
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

.. versionadded:: 1.1.2
"""

import random
import struct

import bson
from bson.binary import OLD_UUID_SUBTYPE
from bson.py3compat import b, StringIO
from bson.son import SON
try:
    from pymongo import _cmessage
    _use_c = True
except ImportError:
    _use_c = False
from pymongo.errors import DocumentTooLarge, InvalidOperation, OperationFailure


MAX_INT32 = 2147483647
MIN_INT32 = -2147483648

_INSERT = 0
_UPDATE = 1
_DELETE = 2

_EMPTY   = b('')
_BSONOBJ = b('\x03')
_ZERO_8  = b('\x00')
_ZERO_16 = b('\x00\x00')
_ZERO_32 = b('\x00\x00\x00\x00')
_ZERO_64 = b('\x00\x00\x00\x00\x00\x00\x00\x00')
_SKIPLIM = b('\x00\x00\x00\x00\xff\xff\xff\xff')
_OP_MAP = {
    _INSERT: b('\x04documents\x00\x00\x00\x00\x00'),
    _UPDATE: b('\x04updates\x00\x00\x00\x00\x00'),
    _DELETE: b('\x04deletes\x00\x00\x00\x00\x00'),
}


def __last_error(namespace, args):
    """Data to send to do a lastError.
    """
    cmd = SON([("getlasterror", 1)])
    cmd.update(args)
    splitns = namespace.split('.', 1)
    return query(0, splitns[0] + '.$cmd', 0, -1, cmd)


def __pack_message(operation, data):
    """Takes message data and adds a message header based on the operation.

    Returns the resultant message string.
    """
    request_id = random.randint(MIN_INT32, MAX_INT32)
    message = struct.pack("<i", 16 + len(data))
    message += struct.pack("<i", request_id)
    message += _ZERO_32  # responseTo
    message += struct.pack("<i", operation)
    return (request_id, message + data)


def insert(collection_name, docs, check_keys,
           safe, last_error_args, continue_on_error, uuid_subtype):
    """Get an **insert** message.

    .. note:: As of PyMongo 2.6, this function is no longer used. It
       is being kept (with tests) for backwards compatibility with 3rd
       party libraries that may currently be using it, but will likely
       be removed in a future release.

    """
    options = 0
    if continue_on_error:
        options += 1
    data = struct.pack("<i", options)
    data += bson._make_c_string(collection_name)
    encoded = [bson.BSON.encode(doc, check_keys, uuid_subtype) for doc in docs]
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
           spec, doc, safe, last_error_args, check_keys, uuid_subtype):
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
    data += bson.BSON.encode(spec, False, uuid_subtype)
    encoded = bson.BSON.encode(doc, check_keys, uuid_subtype)
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
          num_to_return, query, field_selector=None,
          uuid_subtype=OLD_UUID_SUBTYPE):
    """Get a **query** message.
    """
    data = struct.pack("<I", options)
    data += bson._make_c_string(collection_name)
    data += struct.pack("<i", num_to_skip)
    data += struct.pack("<i", num_to_return)
    encoded = bson.BSON.encode(query, False, uuid_subtype)
    data += encoded
    max_bson_size = len(encoded)
    if field_selector is not None:
        encoded = bson.BSON.encode(field_selector, False, uuid_subtype)
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
           last_error_args, uuid_subtype, options=0):
    """Get a **delete** message.
    """
    data = _ZERO_32
    data += bson._make_c_string(collection_name)
    data += struct.pack("<I", options)
    encoded = bson.BSON.encode(spec, False, uuid_subtype)
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


def _do_batched_insert(collection_name, docs, check_keys,
           safe, last_error_args, continue_on_error, uuid_subtype, client):
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
    for doc in docs:
        encoded = bson.BSON.encode(doc, check_keys, uuid_subtype)
        encoded_length = len(encoded)
        too_large = (encoded_length > client.max_bson_size)

        message_length += encoded_length
        if message_length < client.max_message_size and not too_large:
            data.write(encoded)
            has_docs = True
            continue

        if has_docs:
            # We have enough data, send this message.
            try:
                client._send_message(_insert_message(data.getvalue(),
                                                     send_safe), send_safe)
            # Exception type could be OperationFailure or a subtype
            # (e.g. DuplicateKeyError)
            except OperationFailure, exc:
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
            raise DocumentTooLarge("BSON document too large (%d bytes)"
                                   " - the connected server supports"
                                   " BSON document sizes up to %d"
                                   " bytes." %
                                   (encoded_length, client.max_bson_size))

        message_length = begin_loc + encoded_length
        data.seek(begin_loc)
        data.truncate()
        data.write(encoded)

    if not has_docs:
        raise InvalidOperation("cannot do an empty bulk insert")

    client._send_message(_insert_message(data.getvalue(), safe), safe)

    # Re-raise any exception stored due to continue_on_error
    if last_error is not None:
        raise last_error
if _use_c:
    _do_batched_insert = _cmessage._do_batched_insert


def _do_batched_write_command(namespace, operation, command,
                              docs, check_keys, uuid_subtype, client):
    """Execute a batch of insert, update, or delete commands.
    """
    max_bson_size = client.max_bson_size
    max_write_batch_size = client.max_write_batch_size
    # Max BSON object size + 16k - 2 bytes for ending NUL bytes
    # XXX: This should come from the server - SERVER-10643
    max_cmd_size = max_bson_size + 16382

    ordered = command.get('ordered', True)

    buf = StringIO()
    # Save space for message length and request id
    buf.write(_ZERO_64)
    # responseTo, opCode
    buf.write(b("\x00\x00\x00\x00\xd4\x07\x00\x00"))
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

    def send_message():
        """Finalize and send the current OP_QUERY message.
        """
        # Close list and command documents
        buf.write(_ZERO_16)

        # Write document lengths and request id
        length = buf.tell()
        buf.seek(list_start)
        buf.write(struct.pack('<i', length - list_start - 1))
        buf.seek(command_start)
        buf.write(struct.pack('<i', length - command_start))
        buf.seek(4)
        request_id = random.randint(MIN_INT32, MAX_INT32)
        buf.write(struct.pack('<i', request_id))
        buf.seek(0)
        buf.write(struct.pack('<i', length))

        return client._send_message((request_id, buf.getvalue()),
                                    with_last_error=True,
                                    command=True)

    # If there are multiple batches we'll
    # merge results in the caller.
    results = []

    idx = 0
    idx_offset = 0
    has_docs = False
    for doc in docs:
        has_docs = True
        # Encode the current operation
        key = b(str(idx))
        value = bson.BSON.encode(doc, check_keys, uuid_subtype)
        # Send a batch?
        enough_data = (buf.tell() + len(key) + len(value) + 2) >= max_cmd_size
        enough_documents = (idx >= max_write_batch_size)
        if enough_data or enough_documents:
            if not idx:
                if operation == _INSERT:
                    raise DocumentTooLarge("BSON document too large (%d bytes)"
                                           " - the connected server supports"
                                           " BSON document sizes up to %d"
                                           " bytes." % (len(value),
                                                        max_bson_size))
                # There's nothing intelligent we can say
                # about size for update and remove
                raise DocumentTooLarge("command document too large")
            result = send_message()
            results.append((idx_offset, result))
            if ordered and "writeErrors" in result:
                return results

            # Truncate back to the start of list elements
            buf.seek(list_start + 4)
            buf.truncate()
            idx_offset += idx
            idx = 0
            key = b('0')
        buf.write(_BSONOBJ)
        buf.write(key)
        buf.write(_ZERO_8)
        buf.write(value)
        idx += 1

    if not has_docs:
        raise InvalidOperation("cannot do an empty bulk write")

    results.append((idx_offset, send_message()))
    return results
if _use_c:
    _do_batched_write_command = _cmessage._do_batched_write_command
