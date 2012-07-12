# Copyright 2009-2012 10gen, Inc.
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
from bson.py3compat import b
from bson.son import SON
try:
    from pymongo import _cmessage
    _use_c = True
except ImportError:
    _use_c = False
from pymongo.errors import InvalidOperation


__ZERO = b("\x00\x00\x00\x00")

EMPTY  = b("")

MAX_INT32 = 2147483647
MIN_INT32 = -2147483648


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
    message += __ZERO  # responseTo
    message += struct.pack("<i", operation)
    return (request_id, message + data)


def insert(collection_name, docs, check_keys,
           safe, last_error_args, continue_on_error, uuid_subtype):
    """Get an **insert** message.
    """
    max_bson_size = 0
    options = 0
    if continue_on_error:
        options += 1
    data = struct.pack("<i", options)
    data += bson._make_c_string(collection_name)
    encoded = [bson.BSON.encode(doc, check_keys, uuid_subtype) for doc in docs]
    if not encoded:
        raise InvalidOperation("cannot do an empty bulk insert")
    max_bson_size = max(map(len, encoded))
    data += EMPTY.join(encoded)
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

    data = __ZERO
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
    data = __ZERO
    data += bson._make_c_string(collection_name)
    data += struct.pack("<i", num_to_return)
    data += struct.pack("<q", cursor_id)
    return __pack_message(2005, data)
if _use_c:
    get_more = _cmessage._get_more_message


def delete(collection_name, spec, safe, last_error_args, uuid_subtype):
    """Get a **delete** message.
    """
    data = __ZERO
    data += bson._make_c_string(collection_name)
    data += __ZERO
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
    data = __ZERO
    data += struct.pack("<i", len(cursor_ids))
    for cursor_id in cursor_ids:
        data += struct.pack("<q", cursor_id)
    return __pack_message(2007, data)
