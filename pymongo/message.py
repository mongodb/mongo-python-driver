# Copyright 2009 10gen, Inc.
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

"""Tools for creating messages to be sent to MongoDB.

Generally not needed to be used by application developers."""

import threading
import struct
import random
import sys

import bson

try:
    import _cbson
    _use_c = True
except ImportError:
    _use_c = False


__ZERO = "\x00\x00\x00\x00"


def __last_error():
    """Data to send to do a lastError.
    """
    return query(0, "admin.$cmd", 0, -1, {"getlasterror": 1})


def __pack_message(operation, data):
    """Takes message data and adds a message header based on the operation.

    Returns the resultant message string.
    """
    request_id = random.randint(-2**31 - 1, 2**31)
    message = struct.pack("<i", 16 + len(data))
    message += struct.pack("<i", request_id)
    message += __ZERO # responseTo
    message += struct.pack("<i", operation)
    return (request_id, message + data)


def insert(collection_name, docs, check_keys, safe):
    data = __ZERO
    data += bson._make_c_string(collection_name)
    data += "".join([bson.BSON.from_dict(doc, check_keys) for doc in docs])
    if safe:
        (_, insert_message) = __pack_message(2002, data)
        (request_id, error_message) = __last_error()
        return (request_id, insert_message + error_message)
    else:
        return __pack_message(2002, data)
if _use_c:
    insert = _cbson._insert_message


def update(collection_name, upsert, multi, spec, doc, safe):
    options = 0
    if upsert:
        options += 1
    if multi:
        options += 2

    data = __ZERO
    data += bson._make_c_string(collection_name)
    data += struct.pack("<i", options)
    data += bson.BSON.from_dict(spec)
    data += bson.BSON.from_dict(doc)
    if safe:
        (_, update_message) = __pack_message(2001, data)
        (request_id, error_message) = __last_error()
        return (request_id, update_message + error_message)
    else:
        return __pack_message(2001, data)


def query(options, collection_name,
          num_to_skip, num_to_return, query, field_selector=None):
    data = struct.pack("<I", options)
    data += bson._make_c_string(collection_name)
    data += struct.pack("<i", num_to_skip)
    data += struct.pack("<i", num_to_return)
    data += bson.BSON.from_dict(query)
    if field_selector is not None:
        data += bson.BSON.from_dict(field_selector)
    return __pack_message(2004, data)


def get_more(collection_name, num_to_return, cursor_id):
    data = __ZERO
    data += bson._make_c_string(collection_name)
    data += struct.pack("<i", num_to_return)
    data += struct.pack("<q", cursor_id)
    return __pack_message(2005, data)


def delete(collection_name, spec, safe):
    data = __ZERO
    data += bson._make_c_string(collection_name)
    data += __ZERO
    data += bson.BSON.from_dict(spec)
    if safe:
        (_, remove_message) = __pack_message(2006, data)
        (request_id, error_message) = __last_error()
        return (request_id, remove_message + error_message)
    else:
        return __pack_message(2006, data)


def kill_cursors(cursor_ids):
    data = __ZERO
    data += struct.pack("<i", len(cursor_ids))
    for cursor_id in cursor_ids:
        data += struct.pack("<q", cursor_id)
    return __pack_message(2007, data)

