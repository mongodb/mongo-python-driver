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

"""Bits and pieces used by the driver that don't really fit elsewhere."""

import random
import struct

import bson
import pymongo

from bson.binary import OLD_UUID_SUBTYPE
from bson.son import SON
from pymongo.errors import (AutoReconnect,
                            DuplicateKeyError,
                            OperationFailure,
                            TimeoutError)


def _index_list(key_or_list, direction=None):
    """Helper to generate a list of (key, direction) pairs.

    Takes such a list, or a single key, or a single key and direction.
    """
    if direction is not None:
        return [(key_or_list, direction)]
    else:
        if isinstance(key_or_list, basestring):
            return [(key_or_list, pymongo.ASCENDING)]
        elif not isinstance(key_or_list, list):
            raise TypeError("if no direction is specified, "
                            "key_or_list must be an instance of list")
        return key_or_list


def _index_document(index_list):
    """Helper to generate an index specifying document.

    Takes a list of (key, direction) pairs.
    """
    if isinstance(index_list, dict):
        raise TypeError("passing a dict to sort/create_index/hint is not "
                        "allowed - use a list of tuples instead. did you "
                        "mean %r?" % list(index_list.iteritems()))
    elif not isinstance(index_list, list):
        raise TypeError("must use a list of (key, direction) pairs, "
                        "not: " + repr(index_list))
    if not len(index_list):
        raise ValueError("key_or_list must not be the empty list")

    index = SON()
    for (key, value) in index_list:
        if not isinstance(key, basestring):
            raise TypeError("first item in each key pair must be a string")
        if not isinstance(value, (basestring, int)):
            raise TypeError("second item in each key pair must be 1, -1, "
                            "'2d', 'geoHaystack', or another valid MongoDB "
                            "index specifier.")
        index[key] = value
    return index


def _unpack_response(response, cursor_id=None, as_class=dict,
                     tz_aware=False, uuid_subtype=OLD_UUID_SUBTYPE):
    """Unpack a response from the database.

    Check the response for errors and unpack, returning a dictionary
    containing the response data.

    :Parameters:
      - `response`: byte string as returned from the database
      - `cursor_id` (optional): cursor_id we sent to get this response -
        used for raising an informative exception when we get cursor id not
        valid at server response
      - `as_class` (optional): class to use for resulting documents
    """
    response_flag = struct.unpack("<i", response[:4])[0]
    if response_flag & 1:
        # Shouldn't get this response if we aren't doing a getMore
        assert cursor_id is not None

        raise OperationFailure("cursor id '%s' not valid at server" %
                               cursor_id)
    elif response_flag & 2:
        error_object = bson.BSON(response[20:]).decode()
        if error_object["$err"].startswith("not master"):
            raise AutoReconnect(error_object["$err"])
        raise OperationFailure("database error: %s" %
                               error_object["$err"])

    result = {}
    result["cursor_id"] = struct.unpack("<q", response[4:12])[0]
    result["starting_from"] = struct.unpack("<i", response[12:16])[0]
    result["number_returned"] = struct.unpack("<i", response[16:20])[0]
    result["data"] = bson.decode_all(response[20:],
                                     as_class, tz_aware, uuid_subtype)
    assert len(result["data"]) == result["number_returned"]
    return result


def _check_command_response(response, reset, msg="%s", allowable_errors=[]):
    """Check the response to a command for errors.
    """
    if not response["ok"]:
        if "wtimeout" in response and response["wtimeout"]:
            raise TimeoutError(msg % response["errmsg"])

        details = response
        # Mongos returns the error details in a 'raw' object
        # for some errors.
        if "raw" in response:
            for shard in response["raw"].itervalues():
                if not shard.get("ok"):
                    # Just grab the first error...
                    details = shard
                    break

        errmsg = details["errmsg"]
        if not errmsg in allowable_errors:
            if (errmsg.startswith("not master")
                or errmsg.startswith("node is recovering")):
                if reset is not None:
                    reset()
                raise AutoReconnect(errmsg)
            if errmsg == "db assertion failure":
                ex_msg = ("db assertion failure, assertion: '%s'" %
                          details.get("assertion", ""))
                if "assertionCode" in details:
                    ex_msg += (", assertionCode: %d" %
                               (details["assertionCode"],))
                raise OperationFailure(ex_msg, details.get("assertionCode"))
            code = details.get("code")
            # findAndModify with upsert can raise duplicate key error
            if code in (11000, 11001, 12582):
                raise DuplicateKeyError(errmsg, code)
            raise OperationFailure(msg % errmsg, code)


def _fields_list_to_dict(fields):
    """Takes a list of field names and returns a matching dictionary.

    ["a", "b"] becomes {"a": 1, "b": 1}

    and

    ["a.b.c", "d", "a.c"] becomes {"a.b.c": 1, "d": 1, "a.c": 1}
    """
    as_dict = {}
    for field in fields:
        if not isinstance(field, basestring):
            raise TypeError("fields must be a list of key names, "
                            "each an instance of %s" % (basestring.__name__,))
        as_dict[field] = 1
    return as_dict


def shuffled(sequence):
    """Returns a copy of the sequence (as a :class:`list`) which has been
    shuffled by :func:`random.shuffle`.
    """
    out = list(sequence)
    random.shuffle(out)
    return out
