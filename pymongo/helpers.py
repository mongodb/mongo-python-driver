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

"""Bits and pieces used by the driver that don't really fit elsewhere."""

import random
import struct

import bson
import pymongo

from bson.binary import OLD_UUID_SUBTYPE
from bson.son import SON
from pymongo.errors import (AutoReconnect,
                            CursorNotFound,
                            DuplicateKeyError,
                            OperationFailure,
                            ExecutionTimeout,
                            WTimeoutError)


def _index_list(key_or_list, direction=None):
    """Helper to generate a list of (key, direction) pairs.

    Takes such a list, or a single key, or a single key and direction.
    """
    if direction is not None:
        return [(key_or_list, direction)]
    else:
        if isinstance(key_or_list, basestring):
            return [(key_or_list, pymongo.ASCENDING)]
        elif not isinstance(key_or_list, (list, tuple)):
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
    elif not isinstance(index_list, (list, tuple)):
        raise TypeError("must use a list of (key, direction) pairs, "
                        "not: " + repr(index_list))
    if not len(index_list):
        raise ValueError("key_or_list must not be the empty list")

    index = SON()
    for (key, value) in index_list:
        if not isinstance(key, basestring):
            raise TypeError("first item in each key pair must be a string")
        if not isinstance(value, (basestring, int, dict)):
            raise TypeError("second item in each key pair must be 1, -1, "
                            "'2d', 'geoHaystack', or another valid MongoDB "
                            "index specifier.")
        index[key] = value
    return index


def _unpack_response(response, cursor_id=None, as_class=dict,
                     tz_aware=False, uuid_subtype=OLD_UUID_SUBTYPE,
                     compile_re=True):
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

        raise CursorNotFound("cursor id '%s' not valid at server" %
                             cursor_id)
    elif response_flag & 2:
        error_object = bson.BSON(response[20:]).decode()
        if error_object["$err"].startswith("not master"):
            raise AutoReconnect(error_object["$err"])
        elif error_object.get("code") == 50:
            raise ExecutionTimeout(error_object.get("$err"),
                                   error_object.get("code"),
                                   error_object)
        raise OperationFailure("database error: %s" %
                               error_object.get("$err"),
                               error_object.get("code"),
                               error_object)

    result = {}
    result["cursor_id"] = struct.unpack("<q", response[4:12])[0]
    result["starting_from"] = struct.unpack("<i", response[12:16])[0]
    result["number_returned"] = struct.unpack("<i", response[16:20])[0]
    result["data"] = bson.decode_all(response[20:],
                                     as_class, tz_aware, uuid_subtype,
                                     compile_re)
    assert len(result["data"]) == result["number_returned"]
    return result


def _check_command_response(response, reset, msg=None, allowable_errors=None):
    """Check the response to a command for errors.
    """
    if "ok" not in response:
        # Server didn't recognize our message as a command.
        raise OperationFailure(response.get("$err"),
                               response.get("code"),
                               response)

    if response.get("wtimeout", False):
        # MongoDB versions before 1.8.0 return the error message in an "errmsg"
        # field. If "errmsg" exists "err" will also exist set to None, so we
        # have to check for "errmsg" first.
        raise WTimeoutError(response.get("errmsg", response.get("err")),
                            response.get("code"),
                            response)

    if not response["ok"]:

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
        if allowable_errors is None or errmsg not in allowable_errors:

            # Server is "not master" or "recovering"
            if (errmsg.startswith("not master")
                or errmsg.startswith("node is recovering")):
                if reset is not None:
                    reset()
                raise AutoReconnect(errmsg)

            # Server assertion failures
            if errmsg == "db assertion failure":
                errmsg = ("db assertion failure, assertion: '%s'" %
                          details.get("assertion", ""))
                raise OperationFailure(errmsg,
                                       details.get("assertionCode"),
                                       response)

            # Other errors
            code = details.get("code")
            # findAndModify with upsert can raise duplicate key error
            if code in (11000, 11001, 12582):
                raise DuplicateKeyError(errmsg, code, response)
            elif code == 50:
                raise ExecutionTimeout(errmsg, code, response)

            msg = msg or "%s"
            raise OperationFailure(msg % errmsg, code, response)


def _check_write_command_response(results):
    """Backward compatibility helper for write command error handling.
    """
    errors = [res for res in results
              if "writeErrors" in res[1] or "writeConcernError" in res[1]]
    if errors:
        # If multiple batches had errors
        # raise from the last batch.
        offset, result = errors[-1]
        # Prefer write errors over write concern errors
        write_errors = result.get("writeErrors")
        if write_errors:
            # If the last batch had multiple errors only report
            # the last error to emulate continue_on_error.
            error = write_errors[-1]
            error["index"] += offset
            if error.get("code") == 11000:
                raise DuplicateKeyError(error.get("errmsg"), 11000, error)
        else:
            error = result["writeConcernError"]
            if "errInfo" in error and error["errInfo"].get('wtimeout'):
                # Make sure we raise WTimeoutError
                raise WTimeoutError(error.get("errmsg"),
                                    error.get("code"), error)
        raise OperationFailure(error.get("errmsg"), error.get("code"), error)


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
