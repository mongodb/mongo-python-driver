# Copyright 2009-2015 MongoDB, Inc.
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

import collections
import datetime
import struct

import bson
from bson.codec_options import CodecOptions
from bson.py3compat import itervalues, string_type, iteritems, u
from bson.son import SON
from pymongo import ASCENDING
from pymongo.errors import (CursorNotFound,
                            DuplicateKeyError,
                            ExecutionTimeout,
                            NotMasterError,
                            OperationFailure,
                            WriteError,
                            WriteConcernError,
                            WTimeoutError)
from pymongo.message import _Query, _convert_exception


_UUNDER = u("_")


def _gen_index_name(keys):
    """Generate an index name from the set of fields it is over."""
    return _UUNDER.join(["%s_%s" % item for item in keys])


def _index_list(key_or_list, direction=None):
    """Helper to generate a list of (key, direction) pairs.

    Takes such a list, or a single key, or a single key and direction.
    """
    if direction is not None:
        return [(key_or_list, direction)]
    else:
        if isinstance(key_or_list, string_type):
            return [(key_or_list, ASCENDING)]
        elif not isinstance(key_or_list, (list, tuple)):
            raise TypeError("if no direction is specified, "
                            "key_or_list must be an instance of list")
        return key_or_list


def _index_document(index_list):
    """Helper to generate an index specifying document.

    Takes a list of (key, direction) pairs.
    """
    if isinstance(index_list, collections.Mapping):
        raise TypeError("passing a dict to sort/create_index/hint is not "
                        "allowed - use a list of tuples instead. did you "
                        "mean %r?" % list(iteritems(index_list)))
    elif not isinstance(index_list, (list, tuple)):
        raise TypeError("must use a list of (key, direction) pairs, "
                        "not: " + repr(index_list))
    if not len(index_list):
        raise ValueError("key_or_list must not be the empty list")

    index = SON()
    for (key, value) in index_list:
        if not isinstance(key, string_type):
            raise TypeError("first item in each key pair must be a string")
        if not isinstance(value, (string_type, int, collections.Mapping)):
            raise TypeError("second item in each key pair must be 1, -1, "
                            "'2d', 'geoHaystack', or another valid MongoDB "
                            "index specifier.")
        index[key] = value
    return index


def _unpack_response(response, cursor_id=None, codec_options=CodecOptions()):
    """Unpack a response from the database.

    Check the response for errors and unpack, returning a dictionary
    containing the response data.

    Can raise CursorNotFound, NotMasterError, ExecutionTimeout, or
    OperationFailure.

    :Parameters:
      - `response`: byte string as returned from the database
      - `cursor_id` (optional): cursor_id we sent to get this response -
        used for raising an informative exception when we get cursor id not
        valid at server response
      - `codec_options` (optional): an instance of
        :class:`~bson.codec_options.CodecOptions`
    """
    response_flag = struct.unpack("<i", response[:4])[0]
    if response_flag & 1:
        # Shouldn't get this response if we aren't doing a getMore
        assert cursor_id is not None

        # Fake a getMore command response. OP_GET_MORE provides no document.
        msg = "Cursor not found, cursor id: %d" % (cursor_id,)
        errobj = {"ok": 0, "errmsg": msg, "code": 43}
        raise CursorNotFound(msg, 43, errobj)
    elif response_flag & 2:
        error_object = bson.BSON(response[20:]).decode()
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

    result = {}
    result["cursor_id"] = struct.unpack("<q", response[4:12])[0]
    result["starting_from"] = struct.unpack("<i", response[12:16])[0]
    result["number_returned"] = struct.unpack("<i", response[16:20])[0]
    result["data"] = bson.decode_all(response[20:], codec_options)
    assert len(result["data"]) == result["number_returned"]
    return result


def _check_command_response(response, msg=None, allowable_errors=None):
    """Check the response to a command for errors.
    """
    if "ok" not in response:
        # Server didn't recognize our message as a command.
        raise OperationFailure(response.get("$err"),
                               response.get("code"),
                               response)

    # TODO: remove, this is moving to _check_gle_response
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
            for shard in itervalues(response["raw"]):
                # Grab the first non-empty raw error from a shard.
                if shard.get("errmsg") and not shard.get("ok"):
                    details = shard
                    break

        errmsg = details["errmsg"]
        if allowable_errors is None or errmsg not in allowable_errors:

            # Server is "not master" or "recovering"
            if (errmsg.startswith("not master")
                    or errmsg.startswith("node is recovering")):
                raise NotMasterError(errmsg, response)

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


def _check_gle_response(response):
    """Return getlasterror response as a dict, or raise OperationFailure."""
    response = _unpack_response(response)

    assert response["number_returned"] == 1
    result = response["data"][0]

    # Did getlasterror itself fail?
    _check_command_response(result)

    if result.get("wtimeout", False):
        # MongoDB versions before 1.8.0 return the error message in an "errmsg"
        # field. If "errmsg" exists "err" will also exist set to None, so we
        # have to check for "errmsg" first.
        raise WTimeoutError(result.get("errmsg", result.get("err")),
                            result.get("code"),
                            result)

    error_msg = result.get("err", "")
    if error_msg is None:
        return result

    if error_msg.startswith("not master"):
        raise NotMasterError(error_msg, result)

    details = result

    # mongos returns the error code in an error object for some errors.
    if "errObjects" in result:
        for errobj in result["errObjects"]:
            if errobj.get("err") == error_msg:
                details = errobj
                break

    code = details.get("code")
    if code in (11000, 11001, 12582):
        raise DuplicateKeyError(details["err"], code, result)
    raise OperationFailure(details["err"], code, result)


def _first_batch(sock_info, namespace, query, ntoreturn,
                 slave_ok, codec_options, read_preference, cmd, listeners):
    """Simple query helper for retrieving a first (and possibly only) batch."""
    query = _Query(
        0, namespace, 0, ntoreturn, query, None,
        codec_options, read_preference, 0, ntoreturn)

    name = next(iter(cmd))
    duration = None
    publish = listeners.enabled_for_commands
    if publish:
        start = datetime.datetime.now()

    request_id, msg, max_doc_size = query.get_message(slave_ok,
                                                      sock_info.is_mongos)

    if publish:
        encoding_duration = datetime.datetime.now() - start
        listeners.publish_command_start(
            cmd, namespace.split('.', 1)[0], request_id, sock_info.address)
        start = datetime.datetime.now()

    sock_info.send_message(msg, max_doc_size)
    response = sock_info.receive_message(1, request_id)
    try:
        result = _unpack_response(response, None, codec_options)
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
    if publish:
        duration = (datetime.datetime.now() - start) + encoding_duration
        listeners.publish_command_success(
            duration, result, name, request_id, sock_info.address)

    return result


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
            raise WriteError(error.get("errmsg"), error.get("code"), error)
        else:
            error = result["writeConcernError"]
            if "errInfo" in error and error["errInfo"].get('wtimeout'):
                # Make sure we raise WTimeoutError
                raise WTimeoutError(
                    error.get("errmsg"), error.get("code"), error)
            raise WriteConcernError(
                error.get("errmsg"), error.get("code"), error)


def _fields_list_to_dict(fields, option_name):
    """Takes a sequence of field names and returns a matching dictionary.

    ["a", "b"] becomes {"a": 1, "b": 1}

    and

    ["a.b.c", "d", "a.c"] becomes {"a.b.c": 1, "d": 1, "a.c": 1}
    """
    if isinstance(fields, collections.Mapping):
        return fields

    if isinstance(fields, collections.Sequence):
        if not all(isinstance(field, string_type) for field in fields):
            raise TypeError("%s must be a list of key names, each an "
                            "instance of %s" % (option_name,
                                                string_type.__name__))
        return dict.fromkeys(fields, 1)

    raise TypeError("%s must be a mapping or "
                    "list of key names" % (option_name,))
