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

import random
import struct
import warnings

import bson
import pymongo

from bson.binary import OLD_UUID_SUBTYPE
from bson.son import SON
from pymongo import auth
from pymongo.errors import (AutoReconnect,
                            CursorNotFound,
                            DuplicateKeyError,
                            InvalidName,
                            InvalidOperation,
                            OperationFailure,
                            ExecutionTimeout,
                            WTimeoutError)
from pymongo.read_preferences import _ServerMode
from pymongo.write_concern import WriteConcern as _WriteConcern


def _get_common_options(obj, codec_options, read_preference, write_concern):
    """Get the codec options, read preference mode and tags, and write concern
    necessary to create a new Database of Collection instance.
    """
    if codec_options is None:
        codec_options = obj.codec_options

    if read_preference is None:
        rp_mode = obj.read_preference
        rp_tags = obj.tag_sets
    else:
        if isinstance(read_preference, _ServerMode):
            rp_mode = read_preference.mode
            rp_tags = read_preference.tag_sets
        else:
            rp_mode = read_preference
            rp_tags = [{}]

    if write_concern is None:
        wc_document = obj.write_concern
    else:
        if not isinstance(write_concern, _WriteConcern):
            raise TypeError("write_concern must be an instance of "
                            "pymongo.write_concern.WriteConcern")
        wc_document = write_concern.document

    return codec_options, rp_mode, rp_tags, wc_document


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
                # Grab the first non-empty raw error from a shard.
                if shard.get("errmsg") and not shard.get("ok"):
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


def _check_database_name(name):
    """Check if a database name is valid."""
    if not name:
        raise InvalidName("database name cannot be the empty string")

    for invalid_char in [' ', '.', '$', '/', '\\', '\x00', '"']:
        if invalid_char in name:
            raise InvalidName("database names cannot contain the "
                              "character %r" % invalid_char)


def _copy_database(
        fromdb,
        todb,
        fromhost,
        mechanism,
        username,
        password,
        sock_info,
        cmd_func):
    """Copy a database, perhaps from a remote host.

    :Parameters:
      - `fromdb`: Source database.
      - `todb`: Target database.
      - `fromhost`: Source host like 'foo.com', 'foo.com:27017', or None.
      - `mechanism`: An authentication mechanism.
      - `username`: A str or unicode, or None.
      - `password`: A str or unicode, or None.
      - `sock_info`: A SocketInfo instance.
      - `cmd_func`: A callback taking args sock_info, database, command doc.
    """
    if not isinstance(fromdb, basestring):
        raise TypeError('from_name must be an instance '
                        'of %s' % (basestring.__name__,))
    if not isinstance(todb, basestring):
        raise TypeError('to_name must be an instance '
                        'of %s' % (basestring.__name__,))

    _check_database_name(todb)

    warnings.warn("copy_database is deprecated. Use the raw 'copydb' command"
                  " or db.copyDatabase() in the mongo shell. See"
                  " doc/examples/copydb.",
                  DeprecationWarning, stacklevel=2)

    # It would be better if the user told us what mechanism to use, but for
    # backwards compatibility with earlier PyMongos we don't require the
    # mechanism. Hope 'fromhost' runs the same version as the target.
    if mechanism == 'DEFAULT':
        if sock_info.max_wire_version >= 3:
            mechanism = 'SCRAM-SHA-1'
        else:
            mechanism = 'MONGODB-CR'

    if username is not None:
        if mechanism == 'SCRAM-SHA-1':
            credentials = auth._build_credentials_tuple(mech=mechanism,
                                                        source='admin',
                                                        user=username,
                                                        passwd=password,
                                                        extra=None)

            try:
                auth._copydb_scram_sha1(credentials=credentials,
                                        sock_info=sock_info,
                                        cmd_func=cmd_func,
                                        fromdb=fromdb,
                                        todb=todb,
                                        fromhost=fromhost)
            except OperationFailure, exc:
                errmsg = exc.details and exc.details.get('errmsg') or ''
                if 'no such cmd: saslStart' in errmsg:
                    explanation = (
                        "%s doesn't support SCRAM-SHA-1, pass"
                        " mechanism='MONGODB-CR' to copy_database" % fromhost)

                    raise OperationFailure(explanation,
                                           exc.code,
                                           exc.details)
                else:
                    raise

        elif mechanism == 'MONGODB-CR':
            get_nonce_cmd = SON([('copydbgetnonce', 1),
                                 ('fromhost', fromhost)])

            get_nonce_response, _ = cmd_func(sock_info, 'admin', get_nonce_cmd)
            nonce = get_nonce_response['nonce']
            copydb_cmd = SON([('copydb', 1),
                              ('fromdb', fromdb),
                              ('todb', todb)])

            copydb_cmd['username'] = username
            copydb_cmd['nonce'] = nonce
            copydb_cmd['key'] = auth._auth_key(nonce, username, password)
            if fromhost is not None:
                copydb_cmd['fromhost'] = fromhost

            cmd_func(sock_info, 'admin', copydb_cmd)
        else:
            raise InvalidOperation('Authentication mechanism %r not supported'
                                   ' for copy_database' % mechanism)
    else:
        # No username.
        copydb_cmd = SON([('copydb', 1),
                          ('fromdb', fromdb),
                          ('todb', todb)])

        if fromhost:
            copydb_cmd['fromhost'] = fromhost

        cmd_func(sock_info, 'admin', copydb_cmd)


def shuffled(sequence):
    """Returns a copy of the sequence (as a :class:`list`) which has been
    shuffled by :func:`random.shuffle`.
    """
    out = list(sequence)
    random.shuffle(out)
    return out
