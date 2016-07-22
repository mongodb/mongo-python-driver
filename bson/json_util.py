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

"""Tools for using Python's :mod:`json` module with BSON documents.

This module provides two helper methods `dumps` and `loads` that wrap the
native :mod:`json` methods and provide explicit BSON conversion to and from
json.  This allows for specialized encoding and decoding of BSON documents
into `MongoDB Extended JSON
<http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON>`_'s *Strict
mode*.  This lets you encode / decode BSON documents to JSON even when
they use special BSON types.

Example usage (serialization):

.. doctest::

   >>> from bson import Binary, Code
   >>> from bson.json_util import dumps
   >>> dumps([{'foo': [1, 2]},
   ...        {'bar': {'hello': 'world'}},
   ...        {'code': Code("function x() { return 1; }")},
   ...        {'bin': Binary("\x01\x02\x03\x04")}])
   '[{"foo": [1, 2]}, {"bar": {"hello": "world"}}, {"code": {"$code": "function x() { return 1; }", "$scope": {}}}, {"bin": {"$binary": "AQIDBA==", "$type": "00"}}]'

Example usage (deserialization):

.. doctest::

   >>> from bson.json_util import loads
   >>> loads('[{"foo": [1, 2]}, {"bar": {"hello": "world"}}, {"code": {"$scope": {}, "$code": "function x() { return 1; }"}}, {"bin": {"$type": "00", "$binary": "AQIDBA=="}}]')
   [{u'foo': [1, 2]}, {u'bar': {u'hello': u'world'}}, {u'code': Code('function x() { return 1; }', {})}, {u'bin': Binary('...', 0)}]

Alternatively, you can manually pass the `default` to :func:`json.dumps`.
It won't handle :class:`~bson.binary.Binary` and :class:`~bson.code.Code`
instances (as they are extended strings you can't provide custom defaults),
but it will be faster as there is less recursion.

.. versionchanged:: 2.8
   The output format for :class:`~bson.timestamp.Timestamp` has changed from
   '{"t": <int>, "i": <int>}' to '{"$timestamp": {"t": <int>, "i": <int>}}'.
   This new format will be decoded to an instance of
   :class:`~bson.timestamp.Timestamp`. The old format will continue to be
   decoded to a python dict as before. Encoding to the old format is no longer
   supported as it was never correct and loses type information.
   Added support for $numberLong and $undefined - new in MongoDB 2.6 - and
   parsing $date in ISO-8601 format.

.. versionchanged:: 2.7
   Preserves order when rendering SON, Timestamp, Code, Binary, and DBRef
   instances.

.. versionchanged:: 2.3
   Added dumps and loads helpers to automatically handle conversion to and
   from json and supports :class:`~bson.binary.Binary` and
   :class:`~bson.code.Code`
"""

import base64
import collections
import datetime
import json
import re
import uuid

import bson
from bson import EPOCH_AWARE, RE_TYPE, SON
from bson.binary import (Binary, JAVA_LEGACY, CSHARP_LEGACY, OLD_UUID_SUBTYPE,
                         UUID_SUBTYPE)
from bson.code import Code
from bson.codec_options import CodecOptions
from bson.errors import InvalidDatetime
from bson.dbref import DBRef
from bson.int64 import Int64
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.regex import Regex
from bson.timestamp import Timestamp
from bson.tz_util import utc

from bson.py3compat import PY3, iteritems, string_type, text_type


_RE_OPT_TABLE = {
    "i": re.I,
    "l": re.L,
    "m": re.M,
    "s": re.S,
    "u": re.U,
    "x": re.X,
}


class JSONOptions(CodecOptions):
    """Encapsulates JSON options for :func:`dumps` and :func:`loads`.

    :Parameters:
      - `strict_number_long`: If ``True``, :class:`~bson.int64.Int64` objects
        are encoded to MongoDB Extended JSON's *Strict mode* type
        `NumberLong`, ie ``'{"$numberLong": "<number>" }'``. Otherwise they
        will be encoded as an `int`. Defaults to ``False``.
      - `strict_date`: If ``True``, `datetime.datetime` objects are encoded to
        MongoDB Extended JSON's *Strict mode* type `Date`. Otherwise it will
        be encoded as milliseconds since Unix epoch. Defaults to ``False``.
      - `strict_uuid`: If ``True``, :class:`uuid.UUID` object are encoded to
        MongoDB Extended JSON's *Strict mode* type `Binary`. Otherwise it
        will be encoded as ``'{"$uuid": "<hex>" }'``. Defaults to ``False``.
      - `document_class`: BSON documents returned by :func:`loads` will be
        decoded to an instance of this class. Must be a subclass of
        :class:`collections.MutableMapping`. Defaults to :class:`dict`.
      - `uuid_representation`: The BSON representation to use when encoding
        and decoding instances of :class:`uuid.UUID`. Defaults to
        :const:`~bson.binary.PYTHON_LEGACY`.
      - `tz_aware`: If ``True``, MongoDB Extended JSON's *Strict mode* type
        `Date` will be decoded to timezone aware instances of
        :class:`datetime.datetime`. Otherwise they will be naive. Defaults
        to ``True``.
      - `tzinfo`: A :class:`datetime.tzinfo` subclass that specifies the
        timezone from which :class:`~datetime.datetime` objects should be
        decoded. Defaults to :const:`~bson.tz_util.utc`.
      - `args`: arguments to :class:`~bson.codec_options.CodecOptions`
      - `kwargs`: arguments to :class:`~bson.codec_options.CodecOptions`

    .. seealso:: The documentation for `MongoDB Extended JSON
       <http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON>`_.

    .. versionadded:: 3.4
    """

    def __new__(cls, strict_number_long=False, strict_date=False,
                strict_uuid=False, *args, **kwargs):
        kwargs["tz_aware"] = kwargs.get("tz_aware", True)
        if kwargs["tz_aware"]:
            kwargs["tzinfo"] = kwargs.get("tzinfo", utc)
        self = super(JSONOptions, cls).__new__(cls, *args, **kwargs)
        self.strict_number_long = strict_number_long
        self.strict_date = strict_date
        self.strict_uuid = strict_uuid
        return self

    def _arguments_repr(self):
        return 'strict_number_long=%r, strict_date=%r, strict_uuid=%r, %s' % (
            self.strict_number_long,
            self.strict_date,
            self.strict_uuid,
            super(JSONOptions, self)._arguments_repr())


DEFAULT_JSON_OPTIONS = JSONOptions()
"""The default :class:`JSONOptions` for JSON encoding/decoding.

.. versionadded:: 3.4
"""
STRICT_JSON_OPTIONS = JSONOptions(strict_number_long=True, strict_date=True,
                                  strict_uuid=True)
""":class:`JSONOptions` for MongoDB Extended JSON's *Strict mode* encoding.

.. versionadded:: 3.4
"""


def dumps(obj, *args, **kwargs):
    """Helper function that wraps :func:`json.dumps`.

    Recursive function that handles all BSON types including
    :class:`~bson.binary.Binary` and :class:`~bson.code.Code`.

    Raises :class:`~bson.errors.InvalidDatetime` if `obj` contains a
    :class:`datetime.datetime` without a timezone and
    `json_options.strict_date` is ``True``.

    :Parameters:
      - `json_options`: A :class:`JSONOptions` instance used to modify the
        encoding of MongoDB Extended JSON types. Defaults to
        :const:`DEFAULT_JSON_OPTIONS`.

    .. versionchanged:: 3.4
       Accepts optional parameter `json_options`. See :class:`JSONOptions`.

    .. versionchanged:: 2.7
       Preserves order when rendering SON, Timestamp, Code, Binary, and DBRef
       instances.
    """
    json_options = kwargs.pop("json_options", DEFAULT_JSON_OPTIONS)
    return json.dumps(_json_convert(obj, json_options), *args, **kwargs)


def loads(s, *args, **kwargs):
    """Helper function that wraps :func:`json.loads`.

    Automatically passes the object_hook for BSON type conversion.

    :Parameters:
      - `json_options`: A :class:`JSONOptions` instance used to modify the
        decoding of MongoDB Extended JSON types. Defaults to
        :const:`DEFAULT_JSON_OPTIONS`.

    .. versionchanged:: 3.4
       Accepts optional parameter `json_options`. See :class:`JSONOptions`.
    """
    json_options = kwargs.pop("json_options", DEFAULT_JSON_OPTIONS)
    kwargs["object_pairs_hook"] = lambda pairs: object_pairs_hook(pairs,
                                                                  json_options)
    return json.loads(s, *args, **kwargs)


def _json_convert(obj, json_options=DEFAULT_JSON_OPTIONS):
    """Recursive helper method that converts BSON types so they can be
    converted into json.
    """
    if hasattr(obj, 'iteritems') or hasattr(obj, 'items'):  # PY3 support
        return SON(((k, _json_convert(v, json_options))
                    for k, v in iteritems(obj)))
    elif hasattr(obj, '__iter__') and not isinstance(obj, (text_type, bytes)):
        return list((_json_convert(v, json_options) for v in obj))
    try:
        return default(obj, json_options)
    except TypeError:
        return obj


def object_pairs_hook(pairs, json_options=DEFAULT_JSON_OPTIONS):
    return object_hook(json_options.document_class(pairs), json_options)


def object_hook(dct, json_options=DEFAULT_JSON_OPTIONS):
    if "$oid" in dct:
        return ObjectId(str(dct["$oid"]))
    if "$ref" in dct:
        return DBRef(dct["$ref"], dct["$id"], dct.get("$db", None))
    if "$date" in dct:
        dtm = dct["$date"]
        # mongoexport 2.6 and newer
        if isinstance(dtm, string_type):
            # Parse offset
            if dtm[-1] == 'Z':
                dt = dtm[:-1]
                offset = 'Z'
            elif dtm[-3] == ':':
                # (+|-)HH:MM
                dt = dtm[:-6]
                offset = dtm[-6:]
            elif dtm[-5] in ('+', '-'):
                # (+|-)HHMM
                dt = dtm[:-5]
                offset = dtm[-5:]
            elif dtm[-3] in ('+', '-'):
                # (+|-)HH
                dt = dtm[:-3]
                offset = dtm[-3:]
            else:
                dt = dtm
                offset = ''

            aware = datetime.datetime.strptime(
                dt, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=utc)

            if offset and offset != 'Z':
                if len(offset) == 6:
                    hours, minutes = offset[1:].split(':')
                    secs = (int(hours) * 3600 + int(minutes) * 60)
                elif len(offset) == 5:
                    secs = (int(offset[1:3]) * 3600 + int(offset[3:]) * 60)
                elif len(offset) == 3:
                    secs = int(offset[1:3]) * 3600
                if offset[0] == "-":
                    secs *= -1
                aware = aware - datetime.timedelta(seconds=secs)

            if json_options.tz_aware:
                if json_options.tzinfo:
                    aware = aware.astimezone(json_options.tzinfo)
                return aware
            else:
                return aware.replace(tzinfo=None)
        # mongoexport 2.6 and newer, time before the epoch (SERVER-15275)
        elif isinstance(dtm, collections.Mapping):
            millis = int(dtm["$numberLong"])
        # mongoexport before 2.6
        else:
            millis = int(dtm)
        return bson._millis_to_datetime(millis, json_options)
    if "$regex" in dct:
        flags = 0
        # PyMongo always adds $options but some other tools may not.
        for opt in dct.get("$options", ""):
            flags |= _RE_OPT_TABLE.get(opt, 0)
        return Regex(dct["$regex"], flags)
    if "$minKey" in dct:
        return MinKey()
    if "$maxKey" in dct:
        return MaxKey()
    if "$binary" in dct:
        if isinstance(dct["$type"], int):
            dct["$type"] = "%02x" % dct["$type"]
        subtype = int(dct["$type"], 16)
        if subtype >= 0xffffff80:  # Handle mongoexport values
            subtype = int(dct["$type"][6:], 16)
        data = base64.b64decode(dct["$binary"].encode())
        # special handling for UUID
        if subtype == OLD_UUID_SUBTYPE:
            if json_options.uuid_representation == CSHARP_LEGACY:
                return uuid.UUID(bytes_le=data)
            if json_options.uuid_representation == JAVA_LEGACY:
                data = data[7::-1] + data[:7:-1]
            return uuid.UUID(bytes=data)
        if subtype == UUID_SUBTYPE:
            return uuid.UUID(bytes=data)
        return Binary(data, subtype)
    if "$code" in dct:
        return Code(dct["$code"], dct.get("$scope"))
    if "$uuid" in dct:
        return uuid.UUID(dct["$uuid"])
    if "$undefined" in dct:
        return None
    if "$numberLong" in dct:
        return Int64(dct["$numberLong"])
    if "$timestamp" in dct:
        tsp = dct["$timestamp"]
        return Timestamp(tsp["t"], tsp["i"])
    return dct


def default(obj, json_options=DEFAULT_JSON_OPTIONS):
    # We preserve key order when rendering SON, DBRef, etc. as JSON by
    # returning a SON for those types instead of a dict.
    if isinstance(obj, ObjectId):
        return {"$oid": str(obj)}
    if isinstance(obj, DBRef):
        return _json_convert(obj.as_doc())
    if isinstance(obj, datetime.datetime):
        if json_options.strict_date:
            if not obj.tzinfo:
                raise InvalidDatetime("datetime is not timezone aware", obj)
            if obj >= EPOCH_AWARE:
                return {"$date": "%s.%03d%s" % (
                    obj.strftime("%Y-%m-%dT%H:%M:%S"),
                    int(obj.microsecond / 1000),
                    obj.strftime("%z"))}

        millis = bson._datetime_to_millis(obj)
        if json_options.strict_date:
            return {"$date": {"$numberLong": str(millis)}}
        else:
            return {"$date": millis}
    if json_options.strict_number_long and isinstance(obj, Int64):
        return {"$numberLong": str(obj)}
    if isinstance(obj, (RE_TYPE, Regex)):
        flags = ""
        if obj.flags & re.IGNORECASE:
            flags += "i"
        if obj.flags & re.LOCALE:
            flags += "l"
        if obj.flags & re.MULTILINE:
            flags += "m"
        if obj.flags & re.DOTALL:
            flags += "s"
        if obj.flags & re.UNICODE:
            flags += "u"
        if obj.flags & re.VERBOSE:
            flags += "x"
        if isinstance(obj.pattern, text_type):
            pattern = obj.pattern
        else:
            pattern = obj.pattern.decode('utf-8')
        return SON([("$regex", pattern), ("$options", flags)])
    if isinstance(obj, MinKey):
        return {"$minKey": 1}
    if isinstance(obj, MaxKey):
        return {"$maxKey": 1}
    if isinstance(obj, Timestamp):
        return {"$timestamp": SON([("t", obj.time), ("i", obj.inc)])}
    if isinstance(obj, Code):
        return SON([('$code', str(obj)), ('$scope', obj.scope)])
    if isinstance(obj, Binary):
        return SON([
            ('$binary', base64.b64encode(obj).decode()),
            ('$type', "%02x" % obj.subtype)])
    if PY3 and isinstance(obj, bytes):
        return SON([
            ('$binary', base64.b64encode(obj).decode()),
            ('$type', "00")])
    if isinstance(obj, uuid.UUID):
        if json_options.strict_uuid:
            data = obj.bytes
            subtype = OLD_UUID_SUBTYPE
            if json_options.uuid_representation == CSHARP_LEGACY:
                data = obj.bytes_le
            elif json_options.uuid_representation == JAVA_LEGACY:
                data = data[7::-1] + data[:7:-1]
            elif json_options.uuid_representation == UUID_SUBTYPE:
                subtype = UUID_SUBTYPE
            return SON([
                ('$binary', base64.b64encode(data).decode()),
                ('$type', "%02x" % subtype)])
        else:
            return {"$uuid": obj.hex}
    raise TypeError("%r is not JSON serializable" % obj)
