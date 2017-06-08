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

This module provides two helper methods `dumps` and `loads` that wrap the native
:mod:`json` methods and provide explicit BSON conversion to and from
JSON. :class:`~bson.json_util.JSONOptions` provides a way to control how JSON is
emitted and parsed, with the default being the legacy PyMongo format.
:mod:`~bson.json_util` can also generate and parse `canonical extended JSON`_ when
:data:`~bson.json_util.CANONICAL_JSON_OPTIONS` is provided.

.. _canonical extended JSON: https://github.com/mongodb/specifications/blob/master/source/extended-json.rst

Example usage (serialization):

.. doctest::

   >>> from bson import Binary, Code
   >>> from bson.json_util import dumps
   >>> dumps([{'foo': [1, 2]},
   ...        {'bar': {'hello': 'world'}},
   ...        {'code': Code("function x() { return 1; }", {})},
   ...        {'bin': Binary(b"\x01\x02\x03\x04")}])
   '[{"foo": [1, 2]}, {"bar": {"hello": "world"}}, {"code": {"$code": "function x() { return 1; }", "$scope": {}}}, {"bin": {"$binary": "AQIDBA==", "$type": "00"}}]'

Example usage (deserialization):

.. doctest::

   >>> from bson.json_util import loads
   >>> loads('[{"foo": [1, 2]}, {"bar": {"hello": "world"}}, {"code": {"$scope": {}, "$code": "function x() { return 1; }"}}, {"bin": {"$type": "00", "$binary": "AQIDBA=="}}]')
   [{u'foo': [1, 2]}, {u'bar': {u'hello': u'world'}}, {u'code': Code('function x() { return 1; }', {})}, {u'bin': Binary('...', 0)}]

Example usage (with a :class:`~bson.json_util.JSONOptions` given):

.. doctest::

   >>> from bson import Binary, Code
   >>> from bson.json_util import dumps, CANONICAL_JSON_OPTIONS
   >>> dumps([{'foo': [1, 2]},
   ...        {'bar': {'hello': 'world'}},
   ...        {'code': Code("function x() { return 1; }")},
   ...        {'bin': Binary(b"\x01\x02\x03\x04")}],
   ...       json_options=CANONICAL_JSON_OPTIONS)
   '[{"foo": [{"$numberInt": "1"}, {"$numberInt": "2"}]}, {"bar": {"hello": "world"}}, {"code": {"$code": "function x() { return 1; }"}}, {"bin": {"$binary": "AQIDBA==", "$type": "00"}}]'

Alternatively, you can manually pass the `default` to :func:`json.dumps`.
It won't handle :class:`~bson.binary.Binary` and :class:`~bson.code.Code`
instances (as they are extended strings you can't provide custom defaults),
but it will be faster as there is less recursion.

.. note::
   If your application does not need the flexibility offered by
   :class:`JSONOptions` and spends a large amount of time in the `json_util`
   module, look to
   `python-bsonjs <https://pypi.python.org/pypi/python-bsonjs>`_ for a nice
   performance improvement. `python-bsonjs` is a fast BSON to MongoDB
   Extended JSON converter for Python built on top of
   `libbson <https://github.com/mongodb/libbson>`_. `python-bsonjs` works best
   with PyMongo when using :class:`~bson.raw_bson.RawBSONDocument`.

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
import math
import re
import sys
import uuid

_HAS_OBJECT_PAIRS_HOOK = True
if sys.version_info[:2] == (2, 6):
    # In Python 2.6, json does not include object_pairs_hook. Use simplejson
    # instead.
    try:
        import simplejson as json
    except ImportError:
        import json
        _HAS_OBJECT_PAIRS_HOOK = False
else:
    import json

from pymongo.errors import ConfigurationError

import bson
from bson import EPOCH_AWARE, RE_TYPE, SON
from bson.binary import (Binary, JAVA_LEGACY, CSHARP_LEGACY, OLD_UUID_SUBTYPE,
                         UUID_SUBTYPE)
from bson.code import Code
from bson.codec_options import CodecOptions
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.int64 import Int64
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.py3compat import PY3, iteritems, integer_types, string_type, text_type
from bson.regex import Regex
from bson.timestamp import Timestamp
from bson.tz_util import utc


_RE_OPT_TABLE = {
    "i": re.I,
    "l": re.L,
    "m": re.M,
    "s": re.S,
    "u": re.U,
    "x": re.X,
}

# Dollar-prefixed keys which may appear in DBRefs.
_DBREF_KEYS = frozenset(['$id', '$ref', '$db'])


class DatetimeRepresentation:
    LEGACY = 0
    """Legacy MongoDB Extended JSON datetime representation.

    :class:`datetime.datetime` instances will be encoded to JSON in the
    format `{"$date": <dateAsMilliseconds>}`, where `dateAsMilliseconds` is
    a 64-bit signed integer giving the number of milliseconds since the Unix
    epoch UTC. This was the default encoding before PyMongo version 3.4.

    .. versionadded:: 3.4
    """

    NUMBERLONG = 1
    """NumberLong datetime representation.

    :class:`datetime.datetime` instances will be encoded to JSON in the
    format `{"$date": {"$numberLong": "<dateAsMilliseconds>"}}`,
    where `dateAsMilliseconds` is the string representation of a 64-bit signed
    integer giving the number of milliseconds since the Unix epoch UTC.

    .. versionadded:: 3.4
    """

    ISO8601 = 2
    """ISO-8601 datetime representation.

    :class:`datetime.datetime` instances greater than or equal to the Unix
    epoch UTC will be encoded to JSON in the format `{"$date": "<ISO-8601>"}`.
    :class:`datetime.datetime` instances before the Unix epoch UTC will be
    encoded as if the datetime representation is
    :const:`~DatetimeRepresentation.NUMBERLONG`.

    .. versionadded:: 3.4
    """


class JSONOptions(CodecOptions):
    """Encapsulates JSON options for :func:`dumps` and :func:`loads`.

    Raises :exc:`~pymongo.errors.ConfigurationError` on Python 2.6 if
    `simplejson <https://pypi.python.org/pypi/simplejson>`_ is not installed
    and document_class is not the default (:class:`dict`).

    :Parameters:
      - `strict_number_long`: If ``True``, :class:`~bson.int64.Int64` objects
        are encoded to MongoDB Extended JSON's *Strict mode* type
        `NumberLong`, ie ``'{"$numberLong": "<number>" }'``. Otherwise they
        will be encoded as an `int`. Defaults to ``False``.
      - `datetime_representation`: The representation to use when encoding
        instances of :class:`datetime.datetime`. Defaults to
        :const:`~DatetimeRepresentation.LEGACY`.
      - `strict_uuid`: If ``True``, :class:`uuid.UUID` object are encoded to
        MongoDB Extended JSON's *Strict mode* type `Binary`. Otherwise it
        will be encoded as ``'{"$uuid": "<hex>" }'``. Defaults to ``False``.
      - `canonical_extended_json`: If ``True``, use Canonical Extended JSON
        representations for all BSON values. This option implies
        ``strict_number_long=True``,
        ``datetime_representation=DatetimeRepresentation.NUMBERLONG``, and
        ``strict_uuid=True``.
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

    .. versionchanged:: 3.5
       Accepts the optional parameter `canonical_extended_json`.

    """

    def __new__(cls, strict_number_long=False,
                datetime_representation=DatetimeRepresentation.LEGACY,
                strict_uuid=False, canonical_extended_json=False,
                *args, **kwargs):
        kwargs["tz_aware"] = kwargs.get("tz_aware", True)
        if kwargs["tz_aware"]:
            kwargs["tzinfo"] = kwargs.get("tzinfo", utc)
        if datetime_representation not in (DatetimeRepresentation.LEGACY,
                                           DatetimeRepresentation.NUMBERLONG,
                                           DatetimeRepresentation.ISO8601):
            raise ConfigurationError(
                "JSONOptions.datetime_representation must be one of LEGACY,"
                "NUMBERLONG, or ISO8601 from DatetimeRepresentation.")
        self = super(JSONOptions, cls).__new__(cls, *args, **kwargs)
        if not _HAS_OBJECT_PAIRS_HOOK and self.document_class != dict:
            raise ConfigurationError(
                "Support for JSONOptions.document_class on Python 2.6 "
                "requires simplejson "
                "(https://pypi.python.org/pypi/simplejson) to be installed.")
        self.canonical_extended_json = canonical_extended_json
        if self.canonical_extended_json:
            self.strict_number_long = True
            self.datetime_representation = DatetimeRepresentation.NUMBERLONG
            self.strict_uuid = True
        else:
            self.strict_number_long = strict_number_long
            self.datetime_representation = datetime_representation
            self.strict_uuid = strict_uuid
        return self

    def _arguments_repr(self):
        return ('strict_number_long=%r, '
                'datetime_representation=%r, '
                'strict_uuid=%r, canonical_extended_json=%r, %s' % (
                    self.strict_number_long,
                    self.datetime_representation,
                    self.strict_uuid,
                    self.canonical_extended_json,
                    super(JSONOptions, self)._arguments_repr()))


DEFAULT_JSON_OPTIONS = JSONOptions()
"""The default :class:`JSONOptions` for JSON encoding/decoding.

.. versionadded:: 3.4
"""

STRICT_JSON_OPTIONS = JSONOptions(
    strict_number_long=True,
    datetime_representation=DatetimeRepresentation.ISO8601,
    strict_uuid=True)
""":class:`JSONOptions` for MongoDB Extended JSON's *Strict mode* encoding.

.. versionadded:: 3.4
"""

CANONICAL_JSON_OPTIONS = JSONOptions(canonical_extended_json=True)
""":class:`JSONOptions` for `canonical extended JSON`_.

.. versionadded:: 3.5
"""


def dumps(obj, *args, **kwargs):
    """Helper function that wraps :func:`json.dumps`.

    Recursive function that handles all BSON types including
    :class:`~bson.binary.Binary` and :class:`~bson.code.Code`.

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
    if _HAS_OBJECT_PAIRS_HOOK:
        kwargs["object_pairs_hook"] = lambda pairs: object_pairs_hook(
            pairs, json_options)
    else:
        kwargs["object_hook"] = lambda obj: (
            canonical_object_hook(obj, json_options)
            if json_options.canonical_extended_json
            else object_hook(obj, json_options))
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
    document = json_options.document_class(pairs)
    if json_options.canonical_extended_json:
        return canonical_object_hook(document, json_options)
    return object_hook(document, json_options)


def object_hook(dct, json_options=DEFAULT_JSON_OPTIONS):
    if "$oid" in dct:
        return ObjectId(str(dct["$oid"]))
    if "$ref" in dct:
        return DBRef(dct["$ref"], dct["$id"], dct.get("$db", None))
    if "$date" in dct:
        return _get_date(dct, json_options)
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
        return _get_binary(dct, json_options)
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
    if "$numberDecimal" in dct:
        return Decimal128(dct["$numberDecimal"])
    return dct


def _get_binary(doc, json_options):
    if isinstance(doc["$type"], int):
        doc["$type"] = "%02x" % doc["$type"]
    subtype = int(doc["$type"], 16)
    if subtype >= 0xffffff80:  # Handle mongoexport values
        subtype = int(doc["$type"][6:], 16)
    data = base64.b64decode(doc["$binary"].encode())
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


def _get_date(doc, json_options):
    dtm = doc["$date"]
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


def _get_dbpointer(doc, json_options):
    dbref = doc['$dbPointer']
    if isinstance(dbref, DBRef):
        # DBPointer must not contain $db in its value.
        if dbref.database is None:
            return dbref
        # Otherwise, this is just a regular document.
        return json_options.document_class(
            [('$dbPointer', json_options.document_class(dbref.as_doc()))])
    return doc


_CANONICAL_JSON_TABLE = {
    frozenset(['$oid']): lambda d, _: ObjectId(d['$oid']),
    frozenset(['$numberDecimal']): lambda d, _: Decimal128(d['$numberDecimal']),
    frozenset(['$symbol']): lambda d, _: text_type(d['$symbol']),
    frozenset(['$numberInt']): lambda d, _: int(d['$numberInt']),
    frozenset(['$numberDouble']): lambda d, _: float(d['$numberDouble']),
    frozenset(['$numberLong']): lambda d, _: Int64(d['$numberLong']),
    frozenset(['$date']): _get_date,
    frozenset(['$minKey']): lambda dummy0, dummy1: MinKey(),
    frozenset(['$maxKey']): lambda dummy0, dummy1: MaxKey(),
    frozenset(['$undefined']): lambda dummy0, dummy1: None,
    frozenset(['$dbPointer']): _get_dbpointer,
    frozenset(['$ref', '$id']): lambda d, _: DBRef(
        d.pop('$ref'), d.pop('$id'), **d),
    frozenset(['$ref', '$id', '$db']): lambda d, _: DBRef(
        d.pop('$ref'), d.pop('$id'), d.pop('$db'), **d),
    frozenset(['$regex', '$options']): lambda d, _: Regex(
        d['$regex'], d['$options']),
    frozenset(['$binary', '$type']): _get_binary,
    frozenset(['$code']): lambda d, _: Code(d['$code']),
    frozenset(['$code', '$scope']): lambda d, _: Code(
        d['$code'], d['$scope']),
    frozenset(['$timestamp']): lambda d, _: Timestamp(
        int(d['$timestamp']) >> 32, int(d['$timestamp']) & 0xffffffff)
}


def canonical_object_hook(dct, json_options=CANONICAL_JSON_OPTIONS):
    keyset = frozenset(key for key in dct if key.startswith('$'))
    converter = _CANONICAL_JSON_TABLE.get(keyset)
    if converter:
        return converter(dct, json_options)
    elif '$ref' in dct and '$id' in dct:
        # DBRef may contain other keys that don't start with $.
        if keyset - _DBREF_KEYS:
            # Other keys start with $, so dct cannot be parsed as a DBRef.
            return dct
        else:
            return DBRef(dct.pop('$ref'), dct.pop('$id'),
                         dct.pop('$db', None), **dct)
    return dct


def default(obj, json_options=DEFAULT_JSON_OPTIONS):
    # We preserve key order when rendering SON, DBRef, etc. as JSON by
    # returning a SON for those types instead of a dict.
    if isinstance(obj, ObjectId):
        return {"$oid": str(obj)}
    if isinstance(obj, DBRef):
        return _json_convert(obj.as_doc(), json_options=json_options)
    if isinstance(obj, datetime.datetime):
        if (json_options.datetime_representation ==
                DatetimeRepresentation.ISO8601):
            if not obj.tzinfo:
                if json_options.tzinfo \
                        and isinstance(json_options.tzinfo, datetime.tzinfo):
                    obj = obj.replace(tzinfo=json_options.tzinfo)
                else:
                    obj = obj.replace(tzinfo=utc)
            if obj >= EPOCH_AWARE:
                off = obj.tzinfo.utcoffset(obj)
                if (off.days, off.seconds, off.microseconds) == (0, 0, 0):
                    tz_string = 'Z'
                else:
                    tz_string = obj.strftime('%z')
                return {"$date": "%s.%03d%s" % (
                    obj.strftime("%Y-%m-%dT%H:%M:%S"),
                    int(obj.microsecond / 1000),
                    tz_string)}

        millis = bson._datetime_to_millis(obj)
        if (json_options.datetime_representation ==
                DatetimeRepresentation.LEGACY):
            return {"$date": millis}
        return {"$date": {"$numberLong": str(millis)}}
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
        if json_options.canonical_extended_json:
            return {'$timestamp': str((obj.time << 32) + obj.inc)}
        return {"$timestamp": SON([("t", obj.time), ("i", obj.inc)])}
    if isinstance(obj, Code):
        if obj.scope is None:
            return SON([('$code', str(obj))])
        return SON([
            ('$code', str(obj)),
            ('$scope', _json_convert(obj.scope, json_options))])
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
    if isinstance(obj, Decimal128):
        return {"$numberDecimal": str(obj)}
    if isinstance(obj, bool):
        return obj
    if json_options.canonical_extended_json and isinstance(obj, integer_types):
        if -2 ** 31 <= obj < 2 ** 31:
            return {'$numberInt': text_type(obj)}
        return {'$numberLong': text_type(obj)}
    if json_options.canonical_extended_json and isinstance(obj, float):
        if math.isnan(obj):
            representation = 'NaN'
        elif math.isinf(obj):
            representation = 'Infinity' if obj > 0 else '-Infinity'
        else:
            # repr() will return the shortest string guaranteed to produce the
            # original value, when float() is called on it. str produces a
            # shorter string in Python 2.
            representation = text_type(repr(obj))
        return {'$numberDouble': representation}
    raise TypeError("%r is not JSON serializable" % obj)
