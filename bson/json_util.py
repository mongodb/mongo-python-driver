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
into `Mongo Extended JSON
<http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON>`_'s *Strict*
mode.  This lets you encode / decode BSON documents to JSON even when
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
   instances. (But not in Python 2.4.)

.. versionchanged:: 2.3
   Added dumps and loads helpers to automatically handle conversion to and
   from json and supports :class:`~bson.binary.Binary` and
   :class:`~bson.code.Code`

.. versionchanged:: 1.9
   Handle :class:`uuid.UUID` instances, whenever possible.

.. versionchanged:: 1.8
   Handle timezone aware datetime instances on encode, decode to
   timezone aware datetime instances.

.. versionchanged:: 1.8
   Added support for encoding/decoding :class:`~bson.max_key.MaxKey`
   and :class:`~bson.min_key.MinKey`, and for encoding
   :class:`~bson.timestamp.Timestamp`.

.. versionchanged:: 1.2
   Added support for encoding/decoding datetimes and regular expressions.
"""

import base64
import calendar
import datetime
import re
import time

json_lib = True
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        json_lib = False

import bson
from bson import EPOCH_AWARE, RE_TYPE, SON
from bson.binary import Binary
from bson.code import Code
from bson.dbref import DBRef
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.regex import Regex
from bson.timestamp import Timestamp
from bson.tz_util import utc

from bson.py3compat import PY3, binary_type, string_types


_RE_OPT_TABLE = {
    "i": re.I,
    "l": re.L,
    "m": re.M,
    "s": re.S,
    "u": re.U,
    "x": re.X,
}


def dumps(obj, *args, **kwargs):
    """Helper function that wraps :class:`json.dumps`.

    Recursive function that handles all BSON types including
    :class:`~bson.binary.Binary` and :class:`~bson.code.Code`.

    .. versionchanged:: 2.7
       Preserves order when rendering SON, Timestamp, Code, Binary, and DBRef
       instances. (But not in Python 2.4.)
    """
    if not json_lib:
        raise Exception("No json library available")
    return json.dumps(_json_convert(obj), *args, **kwargs)


def loads(s, *args, **kwargs):
    """Helper function that wraps :class:`json.loads`.

    Automatically passes the object_hook for BSON type conversion.

    :Parameters:
      - `compile_re` (optional): if ``False``, don't attempt to compile BSON
        regular expressions into Python regular expressions. Return instances
        of :class:`~bson.bsonregex.BSONRegex` instead.

    .. versionchanged:: 2.7
       Added ``compile_re`` option.
    """
    if not json_lib:
        raise Exception("No json library available")

    compile_re = kwargs.pop('compile_re', True)
    kwargs['object_hook'] = lambda dct: object_hook(dct, compile_re)
    return json.loads(s, *args, **kwargs)


def _json_convert(obj):
    """Recursive helper method that converts BSON types so they can be
    converted into json.
    """
    if hasattr(obj, 'iteritems') or hasattr(obj, 'items'):  # PY3 support
        return SON(((k, _json_convert(v)) for k, v in obj.iteritems()))
    elif hasattr(obj, '__iter__') and not isinstance(obj, string_types):
        return list((_json_convert(v) for v in obj))
    try:
        return default(obj)
    except TypeError:
        return obj


def object_hook(dct, compile_re=True):
    if "$oid" in dct:
        return ObjectId(str(dct["$oid"]))
    if "$ref" in dct:
        return DBRef(dct["$ref"], dct["$id"], dct.get("$db", None))
    if "$date" in dct:
        dtm = dct["$date"]
        # mongoexport 2.6 and newer
        if isinstance(dtm, basestring):
            # datetime.datetime.strptime is new in python 2.5
            naive = datetime.datetime(
                *(time.strptime(dtm[:19], "%Y-%m-%dT%H:%M:%S")[0:6]))
            # The %f format is new in python 2.6
            micros = int(dtm[20:23]) * 1000
            aware = naive.replace(microsecond=micros, tzinfo=utc)
            offset = dtm[23:]
            if not offset or offset == 'Z':
                # UTC
                return aware
            else:
                if len(offset) == 5:
                    # Offset from mongoexport is in format (+|-)HHMM
                    secs = (int(offset[1:3]) * 3600 + int(offset[3:]) * 60)
                elif ':' in offset and len(offset) == 6:
                    # RFC-3339 format (+|-)HH:MM
                    hours, minutes = offset[1:].split(':')
                    secs = (int(hours) * 3600 + int(minutes) * 60)
                else:
                    # Not RFC-3339 compliant or mongoexport output.
                    raise ValueError("invalid format for offset")
                if offset[0] == "-":
                    secs *= -1
                return aware - datetime.timedelta(seconds=secs)
        # mongoexport 2.6 and newer, time before the epoch (SERVER-15275)
        elif isinstance(dtm, dict):
            secs = float(dtm["$numberLong"]) / 1000.0
        # mongoexport before 2.6
        else:
            secs = float(dtm) / 1000.0
        return EPOCH_AWARE + datetime.timedelta(seconds=secs)
    if "$regex" in dct:
        flags = 0
        # PyMongo always adds $options but some other tools may not.
        for opt in dct.get("$options", ""):
            flags |= _RE_OPT_TABLE.get(opt, 0)

        if compile_re:
            return re.compile(dct["$regex"], flags)
        else:
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
        return Binary(base64.b64decode(dct["$binary"].encode()), subtype)
    if "$code" in dct:
        return Code(dct["$code"], dct.get("$scope"))
    if bson.has_uuid() and "$uuid" in dct:
        return bson.uuid.UUID(dct["$uuid"])
    if "$undefined" in dct:
        return None
    if "$numberLong" in dct:
        # 2to3 will change this to int. PyMongo 3.0 supports
        # a new type, Int64, to avoid round trip issues.
        return long(dct["$numberLong"])
    if "$timestamp" in dct:
        tsp = dct["$timestamp"]
        return Timestamp(tsp["t"], tsp["i"])
    return dct


def default(obj):
    # We preserve key order when rendering SON, DBRef, etc. as JSON by
    # returning a SON for those types instead of a dict. This works with
    # the "json" standard library in Python 2.6+ and with simplejson
    # 2.1.0+ in Python 2.5+, because those libraries iterate the SON
    # using PyIter_Next. Python 2.4 must use simplejson 2.0.9 or older,
    # and those versions of simplejson use the lower-level PyDict_Next,
    # which bypasses SON's order-preserving iteration, so we lose key
    # order in Python 2.4.
    if isinstance(obj, ObjectId):
        return {"$oid": str(obj)}
    if isinstance(obj, DBRef):
        return _json_convert(obj.as_doc())
    if isinstance(obj, datetime.datetime):
        # TODO share this code w/ bson.py?
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
        millis = int(calendar.timegm(obj.timetuple()) * 1000 +
                     obj.microsecond / 1000)
        return {"$date": millis}
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
        if isinstance(obj.pattern, unicode):
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
    if PY3 and isinstance(obj, binary_type):
        return SON([
            ('$binary', base64.b64encode(obj).decode()),
            ('$type', "00")])
    if bson.has_uuid() and isinstance(obj, bson.uuid.UUID):
        return {"$uuid": obj.hex}
    raise TypeError("%r is not JSON serializable" % obj)
