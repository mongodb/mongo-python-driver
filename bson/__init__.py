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

"""BSON (Binary JSON) encoding and decoding.
"""

import calendar
import datetime
import re
import struct
import sys

from bson.binary import (Binary, OLD_UUID_SUBTYPE,
                         JAVA_LEGACY, CSHARP_LEGACY)
from bson.code import Code
from bson.dbref import DBRef
from bson.errors import (InvalidBSON,
                         InvalidDocument,
                         InvalidStringData)
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.py3compat import b, binary_type
from bson.regex import Regex
from bson.son import SON, RE_TYPE
from bson.timestamp import Timestamp
from bson.tz_util import utc


try:
    from bson import _cbson
    _use_c = True
except ImportError:
    _use_c = False

try:
    import uuid
    _use_uuid = True
except ImportError:
    _use_uuid = False

PY3 = sys.version_info[0] == 3


MAX_INT32 = 2147483647
MIN_INT32 = -2147483648
MAX_INT64 = 9223372036854775807
MIN_INT64 = -9223372036854775808

EPOCH_AWARE = datetime.datetime.fromtimestamp(0, utc)
EPOCH_NAIVE = datetime.datetime.utcfromtimestamp(0)

# Create constants compatible with all versions of
# python from 2.4 forward. In 2.x b("foo") is just
# "foo". In 3.x it becomes b"foo".
EMPTY = b("")
ZERO  = b("\x00")
ONE   = b("\x01")

BSONNUM = b("\x01") # Floating point
BSONSTR = b("\x02") # UTF-8 string
BSONOBJ = b("\x03") # Embedded document
BSONARR = b("\x04") # Array
BSONBIN = b("\x05") # Binary
BSONUND = b("\x06") # Undefined
BSONOID = b("\x07") # ObjectId
BSONBOO = b("\x08") # Boolean
BSONDAT = b("\x09") # UTC Datetime
BSONNUL = b("\x0A") # Null
BSONRGX = b("\x0B") # Regex
BSONREF = b("\x0C") # DBRef
BSONCOD = b("\x0D") # Javascript code
BSONSYM = b("\x0E") # Symbol
BSONCWS = b("\x0F") # Javascript code with scope
BSONINT = b("\x10") # 32bit int
BSONTIM = b("\x11") # Timestamp
BSONLON = b("\x12") # 64bit int
BSONMIN = b("\xFF") # Min key
BSONMAX = b("\x7F") # Max key


def _get_int(data, position, as_class=None,
             tz_aware=False, uuid_subtype=OLD_UUID_SUBTYPE,
             compile_re=True, unsigned=False):
    format = unsigned and "I" or "i"
    try:
        value = struct.unpack("<%s" % format, data[position:position + 4])[0]
    except struct.error:
        raise InvalidBSON()
    position += 4
    return value, position


def _get_c_string(data, position, length=None):
    if length is None:
        try:
            end = data.index(ZERO, position)
        except ValueError:
            raise InvalidBSON()
    else:
        end = position + length
    value = data[position:end].decode("utf-8")
    position = end + 1

    return value, position


def _make_c_string(string, check_null=False):
    if isinstance(string, unicode):
        if check_null and "\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NULL character")
        return string.encode("utf-8") + ZERO
    else:
        if check_null and ZERO in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NULL character")
        try:
            string.decode("utf-8")
            return string + ZERO
        except UnicodeError:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % string)


def _get_number(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    num = struct.unpack("<d", data[position:position + 8])[0]
    position += 8
    return num, position


def _get_string(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    length = struct.unpack("<i", data[position:position + 4])[0]
    if length <= 0 or (len(data) - position - 4) < length:
        raise InvalidBSON("invalid string length")
    position += 4
    if data[position + length - 1:position + length] != ZERO:
        raise InvalidBSON("invalid end of string")
    return _get_c_string(data, position, length - 1)


def _get_object(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    obj_size = struct.unpack("<i", data[position:position + 4])[0]
    if data[position + obj_size - 1:position + obj_size] != ZERO:
        raise InvalidBSON("bad eoo")
    encoded = data[position + 4:position + obj_size - 1]
    object = _elements_to_dict(
        encoded, as_class, tz_aware, uuid_subtype, compile_re)

    position += obj_size
    if "$ref" in object:
        return (DBRef(object.pop("$ref"), object.pop("$id", None),
                      object.pop("$db", None), object), position)
    return object, position


def _get_array(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    obj, position = _get_object(data, position,
                                as_class, tz_aware, uuid_subtype, compile_re)
    result = []
    i = 0
    while True:
        try:
            result.append(obj[str(i)])
            i += 1
        except KeyError:
            break
    return result, position


def _get_binary(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    length, position = _get_int(data, position)
    subtype = ord(data[position:position + 1])
    position += 1
    if subtype == 2:
        length2, position = _get_int(data, position)
        if length2 != length - 4:
            raise InvalidBSON("invalid binary (st 2) - lengths don't match!")
        length = length2
    if subtype in (3, 4) and _use_uuid:
        # Java Legacy
        if uuid_subtype == JAVA_LEGACY:
            java = data[position:position + length]
            value = uuid.UUID(bytes=java[0:8][::-1] + java[8:16][::-1])
        # C# legacy
        elif uuid_subtype == CSHARP_LEGACY:
            value = uuid.UUID(bytes_le=data[position:position + length])
        # Python
        else:
            value = uuid.UUID(bytes=data[position:position + length])
        position += length
        return (value, position)
    # Python3 special case. Decode subtype 0 to 'bytes'.
    if PY3 and subtype == 0:
        value = data[position:position + length]
    else:
        value = Binary(data[position:position + length], subtype)
    position += length
    return value, position


def _get_oid(data, position, as_class=None,
             tz_aware=False, uuid_subtype=OLD_UUID_SUBTYPE, compile_re=True):
    value = ObjectId(data[position:position + 12])
    position += 12
    return value, position


def _get_boolean(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    value = data[position:position + 1] == ONE
    position += 1
    return value, position


def _get_date(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    millis = struct.unpack("<q", data[position:position + 8])[0]
    diff = millis % 1000
    seconds = (millis - diff) / 1000
    position += 8
    if tz_aware:
        dt = EPOCH_AWARE + datetime.timedelta(seconds=seconds)
    else:
        dt = EPOCH_NAIVE + datetime.timedelta(seconds=seconds)
    return dt.replace(microsecond=diff * 1000), position


def _get_code(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    code, position = _get_string(data, position,
                                 as_class, tz_aware, uuid_subtype, compile_re)
    return Code(code), position


def _get_code_w_scope(
        data, position, as_class, tz_aware, uuid_subtype, compile_re):
    _, position = _get_int(data, position)
    code, position = _get_string(data, position,
                                 as_class, tz_aware, uuid_subtype, compile_re)
    scope, position = _get_object(data, position,
                                  as_class, tz_aware, uuid_subtype, compile_re)
    return Code(code, scope), position


def _get_null(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    return None, position


def _get_regex(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    pattern, position = _get_c_string(data, position)
    bson_flags, position = _get_c_string(data, position)
    bson_re = Regex(pattern, bson_flags)
    if compile_re:
        return bson_re.try_compile(), position
    else:
        return bson_re, position


def _get_ref(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    collection, position = _get_string(data, position, as_class, tz_aware,
                                       uuid_subtype, compile_re)
    oid, position = _get_oid(data, position)
    return DBRef(collection, oid), position


def _get_timestamp(
        data, position, as_class, tz_aware, uuid_subtype, compile_re):
    inc, position = _get_int(data, position, unsigned=True)
    timestamp, position = _get_int(data, position, unsigned=True)
    return Timestamp(timestamp, inc), position


def _get_long(data, position, as_class, tz_aware, uuid_subtype, compile_re):
    # Have to cast to long; on 32-bit unpack may return an int.
    # 2to3 will change long to int. That's fine since long doesn't
    # exist in python3.
    value = long(struct.unpack("<q", data[position:position + 8])[0])
    position += 8
    return value, position


_element_getter = {
    BSONNUM: _get_number,
    BSONSTR: _get_string,
    BSONOBJ: _get_object,
    BSONARR: _get_array,
    BSONBIN: _get_binary,
    BSONUND: _get_null,  # undefined
    BSONOID: _get_oid,
    BSONBOO: _get_boolean,
    BSONDAT: _get_date,
    BSONNUL: _get_null,
    BSONRGX: _get_regex,
    BSONREF: _get_ref,
    BSONCOD: _get_code,  # code
    BSONSYM: _get_string,  # symbol
    BSONCWS: _get_code_w_scope,
    BSONINT: _get_int,  # number_int
    BSONTIM: _get_timestamp,
    BSONLON: _get_long, # Same as _get_int after 2to3 runs.
    BSONMIN: lambda u, v, w, x, y, z: (MinKey(), v),
    BSONMAX: lambda u, v, w, x, y, z: (MaxKey(), v)}


def _element_to_dict(
        data, position, as_class, tz_aware, uuid_subtype, compile_re):
    element_type = data[position:position + 1]
    position += 1
    element_name, position = _get_c_string(data, position)
    value, position = _element_getter[element_type](
        data, position, as_class, tz_aware, uuid_subtype, compile_re)

    return element_name, value, position


def _elements_to_dict(data, as_class, tz_aware, uuid_subtype, compile_re):
    result = as_class()
    position = 0
    end = len(data) - 1
    while position < end:
        (key, value, position) = _element_to_dict(
            data, position, as_class, tz_aware, uuid_subtype, compile_re)
        result[key] = value
    return result

def _bson_to_dict(data, as_class, tz_aware, uuid_subtype, compile_re):
    obj_size = struct.unpack("<i", data[:4])[0]
    length = len(data)
    if length < obj_size:
        raise InvalidBSON("objsize too large")
    if obj_size != length or data[obj_size - 1:obj_size] != ZERO:
        raise InvalidBSON("bad eoo")
    elements = data[4:obj_size - 1]
    dct = _elements_to_dict(
        elements, as_class, tz_aware, uuid_subtype, compile_re)

    return dct, data[obj_size:]
if _use_c:
    _bson_to_dict = _cbson._bson_to_dict


def _element_to_bson(key, value, check_keys, uuid_subtype):
    if not isinstance(key, basestring):
        raise InvalidDocument("documents must have only string keys, "
                              "key was %r" % key)

    if check_keys:
        if key.startswith("$"):
            raise InvalidDocument("key %r must not start with '$'" % key)
        if "." in key:
            raise InvalidDocument("key %r must not contain '.'" % key)

    name = _make_c_string(key, True)
    if isinstance(value, float):
        return BSONNUM + name + struct.pack("<d", value)

    if _use_uuid:
        if isinstance(value, uuid.UUID):
            # Java Legacy
            if uuid_subtype == JAVA_LEGACY:
                # Python 3.0(.1) returns a bytearray instance for bytes (3.1
                # and newer just return a bytes instance). Convert that to
                # binary_type (here and below) for compatibility.
                from_uuid = binary_type(value.bytes)
                as_legacy_java = from_uuid[0:8][::-1] + from_uuid[8:16][::-1]
                value = Binary(as_legacy_java, subtype=OLD_UUID_SUBTYPE)
            # C# legacy
            elif uuid_subtype == CSHARP_LEGACY:
                # Microsoft GUID representation.
                value = Binary(binary_type(value.bytes_le),
                               subtype=OLD_UUID_SUBTYPE)
            # Python
            else:
                value = Binary(binary_type(value.bytes), subtype=uuid_subtype)

    if isinstance(value, Binary):
        subtype = value.subtype
        if subtype == 2:
            value = struct.pack("<i", len(value)) + value
        return (BSONBIN + name +
                struct.pack("<i", len(value)) + b(chr(subtype)) + value)
    if isinstance(value, Code):
        cstring = _make_c_string(value)
        if not value.scope:
            length = struct.pack("<i", len(cstring))
            return BSONCOD + name + length + cstring
        scope = _dict_to_bson(value.scope, False, uuid_subtype, False)
        full_length = struct.pack("<i", 8 + len(cstring) + len(scope))
        length = struct.pack("<i", len(cstring))
        return BSONCWS + name + full_length + length + cstring + scope
    if isinstance(value, binary_type):
        if PY3:
            # Python3 special case. Store 'bytes' as BSON binary subtype 0.
            return (BSONBIN + name +
                    struct.pack("<i", len(value)) + ZERO + value)
        cstring = _make_c_string(value)
        length = struct.pack("<i", len(cstring))
        return BSONSTR + name + length + cstring
    if isinstance(value, unicode):
        cstring = _make_c_string(value)
        length = struct.pack("<i", len(cstring))
        return BSONSTR + name + length + cstring
    if isinstance(value, dict):
        return BSONOBJ + name + _dict_to_bson(value, check_keys, uuid_subtype, False)
    if isinstance(value, (list, tuple)):
        as_dict = SON(zip([str(i) for i in range(len(value))], value))
        return BSONARR + name + _dict_to_bson(as_dict, check_keys, uuid_subtype, False)
    if isinstance(value, ObjectId):
        return BSONOID + name + value.binary
    if value is True:
        return BSONBOO + name + ONE
    if value is False:
        return BSONBOO + name + ZERO
    if isinstance(value, int):
        # TODO this is an ugly way to check for this...
        if value > MAX_INT64 or value < MIN_INT64:
            raise OverflowError("BSON can only handle up to 8-byte ints")
        if value > MAX_INT32 or value < MIN_INT32:
            return BSONLON + name + struct.pack("<q", value)
        return BSONINT + name + struct.pack("<i", value)
    # 2to3 will convert long to int here since there is no long in python3.
    # That's OK. The previous if block will match instead.
    if isinstance(value, long):
        if value > MAX_INT64 or value < MIN_INT64:
            raise OverflowError("BSON can only handle up to 8-byte ints")
        return BSONLON + name + struct.pack("<q", value)
    if isinstance(value, datetime.datetime):
        if value.utcoffset() is not None:
            value = value - value.utcoffset()
        millis = int(calendar.timegm(value.timetuple()) * 1000 +
                     value.microsecond / 1000)
        return BSONDAT + name + struct.pack("<q", millis)
    if isinstance(value, Timestamp):
        time = struct.pack("<I", value.time)
        inc = struct.pack("<I", value.inc)
        return BSONTIM + name + inc + time
    if value is None:
        return BSONNUL + name
    if isinstance(value, (RE_TYPE, Regex)):
        pattern = value.pattern
        flags = ""
        if value.flags & re.IGNORECASE:
            flags += "i"
        if value.flags & re.LOCALE:
            flags += "l"
        if value.flags & re.MULTILINE:
            flags += "m"
        if value.flags & re.DOTALL:
            flags += "s"
        if value.flags & re.UNICODE:
            flags += "u"
        if value.flags & re.VERBOSE:
            flags += "x"
        return BSONRGX + name + _make_c_string(pattern, True) + \
            _make_c_string(flags)
    if isinstance(value, DBRef):
        return _element_to_bson(key, value.as_doc(), False, uuid_subtype)
    if isinstance(value, MinKey):
        return BSONMIN + name
    if isinstance(value, MaxKey):
        return BSONMAX + name

    raise InvalidDocument("cannot convert value of type %s to bson" %
                          type(value))


def _dict_to_bson(dict, check_keys, uuid_subtype, top_level=True):
    try:
        elements = []
        if top_level and "_id" in dict:
            elements.append(_element_to_bson("_id", dict["_id"], False, uuid_subtype))
        for (key, value) in dict.iteritems():
            if not top_level or key != "_id":
                elements.append(_element_to_bson(key, value, check_keys, uuid_subtype))
    except AttributeError:
        raise TypeError("encoder expected a mapping type but got: %r" % dict)

    encoded = EMPTY.join(elements)
    length = len(encoded) + 5
    return struct.pack("<i", length) + encoded + ZERO
if _use_c:
    _dict_to_bson = _cbson._dict_to_bson



def decode_all(data, as_class=dict,
               tz_aware=True, uuid_subtype=OLD_UUID_SUBTYPE, compile_re=True):
    """Decode BSON data to multiple documents.

    `data` must be a string of concatenated, valid, BSON-encoded
    documents.

    :Parameters:
      - `data`: BSON data
      - `as_class` (optional): the class to use for the resulting
        documents
      - `tz_aware` (optional): if ``True``, return timezone-aware
        :class:`~datetime.datetime` instances
      - `compile_re` (optional): if ``False``, don't attempt to compile
        BSON regular expressions into Python regular expressions. Return
        instances of :class:`~bson.regex.Regex` instead. Can avoid
        :exc:`~bson.errors.InvalidBSON` errors when receiving
        Python-incompatible regular expressions, for example from ``currentOp``

    .. versionchanged:: 2.7
       Added `compile_re` option.
    .. versionadded:: 1.9
    """
    docs = []
    position = 0
    end = len(data) - 1
    try:
        while position < end:
            obj_size = struct.unpack("<i", data[position:position + 4])[0]
            if len(data) - position < obj_size:
                raise InvalidBSON("objsize too large")
            if data[position + obj_size - 1:position + obj_size] != ZERO:
                raise InvalidBSON("bad eoo")
            elements = data[position + 4:position + obj_size - 1]
            position += obj_size
            docs.append(_elements_to_dict(elements, as_class,
                                          tz_aware, uuid_subtype, compile_re))
        return docs
    except InvalidBSON:
        raise
    except Exception:
        # Change exception type to InvalidBSON but preserve traceback.
        exc_type, exc_value, exc_tb = sys.exc_info()
        raise InvalidBSON, str(exc_value), exc_tb
if _use_c:
    decode_all = _cbson.decode_all


def is_valid(bson):
    """Check that the given string represents valid :class:`BSON` data.

    Raises :class:`TypeError` if `bson` is not an instance of
    :class:`str` (:class:`bytes` in python 3). Returns ``True``
    if `bson` is valid :class:`BSON`, ``False`` otherwise.

    :Parameters:
      - `bson`: the data to be validated
    """
    if not isinstance(bson, binary_type):
        raise TypeError("BSON data must be an instance "
                        "of a subclass of %s" % (binary_type.__name__,))

    try:
        (_, remainder) = _bson_to_dict(bson, dict, True, OLD_UUID_SUBTYPE, True)
        return remainder == EMPTY
    except:
        return False


class BSON(binary_type):
    """BSON (Binary JSON) data.
    """

    @classmethod
    def encode(cls, document, check_keys=False, uuid_subtype=OLD_UUID_SUBTYPE):
        """Encode a document to a new :class:`BSON` instance.

        A document can be any mapping type (like :class:`dict`).

        Raises :class:`TypeError` if `document` is not a mapping type,
        or contains keys that are not instances of
        :class:`basestring` (:class:`str` in python 3). Raises
        :class:`~bson.errors.InvalidDocument` if `document` cannot be
        converted to :class:`BSON`.

        :Parameters:
          - `document`: mapping type representing a document
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising :class:`~bson.errors.InvalidDocument` in
            either case

        .. versionadded:: 1.9
        """
        return cls(_dict_to_bson(document, check_keys, uuid_subtype))

    def decode(self, as_class=dict,
               tz_aware=False, uuid_subtype=OLD_UUID_SUBTYPE, compile_re=True):
        """Decode this BSON data.

        The default type to use for the resultant document is
        :class:`dict`. Any other class that supports
        :meth:`__setitem__` can be used instead by passing it as the
        `as_class` parameter.

        If `tz_aware` is ``True`` (recommended), any
        :class:`~datetime.datetime` instances returned will be
        timezone-aware, with their timezone set to
        :attr:`bson.tz_util.utc`. Otherwise (default), all
        :class:`~datetime.datetime` instances will be naive (but
        contain UTC).

        :Parameters:
          - `as_class` (optional): the class to use for the resulting
            document
          - `tz_aware` (optional): if ``True``, return timezone-aware
            :class:`~datetime.datetime` instances
          - `compile_re` (optional): if ``False``, don't attempt to compile
            BSON regular expressions into Python regular expressions. Return
            instances of
            :class:`~bson.regex.Regex` instead. Can avoid
            :exc:`~bson.errors.InvalidBSON` errors when receiving
            Python-incompatible regular expressions, for example from
            ``currentOp``

        .. versionchanged:: 2.7
           Added ``compile_re`` option.
        .. versionadded:: 1.9
        """
        (document, _) = _bson_to_dict(
            self, as_class, tz_aware, uuid_subtype, compile_re)

        return document


def has_c():
    """Is the C extension installed?

    .. versionadded:: 1.9
    """
    return _use_c


def has_uuid():
    """Is the uuid module available?

    .. versionadded:: 2.3
    """
    return _use_uuid
