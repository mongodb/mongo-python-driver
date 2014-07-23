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

"""BSON (Binary JSON) encoding and decoding.
"""

import calendar
import collections
import datetime
import itertools
import re
import struct
import sys
import uuid

from bson.binary import (Binary, OLD_UUID_SUBTYPE,
                         JAVA_LEGACY, CSHARP_LEGACY)
from bson.bsonint64 import BSONInt64
from bson.code import Code
from bson.dbref import DBRef
from bson.errors import (InvalidBSON,
                         InvalidDocument,
                         InvalidStringData)
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.py3compat import (b,
                            PY3,
                            iteritems,
                            text_type,
                            string_type,
                            reraise)
from bson.regex import Regex
from bson.son import SON, RE_TYPE
from bson.timestamp import Timestamp
from bson.tz_util import utc


try:
    from bson import _cbson
    _use_c = True
except ImportError:
    _use_c = False


if PY3:
    long = int

MAX_INT32 = 2147483647
MIN_INT32 = -2147483648
MAX_INT64 = 9223372036854775807
MIN_INT64 = -9223372036854775808

EPOCH_AWARE = datetime.datetime.fromtimestamp(0, utc)
EPOCH_NAIVE = datetime.datetime.utcfromtimestamp(0)

EMPTY = b""
ZERO  = b"\x00"
ONE   = b"\x01"

BSONNUM = b"\x01" # Floating point
BSONSTR = b"\x02" # UTF-8 string
BSONOBJ = b"\x03" # Embedded document
BSONARR = b"\x04" # Array
BSONBIN = b"\x05" # Binary
BSONUND = b"\x06" # Undefined
BSONOID = b"\x07" # ObjectId
BSONBOO = b"\x08" # Boolean
BSONDAT = b"\x09" # UTC Datetime
BSONNUL = b"\x0A" # Null
BSONRGX = b"\x0B" # Regex
BSONREF = b"\x0C" # DBRef
BSONCOD = b"\x0D" # Javascript code
BSONSYM = b"\x0E" # Symbol
BSONCWS = b"\x0F" # Javascript code with scope
BSONINT = b"\x10" # 32bit int
BSONTIM = b"\x11" # Timestamp
BSONLON = b"\x12" # 64bit int
BSONMIN = b"\xFF" # Min key
BSONMAX = b"\x7F" # Max key


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

def _make_c_string_check(string):
    """Make a 'C' string, checking for embedded NUL characters."""
    if isinstance(string, bytes):
        if b"\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NUL character")
        try:
            string.decode("utf-8")
            return string + b"\x00"
        except UnicodeError:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % string)
    else:
        if "\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NUL character")
        return string.encode("utf-8") + b"\x00"

def _make_c_string(string):
    """Make a 'C' string."""
    if isinstance(string, bytes):
        try:
            string.decode("utf-8")
            return string + b"\x00"
        except UnicodeError:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % string)
    else:
        return string.encode("utf-8") + b"\x00"

if PY3:
    def _make_name(string):
        """Make a 'C' string suitable for a BSON key."""
        # Keys can only be text in python 3.
        if "\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NUL character")
        return string.encode("utf-8") + b"\x00"
else:
    # Keys can be unicode or bytes in python 2.
    _make_name = _make_c_string_check


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
    if subtype in (3, 4):
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
    # Have to cast to long (for python 2.x); on 32-bit unpack may return
    # an int.
    value = BSONInt64(struct.unpack("<q", data[position:position + 8])[0])
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
    BSONLON: _get_long,
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


_PACK_FLOAT = struct.Struct("<d").pack
_PACK_INT = struct.Struct("<i").pack
_PACK_INT_SUB = struct.Struct("<iB").pack
_PACK_LONG = struct.Struct("<q").pack
_PACK_INC_TIME = struct.Struct("<II").pack
_LIST_NAMES = tuple(b(str(i)) + b"\x00" for i in range(1000))


def gen_list_name():
    """Generate "keys" for encoded lists in the sequence
    b"0\x00", b"1\x00", b"2\x00", ...

    The first 1000 keys are returned from a pre-built cache. All
    subsequent keys are generated on the fly.
    """
    for name in _LIST_NAMES:
        yield name

    counter = itertools.count(1000)
    while True:
        yield b(str(next(counter))) + b"\x00"


def _encode_float(name, value, dummy0, dummy1):
    """Encode a float."""
    return b"\x01" + name + _PACK_FLOAT(value)


if PY3:
    def _encode_bytes(name, value, dummy0, dummy1):
        """Encode a python bytes."""
        # Python3 special case. Store 'bytes' as BSON binary subtype 0.
        return b"\x05" + name + _PACK_INT(len(value)) + b"\x00" + value
else:
    def _encode_bytes(name, value, dummy0, dummy1):
        """Encode a python str (python 2.x)."""
        try:
            value.decode("utf-8")
        except UnicodeError:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % (value,))
        return b"\x02" + name + _PACK_INT(len(value) + 1) + value + b"\x00"


def _encode_dbref(name, value, check_keys, uuid_subtype):
    """Encode bson.dbref.DBRef."""
    buf = bytearray(b"\x03" + name + b"\x00\x00\x00\x00")
    begin = len(buf) - 4

    buf += _name_value_to_bson(b"$ref\x00",
                               value.collection, check_keys, uuid_subtype)
    buf += _name_value_to_bson(b"$id\x00",
                               value.id, check_keys, uuid_subtype)
    if value.database is not None:
        buf += _name_value_to_bson(
            b"$db\x00", value.database, check_keys, uuid_subtype)
    for key, val in iteritems(value._DBRef__kwargs):
        buf += _element_to_bson(key, val, check_keys, uuid_subtype)

    buf += b"\x00"
    buf[begin:begin + 4] = _PACK_INT(len(buf) - begin)
    return bytes(buf)


def _encode_list(name, value, check_keys, uuid_subtype):
    """Encode a list/tuple."""
    lname = gen_list_name()
    data = b"".join([_name_value_to_bson(next(lname), item,
                                         check_keys, uuid_subtype)
                     for item in value])
    return b"\x04" + name + _PACK_INT(len(data) + 5) + data + b"\x00"


def _encode_text(name, value, dummy0, dummy1):
    """Encode a python unicode (python 2.x) / str (python 3.x)."""
    value = value.encode("utf-8")
    return b"\x02" + name + _PACK_INT(len(value) + 1) + value + b"\x00"


def _encode_binary(name, value, dummy0, dummy1):
    """Encode bson.binary.Binary."""
    subtype = value.subtype
    if subtype == 2:
        value = _PACK_INT(len(value)) + value
    return b"\x05" + name + _PACK_INT_SUB(len(value), subtype) + value


def _encode_uuid(name, value, dummy, uuid_subtype):
    """Encode uuid.UUID."""
    # Python Legacy Common Case
    if uuid_subtype == OLD_UUID_SUBTYPE:
        return b"\x05" + name + b'\x10\x00\x00\x00\x03' + value.bytes
    # Java Legacy
    elif uuid_subtype == JAVA_LEGACY:
        from_uuid = value.bytes
        data = from_uuid[0:8][::-1] + from_uuid[8:16][::-1]
        return b"\x05" + name + b'\x10\x00\x00\x00\x03' + data
    # C# legacy
    elif uuid_subtype == CSHARP_LEGACY:
        # Microsoft GUID representation.
        return b"\x05" + name + b'\x10\x00\x00\x00\x03' + value.bytes_le
    # New
    else:
        return b"\x05" + name + b'\x10\x00\x00\x00\x04' + value.bytes


def _encode_objectid(name, value, dummy0, dummy1):
    """Encode bson.objectid.ObjectId."""
    return b"\x07" + name + value.binary


def _encode_bool(name, value, dummy0, dummy1):
    """Encode a python boolean (True/False)."""
    return b"\x08" + name + (value and b"\x01" or b"\x00")


def _encode_datetime(name, value, dummy0, dummy1):
    """Encode datetime.datetime."""
    if value.utcoffset() is not None:
        value = value - value.utcoffset()
    millis = int(calendar.timegm(value.timetuple()) * 1000 +
                 value.microsecond / 1000)
    return b"\x09" + name + _PACK_LONG(millis)


def _encode_none(name, dummy0, dummy1, dummy2):
    """Encode python None."""
    return b"\x0A" + name


def _encode_regex(name, value, dummy0, dummy1):
    """Encode a python regex or bson.regex.Regex."""
    flags = value.flags
    # Python 2 common case
    if flags == 0:
        return b"\x0B" + name + _make_c_string_check(value.pattern) + b"\x00"
    # Python 3 common case
    elif flags == re.UNICODE:
        return b"\x0B" + name + _make_c_string_check(value.pattern) + b"u\x00"
    else:
        sflags = b""
        if flags & re.IGNORECASE:
            sflags += b"i"
        if flags & re.LOCALE:
            sflags += b"l"
        if flags & re.MULTILINE:
            sflags += b"m"
        if flags & re.DOTALL:
            sflags += b"s"
        if flags & re.UNICODE:
            sflags += b"u"
        if flags & re.VERBOSE:
            sflags += b"x"
        sflags += b"\x00"
        return b"\x0B" + name + _make_c_string_check(value.pattern) + sflags


def _encode_code(name, value, dummy, uuid_subtype):
    """Encode bson.code.Code."""
    cstring = _make_c_string(value)
    cstrlen = len(cstring)
    if not value.scope:
        return b"\x0D" + name + _PACK_INT(cstrlen) + cstring
    scope = _dict_to_bson(value.scope, False, uuid_subtype, False)
    full_length = _PACK_INT(8 + cstrlen + len(scope))
    return b"\x0F" + name + full_length + _PACK_INT(cstrlen) + cstring + scope


def _encode_int(name, value, dummy0, dummy1):
    """Encode a python int."""
    if -2147483648 <= value <= 2147483647:
        return b"\x10" + name + _PACK_INT(value)
    else:
        try:
            return b"\x12" + name + _PACK_LONG(value)
        except struct.error:
            raise OverflowError("BSON can only handle up to 8-byte ints")


def _encode_timestamp(name, value, dummy0, dummy1):
    """Encode bson.timestamp.Timestamp."""
    return b"\x11" + name + _PACK_INC_TIME(value.inc, value.time)


def _encode_long(name, value, dummy0, dummy1):
    """Encode a python long (python 2.x)"""
    try:
        return b"\x12" + name + _PACK_LONG(value)
    except struct.error:
        raise OverflowError("BSON can only handle up to 8-byte ints")


def _encode_minkey(name, dummy0, dummy1, dummy2):
    """Encode bson.min_key.MinKey."""
    return b"\xFF" + name


def _encode_maxkey(name, dummy0, dummy1, dummy2):
    """Encode bson.max_key.MaxKey."""
    return b"\x7F" + name


_ENCODERS = {
    bool: _encode_bool,
    bytes: _encode_bytes,
    datetime.datetime: _encode_datetime,
    float: _encode_float,
    int: _encode_int,
    list: _encode_list,
    tuple: _encode_list,
    type(None): _encode_none,
    RE_TYPE: _encode_regex,
    text_type: _encode_text,
    uuid.UUID: _encode_uuid
}


_MARKERS = {
    5: _encode_binary,
    7: _encode_objectid,
    11: _encode_regex,
    13: _encode_code,
    17: _encode_timestamp,
    18: _encode_long,
    100: _encode_dbref,
    127: _encode_maxkey,
    255: _encode_minkey,
}

if not PY3:
    _ENCODERS[long] = _encode_long


def _name_value_to_bson(name, value, check_keys, uuid_subtype):
    """Encode a single name, value pair."""
    func = _ENCODERS.get(type(value))
    if func:
        return func(name, value, check_keys, uuid_subtype)
    marker = getattr(value, "_type_marker", None)
    if isinstance(marker, int) and marker in _MARKERS:
        return _MARKERS.get(marker)(name, value, check_keys, uuid_subtype)

    # Assume dict is the most likely type to be subclassed.
    if isinstance(value, collections.Mapping):
        data = b"".join([_element_to_bson(key, val, check_keys, uuid_subtype)
                         for key, val in iteritems(value)])
        return b"\x03" + name + _PACK_INT(len(data) + 5) + data + b"\x00"

    for base in _ENCODERS:
        if isinstance(value, base):
            return _ENCODERS[base](name, value, check_keys, uuid_subtype)

    raise InvalidDocument("cannot convert value of type %s to bson" %
                          type(value))


def _element_to_bson(key, value, check_keys, uuid_subtype):
    """Encode a single key, value pair."""
    if not isinstance(key, string_type):
        raise InvalidDocument("documents must have only string keys, "
                              "key was %r" % (key,))
    if check_keys:
        if key.startswith("$"):
            raise InvalidDocument("key %r must not start with '$'" % (key,))
        if "." in key:
            raise InvalidDocument("key %r must not contain '.'" % (key,))

    name = _make_name(key)
    return _name_value_to_bson(name, value, check_keys, uuid_subtype)


def _dict_to_bson(doc, check_keys, uuid_subtype, top_level=True):
    """Encode a document to BSON."""
    try:
        elements = []
        if top_level and "_id" in doc:
            elements.append(_name_value_to_bson(b"_id\x00", doc["_id"],
                                                check_keys, uuid_subtype))
        for (key, value) in iteritems(doc):
            if not top_level or key != "_id":
                elements.append(_element_to_bson(key, value,
                                                 check_keys, uuid_subtype))
    except AttributeError:
        raise TypeError("encoder expected a mapping type but got: %r" % (doc,))

    encoded = b"".join(elements)
    return _PACK_INT(len(encoded) + 5) + encoded + b"\x00"
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
        reraise(InvalidBSON, exc_value, exc_tb)


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
    if not isinstance(bson, bytes):
        raise TypeError("BSON data must be an instance of a subclass of bytes")

    try:
        (_, remainder) = _bson_to_dict(bson, dict, True, OLD_UUID_SUBTYPE, True)
        return remainder == EMPTY
    except:
        return False


class BSON(bytes):
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
