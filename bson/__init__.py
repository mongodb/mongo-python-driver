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

from codecs import (utf_8_decode as _utf_8_decode,
                    utf_8_encode as _utf_8_encode)

from bson.binary import (Binary, OLD_UUID_SUBTYPE,
                         JAVA_LEGACY, CSHARP_LEGACY,
                         UUIDLegacy)
from bson.code import Code
from bson.codec_options import CodecOptions, DEFAULT_CODEC_OPTIONS
from bson.dbref import DBRef
from bson.errors import (InvalidBSON,
                         InvalidDocument,
                         InvalidStringData)
from bson.int64 import Int64
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
    _USE_C = True
except ImportError:
    _USE_C = False


EPOCH_AWARE = datetime.datetime.fromtimestamp(0, utc)
EPOCH_NAIVE = datetime.datetime.utcfromtimestamp(0)


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


_UNPACK_FLOAT = struct.Struct("<d").unpack
_UNPACK_INT = struct.Struct("<i").unpack
_UNPACK_LENGTH_SUBTYPE = struct.Struct("<iB").unpack
_UNPACK_LONG = struct.Struct("<q").unpack
_UNPACK_TIMESTAMP = struct.Struct("<II").unpack


def _get_int(data, position, dummy0, dummy1):
    """Decode a BSON int32 to python int."""
    end = position + 4
    return _UNPACK_INT(data[position:end])[0], end


def _get_c_string(data, position, opts):
    """Decode a BSON 'C' string to python unicode string."""
    end = data.index(b"\x00", position)
    return _utf_8_decode(data[position:end],
                         opts.unicode_decode_error_handler, True)[0], end + 1


def _get_float(data, position, dummy0, dummy1):
    """Decode a BSON double to python float."""
    end = position + 8
    return _UNPACK_FLOAT(data[position:end])[0], end


def _get_string(data, position, obj_end, opts):
    """Decode a BSON string to python unicode string."""
    length = _UNPACK_INT(data[position:position + 4])[0]
    position += 4
    if length < 1 or obj_end - position < length:
        raise InvalidBSON("invalid string length")
    end = position + length - 1
    if data[end:end + 1] != b"\x00":
        raise InvalidBSON("invalid end of string")
    return _utf_8_decode(data[position:end],
                         opts.unicode_decode_error_handler, True)[0], end + 1


def _get_object(data, position, obj_end, opts):
    """Decode a BSON subdocument to opts.document_class or bson.dbref.DBRef."""
    obj_size = _UNPACK_INT(data[position:position + 4])[0]
    end = position + obj_size - 1
    if data[end:position + obj_size] != b"\x00":
        raise InvalidBSON("bad eoo")
    if end >= obj_end:
        raise InvalidBSON("invalid object length")
    obj = _elements_to_dict(data, position + 4, end, opts)

    position += obj_size
    if "$ref" in obj:
        return (DBRef(obj.pop("$ref"), obj.pop("$id", None),
                      obj.pop("$db", None), obj), position)
    return obj, position


def _get_array(data, position, obj_end, opts):
    """Decode a BSON array to python list."""
    size = _UNPACK_INT(data[position:position + 4])[0]
    end = position + size - 1
    if data[end:end + 1] != b"\x00":
        raise InvalidBSON("bad eoo")
    position += 4
    end -= 1
    result = []

    # Avoid doing global and attibute lookups in the loop.
    append = result.append
    index = data.index
    getter = _ELEMENT_GETTER

    while position < end:
        element_type = data[position:position + 1]
        # Just skip the keys.
        position = index(b'\x00', position) + 1
        value, position = getter[element_type](data, position, obj_end, opts)
        append(value)
    return result, position + 1


def _get_binary(data, position, dummy, opts):
    """Decode a BSON binary to bson.binary.Binary or python UUID."""
    length, subtype = _UNPACK_LENGTH_SUBTYPE(data[position:position + 5])
    position += 5
    if subtype == 2:
        length2 = _UNPACK_INT(data[position:position + 4])[0]
        position += 4
        if length2 != length - 4:
            raise InvalidBSON("invalid binary (st 2) - lengths don't match!")
        length = length2
    end = position + length
    if subtype in (3, 4):
        # Java Legacy
        uuid_representation = opts.uuid_representation
        if uuid_representation == JAVA_LEGACY:
            java = data[position:end]
            value = uuid.UUID(bytes=java[0:8][::-1] + java[8:16][::-1])
        # C# legacy
        elif uuid_representation == CSHARP_LEGACY:
            value = uuid.UUID(bytes_le=data[position:end])
        # Python
        else:
            value = uuid.UUID(bytes=data[position:end])
        return value, end
    # Python3 special case. Decode subtype 0 to 'bytes'.
    if PY3 and subtype == 0:
        value = data[position:end]
    else:
        value = Binary(data[position:end], subtype)
    return value, end


def _get_oid(data, position, dummy0, dummy1):
    """Decode a BSON ObjectId to bson.objectid.ObjectId."""
    end = position + 12
    return ObjectId(data[position:end]), end


def _get_boolean(data, position, dummy0, dummy1):
    """Decode a BSON true/false to python True/False."""
    end = position + 1
    return data[position:end] == b"\x01", end


def _get_date(data, position, dummy, opts):
    """Decode a BSON datetime to python datetime.datetime."""
    end = position + 8
    millis = _UNPACK_LONG(data[position:end])[0]
    diff = ((millis % 1000) + 1000) % 1000
    seconds = (millis - diff) / 1000
    micros = diff * 1000
    if opts.tz_aware:
        dt = EPOCH_AWARE + datetime.timedelta(
            seconds=seconds, microseconds=micros)
        if opts.tzinfo:
            dt = dt.astimezone(opts.tzinfo)
    else:
        dt = EPOCH_NAIVE + datetime.timedelta(
            seconds=seconds, microseconds=micros)
    return dt, end


def _get_code(data, position, obj_end, opts):
    """Decode a BSON code to bson.code.Code."""
    code, position = _get_string(data, position, obj_end, opts)
    return Code(code), position


def _get_code_w_scope(data, position, obj_end, opts):
    """Decode a BSON code_w_scope to bson.code.Code."""
    code, position = _get_string(data, position + 4, obj_end, opts)
    scope, position = _get_object(data, position, obj_end, opts)
    return Code(code, scope), position


def _get_regex(data, position, dummy0, opts):
    """Decode a BSON regex to bson.regex.Regex or a python pattern object."""
    pattern, position = _get_c_string(data, position, opts)
    bson_flags, position = _get_c_string(data, position, opts)
    bson_re = Regex(pattern, bson_flags)
    return bson_re, position


def _get_ref(data, position, obj_end, opts):
    """Decode (deprecated) BSON DBPointer to bson.dbref.DBRef."""
    collection, position = _get_string(data, position, obj_end, opts)
    oid, position = _get_oid(data, position, obj_end, opts)
    return DBRef(collection, oid), position


def _get_timestamp(data, position, dummy0, dummy1):
    """Decode a BSON timestamp to bson.timestamp.Timestamp."""
    end = position + 8
    inc, timestamp = _UNPACK_TIMESTAMP(data[position:end])
    return Timestamp(timestamp, inc), end


def _get_int64(data, position, dummy0, dummy1):
    """Decode a BSON int64 to bson.int64.Int64."""
    end = position + 8
    return Int64(_UNPACK_LONG(data[position:end])[0]), end


# Each decoder function's signature is:
#   - data: bytes
#   - position: int, beginning of object in 'data' to decode
#   - obj_end: int, end of object to decode in 'data' if variable-length type
#   - opts: a CodecOptions
_ELEMENT_GETTER = {
    BSONNUM: _get_float,
    BSONSTR: _get_string,
    BSONOBJ: _get_object,
    BSONARR: _get_array,
    BSONBIN: _get_binary,
    BSONUND: lambda w, x, y, z: (None, x),  # Deprecated undefined
    BSONOID: _get_oid,
    BSONBOO: _get_boolean,
    BSONDAT: _get_date,
    BSONNUL: lambda w, x, y, z: (None, x),
    BSONRGX: _get_regex,
    BSONREF: _get_ref,  # Deprecated DBPointer
    BSONCOD: _get_code,
    BSONSYM: _get_string,  # Deprecated symbol
    BSONCWS: _get_code_w_scope,
    BSONINT: _get_int,
    BSONTIM: _get_timestamp,
    BSONLON: _get_int64,
    BSONMIN: lambda w, x, y, z: (MinKey(), x),
    BSONMAX: lambda w, x, y, z: (MaxKey(), x)}


def _element_to_dict(data, position, obj_end, opts):
    """Decode a single key, value pair."""
    element_type = data[position:position + 1]
    position += 1
    element_name, position = _get_c_string(data, position, opts)
    value, position = _ELEMENT_GETTER[element_type](data,
                                                    position, obj_end, opts)
    return element_name, value, position


def _elements_to_dict(data, position, obj_end, opts):
    """Decode a BSON document."""
    result = opts.document_class()
    end = obj_end - 1
    while position < end:
        (key, value, position) = _element_to_dict(data, position, obj_end, opts)
        result[key] = value
    return result


def _bson_to_dict(data, opts):
    """Decode a BSON string to document_class."""
    try:
        obj_size = _UNPACK_INT(data[:4])[0]
    except struct.error as exc:
        raise InvalidBSON(str(exc))
    if obj_size != len(data):
        raise InvalidBSON("invalid object size")
    if data[obj_size - 1:obj_size] != b"\x00":
        raise InvalidBSON("bad eoo")
    try:
        return _elements_to_dict(data, 4, obj_size - 1, opts)
    except InvalidBSON:
        raise
    except Exception:
        # Change exception type to InvalidBSON but preserve traceback.
        _, exc_value, exc_tb = sys.exc_info()
        reraise(InvalidBSON, exc_value, exc_tb)
if _USE_C:
    _bson_to_dict = _cbson._bson_to_dict


_PACK_FLOAT = struct.Struct("<d").pack
_PACK_INT = struct.Struct("<i").pack
_PACK_LENGTH_SUBTYPE = struct.Struct("<iB").pack
_PACK_LONG = struct.Struct("<q").pack
_PACK_TIMESTAMP = struct.Struct("<II").pack
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


def _make_c_string_check(string):
    """Make a 'C' string, checking for embedded NUL characters."""
    if isinstance(string, bytes):
        if b"\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NUL character")
        try:
            _utf_8_decode(string, None, True)
            return string + b"\x00"
        except UnicodeError:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % string)
    else:
        if "\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NUL character")
        return _utf_8_encode(string)[0] + b"\x00"


def _make_c_string(string):
    """Make a 'C' string."""
    if isinstance(string, bytes):
        try:
            _utf_8_decode(string, None, True)
            return string + b"\x00"
        except UnicodeError:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % string)
    else:
        return _utf_8_encode(string)[0] + b"\x00"


if PY3:
    def _make_name(string):
        """Make a 'C' string suitable for a BSON key."""
        # Keys can only be text in python 3.
        if "\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NUL character")
        return _utf_8_encode(string)[0] + b"\x00"
else:
    # Keys can be unicode or bytes in python 2.
    _make_name = _make_c_string_check


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
            _utf_8_decode(value, None, True)
        except UnicodeError:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % (value,))
        return b"\x02" + name + _PACK_INT(len(value) + 1) + value + b"\x00"


def _encode_mapping(name, value, check_keys, opts):
    """Encode a mapping type."""
    data = b"".join([_element_to_bson(key, val, check_keys, opts)
                     for key, val in iteritems(value)])
    return b"\x03" + name + _PACK_INT(len(data) + 5) + data + b"\x00"


def _encode_dbref(name, value, check_keys, opts):
    """Encode bson.dbref.DBRef."""
    buf = bytearray(b"\x03" + name + b"\x00\x00\x00\x00")
    begin = len(buf) - 4

    buf += _name_value_to_bson(b"$ref\x00",
                               value.collection, check_keys, opts)
    buf += _name_value_to_bson(b"$id\x00",
                               value.id, check_keys, opts)
    if value.database is not None:
        buf += _name_value_to_bson(
            b"$db\x00", value.database, check_keys, opts)
    for key, val in iteritems(value._DBRef__kwargs):
        buf += _element_to_bson(key, val, check_keys, opts)

    buf += b"\x00"
    buf[begin:begin + 4] = _PACK_INT(len(buf) - begin)
    return bytes(buf)


def _encode_list(name, value, check_keys, opts):
    """Encode a list/tuple."""
    lname = gen_list_name()
    data = b"".join([_name_value_to_bson(next(lname), item,
                                         check_keys, opts)
                     for item in value])
    return b"\x04" + name + _PACK_INT(len(data) + 5) + data + b"\x00"


def _encode_text(name, value, dummy0, dummy1):
    """Encode a python unicode (python 2.x) / str (python 3.x)."""
    value = _utf_8_encode(value)[0]
    return b"\x02" + name + _PACK_INT(len(value) + 1) + value + b"\x00"


def _encode_binary(name, value, dummy0, dummy1):
    """Encode bson.binary.Binary."""
    subtype = value.subtype
    if subtype == 2:
        value = _PACK_INT(len(value)) + value
    return b"\x05" + name + _PACK_LENGTH_SUBTYPE(len(value), subtype) + value


def _encode_uuid(name, value, dummy, opts):
    """Encode uuid.UUID."""
    uuid_representation = opts.uuid_representation
    # Python Legacy Common Case
    if uuid_representation == OLD_UUID_SUBTYPE:
        return b"\x05" + name + b'\x10\x00\x00\x00\x03' + value.bytes
    # Java Legacy
    elif uuid_representation == JAVA_LEGACY:
        from_uuid = value.bytes
        data = from_uuid[0:8][::-1] + from_uuid[8:16][::-1]
        return b"\x05" + name + b'\x10\x00\x00\x00\x03' + data
    # C# legacy
    elif uuid_representation == CSHARP_LEGACY:
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


def _encode_code(name, value, dummy, opts):
    """Encode bson.code.Code."""
    cstring = _make_c_string(value)
    cstrlen = len(cstring)
    if not value.scope:
        return b"\x0D" + name + _PACK_INT(cstrlen) + cstring
    scope = _dict_to_bson(value.scope, False, opts, False)
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
    return b"\x11" + name + _PACK_TIMESTAMP(value.inc, value.time)


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


# Each encoder function's signature is:
#   - name: utf-8 bytes
#   - value: a Python data type, e.g. a Python int for _encode_int
#   - check_keys: bool, whether to check for invalid names
#   - opts: a CodecOptions
_ENCODERS = {
    bool: _encode_bool,
    bytes: _encode_bytes,
    datetime.datetime: _encode_datetime,
    dict: _encode_mapping,
    float: _encode_float,
    int: _encode_int,
    list: _encode_list,
    # unicode in py2, str in py3
    text_type: _encode_text,
    tuple: _encode_list,
    type(None): _encode_none,
    uuid.UUID: _encode_uuid,
    Binary: _encode_binary,
    Int64: _encode_long,
    Code: _encode_code,
    DBRef: _encode_dbref,
    MaxKey: _encode_maxkey,
    MinKey: _encode_minkey,
    ObjectId: _encode_objectid,
    Regex: _encode_regex,
    RE_TYPE: _encode_regex,
    SON: _encode_mapping,
    Timestamp: _encode_timestamp,
    UUIDLegacy: _encode_binary,
    # Special case. This will never be looked up directly.
    collections.Mapping: _encode_mapping,
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


def _name_value_to_bson(name, value, check_keys, opts):
    """Encode a single name, value pair."""

    # First see if the type is already cached. KeyError will only ever
    # happen once per subtype.
    try:
        return _ENCODERS[type(value)](name, value, check_keys, opts)
    except KeyError:
        pass

    # Second, fall back to trying _type_marker. This has to be done
    # before the loop below since users could subclass one of our
    # custom types that subclasses a python built-in (e.g. Binary)
    marker = getattr(value, "_type_marker", None)
    if isinstance(marker, int) and marker in _MARKERS:
        func = _MARKERS[marker]
        # Cache this type for faster subsequent lookup.
        _ENCODERS[type(value)] = func
        return func(name, value, check_keys, opts)

    # If all else fails test each base type. This will only happen once for
    # a subtype of a supported base type.
    for base in _ENCODERS:
        if isinstance(value, base):
            func = _ENCODERS[base]
            # Cache this type for faster subsequent lookup.
            _ENCODERS[type(value)] = func
            return func(name, value, check_keys, opts)

    raise InvalidDocument("cannot convert value of type %s to bson" %
                          type(value))


def _element_to_bson(key, value, check_keys, opts):
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
    return _name_value_to_bson(name, value, check_keys, opts)


def _dict_to_bson(doc, check_keys, opts, top_level=True):
    """Encode a document to BSON."""
    try:
        elements = []
        if top_level and "_id" in doc:
            elements.append(_name_value_to_bson(b"_id\x00", doc["_id"],
                                                check_keys, opts))
        for (key, value) in iteritems(doc):
            if not top_level or key != "_id":
                elements.append(_element_to_bson(key, value,
                                                 check_keys, opts))
    except AttributeError:
        raise TypeError("encoder expected a mapping type but got: %r" % (doc,))

    encoded = b"".join(elements)
    return _PACK_INT(len(encoded) + 5) + encoded + b"\x00"
if _USE_C:
    _dict_to_bson = _cbson._dict_to_bson


_CODEC_OPTIONS_TYPE_ERROR = TypeError(
    "codec_options must be an instance of CodecOptions")


def decode_all(data, codec_options=DEFAULT_CODEC_OPTIONS):
    """Decode BSON data to multiple documents.

    `data` must be a string of concatenated, valid, BSON-encoded
    documents.

    :Parameters:
      - `data`: BSON data
      - `codec_options` (optional): An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionchanged:: 3.0
       Removed `compile_re` option: PyMongo now always represents BSON regular
       expressions as :class:`~bson.regex.Regex` objects. Use
       :meth:`~bson.regex.Regex.try_compile` to attempt to convert from a
       BSON regular expression to a Python regular expression object.

       Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
       `codec_options`.

    .. versionchanged:: 2.7
       Added `compile_re` option. If set to False, PyMongo represented BSON
       regular expressions as :class:`~bson.regex.Regex` objects instead of
       attempting to compile BSON regular expressions as Python native
       regular expressions, thus preventing errors for some incompatible
       patterns, see `PYTHON-500`_.

    .. _PYTHON-500: https://jira.mongodb.org/browse/PYTHON-500
    """
    if not isinstance(codec_options, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    docs = []
    position = 0
    end = len(data) - 1
    try:
        while position < end:
            obj_size = _UNPACK_INT(data[position:position + 4])[0]
            if len(data) - position < obj_size:
                raise InvalidBSON("invalid object size")
            obj_end = position + obj_size - 1
            if data[obj_end:position + obj_size] != b"\x00":
                raise InvalidBSON("bad eoo")
            docs.append(_elements_to_dict(data,
                                          position + 4,
                                          obj_end,
                                          codec_options))
            position += obj_size
        return docs
    except InvalidBSON:
        raise
    except Exception:
        # Change exception type to InvalidBSON but preserve traceback.
        _, exc_value, exc_tb = sys.exc_info()
        reraise(InvalidBSON, exc_value, exc_tb)


if _USE_C:
    decode_all = _cbson.decode_all


def decode_iter(data, codec_options=DEFAULT_CODEC_OPTIONS):
    """Decode BSON data to multiple documents as a generator.

    Works similarly to the decode_all function, but yields one document at a
    time.

    `data` must be a string of concatenated, valid, BSON-encoded
    documents.

    :Parameters:
      - `data`: BSON data
      - `codec_options` (optional): An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionchanged:: 3.0
       Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
       `codec_options`.

    .. versionadded:: 2.8
    """
    if not isinstance(codec_options, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    position = 0
    end = len(data) - 1
    while position < end:
        obj_size = _UNPACK_INT(data[position:position + 4])[0]
        elements = data[position:position + obj_size]
        position += obj_size

        yield _bson_to_dict(elements, codec_options)


def decode_file_iter(file_obj, codec_options=DEFAULT_CODEC_OPTIONS):
    """Decode bson data from a file to multiple documents as a generator.

    Works similarly to the decode_all function, but reads from the file object
    in chunks and parses bson in chunks, yielding one document at a time.

    :Parameters:
      - `file_obj`: A file object containing BSON data.
      - `codec_options` (optional): An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionchanged:: 3.0
       Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
       `codec_options`.

    .. versionadded:: 2.8
    """
    while True:
        # Read size of next object.
        size_data = file_obj.read(4)
        if len(size_data) == 0:
            break  # Finished with file normaly.
        elif len(size_data) != 4:
            raise InvalidBSON("cut off in middle of objsize")
        obj_size = _UNPACK_INT(size_data)[0] - 4
        elements = size_data + file_obj.read(obj_size)
        yield _bson_to_dict(elements, codec_options)


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
        _bson_to_dict(bson, DEFAULT_CODEC_OPTIONS)
        return True
    except Exception:
        return False


class BSON(bytes):
    """BSON (Binary JSON) data.
    """

    @classmethod
    def encode(cls, document, check_keys=False,
               codec_options=DEFAULT_CODEC_OPTIONS):
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
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`.

        .. versionchanged:: 3.0
           Replaced `uuid_subtype` option with `codec_options`.
        """
        if not isinstance(codec_options, CodecOptions):
            raise _CODEC_OPTIONS_TYPE_ERROR

        return cls(_dict_to_bson(document, check_keys, codec_options))

    def decode(self, codec_options=DEFAULT_CODEC_OPTIONS):
        """Decode this BSON data.

        By default, returns a BSON document represented as a Python
        :class:`dict`. To use a different :class:`MutableMapping` class,
        configure a :class:`~bson.codec_options.CodecOptions`::

            >>> import collections  # From Python standard library.
            >>> import bson
            >>> from bson.codec_options import CodecOptions
            >>> data = bson.BSON.encode({'a': 1})
            >>> decoded_doc = bson.BSON.decode(data)
            <type 'dict'>
            >>> options = CodecOptions(document_class=collections.OrderedDict)
            >>> decoded_doc = bson.BSON.decode(data, codec_options=options)
            >>> type(decoded_doc)
            <class 'collections.OrderedDict'>

        :Parameters:
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`.

        .. versionchanged:: 3.0
           Removed `compile_re` option: PyMongo now always represents BSON
           regular expressions as :class:`~bson.regex.Regex` objects. Use
           :meth:`~bson.regex.Regex.try_compile` to attempt to convert from a
           BSON regular expression to a Python regular expression object.

           Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
           `codec_options`.

        .. versionchanged:: 2.7
           Added `compile_re` option. If set to False, PyMongo represented BSON
           regular expressions as :class:`~bson.regex.Regex` objects instead of
           attempting to compile BSON regular expressions as Python native
           regular expressions, thus preventing errors for some incompatible
           patterns, see `PYTHON-500`_.

        .. _PYTHON-500: https://jira.mongodb.org/browse/PYTHON-500
        """
        if not isinstance(codec_options, CodecOptions):
            raise _CODEC_OPTIONS_TYPE_ERROR

        return _bson_to_dict(self, codec_options)


def has_c():
    """Is the C extension installed?
    """
    return _USE_C
