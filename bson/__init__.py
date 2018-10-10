# Copyright 2009-present MongoDB, Inc.
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

The mapping from Python types to BSON types is as follows:

=======================================  =============  ===================
Python Type                              BSON Type      Supported Direction
=======================================  =============  ===================
None                                     null           both
bool                                     boolean        both
int [#int]_                              int32 / int64  py -> bson
long                                     int64          py -> bson
`bson.int64.Int64`                       int64          both
float                                    number (real)  both
string                                   string         py -> bson
unicode                                  string         both
list                                     array          both
dict / `SON`                             object         both
datetime.datetime [#dt]_ [#dt2]_         date           both
`bson.regex.Regex`                       regex          both
compiled re [#re]_                       regex          py -> bson
`bson.binary.Binary`                     binary         both
`bson.objectid.ObjectId`                 oid            both
`bson.dbref.DBRef`                       dbref          both
None                                     undefined      bson -> py
unicode                                  code           bson -> py
`bson.code.Code`                         code           py -> bson
unicode                                  symbol         bson -> py
bytes (Python 3) [#bytes]_               binary         both
=======================================  =============  ===================

Note that, when using Python 2.x, to save binary data it must be wrapped as
an instance of `bson.binary.Binary`. Otherwise it will be saved as a BSON
string and retrieved as unicode. Users of Python 3.x can use the Python bytes
type.

.. [#int] A Python int will be saved as a BSON int32 or BSON int64 depending
   on its size. A BSON int32 will always decode to a Python int. A BSON
   int64 will always decode to a :class:`~bson.int64.Int64`.
.. [#dt] datetime.datetime instances will be rounded to the nearest
   millisecond when saved
.. [#dt2] all datetime.datetime instances are treated as *naive*. clients
   should always use UTC.
.. [#re] :class:`~bson.regex.Regex` instances and regular expression
   objects from ``re.compile()`` are both saved as BSON regular expressions.
   BSON regular expressions are decoded as :class:`~bson.regex.Regex`
   instances.
.. [#bytes] The bytes type from Python 3.x is encoded as BSON binary with
   subtype 0. In Python 3.x it will be decoded back to bytes. In Python 2.x
   it will be decoded to an instance of :class:`~bson.binary.Binary` with
   subtype 0.
"""

import calendar
import datetime
import itertools
import re
import struct
import sys
import uuid

from collections import deque
from contextlib import contextmanager
from enum import Enum
from io import BytesIO

from codecs import (utf_8_decode as _utf_8_decode,
                    utf_8_encode as _utf_8_encode)

from bson.binary import (Binary, OLD_UUID_SUBTYPE,
                         JAVA_LEGACY, CSHARP_LEGACY,
                         UUIDLegacy)
from bson.code import Code
from bson.codec_options import (
    CodecOptions, DEFAULT_CODEC_OPTIONS, _raw_document_class)
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.errors import (InvalidBSON,
                         InvalidDocument,
                         InvalidStringData)
from bson.int64 import Int64
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.py3compat import (abc,
                            b,
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
BSONDEC = b"\x13" # Decimal128
BSONMIN = b"\xFF" # Min key
BSONMAX = b"\x7F" # Max key


_UNPACK_FLOAT = struct.Struct("<d").unpack
_UNPACK_INT = struct.Struct("<i").unpack
_UNPACK_LENGTH_SUBTYPE = struct.Struct("<iB").unpack
_UNPACK_LONG = struct.Struct("<q").unpack
_UNPACK_TIMESTAMP = struct.Struct("<II").unpack


def _raise_unknown_type(element_type, element_name):
    """Unknown type helper."""
    raise InvalidBSON("Detected unknown BSON type %r for fieldname '%s'. Are "
                      "you using the latest driver version?" % (
                          element_type, element_name))


def _get_int(data, position, dummy0, dummy1, dummy2):
    """Decode a BSON int32 to python int."""
    end = position + 4
    return _UNPACK_INT(data[position:end])[0], end


def _get_c_string(data, position, opts):
    """Decode a BSON 'C' string to python unicode string."""
    end = data.index(b"\x00", position)
    return _utf_8_decode(data[position:end],
                         opts.unicode_decode_error_handler, True)[0], end + 1


def _get_float(data, position, dummy0, dummy1, dummy2):
    """Decode a BSON double to python float."""
    end = position + 8
    return _UNPACK_FLOAT(data[position:end])[0], end


def _get_string(data, position, obj_end, opts, dummy):
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


def _get_object(data, position, obj_end, opts, dummy):
    """Decode a BSON subdocument to opts.document_class or bson.dbref.DBRef."""
    obj_size = _UNPACK_INT(data[position:position + 4])[0]
    end = position + obj_size - 1
    if data[end:position + obj_size] != b"\x00":
        raise InvalidBSON("bad eoo")
    if end >= obj_end:
        raise InvalidBSON("invalid object length")
    if _raw_document_class(opts.document_class):
        return (opts.document_class(data[position:end + 1], opts),
                position + obj_size)

    obj = _elements_to_dict(data, position + 4, end, opts)

    position += obj_size
    if "$ref" in obj:
        return (DBRef(obj.pop("$ref"), obj.pop("$id", None),
                      obj.pop("$db", None), obj), position)
    return obj, position


def _get_array(data, position, obj_end, opts, element_name):
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
        try:
            value, position = getter[element_type](
                data, position, obj_end, opts, element_name)
        except KeyError:
            _raise_unknown_type(element_type, element_name)
        append(value)

    if position != end + 1:
        raise InvalidBSON('bad array length')
    return result, position + 1


def _get_binary(data, position, obj_end, opts, dummy1):
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
    if length < 0 or end > obj_end:
        raise InvalidBSON('bad binary object length')
    if subtype == 3:
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
    if subtype == 4:
        return uuid.UUID(bytes=data[position:end]), end
    # Python3 special case. Decode subtype 0 to 'bytes'.
    if PY3 and subtype == 0:
        value = data[position:end]
    else:
        value = Binary(data[position:end], subtype)
    return value, end


def _get_oid(data, position, dummy0, dummy1, dummy2):
    """Decode a BSON ObjectId to bson.objectid.ObjectId."""
    end = position + 12
    return ObjectId(data[position:end]), end


def _get_boolean(data, position, dummy0, dummy1, dummy2):
    """Decode a BSON true/false to python True/False."""
    end = position + 1
    boolean_byte = data[position:end]
    if boolean_byte == b'\x00':
        return False, end
    elif boolean_byte == b'\x01':
        return True, end
    raise InvalidBSON('invalid boolean value: %r' % boolean_byte)


def _get_date(data, position, dummy0, opts, dummy1):
    """Decode a BSON datetime to python datetime.datetime."""
    end = position + 8
    millis = _UNPACK_LONG(data[position:end])[0]
    return _millis_to_datetime(millis, opts), end


def _get_code(data, position, obj_end, opts, element_name):
    """Decode a BSON code to bson.code.Code."""
    code, position = _get_string(data, position, obj_end, opts, element_name)
    return Code(code), position


def _get_code_w_scope(data, position, obj_end, opts, element_name):
    """Decode a BSON code_w_scope to bson.code.Code."""
    code_end = position + _UNPACK_INT(data[position:position + 4])[0]
    code, position = _get_string(
        data, position + 4, code_end, opts, element_name)
    scope, position = _get_object(data, position, code_end, opts, element_name)
    if position != code_end:
        raise InvalidBSON('scope outside of javascript code boundaries')
    return Code(code, scope), position


def _get_regex(data, position, dummy0, opts, dummy1):
    """Decode a BSON regex to bson.regex.Regex or a python pattern object."""
    pattern, position = _get_c_string(data, position, opts)
    bson_flags, position = _get_c_string(data, position, opts)
    bson_re = Regex(pattern, bson_flags)
    return bson_re, position


def _get_ref(data, position, obj_end, opts, element_name):
    """Decode (deprecated) BSON DBPointer to bson.dbref.DBRef."""
    collection, position = _get_string(
        data, position, obj_end, opts, element_name)
    oid, position = _get_oid(data, position, obj_end, opts, element_name)
    return DBRef(collection, oid), position


def _get_timestamp(data, position, dummy0, dummy1, dummy2):
    """Decode a BSON timestamp to bson.timestamp.Timestamp."""
    end = position + 8
    inc, timestamp = _UNPACK_TIMESTAMP(data[position:end])
    return Timestamp(timestamp, inc), end


def _get_int64(data, position, dummy0, dummy1, dummy2):
    """Decode a BSON int64 to bson.int64.Int64."""
    end = position + 8
    return Int64(_UNPACK_LONG(data[position:end])[0]), end


def _get_decimal128(data, position, dummy0, dummy1, dummy2):
    """Decode a BSON decimal128 to bson.decimal128.Decimal128."""
    end = position + 16
    return Decimal128.from_bid(data[position:end]), end


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
    BSONUND: lambda v, w, x, y, z: (None, w),  # Deprecated undefined
    BSONOID: _get_oid,
    BSONBOO: _get_boolean,
    BSONDAT: _get_date,
    BSONNUL: lambda v, w, x, y, z: (None, w),
    BSONRGX: _get_regex,
    BSONREF: _get_ref,  # Deprecated DBPointer
    BSONCOD: _get_code,
    BSONSYM: _get_string,  # Deprecated symbol
    BSONCWS: _get_code_w_scope,
    BSONINT: _get_int,
    BSONTIM: _get_timestamp,
    BSONLON: _get_int64,
    BSONDEC: _get_decimal128,
    BSONMIN: lambda v, w, x, y, z: (MinKey(), w),
    BSONMAX: lambda v, w, x, y, z: (MaxKey(), w)}


def _element_to_dict(data, position, obj_end, opts):
    """Decode a single key, value pair."""
    element_type = data[position:position + 1]
    position += 1
    element_name, position = _get_c_string(data, position, opts)
    try:
        value, position = _ELEMENT_GETTER[element_type](data, position,
                                                        obj_end, opts,
                                                        element_name)
    except KeyError:
        _raise_unknown_type(element_type, element_name)
    return element_name, value, position
if _USE_C:
    _element_to_dict = _cbson._element_to_dict


def _iterate_elements(data, position, obj_end, opts):
    end = obj_end - 1
    while position < end:
        (key, value, position) = _element_to_dict(data, position, obj_end, opts)
        yield key, value, position


def _elements_to_dict(data, position, obj_end, opts):
    """Decode a BSON document."""
    result = opts.document_class()
    pos = position
    for key, value, pos in _iterate_elements(data, position, obj_end, opts):
        result[key] = value
    if pos != obj_end:
        raise InvalidBSON('bad object or element length')
    return result


def _get_namespace_from_entry(current_namespace, entry):
    """entry is a BSONEntry object."""
    if current_namespace:
        return ".".join([current_namespace, entry.name])
    else:
        return entry.name


def _data_namespaces_to_dict(ctx_namespace, bson_reader, data_namespaces,
                       default_opts, user_opts):
    if (bson_reader._current_state == _BSONReaderState.INITIAL or
            bson_reader._current_entry.type == BSONTypes.DOCUMENT):
        if ctx_namespace in data_namespaces:
            return user_opts.document_class_codec.from_bson(bson_reader)
        # Else
        result = default_opts.document_class()
        bson_reader.start_document()
        for entry in bson_reader:
            result[entry.name] = _data_namespaces_to_dict(
                _get_namespace_from_entry(ctx_namespace, entry), bson_reader,
                data_namespaces, default_opts, user_opts
            )
        bson_reader.end_document()
        return result

    if bson_reader._current_entry.type == BSONTypes.ARRAY:
        if ctx_namespace in data_namespaces:
            result = []
            bson_reader.start_array()
            for entry in bson_reader:
                result.append(
                    user_opts.document_class_codec.from_bson(bson_reader))
            bson_reader.end_array()
            return result
        # Else
        return bson_reader._current_entry.value

    # If primitive type is to be interpreted in a 'custom' sense, do that here.
    # This is currently unsupported.

    # Everything else is a normally interpreted primitive type.
    return bson_reader._current_entry.value


def _response_elements_to_dict(data, position, obj_end, opts,
                               data_namespaces=None):
    """Decode a BSON document. The paths in namespaces will be treated with
    the user-provided codec options. This does not extend to elements within
    arrays (at least for now)."""
    if opts.document_class_codec is None or data_namespaces is None:
        return _elements_to_dict(data, position, obj_end, opts)

    # Otherwise return a default doc cls with the datakey fields appropriately decoded.
    def_opts = DEFAULT_CODEC_OPTIONS
    reader = BSONDocumentReader(data)

    # Populate result appropriately.
    result = _data_namespaces_to_dict('', reader, data_namespaces, def_opts, opts)
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
        if _raw_document_class(opts.document_class):
            return opts.document_class(data, opts)
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
    if _raw_document_class(value):
        return b'\x03' + name + value.raw
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
    millis = _datetime_to_millis(value)
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
    if value.scope is None:
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


def _encode_decimal128(name, value, dummy0, dummy1):
    """Encode bson.decimal128.Decimal128."""
    return b"\x13" + name + value.bid


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
    Decimal128: _encode_decimal128,
    # Special case. This will never be looked up directly.
    abc.Mapping: _encode_mapping,
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
    if _raw_document_class(doc):
        return doc.raw
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


class BSONTypes(Enum):
    X00 = b"\x00"
    FLOAT = b"\x01"
    STRING = b"\x02"
    DOCUMENT = b"\x03"
    ARRAY = b"\x04"
    BINARY = b"\x05"
    UNDEFINED = b"\x06"
    OBJECT_ID = b"\x07"
    BOOLEAN = b"\x08"
    UTCDATETIME = b"\x09"
    NULL = b"\x0A"
    INT = b"\x10"
    TIMESTAMP = b"\x11"
    LONG = b"\x12"
    DECIMAL128 = b"\x13"


class _BSONReaderState(Enum):
    INITIAL = 0
    TOP_LEVEL = 1
    DOCUMENT = 2
    ARRAY = 3
    END = 4


def _read_int32_from_bstream(bstream, start):
    try:
        return _UNPACK_INT(bstream[start:start + 4])[0]
    except struct.error as exc:
        raise InvalidBSON(str(exc))


def _get_string_size(bstream, start):
    return 4 + _read_int32_from_bstream(bstream, start)


def _get_document_size(bstream, start):
    return _read_int32_from_bstream(bstream, start)


def _get_array_size(bstream, start):
    return _read_int32_from_bstream(bstream, start)

def _get_binary_size(bstream, start):
    return 5 + _read_int32_from_bstream(bstream, start)


TYPE_SIZE_MAP = {
    BSONTypes.FLOAT: 8,
    BSONTypes.STRING: _get_string_size,
    BSONTypes.DOCUMENT: _get_document_size,
    BSONTypes.ARRAY: _get_array_size,
    BSONTypes.BINARY: _get_binary_size,
    BSONTypes.BOOLEAN: 1,
    BSONTypes.INT: 4,
    BSONTypes.LONG: 8,
    BSONTypes.DECIMAL128: 16,
    BSONTypes.NULL: 0,
    BSONTypes.OBJECT_ID: 12,
    BSONTypes.TIMESTAMP: 8
}


class _BSONEntry(object):
    def __init__(self, bstream, head, codec_options=DEFAULT_CODEC_OPTIONS):
        self._bstream = bstream
        self._codec_options = codec_options
        self.reinit(head)

    def reinit(self, head):
        # Byte-position where this entry begins
        self._head = head
        self._type = None
        self._name = None
        self._name_size = None
        self._value = None
        self._value_size = None

    def with_options(self, codec_options):
        return _BSONEntry(self._bstream, self._head, codec_options)

    @property
    def name_start(self):
        return self._head + 1

    @property
    def name_size(self):
        if self._name_size is None:
            end_idx = self._bstream.index(b"\x00", self.name_start)
            self._name_size = end_idx - self.name_start
        return self._name_size

    @property
    def name(self):
        if self._name is None:
            name_slc = slice(self.name_start, self.name_start + self.name_size)
            name = _utf_8_decode(
                self._bstream[name_slc],
                self._codec_options.unicode_decode_error_handler, True)[0]
            self._name = name
        return self._name

    @property
    def type(self):
        if self._type is None:
            self._type = BSONTypes(self._bstream[self._head:self._head + 1])
        return self._type

    @property
    def value_start(self):
        return self._head + 1 + self.name_size + 1

    @property
    def value_size(self):
        if self._value_size is None:
            size = size_getter = TYPE_SIZE_MAP[self.type]
            if callable(size_getter):
                size = size_getter(self._bstream, self.value_start)
            self._value_size = size
        return self._value_size

    @property
    def value(self):
        if self._value is None:
            self._value, position = _ELEMENT_GETTER[self.type.value](
                self._bstream, self.value_start, self._head + self.size,
                self._codec_options, self.name)
            assert position == self._head + self.size
        return self._value

    @property
    def size(self):
        # Type byte + c-string name size + arbitrary value size
        return 1 + self.name_size + 1 + self.value_size


class BSONDocumentReader(object):
    def __init__(self, bstream, codec_options=DEFAULT_CODEC_OPTIONS):
        self._bstream = bstream
        self._codec_options = codec_options
        self._ends = deque()
        self._heads = deque()
        self._entries = deque()
        self._states = deque()
        self._states.append(_BSONReaderState.INITIAL)

    @property
    def _current_head(self):
        return self._heads[-1]

    @_current_head.setter
    def _current_head(self, value):
        self._heads[-1] = value

    @property
    def _current_entry(self):
        return self._entries[-1]

    @_current_entry.setter
    def _current_entry(self, value):
        self._entries[-1] = value

    @property
    def _current_end(self):
        return self._ends[-1]

    @property
    def _current_state(self):
        return self._states[-1]

    @_current_state.setter
    def _current_state(self, value):
        self._states[-1] = value

    def _validate_document(self):
        # Validates top-level document only.
        # Current end must be properly initialized.
        if self._current_end != len(self._bstream):
            raise InvalidBSON("invalid object size")
        if self._bstream[self._current_end - 1:] != b"\x00":
            raise InvalidBSON("bad eoo")

    def start_document(self):
        if self._current_state == _BSONReaderState.INITIAL:
            self._current_state = _BSONReaderState.TOP_LEVEL
            self._ends.append(_read_int32_from_bstream(self._bstream, 0))
            self._heads.append(4)
            self._validate_document()
        else:
            if self._current_entry.type != BSONTypes.DOCUMENT:
                raise InvalidBSON("not a document")
            self._states.append(_BSONReaderState.DOCUMENT)
            doc_start = self._current_entry.value_start
            self._ends.append(doc_start + _read_int32_from_bstream(
                self._bstream, doc_start))
            self._heads.append(doc_start + 4)
        # Finally.
        self._entries.append(_BSONEntry(
            self._bstream, self._current_head, self._codec_options))

    def end_document(self):
        # Jump out of current document.
        if (self._current_state != _BSONReaderState.TOP_LEVEL and
                self._current_state != _BSONReaderState.DOCUMENT):
            raise InvalidBSON("not in a document")
        if self._current_state == _BSONReaderState.TOP_LEVEL:
            self._current_state = _BSONReaderState.END
            # Add other cleanup here.
        # Else its a nested doc.
        self._states.pop()
        self._entries.pop()
        self._heads.pop()
        self._ends.pop()

    def start_array(self):
        if self._current_entry.type != BSONTypes.ARRAY:
            raise InvalidBSON("not an array")
        self._states.append(_BSONReaderState.ARRAY)
        arr_start = self._current_entry.value_start
        self._ends.append(arr_start + _read_int32_from_bstream(
            self._bstream, arr_start))
        self._heads.append(arr_start + 4)
        self._entries.append(_BSONEntry(
            self._bstream, self._current_head, self._codec_options))

    def end_array(self):
        # Jump out of current array.
        if self._current_state != _BSONReaderState.ARRAY:
            raise InvalidBSON("not an array")
        self._states.pop()
        self._entries.pop()
        self._heads.pop()
        self._ends.pop()

    def read_bson_type(self):
        return self._current_entry.type

    def read_name(self):
        return self._current_entry.name

    def read_element(self):
        return self._current_entry.value

    def __iter__(self):
        while (self._current_head < self._current_end - 1 and
               self._current_state != _BSONReaderState.END):
            self._current_entry.reinit(self._current_head)
            yield self._current_entry
            self._current_head += self._current_entry.size


def _bson_to_dict_buffered(data, opts):
    """Decode a BSON string to document_class."""
    try:
        if _raw_document_class(opts.document_class):
            return opts.document_class(data, opts)
        reader = BSONDocumentReader(data)
        if opts.document_class_codec is not None:
            return opts.document_class_codec.from_bson(reader)
        result = opts.document_class()
        reader.start_document()
        for elt in reader:
            result[elt.name] = elt.value
        reader.end_document()
        return result
    except InvalidBSON:
        raise
    except Exception:
        # Change exception type to InvalidBSON but preserve traceback.
        _, exc_value, exc_tb = sys.exc_info()
        reraise(InvalidBSON, exc_value, exc_tb)


class BSONDocumentWriter(object):
    def __init__(self, check_keys=False, codec_options=DEFAULT_CODEC_OPTIONS):
        self._check_keys = check_keys
        self._codec_options = codec_options
        self.reinit()

    def reinit(self):
        # Bytestream in which to place BSON encoded bytes.
        # Deque used to track streams for nested entities.
        self.__streams = deque()

        # Size of document in bytes.
        # Deque used to track sizes for nested entities.
        self.__sizes = deque()

        # Container type to which we are currently writing.
        # Can be a document or an array.
        # Deque used to track nesting of containers.
        self.__containers = deque()

        # Array index at which we are currently writing.
        # Deque used to track nesting of arrays.
        self.__array_indices = deque()

    @property
    def _level(self):
        # Nesting level of document currently being populated.
        # Top level document is level 1.
        return len(self.__sizes)

    @property
    def _stream(self):
        # Bytestream corresponding to current document.
        return self.__streams[-1]

    @property
    def _size(self):
        # Size of current document.
        return self.__sizes[-1]

    @property
    def _current_index(self):
        return self.__array_indices[-1]

    @property
    def _current_container(self):
        return self.__containers[-1]

    def _insert_bytes(self, b):
        self.__sizes[-1] += self._stream.write(b)

    def _key_name(self, key):
        if key is not None:
            return _make_name(key)

        # Top level document has no name.
        if self._level <= 1:
            return

        # Generate array index names or error if not in array.
        if self._current_container != BSONTypes.ARRAY:
            raise RuntimeError("field name not provided for non-array element")
        key = text_type(self._current_index)
        self.__array_indices[-1] += 1
        return _make_name(key)

    def _unpack_args(self, args):
        nargs = len(args)
        if nargs == 0:
            raise RuntimeError("field value not provided")
        elif nargs == 1:
            key, value = None, args[0]
        else:
            key, value = args[0], args[1]
        return self._key_name(key), value

    def write_bool(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_bool(
            key, value, self._check_keys, self._codec_options))

    def write_datetime(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_datetime(
            key, value, self._check_keys, self._codec_options))

    def write_float(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_float(
            key, value, self._check_keys, self._codec_options))

    def write_int(self, *args):
        # FIXME: this will still encode to Int64 if value is too large.
        # TODO: what should proper behavior be in this case?
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_int(
            key, value, self._check_keys, self._codec_options))

    def write_long(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_long(
            key, value, self._check_keys, self._codec_options))

    def write_string(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_text(
            key, value, self._check_keys, self._codec_options))

    def write_none(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_none(
            key, value, self._check_keys, self._codec_options))

    def write_binary(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_binary(
            key, value, self._check_keys, self._codec_options))

    def write_code(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_code(
            key, value, self._check_keys, self._codec_options))

    def write_dbref(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_dbref(
            key, value, self._check_keys, self._codec_options))

    def write_maxkey(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_maxkey(
            key, value, self._check_keys, self._codec_options))

    def write_minkey(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_minkey(
            key, value, self._check_keys, self._codec_options))

    def write_objectid(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_objectid(
            key, value, self._check_keys, self._codec_options))

    def write_regex(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_regex(
            key, value, self._check_keys, self._codec_options))

    def write_timestamp(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_timestamp(
            key, value, self._check_keys, self._codec_options))

    def write_decimal128(self, *args):
        key, value = self._unpack_args(args)
        self._insert_bytes(_encode_decimal128(
            key, value, self._check_keys, self._codec_options))

    def _write_custom_type(self, key, value):
        #codec = self._codec_options.custom_codec_map[type(value)]
        codec = self._codec_options.get_codec_for_type(type(value))
        codec.encode(key, value, self, self._codec_options)

    def _write_custom_document(self, value):
        self._codec_options.document_class_codec.to_bson(value, self)

    def _write_start_container(self, name_bytes, container_type):
        # Insert element name and type marker.
        if name_bytes is not None:
            self._insert_bytes(container_type.value + name_bytes)

        # Create stream for new container.
        self.__streams.append(BytesIO())

        # Create size counter for new container.
        self.__sizes.append(0)

        # Update container tracking.
        self.__containers.append(container_type)

        # Placeholder for size.
        self._insert_bytes(_PACK_INT(0))

    def _write_end_container(self):
        # End current container.
        self._finalize()

        # If not at the top-level document, render this container
        # and concatenate it to the higher-level container.
        if self._level > 1:
            # Render document.
            bstream = self._stream.getvalue()

            # Drop size, stream and container info for rendered document.
            self.__containers.pop()
            self.__sizes.pop()
            self.__streams.pop()

            # Insert rendered document into higher-level document.
            self._insert_bytes(bstream)

    def _finalize(self):
        # Insert null byte to mark document end.
        self._insert_bytes(b"\x00")

        # Update size bytes.
        self._stream.seek(0)
        self._insert_bytes(_PACK_INT(self._size))

    @contextmanager
    def document(self, name=None):
        self.start_document(name)
        yield
        self.end_document()

    def start_document(self, name=None):
        # Embedded documents must have a name provided unless in an array.
        if (name is None and self._level >= 1 and
                self._current_container != BSONTypes.ARRAY):
            raise RuntimeError("Must provide key name for nested documents.")
        self._write_start_container(
            self._key_name(name), BSONTypes.DOCUMENT)

    def end_document(self):
        self._write_end_container()

    def start_array(self, name):
        self._write_start_container(
            self._key_name(name), BSONTypes.ARRAY)
        self.__array_indices.append(0)

    def end_array(self):
        self._write_end_container()
        self.__array_indices.pop()

    def as_bytes(self):
        if self._level != 1:
            raise RuntimeError("Incomplete document definition")
        return self._stream.getvalue()


class BSONCodecABC(object):

    CLS_ENCODED = None

    @staticmethod
    def encode(value, writer, codec_options=DEFAULT_CODEC_OPTIONS):
        raise NotImplementedError

    @staticmethod
    def decode(value, reader, codec_options=DEFAULT_CODEC_OPTIONS):
        raise NotImplementedError


class CustomDocumentClassCodecBase(object):

    DOCUMENT_CLASS = None

    def __init__(self, doc_cls):
        self.DOCUMENT_CLASS = doc_cls


def _dict_to_bson_buffered(doc, check_keys, opts, top_level=True):
    """Encode a document to BSON using a buffered interface."""
    if _raw_document_class(doc):
        return doc.raw
    try:
        writer = BSONDocumentWriter(codec_options=opts)
        writer.start_document()
        if top_level and "_id" in doc:
            writer._insert_bytes(_element_to_bson('_id', doc['_id'], check_keys, opts))
        for (key, value) in iteritems(doc):
            if not top_level or key != "_id":
                try:
                    writer._insert_bytes(_element_to_bson(
                        key, value, check_keys, opts))
                except InvalidDocument:
                    writer._write_custom_type(key, value,)
    except TypeError:
        # We can end up here if the entire document needs custom encoding, as
        # opposed to only some fields in the document needing it.
        if isinstance(doc, opts.document_class):
            writer.reinit()
            writer._write_custom_document(doc)
            return writer.as_bytes()
        raise TypeError("no custom encodings registered for: %r" % (doc,))
    except AttributeError:
        raise TypeError("encoder expected a mapping type but got: %r" % (doc,))
    else:
        writer.end_document()

    return writer.as_bytes()


def _millis_to_datetime(millis, opts):
    """Convert milliseconds since epoch UTC to datetime."""
    diff = ((millis % 1000) + 1000) % 1000
    seconds = (millis - diff) // 1000
    micros = diff * 1000
    if opts.tz_aware:
        dt = EPOCH_AWARE + datetime.timedelta(seconds=seconds,
                                              microseconds=micros)
        if opts.tzinfo:
            dt = dt.astimezone(opts.tzinfo)
        return dt
    else:
        return EPOCH_NAIVE + datetime.timedelta(seconds=seconds,
                                                microseconds=micros)


def _datetime_to_millis(dtm):
    """Convert datetime to milliseconds since epoch UTC."""
    if dtm.utcoffset() is not None:
        dtm = dtm - dtm.utcoffset()
    return int(calendar.timegm(dtm.timetuple()) * 1000 +
               dtm.microsecond // 1000)


_CODEC_OPTIONS_TYPE_ERROR = TypeError(
    "codec_options must be an instance of CodecOptions")


def decode_cursor_response(data, codec_options=DEFAULT_CODEC_OPTIONS):
    if not isinstance(codec_options, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    docs = []
    position = 0
    end = len(data) - 1
    use_raw = _raw_document_class(codec_options.document_class)
    try:
        while position < end:
            obj_size = _UNPACK_INT(data[position:position + 4])[0]
            if len(data) - position < obj_size:
                raise InvalidBSON("invalid object size")
            obj_end = position + obj_size - 1
            if data[obj_end:position + obj_size] != b"\x00":
                raise InvalidBSON("bad eoo")
            if use_raw:
                docs.append(
                    codec_options.document_class(
                        data[position:obj_end + 1], codec_options))
            else:
                docs.append(_response_elements_to_dict(
                    data,
                    position + 4,
                    obj_end,
                    codec_options,
                    ['cursor.firstBatch', 'cursor.nextBatch'],
                ))
            position += obj_size
        return docs
    except InvalidBSON:
        raise
    except Exception:
        # Change exception type to InvalidBSON but preserve traceback.
        _, exc_value, exc_tb = sys.exc_info()
        reraise(InvalidBSON, exc_value, exc_tb)


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
    use_raw = _raw_document_class(codec_options.document_class)
    try:
        while position < end:
            obj_size = _UNPACK_INT(data[position:position + 4])[0]
            if len(data) - position < obj_size:
                raise InvalidBSON("invalid object size")
            obj_end = position + obj_size - 1
            if data[obj_end:position + obj_size] != b"\x00":
                raise InvalidBSON("bad eoo")
            if use_raw:
                docs.append(
                    codec_options.document_class(
                        data[position:obj_end + 1], codec_options))
            else:
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

    @classmethod
    def encode_buffered(cls, document, check_keys=False,
                        codec_options=DEFAULT_CODEC_OPTIONS):
        return cls(_dict_to_bson_buffered(document, check_keys, codec_options))

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

    def decode_buffered(self, codec_options=DEFAULT_CODEC_OPTIONS):
        if not isinstance(codec_options, CodecOptions):
            raise _CODEC_OPTIONS_TYPE_ERROR

        return _bson_to_dict_buffered(self, codec_options)


def has_c():
    """Is the C extension installed?
    """
    return _USE_C
