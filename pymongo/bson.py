# Copyright 2009 10gen, Inc.
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

"""Tools for dealing with Mongo's BSON data representation.

Generally not needed to be used by application developers."""

import types
import struct
import re
import datetime
import calendar

from binary import Binary
from code import Code
from objectid import ObjectId
from dbref import DBRef
from son import SON
from errors import InvalidBSON, InvalidDocument
from errors import InvalidName, InvalidStringData

try:
    import _cbson
    _use_c = True
except ImportError:
    _use_c = False

try:
    import uuid
    _use_uuid = True
except ImportError:
    _use_uuid = False


def _get_int(data):
    try:
        value = struct.unpack("<i", data[:4])[0]
    except struct.error:
        raise InvalidBSON()

    return (value, data[4:])


def _get_c_string(data):
    try:
        end = data.index("\x00")
    except ValueError:
        raise InvalidBSON()

    return (unicode(data[:end], "utf-8"), data[end + 1:])


def _make_c_string(string):
    if "\x00" in string:
        raise InvalidStringData("BSON strings must not contain a NULL character")
    try:
        return string.encode("utf-8") + "\x00"
    except:
        raise InvalidStringData("strings in documents must be ASCII only")


def _validate_number(data):
    assert len(data) >= 8
    return data[8:]


def _validate_string(data):
    (length, data) = _get_int(data)
    assert len(data) >= length
    assert data[length - 1] == "\x00"
    return data[length:]


def _validate_object(data):
    return _validate_document(data, None)


_valid_array_name = re.compile("^\d+$")


def _validate_array(data):
    return _validate_document(data, _valid_array_name)


def _validate_binary(data):
    (length, data) = _get_int(data)
    # + 1 for the subtype byte
    assert len(data) >= length + 1
    return data[length + 1:]


def _validate_undefined(data):
    return data


_OID_SIZE = 12


def _validate_oid(data):
    assert len(data) >= _OID_SIZE
    return data[_OID_SIZE:]


def _validate_boolean(data):
    assert len(data) >= 1
    return data[1:]


_DATE_SIZE = 8


def _validate_date(data):
    assert len(data) >= _DATE_SIZE
    return data[_DATE_SIZE:]


_validate_null = _validate_undefined


def _validate_regex(data):
    (regex, data) = _get_c_string(data)
    (options, data) = _get_c_string(data)
    return data


def _validate_ref(data):
    data = _validate_string(data)
    return _validate_oid(data)


_validate_code = _validate_string


def _validate_code_w_scope(data):
    (length, data) = _get_int(data)
    assert len(data) >= length + 1
    return data[length + 1:]


_validate_symbol = _validate_string


def _validate_number_int(data):
    assert len(data) >= 4
    return data[4:]


def _validate_timestamp(data):
    assert len(data) >= 8
    return data[8:]

def _validate_number_long(data):
    assert len(data) >= 8
    return data[8:]


_element_validator = {
    "\x01": _validate_number,
    "\x02": _validate_string,
    "\x03": _validate_object,
    "\x04": _validate_array,
    "\x05": _validate_binary,
    "\x06": _validate_undefined,
    "\x07": _validate_oid,
    "\x08": _validate_boolean,
    "\x09": _validate_date,
    "\x0A": _validate_null,
    "\x0B": _validate_regex,
    "\x0C": _validate_ref,
    "\x0D": _validate_code,
    "\x0E": _validate_symbol,
    "\x0F": _validate_code_w_scope,
    "\x10": _validate_number_int,
    "\x11": _validate_timestamp,
    "\x12": _validate_number_long}


def _validate_element_data(type, data):
    try:
        return _element_validator[type](data)
    except KeyError:
        raise InvalidBSON("unrecognized type: %s" % type)


def _validate_element(data, valid_name):
    element_type = data[0]
    (element_name, data) = _get_c_string(data[1:])
    if valid_name:
        assert valid_name.match(element_name), "name is invalid"
    return _validate_element_data(element_type, data)


def _validate_elements(data, valid_name):
    while data:
        data = _validate_element(data, valid_name)


def _validate_document(data, valid_name=None):
    try:
        obj_size = struct.unpack("<i", data[:4])[0]
    except struct.error:
        raise InvalidBSON()

    assert obj_size <= len(data)
    obj = data[4:obj_size]
    assert len(obj)

    eoo = obj[-1]
    assert eoo == "\x00"

    elements = obj[:-1]
    _validate_elements(elements, valid_name)

    return data[obj_size:]


def _get_number(data):
    return (struct.unpack("<d", data[:8])[0], data[8:])


def _get_string(data):
    return _get_c_string(data[4:])


def _get_object(data):
    (object, data) = _bson_to_dict(data)
    if "$ref" in object:
        return (DBRef(object["$ref"], object["$id"]), data)
    return (object, data)


def _get_array(data):
    (obj, data) = _get_object(data)
    result = []
    i = 0
    while True:
        try:
            result.append(obj[str(i)])
            i += 1
        except KeyError:
            break
    return (result, data)


def _get_binary(data):
    (length, data) = _get_int(data)
    subtype = ord(data[0])
    data = data[1:]
    if subtype == 2:
        (length2, data) = _get_int(data)
        if length2 != length - 4:
            raise InvalidBSON("invalid binary (st 2) - lengths don't match!")
        length = length2
    if subtype == 3 and _use_uuid:
        return (uuid.UUID(bytes=data[:length]), data[length:])
    return (Binary(data[:length], subtype), data[length:])


def _get_oid(data):
    return (ObjectId(data[:12]), data[12:])


def _get_boolean(data):
    return (data[0] == "\x01", data[1:])


def _get_date(data):
    seconds = float(struct.unpack("<q", data[:8])[0]) / 1000.0
    return (datetime.datetime.utcfromtimestamp(seconds), data[8:])


def _get_code_w_scope(data):
    (_, data) = _get_int(data)
    (code, data) = _get_string(data)
    (scope, data) = _get_object(data)
    return (Code(code, scope), data)


def _get_null(data):
    return (None, data)


def _get_regex(data):
    (pattern, data) = _get_c_string(data)
    (bson_flags, data) = _get_c_string(data)
    flags = 0
    if "i" in bson_flags:
        flags |= re.IGNORECASE
    if "l" in bson_flags:
        flags |= re.LOCALE
    if "m" in bson_flags:
        flags |= re.MULTILINE
    if "s" in bson_flags:
        flags |= re.DOTALL
    if "u" in bson_flags:
        flags |= re.UNICODE
    if "x" in bson_flags:
        flags |= re.VERBOSE
    return (re.compile(pattern, flags), data)


def _get_ref(data):
    (collection, data) = _get_c_string(data[4:])
    (oid, data) = _get_oid(data)
    return (DBRef(collection, oid), data)


def _get_timestamp(data):
    (timestamp, data) = _get_int(data)
    (inc, data) = _get_int(data)
    return ((timestamp, inc), data)

def _get_long(data):
    return (struct.unpack("<q", data[:8])[0], data[8:])

_element_getter = {
    "\x01": _get_number,
    "\x02": _get_string,
    "\x03": _get_object,
    "\x04": _get_array,
    "\x05": _get_binary,
    "\x06": _get_null, # undefined
    "\x07": _get_oid,
    "\x08": _get_boolean,
    "\x09": _get_date,
    "\x0A": _get_null,
    "\x0B": _get_regex,
    "\x0C": _get_ref,
    "\x0D": _get_string, # code
    "\x0E": _get_string, # symbol
    "\x0F": _get_code_w_scope,
    "\x10": _get_int, # number_int
    "\x11": _get_timestamp,
    "\x12": _get_long,
}


def _element_to_dict(data):
    element_type = data[0]
    (element_name, data) = _get_c_string(data[1:])
    (value, data) = _element_getter[element_type](data)
    return (element_name, value, data)


def _elements_to_dict(data):
    result = {}
    while data:
        (key, value, data) = _element_to_dict(data)
        result[key] = value
    return result


def _bson_to_dict(data):
    obj_size = struct.unpack("<i", data[:4])[0]
    elements = data[4:obj_size - 1]
    return (_elements_to_dict(elements), data[obj_size:])
if _use_c:
    _bson_to_dict = _cbson._bson_to_dict


_RE_TYPE = type(_valid_array_name)


def _element_to_bson(key, value, check_keys):
    if check_keys:
        if key.startswith("$"):
            raise InvalidName("key %r must not start with '$'" % key)
        if "." in key:
            raise InvalidName("key %r must not contain '.'" % key)

    name = _make_c_string(key)
    if isinstance(value, float):
        return "\x01" + name + struct.pack("<d", value)

    # Use Binary w/ subtype 3 for UUID instances
    try:
        import uuid

        if isinstance(value, uuid.UUID):
            value = Binary(value.bytes, subtype=3)
    except ImportError:
        pass

    if isinstance(value, Binary):
        subtype = value.subtype
        if subtype == 2:
            value = struct.pack("<i", len(value)) + value
        return "\x05%s%s%s%s" % (name, struct.pack("<i", len(value)),
                                 chr(subtype), value)
    if isinstance(value, Code):
        cstring = _make_c_string(value)
        scope = _dict_to_bson(value.scope, False)
        full_length = struct.pack("<i", 8 + len(cstring) + len(scope))
        length = struct.pack("<i", len(cstring))
        return "\x0F" + name + full_length + length + cstring + scope
    if isinstance(value, str):
        cstring = _make_c_string(value)
        length = struct.pack("<i", len(cstring))
        return "\x02" + name + length + cstring
    if isinstance(value, unicode):
        cstring = _make_c_string(value)
        length = struct.pack("<i", len(cstring))
        return "\x02" + name + length + cstring
    if isinstance(value, dict):
        return "\x03" + name + _dict_to_bson(value, check_keys)
    if isinstance(value, (list, tuple)):
        as_dict = SON(zip([str(i) for i in range(len(value))], value))
        return "\x04" + name + _dict_to_bson(as_dict, check_keys)
    if isinstance(value, ObjectId):
        return "\x07" + name + value.binary
    if value is True:
        return "\x08" + name + "\x01"
    if value is False:
        return "\x08" + name + "\x00"
    if isinstance(value, (int, long)):
        # TODO this is a really ugly way to check for this...
        if value > 2**64 / 2 - 1 or value < -2**64 / 2:
            raise OverflowError("MongoDB can only handle up to 8-byte ints")
        if value > 2**32 / 2 - 1 or value < -2**32 / 2:
            return "\x12" + name + struct.pack("<q", value)
        return "\x10" + name + struct.pack("<i", value)
    if isinstance(value, datetime.datetime):
        millis = int(calendar.timegm(value.timetuple()) * 1000 +
                     value.microsecond / 1000)
        return "\x09" + name + struct.pack("<q", millis)
    if value is None:
        return "\x0A" + name
    if isinstance(value, _RE_TYPE):
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
        return "\x0B" + name + _make_c_string(pattern) + _make_c_string(flags)
    if isinstance(value, DBRef):
        return _element_to_bson(key,
                                SON([("$ref", value.collection),
                                     ("$id", value.id)]),
                                False)
    raise InvalidDocument("cannot convert value of type %s to bson" %
                          type(value))


def _dict_to_bson(dict, check_keys):
    try:
        elements = ""
        if "_id" in dict:
            elements += _element_to_bson("_id", dict["_id"], False)
        for (key, value) in dict.iteritems():
            if key != "_id":
                elements += _element_to_bson(key, value, check_keys)
    except AttributeError:
        raise TypeError("encoder expected a mapping type but got: %r" % dict)

    length = len(elements) + 5
    return struct.pack("<i", length) + elements + "\x00"
if _use_c:
    _dict_to_bson = _cbson._dict_to_bson


def _to_dicts(data):
    """Convert binary data to sequence of SON objects.

    Data must be concatenated strings of valid BSON data.

    :Parameters:
      - `data`: bson data
    """
    dicts = []
    while len(data):
        (son, data) = _bson_to_dict(data)
        dicts.append(son)
    return dicts
if _use_c:
    _to_dicts = _cbson._to_dicts


def _to_dict(data):
    (son, _) = _bson_to_dict(data)
    return son


def is_valid(bson):
    """Validate that the given string represents valid BSON data.

    Raises TypeError if the data is not an instance of a subclass of str.
    Returns True if the data represents a valid BSON object, False otherwise.

    :Parameters:
      - `bson`: the data to be validated
    """
    if not isinstance(bson, types.StringType):
        raise TypeError("BSON data must be an instance of a subclass of str")

    try:
        remainder = _validate_document(bson)
        return remainder == ""
    except (AssertionError, InvalidBSON):
        return False


class BSON(str):
    """BSON data.

    Represents binary data storable in and retrievable from Mongo.
    """

    def __new__(cls, bson):
        """Initialize a new BSON object with some data.

        Raises TypeError if `bson` is not an instance of str.

        :Parameters:
          - `bson`: the initial data
        """
        return str.__new__(cls, bson)

    def from_dict(cls, dict, check_keys=False):
        """Create a new BSON object from a python mapping type (like dict).

        Raises TypeError if the argument is not a mapping type, or contains
        keys that are not instance of (str, unicode). Raises InvalidDocument
        if the dictionary cannot be converted to BSON.

        :Parameters:
          - `dict`: mapping type representing a Mongo document
          - `check_keys`: check if keys start with '$' or contain '.',
            raising `pymongo.errors.InvalidName` in either case
        """
        return cls(_dict_to_bson(dict, check_keys))
    from_dict = classmethod(from_dict)

    def to_dict(self):
        """Get the dictionary representation of this data."""
        (son, _) = _bson_to_dict(self)
        return son
