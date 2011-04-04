# Copyright 2009-2010 10gen, Inc.
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
import warnings

from bson.binary import Binary
from bson.code import Code
from bson.dbref import DBRef
from bson.errors import (InvalidBSON,
                         InvalidDocument,
                         InvalidStringData)
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.son import SON
from bson.timestamp import Timestamp
from bson.tz_util import utc


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


# This sort of sucks, but seems to be as good as it gets...
RE_TYPE = type(re.compile(""))


def _get_int(data, as_class=None, tz_aware=False, unsigned=False):
    format = unsigned and "I" or "i"
    try:
        value = struct.unpack("<%s" % format, data[:4])[0]
    except struct.error:
        raise InvalidBSON()

    return (value, data[4:])


def _get_c_string(data, length=None):
    if length is None:
        try:
            length = data.index("\x00")
        except ValueError:
            raise InvalidBSON()

    return (unicode(data[:length], "utf-8"), data[length + 1:])


def _make_c_string(string, check_null=False):
    if check_null and "\x00" in string:
        raise InvalidDocument("BSON keys / regex patterns must not "
                              "contain a NULL character")
    if isinstance(string, unicode):
        return string.encode("utf-8") + "\x00"
    else:
        try:
            string.decode("utf-8")
            return string + "\x00"
        except:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % string)


def _get_number(data, as_class, tz_aware):
    return (struct.unpack("<d", data[:8])[0], data[8:])


def _get_string(data, as_class, tz_aware):
    return _get_c_string(data[4:], struct.unpack("<i", data[:4])[0] - 1)


def _get_object(data, as_class, tz_aware):
    (object, data) = _bson_to_dict(data, as_class, tz_aware)
    if "$ref" in object:
        return (DBRef(object.pop("$ref"), object.pop("$id"),
                      object.pop("$db", None), object), data)
    return (object, data)


def _get_array(data, as_class, tz_aware):
    (obj, data) = _get_object(data, as_class, tz_aware)
    result = []
    i = 0
    while True:
        try:
            result.append(obj[str(i)])
            i += 1
        except KeyError:
            break
    return (result, data)


def _get_binary(data, as_class, tz_aware):
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


def _get_oid(data, as_class, tz_aware):
    return (ObjectId(data[:12]), data[12:])


def _get_boolean(data, as_class, tz_aware):
    return (data[0] == "\x01", data[1:])


def _get_date(data, as_class, tz_aware):
    seconds = float(struct.unpack("<q", data[:8])[0]) / 1000.0
    if tz_aware:
        return (datetime.datetime.fromtimestamp(seconds, utc), data[8:])
    return (datetime.datetime.utcfromtimestamp(seconds), data[8:])


def _get_code_w_scope(data, as_class, tz_aware):
    (_, data) = _get_int(data)
    (code, data) = _get_string(data, as_class, tz_aware)
    (scope, data) = _get_object(data, as_class, tz_aware)
    return (Code(code, scope), data)


def _get_null(data, as_class, tz_aware):
    return (None, data)


def _get_regex(data, as_class, tz_aware):
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


def _get_ref(data, as_class, tz_aware):
    (collection, data) = _get_c_string(data[4:])
    (oid, data) = _get_oid(data)
    return (DBRef(collection, oid), data)


def _get_timestamp(data, as_class, tz_aware):
    (inc, data) = _get_int(data, unsigned=True)
    (timestamp, data) = _get_int(data, unsigned=True)
    return (Timestamp(timestamp, inc), data)


def _get_long(data, as_class, tz_aware):
    # Have to cast to long; on 32-bit unpack may return an int.
    return (long(struct.unpack("<q", data[:8])[0]), data[8:])


_element_getter = {
    "\x01": _get_number,
    "\x02": _get_string,
    "\x03": _get_object,
    "\x04": _get_array,
    "\x05": _get_binary,
    "\x06": _get_null,  # undefined
    "\x07": _get_oid,
    "\x08": _get_boolean,
    "\x09": _get_date,
    "\x0A": _get_null,
    "\x0B": _get_regex,
    "\x0C": _get_ref,
    "\x0D": _get_string,  # code
    "\x0E": _get_string,  # symbol
    "\x0F": _get_code_w_scope,
    "\x10": _get_int,  # number_int
    "\x11": _get_timestamp,
    "\x12": _get_long,
    "\xFF": lambda x, y, z: (MinKey(), x),
    "\x7F": lambda x, y, z: (MaxKey(), x)}


def _element_to_dict(data, as_class, tz_aware):
    element_type = data[0]
    (element_name, data) = _get_c_string(data[1:])
    (value, data) = _element_getter[element_type](data, as_class, tz_aware)
    return (element_name, value, data)


def _elements_to_dict(data, as_class, tz_aware):
    result = as_class()
    while data:
        (key, value, data) = _element_to_dict(data, as_class, tz_aware)
        result[key] = value
    return result


def _bson_to_dict(data, as_class, tz_aware):
    obj_size = struct.unpack("<i", data[:4])[0]
    if len(data) < obj_size:
        raise InvalidBSON("objsize too large")
    if data[obj_size - 1] != "\x00":
        raise InvalidBSON("bad eoo")
    elements = data[4:obj_size - 1]
    return (_elements_to_dict(elements, as_class, tz_aware), data[obj_size:])
if _use_c:
    _bson_to_dict = _cbson._bson_to_dict


def _element_to_bson(key, value, check_keys):
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
        scope = _dict_to_bson(value.scope, False, False)
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
        return "\x03" + name + _dict_to_bson(value, check_keys, False)
    if isinstance(value, (list, tuple)):
        as_dict = SON(zip([str(i) for i in range(len(value))], value))
        return "\x04" + name + _dict_to_bson(as_dict, check_keys, False)
    if isinstance(value, ObjectId):
        return "\x07" + name + value.binary
    if value is True:
        return "\x08" + name + "\x01"
    if value is False:
        return "\x08" + name + "\x00"
    if isinstance(value, int):
        # TODO this is a really ugly way to check for this...
        if value > 2 ** 64 / 2 - 1 or value < -2 ** 64 / 2:
            raise OverflowError("BSON can only handle up to 8-byte ints")
        if value > 2 ** 32 / 2 - 1 or value < -2 ** 32 / 2:
            return "\x12" + name + struct.pack("<q", value)
        return "\x10" + name + struct.pack("<i", value)
    if isinstance(value, long):
        # XXX No long type in Python 3
        if value > 2 ** 64 / 2 - 1 or value < -2 ** 64 / 2:
            raise OverflowError("BSON can only handle up to 8-byte ints")
        return "\x12" + name + struct.pack("<q", value)
    if isinstance(value, datetime.datetime):
        if value.utcoffset() is not None:
            value = value - value.utcoffset()
        millis = int(calendar.timegm(value.timetuple()) * 1000 +
                     value.microsecond / 1000)
        return "\x09" + name + struct.pack("<q", millis)
    if isinstance(value, Timestamp):
        time = struct.pack("<I", value.time)
        inc = struct.pack("<I", value.inc)
        return "\x11" + name + inc + time
    if value is None:
        return "\x0A" + name
    if isinstance(value, RE_TYPE):
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
        return "\x0B" + name + _make_c_string(pattern, True) + \
            _make_c_string(flags)
    if isinstance(value, DBRef):
        return _element_to_bson(key, value.as_doc(), False)
    if isinstance(value, MinKey):
        return "\xFF" + name
    if isinstance(value, MaxKey):
        return "\x7F" + name

    raise InvalidDocument("cannot convert value of type %s to bson" %
                          type(value))


def _dict_to_bson(dict, check_keys, top_level=True):
    try:
        elements = ""
        if top_level and "_id" in dict:
            elements += _element_to_bson("_id", dict["_id"], False)
        for (key, value) in dict.iteritems():
            if not top_level or key != "_id":
                elements += _element_to_bson(key, value, check_keys)
    except AttributeError:
        raise TypeError("encoder expected a mapping type but got: %r" % dict)

    length = len(elements) + 5
    return struct.pack("<i", length) + elements + "\x00"
if _use_c:
    _dict_to_bson = _cbson._dict_to_bson


def _to_dicts(data, as_class=dict, tz_aware=True):
    """DEPRECATED - `_to_dicts` has been renamed to `decode_all`.

    .. versionchanged:: 1.9
       Deprecated in favor of :meth:`decode_all`.
    .. versionadded:: 1.7
       The `as_class` parameter.
    """
    warnings.warn("`_to_dicts` has been renamed to `decode_all`",
                  DeprecationWarning)
    return decode_all(data, as_class, tz_aware)


def decode_all(data, as_class=dict, tz_aware=True):
    """Decode BSON data to multiple documents.

    `data` must be a string of concatenated, valid, BSON-encoded
    documents.

    :Parameters:
      - `data`: BSON data
      - `as_class` (optional): the class to use for the resulting
        documents
      - `tz_aware` (optional): if ``True``, return timezone-aware
        :class:`~datetime.datetime` instances

    .. versionadded:: 1.9
    """
    docs = []
    while len(data):
        (doc, data) = _bson_to_dict(data, as_class, tz_aware)
        docs.append(doc)
    return docs
if _use_c:
    decode_all = _cbson.decode_all


def is_valid(bson):
    """Check that the given string represents valid :class:`BSON` data.

    Raises :class:`TypeError` if `bson` is not an instance of
    :class:`str`.  Returns ``True`` if `bson` is valid :class:`BSON`,
    ``False`` otherwise.

    :Parameters:
      - `bson`: the data to be validated
    """
    if not isinstance(bson, str):
        raise TypeError("BSON data must be an instance of a subclass of str")

    try:
        (_, remainder) = _bson_to_dict(bson, dict, True)
        return remainder == ""
    except:
        return False


class BSON(str):
    """BSON (Binary JSON) data.
    """

    @classmethod
    def from_dict(cls, dct, check_keys=False):
        """DEPRECATED - `from_dict` has been renamed to `encode`.

        .. versionchanged:: 1.9
           Deprecated in favor of :meth:`encode`
        """
        warnings.warn("`from_dict` has been renamed to `encode`",
                      DeprecationWarning)
        return cls.encode(dct, check_keys)

    @classmethod
    def encode(cls, document, check_keys=False):
        """Encode a document to a new :class:`BSON` instance.

        A document can be any mapping type (like :class:`dict`).

        Raises :class:`TypeError` if `document` is not a mapping type,
        or contains keys that are not instances of
        :class:`basestring`.  Raises
        :class:`~bson.errors.InvalidDocument` if `document` cannot be
        converted to :class:`BSON`.

        :Parameters:
          - `document`: mapping type representing a document
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising :class:`~bson.errors.InvalidDocument` in
            either case

        .. versionadded:: 1.9
        """
        return cls(_dict_to_bson(document, check_keys))

    def to_dict(self, as_class=dict, tz_aware=False):
        """DEPRECATED - `to_dict` has been renamed to `decode`.

        .. versionchanged:: 1.9
           Deprecated in favor of :meth:`decode`
        .. versionadded:: 1.8
           The `tz_aware` parameter.
        .. versionadded:: 1.7
           The `as_class` parameter.
        """
        warnings.warn("`to_dict` has been renamed to `decode`",
                      DeprecationWarning)
        return self.decode(as_class, tz_aware)

    def decode(self, as_class=dict, tz_aware=False):
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

        .. versionadded:: 1.9
        """
        (document, _) = _bson_to_dict(self, as_class, tz_aware)
        return document


def has_c():
    """Is the C extension installed?

    .. versionadded:: 1.9
    """
    try:
        from bson import _cbson
        return True
    except ImportError:
        return False
