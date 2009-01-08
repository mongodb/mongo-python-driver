"""Tools for dealing with Mongo's BSON data representation.

Generally not needed to be used by application developers."""

import unittest
import types
import struct
import random
import re
import datetime
import time

from test import test_data, qcheck

class InvalidBSON(ValueError):
    """Raised when trying to create a BSON object from invalid data.
    """

class InvalidDocument(ValueError):
    """Raised when trying to create a BSON object from an invalid document.
    """

def _get_int(data):
    try:
        int = struct.unpack("<i", data[:4])[0]
    except struct.error:
        raise InvalidBSON()

    return (int, data[4:])

def _get_c_string(data):
    try:
        end = data.index("\x00")
    except ValueError:
        raise InvalidBSON()

    return (unicode(data[:end], "utf-8"), data[end + 1:])

def _make_c_string(string):
    string = string.encode("utf-8")
    try:
        string.index("\x00")
        raise InvalidDocument("cannot encode string %r: contains '\\x00'" % string)
    except InvalidDocument:
        raise
    except ValueError:
        pass
    return string + "\x00"

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
    assert len(data) >= length
    return data[length:]

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
    (namespace, data) = _get_c_string(data)
    return _validate_oid(data)

_validate_code = _validate_string

_validate_symbol = _validate_string

def _validate_number_int(data):
    assert len(data) >= 4
    return data[4:]

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
# TODO look into this
#    "\x0F": _validate_code_w_scope,
    "\x10": _validate_number_int,
}

def _validate_element_data(type, data):
    try:
        return _element_validator[type](data)
    except KeyError:
        raise InvalidBSON("unrecognized type: %s" % type)

def _validate_element(data, valid_name):
    element_type = data[0]
    (element_name, data) = _get_c_string(data[1:])
    if valid_name:
        assert valid_name.match(element_name), "%r doesn't match %s" % (element_name, valid_name.pattern)
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
    object = data[4:obj_size]
    assert len(object)

    eoo = object[-1]
    assert eoo == "\x00"

    elements = object[:-1]
    _validate_elements(elements, valid_name)

    return data[obj_size:]

def _get_number(data):
    return (struct.unpack("<d", data[:8])[0], data[8:])

def _get_string(data):
    return _get_c_string(data[4:])

def _get_object(data):
    return _document_to_dict(data)

def _get_array(data):
    (dict, data) = _get_object(data)
    result = []
    i = 0
    while True:
        try:
            result.append(dict[str(i)])
            i += 1
        except KeyError:
            break
    return (result, data)

def _get_binary(data):
    (length, data) = _get_int(data)
    return (data[:length], data[length:])

def _get_boolean(data):
    return (data[0] == "\x01", data[1:])

def _get_date(data):
    seconds = float(struct.unpack("<q", data[:8])[0]) / 1000.0
    return (datetime.datetime.fromtimestamp(seconds), data[8:])

def _get_null(data):
    return (None, data)

_re_stack = []

def _get_regex(data):
    (pattern, data) = _get_c_string(data)
    print "out %r" % pattern
    (bson_flags, data) = _get_c_string(data)
    flags = 0
    if bson_flags.find("i") > -1:
        flags |= re.IGNORECASE
    if bson_flags.find("m") > -1:
        flags |= re.MULTILINE
    print "out %r" % flags
    res = re.compile(pattern, flags)
    other = _re_stack.pop(0)
    assert res.pattern == other.pattern, "%r %r" % (res.pattern, other.pattern)
    assert res == other, "%r %r" % (res.pattern, other.pattern)
    return (re.compile(pattern, flags), data)

_element_getter = {
    "\x01": _get_number,
    "\x02": _get_string,
    "\x03": _get_object,
    "\x04": _get_array,
    "\x05": _get_binary,
    "\x06": _get_null, # undefined
#    "\x07": _get_oid,
    "\x08": _get_boolean,
    "\x09": _get_date,
    "\x0A": _get_null,
    "\x0B": _get_regex,
#    "\x0C": _get_ref,
#    "\x0D": _get_code,
#    "\x0E": _get_symbol,
#    "\x0F": _validate_code_w_scope,
    "\x10": _get_int, # number_int
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

def _document_to_dict(data):
    obj_size = struct.unpack("<i", data[:4])[0]
    elements = data[4:obj_size - 1]
    return (_elements_to_dict(elements), data[obj_size:])

def _int_to_bson(int):
    return struct.pack("<i", int)

def _int_64_to_bson(int):
    return struct.pack("<q", int)

_RE_TYPE = type(_valid_array_name)
def _value_to_bson(value):
    if isinstance(value, types.FloatType):
        return ("\x01", struct.pack("<d", value))
    if isinstance(value, types.UnicodeType):
        cstring = _make_c_string(value)
        length = _int_to_bson(len(cstring))
        return ("\x02", length + cstring)
    if isinstance(value, types.DictType):
        return ("\x03", BSON.from_dict(value))
    if isinstance(value, types.ListType):
        as_dict = dict(zip([str(i) for i in range(len(value))], value))
        return ("\x04", BSON.from_dict(as_dict))
    if isinstance(value, types.StringType):
        return ("\x05", _int_to_bson(len(value)) + value)
    if isinstance(value, types.BooleanType):
        if value:
            return ("\x08", "\x01")
        return ("\x08", "\x00")
    if isinstance(value, datetime.datetime):
        millis = int(time.mktime(value.timetuple()) * 1000 + value.microsecond / 1000)
        return ("\x09", _int_64_to_bson(millis))
    if isinstance(value, types.NoneType):
        return ("\x0A", "")
    if isinstance(value, _RE_TYPE):
        _re_stack.append(value)
        pattern = value.pattern
        print "in %r" % pattern
        print "in %r" % value.flags
        flags = "g" # TODO should it be global by default?
        if value.flags & re.IGNORECASE:
            flags += "i"
        if value.flags & re.MULTILINE:
            flags += "m"
        return ("\x0B", _make_c_string(pattern) + _make_c_string(flags))
    if isinstance(value, types.IntType):
        return ("\x10", _int_to_bson(value))
    raise InvalidDocument("cannot convert value of type %s to bson" % type(value))

def _element_to_bson(key, value):
    if not isinstance(key, types.StringTypes):
        raise TypeError("all keys must be instances of (str, unicode)")

    element_name = _make_c_string(key)
    (element_type, element_data) = _value_to_bson(value)

    return element_type + element_name + element_data

def is_valid(bson):
    """Validate that the given string represents valid BSON data.

    Raises TypeError if the data is not an instance of a subclass of str.
    Returns True if the data represents a valid BSON object, False otherwise.

    Arguments:
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

        The data given must be a string instance and represent a valid BSON
        object, otherwise an TypeError or InvalidBSON exception is raised.

        Arguments:
        - `bson`: the initial data
        """
        if not is_valid(bson):
            raise InvalidBSON()

        return str.__new__(cls, bson)

    @classmethod
    def from_dict(cls, dict):
        """Create a new BSON object from a python mapping type (like dict).

        Raises TypeError if the argument is not a mapping type, or contains keys
        that are not instance of (str, unicode). Raises InvalidDocument if the
        dictionary cannot be converted to BSON.

        Arguments:
        - `dict`: mapping type representing a Mongo document
        """
        try:
            elements = "".join([_element_to_bson(key, value) for (key, value) in dict.iteritems()])
        except AttributeError:
            raise TypeError("argument to from_dict must be a mapping type")

        length = len(elements) + 5
        bson = _int_to_bson(length) + elements + "\x00"
        return cls(bson)

    def to_dict(self):
        """Get the dictionary representation of this data."""
        (dict, _) = _document_to_dict(self)
        return dict

class TestBSON(unittest.TestCase):
    def setUp(self):
        pass

    def test_basic_validation(self):
        self.assertRaises(TypeError, is_valid, 100)
        self.assertRaises(TypeError, is_valid, u"test")
        self.assertRaises(TypeError, is_valid, 10.4)

        self.assertFalse(is_valid("test"))

        # the simplest valid BSON document
        self.assertTrue(is_valid("\x05\x00\x00\x00\x00"))
        self.assertTrue(is_valid(BSON("\x05\x00\x00\x00\x00")))
        self.assertFalse(is_valid("\x04\x00\x00\x00\x00"))
        self.assertFalse(is_valid("\x05\x00\x00\x00\x01"))
        self.assertFalse(is_valid("\x05\x00\x00\x00"))
        self.assertFalse(is_valid("\x05\x00\x00\x00\x00\x00"))

        for data in test_data.valid_bson:
            self.assertTrue(is_valid(data))

    def test_random_data_is_not_bson(self):
        qcheck.check_unittest(self, qcheck.isnt(is_valid), qcheck.gen_string(qcheck.gen_range(0, 40)))

    def test_basic_from_dict(self):
        self.assertRaises(TypeError, BSON.from_dict, 100)
        self.assertRaises(TypeError, BSON.from_dict, "hello")
        self.assertRaises(TypeError, BSON.from_dict, None)
        self.assertRaises(TypeError, BSON.from_dict, [])

        self.assertEqual(BSON.from_dict({}), BSON("\x05\x00\x00\x00\x00"))
        self.assertEqual(BSON.from_dict({"test": u"hello world"}),
                         "\x1B\x00\x00\x00\x02\x74\x65\x73\x74\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C\x64\x00\x00")
        self.assertEqual(BSON.from_dict({u"mike": 100}),
                         "\x0F\x00\x00\x00\x10\x6D\x69\x6B\x65\x00\x64\x00\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"hello": 1.5}),
                         "\x14\x00\x00\x00\x01\x68\x65\x6C\x6C\x6F\x00\x00\x00\x00\x00\x00\x00\xF8\x3F\x00")
        self.assertEqual(BSON.from_dict({"true": True}),
                         "\x0C\x00\x00\x00\x08\x74\x72\x75\x65\x00\x01\x00")
        self.assertEqual(BSON.from_dict({"false": False}),
                         "\x0D\x00\x00\x00\x08\x66\x61\x6C\x73\x65\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"empty": []}),
                         "\x11\x00\x00\x00\x04\x65\x6D\x70\x74\x79\x00\x05\x00\x00\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"none": {}}),
                         "\x10\x00\x00\x00\x03\x6E\x6F\x6E\x65\x00\x05\x00\x00\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"test": "test"}),
                         "\x13\x00\x00\x00\x05\x74\x65\x73\x74\x00\x04\x00\x00\x00\x74\x65\x73\x74\x00")
        self.assertEqual(BSON.from_dict({"test": None}),
                         "\x0B\x00\x00\x00\x0A\x74\x65\x73\x74\x00\x00")
        self.assertEqual(BSON.from_dict({"date": datetime.datetime(2007, 1, 7, 19, 30, 11)}),
                         "\x13\x00\x00\x00\x09\x64\x61\x74\x65\x00\x38\xBE\x1C\xFF\x0F\x01\x00\x00\x00")
#        self.assertEqual(BSON.from_dict({"regex": re.compile("a*b", re.IGNORECASE)}),
#                         "\x13\x00\x00\x00\x0B\x72\x65\x67\x65\x78\x00\x61\x2A\x62\x00\x67\x69\x00\x00")

    def test_from_then_to_dict(self):
        def helper(dict):
            self.assertEqual(dict, (BSON.from_dict(dict)).to_dict())
        helper({})
        helper({"test": u"hello"})
        helper({"mike": -10120})
        helper({u"hello": 0.0013109})
        helper({"something": True})
        helper({"false": False})
        helper({"an array": [1, True, 3.8, u"world"]})
        helper({"an object": {"test": u"something"}})

#        helper({"re": re.compile(u"", re.MULTILINE)})

        def from_then_to_dict(dict):
            return dict == (BSON.from_dict(dict)).to_dict()

        qcheck.check_unittest(self, from_then_to_dict, qcheck.gen_mongo_dict(3))

if __name__ == "__main__":
    unittest.main()
