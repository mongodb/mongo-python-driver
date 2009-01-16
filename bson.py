"""Tools for dealing with Mongo's BSON data representation.

Generally not needed to be used by application developers."""

import unittest
import types
import struct
import random
import re
import datetime
import time
import logging
import glob
import sys

from test import test_data, qcheck
from objectid import ObjectId
from dbref import DBRef
from son import SON
from errors import InvalidBSON, InvalidDocument, UnsupportedTag

_logger = logging.getLogger("mongo.bson")
# _logger.setLevel(logging.DEBUG)
# _logger.addHandler(logging.StreamHandler())

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
    _logger.debug("validating number")
    assert len(data) >= 8
    return data[8:]

def _validate_string(data):
    _logger.debug("validating string")
    (length, data) = _get_int(data)
    assert len(data) >= length
    assert data[length - 1] == "\x00"
    return data[length:]

def _validate_object(data):
    _logger.debug("validating object")
    return _validate_document(data, None)

_valid_array_name = re.compile("^\d+$")
def _validate_array(data):
    _logger.debug("validating array")
    return _validate_document(data, _valid_array_name)

def _validate_binary(data):
    _logger.debug("validating binary")
    (length, data) = _get_int(data)
    assert len(data) >= length
    return data[length:]

def _validate_undefined(data):
    _logger.debug("validating undefined")
    return data

_OID_SIZE = 12
def _validate_oid(data):
    _logger.debug("validating oid")
    assert len(data) >= _OID_SIZE
    return data[_OID_SIZE:]

def _validate_boolean(data):
    _logger.debug("validating boolean")
    assert len(data) >= 1
    return data[1:]

_DATE_SIZE = 8
def _validate_date(data):
    _logger.debug("validating date")
    assert len(data) >= _DATE_SIZE
    return data[_DATE_SIZE:]

_validate_null = _validate_undefined

def _validate_regex(data):
    _logger.debug("validating regex")
    (regex, data) = _get_c_string(data)
    (options, data) = _get_c_string(data)
    return data

def _validate_ref(data):
    _logger.debug("validating ref")
    data = _validate_string(data)
    return _validate_oid(data)

_validate_code = _validate_string

# still not sure what is actually stored here, but i know how big it is...
def _validate_code_w_scope(data):
    _logger.debug("validating code w/ scope")
    (length, data) = _get_int(data)
    assert len(data) >= length + 1
    return data[length + 1:]

_validate_symbol = _validate_string

def _validate_number_int(data):
    _logger.debug("validating int")
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
    "\x0F": _validate_code_w_scope,
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
    obj = data[4:obj_size]
    assert len(obj)

    eoo = obj[-1]
    assert eoo == "\x00"

    elements = obj[:-1]
    _validate_elements(elements, valid_name)

    return data[obj_size:]

def _get_number(data):
    _logger.debug("unpacking number")
    return (struct.unpack("<d", data[:8])[0], data[8:])

def _get_string(data):
    _logger.debug("unpacking string")
    return _get_c_string(data[4:])

def _get_object(data):
    _logger.debug("unpacking object")
    return _document_to_dict(data)

def _get_array(data):
    _logger.debug("unpacking array")
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
    _logger.debug("unpacking binary")
    (length, data) = _get_int(data)
    return (data[:length], data[length:])

def _get_oid(data):
    _logger.debug("unpacking oid")
    oid = _shuffle_oid(data[:12])
    return (ObjectId(oid), data[12:])

def _get_boolean(data):
    _logger.debug("unpacking boolean")
    return (data[0] == "\x01", data[1:])

def _get_date(data):
    _logger.debug("unpacking date")
    seconds = float(struct.unpack("<q", data[:8])[0]) / 1000.0
    return (datetime.datetime.fromtimestamp(seconds), data[8:])

def _get_null(data):
    _logger.debug("unpacking null")
    return (None, data)

def _get_regex(data):
    _logger.debug("unpacking regex")
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
    _logger.debug("unpacking ref")
    (collection, data) = _get_c_string(data[4:])
    (oid, data) = _get_oid(data)
    return (DBRef(collection, oid), data)

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
#    "\x0F": _get_code_w_scope
    "\x10": _get_int, # number_int
}

def _element_to_dict(data):
    element_type = data[0]
    (element_name, data) = _get_c_string(data[1:])
    (value, data) = _element_getter[element_type](data)
    return (element_name, value, data)

def _elements_to_dict(data):
    result = SON()
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

def _shuffle_oid(data):
    return data[7::-1] + data[:7:-1]

_RE_TYPE = type(_valid_array_name)
def _value_to_bson(value):
    if isinstance(value, types.FloatType):
        _logger.debug("packing number")
        return ("\x01", struct.pack("<d", value))
    if isinstance(value, types.UnicodeType):
        _logger.debug("packing string")
        cstring = _make_c_string(value)
        length = _int_to_bson(len(cstring))
        return ("\x02", length + cstring)
    if isinstance(value, (types.DictType, SON)):
        _logger.debug("packing object")
        return ("\x03", BSON.from_dict(value))
    if isinstance(value, types.ListType):
        _logger.debug("packing array")
        as_dict = SON(zip([str(i) for i in range(len(value))], value))
        return ("\x04", BSON.from_dict(as_dict))
    if isinstance(value, types.StringType):
        _logger.debug("packing binary")
        return ("\x05", _int_to_bson(len(value)) + value)
    if isinstance(value, ObjectId):
        _logger.debug("packing oid")
        return ("\x07", _shuffle_oid(str(value)))
    if isinstance(value, types.BooleanType):
        _logger.debug("packing boolean")
        if value:
            return ("\x08", "\x01")
        return ("\x08", "\x00")
    if isinstance(value, datetime.datetime):
        _logger.debug("packing date")
        millis = int(time.mktime(value.timetuple()) * 1000 + value.microsecond / 1000)
        return ("\x09", _int_64_to_bson(millis))
    if isinstance(value, types.NoneType):
        _logger.debug("packing null")
        return ("\x0A", "")
    if isinstance(value, _RE_TYPE):
        _logger.debug("packing regex")
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
        return ("\x0B", _make_c_string(pattern) + _make_c_string(flags))
    if isinstance(value, DBRef):
        _logger.debug("packing ref")
        ns = _make_c_string(value.collection())
        return ("\x0C", _int_to_bson(len(ns)) + ns + _shuffle_oid(str(value.id())))
    if isinstance(value, types.IntType):
        _logger.debug("packing int")
        return ("\x10", _int_to_bson(value))
    raise InvalidDocument("cannot convert value of type %s to bson" % type(value))

def _where_value_to_bson(value):
    _logger.debug("packing code")
    if not isinstance(value, types.StringTypes):
        raise TypeError("$where value must be an instance of (str, unicode)")
    cstring = _make_c_string(value)
    length = _int_to_bson(len(cstring))
    return ("\x0D", length + cstring)

def _element_to_bson(key, value):
    if not isinstance(key, types.StringTypes):
        raise TypeError("all keys must be instances of (str, unicode)")

    element_name = _make_c_string(key)
    if key == "$where":
        (element_type, element_data) = _where_value_to_bson(value)
    else:
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

def to_dicts(data):
    """Convert binary data to sequence of SON objects.

    Data must be concatenated strings of valid BSON data.
    """
    dicts = []
    while len(data):
        (son, data) = _document_to_dict(data)
        dicts.append(son)
    return dicts

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
        (son, _) = _document_to_dict(self)
        return son

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

    def test_basic_to_dict(self):
        self.assertEqual({"test": u"hello world"},
                         BSON("\x1B\x00\x00\x00\x0E\x74\x65\x73\x74\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C\x64\x00\x00").to_dict())
        self.assertEqual([{"test": u"hello world"}, {}],
                         to_dicts("\x1B\x00\x00\x00\x0E\x74\x65\x73\x74\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C\x64\x00\x00\x05\x00\x00\x00\x00"))

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
        self.assertEqual(BSON.from_dict({"regex": re.compile("a*b", re.IGNORECASE)}),
                         "\x12\x00\x00\x00\x0B\x72\x65\x67\x65\x78\x00\x61\x2A\x62\x00\x69\x00\x00")
        self.assertRaises(TypeError, BSON.from_dict, {"$where": 5})
        self.assertEqual(BSON.from_dict({"$where": "test"}),
                         "\x16\x00\x00\x00\x0D\x24\x77\x68\x65\x72\x65\x00\x05\x00\x00\x00\x74\x65\x73\x74\x00\x00")
        self.assertEqual(BSON.from_dict({"$where": u"test"}),
                         "\x16\x00\x00\x00\x0D\x24\x77\x68\x65\x72\x65\x00\x05\x00\x00\x00\x74\x65\x73\x74\x00\x00")
        a = ObjectId("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B")
        self.assertEqual(BSON.from_dict({"oid": a}),
                         "\x16\x00\x00\x00\x07\x6F\x69\x64\x00\x07\x06\x05\x04\x03\x02\x01\x00\x0B\x0A\x09\x08\x00")
        self.assertEqual(BSON.from_dict({"ref": DBRef("coll", a)}),
                         "\x1F\x00\x00\x00\x0C\x72\x65\x66\x00\x05\x00\x00\x00\x63\x6F\x6C\x6C\x00\x07\x06\x05\x04\x03\x02\x01\x00\x0B\x0A\x09\x08\x00")

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

        def from_then_to_dict(dict):
            return dict == (BSON.from_dict(dict)).to_dict()

        qcheck.check_unittest(self, from_then_to_dict, qcheck.gen_mongo_dict(3))

    def test_data_files(self):
        # TODO don't hardcode this, actually clone the repo
        data_files = "../mongo-qa/modules/bson_tests/tests/*/*.xson"
        generate = True

        for file_name in glob.iglob(data_files):
            f = open(file_name, "r")
            xml = f.read()
            f.close()

            try:
                doc = SON.from_xml(xml)
                bson = BSON.from_dict(doc)
            except UnsupportedTag:
                print "skipped file %s: %s" % (file_name, sys.exc_info()[1])
                continue
            except:
                print "failed to parse %s: %s" % (file_name, sys.exc_info()[1])
                continue

            try:
                f = open(file_name.replace(".xson", ".bson"), "r")
                expected = f.read()
                f.close()

                self.assertEqual(bson, expected, file_name)
                self.assertEqual(doc, bson.to_dict(), file_name)

            except IOError:
                if generate:
                    print "generating .bson for %s" % file_name

                    f = open(file_name.replace(".xson", ".bson"), "w")
                    f.write(bson)
                    f.close()

if __name__ == "__main__":
    unittest.main()
