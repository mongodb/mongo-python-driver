"""Tools for dealing with Mongo's BSON data representation.

Generally not needed to be used by application developers."""

import unittest
import types
import struct
import random
import re

class InvalidBSON(ValueError):
    """Raised when trying to create a BSON object from invalid data.
    """

def _get_int(data):
    try:
        int = struct.unpack("<I", data[:4])[0]
    except struct.error:
        raise InvalidBSON()

    return (int, data[4:])

def _get_c_string(data):
    try:
        end = data.index("\x00")
    except ValueError:
        raise InvalidBSON()

    return (unicode(data[:end - 1], "utf-8"), data[end:])

def _validate_number(data):
    assert len(data) >= 8
    return data[8:]

def _validate_string(data):
    (length, data) = _get_int(data)
    assert len(data) >= length
    assert data[length - 1] == "\x00"
    return data[length:]

_valid_object_name = re.compile("^.*$")
def _validate_object(data):
    return _validate_document(data, _valid_object_name)

_valid_array_name = re.compile("^\d+$")
def _validate_array(data):
    return _validate_document(data, _valid_array_name)

def _validate_binary(data):
    (length, data) = _get_int(data)
    assert len(data) >= length
    return data[length:]

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

def _validate_null(data):
    return data

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
    "\x06": _validate_oid,
    "\x07": _validate_boolean,
    "\x08": _validate_date,
    "\x09": _validate_null,
    "\x0A": _validate_regex,
    "\x0B": _validate_ref,
    "\x0C": _validate_code,
    "\x0D": _validate_symbol,
# TODO look into this
#    "\x0E": _validate_code_w_scope,
    "\x0F": _validate_number_int,
}

def _validate_element_data(type, data):
    try:
        return _element_validator[type](data)
    except KeyError:
        raise InvalidBSON()

def _validate_element(data, valid_name):
    element_type = data[0]
    (element_name, data) = _get_c_string(data[1:])
    assert valid_name.match(element_name)
    return _validate_element_data(element_type, data)

def _validate_elements(data, valid_name):
    while data:
        data = _validate_element(data, valid_name)

def _validate_document(data, valid_name=_valid_object_name):
    try:
        obj_size = struct.unpack("<I", data[:4])[0]
    except struct.error:
        raise InvalidBSON()

    assert obj_size <= len(data)
    object = data[4:obj_size + 4]

    eoo = object[-1]
    assert eoo == "\x00"

    elements = object[:-1]
    _validate_elements(elements, valid_name)

    return data[obj_size:]

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
        """Initialize a new BSON object with some data

        The data given must be a string instance and represent a valid BSON
        object, otherwise an TypeError or InvalidBSON exception is raised.

        Arguments:
        - `bson`: the initial data
        """
        if not is_valid(bson):
            raise InvalidBSON()

        return str.__new__(cls, bson)

class TestBSON(unittest.TestCase):
    def setUp(self):
        pass

    def testValidate(self):
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

        def random_char(size):
            return chr(random.randint(0, 255))

        def random_string(size):
            return "".join(random_list(random_char)(size))

        def random_list(generator):
            return lambda size: [generator(size) for _ in range(size)]

        # random strings are not valid BSON (with extremely high probability)
        for i in range(100):
            self.assertFalse(is_valid(random_string(i)))

if __name__ == "__main__":
    unittest.main()
