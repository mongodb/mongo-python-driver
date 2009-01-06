"""Tools for dealing with Mongo's BSON data representation.

Generally not needed to be used by application developers."""

import unittest
import types

class InvalidBSON(Exception):
    """Raised when trying to create a BSON object from invalid data.
    """


def is_valid(bson):
    """Validate that the given string represents valid BSON data.

    Returns True if the data represents a valid BSON object (which must be a
    subclass of str), False otherwise.

    Arguments:
    - `bson`: the data to be validated
    """
    if not isinstance(bson, types.StringType):
        return False
    return True


class BSON(str):
    """BSON data.

    Represents binary data storable in and retrievable from Mongo.
    """
    def __new__(cls, bson):
        """Initialize a new BSON object with some data

        The data given must be a string instance and represent a valid BSON
        object, otherwise an InvalidBSON exception is raised.

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
        self.assertFalse(is_valid(100))
        self.assertFalse(is_valid(u"test"))
        self.assertFalse(is_valid(10.4))
        self.assertFalse(is_valid("test"))
        # The simplest valid BSON document
        self.assertTrue(is_valid("\x05\x00\x00\x00\x00"))


if __name__ == "__main__":
    unittest.main()
