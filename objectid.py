"""Representation of an ObjectId for Mongo."""

import random
import types
import unittest

class InvalidId(ValueError):
    """Raised when trying to create an ObjectId from invalid data.
    """

class ObjectId(object):
    """A Mongo ObjectId.
    """
    def __init__(self, id=None):
        """Initialize a new ObjectId.

        If no value of id is given, create a new (unique) ObjectId. If given id
        is an instance of (string, ObjectId) validate it and use that.
        Otherwise, a TypeError is raised. If given an invalid id, InvalidId is
        raised.

        Arguments:
        - `id` (optional): a valid ObjectId
        """
        if id == None:
            self.__generate()
        else:
            self.__validate(id)

    def __generate(self):
        """Generate a new value for this ObjectId.
        """
        # TODO for now, just generate 12 random bytes. this will change when we decide on an _id algorithm...
        id = ""
        for _ in range(12):
            id += chr(random.randint(0, 255))

        self.__id = id

    def __validate(self, id):
        """Validate and use the given id for this ObjectId.

        Raises TypeError if id is not an instance of (str, ObjectId) and
        InvalidId if it is not a valid ObjectId.

        Arguments:
        - `id`: a valid ObjectId
        """
        if isinstance(id, ObjectId):
            self.__id = id.__id
        elif isinstance(id, types.StringType):
            if len(id) == 12:
                self.__id = id
            else:
                raise InvalidId("%s is not a valid ObjectId" % id)
        else:
            raise TypeError("id must be an instance of (str, ObjectId), not %s" % type(id))

    def __str__(self):
        return self.__id

    def __repr__(self):
        return "ObjectId('%s')" % self.__id

    def __cmp__(self, other):
        if isinstance(other, ObjectId):
            return cmp(self.__id, other.__id)
        return NotImplemented

class TestObjectId(unittest.TestCase):
    def setUp(self):
        pass

    def test_creation(self):
        self.assertRaises(TypeError, ObjectId, 4)
        self.assertRaises(TypeError, ObjectId, u"hello")
        self.assertRaises(TypeError, ObjectId, 175.0)
        self.assertRaises(TypeError, ObjectId, {"test": 4})
        self.assertRaises(TypeError, ObjectId, ["something"])
        self.assertRaises(InvalidId, ObjectId, "")
        self.assertRaises(InvalidId, ObjectId, "12345678901")
        self.assertRaises(InvalidId, ObjectId, "1234567890123")
        self.assertTrue(ObjectId())
        self.assertTrue(ObjectId("123456789012"))
        a = ObjectId()
        self.assertTrue(ObjectId(a))

    def test_repr_str(self):
        self.assertEqual(repr(ObjectId("123456789012")), "ObjectId('123456789012')")
        self.assertEqual(str(ObjectId("123456789012")), "123456789012")

    def test_cmp(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(a))
        self.assertEqual(ObjectId("123456789012"), ObjectId("123456789012"))
        self.assertNotEqual(ObjectId(), ObjectId())
        self.assertNotEqual(ObjectId("123456789012"), "123456789012")

if __name__ == "__main__":
    unittest.main()
