"""Tools for manipulating DBRefs (references to Mongo objects)."""

from objectid import ObjectId
import unittest
import types

class DBRef(object):
    """A reference to an object stored in a Mongo database.
    """
    def __init__(self, collection, id):
        """Initialize a new DBRef.

        Raises TypeError if collection is not an instance of (str, unicode) or
        id is not an instance of ObjectId.

        Arguments:
        - `collection`: the collection the object is stored in
        - `id`: the value of the object's _id field
        """
        if not isinstance(collection, types.StringTypes):
            raise TypeError("collection must be an instance of (str, unicode)")
        if not isinstance(id, ObjectId):
            raise TypeError("id must be an instance of ObjectId")

        if isinstance(collection, types.StringType):
            collection = unicode(collection, "utf-8")

        self.__collection = collection
        self.__id = id

    def collection(self):
        """Get this DBRef's collection as unicode.
        """
        return self.__collection

    def id(self):
        """Get this DBRef's _id as an ObjectId.
        """
        return self.__id

    def __repr__(self):
        return "DBRef(" + repr(self.collection()) + ", " + repr(self.id()) + ")"

    def __cmp__(self, other):
        if isinstance(other, DBRef):
            return cmp([self.collection(), self.id()], [other.collection(), other.id()])
        return NotImplemented

class TestDBRef(unittest.TestCase):
    def setUp(self):
        pass

    def test_creation(self):
        a = ObjectId()
        self.assertRaises(TypeError, DBRef)
        self.assertRaises(TypeError, DBRef, "coll")
        self.assertRaises(TypeError, DBRef, "coll", 4)
        self.assertRaises(TypeError, DBRef, "coll", 175.0)
        self.assertRaises(TypeError, DBRef, 4, a)
        self.assertRaises(TypeError, DBRef, 1.5, a)
        self.assertRaises(TypeError, DBRef, a, a)
        self.assertRaises(TypeError, DBRef, None, a)
        self.assertTrue(DBRef("coll", a))
        self.assertTrue(DBRef(u"coll", a))

    def test_repr(self):
        self.assertEqual(repr(DBRef("coll", ObjectId("123456789012"))), "DBRef(u'coll', ObjectId('123456789012'))")
        self.assertEqual(repr(DBRef(u"coll", ObjectId("123456789012"))), "DBRef(u'coll', ObjectId('123456789012'))")

    def test_cmp(self):
        self.assertEqual(DBRef("coll", ObjectId("123456789012")), DBRef(u"coll", ObjectId("123456789012")))
        self.assertNotEqual(DBRef("coll", ObjectId("123456789012")), DBRef("col", ObjectId("123456789012")))
        self.assertNotEqual(DBRef("coll", ObjectId("123456789012")), DBRef("coll", ObjectId("123456789011")))
        self.assertNotEqual(DBRef("coll", ObjectId("123456789012")), 4)

if __name__ == "__main__":
    unittest.main()
