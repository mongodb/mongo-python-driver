"""Tools for manipulating DBRefs (references to Mongo objects)."""

import types

from objectid import ObjectId

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
