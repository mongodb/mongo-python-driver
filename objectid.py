"""Representation of an ObjectId for Mongo."""

import random
import types
from errors import InvalidId

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
        if id is None:
            self.__generate()
        else:
            self.__validate(id)

    def __generate(self):
        """Generate a new value for this ObjectId.
        """
        # TODO for now, just generate 12 random bytes. this will change when we decide on an _id algorithm...
        self.__new = True
        oid = ""
        for _ in range(12):
            oid += chr(random.randint(0, 255))

        self.__id = oid

    def __validate(self, oid):
        """Validate and use the given id for this ObjectId.

        Raises TypeError if id is not an instance of (str, ObjectId) and
        InvalidId if it is not a valid ObjectId.

        Arguments:
        - `id`: a valid ObjectId
        """
        self.__new = False
        if isinstance(oid, ObjectId):
            self.__id = oid.__id
        elif isinstance(oid, types.StringType):
            if len(oid) == 12:
                self.__id = oid
            else:
                raise InvalidId("%s is not a valid ObjectId" % oid)
        else:
            raise TypeError("id must be an instance of (str, ObjectId), not %s" % type(oid))

    def __str__(self):
        return self.__id

    def __repr__(self):
        return "ObjectId(%r)" % self.__id

    def __cmp__(self, other):
        if isinstance(other, ObjectId):
            return cmp(self.__id, other.__id)
        return NotImplemented

    def _use(self):
        self.__new = False

    def is_new(self):
        """Return True if this ObjectId has not been used as an _id field.
        """
        return self.__new
