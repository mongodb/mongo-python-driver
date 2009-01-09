"""Driver for Mongo.

The database is accessed through an instance of the Mongo class."""

import unittest
import socket
import types
import traceback
import os
import struct

from son import SON
import bson
from objectid import ObjectId
from dbref import DBRef

class ConnectionException(IOError):
    """Raised when a connection to the database cannot be made or is lost.
    """

class InvalidCollection(ValueError):
    """Raised when an invalid collection name is used.
    """

class Mongo(object):
    """A connection to a Mongo database.
    """
    def __init__(self, host="localhost", port=27017):
        """Open a new connection to the database at host:port.

        Raises TypeError if host is not an instance of string or port is
        not an instance of int. Raises ConnectionException if the connection
        cannot be made.

        Arguments:
        - `host` (optional): the hostname or IPv4 address of the database to
                             connect to
        - `port` (optional): the port number on which to connect
        """
        if not isinstance(host, types.StringType):
            raise TypeError("host must be an instance of str")
        if not isinstance(port, types.IntType):
            raise TypeError("port must be an instance of int")

        self.__host = host
        self.__port = port
        self.__id = 1

        self.__connect()

    def __connect(self):
        """(Re-)connect to the database."""
        try:
            self.__connection = socket.socket()
            self.__connection.connect((self.__host, self.__port))
        except socket.error:
            raise ConnectionException("could not connect to %s:%s, got: %s" %
                                      (self.__host, self.__port, traceback.format_exc()))

    def _send_message(self, operation, data):
        """Say something to the database. Return the response.

        Arguments:
        - `operation`: the opcode of the message
        - `data`: the data to send
        """
        to_send = ""

        # header
        to_send += struct.pack("<i", 16 + len(data))
        to_send += struct.pack("<i", self.__id)
        self.__id += 1
        to_send += struct.pack("<i", 0) # responseTo
        to_send += struct.pack("<i", operation)

        to_send += data

        # TODO be more robust
        sent = self.__connection.send(to_send)

    def __cmp__(self, other):
        if isinstance(other, Mongo):
            return cmp((self.__host, self.__port), (other.__host, other.__port))
        return NotImplemented

    def __repr__(self):
        return "Mongo(" + repr(self.__host) + ", " + repr(self.__port) + ")"

    def __getattr__(self, name):
        """Get a collection of this database by name.

        Raises InvalidCollection if an invalid collection name is used.

        Arguments:
        - `name`: the name of the collection to get
        """
        return Collection(self, name)

    def __getitem__(self, name):
        return self.__getattr__(name)

class Collection(object):
    """A Mongo collection.
    """
    def __init__(self, database, name):
        """Get / create a Mongo collection.

        Raises TypeError if database is not an instance of Mongo or name is not
        an instance of (str, unicode). Raises InvalidCollection if name is not a
        valid collection name.

        Arguments:
        - `database`: the database to get a collection from
        - `name`: the name of the collection to get
        """
        if not isinstance(database, Mongo):
            raise TypeError("database must be an instance of Mongo")
        if not isinstance(name, types.StringTypes):
            raise TypeError("name must be an instance of (str, unicode)")

        if not name or ".." in name:
            raise InvalidCollection("collection names cannot be empty")
        if "$" in name:
            raise InvalidCollection("collection names must not contain '$'")
        if name[0] == "." or name[-1] == ".":
            raise InvalidCollection("collecion names must not start or end with '.'")

        self.__database = database
        self.__collection_name = name

    def __getattr__(self, name):
        """Get a sub-collection of this collection by name.

        Raises InvalidCollection if an invalid collection name is used.

        Arguments:
        - `name`: the name of the collection to get
        """
        return Collection(self.__database, u"%s.%s" % (self.__collection_name, name))

    def __getitem__(self, name):
        return self.__getattr__(name)

    def __repr__(self):
        return "Collection(%r, %r)" % (self.__database, self.__collection_name)

    def __cmp__(self, other):
        if isinstance(other, Collection):
            return cmp((self.__database, self.__collection_name), (other.__database, other.__collection_name))
        return NotImplemented

    def save(self, to_save):
        """Save a SON object in this collection.

        Raises TypeError if to_save is not an instance of (dict, SON).

        Arguments:
        - `to_save`: the SON object to be saved
        """
        if not isinstance(to_save, (types.DictType, SON)):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if hasattr(to_save, "_id"):
            assert isinstance(to_save["_id"], ObjectId), "'_id' must be an ObjectId"
        else:
            to_save["_id"] = ObjectId()

        # TODO possibly add _ns?

        # TODO support for named databases instead of just prepending test.
        message_data = "\x00\x00\x00\x00%s%s" % (bson._make_c_string("test." + self.__collection_name), bson.BSON.from_dict(to_save))

        self.__database._send_message(2002, message_data)

    def find_one(self, spec):
        pass

class TestMongo(unittest.TestCase):
    def setUp(self):
        self.host = os.environ.get("db_ip", "localhost")
        self.port = int(os.environ.get("db_port", 27017))

    def test_connection(self):
        self.assertRaises(TypeError, Mongo, 1)
        self.assertRaises(TypeError, Mongo, 1.14)
        self.assertRaises(TypeError, Mongo, None)
        self.assertRaises(TypeError, Mongo, [])
        self.assertRaises(TypeError, Mongo, "localhost", "27017")
        self.assertRaises(TypeError, Mongo, "localhost", 1.14)
        self.assertRaises(TypeError, Mongo, "localhost", None)
        self.assertRaises(TypeError, Mongo, "localhost", [])

        self.assertRaises(ConnectionException, Mongo, "somedomainthatdoesntexist.org")
        self.assertRaises(ConnectionException, Mongo, self.host, 123456789)

        self.assertTrue(Mongo(self.host, self.port))

    def test_repr(self):
        self.assertEqual(repr(Mongo(self.host, self.port)),
                         "Mongo('%s', %s)" % (self.host, self.port))
        self.assertEqual(repr(Mongo(self.host, self.port).test),
                         "Collection(Mongo('%s', %s), 'test')" % (self.host, self.port))

    def test_collection(self):
        db = Mongo(self.host, self.port)

        self.assertRaises(TypeError, Collection, db, 5)
        self.assertRaises(TypeError, Collection, 5, "test")

        def make_col(base, name):
            base[name]

        self.assertRaises(InvalidCollection, make_col, db, "")
        self.assertRaises(InvalidCollection, make_col, db, "te$t")
        self.assertRaises(InvalidCollection, make_col, db, ".test")
        self.assertRaises(InvalidCollection, make_col, db, "test.")
        self.assertRaises(InvalidCollection, make_col, db, "tes..t")
        self.assertRaises(InvalidCollection, make_col, db.test, "")
        self.assertRaises(InvalidCollection, make_col, db.test, "te$t")
        self.assertRaises(InvalidCollection, make_col, db.test, ".test")
        self.assertRaises(InvalidCollection, make_col, db.test, "test.")
        self.assertRaises(InvalidCollection, make_col, db.test, "tes..t")

        self.assertTrue(isinstance(db.test, Collection))
        self.assertEqual(db.test, db["test"])
        self.assertEqual(db.test, Collection(db, "test"))
        self.assertEqual(db.test.mike, db["test.mike"])
        self.assertEqual(db.test["mike"], db["test.mike"])

    def test_save_find_one(self):
        db = Mongo(self.host, self.port)

        a_doc = SON({"hello": u"world"})
        db.test.save(a_doc)
        self.assertTrue(isinstance(a_doc["_id"], ObjectId))
        self.assertEqual(a_doc, db.test.find_one({"_id": a_doc["_id"]}))

if __name__ == "__main__":
    unittest.main()
