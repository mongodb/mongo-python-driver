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
    def __init__(self, name, host="localhost", port=27017):
        """Open a new connection to the database at host:port.

        Raises TypeError if name or host is not an instance of string or port is
        not an instance of int. Raises ConnectionException if the connection
        cannot be made.

        Arguments:
        - `name`: the name of the database to connect to
        - `host` (optional): the hostname or IPv4 address of the database to
            connect to
        - `port` (optional): the port number on which to connect
        """
        if not isinstance(name, types.StringTypes):
            raise TypeError("name must be an instance of (str, unicode)")
        if not isinstance(host, types.StringType):
            raise TypeError("host must be an instance of str")
        if not isinstance(port, types.IntType):
            raise TypeError("port must be an instance of int")

        self.__name = name
        self.__host = host
        self.__port = port
        self.__id = 1

        self.__connect()

    def name(self):
        return self.__name

    def __connect(self):
        """(Re-)connect to the database."""
        try:
            self.__connection = socket.socket()
            self.__connection.connect((self.__host, self.__port))
        except socket.error:
            raise ConnectionException("could not connect to %s:%s, got: %s" %
                                      (self.__host, self.__port, traceback.format_exc()))

    def _send_message(self, operation, data):
        """Say something to the database.

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

        total_sent = 0
        while total_sent < len(to_send):
            sent = self.__connection.send(to_send[total_sent:])
            if sent == 0:
                raise ConnectionException("connection closed")
            total_sent += sent

        return self.__id - 1

    def _receive_message(self, operation, request_id):
        """Receive a message from the database.

        Returns the message body. Asserts that the message uses the given opcode
        and request id.

        Arguments:
        - `operation`: the opcode of the message
        - `request_id`: the request id that the message should be in response to
        """
        def receive(length):
            message = ""
            while len(message) < length:
                chunk = self.__connection.recv(length - len(message))
                if chunk == "":
                    raise ConnectionException("connection closed")
                message += chunk
            return message

        header = receive(16)
        length = struct.unpack("<i", header[:4])[0]
        assert request_id == struct.unpack("<i", header[8:12])[0]
        assert operation == struct.unpack("<i", header[12:])[0]

        return receive(length - 16)

    def __cmp__(self, other):
        if isinstance(other, Mongo):
            return cmp((self.__host, self.__port), (other.__host, other.__port))
        return NotImplemented

    def __repr__(self):
        return "Mongo(%r, %r, %r)" % (self.__name, self.__host, self.__port)

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

    def _full_name(self):
        return "%s.%s" % (self.__database.name(), self.__collection_name)

    def _send_message(self, operation, data):
        """Wrap up a message and send it.
        """
        # reserved int, full collection name, message data
        message = "\x00\x00\x00\x00%s%s" % (bson._make_c_string(self._full_name()), data)
        return self.__database._send_message(operation, message)

    def database(self):
        return self.__database

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

        self._send_message(2002, bson.BSON.from_dict(to_save))

        return to_save["_id"]

    def remove(self, spec_or_object_id):
        """Remove an object(s) from this collection.

        Raises TypeEror if the argument is not an instance of
        (dict, SON, ObjectId).

        Arguments:
        - `spec_or_object_id` (optional): a SON object specifying elements
            which must be present for a document to be removed OR an instance of
            ObjectId to be used as the value for an _id element
        """
        spec = spec_or_object_id
        if isinstance(spec, ObjectId):
            spec = SON({"_id": spec})

        if not isinstance(spec, (types.DictType, SON)):
            raise TypeError("spec must be an instance of (dict, SON)")

        self._send_message(2006, "\x00\x00\x00\x00" + bson.BSON.from_dict(spec))

    def find_one(self, spec_or_object_id=SON()):
        """Get a single object from the database.

        Raises TypeError if the argument is of an improper type. Returns a
        single SON object, or None if no result is found.

        Arguments:
        - `spec_or_object_id` (optional): a SON object specifying elements
            which must be present for a document to be included in the result
            set OR an instance of ObjectId to be used as the value for an _id
            query
        """
        spec = spec_or_object_id
        if isinstance(spec, ObjectId):
            spec = SON({"_id": spec})

        for result in self.find(spec, limit=1):
            return result
        return None

    def find(self, spec=SON(), fields=[], skip=0, limit=0):
        """Query the database.

        Raises TypeError if any of the arguments are of improper type. Returns
        an instance of Cursor corresponding to this query.

        Arguments:
        - `spec` (optional): a SON object specifying elements which must be
            present for a document to be included in the result set
        - `fields` (optional): a list of field names that should be returned
            in the result set
        - `skip` (optional): the number of documents to omit (from the start of
            the result set) when returning the results
        - `limit` (optional): the maximum number of results to return in the
            first reply message, or 0 for the default return size
        """
        if not isinstance(spec, (types.DictType, SON)):
            raise TypeError("spec must be an instance of (dict, SON)")
        if not isinstance(fields, types.ListType):
            raise TypeError("fields must be an instance of list")
        if not isinstance(skip, types.IntType):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, types.IntType):
            raise TypeError("limit must be an instance of int")

        return_fields = len(fields) and SON() or None
        for field in fields:
            if not isinstance(field, types.StringTypes):
                raise TypeError("fields must be a list of key names as (string, unicode)")
            return_fields[field] = 1

        return Cursor(self, spec, return_fields, skip, limit)

class Cursor(object):
    """A cursor / iterator over Mongo query results.
    """
    def __init__(self, collection, spec, fields, skip, limit):
        """Create a new cursor.

        Generally not needed to be used by application developers.
        """
        self.__collection = collection
        self.__spec = spec
        self.__fields = fields
        self.__skip = skip
        self.__limit = limit

        self.__data = []
        self.__id = None
        self.__retrieved = 0

    def _refresh(self):
        """Refreshes the cursor with more data from Mongo.

        Returns the length of self.__data after refresh. Will exit early if
        self.__data is already non-empty.
        """
        if len(self.__data):
            return len(self.__data)

        def send_message(operation, message):
            # TODO the send and receive here should be synchronized...
            request_id = self.__collection._send_message(operation, message)
            response = self.__collection.database()._receive_message(1, request_id)

            # TODO handle non-zero response flags
            assert struct.unpack("<i", response[:4])[0] == 0

            self.__id = struct.unpack("<q", response[4:12])[0]
            assert struct.unpack("<i", response[12:16])[0] == self.__skip + self.__retrieved

            number_returned = struct.unpack("<i", response[16:20])[0]
            self.__retrieved += number_returned
            self.__data = bson.to_dicts(response[20:])
            assert len(self.__data) == number_returned

        if self.__id is None:
            # Query
            message = struct.pack("<i", self.__skip)
            message += struct.pack("<i", self.__limit)
            message += bson.BSON.from_dict(self.__spec)
            if self.__fields:
                message += bson.BSON.from_dict(self.__fields)

            send_message(2004, message)
        elif self.__id != 0:
            # Get More
            limit = 0
            if self.__limit:
                if self.__limit > self.__retrieved:
                    limit = self.__limit - self.__retrieved
                else:
                    return 0

            message = struct.pack("<i", limit)
            message += struct.pack("<q", self.__id)

            send_message(2005, message)

        return len(self.__data)

    def __iter__(self):
        return self

    def next(self):
        if len(self.__data):
            return self.__data.pop(0)
        if self._refresh():
            return self.__data.pop(0)
        raise StopIteration

class TestMongo(unittest.TestCase):
    def setUp(self):
        self.host = os.environ.get("db_ip", "localhost")
        self.port = int(os.environ.get("db_port", 27017))

    def test_connection(self):
        self.assertRaises(TypeError, Mongo, 1)
        self.assertRaises(TypeError, Mongo, 1.14)
        self.assertRaises(TypeError, Mongo, None)
        self.assertRaises(TypeError, Mongo, [])
        self.assertRaises(TypeError, Mongo, "test", 1)
        self.assertRaises(TypeError, Mongo, "test", 1.14)
        self.assertRaises(TypeError, Mongo, "test", None)
        self.assertRaises(TypeError, Mongo, "test", [])
        self.assertRaises(TypeError, Mongo, "test", "localhost", "27017")
        self.assertRaises(TypeError, Mongo, "test", "localhost", 1.14)
        self.assertRaises(TypeError, Mongo, "test", "localhost", None)
        self.assertRaises(TypeError, Mongo, "test", "localhost", [])

        self.assertRaises(ConnectionException, Mongo, "test", "somedomainthatdoesntexist.org")
        self.assertRaises(ConnectionException, Mongo, "test", self.host, 123456789)

        self.assertTrue(Mongo("test", self.host, self.port))

    def test_repr(self):
        self.assertEqual(repr(Mongo("test", self.host, self.port)),
                         "Mongo('test', '%s', %s)" % (self.host, self.port))
        self.assertEqual(repr(Mongo("test", self.host, self.port).test),
                         "Collection(Mongo('test', '%s', %s), 'test')" % (self.host, self.port))

    def test_collection(self):
        db = Mongo(u"test", self.host, self.port)

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
        db = Mongo("test", self.host, self.port)
        db.test.remove({})

        a_doc = SON({"hello": u"world"})
        a_key = db.test.save(a_doc)
        self.assertTrue(isinstance(a_doc["_id"], ObjectId))
        self.assertEqual(a_doc["_id"], a_key)
        self.assertEqual(a_doc, db.test.find_one({"_id": a_doc["_id"]}))
        self.assertEqual(a_doc, db.test.find_one(a_key))
        self.assertEqual(None, db.test.find_one(ObjectId()))
        self.assertEqual(a_doc, db.test.find_one({"hello": u"world"}))
        self.assertEqual(None, db.test.find_one({"hello": u"test"}))

    def test_remove(self):
        db = Mongo("test", self.host, self.port)
        db.test.remove({})

        self.assertRaises(TypeError, db.test.remove, 5)
        self.assertRaises(TypeError, db.test.remove, "test")
        self.assertRaises(TypeError, db.test.remove, [])

        one = db.test.save({"x": 1})
        two = db.test.save({"x": 2})
        three = db.test.save({"x": 3})
        length = 0
        for _ in db.test.find():
            length += 1
        self.assertEqual(length, 3)

        db.test.remove(one)
        length = 0
        for _ in db.test.find():
            length += 1
        self.assertEqual(length, 2)

        db.test.remove(db.test.find_one())
        db.test.remove(db.test.find_one())
        self.assertEqual(db.test.find_one(), None)

        one = db.test.save({"x": 1})
        two = db.test.save({"x": 2})
        three = db.test.save({"x": 3})

        self.assertTrue(db.test.find_one({"x": 2}))
        db.test.remove({"x": 2})
        self.assertFalse(db.test.find_one({"x": 2}))

        self.assertTrue(db.test.find_one())
        db.test.remove({})
        self.assertFalse(db.test.find_one())

if __name__ == "__main__":
    unittest.main()
