"""Driver for Mongo.

The database is accessed through an instance of the Mongo class."""

import unittest
import socket
import types
import traceback
import os
import struct
import random

from son import SON
import bson
from objectid import ObjectId
from dbref import DBRef

class DatabaseException(Exception):
    """Raised when a database operation fails.
    """

class InvalidOperation(Exception):
    """Raised when a client attempts to perform an invalid operation.
    """

class ConnectionException(IOError):
    """Raised when a connection to the database cannot be made or is lost.
    """

class InvalidCollection(ValueError):
    """Raised when an invalid collection name is used.
    """

_ZERO = "\x00\x00\x00\x00"
_ONE = "\x01\x00\x00\x00"
_MAX_DYING_CURSORS = 20
_SYSTEM_INDEX_COLLECTION = "system.indexes"

ASCENDING = 1
DESCENDING = -1

class Mongo(object):
    """A connection to a Mongo database.
    """
    def __init__(self, name, host="localhost", port=27017, settings={}):
        """Open a new connection to the database at host:port.

        Raises TypeError if name or host is not an instance of string or port is
        not an instance of int. Raises ConnectionException if the connection
        cannot be made.

        Settings are passed in as a dictionary. Possible settings, along with
        their default values (in parens), are listed below:
        - "auto_dereference" (False): automatically dereference any `DBRef`s
            contained within SON objects being returned from queries
        - "auto_reference" (False): automatically create `DBRef`s out of any
            sub-objects that have already been saved in the database

        Arguments:
        - `name`: the name of the database to connect to
        - `host` (optional): the hostname or IPv4 address of the database to
            connect to
        - `port` (optional): the port number on which to connect
        - `settings` (optional): a dictionary of settings
        """
        if not isinstance(name, types.StringTypes):
            raise TypeError("name must be an instance of (str, unicode)")
        if not isinstance(host, types.StringType):
            raise TypeError("host must be an instance of str")
        if not isinstance(port, types.IntType):
            raise TypeError("port must be an instance of int")
        if not isinstance(settings, types.DictType):
            raise TypeError("settings must be an instance of dict")

        self.__name = name
        self.__host = host
        self.__port = port
        self.__id = 1
        self.__dying_cursors = []
        self.__auto_dereference = settings.get("auto_dereference", False)
        self.__auto_reference = settings.get("auto_reference", False)

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
                                      (self.__host, self.__port,
                                       traceback.format_exc()))

    def _send_message(self, operation, data):
        """Say something to the database.

        Arguments:
        - `operation`: the opcode of the message
        - `data`: the data to send
        """
        # header
        to_send = struct.pack("<i", 16 + len(data))
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

    def _command(self, command):
        """Issue a DB command.
        """
        return self["$cmd"].find_one(command)

    def _kill_cursors(self):
        message = _ZERO
        message += struct.pack("<i", len(self.__dying_cursors))
        for cursor_id in self.__dying_cursors:
            message += struct.pack("<q", cursor_id)
        self._send_message(2007, message)
        self.__dying_cursors = []

    def _kill_cursor(self, cursor_id):
        self.__dying_cursors.append(cursor_id)

        if len(self.__dying_cursors) > _MAX_DYING_CURSORS:
            self._kill_cursors()

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

    def dereference(self, dbref):
        """Dereference a DBRef, getting the SON object it points to.

        Raises TypeError if dbref is not an instance of DBRef. Returns a SON
        object or None if the reference does not point to a valid object.

        Arguments:
        - `dbref`: the reference
        """
        if not isinstance(dbref, DBRef):
            raise TypeError("cannot dereference a %s" % type(dbref))
        return self[dbref.collection()].find_one(dbref.id())

    def _fix_outgoing(self, son):
        """Fixes an object coming out of the database.

        Used to do things like auto dereferencing, if the option is enabled.

        Arguments:
        - `son`: a SON object coming out of the database
        """
        if not self.__auto_dereference:
            return son

        def fix_value(value):
            if isinstance(value, DBRef):
                deref = self.dereference(value)
                if deref is None:
                    return value
                return self._fix_outgoing(deref)
            elif isinstance(value, (SON, types.DictType)):
                return self._fix_outgoing(value)
            elif isinstance(value, types.ListType):
                return [fix_value(v) for v in value]
            return value

        for (key, value) in son.items():
            son[key] = fix_value(value)

        return son

    def _fix_incoming(self, to_save, collection, add_meta):
        """Fixes an object going in to the database.

        Used to do things like auto referencing, if the option is enabled.
        Will also add _id and _ns if they are missing and desired (as specified
        by add_meta).

        Arguments:
        - `to_save`: a SON object going into the database
        - `collection`: collection into which this object is being saved
        - `add_meta`: should _id and other meta-fields be added to the object
        """
        if "_id" in to_save:
            assert isinstance(to_save["_id"], ObjectId), "'_id' must be an ObjectId"
        elif add_meta:
            to_save["_id"] = ObjectId()

        if add_meta:
            to_save["_ns"] = collection._name()

        if not self.__auto_reference:
            return to_save

        # make a copy, so only what is being saved gets auto-ref'ed
        to_save = SON(to_save)

        def fix_value(value):
            if isinstance(value, (SON, types.DictType)):
                if "_id" in value and not value["_id"].is_new() and "_ns" in value:
                    return DBRef(value["_ns"], value["_id"])
            return value

        for (key, value) in to_save.items():
            to_save[key] = fix_value(value)

        return to_save

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
        if "$" in name and name not in ["$cmd"]:
            raise InvalidCollection("collection names must not contain '$'")
        if name[0] == "." or name[-1] == ".":
            raise InvalidCollection("collecion names must not start or end with '.'")

        self.__database = database
        self.__collection_name = unicode(name)

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
            return cmp((self.__database, self.__collection_name),
                       (other.__database, other.__collection_name))
        return NotImplemented

    def _full_name(self):
        return u"%s.%s" % (self.__database.name(), self.__collection_name)

    def _name(self):
        return self.__collection_name

    def _send_message(self, operation, data):
        """Wrap up a message and send it.
        """
        # reserved int, full collection name, message data
        message = _ZERO
        message += bson._make_c_string(self._full_name())
        message += data
        return self.__database._send_message(operation, message)

    def database(self):
        return self.__database

    def save(self, to_save, add_meta=True):
        """Save a SON object in this collection.

        Raises TypeError if to_save is not an instance of (dict, SON).

        Arguments:
        - `to_save`: the SON object to be saved
        - `add_meta` (optional): add meta information (like _id) to the object
            if it's missing
        """
        if not isinstance(to_save, (types.DictType, SON)):
            raise TypeError("cannot save object of type %s" % type(to_save))

        to_save = self.__database._fix_incoming(to_save, self, add_meta)

        if "_id" not in to_save:
            self._send_message(2002, bson.BSON.from_dict(to_save))
        elif to_save["_id"].is_new():
            to_save["_id"]._use()
            self._send_message(2002, bson.BSON.from_dict(to_save))
        else:
            self._update({"_id": to_save["_id"]}, to_save, True)

        return to_save.get("_id", None)

    def _update(self, spec, document, upsert=False):
        """Update an object(s) in this collection.

        Raises TypeError if either spec or document isn't an instance of
        (dict, SON) or upsert isn't an instance of bool.

        - `spec`: a SON object specifying elements which must be present for a
            document to be updated
        - `document`: a SON object specifying the fields to be changed in the
            selected document(s), or (in the case of an upsert) the document to
            be inserted.
        - `upsert` (optional): perform an upsert operation
        """
        if not isinstance(spec, (types.DictType, SON)):
            raise TypeError("spec must be an instance of (dict, SON)")
        if not isinstance(document, (types.DictType, SON)):
            raise TypeError("document must be an instance of (dict, SON)")
        if not isinstance(upsert, types.BooleanType):
            raise TypeError("upsert must be an instance of bool")

        message = upsert and _ONE or _ZERO
        message += bson.BSON.from_dict(spec)
        message += bson.BSON.from_dict(document)

        self._send_message(2001, message)

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

        self._send_message(2006, _ZERO + bson.BSON.from_dict(spec))

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

    def _gen_index_name(self, keys):
        """Generate an index name from the set of fields it is over.
        """
        return u"_".join([u"%s_%s" % item for item in keys])

    def create_index(self, key_or_list, direction=None):
        """Creates an index on this collection.

        Takes either a single key and a direction, or a list of (key, direction)
        pairs. The key(s) must be an instance of (str, unicode), and the
        direction(s) must be one of (Mongo.ASCENDING, Mongo.DESCENDING).

        Arguments:
        - `key_or_list`: a single key or a list of (key, direction) pairs
            specifying the index to ensure
        - `direction` (optional): must be included if key_or_list is a single
            key, otherwise must be None
        """
        if direction:
            keys = [(key_or_list, direction)]
        else:
            keys = key_or_list

        if not isinstance(keys, types.ListType):
            raise TypeError("if no direction is specified, key_or_list must be an instance of list")
        if not len(keys):
            raise ValueError("key_or_list must not be the empty list")

        to_save = SON()
        to_save["name"] = self._gen_index_name(keys)
        to_save["ns"] = self._full_name()

        key_object = SON()
        for (key, value) in keys:
            if not isinstance(key, types.StringTypes):
                raise TypeError("first item in each key pair must be a string")
            if not isinstance(value, types.IntType):
                raise TypeError("second item in each key pair must be Mongo.ASCENDING or Mongo.DESCENDING")
            key_object[key] = value
        to_save["key"] = key_object

        self.__database[_SYSTEM_INDEX_COLLECTION].save(to_save, False)

    def drop_indexes(self):
        """Drops all indexes on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises DatabaseException on an error.
        """
        response = self.__database._command(SON([("deleteIndexes", self.__collection_name),
                                                 ("index", u"*")]))
        if response["ok"] != 1:
            if response["errmsg"] == "ns not found":
                return
            raise DatabaseException("error dropping indexes: %s" % response["errmsg"])

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
        self.__ordering = None

        self.__data = []
        self.__id = None
        self.__retrieved = 0
        self.__killed = False

    def __del__(self):
        if self.__id and not self.__killed:
            self._die()

    def _die(self):
        """Kills this cursor.
        """
        self.__collection.database()._kill_cursor(self.__id)
        self.__killed = True

    def __query_spec(self):
        """Get the spec to use for a query.

        Just `self.__spec`, unless this cursor needs special query fields, like
        orderby.
        """
        if not self.__ordering:
            return self.__spec
        return SON([("query", self.__spec),
                    ("orderby", self.__ordering)])

    def __check_okay_to_chain(self):
        """Check if it is okay to chain more options onto this cursor.
        """
        if self.__retrieved or self.__id is not None:
            raise InvalidOperation("cannot set options after executing query")

    def limit(self, limit):
        """Limits the number of results to be returned by this cursor.

        Raises TypeError if limit is not an instance of int. Raises
        InvalidOperation if this cursor has already been used.

        Arguments:
        - `limit`: the number of results to return
        """
        if not isinstance(limit, types.IntType):
            raise TypeError("limit must be an int")
        self.__check_okay_to_chain()

        self.__limit = limit
        return self

    def skip(self, skip):
        """Skips the first `skip` results of this cursor.

        Raises TypeError if skip is not an instance of int. Raises
        InvalidOperation if this cursor has already been used.

        Arguments:
        - `skip`: the number of results to skip
        """
        if not isinstance(skip, types.IntType):
            raise TypeError("skip must be an int")
        self.__check_okay_to_chain()

        self.__skip = skip
        return self

    def sort(self, key_or_list, direction=None):
        """Sorts this cursors results.

        Takes either a single key and a direction, or a list of (key, direction)
        pairs. The key(s) must be an instance of (str, unicode), and the
        direction(s) must be one of (Mongo.ASCENDING, Mongo.DESCENDING). Raises
        InvalidOperation if this cursor has already been used.

        Arguments:
        - `key_or_list`: a single key or a list of (key, direction) pairs
            specifying the keys to sort on
        - `direction` (optional): must be included if key_or_list is a single
            key, otherwise must be None
        """
        self.__check_okay_to_chain()

        # TODO a lot of this logic could be shared with create_index()
        if direction:
            keys = [(key_or_list, direction)]
        else:
            keys = key_or_list

        if not isinstance(keys, types.ListType):
            raise TypeError("if no direction is specified, key_or_list must be an instance of list")
        if not len(keys):
            raise ValueError("key_or_list must not be the empty list")

        orderby = SON()
        for (key, value) in keys:
            if not isinstance(key, types.StringTypes):
                raise TypeError("first item in each key pair must be a string")
            if not isinstance(value, types.IntType):
                raise TypeError("second item in each key pair must be Mongo.ASCENDING or Mongo.DESCENDING")
            orderby[key] = value

        self.__ordering = orderby
        return self

    def count(self):
        """Get the size of the results set for this query.

        Returns the number of objects in the results set for this query. Does
        not take limit and skip into account. Raises Invalid Operation if this
        cursor has already been used. Raises DatabaseException on a database
        error.
        """
        self.__check_okay_to_chain()

        command = SON([("count", self.__collection._name()),
                       ("query", self.__spec)])
        response = self.__collection.database()._command(command)
        if response["ok"] != 1:
            raise DatabaseException("error getting count")
        return int(response["n"])

    def _refresh(self):
        """Refreshes the cursor with more data from Mongo.

        Returns the length of self.__data after refresh. Will exit early if
        self.__data is already non-empty.
        """
        if len(self.__data) or self.__killed:
            return len(self.__data)

        def send_message(operation, message):
            # TODO the send and receive here should be synchronized...
            request_id = self.__collection._send_message(operation, message)
            response = self.__collection.database()._receive_message(1, request_id)

            # TODO handle non-zero response flags
            assert struct.unpack("<i", response[:4])[0] == 0

            self.__id = struct.unpack("<q", response[4:12])[0]
            assert struct.unpack("<i", response[12:16])[0] == self.__retrieved

            number_returned = struct.unpack("<i", response[16:20])[0]
            self.__retrieved += number_returned
            self.__data = bson.to_dicts(response[20:])
            assert len(self.__data) == number_returned

        if self.__id is None:
            # Query
            message = struct.pack("<i", self.__skip)
            message += struct.pack("<i", self.__limit)
            message += bson.BSON.from_dict(self.__query_spec())
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
                    self._die()
                    return 0

            message = struct.pack("<i", limit)
            message += struct.pack("<q", self.__id)

            send_message(2005, message)

        length = len(self.__data)
        if not length:
            self._die()
        return length

    def __iter__(self):
        return self

    def next(self):
        if len(self.__data):
            return self.__collection.database()._fix_outgoing(self.__data.pop(0))
        if self._refresh():
            return self.__collection.database()._fix_outgoing(self.__data.pop(0))
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
        self.assertRaises(TypeError, Mongo, "test", "localhost", 27017, "settings")
        self.assertRaises(TypeError, Mongo, "test", "localhost", 27017, None)

        self.assertRaises(ConnectionException, Mongo, "test", "somedomainthatdoesntexist.org")
        self.assertRaises(ConnectionException, Mongo, "test", self.host, 123456789)

        self.assertTrue(Mongo("test", self.host, self.port))

    def test_repr(self):
        self.assertEqual(repr(Mongo("test", self.host, self.port)),
                         "Mongo('test', '%s', %s)" % (self.host, self.port))
        self.assertEqual(repr(Mongo("test", self.host, self.port).test),
                         "Collection(Mongo('test', '%s', %s), u'test')" % (self.host, self.port))

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

        b = db.test.find_one()
        self.assertFalse(b["_id"].is_new())
        b["hello"] = u"mike"
        db.test.save(b)

        self.assertNotEqual(a_doc, db.test.find_one(a_key))
        self.assertEqual(b, db.test.find_one(a_key))
        self.assertEqual(b, db.test.find_one())

        count = 0
        for _ in db.test.find():
            count += 1
        self.assertEqual(count, 1)

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

    def test_save_a_bunch(self):
        db = Mongo("test", self.host, self.port)
        db.test.remove({})

        for i in xrange(1000):
            db.test.save({"x": i})

        count = 0
        for _ in db.test.find():
            count += 1

        self.assertEqual(1000, count)

        # test that kill cursors doesn't assert or anything
        for _ in xrange(3 * _MAX_DYING_CURSORS + 2):
            for _ in db.test.find():
                break

    def test_create_index(self):
        db = Mongo("test", self.host, self.port)

        self.assertRaises(TypeError, db.test.create_index, 5)
        self.assertRaises(TypeError, db.test.create_index, "hello")
        self.assertRaises(ValueError, db.test.create_index, [])
        self.assertRaises(TypeError, db.test.create_index, [], ASCENDING)
        self.assertRaises(TypeError, db.test.create_index, [("hello", DESCENDING)], DESCENDING)
        self.assertRaises(TypeError, db.test.create_index, "hello", "world")

        db.test.drop_indexes()
        self.assertFalse(db[_SYSTEM_INDEX_COLLECTION].find_one({"ns": u"test.test"}))

        db.test.create_index("hello", ASCENDING)
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])

        count = 0
        for _ in db[_SYSTEM_INDEX_COLLECTION].find({"ns": u"test.test"}):
            count += 1
        self.assertEqual(count, 2)

        db.test.drop_indexes()
        self.assertFalse(db[_SYSTEM_INDEX_COLLECTION].find_one({"ns": u"test.test"}))
        db.test.create_index("hello", ASCENDING)
        self.assertEqual(db[_SYSTEM_INDEX_COLLECTION].find_one({"ns": u"test.test"}),
                         SON([(u"name", u"hello_1"),
                              (u"ns", u"test.test"),
                              (u"key", SON([(u"hello", 1)]))]))

        db.test.drop_indexes()
        self.assertFalse(db[_SYSTEM_INDEX_COLLECTION].find_one({"ns": u"test.test"}))
        db.test.create_index([("hello", DESCENDING), ("world", ASCENDING)])
        self.assertEqual(db[_SYSTEM_INDEX_COLLECTION].find_one({"ns": u"test.test"}),
                         SON([(u"name", u"hello_-1_world_1"),
                              (u"ns", u"test.test"),
                              (u"key", SON([(u"hello", -1),
                                            (u"world", 1)]))]))

    def test_limit(self):
        db = Mongo("test", self.host, self.port)

        self.assertRaises(TypeError, db.test.find().limit, None)
        self.assertRaises(TypeError, db.test.find().limit, "hello")
        self.assertRaises(TypeError, db.test.find().limit, 5.5)

        db.test.remove({})
        for i in range(100):
            db.test.save({"x": i})

        count = 0
        for _ in db.test.find():
            count += 1
        self.assertEqual(count, 100)

        count = 0
        for _ in db.test.find().limit(20):
            count += 1
        self.assertEqual(count, 20)

        count = 0
        for _ in db.test.find().limit(99):
            count += 1
        self.assertEqual(count, 99)

        count = 0
        for _ in db.test.find().limit(1):
            count += 1
        self.assertEqual(count, 1)

        count = 0
        for _ in db.test.find().limit(0):
            count += 1
        self.assertEqual(count, 100)

        count = 0
        for _ in db.test.find().limit(0).limit(50).limit(10):
            count += 1
        self.assertEqual(count, 10)

        a = db.test.find()
        a.limit(10)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.limit, 5)

    def test_limit(self):
        db = Mongo("test", self.host, self.port)

        self.assertRaises(TypeError, db.test.find().skip, None)
        self.assertRaises(TypeError, db.test.find().skip, "hello")
        self.assertRaises(TypeError, db.test.find().skip, 5.5)

        db.test.remove({})
        for i in range(100):
            db.test.save({"x": i})

        for i in db.test.find():
            self.assertEqual(i["x"], 0)
            break

        for i in db.test.find().skip(20):
            self.assertEqual(i["x"], 20)
            break

        for i in db.test.find().skip(99):
            self.assertEqual(i["x"], 99)
            break

        for i in db.test.find().skip(1):
            self.assertEqual(i["x"], 1)
            break

        for i in db.test.find().skip(0):
            self.assertEqual(i["x"], 0)
            break

        for i in db.test.find().skip(0).skip(50).skip(10):
            self.assertEqual(i["x"], 10)
            break

        for i in db.test.find().skip(1000):
            self.fail()

        a = db.test.find()
        a.skip(10)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.skip, 5)

    def test_sort(self):
        db = Mongo("test", self.host, self.port)

        self.assertRaises(TypeError, db.test.find().sort, 5)
        self.assertRaises(TypeError, db.test.find().sort, "hello")
        self.assertRaises(ValueError, db.test.find().sort, [])
        self.assertRaises(TypeError, db.test.find().sort, [], ASCENDING)
        self.assertRaises(TypeError, db.test.find().sort, [("hello", DESCENDING)], DESCENDING)
        self.assertRaises(TypeError, db.test.find().sort, "hello", "world")

        db.test.remove({})

        unsort = range(10)
        random.shuffle(unsort)

        for i in unsort:
            db.test.save({"x": i})

        asc = [i["x"] for i in db.test.find().sort("x", ASCENDING)]
        self.assertEqual(asc, range(10))
        asc = [i["x"] for i in db.test.find().sort([("x", ASCENDING)])]
        self.assertEqual(asc, range(10))

        expect = range(10)
        expect.reverse()
        desc = [i["x"] for i in db.test.find().sort("x", DESCENDING)]
        self.assertEqual(desc, expect)
        desc = [i["x"] for i in db.test.find().sort([("x", DESCENDING)])]
        self.assertEqual(desc, expect)
        desc = [i["x"] for i in db.test.find().sort("x", ASCENDING).sort("x", DESCENDING)]
        self.assertEqual(desc, expect)

        expected = [(1, 5), (2, 5), (0, 3), (7, 3), (9, 2), (2, 1), (3, 1)]
        shuffled = list(expected)
        random.shuffle(shuffled)

        db.test.remove({})
        for (a, b) in shuffled:
            db.test.save({"a": a, "b": b})

        result = [(i["a"], i["b"]) for i in db.test.find().sort([("b", DESCENDING),
                                                                 ("a", ASCENDING)])]
        self.assertEqual(result, expected)

        a = db.test.find()
        a.sort("x", ASCENDING)
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.sort, "x", ASCENDING)

    def test_count(self):
        db = Mongo("test", self.host, self.port)
        db.test.remove({})

        self.assertEqual(0, db.test.find().count())

        for i in range(10):
            db.test.save({"x": i})

        self.assertEqual(10, db.test.find().count())
        self.assertTrue(isinstance(db.test.find().count(), types.IntType))
        self.assertEqual(10, db.test.find().limit(5).count())
        self.assertEqual(10, db.test.find().skip(5).count())

        self.assertEqual(1, db.test.find({"x": 1}).count())
        self.assertEqual(5, db.test.find({"x": {"$lt": 5}}).count())

        a = db.test.find()
        b = a.count()
        for _ in a:
            break
        self.assertRaises(InvalidOperation, a.count)

    def test_deref(self):
        db = Mongo("test", self.host, self.port)
        db.test.remove({})

        self.assertRaises(TypeError, db.dereference, 5)
        self.assertRaises(TypeError, db.dereference, "hello")
        self.assertRaises(TypeError, db.dereference, None)

        self.assertEqual(None, db.dereference(DBRef("test", ObjectId())))

        obj = {"x": True}
        key = db.test.save(obj)
        self.assertEqual(obj, db.dereference(DBRef("test", key)))

    def test_auto_deref(self):
        db = Mongo("test", self.host, self.port)
        db.test.a.remove({})
        db.test.b.remove({})
        db.test.remove({})

        a = {"hello": u"world"}
        key = db.test.b.save(a)
        dbref = DBRef("test.b", key)

        self.assertEqual(db.dereference(dbref), a)

        b = {"b_obj": dbref}
        db.test.a.save(b)
        self.assertEqual(dbref, db.test.a.find_one()["b_obj"])
        self.assertEqual(a, db.dereference(db.test.a.find_one()["b_obj"]))

        db = Mongo("test", self.host, self.port, {"auto_dereference": False})
        self.assertEqual(dbref, db.test.a.find_one()["b_obj"])

        db = Mongo("test", self.host, self.port, {"auto_dereference": True})
        self.assertNotEqual(dbref, db.test.a.find_one()["b_obj"])
        self.assertEqual(a, db.test.a.find_one()["b_obj"])

        key2 = db.test.a.save({"x": [dbref]})
        self.assertEqual(a, db.test.a.find_one(key2)["x"][0])

        dbref2 = DBRef("test.a", key2)
        key3 = db.test.b.save({"x": dbref2})
        self.assertEqual(a, db.test.b.find_one(key3)["x"]["x"][0])

        dbref = DBRef("test.c", ObjectId())
        key = db.test.save({"x": dbref})
        self.assertEqual(dbref, db.test.find_one(key)["x"])

    def test_auto_ref(self):
        db = Mongo("test", self.host, self.port)
        db.test.a.remove({})
        db.test.b.remove({})

        a = SON({u"hello": u"world"})
        db.test.a.save(a)
        self.assertEqual(a["_ns"], "test.a")

        b = SON({"ref?": a})
        key = db.test.b.save(b)
        self.assertEqual(b["_ns"], "test.b")
        self.assertEqual(b["ref?"], a)
        self.assertEqual(db.test.b.find_one(key)["ref?"], a)

        db = Mongo("test", self.host, self.port, {"auto_reference": False})
        key = db.test.b.save(b)
        self.assertEqual(b["_ns"], "test.b")
        self.assertEqual(b["ref?"], a)
        self.assertEqual(db.test.b.find_one(key)["ref?"], a)

        db = Mongo("test", self.host, self.port, {"auto_reference": True})
        key = db.test.b.save(b)
        self.assertEqual(b["_ns"], "test.b")
        self.assertEqual(b["ref?"], a)
        self.assertNotEqual(db.test.b.find_one(key)["ref?"], a)
        self.assertEqual(db.dereference(db.test.b.find_one(key)["ref?"]), a)

    def test_auto_ref_and_deref(self):
        db = Mongo("test", self.host, self.port, {"auto_reference": True, "auto_dereference": True})
        db.test.a.remove({})
        db.test.b.remove({})
        db.test.c.remove({})

        a = SON({"hello": u"world"})
        b = SON({"test": a})
        c = SON({"another test": b})

        db.test.a.save(a)
        db.test.b.save(b)
        db.test.c.save(c)

        self.assertEqual(db.test.a.find_one(), a)
        self.assertEqual(db.test.b.find_one()["test"], a)
        self.assertEqual(db.test.c.find_one()["another test"]["test"], a)
        self.assertEqual(db.test.b.find_one(), b)
        self.assertEqual(db.test.c.find_one()["another test"], b)
        self.assertEqual(db.test.c.find_one(), c)


if __name__ == "__main__":
    unittest.main()
