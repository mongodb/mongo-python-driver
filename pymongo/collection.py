# Copyright 2009 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Collection level utilities for Mongo."""

import types

import bson
from objectid import ObjectId
from cursor import Cursor
from son import SON
from errors import InvalidName, OperationFailure

_ZERO = "\x00\x00\x00\x00"
_ONE = "\x01\x00\x00\x00"

class Collection(object):
    """A Mongo collection.
    """
    def __init__(self, database, name, options={}):
        """Get / create a Mongo collection.

        Raises TypeError if name is not an instance of (str, unicode). Raises
        InvalidName if name is not a valid collection name. Raises TypeError if
        options is not an instance of dict. If options is non-empty a create
        command will be sent to the database. Otherwise the collection will be
        created implicitly on first use.

        :Parameters:
          - `database`: the database to get a collection from
          - `name`: the name of the collection to get
          - `options`: dictionary of collection options.
            see `pymongo.database.Database.create_collection` for details.
        """
        if not isinstance(name, types.StringTypes):
            raise TypeError("name must be an instance of (str, unicode)")

        if not isinstance(options, types.DictType):
            raise TypeError("options must be an instance of dict")

        if not name or ".." in name:
            raise InvalidName("collection names cannot be empty")
        if "$" in name and name not in ["$cmd"]:
            raise InvalidName("collection names must not contain '$'")
        if name[0] == "." or name[-1] == ".":
            raise InvalidName("collecion names must not start or end with '.'")

        self.__database = database
        self.__collection_name = unicode(name)
        if options:
            self.__create(options)

    def __create(self, options):
        """Sends a create command with the given options.
        """
        command = SON({"create": self.__collection_name})
        command.update(options)

        response = self.__database._command(command)
        if response["ok"] not in [0, 1]:
            raise OperationFailure("error creating collection: %s" %
                                   response["errmsg"])

    def __getattr__(self, name):
        """Get a sub-collection of this collection by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
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

    def full_name(self):
        return u"%s.%s" % (self.__database.name(), self.__collection_name)

    def name(self):
        return self.__collection_name

    def _send_message(self, operation, data):
        """Wrap up a message and send it.
        """
        # reserved int, full collection name, message data
        message = _ZERO
        message += bson._make_c_string(self.full_name())
        message += data
        return self.__database.connection()._send_message(operation, message)

    def database(self):
        return self.__database

    def save(self, to_save, manipulate=True):
        """Save a SON object in this collection.

        Raises TypeError if to_save is not an instance of (dict, SON).

        :Parameters:
          - `to_save`: the SON object to be saved
          - `manipulate` (optional): manipulate the son object before saving it
        """
        if not isinstance(to_save, (types.DictType, SON)):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if "_id" not in to_save:
            result = self.insert(to_save, manipulate)
            return result.get("_id", None)
        else:
            self.update({"_id": to_save["_id"]}, to_save, True, manipulate)
            return to_save.get("_id", None)

    def insert(self, doc_or_docs, manipulate=True):
        """Insert a document(s) into this collection.

        If manipulate is set the document(s) are manipulated using any
        SONManipulators that have been added to this database. Returns the
        inserted object or a list of inserted objects.

        :Parameters:
          - `doc_or_docs`: a SON object or list of SON objects to be inserted
          - `manipulate` (optional): monipulate the objects before inserting?
        """
        docs = doc_or_docs
        if isinstance(docs, (types.DictType, SON)):
            docs = [docs]

        if not isinstance(docs, types.ListType):
            raise TypeError("insert takes a document or list of documents")

        if manipulate:
            docs = [self.__database._fix_incoming(doc, self) for doc in docs]

        data = [bson.BSON.from_dict(doc) for doc in docs]
        self._send_message(2002, "".join(data))

        return len(docs) == 1 and docs[0] or docs

    def update(self, spec, document, upsert=False, manipulate=True):
        """Update an object(s) in this collection.

        Raises TypeError if either spec or document isn't an instance of
        (dict, SON) or upsert isn't an instance of bool.

        :Parameters:
          - `spec`: a SON object specifying elements which must be present for a
            document to be updated
          - `document`: a SON object specifying the fields to be changed in the
            selected document(s), or (in the case of an upsert) the document to
            be inserted.
          - `upsert` (optional): perform an upsert operation
          - `manipulate` (optional): monipulate the document before updating?
        """
        if not isinstance(spec, (types.DictType, SON)):
            raise TypeError("spec must be an instance of (dict, SON)")
        if not isinstance(document, (types.DictType, SON)):
            raise TypeError("document must be an instance of (dict, SON)")
        if not isinstance(upsert, types.BooleanType):
            raise TypeError("upsert must be an instance of bool")

        if manipulate:
            document = self.__database._fix_incoming(document, self)

        message = upsert and _ONE or _ZERO
        message += bson.BSON.from_dict(spec)
        message += bson.BSON.from_dict(document)

        self._send_message(2001, message)

    def remove(self, spec_or_object_id):
        """Remove an object(s) from this collection.

        Raises TypeEror if the argument is not an instance of
        (dict, SON, ObjectId).

        :Parameters:
          - `spec_or_object_id` (optional): a SON object specifying elements
            which must be present for a document to be removed OR an instance of
            ObjectId to be used as the value for an _id element
        """
        spec = spec_or_object_id
        if isinstance(spec, ObjectId):
            spec = SON({"_id": spec})

        if not isinstance(spec, (types.DictType, SON)):
            raise TypeError("spec must be an instance of (dict, SON), not %s" % type(spec))

        self._send_message(2006, _ZERO + bson.BSON.from_dict(spec))

    def find_one(self, spec_or_object_id=SON()):
        """Get a single object from the database.

        Raises TypeError if the argument is of an improper type. Returns a
        single SON object, or None if no result is found.

        :Parameters:
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

        :Parameters:
          - `spec` (optional): a SON object specifying elements which must be
            present for a document to be included in the result set
          - `fields` (optional): a list of field names that should be returned
            in the result set
          - `skip` (optional): the number of documents to omit (from the start
            of the result set) when returning the results
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
        direction(s) must be one of (Mongo.ASCENDING, Mongo.DESCENDING). Returns
        the name of the created index.

        :Parameters:
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
        to_save["ns"] = self.full_name()

        key_object = SON()
        for (key, value) in keys:
            if not isinstance(key, types.StringTypes):
                raise TypeError("first item in each key pair must be a string")
            if not isinstance(value, types.IntType):
                raise TypeError("second item in each key pair must be Mongo.ASCENDING or Mongo.DESCENDING")
            key_object[key] = value
        to_save["key"] = key_object

        self.__database.system.indexes.save(to_save, False)
        return to_save["name"]

    def drop_indexes(self):
        """Drops all indexes on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error.
        """
        self.drop_index(u"*")

    def drop_index(self, index):
        """Drops the specified index on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error. Raises TypeError if index is not an
        instance of (str, unicode).

        :Parameters:
          - `index`: the name of the index to drop
        """
        if not isinstance(index, types.StringTypes):
            raise TypeError("index must be an instance of (str, unicode)")

        response = self.__database._command(SON([("deleteIndexes",
                                                  self.__collection_name),
                                                 ("index", unicode(index))]))
        if response["ok"] != 1:
            if response["errmsg"] == "ns not found":
                return
            raise OperationFailure("error dropping index(es): %s" %
                                   response["errmsg"])

    def index_information(self):
        """Get information on this collection's indexes.

        Returns a dictionary where the keys are index names (as returned by
        create_index()) and the values are lists of (key, direction) pairs
        specifying the index (as passed to create_index()).
        """
        raw = self.__database.system.indexes.find({"ns": self.full_name()})
        info = {}
        for index in raw:
            info[index["name"]] = index["key"].items()
        return info

    def options(self):
        """Get the options set on this collection.

        Returns a dictionary of options and their values - see
        `pymongo.database.Database.create_collection` for more information on
        the options dictionary. Returns an empty dictionary if the collection
        has not been created yet.
        """
        result = self.__database.system.namespaces.find_one(
            {"name": self.full_name()})

        if not result:
            return {}

        options = result.get("options", {})
        if "create" in options:
            del options["create"]

        return options

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Collection' object is not iterable")
