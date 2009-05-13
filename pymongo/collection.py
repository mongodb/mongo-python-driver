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

import pymongo
import bson
from objectid import ObjectId
from cursor import Cursor
from son import SON
from errors import InvalidName, OperationFailure
from code import Code

_ZERO = "\x00\x00\x00\x00"
_ONE = "\x01\x00\x00\x00"

class Collection(object):
    """A Mongo collection.
    """
    def __init__(self, database, name, options=None):
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

        if not isinstance(options, (types.DictType, types.NoneType)):
            raise TypeError("options must be an instance of dict")

        if not name or ".." in name:
            raise InvalidName("collection names cannot be empty")
        if "$" in name and not (name in ["$cmd"] or name.startswith("$cmd")):
            raise InvalidName("collection names must not contain '$'")
        if name[0] == "." or name[-1] == ".":
            raise InvalidName("collecion names must not start or end with '.'")

        self.__database = database
        self.__collection_name = unicode(name)
        if options is not None:
            self.__create(options)

    def __create(self, options):
        """Sends a create command with the given options.
        """

        # Send size as a float, not an int/long. BSON can only handle 32-bit
        # ints which conflicts w/ max collection size of 10000000000.
        if "size" in options:
            options["size"] = float(options["size"])

        command = SON({"create": self.__collection_name})
        command.update(options)

        self.__database._command(command)

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
        """Get the full name of this collection.

        The full name is of the form database_name.collection_name.
        """
        return u"%s.%s" % (self.__database.name(), self.__collection_name)

    def name(self):
        """Get the name of this collection.
        """
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
        """Get the database that this collection is a part of.
        """
        return self.__database

    def save(self, to_save, manipulate=True, safe=False):
        """Save a SON object in this collection.

        Raises TypeError if to_save is not an instance of dict. If `safe`
        is True then the save will be checked for errors, raising
        OperationFailure if one occurred. Checking for safety requires an extra
        round-trip to the database.

        :Parameters:
          - `to_save`: the SON object to be saved
          - `manipulate` (optional): manipulate the son object before saving it
          - `safe` (optional): check that the save succeeded?
        """
        if not isinstance(to_save, types.DictType):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if "_id" not in to_save:
            result = self.insert(to_save, manipulate, safe)
            return result.get("_id", None)
        else:
            self.update({"_id": to_save["_id"]}, to_save, True, manipulate, safe)
            return to_save.get("_id", None)

    def insert(self, doc_or_docs, manipulate=True, safe=False):
        """Insert a document(s) into this collection.

        If manipulate is set the document(s) are manipulated using any
        SONManipulators that have been added to this database. Returns the
        inserted object or a list of inserted objects. If `safe` is True then
        the insert will be checked for errors, raising OperationFailure if one
        occurred. Checking for safety requires an extra round-trip to the
        database.

        :Parameters:
          - `doc_or_docs`: a SON object or list of SON objects to be inserted
          - `manipulate` (optional): monipulate the objects before inserting?
          - `safe` (optional): check that the insert succeeded?
        """
        docs = doc_or_docs
        if isinstance(docs, types.DictType):
            docs = [docs]

        if not isinstance(docs, types.ListType):
            raise TypeError("insert takes a document or list of documents")

        if manipulate:
            docs = [self.__database._fix_incoming(doc, self) for doc in docs]

        data = [bson.BSON.from_dict(doc) for doc in docs]
        self._send_message(2002, "".join(data))

        if safe:
            error = self.__database.error()
            if error:
                raise OperationFailure("insert failed: " + error["err"])

        return len(docs) == 1 and docs[0] or docs

    def update(self, spec, document, upsert=False, manipulate=False, safe=False):
        """Update an object(s) in this collection.

        Raises TypeError if either spec or document isn't an instance of
        dict or upsert isn't an instance of bool. If `safe` is True then
        the update will be checked for errors, raising OperationFailure if one
        occurred. Checking for safety requires an extra round-trip to the
        database.

        :Parameters:
          - `spec`: a SON object specifying elements which must be present for a
            document to be updated
          - `document`: a SON object specifying the fields to be changed in the
            selected document(s), or (in the case of an upsert) the document to
            be inserted.
          - `upsert` (optional): perform an upsert operation
          - `manipulate` (optional): monipulate the document before updating?
          - `safe` (optional): check that the update succeeded?
        """
        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(document, types.DictType):
            raise TypeError("document must be an instance of dict")
        if not isinstance(upsert, types.BooleanType):
            raise TypeError("upsert must be an instance of bool")

        if upsert and manipulate:
            document = self.__database._fix_incoming(document, self)

        message = upsert and _ONE or _ZERO
        message += bson.BSON.from_dict(spec)
        message += bson.BSON.from_dict(document)

        self._send_message(2001, message)

        if safe:
            error = self.__database.error()
            if error:
                raise OperationFailure("update failed: " + error["err"])

    def remove(self, spec_or_object_id):
        """Remove an object(s) from this collection.

        Raises TypeEror if the argument is not an instance of
        (dict, ObjectId).

        :Parameters:
          - `spec_or_object_id` (optional): a SON object specifying elements
            which must be present for a document to be removed OR an instance of
            ObjectId to be used as the value for an _id element
        """
        spec = spec_or_object_id
        if isinstance(spec, ObjectId):
            spec = SON({"_id": spec})

        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict, not %s" % type(spec))

        self._send_message(2006, _ZERO + bson.BSON.from_dict(spec))

    def find_one(self, spec_or_object_id=None, _sock=None):
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
        if spec is None:
            spec = SON()
        if isinstance(spec, ObjectId):
            spec = SON({"_id": spec})

        for result in self.find(spec, limit=1, _sock=_sock):
            return result
        return None

    def find(self, spec=None, fields=None, skip=0, limit=0, _sock=None):
        """Query the database.

        The `spec` argument is a prototype document that all results must
        match. For example:

        >>> db.test.find({"hello": "world"})

        only matches documents that have a key "hello" with value "world".
        Matches can have other keys *in addition* to "hello". The `fields`
        argument is used to specify a subset of fields that should be included
        in the result documents. By limiting results to a certain subset of
        fields we cut down on network traffic and decoding time.

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
        if spec is None:
            spec = SON()
        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(fields, (types.ListType, types.NoneType)):
            raise TypeError("fields must be an instance of list")
        if not isinstance(skip, types.IntType):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, types.IntType):
            raise TypeError("limit must be an instance of int")

        return_fields = None
        if fields is not None:
            return_fields = {}
            for field in fields:
                if not isinstance(field, types.StringTypes):
                    raise TypeError("fields must be a list of key names as (string, unicode)")
                return_fields[field] = 1

        return Cursor(self, spec, return_fields, skip, limit, _sock=_sock)

    def count(self):
        return self.find().count()

    def _gen_index_name(self, keys):
        """Generate an index name from the set of fields it is over.
        """
        return u"_".join([u"%s_%s" % item for item in keys])

    def create_index(self, key_or_list, direction=None, unique=False):
        """Creates an index on this collection.

        Takes either a single key and a direction, or a list of (key, direction)
        pairs. The key(s) must be an instance of (str, unicode), and the
        direction(s) must be one of (`pymongo.ASCENDING`, `pymongo.DESCENDING`).
        Returns the name of the created index.

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction) pairs
            specifying the index to ensure
          - `direction` (optional): must be included if key_or_list is a single
            key, otherwise must be None
          - `unique` (optional): should this index guarantee uniqueness?
        """
        to_save = SON()
        keys = pymongo._index_list(key_or_list, direction)
        to_save["name"] = self._gen_index_name(keys)
        to_save["ns"] = self.full_name()
        to_save["key"] = pymongo._index_document(keys)
        to_save["unique"] = unique

        self.__database.system.indexes.save(to_save, False)
        return to_save["name"]

    def drop_indexes(self):
        """Drops all indexes on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error.
        """
        self.drop_index(u"*")

    def drop_index(self, index_or_name):
        """Drops the specified index on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error. `index_or_name` can be either an
        index name (as returned by `create_index`), or an index specifier (as
        passed to `create_index`). Raises TypeError if index is not an
        instance of (str, unicode, list).

        :Parameters:
          - `index_or_name`: index (or name of index) to drop
        """
        name = index_or_name
        if isinstance(index_or_name, types.ListType):
            name = self._gen_index_name(index_or_name)

        if not isinstance(name, types.StringTypes):
            raise TypeError("index_or_name must be an index name or list")

        self.__database._command(SON([("deleteIndexes",
                                       self.__collection_name),
                                      ("index", name)]),
                                 ["ns not found"])

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

    def group(self, keys, condition, initial, reduce):
        """Perform a query similar to an SQL group by operation.

        Returns an array of grouped items.

        :Parameters:
          - `keys`: list of fields to group by
          - `condition`: specification of rows to be considered (as a `find`
            query specification)
          - `initial`: initial value of the aggregation counter object
          - `reduce`: aggregation function as a JavaScript string
        """
        group_function = """function () {
    var c = db[ns].find(condition);
    var map = new Map();
    var reduce_function = %s;
    while (c.hasNext()) {
        var obj = c.next();

        var key = {};
        for (var i in keys) {
            key[keys[i]] = obj[keys[i]];
        }

        var aggObj = map[key];
        if (aggObj == null) {
            var newObj = Object.extend({}, key);
            aggObj = map[key] = Object.extend(newObj, initial);
        }
        reduce_function(obj, aggObj);
    }
    return {"result": map.values()};
}""" % reduce
        return self.__database.eval(Code(group_function, {"ns": self.__collection_name,
                                                          "keys": keys,
                                                          "condition": condition,
                                                          "initial": initial}))["result"]

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Collection' object is not iterable")
