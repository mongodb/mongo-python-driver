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
import warnings
import struct

import helpers
import message
from objectid import ObjectId
from cursor import Cursor
from son import SON
from errors import InvalidName, OperationFailure
from code import Code

_ZERO = "\x00\x00\x00\x00"


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
        if "$" in name and not (name.startswith("oplog.$main") or
                                name.startswith("$cmd")):
            raise InvalidName("collection names must not "
                              "contain '$': %r" % name)
        if name[0] == "." or name[-1] == ".":
            raise InvalidName("collecion names must not start "
                              "or end with '.': %r" % name)

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
        return Collection(self.__database, u"%s.%s" % (self.__collection_name,
                                                       name))

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

    def database(self):
        """Get the database that this collection is a part of.
        """
        return self.__database

    def save(self, to_save, manipulate=True, safe=False):
        """Save a document in this collection.

        If `to_save` already has an '_id' then an update (upsert) operation
        is performed and any existing document with that _id is overwritten.
        Otherwise an '_id' will be added to `to_save` and an insert operation
        is performed. Returns the _id of the saved document.

        Raises TypeError if to_save is not an instance of dict. If `safe`
        is True then the save will be checked for errors, raising
        OperationFailure if one occurred. Safe inserts wait for a
        response from the database, while normal inserts do not. Returns the
        _id of the saved document.

        :Parameters:
          - `to_save`: the SON object to be saved
          - `manipulate` (optional): manipulate the SON object before saving it
          - `safe` (optional): check that the save succeeded?
        """
        if not isinstance(to_save, types.DictType):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if "_id" not in to_save:
            return self.insert(to_save, manipulate, safe)
        else:
            self.update({"_id": to_save["_id"]}, to_save, True,
                        manipulate, safe)
            return to_save.get("_id", None)

    def insert(self, doc_or_docs,
               manipulate=True, safe=False, check_keys=True):
        """Insert a document(s) into this collection.

        If manipulate is set the document(s) are manipulated using any
        SONManipulators that have been added to this database. Returns the _id
        of the inserted document or a list of _ids of the inserted documents.
        If the document(s) does not already contain an '_id' one will be added.
        If `safe` is True then the insert will be checked for errors, raising
        OperationFailure if one occurred. Safe inserts wait for a response from
        the database, while normal inserts do not.

        :Parameters:
          - `doc_or_docs`: a SON object or list of SON objects to be inserted
          - `manipulate` (optional): manipulate the documents before inserting?
          - `safe` (optional): check that the insert succeeded?
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising `pymongo.errors.InvalidName` in either case
        """
        docs = doc_or_docs
        if isinstance(docs, types.DictType):
            docs = [docs]

        if manipulate:
            docs = [self.__database._fix_incoming(doc, self) for doc in docs]

        self.__database.connection()._send_message(
            message.insert(self.full_name(), docs, check_keys, safe), safe)

        ids = [doc.get("_id", None) for doc in docs]
        return len(ids) == 1 and ids[0] or ids

    def update(self, spec, document,
               upsert=False, manipulate=False, safe=False, multi=False):
        """Update a document(s) in this collection.

        Raises :class:`TypeError` if either `spec` or `document` is not an
        instance of ``dict`` or `upsert` is not an instance of ``bool``. If
        `safe` is ``True`` then the update will be checked for errors, raising
        :class:`~pymongo.errors.OperationFailure` if one occurred. Safe inserts
        wait for a response from the database, while normal inserts do not.

        There are many useful `update modifiers`_ which can be used when
        performing updates. For example, here we use the ``"$set"`` modifier to
        modify some fields in a matching document::

          >>> db.test.insert({"x": "y", "a": "b"})
          ObjectId('...')
          >>> list(db.test.find())
          [{u'a': u'b', u'x': u'y', u'_id': ObjectId('...')}]
          >>> db.test.update({"x": "y"}, {"$set": {"a": "c"}})
          >>> list(db.test.find())
          [{u'a': u'c', u'x': u'y', u'_id': ObjectId('...')}]

        :Parameters:
          - `spec`: a ``dict`` or :class:`~pymongo.son.SON` instance specifying
            elements which must be present for a document to be updated
          - `document`: a ``dict`` or :class:`~pymongo.son.SON` instance
            specifying the document to be used for the update or (in the case
            of an upsert) insert - see docs on MongoDB `update modifiers`_
          - `upsert` (optional): perform an upsert operation
          - `manipulate` (optional): manipulate the document before updating?
          - `safe` (optional): check that the update succeeded?
          - `multi` (optional): update all documents that match `spec`, rather
            than just the first matching document. The default value for
            `multi` is currently ``False``, but this might eventually change to
            ``True``. It is recommended that you specify this argument
            explicitly for all update operations in order to prepare your code
            for that change.

        .. versionadded:: 1.1.1
           The `multi` parameter.

        .. _update modifiers: http://www.mongodb.org/display/DOCS/Updating
        """
        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(document, types.DictType):
            raise TypeError("document must be an instance of dict")
        if not isinstance(upsert, types.BooleanType):
            raise TypeError("upsert must be an instance of bool")

        if upsert and manipulate:
            document = self.__database._fix_incoming(document, self)

        self.__database.connection()._send_message(
            message.update(self.full_name(), upsert, multi,
                           spec, document, safe), safe)

    def remove(self, spec_or_object_id=None, safe=False):
        """Remove a document(s) from this collection.

        .. warning:: Calls to :meth:`remove` should be performed with
           care, as removed data cannot be restored.

        Raises :class:`~pymongo.errors.TypeError` if
        `spec_or_object_id` is not an instance of (``dict``,
        :class:`~pymongo.objectid.ObjectId`). If `safe` is ``True``
        then the remove operation will be checked for errors, raising
        :class:`~pymongo.errors.OperationFailure` if one
        occurred. Safe removes wait for a response from the database,
        while normal removes do not.

        If no `spec_or_object_id` is given all documents in this
        collection will be removed. This is not equivalent to calling
        :meth:`~pymongo.database.Database.drop_collection`, however, as
        indexes will not be removed.

        :Parameters:
          - `spec_or_object_id` (optional): a ``dict`` or
            :class:`~pymongo.son.SON` instance specifying which documents
            should be removed; or an instance of
            :class:`~pymongo.objectid.ObjectId` specifying the value of the
            ``_id`` field for the document to be removed
          - `safe` (optional): check that the remove succeeded?

        .. versionchanged:: 1.2
           The `spec_or_object_id` parameter is now optional. If it is
           not specified *all* documents in the collection will be
           removed.
        """
        spec = spec_or_object_id
        if spec is None:
            spec = {}
        if isinstance(spec, ObjectId):
            spec = {"_id": spec}

        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict, not %s" %
                            type(spec))

        self.__database.connection()._send_message(
            message.delete(self.full_name(), spec, safe), safe)

    def find_one(self, spec_or_object_id=None, fields=None, slave_okay=None,
                 _sock=None, _must_use_master=False):
        """Get a single object from the database.

        Raises TypeError if the argument is of an improper type. Returns a
        single SON object, or None if no result is found.

        :Parameters:
          - `spec_or_object_id` (optional): a SON object specifying elements
            which must be present for a document to be returned OR an instance
            of ObjectId to be used as the value for an _id query
          - `fields` (optional): a list of field names that should be included
            in the returned document ("_id" will always be included)
          - `slave_okay` (optional): DEPRECATED this option is deprecated and
            will be removed - see the slave_okay parameter to
            `pymongo.Connection.__init__`.
        """
        spec = spec_or_object_id
        if spec is None:
            spec = SON()
        if isinstance(spec, ObjectId):
            spec = SON({"_id": spec})

        for result in self.find(spec, limit=-1, fields=fields,
                                slave_okay=slave_okay, _sock=_sock,
                                _must_use_master=_must_use_master):
            return result
        return None

    def _fields_list_to_dict(self, fields):
        """Takes a list of field names and returns a matching dictionary.

        ["a", "b"] becomes {"a": 1, "b": 1}

        and

        ["a.b.c", "d", "a.c"] becomes {"a.b.c": 1, "d": 1, "a.c": 1}
        """
        as_dict = {}
        for field in fields:
            if not isinstance(field, types.StringTypes):
                raise TypeError("fields must be a list of key names as "
                                "(string, unicode)")
            as_dict[field] = 1
        return as_dict

    def find(self, spec=None, fields=None, skip=0, limit=0,
             slave_okay=None, timeout=True, snapshot=False, tailable=False,
             _sock=None, _must_use_master=False):
        """Query the database.

        The `spec` argument is a prototype document that all results must
        match. For example:

        >>> db.test.find({"hello": "world"})

        only matches documents that have a key "hello" with value "world".
        Matches can have other keys *in addition* to "hello". The `fields`
        argument is used to specify a subset of fields that should be included
        in the result documents. By limiting results to a certain subset of
        fields you can cut down on network traffic and decoding time.

        Raises TypeError if any of the arguments are of improper type. Returns
        an instance of Cursor corresponding to this query.

        :Parameters:
          - `spec` (optional): a SON object specifying elements which must be
            present for a document to be included in the result set
          - `fields` (optional): a list of field names that should be returned
            in the result set ("_id" will always be included)
          - `skip` (optional): the number of documents to omit (from the start
            of the result set) when returning the results
          - `limit` (optional): the maximum number of results to return
          - `slave_okay` (optional): DEPRECATED this option is deprecated and
            will be removed - see the slave_okay parameter to
            `pymongo.Connection.__init__`.
          - `timeout` (optional): if True, any returned cursor will be subject
            to the normal timeout behavior of the mongod process. Otherwise,
            the returned cursor will never timeout at the server. Care should
            be taken to ensure that cursors with timeout turned off are
            properly closed.
          - `snapshot` (optional): if True, snapshot mode will be used for this
            query. Snapshot mode assures no duplicates are returned, or objects
            missed, which were present at both the start and end of the query's
            execution. For details, see the `snapshot documentation
            <http://www.mongodb.org/display/DOCS/How+to+do+Snapshotting+in+the+Mongo+Database>`_.
          - `tailable` (optional): the result of this find call will be a
            tailable cursor - tailable cursors aren't closed when the last data
            is retrieved but are kept open and the cursors location marks the
            final document's position. if more data is received iteration of
            the cursor will continue from the last document received. For
            details, see the `tailable cursor documentation
            <http://www.mongodb.org/display/DOCS/Tailable+Cursors>`_.
        """
        if spec is None:
            spec = SON()
        if slave_okay is None:
            slave_okay = self.__database.connection().slave_okay
        else:
            warnings.warn("The slave_okay option to find and find_one is "
                          "deprecated. Please set slave_okay on the Connection "
                          "itself.",
                          DeprecationWarning)

        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(fields, (types.ListType, types.NoneType)):
            raise TypeError("fields must be an instance of list")
        if not isinstance(skip, types.IntType):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, types.IntType):
            raise TypeError("limit must be an instance of int")
        if not isinstance(slave_okay, types.BooleanType):
            raise TypeError("slave_okay must be an instance of bool")
        if not isinstance(timeout, types.BooleanType):
            raise TypeError("timeout must be an instance of bool")
        if not isinstance(snapshot, types.BooleanType):
            raise TypeError("snapshot must be an instance of bool")
        if not isinstance(tailable, types.BooleanType):
            raise TypeError("tailable must be an instance of bool")

        if fields is not None:
            if not fields:
                fields = ["_id"]
            fields = self._fields_list_to_dict(fields)

        return Cursor(self, spec, fields, skip, limit, slave_okay, timeout,
                      tailable, snapshot, _sock=_sock,
                      _must_use_master=_must_use_master)

    def count(self):
        """Get the number of documents in this collection.

        To get the number of documents matching a specific query use
        :meth:`pymongo.cursor.Cursor.count`.
        """
        return self.find().count()

    def _gen_index_name(self, keys):
        """Generate an index name from the set of fields it is over.
        """
        return u"_".join([u"%s_%s" % item for item in keys])

    def create_index(self, key_or_list, direction=None, unique=False, ttl=300):
        """Creates an index on this collection.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of ``(str, unicode)``, and the
        directions must be one of (:data:`~pymongo.ASCENDING`,
        :data:`~pymongo.DESCENDING`). Returns the name of the created
        index.

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction) pairs
            specifying the index to create
          - `direction` (optional): DEPRECATED this option will be removed
          - `unique` (optional): should this index guarantee uniqueness?
          - `ttl` (optional): time window (in seconds) during which this index
            will be recognized by subsequent calls to :meth:`ensure_index` -
            see documentation for :meth:`ensure_index` for details
        """
        if not isinstance(key_or_list, (str, unicode, list)):
            raise TypeError("key_or_list must either be a single key or a list of (key, direction) pairs")

        if direction is not None:
            warnings.warn("specifying a direction for a single key index is "
                          "deprecated and will be removed. there is no need "
                          "for a direction on a single key index",
                          DeprecationWarning)

        to_save = SON()
        keys = helpers._index_list(key_or_list)
        name = self._gen_index_name(keys)
        to_save["name"] = name
        to_save["ns"] = self.full_name()
        to_save["key"] = helpers._index_document(keys)
        to_save["unique"] = unique

        self.database().connection()._cache_index(self.__database.name(),
                                                  self.name(),
                                                  name, ttl)

        self.database().system.indexes.insert(to_save, manipulate=False,
                                              check_keys=False)
        return to_save["name"]

    def ensure_index(self, key_or_list, direction=None, unique=False, ttl=300):
        """Ensures that an index exists on this collection.

        Takes either a single key or a list of (key, direction)
        pairs.  The key(s) must be an instance of ``(str, unicode)``,
        and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`).

        Unlike :meth:`create_index`, which attempts to create an index
        unconditionally, :meth:`ensure_index` takes advantage of some
        caching within the driver such that it only attempts to create
        indexes that might not already exist. When an index is created
        (or ensured) by PyMongo it is "remembered" for `ttl`
        seconds. Repeated calls to :meth:`ensure_index` within that
        time limit will be lightweight - they will not attempt to
        actually create the index.

        Care must be taken when the database is being accessed through
        multiple connections at once. If an index is created using
        PyMongo and then deleted using another connection any call to
        :meth:`ensure_index` within the cache window will fail to
        re-create the missing index.

        Returns the name of the created index if an index is actually
        created. Returns ``None`` if the index already exists.

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction) pairs
            specifying the index to ensure
          - `direction` (optional): DEPRECATED this option will be removed
          - `unique` (optional): should this index guarantee uniqueness?
          - `ttl` (optional): time window (in seconds) during which this index
            will be recognized by subsequent calls to :meth:`ensure_index`
        """
        if not isinstance(key_or_list, (str, unicode, list)):
            raise TypeError("key_or_list must either be a single key or a list of (key, direction) pairs")

        if direction is not None:
            warnings.warn("specifying a direction for a single key index is "
                          "deprecated and will be removed. there is no need "
                          "for a direction on a single key index",
                          DeprecationWarning)

        keys = helpers._index_list(key_or_list)
        name = self._gen_index_name(keys)
        if self.database().connection()._cache_index(self.__database.name(),
                                                     self.name(),
                                                     name, ttl):
            return self.create_index(key_or_list, unique=unique, ttl=ttl)
        return None

    def drop_indexes(self):
        """Drops all indexes on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error.
        """
        self.database().connection()._purge_index(self.database().name(),
                                                  self.name())
        self.drop_index(u"*")

    def drop_index(self, index_or_name):
        """Drops the specified index on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error. `index_or_name` can be either an
        index name (as returned by `create_index`), or an index specifier (as
        passed to `create_index`). An index specifier should be a list of (key,
        direction) pairs. Raises TypeError if index is not an instance of (str,
        unicode, list).

        :Parameters:
          - `index_or_name`: index (or name of index) to drop
        """
        name = index_or_name
        if isinstance(index_or_name, types.ListType):
            name = self._gen_index_name(index_or_name)

        if not isinstance(name, types.StringTypes):
            raise TypeError("index_or_name must be an index name or list")

        self.database().connection()._purge_index(self.database().name(),
                                                  self.name(), name)
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

    # TODO send all groups as commands once 1.2 is out
    #
    # Waiting on this because group command support for CodeWScope
    # wasn't added until 1.1
    def group(self, keys, condition, initial, reduce, finalize=None,
              command=False):
        """Perform a query similar to an SQL group by operation.

        Returns an array of grouped items.

        :Parameters:
          - `keys`: list of fields to group by
          - `condition`: specification of rows to be considered (as a `find`
            query specification)
          - `initial`: initial value of the aggregation counter object
          - `reduce`: aggregation function as a JavaScript string
          - `finalize`: function to be called on each object in output list.
          - `command` (optional): if True, run the group as a command instead
            of in an eval - it is likely that this option will eventually be
            deprecated and all groups will be run as commands. Please only use
            as a keyword argument, not as a positional argument.
        """

        #for now support people passing command in its old position
        if finalize in (True, False):
            command = finalize
            finalize = None
            warnings.warn("Please only pass 'command' as a keyword argument."
                         ,DeprecationWarning)

        if command:
            if not isinstance(reduce, Code):
                reduce = Code(reduce)
            group = {"ns": self.__collection_name,
                    "$reduce": reduce,
                    "key": self._fields_list_to_dict(keys),
                    "cond": condition,
                    "initial": initial}
            if finalize is not None:
                if not isinstance(finalize, Code):
                    finalize = Code(finalize)
                group["finalize"] = finalize
            return self.__database._command({"group":group})["retval"]

        scope = {}
        if isinstance(reduce, Code):
            scope = reduce.scope
        scope.update({"ns": self.__collection_name,
                      "keys": keys,
                      "condition": condition,
                      "initial": initial})

        group_function = """function () {
    var c = db[ns].find(condition);
    var map = new Map();
    var reduce_function = %s;
    var finalize_function = %s; //function or null
    while (c.hasNext()) {
        var obj = c.next();

        var key = {};
        for (var i = 0; i < keys.length; i++) {
            var k = keys[i];
            key[k] = obj[k];
        }

        var aggObj = map.get(key);
        if (aggObj == null) {
            var newObj = Object.extend({}, key);
            aggObj = Object.extend(newObj, initial);
            map.put(key, aggObj);
        }
        reduce_function(obj, aggObj);
    }

    out = map.values();
    if (finalize_function !== null){
        for (var i=0; i < out.length; i++){
            var ret = finalize_function(out[i]);
            if (ret !== undefined)
                out[i] = ret;
        }
    }

    return {"result": out};
}""" % (reduce, (finalize or 'null'));
        return self.__database.eval(Code(group_function, scope))["result"]

    def rename(self, new_name):
        """Rename this collection.

        If operating in auth mode, client must be authorized as an admin to
        perform this operation. Raises TypeError if new_name is not an instance
        of (str, unicode). Raises InvalidName if new_name is not a valid
        collection name.

        :Parameters:
          - `new_name`: new name for this collection
        """
        if not isinstance(new_name, types.StringTypes):
            raise TypeError("new_name must be an instance of (str, unicode)")

        if not new_name or ".." in new_name:
            raise InvalidName("collection names cannot be empty")
        if "$" in new_name:
            raise InvalidName("collection names must not contain '$'")
        if new_name[0] == "." or new_name[-1] == ".":
            raise InvalidName("collecion names must not start or end with '.'")

        rename_command = SON([("renameCollection", self.full_name()),
                              ("to", "%s.%s" % (self.__database.name(),
                                                new_name))])

        self.__database.connection().admin._command(rename_command)

    def distinct(self, key):
        """Get a list of distinct values for `key` among all documents in this
        collection.

        Raises :class:`TypeError` if `key` is not an instance of
        ``(str, unicode)``.

        To get the distinct values for a key in the result set of a query
        use :meth:`pymongo.cursor.Cursor.distinct`.

        :Parameters:
          - `key`: name of key for which we want to get the distinct values

        .. note:: Requires server version **>= 1.1.0**

        .. versionadded:: 1.1.1
        """
        return self.find().distinct(key)

    def map_reduce(self, map, reduce, full_response=False, **kwargs):
        """Perform a map/reduce operation on this collection.

        If `full_response` is ``False`` (default) returns a
        :class:`~pymongo.collection.Collection` instance containing
        the results of the operation. Otherwise, returns the full
        response from the server to the `map reduce command`_.

        :Parameters:
          - `map`: map function (as a JavaScript string)
          - `reduce`: reduce function (as a JavaScript string)
          - `full_response` (optional): if ``True``, return full response to
            this command - otherwise just return the result collection
          - `**kwargs` (optional): additional arguments to the
            `map reduce command`_ may be passed as keyword arguments to this
            helper method, e.g.::

            >>> db.test.map_reduce(map, reduce, limit=2)

        .. note:: Requires server version **>= 1.1.1**

        .. seealso:: :doc:`/examples/map_reduce`

        .. versionadded:: 1.2

        .. _map reduce command: http://www.mongodb.org/display/DOCS/MapReduce
        """
        command = SON([("mapreduce", self.__collection_name),
                       ("map", map), ("reduce", reduce)])
        command.update(**kwargs)

        response = self.__database._command(command)
        if full_response:
            return response
        return self.__database[response["result"]]

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Collection' object is not iterable")

    def __call__(self, *args, **kwargs):
        """This is only here so that some API misusages are easier to debug.
        """
        if "." not in self.__collection_name:
            raise TypeError("'Collection' object is not callable. If you "
                            "meant to call the '%s' method on a 'Database' "
                            "object it is failing because no such method "
                            "exists." %
                            self.__collection_name)
        raise TypeError("'Collection' object is not callable. If you meant to "
                        "call the '%s' method on a 'Collection' object it is "
                        "failing because no such method exists." %
                        self.__collection_name.split(".")[-1])
