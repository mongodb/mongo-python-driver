# Copyright 2009-2010 10gen, Inc.
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

import warnings
import struct

from pymongo import (helpers,
                     message)
from pymongo.code import Code
from pymongo.cursor import Cursor
from pymongo.errors import InvalidName
from pymongo.objectid import ObjectId
from pymongo.son import SON

_ZERO = "\x00\x00\x00\x00"


def _gen_index_name(keys):
    """Generate an index name from the set of fields it is over.
    """
    return u"_".join([u"%s_%s" % item for item in keys])


class Collection(object):
    """A Mongo collection.
    """

    def __init__(self, database, name, options=None, create=False, **kwargs):
        """Get / create a Mongo collection.

        Raises :class:`TypeError` if `name` is not an instance of
        :class:`basestring`. Raises
        :class:`~pymongo.errors.InvalidName` if `name` is not a valid
        collection name. Any additional keyword arguments will be used
        as options passed to the create command. See
        :meth:`~pymongo.database.Database.create_collection` for valid
        options.

        If `create` is ``True`` or additional keyword arguments are
        present a create command will be sent. Otherwise, a create
        command will not be sent and the collection will be created
        implicitly on first use.

        :Parameters:
          - `database`: the database to get a collection from
          - `name`: the name of the collection to get
          - `options`: DEPRECATED dictionary of collection options
          - `create` (optional): if ``True``, force collection
            creation even without options being set
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 1.5
           deprecating `options` in favor of kwargs
        .. versionadded:: 1.5
           the `create` parameter

        .. mongodoc:: collections
        """
        if not isinstance(name, basestring):
            raise TypeError("name must be an instance of basestring")

        if options is not None:
            warnings.warn("the options argument to Collection is deprecated "
                          "and will be removed. please use kwargs instead.",
                          DeprecationWarning)
            if not isinstance(options, dict):
                raise TypeError("options must be an instance of dict")
            options.update(kwargs)
        elif kwargs:
            options = kwargs

        if not name or ".." in name:
            raise InvalidName("collection names cannot be empty")
        if "$" in name and not (name.startswith("oplog.$main") or
                                name.startswith("$cmd")):
            raise InvalidName("collection names must not "
                              "contain '$': %r" % name)
        if name[0] == "." or name[-1] == ".":
            raise InvalidName("collecion names must not start "
                              "or end with '.': %r" % name)
        if "\x00" in name:
            raise InvalidName("collection names must not contain the "
                              "null character")

        self.__database = database
        self.__name = unicode(name)
        self.__full_name = u"%s.%s" % (self.__database.name, self.__name)
        # TODO remove the callable_value wrappers after deprecation is complete
        self.__database_w = helpers.callable_value(self.__database,
                                                   "Collection.database")
        self.__name_w = helpers.callable_value(self.__name,
                                               "Collection.name")
        self.__full_name_w = helpers.callable_value(self.__full_name,
                                                    "Collection.full_name")
        if create or options is not None:
            self.__create(options)

    def __create(self, options):
        """Sends a create command with the given options.
        """

        # Send size as a float, not an int/long. BSON can only handle 32-bit
        # ints which conflicts w/ max collection size of 10000000000.
        if options and "size" in options:
            options["size"] = float(options["size"])

        command = SON({"create": self.__name})
        command.update(options)

        self.__database.command(command)

    def __getattr__(self, name):
        """Get a sub-collection of this collection by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        return Collection(self.__database, u"%s.%s" % (self.__name, name))

    def __getitem__(self, name):
        return self.__getattr__(name)

    def __repr__(self):
        return "Collection(%r, %r)" % (self.__database, self.__name)

    def __cmp__(self, other):
        if isinstance(other, Collection):
            return cmp((self.__database, self.__name),
                       (other.__database, other.__name))
        return NotImplemented

    @property
    def full_name(self):
        """The full name of this :class:`Collection`.

        The full name is of the form `database_name.collection_name`.

        .. versionchanged:: 1.3
           ``full_name`` is now a property rather than a method. The
           ``full_name()`` method is deprecated.
        """
        return self.__full_name_w

    @property
    def name(self):
        """The name of this :class:`Collection`.

        .. versionchanged:: 1.3
           ``name`` is now a property rather than a method. The
           ``name()`` method is deprecated.
        """
        return self.__name_w

    @property
    def database(self):
        """The :class:`~pymongo.database.Database` that this
        :class:`Collection` is a part of.

        .. versionchanged:: 1.3
           ``database`` is now a property rather than a method. The
           ``database()`` method is deprecated.
        """
        return self.__database_w

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

        .. mongodoc:: insert
        """
        if not isinstance(to_save, dict):
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

        .. versionchanged:: 1.1
           Bulk insert works with any iterable

        .. mongodoc:: insert
        """
        docs = doc_or_docs
        return_one = False
        if isinstance(docs, dict):
            return_one = True
            docs = [docs]

        if manipulate:
            docs = [self.__database._fix_incoming(doc, self) for doc in docs]

        self.__database.connection._send_message(
            message.insert(self.__full_name, docs, check_keys, safe), safe)

        ids = [doc.get("_id", None) for doc in docs]
        return return_one and ids[0] or ids

    def update(self, spec, document,
               upsert=False, manipulate=False, safe=False, multi=False):
        """Update a document(s) in this collection.

        Raises :class:`TypeError` if either `spec` or `document` is
        not an instance of ``dict`` or `upsert` is not an instance of
        ``bool``. If `safe` is ``True`` then the update will be
        checked for errors, raising
        :class:`~pymongo.errors.OperationFailure` if one
        occurred. Safe updates require a response from the database,
        while normal updates do not - thus, setting `safe` to ``True``
        will negatively impact performance.

        There are many useful `update modifiers`_ which can be used
        when performing updates. For example, here we use the
        ``"$set"`` modifier to modify some fields in a matching
        document:

        .. doctest::

          >>> db.test.insert({"x": "y", "a": "b"})
          ObjectId('...')
          >>> list(db.test.find())
          [{u'a': u'b', u'x': u'y', u'_id': ObjectId('...')}]
          >>> db.test.update({"x": "y"}, {"$set": {"a": "c"}})
          >>> list(db.test.find())
          [{u'a': u'c', u'x': u'y', u'_id': ObjectId('...')}]

        If `safe` is ``True`` returns the response to the *lastError*
        command. Otherwise, returns ``None``.

        :Parameters:
          - `spec`: a ``dict`` or :class:`~pymongo.son.SON` instance
            specifying elements which must be present for a document
            to be updated
          - `document`: a ``dict`` or :class:`~pymongo.son.SON`
            instance specifying the document to be used for the update
            or (in the case of an upsert) insert - see docs on MongoDB
            `update modifiers`_
          - `upsert` (optional): perform an `upsert`_ if ``True``
          - `manipulate` (optional): manipulate the document before
            updating? If ``True`` all instances of
            :mod:`~pymongo.son_manipulator.SONManipulator` added to
            this :class:`~pymongo.database.Database` will be applied
            to the document before performing the update.
          - `safe` (optional): check that the update succeeded?
          - `multi` (optional): update all documents that match
            `spec`, rather than just the first matching document. The
            default value for `multi` is currently ``False``, but this
            might eventually change to ``True``. It is recommended
            that you specify this argument explicitly for all update
            operations in order to prepare your code for that change.

        .. versionchanged:: 1.4
           Return the response to *lastError* if `safe` is ``True``.
        .. versionadded:: 1.1.1
           The `multi` parameter.

        .. _update modifiers: http://www.mongodb.org/display/DOCS/Updating
        .. _upsert: http://www.mongodb.org/display/DOCS/Updating#Updating-Upserts

        .. mongodoc:: update
        """
        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(document, dict):
            raise TypeError("document must be an instance of dict")
        if not isinstance(upsert, bool):
            raise TypeError("upsert must be an instance of bool")

        if upsert and manipulate:
            document = self.__database._fix_incoming(document, self)

        return self.__database.connection._send_message(
            message.update(self.__full_name, upsert, multi,
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

        If `safe` is ``True`` returns the response to the *lastError*
        command. Otherwise, returns ``None``.

        :Parameters:
          - `spec_or_object_id` (optional): a ``dict`` or
            :class:`~pymongo.son.SON` instance specifying which documents
            should be removed; or an instance of
            :class:`~pymongo.objectid.ObjectId` specifying the value of the
            ``_id`` field for the document to be removed
          - `safe` (optional): check that the remove succeeded?

        .. versionchanged:: 1.4
           Return the response to *lastError* if `safe` is ``True``.
        .. versionchanged:: 1.2
           The `spec_or_object_id` parameter is now optional. If it is
           not specified *all* documents in the collection will be
           removed.
        .. versionadded:: 1.1
           The `safe` parameter.

        .. mongodoc:: remove
        """
        spec = spec_or_object_id
        if spec is None:
            spec = {}
        if isinstance(spec, ObjectId):
            spec = {"_id": spec}

        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict, not %s" %
                            type(spec))

        return self.__database.connection._send_message(
            message.delete(self.__full_name, spec, safe), safe)

    def find_one(self, spec_or_object_id=None, fields=None,
                 _sock=None, _must_use_master=False, _is_command=False):
        """Get a single object from the database.

        Raises TypeError if the argument is of an improper type. Returns a
        single SON object, or None if no result is found.

        :Parameters:
          - `spec_or_object_id` (optional): a SON object specifying elements
            which must be present for a document to be returned OR an instance
            of ObjectId to be used as the value for an _id query
          - `fields` (optional): a list of field names that should be included
            in the returned document ("_id" will always be included)
        """
        spec = spec_or_object_id
        if spec is None:
            spec = SON()
        if isinstance(spec, ObjectId):
            spec = SON({"_id": spec})

        for result in self.find(spec, limit=-1, fields=fields,
                                _sock=_sock, _must_use_master=_must_use_master,
                                _is_command=_is_command):
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
            if not isinstance(field, basestring):
                raise TypeError("fields must be a list of key names as "
                                "(string, unicode)")
            as_dict[field] = 1
        return as_dict

    def find(self, spec=None, fields=None, skip=0, limit=0,
             timeout=True, snapshot=False, tailable=False,
             _sock=None, _must_use_master=False, _is_command=False):
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

        .. versionadded:: 1.1
           The `tailable` parameter.

        .. mongodoc:: find
        """
        if spec is None:
            spec = SON()

        slave_okay = self.__database.connection.slave_okay

        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if fields is not None and not isinstance(fields, list):
            raise TypeError("fields must be an instance of list")
        if not isinstance(skip, int):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, int):
            raise TypeError("limit must be an instance of int")
        if not isinstance(timeout, bool):
            raise TypeError("timeout must be an instance of bool")
        if not isinstance(snapshot, bool):
            raise TypeError("snapshot must be an instance of bool")
        if not isinstance(tailable, bool):
            raise TypeError("tailable must be an instance of bool")

        if fields is not None:
            if not fields:
                fields = ["_id"]
            fields = self._fields_list_to_dict(fields)

        return Cursor(self, spec, fields, skip, limit, slave_okay, timeout,
                      tailable, snapshot, _sock=_sock,
                      _must_use_master=_must_use_master,
                      _is_command=_is_command)

    def count(self):
        """Get the number of documents in this collection.

        To get the number of documents matching a specific query use
        :meth:`pymongo.cursor.Cursor.count`.
        """
        return self.find().count()

    def create_index(self, key_or_list, deprecated_unique=None, ttl=300, **kwargs):
        """Creates an index on this collection.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`, and the
        directions must be one of (:data:`~pymongo.ASCENDING`,
        :data:`~pymongo.DESCENDING`, :data:`~pymongo.GEO2D`). Returns
        the name of the created index.

        To create a single key index on the key ``'mike'`` we just use
        a string argument:

        >>> my_collection.create_index("mike")

        For a `compound index`_ on ``'mike'`` descending and
        ``'eliot'`` ascending we need to use a list of tuples:

        >>> my_collection.create_index([("mike", pymongo.DESCENDING),
        ...                             ("eliot", pymongo.ASCENDING)])

        All optional index creation paramaters should be passed as
        keyword arguments to this method. Valid options include:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated
          - `unique`: should this index guarantee uniqueness?
          - `dropDups` or `drop_dups`: should we drop duplicates
            during index creation when creating a unique index?
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction)
            pairs specifying the index to create
          - `deprecated_unique`: DEPRECATED - use `unique` as a kwarg
          - `ttl` (optional): time window (in seconds) during which
            this index will be recognized by subsequent calls to
            :meth:`ensure_index` - see documentation for
            :meth:`ensure_index` for details
          - `kwargs` (optional): any additional index creation options
            (see the above list) should be passed as keyword arguments

        .. versionchanged:: 1.5.1
           Accept kwargs to support all index creation options.

        .. versionadded:: 1.5
           The `name` parameter.

        .. seealso:: :meth:`ensure_index`

        .. _compound index: http://www.mongodb.org/display/DOCS/Indexes#Indexes-CompoundKeysIndexes

        .. mongodoc:: indexes
        """
        keys = helpers._index_list(key_or_list)
        index_doc = helpers._index_document(keys)

        index = {"key": index_doc, "ns": self.__full_name}

        if deprecated_unique is not None:
            warnings.warn("using a positional arg to specify unique is "
                          "deprecated, please use kwargs",
                          DeprecationWarning)
            index["unique"] = deprecated_unique

        name = "name" in kwargs and kwargs["name"] or _gen_index_name(keys)
        index["name"] = name

        if "drop_dups" in kwargs:
            kwargs["dropDups"] = kwargs.pop("drop_dups")

        index.update(kwargs)

        self.__database.connection._cache_index(self.__database.name,
                                                self.__name, name, ttl)

        self.__database.system.indexes.insert(index, manipulate=False,
                                              check_keys=False)
        return name

    def ensure_index(self, key_or_list, deprecated_unique=None, ttl=300, **kwargs):
        """Ensures that an index exists on this collection.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`, and the
        direction(s) must be one of (:data:`~pymongo.ASCENDING`,
        :data:`~pymongo.DESCENDING`, :data:`~pymongo.GEO2D`). See
        :meth:`create_index` for a detailed example.

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

        All optional index creation paramaters should be passed as
        keyword arguments to this method. Valid options include:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated
          - `unique`: should this index guarantee uniqueness?
          - `dropDups` or `drop_dups`: should we drop duplicates
            during index creation when creating a unique index?
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction)
            pairs specifying the index to create
          - `deprecated_unique`: DEPRECATED - use `unique` as a kwarg
          - `ttl` (optional): time window (in seconds) during which
            this index will be recognized by subsequent calls to
            :meth:`ensure_index`
          - `kwargs` (optional): any additional index creation options
            (see the above list) should be passed as keyword arguments

        .. versionchanged:: 1.5.1
           Accept kwargs to support all index creation options.

        .. versionadded:: 1.5
           The `name` parameter.

        .. seealso:: :meth:`create_index`
        """
        if "name" in kwargs:
            name = kwargs["name"]
        else:
            keys = helpers._index_list(key_or_list)
            name = kwargs["name"] = _gen_index_name(keys)

        if self.__database.connection._cache_index(self.__database.name,
                                                   self.__name, name, ttl):
            return self.create_index(key_or_list, deprecated_unique, ttl, **kwargs)
        return None

    def drop_indexes(self):
        """Drops all indexes on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error.
        """
        self.__database.connection._purge_index(self.__database.name,
                                                self.__name)
        self.drop_index(u"*")

    def drop_index(self, index_or_name):
        """Drops the specified index on this collection.

        Can be used on non-existant collections or collections with no
        indexes.  Raises OperationFailure on an error. `index_or_name`
        can be either an index name (as returned by `create_index`),
        or an index specifier (as passed to `create_index`). An index
        specifier should be a list of (key, direction) pairs. Raises
        TypeError if index is not an instance of (str, unicode, list).

        .. warning:: if a custom name was used on index creation (by
          passing the `name` parameter to :meth:`create_index` or
          :meth:`ensure_index`) the index **must** be dropped by name.

        :Parameters:
          - `index_or_name`: index (or name of index) to drop
        """
        name = index_or_name
        if isinstance(index_or_name, list):
            name = _gen_index_name(index_or_name)

        if not isinstance(name, basestring):
            raise TypeError("index_or_name must be an index name or list")

        self.__database.connection._purge_index(self.__database.name,
                                                self.__name, name)
        self.__database.command(SON([("deleteIndexes",
                                      self.__name),
                                     ("index", name)]),
                                ["ns not found"])

    def index_information(self):
        """Get information on this collection's indexes.

        Returns a dictionary where the keys are index names (as returned by
        create_index()) and the values are lists of (key, direction) pairs
        specifying the index (as passed to create_index()).
        """
        raw = self.__database.system.indexes.find({"ns": self.__full_name})
        info = {}
        for index in raw:
            info[index["name"]] = index["key"].items()
        return info

    def options(self):
        """Get the options set on this collection.

        Returns a dictionary of options and their values - see
        :meth:`~pymongo.database.Database.create_collection` for more
        information on the possible options. Returns an empty
        dictionary if the collection has not been created yet.
        """
        result = self.__database.system.namespaces.find_one(
            {"name": self.__full_name})

        if not result:
            return {}

        options = result.get("options", {})
        if "create" in options:
            del options["create"]

        return options

    # TODO key and condition ought to be optional, but deprecation
    # could be painful as argument order would have to change.
    def group(self, key, condition, initial, reduce, finalize=None,
              command=True):
        """Perform a query similar to an SQL *group by* operation.

        Returns an array of grouped items.

        The `key` parameter can be:

          - ``None`` to use the entire document as a key.
          - A :class:`list` of keys (each a :class:`basestring`) to group by.
          - A :class:`basestring` or :class:`~pymongo.code.Code` instance
            containing a JavaScript function to be applied to each document,
            returning the key to group by.

        :Parameters:
          - `key`: fields to group by (see above description)
          - `condition`: specification of rows to be
            considered (as a :meth:`find` query specification)
          - `initial`: initial value of the aggregation counter object
          - `reduce`: aggregation function as a JavaScript string
          - `finalize`: function to be called on each object in output list.
          - `command` (optional): DEPRECATED if ``True``, run the group as a
            command instead of in an eval - this option is deprecated and
            will be removed in favor of running all groups as commands

        .. versionchanged:: 1.4
           The `key` argument can now be ``None`` or a JavaScript function,
           in addition to a :class:`list` of keys.
        .. versionchanged:: 1.3
           The `command` argument now defaults to ``True`` and is deprecated.
        """
        if not command:
            warnings.warn("eval-based groups are deprecated, and the "
                          "command option will be removed.",
                          DeprecationWarning)

        group = {}
        if isinstance(key, basestring):
            group["$keyf"] = Code(key)
        elif key is not None:
            group = {"key": self._fields_list_to_dict(key)}
        group["ns"] = self.__name
        group["$reduce"] = Code(reduce)
        group["cond"] = condition
        group["initial"] = initial
        if finalize is not None:
            group["finalize"] = Code(finalize)

        return self.__database.command({"group":group})["retval"]

    def rename(self, new_name):
        """Rename this collection.

        If operating in auth mode, client must be authorized as an
        admin to perform this operation. Raises :class:`TypeError` if
        `new_name` is not an instance of :class:`basestring`. Raises
        :class:`~pymongo.errors.InvalidName` if `new_name` is not a
        valid collection name.

        :Parameters:
          - `new_name`: new name for this collection
        """
        if not isinstance(new_name, basestring):
            raise TypeError("new_name must be an instance of basestring")

        if not new_name or ".." in new_name:
            raise InvalidName("collection names cannot be empty")
        if "$" in new_name:
            raise InvalidName("collection names must not contain '$'")
        if new_name[0] == "." or new_name[-1] == ".":
            raise InvalidName("collecion names must not start or end with '.'")

        rename_command = SON([("renameCollection", self.__full_name),
                              ("to", "%s.%s" % (self.__database.name,
                                                new_name))])

        self.__database.connection.admin.command(rename_command)

    def distinct(self, key):
        """Get a list of distinct values for `key` among all documents
        in this collection.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring`.

        To get the distinct values for a key in the result set of a
        query use :meth:`~pymongo.cursor.Cursor.distinct`.

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

        .. mongodoc:: mapreduce
        """
        command = SON([("mapreduce", self.__name),
                       ("map", map), ("reduce", reduce)])
        command.update(**kwargs)

        response = self.__database.command(command)
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
        if "." not in self.__name:
            raise TypeError("'Collection' object is not callable. If you "
                            "meant to call the '%s' method on a 'Database' "
                            "object it is failing because no such method "
                            "exists." %
                            self.__name)
        raise TypeError("'Collection' object is not callable. If you meant to "
                        "call the '%s' method on a 'Collection' object it is "
                        "failing because no such method exists." %
                        self.__name.split(".")[-1])
