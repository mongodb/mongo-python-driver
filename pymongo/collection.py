# Copyright 2009-2012 10gen, Inc.
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

from bson.binary import ALL_UUID_SUBTYPES, OLD_UUID_SUBTYPE
from bson.code import Code
from bson.son import SON
from pymongo import (common,
                     helpers,
                     message)
from pymongo.cursor import Cursor
from pymongo.errors import ConfigurationError, InvalidName


def _gen_index_name(keys):
    """Generate an index name from the set of fields it is over.
    """
    return u"_".join([u"%s_%s" % item for item in keys])


class Collection(common.BaseObject):
    """A Mongo collection.
    """

    def __init__(self, database, name, create=False, **kwargs):
        """Get / create a Mongo collection.

        Raises :class:`TypeError` if `name` is not an instance of
        :class:`basestring` (:class:`str` in python 3). Raises
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
          - `create` (optional): if ``True``, force collection
            creation even without options being set
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 2.2
           Removed deprecated argument: options

        .. versionadded:: 2.1
           uuid_subtype attribute

        .. versionchanged:: 1.5
           deprecating `options` in favor of kwargs

        .. versionadded:: 1.5
           the `create` parameter

        .. mongodoc:: collections
        """
        super(Collection, self).__init__(
            slave_okay=database.slave_okay,
            read_preference=database.read_preference,
            tag_sets=database.tag_sets,
            secondary_acceptable_latency_ms=(
                database.secondary_acceptable_latency_ms),
            safe=database.safe,
            **(database.get_lasterror_options()))

        if not isinstance(name, basestring):
            raise TypeError("name must be an instance "
                            "of %s" % (basestring.__name__,))

        if not name or ".." in name:
            raise InvalidName("collection names cannot be empty")
        if "$" in name and not (name.startswith("oplog.$main") or
                                name.startswith("$cmd")):
            raise InvalidName("collection names must not "
                              "contain '$': %r" % name)
        if name[0] == "." or name[-1] == ".":
            raise InvalidName("collection names must not start "
                              "or end with '.': %r" % name)
        if "\x00" in name:
            raise InvalidName("collection names must not contain the "
                              "null character")

        self.__database = database
        self.__name = unicode(name)
        self.__uuid_subtype = OLD_UUID_SUBTYPE
        self.__full_name = u"%s.%s" % (self.__database.name, self.__name)
        if create or kwargs:
            self.__create(kwargs)

    def __create(self, options):
        """Sends a create command with the given options.
        """

        # Send size as a float, not an int/long. BSON can only handle 32-bit
        # ints which conflicts w/ max collection size of 10000000000.
        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            self.__database.command("create", self.__name, **options)
        else:
            self.__database.command("create", self.__name)

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

    def __eq__(self, other):
        if isinstance(other, Collection):
            us = (self.__database, self.__name)
            them = (other.__database, other.__name)
            return us == them
        return NotImplemented

    @property
    def full_name(self):
        """The full name of this :class:`Collection`.

        The full name is of the form `database_name.collection_name`.

        .. versionchanged:: 1.3
           ``full_name`` is now a property rather than a method.
        """
        return self.__full_name

    @property
    def name(self):
        """The name of this :class:`Collection`.

        .. versionchanged:: 1.3
           ``name`` is now a property rather than a method.
        """
        return self.__name

    @property
    def database(self):
        """The :class:`~pymongo.database.Database` that this
        :class:`Collection` is a part of.

        .. versionchanged:: 1.3
           ``database`` is now a property rather than a method.
        """
        return self.__database

    def __get_uuid_subtype(self):
        return self.__uuid_subtype

    def __set_uuid_subtype(self, subtype):
        if subtype not in ALL_UUID_SUBTYPES:
            raise ConfigurationError("Not a valid setting for uuid_subtype.")
        self.__uuid_subtype = subtype

    uuid_subtype = property(__get_uuid_subtype, __set_uuid_subtype,
                            doc="""This attribute specifies which BSON Binary
                            subtype is used when storing UUIDs. Historically
                            UUIDs have been stored as BSON Binary subtype 3.
                            This attribute is used to switch to the newer BSON
                            binary subtype 4. It can also be used to force
                            legacy byte order and subtype compatibility with
                            the Java and C# drivers. See the
                            :mod:`bson.binary` module for all options.""")

    def save(self, to_save, manipulate=True,
             safe=None, check_keys=True, **kwargs):
        """Save a document in this collection.

        If `to_save` already has an ``"_id"`` then an :meth:`update`
        (upsert) operation is performed and any existing document with
        that ``"_id"`` is overwritten. Otherwise an :meth:`insert`
        operation is performed. In this case if `manipulate` is ``True``
        an ``"_id"`` will be added to `to_save` and this method returns
        the ``"_id"`` of the saved document. If `manipulate` is ``False``
        the ``"_id"`` will be added by the server but this method will
        return ``None``.

        Raises :class:`TypeError` if `to_save` is not an instance of
        :class:`dict`. If `safe` is ``True`` then the save will be
        checked for errors, raising
        :class:`~pymongo.errors.OperationFailure` if one
        occurred. Safe inserts wait for a response from the database,
        while normal inserts do not.

        Any additional keyword arguments imply ``safe=True``, and will
        be used as options for the resultant `getLastError`
        command. For example, to wait for replication to 3 nodes, pass
        ``w=3``.

        :Parameters:
          - `to_save`: the document to be saved
          - `manipulate` (optional): manipulate the document before
            saving it?
          - `safe` (optional): check that the save succeeded?
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising :class:`~pymongo.errors.InvalidName`
            in either case.
          - `**kwargs` (optional): any additional arguments imply
            ``safe=True``, and will be used as options for the
            `getLastError` command

        .. versionadded:: 1.8
           Support for passing `getLastError` options as keyword
           arguments.

        .. mongodoc:: insert
        """
        if not isinstance(to_save, dict):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if "_id" not in to_save:
            return self.insert(to_save, manipulate, safe, check_keys, **kwargs)
        else:
            self.update({"_id": to_save["_id"]}, to_save, True,
                        manipulate, safe, _check_keys=check_keys, **kwargs)
            return to_save.get("_id", None)

    def insert(self, doc_or_docs, manipulate=True,
               safe=None, check_keys=True, continue_on_error=False, **kwargs):
        """Insert a document(s) into this collection.

        If `manipulate` is ``True``, the document(s) are manipulated using
        any :class:`~pymongo.son_manipulator.SONManipulator` instances
        that have been added to this :class:`~pymongo.database.Database`.
        In this case an ``"_id"`` will be added if the document(s) does
        not already contain one and the ``"id"`` (or list of ``"_id"``
        values for more than one document) will be returned.
        If `manipulate` is ``False`` and the document(s) does not include
        an ``"_id"`` one will be added by the server. The server
        does not return the ``"_id"`` it created so ``None`` is returned.

        If `safe` is ``True`` then the insert will be checked for
        errors, raising :class:`~pymongo.errors.OperationFailure` if
        one occurred. Safe inserts wait for a response from the
        database, while normal inserts do not.

        Any additional keyword arguments imply ``safe=True``, and
        will be used as options for the resultant `getLastError`
        command. For example, to wait for replication to 3 nodes, pass
        ``w=3``.

        :Parameters:
          - `doc_or_docs`: a document or list of documents to be
            inserted
          - `manipulate` (optional): manipulate the documents before
            inserting?
          - `safe` (optional): check that the insert succeeded?
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising :class:`~pymongo.errors.InvalidName`
            in either case
          - `continue_on_error` (optional): If True, the database will not stop
            processing a bulk insert if one fails (e.g. due to duplicate IDs).
            This makes bulk insert behave similarly to a series of single
            inserts, except lastError will be set if any insert fails, not just
            the last one. If multiple errors occur, only the most recent will
            be reported by :meth:`~pymongo.database.Database.error`.
          - `**kwargs` (optional): any additional arguments imply
            ``safe=True``, and will be used as options for the
            `getLastError` command

        .. note:: `continue_on_error` requires server version **>= 1.9.1**

        .. versionadded:: 2.1
           Support for continue_on_error.
        .. versionadded:: 1.8
           Support for passing `getLastError` options as keyword
           arguments.
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

        safe, options = self._get_safe_and_lasterror_options(safe, **kwargs)
        self.__database.connection._send_message(
            message.insert(self.__full_name, docs,
                           check_keys, safe, options,
                           continue_on_error, self.__uuid_subtype), safe)

        ids = [doc.get("_id", None) for doc in docs]
        return return_one and ids[0] or ids

    def update(self, spec, document, upsert=False, manipulate=False,
               safe=None, multi=False, _check_keys=False, **kwargs):
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

        Any additional keyword arguments imply ``safe=True``, and will
        be used as options for the resultant `getLastError`
        command. For example, to wait for replication to 3 nodes, pass
        ``w=3``.

        :Parameters:
          - `spec`: a ``dict`` or :class:`~bson.son.SON` instance
            specifying elements which must be present for a document
            to be updated
          - `document`: a ``dict`` or :class:`~bson.son.SON`
            instance specifying the document to be used for the update
            or (in the case of an upsert) insert - see docs on MongoDB
            `update modifiers`_
          - `upsert` (optional): perform an upsert if ``True``
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
          - `**kwargs` (optional): any additional arguments imply
            ``safe=True``, and will be used as options for the
            `getLastError` command

        .. versionadded:: 1.8
           Support for passing `getLastError` options as keyword
           arguments.
        .. versionchanged:: 1.4
           Return the response to *lastError* if `safe` is ``True``.
        .. versionadded:: 1.1.1
           The `multi` parameter.

        .. _update modifiers: http://www.mongodb.org/display/DOCS/Updating

        .. mongodoc:: update
        """
        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(document, dict):
            raise TypeError("document must be an instance of dict")
        if not isinstance(upsert, bool):
            raise TypeError("upsert must be an instance of bool")

        if manipulate:
            document = self.__database._fix_incoming(document, self)

        safe, options = self._get_safe_and_lasterror_options(safe, **kwargs)

        # _check_keys is used by save() so we don't upsert pre-existing
        # documents after adding an invalid key like 'a.b'. It can't really
        # be used for any other update operations.
        return self.__database.connection._send_message(
            message.update(self.__full_name, upsert, multi,
                           spec, document, safe, options,
                           _check_keys, self.__uuid_subtype), safe)

    def drop(self):
        """Alias for :meth:`~pymongo.database.Database.drop_collection`.

        The following two calls are equivalent:

          >>> db.foo.drop()
          >>> db.drop_collection("foo")

        .. versionadded:: 1.8
        """
        self.__database.drop_collection(self.__name)

    def remove(self, spec_or_id=None, safe=None, **kwargs):
        """Remove a document(s) from this collection.

        .. warning:: Calls to :meth:`remove` should be performed with
           care, as removed data cannot be restored.

        If `safe` is ``True`` then the remove operation will be
        checked for errors, raising
        :class:`~pymongo.errors.OperationFailure` if one
        occurred. Safe removes wait for a response from the database,
        while normal removes do not.

        If `spec_or_id` is ``None``, all documents in this collection
        will be removed. This is not equivalent to calling
        :meth:`~pymongo.database.Database.drop_collection`, however,
        as indexes will not be removed.

        If `safe` is ``True`` returns the response to the *lastError*
        command. Otherwise, returns ``None``.

        Any additional keyword arguments imply ``safe=True``, and will
        be used as options for the resultant `getLastError`
        command. For example, to wait for replication to 3 nodes, pass
        ``w=3``.

        :Parameters:
          - `spec_or_id` (optional): a dictionary specifying the
            documents to be removed OR any other type specifying the
            value of ``"_id"`` for the document to be removed
          - `safe` (optional): check that the remove succeeded?
          - `**kwargs` (optional): any additional arguments imply
            ``safe=True``, and will be used as options for the
            `getLastError` command

        .. versionadded:: 1.8
           Support for passing `getLastError` options as keyword arguments.
        .. versionchanged:: 1.7 Accept any type other than a ``dict``
           instance for removal by ``"_id"``, not just
           :class:`~bson.objectid.ObjectId` instances.
        .. versionchanged:: 1.4
           Return the response to *lastError* if `safe` is ``True``.
        .. versionchanged:: 1.2
           The `spec_or_id` parameter is now optional. If it is
           not specified *all* documents in the collection will be
           removed.
        .. versionadded:: 1.1
           The `safe` parameter.

        .. mongodoc:: remove
        """
        if spec_or_id is None:
            spec_or_id = {}
        if not isinstance(spec_or_id, dict):
            spec_or_id = {"_id": spec_or_id}

        safe, options = self._get_safe_and_lasterror_options(safe, **kwargs)
        return self.__database.connection._send_message(
            message.delete(self.__full_name, spec_or_id, safe,
                           options, self.__uuid_subtype), safe)

    def find_one(self, spec_or_id=None, *args, **kwargs):
        """Get a single document from the database.

        All arguments to :meth:`find` are also valid arguments for
        :meth:`find_one`, although any `limit` argument will be
        ignored. Returns a single document, or ``None`` if no matching
        document is found.

        :Parameters:

          - `spec_or_id` (optional): a dictionary specifying
            the query to be performed OR any other type to be used as
            the value for a query for ``"_id"``.

          - `*args` (optional): any additional positional arguments
            are the same as the arguments to :meth:`find`.

          - `**kwargs` (optional): any additional keyword arguments
            are the same as the arguments to :meth:`find`.

        .. versionchanged:: 1.7
           Allow passing any of the arguments that are valid for
           :meth:`find`.

        .. versionchanged:: 1.7 Accept any type other than a ``dict``
           instance as an ``"_id"`` query, not just
           :class:`~bson.objectid.ObjectId` instances.
        """
        if spec_or_id is not None and not isinstance(spec_or_id, dict):
            spec_or_id = {"_id": spec_or_id}

        for result in self.find(spec_or_id, *args, **kwargs).limit(-1):
            return result
        return None

    def find(self, *args, **kwargs):
        """Query the database.

        The `spec` argument is a prototype document that all results
        must match. For example:

        >>> db.test.find({"hello": "world"})

        only matches documents that have a key "hello" with value
        "world".  Matches can have other keys *in addition* to
        "hello". The `fields` argument is used to specify a subset of
        fields that should be included in the result documents. By
        limiting results to a certain subset of fields you can cut
        down on network traffic and decoding time.

        Raises :class:`TypeError` if any of the arguments are of
        improper type. Returns an instance of
        :class:`~pymongo.cursor.Cursor` corresponding to this query.

        :Parameters:
          - `spec` (optional): a SON object specifying elements which
            must be present for a document to be included in the
            result set
          - `fields` (optional): a list of field names that should be
            returned in the result set ("_id" will always be
            included), or a dict specifying the fields to return
          - `skip` (optional): the number of documents to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return
          - `timeout` (optional): if True, any returned cursor will be
            subject to the normal timeout behavior of the mongod
            process. Otherwise, the returned cursor will never timeout
            at the server. Care should be taken to ensure that cursors
            with timeout turned off are properly closed.
          - `snapshot` (optional): if True, snapshot mode will be used
            for this query. Snapshot mode assures no duplicates are
            returned, or objects missed, which were present at both
            the start and end of the query's execution. For details,
            see the `snapshot documentation
            <http://dochub.mongodb.org/core/snapshot>`_.
          - `tailable` (optional): the result of this find call will
            be a tailable cursor - tailable cursors aren't closed when
            the last data is retrieved but are kept open and the
            cursors location marks the final document's position. if
            more data is received iteration of the cursor will
            continue from the last document received. For details, see
            the `tailable cursor documentation
            <http://www.mongodb.org/display/DOCS/Tailable+Cursors>`_.
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~pymongo.cursor.Cursor.sort` for details.
          - `max_scan` (optional): limit the number of documents
            examined when performing the query
          - `as_class` (optional): class to use for documents in the
            query result (default is
            :attr:`~pymongo.connection.Connection.document_class`)
          - `slave_okay` (optional): if True, allows this query to
            be run against a replica secondary.
          - `await_data` (optional): if True, the server will block for
            some extra time before returning, waiting for more data to
            return. Ignored if `tailable` is False.
          - `partial` (optional): if True, mongos will return partial
            results if some shards are down instead of returning an error.
          - `manipulate`: (optional): If True (the default), apply any
            outgoing SON manipulators before returning.
          - `network_timeout` (optional): specify a timeout to use for
            this query, which will override the
            :class:`~pymongo.connection.Connection`-level default
          - `read_preference` (optional): The read preference for
            this query.
          - `tag_sets` (optional): The tag sets for this query.
          - `secondary_acceptable_latency_ms` (optional): Any replica-set
            member whose ping time is within secondary_acceptable_latency_ms of
            the nearest member may accept reads. Default 15 milliseconds.

        .. note:: The `manipulate` parameter may default to False in
           a future release.

        .. note:: The `max_scan` parameter requires server
           version **>= 1.5.1**

        .. versionadded:: 2.3
           The `tag_sets` and `secondary_acceptable_latency_ms` parameters.

        .. versionadded:: 1.11+
           The `await_data`, `partial`, and `manipulate` parameters.

        .. versionadded:: 1.8
           The `network_timeout` parameter.

        .. versionadded:: 1.7
           The `sort`, `max_scan` and `as_class` parameters.

        .. versionchanged:: 1.7
           The `fields` parameter can now be a dict or any iterable in
           addition to a list.

        .. versionadded:: 1.1
           The `tailable` parameter.

        .. mongodoc:: find
        """
        if not 'slave_okay' in kwargs:
            kwargs['slave_okay'] = self.slave_okay
        if not 'read_preference' in kwargs:
            kwargs['read_preference'] = self.read_preference
        if not 'tag_sets' in kwargs:
            kwargs['tag_sets'] = self.tag_sets
        if not 'secondary_acceptable_latency_ms' in kwargs:
            kwargs['secondary_acceptable_latency_ms'] = (
                self.secondary_acceptable_latency_ms)
        return Cursor(self, *args, **kwargs)

    def count(self):
        """Get the number of documents in this collection.

        To get the number of documents matching a specific query use
        :meth:`pymongo.cursor.Cursor.count`.
        """
        return self.find().count()

    def create_index(self, key_or_list, cache_for=300, **kwargs):
        """Creates an index on this collection.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`
        (:class:`str` in python 3), and the directions must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`). Returns the name of the created index.

        To create a single key index on the key ``'mike'`` we just use
        a string argument:

        >>> my_collection.create_index("mike")

        For a compound index on ``'mike'`` descending and ``'eliot'``
        ascending we need to use a list of tuples:

        >>> my_collection.create_index([("mike", pymongo.DESCENDING),
        ...                             ("eliot", pymongo.ASCENDING)])

        All optional index creation paramaters should be passed as
        keyword arguments to this method. Valid options include:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated
          - `unique`: should this index guarantee uniqueness?
          - `dropDups` or `drop_dups`: should we drop duplicates
          - `bucketSize` or `bucket_size`: size of buckets for geoHaystack indexes
            during index creation when creating a unique index?
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction)
            pairs specifying the index to create
          - `cache_for` (optional): time window (in seconds) during which
            this index will be recognized by subsequent calls to
            :meth:`ensure_index` - see documentation for
            :meth:`ensure_index` for details
          - `**kwargs` (optional): any additional index creation
            options (see the above list) should be passed as keyword
            arguments
          - `ttl` (deprecated): Use `cache_for` instead.

        .. versionchanged:: 2.3
            The `ttl` parameter has been deprecated to avoid confusion with
            TTL collections.  Use `cache_for` instead.

        .. versionchanged:: 2.2
           Removed deprecated argument: deprecated_unique

        .. versionchanged:: 1.5.1
           Accept kwargs to support all index creation options.

        .. versionadded:: 1.5
           The `name` parameter.

        .. seealso:: :meth:`ensure_index`

        .. mongodoc:: indexes
        """

        if 'ttl' in kwargs:
            cache_for = kwargs.pop('ttl')
            warnings.warn("ttl is deprecated. Please use cache_for instead.",
                          DeprecationWarning)

        keys = helpers._index_list(key_or_list)
        index_doc = helpers._index_document(keys)

        index = {"key": index_doc, "ns": self.__full_name}

        name = "name" in kwargs and kwargs["name"] or _gen_index_name(keys)
        index["name"] = name

        if "drop_dups" in kwargs:
            kwargs["dropDups"] = kwargs.pop("drop_dups")

        if "bucket_size" in kwargs:
            kwargs["bucketSize"] = kwargs.pop("bucket_size")

        index.update(kwargs)

        self.__database.system.indexes.insert(index, manipulate=False,
                                              check_keys=False,
                                              safe=True)

        self.__database.connection._cache_index(self.__database.name,
                                                self.__name, name, cache_for)

        return name

    def ensure_index(self, key_or_list, cache_for=300, **kwargs):
        """Ensures that an index exists on this collection.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`
        (:class:`str` in python 3), and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`). See :meth:`create_index` for a detailed
        example.

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
          - `background`: if this index should be created in the
            background
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction)
            pairs specifying the index to create
          - `cache_for` (optional): time window (in seconds) during which
            this index will be recognized by subsequent calls to
            :meth:`ensure_index`
          - `**kwargs` (optional): any additional index creation
            options (see the above list) should be passed as keyword
            arguments
          - `ttl` (deprecated): Use `cache_for` instead.

        .. versionchanged:: 2.3
            The `ttl` parameter has been deprecated to avoid confusion with
            TTL collections.  Use `cache_for` instead.

        .. versionchanged:: 2.2
           Removed deprecated argument: deprecated_unique

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

        if not self.__database.connection._cached(self.__database.name,
                                                  self.__name, name):
            return self.create_index(key_or_list, cache_for, **kwargs)
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

        .. warning::

          if a custom name was used on index creation (by
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
        self.__database.command("dropIndexes", self.__name, index=name,
                                allowable_errors=["ns not found"])

    def reindex(self):
        """Rebuilds all indexes on this collection.

        .. warning:: reindex blocks all other operations (indexes
           are built in the foreground) and will be slow for large
           collections.

        .. versionadded:: 1.11+
        """
        return self.__database.command("reIndex", self.__name)

    def index_information(self):
        """Get information on this collection's indexes.

        Returns a dictionary where the keys are index names (as
        returned by create_index()) and the values are dictionaries
        containing information about each index. The dictionary is
        guaranteed to contain at least a single key, ``"key"`` which
        is a list of (key, direction) pairs specifying the index (as
        passed to create_index()). It will also contain any other
        information in `system.indexes`, except for the ``"ns"`` and
        ``"name"`` keys, which are cleaned. Example output might look
        like this:

        >>> db.test.ensure_index("x", unique=True)
        u'x_1'
        >>> db.test.index_information()
        {u'_id_': {u'key': [(u'_id', 1)]},
         u'x_1': {u'unique': True, u'key': [(u'x', 1)]}}


        .. versionchanged:: 1.7
           The values in the resultant dictionary are now dictionaries
           themselves, whose ``"key"`` item contains the list that was
           the value in previous versions of PyMongo.
        """
        raw = self.__database.system.indexes.find({"ns": self.__full_name},
                                                  {"ns": 0}, as_class=SON)
        info = {}
        for index in raw:
            index["key"] = index["key"].items()
            index = dict(index)
            info[index.pop("name")] = index
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

    def aggregate(self, pipeline):
        """Perform an aggregation using the aggregation framework on this
        collection.

        With :class:`~pymongo.replica_set_connection.ReplicaSetConnection`
        or :class:`~pymongo.master_slave_connection.MasterSlaveConnection`,
        if the `read_preference` attribute of this instance is not set to
        :attr:`pymongo.ReadPreference.PRIMARY` or the (deprecated)
        `slave_okay` attribute of this instance is set to `True` the
        `aggregate command`_. will be sent to a secondary or slave.

        :Parameters:
          - `pipeline`: a single command or list of aggregation commands

        .. note:: Requires server version **>= 2.1.0**

        .. versionadded:: 2.3

        .. _aggregate command:
            http://docs.mongodb.org/manual/applications/aggregation
        """
        if not isinstance(pipeline, (dict, list, tuple)):
            raise TypeError("pipeline must be a dict, list or tuple")

        if isinstance(pipeline, dict):
            pipeline = [pipeline]

        use_master = not self.slave_okay and not self.read_preference

        return self.__database.command("aggregate", self.__name,
                                        pipeline=pipeline,
                                        read_preference=self.read_preference,
                                        tag_sets=self.tag_sets,
                                        secondary_acceptable_latency_ms=(
                                         self.secondary_acceptable_latency_ms),
                                        slave_okay=self.slave_okay,
                                        _use_master=use_master)

    # TODO key and condition ought to be optional, but deprecation
    # could be painful as argument order would have to change.
    def group(self, key, condition, initial, reduce, finalize=None):
        """Perform a query similar to an SQL *group by* operation.

        Returns an array of grouped items.

        The `key` parameter can be:

          - ``None`` to use the entire document as a key.
          - A :class:`list` of keys (each a :class:`basestring`
            (:class:`str` in python 3)) to group by.
          - A :class:`basestring` (:class:`str` in python 3), or
            :class:`~bson.code.Code` instance containing a JavaScript
            function to be applied to each document, returning the key
            to group by.

        With :class:`~pymongo.replica_set_connection.ReplicaSetConnection`
        or :class:`~pymongo.master_slave_connection.MasterSlaveConnection`,
        if the `read_preference` attribute of this instance is not set to
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY` or
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY_PREFERRED`, or
        the (deprecated) `slave_okay` attribute of this instance is set to
        `True`, the group command will be sent to a secondary or slave.

        :Parameters:
          - `key`: fields to group by (see above description)
          - `condition`: specification of rows to be
            considered (as a :meth:`find` query specification)
          - `initial`: initial value of the aggregation counter object
          - `reduce`: aggregation function as a JavaScript string
          - `finalize`: function to be called on each object in output list.

        .. versionchanged:: 2.2
           Removed deprecated argument: command

        .. versionchanged:: 1.4
           The `key` argument can now be ``None`` or a JavaScript function,
           in addition to a :class:`list` of keys.

        .. versionchanged:: 1.3
           The `command` argument now defaults to ``True`` and is deprecated.
        """

        group = {}
        if isinstance(key, basestring):
            group["$keyf"] = Code(key)
        elif key is not None:
            group = {"key": helpers._fields_list_to_dict(key)}
        group["ns"] = self.__name
        group["$reduce"] = Code(reduce)
        group["cond"] = condition
        group["initial"] = initial
        if finalize is not None:
            group["finalize"] = Code(finalize)

        use_master = not self.slave_okay and not self.read_preference

        return self.__database.command("group", group,
                                       uuid_subtype=self.__uuid_subtype,
                                       read_preference=self.read_preference,
                                       tag_sets=self.tag_sets,
                                       secondary_acceptable_latency_ms=(
                                           self.secondary_acceptable_latency_ms),
                                       slave_okay=self.slave_okay,
                                       _use_master=use_master)["retval"]

    def rename(self, new_name, **kwargs):
        """Rename this collection.

        If operating in auth mode, client must be authorized as an
        admin to perform this operation. Raises :class:`TypeError` if
        `new_name` is not an instance of :class:`basestring`
        (:class:`str` in python 3). Raises :class:`~pymongo.errors.InvalidName`
        if `new_name` is not a valid collection name.

        :Parameters:
          - `new_name`: new name for this collection
          - `**kwargs` (optional): any additional rename options
            should be passed as keyword arguments
            (i.e. ``dropTarget=True``)

        .. versionadded:: 1.7
           support for accepting keyword arguments for rename options
        """
        if not isinstance(new_name, basestring):
            raise TypeError("new_name must be an instance "
                            "of %s" % (basestring.__name__,))

        if not new_name or ".." in new_name:
            raise InvalidName("collection names cannot be empty")
        if new_name[0] == "." or new_name[-1] == ".":
            raise InvalidName("collecion names must not start or end with '.'")
        if "$" in new_name and not new_name.startswith("oplog.$main"):
            raise InvalidName("collection names must not contain '$'")

        new_name = "%s.%s" % (self.__database.name, new_name)
        self.__database.connection.admin.command("renameCollection",
                                                 self.__full_name,
                                                 to=new_name, **kwargs)

    def distinct(self, key):
        """Get a list of distinct values for `key` among all documents
        in this collection.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring` (:class:`str` in python 3).

        To get the distinct values for a key in the result set of a
        query use :meth:`~pymongo.cursor.Cursor.distinct`.

        :Parameters:
          - `key`: name of key for which we want to get the distinct values

        .. note:: Requires server version **>= 1.1.0**

        .. versionadded:: 1.1.1
        """
        return self.find().distinct(key)

    def map_reduce(self, map, reduce, out, full_response=False, **kwargs):
        """Perform a map/reduce operation on this collection.

        If `full_response` is ``False`` (default) returns a
        :class:`~pymongo.collection.Collection` instance containing
        the results of the operation. Otherwise, returns the full
        response from the server to the `map reduce command`_.

        :Parameters:
          - `map`: map function (as a JavaScript string)
          - `reduce`: reduce function (as a JavaScript string)
          - `out`: output collection name or `out object` (dict). See
            the `map reduce command`_ documentation for available options.
            Note: `out` options are order sensitive. :class:`~bson.son.SON`
            can be used to specify multiple options.
            e.g. SON([('replace', <collection name>), ('db', <database name>)])
          - `full_response` (optional): if ``True``, return full response to
            this command - otherwise just return the result collection
          - `**kwargs` (optional): additional arguments to the
            `map reduce command`_ may be passed as keyword arguments to this
            helper method, e.g.::

            >>> db.test.map_reduce(map, reduce, "myresults", limit=2)

        .. note:: Requires server version **>= 1.1.1**

        .. seealso:: :doc:`/examples/aggregation`

        .. versionchanged:: 2.2
           Removed deprecated arguments: merge_output and reduce_output

        .. versionchanged:: 1.11+
           DEPRECATED The merge_output and reduce_output parameters.

        .. versionadded:: 1.2

        .. _map reduce command: http://www.mongodb.org/display/DOCS/MapReduce

        .. mongodoc:: mapreduce
        """
        if not isinstance(out, (basestring, dict)):
            raise TypeError("'out' must be an instance of "
                            "%s or dict" % (basestring.__name__,))

        if isinstance(out, dict) and out.get('inline'):
            must_use_master = False
        else:
            must_use_master = True

        response = self.__database.command("mapreduce", self.__name,
                                           uuid_subtype=self.__uuid_subtype,
                                           map=map, reduce=reduce,
                                           read_preference=self.read_preference,
                                           tag_sets=self.tag_sets,
                                           secondary_acceptable_latency_ms=(
                                               self.secondary_acceptable_latency_ms),
                                           out=out, _use_master=must_use_master,
                                           **kwargs)

        if full_response or not response.get('result'):
            return response
        elif isinstance(response['result'], dict):
            dbase = response['result']['db']
            coll = response['result']['collection']
            return self.__database.connection[dbase][coll]
        else:
            return self.__database[response["result"]]

    def inline_map_reduce(self, map, reduce, full_response=False, **kwargs):
        """Perform an inline map/reduce operation on this collection.

        Perform the map/reduce operation on the server in RAM. A result
        collection is not created. The result set is returned as a list
        of documents.

        If `full_response` is ``False`` (default) returns the
        result documents in a list. Otherwise, returns the full
        response from the server to the `map reduce command`_.

        With :class:`~pymongo.replica_set_connection.ReplicaSetConnection`
        or :class:`~pymongo.master_slave_connection.MasterSlaveConnection`,
        if the `read_preference` attribute of this instance is not set to
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY` or
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY_PREFERRED`, or
        the (deprecated) `slave_okay` attribute of this instance is set to
        `True`, the inline map reduce will be run on a secondary or slave.

        :Parameters:
          - `map`: map function (as a JavaScript string)
          - `reduce`: reduce function (as a JavaScript string)
          - `full_response` (optional): if ``True``, return full response to
            this command - otherwise just return the result collection
          - `**kwargs` (optional): additional arguments to the
            `map reduce command`_ may be passed as keyword arguments to this
            helper method, e.g.::

            >>> db.test.inline_map_reduce(map, reduce, limit=2)

        .. note:: Requires server version **>= 1.7.4**

        .. versionadded:: 1.10
        """

        use_master = not self.slave_okay and not self.read_preference

        res = self.__database.command("mapreduce", self.__name,
                                      uuid_subtype=self.__uuid_subtype,
                                      read_preference=self.read_preference,
                                      tag_sets=self.tag_sets,
                                      secondary_acceptable_latency_ms=(
                                          self.secondary_acceptable_latency_ms),
                                      slave_okay=self.slave_okay,
                                      _use_master=use_master,
                                      map=map, reduce=reduce,
                                      out={"inline": 1}, **kwargs)

        if full_response:
            return res
        else:
            return res.get("results")

    def find_and_modify(self, query={}, update=None, upsert=False, **kwargs):
        """Update and return an object.

        This is a thin wrapper around the findAndModify_ command. The
        positional arguments are designed to match the first three arguments
        to :meth:`update` however most options should be passed as named
        parameters. Either `update` or `remove` arguments are required, all
        others are optional.

        Returns either the object before or after modification based on `new`
        parameter. If no objects match the `query` and `upsert` is false,
        returns ``None``. If upserting and `new` is false, returns ``{}``.

        :Parameters:
            - `query`: filter for the update (default ``{}``)
            - `sort`: priority if multiple objects match (default ``{}``)
            - `update`: see second argument to :meth:`update` (no default)
            - `remove`: remove rather than updating (default ``False``)
            - `new`: return updated rather than original object
              (default ``False``)
            - `fields`: see second argument to :meth:`find` (default all)
            - `upsert`: insert if object doesn't exist (default ``False``)
            - `**kwargs`: any other options the findAndModify_ command
              supports can be passed here.


        .. mongodoc:: findAndModify

        .. _findAndModify: http://dochub.mongodb.org/core/findAndModify

        .. note:: Requires server version **>= 1.3.0**

        .. versionadded:: 1.10
        """
        if (not update and not kwargs.get('remove', None)):
            raise ValueError("Must either update or remove")

        if (update and kwargs.get('remove', None)):
            raise ValueError("Can't do both update and remove")

        # No need to include empty args
        if query:
            kwargs['query'] = query
        if update:
            kwargs['update'] = update
        if upsert:
            kwargs['upsert'] = upsert

        no_obj_error = "No matching object found"

        out = self.__database.command("findAndModify", self.__name,
                                      allowable_errors=[no_obj_error],
                                      uuid_subtype=self.__uuid_subtype,
                                      **kwargs)

        if not out['ok']:
            if out["errmsg"] == no_obj_error:
                return None
            else:
                # Should never get here b/c of allowable_errors
                raise ValueError("Unexpected Error: %s" % (out,))

        return out.get('value')

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
