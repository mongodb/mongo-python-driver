# Copyright 2009-2015 MongoDB, Inc.
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

from bson.code import Code
from bson.objectid import ObjectId
from bson.son import SON
from pymongo import (bulk,
                     common,
                     helpers,
                     message,
                     results)
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.errors import InvalidName, OperationFailure
from pymongo.helpers import _check_write_command_response
from pymongo.message import _INSERT, _UPDATE, _DELETE
from pymongo.operations import _WriteOp
from pymongo.read_preferences import ReadPreference


try:
    from collections import OrderedDict
    ordered_types = (SON, OrderedDict)
except ImportError:
    ordered_types = SON


def _gen_index_name(keys):
    """Generate an index name from the set of fields it is over.
    """
    return u"_".join([u"%s_%s" % item for item in keys])


class ReturnDocument(object):
    """An enum used with
    :meth:`~pymongo.collection.Collection.find_one_and_replace` and
    :meth:`~pymongo.collection.Collection.find_one_and_update`.
    """
    BEFORE = False
    """Return the original document before it was updated/replaced, or
    ``None`` if no document matches the query.
    """
    AFTER = True
    """Return the updated/replaced or inserted document."""


class Collection(common.BaseObject):
    """A Mongo collection.
    """

    def __init__(self, database, name, create=False, codec_options=None,
                 read_preference=None, write_concern=None, **kwargs):
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
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) database.codec_options is used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) database.read_preference is used.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) database.write_concern is used.
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 2.9
           Added the codec_options, read_preference, and write_concern options.

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
        opts, mode, tags, wc_doc = helpers._get_common_options(
            database, codec_options, read_preference, write_concern)
        salms = database.secondary_acceptable_latency_ms

        super(Collection, self).__init__(
            codec_options=opts,
            read_preference=mode,
            tag_sets=tags,
            secondary_acceptable_latency_ms=salms,
            slave_okay=database.slave_okay,
            safe=database.safe,
            **wc_doc)

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
        self.__full_name = u"%s.%s" % (self.__database.name, self.__name)
        if create or kwargs:
            self.__create(kwargs)

    def __create(self, options):
        """Sends a create command with the given options.
        """

        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            self.__database.command("create", self.__name,
                                    read_preference=ReadPreference.PRIMARY,
                                    **options)
        else:
            self.__database.command("create", self.__name,
                                    read_preference=ReadPreference.PRIMARY)

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

    def __ne__(self, other):
        return not self == other

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

    def with_options(
            self, codec_options=None, read_preference=None, write_concern=None):
        """Get a clone of this collection changing the specified settings.

          >>> from pymongo import ReadPreference
          >>> coll1.read_preference == ReadPreference.PRIMARY
          True
          >>> coll2 = coll1.with_options(read_preference=ReadPreference.SECONDARY)
          >>> coll1.read_preference == ReadPreference.PRIMARY
          True
          >>> coll2.read_preference == ReadPreference.SECONDARY
          True

        :Parameters:
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Collection`
            is used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Collection` is used. See :mod:`~pymongo.read_preferences`
            for options.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Collection`
            is used.

        .. versionadded:: 2.9
        """
        opts, mode, tags, wc_doc = helpers._get_common_options(
            self, codec_options, read_preference, write_concern)
        coll = Collection(self.__database, self.__name, False, opts)
        coll._write_concern = wc_doc
        coll._read_pref = mode
        coll._tag_sets = tags
        return coll

    def initialize_unordered_bulk_op(self):
        """Initialize an unordered batch of write operations.

        Operations will be performed on the server in arbitrary order,
        possibly in parallel. All operations will be attempted.

        Returns a :class:`~pymongo.bulk.BulkOperationBuilder` instance.

        See :ref:`unordered_bulk` for examples.

        .. versionadded:: 2.7
        """
        return bulk.BulkOperationBuilder(self, ordered=False)

    def initialize_ordered_bulk_op(self):
        """Initialize an ordered batch of write operations.

        Operations will be performed on the server serially, in the
        order provided. If an error occurs all remaining operations
        are aborted.

        Returns a :class:`~pymongo.bulk.BulkOperationBuilder` instance.

        See :ref:`ordered_bulk` for examples.

        .. versionadded:: 2.7
        """
        return bulk.BulkOperationBuilder(self, ordered=True)

    def bulk_write(self, requests, ordered=True):
        """Send a batch of write operations to the server.

        Requests are passed as a list of write operation instances (
        :class:`~pymongo.operations.InsertOne`,
        :class:`~pymongo.operations.UpdateOne`,
        :class:`~pymongo.operations.UpdateMany`,
        :class:`~pymongo.operations.ReplaceOne`,
        :class:`~pymongo.operations.DeleteOne`, or
        :class:`~pymongo.operations.DeleteMany`).

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'x': 1, u'_id': ObjectId('54f62e60fba5226811f634ef')}
          {u'x': 1, u'_id': ObjectId('54f62e60fba5226811f634f0')}
          >>> # DeleteMany, UpdateOne, and UpdateMany are also available.
          ...
          >>> from pymongo import InsertOne, DeleteOne, ReplaceOne
          >>> requests = [InsertOne({'y': 1}), DeleteOne({'x': 1}),
          ...             ReplaceOne({'w': 1}, {'z': 1}, upsert=True)]
          >>> result = db.test.bulk_write(requests)
          >>> result.inserted_count
          1
          >>> result.deleted_count
          1
          >>> result.modified_count
          0
          >>> result.upserted_ids
          {2: ObjectId('54f62ee28891e756a6e1abd5')}
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'x': 1, u'_id': ObjectId('54f62e60fba5226811f634f0')}
          {u'y': 1, u'_id': ObjectId('54f62ee2fba5226811f634f1')}
          {u'z': 1, u'_id': ObjectId('54f62ee28891e756a6e1abd5')}

        :Parameters:
          - `requests`: A list of write operations (see examples above).
          - `ordered` (optional): If ``True`` (the default) requests will be
            performed on the server serially, in the order provided. If an error
            occurs all remaining operations are aborted. If ``False`` requests
            will be performed on the server in arbitrary order, possibly in
            parallel, and all operations will be attempted.

        :Returns:
          An instance of :class:`~pymongo.results.BulkWriteResult`.

        .. versionadded:: 2.9
        """
        if not isinstance(requests, list):
            raise TypeError("requests must be a list")

        blk = bulk._Bulk(self, ordered)
        for request in requests:
            if not isinstance(request, _WriteOp):
                raise TypeError("%r is not a valid request" % (request,))
            request._add_to_bulk(blk)

        bulk_api_result = blk.execute(self.write_concern)
        if bulk_api_result is not None:
            return results.BulkWriteResult(bulk_api_result, True)
        return results.BulkWriteResult({}, False)

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
        :class:`dict`.

        Write concern options can be passed as keyword arguments, overriding
        any global defaults. Valid options include w=<int/string>,
        wtimeout=<int>, j=<bool>, or fsync=<bool>. See the parameter list below
        for a detailed explanation of these options.

        By default an acknowledgment is requested from the server that the
        save was successful, raising :class:`~pymongo.errors.OperationFailure`
        if an error occurred. **Passing w=0 disables write acknowledgement
        and all other write concern options.**

        :Parameters:
          - `to_save`: the document to be saved
          - `manipulate` (optional): manipulate the document before
            saving it?
          - `safe` (optional): **DEPRECATED** - Use `w` instead.
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising :class:`~pymongo.errors.InvalidName`
            in either case.
          - `w` (optional): (integer or string) If this is a replica set, write
            operations will block until they have been replicated to the
            specified number or tagged set of servers. `w=<int>` always includes
            the replica set primary (e.g. w=3 means write to the primary and wait
            until replicated to **two** secondaries). **Passing w=0 disables
            write acknowledgement and all other write concern options.**
          - `wtimeout` (optional): (integer) Used in conjunction with `w`.
            Specify a value in milliseconds to control how long to wait for
            write propagation to complete. If replication does not complete in
            the given timeframe, a timeout exception is raised.
          - `j` (optional): If ``True`` block until write operations have been
            committed to the journal. Ignored if the server is running without
            journaling.
          - `fsync` (optional): If ``True`` force the database to fsync all
            files before returning. When used with `j` the server awaits the
            next group commit before returning.
        :Returns:
          - The ``'_id'`` value of `to_save` or ``[None]`` if `manipulate` is
            ``False`` and `to_save` has no '_id' field.

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
                        manipulate, safe, check_keys=check_keys, **kwargs)
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

        Write concern options can be passed as keyword arguments, overriding
        any global defaults. Valid options include w=<int/string>,
        wtimeout=<int>, j=<bool>, or fsync=<bool>. See the parameter list below
        for a detailed explanation of these options.

        By default an acknowledgment is requested from the server that the
        insert was successful, raising :class:`~pymongo.errors.OperationFailure`
        if an error occurred. **Passing w=0 disables write acknowledgement
        and all other write concern options.**

        :Parameters:
          - `doc_or_docs`: a document or list of documents to be
            inserted
          - `manipulate` (optional): If ``True`` manipulate the documents
            before inserting.
          - `safe` (optional): **DEPRECATED** - Use `w` instead.
          - `check_keys` (optional): If ``True`` check if keys start with '$'
            or contain '.', raising :class:`~pymongo.errors.InvalidName` in
            either case.
          - `continue_on_error` (optional): If ``True``, the database will not
            stop processing a bulk insert if one fails (e.g. due to duplicate
            IDs). This makes bulk insert behave similarly to a series of single
            inserts, except lastError will be set if any insert fails, not just
            the last one. If multiple errors occur, only the most recent will
            be reported by :meth:`~pymongo.database.Database.error`.
          - `w` (optional): (integer or string) If this is a replica set, write
            operations will block until they have been replicated to the
            specified number or tagged set of servers. `w=<int>` always includes
            the replica set primary (e.g. w=3 means write to the primary and wait
            until replicated to **two** secondaries). **Passing w=0 disables
            write acknowledgement and all other write concern options.**
          - `wtimeout` (optional): (integer) Used in conjunction with `w`.
            Specify a value in milliseconds to control how long to wait for
            write propagation to complete. If replication does not complete in
            the given timeframe, a timeout exception is raised.
          - `j` (optional): If ``True`` block until write operations have been
            committed to the journal. Ignored if the server is running without
            journaling.
          - `fsync` (optional): If ``True`` force the database to fsync all
            files before returning. When used with `j` the server awaits the
            next group commit before returning.
        :Returns:
          - The ``'_id'`` value (or list of '_id' values) of `doc_or_docs` or
            ``[None]`` if manipulate is ``False`` and the documents passed
            as `doc_or_docs` do not include an '_id' field.

        .. note:: `continue_on_error` requires server version **>= 1.9.1**

        .. versionadded:: 2.1
           Support for continue_on_error.
        .. versionadded:: 1.8
           Support for passing `getLastError` options as keyword
           arguments.
        .. versionchanged:: 1.1
           Bulk insert works with an iterable sequence of documents.

        .. mongodoc:: insert
        """
        client = self.database.connection
        # Batch inserts require us to know the connected primary's
        # max_bson_size, max_message_size, and max_write_batch_size.
        # We have to be connected to the primary to know that.
        client._ensure_connected(True)

        docs = doc_or_docs
        return_one = False
        if isinstance(docs, dict):
            return_one = True
            docs = [docs]

        ids = []

        if manipulate:
            def gen():
                db = self.__database
                for doc in docs:
                    # Apply user-configured SON manipulators. This order of
                    # operations is required for backwards compatibility,
                    # see PYTHON-709.
                    doc = db._apply_incoming_manipulators(doc, self)
                    if '_id' not in doc:
                        doc['_id'] = ObjectId()

                    doc = db._apply_incoming_copying_manipulators(doc, self)
                    ids.append(doc['_id'])
                    yield doc
        else:
            def gen():
                for doc in docs:
                    ids.append(doc.get('_id'))
                    yield doc

        safe, options = self._get_write_mode(safe, **kwargs)

        if client.max_wire_version > 1 and safe:
            # Insert command
            command = SON([('insert', self.name),
                           ('ordered', not continue_on_error)])

            if options:
                command['writeConcern'] = options

            results = message._do_batched_write_command(
                    self.database.name + ".$cmd", _INSERT, command,
                    gen(), check_keys, self.uuid_subtype, client)
            _check_write_command_response(results)
        else:
            # Legacy batched OP_INSERT
            message._do_batched_insert(self.__full_name, gen(), check_keys,
                                       safe, options, continue_on_error,
                                       self.uuid_subtype, client)

        if return_one:
            return ids[0]
        else:
            return ids

    def insert_one(self, document):
        """Insert a single document.

          >>> db.test.count({'x': 1})
          0
          >>> result = db.test.insert_one({'x': 1})
          >>> result.inserted_id
          ObjectId('54f112defba522406c9cc208')
          >>> db.test.find_one({'x': 1})
          {u'x': 1, u'_id': ObjectId('54f112defba522406c9cc208')}

        :Parameters:
          - `document`: The document to insert. Must be a mapping
            type. If the document does not have an _id field one will be
            added automatically.

        :Returns:
          - An instance of :class:`~pymongo.results.InsertOneResult`.

        .. versionadded:: 2.9
        """
        common.validate_is_dict("document", document)
        ids = self.insert(document)
        return results.InsertOneResult(ids, self._get_write_mode()[0])

    def insert_many(self, documents, ordered=True):
        """Insert a list of documents.

          >>> db.test.count()
          0
          >>> result = db.test.insert_many([{'x': i} for i in range(2)])
          >>> result.inserted_ids
          [ObjectId('54f113fffba522406c9cc20e'), ObjectId('54f113fffba522406c9cc20f')]
          >>> db.test.count()
          2

        :Parameters:
          - `documents`: A list of documents to insert.
          - `ordered` (optional): If ``True`` (the default) documents will be
            inserted on the server serially, in the order provided. If an error
            occurs all remaining inserts are aborted. If ``False``, documents
            will be inserted on the server in arbitrary order, possibly in
            parallel, and all document inserts will be attempted.

        :Returns:
          An instance of :class:`~pymongo.results.InsertManyResult`.

        .. versionadded:: 2.9
        """
        if not isinstance(documents, list) or not documents:
            raise TypeError("documents must be a non-empty list")
        inserted_ids = []

        def gen():
            """A generator that validates documents and handles _ids."""
            for document in documents:
                common.validate_is_dict("document", document)
                if "_id" not in document:
                    document["_id"] = ObjectId()
                inserted_ids.append(document["_id"])
                yield (_INSERT, document)

        blk = bulk._Bulk(self, ordered)
        blk.ops = [doc for doc in gen()]
        blk.execute(self.write_concern)
        return results.InsertManyResult(inserted_ids,
                                        self._get_write_mode()[0])

    def update(self, spec, document, upsert=False, manipulate=False,
               safe=None, multi=False, check_keys=True, **kwargs):
        """Update a document(s) in this collection.

        Raises :class:`TypeError` if either `spec` or `document` is
        not an instance of ``dict`` or `upsert` is not an instance of
        ``bool``.

        Write concern options can be passed as keyword arguments, overriding
        any global defaults. Valid options include w=<int/string>,
        wtimeout=<int>, j=<bool>, or fsync=<bool>. See the parameter list below
        for a detailed explanation of these options.

        By default an acknowledgment is requested from the server that the
        update was successful, raising :class:`~pymongo.errors.OperationFailure`
        if an error occurred. **Passing w=0 disables write acknowledgement
        and all other write concern options.**

        There are many useful `update modifiers`_ which can be used
        when performing updates. For example, here we use the
        ``"$set"`` modifier to modify some fields in a matching
        document:

        .. doctest::

          >>> db.test.insert({"x": "y", "a": "b"})
          ObjectId('...')
          >>> import pprint
          >>> pprint.pprint(list(db.test.find()))
          [{u'_id': ObjectId('...'), u'a': u'b', u'x': u'y'}]
          >>> db.test.update({"x": "y"}, {"$set": {"a": "c"}})
          {...}
          >>> pprint.pprint(list(db.test.find()))
          [{u'_id': ObjectId('...'), u'a': u'c', u'x': u'y'}]

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
          - `check_keys` (optional): check if keys in `document` start
            with '$' or contain '.', raising
            :class:`~pymongo.errors.InvalidName`. Only applies to
            document replacement, not modification through $
            operators.
          - `safe` (optional): **DEPRECATED** - Use `w` instead.
          - `multi` (optional): update all documents that match
            `spec`, rather than just the first matching document. The
            default value for `multi` is currently ``False``, but this
            might eventually change to ``True``. It is recommended
            that you specify this argument explicitly for all update
            operations in order to prepare your code for that change.
          - `w` (optional): (integer or string) If this is a replica set, write
            operations will block until they have been replicated to the
            specified number or tagged set of servers. `w=<int>` always includes
            the replica set primary (e.g. w=3 means write to the primary and wait
            until replicated to **two** secondaries). **Passing w=0 disables
            write acknowledgement and all other write concern options.**
          - `wtimeout` (optional): (integer) Used in conjunction with `w`.
            Specify a value in milliseconds to control how long to wait for
            write propagation to complete. If replication does not complete in
            the given timeframe, a timeout exception is raised.
          - `j` (optional): If ``True`` block until write operations have been
            committed to the journal. Ignored if the server is running without
            journaling.
          - `fsync` (optional): If ``True`` force the database to fsync all
            files before returning. When used with `j` the server awaits the
            next group commit before returning.
        :Returns:
          - A document (dict) describing the effect of the update or ``None``
            if write acknowledgement is disabled.

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

        client = self.database.connection
        # Need to connect to know the wire version, and may want to connect
        # before applying SON manipulators.
        client._ensure_connected(True)
        if manipulate:
            document = self.__database._fix_incoming(document, self)

        safe, options = self._get_write_mode(safe, **kwargs)

        if document:
            # If a top level key begins with '$' this is a modify operation
            # and we should skip key validation. It doesn't matter which key
            # we check here. Passing a document with a mix of top level keys
            # starting with and without a '$' is invalid and the server will
            # raise an appropriate exception.
            first = (document.iterkeys()).next()
            if first.startswith('$'):
                check_keys = False

        if client.max_wire_version > 1 and safe:
            # Update command
            command = SON([('update', self.name)])
            if options:
                command['writeConcern'] = options

            docs = [SON([('q', spec), ('u', document),
                         ('multi', multi), ('upsert', upsert)])]

            results = message._do_batched_write_command(
                self.database.name + '.$cmd', _UPDATE, command,
                docs, check_keys, self.uuid_subtype, client)
            _check_write_command_response(results)

            _, result = results[0]
            # Add the updatedExisting field for compatibility
            if result.get('n') and 'upserted' not in result:
                result['updatedExisting'] = True
            else:
                result['updatedExisting'] = False
                # MongoDB >= 2.6.0 returns the upsert _id in an array
                # element. Break it out for backward compatibility.
                if isinstance(result.get('upserted'), list):
                    result['upserted'] = result['upserted'][0]['_id']

            return result

        else:
            # Legacy OP_UPDATE
            return client._send_message(
                message.update(self.__full_name, upsert, multi,
                               spec, document, safe, options,
                               check_keys, self.uuid_subtype), safe)

    def update_one(self, filter, update, upsert=False):
        """Update a single document matching the filter.

          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {u'x': 1, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}
          >>> result = db.test.update_one({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {u'x': 4, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionadded:: 2.9
        """
        common.validate_ok_for_update(update)
        result = self.update(
            filter, update, upsert, multi=False, check_keys=False)
        return results.UpdateResult(result, self._get_write_mode()[0])

    def update_many(self, filter, update, upsert=False):
        """Update one or more documents that match the filter.

          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {u'x': 1, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}
          >>> result = db.test.update_many({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          3
          >>> result.modified_count
          3
          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {u'x': 4, u'_id': 0}
          {u'x': 4, u'_id': 1}
          {u'x': 4, u'_id': 2}

        :Parameters:
          - `filter`: A query that matches the documents to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionadded:: 2.9
        """
        common.validate_ok_for_update(update)
        result = self.update(
            filter, update, upsert, multi=True, check_keys=False)
        return results.UpdateResult(result, self._get_write_mode()[0])

    def drop(self):
        """Alias for :meth:`~pymongo.database.Database.drop_collection`.

        The following two calls are equivalent:

          >>> db.foo.drop()
          >>> db.drop_collection("foo")

        .. versionadded:: 1.8
        """
        self.__database.drop_collection(self.__name)

    def remove(self, spec_or_id=None, safe=None, multi=True, **kwargs):
        """Remove a document(s) from this collection.

        .. warning:: Calls to :meth:`remove` should be performed with
           care, as removed data cannot be restored.

        If `spec_or_id` is ``None``, all documents in this collection
        will be removed. This is not equivalent to calling
        :meth:`~pymongo.database.Database.drop_collection`, however,
        as indexes will not be removed.

        Write concern options can be passed as keyword arguments, overriding
        any global defaults. Valid options include w=<int/string>,
        wtimeout=<int>, j=<bool>, or fsync=<bool>. See the parameter list below
        for a detailed explanation of these options.

        By default an acknowledgment is requested from the server that the
        remove was successful, raising :class:`~pymongo.errors.OperationFailure`
        if an error occurred. **Passing w=0 disables write acknowledgement
        and all other write concern options.**

        :Parameters:
          - `spec_or_id` (optional): a dictionary specifying the
            documents to be removed OR any other type specifying the
            value of ``"_id"`` for the document to be removed
          - `safe` (optional): **DEPRECATED** - Use `w` instead.
          - `multi` (optional): If ``True`` (the default) remove all documents
            matching `spec_or_id`, otherwise remove only the first matching
            document.
          - `w` (optional): (integer or string) If this is a replica set, write
            operations will block until they have been replicated to the
            specified number or tagged set of servers. `w=<int>` always includes
            the replica set primary (e.g. w=3 means write to the primary and wait
            until replicated to **two** secondaries). **Passing w=0 disables
            write acknowledgement and all other write concern options.**
          - `wtimeout` (optional): (integer) Used in conjunction with `w`.
            Specify a value in milliseconds to control how long to wait for
            write propagation to complete. If replication does not complete in
            the given timeframe, a timeout exception is raised.
          - `j` (optional): If ``True`` block until write operations have been
            committed to the journal. Ignored if the server is running without
            journaling.
          - `fsync` (optional): If ``True`` force the database to fsync all
            files before returning. When used with `j` the server awaits the
            next group commit before returning.
        :Returns:
          - A document (dict) describing the effect of the remove or ``None``
            if write acknowledgement is disabled.

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

        safe, options = self._get_write_mode(safe, **kwargs)

        client = self.database.connection

        # Need to connect to know the wire version.
        client._ensure_connected(True)
        if client.max_wire_version > 1 and safe:
            # Delete command
            command = SON([('delete', self.name)])
            if options:
                command['writeConcern'] = options

            docs = [SON([('q', spec_or_id), ('limit', int(not multi))])]

            results = message._do_batched_write_command(
                self.database.name + '.$cmd', _DELETE, command,
                docs, False, self.uuid_subtype, client)
            _check_write_command_response(results)

            _, result = results[0]
            return result

        else:
            # Legacy OP_DELETE
            return client._send_message(
                message.delete(self.__full_name, spec_or_id, safe,
                               options, self.uuid_subtype, int(not multi)), safe)

    def delete_one(self, filter):
        """Delete a single document matching the filter.

          >>> db.test.count({'x': 1})
          3
          >>> result = db.test.delete_one({'x': 1})
          >>> result.deleted_count
          1
          >>> db.test.count({'x': 1})
          2

        :Parameters:
          - `filter`: A query that matches the document to delete.
        :Returns:
          - An instance of :class:`~pymongo.results.DeleteResult`.

        .. versionadded:: 2.9
        """
        result = self.remove(filter, multi=False)
        return results.DeleteResult(result, self._get_write_mode()[0])

    def delete_many(self, filter):
        """Delete one or more documents matching the filter.

          >>> db.test.count({'x': 1})
          3
          >>> result = db.test.delete_many({'x': 1})
          >>> result.deleted_count
          3
          >>> db.test.count({'x': 1})
          0

        :Parameters:
          - `filter`: A query that matches the documents to delete.
        :Returns:
          - An instance of :class:`~pymongo.results.DeleteResult`.

        .. versionadded:: 2.9
        """
        result = self.remove(filter, multi=True)
        return results.DeleteResult(result, self._get_write_mode()[0])

    def replace_one(self, filter, replacement, upsert=False):
        """Replace a single document matching the filter.

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'x': 1, u'_id': ObjectId('54f4c5befba5220aa4d6dee7')}
          >>> result = db.test.replace_one({'x': 1}, {'y': 1})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'y': 1, u'_id': ObjectId('54f4c5befba5220aa4d6dee7')}

        The *upsert* option can be used to insert a new document if a matching
        document does not exist.

          >>> result = db.test.replace_one({'x': 1}, {'x': 1}, True)
          >>> result.matched_count
          0
          >>> result.modified_count
          0
          >>> result.upserted_id
          ObjectId('54f11e5c8891e756a6e1abd4')
          >>> db.test.find_one({'x': 1})
          {u'x': 1, u'_id': ObjectId('54f11e5c8891e756a6e1abd4')}

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The new document.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionadded:: 2.9
        """
        common.validate_ok_for_replace(replacement)
        result = self.update(filter, replacement, upsert, multi=False)
        return results.UpdateResult(result, self._get_write_mode()[0])

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

          - `max_time_ms` (optional): a value for max_time_ms may be
            specified as part of `**kwargs`, e.g.

              >>> find_one(max_time_ms=100)

        .. versionchanged:: 1.7
           Allow passing any of the arguments that are valid for
           :meth:`find`.

        .. versionchanged:: 1.7 Accept any type other than a ``dict``
           instance as an ``"_id"`` query, not just
           :class:`~bson.objectid.ObjectId` instances.
        """
        if spec_or_id is not None and not isinstance(spec_or_id, dict):
            spec_or_id = {"_id": spec_or_id}

        max_time_ms = kwargs.pop("max_time_ms", None)
        cursor = self.find(spec_or_id,
                           *args, **kwargs).max_time_ms(max_time_ms)

        for result in cursor.limit(-1):
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
            returned in the result set or a dict specifying the fields
            to include or exclude. If `fields` is a list "_id" will
            always be returned. Use a dict to exclude fields from
            the result (e.g. fields={'_id': False}).
          - `skip` (optional): the number of documents to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return
          - `timeout` (optional): if True (the default), any returned
            cursor is closed by the server after 10 minutes of
            inactivity. If set to False, the returned cursor will never
            time out on the server. Care should be taken to ensure that
            cursors with timeout turned off are properly closed.
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
            :attr:`~pymongo.mongo_client.MongoClient.document_class`)
          - `slave_okay` (optional): if True, allows this query to
            be run against a replica secondary.
          - `await_data` (optional): if True, the server will block for
            some extra time before returning, waiting for more data to
            return. Ignored if `tailable` is False.
          - `partial` (optional): if True, mongos will return partial
            results if some shards are down instead of returning an error.
          - `manipulate`: (optional): If True (the default), apply any
            outgoing SON manipulators before returning.
          - `read_preference` (optional): The read preference for
            this query.
          - `tag_sets` (optional): The tag sets for this query.
          - `secondary_acceptable_latency_ms` (optional): Any replica-set
            member whose ping time is within secondary_acceptable_latency_ms of
            the nearest member may accept reads. Default 15 milliseconds.
            **Ignored by mongos** and must be configured on the command line.
            See the localThreshold_ option for more information.
          - `exhaust` (optional): If ``True`` create an "exhaust" cursor.
            MongoDB will stream batched results to the client without waiting
            for the client to request each batch, reducing latency.
          - `compile_re` (optional): if ``False``, don't attempt to compile
            BSON regex objects into Python regexes. Return instances of
            :class:`~bson.regex.Regex` instead.
          - `oplog_replay` (optional): If True, set the oplogReplay query
            flag.
          - `modifiers` (optional): A dict specifying the MongoDB `query
            modifiers`_ that should be used for this query. For example::

            >>> db.test.find(modifiers={"$maxTimeMS": 500})

          - `network_timeout` (optional): specify a timeout to use for
            this query, which will override the
            :class:`~pymongo.mongo_client.MongoClient`-level default
          - `filter` (optional): a SON object specifying elements which
            must be present for a document to be included in the
            result set. Takes precedence over `spec`.
          - `projection` (optional): a list of field names that should be
            returned in the result set or a dict specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a dict to exclude fields from
            the result (e.g. projection={'_id': False}). Takes precedence
            over `fields`.
          - `no_cursor_timeout` (optional): if False (the default), any
            returned cursor is closed by the server after 10 minutes of
            inactivity. If set to True, the returned cursor will never
            time out on the server. Care should be taken to ensure that
            cursors with no_cursor_timeout turned on are properly closed.
            Takes precedence over `timeout`.
          - `allow_partial_results` (optional): if True, mongos will return
            partial results if some shards are down instead of returning an
            error. Takes precedence over `partial`.
          - `cursor_type` (optional): the type of cursor to return. Takes
            precedence over `tailable`, `await_data` and `exhaust`. The valid
            options are defined by :class:`~pymongo.cursor.CursorType`:

            - :attr:`~pymongo.cursor.CursorType.NON_TAILABLE` - the result of
              this find call will return a standard cursor over the result set.
            - :attr:`~pymongo.cursor.CursorType.TAILABLE` - the result of this
              find call will be a tailable cursor - tailable cursors are only
              for use with capped collections. They are not closed when the
              last data is retrieved but are kept open and the cursor location
              marks the final document position. If more data is received
              iteration of the cursor will continue from the last document
              received. For details, see the `tailable cursor documentation
              <http://www.mongodb.org/display/DOCS/Tailable+Cursors>`_.
            - :attr:`~pymongo.cursor.CursorType.TAILABLE_AWAIT` - the result
              of this find call will be a tailable cursor with the await flag
              set. The server will wait for a few seconds after returning the
              full result set so that it can capture and return additional data
              added during the query.
            - :attr:`~pymongo.cursor.CursorType.EXHAUST` - the result of this
              find call will be an exhaust cursor. MongoDB will stream batched
              results to the client without waiting for the client to request
              each batch, reducing latency. See notes on compatibility below.

        .. note:: There are a number of caveats to using the `exhaust`
           parameter:

            1. The `exhaust` and `limit` options are incompatible and can
            not be used together.

            2. The `exhaust` option is not supported by mongos and can not be
            used with a sharded cluster.

            3. A :class:`~pymongo.cursor.Cursor` instance created with the
            `exhaust` option requires an exclusive :class:`~socket.socket`
            connection to MongoDB. If the :class:`~pymongo.cursor.Cursor` is
            discarded without being completely iterated the underlying
            :class:`~socket.socket` connection will be closed and discarded
            without being returned to the connection pool.

            4. A :class:`~pymongo.cursor.Cursor` instance created with the
            `exhaust` option in a :doc:`request </examples/requests>` **must**
            be completely iterated before executing any other operation.

            5. The `network_timeout` option is ignored when using the
            `exhaust` option.

        .. note:: The `manipulate` and `compile_re` parameters may default to
           False in future releases.

        .. note:: The `max_scan` parameter requires server
           version **>= 1.5.1**

        .. versionadded:: 2.9
           The ``filter``, ``projection``, ``no_cursor_timeout``,
           ``allow_partial_results``, ``cursor_type``, ``modifiers`` parameters.

        .. versionadded:: 2.7
           The ``compile_re`` parameter.

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
        .. _query modifiers:
          http://docs.mongodb.org/manual/reference/operator/query-modifier
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

    def parallel_scan(self, num_cursors, **kwargs):
        """Scan this entire collection in parallel.

        Returns a list of up to ``num_cursors`` cursors that can be iterated
        concurrently. As long as the collection is not modified during
        scanning, each document appears once in one of the cursors' result
        sets.

        For example, to process each document in a collection using some
        thread-safe ``process_document()`` function::

            def process_cursor(cursor):
                for document in cursor:
                    # Some thread-safe processing function:
                    process_document(document)

            # Get up to 4 cursors.
            cursors = collection.parallel_scan(4)
            threads = [
                threading.Thread(target=process_cursor, args=(cursor,))
                for cursor in cursors]

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

            # All documents have now been processed.

        With :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`
        or :class:`~pymongo.master_slave_connection.MasterSlaveConnection`,
        if the `read_preference` attribute of this instance is not set to
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY` or the
        (deprecated) `slave_okay` attribute of this instance is set to `True`
        the command will be sent to a secondary or slave.

        :Parameters:
          - `num_cursors`: the number of cursors to return

        .. note:: Requires server version **>= 2.5.5**.

        """
        use_master = not self.slave_okay and not self.read_preference
        compile_re = kwargs.get('compile_re', False)

        command_kwargs = {
            'numCursors': num_cursors,
            'read_preference': self.read_preference,
            'tag_sets': self.tag_sets,
            'secondary_acceptable_latency_ms': (
                self.secondary_acceptable_latency_ms),
            'slave_okay': self.slave_okay,
            '_use_master': use_master}
        command_kwargs.update(kwargs)

        result, conn_id = self.__database._command(
            "parallelCollectionScan", self.__name, **command_kwargs)

        return [CommandCursor(self,
                              cursor['cursor'],
                              conn_id,
                              compile_re) for cursor in result['cursors']]

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
        (:class:`str` in python 3), and the direction(s) should be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`, :data:`~pymongo.GEOHAYSTACK`,
        :data:`~pymongo.GEOSPHERE`, :data:`~pymongo.HASHED`,
        :data:`~pymongo.TEXT`).

        To create a simple ascending index on the key ``'mike'`` we just
        use a string argument::

          >>> my_collection.create_index("mike")

        For a compound index on ``'mike'`` descending and ``'eliot'``
        ascending we need to use a list of tuples::

          >>> my_collection.create_index([("mike", pymongo.DESCENDING),
          ...                             ("eliot", pymongo.ASCENDING)])

        All optional index creation parameters should be passed as
        keyword arguments to this method. For example::

          >>> my_collection.create_index([("mike", pymongo.DESCENDING)],
          ...                            background=True)

        Valid options include, but are not limited to:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated
          - `unique`: if ``True`` creates a unique constraint on the index
          - `background`: if ``True`` this index should be created in the
            background
          - `sparse`: if ``True``, omit from the index any documents that lack
            the indexed field
          - `bucketSize` or `bucket_size`: for use with geoHaystack indexes.
            Number of documents to group together within a certain proximity
            to a given longitude and latitude.
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index
          - `expireAfterSeconds`: <int> Used to create an expiring (TTL)
            collection. MongoDB will automatically delete documents from
            this collection after <int> seconds. The indexed field must
            be a UTC datetime or the data will not expire.
          - `dropDups` or `drop_dups` (**deprecated**): if ``True`` duplicate
            values are dropped during index creation when creating a unique
            index

        See the MongoDB documentation for a full list of supported options by
        server version.

        .. warning:: `dropDups` / `drop_dups` is no longer supported by
          MongoDB starting with server version 2.7.5. The option is silently
          ignored by the server and unique index builds using the option will
          fail if a duplicate value is detected.

        .. note:: `expireAfterSeconds` requires server version **>= 2.1.2**

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
                          DeprecationWarning, stacklevel=2)

        # The types supported by datetime.timedelta. 2to3 removes long.
        if not isinstance(cache_for, (int, long, float)):
            raise TypeError("cache_for must be an integer or float.")

        keys = helpers._index_list(key_or_list)
        index_doc = helpers._index_document(keys)

        name = "name" in kwargs and kwargs["name"] or _gen_index_name(keys)
        index = {"key": index_doc, "name": name}

        if "drop_dups" in kwargs:
            kwargs["dropDups"] = kwargs.pop("drop_dups")

        if "bucket_size" in kwargs:
            kwargs["bucketSize"] = kwargs.pop("bucket_size")

        index.update(kwargs)

        try:
            self.__database.command('createIndexes', self.name,
                                    read_preference=ReadPreference.PRIMARY,
                                    indexes=[index])
        except OperationFailure, exc:
            if exc.code in common.COMMAND_NOT_FOUND_CODES:
                index["ns"] = self.__full_name
                self.__database.system.indexes.insert(index, manipulate=False,
                                                      check_keys=False,
                                                      **self._get_wc_override())
            else:
                raise

        self.__database.connection._cache_index(self.__database.name,
                                                self.__name, name, cache_for)

        return name

    def ensure_index(self, key_or_list, cache_for=300, **kwargs):
        """Ensures that an index exists on this collection.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`
        (:class:`str` in python 3), and the direction(s) should be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`, :data:`~pymongo.GEOHAYSTACK`,
        :data:`~pymongo.GEOSPHERE`, :data:`~pymongo.HASHED`,
        :data:`pymongo.TEXT`).

        See :meth:`create_index` for detailed examples.

        Unlike :meth:`create_index`, which attempts to create an index
        unconditionally, :meth:`ensure_index` takes advantage of some
        caching within the driver such that it only attempts to create
        indexes that might not already exist. When an index is created
        (or ensured) by PyMongo it is "remembered" for `cache_for`
        seconds. Repeated calls to :meth:`ensure_index` within that
        time limit will be lightweight - they will not attempt to
        actually create the index.

        Care must be taken when the database is being accessed through
        multiple clients at once. If an index is created using
        this client and deleted using another, any call to
        :meth:`ensure_index` within the cache window will fail to
        re-create the missing index.

        Returns the specified or generated index name used if
        :meth:`ensure_index` attempts to create the index. Returns
        ``None`` if the index is already cached.

        All optional index creation parameters should be passed as
        keyword arguments to this method. Valid options include, but are not
        limited to:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated
          - `unique`: if ``True`` creates a unique constraint on the index
          - `background`: if ``True`` this index should be created in the
            background
          - `sparse`: if ``True``, omit from the index any documents that lack
            the indexed field
          - `bucketSize` or `bucket_size`: for use with geoHaystack indexes.
            Number of documents to group together within a certain proximity
            to a given longitude and latitude.
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index
          - `expireAfterSeconds`: <int> Used to create an expiring (TTL)
            collection. MongoDB will automatically delete documents from
            this collection after <int> seconds. The indexed field must
            be a UTC datetime or the data will not expire.
          - `dropDups` or `drop_dups` (**deprecated**): if ``True`` duplicate
            values are dropped during index creation when creating a unique
            index

        See the MongoDB documentation for a full list of supported options by
        server version.

        .. warning:: `dropDups` / `drop_dups` is no longer supported by
          MongoDB starting with server version 2.7.5. The option is silently
          ignored by the server and unique index builds using the option will
          fail if a duplicate value is detected.

        .. note:: `expireAfterSeconds` requires server version **>= 2.1.2**

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

        # Note that there is a race condition here. One thread could
        # check if the index is cached and be preempted before creating
        # and caching the index. This means multiple threads attempting
        # to create the same index concurrently could send the index
        # to the server two or more times. This has no practical impact
        # other than wasted round trips.
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
        indexes.  Raises OperationFailure on an error (e.g. trying to
        drop an index that does not exist). `index_or_name`
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
        self.__database.command("dropIndexes", self.__name,
                                read_preference=ReadPreference.PRIMARY,
                                index=name,
                                allowable_errors=["ns not found"])

    def reindex(self):
        """Rebuilds all indexes on this collection.

        .. warning:: reindex blocks all other operations (indexes
           are built in the foreground) and will be slow for large
           collections.

        .. versionadded:: 1.11+
        """
        return self.__database.command("reIndex", self.__name,
                                       read_preference=ReadPreference.PRIMARY)

    def index_information(self):
        """Get information on this collection's indexes.

        Returns a dictionary where the keys are index names (as
        returned by create_index()) and the values are dictionaries
        containing information about each index. The dictionary is
        guaranteed to contain at least a single key, ``"key"`` which
        is a list of (key, direction) pairs specifying the index (as
        passed to create_index()). It will also contain any other
        metadata about the indexes, except for the ``"ns"`` and
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
        client = self.database.connection
        client._ensure_connected(True)

        slave_okay = not client._rs_client and not client.is_mongos
        if client.max_wire_version > 2:
            res, addr = self.__database._command(
                "listIndexes", self.__name, as_class=SON,
                cursor={}, slave_okay=slave_okay,
                read_preference=ReadPreference.PRIMARY)
            # MongoDB 2.8rc2
            if "indexes" in res:
                raw = res["indexes"]
            # >= MongoDB 2.8rc3
            else:
                raw = CommandCursor(self, res["cursor"], addr)
        else:
            raw = self.__database.system.indexes.find({"ns": self.__full_name},
                                                      {"ns": 0}, as_class=SON,
                                                      slave_okay=slave_okay,
                                                      _must_use_master=True)
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
        client = self.database.connection
        client._ensure_connected(True)

        result = None
        slave_okay = not client._rs_client and not client.is_mongos
        if client.max_wire_version > 2:
            res, addr = self.__database._command(
                "listCollections",
                cursor={},
                filter={"name": self.__name},
                read_preference=ReadPreference.PRIMARY,
                slave_okay=slave_okay)
            # MongoDB 2.8rc2
            if "collections" in res:
                results = res["collections"]
            # >= MongoDB 2.8rc3
            else:
                results = CommandCursor(self, res["cursor"], addr)
            for doc in results:
                result = doc
                break
        else:
            result = self.__database.system.namespaces.find_one(
                {"name": self.__full_name},
                slave_okay=slave_okay,
                _must_use_master=True)

        if not result:
            return {}

        options = result.get("options", {})
        if "create" in options:
            del options["create"]

        return options

    def aggregate(self, pipeline, **kwargs):
        """Perform an aggregation using the aggregation framework on this
        collection.

        With :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`
        or :class:`~pymongo.master_slave_connection.MasterSlaveConnection`,
        if the `read_preference` attribute of this instance is not set to
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY` or the
        (deprecated) `slave_okay` attribute of this instance is set to `True`
        the `aggregate command`_ will be sent to a secondary or slave.

        :Parameters:
          - `pipeline`: a single command or list of aggregation commands
          - `**kwargs`: send arbitrary parameters to the aggregate command

        .. note:: Requires server version **>= 2.1.0**.

        With server version **>= 2.5.1**, pass
        ``cursor={}`` to retrieve unlimited aggregation results
        with a :class:`~pymongo.command_cursor.CommandCursor`::

            pipeline = [{'$project': {'name': {'$toUpper': '$name'}}}]
            cursor = collection.aggregate(pipeline, cursor={})
            for doc in cursor:
                print doc

        .. versionchanged:: 2.9
           The :meth:`aggregate` helper always returns a
           :class:`~pymongo.command_cursor.CommandCursor` when the cursor
           option is passed, regardless of MongoDB server version.
        .. versionchanged:: 2.7
           When the cursor option is used, return
           :class:`~pymongo.command_cursor.CommandCursor` instead of
           :class:`~pymongo.cursor.Cursor`.
        .. versionchanged:: 2.6
           Added cursor support.
        .. versionadded:: 2.3

        .. _aggregate command:
            http://docs.mongodb.org/manual/applications/aggregation
        """
        if not isinstance(pipeline, (dict, list, tuple)):
            raise TypeError("pipeline must be a dict, list or tuple")

        if isinstance(pipeline, dict):
            pipeline = [pipeline]

        use_master = not self.slave_okay and not self.read_preference

        command_kwargs = {
            'pipeline': pipeline,
            'codec_options': self.codec_options,
            'read_preference': self.read_preference,
            'tag_sets': self.tag_sets,
            'secondary_acceptable_latency_ms': (
                self.secondary_acceptable_latency_ms),
            'slave_okay': self.slave_okay,
            '_use_master': use_master}

        command_kwargs.update(kwargs)

        # If the server version can't support 'cursor'.
        if self.database.connection.max_wire_version < 1:
            command_kwargs.pop('cursor', None)

        result, conn_id = self.__database._command(
            "aggregate", self.__name, **command_kwargs)

        if "cursor" in kwargs:
            if 'cursor' in result:
                cursor = result['cursor']
            else:
                # Pre-MongoDB 2.6. Fake a cursor.
                cursor = {
                    "id": 0,
                    "firstBatch": result["result"],
                    "ns": self.full_name,
                }
            return CommandCursor(
                self,
                cursor,
                conn_id,
                command_kwargs.get('compile_re', True))
        else:
            return result

    # TODO key and condition ought to be optional, but deprecation
    # could be painful as argument order would have to change.
    def group(self, key, condition, initial, reduce, finalize=None, **kwargs):
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

        With :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`
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
                                       codec_options=self.codec_options,
                                       read_preference=self.read_preference,
                                       tag_sets=self.tag_sets,
                                       secondary_acceptable_latency_ms=(
                                           self.secondary_acceptable_latency_ms),
                                       slave_okay=self.slave_okay,
                                       _use_master=use_master,
                                       **kwargs)["retval"]

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
        client = self.__database.connection
        client.admin.command("renameCollection", self.__full_name,
                             read_preference=ReadPreference.PRIMARY,
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
                                           codec_options=self.codec_options,
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

        With :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`
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
                                      codec_options=self.codec_options,
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

    def find_and_modify(self, query={}, update=None,
                        upsert=False, sort=None, full_response=False,
                        manipulate=False, fields=None, **kwargs):
        """Update and return an object.

        This is a thin wrapper around the findAndModify_ command. The
        positional arguments are designed to match the first three arguments
        to :meth:`update` however most options should be passed as named
        parameters. Either `update` or `remove` arguments are required, all
        others are optional.

        Returns either the object before or after modification based on `new`
        parameter. If no objects match the `query` and `upsert` is false,
        returns ``None``. If upserting and `new` is false, returns ``{}``.

        If the full_response parameter is ``True``, the return value will be
        the entire response object from the server, including the 'ok' and
        'lastErrorObject' fields, rather than just the modified object.
        This is useful mainly because the 'lastErrorObject' document holds 
        information about the command's execution.

        :Parameters:
            - `query`: filter for the update (default ``{}``)
            - `update`: see second argument to :meth:`update` (no default)
            - `upsert`: insert if object doesn't exist (default ``False``)
            - `sort`: a list of (key, direction) pairs specifying the sort
              order for this query. See :meth:`~pymongo.cursor.Cursor.sort`
              for details.
            - `full_response`: return the entire response object from the
              server (default ``False``)
            - `remove`: remove rather than updating (default ``False``)
            - `new`: return updated rather than original object
              (default ``False``)
            - `fields`: (optional): see second argument to :meth:`find` (default all)
            - `manipulate`: (optional): If ``True``, apply any outgoing SON
              manipulators before returning. Ignored when `full_response`
              is set to True. Defaults to ``False``.
            - `**kwargs`: any other options the findAndModify_ command
              supports can be passed here.


        .. mongodoc:: findAndModify

        .. _findAndModify: http://dochub.mongodb.org/core/findAndModify

        .. note:: Requires server version **>= 1.3.0**

        .. versionchanged:: 2.9
           Made fields a named parameter.

        .. versionchanged:: 2.8
           Added the optional manipulate parameter

        .. versionchanged:: 2.5
           Added the optional full_response parameter

        .. versionchanged:: 2.4
           Deprecated the use of mapping types for the sort parameter

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
        if fields:
            kwargs['fields'] = fields
        if sort:
            # Accept a list of tuples to match Cursor's sort parameter.
            if isinstance(sort, list):
                kwargs['sort'] = helpers._index_document(sort)
            # Accept OrderedDict, SON, and dict with len == 1 so we
            # don't break existing code already using find_and_modify.
            elif (isinstance(sort, ordered_types) or
                  isinstance(sort, dict) and len(sort) == 1):
                warnings.warn("Passing mapping types for `sort` is deprecated,"
                              " use a list of (key, direction) pairs instead",
                              DeprecationWarning, stacklevel=2)
                kwargs['sort'] = sort
            else:
                raise TypeError("sort must be a list of (key, direction) "
                                 "pairs, a dict of len 1, or an instance of "
                                 "SON or OrderedDict")

        no_obj_error = "No matching object found"

        out = self.__database.command("findAndModify", self.__name,
                                      allowable_errors=[no_obj_error],
                                      read_preference=ReadPreference.PRIMARY,
                                      codec_options=self.codec_options,
                                      **kwargs)

        if not out['ok']:
            if out["errmsg"] == no_obj_error:
                return None
            else:
                # Should never get here b/c of allowable_errors
                raise ValueError("Unexpected Error: %s" % (out,))

        if full_response:
            return out
        else:
            document = out.get('value')
            if manipulate:
                document = self.__database._fix_outgoing(document, self)
            return document

    def find_one_and_delete(self, filter,
                            projection=None, sort=None, **kwargs):
        """Finds a single document and deletes it, returning the document.

          >>> db.test.count({'x': 1})
          2
          >>> db.test.find_one_and_delete({'x': 1})
          {u'x': 1, u'_id': ObjectId('54f4e12bfba5220aa4d6dee8')}
          >>> db.test.count({'x': 1})
          1

        If multiple documents match *filter*, a *sort* can be applied.

          >>> for doc in db.test.find({'x': 1}):
          ...     print(doc)
          ...
          {u'x': 1, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}
          >>> db.test.find_one_and_delete(
          ...     {'x': 1}, sort=[('_id', pymongo.DESCENDING)])
          {u'x': 1, u'_id': 2}

        The *projection* option can be used to limit the fields returned.

          >>> db.test.find_one_and_delete({'x': 1}, projection={'_id': False})
          {u'x': 1}

        :Parameters:
          - `filter`: A query that matches the document to delete.
          - `projection` (optional): a list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a mapping to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is deleted.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionadded:: 2.9
        """
        common.validate_is_dict("filter", filter)
        kwargs['remove'] = True
        return self.find_and_modify(filter, fields=projection, sort=sort,
                                    **kwargs)

    def find_one_and_replace(self, filter, replacement,
                             projection=None, sort=None, upsert=False,
                             return_document=ReturnDocument.BEFORE, **kwargs):
        """Finds a single document and replaces it, returning either the
        original or the replaced document.

        The :meth:`find_one_and_replace` method differs from
        :meth:`find_one_and_update` by replacing the document matched by
        *filter*, rather than modifying the existing document.

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'x': 1, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}
          >>> db.test.find_one_and_replace({'x': 1}, {'y': 1})
          {u'x': 1, u'_id': 0}
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'y': 1, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The replacement document.
          - `projection` (optional): A list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a mapping to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is replaced.
          - `upsert` (optional): When ``True``, inserts a new document if no
            document matches the query. Defaults to ``False``.
          - `return_document`: If
            :attr:`ReturnDocument.BEFORE` (the default),
            returns the original document before it was replaced, or ``None``
            if no document matches. If
            :attr:`ReturnDocument.AFTER`, returns the replaced
            or inserted document.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionadded:: 2.9
        """
        common.validate_ok_for_replace(replacement)
        kwargs['update'] = replacement
        new = return_document == ReturnDocument.AFTER
        return self.find_and_modify(filter, fields=projection,
                                    sort=sort, upsert=upsert,
                                    new=new,
                                    **kwargs)

    def find_one_and_update(self, filter, update,
                            projection=None, sort=None, upsert=False,
                            return_document=ReturnDocument.BEFORE, **kwargs):
        """Finds a single document and updates it, returning either the
        original or the updated document.

          >>> db.test.find_one_and_update(
          ...    {'_id': 665}, {'$inc': {'count': 1}, '$set': {'done': True}})
          {u'_id': 665, u'done': False, u'count': 25}}

        By default :meth:`find_one_and_update` returns the original version of
        the document before the update was applied. To return the updated
        version of the document instead, use the *return_document* option.

          >>> from pymongo import ReturnDocument
          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     return_document=ReturnDocument.AFTER)
          {u'_id': u'userid', u'seq': 1}

        You can limit the fields returned with the *projection* option.

          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     projection={'seq': True, '_id': False},
          ...     return_document=ReturnDocument.AFTER)
          {u'seq': 2}

        The *upsert* option can be used to create the document if it doesn't
        already exist.

          >>> db.example.delete_many({}).deleted_count
          1
          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     projection={'seq': True, '_id': False},
          ...     upsert=True,
          ...     return_document=ReturnDocument.AFTER)
          {u'seq': 1}

        If multiple documents match *filter*, a *sort* can be applied.

          >>> for doc in db.test.find({'done': True}):
          ...     print(doc)
          ...
          {u'_id': 665, u'done': True, u'result': {u'count': 26}}
          {u'_id': 701, u'done': True, u'result': {u'count': 17}}
          >>> db.test.find_one_and_update(
          ...     {'done': True},
          ...     {'$set': {'final': True}},
          ...     sort=[('_id', pymongo.DESCENDING)])
          {u'_id': 701, u'done': True, u'result': {u'count': 17}}

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The update operations to apply.
          - `projection` (optional): A list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a dict to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is updated.
          - `upsert` (optional): When ``True``, inserts a new document if no
            document matches the query. Defaults to ``False``.
          - `return_document`: If
            :attr:`ReturnDocument.BEFORE` (the default),
            returns the original document before it was updated, or ``None``
            if no document matches. If
            :attr:`ReturnDocument.AFTER`, returns the updated
            or inserted document.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionadded:: 2.9
        """
        common.validate_ok_for_update(update)
        kwargs['update'] = update
        new = return_document == ReturnDocument.AFTER
        return self.find_and_modify(filter, fields=projection,
                                    sort=sort, upsert=upsert,
                                    new=new,
                                    **kwargs)

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
