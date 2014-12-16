# Copyright 2009-2014 MongoDB, Inc.
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

from __future__ import unicode_literals

import collections
import warnings

from bson.code import Code
from bson.objectid import ObjectId
from bson.py3compat import (_unicode,
                            integer_types,
                            string_type)
from bson.son import SON
from pymongo import (bulk,
                     common,
                     helpers,
                     message)
from pymongo.codec_options import CodecOptions
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.errors import InvalidName, OperationFailure
from pymongo.helpers import _check_write_command_response, _command
from pymongo.message import _INSERT, _UPDATE, _DELETE
from pymongo.read_preferences import ReadPreference


try:
    from collections import OrderedDict
    ordered_types = (SON, OrderedDict)
except ImportError:
    ordered_types = SON


def _gen_index_name(keys):
    """Generate an index name from the set of fields it is over.
    """
    return "_".join(["%s_%s" % item for item in keys])


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
            :class:`~pymongo.codec_options.CodecOptions`. If ``None`` (the
            default) database.codec_options is used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) database.read_preference is used.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) database.write_concern is used.
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 3.0
           Added the codec_options, read_preference, and write_concern options.
           :class:`~pymongo.collection.Collection` no longer returns an
           instance of :class:`~pymongo.collection.Collection` for attribute
           names with leading underscores. You must use dict-style lookups
           instead::

               collection['__my_collection__']

           Not:

               collection.__my_collection__

        .. versionchanged:: 2.2
           Removed deprecated argument: options

        .. versionadded:: 2.1
           uuid_subtype attribute

        .. mongodoc:: collections
        """
        super(Collection, self).__init__(
            codec_options or database.codec_options,
            read_preference or database.read_preference,
            write_concern or database.write_concern)

        if not isinstance(name, string_type):
            raise TypeError("name must be an instance "
                            "of %s" % (string_type.__name__,))

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
        self.__name = _unicode(name)
        self.__full_name = "%s.%s" % (self.__database.name, self.__name)
        if create or kwargs:
            self.__create(kwargs)

    def _command(
            self, command, read_preference=None, codec_options=None, **kwargs):
        """Internal command helper.
        
        :Parameters:
          - `command` - The command itself, as a SON instance.
          - `read_preference` (optional) - An subclass of
            :class:`~pymongo.read_preferences.ServerMode`.
          - `codec_options` (optional) - An instance of
            :class:`~pymongo.codec_options.CodecOptions`.
          - `**kwargs` - any optional keyword arguments accepted by
            :func:`~pymongo.helpers._command`.

        :Returns:
          (result document, address of server the command was run on)
        """
        return _command(self.__database.connection,
                        self.__database.name + ".$cmd",
                        command,
                        read_preference or self.read_preference,
                        codec_options or self.codec_options,
                        **kwargs)

    def __create(self, options):
        """Sends a create command with the given options.
        """
        cmd = SON([("create", self.__name)])
        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            cmd.update(options)
        self._command(cmd, ReadPreference.PRIMARY)

    def __getattr__(self, name):
        """Get a sub-collection of this collection by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        if name.startswith('_'):
            full_name = '%s.%s' % (self.__name, name)
            raise AttributeError(
                "Collection has no attribute %r. To access the %s"
                " collection, use database['%s']." % (
                    name, full_name, full_name))
        return self.__getitem__(name)

    def __getitem__(self, name):
        return Collection(self.__database, "%s.%s" % (self.__name, name))

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
        """
        return self.__full_name

    @property
    def name(self):
        """The name of this :class:`Collection`."""
        return self.__name

    @property
    def database(self):
        """The :class:`~pymongo.database.Database` that this
        :class:`Collection` is a part of.
        """
        return self.__database

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

    def save(self, to_save, manipulate=True, check_keys=True, **kwargs):
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
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising :class:`~pymongo.errors.InvalidName`
            in either case.
          - `w`: (integer or string) Used with replication, write operations
            will block until they have been replicated to the specified number
            or tagged set of servers. `w=<integer>` always includes the replica
            set primary (e.g. w=3 means write to the primary and wait until
            replicated to **two** secondaries). **w=0 disables acknowledgement
            of write operations and can not be used with other write concern
            options.**
          - `wtimeout`: (integer) Used in conjunction with `w`. Specify a value
            in milliseconds to control how long to wait for write propagation
            to complete. If replication does not complete in the given
            timeframe, a timeout exception is raised.
          - `j`: If ``True`` block until write operations have been committed
            to the journal. Cannot be used in combination with `fsync`. Prior
            to MongoDB 2.6 this option was ignored if the server was running
            without journaling. Starting with MongoDB 2.6 write operations will
            fail with an exception if this option is used when the server is
            running without journaling.
          - `fsync`: If ``True`` and the server is running without journaling,
            blocks until the server has synced all data files to disk. If the
            server is running with journaling, this acts the same as the `j`
            option, blocking until write operations have been committed to the
            journal. Cannot be used in combination with `j`.
        :Returns:
          - The ``'_id'`` value of `to_save` or ``[None]`` if `manipulate` is
            ``False`` and `to_save` has no '_id' field.

        .. versionchanged:: 3.0
           Removed the `safe` parameter

        .. mongodoc:: insert
        """
        if not isinstance(to_save, collections.MutableMapping):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if "_id" not in to_save:
            return self.insert(to_save, manipulate, check_keys, **kwargs)
        else:
            self.update({"_id": to_save["_id"]}, to_save, True,
                        manipulate, check_keys=check_keys, **kwargs)
            return to_save.get("_id", None)

    def insert(self, doc_or_docs, manipulate=True,
               check_keys=True, continue_on_error=False, **kwargs):
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
          - `check_keys` (optional): If ``True`` check if keys start with '$'
            or contain '.', raising :class:`~pymongo.errors.InvalidName` in
            either case.
          - `continue_on_error` (optional): If ``True``, the database will not
            stop processing a bulk insert if one fails (e.g. due to duplicate
            IDs). This makes bulk insert behave similarly to a series of single
            inserts, except lastError will be set if any insert fails, not just
            the last one. If multiple errors occur, only the most recent will
            be reported by :meth:`~pymongo.database.Database.error`.
          - `w`: (integer or string) Used with replication, write operations
            will block until they have been replicated to the specified number
            or tagged set of servers. `w=<integer>` always includes the replica
            set primary (e.g. w=3 means write to the primary and wait until
            replicated to **two** secondaries). **w=0 disables acknowledgement
            of write operations and can not be used with other write concern
            options.**
          - `wtimeout`: (integer) Used in conjunction with `w`. Specify a value
            in milliseconds to control how long to wait for write propagation
            to complete. If replication does not complete in the given
            timeframe, a timeout exception is raised.
          - `j`: If ``True`` block until write operations have been committed
            to the journal. Cannot be used in combination with `fsync`. Prior
            to MongoDB 2.6 this option was ignored if the server was running
            without journaling. Starting with MongoDB 2.6 write operations will
            fail with an exception if this option is used when the server is
            running without journaling.
          - `fsync`: If ``True`` and the server is running without journaling,
            blocks until the server has synced all data files to disk. If the
            server is running with journaling, this acts the same as the `j`
            option, blocking until write operations have been committed to the
            journal. Cannot be used in combination with `j`.
        :Returns:
          - The ``'_id'`` value (or list of '_id' values) of `doc_or_docs` or
            ``[None]`` if manipulate is ``False`` and the documents passed
            as `doc_or_docs` do not include an '_id' field.

        .. note:: `continue_on_error` requires server version **>= 1.9.1**

        .. versionchanged:: 3.0
           Removed the `safe` parameter
        .. versionadded:: 2.1
           Support for continue_on_error.

        .. mongodoc:: insert
        """
        client = self.database.connection
        docs = doc_or_docs
        return_one = False
        if isinstance(docs, collections.MutableMapping):
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

        concern = kwargs or self.write_concern
        safe = concern.get("w") != 0

        if client._writable_max_wire_version() > 1 and safe:
            # Insert command
            command = SON([('insert', self.name),
                           ('ordered', not continue_on_error)])

            if concern:
                command['writeConcern'] = concern

            results = message._do_batched_write_command(
                    self.database.name + ".$cmd", _INSERT, command,
                    gen(), check_keys, self.uuid_subtype, client)
            _check_write_command_response(results)
        else:
            # Legacy batched OP_INSERT
            message._do_batched_insert(self.__full_name, gen(), check_keys,
                                       safe, concern, continue_on_error,
                                       self.uuid_subtype, client)

        if return_one:
            return ids[0]
        else:
            return ids

    def update(self, spec, document, upsert=False, manipulate=False,
               multi=False, check_keys=True, **kwargs):
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
          >>> list(db.test.find())
          [{u'a': u'b', u'x': u'y', u'_id': ObjectId('...')}]
          >>> db.test.update({"x": "y"}, {"$set": {"a": "c"}})
          {...}
          >>> list(db.test.find())
          [{u'a': u'c', u'x': u'y', u'_id': ObjectId('...')}]

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
          - `multi` (optional): update all documents that match
            `spec`, rather than just the first matching document. The
            default value for `multi` is currently ``False``, but this
            might eventually change to ``True``. It is recommended
            that you specify this argument explicitly for all update
            operations in order to prepare your code for that change.
          - `w`: (integer or string) Used with replication, write operations
            will block until they have been replicated to the specified number
            or tagged set of servers. `w=<integer>` always includes the replica
            set primary (e.g. w=3 means write to the primary and wait until
            replicated to **two** secondaries). **w=0 disables acknowledgement
            of write operations and can not be used with other write concern
            options.**
          - `wtimeout`: (integer) Used in conjunction with `w`. Specify a value
            in milliseconds to control how long to wait for write propagation
            to complete. If replication does not complete in the given
            timeframe, a timeout exception is raised.
          - `j`: If ``True`` block until write operations have been committed
            to the journal. Cannot be used in combination with `fsync`. Prior
            to MongoDB 2.6 this option was ignored if the server was running
            without journaling. Starting with MongoDB 2.6 write operations will
            fail with an exception if this option is used when the server is
            running without journaling.
          - `fsync`: If ``True`` and the server is running without journaling,
            blocks until the server has synced all data files to disk. If the
            server is running with journaling, this acts the same as the `j`
            option, blocking until write operations have been committed to the
            journal. Cannot be used in combination with `j`.
        :Returns:
          - A document (dict) describing the effect of the update or ``None``
            if write acknowledgement is disabled.

        .. versionchanged:: 3.0
           Removed the `safe` parameter

        .. _update modifiers: http://www.mongodb.org/display/DOCS/Updating

        .. mongodoc:: update
        """
        if not isinstance(spec, collections.Mapping):
            raise TypeError("spec must be a mapping type")
        if not isinstance(document, collections.Mapping):
            raise TypeError("document must be a mapping type")
        if not isinstance(upsert, bool):
            raise TypeError("upsert must be an instance of bool")

        if manipulate:
            document = self.__database._fix_incoming(document, self)

        concern = kwargs or self.write_concern
        safe = concern.get("w") != 0

        if document:
            # If a top level key begins with '$' this is a modify operation
            # and we should skip key validation. It doesn't matter which key
            # we check here. Passing a document with a mix of top level keys
            # starting with and without a '$' is invalid and the server will
            # raise an appropriate exception.
            first = next(iter(document))
            if first.startswith('$'):
                check_keys = False

        client = self.database.connection
        if client._writable_max_wire_version() > 1 and safe:
            # Update command
            command = SON([('update', self.name)])
            if concern:
                command['writeConcern'] = concern

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
                               spec, document, safe, concern,
                               check_keys, self.uuid_subtype), safe)

    def drop(self):
        """Alias for :meth:`~pymongo.database.Database.drop_collection`.

        The following two calls are equivalent:

          >>> db.foo.drop()
          >>> db.drop_collection("foo")
        """
        self.__database.drop_collection(self.__name)

    def remove(self, spec_or_id=None, multi=True, **kwargs):
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
          - `multi` (optional): If ``True`` (the default) remove all documents
            matching `spec_or_id`, otherwise remove only the first matching
            document.
          - `w`: (integer or string) Used with replication, write operations
            will block until they have been replicated to the specified number
            or tagged set of servers. `w=<integer>` always includes the replica
            set primary (e.g. w=3 means write to the primary and wait until
            replicated to **two** secondaries). **w=0 disables acknowledgement
            of write operations and can not be used with other write concern
            options.**
          - `wtimeout`: (integer) Used in conjunction with `w`. Specify a value
            in milliseconds to control how long to wait for write propagation
            to complete. If replication does not complete in the given
            timeframe, a timeout exception is raised.
          - `j`: If ``True`` block until write operations have been committed
            to the journal. Cannot be used in combination with `fsync`. Prior
            to MongoDB 2.6 this option was ignored if the server was running
            without journaling. Starting with MongoDB 2.6 write operations will
            fail with an exception if this option is used when the server is
            running without journaling.
          - `fsync`: If ``True`` and the server is running without journaling,
            blocks until the server has synced all data files to disk. If the
            server is running with journaling, this acts the same as the `j`
            option, blocking until write operations have been committed to the
            journal. Cannot be used in combination with `j`.
        :Returns:
          - A document (dict) describing the effect of the remove or ``None``
            if write acknowledgement is disabled.

        .. versionchanged:: 3.0
           Removed the `safe` parameter

        .. mongodoc:: remove
        """
        if spec_or_id is None:
            spec_or_id = {}
        if not isinstance(spec_or_id, collections.Mapping):
            spec_or_id = {"_id": spec_or_id}

        concern = kwargs or self.write_concern
        safe = concern.get("w") != 0

        client = self.database.connection
        if client._writable_max_wire_version() > 1 and safe:
            # Delete command
            command = SON([('delete', self.name)])
            if concern:
                command['writeConcern'] = concern

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
                               concern, self.uuid_subtype, int(not multi)), safe)

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
        """
        if (spec_or_id is not None and not
                isinstance(spec_or_id, collections.Mapping)):
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
          - `await_data` (optional): if True, the server will block for
            some extra time before returning, waiting for more data to
            return. Ignored if `tailable` is False.
          - `partial` (optional): if True, mongos will return partial
            results if some shards are down instead of returning an error.
          - `manipulate`: (optional): If True (the default), apply any
            outgoing SON manipulators before returning.
          - `read_preference` (optional): The read preference for
            this query.
          - `tag_sets` **DEPRECATED**
          - `secondary_acceptable_latency_ms` **DEPRECATED**
          - `exhaust` (optional): If ``True`` create an "exhaust" cursor.
            MongoDB will stream batched results to the client without waiting
            for the client to request each batch, reducing latency.

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

        .. note:: The `manipulate` parameter may default to False in a future
           release.

        .. versionchanged:: 3.0
           Removed the `network_timeout` parameter.
           Deprecated the `tag_sets`, and
           `secondary_acceptable_latency_ms` parameters.
           Removed `compile_re` option: PyMongo now always represents BSON
           regular expressions as :class:`~bson.regex.Regex` objects. Use
           :meth:`~bson.regex.Regex.try_compile` to attempt to convert from a
           BSON regular expression to a Python regular expression object.

        .. versionchanged:: 2.7
           Added `compile_re` option. If set to False, PyMongo represented BSON
           regular expressions as :class:`~bson.regex.Regex` objects instead of
           attempting to compile BSON regular expressions as Python native
           regular expressions, thus preventing errors for some incompatible
           patterns, see `PYTHON-500`_.

        .. versionadded:: 2.3
           The `tag_sets` and `secondary_acceptable_latency_ms` parameters.

        .. _PYTHON-500: https://jira.mongodb.org/browse/PYTHON-500

        .. mongodoc:: find
        """
        return Cursor(self, *args, **kwargs)

    def parallel_scan(self, num_cursors, read_preference=None, **kwargs):
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

        With :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`,
        if the `read_preference` attribute of this instance is not set to
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY` or
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY_PREFERRED`
        the command will be sent to a secondary.

        :Parameters:
          - `num_cursors`: the number of cursors to return
          - `read_preference`: the read preference to use for this scan

        .. note:: Requires server version **>= 2.5.5**.

        """
        cmd = SON([('parallelCollectionScan', self.__name),
                   ('numCursors', num_cursors)])
        cmd.update(kwargs)

        result, address = self._command(cmd, read_preference)

        return [CommandCursor(self,
                              cursor['cursor'],
                              address) for cursor in result['cursors']]

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
        (:class:`str` in python 3), and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`, :data:`~pymongo.GEOHAYSTACK`,
        :data:`~pymongo.GEOSPHERE`, :data:`~pymongo.HASHED`,
        :data:`~pymongo.TEXT`).

        To create a single key ascending index on the key ``'mike'`` we just
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
          - `unique`: if ``True`` creates a uniqueness constraint on the index
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

        .. seealso:: :meth:`ensure_index`

        .. mongodoc:: indexes
        """

        if 'ttl' in kwargs:
            cache_for = kwargs.pop('ttl')
            warnings.warn("ttl is deprecated. Please use cache_for instead.",
                          DeprecationWarning, stacklevel=2)

        # The types supported by datetime.timedelta.
        if not (isinstance(cache_for, integer_types) or
                isinstance(cache_for, float)):
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

        cmd = SON([('createIndexes', self.name), ('indexes', [index])])

        try:
            self._command(cmd, ReadPreference.PRIMARY)
        except OperationFailure as exc:
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
        (:class:`str` in python 3), and the direction(s) must be one of
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
          - `unique`: if ``True`` creates a uniqueness constraint on the index
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
        self.drop_index("*")

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

        if not isinstance(name, string_type):
            raise TypeError("index_or_name must be an index name or list")

        self.__database.connection._purge_index(self.__database.name,
                                                self.__name, name)
        cmd = SON([("dropIndexes", self.__name), ("index", name)])
        self._command(
            cmd, ReadPreference.PRIMARY, allowable_errors=["ns not found"])

    def reindex(self):
        """Rebuilds all indexes on this collection.

        .. warning:: reindex blocks all other operations (indexes
           are built in the foreground) and will be slow for large
           collections.
        """
        cmd = SON([("reIndex", self.__name)])
        return self._command(cmd, ReadPreference.PRIMARY)[0]

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
        """
        client = self.__database.connection
        if client._writable_max_wire_version() > 2:
            cmd = SON([("listIndexes", self.__name)])
            result = self._command(cmd,
                                   ReadPreference.PRIMARY,
                                   CodecOptions(as_class=SON))[0]
            raw = result.get("indexes", [])
        else:
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
        client = self.__database.connection

        result = None
        if client._writable_max_wire_version() > 2:
            cmd = SON([("listCollections", self.__name),
                       ("filter", {"name": self.__name})])
            res = self._command(cmd, ReadPreference.PRIMARY)[0]
            for doc in res.get("collections", []):
                result = doc
                break
        else:
            result = self.__database.system.namespaces.find_one(
                {"name": self.__full_name})

        if not result:
            return {}

        options = result.get("options", {})
        if "create" in options:
            del options["create"]

        return options

    def aggregate(self, pipeline, read_preference=None, **kwargs):
        """Perform an aggregation using the aggregation framework on this
        collection.

        With :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`,
        if the `read_preference` attribute of this instance is not set to
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY` or
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY_PREFERRED`
        the command will be sent to a secondary.

        :Parameters:
          - `pipeline`: a single command or list of aggregation commands
          - `read_preference`: read preference to use for this aggregate
          - `**kwargs`: send arbitrary parameters to the aggregate command

        .. note:: Requires server version **>= 2.1.0**.

        With server version **>= 2.5.1**, pass
        ``cursor={}`` to retrieve unlimited aggregation results
        with a :class:`~pymongo.command_cursor.CommandCursor`::

            pipeline = [{'$project': {'name': {'$toUpper': '$name'}}}]
            cursor = collection.aggregate(pipeline, cursor={})
            for doc in cursor:
                print doc

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
        if not isinstance(pipeline, (collections.Mapping, list, tuple)):
            raise TypeError("pipeline must be a dict, list or tuple")

        if isinstance(pipeline, collections.Mapping):
            pipeline = [pipeline]

        cmd = SON([("aggregate", self.__name),
                   ("pipeline", pipeline)])
        cmd.update(kwargs)

        # XXX: Keep doing this automatically?
        for stage in pipeline:
            if '$out' in stage:
                read_preference = ReadPreference.PRIMARY
                break

        result, address = self._command(cmd, read_preference)

        if 'cursor' in result:
            return CommandCursor(
                self,
                result['cursor'],
                address)
        else:
            return result

    # TODO key and condition ought to be optional, but deprecation
    # could be painful as argument order would have to change.
    def group(self, key, condition, initial,
              reduce, finalize=None, read_preference=None, **kwargs):
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

        With :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`,
        if the `read_preference` attribute of this instance is not set to
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY` or
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY_PREFERRED`
        the command will be sent to a secondary.

        :Parameters:
          - `key`: fields to group by (see above description)
          - `condition`: specification of rows to be
            considered (as a :meth:`find` query specification)
          - `initial`: initial value of the aggregation counter object
          - `reduce`: aggregation function as a JavaScript string
          - `finalize`: function to be called on each object in output list.
          - `read_preference`: read preference to use for this group

        .. versionchanged:: 2.2
           Removed deprecated argument: command
        """
        group = {}
        if isinstance(key, string_type):
            group["$keyf"] = Code(key)
        elif key is not None:
            group = {"key": helpers._fields_list_to_dict(key)}
        group["ns"] = self.__name
        group["$reduce"] = Code(reduce)
        group["cond"] = condition
        group["initial"] = initial
        if finalize is not None:
            group["finalize"] = Code(finalize)

        cmd = SON([("group", group)])
        cmd.update(kwargs)

        return self._command(cmd, read_preference)[0]["retval"]

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
        """
        if not isinstance(new_name, string_type):
            raise TypeError("new_name must be an "
                            "instance of %s" % (string_type.__name__,))

        if not new_name or ".." in new_name:
            raise InvalidName("collection names cannot be empty")
        if new_name[0] == "." or new_name[-1] == ".":
            raise InvalidName("collecion names must not start or end with '.'")
        if "$" in new_name and not new_name.startswith("oplog.$main"):
            raise InvalidName("collection names must not contain '$'")

        new_name = "%s.%s" % (self.__database.name, new_name)
        cmd = SON([("renameCollection", self.__full_name), ("to", new_name)])
        cmd.update(kwargs)
        _command(self.__database.connection, "admin.$cmd", cmd,
                 ReadPreference.PRIMARY, CodecOptions())

    def distinct(self, key):
        """Get a list of distinct values for `key` among all documents
        in this collection.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring` (:class:`str` in python 3).

        To get the distinct values for a key in the result set of a
        query use :meth:`~pymongo.cursor.Cursor.distinct`.

        :Parameters:
          - `key`: name of key for which we want to get the distinct values
        """
        return self.find().distinct(key)

    def map_reduce(self, map, reduce, out,
                   full_response=False, read_preference=None, **kwargs):
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
          - `read_preference`: read preference to use for this map reduce
          - `**kwargs` (optional): additional arguments to the
            `map reduce command`_ may be passed as keyword arguments to this
            helper method, e.g.::

            >>> db.test.map_reduce(map, reduce, "myresults", limit=2)

        .. seealso:: :doc:`/examples/aggregation`

        .. versionchanged:: 2.2
           Removed deprecated arguments: merge_output and reduce_output

        .. _map reduce command: http://www.mongodb.org/display/DOCS/MapReduce

        .. mongodoc:: mapreduce
        """
        if not isinstance(out, (string_type, collections.Mapping)):
            raise TypeError("'out' must be an instance of "
                            "%s or a mapping" % (string_type.__name__,))

        cmd = SON([("mapreduce", self.__name),
                   ("map", map),
                   ("reduce", reduce),
                   ("out", out)])
        cmd.update(kwargs)

        # XXX: Keep doing this automatically?
        if not isinstance(out, collections.Mapping) or not out.get('inline'):
            read_preference = ReadPreference.PRIMARY

        response = self._command(cmd, read_preference)[0]

        if full_response or not response.get('result'):
            return response
        elif isinstance(response['result'], dict):
            dbase = response['result']['db']
            coll = response['result']['collection']
            return self.__database.connection[dbase][coll]
        else:
            return self.__database[response["result"]]

    def inline_map_reduce(self, map, reduce,
                          full_response=False, read_preference=None, **kwargs):
        """Perform an inline map/reduce operation on this collection.

        Perform the map/reduce operation on the server in RAM. A result
        collection is not created. The result set is returned as a list
        of documents.

        If `full_response` is ``False`` (default) returns the
        result documents in a list. Otherwise, returns the full
        response from the server to the `map reduce command`_.

        With :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`,
        if the `read_preference` attribute of this instance is not set to
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY` or
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY_PREFERRED`
        the command will be sent to a secondary.

        :Parameters:
          - `map`: map function (as a JavaScript string)
          - `reduce`: reduce function (as a JavaScript string)
          - `full_response` (optional): if ``True``, return full response to
            this command - otherwise just return the result collection
          - `read_preference`: read preference to use for this map reduce
          - `**kwargs` (optional): additional arguments to the
            `map reduce command`_ may be passed as keyword arguments to this
            helper method, e.g.::

            >>> db.test.inline_map_reduce(map, reduce, limit=2)
        """

        cmd = SON([("mapreduce", self.__name),
                   ("map", map),
                   ("reduce", reduce),
                   ("out", {"inline": 1})])
        cmd.update(kwargs)
        res = self._command(cmd, read_preference)[0]

        if full_response:
            return res
        else:
            return res.get("results")

    def find_and_modify(self, query={}, update=None,
                        upsert=False, sort=None, full_response=False,
                        manipulate=False, **kwargs):
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
            - `fields`: see second argument to :meth:`find` (default all)
            - `manipulate`: (optional): If ``True``, apply any outgoing SON
              manipulators before returning. Ignored when `full_response`
              is set to True. Defaults to ``False``.
            - `**kwargs`: any other options the findAndModify_ command
              supports can be passed here.


        .. mongodoc:: findAndModify

        .. _findAndModify: http://dochub.mongodb.org/core/findAndModify

        .. versionchanged:: 2.8
           Added the optional manipulate parameter

        .. versionchanged:: 2.5
           Added the optional full_response parameter

        .. versionchanged:: 2.4
           Deprecated the use of mapping types for the sort parameter
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

        # XXX: Keep supporting this?
        fields = kwargs.pop("fields", None)
        if (fields is not None and not
                isinstance(fields, collections.Mapping)):
            kwargs["fields"] = helpers._fields_list_to_dict(fields)

        cmd = SON([("findAndModify", self.__name)])
        cmd.update(kwargs)
        out = self._command(cmd,
                            ReadPreference.PRIMARY,
                            allowable_errors=[no_obj_error])[0]

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

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Collection' object is not iterable")

    __next__ = next

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
