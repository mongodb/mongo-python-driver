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

import collections
import warnings

from bson.code import Code
from bson.objectid import ObjectId
from bson.py3compat import (_unicode,
                            integer_types,
                            string_type,
                            u)
from bson.codec_options import CodecOptions
from bson.son import SON
from pymongo import (common,
                     helpers,
                     message)
from pymongo.bulk import BulkOperationBuilder, _Bulk
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.errors import ConfigurationError, InvalidName, OperationFailure
from pymongo.helpers import _check_write_command_response
from pymongo.message import _INSERT, _UPDATE, _DELETE
from pymongo.operations import _WriteOp, IndexModel
from pymongo.read_preferences import ReadPreference
from pymongo.results import (BulkWriteResult,
                             DeleteResult,
                             InsertOneResult,
                             InsertManyResult,
                             UpdateResult)
from pymongo.write_concern import WriteConcern

try:
    from collections import OrderedDict
    _ORDERED_TYPES = (SON, OrderedDict)
except ImportError:
    _ORDERED_TYPES = (SON,)

_NO_OBJ_ERROR = "No matching object found"
_UJOIN = u("%s.%s")


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

        .. versionchanged:: 3.0
           Added the codec_options, read_preference, and write_concern options.
           Removed the uuid_subtype attribute.
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
        self.__full_name = _UJOIN % (self.__database.name, self.__name)
        if create or kwargs:
            self.__create(kwargs)

    def _socket_for_reads(self):
        return self.__database.client._socket_for_reads(self.read_preference)

    def _socket_for_primary_reads(self):
        return self.__database.client._socket_for_reads(ReadPreference.PRIMARY)

    def _socket_for_writes(self):
        return self.__database.client._socket_for_writes()

    def _command(self, sock_info, command, slave_ok=False,
                 read_preference=None,
                 codec_options=None, check=True, allowable_errors=None):
        """Internal command helper.

        :Parameters:
          - `sock_info` - A SocketInfo instance.
          - `command` - The command itself, as a SON instance.
          - `slave_ok`: whether to set the SlaveOkay wire protocol bit.
          - `codec_options` (optional) - An instance of
            :class:`~bson.codec_options.CodecOptions`.
          - `check`: raise OperationFailure if there are errors
          - `allowable_errors`: errors to ignore if `check` is True

        :Returns:

            # todo: don't return address

          (result document, address of server the command was run on)
        """
        return sock_info.command(self.__database.name,
                                 command,
                                 slave_ok,
                                 read_preference or self.read_preference,
                                 codec_options or self.codec_options,
                                 check,
                                 allowable_errors)

    def __create(self, options):
        """Sends a create command with the given options.
        """
        cmd = SON([("create", self.__name)])
        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            cmd.update(options)
        with self._socket_for_writes() as sock_info:
            self._command(
                sock_info, cmd, read_preference=ReadPreference.PRIMARY)

    def __getattr__(self, name):
        """Get a sub-collection of this collection by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        if name.startswith('_'):
            full_name = _UJOIN % (self.__name, name)
            raise AttributeError(
                "Collection has no attribute %r. To access the %s"
                " collection, use database['%s']." % (
                    name, full_name, full_name))
        return self.__getitem__(name)

    def __getitem__(self, name):
        return Collection(self.__database, _UJOIN % (self.__name, name))

    def __repr__(self):
        return "Collection(%r, %r)" % (self.__database, self.__name)

    def __eq__(self, other):
        if isinstance(other, Collection):
            return (self.__database == other.database and
                    self.__name == other.name)
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

    def with_options(
            self, codec_options=None, read_preference=None, write_concern=None):
        """Get a clone of this collection changing the specified settings.

          >>> coll1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> coll2 = coll1.with_options(read_preference=ReadPreference.SECONDARY)
          >>> coll1.read_preference
          Primary()
          >>> coll2.read_preference
          Secondary(tag_sets=None)

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
        """
        return Collection(self.__database,
                          self.__name,
                          False,
                          codec_options or self.codec_options,
                          read_preference or self.read_preference,
                          write_concern or self.write_concern)

    def initialize_unordered_bulk_op(self):
        """Initialize an unordered batch of write operations.

        Operations will be performed on the server in arbitrary order,
        possibly in parallel. All operations will be attempted.

        Returns a :class:`~pymongo.bulk.BulkOperationBuilder` instance.

        See :ref:`unordered_bulk` for examples.

        .. versionadded:: 2.7
        """
        return BulkOperationBuilder(self, ordered=False)

    def initialize_ordered_bulk_op(self):
        """Initialize an ordered batch of write operations.

        Operations will be performed on the server serially, in the
        order provided. If an error occurs all remaining operations
        are aborted.

        Returns a :class:`~pymongo.bulk.BulkOperationBuilder` instance.

        See :ref:`ordered_bulk` for examples.

        .. versionadded:: 2.7
        """
        return BulkOperationBuilder(self, ordered=True)

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

        .. versionadded:: 3.0
        """
        if not isinstance(requests, list):
            raise TypeError("requests must be a list")

        blk = _Bulk(self, ordered)
        for request in requests:
            if not isinstance(request, _WriteOp):
                raise TypeError("%r is not a valid request" % (request,))
            request._add_to_bulk(blk)

        bulk_api_result = blk.execute(self.write_concern.document)
        if bulk_api_result is not None:
            return BulkWriteResult(bulk_api_result, True)
        return BulkWriteResult({}, False)

    def _insert(self, sock_info, docs, ordered=True,
                check_keys=True, manipulate=False, write_concern=None):
        """Internal insert helper."""
        return_one = False
        if isinstance(docs, collections.MutableMapping):
            return_one = True
            docs = [docs]

        ids = []

        if manipulate:
            def gen():
                """Generator that applies SON manipulators to each document
                and adds _id if necessary.
                """
                _db = self.__database
                for doc in docs:
                    # Apply user-configured SON manipulators. This order of
                    # operations is required for backwards compatibility,
                    # see PYTHON-709.
                    doc = _db._apply_incoming_manipulators(doc, self)
                    if '_id' not in doc:
                        doc['_id'] = ObjectId()

                    doc = _db._apply_incoming_copying_manipulators(doc, self)
                    ids.append(doc['_id'])
                    yield doc
        else:
            def gen():
                """Generator that only tracks existing _ids."""
                for doc in docs:
                    ids.append(doc.get('_id'))
                    yield doc

        concern = (write_concern or self.write_concern).document
        safe = concern.get("w") != 0

        if sock_info.max_wire_version > 1 and safe:
            # Insert command.
            command = SON([('insert', self.name),
                           ('ordered', ordered)])

            if concern:
                command['writeConcern'] = concern

            results = message._do_batched_write_command(
                self.database.name + ".$cmd", _INSERT, command,
                gen(), check_keys, self.codec_options, sock_info)
            _check_write_command_response(results)
        else:
            # Legacy batched OP_INSERT.
            message._do_batched_insert(self.__full_name, gen(), check_keys,
                                       safe, concern, not ordered,
                                       self.codec_options, sock_info)
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
          - `document`: The document to insert. Must be a mutable mapping
            type. If the document does not have an _id field one will be
            added automatically.

        :Returns:
          - An instance of :class:`~pymongo.results.InsertOneResult`.

        .. versionadded:: 3.0
        """
        common.validate_is_mutable_mapping("document", document)
        if "_id" not in document:
            document["_id"] = ObjectId()
        with self._socket_for_writes() as sock_info:
            return InsertOneResult(self._insert(sock_info, document),
                                   self.write_concern.acknowledged)

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

        .. versionadded:: 3.0
        """
        if not isinstance(documents, list) or not documents:
            raise TypeError("documents must be a non-empty list")
        inserted_ids = []
        def gen():
            """A generator that validates documents and handles _ids."""
            for document in documents:
                common.validate_is_mutable_mapping("document", document)
                if "_id" not in document:
                    document["_id"] = ObjectId()
                inserted_ids.append(document["_id"])
                yield (_INSERT, document)

        blk = _Bulk(self, ordered)
        blk.ops = [doc for doc in gen()]
        blk.execute(self.write_concern.document)
        return InsertManyResult(inserted_ids, self.write_concern.acknowledged)

    def _update(self, sock_info, filter, document, upsert=False,
                check_keys=True, multi=False, manipulate=False,
                write_concern=None):
        """Internal update / replace helper."""
        common.validate_is_mapping("filter", filter)
        common.validate_boolean("upsert", upsert)
        if manipulate:
            document = self.__database._fix_incoming(document, self)

        concern = (write_concern or self.write_concern).document
        safe = concern.get("w") != 0

        if sock_info.max_wire_version > 1 and safe:
            # Update command.
            command = SON([('update', self.name)])
            if concern:
                command['writeConcern'] = concern

            docs = [SON([('q', filter), ('u', document),
                         ('multi', multi), ('upsert', upsert)])]

            results = message._do_batched_write_command(
                self.database.name + '.$cmd', _UPDATE, command,
                docs, check_keys, self.codec_options, sock_info)
            _check_write_command_response(results)

            _, result = results[0]
            # Add the updatedExisting field for compatibility.
            if result.get('n') and 'upserted' not in result:
                result['updatedExisting'] = True
            else:
                result['updatedExisting'] = False
                # MongoDB >= 2.6.0 returns the upsert _id in an array
                # element. Break it out for backward compatibility.
                if 'upserted' in result:
                    result['upserted'] = result['upserted'][0]['_id']

            return result

        else:
            # Legacy OP_UPDATE.
            request_id, msg, max_size = message.update(
                self.__full_name, upsert, multi, filter, document, safe,
                concern, check_keys, self.codec_options)
            return sock_info.legacy_write(request_id, msg, max_size, safe)

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

        .. versionadded:: 3.0
        """
        common.validate_ok_for_replace(replacement)
        with self._socket_for_writes() as sock_info:
            result = self._update(sock_info, filter, replacement, upsert)
        return UpdateResult(result, self.write_concern.acknowledged)

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

        .. versionadded:: 3.0
        """
        common.validate_ok_for_update(update)
        with self._socket_for_writes() as sock_info:
            result = self._update(sock_info, filter, update,
                                  upsert, check_keys=False)
        return UpdateResult(result, self.write_concern.acknowledged)

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

        .. versionadded:: 3.0
        """
        common.validate_ok_for_update(update)
        with self._socket_for_writes() as sock_info:
            result = self._update(sock_info, filter, update, upsert,
                                  check_keys=False, multi=True)
        return UpdateResult(result, self.write_concern.acknowledged)

    def drop(self):
        """Alias for :meth:`~pymongo.database.Database.drop_collection`.

        The following two calls are equivalent:

          >>> db.foo.drop()
          >>> db.drop_collection("foo")
        """
        self.__database.drop_collection(self.__name)

    def _delete(self, sock_info, filter, multi, write_concern=None):
        """Internal delete helper."""
        common.validate_is_mapping("filter", filter)
        concern = (write_concern or self.write_concern).document
        safe = concern.get("w") != 0

        if sock_info.max_wire_version > 1 and safe:
            # Delete command.
            command = SON([('delete', self.name)])
            if concern:
                command['writeConcern'] = concern

            docs = [SON([('q', filter), ('limit', int(not multi))])]

            results = message._do_batched_write_command(
                self.database.name + '.$cmd', _DELETE, command,
                docs, False, self.codec_options, sock_info)
            _check_write_command_response(results)

            _, result = results[0]
            return result

        else:
            # Legacy OP_DELETE.
            request_id, msg, max_size = message.delete(
                self.__full_name, filter, safe, concern,
                self.codec_options, int(not multi))
            return sock_info.legacy_write(request_id, msg, max_size, safe)

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

        .. versionadded:: 3.0
        """
        with self._socket_for_writes() as sock_info:
            return DeleteResult(self._delete(sock_info, filter, False),
                                self.write_concern.acknowledged)

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

        .. versionadded:: 3.0
        """
        with self._socket_for_writes() as sock_info:
            return DeleteResult(self._delete(sock_info, filter, True),
                                self.write_concern.acknowledged)

    def find_one(self, filter=None, *args, **kwargs):
        """Get a single document from the database.

        All arguments to :meth:`find` are also valid arguments for
        :meth:`find_one`, although any `limit` argument will be
        ignored. Returns a single document, or ``None`` if no matching
        document is found.

        The :meth:`find_one` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:

          - `filter` (optional): a dictionary specifying
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
        if (filter is not None and not
                isinstance(filter, collections.Mapping)):
            filter = {"_id": filter}

        max_time_ms = kwargs.pop("max_time_ms", None)
        cursor = self.find(filter,
                           *args, **kwargs).max_time_ms(max_time_ms)

        for result in cursor.limit(-1):
            return result
        return None

    def find(self, *args, **kwargs):
        """Query the database.

        The `filter` argument is a prototype document that all results
        must match. For example:

        >>> db.test.find({"hello": "world"})

        only matches documents that have a key "hello" with value
        "world".  Matches can have other keys *in addition* to
        "hello". The `projection` argument is used to specify a subset
        of fields that should be included in the result documents. By
        limiting results to a certain subset of fields you can cut
        down on network traffic and decoding time.

        Raises :class:`TypeError` if any of the arguments are of
        improper type. Returns an instance of
        :class:`~pymongo.cursor.Cursor` corresponding to this query.

        The :meth:`find` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `filter` (optional): a SON object specifying elements which
            must be present for a document to be included in the
            result set
          - `projection` (optional): a list of field names that should be
            returned in the result set or a dict specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a dict to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `skip` (optional): the number of documents to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return
          - `no_cursor_timeout` (optional): if False (the default), any
            returned cursor is closed by the server after 10 minutes of
            inactivity. If set to True, the returned cursor will never
            time out on the server. Care should be taken to ensure that
            cursors with no_cursor_timeout turned on are properly closed.
          - `cursor_type` (optional): the type of cursor to return. The valid
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

          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~pymongo.cursor.Cursor.sort` for details.
          - `allow_partial_results` (optional): if True, mongos will return
            partial results if some shards are down instead of returning an
            error.
          - `oplog_replay` (optional): If True, set the oplogReplay query
            flag.
          - `modifiers` (optional): A dict specifying the MongoDB `query
            modifiers`_ that should be used for this query. For example::

              >>> db.test.find(modifiers={"$maxTimeMS": 500})

          - `batch_size` (optional): Limits the number of documents returned in
            a single batch.
          - `manipulate` (optional): **DEPRECATED** - If True (the default),
            apply any outgoing SON manipulators before returning.

        .. note:: There are a number of caveats to using
          :attr:`~pymongo.cursor.CursorType.EXHAUST` as cursor_type:

          - The `limit` option can not be used with an exhaust cursor.

          - Exhaust cursors are not supported by mongos and can not be
            used with a sharded cluster.

          - A :class:`~pymongo.cursor.Cursor` instance created with the
            :attr:`~pymongo.cursor.CursorType.EXHAUST` cursor_type requires an
            exclusive :class:`~socket.socket` connection to MongoDB. If the
            :class:`~pymongo.cursor.Cursor` is discarded without being
            completely iterated the underlying :class:`~socket.socket`
            connection will be closed and discarded without being returned to
            the connection pool.

        .. versionchanged:: 3.0
           Changed the parameter names `spec`, `fields`, `timeout`, and
           `partial` to `filter`, `projection`, `no_cursor_timeout`, and
           `allow_partial_results` respectively.
           Added the `cursor_type`, `oplog_replay`, and `modifiers` options.
           Removed the `network_timeout`, `read_preference`, `tag_sets`,
           `secondary_acceptable_latency_ms`, `max_scan`, `snapshot`,
           `tailable`, `await_data`, `exhaust`, `as_class`, and slave_okay
           parameters. Removed `compile_re` option: PyMongo now always
           represents BSON regular expressions as :class:`~bson.regex.Regex`
           objects. Use :meth:`~bson.regex.Regex.try_compile` to attempt to
           convert from a BSON regular expression to a Python regular
           expression object. Soft deprecated the `manipulate` option.

        .. versionchanged:: 2.7
           Added `compile_re` option. If set to False, PyMongo represented BSON
           regular expressions as :class:`~bson.regex.Regex` objects instead of
           attempting to compile BSON regular expressions as Python native
           regular expressions, thus preventing errors for some incompatible
           patterns, see `PYTHON-500`_.

        .. versionadded:: 2.3
           The `tag_sets` and `secondary_acceptable_latency_ms` parameters.

        .. _PYTHON-500: https://jira.mongodb.org/browse/PYTHON-500
        .. _query modifiers:
          http://docs.mongodb.org/manual/reference/operator/query-modifier/

        .. mongodoc:: find
        """
        return Cursor(self, *args, **kwargs)

    def parallel_scan(self, num_cursors):
        """Scan this entire collection in parallel.

        Returns a list of up to ``num_cursors`` cursors that can be iterated
        concurrently. As long as the collection is not modified during
        scanning, each document appears once in one of the cursors result
        sets.

        For example, to process each document in a collection using some
        thread-safe ``process_document()`` function:

          >>> def process_cursor(cursor):
          ...     for document in cursor:
          ...     # Some thread-safe processing function:
          ...     process_document(document)
          >>>
          >>> # Get up to 4 cursors.
          ...
          >>> cursors = collection.parallel_scan(4)
          >>> threads = [
          ...     threading.Thread(target=process_cursor, args=(cursor,))
          ...     for cursor in cursors]
          >>>
          >>> for thread in threads:
          ...     thread.start()
          >>>
          >>> for thread in threads:
          ...     thread.join()
          >>>
          >>> # All documents have now been processed.

        The :meth:`parallel_scan` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `num_cursors`: the number of cursors to return

        .. note:: Requires server version **>= 2.5.5**.

        .. versionchanged:: 3.0
           Removed support for arbitrary keyword arguments, since
           the parallelCollectionScan command has no optional arguments.
        """
        cmd = SON([('parallelCollectionScan', self.__name),
                   ('numCursors', num_cursors)])

        with self._socket_for_reads() as (sock_info, slave_ok):
            result = self._command(sock_info, cmd, slave_ok)

        return [CommandCursor(self, cursor['cursor'], sock_info.address)
                for cursor in result['cursors']]

    def _count(self, cmd):
        """Internal count helper."""
        with self._socket_for_reads() as (sock_info, slave_ok):
            res = self._command(sock_info, cmd, slave_ok,
                                allowable_errors=["ns missing"])
        if res.get("errmsg", "") == "ns missing":
            return 0
        return int(res["n"])

    def count(self, filter=None, **kwargs):
        """Get the number of documents in this collection.

        All optional count parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `hint` (string or list of tuples): The index to use. Specify either
            the index name as a string or the index specification as a list of
            tuples (e.g. [('a', pymongo.ASCENDING), ('b', pymongo.ASCENDING)]).
          - `limit` (int): The maximum number of documents to count.
          - `skip` (int): The number of matching documents to skip before
            returning results.
          - `maxTimeMS` (int): The maximum amount of time to allow the count
            command to run, in milliseconds.

        The :meth:`count` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `filter` (optional): A query document that selects which documents
            to count in the collection.
          - `**kwargs` (optional): See list of options above.
        """
        cmd = SON([("count", self.__name)])
        if filter is not None:
            if "query" in kwargs:
                raise ConfigurationError("can't pass both filter and query")
            kwargs["query"] = filter
        if "hint" in kwargs and not isinstance(kwargs["hint"], string_type):
            kwargs["hint"] = helpers._index_document(kwargs["hint"])
        cmd.update(kwargs)
        return self._count(cmd)

    def create_indexes(self, indexes):
        """Create one or more indexes on this collection.

          >>> from pymongo import IndexModel, ASCENDING, DESCENDING
          >>> index1 = IndexModel([("hello", DESCENDING),
          ...                      ("world", ASCENDING)], name="hello_world")
          >>> index2 = IndexModel([("goodbye", DESCENDING)])
          >>> db.test.create_indexes([index1, index2])
          ["hello_world"]

        :Parameters:
          - `indexes`: A list of :class:`~pymongo.operations.IndexModel`
            instances.

        .. note:: `create_indexes` uses the ``createIndexes`` command
           introduced in MongoDB **2.6** and cannot be used with earlier
           versions.

        .. versionadded:: 3.0
        """
        if not isinstance(indexes, list):
            raise TypeError("indexes must be a list")
        names = []
        def gen_indexes():
            for index in indexes:
                if not isinstance(index, IndexModel):
                    raise TypeError("%r is not an instance of "
                                    "pymongo.operations.IndexModel" % (index,))
                document = index.document
                names.append(document["name"])
                yield document
        cmd = SON([('createIndexes', self.name),
                   ('indexes', list(gen_indexes()))])
        with self._socket_for_writes() as sock_info:
            self._command(
                sock_info, cmd, read_preference=ReadPreference.PRIMARY)
        return names

    def __create_index(self, keys, index_options):
        """Internal create index helper.

        :Parameters:
          - `keys`: a list of tuples [(key, type), (key, type), ...]
          - `index_options`: a dict of index options.
        """
        index_doc = helpers._index_document(keys)
        index = {"key": index_doc}
        index.update(index_options)

        with self._socket_for_writes() as sock_info:
            cmd = SON([('createIndexes', self.name), ('indexes', [index])])
            try:
                self._command(
                    sock_info, cmd, read_preference=ReadPreference.PRIMARY)
            except OperationFailure as exc:
                if exc.code in common.COMMAND_NOT_FOUND_CODES:
                    index["ns"] = self.__full_name
                    wcn = (self.write_concern if
                           self.write_concern.acknowledged else WriteConcern())
                    self.__database.system.indexes._insert(
                        sock_info, index, True, False, False, wcn)
                else:
                    raise

    def create_index(self, keys, **kwargs):
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
            given, a name will be generated.
          - `unique`: if ``True`` creates a uniqueness constraint on the index.
          - `background`: if ``True`` this index should be created in the
            background.
          - `sparse`: if ``True``, omit from the index any documents that lack
            the indexed field.
          - `bucketSize`: for use with geoHaystack indexes.
            Number of documents to group together within a certain proximity
            to a given longitude and latitude.
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `expireAfterSeconds`: <int> Used to create an expiring (TTL)
            collection. MongoDB will automatically delete documents from
            this collection after <int> seconds. The indexed field must
            be a UTC datetime or the data will not expire.

        See the MongoDB documentation for a full list of supported options by
        server version.

        .. warning:: `dropDups` is not supported by MongoDB 2.7.5 or newer. The
          option is silently ignored by the server and unique index builds
          using the option will fail if a duplicate value is detected.

        .. note:: `expireAfterSeconds` requires server version **>= 2.2**

        :Parameters:
          - `keys`: a single key or a list of (key, direction)
            pairs specifying the index to create
          - `**kwargs` (optional): any additional index creation
            options (see the above list) should be passed as keyword
            arguments

        .. versionchanged:: 3.0
            Renamed `key_or_list` to `keys`. Removed the `cache_for` option.
            :meth:`create_index` no longer caches index names. Removed support
            for the drop_dups and bucket_size aliases.

        .. mongodoc:: indexes
        """
        keys = helpers._index_list(keys)
        name = kwargs.setdefault("name", helpers._gen_index_name(keys))
        self.__create_index(keys, kwargs)
        return name

    def ensure_index(self, key_or_list, cache_for=300, **kwargs):
        """**DEPRECATED** - Ensures that an index exists on this collection.

        .. versionchanged:: 3.0
            **DEPRECATED**
        """
        warnings.warn("ensure_index is deprecated. Use create_index instead.",
                      DeprecationWarning, stacklevel=2)
        # The types supported by datetime.timedelta.
        if not (isinstance(cache_for, integer_types) or
                isinstance(cache_for, float)):
            raise TypeError("cache_for must be an integer or float.")

        if "drop_dups" in kwargs:
            kwargs["dropDups"] = kwargs.pop("drop_dups")

        if "bucket_size" in kwargs:
            kwargs["bucketSize"] = kwargs.pop("bucket_size")

        keys = helpers._index_list(key_or_list)
        name = kwargs.setdefault("name", helpers._gen_index_name(keys))

        if not self.__database.client._cached(self.__database.name,
                                              self.__name, name):
            self.__create_index(keys, kwargs)
            self.__database.client._cache_index(self.__database.name,
                                                self.__name, name, cache_for)
            return name
        return None

    def drop_indexes(self):
        """Drops all indexes on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error.
        """
        self.__database.client._purge_index(self.__database.name, self.__name)
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
            name = helpers._gen_index_name(index_or_name)

        if not isinstance(name, string_type):
            raise TypeError("index_or_name must be an index name or list")

        self.__database.client._purge_index(
            self.__database.name, self.__name, name)
        cmd = SON([("dropIndexes", self.__name), ("index", name)])
        with self._socket_for_writes() as sock_info:
            self._command(sock_info,
                          cmd,
                          read_preference=ReadPreference.PRIMARY,
                          allowable_errors=["ns not found"])

    def reindex(self):
        """Rebuilds all indexes on this collection.

        .. warning:: reindex blocks all other operations (indexes
           are built in the foreground) and will be slow for large
           collections.
        """
        cmd = SON([("reIndex", self.__name)])
        with self._socket_for_writes() as sock_info:
            return self._command(
                sock_info, cmd, read_preference=ReadPreference.PRIMARY)

    def list_indexes(self):
        """Get a cursor over the index documents for this collection.

          >>> for index in db.test.list_indexes():
          ...     print(index)
          ...
          SON([(u'v', 1), (u'key', SON([(u'_id', 1)])),
               (u'name', u'_id_'), (u'ns', u'test.test')])

        :Returns:
          An instance of :class:`~pymongo.command_cursor.CommandCursor`.

        .. versionadded:: 3.0
        """
        codec_options = CodecOptions(SON)
        coll = self.with_options(codec_options)
        with self._socket_for_primary_reads() as (sock_info, slave_ok):
            if sock_info.max_wire_version > 2:
                cmd = SON([("listIndexes", self.__name), ("cursor", {})])
                cursor = self._command(sock_info, cmd, slave_ok,
                                       ReadPreference.PRIMARY,
                                       codec_options)["cursor"]
                return CommandCursor(coll, cursor, sock_info.address)
            else:
                namespace = _UJOIN % (self.__database.name, "system.indexes")
                res = helpers._first_batch(
                    sock_info, namespace, {"ns": self.__full_name},
                    0, slave_ok, codec_options, ReadPreference.PRIMARY)
                data = res["data"]
                cursor = {
                    "id": res["cursor_id"],
                    "firstBatch": data,
                    "ns": namespace,
                }
                # Note that a collection can only have 64 indexes, so we don't
                # technically have to pass len(data) here. There will never be
                # an OP_GET_MORE call.
                return CommandCursor(
                    coll, cursor, sock_info.address, len(data))

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
        cursor = self.list_indexes()
        info = {}
        for index in cursor:
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
        with self._socket_for_primary_reads() as (sock_info, slave_ok):
            if sock_info.max_wire_version > 2:
                criteria = {"name": self.__name}
            else:
                criteria = {"name": self.__full_name}
            cursor = self.__database._list_collections(sock_info,
                                                       slave_ok,
                                                       criteria)
            result = None
            for doc in cursor:
                result = doc
                break

            if not result:
                return {}

            options = result.get("options", {})
            if "create" in options:
                del options["create"]

            return options

    def aggregate(self, pipeline, **kwargs):
        """Perform an aggregation using the aggregation framework on this
        collection.

        All optional aggregate parameters should be passed as keyword arguments
        to this method. Valid options include, but are not limited to:

          - `allowDiskUse` (bool): Enables writing to temporary files. When set
            to True, aggregation stages can write data to the _tmp subdirectory
            of the --dbpath directory. The default is False.
          - `maxTimeMS` (int): The maximum amount of time to allow the operation
            to run in milliseconds.
          - `batchSize` (int): The maximum number of documents to return per
            batch. Ignored if the connected mongod or mongos does not support
            returning aggregate results using a cursor, or `useCursor` is
            ``False``.
          - `useCursor` (bool): Requests that the `server` provide results
            using a cursor, if possible. Ignored if the connected mongod or
            mongos does not support returning aggregate results using a cursor.
            The default is ``True``. Set this to ``False`` when upgrading a 2.4
            or older sharded cluster to 2.6 or newer (see the warning below).

        The :meth:`aggregate` method obeys the :attr:`read_preference` of this
        :class:`Collection`. Please note that using the ``$out`` pipeline stage
        requires a read preference of
        :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY` (the default).
        The server will raise an error if the ``$out`` pipeline stage is used
        with any other read preference.

        .. warning:: When upgrading a 2.4 or older sharded cluster to 2.6 or
           newer the `useCursor` option **must** be set to ``False``
           until all shards have been upgraded to 2.6 or newer.

        .. note:: This method does not support the 'explain' option. Please
           use :meth:`~pymongo.database.Database.command` instead. An
           example is included in the :ref:`aggregate-examples` documentation.

        :Parameters:
          - `pipeline`: a list of aggregation pipeline stages
          - `**kwargs` (optional): See list of options above.

        :Returns:
          A :class:`~pymongo.command_cursor.CommandCursor` over the result
          set.

        .. versionchanged:: 3.0
           The :meth:`aggregate` method always returns a CommandCursor. The
           pipeline argument must be a list.
        .. versionchanged:: 2.7
           When the cursor option is used, return
           :class:`~pymongo.command_cursor.CommandCursor` instead of
           :class:`~pymongo.cursor.Cursor`.
        .. versionchanged:: 2.6
           Added cursor support.
        .. versionadded:: 2.3

        .. seealso:: :doc:`/examples/aggregation`

        .. _aggregate command:
            http://docs.mongodb.org/manual/applications/aggregation
        """
        if not isinstance(pipeline, list):
            raise TypeError("pipeline must be a list")

        if "explain" in kwargs:
            raise ConfigurationError("The explain option is not supported. "
                                     "Use Database.command instead.")

        cmd = SON([("aggregate", self.__name),
                   ("pipeline", pipeline)])

        # Remove things that are not command options.
        batch_size = common.validate_positive_integer_or_none(
            "batchSize", kwargs.pop("batchSize", None))
        use_cursor = common.validate_boolean(
            "useCursor", kwargs.pop("useCursor", True))
        # If the server does not support the "cursor" option we
        # ignore useCursor and batchSize.
        with self._socket_for_reads() as (sock_info, slave_ok):
            if sock_info.max_wire_version > 0:
                if use_cursor:
                    if "cursor" not in kwargs:
                        kwargs["cursor"] = {}
                    if batch_size is not None:
                        kwargs["cursor"]["batchSize"] = batch_size

            cmd.update(kwargs)

            result = self._command(sock_info, cmd, slave_ok)

            if "cursor" in result:
                cursor = result["cursor"]
            else:
                # Pre-MongoDB 2.6. Fake a cursor.
                cursor = {
                    "id": 0,
                    "firstBatch": result["result"],
                    "ns": self.full_name,
                }
            return CommandCursor(
                self, cursor, sock_info.address).batch_size(batch_size or 0)

    # key and condition ought to be optional, but deprecation
    # would be painful as argument order would have to change.
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

        The :meth:`group` method obeys the :attr:`read_preference` of this
        :class:`Collection`.

        :Parameters:
          - `key`: fields to group by (see above description)
          - `condition`: specification of rows to be
            considered (as a :meth:`find` query specification)
          - `initial`: initial value of the aggregation counter object
          - `reduce`: aggregation function as a JavaScript string
          - `finalize`: function to be called on each object in output list.
          - `**kwargs` (optional): additional arguments to the group command
            may be passed as keyword arguments to this helper method

        .. versionchanged:: 2.2
           Removed deprecated argument: command
        """
        group = {}
        if isinstance(key, string_type):
            group["$keyf"] = Code(key)
        elif key is not None:
            group = {"key": helpers._fields_list_to_dict(key, "key")}
        group["ns"] = self.__name
        group["$reduce"] = Code(reduce)
        group["cond"] = condition
        group["initial"] = initial
        if finalize is not None:
            group["finalize"] = Code(finalize)

        cmd = SON([("group", group)])
        cmd.update(kwargs)

        with self._socket_for_reads() as (sock_info, slave_ok):
            return self._command(sock_info, cmd, slave_ok)["retval"]

    def rename(self, new_name, **kwargs):
        """Rename this collection.

        If operating in auth mode, client must be authorized as an
        admin to perform this operation. Raises :class:`TypeError` if
        `new_name` is not an instance of :class:`basestring`
        (:class:`str` in python 3). Raises :class:`~pymongo.errors.InvalidName`
        if `new_name` is not a valid collection name.

        :Parameters:
          - `new_name`: new name for this collection
          - `**kwargs` (optional): additional arguments to the rename command
            may be passed as keyword arguments to this helper method
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
        with self._socket_for_writes() as sock_info:
            sock_info.command('admin', cmd)

    def distinct(self, key, filter=None, **kwargs):
        """Get a list of distinct values for `key` among all documents
        in this collection.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring` (:class:`str` in python 3).

        All optional distinct parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `maxTimeMS` (int): The maximum amount of time to allow the count
            command to run, in milliseconds.

        The :meth:`distinct` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `key`: name of the field for which we want to get the distinct
            values
          - `filter` (optional): A query document that specifies the documents
            from which to retrieve the distinct values.
          - `**kwargs` (optional): See list of options above.
        """
        if not isinstance(key, string_type):
            raise TypeError("key must be an "
                            "instance of %s" % (string_type.__name__,))
        cmd = SON([("distinct", self.__name),
                   ("key", key)])
        if filter is not None:
            if "query" in kwargs:
                raise ConfigurationError("can't pass both filter and query")
            kwargs["query"] = filter
        cmd.update(kwargs)
        with self._socket_for_reads() as (sock_info, slave_ok):
            return self._command(sock_info, cmd, slave_ok)["values"]

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

        .. note:: The :meth:`map_reduce` method does **not** obey the
           :attr:`read_preference` of this :class:`Collection`. To run
           mapReduce on a secondary use the :meth:`inline_map_reduce` method
           instead.

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

        with self._socket_for_primary_reads() as (sock_info, slave_ok):
            response = self._command(
                sock_info, cmd, slave_ok, ReadPreference.PRIMARY)

        if full_response or not response.get('result'):
            return response
        elif isinstance(response['result'], dict):
            dbase = response['result']['db']
            coll = response['result']['collection']
            return self.__database.client[dbase][coll]
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

        The :meth:`inline_map_reduce` method obeys the :attr:`read_preference`
        of this :class:`Collection`.

        :Parameters:
          - `map`: map function (as a JavaScript string)
          - `reduce`: reduce function (as a JavaScript string)
          - `full_response` (optional): if ``True``, return full response to
            this command - otherwise just return the result collection
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
        with self._socket_for_reads() as (sock_info, slave_ok):
            res = self._command(sock_info, cmd, slave_ok)

        if full_response:
            return res
        else:
            return res.get("results")

    def __find_and_modify(self, filter, projection, sort, upsert=None,
                          return_document=ReturnDocument.BEFORE, **kwargs):
        """Internal findAndModify helper."""
        common.validate_is_mapping("filter", filter)
        if not isinstance(return_document, bool):
            raise ValueError("return_document must be "
                             "ReturnDocument.BEFORE or ReturnDocument.AFTER")
        cmd = SON([("findAndModify", self.__name),
                   ("query", filter),
                   ("new", return_document)])
        cmd.update(kwargs)
        if projection is not None:
            cmd["fields"] = helpers._fields_list_to_dict(projection,
                                                         "projection")
        if sort is not None:
            cmd["sort"] = helpers._index_document(sort)
        if upsert is not None:
            common.validate_boolean("upsert", upsert)
            cmd["upsert"] = upsert
        with self._socket_for_writes() as sock_info:
            out = self._command(sock_info, cmd,
                                read_preference=ReadPreference.PRIMARY,
                                allowable_errors=[_NO_OBJ_ERROR])
        return out.get("value")

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

        .. versionadded:: 3.0
        """
        kwargs['remove'] = True
        return self.__find_and_modify(filter, projection, sort, **kwargs)

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

        .. versionadded:: 3.0
        """
        common.validate_ok_for_replace(replacement)
        kwargs['update'] = replacement
        return self.__find_and_modify(filter, projection,
                                      sort, upsert, return_document, **kwargs)

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

        .. versionadded:: 3.0
        """
        common.validate_ok_for_update(update)
        kwargs['update'] = update
        return self.__find_and_modify(filter, projection,
                                      sort, upsert, return_document, **kwargs)

    def save(self, to_save, manipulate=True, check_keys=True, **kwargs):
        """Save a document in this collection.

        **DEPRECATED** - Use :meth:`insert_one` or :meth:`replace_one` instead.

        .. versionchanged:: 3.0
           Removed the `safe` parameter. Pass ``w=0`` for unacknowledged write
           operations.
        """
        warnings.warn("save is deprecated. Use insert_one or replace_one "
                      "instead", DeprecationWarning, stacklevel=2)
        common.validate_is_mutable_mapping("to_save", to_save)

        write_concern = None
        if kwargs:
            write_concern = WriteConcern(**kwargs)

        with self._socket_for_writes() as sock_info:
            if "_id" not in to_save:
                return self._insert(sock_info, to_save, True,
                                    check_keys, manipulate, write_concern)
            else:
                self._update(sock_info, {"_id": to_save["_id"]}, to_save, True,
                             check_keys, False, manipulate, write_concern)
                return to_save.get("_id")

    def insert(self, doc_or_docs, manipulate=True,
               check_keys=True, continue_on_error=False, **kwargs):
        """Insert a document(s) into this collection.

        **DEPRECATED** - Use :meth:`insert_one` or :meth:`insert_many` instead.

        .. versionchanged:: 3.0
           Removed the `safe` parameter. Pass ``w=0`` for unacknowledged write
           operations.
        """
        warnings.warn("insert is deprecated. Use insert_one or insert_many "
                      "instead.", DeprecationWarning, stacklevel=2)
        write_concern = None
        if kwargs:
            write_concern = WriteConcern(**kwargs)
        with self._socket_for_writes() as sock_info:
            return self._insert(sock_info, doc_or_docs, not continue_on_error,
                                check_keys, manipulate, write_concern)

    def update(self, spec, document, upsert=False, manipulate=False,
               multi=False, check_keys=True, **kwargs):
        """Update a document(s) in this collection.

        **DEPRECATED** - Use :meth:`replace_one`, :meth:`update_one`, or
        :meth:`update_many` instead.

        .. versionchanged:: 3.0
           Removed the `safe` parameter. Pass ``w=0`` for unacknowledged write
           operations.
        """
        warnings.warn("update is deprecated. Use replace_one, update_one or "
                      "update_many instead.", DeprecationWarning, stacklevel=2)
        common.validate_is_mapping("spec", spec)
        common.validate_is_mapping("document", document)
        if document:
            # If a top level key begins with '$' this is a modify operation
            # and we should skip key validation. It doesn't matter which key
            # we check here. Passing a document with a mix of top level keys
            # starting with and without a '$' is invalid and the server will
            # raise an appropriate exception.
            first = next(iter(document))
            if first.startswith('$'):
                check_keys = False

        write_concern = None
        if kwargs:
            write_concern = WriteConcern(**kwargs)
        with self._socket_for_writes() as sock_info:
            return self._update(sock_info, spec, document, upsert,
                                check_keys, multi, manipulate, write_concern)

    def remove(self, spec_or_id=None, multi=True, **kwargs):
        """Remove a document(s) from this collection.

        **DEPRECATED** - Use :meth:`delete_one` or :meth:`delete_many` instead.

        .. versionchanged:: 3.0
           Removed the `safe` parameter. Pass ``w=0`` for unacknowledged write
           operations.
        """
        warnings.warn("remove is deprecated. Use delete_one or delete_many "
                      "instead.", DeprecationWarning, stacklevel=2)
        if spec_or_id is None:
            spec_or_id = {}
        if not isinstance(spec_or_id, collections.Mapping):
            spec_or_id = {"_id": spec_or_id}
        write_concern = None
        if kwargs:
            write_concern = WriteConcern(**kwargs)
        with self._socket_for_writes() as sock_info:
            return self._delete(sock_info, spec_or_id, multi, write_concern)

    def find_and_modify(self, query={}, update=None,
                        upsert=False, sort=None, full_response=False,
                        manipulate=False, **kwargs):
        """Update and return an object.

        **DEPRECATED** - Use :meth:`find_one_and_delete`,
        :meth:`find_one_and_replace`, or :meth:`find_one_and_update` instead.
        """
        warnings.warn("find_and_modify is deprecated, use find_one_and_delete"
                      ", find_one_and_replace, or find_one_and_update instead",
                      DeprecationWarning, stacklevel=2)

        if not update and not kwargs.get('remove', None):
            raise ValueError("Must either update or remove")

        if update and kwargs.get('remove', None):
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
            elif (isinstance(sort, _ORDERED_TYPES) or
                  isinstance(sort, dict) and len(sort) == 1):
                warnings.warn("Passing mapping types for `sort` is deprecated,"
                              " use a list of (key, direction) pairs instead",
                              DeprecationWarning, stacklevel=2)
                kwargs['sort'] = sort
            else:
                raise TypeError("sort must be a list of (key, direction) "
                                "pairs, a dict of len 1, or an instance of "
                                "SON or OrderedDict")


        fields = kwargs.pop("fields", None)
        if fields is not None:
            kwargs["fields"] = helpers._fields_list_to_dict(fields, "fields")

        cmd = SON([("findAndModify", self.__name)])
        cmd.update(kwargs)
        with self._socket_for_writes() as sock_info:
            out = self._command(sock_info, cmd,
                                read_preference=ReadPreference.PRIMARY,
                                allowable_errors=[_NO_OBJ_ERROR])

        if not out['ok']:
            if out["errmsg"] == _NO_OBJ_ERROR:
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

    def __next__(self):
        raise TypeError("'Collection' object is not iterable")

    next = __next__

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
