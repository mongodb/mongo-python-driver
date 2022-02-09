# Copyright 2009-present MongoDB, Inc.
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

import datetime
import warnings
from collections import abc

from bson.code import Code
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from bson.raw_bson import RawBSONDocument
from bson.son import SON
from pymongo import common, helpers, message
from pymongo.aggregation import (
    _CollectionAggregationCommand,
    _CollectionRawAggregationCommand,
)
from pymongo.bulk import _Bulk
from pymongo.change_stream import CollectionChangeStream
from pymongo.collation import validate_collation_or_none
from pymongo.command_cursor import CommandCursor, RawBatchCommandCursor
from pymongo.cursor import Cursor, RawBatchCursor
from pymongo.errors import (
    ConfigurationError,
    InvalidName,
    InvalidOperation,
    OperationFailure,
)
from pymongo.helpers import _check_write_command_response
from pymongo.message import _UNICODE_REPLACE_CODEC_OPTIONS
from pymongo.operations import IndexModel
from pymongo.read_preferences import ReadPreference
from pymongo.results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from pymongo.write_concern import WriteConcern

_FIND_AND_MODIFY_DOC_FIELDS = {"value": 1}


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
    """A Mongo collection."""

    def __init__(
        self,
        database,
        name,
        create=False,
        codec_options=None,
        read_preference=None,
        write_concern=None,
        read_concern=None,
        session=None,
        **kwargs
    ):
        """Get / create a Mongo collection.

        Raises :class:`TypeError` if `name` is not an instance of
        :class:`basestring` (:class:`str` in python 3). Raises
        :class:`~pymongo.errors.InvalidName` if `name` is not a valid
        collection name. Any additional keyword arguments will be used
        as options passed to the create command. See
        :meth:`~pymongo.database.Database.create_collection` for valid
        options.

        If `create` is ``True``, `collation` is specified, or any additional
        keyword arguments are present, a ``create`` command will be
        sent, using ``session`` if specified. Otherwise, a ``create`` command
        will not be sent and the collection will be created implicitly on first
        use. The optional ``session`` argument is *only* used for the ``create``
        command, it is not associated with the collection afterward.

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
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) database.read_concern is used.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. If a collation is provided,
            it will be passed to the create collection command. This option is
            only supported on MongoDB 3.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession` that is used with
            the create collection command
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 4.0
           Removed the reindex, map_reduce, inline_map_reduce,
           parallel_scan, initialize_unordered_bulk_op,
           initialize_ordered_bulk_op, group, count, insert, save,
           update, remove, find_and_modify, and ensure_index methods. See the
           :ref:`pymongo4-migration-guide`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Support the `collation` option.

        .. versionchanged:: 3.2
           Added the read_concern option.

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

        .. seealso:: The MongoDB documentation on `collections <https://dochub.mongodb.org/core/collections>`_.
        """
        super(Collection, self).__init__(
            codec_options or database.codec_options,
            read_preference or database.read_preference,
            write_concern or database.write_concern,
            read_concern or database.read_concern,
        )

        if not isinstance(name, str):
            raise TypeError("name must be an instance of str")

        if not name or ".." in name:
            raise InvalidName("collection names cannot be empty")
        if "$" in name and not (name.startswith("oplog.$main") or name.startswith("$cmd")):
            raise InvalidName("collection names must not " "contain '$': %r" % name)
        if name[0] == "." or name[-1] == ".":
            raise InvalidName("collection names must not start " "or end with '.': %r" % name)
        if "\x00" in name:
            raise InvalidName("collection names must not contain the " "null character")
        collation = validate_collation_or_none(kwargs.pop("collation", None))

        self.__database = database
        self.__name = name
        self.__full_name = "%s.%s" % (self.__database.name, self.__name)
        if create or kwargs or collation:
            self.__create(kwargs, collation, session)

        self.__write_response_codec_options = self.codec_options._replace(
            unicode_decode_error_handler="replace", document_class=dict
        )

    def _socket_for_reads(self, session):
        return self.__database.client._socket_for_reads(self._read_preference_for(session), session)

    def _socket_for_writes(self, session):
        return self.__database.client._socket_for_writes(session)

    def _command(
        self,
        sock_info,
        command,
        secondary_ok=False,
        read_preference=None,
        codec_options=None,
        check=True,
        allowable_errors=None,
        read_concern=None,
        write_concern=None,
        collation=None,
        session=None,
        retryable_write=False,
        user_fields=None,
    ):
        """Internal command helper.

        :Parameters:
          - `sock_info` - A SocketInfo instance.
          - `command` - The command itself, as a SON instance.
          - `secondary_ok`: whether to set the secondaryOkay wire protocol bit.
          - `codec_options` (optional) - An instance of
            :class:`~bson.codec_options.CodecOptions`.
          - `check`: raise OperationFailure if there are errors
          - `allowable_errors`: errors to ignore if `check` is True
          - `read_concern` (optional) - An instance of
            :class:`~pymongo.read_concern.ReadConcern`.
          - `write_concern`: An instance of
            :class:`~pymongo.write_concern.WriteConcern`. This option is only
            valid for MongoDB 3.4 and above.
          - `collation` (optional) - An instance of
            :class:`~pymongo.collation.Collation`.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `retryable_write` (optional): True if this command is a retryable
            write.
          - `user_fields` (optional): Response fields that should be decoded
            using the TypeDecoders from codec_options, passed to
            bson._decode_all_selective.

        :Returns:
          The result document.
        """
        with self.__database.client._tmp_session(session) as s:
            return sock_info.command(
                self.__database.name,
                command,
                secondary_ok,
                read_preference or self._read_preference_for(session),
                codec_options or self.codec_options,
                check,
                allowable_errors,
                read_concern=read_concern,
                write_concern=write_concern,
                parse_write_concern_error=True,
                collation=collation,
                session=s,
                client=self.__database.client,
                retryable_write=retryable_write,
                user_fields=user_fields,
            )

    def __create(self, options, collation, session):
        """Sends a create command with the given options."""
        cmd = SON([("create", self.__name)])
        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            cmd.update(options)
        with self._socket_for_writes(session) as sock_info:
            self._command(
                sock_info,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                write_concern=self._write_concern_for(session),
                collation=collation,
                session=session,
            )

    def __getattr__(self, name):
        """Get a sub-collection of this collection by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        if name.startswith("_"):
            full_name = "%s.%s" % (self.__name, name)
            raise AttributeError(
                "Collection has no attribute %r. To access the %s"
                " collection, use database['%s']." % (name, full_name, full_name)
            )
        return self.__getitem__(name)

    def __getitem__(self, name):
        return Collection(
            self.__database,
            "%s.%s" % (self.__name, name),
            False,
            self.codec_options,
            self.read_preference,
            self.write_concern,
            self.read_concern,
        )

    def __repr__(self):
        return "Collection(%r, %r)" % (self.__database, self.__name)

    def __eq__(self, other):
        if isinstance(other, Collection):
            return self.__database == other.database and self.__name == other.name
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.__database, self.__name))

    def __bool__(self):
        raise NotImplementedError(
            "Collection objects do not implement truth "
            "value testing or bool(). Please compare "
            "with None instead: collection is not None"
        )

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
        self, codec_options=None, read_preference=None, write_concern=None, read_concern=None
    ):
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
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Collection`
            is used.
        """
        return Collection(
            self.__database,
            self.__name,
            False,
            codec_options or self.codec_options,
            read_preference or self.read_preference,
            write_concern or self.write_concern,
            read_concern or self.read_concern,
        )

    def bulk_write(self, requests, ordered=True, bypass_document_validation=False, session=None):
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
          {'x': 1, '_id': ObjectId('54f62e60fba5226811f634ef')}
          {'x': 1, '_id': ObjectId('54f62e60fba5226811f634f0')}
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
          {'x': 1, '_id': ObjectId('54f62e60fba5226811f634f0')}
          {'y': 1, '_id': ObjectId('54f62ee2fba5226811f634f1')}
          {'z': 1, '_id': ObjectId('54f62ee28891e756a6e1abd5')}

        :Parameters:
          - `requests`: A list of write operations (see examples above).
          - `ordered` (optional): If ``True`` (the default) requests will be
            performed on the server serially, in the order provided. If an error
            occurs all remaining operations are aborted. If ``False`` requests
            will be performed on the server in arbitrary order, possibly in
            parallel, and all operations will be attempted.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          An instance of :class:`~pymongo.results.BulkWriteResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        common.validate_list("requests", requests)

        blk = _Bulk(self, ordered, bypass_document_validation)
        for request in requests:
            try:
                request._add_to_bulk(blk)
            except AttributeError:
                raise TypeError("%r is not a valid request" % (request,))

        write_concern = self._write_concern_for(session)
        bulk_api_result = blk.execute(write_concern, session)
        if bulk_api_result is not None:
            return BulkWriteResult(bulk_api_result, True)
        return BulkWriteResult({}, False)

    def _insert_one(self, doc, ordered, check_keys, write_concern, op_id, bypass_doc_val, session):
        """Internal helper for inserting a single document."""
        write_concern = write_concern or self.write_concern
        acknowledged = write_concern.acknowledged
        command = SON([("insert", self.name), ("ordered", ordered), ("documents", [doc])])
        if not write_concern.is_server_default:
            command["writeConcern"] = write_concern.document

        def _insert_command(session, sock_info, retryable_write):
            if bypass_doc_val:
                command["bypassDocumentValidation"] = True

            result = sock_info.command(
                self.__database.name,
                command,
                write_concern=write_concern,
                codec_options=self.__write_response_codec_options,
                check_keys=check_keys,
                session=session,
                client=self.__database.client,
                retryable_write=retryable_write,
            )

            _check_write_command_response(result)

        self.__database.client._retryable_write(acknowledged, _insert_command, session)

        if not isinstance(doc, RawBSONDocument):
            return doc.get("_id")

    def insert_one(self, document, bypass_document_validation=False, session=None):
        """Insert a single document.

          >>> db.test.count_documents({'x': 1})
          0
          >>> result = db.test.insert_one({'x': 1})
          >>> result.inserted_id
          ObjectId('54f112defba522406c9cc208')
          >>> db.test.find_one({'x': 1})
          {'x': 1, '_id': ObjectId('54f112defba522406c9cc208')}

        :Parameters:
          - `document`: The document to insert. Must be a mutable mapping
            type. If the document does not have an _id field one will be
            added automatically.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.InsertOneResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        common.validate_is_document_type("document", document)
        if not (isinstance(document, RawBSONDocument) or "_id" in document):
            document["_id"] = ObjectId()

        write_concern = self._write_concern_for(session)
        return InsertOneResult(
            self._insert_one(
                document,
                ordered=True,
                check_keys=False,
                write_concern=write_concern,
                op_id=None,
                bypass_doc_val=bypass_document_validation,
                session=session,
            ),
            write_concern.acknowledged,
        )

    def insert_many(self, documents, ordered=True, bypass_document_validation=False, session=None):
        """Insert an iterable of documents.

          >>> db.test.count_documents({})
          0
          >>> result = db.test.insert_many([{'x': i} for i in range(2)])
          >>> result.inserted_ids
          [ObjectId('54f113fffba522406c9cc20e'), ObjectId('54f113fffba522406c9cc20f')]
          >>> db.test.count_documents({})
          2

        :Parameters:
          - `documents`: A iterable of documents to insert.
          - `ordered` (optional): If ``True`` (the default) documents will be
            inserted on the server serially, in the order provided. If an error
            occurs all remaining inserts are aborted. If ``False``, documents
            will be inserted on the server in arbitrary order, possibly in
            parallel, and all document inserts will be attempted.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          An instance of :class:`~pymongo.results.InsertManyResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        if (
            not isinstance(documents, abc.Iterable)
            or isinstance(documents, abc.Mapping)
            or not documents
        ):
            raise TypeError("documents must be a non-empty list")
        inserted_ids = []

        def gen():
            """A generator that validates documents and handles _ids."""
            for document in documents:
                common.validate_is_document_type("document", document)
                if not isinstance(document, RawBSONDocument):
                    if "_id" not in document:
                        document["_id"] = ObjectId()
                    inserted_ids.append(document["_id"])
                yield (message._INSERT, document)

        write_concern = self._write_concern_for(session)
        blk = _Bulk(self, ordered, bypass_document_validation)
        blk.ops = [doc for doc in gen()]
        blk.execute(write_concern, session=session)
        return InsertManyResult(inserted_ids, write_concern.acknowledged)

    def _update(
        self,
        sock_info,
        criteria,
        document,
        upsert=False,
        check_keys=False,
        multi=False,
        write_concern=None,
        op_id=None,
        ordered=True,
        bypass_doc_val=False,
        collation=None,
        array_filters=None,
        hint=None,
        session=None,
        retryable_write=False,
    ):
        """Internal update / replace helper."""
        common.validate_boolean("upsert", upsert)
        collation = validate_collation_or_none(collation)
        write_concern = write_concern or self.write_concern
        acknowledged = write_concern.acknowledged
        update_doc = SON([("q", criteria), ("u", document), ("multi", multi), ("upsert", upsert)])
        if collation is not None:
            if not acknowledged:
                raise ConfigurationError("Collation is unsupported for unacknowledged writes.")
            else:
                update_doc["collation"] = collation
        if array_filters is not None:
            if not acknowledged:
                raise ConfigurationError("arrayFilters is unsupported for unacknowledged writes.")
            else:
                update_doc["arrayFilters"] = array_filters
        if hint is not None:
            if not acknowledged:
                raise ConfigurationError("hint is unsupported for unacknowledged writes.")
            if not isinstance(hint, str):
                hint = helpers._index_document(hint)
            update_doc["hint"] = hint

        command = SON([("update", self.name), ("ordered", ordered), ("updates", [update_doc])])
        if not write_concern.is_server_default:
            command["writeConcern"] = write_concern.document

        # Update command.
        if bypass_doc_val:
            command["bypassDocumentValidation"] = True

        # The command result has to be published for APM unmodified
        # so we make a shallow copy here before adding updatedExisting.
        result = sock_info.command(
            self.__database.name,
            command,
            write_concern=write_concern,
            codec_options=self.__write_response_codec_options,
            session=session,
            client=self.__database.client,
            retryable_write=retryable_write,
        ).copy()
        _check_write_command_response(result)
        # Add the updatedExisting field for compatibility.
        if result.get("n") and "upserted" not in result:
            result["updatedExisting"] = True
        else:
            result["updatedExisting"] = False
            # MongoDB >= 2.6.0 returns the upsert _id in an array
            # element. Break it out for backward compatibility.
            if "upserted" in result:
                result["upserted"] = result["upserted"][0]["_id"]

        if not acknowledged:
            return None
        return result

    def _update_retryable(
        self,
        criteria,
        document,
        upsert=False,
        check_keys=False,
        multi=False,
        write_concern=None,
        op_id=None,
        ordered=True,
        bypass_doc_val=False,
        collation=None,
        array_filters=None,
        hint=None,
        session=None,
    ):
        """Internal update / replace helper."""

        def _update(session, sock_info, retryable_write):
            return self._update(
                sock_info,
                criteria,
                document,
                upsert=upsert,
                check_keys=check_keys,
                multi=multi,
                write_concern=write_concern,
                op_id=op_id,
                ordered=ordered,
                bypass_doc_val=bypass_doc_val,
                collation=collation,
                array_filters=array_filters,
                hint=hint,
                session=session,
                retryable_write=retryable_write,
            )

        return self.__database.client._retryable_write(
            (write_concern or self.write_concern).acknowledged and not multi, _update, session
        )

    def replace_one(
        self,
        filter,
        replacement,
        upsert=False,
        bypass_document_validation=False,
        collation=None,
        hint=None,
        session=None,
    ):
        """Replace a single document matching the filter.

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': ObjectId('54f4c5befba5220aa4d6dee7')}
          >>> result = db.test.replace_one({'x': 1}, {'y': 1})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'y': 1, '_id': ObjectId('54f4c5befba5220aa4d6dee7')}

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
          {'x': 1, '_id': ObjectId('54f11e5c8891e756a6e1abd4')}

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The new document.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``. This option is only supported on MongoDB 3.2 and above.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionchanged:: 3.2
          Added bypass_document_validation support.

        .. versionadded:: 3.0
        """
        common.validate_is_mapping("filter", filter)
        common.validate_ok_for_replace(replacement)

        write_concern = self._write_concern_for(session)
        return UpdateResult(
            self._update_retryable(
                filter,
                replacement,
                upsert,
                write_concern=write_concern,
                bypass_doc_val=bypass_document_validation,
                collation=collation,
                hint=hint,
                session=session,
            ),
            write_concern.acknowledged,
        )

    def update_one(
        self,
        filter,
        update,
        upsert=False,
        bypass_document_validation=False,
        collation=None,
        array_filters=None,
        hint=None,
        session=None,
    ):
        """Update a single document matching the filter.

          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> result = db.test.update_one({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 4, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``. This option is only supported on MongoDB 3.2 and above.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply. This option is only
            supported on MongoDB 3.6 and above.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the ``update``.
        .. versionchanged:: 3.6
           Added the ``array_filters`` and ``session`` parameters.
        .. versionchanged:: 3.4
          Added the ``collation`` option.
        .. versionchanged:: 3.2
          Added ``bypass_document_validation`` support.

        .. versionadded:: 3.0
        """
        common.validate_is_mapping("filter", filter)
        common.validate_ok_for_update(update)
        common.validate_list_or_none("array_filters", array_filters)

        write_concern = self._write_concern_for(session)
        return UpdateResult(
            self._update_retryable(
                filter,
                update,
                upsert,
                check_keys=False,
                write_concern=write_concern,
                bypass_doc_val=bypass_document_validation,
                collation=collation,
                array_filters=array_filters,
                hint=hint,
                session=session,
            ),
            write_concern.acknowledged,
        )

    def update_many(
        self,
        filter,
        update,
        upsert=False,
        array_filters=None,
        bypass_document_validation=False,
        collation=None,
        hint=None,
        session=None,
    ):
        """Update one or more documents that match the filter.

          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> result = db.test.update_many({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          3
          >>> result.modified_count
          3
          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 4, '_id': 0}
          {'x': 4, '_id': 1}
          {'x': 4, '_id': 2}

        :Parameters:
          - `filter`: A query that matches the documents to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation` (optional): If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``. This option is only supported on MongoDB 3.2 and above.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply. This option is only
            supported on MongoDB 3.6 and above.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the `update`.
        .. versionchanged:: 3.6
           Added ``array_filters`` and ``session`` parameters.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionchanged:: 3.2
          Added bypass_document_validation support.

        .. versionadded:: 3.0
        """
        common.validate_is_mapping("filter", filter)
        common.validate_ok_for_update(update)
        common.validate_list_or_none("array_filters", array_filters)

        write_concern = self._write_concern_for(session)
        return UpdateResult(
            self._update_retryable(
                filter,
                update,
                upsert,
                check_keys=False,
                multi=True,
                write_concern=write_concern,
                bypass_doc_val=bypass_document_validation,
                collation=collation,
                array_filters=array_filters,
                hint=hint,
                session=session,
            ),
            write_concern.acknowledged,
        )

    def drop(self, session=None):
        """Alias for :meth:`~pymongo.database.Database.drop_collection`.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        The following two calls are equivalent:

          >>> db.foo.drop()
          >>> db.drop_collection("foo")

        .. versionchanged:: 3.7
           :meth:`drop` now respects this :class:`Collection`'s :attr:`write_concern`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        dbo = self.__database.client.get_database(
            self.__database.name,
            self.codec_options,
            self.read_preference,
            self.write_concern,
            self.read_concern,
        )
        dbo.drop_collection(self.__name, session=session)

    def _delete(
        self,
        sock_info,
        criteria,
        multi,
        write_concern=None,
        op_id=None,
        ordered=True,
        collation=None,
        hint=None,
        session=None,
        retryable_write=False,
    ):
        """Internal delete helper."""
        common.validate_is_mapping("filter", criteria)
        write_concern = write_concern or self.write_concern
        acknowledged = write_concern.acknowledged
        delete_doc = SON([("q", criteria), ("limit", int(not multi))])
        collation = validate_collation_or_none(collation)
        if collation is not None:
            if not acknowledged:
                raise ConfigurationError("Collation is unsupported for unacknowledged writes.")
            else:
                delete_doc["collation"] = collation
        if hint is not None:
            if not acknowledged:
                raise ConfigurationError("hint is unsupported for unacknowledged writes.")
            if not isinstance(hint, str):
                hint = helpers._index_document(hint)
            delete_doc["hint"] = hint
        command = SON([("delete", self.name), ("ordered", ordered), ("deletes", [delete_doc])])
        if not write_concern.is_server_default:
            command["writeConcern"] = write_concern.document

        # Delete command.
        result = sock_info.command(
            self.__database.name,
            command,
            write_concern=write_concern,
            codec_options=self.__write_response_codec_options,
            session=session,
            client=self.__database.client,
            retryable_write=retryable_write,
        )
        _check_write_command_response(result)
        return result

    def _delete_retryable(
        self,
        criteria,
        multi,
        write_concern=None,
        op_id=None,
        ordered=True,
        collation=None,
        hint=None,
        session=None,
    ):
        """Internal delete helper."""

        def _delete(session, sock_info, retryable_write):
            return self._delete(
                sock_info,
                criteria,
                multi,
                write_concern=write_concern,
                op_id=op_id,
                ordered=ordered,
                collation=collation,
                hint=hint,
                session=session,
                retryable_write=retryable_write,
            )

        return self.__database.client._retryable_write(
            (write_concern or self.write_concern).acknowledged and not multi, _delete, session
        )

    def delete_one(self, filter, collation=None, hint=None, session=None):
        """Delete a single document matching the filter.

          >>> db.test.count_documents({'x': 1})
          3
          >>> result = db.test.delete_one({'x': 1})
          >>> result.deleted_count
          1
          >>> db.test.count_documents({'x': 1})
          2

        :Parameters:
          - `filter`: A query that matches the document to delete.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.DeleteResult`.

        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionadded:: 3.0
        """
        write_concern = self._write_concern_for(session)
        return DeleteResult(
            self._delete_retryable(
                filter,
                False,
                write_concern=write_concern,
                collation=collation,
                hint=hint,
                session=session,
            ),
            write_concern.acknowledged,
        )

    def delete_many(self, filter, collation=None, hint=None, session=None):
        """Delete one or more documents matching the filter.

          >>> db.test.count_documents({'x': 1})
          3
          >>> result = db.test.delete_many({'x': 1})
          >>> result.deleted_count
          3
          >>> db.test.count_documents({'x': 1})
          0

        :Parameters:
          - `filter`: A query that matches the documents to delete.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.DeleteResult`.

        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionadded:: 3.0
        """
        write_concern = self._write_concern_for(session)
        return DeleteResult(
            self._delete_retryable(
                filter,
                True,
                write_concern=write_concern,
                collation=collation,
                hint=hint,
                session=session,
            ),
            write_concern.acknowledged,
        )

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

              >>> collection.find_one(max_time_ms=100)
        """
        if filter is not None and not isinstance(filter, abc.Mapping):
            filter = {"_id": filter}

        cursor = self.find(filter, *args, **kwargs)
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
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `skip` (optional): the number of documents to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return. A limit of 0 (the default) is equivalent to setting no
            limit.
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
          - `oplog_replay` (optional): **DEPRECATED** - if True, set the
            oplogReplay query flag. Default: False.
          - `batch_size` (optional): Limits the number of documents returned in
            a single batch.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `return_key` (optional): If True, return only the index keys in
            each document.
          - `show_record_id` (optional): If True, adds a field ``$recordId`` in
            each document with the storage engine's internal record identifier.
          - `snapshot` (optional): **DEPRECATED** - If True, prevents the
            cursor from returning a document more than once because of an
            intervening write operation.
          - `hint` (optional): An index, in the same format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.hint` on the cursor to tell Mongo the
            proper index to use for the query.
          - `max_time_ms` (optional): Specifies a time limit for a query
            operation. If the specified time is exceeded, the operation will be
            aborted and :exc:`~pymongo.errors.ExecutionTimeout` is raised. Pass
            this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max_time_ms` on the cursor.
          - `max_scan` (optional): **DEPRECATED** - The maximum number of
            documents to scan. Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max_scan` on the cursor.
          - `min` (optional): A list of field, limit pairs specifying the
            inclusive lower bound for all keys of a specific index in order.
            Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.min` on the cursor. ``hint`` must
            also be passed to ensure the query utilizes the correct index.
          - `max` (optional): A list of field, limit pairs specifying the
            exclusive upper bound for all keys of a specific index in order.
            Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max` on the cursor. ``hint`` must
            also be passed to ensure the query utilizes the correct index.
          - `comment` (optional): A string to attach to the query to help
            interpret and trace the operation in the server logs and in profile
            data. Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.comment` on the cursor.
          - `allow_disk_use` (optional): if True, MongoDB may use temporary
            disk files to store data exceeding the system memory limit while
            processing a blocking sort operation. The option has no effect if
            MongoDB can satisfy the specified sort using an index, or if the
            blocking sort requires less memory than the 100 MiB limit. This
            option is only supported on MongoDB 4.4 and above.

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

        .. versionchanged:: 4.0
           Removed the ``modifiers`` option.
           Empty projections (eg {} or []) are passed to the server as-is,
           rather than the previous behavior which substituted in a
           projection of ``{"_id": 1}``. This means that an empty projection
           will now return the entire document, not just the ``"_id"`` field.

        .. versionchanged:: 3.11
           Added the ``allow_disk_use`` option.
           Deprecated the ``oplog_replay`` option. Support for this option is
           deprecated in MongoDB 4.4. The query engine now automatically
           optimizes queries against the oplog without requiring this
           option to be set.

        .. versionchanged:: 3.7
           Deprecated the ``snapshot`` option, which is deprecated in MongoDB
           3.6 and removed in MongoDB 4.0.
           Deprecated the ``max_scan`` option. Support for this option is
           deprecated in MongoDB 4.0. Use ``max_time_ms`` instead to limit
           server-side execution time.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.5
           Added the options ``return_key``, ``show_record_id``, ``snapshot``,
           ``hint``, ``max_time_ms``, ``max_scan``, ``min``, ``max``, and
           ``comment``.
           Deprecated the ``modifiers`` option.

        .. versionchanged:: 3.4
           Added support for the ``collation`` option.

        .. versionchanged:: 3.0
           Changed the parameter names ``spec``, ``fields``, ``timeout``, and
           ``partial`` to ``filter``, ``projection``, ``no_cursor_timeout``,
           and ``allow_partial_results`` respectively.
           Added the ``cursor_type``, ``oplog_replay``, and ``modifiers``
           options.
           Removed the ``network_timeout``, ``read_preference``, ``tag_sets``,
           ``secondary_acceptable_latency_ms``, ``max_scan``, ``snapshot``,
           ``tailable``, ``await_data``, ``exhaust``, ``as_class``, and
           slave_okay parameters.
           Removed ``compile_re`` option: PyMongo now always
           represents BSON regular expressions as :class:`~bson.regex.Regex`
           objects. Use :meth:`~bson.regex.Regex.try_compile` to attempt to
           convert from a BSON regular expression to a Python regular
           expression object.
           Soft deprecated the ``manipulate`` option.

        .. seealso:: The MongoDB documentation on `find <https://dochub.mongodb.org/core/find>`_.
        """
        return Cursor(self, *args, **kwargs)

    def find_raw_batches(self, *args, **kwargs):
        """Query the database and retrieve batches of raw BSON.

        Similar to the :meth:`find` method but returns a
        :class:`~pymongo.cursor.RawBatchCursor`.

        This example demonstrates how to work with raw batches, but in practice
        raw batches should be passed to an external library that can decode
        BSON into another data type, rather than used with PyMongo's
        :mod:`bson` module.

          >>> import bson
          >>> cursor = db.test.find_raw_batches()
          >>> for batch in cursor:
          ...     print(bson.decode_all(batch))

        .. note:: find_raw_batches does not support auto encryption.

        .. versionchanged:: 3.12
           Instead of ignoring the user-specified read concern, this method
           now sends it to the server when connected to MongoDB 3.6+.

           Added session support.

        .. versionadded:: 3.6
        """
        # OP_MSG is required to support encryption.
        if self.__database.client._encrypter:
            raise InvalidOperation("find_raw_batches does not support auto encryption")

        return RawBatchCursor(self, *args, **kwargs)

    def _count_cmd(self, session, sock_info, secondary_ok, cmd, collation):
        """Internal count command helper."""
        # XXX: "ns missing" checks can be removed when we drop support for
        # MongoDB 3.0, see SERVER-17051.
        res = self._command(
            sock_info,
            cmd,
            secondary_ok,
            allowable_errors=["ns missing"],
            codec_options=self.__write_response_codec_options,
            read_concern=self.read_concern,
            collation=collation,
            session=session,
        )
        if res.get("errmsg", "") == "ns missing":
            return 0
        return int(res["n"])

    def _aggregate_one_result(self, sock_info, secondary_ok, cmd, collation, session):
        """Internal helper to run an aggregate that returns a single result."""
        result = self._command(
            sock_info,
            cmd,
            secondary_ok,
            allowable_errors=[26],  # Ignore NamespaceNotFound.
            codec_options=self.__write_response_codec_options,
            read_concern=self.read_concern,
            collation=collation,
            session=session,
        )
        # cursor will not be present for NamespaceNotFound errors.
        if "cursor" not in result:
            return None
        batch = result["cursor"]["firstBatch"]
        return batch[0] if batch else None

    def estimated_document_count(self, **kwargs):
        """Get an estimate of the number of documents in this collection using
        collection metadata.

        The :meth:`estimated_document_count` method is **not** supported in a
        transaction.

        All optional parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `maxTimeMS` (int): The maximum amount of time to allow this
            operation to run, in milliseconds.

        :Parameters:
          - `**kwargs` (optional): See list of options above.

        .. versionadded:: 3.7
        """
        if "session" in kwargs:
            raise ConfigurationError("estimated_document_count does not support sessions")

        def _cmd(session, server, sock_info, secondary_ok):
            if sock_info.max_wire_version >= 12:
                # MongoDB 4.9+
                pipeline = [
                    {"$collStats": {"count": {}}},
                    {"$group": {"_id": 1, "n": {"$sum": "$count"}}},
                ]
                cmd = SON([("aggregate", self.__name), ("pipeline", pipeline), ("cursor", {})])
                cmd.update(kwargs)
                result = self._aggregate_one_result(
                    sock_info, secondary_ok, cmd, collation=None, session=session
                )
                if not result:
                    return 0
                return int(result["n"])
            else:
                # MongoDB < 4.9
                cmd = SON([("count", self.__name)])
                cmd.update(kwargs)
                return self._count_cmd(None, sock_info, secondary_ok, cmd, None)

        return self.__database.client._retryable_read(_cmd, self.read_preference, None)

    def count_documents(self, filter, session=None, **kwargs):
        """Count the number of documents in this collection.

        .. note:: For a fast count of the total documents in a collection see
           :meth:`estimated_document_count`.

        The :meth:`count_documents` method is supported in a transaction.

        All optional parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `skip` (int): The number of matching documents to skip before
            returning results.
          - `limit` (int): The maximum number of documents to count. Must be
            a positive integer. If not provided, no limit is imposed.
          - `maxTimeMS` (int): The maximum amount of time to allow this
            operation to run, in milliseconds.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `hint` (string or list of tuples): The index to use. Specify either
            the index name as a string or the index specification as a list of
            tuples (e.g. [('a', pymongo.ASCENDING), ('b', pymongo.ASCENDING)]).
            This option is only supported on MongoDB 3.6 and above.

        The :meth:`count_documents` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        .. note:: When migrating from :meth:`count` to :meth:`count_documents`
           the following query operators must be replaced:

           +-------------+-------------------------------------+
           | Operator    | Replacement                         |
           +=============+=====================================+
           | $where      | `$expr`_                            |
           +-------------+-------------------------------------+
           | $near       | `$geoWithin`_ with `$center`_       |
           +-------------+-------------------------------------+
           | $nearSphere | `$geoWithin`_ with `$centerSphere`_ |
           +-------------+-------------------------------------+

           $expr requires MongoDB 3.6+

        :Parameters:
          - `filter` (required): A query document that selects which documents
            to count in the collection. Can be an empty document to count all
            documents.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): See list of options above.

        .. versionadded:: 3.7

        .. _$expr: https://docs.mongodb.com/manual/reference/operator/query/expr/
        .. _$geoWithin: https://docs.mongodb.com/manual/reference/operator/query/geoWithin/
        .. _$center: https://docs.mongodb.com/manual/reference/operator/query/center/#op._S_center
        .. _$centerSphere: https://docs.mongodb.com/manual/reference/operator/query/centerSphere/#op._S_centerSphere
        """
        pipeline = [{"$match": filter}]
        if "skip" in kwargs:
            pipeline.append({"$skip": kwargs.pop("skip")})
        if "limit" in kwargs:
            pipeline.append({"$limit": kwargs.pop("limit")})
        pipeline.append({"$group": {"_id": 1, "n": {"$sum": 1}}})
        cmd = SON([("aggregate", self.__name), ("pipeline", pipeline), ("cursor", {})])
        if "hint" in kwargs and not isinstance(kwargs["hint"], str):
            kwargs["hint"] = helpers._index_document(kwargs["hint"])
        collation = validate_collation_or_none(kwargs.pop("collation", None))
        cmd.update(kwargs)

        def _cmd(session, server, sock_info, secondary_ok):
            result = self._aggregate_one_result(sock_info, secondary_ok, cmd, collation, session)
            if not result:
                return 0
            return result["n"]

        return self.__database.client._retryable_read(
            _cmd, self._read_preference_for(session), session
        )

    def create_indexes(self, indexes, session=None, **kwargs):
        """Create one or more indexes on this collection.

          >>> from pymongo import IndexModel, ASCENDING, DESCENDING
          >>> index1 = IndexModel([("hello", DESCENDING),
          ...                      ("world", ASCENDING)], name="hello_world")
          >>> index2 = IndexModel([("goodbye", DESCENDING)])
          >>> db.test.create_indexes([index1, index2])
          ["hello_world", "goodbye_-1"]

        :Parameters:
          - `indexes`: A list of :class:`~pymongo.operations.IndexModel`
            instances.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        .. note:: `create_indexes` uses the `createIndexes`_ command
           introduced in MongoDB **2.6** and cannot be used with earlier
           versions.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for arbitrary keyword
           arguments.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.
        .. versionadded:: 3.0

        .. _createIndexes: https://docs.mongodb.com/manual/reference/command/createIndexes/
        """
        common.validate_list("indexes", indexes)
        return self.__create_indexes(indexes, session, **kwargs)

    def __create_indexes(self, indexes, session, **kwargs):
        """Internal createIndexes helper.

        :Parameters:
          - `indexes`: A list of :class:`~pymongo.operations.IndexModel`
            instances.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.
        """
        names = []
        with self._socket_for_writes(session) as sock_info:
            supports_quorum = sock_info.max_wire_version >= 9

            def gen_indexes():
                for index in indexes:
                    if not isinstance(index, IndexModel):
                        raise TypeError(
                            "%r is not an instance of " "pymongo.operations.IndexModel" % (index,)
                        )
                    document = index.document
                    names.append(document["name"])
                    yield document

            cmd = SON([("createIndexes", self.name), ("indexes", list(gen_indexes()))])
            cmd.update(kwargs)
            if "commitQuorum" in kwargs and not supports_quorum:
                raise ConfigurationError(
                    "Must be connected to MongoDB 4.4+ to use the "
                    "commitQuorum option for createIndexes"
                )

            self._command(
                sock_info,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                codec_options=_UNICODE_REPLACE_CODEC_OPTIONS,
                write_concern=self._write_concern_for(session),
                session=session,
            )
        return names

    def create_index(self, keys, session=None, **kwargs):
        """Creates an index on this collection.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`
        (:class:`str` in python 3), and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`, :data:`~pymongo.GEOSPHERE`,
        :data:`~pymongo.HASHED`, :data:`~pymongo.TEXT`).

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
          - `unique`: if ``True``, creates a uniqueness constraint on the
            index.
          - `background`: if ``True``, this index should be created in the
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
          - `partialFilterExpression`: A document that specifies a filter for
            a partial index. Requires MongoDB >=3.2.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. Requires MongoDB >= 3.4.
          - `wildcardProjection`: Allows users to include or exclude specific
            field paths from a `wildcard index`_ using the {"$**" : 1} key
            pattern. Requires MongoDB >= 4.2.
          - `hidden`: if ``True``, this index will be hidden from the query
            planner and will not be evaluated as part of query plan
            selection. Requires MongoDB >= 4.4.

        See the MongoDB documentation for a full list of supported options by
        server version.

        .. warning:: `dropDups` is not supported by MongoDB 3.0 or newer. The
          option is silently ignored by the server and unique index builds
          using the option will fail if a duplicate value is detected.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        :Parameters:
          - `keys`: a single key or a list of (key, direction)
            pairs specifying the index to create
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): any additional index creation
            options (see the above list) should be passed as keyword
            arguments

        .. versionchanged:: 3.11
           Added the ``hidden`` option.
        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for passing maxTimeMS
           in kwargs.
        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4. Support the `collation` option.
        .. versionchanged:: 3.2
           Added partialFilterExpression to support partial indexes.
        .. versionchanged:: 3.0
           Renamed `key_or_list` to `keys`. Removed the `cache_for` option.
           :meth:`create_index` no longer caches index names. Removed support
           for the drop_dups and bucket_size aliases.

        .. seealso:: The MongoDB documentation on `indexes <https://dochub.mongodb.org/core/indexes>`_.

        .. _wildcard index: https://docs.mongodb.com/master/core/index-wildcard/#wildcard-index-core
        """
        cmd_options = {}
        if "maxTimeMS" in kwargs:
            cmd_options["maxTimeMS"] = kwargs.pop("maxTimeMS")
        index = IndexModel(keys, **kwargs)
        return self.__create_indexes([index], session, **cmd_options)[0]

    def drop_indexes(self, session=None, **kwargs):
        """Drops all indexes on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for arbitrary keyword
           arguments.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        self.drop_index("*", session=session, **kwargs)

    def drop_index(self, index_or_name, session=None, **kwargs):
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
          passing the `name` parameter to :meth:`create_index`) the index
          **must** be dropped by name.

        :Parameters:
          - `index_or_name`: index (or name of index) to drop
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for arbitrary keyword
           arguments.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        name = index_or_name
        if isinstance(index_or_name, list):
            name = helpers._gen_index_name(index_or_name)

        if not isinstance(name, str):
            raise TypeError("index_or_name must be an instance of str or list")

        cmd = SON([("dropIndexes", self.__name), ("index", name)])
        cmd.update(kwargs)
        with self._socket_for_writes(session) as sock_info:
            self._command(
                sock_info,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                allowable_errors=["ns not found", 26],
                write_concern=self._write_concern_for(session),
                session=session,
            )

    def list_indexes(self, session=None):
        """Get a cursor over the index documents for this collection.

          >>> for index in db.test.list_indexes():
          ...     print(index)
          ...
          SON([('v', 2), ('key', SON([('_id', 1)])), ('name', '_id_')])

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          An instance of :class:`~pymongo.command_cursor.CommandCursor`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionadded:: 3.0
        """
        codec_options = CodecOptions(SON)
        coll = self.with_options(
            codec_options=codec_options, read_preference=ReadPreference.PRIMARY
        )
        read_pref = (session and session._txn_read_preference()) or ReadPreference.PRIMARY

        def _cmd(session, server, sock_info, secondary_ok):
            cmd = SON([("listIndexes", self.__name), ("cursor", {})])
            with self.__database.client._tmp_session(session, False) as s:
                try:
                    cursor = self._command(
                        sock_info, cmd, secondary_ok, read_pref, codec_options, session=s
                    )["cursor"]
                except OperationFailure as exc:
                    # Ignore NamespaceNotFound errors to match the behavior
                    # of reading from *.system.indexes.
                    if exc.code != 26:
                        raise
                    cursor = {"id": 0, "firstBatch": []}
            cmd_cursor = CommandCursor(
                coll, cursor, sock_info.address, session=s, explicit_session=session is not None
            )
            cmd_cursor._maybe_pin_connection(sock_info)
            return cmd_cursor

        return self.__database.client._retryable_read(_cmd, read_pref, session)

    def index_information(self, session=None):
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

        >>> db.test.create_index("x", unique=True)
        'x_1'
        >>> db.test.index_information()
        {'_id_': {'key': [('_id', 1)]},
         'x_1': {'unique': True, 'key': [('x', 1)]}}

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        cursor = self.list_indexes(session=session)
        info = {}
        for index in cursor:
            index["key"] = list(index["key"].items())
            index = dict(index)
            info[index.pop("name")] = index
        return info

    def options(self, session=None):
        """Get the options set on this collection.

        Returns a dictionary of options and their values - see
        :meth:`~pymongo.database.Database.create_collection` for more
        information on the possible options. Returns an empty
        dictionary if the collection has not been created yet.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        dbo = self.__database.client.get_database(
            self.__database.name,
            self.codec_options,
            self.read_preference,
            self.write_concern,
            self.read_concern,
        )
        cursor = dbo.list_collections(session=session, filter={"name": self.__name})

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

    def _aggregate(
        self, aggregation_command, pipeline, cursor_class, session, explicit_session, **kwargs
    ):
        cmd = aggregation_command(
            self,
            cursor_class,
            pipeline,
            kwargs,
            explicit_session,
            user_fields={"cursor": {"firstBatch": 1}},
        )
        return self.__database.client._retryable_read(
            cmd.get_cursor,
            cmd.get_read_preference(session),
            session,
            retryable=not cmd._performs_write,
        )

    def aggregate(self, pipeline, session=None, **kwargs):
        """Perform an aggregation using the aggregation framework on this
        collection.

        The :meth:`aggregate` method obeys the :attr:`read_preference` of this
        :class:`Collection`, except when ``$out`` or ``$merge`` are used, in
        which case  :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`
        is used.

        .. note:: This method does not support the 'explain' option. Please
           use :meth:`~pymongo.database.Database.command` instead. An
           example is included in the :ref:`aggregate-examples` documentation.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        :Parameters:
          - `pipeline`: a list of aggregation pipeline stages
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): extra `aggregate command`_ parameters.

        All optional `aggregate command`_ parameters should be passed as
        keyword arguments to this method. Valid options include, but are not
        limited to:

          - `allowDiskUse` (bool): Enables writing to temporary files. When set
            to True, aggregation stages can write data to the _tmp subdirectory
            of the --dbpath directory. The default is False.
          - `maxTimeMS` (int): The maximum amount of time to allow the operation
            to run in milliseconds.
          - `batchSize` (int): The maximum number of documents to return per
            batch. Ignored if the connected mongod or mongos does not support
            returning aggregate results using a cursor.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `let` (dict): A dict of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. ``"$$var"``). This option is
            only supported on MongoDB >= 5.0.

        :Returns:
          A :class:`~pymongo.command_cursor.CommandCursor` over the result
          set.

        .. versionchanged:: 4.0
           Removed the ``useCursor`` option.
        .. versionchanged:: 3.9
           Apply this collection's read concern to pipelines containing the
           `$out` stage when connected to MongoDB >= 4.2.
           Added support for the ``$merge`` pipeline stage.
           Aggregations that write always use read preference
           :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.
        .. versionchanged:: 3.6
           Added the `session` parameter. Added the `maxAwaitTimeMS` option.
           Deprecated the `useCursor` option.
        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4. Support the `collation` option.
        .. versionchanged:: 3.0
           The :meth:`aggregate` method always returns a CommandCursor. The
           pipeline argument must be a list.

        .. seealso:: :doc:`/examples/aggregation`

        .. _aggregate command:
            https://docs.mongodb.com/manual/reference/command/aggregate
        """
        with self.__database.client._tmp_session(session, close=False) as s:
            return self._aggregate(
                _CollectionAggregationCommand,
                pipeline,
                CommandCursor,
                session=s,
                explicit_session=session is not None,
                **kwargs
            )

    def aggregate_raw_batches(self, pipeline, session=None, **kwargs):
        """Perform an aggregation and retrieve batches of raw BSON.

        Similar to the :meth:`aggregate` method but returns a
        :class:`~pymongo.cursor.RawBatchCursor`.

        This example demonstrates how to work with raw batches, but in practice
        raw batches should be passed to an external library that can decode
        BSON into another data type, rather than used with PyMongo's
        :mod:`bson` module.

          >>> import bson
          >>> cursor = db.test.aggregate_raw_batches([
          ...     {'$project': {'x': {'$multiply': [2, '$x']}}}])
          >>> for batch in cursor:
          ...     print(bson.decode_all(batch))

        .. note:: aggregate_raw_batches does not support auto encryption.

        .. versionchanged:: 3.12
           Added session support.

        .. versionadded:: 3.6
        """
        # OP_MSG is required to support encryption.
        if self.__database.client._encrypter:
            raise InvalidOperation("aggregate_raw_batches does not support auto encryption")

        with self.__database.client._tmp_session(session, close=False) as s:
            return self._aggregate(
                _CollectionRawAggregationCommand,
                pipeline,
                RawBatchCommandCursor,
                session=s,
                explicit_session=session is not None,
                **kwargs
            )

    def watch(
        self,
        pipeline=None,
        full_document=None,
        resume_after=None,
        max_await_time_ms=None,
        batch_size=None,
        collation=None,
        start_at_operation_time=None,
        session=None,
        start_after=None,
    ):
        """Watch changes on this collection.

        Performs an aggregation with an implicit initial ``$changeStream``
        stage and returns a
        :class:`~pymongo.change_stream.CollectionChangeStream` cursor which
        iterates over changes on this collection.

        Introduced in MongoDB 3.6.

        .. code-block:: python

           with db.collection.watch() as stream:
               for change in stream:
                   print(change)

        The :class:`~pymongo.change_stream.CollectionChangeStream` iterable
        blocks until the next change document is returned or an error is
        raised. If the
        :meth:`~pymongo.change_stream.CollectionChangeStream.next` method
        encounters a network error when retrieving a batch from the server,
        it will automatically attempt to recreate the cursor such that no
        change events are missed. Any error encountered during the resume
        attempt indicates there may be an outage and will be raised.

        .. code-block:: python

            try:
                with db.collection.watch(
                        [{'$match': {'operationType': 'insert'}}]) as stream:
                    for insert_change in stream:
                        print(insert_change)
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                logging.error('...')

        For a precise description of the resume process see the
        `change streams specification`_.

        .. note:: Using this helper method is preferred to directly calling
            :meth:`~pymongo.collection.Collection.aggregate` with a
            ``$changeStream`` stage, for the purpose of supporting
            resumability.

        .. warning:: This Collection's :attr:`read_concern` must be
            ``ReadConcern("majority")`` in order to use the ``$changeStream``
            stage.

        :Parameters:
          - `pipeline` (optional): A list of aggregation pipeline stages to
            append to an initial ``$changeStream`` stage. Not all
            pipeline stages are valid after a ``$changeStream`` stage, see the
            MongoDB documentation on change streams for the supported stages.
          - `full_document` (optional): The fullDocument to pass as an option
            to the ``$changeStream`` stage. Allowed values: 'updateLookup'.
            When set to 'updateLookup', the change notification for partial
            updates will include both a delta describing the changes to the
            document, as well as a copy of the entire document that was
            changed from some time after the change occurred.
          - `resume_after` (optional): A resume token. If provided, the
            change stream will start returning changes that occur directly
            after the operation specified in the resume token. A resume token
            is the _id value of a change document.
          - `max_await_time_ms` (optional): The maximum time in milliseconds
            for the server to wait for changes before responding to a getMore
            operation.
          - `batch_size` (optional): The maximum number of documents to return
            per batch.
          - `collation` (optional): The :class:`~pymongo.collation.Collation`
            to use for the aggregation.
          - `start_at_operation_time` (optional): If provided, the resulting
            change stream will only return changes that occurred at or after
            the specified :class:`~bson.timestamp.Timestamp`. Requires
            MongoDB >= 4.0.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `start_after` (optional): The same as `resume_after` except that
            `start_after` can resume notifications after an invalidate event.
            This option and `resume_after` are mutually exclusive.

        :Returns:
          A :class:`~pymongo.change_stream.CollectionChangeStream` cursor.

        .. versionchanged:: 3.9
           Added the ``start_after`` parameter.

        .. versionchanged:: 3.7
           Added the ``start_at_operation_time`` parameter.

        .. versionadded:: 3.6

        .. seealso:: The MongoDB documentation on `changeStreams <https://dochub.mongodb.org/core/changeStreams>`_.

        .. _change streams specification:
            https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst
        """
        return CollectionChangeStream(
            self,
            pipeline,
            full_document,
            resume_after,
            max_await_time_ms,
            batch_size,
            collation,
            start_at_operation_time,
            session,
            start_after,
        )

    def rename(self, new_name, session=None, **kwargs):
        """Rename this collection.

        If operating in auth mode, client must be authorized as an
        admin to perform this operation. Raises :class:`TypeError` if
        `new_name` is not an instance of :class:`basestring`
        (:class:`str` in python 3). Raises :class:`~pymongo.errors.InvalidName`
        if `new_name` is not a valid collection name.

        :Parameters:
          - `new_name`: new name for this collection
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional arguments to the rename command
            may be passed as keyword arguments to this helper method
            (i.e. ``dropTarget=True``)

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        if not isinstance(new_name, str):
            raise TypeError("new_name must be an instance of str")

        if not new_name or ".." in new_name:
            raise InvalidName("collection names cannot be empty")
        if new_name[0] == "." or new_name[-1] == ".":
            raise InvalidName("collecion names must not start or end with '.'")
        if "$" in new_name and not new_name.startswith("oplog.$main"):
            raise InvalidName("collection names must not contain '$'")

        new_name = "%s.%s" % (self.__database.name, new_name)
        cmd = SON([("renameCollection", self.__full_name), ("to", new_name)])
        cmd.update(kwargs)
        write_concern = self._write_concern_for_cmd(cmd, session)

        with self._socket_for_writes(session) as sock_info:
            with self.__database.client._tmp_session(session) as s:
                return sock_info.command(
                    "admin",
                    cmd,
                    write_concern=write_concern,
                    parse_write_concern_error=True,
                    session=s,
                    client=self.__database.client,
                )

    def distinct(self, key, filter=None, session=None, **kwargs):
        """Get a list of distinct values for `key` among all documents
        in this collection.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring` (:class:`str` in python 3).

        All optional distinct parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `maxTimeMS` (int): The maximum amount of time to allow the count
            command to run, in milliseconds.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.

        The :meth:`distinct` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `key`: name of the field for which we want to get the distinct
            values
          - `filter` (optional): A query document that specifies the documents
            from which to retrieve the distinct values.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): See list of options above.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Support the `collation` option.

        """
        if not isinstance(key, str):
            raise TypeError("key must be an instance of str")
        cmd = SON([("distinct", self.__name), ("key", key)])
        if filter is not None:
            if "query" in kwargs:
                raise ConfigurationError("can't pass both filter and query")
            kwargs["query"] = filter
        collation = validate_collation_or_none(kwargs.pop("collation", None))
        cmd.update(kwargs)

        def _cmd(session, server, sock_info, secondary_ok):
            return self._command(
                sock_info,
                cmd,
                secondary_ok,
                read_concern=self.read_concern,
                collation=collation,
                session=session,
                user_fields={"values": 1},
            )["values"]

        return self.__database.client._retryable_read(
            _cmd, self._read_preference_for(session), session
        )

    def _write_concern_for_cmd(self, cmd, session):
        raw_wc = cmd.get("writeConcern")
        if raw_wc is not None:
            return WriteConcern(**raw_wc)
        else:
            return self._write_concern_for(session)

    def __find_and_modify(
        self,
        filter,
        projection,
        sort,
        upsert=None,
        return_document=ReturnDocument.BEFORE,
        array_filters=None,
        hint=None,
        session=None,
        **kwargs
    ):
        """Internal findAndModify helper."""

        common.validate_is_mapping("filter", filter)
        if not isinstance(return_document, bool):
            raise ValueError(
                "return_document must be " "ReturnDocument.BEFORE or ReturnDocument.AFTER"
            )
        collation = validate_collation_or_none(kwargs.pop("collation", None))
        cmd = SON([("findAndModify", self.__name), ("query", filter), ("new", return_document)])
        cmd.update(kwargs)
        if projection is not None:
            cmd["fields"] = helpers._fields_list_to_dict(projection, "projection")
        if sort is not None:
            cmd["sort"] = helpers._index_document(sort)
        if upsert is not None:
            common.validate_boolean("upsert", upsert)
            cmd["upsert"] = upsert
        if hint is not None:
            if not isinstance(hint, str):
                hint = helpers._index_document(hint)

        write_concern = self._write_concern_for_cmd(cmd, session)

        def _find_and_modify(session, sock_info, retryable_write):
            if array_filters is not None:
                if not write_concern.acknowledged:
                    raise ConfigurationError(
                        "arrayFilters is unsupported for unacknowledged " "writes."
                    )
                cmd["arrayFilters"] = array_filters
            if hint is not None:
                if sock_info.max_wire_version < 8:
                    raise ConfigurationError("Must be connected to MongoDB 4.2+ to use hint.")
                if not write_concern.acknowledged:
                    raise ConfigurationError("hint is unsupported for unacknowledged writes.")
                cmd["hint"] = hint
            if not write_concern.is_server_default:
                cmd["writeConcern"] = write_concern.document
            out = self._command(
                sock_info,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                write_concern=write_concern,
                collation=collation,
                session=session,
                retryable_write=retryable_write,
                user_fields=_FIND_AND_MODIFY_DOC_FIELDS,
            )
            _check_write_command_response(out)

            return out.get("value")

        return self.__database.client._retryable_write(
            write_concern.acknowledged, _find_and_modify, session
        )

    def find_one_and_delete(
        self, filter, projection=None, sort=None, hint=None, session=None, **kwargs
    ):
        """Finds a single document and deletes it, returning the document.

          >>> db.test.count_documents({'x': 1})
          2
          >>> db.test.find_one_and_delete({'x': 1})
          {'x': 1, '_id': ObjectId('54f4e12bfba5220aa4d6dee8')}
          >>> db.test.count_documents({'x': 1})
          1

        If multiple documents match *filter*, a *sort* can be applied.

          >>> for doc in db.test.find({'x': 1}):
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> db.test.find_one_and_delete(
          ...     {'x': 1}, sort=[('_id', pymongo.DESCENDING)])
          {'x': 1, '_id': 2}

        The *projection* option can be used to limit the fields returned.

          >>> db.test.find_one_and_delete({'x': 1}, projection={'_id': False})
          {'x': 1}

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
          - `hint` (optional): An index to use to support the query predicate
            specified either by its string name, or in the same format as
            passed to :meth:`~pymongo.collection.Collection.create_index`
            (e.g. ``[('field', ASCENDING)]``). This option is only supported
            on MongoDB 4.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionchanged:: 3.4
           Added the `collation` option.
        .. versionadded:: 3.0
        """
        kwargs["remove"] = True
        return self.__find_and_modify(
            filter, projection, sort, hint=hint, session=session, **kwargs
        )

    def find_one_and_replace(
        self,
        filter,
        replacement,
        projection=None,
        sort=None,
        upsert=False,
        return_document=ReturnDocument.BEFORE,
        hint=None,
        session=None,
        **kwargs
    ):
        """Finds a single document and replaces it, returning either the
        original or the replaced document.

        The :meth:`find_one_and_replace` method differs from
        :meth:`find_one_and_update` by replacing the document matched by
        *filter*, rather than modifying the existing document.

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> db.test.find_one_and_replace({'x': 1}, {'y': 1})
          {'x': 1, '_id': 0}
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'y': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}

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
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 3.11
           Added the ``hint`` option.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
           Added the ``collation`` option.
        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionadded:: 3.0
        """
        common.validate_ok_for_replace(replacement)
        kwargs["update"] = replacement
        return self.__find_and_modify(
            filter, projection, sort, upsert, return_document, hint=hint, session=session, **kwargs
        )

    def find_one_and_update(
        self,
        filter,
        update,
        projection=None,
        sort=None,
        upsert=False,
        return_document=ReturnDocument.BEFORE,
        array_filters=None,
        hint=None,
        session=None,
        **kwargs
    ):
        """Finds a single document and updates it, returning either the
        original or the updated document.

          >>> db.test.find_one_and_update(
          ...    {'_id': 665}, {'$inc': {'count': 1}, '$set': {'done': True}})
          {'_id': 665, 'done': False, 'count': 25}}

        Returns ``None`` if no document matches the filter.

          >>> db.test.find_one_and_update(
          ...    {'_exists': False}, {'$inc': {'count': 1}})

        When the filter matches, by default :meth:`find_one_and_update`
        returns the original version of the document before the update was
        applied. To return the updated (or inserted in the case of
        *upsert*) version of the document instead, use the *return_document*
        option.

          >>> from pymongo import ReturnDocument
          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     return_document=ReturnDocument.AFTER)
          {'_id': 'userid', 'seq': 1}

        You can limit the fields returned with the *projection* option.

          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     projection={'seq': True, '_id': False},
          ...     return_document=ReturnDocument.AFTER)
          {'seq': 2}

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
          {'seq': 1}

        If multiple documents match *filter*, a *sort* can be applied.

          >>> for doc in db.test.find({'done': True}):
          ...     print(doc)
          ...
          {'_id': 665, 'done': True, 'result': {'count': 26}}
          {'_id': 701, 'done': True, 'result': {'count': 17}}
          >>> db.test.find_one_and_update(
          ...     {'done': True},
          ...     {'$set': {'final': True}},
          ...     sort=[('_id', pymongo.DESCENDING)])
          {'_id': 701, 'done': True, 'result': {'count': 17}}

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
            returns the original document before it was updated. If
            :attr:`ReturnDocument.AFTER`, returns the updated
            or inserted document.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply. This option is only
            supported on MongoDB 3.6 and above.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 3.11
           Added the ``hint`` option.
        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the ``update``.
        .. versionchanged:: 3.6
           Added the ``array_filters`` and ``session`` options.
        .. versionchanged:: 3.4
           Added the ``collation`` option.
        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionadded:: 3.0
        """
        common.validate_ok_for_update(update)
        common.validate_list_or_none("array_filters", array_filters)
        kwargs["update"] = update
        return self.__find_and_modify(
            filter,
            projection,
            sort,
            upsert,
            return_document,
            array_filters,
            hint=hint,
            session=session,
            **kwargs
        )

    def __iter__(self):
        return self

    def __next__(self):
        raise TypeError("'Collection' object is not iterable")

    next = __next__

    def __call__(self, *args, **kwargs):
        """This is only here so that some API misusages are easier to debug."""
        if "." not in self.__name:
            raise TypeError(
                "'Collection' object is not callable. If you "
                "meant to call the '%s' method on a 'Database' "
                "object it is failing because no such method "
                "exists." % self.__name
            )
        raise TypeError(
            "'Collection' object is not callable. If you meant to "
            "call the '%s' method on a 'Collection' object it is "
            "failing because no such method exists." % self.__name.split(".")[-1]
        )
