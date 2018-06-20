# Copyright 2017 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""ChangeStream cursor to iterate over changes on a collection."""

import copy

from bson.son import SON

from pymongo import common
from pymongo.collation import validate_collation_or_none
from pymongo.command_cursor import CommandCursor
from pymongo.errors import (ConnectionFailure,
                            InvalidOperation,
                            OperationFailure,
                            PyMongoError)


# The change streams spec considers the following server errors from the
# getMore command non-resumable. All other getMore errors are resumable.
_NON_RESUMABLE_GETMORE_ERRORS = frozenset([
    11601,  # Interrupted
    136,    # CappedPositionLost
    237,    # CursorKilled
    None,   # No error code was returned.
])


class ChangeStream(object):
    """The internal abstract base class for change stream cursors.

    Should not be called directly by application developers. Use 
    :meth:pymongo.collection.Collection.watch,
    :meth:pymongo.database.Database.watch, or
    :meth:pymongo.mongo_client.MongoClient.watch instead.

    Defines the interface for change streams. Should be subclassed to
    implement the `ChangeStream._create_cursor` abstract method, and
    the `ChangeStream._database`and ChangeStream._aggregation_target`
    abstract properties.
    """
    def __init__(self, target, pipeline, full_document, resume_after,
                 max_await_time_ms, batch_size, collation,
                 start_at_operation_time, session):
        if pipeline is None:
            pipeline = []
        elif not isinstance(pipeline, list):
            raise TypeError("pipeline must be a list")

        common.validate_string_or_none('full_document', full_document)
        validate_collation_or_none(collation)
        common.validate_non_negative_integer_or_none("batchSize", batch_size)

        self._target = target
        self._pipeline = copy.deepcopy(pipeline)
        self._full_document = full_document
        self._resume_token = copy.deepcopy(resume_after)
        self._max_await_time_ms = max_await_time_ms
        self._batch_size = batch_size
        self._collation = collation
        self._start_at_operation_time = start_at_operation_time
        self._session = session
        self._cursor = self._create_cursor()

    @property
    def _aggregation_target(self):
        """The argument to pass to the aggregate command."""
        raise NotImplementedError

    @property
    def _database(self):
        """The database against which the aggregation commands for
        this ChangeStream will be run. """
        raise NotImplementedError

    def _pipeline_options(self):
        options = {}
        if self._full_document is not None:
            options['fullDocument'] = self._full_document
        if self._resume_token is not None:
            options['resumeAfter'] = self._resume_token
        if self._start_at_operation_time is not None:
            options['startAtOperationTime'] = self._start_at_operation_time
        return options

    def _full_pipeline(self):
        """Return the full aggregation pipeline for this ChangeStream."""
        options = self._pipeline_options()
        full_pipeline = [{'$changeStream': options}]
        full_pipeline.extend(self._pipeline)
        return full_pipeline

    def _run_aggregation_cmd(self, session, explicit_session):
        """Run the full aggregation pipeline for this ChangeStream and return
        the corresponding CommandCursor.
        """
        read_preference = self._target._read_preference_for(session)
        client = self._database.client
        with client._socket_for_reads(read_preference) as (sock_info, slave_ok):
            pipeline = self._full_pipeline()
            cmd = SON([("aggregate", self._aggregation_target),
                       ("pipeline", pipeline),
                       ("cursor", {})])

            result = sock_info.command(
                self._database.name,
                cmd,
                slave_ok,
                read_preference,
                self._target.codec_options,
                parse_write_concern_error=True,
                read_concern=self._target.read_concern,
                collation=self._collation,
                session=session,
                client=self._database.client)

            cursor = result["cursor"]

            if (self._start_at_operation_time is None and
                self._resume_token is None and
                cursor.get("_id") is None and
                sock_info.max_wire_version >= 7):
                self._start_at_operation_time = result["operationTime"]

            ns = cursor["ns"]
            _, collname = ns.split(".", 1)
            aggregation_collection = self._database.get_collection(
                collname, codec_options=self._target.codec_options,
                read_preference=read_preference,
                write_concern=self._target.write_concern,
                read_concern=self._target.read_concern
            )

            return CommandCursor(
                aggregation_collection, cursor, sock_info.address,
                batch_size=self._batch_size or 0,
                max_await_time_ms=self._max_await_time_ms,
                session=session, explicit_session=explicit_session
            )

    def _create_cursor(self):
        with self._database.client._tmp_session(self._session, close=False) as s:
            return self._run_aggregation_cmd(
                session=s,
                explicit_session=self._session is not None
            )

    def _resume(self):
        """Reestablish this change stream after a resumable error."""
        try:
            self._cursor.close()
        except PyMongoError:
            pass
        self._cursor = self._create_cursor()

    def close(self):
        """Close this ChangeStream."""
        self._cursor.close()

    def __iter__(self):
        return self

    def next(self):
        """Advance the cursor.

        This method blocks until the next change document is returned or an
        unrecoverable error is raised.

        Raises :exc:`StopIteration` if this ChangeStream is closed.
        """
        while True:
            try:
                change = self._cursor.next()
            except ConnectionFailure:
                self._resume()
                continue
            except OperationFailure as exc:
                if exc.code in _NON_RESUMABLE_GETMORE_ERRORS:
                    raise
                self._resume()
                continue
            try:
                resume_token = change['_id']
            except KeyError:
                self.close()
                raise InvalidOperation(
                    "Cannot provide resume functionality when the resume "
                    "token is missing.")
            self._resume_token = copy.copy(resume_token)
            self._start_at_operation_time = None
            return change

    __next__ = next

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class CollectionChangeStream(ChangeStream):
    """Class for creating a change stream on a collection.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.collection.Collection.watch` instead.

    .. versionadded: 3.6
    .. mongodoc:: changeStreams
    """
    @property
    def _aggregation_target(self):
        return self._target.name

    @property
    def _database(self):
        return self._target.database


class DatabaseChangeStream(ChangeStream):
    """Class for creating a change stream on all collections in a database.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.database.Database.watch` instead.

    .. versionadded: 3.7
    .. mongodoc:: changeStreams
    """
    @property
    def _aggregation_target(self):
        return 1

    @property
    def _database(self):
        return self._target


class ClusterChangeStream(DatabaseChangeStream):
    """Class for creating a change stream on all collections on a cluster.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.mongo_client.MongoClient.watch` instead.

    .. versionadded: 3.7
    .. mongodoc:: changeStreams
    """
    def _pipeline_options(self):
        options = super(ClusterChangeStream, self)._pipeline_options()
        options["allChangesForCluster"] = True
        return options
