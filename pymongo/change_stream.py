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
from pymongo.errors import (ConnectionFailure, CursorNotFound,
                            InvalidOperation, PyMongoError)


class ChangeStream(object):
    """The change stream cursor abstract base class.

    Defines the interface for change streams. Should be subclassed to
    implement the `ChangeStream._create_cursor` abstract method and
    the `ChangeStream._database` abstract property.
    """
    def __init__(self, target, pipeline, full_document,
                 resume_after=None, max_await_time_ms=None, batch_size=None,
                 collation=None, start_at_operation_time=None, session=None):
        # Validate inputs
        if not isinstance(pipeline, list):
            raise TypeError("pipeline must be a list")
        common.validate_string_or_none('full_document', full_document)

        # Initialize class
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
    def _default_start_at_operation_time(self):
        """
        Return the current operationTime timestamp from the server. 
        Returns None if the resumeAfter option has been set.
        """
        return self._database.command('isMaster')['operationTime']

    def _create_cursor(self):
        """
        Instantiate the cursor corresponding to this ChangeStream.
        """
        raise NotImplementedError

    def _full_pipeline(self, inject_options=None):
        """Return the full aggregation pipeline for this ChangeStream."""
        options = {}
        if self._full_document is not None:
            options['fullDocument'] = self._full_document
        if self._resume_token is not None:
            options['resumeAfter'] = self._resume_token
        else:
            options['startAtOperationTime'] = (
                self._start_at_operation_time or 
                self._default_start_at_operation_time
            )

        if inject_options is not None:
            options.update(inject_options)
        full_pipeline = [{'$changeStream': options}]
        full_pipeline.extend(self._pipeline)
        return full_pipeline

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
            except (ConnectionFailure, CursorNotFound):
                try:
                    self._cursor.close()
                except PyMongoError:
                    pass
                self._cursor = self._create_cursor()
                continue
            try:
                resume_token = change['_id']
            except KeyError:
                self.close()
                raise InvalidOperation(
                    "Cannot provide resume functionality when the resume "
                    "token is missing.")
            self._resume_token = copy.copy(resume_token)
            return change

    __next__ = next

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def _database(self):
        """The database against which the aggregation commands for
        this ChangeStream will be run. """
        raise NotImplementedError


class ChangeStreamCollection(ChangeStream):
    """ Class for creating a change stream on a collection. 

    Should not be called directly by application developers. Use
    helper method :meth:`~pymongo.collection.Collection.watch` instead.

    .. versionadded: 3.6
    .. mongodoc:: changeStreams
    """

    def _create_cursor(self):
        return self._target.aggregate(
            self._full_pipeline(), self._session, batchSize=self._batch_size,
            collation=self._collation, maxAwaitTimeMS=self._max_await_time_ms,
        )

    @property
    def _database(self):
        return self._target.database


class ChangeStreamDatabase(ChangeStream):
    """ Class for creating a change stream on all collections in a database.

    Should not be called directly by application developers. Use
    helper method :meth:`~pymongo.database.Database.watch` instead.

    .. versionadded: 3.7
    .. mongodoc:: changeStreams
    """

    def _run_aggregation_cmd(self, pipeline, session, explicit_session):
        """Run the full aggregation pipeline for this ChangeStream and return
        the corresponding CommandCursor.
        """
        common.validate_list('pipeline', pipeline)
        validate_collation_or_none(self._collation)
        common.validate_non_negative_integer_or_none(
            "batchSize", self._batch_size
        )

        cmd = SON([("aggregate", 1),
                   ("pipeline", pipeline),
                   ("cursor", {})])

        with self._database.client._socket_for_reads(self._target._read_preference_for(session)) as (sock_info, slave_ok):
            # Avoid auto-injecting a session: aggregate() passes a session,
            # aggregate_raw_batches() passes none.
            result = sock_info.command(
                self._database.name,
                cmd,
                slave_ok,
                self._target._read_preference_for(session),
                self._target.codec_options,
                parse_write_concern_error=True,
                read_concern=self._target.read_concern,
                collation=self._collation,
                session=session,
                client=self._database.client)

            cursor = result["cursor"]
            ns = cursor["ns"]
            _, collname = ns.split(".", 1)
            aggregation_collection = self._database.get_collection(
                collname, codec_options=self._target.codec_options,
                read_preference=self._target._read_preference_for(session),
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
                self._full_pipeline(),
                session=s,
                explicit_session=self._session is not None
            )

    @property
    def _database(self):
        return self._target


class ChangeStreamClient(ChangeStreamDatabase):
    """ Class for creating a change stream on all collections on a cluster. 
    
    Should not be called directly by application developers. Use
    helper method :meth:`~pymongo.mongo_client.MongoClient.watch` instead.

    .. versionadded: 3.7
    .. mongodoc:: changeStreams
    """

    def _full_pipeline(self, inject_options=None):
        options = {"allChangesForCluster": True}
        if inject_options is not None:
            options.update(inject_options)
        full_pipeline = super(ChangeStreamClient, self)._full_pipeline(
            inject_options=options
        )
        return full_pipeline

    @property
    def _database(self):
        # $changeStream aggregation operations are performed on admin DB.
        return getattr(self._target, "admin")