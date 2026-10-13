# Copyright 2017 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Watch changes on a collection, a database, or the entire cluster."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from bson import _bson_to_dict
from pymongo import _csot
from pymongo.change_stream_shared import (
    _AgnosticChangeStream,
    _AgnosticClusterChangeStream,
    _AgnosticCollectionChangeStream,
    _AgnosticDatabaseChangeStream,
    _resumable,
)
from pymongo.errors import (
    InvalidOperation,
    PyMongoError,
)
from pymongo.operations import _Op
from pymongo.synchronous.aggregation import (
    _CollectionAggregationCommand,
    _DatabaseAggregationCommand,
)
from pymongo.synchronous.command_cursor import CommandCursor
from pymongo.typings import _DocumentType

_IS_SYNC = True

if TYPE_CHECKING:
    from pymongo.synchronous.client_session import ClientSession
    from pymongo.synchronous.collection import Collection
    from pymongo.synchronous.database import Database
    from pymongo.synchronous.mongo_client import MongoClient  # noqa: F401


class ChangeStream(_AgnosticChangeStream[_DocumentType, "MongoClient[_DocumentType]"]):
    """The internal abstract base class for change stream cursors.

    Should not be called directly by application developers. Use
    :meth:`pymongo.collection.Collection.watch`,
    :meth:`pymongo.database.Database.watch`, or
    :meth:`pymongo.mongo_client.MongoClient.watch` instead.

    .. versionadded:: 3.6
    .. seealso:: The MongoDB documentation on `changeStreams <https://mongodb.com/docs/manual/changeStreams/>`_.
    """

    _session: Optional[ClientSession]

    def _initialize_cursor(self) -> None:
        # Initialize cursor.
        self._cursor = self._create_cursor()

    def _run_aggregation_cmd(self, session: Optional[ClientSession]) -> CommandCursor:  # type: ignore[type-arg]
        """Run the full aggregation pipeline for this ChangeStream and return
        the corresponding CommandCursor.
        """
        cmd = self._aggregation_command_class(
            self._target,
            CommandCursor,
            self._aggregation_pipeline(),
            self._command_options(),
            result_processor=self._process_result,
            comment=self._comment,
        )
        return self._client._retryable_read(
            cmd.get_cursor,
            self._target._read_preference_for(session),
            session,
            operation=_Op.AGGREGATE,
        )

    def _create_cursor(self) -> CommandCursor:  # type: ignore[type-arg]
        with self._client._tmp_session(self._session) as s:
            return self._run_aggregation_cmd(session=s)

    def _resume(self) -> None:
        """Reestablish this change stream after a resumable error."""
        try:
            self._cursor.close()
        except PyMongoError:
            pass
        self._cursor = self._create_cursor()

    def close(self) -> None:
        """Close this ChangeStream."""
        self._closed = True
        self._cursor.close()

    def __iter__(self) -> ChangeStream[_DocumentType]:
        return self

    @_csot.apply
    def next(self) -> _DocumentType:
        """Advance the cursor.

        This method blocks until the next change document is returned or an
        unrecoverable error is raised. This method is used when iterating over
        all changes in the cursor. For example::

            try:
                resume_token = None
                pipeline = [{'$match': {'operationType': 'insert'}}]
                with db.collection.watch(pipeline) as stream:
                    for insert_change in stream:
                        print(insert_change)
                        resume_token = stream.resume_token
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                if resume_token is None:
                    # There is no usable resume token because there was a
                    # failure during ChangeStream initialization.
                    logging.error('...')
                else:
                    # Use the interrupted ChangeStream's resume token to create
                    # a new ChangeStream. The new stream will continue from the
                    # last seen insert change without missing any events.
                    with db.collection.watch(
                            pipeline, resume_after=resume_token) as stream:
                        for insert_change in stream:
                            print(insert_change)

        Raises :exc:`StopIteration` if this ChangeStream is closed.
        """
        while self.alive:
            doc = self.try_next()
            if doc is not None:
                return doc

        raise StopIteration

    __next__ = next

    @_csot.apply
    def try_next(self) -> Optional[_DocumentType]:
        """Advance the cursor without blocking indefinitely.

        This method returns the next change document without waiting
        indefinitely for the next change. For example::

            with db.collection.watch() as stream:
                while stream.alive:
                    change = stream.try_next()
                    # Note that the ChangeStream's resume token may be updated
                    # even when no changes are returned.
                    print("Current resume token: %r" % (stream.resume_token,))
                    if change is not None:
                        print("Change document: %r" % (change,))
                        continue
                    # We end up here when there are no recent changes.
                    # Sleep for a while before trying again to avoid flooding
                    # the server with getMore requests when no changes are
                    # available.
                    time.sleep(10)

        If no change document is cached locally then this method runs a single
        getMore command. If the getMore yields any documents, the next
        document is returned, otherwise, if the getMore returns no documents
        (because there have been no changes) then ``None`` is returned.

        :return: The next change document or ``None`` when no document is available
          after running a single getMore or when the cursor is closed.

        .. versionadded:: 3.8
        """
        if not self._closed and not self._cursor.alive:
            self._resume()

        # Attempt to get the next change with at most one getMore and at most
        # one resume attempt.
        try:
            try:
                change = self._cursor._try_next(True)
            except PyMongoError as exc:
                if not _resumable(exc):
                    raise
                self._resume()
                change = self._cursor._try_next(False)
        except PyMongoError as exc:
            # Close the stream after a fatal error.
            if not _resumable(exc) and not exc.timeout:
                self.close()
            raise
        # Catch KeyboardInterrupt, CancelledError, etc. and cleanup.
        except BaseException:
            self.close()
            raise

        # Check if the cursor was invalidated.
        if not self._cursor.alive:
            self._closed = True

        # If no changes are available.
        if change is None:
            # We have either iterated over all documents in the cursor,
            # OR the most-recently returned batch is empty. In either case,
            # update the cached resume token with the postBatchResumeToken if
            # one was returned. We also clear the startAtOperationTime.
            if self._cursor._post_batch_resume_token is not None:
                self._resume_token = self._cursor._post_batch_resume_token
                self._start_at_operation_time = None
            return change

        # Else, changes are available.
        try:
            resume_token = change["_id"]
        except KeyError:
            self.close()
            raise InvalidOperation(
                "Cannot provide resume functionality when the resume token is missing."
            ) from None

        # If this is the last change document from the current batch, cache the
        # postBatchResumeToken.
        if not self._cursor._has_next() and self._cursor._post_batch_resume_token:
            resume_token = self._cursor._post_batch_resume_token

        # Hereafter, don't use startAfter; instead use resumeAfter.
        self._uses_start_after = False
        self._uses_resume_after = True

        # Cache the resume token and clear startAtOperationTime.
        self._resume_token = resume_token
        self._start_at_operation_time = None

        if self._decode_custom:
            return _bson_to_dict(change.raw, self._orig_codec_options)
        return change

    def __enter__(self) -> ChangeStream[_DocumentType]:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class CollectionChangeStream(
    ChangeStream[_DocumentType],
    _AgnosticCollectionChangeStream[_DocumentType, "MongoClient[_DocumentType]"],
):
    """A change stream that watches changes on a single collection.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.collection.Collection.watch` instead.

    .. versionadded:: 3.7
    """

    _target: Collection[_DocumentType]

    @property
    def _aggregation_command_class(self) -> type[_CollectionAggregationCommand]:
        return _CollectionAggregationCommand


class DatabaseChangeStream(
    ChangeStream[_DocumentType],
    _AgnosticDatabaseChangeStream[_DocumentType, "MongoClient[_DocumentType]"],
):
    """A change stream that watches changes on all collections in a database.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.database.Database.watch` instead.

    .. versionadded:: 3.7
    """

    _target: Database[_DocumentType]

    @property
    def _aggregation_command_class(self) -> type[_DatabaseAggregationCommand]:
        return _DatabaseAggregationCommand


class ClusterChangeStream(
    DatabaseChangeStream[_DocumentType],
    _AgnosticClusterChangeStream[_DocumentType, "MongoClient[_DocumentType]"],
):
    """A change stream that watches changes on all collections in the cluster.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.mongo_client.MongoClient.watch` instead.

    .. versionadded:: 3.7
    """
