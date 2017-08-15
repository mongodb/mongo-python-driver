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

from bson import _bson_to_dict
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument

from pymongo.errors import (ConnectionFailure, CursorNotFound,
                            InvalidOperation, PyMongoError)


class ChangeStream(object):
    """A change stream cursor.

    Should not be called directly by application developers. Use
    :meth:`~pymongo.collection.Collection.watch` instead.

    :Parameters:
      - `collection`: The watched :class:`~pymongo.collection.Collection`.
      - `pipeline`: A list of aggregation pipeline stages to append to an
        initial `$changeStream` aggregation stage.
      - `full_document` (string): The fullDocument to pass as an option
        to the $changeStream pipeline stage. Allowed values: 'default',
        'updateLookup'. When set to 'updateLookup', the change notification
        for partial updates will include both a delta describing the
        changes to the document, as well as a copy of the entire document
        that was changed from some time after the change occurred.
      - `resume_after` (optional): The logical starting point for this
        change stream.
      - `max_await_time_ms` (optional): The maximum time in milliseconds
        for the server to wait for changes before responding to a getMore
        operation.
      - `batch_size` (optional): The maximum number of documents to return
        per batch.
      - `collation` (optional): The :class:`~pymongo.collation.Collation`
        to use for the aggregation.

    .. versionadded: 3.6
    """
    def __init__(self, collection, pipeline, full_document,
                 resume_after=None, max_await_time_ms=None, batch_size=None,
                 collation=None):
        self._codec_options = collection.codec_options
        self._collection = collection.with_options(
            codec_options=DEFAULT_RAW_BSON_OPTIONS)
        self._pipeline = copy.deepcopy(pipeline)
        self._full_document = full_document
        self._resume_token = copy.deepcopy(resume_after)
        self._max_await_time_ms = max_await_time_ms
        self._batch_size = batch_size
        self._collation = collation
        self._cursor = self._create_cursor()

    def _full_pipeline(self):
        """Return the full aggregation pipeline for this ChangeStream."""
        options = {}
        if self._full_document is not None:
            options['fullDocument'] = self._full_document
        if self._resume_token is not None:
            options['resumeAfter'] = self._resume_token
        full_pipeline = [{'$changeStream': options}]
        full_pipeline.extend(self._pipeline)
        return full_pipeline

    def _create_cursor(self):
        """Initialize the cursor or raise a fatal error"""
        return self._collection.aggregate(
            self._full_pipeline(), batchSize=self._batch_size,
            collation=self._collation, maxAwaitTimeMS=self._max_await_time_ms)

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
                raw_change = self._cursor.next()
            except (ConnectionFailure, CursorNotFound):
                try:
                    self._cursor.close()
                except PyMongoError:
                    pass
                self._cursor = self._create_cursor()
                continue
            try:
                self._resume_token = raw_change['_id']
            except KeyError:
                raise InvalidOperation(
                    "Cannot provide resume functionality when the resume "
                    "token is missing.")
            if self._codec_options.document_class == RawBSONDocument:
                return raw_change
            next_change = _bson_to_dict(raw_change.raw, self._codec_options)
            next_change['_id'] = self._resume_token
            return next_change

    __next__ = next

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
