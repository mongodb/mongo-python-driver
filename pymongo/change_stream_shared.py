# Copyright 2017-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Internal helpers for change streams, shared between the asynchronous and synchronous APIs."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar, Union, cast

from bson import CodecOptions
from bson.raw_bson import RawBSONDocument
from bson.timestamp import Timestamp
from pymongo import common
from pymongo.collation import validate_collation_or_none
from pymongo.errors import (
    ConnectionFailure,
    CursorNotFound,
    OperationFailure,
    PyMongoError,
)
from pymongo.typings import _DocumentType

if TYPE_CHECKING:
    from pymongo.typings import (
        _AgnosticClientSession,
        _AgnosticCollection,
        _AgnosticDatabase,
        _AgnosticMongoClient,
        _CollationIn,
        _Pipeline,
    )

_ClientT = TypeVar("_ClientT", bound="_AgnosticMongoClient")


def _resumable(exc: PyMongoError) -> bool:
    """Return True if given a resumable change stream error."""
    if isinstance(exc, (ConnectionFailure, CursorNotFound)):
        return True
    if isinstance(exc, OperationFailure):
        return exc.has_error_label("ResumableChangeStreamError")
    return False


class _AgnosticChangeStream(Generic[_DocumentType, _ClientT]):
    """Shared base for the sync and async ChangeStream classes."""

    def __init__(
        self,
        target: Union[
            _AgnosticMongoClient,
            _AgnosticDatabase[_DocumentType],
            _AgnosticCollection[_DocumentType],
        ],
        pipeline: Optional[_Pipeline],
        full_document: Optional[str],
        resume_after: Optional[Mapping[str, Any]],
        max_await_time_ms: Optional[int],
        batch_size: Optional[int],
        collation: Optional[_CollationIn],
        start_at_operation_time: Optional[Timestamp],
        session: Optional[_AgnosticClientSession],
        start_after: Optional[Mapping[str, Any]],
        comment: Optional[Any] = None,
        full_document_before_change: Optional[str] = None,
        show_expanded_events: Optional[bool] = None,
    ) -> None:
        if pipeline is None:
            pipeline = []
        pipeline = common.validate_list("pipeline", pipeline)
        common.validate_string_or_none("full_document", full_document)
        validate_collation_or_none(collation)
        common.validate_non_negative_integer_or_none("batchSize", batch_size)

        self._decode_custom = False
        self._orig_codec_options: CodecOptions[_DocumentType] = target.codec_options
        if target.codec_options.type_registry._decoder_map:
            self._decode_custom = True
            # Keep the type registry so that we support encoding custom types
            # in the pipeline.
            self._target = target.with_options(  # type: ignore
                codec_options=target.codec_options.with_options(document_class=RawBSONDocument)
            )
        else:
            self._target = target

        self._pipeline = copy.deepcopy(pipeline)
        self._full_document = full_document
        self._full_document_before_change = full_document_before_change
        self._uses_start_after = start_after is not None
        self._uses_resume_after = resume_after is not None
        self._resume_token = copy.deepcopy(start_after or resume_after)
        self._max_await_time_ms = max_await_time_ms
        self._batch_size = batch_size
        self._collation = collation
        self._start_at_operation_time = start_at_operation_time
        self._session = session
        self._comment = comment
        self._closed = False
        self._timeout = self._target._timeout
        self._show_expanded_events = show_expanded_events

    # Any: the async/sync _AggregationCommand classes are unrelated, so no common return type.
    @property
    def _aggregation_command_class(self) -> type[Any]:
        """The aggregation command class to be used."""
        raise NotImplementedError

    @property
    def _client(self) -> _ClientT:
        """The client against which the aggregation commands for
        this ChangeStream will be run.
        """
        raise NotImplementedError

    def _change_stream_options(self) -> dict[str, Any]:
        """Return the options dict for the $changeStream pipeline stage."""
        options: dict[str, Any] = {}
        if self._full_document is not None:
            options["fullDocument"] = self._full_document

        if self._full_document_before_change is not None:
            options["fullDocumentBeforeChange"] = self._full_document_before_change

        resume_token = self.resume_token
        if resume_token is not None:
            if self._uses_start_after:
                options["startAfter"] = resume_token
            else:
                options["resumeAfter"] = resume_token

        elif self._start_at_operation_time is not None:
            options["startAtOperationTime"] = self._start_at_operation_time

        if self._show_expanded_events:
            options["showExpandedEvents"] = self._show_expanded_events

        return options

    def _command_options(self) -> dict[str, Any]:
        """Return the options dict for the aggregation command."""
        options = {}
        if self._max_await_time_ms is not None:
            options["maxAwaitTimeMS"] = self._max_await_time_ms
        if self._batch_size is not None:
            options["batchSize"] = self._batch_size
        return options

    def _aggregation_pipeline(self) -> list[dict[str, Any]]:
        """Return the full aggregation pipeline for this ChangeStream."""
        options = self._change_stream_options()
        full_pipeline: list[dict[str, Any]] = [{"$changeStream": options}]
        full_pipeline.extend(self._pipeline)
        return full_pipeline

    def _process_result(self, result: Mapping[str, Any]) -> None:
        """Callback that caches the postBatchResumeToken or
        startAtOperationTime from a changeStream aggregate command response
        containing an empty batch of change documents.
        """
        if not result["cursor"]["firstBatch"]:
            if "postBatchResumeToken" in result["cursor"]:
                self._resume_token = result["cursor"]["postBatchResumeToken"]
            elif (
                self._start_at_operation_time is None
                and self._uses_resume_after is False
                and self._uses_start_after is False
            ):
                self._start_at_operation_time = result.get("operationTime")
                # PYTHON-2181: informative error on missing operationTime.
                if self._start_at_operation_time is None:
                    raise OperationFailure(
                        f"Expected field 'operationTime' missing from command response : {result!r}"
                    )

    @property
    def resume_token(self) -> Optional[Mapping[str, Any]]:
        """The cached resume token that will be used to resume after the most
        recently returned change.

        .. versionadded:: 3.9
        """
        return copy.deepcopy(self._resume_token)

    @property
    def alive(self) -> bool:
        """Does this cursor have the potential to return more data?

        .. note:: Even if :attr:`alive` is ``True``, :meth:`next` can raise
            :exc:`StopIteration` (sync) or :exc:`StopAsyncIteration` (async),
            and :meth:`try_next` can return ``None``.

        .. versionadded:: 3.8
        """
        return not self._closed


class _AgnosticCollectionChangeStream(_AgnosticChangeStream[_DocumentType, _ClientT]):
    """A change stream that watches changes on a single collection.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.collection.Collection.watch` instead.

    .. versionadded:: 3.7
    """

    _target: _AgnosticCollection[_DocumentType]

    @property
    def _client(self) -> _ClientT:
        return cast("_ClientT", self._target.database.client)


class _AgnosticDatabaseChangeStream(_AgnosticChangeStream[_DocumentType, _ClientT]):
    """A change stream that watches changes on all collections in a database.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.database.Database.watch` instead.

    .. versionadded:: 3.7
    """

    _target: _AgnosticDatabase[_DocumentType]

    @property
    def _client(self) -> _ClientT:
        return cast("_ClientT", self._target.client)


class _AgnosticClusterChangeStream(_AgnosticDatabaseChangeStream[_DocumentType, _ClientT]):
    """A change stream that watches changes on all collections in the cluster.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.mongo_client.MongoClient.watch` instead.

    .. versionadded:: 3.7
    """

    def _change_stream_options(self) -> dict[str, Any]:
        options = super()._change_stream_options()
        options["allChangesForCluster"] = True
        return options
