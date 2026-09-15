# Copyright 2024-present MongoDB, Inc.
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


"""Constants and types shared across all cursor classes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic, Optional, Union

from bson import CodecOptions
from pymongo.message import _CursorAddress, _GetMore, _OpMsg
from pymongo.typings import (
    _Address,
    _AgnosticClientSession,
    _AgnosticCollection,
    _DocumentOut,
    _DocumentType,
)

if TYPE_CHECKING:
    from typing_extensions import Self

_CURSOR_DOC_FIELDS = {"cursor": {"firstBatch": 1, "nextBatch": 1}}


def _split_message(
    message: Union[tuple[int, bytes], tuple[int, bytes, int]],
) -> tuple[int, bytes, int]:
    """Return request_id, data, max_doc_size.

    :param message: (request_id, data, max_doc_size) or (request_id, data)
    """
    if len(message) == 3:
        return message
    # get_more and kill_cursors messages don't include BSON documents.
    request_id, data = message
    return request_id, data, 0


class _AgnosticCursorBase(Generic[_DocumentType], ABC):
    """
    Shared IO-agnostic cursor base used by both async and sync cursor classes.
    All IO-specific behavior is implemented in subclasses.
    """

    # These are all typed more accurately in subclasses.
    _collection: Any
    _id: Optional[int]
    _data: Any
    _address: Optional[_Address]
    _sock_mgr: Any
    _session: Optional[Any]
    _killed: bool

    @abstractmethod
    def _get_namespace(self) -> str:
        """Return the full namespace (dbname.collname) for this cursor."""
        ...

    def __del__(self) -> None:
        self._die_no_lock()

    @property
    def alive(self) -> bool:
        """Does this cursor have the potential to return more data?

        This is mostly useful with `tailable cursors
        <https://www.mongodb.com/docs/manual/core/tailable-cursors/>`_
        since they will stop iterating even though they *may* return more
        results in the future.

        With regular cursors, simply use an asynchronous for loop instead of :attr:`alive`::

            async for doc in collection.find():
                print(doc)

        .. note:: Even if :attr:`alive` is True, :meth:`next` can raise
          :exc:`StopIteration`. :attr:`alive` can also be True while iterating
          a cursor from a failed server. In this case :attr:`alive` will
          return False after :meth:`next` fails to retrieve the next batch
          of results from the server.
        """
        return bool(len(self._data) or (not self._killed))

    @property
    def cursor_id(self) -> Optional[int]:
        """Returns the id of the cursor.

        .. versionadded:: 2.2
        """
        return self._id

    @property
    def address(self) -> Optional[_Address]:
        """The (host, port) of the server used, or None.

        .. versionchanged:: 3.0
           Renamed from "conn_id".
        """
        return self._address

    def _prepare_to_die(self, already_killed: bool) -> tuple[int, Optional[_CursorAddress]]:
        self._killed = True
        if self._id and not already_killed:
            cursor_id = self._id
            assert self._address is not None
            address = _CursorAddress(self._address, self._get_namespace())
        else:
            # Skip killCursors.
            cursor_id = 0
            address = None
        return cursor_id, address

    def _die_no_lock(self) -> None:
        """Closes this cursor without acquiring a lock."""
        try:
            already_killed = self._killed
        except AttributeError:
            # ___init__ did not run to completion (or at all).
            return

        cursor_id, address = self._prepare_to_die(already_killed)
        self._collection.database.client._cleanup_cursor_no_lock(
            cursor_id, address, self._sock_mgr, self._session
        )
        if self._session and self._session._implicit:
            self._session._attached_to_cursor = False
            self._session = None
        self._sock_mgr = None


class _AgnosticCommandCursorBase(_AgnosticCursorBase[_DocumentType]):
    """An agnostic cursor / iterator over command cursors.
    Used by aggregate, list_indexes, list_search_indexes, list_collections, cursor_command,
    and list_databases helpers on both synchronous and asynchronous APIs to iterate MongoDB
    command results.

    Should not be called directly by application developers.
    """

    _getmore_class = _GetMore

    def __init__(
        self,
        collection: _AgnosticCollection[_DocumentType],
        cursor_info: Mapping[str, Any],
        address: Optional[_Address],
        batch_size: int = 0,
        max_await_time_ms: Optional[int] = None,
        session: Optional[_AgnosticClientSession] = None,
        comment: Any = None,
    ) -> None:
        """Create a new command cursor."""
        self._sock_mgr: Any = None
        self._collection = collection
        self._id = cursor_info["id"]
        self._data = deque(cursor_info["firstBatch"])
        self._postbatchresumetoken: Optional[Mapping[str, Any]] = cursor_info.get(
            "postBatchResumeToken"
        )
        self._address = address
        self._batch_size = batch_size
        self._max_await_time_ms = max_await_time_ms
        self._timeout = self._collection.database.client.options.timeout
        self._session = session
        if self._session is not None:
            self._session._attached_to_cursor = True
        self._killed = self._id == 0
        self._comment = comment
        if self._killed:
            self._end_session()

        if "ns" in cursor_info:
            self._ns = cursor_info["ns"]
        else:
            self._ns = collection.full_name

        self.batch_size(batch_size)

        if not isinstance(max_await_time_ms, int) and max_await_time_ms is not None:
            raise TypeError(
                f"max_await_time_ms must be an integer or None, not {type(max_await_time_ms)}"
            )

    def _get_namespace(self) -> str:
        return self._ns

    def batch_size(self, batch_size: int) -> Self:
        """Limits the number of documents returned in one batch. Each batch
        requires a round trip to the server. It can be adjusted to optimize
        performance and limit data transfer.

        .. note:: batch_size can not override MongoDB's internal limits on the
           amount of data it will return to the client in a single batch (i.e
           if you set batch size to 1,000,000,000, MongoDB will currently only
           return 4-16MB of results per batch).

        Raises :exc:`TypeError` if `batch_size` is not an integer.
        Raises :exc:`ValueError` if `batch_size` is less than ``0``.

        :param batch_size: The size of each batch of results requested.
        """
        if not isinstance(batch_size, int):
            raise TypeError(f"batch_size must be an integer, not {type(batch_size)}")
        if batch_size < 0:
            raise ValueError("batch_size must be >= 0")

        self._batch_size = (batch_size == 1 and 2) or batch_size
        return self

    def _has_next(self) -> bool:
        """Returns `True` if the cursor has documents remaining from the
        previous batch.
        """
        return len(self._data) > 0

    @property
    def _post_batch_resume_token(self) -> Optional[Mapping[str, Any]]:
        """Retrieve the postBatchResumeToken from the response to a
        changeStream aggregate or getMore.
        """
        return self._postbatchresumetoken

    def _unpack_response(
        self,
        response: _OpMsg,
        cursor_id: Optional[int],
        codec_options: CodecOptions[Mapping[str, Any]],
        user_fields: Optional[Mapping[str, Any]] = None,
        legacy_response: bool = False,
    ) -> Sequence[_DocumentOut]:
        return response.unpack_response(cursor_id, codec_options, user_fields, legacy_response)

    def _end_session(self) -> None:
        if self._session and self._session._implicit:
            self._session._attached_to_cursor = False
            self._session._end_implicit_session()
            self._session = None


# These errors mean that the server has already killed the cursor so there is
# no need to send killCursors.
_CURSOR_CLOSED_ERRORS = frozenset(
    [
        43,  # CursorNotFound
        175,  # QueryPlanKilled
        237,  # CursorKilled
        # On a tailable cursor, the following errors mean the capped collection
        # rolled over.
        # MongoDB 2.6:
        # {'$err': 'Runner killed during getMore', 'code': 28617, 'ok': 0}
        28617,
        # MongoDB 3.0:
        # {'$err': 'getMore executor error: UnknownError no details available',
        #  'code': 17406, 'ok': 0}
        17406,
        # MongoDB 3.2 + 3.4:
        # {'ok': 0.0, 'errmsg': 'GetMore command executor error:
        #  CappedPositionLost: CollectionScan died due to failure to restore
        #  tailable cursor position. Last seen record id: RecordId(3)',
        #  'code': 96}
        96,
        # MongoDB 3.6+:
        # {'ok': 0.0, 'errmsg': 'errmsg: "CollectionScan died due to failure to
        #  restore tailable cursor position. Last seen record id: RecordId(3)"',
        #  'code': 136, 'codeName': 'CappedPositionLost'}
        136,
    ]
)

_QUERY_OPTIONS = {
    "tailable_cursor": 2,
    "secondary_okay": 4,
    "oplog_replay": 8,
    "no_timeout": 16,
    "await_data": 32,
    "exhaust": 64,
    "partial": 128,
}


class CursorType:
    NON_TAILABLE = 0
    """The standard cursor type."""

    TAILABLE = _QUERY_OPTIONS["tailable_cursor"]
    """The tailable cursor type.

    Tailable cursors are only for use with capped collections. They are not
    closed when the last data is retrieved but are kept open and the cursor
    location marks the final document position. If more data is received
    iteration of the cursor will continue from the last document received.
    """

    TAILABLE_AWAIT = TAILABLE | _QUERY_OPTIONS["await_data"]
    """A tailable cursor with the await option set.

    Creates a tailable cursor that will wait for a few seconds after returning
    the full result set so that it can capture and return additional data added
    during the query.
    """

    EXHAUST = _QUERY_OPTIONS["exhaust"]
    """An exhaust cursor.

    MongoDB will stream batched results to the client without waiting for the
    client to request each batch, reducing latency.
    """


_Sort = Union[
    Sequence[Union[str, tuple[str, Union[int, str, Mapping[str, Any]]]]], Mapping[str, Any]
]
_Hint = Union[str, _Sort]
