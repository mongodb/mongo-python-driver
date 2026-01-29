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
from typing import Any, Generic, Mapping, Optional, Sequence, Tuple, Union

from pymongo.message import _CursorAddress
from pymongo.typings import _Address, _DocumentType


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
    Sequence[Union[str, Tuple[str, Union[int, str, Mapping[str, Any]]]]], Mapping[str, Any]
]
_Hint = Union[str, _Sort]
