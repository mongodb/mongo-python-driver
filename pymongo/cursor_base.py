# Copyright 2026-present MongoDB, Inc.
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

"""Shared IO-agnostic cursor base used by both async and sync cursor classes."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, Optional

from pymongo.message import _CursorAddress
from pymongo.typings import _DocumentType


class _AgnosticCursorBase(Generic[_DocumentType], ABC):
    """
    A cursor base class shared by both the async and sync APIs.
    All IO-specific behavior is implemented in subclasses.
    """

    # These are all typed more accurately in subclasses.
    _collection: Any
    _id: Optional[int]
    _data: Any  # deque in practice
    _address: Optional[tuple[str, Any]]
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
        """Returns the id of the cursor

        .. versionadded:: 2.2
        """
        return self._id

    @property
    def address(self) -> Optional[tuple[str, Any]]:
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
