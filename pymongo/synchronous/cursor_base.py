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

"""Synchronous cursor base extending the shared agnostic cursor base."""
from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Optional

from pymongo import _csot
from pymongo.cursor_shared import _AgnosticCursorBase
from pymongo.lock import _create_lock
from pymongo.typings import _DocumentType

if TYPE_CHECKING:
    from pymongo.synchronous.client_session import ClientSession
    from pymongo.synchronous.pool import Connection

_IS_SYNC = True


class _ConnectionManager:
    """Used with exhaust cursors to ensure the connection is returned."""

    def __init__(self, conn: Connection, more_to_come: bool):
        self.conn: Optional[Connection] = conn
        self.more_to_come = more_to_come
        self._lock = _create_lock()

    def update_exhaust(self, more_to_come: bool) -> None:
        self.more_to_come = more_to_come

    def close(self) -> None:
        """Return this instance's connection to the connection pool."""
        if self.conn:
            self.conn.unpin()
            self.conn = None


class _CursorBase(_AgnosticCursorBase[_DocumentType], Generic[_DocumentType]):
    """Synchronous cursor base class."""

    @property
    def session(self) -> Optional[ClientSession]:
        """The cursor's :class:`~pymongo.client_session.ClientSession`, or None.

        .. versionadded:: 3.6
        """
        if self._session and not self._session._implicit:
            return self._session
        return None

    @abstractmethod
    def _next_batch(self, result: list, total: Optional[int] = None) -> bool:  # type: ignore[type-arg]
        ...

    def _die_lock(self) -> None:
        """Closes this cursor."""
        try:
            already_killed = self._killed
        except AttributeError:
            # ___init__ did not run to completion (or at all).
            return

        cursor_id, address = self._prepare_to_die(already_killed)
        self._collection.database.client._cleanup_cursor_lock(
            cursor_id,
            address,
            self._sock_mgr,
            self._session,
        )
        if self._session and self._session._implicit:
            self._session._attached_to_cursor = False
            self._session = None
        self._sock_mgr = None

    def close(self) -> None:
        """Explicitly close / kill this cursor."""
        self._die_lock()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    @_csot.apply
    def to_list(self, length: Optional[int] = None) -> list[_DocumentType]:
        """Converts the contents of this cursor to a list more efficiently than ``[doc for doc in cursor]``.

        To use::

          >>> cursor.to_list()

        Or, to read at most n items from the cursor::

          >>> cursor.to_list(n)

        If the cursor is empty or has no more results, an empty list will be returned.

        .. versionadded:: 4.9
        """
        res: list[_DocumentType] = []
        remaining = length
        if isinstance(length, int) and length < 1:
            raise ValueError("to_list() length must be greater than 0")
        while self.alive:
            if not self._next_batch(res, remaining):
                break
            if length is not None:
                remaining = length - len(res)
                if remaining == 0:
                    break
        return res
