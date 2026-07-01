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
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from pymongo import _csot
from pymongo.cursor_shared import _AgnosticCursorBase
from pymongo.lock import _create_lock
from pymongo.message import _GetMore, _OpMsg, _Query
from pymongo.response import PinnedResponse, Response
from pymongo.synchronous.command_runner import run_cursor_command
from pymongo.synchronous.helpers import _handle_reauth
from pymongo.typings import _DocumentOut, _DocumentType

if TYPE_CHECKING:
    from pymongo.read_preferences import _ServerMode
    from pymongo.synchronous.client_session import ClientSession
    from pymongo.synchronous.pool import Connection

_IS_SYNC = True

_CURSOR_DOC_FIELDS = {"cursor": {"firstBatch": 1, "nextBatch": 1}}


def _split_message(
    message: Union[tuple[int, Any], tuple[int, Any, int]],
) -> tuple[int, Any, int]:
    """Return request_id, data, max_doc_size.

    :param message: (request_id, data, max_doc_size) or (request_id, data)
    """
    if len(message) == 3:
        return message  # type: ignore[return-value]
    # get_more and kill_cursors messages don't include BSON documents.
    request_id, data = message  # type: ignore[misc]
    return request_id, data, 0


def _operation_to_command(
    operation: Union[_Query, _GetMore],
    conn: Connection,
    use_cmd: bool,
) -> tuple[dict[str, Any], str]:
    cmd, db = operation.as_command(conn, use_cmd)
    if operation.client._encrypter and not operation.client._encrypter._bypass_auto_encryption:
        cmd = operation.client._encrypter.encrypt(  # type: ignore[misc, assignment]
            operation.db, cmd, operation.codec_options
        )
    operation.update_command(cmd)
    return cmd, db


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


class _CursorBase(_AgnosticCursorBase[_DocumentType]):
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

    @abstractmethod
    def _unpack_response(
        self,
        response: _OpMsg,
        cursor_id: Optional[int],
        codec_options: Any,
        user_fields: Optional[Mapping[str, Any]] = None,
        legacy_response: bool = False,
    ) -> Sequence[_DocumentOut]: ...

    @_handle_reauth
    def _run_with_conn(
        self,
        conn: Connection,
        operation: Union[_Query, _GetMore],
        read_preference: _ServerMode,
    ) -> Response:
        """Execute a cursor operation on the given connection and return a Response."""
        client = self._collection.database.client
        use_cmd = operation.use_command(conn)
        more_to_come = bool(operation.conn_mgr and operation.conn_mgr.more_to_come)
        cmd, dbn = _operation_to_command(operation, conn, use_cmd)
        if more_to_come:
            request_id, data, max_doc_size = 0, b"", 0
        else:
            message = operation.get_message(read_preference, conn, use_cmd)
            request_id, data, max_doc_size = _split_message(message)
        user_fields = _CURSOR_DOC_FIELDS if use_cmd else None
        docs, reply, duration = run_cursor_command(
            conn,
            cmd,
            dbn,
            request_id,
            data,
            client=client,
            session=operation.session,  # type: ignore[arg-type]
            listeners=client._event_listeners,
            codec_options=operation.codec_options,
            user_fields=user_fields,
            command_name=operation.name,
            pool_opts=conn.opts,
            max_doc_size=max_doc_size,
            more_to_come=more_to_come,
            unpack_res=self._unpack_response,
            cursor_id=operation.cursor_id,
        )
        assert reply is not None
        if client._should_pin_cursor(operation.session) or operation.exhaust:  # type: ignore[arg-type]
            conn.pin_cursor()
            if isinstance(reply, _OpMsg):
                # In OP_MSG, the server keeps sending only if the more_to_come flag is set.
                more_to_come = reply.more_to_come
            else:
                # In OP_REPLY, the server keeps sending until cursor_id is 0.
                more_to_come = bool(operation.exhaust and reply.cursor_id)
            if operation.conn_mgr:
                operation.conn_mgr.update_exhaust(more_to_come)
            return PinnedResponse(
                data=reply,
                address=conn.address,
                conn=conn,
                duration=duration,
                request_id=request_id,
                from_command=use_cmd,
                docs=docs,  # type: ignore[arg-type]
                more_to_come=more_to_come,
            )
        return Response(
            data=reply,
            address=conn.address,
            duration=duration,
            request_id=request_id,
            from_command=use_cmd,
            docs=docs,  # type: ignore[arg-type]
        )

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
