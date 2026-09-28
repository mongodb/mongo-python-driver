# Copyright 2009-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cursor class to iterate over Mongo query results."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    NoReturn,
    Optional,
    Union,
    cast,
    overload,
)

from bson import _convert_raw_document_lists_to_streams
from pymongo.asynchronous.cursor_base import _AsyncCursorBase, _ConnectionManager
from pymongo.asynchronous.helpers import anext
from pymongo.cursor_shared import (
    _CURSOR_CLOSED_ERRORS,
    _QUERY_OPTIONS,
    CursorType,
    _AgnosticCursor,
)
from pymongo.errors import ConnectionFailure, InvalidOperation, OperationFailure
from pymongo.message import (
    _GetMore,
    _OpMsg,
    _Query,
    _RawBatchGetMore,
    _RawBatchQuery,
)
from pymongo.response import PinnedResponse
from pymongo.typings import _DocumentOut, _DocumentType

if TYPE_CHECKING:
    from bson.codec_options import CodecOptions
    from pymongo.asynchronous.collection import AsyncCollection

_IS_SYNC = False


class AsyncCursor(_AgnosticCursor[_DocumentType], _AsyncCursorBase[_DocumentType]):
    @property
    def collection(self) -> AsyncCollection[_DocumentType]:
        """The :class:`~pymongo.asynchronous.collection.AsyncCollection` that this
        :class:`AsyncCursor` is iterating.
        """
        return self._collection

    async def add_option(self, mask: int) -> AsyncCursor[_DocumentType]:
        """Set arbitrary query flags using a bitmask.

        To set the tailable flag:
        cursor.add_option(2)
        """
        if not isinstance(mask, int):
            raise TypeError(f"mask must be an int, not {type(mask)}")
        self._check_okay_to_chain()

        if mask & _QUERY_OPTIONS["exhaust"]:
            if self._limit:
                raise InvalidOperation("Can't use limit and exhaust together.")
            self._exhaust = True

        self._query_flags |= mask
        return self

    @overload
    def __getitem__(self, index: int) -> _DocumentType: ...

    @overload
    def __getitem__(self, index: slice) -> AsyncCursor[_DocumentType]: ...

    def __getitem__(
        self, index: Union[int, slice]
    ) -> Union[_DocumentType, AsyncCursor[_DocumentType]]:
        """Get a single document or a slice of documents from this cursor.

        .. warning:: A :class:`~AsyncCursor` is not a Python :class:`list`. Each
          index access or slice requires that a new query be run using skip
          and limit. Do not iterate the cursor using index accesses.
          The following example is **extremely inefficient** and may return
          surprising results::

            cursor = db.collection.find()
            # Warning: This runs a new query for each document.
            # Don't do this!
            for idx in range(10):
                print(cursor[idx])

        Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used.

        To get a single document use an integral index, e.g.::

          >>> db.test.find()[50]

        An :class:`IndexError` will be raised if the index is negative
        or greater than the amount of documents in this cursor. Any
        limit previously applied to this cursor will be ignored.

        To get a slice of documents use a slice index, e.g.::

          >>> db.test.find()[20:25]

        This will return this cursor with a limit of ``5`` and skip of
        ``20`` applied.  Using a slice index will override any prior
        limits or skips applied to this cursor (including those
        applied through previous calls to this method). Raises
        :class:`IndexError` when the slice has a step, a negative
        start value, or a stop value less than or equal to the start
        value.

        :param index: An integer or slice index to be applied to this cursor
        """
        if _IS_SYNC:
            self._check_okay_to_chain()
            self._empty = False
            if isinstance(index, slice):
                if index.step is not None:
                    raise IndexError("AsyncCursor instances do not support slice steps")

                skip = 0
                if index.start is not None:
                    if index.start < 0:
                        raise IndexError("AsyncCursor instances do not support negative indices")
                    skip = index.start

                if index.stop is not None:
                    limit = index.stop - skip
                    if limit < 0:
                        raise IndexError(
                            f"stop index must be greater than start index for slice {index!r}"
                        )
                    if limit == 0:
                        self._empty = True
                else:
                    limit = 0

                self._skip = skip
                self._limit = limit
                return self

            if isinstance(index, int):
                if index < 0:
                    raise IndexError("AsyncCursor instances do not support negative indices")
                clone = self.clone()
                clone.skip(index + self._skip)
                clone.limit(-1)  # use a hard limit
                clone._query_flags &= ~CursorType.TAILABLE_AWAIT  # PYTHON-1371
                for doc in clone:  # type: ignore[attr-defined]
                    return doc
                raise IndexError("no such item for AsyncCursor instance")
            raise TypeError(f"index {index!r} cannot be applied to AsyncCursor instances")
        else:
            raise IndexError("AsyncCursor does not support indexing")

    async def explain(self) -> _DocumentType:
        """Returns an explain plan record for this cursor.

        .. note:: This method uses the default verbosity mode of the
          `explain command
          <https://mongodb.com/docs/manual/reference/command/explain/>`_,
          ``allPlansExecution``. To use a different verbosity use
          :meth:`~pymongo.asynchronous.database.AsyncDatabase.command` to run the explain
          command directly.

        .. note:: The timeout of this method can be set using :func:`pymongo.timeout`.

        .. seealso:: The MongoDB documentation on `explain <https://dochub.mongodb.org/core/explain>`_.
        """
        c = self.clone()
        c._explain = True

        # always use a hard limit for explains
        if c._limit:
            c._limit = -abs(c._limit)
        return await anext(c)

    async def distinct(self, key: str) -> list[Any]:
        """Get a list of distinct values for `key` among all documents
        in the result set of this query.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`str`.

        The :meth:`distinct` method obeys the
        :attr:`~pymongo.asynchronous.collection.AsyncCollection.read_preference` of the
        :class:`~pymongo.asynchronous.collection.AsyncCollection` instance on which
        :meth:`~pymongo.asynchronous.collection.AsyncCollection.find` was called.

        :param key: name of key for which we want to get the distinct values

        .. seealso:: :meth:`pymongo.asynchronous.collection.AsyncCollection.distinct`
        """
        options: dict[str, Any] = {}
        if self._spec:
            options["query"] = self._spec
        if self._max_time_ms is not None:
            options["maxTimeMS"] = self._max_time_ms
        if self._comment:
            options["comment"] = self._comment
        if self._collation is not None:
            options["collation"] = self._collation

        return await self._collection.distinct(key, session=self._session, **options)

    async def _send_message(self, operation: Union[_Query, _GetMore]) -> None:
        """Send a query or getmore operation and handles the response.

        If operation is ``None`` this is an exhaust cursor, which reads
        the next result batch off the exhaust socket instead of
        sending getMore messages to the server.

        Can raise ConnectionFailure.
        """
        client = self._collection.database.client
        # OP_MSG is required to support exhaust cursors with encryption.
        if client._encrypter and self._exhaust:
            raise InvalidOperation("exhaust cursors do not support auto encryption")

        try:
            response = await client._run_operation(
                operation, self._run_with_conn, address=self._address
            )
        except OperationFailure as exc:
            if exc.code in _CURSOR_CLOSED_ERRORS or self._exhaust:
                # Don't send killCursors because the cursor is already closed.
                self._killed = True
            if exc.timeout:
                self._die_no_lock()
            else:
                await self.close()
            # If this is a tailable cursor the error is likely
            # due to capped collection roll over. Setting
            # self._killed to True ensures AsyncCursor.alive will be
            # False. No need to re-raise.
            if (
                exc.code in _CURSOR_CLOSED_ERRORS
                and self._query_flags & _QUERY_OPTIONS["tailable_cursor"]
            ):
                return
            raise
        except ConnectionFailure:
            self._killed = True
            await self.close()
            raise
        # Catch KeyboardInterrupt, CancelledError, etc. and cleanup.
        except BaseException:
            await self.close()
            raise
        self._address = response.address
        if isinstance(response, PinnedResponse):
            if not self._sock_mgr:
                self._sock_mgr = _ConnectionManager(response.conn, response.more_to_come)  # type: ignore[arg-type]

        cmd_name = operation.name
        docs = response.docs
        if cmd_name != "explain":
            cursor = docs[0]["cursor"]
            self._id = cursor["id"]
            if cmd_name == "find":
                documents = cursor["firstBatch"]
                # Update the namespace used for future getMore commands.
                ns = cursor.get("ns")
                if ns:
                    self._dbname, self._collname = ns.split(".", 1)
            else:
                documents = cursor["nextBatch"]
            self._data = deque(documents)
            self._retrieved += len(documents)
        else:
            self._id = 0
            self._data = deque(docs)
            self._retrieved += len(docs)

        if self._id == 0:
            # Don't wait for garbage collection to call __del__, return the
            # socket and the session to the pool now.
            await self.close()

        if self._limit and self._id and self._limit <= self._retrieved:
            await self.close()

    async def _refresh(self) -> int:
        """Refreshes the cursor with more data from Mongo.

        Returns the length of self._data after refresh. Will exit early if
        self._data is already non-empty. Raises OperationFailure when the
        cursor cannot be refreshed due to an error on the query.
        """
        if len(self._data) or self._killed:
            return len(self._data)

        if not self._session:
            self._session = self._collection.database.client._ensure_session()

        if self._id is None:  # Query
            if (self._min or self._max) and not self._hint:
                raise InvalidOperation(
                    "Passing a 'hint' is required when using the min/max query"
                    " option to ensure the query utilizes the correct index"
                )
            q = self._query_class(
                self._query_flags,
                self._collection.database.name,
                self._collection.name,
                self._skip,
                self._query_spec(),
                self._projection,
                self._codec_options,
                self._get_read_preference(),
                self._limit,
                self._batch_size,
                self._read_concern,
                self._collation,
                self._session,
                self._collection.database.client,
                self._allow_disk_use,
                self._exhaust,
            )
            await self._send_message(q)
        elif self._id:  # Get More
            if self._limit:
                limit = self._limit - self._retrieved
                if self._batch_size:
                    limit = min(limit, self._batch_size)
            else:
                limit = self._batch_size
            # Exhaust cursors don't send getMore messages.
            g = self._getmore_class(
                self._dbname,
                self._collname,
                limit,
                self._id,
                self._codec_options,
                self._get_read_preference(),
                self._session,
                self._collection.database.client,
                self._max_await_time_ms,
                self._sock_mgr,
                self._exhaust,
                self._comment,
            )
            await self._send_message(g)

        return len(self._data)

    async def rewind(self) -> AsyncCursor[_DocumentType]:
        """Rewind this cursor to its unevaluated state.

        Reset this cursor if it has been partially or completely evaluated.
        Any options that are present on the cursor will remain in effect.
        Future iterating performed on this cursor will cause new queries to
        be sent to the server, even if the resultant data has already been
        retrieved by this cursor.
        """
        await self.close()
        self._data = deque()
        self._id = None
        self._address = None
        self._retrieved = 0
        self._killed = False

        return self

    async def next(self) -> _DocumentType:
        """Advance the cursor."""
        if self._empty:
            raise StopAsyncIteration
        if len(self._data) or await self._refresh():
            return self._data.popleft()
        else:
            raise StopAsyncIteration

    async def _next_batch(self, result: list, total: Optional[int] = None) -> bool:  # type: ignore[type-arg]
        """Get all or some documents from the cursor."""
        if self._empty:
            return False
        if len(self._data) or await self._refresh():
            if total is None:
                result.extend(self._data)
                self._data.clear()
            else:
                for _ in range(min(len(self._data), total)):
                    result.append(self._data.popleft())
            return True
        else:
            return False

    async def __anext__(self) -> _DocumentType:
        return await self.next()

    def __aiter__(self) -> AsyncCursor[_DocumentType]:
        return self

    async def __aenter__(self) -> AsyncCursor[_DocumentType]:
        return self


class AsyncRawBatchCursor(AsyncCursor[_DocumentType]):
    """An asynchronous cursor / iterator over raw batches of BSON data from a query result."""

    _query_class = _RawBatchQuery
    _getmore_class = _RawBatchGetMore

    def __init__(
        self, collection: AsyncCollection[_DocumentType], *args: Any, **kwargs: Any
    ) -> None:
        """Create a new cursor / iterator over raw batches of BSON data.

        Should not be called directly by application developers -
        see :meth:`~pymongo.asynchronous.collection.AsyncCollection.find_raw_batches`
        instead.

        .. seealso:: The MongoDB documentation on `cursors <https://dochub.mongodb.org/core/cursors>`_.
        """
        super().__init__(collection, *args, **kwargs)

    def _unpack_response(
        self,
        response: _OpMsg,
        cursor_id: Optional[int],
        codec_options: CodecOptions[Mapping[str, Any]],
        user_fields: Optional[Mapping[str, Any]] = None,
        legacy_response: bool = False,
    ) -> list[_DocumentOut]:
        raw_response = response.raw_response(cursor_id, user_fields=user_fields)
        if not legacy_response:
            # OP_MSG returns firstBatch/nextBatch documents as a BSON array
            # Re-assemble the array of documents into a document stream
            _convert_raw_document_lists_to_streams(raw_response[0])
        return cast(list["_DocumentOut"], raw_response)

    async def explain(self) -> _DocumentType:
        """Returns an explain plan record for this cursor.

        .. seealso:: The MongoDB documentation on `explain <https://dochub.mongodb.org/core/explain>`_.
        """
        clone = self._clone(deepcopy=True, base=AsyncCursor(self.collection))
        return await clone.explain()

    def __getitem__(self, index: Any) -> NoReturn:
        raise InvalidOperation("Cannot call __getitem__ on AsyncRawBatchCursor")
