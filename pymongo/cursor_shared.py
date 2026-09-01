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

import copy
import warnings
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic, Optional, Union, cast, overload

from bson import RE_TYPE
from bson.code import Code
from bson.son import SON
from pymongo import helpers_shared
from pymongo.collation import validate_collation_or_none
from pymongo.common import validate_is_document_type, validate_is_mapping
from pymongo.errors import InvalidOperation
from pymongo.message import _CursorAddress, _GetMore, _Query
from pymongo.typings import _Address, _CollationIn, _DocumentOut, _DocumentType
from pymongo.write_concern import validate_boolean

if TYPE_CHECKING:
    import sys

    from _typeshed import SupportsItems

    from bson.codec_options import CodecOptions
    from pymongo.message import _OpMsg
    from pymongo.read_preferences import _ServerMode

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
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

    @property
    @abstractmethod
    def session(self) -> Optional[Any]:
        """The cursor's session, or None. Typed more accurately in subclasses."""
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
    Sequence[Union[str, tuple[str, Union[int, str, Mapping[str, Any]]]]], Mapping[str, Any]
]
_Hint = Union[str, _Sort]


class _AgnosticCursor(_AgnosticCursorBase[_DocumentType]):
    """Shared non-IO cursor implementation used by both AsyncCursor and Cursor.

    All IO-specific behavior is implemented in subclasses.
    """

    _query_class = _Query
    _getmore_class = _GetMore

    def __init__(
        self,
        collection: Any,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        skip: int = 0,
        limit: int = 0,
        no_cursor_timeout: bool = False,
        cursor_type: int = CursorType.NON_TAILABLE,
        sort: Optional[_Sort] = None,
        allow_partial_results: bool = False,
        oplog_replay: bool = False,
        batch_size: int = 0,
        collation: Optional[_CollationIn] = None,
        hint: Optional[_Hint] = None,
        max_scan: Optional[int] = None,
        max_time_ms: Optional[int] = None,
        max: Optional[_Sort] = None,
        min: Optional[_Sort] = None,
        return_key: Optional[bool] = None,
        show_record_id: Optional[bool] = None,
        snapshot: Optional[bool] = None,
        comment: Optional[Any] = None,
        session: Optional[Any] = None,
        allow_disk_use: Optional[bool] = None,
        let: Optional[bool] = None,
    ) -> None:
        """Create a new cursor.
        Used by a collection's ``find()`` method to iterate over MongoDB query results.

        Should not be called directly by application developers.

        .. seealso:: The MongoDB documentation on `cursors <https://dochub.mongodb.org/core/cursors>`_.
        """
        # Initialize all attributes used in __del__ before possibly raising
        # an error to avoid attribute errors during garbage collection.
        self._collection = collection
        self._id: Any = None
        self._exhaust = False
        self._sock_mgr: Any = None
        self._killed = False
        self._session: Optional[Any]

        if session:
            self._session = session
            self._session._attached_to_cursor = True
        else:
            self._session = None

        spec: Mapping[str, Any] = filter or {}
        validate_is_mapping("filter", spec)
        if not isinstance(skip, int):
            raise TypeError(f"skip must be an instance of int, not {type(skip)}")
        if not isinstance(limit, int):
            raise TypeError(f"limit must be an instance of int, not {type(limit)}")
        validate_boolean("no_cursor_timeout", no_cursor_timeout)
        if no_cursor_timeout and self._session and self._session._implicit:
            warnings.warn(
                "use an explicit session with no_cursor_timeout=True "
                "otherwise the cursor may still timeout after "
                "30 minutes, for more info see "
                "https://mongodb.com/docs/v4.4/reference/method/"
                "cursor.noCursorTimeout/"
                "#session-idle-timeout-overrides-nocursortimeout",
                UserWarning,
                stacklevel=2,
            )
        if cursor_type not in (
            CursorType.NON_TAILABLE,
            CursorType.TAILABLE,
            CursorType.TAILABLE_AWAIT,
            CursorType.EXHAUST,
        ):
            raise ValueError("not a valid value for cursor_type")
        validate_boolean("allow_partial_results", allow_partial_results)
        validate_boolean("oplog_replay", oplog_replay)
        if not isinstance(batch_size, int):
            raise TypeError(f"batch_size must be an integer, not {type(batch_size)}")
        if batch_size < 0:
            raise ValueError("batch_size must be >= 0")
        # Only set if allow_disk_use is provided by the user, else None.
        if allow_disk_use is not None:
            allow_disk_use = validate_boolean("allow_disk_use", allow_disk_use)

        if projection is not None:
            projection = helpers_shared._fields_list_to_dict(projection, "projection")

        if let is not None:
            validate_is_document_type("let", let)

        self._let = let
        self._spec = spec
        self._has_filter = filter is not None
        self._projection = projection
        self._skip = skip
        self._limit = limit
        self._batch_size = batch_size
        self._ordering = (sort and helpers_shared._index_document(sort)) or None
        self._max_scan = max_scan
        self._explain = False
        self._comment = comment
        self._max_time_ms = max_time_ms
        self._timeout = self._collection.database.client.options.timeout
        self._max_await_time_ms: Optional[int] = None
        self._max: Optional[Union[dict[Any, Any], _Sort]] = max
        self._min: Optional[Union[dict[Any, Any], _Sort]] = min
        self._collation = validate_collation_or_none(collation)
        self._return_key = return_key
        self._show_record_id = show_record_id
        self._allow_disk_use = allow_disk_use
        self._snapshot = snapshot
        self._hint: Union[str, dict[str, Any], None]
        self._set_hint(hint)

        # This is ugly. People want to be able to do cursor[5:5] and
        # get an empty result set (old behavior was an
        # exception). It's hard to do that right, though, because the
        # server uses limit(0) to mean 'no limit'. So we set __empty
        # in that case and check for it when iterating. We also unset
        # it anytime we change __limit.
        self._empty = False

        self._data: deque = deque()  # type: ignore[type-arg]
        self._address: Optional[_Address] = None
        self._retrieved = 0

        self._codec_options = collection.codec_options
        # Read preference is set when the initial find is sent.
        self._read_preference: Optional[_ServerMode] = None
        self._read_concern = collection.read_concern

        self._query_flags = cursor_type
        self._cursor_type = cursor_type
        if no_cursor_timeout:
            self._query_flags |= _QUERY_OPTIONS["no_timeout"]
        if allow_partial_results:
            self._query_flags |= _QUERY_OPTIONS["partial"]
        if oplog_replay:
            self._query_flags |= _QUERY_OPTIONS["oplog_replay"]

        # The namespace to use for find/getMore commands.
        self._dbname = collection.database.name
        self._collname = collection.name

        self._validate_exhaust_handling()

    def _validate_exhaust_handling(self) -> None:
        """Reject option combinations an exhaust cursor cannot serve.

        Server support is checked against the connection in use, in
        _check_exhaust_supported.
        """
        if self._cursor_type == CursorType.EXHAUST:
            if self._limit:
                raise InvalidOperation("Can't use limit and exhaust together.")
            self._exhaust = True

    @property
    def retrieved(self) -> int:
        """The number of documents retrieved so far."""
        return self._retrieved

    def _get_namespace(self) -> str:
        return f"{self._dbname}.{self._collname}"

    def clone(self) -> Self:
        """Get a clone of this cursor.

        Returns a new cursor instance with options matching those that have
        been set on the current instance. The clone will be completely
        unevaluated, even if the current instance has been partially or
        completely evaluated.
        """
        return self._clone(True)

    def _clone(self, deepcopy: bool = True, base: Optional[Any] = None) -> Any:
        """Internal clone helper."""
        if not base:
            if self._session and not self._session._implicit:
                base = self._clone_base(self._session)
            else:
                base = self._clone_base(None)

        values_to_clone = (
            "spec",
            "projection",
            "skip",
            "limit",
            "max_time_ms",
            "max_await_time_ms",
            "comment",
            "max",
            "min",
            "ordering",
            "explain",
            "hint",
            "batch_size",
            "max_scan",
            "query_flags",
            "collation",
            "empty",
            "show_record_id",
            "return_key",
            "allow_disk_use",
            "snapshot",
            "exhaust",
            "has_filter",
            "cursor_type",
        )
        data = {
            k: v for k, v in self.__dict__.items() if k.startswith("_") and k[1:] in values_to_clone
        }
        if deepcopy:
            data = self._deepcopy(data)
        base.__dict__.update(data)
        return base

    def _clone_base(self, session: Optional[Any]) -> Self:
        """Creates an empty cursor object for information to be copied into."""
        return self.__class__(self._collection, session=session)

    def _query_spec(self) -> Mapping[str, Any]:
        """Get the spec to use for a query."""
        operators: dict[str, Any] = {}
        if self._ordering:
            operators["$orderby"] = self._ordering
        if self._explain:
            operators["$explain"] = True
        if self._hint:
            operators["$hint"] = self._hint
        if self._let:
            operators["let"] = self._let
        if self._comment:
            operators["$comment"] = self._comment
        if self._max_scan:
            operators["$maxScan"] = self._max_scan
        if self._max_time_ms is not None:
            operators["$maxTimeMS"] = self._max_time_ms
        if self._max:
            operators["$max"] = self._max
        if self._min:
            operators["$min"] = self._min
        if self._return_key is not None:
            operators["$returnKey"] = self._return_key
        if self._show_record_id is not None:
            # This is upgraded to showRecordId for MongoDB 3.2+ "find" command.
            operators["$showDiskLoc"] = self._show_record_id
        if self._snapshot is not None:
            operators["$snapshot"] = self._snapshot

        if operators:
            # Make a shallow copy so we can cleanly rewind or clone.
            spec = dict(self._spec)

            # Allow-listed commands must be wrapped in $query.
            if "$query" not in spec:
                # $query has to come first
                spec = {"$query": spec}

            spec.update(operators)
            return spec
        # Have to wrap with $query if "query" is the first key.
        # We can't just use $query anytime "query" is a key as
        # that breaks commands like count and find_and_modify.
        # Checking spec.keys()[0] covers the case that the spec
        # was passed as an instance of SON or OrderedDict.
        elif "query" in self._spec and (len(self._spec) == 1 or next(iter(self._spec)) == "query"):
            return {"$query": self._spec}

        return self._spec

    def _check_okay_to_chain(self) -> None:
        """Check if it is okay to chain more options onto this cursor."""
        if self._retrieved or self._id is not None:
            raise InvalidOperation("cannot set options after executing query")

    def remove_option(self, mask: int) -> Self:
        """Unset arbitrary query flags using a bitmask.

        To unset the tailable flag:
        cursor.remove_option(2)
        """
        if not isinstance(mask, int):
            raise TypeError(f"mask must be an int, not {type(mask)}")
        self._check_okay_to_chain()

        if mask & _QUERY_OPTIONS["exhaust"]:
            self._exhaust = False

        self._query_flags &= ~mask
        return self

    def allow_disk_use(self, allow_disk_use: bool) -> Self:
        """Specifies whether MongoDB can use temporary disk files while
        processing a blocking sort operation.

        Raises :exc:`TypeError` if `allow_disk_use` is not a boolean.

        .. note:: `allow_disk_use` requires server version **>= 4.4**

        :param allow_disk_use: if True, MongoDB may use temporary
            disk files to store data exceeding the system memory limit while
            processing a blocking sort operation.

        .. versionadded:: 3.11
        """
        if not isinstance(allow_disk_use, bool):
            raise TypeError(f"allow_disk_use must be a bool, not {type(allow_disk_use)}")
        self._check_okay_to_chain()

        self._allow_disk_use = allow_disk_use
        return self

    def limit(self, limit: int) -> Self:
        """Limits the number of results to be returned by this cursor.

        Raises :exc:`TypeError` if `limit` is not an integer. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this cursor
        has already been used. The last `limit` applied to this cursor
        takes precedence. A limit of ``0`` is equivalent to no limit.

        :param limit: the number of results to return

        .. seealso:: The MongoDB documentation on `limit <https://dochub.mongodb.org/core/limit>`_.
        """
        if not isinstance(limit, int):
            raise TypeError(f"limit must be an integer, not {type(limit)}")
        if self._exhaust:
            raise InvalidOperation("Can't use limit and exhaust together.")
        self._check_okay_to_chain()

        self._empty = False
        self._limit = limit
        return self

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
        Raises :exc:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used. The last `batch_size`
        applied to this cursor takes precedence.

        :param batch_size: The size of each batch of results requested.
        """
        if not isinstance(batch_size, int):
            raise TypeError(f"batch_size must be an integer, not {type(batch_size)}")
        if batch_size < 0:
            raise ValueError("batch_size must be >= 0")
        self._check_okay_to_chain()

        self._batch_size = batch_size
        return self

    def skip(self, skip: int) -> Self:
        """Skips the first `skip` results of this cursor.

        Raises :exc:`TypeError` if `skip` is not an integer. Raises
        :exc:`ValueError` if `skip` is less than ``0``. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used. The last `skip` applied to this cursor takes
        precedence.

        :param skip: the number of results to skip
        """
        if not isinstance(skip, int):
            raise TypeError(f"skip must be an integer, not {type(skip)}")
        if skip < 0:
            raise ValueError("skip must be >= 0")
        self._check_okay_to_chain()

        self._skip = skip
        return self

    def max_time_ms(self, max_time_ms: Optional[int]) -> Self:
        """Specifies a time limit for a query operation. If the specified
        time is exceeded, the operation will be aborted and
        :exc:`~pymongo.errors.ExecutionTimeout` is raised. If `max_time_ms`
        is ``None`` no limit is applied.

        Raises :exc:`TypeError` if `max_time_ms` is not an integer or ``None``.
        Raises :exc:`~pymongo.errors.InvalidOperation` if this cursor
        has already been used.

        :param max_time_ms: the time limit after which the operation is aborted
        """
        if not isinstance(max_time_ms, int) and max_time_ms is not None:
            raise TypeError(f"max_time_ms must be an integer or None, not {type(max_time_ms)}")
        self._check_okay_to_chain()

        self._max_time_ms = max_time_ms
        return self

    def max_await_time_ms(self, max_await_time_ms: Optional[int]) -> Self:
        """Specifies a time limit for a getMore operation on a
        :attr:`~pymongo.cursor.CursorType.TAILABLE_AWAIT` cursor. For all other
        types of cursor max_await_time_ms is ignored.

        Raises :exc:`TypeError` if `max_await_time_ms` is not an integer or
        ``None``. Raises :exc:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used.

        .. note:: `max_await_time_ms` requires server version **>= 3.2**

        :param max_await_time_ms: the time limit after which the operation is
            aborted

        .. versionadded:: 3.2
        """
        if not isinstance(max_await_time_ms, int) and max_await_time_ms is not None:
            raise TypeError(
                f"max_await_time_ms must be an integer or None, not {type(max_await_time_ms)}"
            )
        self._check_okay_to_chain()

        # Ignore max_await_time_ms if not tailable or await_data is False.
        if self._query_flags & CursorType.TAILABLE_AWAIT:
            self._max_await_time_ms = max_await_time_ms

        return self

    def max_scan(self, max_scan: Optional[int]) -> Self:
        """**DEPRECATED** - Limit the number of documents to scan when
        performing the query.

        Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used. Only the last :meth:`max_scan`
        applied to this cursor has any effect.

        :param max_scan: the maximum number of documents to scan

        .. versionchanged:: 3.7
          Deprecated :meth:`max_scan`. Support for this option is deprecated in
          MongoDB 4.0. Use :meth:`max_time_ms` instead to limit server side
          execution time.
        """
        self._check_okay_to_chain()
        self._max_scan = max_scan
        return self

    def max(self, spec: _Sort) -> Self:
        """Adds ``max`` operator that specifies upper bound for specific index.

        When using ``max``, :meth:`~hint` is required to ensure the query
        uses the expected index.

        :param spec: a list of field, limit pairs specifying the exclusive
            upper bound for all keys of a specific index in order.

        .. versionchanged:: 3.8
           Deprecated cursors that use ``max`` without a :meth:`~hint`.

        .. versionadded:: 2.7
        """
        if not isinstance(spec, (list, tuple)):
            raise TypeError(f"spec must be an instance of list or tuple, not {type(spec)}")

        self._check_okay_to_chain()
        self._max = dict(spec)
        return self

    def min(self, spec: _Sort) -> Self:
        """Adds ``min`` operator that specifies lower bound for specific index.

        When using ``min``, :meth:`~hint` is required to ensure the query
        uses the expected index.

        :param spec: a list of field, limit pairs specifying the inclusive
            lower bound for all keys of a specific index in order.

        .. versionchanged:: 3.8
           Deprecated cursors that use ``min`` without a :meth:`~hint`.

        .. versionadded:: 2.7
        """
        if not isinstance(spec, (list, tuple)):
            raise TypeError(f"spec must be an instance of list or tuple, not {type(spec)}")

        self._check_okay_to_chain()
        self._min = dict(spec)
        return self

    def sort(self, key_or_list: _Hint, direction: Optional[Union[int, str]] = None) -> Self:
        """Sorts this cursor's results.

        Pass a field name and a direction, either
        :data:`~pymongo.ASCENDING` or :data:`~pymongo.DESCENDING`.::

            async for doc in collection.find().sort('field', pymongo.ASCENDING):
                print(doc)

        To sort by multiple fields, pass a list of (key, direction) pairs.
        If just a name is given, :data:`~pymongo.ASCENDING` will be inferred::

            async for doc in collection.find().sort([
                    'field1',
                    ('field2', pymongo.DESCENDING)]):
                print(doc)

        Text search results can be sorted by relevance::

            cursor = db.test.find(
                {'$text': {'$search': 'some words'}},
                {'score': {'$meta': 'textScore'}})

            # Sort by 'score' field.
            cursor.sort([('score', {'$meta': 'textScore'})])

            async for doc in cursor:
                print(doc)

        For more advanced text search functionality, see MongoDB's
        `Atlas Search <https://docs.atlas.mongodb.com/atlas-search/>`_.

        Raises :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used. Only the last :meth:`sort` applied to this
        cursor has any effect.

        :param key_or_list: a single key or a list of (key, direction)
            pairs specifying the keys to sort on
        :param direction: only used if `key_or_list` is a single
            key, if not given :data:`~pymongo.ASCENDING` is assumed
        """
        self._check_okay_to_chain()
        keys = helpers_shared._index_list(key_or_list, direction)
        self._ordering = helpers_shared._index_document(keys)
        return self

    def _set_hint(self, index: Optional[_Hint]) -> None:
        if index is None:
            self._hint = None
            return

        if isinstance(index, str):
            self._hint = index
        else:
            self._hint = helpers_shared._index_document(index)

    def hint(self, index: Optional[_Hint]) -> Self:
        """Adds a 'hint', telling Mongo the proper index to use for the query.

        Judicious use of hints can greatly improve query
        performance. When doing a query on multiple fields (at least
        one of which is indexed) pass the indexed field as a hint to
        the query. Raises :class:`~pymongo.errors.OperationFailure` if the
        provided hint requires an index that does not exist on this collection,
        and raises :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used.

        `index` should be an index as passed to a collection's
        ``create_index()`` method (e.g. ``[('field', ASCENDING)]``) or the name
        of the index. If `index` is ``None`` any existing hint for this query
        is cleared. The last hint applied to this cursor takes precedence
        over all others.

        :param index: index to hint on (as an index specifier)
        """
        self._check_okay_to_chain()
        self._set_hint(index)
        return self

    def comment(self, comment: Any) -> Self:
        """Adds a 'comment' to the cursor.

        http://mongodb.com/docs/manual/reference/operator/comment/

        :param comment: A string to attach to the query to help interpret and
            trace the operation in the server logs and in profile data.

        .. versionadded:: 2.7
        """
        self._check_okay_to_chain()
        self._comment = comment
        return self

    def where(self, code: Union[str, Code]) -> Self:
        """Adds a `$where`_ clause to this query.

        The `code` argument must be an instance of :class:`str` or
        :class:`~bson.code.Code` containing a JavaScript expression.
        This expression will be evaluated for each document scanned.
        Only those documents for which the expression evaluates to
        *true* will be returned as results. The keyword *this* refers
        to the object currently being scanned. For example::

            # Find all documents where field "a" is less than "b" plus "c".
            async for doc in db.test.find().where('this.a < (this.b + this.c)'):
                print(doc)

        Raises :class:`TypeError` if `code` is not an instance of
        :class:`str`. Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used. Only the last call to
        :meth:`where` applied to a cursor has any effect.

        .. note:: MongoDB 4.4 drops support for :class:`~bson.code.Code`
          with scope variables. Consider using `$expr`_ instead.

        :param code: JavaScript expression to use as a filter

        .. _$expr: https://mongodb.com/docs/manual/reference/operator/query/expr/
        .. _$where: https://mongodb.com/docs/manual/reference/operator/query/where/
        """
        self._check_okay_to_chain()
        if not isinstance(code, Code):
            code = Code(code)

        # Avoid overwriting a filter argument that was given by the user
        # when updating the spec.
        spec: dict[str, Any]
        if self._has_filter:
            spec = dict(self._spec)
        else:
            spec = cast(dict, self._spec)  # type: ignore[type-arg]
        spec["$where"] = code
        self._spec = spec
        return self

    def collation(self, collation: Optional[_CollationIn]) -> Self:
        """Adds a :class:`~pymongo.collation.Collation` to this query.

        Raises :exc:`TypeError` if `collation` is not an instance of
        :class:`~pymongo.collation.Collation` or a ``dict``. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used. Only the last collation applied to this cursor has
        any effect.

        :param collation: An instance of :class:`~pymongo.collation.Collation`.
        """
        self._check_okay_to_chain()
        self._collation = validate_collation_or_none(collation)
        return self

    def _unpack_response(
        self,
        response: _OpMsg,
        cursor_id: Optional[int],
        codec_options: CodecOptions,  # type: ignore[type-arg]
        user_fields: Optional[Mapping[str, Any]] = None,
        legacy_response: bool = False,
    ) -> Sequence[_DocumentOut]:
        return response.unpack_response(cursor_id, codec_options, user_fields, legacy_response)

    def _get_read_preference(self) -> _ServerMode:
        if self._read_preference is None:
            # Save the read preference for getMore commands.
            self._read_preference = self._collection._read_preference_for(self.session)
        return self._read_preference

    def __copy__(self) -> Self:
        """Support function for `copy.copy()`.

        .. versionadded:: 2.4
        """
        return self._clone(deepcopy=False)

    def __deepcopy__(self, memo: Any) -> Any:
        """Support function for `copy.deepcopy()`.

        .. versionadded:: 2.4
        """
        return self._clone(deepcopy=True)

    @overload
    def _deepcopy(self, x: Iterable, memo: Optional[dict[int, Union[list, dict]]] = None) -> list:  # type: ignore[type-arg]
        ...

    @overload
    def _deepcopy(
        self,
        x: SupportsItems,  # type: ignore[type-arg]
        memo: Optional[dict[int, Union[list, dict]]] = None,  # type: ignore[type-arg]
    ) -> dict:  # type: ignore[type-arg]
        ...

    def _deepcopy(
        self,
        x: Union[Iterable, SupportsItems],  # type: ignore[type-arg]
        memo: Optional[dict[int, Union[list, dict]]] = None,  # type: ignore[type-arg]
    ) -> Union[list[Any], dict[str, Any]]:
        """Deepcopy helper for the data dictionary or list.

        Regular expressions cannot be deep copied but as they are immutable we
        don't have to copy them when cloning.
        """
        y: Union[list[Any], dict[str, Any]]
        iterator: Iterable[tuple[Any, Any]]
        if not hasattr(x, "items"):
            y, is_list, iterator = [], True, enumerate(x)
        else:
            y, is_list, iterator = {}, False, cast("SupportsItems", x).items()  # type: ignore[type-arg]
        if memo is None:
            memo = {}
        val_id = id(x)
        if val_id in memo:
            return memo[val_id]
        memo[val_id] = y

        for key, value in iterator:
            if isinstance(value, (dict, list)) and not isinstance(value, SON):
                value = self._deepcopy(value, memo)  # noqa: PLW2901
            elif not isinstance(value, RE_TYPE):
                value = copy.deepcopy(value, memo)  # noqa: PLW2901

            if is_list:
                y.append(value)  # type: ignore[union-attr]
            else:
                if not isinstance(key, RE_TYPE):
                    key = copy.deepcopy(key, memo)  # noqa: PLW2901
                y[key] = value  # type:ignore[index]
        return y
