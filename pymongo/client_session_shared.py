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

"""Internal helpers for logical sessions, shared between the asynchronous and synchronous APIs."""

from __future__ import annotations

import collections
import time
import uuid
from collections.abc import Mapping, MutableMapping
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    NoReturn,
    Optional,
    Protocol,
    TypeVar,
    cast,
)

from bson.binary import Binary
from bson.int64 import Int64
from bson.timestamp import Timestamp
from pymongo import _csot
from pymongo.errors import (
    ConfigurationError,
    ExecutionTimeout,
    InvalidOperation,
    NetworkTimeout,
    OperationFailure,
    PyMongoError,
)
from pymongo.helpers_shared import _RETRYABLE_ERROR_CODES
from pymongo.operations import _WRITES_WITH_CLUSTER_TIME
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference, _ServerMode
from pymongo.server_type import SERVER_TYPE
from pymongo.write_concern import WriteConcern

if TYPE_CHECKING:
    from pymongo.typings import (
        ClusterTime,
        _Address,
        _AgnosticClientSession,
        _AgnosticConnection,
        _AgnosticMongoClient,
        _AgnosticServer,
    )

_ClientSessionT = TypeVar("_ClientSessionT", bound="_AgnosticClientSession")
_T = TypeVar("_T")
_ClientT = TypeVar("_ClientT", bound="_AgnosticMongoClient")
_ConnectionT = TypeVar("_ConnectionT", bound="_AgnosticConnection")
_BoundSessionContextT = TypeVar("_BoundSessionContextT")


class SessionOptions:
    """Options for a new :class:`~pymongo.asynchronous.client_session.AsyncClientSession`
    or :class:`~pymongo.client_session.ClientSession`.

    :param causal_consistency: If True, read operations are causally
        ordered within the session. Defaults to True when the ``snapshot``
        option is ``False``.
    :param default_transaction_options: The default
        TransactionOptions to use for transactions started on this session.
    :param snapshot: If True, then all reads performed using this
        session will read from the same snapshot. This option is incompatible
        with ``causal_consistency=True``. Defaults to ``False``.

    .. versionchanged:: 3.12
       Added the ``snapshot`` parameter.
    """

    def __init__(
        self,
        causal_consistency: Optional[bool] = None,
        default_transaction_options: Optional[TransactionOptions] = None,
        snapshot: Optional[bool] = False,
    ) -> None:
        if snapshot:
            if causal_consistency:
                raise ConfigurationError("snapshot reads do not support causal_consistency=True")
            causal_consistency = False
        elif causal_consistency is None:
            causal_consistency = True
        self._causal_consistency = causal_consistency
        if default_transaction_options is not None:
            if not isinstance(default_transaction_options, TransactionOptions):
                raise TypeError(
                    "default_transaction_options must be an instance of "
                    f"pymongo.client_session.TransactionOptions, not: {default_transaction_options!r}"
                )
        self._default_transaction_options = default_transaction_options
        self._snapshot = snapshot

    @property
    def causal_consistency(self) -> bool:
        """Whether causal consistency is configured."""
        return self._causal_consistency

    @property
    def default_transaction_options(self) -> Optional[TransactionOptions]:
        """The default TransactionOptions to use for transactions started on
        this session.

        .. versionadded:: 3.7
        """
        return self._default_transaction_options

    @property
    def snapshot(self) -> Optional[bool]:
        """Whether snapshot reads are configured.

        .. versionadded:: 3.12
        """
        return self._snapshot


class TransactionOptions:
    """Options for :meth:`~pymongo.asynchronous.client_session.AsyncClientSession.start_transaction`
    or :meth:`~pymongo.client_session.ClientSession.start_transaction`.

    :param read_concern: The
        :class:`~pymongo.read_concern.ReadConcern` to use for this transaction.
        If ``None`` (the default) the :attr:`read_preference` of
        the client is used.
    :param write_concern: The
        :class:`~pymongo.write_concern.WriteConcern` to use for this
        transaction. If ``None`` (the default) the :attr:`read_preference` of
        the client is used.
    :param read_preference: The read preference to use. If
        ``None`` (the default) the :attr:`read_preference` of this
        client is used. See :mod:`~pymongo.read_preferences`
        for options. Transactions which read must use
        :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.
    :param max_commit_time_ms: The maximum amount of time to allow a
        single commitTransaction command to run. This option is an alias for
        maxTimeMS option on the commitTransaction command. If ``None`` (the
        default) maxTimeMS is not used.

    .. versionchanged:: 3.9
       Added the ``max_commit_time_ms`` option.

    .. versionadded:: 3.7
    """

    def __init__(
        self,
        read_concern: Optional[ReadConcern] = None,
        write_concern: Optional[WriteConcern] = None,
        read_preference: Optional[_ServerMode] = None,
        max_commit_time_ms: Optional[int] = None,
    ) -> None:
        self._read_concern = read_concern
        self._write_concern = write_concern
        self._read_preference = read_preference
        self._max_commit_time_ms = max_commit_time_ms
        if read_concern is not None:
            if not isinstance(read_concern, ReadConcern):
                raise TypeError(
                    "read_concern must be an instance of "
                    f"pymongo.read_concern.ReadConcern, not: {read_concern!r}"
                )
        if write_concern is not None:
            if not isinstance(write_concern, WriteConcern):
                raise TypeError(
                    "write_concern must be an instance of "
                    f"pymongo.write_concern.WriteConcern, not: {write_concern!r}"
                )
            if not write_concern.acknowledged:
                raise ConfigurationError(
                    f"transactions do not support unacknowledged write concern: {write_concern!r}"
                )
        if read_preference is not None:
            if not isinstance(read_preference, _ServerMode):
                raise TypeError(
                    f"{read_preference!r} is not valid for read_preference. See "
                    "pymongo.read_preferences for valid "
                    "options."
                )
        if max_commit_time_ms is not None:
            if not isinstance(max_commit_time_ms, int):
                raise TypeError(
                    f"max_commit_time_ms must be an integer or None, not {type(max_commit_time_ms)}"
                )

    @property
    def read_concern(self) -> Optional[ReadConcern]:
        """This transaction's :class:`~pymongo.read_concern.ReadConcern`."""
        return self._read_concern

    @property
    def write_concern(self) -> Optional[WriteConcern]:
        """This transaction's :class:`~pymongo.write_concern.WriteConcern`."""
        return self._write_concern

    @property
    def read_preference(self) -> Optional[_ServerMode]:
        """This transaction's :class:`~pymongo.read_preferences.ReadPreference`."""
        return self._read_preference

    @property
    def max_commit_time_ms(self) -> Optional[int]:
        """The maxTimeMS to use when running a commitTransaction command.

        .. versionadded:: 3.9
        """
        return self._max_commit_time_ms


def _validate_session_write_concern(
    session: Optional[_ClientSessionT], write_concern: Optional[WriteConcern]
) -> Optional[_ClientSessionT]:
    """Validate that an explicit session is not used with an unack'ed write.

    Returns the session to use for the next operation.
    """
    if session:
        if write_concern is not None and not write_concern.acknowledged:
            # For unacknowledged writes without an explicit session,
            # drivers SHOULD NOT use an implicit session. If a driver
            # creates an implicit session for unacknowledged writes
            # without an explicit session, the driver MUST NOT send the
            # session ID.
            if session._implicit:
                return None
            else:
                raise ConfigurationError(
                    "Explicit sessions are incompatible with "
                    f"unacknowledged write concern: {write_concern!r}"
                )
    return session


class _TxnState:
    NONE = 1
    STARTING = 2
    IN_PROGRESS = 3
    COMMITTED = 4
    COMMITTED_EMPTY = 5
    ABORTED = 6


def _reraise_with_unknown_commit(exc: Any) -> NoReturn:
    """Re-raise an exception with the UnknownTransactionCommitResult label."""
    exc._add_error_label("UnknownTransactionCommitResult")
    raise exc


def _max_time_expired_error(exc: PyMongoError) -> bool:
    """Return true if exc is a MaxTimeMSExpired error."""
    return isinstance(exc, OperationFailure) and exc.code == 50


# From the transactions spec, all the retryable writes errors plus
# WriteConcernTimeout.
_UNKNOWN_COMMIT_ERROR_CODES: frozenset = _RETRYABLE_ERROR_CODES | frozenset(  # type: ignore[type-arg]
    [
        64,  # WriteConcernTimeout
        50,  # MaxTimeMSExpired
    ]
)

# From the Convenient API for Transactions spec, with_transaction must
# halt retries after 120 seconds.
# This limit is non-configurable and was chosen to be twice the 60 second
# default value of MongoDB's `transactionLifetimeLimitSeconds` parameter.
_WITH_TRANSACTION_RETRY_TIME_LIMIT = 120
_BACKOFF_MAX = 0.500  # 500ms max backoff
_BACKOFF_INITIAL = 0.005  # 5ms initial backoff


def _within_time_limit(start_time: float, backoff: float = 0) -> bool:
    """Are we within the with_transaction retry limit?"""
    remaining = _csot.remaining()
    if remaining is not None and remaining <= 0:
        return False
    return time.monotonic() + backoff - start_time < _WITH_TRANSACTION_RETRY_TIME_LIMIT


def _make_timeout_error(error: BaseException) -> PyMongoError:
    """Convert error to a NetworkTimeout or ExecutionTimeout as appropriate."""
    if _csot.remaining() is not None:
        timeout_error: PyMongoError = ExecutionTimeout(
            str(error), 50, {"ok": 0, "errmsg": str(error), "code": 50}
        )
    else:
        timeout_error = NetworkTimeout(str(error))
    if isinstance(error, PyMongoError):
        timeout_error._error_labels = error._error_labels.copy()
    return timeout_error


class _EmptyServerSession:
    __slots__ = "dirty", "started_retryable_write"

    def __init__(self) -> None:
        self.dirty = False
        self.started_retryable_write = False

    def mark_dirty(self) -> None:
        self.dirty = True

    def inc_transaction_id(self) -> None:
        self.started_retryable_write = True


class _ServerSession:
    def __init__(self, generation: int):
        # Ensure id is type 4, regardless of CodecOptions.uuid_representation.
        self.session_id = {"id": Binary(uuid.uuid4().bytes, 4)}
        self.last_use = time.monotonic()
        self._transaction_id = 0
        self.dirty = False
        self.generation = generation

    def mark_dirty(self) -> None:
        """Mark this session as dirty.

        A server session is marked dirty when a command fails with a network
        error. Dirty sessions are later discarded from the server session pool.
        """
        self.dirty = True

    def timed_out(self, session_timeout_minutes: Optional[int]) -> bool:
        if session_timeout_minutes is None:
            return False

        idle_seconds = time.monotonic() - self.last_use

        # Timed out if we have less than a minute to live.
        return idle_seconds > (session_timeout_minutes - 1) * 60

    @property
    def transaction_id(self) -> Int64:
        """Positive 64-bit integer."""
        return Int64(self._transaction_id)

    def inc_transaction_id(self) -> None:
        self._transaction_id += 1


class _ServerSessionPool(collections.deque):  # type: ignore[type-arg]
    """Pool of _ServerSession objects.

    This class is thread-safe.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.generation = 0

    def reset(self) -> None:
        self.generation += 1
        self.clear()

    def pop_all(self) -> list[_ServerSession]:
        ids = []
        while True:
            try:
                ids.append(self.pop().session_id)
            except IndexError:
                break
        return ids

    def get_server_session(self, session_timeout_minutes: Optional[int]) -> _ServerSession:
        # Although the Driver Sessions Spec says we only clear stale sessions
        # in return_server_session, PyMongo can't take a lock when returning
        # sessions from a __del__ method (like in Cursor.__die), so it can't
        # clear stale sessions there. In case many sessions were returned via
        # __del__, check for stale sessions here too.
        self._clear_stale(session_timeout_minutes)

        # The most recently used sessions are on the left.
        while True:
            try:
                s = self.popleft()
            except IndexError:
                break
            if not s.timed_out(session_timeout_minutes):
                return s

        return _ServerSession(self.generation)

    def return_server_session(self, server_session: _ServerSession) -> None:
        # Discard sessions from an old pool to avoid duplicate sessions in the
        # child process after a fork.
        if server_session.generation == self.generation and not server_session.dirty:
            self.appendleft(server_session)

    def _clear_stale(self, session_timeout_minutes: Optional[int]) -> None:
        # Clear stale sessions. The least recently used are on the right.
        while True:
            try:
                s = self.pop()
            except IndexError:
                break
            if not s.timed_out(session_timeout_minutes):
                self.append(s)
                # The remaining sessions also haven't timed out.
                break


class _AgnosticClientSessionBase(Generic[_ClientT, _ConnectionT, _BoundSessionContextT]):
    """A session for ordering sequential operations.

    Session instances are **not thread-safe or fork-safe**. They can only be used
    by one thread or process at a time. A single session cannot be used to run
    multiple operations concurrently.

    Should not be initialized directly by application developers - to create a
    session, call the client's ``start_session`` method.
    """

    # Set by the async/sync subclasses to the IO-specific classes.
    _transaction_cls: type[_TransactionBase[Any]]
    _bound_session_context_cls: Callable[[Any, bool], _BoundSessionContextT]

    def __init__(
        self,
        client: _ClientT,
        server_session: Any,
        options: SessionOptions,
        implicit: bool,
    ) -> None:
        # An _AgnosticMongoClient, a _ServerSession, a SessionOptions, and a set.
        self._client: _ClientT = client
        self._server_session = server_session
        self._options = options
        self._cluster_time: Optional[Mapping[str, Any]] = None
        self._operation_time: Optional[Timestamp] = None
        self._snapshot_time = None
        # Is this an implicitly created session?
        self._implicit = implicit
        self._transaction: _TransactionBase[_ConnectionT] = self._transaction_cls(None, client)
        # Is this session attached to a cursor?
        self._attached_to_cursor = False
        # Should we leave the session alive when the cursor is closed?
        self._leave_alive = False

    def _end_implicit_session(self) -> None:
        # Implicit sessions can't be part of transactions or pinned connections
        if not self._leave_alive and self._server_session is not None:
            self._client._return_server_session(self._server_session)
            self._server_session = None

    def _check_ended(self) -> None:
        if self._server_session is None:
            raise InvalidOperation("Cannot use ended session")

    def bind(self, end_session: bool = True) -> _BoundSessionContextT:
        """Bind this session so it is implicitly passed to all database operations within the returned context.

        .. code-block:: python

           # Synchronous
           with client.start_session() as s:
               with s.bind():
                   # session=s is passed implicitly
                   client.db.collection.insert_one({"x": 1})

           # Asynchronous
           async with client.start_session() as s:
               async with s.bind():
                   # session=s is passed implicitly
                   await client.db.collection.insert_one({"x": 1})

        :param end_session: Whether to end the session on exiting the returned context. Defaults to True.
            If set to False, ``end_session()`` must be called once the session is no longer used.

        .. versionadded:: 4.17
        """
        return self._bound_session_context_cls(self, end_session)

    @property
    def client(self) -> _ClientT:
        """The client this session was created from."""
        return self._client

    @property
    def options(self) -> SessionOptions:
        """The :class:`SessionOptions` this session was created with."""
        return self._options

    @property
    def session_id(self) -> Mapping[str, Any]:
        """A BSON document, the opaque server session identifier."""
        self._check_ended()
        self._materialize(self._client.topology_description.logical_session_timeout_minutes)
        return self._server_session.session_id

    @property
    def _transaction_id(self) -> Int64:
        """The current transaction id for the underlying server session."""
        self._materialize(self._client.topology_description.logical_session_timeout_minutes)
        return self._server_session.transaction_id

    @property
    def cluster_time(self) -> Optional[ClusterTime]:
        """The cluster time returned by the last operation executed
        in this session.
        """
        return self._cluster_time

    @property
    def operation_time(self) -> Optional[Timestamp]:
        """The operation time returned by the last operation executed
        in this session.
        """
        return self._operation_time

    def _inherit_option(self, name: str, val: _T) -> _T:
        """Return the inherited TransactionOption value."""
        if val:
            return val
        txn_opts = self.options.default_transaction_options
        parent_val = txn_opts and getattr(txn_opts, name)
        if parent_val:
            return parent_val
        return getattr(self.client, name)

    def _advance_cluster_time(self, cluster_time: Optional[Mapping[str, Any]]) -> None:
        """Internal cluster time helper."""
        if self._cluster_time is None:
            self._cluster_time = cluster_time
        elif cluster_time is not None:
            if cluster_time["clusterTime"] > self._cluster_time["clusterTime"]:
                self._cluster_time = cluster_time

    def advance_cluster_time(self, cluster_time: Mapping[str, Any]) -> None:
        """Update the cluster time for this session.

        :param cluster_time: The ``cluster_time`` from another session.
        """
        if not isinstance(cluster_time, Mapping):
            raise TypeError(
                f"cluster_time must be a subclass of collections.Mapping, not {type(cluster_time)}"
            )
        if not isinstance(cluster_time.get("clusterTime"), Timestamp):
            raise ValueError("Invalid cluster_time")
        self._advance_cluster_time(cluster_time)

    def _advance_operation_time(self, operation_time: Optional[Timestamp]) -> None:
        """Internal operation time helper."""
        if self._operation_time is None:
            self._operation_time = operation_time
        elif operation_time is not None:
            if operation_time > self._operation_time:
                self._operation_time = operation_time

    def advance_operation_time(self, operation_time: Timestamp) -> None:
        """Update the operation time for this session.

        :param operation_time: The ``operation_time`` from another session.
        """
        if not isinstance(operation_time, Timestamp):
            raise TypeError(
                f"operation_time must be an instance of bson.timestamp.Timestamp, not {type(operation_time)}"
            )
        self._advance_operation_time(operation_time)

    def _process_response(self, reply: Mapping[str, Any]) -> None:
        """Process a response to a command that was run with this session."""
        self._advance_cluster_time(reply.get("$clusterTime"))
        self._advance_operation_time(reply.get("operationTime"))
        if self._options.snapshot and self._snapshot_time is None:
            if "cursor" in reply:
                ct = reply["cursor"].get("atClusterTime")
            else:
                ct = reply.get("atClusterTime")
            self._snapshot_time = ct
        if self.in_transaction and self._transaction.sharded:
            recovery_token = reply.get("recoveryToken")
            if recovery_token:
                self._transaction.recovery_token = recovery_token

    @property
    def has_ended(self) -> bool:
        """True if this session is finished."""
        return self._server_session is None

    @property
    def in_transaction(self) -> bool:
        """True if this session has an active multi-statement transaction.

        .. versionadded:: 3.10
        """
        return self._transaction.active()

    @property
    def _starting_transaction(self) -> bool:
        """True if this session is starting a multi-statement transaction."""
        return self._transaction.starting()

    @property
    def _pinned_address(self) -> Optional[_Address]:
        """The mongos address this transaction was created on."""
        if self._transaction.active():
            return self._transaction.pinned_address
        return None

    @property
    def _pinned_connection(self) -> Optional[_ConnectionT]:
        """The connection this transaction was started on."""
        return self._transaction.pinned_conn

    def _pin(self, server: _AgnosticServer, conn: _ConnectionT) -> None:
        """Pin this session to the given Server or to the given connection."""
        self._transaction.pin(server, conn)

    def _txn_read_preference(self) -> Optional[_ServerMode]:
        """Return read preference of this transaction or None."""
        if self.in_transaction:
            assert self._transaction.opts
            return self._transaction.opts.read_preference
        return None

    def _materialize(self, logical_session_timeout_minutes: Optional[int] = None) -> None:
        if isinstance(self._server_session, _EmptyServerSession):
            old = self._server_session
            self._server_session = self._client._topology.get_server_session(
                logical_session_timeout_minutes
            )
            if old.started_retryable_write:
                self._server_session.inc_transaction_id()

    def _apply_to(
        self,
        command: MutableMapping[str, Any],
        is_retryable: bool,
        read_preference: _ServerMode,
        conn: _AgnosticConnection,
    ) -> None:
        # getMores must be sent with a session if the cursor was opened with one
        operation = next(iter(command))
        if not conn.supports_sessions and (
            isinstance(self._server_session, _EmptyServerSession) or operation != "getMore"
        ):
            if not self._implicit:
                raise ConfigurationError("Sessions are not supported by this MongoDB deployment")
            return
        self._check_ended()
        self._materialize(conn.logical_session_timeout_minutes)
        # Add afterClusterTime on snapshot reads or writes in causally-consistent sessions
        if self.options.snapshot or (
            self.options.causal_consistency
            and not self.in_transaction
            and operation in _WRITES_WITH_CLUSTER_TIME
        ):
            self._update_read_concern(command, conn)

        self._server_session.last_use = time.monotonic()
        command["lsid"] = self._server_session.session_id

        if is_retryable:
            command["txnNumber"] = self._server_session.transaction_id
            return

        if self.in_transaction:
            if read_preference != ReadPreference.PRIMARY:
                raise InvalidOperation(
                    f"read preference in a transaction must be primary, not: {read_preference!r}"
                )

            if self._transaction.state == _TxnState.STARTING:
                # First command begins a new transaction.
                command["startTransaction"] = True

                assert self._transaction.opts
                if self._transaction.opts.read_concern:
                    rc = self._transaction.opts.read_concern.document
                    if rc:
                        command["readConcern"] = rc
                self._update_read_concern(command, conn)

            command["txnNumber"] = self._server_session.transaction_id
            command["autocommit"] = False

    def _start_retryable_write(self) -> None:
        self._check_ended()
        self._server_session.inc_transaction_id()

    def _update_read_concern(
        self, cmd: MutableMapping[str, Any], conn: _AgnosticConnection
    ) -> None:
        if self.options.causal_consistency and self.operation_time is not None:
            cmd.setdefault("readConcern", {})["afterClusterTime"] = self.operation_time
        if self.options.snapshot:
            if conn.max_wire_version < 13:
                raise ConfigurationError("Snapshot reads require MongoDB 5.0 or later")
            rc = cmd.setdefault("readConcern", {})
            rc["level"] = "snapshot"
            if self._snapshot_time is not None:
                rc["atClusterTime"] = self._snapshot_time

    def __copy__(self) -> NoReturn:
        raise TypeError(f"A {type(self).__name__} cannot be copied, create a new session instead")


class _ConnectionManagerProtocol(Protocol[_ConnectionT]):
    """Protocol for the connection manager used to pin a connection to a transaction."""

    conn: Optional[_ConnectionT]


class _TransactionBase(Generic[_ConnectionT]):
    """Internal class to hold transaction information in a ClientSession."""

    # Set by the async/sync subclasses to the correct _ConnectionManager class.
    _conn_mgr_cls: Callable[[_ConnectionT, bool], _ConnectionManagerProtocol[_ConnectionT]]

    def __init__(self, opts: Optional[TransactionOptions], client: _AgnosticMongoClient):
        self.opts = opts
        self.state = _TxnState.NONE
        self.sharded = False
        self.pinned_address: Optional[_Address] = None
        self.conn_mgr: Optional[_ConnectionManagerProtocol[_ConnectionT]] = None
        self.recovery_token = None
        self.attempt = 0
        self.client = client
        self.has_completed_command = False

    def active(self) -> bool:
        return self.state in (_TxnState.STARTING, _TxnState.IN_PROGRESS)

    def starting(self) -> bool:
        return self.state == _TxnState.STARTING

    def set_starting(self) -> None:
        self.state = _TxnState.STARTING

    def set_in_progress(self) -> None:
        if self.state == _TxnState.STARTING:
            self.state = _TxnState.IN_PROGRESS

    @property
    def pinned_conn(self) -> Optional[_ConnectionT]:
        if self.active() and self.conn_mgr:
            return self.conn_mgr.conn
        return None

    def pin(self, server: _AgnosticServer, conn: _ConnectionT) -> None:
        self.sharded = True
        self.pinned_address = server.description.address
        if server.description.server_type == SERVER_TYPE.LoadBalancer:
            conn.pin_txn()
            self.conn_mgr = self._conn_mgr_cls(conn, False)

    def __del__(self) -> None:
        if self.conn_mgr:
            # Reuse the cursor closing machinery to return the socket to the
            # pool soon. _close_cursor_soon expects the IO-specific
            # _ConnectionManager, which conn_mgr is at runtime.
            self.client._close_cursor_soon(0, None, cast(Any, self.conn_mgr))
            self.conn_mgr = None
