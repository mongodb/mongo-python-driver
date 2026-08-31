# Copyright 2017 MongoDB, Inc.
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

"""Logical sessions for ordering sequential operations.

.. versionadded:: 3.6

Causally Consistent Reads
=========================

.. code-block:: python

  async with client.start_session(causal_consistency=True) as session:
      collection = client.db.collection
      await collection.update_one({"_id": 1}, {"$set": {"x": 10}}, session=session)
      secondary_c = collection.with_options(read_preference=ReadPreference.SECONDARY)

      # A secondary read waits for replication of the write.
      await secondary_c.find_one({"_id": 1}, session=session)

If `causal_consistency` is True (the default), read operations that use
the session are causally after previous read and write operations. Using a
causally consistent session, an application can read its own writes and is
guaranteed monotonic reads, even when reading from replica set secondaries.

.. seealso:: The MongoDB documentation on `causal-consistency <https://dochub.mongodb.org/core/causal-consistency>`_.

.. _async-transactions-ref:

Transactions
============

.. versionadded:: 3.7

MongoDB 4.0 adds support for transactions on replica set primaries. A
transaction is associated with a :class:`AsyncClientSession`. To start a transaction
on a session, use :meth:`AsyncClientSession.start_transaction` in a with-statement.
Then, execute an operation within the transaction by passing the session to the
operation:

.. code-block:: python

  orders = client.db.orders
  inventory = client.db.inventory
  async with client.start_session() as session:
      async with await session.start_transaction():
          await orders.insert_one({"sku": "abc123", "qty": 100}, session=session)
          await inventory.update_one(
              {"sku": "abc123", "qty": {"$gte": 100}},
              {"$inc": {"qty": -100}},
              session=session,
          )

Upon normal completion of ``async with await session.start_transaction()`` block, the
transaction automatically calls :meth:`AsyncClientSession.commit_transaction`.
If the block exits with an exception, the transaction automatically calls
:meth:`AsyncClientSession.abort_transaction`.

In general, multi-document transactions only support read/write (CRUD)
operations on existing collections. However, MongoDB 4.4 adds support for
creating collections and indexes with some limitations, including an
insert operation that would result in the creation of a new collection.
For a complete description of all the supported and unsupported operations
see the `MongoDB server's documentation for transactions
<http://dochub.mongodb.org/core/transactions>`_.

A session may only have a single active transaction at a time, multiple
transactions on the same session can be executed in sequence.

Sharded Transactions
^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 3.9

PyMongo 3.9 adds support for transactions on sharded clusters running MongoDB
>=4.2. Sharded transactions have the same API as replica set transactions.
When running a transaction against a sharded cluster, the session is
pinned to the mongos server selected for the first operation in the
transaction. All subsequent operations that are part of the same transaction
are routed to the same mongos server. When the transaction is completed, by
running either commitTransaction or abortTransaction, the session is unpinned.

.. seealso:: The MongoDB documentation on `transactions <https://dochub.mongodb.org/core/transactions>`_.

.. _async-snapshot-reads-ref:

Snapshot Reads
==============

.. versionadded:: 3.12

MongoDB 5.0 adds support for snapshot reads. Snapshot reads are requested by
passing the ``snapshot`` option to
:meth:`~pymongo.asynchronous.mongo_client.AsyncMongoClient.start_session`.
If ``snapshot`` is True, all read operations that use this session read data
from the same snapshot timestamp. The server chooses the latest
majority-committed snapshot timestamp when executing the first read operation
using the session. Subsequent reads on this session read from the same
snapshot timestamp. Snapshot reads are also supported when reading from
replica set secondaries.

.. code-block:: python

  # Each read using this session reads data from the same point in time.
  async with client.start_session(snapshot=True) as session:
      order = await orders.find_one({"sku": "abc123"}, session=session)
      inventory = await inventory.find_one({"sku": "abc123"}, session=session)

Snapshot Reads Limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^

Snapshot reads sessions are incompatible with ``causal_consistency=True``.
Only the following read operations are supported in a snapshot reads session:

- :meth:`~pymongo.asynchronous.collection.AsyncCollection.find`
- :meth:`~pymongo.asynchronous.collection.AsyncCollection.find_one`
- :meth:`~pymongo.asynchronous.collection.AsyncCollection.aggregate`
- :meth:`~pymongo.asynchronous.collection.AsyncCollection.count_documents`
- :meth:`~pymongo.asynchronous.collection.AsyncCollection.distinct` (on unsharded collections)

Classes
=======
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Mapping, MutableMapping
from collections.abc import Mapping as _Mapping
from contextlib import AbstractAsyncContextManager
from contextvars import ContextVar, Token
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    NoReturn,
    Optional,
    TypeVar,
)

from bson.int64 import Int64
from bson.timestamp import Timestamp
from pymongo import _csot
from pymongo.asynchronous.cursor_base import _ConnectionManager
from pymongo.client_session_shared import (
    _BACKOFF_INITIAL,
    _BACKOFF_MAX,
    _UNKNOWN_COMMIT_ERROR_CODES,
    SessionOptions,
    TransactionOptions,
    _EmptyServerSession,
    _make_timeout_error,
    _max_time_expired_error,
    _reraise_with_unknown_commit,
    _TxnState,
    _within_time_limit,
)
from pymongo.errors import (
    ConfigurationError,
    ConnectionFailure,
    InvalidOperation,
    OperationFailure,
    PyMongoError,
    WTimeoutError,
)
from pymongo.operations import _WRITES_WITH_CLUSTER_TIME
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference, _ServerMode
from pymongo.server_type import SERVER_TYPE
from pymongo.write_concern import WriteConcern

if TYPE_CHECKING:
    from types import TracebackType

    from pymongo.asynchronous.pool import AsyncConnection
    from pymongo.asynchronous.server import Server
    from pymongo.typings import ClusterTime, _Address

_IS_SYNC = False

# Re-export SessionOptions and TransactionOptions here so Sphinx includes them in the docs
__all__ = ["AsyncClientSession", "SessionOptions", "TransactionOptions"]

_SESSION: ContextVar[Optional[AsyncClientSession]] = ContextVar("SESSION", default=None)


class _AsyncBoundSessionContext:
    """Context manager returned by AsyncClientSession.bind() that manages bound state."""

    def __init__(self, session: AsyncClientSession, end_session: bool) -> None:
        self._session = session
        self._session_token: Optional[Token[AsyncClientSession]] = None
        self._end_session = end_session

    async def __aenter__(self) -> AsyncClientSession:
        self._session_token = _SESSION.set(self._session)  # type: ignore[assignment]
        return self._session

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._session_token:
            _SESSION.reset(self._session_token)  # type: ignore[arg-type]
            self._session_token = None
        if self._end_session:
            await self._session.end_session()


class _TransactionContext:
    """Internal transaction context manager for start_transaction."""

    def __init__(self, session: AsyncClientSession):
        self.__session = session

    async def __aenter__(self) -> _TransactionContext:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.__session.in_transaction:
            if exc_val is None:
                await self.__session.commit_transaction()
            else:
                await self.__session.abort_transaction()


class _Transaction:
    """Internal class to hold transaction information in a AsyncClientSession."""

    def __init__(self, opts: Optional[TransactionOptions], client: AsyncMongoClient[Any]):
        self.opts = opts
        self.state = _TxnState.NONE
        self.sharded = False
        self.pinned_address: Optional[_Address] = None
        self.conn_mgr: Optional[_ConnectionManager] = None
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
    def pinned_conn(self) -> Optional[AsyncConnection]:
        if self.active() and self.conn_mgr:
            return self.conn_mgr.conn
        return None

    def pin(self, server: Server, conn: AsyncConnection) -> None:
        self.sharded = True
        self.pinned_address = server.description.address
        if server.description.server_type == SERVER_TYPE.LoadBalancer:
            conn.pin_txn()
            self.conn_mgr = _ConnectionManager(conn, False)

    async def unpin(self) -> None:
        self.pinned_address = None
        if self.conn_mgr:
            await self.conn_mgr.close()
        self.conn_mgr = None

    async def reset(self) -> None:
        await self.unpin()
        self.state = _TxnState.NONE
        self.sharded = False
        self.recovery_token = None
        self.attempt = 0
        self.has_completed_command = False

    def __del__(self) -> None:
        if self.conn_mgr:
            # Reuse the cursor closing machinery to return the socket to the
            # pool soon.
            self.client._close_cursor_soon(0, None, self.conn_mgr)
            self.conn_mgr = None


_T = TypeVar("_T")

if TYPE_CHECKING:
    from pymongo.asynchronous.mongo_client import AsyncMongoClient


class AsyncClientSession:
    """A session for ordering sequential operations.

    :class:`AsyncClientSession` instances are **not thread-safe or fork-safe**.
    They can only be used by one thread or process at a time. A single
    :class:`AsyncClientSession` cannot be used to run multiple operations
    concurrently.

    Should not be initialized directly by application developers - to create a
    :class:`AsyncClientSession`, call
    :meth:`~pymongo.asynchronous.mongo_client.AsyncMongoClient.start_session`.
    """

    def __init__(
        self,
        client: AsyncMongoClient[Any],
        server_session: Any,
        options: SessionOptions,
        implicit: bool,
    ) -> None:
        # An AsyncMongoClient, a _ServerSession, a SessionOptions, and a set.
        self._client: AsyncMongoClient[Any] = client
        self._server_session = server_session
        self._options = options
        self._cluster_time: Optional[Mapping[str, Any]] = None
        self._operation_time: Optional[Timestamp] = None
        self._snapshot_time = None
        # Is this an implicitly created session?
        self._implicit = implicit
        self._transaction = _Transaction(None, client)
        # Is this session attached to a cursor?
        self._attached_to_cursor = False
        # Should we leave the session alive when the cursor is closed?
        self._leave_alive = False

    async def end_session(self) -> None:
        """Finish this session. If a transaction has started, abort it.

        It is an error to use the session after the session has ended.
        """
        await self._end_session(lock=True)

    async def _end_session(self, lock: bool) -> None:
        if self._server_session is not None:
            try:
                if self.in_transaction:
                    await self.abort_transaction()
                # It's possible we're still pinned here when the transaction
                # is in the committed state when the session is discarded.
                await self._unpin()
            finally:
                self._client._return_server_session(self._server_session)
                self._server_session = None

    def _end_implicit_session(self) -> None:
        # Implicit sessions can't be part of transactions or pinned connections
        if not self._leave_alive and self._server_session is not None:
            self._client._return_server_session(self._server_session)
            self._server_session = None

    def _check_ended(self) -> None:
        if self._server_session is None:
            raise InvalidOperation("Cannot use ended session")

    def bind(self, end_session: bool = True) -> _AsyncBoundSessionContext:
        """Bind this session so it is implicitly passed to all database operations within the returned context.

        .. code-block:: python

           async with client.start_session() as s:
               async with s.bind():
                   # session=s is passed implicitly
                   await client.db.collection.insert_one({"x": 1})

        :param end_session: Whether to end the session on exiting the returned context. Defaults to True.
            If set to False, :meth:`~pymongo.asynchronous.client_session.AsyncClientSession.end_session()` must be called
            once the session is no longer used.

        .. versionadded:: 4.17
        """
        return _AsyncBoundSessionContext(self, end_session)

    async def __aenter__(self) -> AsyncClientSession:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self._end_session(lock=True)

    @property
    def client(self) -> AsyncMongoClient[Any]:
        """The :class:`~pymongo.asynchronous.mongo_client.AsyncMongoClient` this session was
        created from.
        """
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

    async def with_transaction(
        self,
        callback: Callable[[AsyncClientSession], Awaitable[_T]],
        read_concern: Optional[ReadConcern] = None,
        write_concern: Optional[WriteConcern] = None,
        read_preference: Optional[_ServerMode] = None,
        max_commit_time_ms: Optional[int] = None,
    ) -> _T:
        """Execute a callback in a transaction.

        This method starts a transaction on this session, executes ``callback``
        once, and then commits the transaction. For example::

          async def callback(session):
              orders = session.client.db.orders
              inventory = session.client.db.inventory
              await orders.insert_one({"sku": "abc123", "qty": 100}, session=session)
              await inventory.update_one({"sku": "abc123", "qty": {"$gte": 100}},
                                   {"$inc": {"qty": -100}}, session=session)

          async with client.start_session() as session:
              await session.with_transaction(callback)

        To pass arbitrary arguments to the ``callback``, wrap your callable
        with a ``lambda`` like this::

          async def callback(session, custom_arg, custom_kwarg=None):
              # Transaction operations...

          async with client.start_session() as session:
              await session.with_transaction(
                  lambda s: callback(s, "custom_arg", custom_kwarg=1))

        In the event of an exception, ``with_transaction`` may retry the commit
        or the entire transaction, therefore ``callback`` may be invoked
        multiple times by a single call to ``with_transaction``. Developers
        should be mindful of this possibility when writing a ``callback`` that
        modifies application state or has any other side-effects.
        Note that even when the ``callback`` is invoked multiple times,
        ``with_transaction`` ensures that the transaction will be committed
        at-most-once on the server.

        The ``callback`` should not attempt to start new transactions, but
        should simply run operations meant to be contained within a
        transaction. The ``callback`` should also not commit the transaction;
        this is handled automatically by ``with_transaction``. If the
        ``callback`` does commit or abort the transaction without error,
        however, ``with_transaction`` will return without taking further
        action.

        :class:`AsyncClientSession` instances are **not thread-safe or fork-safe**.
        Consequently, the ``callback`` must not attempt to execute multiple
        operations concurrently.

        When ``callback`` raises an exception, ``with_transaction``
        automatically aborts the current transaction. When ``callback`` or
        :meth:`~AsyncClientSession.commit_transaction` raises an exception that
        includes the ``"TransientTransactionError"`` error label,
        ``with_transaction`` starts a new transaction and re-executes
        the ``callback``.

        The ``callback`` MUST NOT silently handle command errors
        without allowing such errors to propagate. Command errors may abort the
        transaction on the server, and an attempt to commit the transaction will
        be rejected with a ``NoSuchTransaction`` error.  For more information see
        the `transactions specification`_.

        When :meth:`~AsyncClientSession.commit_transaction` raises an exception with
        the ``"UnknownTransactionCommitResult"`` error label,
        ``with_transaction`` retries the commit until the result of the
        transaction is known.

        This method will cease retrying after 120 seconds has elapsed. This
        timeout is not configurable and any exception raised by the
        ``callback`` or by :meth:`AsyncClientSession.commit_transaction` after the
        timeout is reached will be re-raised. Applications that desire a
        different timeout duration should not use this method.

        :param callback: The callable ``callback`` to run inside a transaction.
            The callable must accept a single argument, this session. Note,
            under certain error conditions the callback may be run multiple
            times.
        :param read_concern: The
            :class:`~pymongo.read_concern.ReadConcern` to use for this
            transaction.
        :param write_concern: The
            :class:`~pymongo.write_concern.WriteConcern` to use for this
            transaction.
        :param read_preference: The read preference to use for this
            transaction. If ``None`` (the default) the :attr:`read_preference`
            of this :class:`AsyncDatabase` is used. See
            :mod:`~pymongo.read_preferences` for options.

        :return: The return value of the ``callback``.

        .. versionadded:: 3.9

        .. _transactions specification:
            https://github.com/mongodb/specifications/blob/master/source/transactions-convenient-api/transactions-convenient-api.md#handling-errors-inside-the-callback
        """
        start_time = time.monotonic()
        retry = 0
        last_error: Optional[BaseException] = None
        while True:
            if retry:  # Implement exponential backoff on retry.
                jitter = random.random()  # noqa: S311
                backoff = jitter * min(_BACKOFF_INITIAL * (1.5**retry), _BACKOFF_MAX)
                if not _within_time_limit(start_time, backoff):
                    assert last_error is not None
                    raise _make_timeout_error(last_error) from last_error
                await asyncio.sleep(backoff)
            retry += 1
            await self.start_transaction(
                read_concern, write_concern, read_preference, max_commit_time_ms
            )
            try:
                ret = await callback(self)
            # Catch KeyboardInterrupt, CancelledError, etc. and cleanup.
            except BaseException as exc:
                last_error = exc
                if self.in_transaction:
                    await self.abort_transaction()
                if isinstance(exc, PyMongoError) and exc.has_error_label(
                    "TransientTransactionError"
                ):
                    if _within_time_limit(start_time):
                        # Retry the entire transaction.
                        continue
                    raise _make_timeout_error(last_error) from exc
                raise

            if not self.in_transaction:
                # Assume callback intentionally ended the transaction.
                return ret

            while True:
                try:
                    await self.commit_transaction()
                except PyMongoError as exc:
                    last_error = exc
                    if exc.has_error_label(
                        "UnknownTransactionCommitResult"
                    ) and not _max_time_expired_error(exc):
                        if not _within_time_limit(start_time):
                            raise _make_timeout_error(last_error) from exc
                        # Retry the commit.
                        continue

                    if exc.has_error_label("TransientTransactionError"):
                        if not _within_time_limit(start_time):
                            raise _make_timeout_error(last_error) from exc
                        # Retry the entire transaction.
                        break
                    raise

                # Commit succeeded.
                return ret

    async def start_transaction(
        self,
        read_concern: Optional[ReadConcern] = None,
        write_concern: Optional[WriteConcern] = None,
        read_preference: Optional[_ServerMode] = None,
        max_commit_time_ms: Optional[int] = None,
    ) -> AbstractAsyncContextManager[Any]:
        """Start a multi-statement transaction.

        Takes the same arguments as :class:`TransactionOptions`.

        .. versionchanged:: 3.9
           Added the ``max_commit_time_ms`` option.

        .. versionadded:: 3.7
        """
        self._check_ended()

        if self.options.snapshot:
            raise InvalidOperation("Transactions are not supported in snapshot sessions")

        if self.in_transaction:
            raise InvalidOperation("Transaction already in progress")

        read_concern = self._inherit_option("read_concern", read_concern)
        write_concern = self._inherit_option("write_concern", write_concern)
        read_preference = self._inherit_option("read_preference", read_preference)
        if max_commit_time_ms is None:
            opts = self.options.default_transaction_options
            if opts:
                max_commit_time_ms = opts.max_commit_time_ms

        self._transaction.opts = TransactionOptions(
            read_concern, write_concern, read_preference, max_commit_time_ms
        )
        await self._transaction.reset()
        self._transaction.state = _TxnState.STARTING
        self._start_retryable_write()
        return _TransactionContext(self)

    async def commit_transaction(self) -> None:
        """Commit a multi-statement transaction.

        .. versionadded:: 3.7
        """
        self._check_ended()
        state = self._transaction.state
        if state is _TxnState.NONE:
            raise InvalidOperation("No transaction started")
        elif state in (_TxnState.STARTING, _TxnState.COMMITTED_EMPTY):
            # Server transaction was never started, no need to send a command.
            self._transaction.state = _TxnState.COMMITTED_EMPTY
            return
        elif state is _TxnState.ABORTED:
            raise InvalidOperation("Cannot call commitTransaction after calling abortTransaction")
        elif state is _TxnState.COMMITTED:
            # We're explicitly retrying the commit, move the state back to
            # "in progress" so that in_transaction returns true.
            self._transaction.state = _TxnState.IN_PROGRESS

        try:
            await self._finish_transaction_with_retry("commitTransaction")
        except ConnectionFailure as exc:
            # We do not know if the commit was successfully applied on the
            # server or if it satisfied the provided write concern, set the
            # unknown commit error label.
            exc._remove_error_label("TransientTransactionError")
            _reraise_with_unknown_commit(exc)
        except WTimeoutError as exc:
            # We do not know if the commit has satisfied the provided write
            # concern, add the unknown commit error label.
            _reraise_with_unknown_commit(exc)
        except OperationFailure as exc:
            if exc.code not in _UNKNOWN_COMMIT_ERROR_CODES:
                # The server reports errorLabels in the case.
                raise
            # We do not know if the commit was successfully applied on the
            # server or if it satisfied the provided write concern, set the
            # unknown commit error label.
            _reraise_with_unknown_commit(exc)
        finally:
            self._transaction.state = _TxnState.COMMITTED

    async def abort_transaction(self) -> None:
        """Abort a multi-statement transaction.

        .. versionadded:: 3.7
        """
        self._check_ended()

        state = self._transaction.state
        if state is _TxnState.NONE:
            raise InvalidOperation("No transaction started")
        elif state is _TxnState.STARTING:
            # Server transaction was never started, no need to send a command.
            self._transaction.state = _TxnState.ABORTED
            return
        elif state is _TxnState.ABORTED:
            raise InvalidOperation("Cannot call abortTransaction twice")
        elif state in (_TxnState.COMMITTED, _TxnState.COMMITTED_EMPTY):
            raise InvalidOperation("Cannot call abortTransaction after calling commitTransaction")

        try:
            await self._finish_transaction_with_retry("abortTransaction")
        except (OperationFailure, ConnectionFailure):
            # The transactions spec says to ignore abortTransaction errors.
            pass
        finally:
            self._transaction.state = _TxnState.ABORTED
            await self._unpin()

    async def _finish_transaction_with_retry(self, command_name: str) -> dict[str, Any]:
        """Run commit or abort with one retry after any retryable error.

        :param command_name: Either "commitTransaction" or "abortTransaction".
        """

        async def func(
            _session: Optional[AsyncClientSession], conn: AsyncConnection, _retryable: bool
        ) -> dict[str, Any]:
            return await self._finish_transaction(conn, command_name)

        return await self._client._retry_internal(
            func, self, None, retryable=True, operation=command_name
        )

    async def _finish_transaction(self, conn: AsyncConnection, command_name: str) -> dict[str, Any]:
        self._transaction.attempt += 1
        opts = self._transaction.opts
        assert opts
        wc = opts.write_concern
        cmd = {command_name: 1}
        if command_name == "commitTransaction":
            if opts.max_commit_time_ms and _csot.get_timeout() is None:
                cmd["maxTimeMS"] = opts.max_commit_time_ms

            # Transaction spec says that after the initial commit attempt,
            # subsequent commitTransaction commands should be upgraded to use
            # w:"majority" and set a default value of 10 seconds for wtimeout.
            if self._transaction.attempt > 1:
                assert wc
                wc_doc = wc.document
                wc_doc["w"] = "majority"
                wc_doc.setdefault("wtimeout", 10000)
                wc = WriteConcern(**wc_doc)

        if self._transaction.recovery_token:
            cmd["recoveryToken"] = self._transaction.recovery_token

        return await self._client.admin._command(
            conn, cmd, session=self, write_concern=wc, parse_write_concern_error=True
        )

    def _advance_cluster_time(self, cluster_time: Optional[Mapping[str, Any]]) -> None:
        """Internal cluster time helper."""
        if self._cluster_time is None:
            self._cluster_time = cluster_time
        elif cluster_time is not None:
            if cluster_time["clusterTime"] > self._cluster_time["clusterTime"]:
                self._cluster_time = cluster_time

    def advance_cluster_time(self, cluster_time: Mapping[str, Any]) -> None:
        """Update the cluster time for this session.

        :param cluster_time: The
            :data:`~pymongo.asynchronous.client_session.AsyncClientSession.cluster_time` from
            another `AsyncClientSession` instance.
        """
        if not isinstance(cluster_time, _Mapping):
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

        :param operation_time: The
            :data:`~pymongo.asynchronous.client_session.AsyncClientSession.operation_time` from
            another `AsyncClientSession` instance.
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
    def _pinned_connection(self) -> Optional[AsyncConnection]:
        """The connection this transaction was started on."""
        return self._transaction.pinned_conn

    def _pin(self, server: Server, conn: AsyncConnection) -> None:
        """Pin this session to the given Server or to the given connection."""
        self._transaction.pin(server, conn)

    async def _unpin(self) -> None:
        """Unpin this session from any pinned Server."""
        await self._transaction.unpin()

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
        conn: AsyncConnection,
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

    def _update_read_concern(self, cmd: MutableMapping[str, Any], conn: AsyncConnection) -> None:
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
        raise TypeError("A AsyncClientSession cannot be copied, create a new session instead")
