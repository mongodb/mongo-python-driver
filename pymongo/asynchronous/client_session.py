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
from collections.abc import Awaitable
from contextlib import AbstractAsyncContextManager
from contextvars import ContextVar, Token
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    TypeVar,
)

from pymongo import _csot
from pymongo.asynchronous.cursor_base import _ConnectionManager
from pymongo.client_session_shared import (
    _BACKOFF_INITIAL,
    _BACKOFF_MAX,
    _UNKNOWN_COMMIT_ERROR_CODES,
    SessionOptions,  # noqa: F401  (re-exported for pymongo.client_session)
    TransactionOptions,
    _AgnosticClientSessionBase,
    _make_timeout_error,
    _max_time_expired_error,
    _reraise_with_unknown_commit,
    _TransactionBase,
    _TxnState,
    _within_time_limit,
)
from pymongo.errors import (
    ConnectionFailure,
    InvalidOperation,
    OperationFailure,
    PyMongoError,
    WTimeoutError,
)
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode
from pymongo.write_concern import WriteConcern

if TYPE_CHECKING:
    from types import TracebackType

    from pymongo.asynchronous.mongo_client import AsyncMongoClient
    from pymongo.asynchronous.pool import AsyncConnection

_IS_SYNC = False

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


class _Transaction(_TransactionBase["AsyncConnection"]):
    """Internal class to hold transaction information in a AsyncClientSession."""

    _conn_mgr_cls = _ConnectionManager
    conn_mgr: Optional[_ConnectionManager]

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


_T = TypeVar("_T")


class AsyncClientSession(
    _AgnosticClientSessionBase[
        "AsyncMongoClient[Any]", "AsyncConnection", _AsyncBoundSessionContext
    ]
):
    """A session for ordering sequential operations.

    :class:`AsyncClientSession` instances are **not thread-safe or fork-safe**.
    They can only be used by one thread or process at a time. A single
    :class:`AsyncClientSession` cannot be used to run multiple operations
    concurrently.

    Should not be initialized directly by application developers - to create a
    :class:`AsyncClientSession`, call
    :meth:`~pymongo.asynchronous.mongo_client.AsyncMongoClient.start_session`.
    """

    _transaction_cls = _Transaction
    _bound_session_context_cls = _AsyncBoundSessionContext
    _transaction: _Transaction
    _client: AsyncMongoClient[Any]

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

    async def __aenter__(self) -> AsyncClientSession:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self._end_session(lock=True)

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

    async def _unpin(self) -> None:
        """Unpin this session from any pinned Server."""
        await self._transaction.unpin()
