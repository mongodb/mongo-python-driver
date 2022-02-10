# Copyright 2017 MongoDB, Inc.
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

"""Logical sessions for ordering sequential operations.

Requires MongoDB 3.6.

.. versionadded:: 3.6

Causally Consistent Reads
=========================

.. code-block:: python

  with client.start_session(causal_consistency=True) as session:
      collection = client.db.collection
      collection.update_one({'_id': 1}, {'$set': {'x': 10}}, session=session)
      secondary_c = collection.with_options(
          read_preference=ReadPreference.SECONDARY)

      # A secondary read waits for replication of the write.
      secondary_c.find_one({'_id': 1}, session=session)

If `causal_consistency` is True (the default), read operations that use
the session are causally after previous read and write operations. Using a
causally consistent session, an application can read its own writes and is
guaranteed monotonic reads, even when reading from replica set secondaries.

.. seealso:: The MongoDB documentation on `causal-consistency <https://dochub.mongodb.org/core/causal-consistency>`_.

.. _transactions-ref:

Transactions
============

.. versionadded:: 3.7

MongoDB 4.0 adds support for transactions on replica set primaries. A
transaction is associated with a :class:`ClientSession`. To start a transaction
on a session, use :meth:`ClientSession.start_transaction` in a with-statement.
Then, execute an operation within the transaction by passing the session to the
operation:

.. code-block:: python

  orders = client.db.orders
  inventory = client.db.inventory
  with client.start_session() as session:
      with session.start_transaction():
          orders.insert_one({"sku": "abc123", "qty": 100}, session=session)
          inventory.update_one({"sku": "abc123", "qty": {"$gte": 100}},
                               {"$inc": {"qty": -100}}, session=session)

Upon normal completion of ``with session.start_transaction()`` block, the
transaction automatically calls :meth:`ClientSession.commit_transaction`.
If the block exits with an exception, the transaction automatically calls
:meth:`ClientSession.abort_transaction`.

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

.. _snapshot-reads-ref:

Snapshot Reads
==============

.. versionadded:: 3.12

MongoDB 5.0 adds support for snapshot reads. Snapshot reads are requested by
passing the ``snapshot`` option to
:meth:`~pymongo.mongo_client.MongoClient.start_session`.
If ``snapshot`` is True, all read operations that use this session read data
from the same snapshot timestamp. The server chooses the latest
majority-committed snapshot timestamp when executing the first read operation
using the session. Subsequent reads on this session read from the same
snapshot timestamp. Snapshot reads are also supported when reading from
replica set secondaries.

.. code-block:: python

  # Each read using this session reads data from the same point in time.
  with client.start_session(snapshot=True) as session:
      order = orders.find_one({"sku": "abc123"}, session=session)
      inventory = inventory.find_one({"sku": "abc123"}, session=session)

Snapshot Reads Limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^

Snapshot reads sessions are incompatible with ``causal_consistency=True``.
Only the following read operations are supported in a snapshot reads session:

- :meth:`~pymongo.collection.Collection.find`
- :meth:`~pymongo.collection.Collection.find_one`
- :meth:`~pymongo.collection.Collection.aggregate`
- :meth:`~pymongo.collection.Collection.count_documents`
- :meth:`~pymongo.collection.Collection.distinct` (on unsharded collections)

Classes
=======
"""

import collections
import uuid

from bson.binary import Binary
from bson.int64 import Int64
from bson.py3compat import abc, integer_types
from bson.son import SON
from bson.timestamp import Timestamp
from pymongo import monotonic
from pymongo.cursor import _SocketManager
from pymongo.errors import (
    ConfigurationError,
    ConnectionFailure,
    InvalidOperation,
    OperationFailure,
    PyMongoError,
    WTimeoutError,
)
from pymongo.helpers import _RETRYABLE_ERROR_CODES
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference, _ServerMode
from pymongo.server_type import SERVER_TYPE
from pymongo.write_concern import WriteConcern


class SessionOptions(object):
    """Options for a new :class:`ClientSession`.

    :Parameters:
      - `causal_consistency` (optional): If True, read operations are causally
        ordered within the session. Defaults to True when the ``snapshot``
        option is ``False``.
      - `default_transaction_options` (optional): The default
        TransactionOptions to use for transactions started on this session.
      - `snapshot` (optional): If True, then all reads performed using this
        session will read from the same snapshot. This option is incompatible
        with ``causal_consistency=True``. Defaults to ``False``.

    .. versionchanged:: 3.12
       Added the ``snapshot`` parameter.
    """

    def __init__(self, causal_consistency=None, default_transaction_options=None, snapshot=False):
        if snapshot:
            if causal_consistency:
                raise ConfigurationError("snapshot reads do not support " "causal_consistency=True")
            causal_consistency = False
        elif causal_consistency is None:
            causal_consistency = True
        self._causal_consistency = causal_consistency
        if default_transaction_options is not None:
            if not isinstance(default_transaction_options, TransactionOptions):
                raise TypeError(
                    "default_transaction_options must be an instance of "
                    "pymongo.client_session.TransactionOptions, not: %r"
                    % (default_transaction_options,)
                )
        self._default_transaction_options = default_transaction_options
        self._snapshot = snapshot

    @property
    def causal_consistency(self):
        """Whether causal consistency is configured."""
        return self._causal_consistency

    @property
    def default_transaction_options(self):
        """The default TransactionOptions to use for transactions started on
        this session.

        .. versionadded:: 3.7
        """
        return self._default_transaction_options

    @property
    def snapshot(self):
        """Whether snapshot reads are configured.

        .. versionadded:: 3.12
        """
        return self._snapshot


class TransactionOptions(object):
    """Options for :meth:`ClientSession.start_transaction`.

    :Parameters:
      - `read_concern` (optional): The
        :class:`~pymongo.read_concern.ReadConcern` to use for this transaction.
        If ``None`` (the default) the :attr:`read_preference` of
        the :class:`MongoClient` is used.
      - `write_concern` (optional): The
        :class:`~pymongo.write_concern.WriteConcern` to use for this
        transaction. If ``None`` (the default) the :attr:`read_preference` of
        the :class:`MongoClient` is used.
      - `read_preference` (optional): The read preference to use. If
        ``None`` (the default) the :attr:`read_preference` of this
        :class:`MongoClient` is used. See :mod:`~pymongo.read_preferences`
        for options. Transactions which read must use
        :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.
      - `max_commit_time_ms` (optional): The maximum amount of time to allow a
        single commitTransaction command to run. This option is an alias for
        maxTimeMS option on the commitTransaction command. If ``None`` (the
        default) maxTimeMS is not used.

    .. versionchanged:: 3.9
       Added the ``max_commit_time_ms`` option.

    .. versionadded:: 3.7
    """

    def __init__(
        self, read_concern=None, write_concern=None, read_preference=None, max_commit_time_ms=None
    ):
        self._read_concern = read_concern
        self._write_concern = write_concern
        self._read_preference = read_preference
        self._max_commit_time_ms = max_commit_time_ms
        if read_concern is not None:
            if not isinstance(read_concern, ReadConcern):
                raise TypeError(
                    "read_concern must be an instance of "
                    "pymongo.read_concern.ReadConcern, not: %r" % (read_concern,)
                )
        if write_concern is not None:
            if not isinstance(write_concern, WriteConcern):
                raise TypeError(
                    "write_concern must be an instance of "
                    "pymongo.write_concern.WriteConcern, not: %r" % (write_concern,)
                )
            if not write_concern.acknowledged:
                raise ConfigurationError(
                    "transactions do not support unacknowledged write concern"
                    ": %r" % (write_concern,)
                )
        if read_preference is not None:
            if not isinstance(read_preference, _ServerMode):
                raise TypeError(
                    "%r is not valid for read_preference. See "
                    "pymongo.read_preferences for valid "
                    "options." % (read_preference,)
                )
        if max_commit_time_ms is not None:
            if not isinstance(max_commit_time_ms, integer_types):
                raise TypeError("max_commit_time_ms must be an integer or None")

    @property
    def read_concern(self):
        """This transaction's :class:`~pymongo.read_concern.ReadConcern`."""
        return self._read_concern

    @property
    def write_concern(self):
        """This transaction's :class:`~pymongo.write_concern.WriteConcern`."""
        return self._write_concern

    @property
    def read_preference(self):
        """This transaction's :class:`~pymongo.read_preferences.ReadPreference`."""
        return self._read_preference

    @property
    def max_commit_time_ms(self):
        """The maxTimeMS to use when running a commitTransaction command.

        .. versionadded:: 3.9
        """
        return self._max_commit_time_ms


def _validate_session_write_concern(session, write_concern):
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
                    "unacknowledged write concern: %r" % (write_concern,)
                )
    return session


class _TransactionContext(object):
    """Internal transaction context manager for start_transaction."""

    def __init__(self, session):
        self.__session = session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__session.in_transaction:
            if exc_val is None:
                self.__session.commit_transaction()
            else:
                self.__session.abort_transaction()


class _TxnState(object):
    NONE = 1
    STARTING = 2
    IN_PROGRESS = 3
    COMMITTED = 4
    COMMITTED_EMPTY = 5
    ABORTED = 6


class _Transaction(object):
    """Internal class to hold transaction information in a ClientSession."""

    def __init__(self, opts, client):
        self.opts = opts
        self.state = _TxnState.NONE
        self.sharded = False
        self.pinned_address = None
        self.sock_mgr = None
        self.recovery_token = None
        self.attempt = 0
        self.client = client

    def active(self):
        return self.state in (_TxnState.STARTING, _TxnState.IN_PROGRESS)

    def starting(self):
        return self.state == _TxnState.STARTING

    @property
    def pinned_conn(self):
        if self.active() and self.sock_mgr:
            return self.sock_mgr.sock
        return None

    def pin(self, server, sock_info):
        self.sharded = True
        self.pinned_address = server.description.address
        if server.description.server_type == SERVER_TYPE.LoadBalancer:
            sock_info.pin_txn()
            self.sock_mgr = _SocketManager(sock_info, False)

    def unpin(self):
        self.pinned_address = None
        if self.sock_mgr:
            self.sock_mgr.close()
        self.sock_mgr = None

    def reset(self):
        self.unpin()
        self.state = _TxnState.NONE
        self.sharded = False
        self.recovery_token = None
        self.attempt = 0

    def __del__(self):
        if self.sock_mgr:
            # Reuse the cursor closing machinery to return the socket to the
            # pool soon.
            self.client._close_cursor_soon(0, None, self.sock_mgr)
            self.sock_mgr = None


def _reraise_with_unknown_commit(exc):
    """Re-raise an exception with the UnknownTransactionCommitResult label."""
    exc._add_error_label("UnknownTransactionCommitResult")
    raise


def _max_time_expired_error(exc):
    """Return true if exc is a MaxTimeMSExpired error."""
    return isinstance(exc, OperationFailure) and exc.code == 50


# From the transactions spec, all the retryable writes errors plus
# WriteConcernFailed.
_UNKNOWN_COMMIT_ERROR_CODES = _RETRYABLE_ERROR_CODES | frozenset(
    [
        64,  # WriteConcernFailed
        50,  # MaxTimeMSExpired
    ]
)

# From the Convenient API for Transactions spec, with_transaction must
# halt retries after 120 seconds.
# This limit is non-configurable and was chosen to be twice the 60 second
# default value of MongoDB's `transactionLifetimeLimitSeconds` parameter.
_WITH_TRANSACTION_RETRY_TIME_LIMIT = 120


def _within_time_limit(start_time):
    """Are we within the with_transaction retry limit?"""
    return monotonic.time() - start_time < _WITH_TRANSACTION_RETRY_TIME_LIMIT


class ClientSession(object):
    """A session for ordering sequential operations.

    :class:`ClientSession` instances are **not thread-safe or fork-safe**.
    They can only be used by one thread or process at a time. A single
    :class:`ClientSession` cannot be used to run multiple operations
    concurrently.

    Should not be initialized directly by application developers - to create a
    :class:`ClientSession`, call
    :meth:`~pymongo.mongo_client.MongoClient.start_session`.
    """

    def __init__(self, client, server_session, options, authset, implicit):
        # A MongoClient, a _ServerSession, a SessionOptions, and a set.
        self._client = client
        self._server_session = server_session
        self._options = options
        self._authset = authset
        self._cluster_time = None
        self._operation_time = None
        self._snapshot_time = None
        # Is this an implicitly created session?
        self._implicit = implicit
        self._transaction = _Transaction(None, client)

    def end_session(self):
        """Finish this session. If a transaction has started, abort it.

        It is an error to use the session after the session has ended.
        """
        self._end_session(lock=True)

    def _end_session(self, lock):
        if self._server_session is not None:
            try:
                if self.in_transaction:
                    self.abort_transaction()
                # It's possible we're still pinned here when the transaction
                # is in the committed state when the session is discarded.
                self._unpin()
            finally:
                self._client._return_server_session(self._server_session, lock)
                self._server_session = None

    def _check_ended(self):
        if self._server_session is None:
            raise InvalidOperation("Cannot use ended session")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._end_session(lock=True)

    @property
    def client(self):
        """The :class:`~pymongo.mongo_client.MongoClient` this session was
        created from.
        """
        return self._client

    @property
    def options(self):
        """The :class:`SessionOptions` this session was created with."""
        return self._options

    @property
    def session_id(self):
        """A BSON document, the opaque server session identifier."""
        self._check_ended()
        return self._server_session.session_id

    @property
    def cluster_time(self):
        """The cluster time returned by the last operation executed
        in this session.
        """
        return self._cluster_time

    @property
    def operation_time(self):
        """The operation time returned by the last operation executed
        in this session.
        """
        return self._operation_time

    def _inherit_option(self, name, val):
        """Return the inherited TransactionOption value."""
        if val:
            return val
        txn_opts = self.options.default_transaction_options
        val = txn_opts and getattr(txn_opts, name)
        if val:
            return val
        return getattr(self.client, name)

    def with_transaction(
        self,
        callback,
        read_concern=None,
        write_concern=None,
        read_preference=None,
        max_commit_time_ms=None,
    ):
        """Execute a callback in a transaction.

        This method starts a transaction on this session, executes ``callback``
        once, and then commits the transaction. For example::

          def callback(session):
              orders = session.client.db.orders
              inventory = session.client.db.inventory
              orders.insert_one({"sku": "abc123", "qty": 100}, session=session)
              inventory.update_one({"sku": "abc123", "qty": {"$gte": 100}},
                                   {"$inc": {"qty": -100}}, session=session)

          with client.start_session() as session:
              session.with_transaction(callback)

        To pass arbitrary arguments to the ``callback``, wrap your callable
        with a ``lambda`` like this::

          def callback(session, custom_arg, custom_kwarg=None):
              # Transaction operations...

          with client.start_session() as session:
              session.with_transaction(
                  lambda s: callback(s, "custom_arg", custom_kwarg=1))

        In the event of an exception, ``with_transaction`` may retry the commit
        or the entire transaction, therefore ``callback`` may be invoked
        multiple times by a single call to ``with_transaction``. Developers
        should be mindful of this possiblity when writing a ``callback`` that
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

        :class:`ClientSession` instances are **not thread-safe or fork-safe**.
        Consequently, the ``callback`` must not attempt to execute multiple
        operations concurrently.

        When ``callback`` raises an exception, ``with_transaction``
        automatically aborts the current transaction. When ``callback`` or
        :meth:`~ClientSession.commit_transaction` raises an exception that
        includes the ``"TransientTransactionError"`` error label,
        ``with_transaction`` starts a new transaction and re-executes
        the ``callback``.

        When :meth:`~ClientSession.commit_transaction` raises an exception with
        the ``"UnknownTransactionCommitResult"`` error label,
        ``with_transaction`` retries the commit until the result of the
        transaction is known.

        This method will cease retrying after 120 seconds has elapsed. This
        timeout is not configurable and any exception raised by the
        ``callback`` or by :meth:`ClientSession.commit_transaction` after the
        timeout is reached will be re-raised. Applications that desire a
        different timeout duration should not use this method.

        :Parameters:
          - `callback`: The callable ``callback`` to run inside a transaction.
            The callable must accept a single argument, this session. Note,
            under certain error conditions the callback may be run multiple
            times.
          - `read_concern` (optional): The
            :class:`~pymongo.read_concern.ReadConcern` to use for this
            transaction.
          - `write_concern` (optional): The
            :class:`~pymongo.write_concern.WriteConcern` to use for this
            transaction.
          - `read_preference` (optional): The read preference to use for this
            transaction. If ``None`` (the default) the :attr:`read_preference`
            of this :class:`Database` is used. See
            :mod:`~pymongo.read_preferences` for options.

        :Returns:
          The return value of the ``callback``.

        .. versionadded:: 3.9
        """
        start_time = monotonic.time()
        while True:
            self.start_transaction(read_concern, write_concern, read_preference, max_commit_time_ms)
            try:
                ret = callback(self)
            except Exception as exc:
                if self.in_transaction:
                    self.abort_transaction()
                if (
                    isinstance(exc, PyMongoError)
                    and exc.has_error_label("TransientTransactionError")
                    and _within_time_limit(start_time)
                ):
                    # Retry the entire transaction.
                    continue
                raise

            if not self.in_transaction:
                # Assume callback intentionally ended the transaction.
                return ret

            while True:
                try:
                    self.commit_transaction()
                except PyMongoError as exc:
                    if (
                        exc.has_error_label("UnknownTransactionCommitResult")
                        and _within_time_limit(start_time)
                        and not _max_time_expired_error(exc)
                    ):
                        # Retry the commit.
                        continue

                    if exc.has_error_label("TransientTransactionError") and _within_time_limit(
                        start_time
                    ):
                        # Retry the entire transaction.
                        break
                    raise

                # Commit succeeded.
                return ret

    def start_transaction(
        self, read_concern=None, write_concern=None, read_preference=None, max_commit_time_ms=None
    ):
        """Start a multi-statement transaction.

        Takes the same arguments as :class:`TransactionOptions`.

        .. versionchanged:: 3.9
           Added the ``max_commit_time_ms`` option.

        .. versionadded:: 3.7
        """
        self._check_ended()

        if self.options.snapshot:
            raise InvalidOperation("Transactions are not supported in " "snapshot sessions")

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
        self._transaction.reset()
        self._transaction.state = _TxnState.STARTING
        self._start_retryable_write()
        return _TransactionContext(self)

    def commit_transaction(self):
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
            self._finish_transaction_with_retry("commitTransaction")
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

    def abort_transaction(self):
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
            self._finish_transaction_with_retry("abortTransaction")
        except (OperationFailure, ConnectionFailure):
            # The transactions spec says to ignore abortTransaction errors.
            pass
        finally:
            self._transaction.state = _TxnState.ABORTED
            self._unpin()

    def _finish_transaction_with_retry(self, command_name):
        """Run commit or abort with one retry after any retryable error.

        :Parameters:
          - `command_name`: Either "commitTransaction" or "abortTransaction".
        """

        def func(session, sock_info, retryable):
            return self._finish_transaction(sock_info, command_name)

        return self._client._retry_internal(True, func, self, None)

    def _finish_transaction(self, sock_info, command_name):
        self._transaction.attempt += 1
        opts = self._transaction.opts
        wc = opts.write_concern
        cmd = SON([(command_name, 1)])
        if command_name == "commitTransaction":
            if opts.max_commit_time_ms:
                cmd["maxTimeMS"] = opts.max_commit_time_ms

            # Transaction spec says that after the initial commit attempt,
            # subsequent commitTransaction commands should be upgraded to use
            # w:"majority" and set a default value of 10 seconds for wtimeout.
            if self._transaction.attempt > 1:
                wc_doc = wc.document
                wc_doc["w"] = "majority"
                wc_doc.setdefault("wtimeout", 10000)
                wc = WriteConcern(**wc_doc)

        if self._transaction.recovery_token:
            cmd["recoveryToken"] = self._transaction.recovery_token

        return self._client.admin._command(
            sock_info, cmd, session=self, write_concern=wc, parse_write_concern_error=True
        )

    def _advance_cluster_time(self, cluster_time):
        """Internal cluster time helper."""
        if self._cluster_time is None:
            self._cluster_time = cluster_time
        elif cluster_time is not None:
            if cluster_time["clusterTime"] > self._cluster_time["clusterTime"]:
                self._cluster_time = cluster_time

    def advance_cluster_time(self, cluster_time):
        """Update the cluster time for this session.

        :Parameters:
          - `cluster_time`: The
            :data:`~pymongo.client_session.ClientSession.cluster_time` from
            another `ClientSession` instance.
        """
        if not isinstance(cluster_time, abc.Mapping):
            raise TypeError("cluster_time must be a subclass of collections.Mapping")
        if not isinstance(cluster_time.get("clusterTime"), Timestamp):
            raise ValueError("Invalid cluster_time")
        self._advance_cluster_time(cluster_time)

    def _advance_operation_time(self, operation_time):
        """Internal operation time helper."""
        if self._operation_time is None:
            self._operation_time = operation_time
        elif operation_time is not None:
            if operation_time > self._operation_time:
                self._operation_time = operation_time

    def advance_operation_time(self, operation_time):
        """Update the operation time for this session.

        :Parameters:
          - `operation_time`: The
            :data:`~pymongo.client_session.ClientSession.operation_time` from
            another `ClientSession` instance.
        """
        if not isinstance(operation_time, Timestamp):
            raise TypeError("operation_time must be an instance " "of bson.timestamp.Timestamp")
        self._advance_operation_time(operation_time)

    def _process_response(self, reply):
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
    def has_ended(self):
        """True if this session is finished."""
        return self._server_session is None

    @property
    def in_transaction(self):
        """True if this session has an active multi-statement transaction.

        .. versionadded:: 3.10
        """
        return self._transaction.active()

    @property
    def _starting_transaction(self):
        """True if this session is starting a multi-statement transaction."""
        return self._transaction.starting()

    @property
    def _pinned_address(self):
        """The mongos address this transaction was created on."""
        if self._transaction.active():
            return self._transaction.pinned_address
        return None

    @property
    def _pinned_connection(self):
        """The connection this transaction was started on."""
        return self._transaction.pinned_conn

    def _pin(self, server, sock_info):
        """Pin this session to the given Server or to the given connection."""
        self._transaction.pin(server, sock_info)

    def _unpin(self):
        """Unpin this session from any pinned Server."""
        self._transaction.unpin()

    def _txn_read_preference(self):
        """Return read preference of this transaction or None."""
        if self.in_transaction:
            return self._transaction.opts.read_preference
        return None

    def _apply_to(self, command, is_retryable, read_preference, sock_info):
        self._check_ended()

        if self.options.snapshot:
            self._update_read_concern(command, sock_info)

        self._server_session.last_use = monotonic.time()
        command["lsid"] = self._server_session.session_id

        if is_retryable:
            command["txnNumber"] = self._server_session.transaction_id
            return

        if self.in_transaction:
            if read_preference != ReadPreference.PRIMARY:
                raise InvalidOperation(
                    "read preference in a transaction must be primary, not: "
                    "%r" % (read_preference,)
                )

            if self._transaction.state == _TxnState.STARTING:
                # First command begins a new transaction.
                self._transaction.state = _TxnState.IN_PROGRESS
                command["startTransaction"] = True

                if self._transaction.opts.read_concern:
                    rc = self._transaction.opts.read_concern.document
                    if rc:
                        command["readConcern"] = rc
                self._update_read_concern(command, sock_info)

            command["txnNumber"] = self._server_session.transaction_id
            command["autocommit"] = False

    def _start_retryable_write(self):
        self._check_ended()
        self._server_session.inc_transaction_id()

    def _update_read_concern(self, cmd, sock_info):
        if self.options.causal_consistency and self.operation_time is not None:
            cmd.setdefault("readConcern", {})["afterClusterTime"] = self.operation_time
        if self.options.snapshot:
            if sock_info.max_wire_version < 13:
                raise ConfigurationError("Snapshot reads require MongoDB 5.0 or later")
            rc = cmd.setdefault("readConcern", {})
            rc["level"] = "snapshot"
            if self._snapshot_time is not None:
                rc["atClusterTime"] = self._snapshot_time


class _ServerSession(object):
    def __init__(self, generation):
        # Ensure id is type 4, regardless of CodecOptions.uuid_representation.
        self.session_id = {"id": Binary(uuid.uuid4().bytes, 4)}
        self.last_use = monotonic.time()
        self._transaction_id = 0
        self.dirty = False
        self.generation = generation

    def mark_dirty(self):
        """Mark this session as dirty.

        A server session is marked dirty when a command fails with a network
        error. Dirty sessions are later discarded from the server session pool.
        """
        self.dirty = True

    def timed_out(self, session_timeout_minutes):
        idle_seconds = monotonic.time() - self.last_use

        # Timed out if we have less than a minute to live.
        return idle_seconds > (session_timeout_minutes - 1) * 60

    @property
    def transaction_id(self):
        """Positive 64-bit integer."""
        return Int64(self._transaction_id)

    def inc_transaction_id(self):
        self._transaction_id += 1


class _ServerSessionPool(collections.deque):
    """Pool of _ServerSession objects.

    This class is not thread-safe, access it while holding the Topology lock.
    """

    def __init__(self, *args, **kwargs):
        super(_ServerSessionPool, self).__init__(*args, **kwargs)
        self.generation = 0

    def reset(self):
        self.generation += 1
        self.clear()

    def pop_all(self):
        ids = []
        while self:
            ids.append(self.pop().session_id)
        return ids

    def get_server_session(self, session_timeout_minutes):
        # Although the Driver Sessions Spec says we only clear stale sessions
        # in return_server_session, PyMongo can't take a lock when returning
        # sessions from a __del__ method (like in Cursor.__die), so it can't
        # clear stale sessions there. In case many sessions were returned via
        # __del__, check for stale sessions here too.
        self._clear_stale(session_timeout_minutes)

        # The most recently used sessions are on the left.
        while self:
            s = self.popleft()
            if not s.timed_out(session_timeout_minutes):
                return s

        return _ServerSession(self.generation)

    def return_server_session(self, server_session, session_timeout_minutes):
        if session_timeout_minutes is not None:
            self._clear_stale(session_timeout_minutes)
            if server_session.timed_out(session_timeout_minutes):
                return
        self.return_server_session_no_lock(server_session)

    def return_server_session_no_lock(self, server_session):
        # Discard sessions from an old pool to avoid duplicate sessions in the
        # child process after a fork.
        if server_session.generation == self.generation and not server_session.dirty:
            self.appendleft(server_session)

    def _clear_stale(self, session_timeout_minutes):
        # Clear stale sessions. The least recently used are on the right.
        while self:
            if self[-1].timed_out(session_timeout_minutes):
                self.pop()
            else:
                # The remaining sessions also haven't timed out.
                break
