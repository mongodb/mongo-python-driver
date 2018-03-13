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

.. mongodoc:: causal-consistency

Classes
=======
"""

import collections
import uuid

from bson.binary import Binary
from bson.int64 import Int64
from bson.py3compat import abc
from bson.timestamp import Timestamp

from pymongo import monotonic
from pymongo.errors import InvalidOperation


class SessionOptions(object):
    """Options for a new :class:`ClientSession`.

    :Parameters:
      - `causal_consistency` (optional): If True (the default), read
        operations are causally ordered within the session.
      - `auto_start_transaction` (optional): If True, any operation using
        the session begins a transaction if none is in progress.
    """
    # TODO: accept a TransactionOptions.
    def __init__(self,
                 causal_consistency=True,
                 auto_start_transaction=False):
        self._causal_consistency = causal_consistency
        self._auto_start_transaction = auto_start_transaction

    @property
    def causal_consistency(self):
        """Whether causal consistency is configured."""
        return self._causal_consistency

    @property
    def auto_start_transaction(self):
        """Whether the session is configured to always start a transaction."""
        return self._auto_start_transaction


class TransactionOptions(object):
    """Options for :meth:`ClientSession.start_transaction`.
    
    :Parameters:
      - `read_concern`: The :class:`~read_concern.ReadConcern` to use for this 
        transaction.
      - `write_concern`: The :class:`~write_concern.WriteConcern` to use for 
        this transaction.
    """
    def __init__(self, read_concern=None, write_concern=None):
        # TODO: validate arguments.
        self._read_concern = read_concern
        self._write_concern = write_concern
    
    @property
    def read_concern(self):
        """This transaction's :class:`~read_concern.ReadConcern`."""
        return self._read_concern
    
    @property
    def write_concern(self):
        """This transaction's :class:`~write_concern.WriteConcern`."""
        return self._write_concern
        

class ClientSession(object):
    """A session for ordering sequential operations."""
    def __init__(self, client, server_session, options, authset):
        # A MongoClient, a _ServerSession, a SessionOptions, and a set.
        self._client = client
        self._server_session = server_session
        self._options = options
        self._authset = authset
        self._cluster_time = None
        self._operation_time = None
        if self.options.auto_start_transaction:
            # TODO: Get transaction options from self.options.
            self._current_transaction_opts = TransactionOptions()
        else:
            self._current_transaction_opts = None

    def end_session(self):
        """Finish this session. If a transaction has started, abort it.

        It is an error to use the session or any derived
        :class:`~pymongo.database.Database`,
        :class:`~pymongo.collection.Collection`, or
        :class:`~pymongo.cursor.Cursor` after the session has ended.
        """
        self._end_session(lock=True, abort_txn=True)

    def _end_session(self, lock, abort_txn):
        if self._server_session is not None:
            try:
                if self._current_transaction_opts is not None:
                    if abort_txn:
                        self.abort_transaction()
                    else:
                        self.commit_transaction()
            finally:
                self._client._return_server_session(self._server_session, lock)
                self._server_session = None
                self._current_transaction_opts = None

    def _check_ended(self):
        if self._server_session is None:
            raise InvalidOperation("Cannot use ended session")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Abort when exiting with an exception, otherwise commit.
        # TODO: test and document this.
        self._end_session(lock=True, abort_txn=exc_val is not None)

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

    def start_transaction(self, **kwargs):
        """Start a multi-statement transaction.

        Takes the same arguments as :class:`TransactionOptions`.

        Do not use this method if the session is configured to automatically
        start a transaction.
        """
        self._check_ended()

        if self._current_transaction_opts is not None:
            raise InvalidOperation("Transaction already in progress")

        self._current_transaction_opts = TransactionOptions(**kwargs)
        self._server_session.statement_id = 0

    def commit_transaction(self):
        """Commit a multi-statement transaction."""
        self._finish_transaction("commitTransaction")

    def abort_transaction(self):
        """Abort a multi-statement transaction."""
        self._finish_transaction("abortTransaction")

    def _finish_transaction(self, command_name):
        if self._current_transaction_opts is None:
            raise InvalidOperation("No transaction in progress")

        if self._server_session.statement_id == 0:
            # Not really started.
            return

        if command_name == 'abortTransaction':
            assert False, "Not implemented"  # Await server.

        try:
            # TODO: retryable. And it's weird to pass parse_write_concern_error
            # from outside database.py.
            self._client.admin.command(
                command_name,
                txnNumber=self._server_session.transaction_id,
                session=self,
                write_concern=self._current_transaction_opts.write_concern,
                parse_write_concern_error=True)
        finally:
            self._server_session.reset_transaction()
            self._current_transaction_opts = None

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
            raise TypeError(
                "cluster_time must be a subclass of collections.Mapping")
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
            raise TypeError("operation_time must be an instance "
                            "of bson.timestamp.Timestamp")
        self._advance_operation_time(operation_time)

    @property
    def has_ended(self):
        """True if this session is finished."""
        return self._server_session is None

    @property
    def in_transaction(self):
        """True if this session has an active multi-statement transaction."""
        return self._current_transaction_opts is not None

    def _apply_to(self, command, is_retryable):
        self._check_ended()

        if self.options.auto_start_transaction and not self.in_transaction:
            self.start_transaction()

        self._server_session.last_use = monotonic.time()
        command['lsid'] = self._server_session.session_id

        if is_retryable:
            self._server_session._transaction_id += 1
            command['txnNumber'] = self._server_session.transaction_id
            return

        if self._current_transaction_opts:
            if self._server_session.statement_id == 0:
                # First statement begins a new transaction.
                self._server_session._transaction_id += 1
                command['readConcern'] = {'level': 'snapshot'}
                command['autocommit'] = False

            command['txnNumber'] = self._server_session.transaction_id
            # TODO: Allow stmtId for find/getMore, SERVER-33213.
            name = next(iter(command))
            if name not in ('find', 'getMore'):
                command['stmtId'] = self._server_session.statement_id
                self._server_session.statement_id += 1

    def _advance_statement_id(self, n):
        self._check_ended()
        self._server_session.advance_statement_id(n)

    def _retry_transaction_id(self):
        self._check_ended()
        self._server_session.retry_transaction_id()


class _ServerSession(object):
    def __init__(self):
        # Ensure id is type 4, regardless of CodecOptions.uuid_representation.
        self.session_id = {'id': Binary(uuid.uuid4().bytes, 4)}
        self.last_use = monotonic.time()
        self._transaction_id = 0
        self.statement_id = 0

    def timed_out(self, session_timeout_minutes):
        idle_seconds = monotonic.time() - self.last_use

        # Timed out if we have less than a minute to live.
        return idle_seconds > (session_timeout_minutes - 1) * 60

    def advance_statement_id(self, n):
        # Every command advances the statement id by 1 already.
        self.statement_id += (n - 1)

    @property
    def transaction_id(self):
        """Positive 64-bit integer."""
        return Int64(self._transaction_id)

    def reset_transaction(self):
        self.statement_id = 0

    def retry_transaction_id(self):
        self._transaction_id -= 1


class _ServerSessionPool(collections.deque):
    """Pool of _ServerSession objects.

    This class is not thread-safe, access it while holding the Topology lock.
    """
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

        return _ServerSession()

    def return_server_session(self, server_session, session_timeout_minutes):
        self._clear_stale(session_timeout_minutes)
        if not server_session.timed_out(session_timeout_minutes):
            self.appendleft(server_session)

    def return_server_session_no_lock(self, server_session):
        self.appendleft(server_session)

    def _clear_stale(self, session_timeout_minutes):
        # Clear stale sessions. The least recently used are on the right.
        while self:
            if self[-1].timed_out(session_timeout_minutes):
                self.pop()
            else:
                # The remaining sessions also haven't timed out.
                break
