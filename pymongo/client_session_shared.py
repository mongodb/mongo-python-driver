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
from typing import (
    TYPE_CHECKING,
    Any,
    NoReturn,
    Optional,
    TypeVar,
)

from bson.binary import Binary
from bson.int64 import Int64
from pymongo import _csot
from pymongo.errors import (
    ConfigurationError,
    ExecutionTimeout,
    NetworkTimeout,
    OperationFailure,
    PyMongoError,
)
from pymongo.helpers_shared import _RETRYABLE_ERROR_CODES
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode
from pymongo.write_concern import WriteConcern

if TYPE_CHECKING:
    from pymongo.typings import _AgnosticClientSession

_ClientSessionT = TypeVar("_ClientSessionT", bound="_AgnosticClientSession")


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
