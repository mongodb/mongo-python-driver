# Copyright 2009-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Internal helpers for MongoClient, shared between the asynchronous and synchronous APIs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from pymongo import _csot
from pymongo._telemetry import _generate_op_id_or_none, log_command_retry
from pymongo.errors import (
    BulkWriteError,
    ClientBulkWriteException,
    ConnectionFailure,
    NotPrimaryError,
    OperationFailure,
    PyMongoError,
    WaitQueueTimeoutError,
)
from pymongo.lock import _release_locks
from pymongo.logger import (
    _CLIENT_LOGGER,
    _log_or_warn,
)
from pymongo.server_selectors import writable_server_selector

if TYPE_CHECKING:
    from pymongo.read_preferences import _ServerMode
    from pymongo.server_selectors import Selection
    from pymongo.typings import (
        _Address,
        _AgnosticBulk,
        _AgnosticClientBulk,
        _AgnosticClientSession,
        _AgnosticMongoClient,
        _AgnosticServer,
    )

T = TypeVar("T")
_ServerT = TypeVar("_ServerT", bound="_AgnosticServer")
_FuncT = TypeVar("_FuncT", bound=Callable[..., Any])


def _retryable_error_doc(exc: PyMongoError) -> Mapping[str, Any] | None:
    """Return the server response from PyMongo exception or None."""
    if isinstance(exc, (BulkWriteError, ClientBulkWriteException)):
        # Check the last writeConcernError to determine if this
        # BulkWriteError is retryable.
        wces = exc.details["writeConcernErrors"]
        return wces[-1] if wces else None
    if isinstance(exc, (NotPrimaryError, OperationFailure)):
        return cast("Mapping[str, Any]", exc.details)
    return None


def _add_retryable_write_error(exc: PyMongoError) -> None:
    doc = _retryable_error_doc(exc)
    if doc:
        code = doc.get("code", 0)
        # retryWrites on MMAPv1 should raise an actionable error.
        if code == 20 and str(exc).startswith("Transaction numbers"):
            errmsg = (
                "This MongoDB deployment does not support "
                "retryable writes. Please add retryWrites=false "
                "to your connection string."
            )
            raise OperationFailure(errmsg, code, exc.details)  # type: ignore[attr-defined]
        for label in doc.get("errorLabels", []):
            exc._add_error_label(label)

    # Connection errors are always retryable except NotPrimaryError and WaitQueueTimeoutError which is
    # handled above.
    if isinstance(exc, ClientBulkWriteException):
        exc_to_check = exc.error
    else:
        exc_to_check = exc
    if isinstance(exc_to_check, ConnectionFailure) and not isinstance(
        exc_to_check, (NotPrimaryError, WaitQueueTimeoutError)
    ):
        exc_to_check._add_error_label("RetryableWriteError")


def _after_fork_child(clients: Mapping[Any, Any]) -> None:
    """Releases the locks in child process and resets the
    topologies in the passed clients.
    """
    # Reinitialize locks
    _release_locks()

    # Perform cleanup in clients (i.e. get rid of topology)
    for client in clients.values():
        client._after_fork()


def _detect_external_db(entity: str) -> bool:
    """Detects external database hosts and logs an informational message at the INFO level."""
    entity = entity.lower()
    cosmos_db_hosts = [".cosmos.azure.com"]
    document_db_hosts = [".docdb.amazonaws.com", ".docdb-elastic.amazonaws.com"]

    for host in cosmos_db_hosts:
        if entity.endswith(host):
            _log_or_warn(
                _CLIENT_LOGGER,
                "You appear to be connected to a CosmosDB cluster. For more information regarding feature "
                "compatibility and support please visit https://www.mongodb.com/supportability/cosmosdb",
            )
            return True
    for host in document_db_hosts:
        if entity.endswith(host):
            _log_or_warn(
                _CLIENT_LOGGER,
                "You appear to be connected to a DocumentDB cluster. For more information regarding feature "
                "compatibility and support please visit https://www.mongodb.com/supportability/documentdb",
            )
            return True
    return False


class _AgnosticClientConnectionRetryableBase(Generic[T, _ServerT, _FuncT]):
    """Responsible for executing retryable connections on read or write operations"""

    __slots__ = (
        "_address",
        "_always_retryable",
        "_attempt_number",
        "_base_backoff_ms",
        "_bulk",
        "_client",
        "_deprioritized_servers",
        "_func",
        "_is_aggregate_write",
        "_is_read",
        "_is_run_command",
        "_last_error",
        "_max_retries",
        "_operation",
        "_operation_id",
        "_read_pref",
        "_retry_policy",
        "_retryable",
        "_retrying",
        "_server",
        "_server_selector",
        "_session",
    )

    def __init__(
        self,
        mongo_client: _AgnosticMongoClient,
        func: _FuncT,
        bulk: _AgnosticBulk | _AgnosticClientBulk | None,
        operation: str,
        is_read: bool = False,
        session: _AgnosticClientSession | None = None,
        read_pref: _ServerMode | None = None,
        address: _Address | None = None,
        retryable: bool = False,
        operation_id: int | None = None,
        is_run_command: bool = False,
        is_aggregate_write: bool = False,
    ):
        self._last_error: Exception | None = None
        self._retrying = False
        self._always_retryable = False
        self._max_retries = float("inf") if _csot.get_timeout() is not None else 1
        self._client = mongo_client
        self._retry_policy = mongo_client._retry_policy
        self._func: _FuncT = func
        self._bulk = bulk
        self._session = session
        self._is_read = is_read
        self._retryable = retryable
        self._read_pref = read_pref
        self._server_selector: Callable[[Selection], Selection] = (
            read_pref if is_read else writable_server_selector  # type: ignore
        )
        self._address = address
        self._server: _ServerT = None  # type: ignore
        self._deprioritized_servers: list[_ServerT] | None = None
        self._operation = operation
        # Only generate an operation id when APM/logging is enabled.
        if operation_id is None:
            operation_id = _generate_op_id_or_none(self._client._event_listeners)
        self._operation_id = operation_id
        self._attempt_number = 0
        self._is_run_command = is_run_command
        self._is_aggregate_write = is_aggregate_write
        self._base_backoff_ms: float | None = None

    def _is_not_eligible_for_retry(self) -> bool:
        """Checks if the exchange is not eligible for retry"""
        return not self._retryable or (
            self._is_retrying() and self._attempt_number >= self._max_retries
        )

    def _is_retrying(self) -> bool:
        """Checks if the exchange is currently undergoing a retry"""
        return self._bulk.retrying if self._bulk else self._retrying

    def _is_session_state_retryable(self) -> bool:
        """Checks if provided session is eligible for retry

        reads: Make sure there is no ongoing transaction (if provided a session)
        writes: Make sure there is a session without an active transaction
        """
        if self._is_read:
            return not (self._session and self._session.in_transaction)
        return bool(self._session and not self._session.in_transaction)

    def _check_last_error(self, check_csot: bool = False) -> None:
        """Checks if the ongoing client exchange experienced a exception previously.
        If so, raise last error

        :param check_csot: Checks CSOT to ensure we are retrying with time remaining defaults to False
        """
        if self._is_retrying():
            remaining = _csot.remaining()
            if not check_csot or (remaining is not None and remaining <= 0):
                assert self._last_error is not None
                raise self._last_error

    def _log_retry(self, is_write: bool) -> None:
        log_command_retry(
            self._client._topology_id,
            self._operation,
            self._operation_id,
            self._attempt_number,
            is_write,
        )
