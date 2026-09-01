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

from collections.abc import Mapping
from typing import Any, Optional, cast

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


def _retryable_error_doc(exc: PyMongoError) -> Optional[Mapping[str, Any]]:
    """Return the server response from PyMongo exception or None."""
    if isinstance(exc, (BulkWriteError, ClientBulkWriteException)):
        # Check the last writeConcernError to determine if this
        # BulkWriteError is retryable.
        wces = exc.details["writeConcernErrors"]
        return wces[-1] if wces else None
    if isinstance(exc, (NotPrimaryError, OperationFailure)):
        return cast(Mapping[str, Any], exc.details)
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
    for _, client in clients.items():
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
