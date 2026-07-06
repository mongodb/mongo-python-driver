# Copyright 2026-present MongoDB, Inc.
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

"""Sans-I/O core for SDAM application-error handling.

This module contains the *pure* decision logic that classifies an application
error and decides which Server Discovery And Monitoring (SDAM) actions to take,
independent of any I/O or concurrency. Because it performs no ``await`` and
acquires no locks, both the synchronous and asynchronous topologies import and
call it directly -- there is a single implementation of the SDAM error
state-transition rules, and it can be unit-tested without a live server.

The caller (``Topology._handle_error``) is responsible only for *executing* the
returned :class:`_SDAMAction` (marking the server Unknown, resetting the pool,
requesting a check, cancelling the monitor check), each of which is inherently
I/O- or concurrency-colored and therefore stays in the topology layer.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, NamedTuple, Optional

from pymongo import helpers_shared

if TYPE_CHECKING:
    from pymongo.server_description import ServerDescription
from pymongo.errors import (
    ConnectionFailure,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
    WaitQueueTimeoutError,
    WriteError,
)


def error_topology_version(error: Optional[BaseException]) -> Optional[Mapping[str, Any]]:
    """Extract the ``topologyVersion`` from an error's details, if present.

    Pure helper: returns the ``topologyVersion`` mapping embedded in a command
    error's ``details``, or ``None`` when the error carries none.
    """
    if error and hasattr(error, "details") and isinstance(error.details, dict):
        return error.details.get("topologyVersion")
    return None


def is_stale_error_topology_version(
    current_tv: Optional[Mapping[str, Any]], error_tv: Optional[Mapping[str, Any]]
) -> bool:
    """Return True if the error's topologyVersion is <= the current one.

    Pure comparison of two ``topologyVersion`` mappings; an error whose
    topologyVersion is not newer than what we already know about the server is
    stale and should be ignored.
    """
    if current_tv is None or error_tv is None:
        return False
    if current_tv["processId"] != error_tv["processId"]:
        return False
    return current_tv["counter"] >= error_tv["counter"]


def is_stale_server_description(current_sd: ServerDescription, new_sd: ServerDescription) -> bool:
    """Return True if ``new_sd``'s topologyVersion is older than ``current_sd``'s.

    Pure comparison used to discard a server description update that is stale
    relative to what the topology already knows.
    """
    current_tv, new_tv = current_sd.topology_version, new_sd.topology_version
    if current_tv is None or new_tv is None:
        return False
    if current_tv["processId"] != new_tv["processId"]:
        return False
    return current_tv["counter"] > new_tv["counter"]


class _SDAMAction(NamedTuple):
    """The set of SDAM actions to take in response to an application error.

    This is the pure, immutable result of classifying an error; it contains no
    I/O. All fields default to ``False`` (i.e. "do nothing").
    """

    # Replace the server's description with type Unknown.
    mark_unknown: bool = False
    # Clear the server's connection pool.
    reset_pool: bool = False
    # Request an immediate check of the server.
    request_check: bool = False
    # Cancel the in-progress monitor hello check.
    cancel_check: bool = False


# Shared singleton for the common "ignore this error" outcome.
_NO_ACTION = _SDAMAction()


def decide_error_action(
    error: BaseException,
    *,
    load_balanced: bool,
    has_service_id: bool,
    completed_handshake: bool,
    max_wire_version: int,
) -> _SDAMAction:
    """Classify an application error into the SDAM actions it requires.

    This is the sans-I/O core of ``Topology._handle_error``. It implements the
    Server Discovery And Monitoring specification's application-error rules and
    performs no I/O.

    :param error: The error raised by the failed operation.
    :param load_balanced: Whether the client is in load-balanced mode.
    :param has_service_id: Whether a service ID is known for the connection.
    :param completed_handshake: Whether the connection finished its handshake
        before the error occurred.
    :param max_wire_version: The connection's negotiated max wire version.

    :return: An :class:`_SDAMAction` describing which state transitions the
        topology should perform. Returns :data:`_NO_ACTION` when the error
        should be ignored.
    """
    # Ignore a handshake error if the server is behind a load balancer but the
    # service ID is unknown. This indicates that the error happened when dialing
    # the connection or during the MongoDB handshake, so we don't know the
    # service ID to use for clearing the pool.
    if load_balanced and not has_service_id and not completed_handshake:
        return _NO_ACTION

    if isinstance(error, NetworkTimeout) and completed_handshake:
        # The socket has been closed. Don't reset the server. SDAM Spec: "When
        # an application operation fails because of any network error besides a
        # socket timeout...."
        return _NO_ACTION
    elif isinstance(error, WriteError):
        # Ignore writeErrors.
        return _NO_ACTION
    elif isinstance(error, (NotPrimaryError, OperationFailure)):
        # As per the SDAM spec if:
        #   - the server sees a "not primary" error, and
        #   - the server is not shutting down, and
        #   - the server version is >= 4.2, then
        # we keep the existing connection pool, but mark the server type as
        # Unknown and request an immediate check of the server. Otherwise, we
        # clear the connection pool, mark the server as Unknown and request an
        # immediate check of the server.
        if hasattr(error, "code"):
            err_code = error.code
        else:
            # Default error code if one does not exist.
            default = 10107 if isinstance(error, NotPrimaryError) else None
            err_code = error.details.get("code", default)  # type: ignore[union-attr]
        if err_code in helpers_shared._NOT_PRIMARY_CODES:
            is_shutting_down = err_code in helpers_shared._SHUTDOWN_CODES
            # Mark server Unknown, clear the pool, and request check.
            return _SDAMAction(
                mark_unknown=not load_balanced,
                reset_pool=is_shutting_down or (max_wire_version <= 7),
                request_check=True,
            )
        elif not completed_handshake:
            # Unknown command error during the connection handshake.
            return _SDAMAction(mark_unknown=not load_balanced, reset_pool=True)
        return _NO_ACTION
    elif isinstance(error, ConnectionFailure):
        if isinstance(error, WaitQueueTimeoutError) or error.has_error_label(
            "SystemOverloadedError"
        ):
            return _NO_ACTION
        # "Client MUST replace the server's description with type Unknown ...
        # MUST NOT request an immediate check of the server." When a client
        # marks a server Unknown from a network error when reading or writing,
        # it MUST cancel the hello check on that server and close the current
        # monitoring connection.
        return _SDAMAction(mark_unknown=not load_balanced, reset_pool=True, cancel_check=True)

    return _NO_ACTION
