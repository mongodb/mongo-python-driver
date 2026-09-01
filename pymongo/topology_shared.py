# Copyright 2014-present MongoDB, Inc.
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

"""Topology utilities and shared helper methods."""

from __future__ import annotations

import queue
import weakref
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from bson import ObjectId
    from pymongo.server_description import ServerDescription


def process_events_queue(queue_ref: weakref.ReferenceType[queue.Queue]) -> bool:  # type: ignore[type-arg]
    q = queue_ref()
    if not q:
        return False  # Cancel PeriodicExecutor.

    while True:
        try:
            event = q.get_nowait()
        except queue.Empty:
            break
        else:
            fn, args = event
            fn(*args)

    return True  # Continue PeriodicExecutor.


class _ErrorContext:
    """An error with context for SDAM error handling."""

    def __init__(
        self,
        error: BaseException,
        max_wire_version: int,
        sock_generation: int,
        completed_handshake: bool,
        service_id: Optional[ObjectId],
    ):
        self.error = error
        self.max_wire_version = max_wire_version
        self.sock_generation = sock_generation
        self.completed_handshake = completed_handshake
        self.service_id = service_id


def _is_stale_error_topology_version(
    current_tv: Optional[Mapping[str, Any]], error_tv: Optional[Mapping[str, Any]]
) -> bool:
    """Return True if the error's topologyVersion is <= current."""
    if current_tv is None or error_tv is None:
        return False
    if current_tv["processId"] != error_tv["processId"]:
        return False
    return current_tv["counter"] >= error_tv["counter"]


def _is_stale_server_description(current_sd: ServerDescription, new_sd: ServerDescription) -> bool:
    """Return True if the new topologyVersion is < current."""
    current_tv, new_tv = current_sd.topology_version, new_sd.topology_version
    if current_tv is None or new_tv is None:
        return False
    if current_tv["processId"] != new_tv["processId"]:
        return False
    return current_tv["counter"] > new_tv["counter"]
