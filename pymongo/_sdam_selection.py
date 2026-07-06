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

"""Sans-I/O server-selection tie-breaking.

Pure implementation of the SDAM "power of two random choices" load balancing
used to pick among equally-suitable servers. It is generic over the server type
(via a ``load`` callable), performs no I/O, and is shared by the synchronous and
asynchronous topologies.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from typing import TYPE_CHECKING, Callable, Optional, TypeVar

from pymongo.server_selectors import Selection, writable_server_selector
from pymongo.topology_description import TOPOLOGY_TYPE

if TYPE_CHECKING:
    from pymongo.topology_description import TopologyDescription
    from pymongo.typings import _Address

_T = TypeVar("_T")


def format_selection_error(
    description: TopologyDescription,
    selector: Callable[[Selection], Selection],
    replica_set_name: Optional[str],
    seed_addresses: Sequence[_Address],
) -> str:
    """Build the human-readable reason server selection failed.

    Pure formatting over a :class:`TopologyDescription`; performs no I/O. Shared
    by the synchronous and asynchronous topologies so the diagnostic text has a
    single source of truth.
    """
    is_replica_set = description.topology_type in (
        TOPOLOGY_TYPE.ReplicaSetWithPrimary,
        TOPOLOGY_TYPE.ReplicaSetNoPrimary,
    )

    if is_replica_set:
        server_plural = "replica set members"
    elif description.topology_type == TOPOLOGY_TYPE.Sharded:
        server_plural = "mongoses"
    else:
        server_plural = "servers"

    if description.known_servers:
        # We've connected, but no servers match the selector.
        if selector is writable_server_selector:
            if is_replica_set:
                return "No primary available for writes"
            else:
                return f"No {server_plural} available for writes"
        else:
            return f'No {server_plural} match selector "{selector}"'
    else:
        addresses = list(description.server_descriptions())
        servers = list(description.server_descriptions().values())
        if not servers:
            if is_replica_set:
                # We removed all servers because of the wrong setName?
                return f'No {server_plural} available for replica set name "{replica_set_name}"'
            else:
                return f"No {server_plural} available"

        # 1 or more servers, all Unknown. Are they unknown for one reason?
        error = servers[0].error
        same = all(server.error == error for server in servers[1:])
        if same:
            if error is None:
                # We're still discovering.
                return f"No {server_plural} found yet"

            if is_replica_set and not set(addresses).intersection(seed_addresses):
                # We replaced our seeds with new hosts but can't reach any.
                return (
                    f"Could not reach any servers in {addresses}. Replica set is"
                    " configured with internal hostnames or IPs?"
                )

            return str(error)
        else:
            return ",".join(str(server.error) for server in servers if server.error)


def select_least_loaded(
    candidates: Sequence[_T],
    load: Callable[[_T], int],
    *,
    sample: Callable[[Sequence[_T], int], Sequence[_T]] = random.sample,
) -> _T:
    """Choose a server from ``candidates`` using power-of-two random choices.

    Per the SDAM spec, when more than one server is suitable we sample two at
    random and pick the one with the lower in-progress operation count, which
    spreads load without a full scan.

    :param candidates: The suitable servers; must be non-empty.
    :param load: Returns a candidate's current operation count.
    :param sample: Injectable sampler (defaults to :func:`random.sample`),
        used to make selection deterministic in tests.
    """
    if len(candidates) == 1:
        return candidates[0]
    first, second = sample(candidates, 2)
    return first if load(first) <= load(second) else second
