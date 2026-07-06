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

"""Sans-I/O decision helpers for SDAM server monitoring.

Pure predicates over a :class:`~pymongo.server_description.ServerDescription`
shared by the synchronous and asynchronous monitors. They perform no I/O and
hold no locks, so a single implementation serves both flavors and can be
unit-tested without a live server.
"""

from __future__ import annotations

from typing import Any, Optional, Protocol


class _StreamableServer(Protocol):
    """The minimal view of a ServerDescription these predicates depend on."""

    @property
    def is_server_type_known(self) -> bool: ...

    @property
    def topology_version(self) -> Optional[Any]: ...


def is_streaming_check(stream: bool, sd: _StreamableServer) -> bool:
    """Return True if the monitor will use the streaming protocol for ``sd``.

    A monitor uses the streaming (awaitable ``hello``) protocol when streaming
    is enabled and the server's type and topologyVersion are already known.
    This same condition decides whether an in-flight check is "awaited" and
    whether to continue streaming immediately after a successful check.
    """
    return bool(stream and sd.is_server_type_known and sd.topology_version)
