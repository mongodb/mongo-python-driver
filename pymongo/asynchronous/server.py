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

"""Communicate with one MongoDB server in a topology."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

from pymongo._telemetry import _SdamTelemetry

if TYPE_CHECKING:
    from queue import Queue
    from weakref import ReferenceType

    from bson.objectid import ObjectId
    from pymongo.asynchronous.mongo_client import _MongoClientErrorHandler
    from pymongo.asynchronous.monitor import Monitor
    from pymongo.asynchronous.pool import AsyncConnection, Pool
    from pymongo.monitoring import _EventListeners
    from pymongo.server_description import ServerDescription

_IS_SYNC = False


class Server:
    def __init__(
        self,
        server_description: ServerDescription,
        pool: Pool,
        monitor: Monitor,
        topology_id: Optional[ObjectId] = None,
        listeners: Optional[_EventListeners] = None,
        events: Optional[ReferenceType[Queue[Any]]] = None,
    ) -> None:
        """Represent one MongoDB server."""
        self._description = server_description
        self._pool = pool
        self._monitor = monitor
        _events = events() if listeners is not None and listeners.enabled_for_server else None  # type: ignore[misc]
        self._sdam = _SdamTelemetry(topology_id, listeners, _events)  # type: ignore[arg-type]

    async def open(self) -> None:
        """Start monitoring, or restart after a fork.

        Multiple calls have no effect.
        """
        if not self._pool.opts.load_balanced:
            self._monitor.open()

    async def reset(self, service_id: Optional[ObjectId] = None) -> None:
        """Clear the connection pool."""
        await self.pool.reset(service_id)

    async def close(self) -> None:
        """Clear the connection pool and stop the monitor.

        Reconnect with open().
        """
        self._sdam.server_closed(self._description.address)

        await self._monitor.close()
        await self._pool.close()

    def request_check(self) -> None:
        """Check the server's state soon."""
        self._monitor.request_check()

    async def checkout(
        self, handler: Optional[_MongoClientErrorHandler] = None
    ) -> AbstractAsyncContextManager[AsyncConnection]:
        return self.pool.checkout(handler)

    @property
    def description(self) -> ServerDescription:
        return self._description

    @description.setter
    def description(self, server_description: ServerDescription) -> None:
        assert server_description.address == self._description.address
        self._description = server_description

    @property
    def pool(self) -> Pool:
        return self._pool

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self._description!r}>"
