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

import logging
from contextlib import AbstractContextManager
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

from pymongo.logger import (
    _SDAM_LOGGER,
    _debug_log,
    _SDAMStatusMessage,
)

if TYPE_CHECKING:
    from queue import Queue
    from weakref import ReferenceType

    from bson.objectid import ObjectId
    from pymongo.monitoring import _EventListeners
    from pymongo.server_description import ServerDescription
    from pymongo.synchronous.mongo_client import _MongoClientErrorHandler
    from pymongo.synchronous.monitor import Monitor
    from pymongo.synchronous.pool import Connection, Pool

_IS_SYNC = True


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
        self._topology_id = topology_id
        self._publish = listeners is not None and listeners.enabled_for_server
        self._listener = listeners
        self._events = None
        if self._publish:
            self._events = events()  # type: ignore[misc]

    def open(self) -> None:
        """Start monitoring, or restart after a fork.

        Multiple calls have no effect.
        """
        if not self._pool.opts.load_balanced:
            self._monitor.open()

    def reset(self, service_id: Optional[ObjectId] = None) -> None:
        """Clear the connection pool."""
        self.pool.reset(service_id)

    def close(self) -> None:
        """Clear the connection pool and stop the monitor.

        Reconnect with open().
        """
        if self._publish:
            assert self._listener is not None
            assert self._events is not None
            self._events.put(
                (
                    self._listener.publish_server_closed,
                    (self._description.address, self._topology_id),
                )
            )
        if _SDAM_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _SDAM_LOGGER,
                message=_SDAMStatusMessage.STOP_SERVER,
                topologyId=self._topology_id,
                serverHost=self._description.address[0],
                serverPort=self._description.address[1],
            )

        self._monitor.close()
        self._pool.close()

    def request_check(self) -> None:
        """Check the server's state soon."""
        self._monitor.request_check()

    def checkout(
        self, handler: Optional[_MongoClientErrorHandler] = None
    ) -> AbstractContextManager[Connection]:
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
