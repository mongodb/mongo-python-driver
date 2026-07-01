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
    Callable,
    Optional,
    Union,
)

from pymongo._telemetry import _SdamTelemetry
from pymongo.asynchronous.command_runner import run_cursor_command
from pymongo.asynchronous.helpers import _handle_reauth
from pymongo.message import _GetMore, _OpMsg, _Query
from pymongo.response import PinnedResponse, Response

if TYPE_CHECKING:
    from queue import Queue
    from weakref import ReferenceType

    from bson.objectid import ObjectId
    from pymongo.asynchronous.mongo_client import AsyncMongoClient, _MongoClientErrorHandler
    from pymongo.asynchronous.monitor import Monitor
    from pymongo.asynchronous.pool import AsyncConnection, Pool
    from pymongo.monitoring import _EventListeners
    from pymongo.read_preferences import _ServerMode
    from pymongo.server_description import ServerDescription
    from pymongo.typings import _DocumentOut

_IS_SYNC = False

_CURSOR_DOC_FIELDS = {"cursor": {"firstBatch": 1, "nextBatch": 1}}


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

    async def operation_to_command(
        self, operation: Union[_Query, _GetMore], conn: AsyncConnection, apply_timeout: bool = False
    ) -> tuple[dict[str, Any], str]:
        cmd, db = operation.as_command(conn, apply_timeout)
        # Support auto encryption
        if operation.client._encrypter and not operation.client._encrypter._bypass_auto_encryption:
            cmd = await operation.client._encrypter.encrypt(  # type: ignore[misc, assignment]
                operation.db, cmd, operation.codec_options
            )
        operation.update_command(cmd)

        return cmd, db

    @_handle_reauth
    async def run_operation(
        self,
        conn: AsyncConnection,
        operation: Union[_Query, _GetMore],
        read_preference: _ServerMode,
        listeners: Optional[_EventListeners],
        unpack_res: Callable[..., list[_DocumentOut]],
        client: AsyncMongoClient[Any],
    ) -> Response:
        """Run a _Query or _GetMore operation and return a Response object.

        This method is used only to run _Query/_GetMore operations from
        cursors.
        Can raise ConnectionFailure, OperationFailure, etc.

        :param conn: An AsyncConnection instance.
        :param operation: A _Query or _GetMore object.
        :param read_preference: The read preference to use.
        :param listeners: Instance of _EventListeners or None.
        :param unpack_res: A callable that decodes the wire protocol response.
        :param client: An AsyncMongoClient instance.
        """
        assert listeners is not None

        use_cmd = operation.use_command(conn)
        more_to_come = bool(operation.conn_mgr and operation.conn_mgr.more_to_come)
        cmd, dbn = await self.operation_to_command(operation, conn, use_cmd)
        if more_to_come:
            request_id = 0
            data = b""
            max_doc_size = 0
        else:
            message = operation.get_message(read_preference, conn, use_cmd)
            request_id, data, max_doc_size = self._split_message(message)

        user_fields = _CURSOR_DOC_FIELDS if use_cmd else None

        docs, reply, duration = await run_cursor_command(
            conn,
            cmd,
            dbn,
            request_id,
            data,
            client=client,
            session=operation.session,  # type: ignore[arg-type]
            listeners=listeners,
            codec_options=operation.codec_options,
            user_fields=user_fields,
            command_name=operation.name,
            pool_opts=conn.opts,
            max_doc_size=max_doc_size,
            more_to_come=more_to_come,
            unpack_res=unpack_res,
            cursor_id=operation.cursor_id,
        )
        assert reply is not None

        response: Response

        if client._should_pin_cursor(operation.session) or operation.exhaust:  # type: ignore[arg-type]
            conn.pin_cursor()
            if isinstance(reply, _OpMsg):
                # In OP_MSG, the server keeps sending only if the
                # more_to_come flag is set.
                more_to_come = reply.more_to_come
            else:
                # In OP_REPLY, the server keeps sending until cursor_id is 0.
                more_to_come = bool(operation.exhaust and reply.cursor_id)
            if operation.conn_mgr:
                operation.conn_mgr.update_exhaust(more_to_come)
            response = PinnedResponse(
                data=reply,
                address=self._description.address,
                conn=conn,
                duration=duration,
                request_id=request_id,
                from_command=use_cmd,
                docs=docs,  # type: ignore[arg-type]
                more_to_come=more_to_come,
            )
        else:
            response = Response(
                data=reply,
                address=self._description.address,
                duration=duration,
                request_id=request_id,
                from_command=use_cmd,
                docs=docs,  # type: ignore[arg-type]
            )

        return response

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

    def _split_message(
        self, message: Union[tuple[int, Any], tuple[int, Any, int]]
    ) -> tuple[int, Any, int]:
        """Return request_id, data, max_doc_size.

        :param message: (request_id, data, max_doc_size) or (request_id, data)
        """
        if len(message) == 3:
            return message  # type: ignore[return-value]
        else:
            # get_more and kill_cursors messages don't include BSON documents.
            request_id, data = message  # type: ignore[misc]
            return request_id, data, 0

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self._description!r}>"
