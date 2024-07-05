# Copyright 2024-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The client-level bulk write operations interface.

.. versionadded:: TODO
"""
from __future__ import annotations

from collections.abc import MutableMapping
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    Mapping,
    Optional,
    Type,
    Union,
)

from bson.objectid import ObjectId
from bson.raw_bson import RawBSONDocument
from pymongo import common
from pymongo.asynchronous.client_session import AsyncClientSession, _validate_session_write_concern
from pymongo.asynchronous.helpers import _handle_reauth
from pymongo.bulk_shared import (
    _Run,
)
from pymongo.common import (
    validate_is_document_type,
    validate_ok_for_replace,
    validate_ok_for_update,
)
from pymongo.errors import (
    InvalidOperation,
)
from pymongo.message import (
    _ClientBulkWriteContext,
)
from pymongo.results import ClientBulkWriteResult
from pymongo.write_concern import WriteConcern

if TYPE_CHECKING:
    from pymongo.asynchronous.mongo_client import AsyncMongoClient
    from pymongo.asynchronous.pool import AsyncConnection
    from pymongo.typings import _DocumentOut, _Pipeline

_IS_SYNC = False


class _AsyncClientBulk:
    """The private guts of the client-level bulk write API."""

    def __init__(
        self,
        client: AsyncMongoClient,
        ordered: Optional[bool] = True,
        bypass_document_validation: Optional[bool] = False,
        comment: Optional[str] = None,
        let: Optional[Any] = None,
        write_concern: Optional[WriteConcern] = None,
        verbose_results: Optional[bool] = False,
    ) -> None:
        """Initialize a _AsyncClientBulk instance."""
        self.client = client
        self.let = let
        if self.let is not None:
            common.validate_is_document_type("let", self.let)
        self.ordered = ordered
        self.bypass_doc_val = bypass_document_validation
        self.comment = comment
        self.write_concern = write_concern
        self.verbose_results = verbose_results

        self.ops: list[tuple[str, Mapping[str, Any]]] = []
        self.namespaces: list[str] = []

        self.executed = False
        self.uses_collation = False
        self.uses_array_filters = False
        self.uses_hint_update = False
        self.uses_hint_delete = False

    @property
    def bulk_ctx_class(self) -> Type[_ClientBulkWriteContext]:
        return _ClientBulkWriteContext

    def add_insert(self, namespace: str, document: _DocumentOut) -> None:
        """Add an insert document to the list of ops."""
        if namespace not in self.namespaces:
            self.namespaces.append(namespace)
        validate_is_document_type("document", document)
        # Generate ObjectId client side.
        if not (isinstance(document, RawBSONDocument) or "_id" in document):
            document["_id"] = ObjectId()
        cmd = {"insert": namespace, "document": document}
        self.ops.append(cmd)

    def add_update(
        self,
        namespace: str,
        selector: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        multi: bool = False,
        upsert: bool = False,
        collation: Optional[Mapping[str, Any]] = None,
        array_filters: Optional[list[Mapping[str, Any]]] = None,
        hint: Union[str, dict[str, Any], None] = None,
    ) -> None:
        """Create an update document and add it to the list of ops."""
        if namespace not in self.namespaces:
            self.namespaces.append(namespace)
        validate_ok_for_update(update)
        cmd: dict[str, Any] = dict(  # noqa: C406
            [
                ("update", namespace),
                ("filter", selector),
                ("updateMods", update),
                ("multi", multi),
                ("upsert", upsert),
            ]
        )
        if array_filters is not None:
            self.uses_array_filters = True
            cmd["arrayFilters"] = array_filters
        if hint is not None:
            self.uses_hint_update = True
            cmd["hint"] = hint
        if collation is not None:
            self.uses_collation = True
            cmd["collation"] = collation
        self.ops.append(("update", cmd))

    def add_replace(
        self,
        namespace: str,
        selector: Mapping[str, Any],
        replacement: Mapping[str, Any],
        upsert: bool = False,
        collation: Optional[Mapping[str, Any]] = None,
        hint: Union[str, dict[str, Any], None] = None,
    ) -> None:
        """Create a replace document and add it to the list of ops."""
        if namespace not in self.namespaces:
            self.namespaces.append(namespace)
        validate_ok_for_replace(replacement)
        cmd: dict[str, Any] = dict(  # noqa: C406
            [
                ("update", namespace),
                ("filter", selector),
                ("updateMods", replacement),
                ("multi", False),
                ("upsert", upsert),
            ]
        )
        if hint is not None:
            self.uses_hint_update = True
            cmd["hint"] = hint
        if collation is not None:
            self.uses_collation = True
            cmd["collation"] = collation
        self.ops.append(("update", cmd))

    def add_delete(
        self,
        namespace: str,
        selector: Mapping[str, Any],
        multi: bool = False,
        collation: Optional[Mapping[str, Any]] = None,
        hint: Union[str, dict[str, Any], None] = None,
    ) -> None:
        """Create a delete document and add it to the list of ops."""
        if namespace not in self.namespaces:
            self.namespaces.append(namespace)
        cmd = {"delete": namespace, "filter": selector, "multi": multi}
        if hint is not None:
            self.uses_hint_delete = True
            cmd["hint"] = hint
        if collation is not None:
            self.uses_collation = True
            cmd["collation"] = collation
        self.ops.append(("delete", cmd))

    def gen_ordered(self) -> Iterator[Optional[_Run]]:
        """TODO: Generate batches of operations, in the order **provided**."""
        run = None
        for idx, operation in enumerate(self.ops):
            if run is None:
                run = _Run()
            run.add(idx, operation)
        yield run

    def gen_unordered(self) -> Iterator[_Run]:
        """TODO: Generate batches of operations, in arbitrary order."""
        operations = _Run()
        for idx, operation in enumerate(self.ops):
            operations.add(idx, operation)

        for run in operations:
            if run.ops:
                yield run

    @_handle_reauth
    async def write_command(
        self,
        bwc: _ClientBulkWriteContext,
        cmd: MutableMapping[str, Any],
        request_id: int,
        msg: bytes,
        docs: list[Mapping[str, Any]],
        client: AsyncMongoClient,
    ) -> dict[str, Any]:
        """TODO: A proxy for SocketInfo.write_command that handles event publishing.
        - send a single bulkWrite command to the server and receive result.
        """

    async def unack_write(
        self,
        bwc: _ClientBulkWriteContext,
        cmd: MutableMapping[str, Any],
        request_id: int,
        msg: bytes,
        max_doc_size: int,
        docs: list[Mapping[str, Any]],
        client: AsyncMongoClient,
    ) -> Optional[Mapping[str, Any]]:
        """TODO: A proxy for AsyncConnection.unack_write that handles event publishing.
        - send a single bulkWrite command to the server and receive result.
        """

    async def _execute_batch_unack(
        self,
        bwc: _ClientBulkWriteContext,
        cmd: dict[str, Any],
        ops: list[Mapping[str, Any]],
        client: AsyncMongoClient,
    ) -> list[Mapping[str, Any]]:
        """TODO: This function executes a batch of bulkWrite server commands (unack).
        - must handle all the batch splitting logic in message.py.
        - call batch_command() on the _ClientBulkWriteContext instance.
        - pass the result of that to write_command() to run a single write.
        """
        request_id, msg, to_send = bwc.batch_command(cmd, ops)
        # Though this isn't strictly a "legacy" write, the helper
        # handles publishing commands and sending our message
        # without receiving a result. Send 0 for max_doc_size
        # to disable size checking. Size checking is handled while
        # the documents are encoded to BSON.
        await self.unack_write(bwc, cmd, request_id, msg, 0, to_send, client)  # type: ignore[arg-type]

        return to_send

    async def _execute_batch(
        self,
        bwc: _ClientBulkWriteContext,
        cmd: dict[str, Any],
        ops: list[Mapping[str, Any]],
        client: AsyncMongoClient,
    ) -> tuple[dict[str, Any], list[Mapping[str, Any]]]:
        """TODO: This function executes a batch of bulkWrite server commands (ack).
        - must handle all the batch splitting logic in message.py.
        - call batch_command() on the _ClientBulkWriteContext instance.
        - pass the result of that to write_command() to run a single write.
        """
        request_id, msg, to_send = bwc.batch_command(cmd, ops)
        result = await self.write_command(bwc, cmd, request_id, msg, to_send, client)  # type: ignore[arg-type]
        await client._process_response(result, bwc.session)  # type: ignore[arg-type]
        return result, to_send  # type: ignore[return-value]

    async def _execute_command(
        self,
        generator: Iterator[Any],
        write_concern: WriteConcern,
        session: Optional[AsyncClientSession],
        conn: AsyncConnection,
        op_id: int,
        full_result: MutableMapping[str, Any],
        final_write_concern: Optional[WriteConcern] = None,
    ) -> None:
        """TODO: Takes in a generator of _Run instances (batches).
        For each batch of operations:
            - define a _ClientBulkContext instance.
            - construct a bulkWrite server command.
            - call _execute_batch() to run the command.
            - handle the resulting command cursor.
            - merge the results of this batch with the full results.
            - throw error/exception as necessary.
            - if self.ordered and there's a write error, break.
        """

    async def execute_command(
        self,
        generator: Iterator[Any],
        write_concern: WriteConcern,
        session: Optional[AsyncClientSession],
        operation: str,
    ) -> dict[str, Any]:
        """TODO: Execute using write commands.
        - call private helper to execute batches.
        - populate and return the ClientBulkWriteResult.
        """

    async def execute_op_msg_no_results(
        self, conn: AsyncConnection, generator: Iterator[Any]
    ) -> None:
        """TODO: Execute write commands with OP_MSG and w=0 writeConcern, unordered."""

    async def execute_command_no_results(
        self,
        conn: AsyncConnection,
        generator: Iterator[Any],
        write_concern: WriteConcern,
    ) -> None:
        """TODO: Execute write commands with OP_MSG and w=0 WriteConcern, ordered."""
        # Ordered bulk writes have to be acknowledged so that we stop
        # processing at the first error, even when the application
        # specified unacknowledged writeConcern.

    async def execute_no_results(
        self,
        conn: AsyncConnection,
        generator: Iterator[Any],
        write_concern: WriteConcern,
    ) -> None:
        """TODO: Execute all operations, returning no results (w=0)."""

    async def execute(
        self,
        session: Optional[AsyncClientSession],
        operation: str,
    ) -> Any:
        """TODO: Execute operations.
        - gets called in the AsyncMongoClient.bulk_write() endpoint.
        - create generator of _Run instances (command batches).
        - call execute_command() to execute batches and get result.
        """

        if not self.ops:
            raise InvalidOperation("No operations to execute")
        if self.executed:
            raise InvalidOperation("Bulk operations can only be executed once.")
        self.executed = True

        if self.write_concern:
            session = _validate_session_write_concern(session, self.write_concern)

            if self.ordered:
                generator = self.gen_ordered()
            else:
                generator = self.gen_unordered()

            if not self.write_concern.acknowledged:
                async with await self.client._conn_for_writes(session, operation) as connection:
                    await self.execute_no_results(connection, generator, self.write_concern)
                    return ClientBulkWriteResult(acknowledged=False)
            else:
                return await self.execute_command(generator, self.write_concern, session, operation)
        return None
