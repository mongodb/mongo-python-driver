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

import datetime
import logging
from collections.abc import MutableMapping
from itertools import islice
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
from pymongo import _csot, common
from pymongo.asynchronous.client_session import AsyncClientSession, _validate_session_write_concern
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.helpers import _handle_reauth

if TYPE_CHECKING:
    from pymongo.asynchronous.mongo_client import AsyncMongoClient
    from pymongo.asynchronous.pool import AsyncConnection
from pymongo.client_bulk_shared import (
    _merge_command,
    _Run,
)
from pymongo.common import (
    validate_is_document_type,
    validate_ok_for_replace,
    validate_ok_for_update,
)
from pymongo.errors import (
    InvalidOperation,
    NotPrimaryError,
    OperationFailure,
)
from pymongo.logger import _COMMAND_LOGGER, _CommandStatusMessage, _debug_log
from pymongo.message import (
    _ClientBulkWriteContext,
    _convert_exception,
    _randint,
)
from pymongo.read_preferences import ReadPreference
from pymongo.results import (
    ClientBulkWriteResult,
    ClientDeleteResult,
    ClientInsertOneResult,
    ClientUpdateResult,
)
from pymongo.typings import _DocumentOut, _Pipeline
from pymongo.write_concern import WriteConcern

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
        self.uses_upsert = False
        self.uses_collation = False
        self.uses_array_filters = False
        self.uses_hint_update = False
        self.uses_hint_delete = False
        self.current_run = None
        self.next_run = None
        self.retrying = False

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
        self.ops.append(("insert", cmd))

    def add_update(
        self,
        namespace: str,
        selector: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        multi: bool = False,
        upsert: Optional[bool] = None,
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
            ]
        )
        if upsert is not None:
            self.uses_upsert = True
            cmd["upsert"] = upsert
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
        upsert: Optional[bool] = None,
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
            ]
        )
        if upsert is not None:
            self.uses_upsert = True
            cmd["upsert"] = upsert
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
        multi: bool,
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
        runs = _Run()
        for idx, operation in enumerate(self.ops):
            runs.add(idx, operation)

        for run in runs:
            if run.ops:
                yield run

    @_handle_reauth
    async def write_command(
        self,
        bwc: _ClientBulkWriteContext,
        cmd: MutableMapping[str, Any],
        request_id: int,
        msg: bytes,
        op_docs: list[Mapping[str, Any]],
        ns_docs: list[Mapping[str, Any]],
        client: AsyncMongoClient,
    ) -> dict[str, Any]:
        """A proxy for SocketInfo.write_command that handles event publishing."""
        cmd["ops"] = op_docs
        cmd["nsInfo"] = ns_docs
        if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _COMMAND_LOGGER,
                clientId=client._topology_settings._topology_id,
                message=_CommandStatusMessage.STARTED,
                command=cmd,
                commandName=next(iter(cmd)),
                databaseName=bwc.db_name,
                requestId=request_id,
                operationId=request_id,
                driverConnectionId=bwc.conn.id,
                serverConnectionId=bwc.conn.server_connection_id,
                serverHost=bwc.conn.address[0],
                serverPort=bwc.conn.address[1],
                serviceId=bwc.conn.service_id,
            )
        if bwc.publish:
            bwc._start(cmd, request_id, op_docs, ns_docs)
        try:
            reply = await bwc.conn.write_command(request_id, msg, bwc.codec)  # type: ignore[misc]
            duration = datetime.datetime.now() - bwc.start_time
            if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _COMMAND_LOGGER,
                    clientId=client._topology_settings._topology_id,
                    message=_CommandStatusMessage.SUCCEEDED,
                    durationMS=duration,
                    reply=reply,
                    commandName=next(iter(cmd)),
                    databaseName=bwc.db_name,
                    requestId=request_id,
                    operationId=request_id,
                    driverConnectionId=bwc.conn.id,
                    serverConnectionId=bwc.conn.server_connection_id,
                    serverHost=bwc.conn.address[0],
                    serverPort=bwc.conn.address[1],
                    serviceId=bwc.conn.service_id,
                )
            if bwc.publish:
                bwc._succeed(request_id, reply, duration)  # type: ignore[arg-type]
        except Exception as exc:
            duration = datetime.datetime.now() - bwc.start_time
            if isinstance(exc, (NotPrimaryError, OperationFailure)):
                failure: _DocumentOut = exc.details  # type: ignore[assignment]
            else:
                failure = _convert_exception(exc)
            if _COMMAND_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _COMMAND_LOGGER,
                    clientId=client._topology_settings._topology_id,
                    message=_CommandStatusMessage.FAILED,
                    durationMS=duration,
                    failure=failure,
                    commandName=next(iter(cmd)),
                    databaseName=bwc.db_name,
                    requestId=request_id,
                    operationId=request_id,
                    driverConnectionId=bwc.conn.id,
                    serverConnectionId=bwc.conn.server_connection_id,
                    serverHost=bwc.conn.address[0],
                    serverPort=bwc.conn.address[1],
                    serviceId=bwc.conn.service_id,
                    isServerSideError=isinstance(exc, OperationFailure),
                )

            if bwc.publish:
                bwc._fail(request_id, failure, duration)
            raise
        finally:
            bwc.start_time = datetime.datetime.now()
        return reply  # type: ignore[return-value]

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
    ) -> tuple[dict[str, Any], list[Mapping[str, Any]]]:
        """Executes a batch of bulkWrite server commands (ack)."""
        request_id, msg, to_send_ops, to_send_ns = bwc.batch_command(cmd, ops)
        result = await self.write_command(
            bwc, cmd, request_id, msg, to_send_ops, to_send_ns, self.client
        )  # type: ignore[arg-type]
        await self.client._process_response(result, bwc.session)  # type: ignore[arg-type]
        return result, to_send_ops, to_send_ns  # type: ignore[return-value]

    async def _execute_command(
        self,
        generator: Iterator[Any],
        session: Optional[AsyncClientSession],
        conn: AsyncConnection,
        op_id: int,
        full_result: MutableMapping[str, Any],
        retryable: bool,
    ) -> None:
        """Internal helper for executing batches of commands."""
        db_name = "admin"
        cmd_name = "bulkWrite"
        listeners = self.client._event_listeners

        if retryable:
            pass

        if not self.current_run:
            self.current_run = next(generator)
            self.next_run = None
        run = self.current_run

        # AsyncConnection.command validates the session, but we use
        # AsyncConnection.write_command
        conn.validate_session(self.client, session)
        # last_run = False

        while run:
            self.next_run = next(generator, None)
            # if self.next_run is None:
            #     last_run = True

            bwc = self.bulk_ctx_class(
                db_name, cmd_name, conn, op_id, listeners, session, self.client.codec_options
            )

            while run.idx_offset < len(run.ops):
                # Construct the server command, specifying the relevant options.
                cmd = {"bulkWrite": 1}
                cmd["errorsOnly"] = not self.verbose_results
                cmd["ordered"] = self.ordered
                cmd["bypassDocumentValidation"] = self.bypass_doc_val
                _csot.apply_write_concern(cmd, self.write_concern)
                if self.comment:
                    cmd["comment"] = self.comment
                if self.let:
                    cmd["let"] = self.let

                if session:
                    session._apply_to(cmd, False, ReadPreference.PRIMARY, conn)
                conn.send_cluster_time(cmd, session, self.client)
                conn.add_server_api(cmd)
                # CSOT: apply timeout before encoding the command.
                conn.apply_timeout(self.client, cmd)
                ops = islice(run.ops, run.idx_offset, None)

                # Run as many ops as possible in one server command.
                if self.write_concern.acknowledged:
                    result, to_send_ops, to_send_ns = await self._execute_batch(
                        bwc,
                        cmd,
                        ops,
                    )
                    # Process the server reply as a command cursor.
                    if result.get("cursor"):
                        coll = AsyncCollection(
                            database=AsyncDatabase(self.client, "admin"),
                            name="$cmd.bulkWrite",
                        )
                        cmd_cursor = AsyncCommandCursor(
                            coll,
                            result["cursor"],
                            conn.address,
                            session=session,
                            explicit_session=session is not None,
                            comment=self.comment,
                        )
                        await cmd_cursor._maybe_pin_connection(conn)
                        # Iterate through the cursor to get individual write results.
                        doc = await cmd_cursor.try_next()
                        while doc is not None:
                            op_type, op = self.ops[doc["idx"]]
                            if self.verbose_results:
                                if op_type == "insert" and doc["ok"]:
                                    inserted_id = op["document"]["_id"]
                                    insert_res = ClientInsertOneResult(inserted_id)
                                    full_result["insertResults"][doc["idx"]] = insert_res
                                if op_type == "update" and doc["ok"]:
                                    update_res = ClientUpdateResult(doc)
                                    full_result["updateResults"][doc["idx"]] = update_res
                                if op_type == "delete" and doc["ok"]:
                                    delete_res = ClientDeleteResult(doc)
                                    full_result["deleteResults"][doc["idx"]] = delete_res
                            doc = await cmd_cursor.try_next()

                    # # Retryable writeConcernErrors halt the execution of this run.
                    # wce = result.get("writeConcernError", {})
                    # if wce.get("code", 0) in _RETRYABLE_ERROR_CODES:
                    #     # Synthesize the full bulk result without modifying the
                    #     # current one because this write operation may be retried.
                    #     full = copy.deepcopy(full_result)
                    #     _merge_command(run, full, run.idx_offset, result)
                    #     _raise_bulk_write_error(full)

                    _merge_command(run, full_result, run.idx_offset, result)

                    # # We're no longer in a retry once a command succeeds.
                    # self.retrying = False
                    # self.started_retryable_write = False

                    # if self.ordered and "writeErrors" in result:
                    #     break
                else:
                    to_send_ops, to_send_ns = await self._execute_batch_unack(
                        bwc, cmd, ops, self.client
                    )

                run.idx_offset += len(to_send_ops)

            # We're supposed to continue if errors are
            # at the write concern level (e.g. wtimeout)
            if self.ordered and full_result["writeErrors"]:
                break
            # Reset our state
            self.current_run = run = self.next_run

    async def execute_command(
        self,
        generator: Iterator[Any],
        session: Optional[AsyncClientSession],
        operation: str,
    ) -> dict[str, Any]:
        """Execute batches of bulkWrite commands."""
        # nModified is only reported for write commands, not legacy ops.
        full_result = {
            "writeErrors": [],
            "writeConcernErrors": [],
            "nInserted": 0,
            "nUpserted": 0,
            "nMatched": 0,
            "nModified": 0,
            "nDeleted": 0,
            "upserted": [],
            "insertResults": {},
            "updateResults": {},
            "deleteResults": {},
        }
        op_id = _randint()

        async def retryable_bulk(
            session: Optional[AsyncClientSession],
            conn: AsyncConnection,
            retryable: bool = False,
        ) -> None:
            await self._execute_command(
                generator,
                session,
                conn,
                op_id,
                full_result,
                retryable,
            )

        _ = await self.client._retryable_write(
            False,
            retryable_bulk,
            session,
            operation,
            bulk=self,  # type: ignore[arg-type]
            operation_id=op_id,
        )

        return full_result

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
        """Execute operations."""
        if not self.ops:
            raise InvalidOperation("No operations to execute")
        if self.executed:
            raise InvalidOperation("Bulk operations can only be executed once.")
        self.executed = True

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
            return await self.execute_command(generator, session, operation)
