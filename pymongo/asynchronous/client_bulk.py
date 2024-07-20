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

import copy
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
    _throw_client_bulk_write_exception,
)
from pymongo.common import (
    validate_is_document_type,
    validate_ok_for_replace,
    validate_ok_for_update,
)
from pymongo.errors import (
    ConfigurationError,
    InvalidOperation,
    NotPrimaryError,
    OperationFailure,
)
from pymongo.helpers_shared import _RETRYABLE_ERROR_CODES
from pymongo.logger import _COMMAND_LOGGER, _CommandStatusMessage, _debug_log
from pymongo.message import (
    _ClientBulkWriteContext,
    _convert_bulk_exception,
    _convert_exception,
    _convert_write_result,
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
        bypass_document_validation: Optional[bool] = None,
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

        self.is_retryable = self.client.options.retry_writes
        self.retrying = False
        self.started_retryable_write = False

        self.current_run = None
        self.next_run = None

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
        if multi:
            # A bulk_write containing an update_many is not retryable.
            self.is_retryable = False
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
        self.ops.append(("replace", cmd))

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
        if multi:
            # A bulk_write containing an update_many is not retryable.
            self.is_retryable = False
        self.ops.append(("delete", cmd))

    def gen_ordered(self) -> Iterator[Optional[_Run]]:
        """Generate batches of operations, in the order **provided**."""
        run = _Run()
        for idx, operation in enumerate(self.ops):
            run.add(idx, operation)
        yield run

    def gen_unordered(self) -> Iterator[_Run]:
        """Generate batches of operations, batched by type of
        operation, in arbitrary order.
        """
        # runs = [_Run(), _Run(), _Run()]
        # for idx, operation in enumerate(self.ops):
        #     op_type, _ = operation
        #     runs[OP_STR_TO_INT[op_type]].add(idx, operation)

        # for run in runs:
        #     if run.ops:
        #         yield run
        run = _Run()
        for idx, operation in enumerate(self.ops):
            run.add(idx, operation)
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
        op_docs: list[Mapping[str, Any]],
        ns_docs: list[Mapping[str, Any]],
        client: AsyncMongoClient,
    ) -> Optional[Mapping[str, Any]]:
        """A proxy for AsyncConnection.unack_write that handles event publishing."""
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
            cmd = bwc._start(cmd, request_id, op_docs, ns_docs)
        try:
            result = await bwc.conn.unack_write(msg, bwc.max_bson_size)  # type: ignore[func-returns-value, misc, override]
            duration = datetime.datetime.now() - bwc.start_time
            if result is not None:
                reply = _convert_write_result(bwc.name, cmd, result)  # type: ignore[arg-type]
            else:
                # Comply with APM spec.
                reply = {"ok": 1}
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
                bwc._succeed(request_id, reply, duration)
        except Exception as exc:
            duration = datetime.datetime.now() - bwc.start_time
            if isinstance(exc, OperationFailure):
                failure: _DocumentOut = _convert_write_result(bwc.name, cmd, exc.details)  # type: ignore[arg-type]
            elif isinstance(exc, NotPrimaryError):
                failure = exc.details  # type: ignore[assignment]
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
                assert bwc.start_time is not None
                bwc._fail(request_id, failure, duration)
            raise
        finally:
            bwc.start_time = datetime.datetime.now()
        return result  # type: ignore[return-value]

    async def _execute_batch_unack(
        self,
        bwc: _ClientBulkWriteContext,
        cmd: dict[str, Any],
        ops: list[Mapping[str, Any]],
    ) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
        """Executes a batch of bulkWrite server commands (unack)."""
        request_id, msg, to_send_ops, to_send_ns = bwc.batch_command(cmd, ops)
        # Though this isn't strictly a "legacy" write, the helper
        # handles publishing commands and sending our message
        # without receiving a result. Send 0 for max_doc_size
        # to disable size checking. Size checking is handled while
        # the documents are encoded to BSON.
        await self.unack_write(bwc, cmd, request_id, msg, 0, to_send_ops, to_send_ns, self.client)  # type: ignore[arg-type]

        return to_send_ops, to_send_ns

    async def _execute_batch(
        self,
        bwc: _ClientBulkWriteContext,
        cmd: dict[str, Any],
        ops: list[Mapping[str, Any]],
    ) -> tuple[dict[str, Any], list[Mapping[str, Any]], list[Mapping[str, Any]]]:
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
        write_concern: WriteConcern,
        session: Optional[AsyncClientSession],
        conn: AsyncConnection,
        op_id: int,
        retryable: bool,
        full_result: MutableMapping[str, Any],
        final_write_concern: Optional[WriteConcern] = None,
    ) -> None:
        """Internal helper for executing batches of bulkWrite commands."""
        db_name = "admin"
        cmd_name = "bulkWrite"
        listeners = self.client._event_listeners

        if not self.current_run:
            self.current_run = next(generator)
            self.next_run = None
        run = self.current_run

        # AsyncConnection.command validates the session, but we use
        # AsyncConnection.write_command
        conn.validate_session(self.client, session)
        last_run = False

        while run:
            if not self.retrying:
                self.next_run = next(generator, None)
                if self.next_run is None:
                    last_run = True

            bwc = self.bulk_ctx_class(
                db_name, cmd_name, conn, op_id, listeners, session, self.client.codec_options
            )

            while run.idx_offset < len(run.ops):
                # If this is the last possible operation, use the
                # final write concern.
                if last_run and (len(run.ops) - run.idx_offset) < 10000:
                    write_concern = final_write_concern or write_concern

                # Construct the server command, specifying the relevant options.
                cmd = {"bulkWrite": 1}
                cmd["errorsOnly"] = not self.verbose_results
                cmd["ordered"] = self.ordered
                not_in_transaction = session and not session.in_transaction
                if not_in_transaction or not session:
                    _csot.apply_write_concern(cmd, write_concern)
                if self.bypass_doc_val:
                    cmd["bypassDocumentValidation"] = self.bypass_doc_val
                if self.comment:
                    cmd["comment"] = self.comment
                if self.let:
                    cmd["let"] = self.let

                if session:
                    # Start a new retryable write unless one was already
                    # started for this command.
                    if retryable and not self.started_retryable_write:
                        session._start_retryable_write()
                        self.started_retryable_write = True
                    session._apply_to(cmd, retryable, ReadPreference.PRIMARY, conn)
                conn.send_cluster_time(cmd, session, self.client)
                conn.add_server_api(cmd)
                # CSOT: apply timeout before encoding the command.
                conn.apply_timeout(self.client, cmd)
                ops = islice(run.ops, run.idx_offset, None)

                # Run as many ops as possible in one server command.
                if write_concern.acknowledged:
                    raw_result, to_send_ops, _ = await self._execute_batch(bwc, cmd, ops)

                    result = copy.deepcopy(raw_result)
                    result["error"] = None
                    result["writeErrors"] = []
                    if result.get("nErrors") < len(to_send_ops):
                        full_result["anySuccessful"] = True

                    # Make the raw server reply accessible
                    # if we have a top-level command error.
                    if not result["ok"]:
                        result["error"] = raw_result
                        _merge_command(run, full_result, run.idx_offset, result)
                        break

                    if retryable:
                        # Retryable writeConcernErrors halt the execution of this run.
                        wce = result.get("writeConcernError", {})
                        if wce.get("code", 0) in _RETRYABLE_ERROR_CODES:
                            # Synthesize the full bulk result without modifying the
                            # current one because this write operation may be retried.
                            full = copy.deepcopy(full_result)
                            _merge_command(run, full, run.idx_offset, result)
                            _throw_client_bulk_write_exception(full, self.verbose_results)

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

                        # Iterate the cursor to get individual write results.
                        try:
                            async for doc in cmd_cursor:
                                op_type, op = self.ops[doc["idx"]]
                                # Process individual write error.
                                if not doc["ok"]:
                                    result["writeErrors"].append(doc)
                                    if self.ordered:
                                        break
                                # Record individual write result.
                                if doc["ok"] and self.verbose_results:
                                    if op_type == "insert":
                                        inserted_id = op["document"]["_id"]
                                        res = ClientInsertOneResult(inserted_id)
                                    if op_type == "update":
                                        res = ClientUpdateResult(doc)
                                    if op_type == "delete":
                                        res = ClientDeleteResult(doc)
                                    full_result[f"{op_type}Results"][doc["idx"]] = res
                        except Exception as exc:
                            # Attempt to close the cursor, then raise top-level error.
                            if cmd_cursor.alive:
                                await cmd_cursor.close()
                            result["error"] = _convert_bulk_exception(exc)

                    # Merge this batch's results with the full results.
                    _merge_command(run, full_result, run.idx_offset, result)

                    # We're no longer in a retry once a command succeeds.
                    self.retrying = False
                    self.started_retryable_write = False

                    if result["error"] or (self.ordered and result["writeErrors"]):
                        break
                else:
                    to_send_ops, _ = await self._execute_batch_unack(
                        bwc,
                        cmd,
                        ops,
                    )

                run.idx_offset += len(to_send_ops)

            # We halt execution if we hit a top-level error,
            # or an individual error in an ordered bulk write.
            if full_result["error"] or (self.ordered and full_result["writeErrors"]):
                break

            # Reset our state
            self.current_run = run = self.next_run

    async def execute_command(
        self,
        generator: Iterator[Any],
        session: Optional[AsyncClientSession],
        operation: str,
    ) -> dict[str, Any]:
        """Execute commands with w=1 WriteConcern."""
        full_result = {
            "anySuccessful": False,
            "error": None,
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
            retryable: bool,
        ) -> None:
            await self._execute_command(
                generator,
                self.write_concern,
                session,
                conn,
                op_id,
                retryable,
                full_result,
            )

        _ = await self.client._retryable_write(
            self.is_retryable,
            retryable_bulk,
            session,
            operation,
            bulk=self,  # type: ignore[arg-type]
            operation_id=op_id,
        )

        if full_result["error"] or full_result["writeErrors"] or full_result["writeConcernErrors"]:
            _throw_client_bulk_write_exception(full_result, self.verbose_results)
        return full_result

    async def execute_command_unack_unordered(
        self, conn: AsyncConnection, generator: Iterator[Any]
    ) -> None:
        """Execute commands with OP_MSG and w=0 writeConcern, unordered."""
        db_name = "admin"
        cmd_name = "bulkWrite"
        listeners = self.client._event_listeners
        op_id = _randint()

        if not self.current_run:
            self.current_run = next(generator)
        run = self.current_run

        while run:
            bwc = self.bulk_ctx_class(
                db_name, cmd_name, conn, op_id, listeners, None, self.client.codec_options
            )

            while run.idx_offset < len(run.ops):
                # Construct the server command, specifying the relevant options.
                cmd = {"bulkWrite": 1}
                cmd["errorsOnly"] = not self.verbose_results
                cmd["ordered"] = self.ordered
                if self.bypass_doc_val:
                    cmd["bypassDocumentValidation"] = self.bypass_doc_val
                cmd["writeConcern"] = {"w": 0}
                if self.comment:
                    cmd["comment"] = self.comment
                if self.let:
                    cmd["let"] = self.let

                conn.add_server_api(cmd)
                ops = islice(run.ops, run.idx_offset, None)

                # Run as many ops as possible in one server command.
                to_send_ops, _ = await self._execute_batch_unack(
                    bwc,
                    cmd,
                    ops,
                )
                run.idx_offset += len(to_send_ops)

            # Reset our state.
            self.current_run = run = next(generator, None)

    async def execute_command_unack_ordered(
        self,
        conn: AsyncConnection,
        generator: Iterator[Any],
    ) -> None:
        """Execute commands with OP_MSG and w=0 WriteConcern, ordered."""
        full_result = {
            "anySuccessful": False,
            "error": None,
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
        # Ordered bulk writes have to be acknowledged so that we stop
        # processing at the first error, even when the application
        # specified unacknowledged writeConcern.
        initial_write_concern = WriteConcern()
        op_id = _randint()
        try:
            await self._execute_command(
                generator,
                initial_write_concern,
                None,
                conn,
                op_id,
                False,
                full_result,
                self.write_concern,
            )
        except OperationFailure:
            pass

    async def execute_no_results(
        self,
        conn: AsyncConnection,
        generator: Iterator[Any],
    ) -> None:
        """Execute all operations, returning no results (w=0)."""
        if self.uses_collation:
            raise ConfigurationError("Collation is unsupported for unacknowledged writes.")
        if self.uses_array_filters:
            raise ConfigurationError("arrayFilters is unsupported for unacknowledged writes.")
        # Guard against unsupported unacknowledged writes.
        unack = self.write_concern and not self.write_concern.acknowledged
        if unack and self.uses_hint_delete and conn.max_wire_version < 9:
            raise ConfigurationError(
                "Must be connected to MongoDB 4.4+ to use hint on unacknowledged delete commands."
            )
        if unack and self.uses_hint_update and conn.max_wire_version < 8:
            raise ConfigurationError(
                "Must be connected to MongoDB 4.2+ to use hint on unacknowledged update commands."
            )
        # Cannot have both unacknowledged writes and bypass document validation.
        if self.bypass_doc_val:
            raise OperationFailure(
                "Cannot set bypass_document_validation with unacknowledged write concern"
            )

        if self.ordered:
            return await self.execute_command_unack_ordered(conn, generator)
        return await self.execute_command_unack_unordered(conn, generator)

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
                await self.execute_no_results(connection, generator)
                return ClientBulkWriteResult(None, False, False)

        return await self.execute_command(generator, session, operation)
