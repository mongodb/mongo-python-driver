# Copyright 2024-present MongoDB, Inc.
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

"""Atlas Stream Processing client for stream processing workspaces.

A stream processing workspace endpoint uses the ``mongodb://`` URI scheme with
a hostname that follows this pattern::

    atlas-stream-<workspaceId>-<suffix>.<region>.a.query.mongodb.net

For example::

    mongodb://atlas-stream-699c842ef433fe6001480b17-etif1.virginia-usa.a.query.mongodb.net/

TLS is always required for workspace connections and cannot be disabled.
``authSource=admin`` is applied by default when not explicitly set.

For commands not yet wrapped by this API, users can connect via a plain
:class:`~pymongo.mongo_client.MongoClient` and call
``run_command`` directly — that path remains fully supported.

Error handling
~~~~~~~~~~~~~~

ASP commands raise :class:`pymongo.errors.OperationFailure` on server-side
errors. The following error codes are known to be returned by Atlas Stream
Processing commands:

================  =================  ====================================
Code              Name               When returned
================  =================  ====================================
9                 FailedToParse      Invalid pipeline or command document
72                InvalidOptions     Invalid option values
125               CommandFailed      General command execution failure
1                 InternalError      Unexpected server-side error
================  =================  ====================================

This list is **non-exhaustive** and may grow as the server evolves. Drivers
do not maintain a closed list of valid codes — applications should branch
on ``exc.code`` only when they need to react to a specific known code, and
should always have a generic fallback for unknown codes.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Optional

from pymongo.synchronous.mongo_client import MongoClient
from pymongo.errors import ConfigurationError, InvalidOperation
from pymongo.operations import _Op
from pymongo.stream_processing_options import (
    CreateStreamProcessorOptions,
    GetStreamProcessorSamplesOptions,
    GetStreamProcessorSamplesResult,
    GetStreamProcessorStatsOptions,
    StartStreamProcessorOptions,
    StreamProcessorInfo,
)
from pymongo.uri_parser_shared import SRV_SCHEME, _validate_uri

if TYPE_CHECKING:
    from types import TracebackType

    from pymongo.synchronous.client_session import ClientSession

_IS_SYNC = True


def _is_workspace_endpoint(host: str) -> bool:
    """Return True if *host* looks like an ASP workspace endpoint.

    Workspace hostnames begin with ``atlas-stream-`` or end with
    ``.a.query.mongodb.net``.
    """
    return host.startswith("atlas-stream-") or host.endswith(".a.query.mongodb.net")


class StreamProcessingClient:
    """A client connected to an Atlas Stream Processing workspace.

    Wraps :class:`~pymongo.mongo_client.MongoClient` with
    Atlas Stream Processing constraints:

    * Only the ``mongodb://`` URI scheme is accepted (``mongodb+srv://`` is
      not supported for workspace endpoints).
    * TLS is always enabled and cannot be disabled.
    * ``authSource`` defaults to ``"admin"`` if not explicitly set.

    Usage::

        with StreamProcessingClient(
            "mongodb://atlas-stream-<id>.<region>.a.query.mongodb.net/",
            username="user",
            password="pass",
        ) as client:
            sps = client.stream_processors()
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        host: Any = args[0] if args else kwargs.get("host")

        uris: list[str] = []
        if isinstance(host, str):
            uris = [host]
        elif isinstance(host, (list, tuple)):
            uris = [h for h in host if isinstance(h, str)]

        uri_has_auth_source = False
        for uri_str in uris:
            if not uri_str.startswith(("mongodb://", SRV_SCHEME)):
                # Plain hostname — no URI scheme to check.
                continue
            if uri_str.startswith(SRV_SCHEME):
                raise ConfigurationError(
                    "StreamProcessingClient does not support mongodb+srv:// URIs; "
                    "use mongodb:// with a workspace endpoint."
                )
            parsed = _validate_uri(uri_str, validate=True, warn=False, normalize=True)
            uri_opts = parsed["options"]
            if uri_opts.get("tls") is False:
                raise ConfigurationError(
                    "TLS cannot be disabled for stream processing workspace connections."
                )
            if uri_opts.get("authsource") is not None:
                uri_has_auth_source = True

        # Also reject explicit tls=False / ssl=False in kwargs.
        if kwargs.get("tls") is False or kwargs.get("ssl") is False:
            raise ConfigurationError(
                "TLS cannot be disabled for stream processing workspace connections."
            )

        kwargs.pop("ssl", None)
        kwargs["tls"] = True

        if not uri_has_auth_source and not any(k.lower() == "authsource" for k in kwargs):
            kwargs["authSource"] = "admin"

        self._client: MongoClient = MongoClient(*args, **kwargs)

    # NOTE: Per the ASP driver spec, server errors MUST be surfaced as-is.
    # Do NOT introduce error-code branching, rewrapping, or filtering anywhere
    # in this module. Known codes are documented at the module level for
    # reference only — they are not runtime invariants.
    def _command(
        self,
        cmd: dict[str, Any],
        *,
        retryable_read: bool = False,
        session: Optional[ClientSession] = None,
    ) -> Mapping[str, Any]:
        """Send a top-level ASP command to the admin database.

        Routes through the existing retry machinery: retryable reads use
        :meth:`~pymongo.database.Database._retryable_read_command`;
        everything else uses the standard
        :meth:`~pymongo.database.Database.command` path.

        :param cmd: The command document.
        :param retryable_read: If ``True``, the command is sent as a retryable read.
        :param session: A
            :class:`~pymongo.client_session.ClientSession` to use
            for this operation.
        """
        admin = self._client._database_default_options("admin")
        if retryable_read:
            # The first key of the command document is the operation name,
            # which matches the corresponding _Op enum value string.
            operation = next(iter(cmd))
            return admin._retryable_read_command(cmd, session=session, operation=operation)
        return admin.command(cmd, session=session)

    def stream_processors(self) -> "StreamProcessors":
        """Return a handle for managing stream processors in this workspace."""
        return StreamProcessors(self)

    def close(self) -> None:
        """Close the underlying client and release all resources."""
        self._client.close()

    def __enter__(self) -> "StreamProcessingClient":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional["TracebackType"],
    ) -> None:
        self.close()

    @property
    def address(self) -> Any:
        """(host, port) of the current workspace endpoint, or None.

        Delegates to the underlying
        :attr:`~pymongo.mongo_client.MongoClient.address`.
        """
        return self._client.address


class StreamProcessors:
    """Handle for managing stream processors in a workspace.

    Obtained via :meth:`StreamProcessingClient.stream_processors`.
    """

    def __init__(self, client: "StreamProcessingClient") -> None:
        self._client = client

    def create(
        self,
        name: str,
        pipeline: list[Mapping[str, Any]],
        options: Optional[CreateStreamProcessorOptions] = None,
        *,
        session: Optional[ClientSession] = None,
    ) -> None:
        """Create a new stream processor in the workspace.

        Sends the ``createStreamProcessor`` command to the ``admin`` database.
        This operation is **not** retryable.

        :param name: The name of the stream processor to create.
        :param pipeline: The aggregation pipeline that defines the processor.
        :param options: Optional :class:`~pymongo.stream_processing_options.CreateStreamProcessorOptions`
            controlling DLQ, tier, and other settings.
        :param session: A
            :class:`~pymongo.client_session.ClientSession` to use
            for this operation.
        """
        if not name or not name.strip():
            raise InvalidOperation("Stream processor name must be a non-empty string.")
        if not pipeline:
            raise InvalidOperation("createStreamProcessor requires a non-empty pipeline.")
        cmd: dict[str, Any] = {
            _Op.CREATE_STREAM_PROCESSOR: name,
            "pipeline": list(pipeline),
        }
        if options is not None:
            opts: dict[str, Any] = {}
            if options.dlq is not None:
                opts["dlq"] = options.dlq
            if options.stream_meta_field_name is not None:
                opts["streamMetaFieldName"] = options.stream_meta_field_name
            if options.tier is not None:
                opts["tier"] = options.tier
            if options.failover is not None:
                opts["failover"] = options.failover
            if opts:
                cmd["options"] = opts
        self._client._command(cmd, session=session)

    def get(self, name: str) -> "StreamProcessor":
        """Return a handle for an existing stream processor by name.

        This is a pure factory method — it does **not** contact the server
        or verify that the processor exists.

        :param name: The name of the stream processor.
        :returns: An :class:`StreamProcessor` handle.
        """
        if not name or not name.strip():
            raise InvalidOperation("Stream processor name must be a non-empty string.")
        return StreamProcessor(client=self._client, name=name)

    def get_info(
        self,
        name: str,
        *,
        session: Optional[ClientSession] = None,
    ) -> StreamProcessorInfo:
        """Return information about a single stream processor.

        Sends the ``getStreamProcessor`` command to the ``admin`` database.
        This operation is a **retryable read**.

        :param name: The name of the stream processor.
        :param session: A
            :class:`~pymongo.client_session.ClientSession` to use
            for this operation.
        :returns: A :class:`~pymongo.stream_processing_options.StreamProcessorInfo`
            populated from the server response. Unknown server fields are preserved
            in :attr:`~pymongo.stream_processing_options.StreamProcessorInfo.raw`.
        """
        if not name or not name.strip():
            raise InvalidOperation("Stream processor name must be a non-empty string.")
        cmd: dict[str, Any] = {_Op.GET_STREAM_PROCESSOR: name}
        response = self._client._command(cmd, retryable_read=True, session=session)
        return StreamProcessorInfo.from_response(response)


class StreamProcessor:
    """Handle for a specific named stream processor.

    Does not imply the processor currently exists on the server.
    Obtain via :meth:`StreamProcessors.get`.
    """

    def __init__(self, *, client: "StreamProcessingClient", name: str) -> None:
        if not name or not name.strip():
            raise InvalidOperation("Stream processor name must be a non-empty string.")
        self._client = client
        self.name = name

    def start(
        self,
        options: Optional[StartStreamProcessorOptions] = None,
        *,
        session: Optional[ClientSession] = None,
    ) -> None:
        """Start this stream processor.

        Sends the ``startStreamProcessor`` command to the ``admin`` database.
        This operation is **not** retryable.

        The processor must be in the ``STOPPED`` or ``FAILED`` state; starting
        an already-``STARTED`` processor returns a server error.

        Mutual exclusivity of ``start_after`` / ``start_at_operation_time`` and
        tier validation are enforced by
        :class:`~pymongo.stream_processing_options.StartStreamProcessorOptions`
        at construction time.

        :param options: Optional
            :class:`~pymongo.stream_processing_options.StartStreamProcessorOptions`.
        :param session: A
            :class:`~pymongo.client_session.ClientSession` to use
            for this operation.
        """
        cmd: dict[str, Any] = {_Op.START_STREAM_PROCESSOR: self.name}
        if options is not None:
            if options.workers is not None:
                cmd["workers"] = options.workers
            opts: dict[str, Any] = {}
            if options.clear_checkpoints is not None:
                opts["clearCheckpoints"] = options.clear_checkpoints
            if options.start_at_operation_time is not None:
                opts["startAtOperationTime"] = options.start_at_operation_time
            if options.start_after is not None:
                opts["startAfter"] = options.start_after
            if options.tier is not None:
                opts["tier"] = options.tier
            if options.enable_auto_scaling is not None:
                opts["enableAutoScaling"] = options.enable_auto_scaling
            if options.failover is not None:
                opts["failover"] = options.failover
            if opts:
                cmd["options"] = opts
        self._client._command(cmd, session=session)

    def stop(
        self,
        *,
        session: Optional[ClientSession] = None,
    ) -> None:
        """Stop this stream processor.

        Sends the ``stopStreamProcessor`` command to the ``admin`` database.
        This operation is **not** retryable. The processor transitions to the
        ``STOPPED`` state and can be restarted.

        :param session: A
            :class:`~pymongo.client_session.ClientSession` to use
            for this operation.
        """
        self._client._command(
            {_Op.STOP_STREAM_PROCESSOR: self.name}, session=session
        )

    def drop(
        self,
        *,
        session: Optional[ClientSession] = None,
    ) -> None:
        """Permanently delete this stream processor.

        Sends the ``dropStreamProcessor`` command to the ``admin`` database.
        This operation is **not** retryable. A dropped processor cannot be
        recovered.

        :param session: A
            :class:`~pymongo.client_session.ClientSession` to use
            for this operation.
        """
        self._client._command(
            {_Op.DROP_STREAM_PROCESSOR: self.name}, session=session
        )

    def stats(
        self,
        options: Optional[GetStreamProcessorStatsOptions] = None,
        *,
        session: Optional[ClientSession] = None,
    ) -> Mapping[str, Any]:
        """Return runtime statistics for this stream processor.

        Sends the ``getStreamProcessorStats`` command to the ``admin`` database.
        This operation is a **retryable read**. The server returns an error if
        the processor is not in the ``STARTED`` state.

        Unknown fields in the response are preserved — the raw response dict
        is returned unchanged so callers are not affected by server additions.

        :param options: Optional
            :class:`~pymongo.stream_processing_options.GetStreamProcessorStatsOptions`
            controlling scale units and verbosity.
        :param session: A
            :class:`~pymongo.client_session.ClientSession` to use
            for this operation.
        :returns: The raw server response document.
        """
        cmd: dict[str, Any] = {_Op.GET_STREAM_PROCESSOR_STATS: self.name}
        if options is not None:
            opts: dict[str, Any] = {}
            if options.scale is not None:
                opts["scale"] = options.scale
            if options.verbose is not None:
                opts["verbose"] = options.verbose
            if opts:
                cmd["options"] = opts
        return self._client._command(cmd, retryable_read=True, session=session)

    def get_stream_processor_samples(
        self,
        options: Optional[GetStreamProcessorSamplesOptions] = None,
        *,
        session: Optional[ClientSession] = None,
    ) -> GetStreamProcessorSamplesResult:
        """Fetch one batch of sampled documents from a running stream processor.

        Spec-literal entry point. Inspects ``options.cursor_id`` and routes to
        ``startSampleStreamProcessor`` (initial call) or
        ``getMoreSampleStreamProcessor`` (continuation). Most users should
        prefer :meth:`sample`, which wraps this in an async iterator.

        A returned ``cursor_id`` of ``0`` means the cursor is exhausted; callers
        MUST NOT call this method again with that cursor id.

        Sends to the ``admin`` database. Non-retryable.

        :param options: Optional
            :class:`~pymongo.stream_processing_options.GetStreamProcessorSamplesOptions`.
        :param session: A
            :class:`~pymongo.client_session.ClientSession` to use
            for this operation.
        :returns: A :class:`~pymongo.stream_processing_options.GetStreamProcessorSamplesResult`
            containing the batch of documents and the cursor id for the next call.
        """
        if options is None:
            options = GetStreamProcessorSamplesOptions()

        if options.cursor_id == 0:
            raise InvalidOperation(
                "Sample cursor is exhausted; cursor_id 0 cannot be continued."
            )

        if options.cursor_id is None:
            cmd: dict[str, Any] = {_Op.START_SAMPLE_STREAM_PROCESSOR: self.name}
            if options.limit is not None:
                cmd["limit"] = options.limit
            resp = self._client._command(cmd, session=session)
            return GetStreamProcessorSamplesResult(
                cursor_id=int(resp["cursorId"]),
                documents=list(resp["firstBatch"]),
            )
        else:
            cmd = {
                _Op.GET_MORE_SAMPLE_STREAM_PROCESSOR: self.name,
                "cursorId": options.cursor_id,
            }
            if options.batch_size is not None:
                cmd["batchSize"] = options.batch_size
            resp = self._client._command(cmd, session=session)
            return GetStreamProcessorSamplesResult(
                cursor_id=int(resp["cursorId"]),
                documents=list(resp["nextBatch"]),
            )

    def sample(
        self,
        limit: Optional[int] = None,
        batch_size: Optional[int] = None,
        *,
        session: Optional[ClientSession] = None,
    ) -> "SampleCursor":
        """Open a sample cursor over this stream processor's output.

        Returns an async iterator that yields sampled documents until the
        server-side cursor is exhausted. Internally drives the two-phase
        ``startSampleStreamProcessor`` / ``getMoreSampleStreamProcessor``
        protocol on the caller's behalf.

        Usage::

            for doc in processor.sample(limit=100, batch_size=10):
                print(doc)

        :param limit: Maximum number of documents to sample (sent only on the
            initial call).
        :param batch_size: Number of documents per continuation batch (sent
            only on subsequent calls).
        :param session: Optional :class:`ClientSession` propagated to all
            underlying commands.
        """
        return SampleCursor(
            processor=self,
            limit=limit,
            batch_size=batch_size,
            session=session,
        )


class SampleCursor:
    """Async iterator over sampled stream processor output.

    A custom two-phase cursor used to retrieve sampled documents from a
    running stream processor. This cursor MUST NOT be wrapped or re-used
    via the standard MongoDB ``Cursor`` types because it does not use the
    standard ``getMore`` command — it uses the dedicated
    ``startSampleStreamProcessor`` / ``getMoreSampleStreamProcessor``
    commands instead.

    Obtained via :meth:`StreamProcessor.sample`. Iterate with
    ``for``.

    The cursor is exhausted when the server returns ``cursorId: 0``;
    after that, no further wire calls are issued and iteration ends.
    """

    def __init__(
        self,
        *,
        processor: "StreamProcessor",
        limit: Optional[int] = None,
        batch_size: Optional[int] = None,
        session: Optional["ClientSession"] = None,
    ) -> None:
        self._processor = processor
        self._limit = limit
        self._batch_size = batch_size
        self._session = session

        self._buffer: list[Mapping[str, Any]] = []
        self._cursor_id: Optional[int] = None  # None = not yet opened
        self._exhausted: bool = False
        self._closed: bool = False

    @property
    def cursor_id(self) -> Optional[int]:
        """Current server-side cursor id, or ``None`` if not yet opened.

        A value of ``0`` indicates the cursor has been exhausted.
        """
        return self._cursor_id

    @property
    def alive(self) -> bool:
        """``True`` if more documents may be available; ``False`` once exhausted or closed."""
        return not self._exhausted and not self._closed

    def _refill(self) -> None:
        """Fetch the next batch from the server. No-op if exhausted or closed."""
        if self._exhausted or self._closed:
            return

        if self._cursor_id is None:
            opts = GetStreamProcessorSamplesOptions(
                cursor_id=None,
                limit=self._limit,
                batch_size=None,
            )
        else:
            opts = GetStreamProcessorSamplesOptions(
                cursor_id=self._cursor_id,
                limit=None,
                batch_size=self._batch_size,
            )

        result = self._processor.get_stream_processor_samples(
            opts, session=self._session
        )
        self._cursor_id = result.cursor_id
        self._buffer.extend(result.documents)

        # Spec: cursorId == 0 means exhausted. MUST NOT call getMore again.
        if result.cursor_id == 0:
            self._exhausted = True

    def __iter__(self) -> "SampleCursor":
        return self

    def __next__(self) -> Mapping[str, Any]:
        if self._buffer:
            return self._buffer.pop(0)

        if self._closed or self._exhausted:
            raise StopIteration

        # Loop guards against an empty batch from the server with a non-zero
        # cursor id — keep pulling until we get documents or hit exhaustion.
        while not self._buffer and not self._exhausted:
            self._refill()

        if self._buffer:
            return self._buffer.pop(0)

        raise StopIteration

    def close(self) -> None:
        """Mark the cursor closed locally.

        Note: ASP does not currently expose a way to explicitly kill a
        sample cursor server-side. ``close`` only stops local iteration;
        the server-side cursor will be cleaned up on its own timeout or
        when the processor stops.
        """
        self._closed = True

    def __enter__(self) -> "SampleCursor":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional["TracebackType"],
    ) -> None:
        self.close()
