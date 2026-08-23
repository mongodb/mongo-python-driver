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

"""Optional OpenTelemetry span support.

Kept separate from :mod:`pymongo._telemetry` so that module stays free of
``opentelemetry`` import guards. Every function here is a no-op when
``opentelemetry`` isn't installed or tracing isn't enabled.

This module also owns the specification's naming and attribute rules, such as
how span names, ``db.operation.name`` and ``db.query.summary`` are built.
:mod:`pymongo._telemetry` owns span lifecycles. A specification change to a
name or an attribute value stays here; one that changes when a span starts or
ends, or what it nests under, affects both.
"""

from __future__ import annotations

import contextlib
import enum
import os
import traceback
from collections.abc import Iterator, Mapping, MutableMapping
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Optional, TypedDict

from bson import json_util
from bson.json_util import _truncate_documents
from pymongo._version import __version__
from pymongo.logger import _HELLO_COMMANDS, _JSON_OPTIONS, _SENSITIVE_COMMANDS

try:
    from opentelemetry import context, trace  # type:ignore[import-not-found,unused-ignore]
    from opentelemetry.trace import (  # type:ignore[import-not-found,unused-ignore]
        SpanKind,
        Status,
        StatusCode,
    )

    _HAS_OPENTELEMETRY = True
    # Safe to cache: get_tracer() returns a ProxyTracer when no provider is
    # registered yet, and that proxy starts delegating once the application
    # calls set_tracer_provider(), so this is not bound to a no-op tracer.
    _TRACER: Optional[Tracer] = trace.get_tracer("PyMongo", __version__)
except ImportError:
    _HAS_OPENTELEMETRY = False
    _TRACER = None

# Name of the active operation span, so start_command_span can backfill that
# span's name and namespace attributes from the first command built inside it.
# Call sites that know the namespace up front pass it to start_operation_span;
# the generic retry path does not, and relies on this backfill.
_CURRENT_OPERATION_NAME: ContextVar[Optional[str]] = ContextVar(
    "_CURRENT_OPERATION_NAME", default=None
)

if TYPE_CHECKING:
    from opentelemetry.trace import Span, Tracer

    from pymongo.pool_shared import _ConnectionTelemetryInfo
    from pymongo.typings import _DocumentOut


class _UnresolvedTracingOptions(TypedDict):
    """The ``MongoClient`` ``tracing`` option as validated from user input.

    ``query_text_max_length`` is None when unset, which is distinct from an
    explicit 0: None lets the environment variable supply a length, 0 turns
    ``db.query.text`` off.
    """

    enabled: bool
    query_text_max_length: Optional[int]


class TracingOptions(TypedDict):
    """The ``MongoClient`` ``tracing`` option as a client holds it.

    :func:`_resolve_tracing_options` has folded in the environment variables,
    so ``query_text_max_length`` is an int of 0 or more.
    """

    enabled: bool
    query_text_max_length: int


_OTEL_ENABLED_ENV = "OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED"
_OTEL_QUERY_TEXT_MAX_LENGTH_ENV = "OTEL_PYTHON_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH"
_TRUTHY = frozenset({"1", "true", "yes"})

# Fields redacted from the db.query.text attribute, mirroring the fields excluded
# from the equivalent CommandStartedEvent.command per the OpenTelemetry spec.
_QUERY_TEXT_EXCLUDED_FIELDS = frozenset({"lsid", "$db", "$clusterTime", "signature"})

# getMore's command value is the cursor id, not a collection name; the collection
# lives under a separate "collection" key. See _gen_get_more_command in message.py.
_GET_MORE = "getMore"

# explain wraps the real command rather than naming a collection directly:
# {"explain": {"find": "coll", ...}}. See _Query.as_command in message.py.
_EXPLAIN = "explain"

# Commands against this database (e.g. user/role management, renameCollection)
# never have a real collection name, even when their command value is a string.
_ADMIN_DB = "admin"

# A cursor opened by a command rather than over a collection (listCollections,
# listIndexes, a database-level aggregate) has a namespace like
# "$cmd.listCollections", which names no user collection, so per the spec
# db.collection.name is omitted.
_CMD_NAMESPACE_PREFIX = "$cmd"


def _env_truthy(name: str) -> bool:
    """Return True if the environment variable ``name`` is set to "1", "true", or "yes"."""
    return os.getenv(name, "").strip().lower() in _TRUTHY


def _is_tracing_enabled(tracing_options: Optional[TracingOptions]) -> bool:
    """Return True if spans should be created for this client.

    ClientOptions folds ``OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED`` into
    ``tracing.enabled`` once at construction, so this is a lookup rather than
    an os.environ read per command. None means there is no client to read the
    option from, as for monitor and handshake connections, which are never
    traced.
    """
    return _HAS_OPENTELEMETRY and tracing_options is not None and tracing_options["enabled"]


def _resolve_tracing_options(tracing_options: _UnresolvedTracingOptions) -> TracingOptions:
    """Fold both environment variables into a client's validated tracing options.

    Called once when the client is built, so nothing re-reads the environment
    per command. An explicit client value wins, including a
    ``query_text_max_length`` of 0, which turns ``db.query.text`` off.
    """
    max_length = tracing_options["query_text_max_length"]
    if max_length is None:
        try:
            max_length = int(os.getenv(_OTEL_QUERY_TEXT_MAX_LENGTH_ENV, "0"))
        except ValueError:
            max_length = 0
    return {
        "enabled": tracing_options["enabled"] or _env_truthy(_OTEL_ENABLED_ENV),
        "query_text_max_length": max(0, max_length),
    }


def _get_query_text_max_length(tracing_options: Optional[TracingOptions]) -> int:
    """Return the db.query.text truncation length, or 0 to omit the attribute."""
    return tracing_options["query_text_max_length"] if tracing_options else 0


def _build_query_text(cmd: Mapping[str, Any], max_length: int) -> str:
    """Serialize ``cmd`` to extended JSON, redacted and truncated to ``max_length``.

    Mirrors log-message truncation: shorten field values first, which usually
    keeps the result valid JSON, then hard-cut as a safety net. The "..."
    marker comes out of the budget, so the result never exceeds ``max_length``.
    """
    filtered = {k: v for k, v in cmd.items() if k not in _QUERY_TEXT_EXCLUDED_FIELDS}
    truncated_cmd = _truncate_documents(filtered, max_length)[0]
    # default=repr mirrors the structured logger: tracing is best-effort and must
    # not raise for commands containing custom/codec-managed Python types.
    text = json_util.dumps(truncated_cmd, json_options=_JSON_OPTIONS, default=repr)
    if len(text) > max_length:
        suffix = "..."
        text = text[: max(0, max_length - len(suffix))] + suffix
    return text


def _extract_collection_name(
    command_name: str, dbname: str, cmd: Mapping[str, Any]
) -> Optional[str]:
    """Return the collection name targeted by ``cmd``, or None if it doesn't target one.

    Always None for commands against the admin database: several (e.g. dropUser,
    renameCollection) carry a string command value that names a user, role, or
    namespace rather than a collection.
    """
    if dbname == _ADMIN_DB:
        return None
    if command_name == _EXPLAIN:
        inner = cmd.get(_EXPLAIN)
        if not isinstance(inner, Mapping) or not inner:
            return None
        inner_name = next(iter(inner))
        return _extract_collection_name(inner_name, dbname, inner)
    key = "collection" if command_name == _GET_MORE else command_name
    value = cmd.get(key)
    if not isinstance(value, str) or is_command_namespace(value):
        return None
    return value


def is_command_namespace(collection: Optional[str]) -> bool:
    """Return True if ``collection`` is a command pseudo-namespace, not a user collection.

    A cursor opened by a command (listCollections, a database-level aggregate)
    reports something like "$cmd.listCollections", which targets no specific
    collection, so per the spec ``db.collection.name`` is omitted.
    """
    return collection is not None and (
        collection == _CMD_NAMESPACE_PREFIX or collection.startswith(_CMD_NAMESPACE_PREFIX + ".")
    )


def _build_query_summary(command_name: str, dbname: str, collection: Optional[str]) -> str:
    """Build the ``db.query.summary`` attribute value for a command."""
    if collection:
        return f"{command_name} {dbname}.{collection}"
    return f"{command_name} {dbname}"


# db.operation.name for operations the spec names differently from our `_Op`
# values. The nested command span keeps the wire name in db.command.name, so
# dropping a collection reports "dropCollection" over a "drop" command span.
_OPERATION_NAME_OVERRIDES = {
    "drop": "dropCollection",
    "create": "createCollection",
    "dropSearchIndexes": "dropSearchIndex",
}

# The spec names anything sent through the generic `Database.command()` API "runCommand",
# not after the command it carries.
_RUN_COMMAND_OPERATION_NAME = "runCommand"


def _normalize_operation_name(operation: Any) -> str:
    """Return the plain ``str`` form of an operation name.

    Call sites pass an `_Op` (a `str`-mixin enum), and Python 3.11 changed
    ``Enum.__format__`` so ``str(_Op.INSERT)`` yields ``"_Op.INSERT"`` rather
    than ``"insert"``. Normalizing once here keeps every span name and
    attribute stable across versions.
    """
    if isinstance(operation, enum.Enum):
        return operation.value
    return str(operation)


def _build_operation_name(operation: Any, is_run_command: bool = False) -> str:
    """Return the ``db.operation.name`` the spec wants for this operation."""
    if is_run_command:
        return _RUN_COMMAND_OPERATION_NAME
    name = _normalize_operation_name(operation)
    return _OPERATION_NAME_OVERRIDES.get(name, name)


def _is_sensitive_command(command_name: str, speculative_hello: bool) -> bool:
    """Mirror the redaction rules in ``pymongo.logger.LogMessage._is_sensitive``."""
    if command_name in _SENSITIVE_COMMANDS:
        return True
    return command_name in _HELLO_COMMANDS and speculative_hello


def _format_lsid(lsid: Mapping[str, Any]) -> Optional[str]:
    """Return the ``db.mongodb.lsid`` attribute value for a session id document."""
    id_value = lsid.get("id")
    if id_value is None:
        return None
    try:
        return str(id_value.as_uuid())
    except (AttributeError, ValueError):
        return str(id_value)


def start_command_span(
    tracing_options: Optional[TracingOptions],
    conn: _ConnectionTelemetryInfo,
    cmd: MutableMapping[str, Any],
    dbname: str,
    command_name: str,
    speculative_hello: bool,
) -> Optional[Span]:
    """Start and return a CLIENT-kind span for a server command, or None.

    A no-op returning None when tracing is off; a sensitive command also
    returns None but still backfills the current operation span first. One span
    per wire-protocol message, parented to the current operation span but never
    made current itself.
    """
    if not _is_tracing_enabled(tracing_options):
        return None

    collection = _extract_collection_name(command_name, dbname, cmd)
    # Must stay above the sensitive-command return: the operation span needs
    # these attributes even when the command itself gets no span. Runs per
    # attempt, since an attempt that dies before building a command never
    # reaches here, so a retry may be where the span learns its namespace.
    current_operation = _CURRENT_OPERATION_NAME.get()
    if current_operation is not None:
        current_span = trace.get_current_span()
        if current_span.is_recording():
            summary = _build_query_summary(current_operation, dbname, collection)
            current_span.update_name(summary)
            current_span.set_attribute("db.namespace", dbname)
            current_span.set_attribute("db.operation.summary", summary)
            if collection:
                current_span.set_attribute("db.collection.name", collection)

    if _is_sensitive_command(command_name, speculative_hello):
        return None

    address = conn.address
    transport = "unix" if address[1] is None else "tcp"
    attributes: dict[str, Any] = {
        "db.system.name": "mongodb",
        "db.namespace": dbname,
        "db.command.name": command_name,
        "db.query.summary": _build_query_summary(command_name, dbname, collection),
        "server.address": address[0],
        "network.transport": transport,
        "db.mongodb.driver_connection_id": conn.id,
    }
    if address[1] is not None:
        attributes["server.port"] = address[1]
    if collection:
        attributes["db.collection.name"] = collection
    if conn.server_connection_id is not None:
        attributes["db.mongodb.server_connection_id"] = conn.server_connection_id
    lsid = cmd.get("lsid")
    if isinstance(lsid, Mapping):
        formatted_lsid = _format_lsid(lsid)
        if formatted_lsid is not None:
            attributes["db.mongodb.lsid"] = formatted_lsid
    txn_number = cmd.get("txnNumber")
    if txn_number is not None:
        attributes["db.mongodb.txn_number"] = txn_number
    max_query_text_length = _get_query_text_max_length(tracing_options)
    if max_query_text_length > 0:
        attributes["db.query.text"] = _build_query_text(cmd, max_query_text_length)

    assert _TRACER is not None  # _is_tracing_enabled already checked _HAS_OPENTELEMETRY
    return _TRACER.start_span(command_name, kind=SpanKind.CLIENT, attributes=attributes)


def end_command_span_success(span: Optional[Span], reply: _DocumentOut) -> None:
    """Set the cursor id (if any open cursor) and end the span."""
    if span is None:
        return
    cursor = reply.get("cursor")
    if isinstance(cursor, Mapping) and cursor.get("id"):
        # Per the spec the attribute is omitted rather than set to 0, so a
        # cursor-creating command that leaves no cursor open reports nothing.
        span.set_attribute("db.mongodb.cursor_id", cursor["id"])
    span.end()


def _set_exception_attributes(span: Span, exc: BaseException) -> None:
    """Set exception.type/exception.message/exception.stacktrace span attributes.

    ``record_exception`` attaches these to an "exception" *event* only, but the
    spec requires them as span *attributes* too, for both command and operation
    spans. Formatting mirrors ``record_exception``.
    """
    module = type(exc).__module__
    qualname = type(exc).__qualname__
    exception_type = f"{module}.{qualname}" if module and module != "builtins" else qualname
    span.set_attribute("exception.type", exception_type)
    span.set_attribute("exception.message", str(exc))
    span.set_attribute(
        "exception.stacktrace",
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
    )


def end_command_span_failure(
    span: Optional[Span],
    failure: _DocumentOut,
    exc: BaseException,
) -> None:
    """Record the exception, set the error status, and end the span."""
    if span is None:
        return
    span.record_exception(exc)
    _set_exception_attributes(span, exc)
    code = failure.get("code")
    if code is not None:
        span.set_attribute("db.response.status_code", str(code))
    span.set_status(Status(StatusCode.ERROR, description=failure.get("errmsg")))
    span.end()


class _OperationSpanHandle:
    """Bundles an operation span with what's needed to end it later.

    ``_cm`` is the ``start_as_current_span`` context manager, or None in
    detached mode, where ``use_operation_span`` makes the span current per use.
    """

    __slots__ = ("_cm", "_name_token", "operation_name", "span")

    def __init__(
        self,
        span: Span,
        cm: Any,
        name_token: Any,
        operation_name: str,
    ) -> None:
        self.span = span
        self._cm = cm
        self._name_token = name_token
        self.operation_name = operation_name


def start_operation_span(
    tracing_options: Optional[TracingOptions],
    operation: str,
    parent_span: Optional[Span],
    dbname: Optional[str] = None,
    collection: Optional[str] = None,
    set_current: bool = True,
) -> Optional[_OperationSpanHandle]:
    """Start a CLIENT-kind span for one logical operation, or None.

    Spans all retry attempts of one ``_retry_internal`` call. Namespace
    attributes are set eagerly from ``dbname``/``collection`` so an operation
    that fails before building any command, such as a server selection
    timeout, still produces a conformant span; ``start_command_span``
    backfills the authoritative values once a command exists.

    ``parent_span`` becomes an *explicit* parent rather than being read from
    ambient context, so a concurrent unrelated session cannot be captured.

    ``set_current=False`` leaves the span and the operation-name contextvar
    alone, for a caller that makes it current with ``use_operation_span``.
    """
    if not _is_tracing_enabled(tracing_options):
        return None
    assert _TRACER is not None  # _is_tracing_enabled already checked _HAS_OPENTELEMETRY
    context = trace.set_span_in_context(parent_span) if parent_span is not None else None
    attributes: dict[str, Any] = {
        "db.system.name": "mongodb",
        "db.operation.name": operation,
    }
    name = operation
    if dbname is not None:
        name = _build_query_summary(operation, dbname, collection)
        attributes["db.namespace"] = dbname
        if collection:
            attributes["db.collection.name"] = collection
    attributes["db.operation.summary"] = name
    if not set_current:
        span = _TRACER.start_span(
            name, kind=SpanKind.CLIENT, context=context, attributes=attributes
        )
        return _OperationSpanHandle(span, None, None, operation)
    cm = _TRACER.start_as_current_span(
        name,
        kind=SpanKind.CLIENT,
        context=context,
        attributes=attributes,
    )
    span = cm.__enter__()
    name_token = _CURRENT_OPERATION_NAME.set(operation)
    return _OperationSpanHandle(span, cm, name_token, operation)


@contextlib.contextmanager
def use_operation_span(handle: Optional[_OperationSpanHandle]) -> Iterator[None]:
    """Make a detached operation span current for the duration of the block.

    Does not end the span; its owner ends it explicitly. A no-op when
    ``handle`` is None.
    """
    if handle is None:
        yield
        return
    token = _CURRENT_OPERATION_NAME.set(handle.operation_name)
    try:
        # Left on, these would auto-record any exception leaving the block and
        # set ERROR status, duplicating the caller's end_operation_span_failure
        # once the operation's final outcome is known.
        with trace.use_span(
            handle.span,
            end_on_exit=False,
            record_exception=False,
            set_status_on_exception=False,
        ):
            yield
    finally:
        _CURRENT_OPERATION_NAME.reset(token)


def reset_context() -> None:
    """Clear the OTel ambient span and operation-name contextvar.

    ``asyncio.create_task`` freezes the caller's context, so without this a
    long-lived background task parents every span it emits under an unrelated,
    long-ended operation. Attaching an empty context makes spans started
    afterwards trace roots. Deliberately never detached: the task's context is
    wrong for its whole life and dies with it.
    """
    if not _HAS_OPENTELEMETRY:
        return
    _CURRENT_OPERATION_NAME.set(None)
    context.attach(context.Context())


def end_operation_span_success(handle: Optional[_OperationSpanHandle]) -> None:
    """End the operation span with no error status."""
    if handle is None:
        return
    if handle._cm is None:
        handle.span.end()
        return
    _CURRENT_OPERATION_NAME.reset(handle._name_token)
    handle._cm.__exit__(None, None, None)


def end_operation_span_failure(handle: Optional[_OperationSpanHandle], exc: BaseException) -> None:
    """Record the exception, set the error status, and end the operation span."""
    if handle is None:
        return
    handle.span.record_exception(exc)
    _set_exception_attributes(handle.span, exc)
    handle.span.set_status(Status(StatusCode.ERROR, description=str(exc)))
    if handle._cm is None:
        handle.span.end()
        return
    _CURRENT_OPERATION_NAME.reset(handle._name_token)
    handle._cm.__exit__(None, None, None)
