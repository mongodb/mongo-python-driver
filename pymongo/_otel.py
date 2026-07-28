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

"""Optional OpenTelemetry command-span support.

Kept separate from :mod:`pymongo._telemetry` so that module stays free of
``opentelemetry`` import guards. Every function here is a no-op when
``opentelemetry`` isn't installed or tracing isn't enabled.
"""

from __future__ import annotations

import contextlib
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
    from opentelemetry import trace
    from opentelemetry.trace import SpanKind, Status, StatusCode

    _HAS_OPENTELEMETRY = True
    # Safe to cache at import time: opentelemetry.trace.get_tracer() returns a
    # ProxyTracer when no real TracerProvider is registered yet, and that proxy
    # transparently starts delegating to the real tracer once the application
    # calls trace.set_tracer_provider() later, so this doesn't bind us to a
    # permanently-inert no-op tracer.
    _TRACER: Optional[Tracer] = trace.get_tracer("PyMongo", __version__)
except ImportError:
    _HAS_OPENTELEMETRY = False
    _TRACER = None

# The operation name of whichever operation span is currently active (entered
# via start_operation_span), so start_command_span can backfill the operation
# span's name/namespace attributes from the first command executed inside it
# (dbname/collection aren't known until then -- see start_operation_span).
_CURRENT_OPERATION_NAME: ContextVar[Optional[str]] = ContextVar(
    "_CURRENT_OPERATION_NAME", default=None
)

if TYPE_CHECKING:
    from opentelemetry.trace import Span, Tracer

    from pymongo.pool_shared import _ConnectionTelemetryInfo
    from pymongo.typings import _DocumentOut


class TracingOptions(TypedDict):
    """The shape of the ``MongoClient`` ``tracing`` option.

    ``query_text_max_length`` is None when the client didn't configure it, so
    the environment variable can be consulted; any explicit value (including
    0, to force ``db.query.text`` off) overrides the environment variable.
    """

    enabled: bool
    query_text_max_length: Optional[int]


_OTEL_ENABLED_ENV = "OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED"
_OTEL_QUERY_TEXT_MAX_LENGTH_ENV = "OTEL_PYTHON_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH"
_TRUTHY = frozenset({"1", "true", "yes"})

# Fields redacted from the db.query.text attribute, mirroring the fields excluded
# from the equivalent CommandStartedEvent.command per the OpenTelemetry spec.
_QUERY_TEXT_EXCLUDED_FIELDS = frozenset({"lsid", "$db", "$clusterTime", "signature"})

# getMore's own command value is the cursor id, not the collection name; the
# collection lives under a separate "collection" key instead.
# See _gen_get_more_command in pymongo/message.py.
_GET_MORE = "getMore"

# explain wraps the real command (e.g. find/aggregate) rather than naming a
# collection directly: {"explain": {"find": "coll", ...}}. See _Query.as_command
# in pymongo/message.py.
_EXPLAIN = "explain"

# Commands against this database (e.g. user/role management, renameCollection)
# never have a real collection name, even when their command value is a string.
_ADMIN_DB = "admin"


def _env_truthy(name: str) -> bool:
    """Return True if the environment variable ``name`` is set to "1", "true", or "yes"."""
    return os.getenv(name, "").strip().lower() in _TRUTHY


def _is_tracing_enabled(tracing_options: Optional[TracingOptions]) -> bool:
    """Return True if OTel command spans should be created for this client.

    The ``MongoClient`` ``tracing.enabled`` option and the
    ``OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED`` environment variable both
    gate enablement; either one being truthy is sufficient.
    """
    if not _HAS_OPENTELEMETRY:
        return False
    if tracing_options and tracing_options.get("enabled"):
        return True
    return _env_truthy(_OTEL_ENABLED_ENV)


def _get_query_text_max_length(tracing_options: Optional[TracingOptions]) -> int:
    """Return the configured db.query.text truncation length, or 0 to omit the attribute.

    An explicit client value (including 0) always wins; the environment
    variable is only consulted when the client didn't configure it at all.
    """
    client_value = tracing_options.get("query_text_max_length") if tracing_options else None
    if client_value is not None:
        return max(0, client_value)
    try:
        return max(0, int(os.getenv(_OTEL_QUERY_TEXT_MAX_LENGTH_ENV, "0")))
    except ValueError:
        return 0


def _build_query_text(cmd: Mapping[str, Any], max_length: int) -> str:
    """Serialize ``cmd`` to extended JSON, redacted and truncated to ``max_length``.

    Mirrors the truncation approach used for log messages: truncate field
    values first, which usually keeps the result well-formed JSON (unlike a
    blind cut of the fully-serialized string), then fall back to a hard
    string cut as a safety net for whatever the field truncation's size
    estimate still leaves over ``max_length``. The "..." marker is carved out
    of the budget (not appended on top of it) so the result never exceeds
    ``max_length``.
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
    return value if isinstance(value, str) else None


def _build_query_summary(command_name: str, dbname: str, collection: Optional[str]) -> str:
    """Build the ``db.query.summary`` attribute value for a command."""
    if collection:
        return f"{command_name} {dbname}.{collection}"
    return f"{command_name} {dbname}"


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

    Returns None when tracing is disabled/unavailable or the command is
    sensitive (mirroring the redaction applied to logs).
    """
    if not _is_tracing_enabled(tracing_options):
        return None

    collection = _extract_collection_name(command_name, dbname, cmd)
    # Backfill the ambient operation span's name/namespace/summary from the
    # first command built inside it, before the sensitive-command early
    # return below -- the operation span still needs its (Required, per the
    # OTel spec) db.namespace/db.operation.summary attributes even when the
    # command itself is sensitive and gets no command span of its own.
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
        # A cursor id of 0 means the cursor is already exhausted -- i.e. there
        # is no cursor left to track -- so per the OTel spec ("If the command
        # returns a cursor, or uses a cursor, the cursor_id attribute SHOULD
        # be added") the attribute is only meaningful, and only added, when
        # id is nonzero.
        span.set_attribute("db.mongodb.cursor_id", cursor["id"])
    span.end()


def _set_exception_attributes(span: Span, exc: BaseException) -> None:
    """Set exception.type/exception.message/exception.stacktrace span attributes.

    ``span.record_exception`` only attaches these to an "exception" *event*,
    but the OTel spec requires them as span *attributes* too ("drivers SHOULD
    add the following attributes to the span"); mirror record_exception's own
    formatting for consistency. Shared by the command-span and operation-span
    failure paths, since the spec states the same requirement for both.
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

    ``_cm`` is the ``start_as_current_span`` context manager when the span was
    made current at creation, or None in detached mode (see
    ``start_operation_span``'s ``set_current``), where the span is made current
    per-use by ``use_operation_span`` instead.
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

    Spans all retry attempts of one call to _retry_internal. When ``dbname`` is
    given, the spec-required ``db.namespace``/``db.operation.summary`` (and
    ``db.collection.name``, when ``collection`` is given) are set immediately,
    so an operation that fails before any command is ever built -- e.g. server
    selection timing out -- still produces a conformant span.
    ``start_command_span`` still backfills these from the real command once one
    is built, overwriting these values with the authoritative ones.

    ``parent_span`` (the active transaction span, if any) becomes this span's
    *explicit* parent; it is deliberately not read from ambient context, to
    avoid a concurrently-running unrelated session's operations picking up
    this transaction by accident. Pass None outside of a transaction.

    With ``set_current=False`` the span is created but not made current and the
    operation-name contextvar is left alone -- for spans whose lifetime spans
    several ``_retry_internal`` calls (cursor getMores), where the caller makes
    it current per-call via ``use_operation_span``.
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
        attributes["db.operation.summary"] = name
        if collection is not None:
            attributes["db.collection.name"] = collection
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

    Does not end the span -- the owner (e.g. a cursor, across all of its
    getMore calls) ends it explicitly. A no-op when ``handle`` is None.
    """
    if handle is None:
        yield
        return
    token = _CURRENT_OPERATION_NAME.set(handle.operation_name)
    try:
        # record_exception/set_status_on_exception default to True, which would
        # auto-record any exception propagating out of the block and set ERROR
        # status here -- duplicating what the caller's own
        # end_operation_span_failure does explicitly once the operation's
        # final outcome is known. Disabled for the same reason the
        # attached-mode path passes hardcoded Nones to cm.__exit__.
        with trace.use_span(
            handle.span,
            end_on_exit=False,
            record_exception=False,
            set_status_on_exception=False,
        ):
            yield
    finally:
        _CURRENT_OPERATION_NAME.reset(token)


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


def start_transaction_span(tracing_options: Optional[TracingOptions]) -> Optional[Span]:
    """Start (but do not make current) the ``"transaction"`` pseudo-span, or None.

    Not pushed as ambient/current context -- it's stored explicitly on
    ``session._transaction.span`` and passed as the explicit ``parent_span``
    wherever an operation span is started under this transaction (see
    :func:`start_operation_span`). Per the OTel driver spec, this span has
    exactly one attribute.
    """
    if not _is_tracing_enabled(tracing_options):
        return None
    assert _TRACER is not None
    return _TRACER.start_span(
        "transaction", kind=SpanKind.CLIENT, attributes={"db.system.name": "mongodb"}
    )


def end_transaction_span(span: Optional[Span]) -> None:
    """End the transaction span, if any."""
    if span is None:
        return
    span.end()
