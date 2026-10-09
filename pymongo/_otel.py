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

import os
from collections.abc import Mapping, MutableMapping
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
    # Command spans are always CLIENT kind; hoisted to avoid an attribute
    # lookup on every span creation.
    _SPAN_KIND_CLIENT = SpanKind.CLIENT
except ImportError:
    _HAS_OPENTELEMETRY = False
    _TRACER = None

if TYPE_CHECKING:
    from opentelemetry.trace import Span, Tracer

    from pymongo.pool_shared import _ConnectionTelemetryInfo
    from pymongo.typings import _DocumentOut


class TracingOptions(TypedDict):
    """The shape of the ``MongoClient`` ``tracing`` option.

    ``enabled`` and ``query_text_max_length`` are None when the client didn't
    configure them, so the environment variables can be consulted; any explicit
    value (including ``False``, to force tracing off, or 0, to force
    ``db.query.text`` off) overrides the environment variable.
    """

    enabled: Optional[bool]
    query_text_max_length: Optional[int]


_OTEL_ENABLED_ENV = "OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED"
_OTEL_QUERY_TEXT_MAX_LENGTH_ENV = "OTEL_PYTHON_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH"
_TRUTHY = frozenset({"1", "true", "yes"})

# Redaction compares command names case-insensitively, mirroring the
# normalization in command monitoring, so a differently-cased sensitive
# command still gets no span.
_SENSITIVE_COMMANDS_LOWER = frozenset(name.lower() for name in _SENSITIVE_COMMANDS)
_HELLO_COMMANDS_LOWER = frozenset(name.lower() for name in _HELLO_COMMANDS)

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

# Commands whose string command value names a user or role, not a collection
# (against any database): db.collection.name must be omitted rather than
# expose the username or role name as a collection.
_NOT_COLLECTION_COMMANDS = frozenset(
    {
        "createUser",
        "dropAllRolesFromDatabase",
        "dropAllUsersFromDatabase",
        "dropRole",
        "dropUser",
        "grantPrivilegesToRole",
        "grantRolesToRole",
        "grantPrivilegesToUser",
        "grantRolesToUser",
        "invalidateUserCache",
        "revokePrivilegesFromRole",
        "revokePrivilegesFromUser",
        "revokeRolesFromRole",
        "revokeRolesFromUser",
        "rolesInfo",
        "createRole",
        "updateRole",
        "updateUser",
        "usersInfo",
    }
)


def _env_truthy(name: str) -> bool:
    """Return True if the environment variable ``name`` is set to "1", "true", or "yes"."""
    return os.getenv(name, "").strip().lower() in _TRUTHY


def _resolve_tracing_options(tracing: Optional[TracingOptions]) -> TracingOptions:
    """Resolve a client's raw ``tracing`` option against the environment, once.

    Called once per ``MongoClient`` construction: an explicit value (including
    ``False``, to force tracing off, or ``0``, to force ``db.query.text`` off)
    wins over the environment variable; otherwise the environment variable
    decides. When opentelemetry isn't installed, tracing is always disabled.
    The resolved values are then consulted on every command without touching
    the environment again.
    """
    if not _HAS_OPENTELEMETRY:
        return {"enabled": False, "query_text_max_length": 0}
    enabled = tracing.get("enabled") if tracing is not None else None
    if enabled is None:
        enabled = _env_truthy(_OTEL_ENABLED_ENV)
    max_length = tracing.get("query_text_max_length") if tracing is not None else None
    if max_length is None:
        try:
            max_length = int(os.getenv(_OTEL_QUERY_TEXT_MAX_LENGTH_ENV, "0"))
        except ValueError:
            max_length = 0
    return {"enabled": bool(enabled), "query_text_max_length": max(0, max_length)}


def _is_tracing_enabled(tracing_options: Optional[TracingOptions]) -> bool:
    """Return True if OTel command spans should be created for this client.

    ``tracing_options`` is the client's resolved ``tracing`` option (see
    :func:`_resolve_tracing_options`); ``None`` means there is no client
    context (connection handshakes, server monitoring), which is never traced.
    This runs on every command and must be cheap: no environment lookups.
    """
    if tracing_options is None:
        return False
    return bool(tracing_options.get("enabled"))


def _get_query_text_max_length(tracing_options: Optional[TracingOptions]) -> int:
    """Return the resolved ``db.query.text`` truncation length, or 0 to omit the attribute."""
    if tracing_options is None:
        return 0
    return tracing_options.get("query_text_max_length") or 0


def _connection_attributes(conn: _ConnectionTelemetryInfo) -> dict[str, Any]:
    """Return the connection-static span attributes as a fresh dict.

    The values are derived from the connection's address and ids, which do not
    change during the life of a connection, so they are computed once and
    cached on the connection (keyed by the server connection id, which changes
    when the underlying socket is re-established, invalidating the cache). A
    checked-out connection is owned by one caller at a time, so reading and
    updating the cache needs no lock.
    """
    server_connection_id = conn.server_connection_id
    cached = getattr(conn, "_otel_connection_attributes", None)
    if cached is not None and cached[0] == server_connection_id and cached[1]:
        return cached[1].copy()
    address = conn.address
    attrs: dict[str, Any] = {
        "db.system.name": "mongodb",
        "server.address": address[0],
        "network.transport": "unix" if address[1] is None else "tcp",
        "db.mongodb.driver_connection_id": conn.id,
    }
    if address[1] is not None:
        attrs["server.port"] = address[1]
    if server_connection_id is not None:
        attrs["db.mongodb.server_connection_id"] = server_connection_id
    if cached is not None:
        # Duck-typed fakes without the attribute (test doubles for the
        # Protocol) skip caching rather than growing a new attribute.
        conn._otel_connection_attributes = (server_connection_id, attrs)
    return attrs.copy()


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
        # A budget smaller than the marker truncates without it so the result
        # still never exceeds max_length.
        suffix = "..." if max_length >= 3 else ""
        text = text[: max_length - len(suffix)] + suffix
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
    if command_name in _NOT_COLLECTION_COMMANDS:
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
    name = command_name.lower()
    if name in _SENSITIVE_COMMANDS_LOWER:
        return True
    return name in _HELLO_COMMANDS_LOWER and speculative_hello


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

    Only cheap attributes (constants and values already held by the
    connection) are provided at span creation, so they are visible to the
    sampler. The expensive ones are added only when the span is being
    recorded, per the OpenTelemetry spec's performance guidelines, so
    unsampled configurations pay little more than the span creation itself.
    """
    if not _is_tracing_enabled(tracing_options):
        return None
    if _is_sensitive_command(command_name, speculative_hello):
        return None

    collection = _extract_collection_name(command_name, dbname, cmd)
    attributes = _connection_attributes(conn)
    attributes["db.namespace"] = dbname
    attributes["db.command.name"] = command_name
    if collection:
        attributes["db.collection.name"] = collection

    assert _TRACER is not None  # _is_tracing_enabled already checked _HAS_OPENTELEMETRY
    span = _TRACER.start_span(command_name, kind=_SPAN_KIND_CLIENT, attributes=attributes)
    if not span.is_recording():
        # The span was dropped by the sampler (or tracing is a no-op): the
        # spec says no further attributes are added to it.
        return span

    # Expensive attributes, added after the sampling decision; they are not
    # visible to samplers.
    span.set_attribute("db.query.summary", _build_query_summary(command_name, dbname, collection))
    lsid = cmd.get("lsid")
    if isinstance(lsid, Mapping):
        formatted_lsid = _format_lsid(lsid)
        if formatted_lsid is not None:
            span.set_attribute("db.mongodb.lsid", formatted_lsid)
    txn_number = cmd.get("txnNumber")
    if txn_number is not None:
        span.set_attribute("db.mongodb.txn_number", txn_number)
    max_query_text_length = _get_query_text_max_length(tracing_options)
    if max_query_text_length > 0:
        span.set_attribute("db.query.text", _build_query_text(cmd, max_query_text_length))
    return span


def end_command_span_success(
    span: Optional[Span],
    cmd: Mapping[str, Any],
    command_name: str,
    reply: _DocumentOut,
) -> None:
    """Set the cursor id (if any) and end the span.

    The ``db.mongodb.cursor_id`` rules (spec: "db.mongodb.cursor_id"): a
    non-zero reply cursor id is recorded as-is; a zero reply id means the
    cursor is exhausted, so the attribute is omitted unless the command
    operated on an existing cursor (getMore), in which case the cursor id the
    command sent is recorded; a literal zero is never recorded.
    """
    if span is None:
        return
    cursor = reply.get("cursor")
    if isinstance(cursor, Mapping) and "id" in cursor:
        cursor_id = cursor["id"]
        if cursor_id:
            span.set_attribute("db.mongodb.cursor_id", cursor_id)
        elif command_name == _GET_MORE:
            sent_id = cmd.get(_GET_MORE)
            if sent_id:
                span.set_attribute("db.mongodb.cursor_id", sent_id)
    span.end()


def end_command_span_failure(
    span: Optional[Span],
    failure: _DocumentOut,
    exc: BaseException,
) -> None:
    """Record the exception, set the error status, and end the span."""
    if span is None:
        return
    span.record_exception(exc)
    code = failure.get("code")
    if code is not None:
        span.set_attribute("db.response.status_code", str(code))
    span.set_status(Status(StatusCode.ERROR, description=failure.get("errmsg")))
    span.end()
