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
except ImportError:
    _HAS_OPENTELEMETRY = False

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


def _get_tracer() -> Tracer:
    """Return a Tracer scoped to this driver's name and version."""
    return trace.get_tracer("PyMongo", __version__)


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
    string cut with a "..." marker as a safety net for whatever the field
    truncation's size estimate still leaves over ``max_length``.
    """
    filtered = {k: v for k, v in cmd.items() if k not in _QUERY_TEXT_EXCLUDED_FIELDS}
    truncated_cmd = _truncate_documents(filtered, max_length)[0]
    text = json_util.dumps(truncated_cmd, json_options=_JSON_OPTIONS)
    if len(text) > max_length:
        text = text[:max_length] + "..."
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
    if _is_sensitive_command(command_name, speculative_hello):
        return None

    collection = _extract_collection_name(command_name, dbname, cmd)
    address = conn.address
    attributes: dict[str, Any] = {
        "db.system.name": "mongodb",
        "db.namespace": dbname,
        "db.command.name": command_name,
        "db.query.summary": _build_query_summary(command_name, dbname, collection),
        "server.address": address[0],
        "network.transport": "unix" if address[0].endswith(".sock") else "tcp",
        "db.mongodb.driver_connection_id": conn.id,
    }
    # Unix domain socket addresses have no port.
    if address[1] is not None:
        attributes["server.port"] = address[1]
    if collection:
        attributes["db.collection.name"] = collection
    if conn.server_connection_id is not None:
        attributes["db.mongodb.server_connection_id"] = conn.server_connection_id
    lsid = cmd.get("lsid")
    if lsid:
        formatted_lsid = _format_lsid(lsid)
        if formatted_lsid is not None:
            attributes["db.mongodb.lsid"] = formatted_lsid
    txn_number = cmd.get("txnNumber")
    if txn_number is not None:
        attributes["db.mongodb.txn_number"] = txn_number
    max_query_text_length = _get_query_text_max_length(tracing_options)
    if max_query_text_length > 0:
        attributes["db.query.text"] = _build_query_text(cmd, max_query_text_length)

    tracer = _get_tracer()
    return tracer.start_span(command_name, kind=SpanKind.CLIENT, attributes=attributes)


def end_command_span_success(span: Optional[Span], reply: _DocumentOut) -> None:
    """Set the cursor id (if any) and end the span."""
    if span is None:
        return
    cursor = reply.get("cursor")
    if isinstance(cursor, Mapping) and "id" in cursor:
        span.set_attribute("db.mongodb.cursor_id", cursor["id"])
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
