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

"""Optional OpenTelemetry command-span support (DRIVERS-719).

Kept separate from :mod:`pymongo._telemetry` so that module stays free of
``opentelemetry`` import guards. Every function here is a no-op when
``opentelemetry`` isn't installed or tracing isn't enabled.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING, Any, Optional, TypedDict

from bson import json_util
from pymongo._version import __version__
from pymongo.logger import _HELLO_COMMANDS, _SENSITIVE_COMMANDS

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
    """The shape of the ``MongoClient`` ``tracing`` option."""

    enabled: bool
    query_text_max_length: int


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


def _env_truthy(name: str) -> bool:
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
    return trace.get_tracer("PyMongo", __version__)


def _get_query_text_max_length(tracing_options: Optional[TracingOptions]) -> int:
    """Return the configured db.query.text truncation length, or 0 to omit the attribute."""
    client_value = tracing_options.get("query_text_max_length", 0) if tracing_options else 0
    if client_value > 0:
        return client_value
    try:
        return max(0, int(os.getenv(_OTEL_QUERY_TEXT_MAX_LENGTH_ENV, "0")))
    except ValueError:
        return 0


def _build_query_text(cmd: Mapping[str, Any], max_length: int) -> str:
    filtered = {k: v for k, v in cmd.items() if k not in _QUERY_TEXT_EXCLUDED_FIELDS}
    text = json_util.dumps(filtered)
    return text[:max_length]


def _extract_collection_name(command_name: str, cmd: Mapping[str, Any]) -> Optional[str]:
    """Return the collection name targeted by ``cmd``, or None if it doesn't target one."""
    key = "collection" if command_name == _GET_MORE else command_name
    value = cmd.get(key)
    return value if isinstance(value, str) else None


def _build_query_summary(command_name: str, dbname: str, collection: Optional[str]) -> str:
    if collection:
        return f"{command_name} {dbname}.{collection}"
    return f"{command_name} {dbname}"


def _is_sensitive_command(command_name: str, speculative_hello: bool) -> bool:
    """Mirror the redaction rules in ``pymongo.logger.LogMessage._is_sensitive``."""
    if command_name in _SENSITIVE_COMMANDS:
        return True
    return command_name in _HELLO_COMMANDS and speculative_hello


def _format_lsid(lsid: Mapping[str, Any]) -> Optional[str]:
    id_value = lsid.get("id")
    if id_value is None:
        return None
    try:
        return str(id_value.as_uuid())
    except (AttributeError, ValueError):
        return str(id_value)


def start_span(
    tracing_options: Optional[TracingOptions],
    conn: _ConnectionTelemetryInfo,
    cmd: MutableMapping[str, Any],
    dbname: str,
    command_name: str,
    speculative_hello: bool,
) -> Optional[Span]:
    """Start and return a CLIENT-kind span for a server command, or None.

    Returns None when tracing is disabled/unavailable or the command is
    sensitive (mirroring the redaction applied to logs and APM events).
    """
    if not _is_tracing_enabled(tracing_options):
        return None
    if _is_sensitive_command(command_name, speculative_hello):
        return None

    collection = _extract_collection_name(command_name, cmd)
    address = conn.address
    attributes: dict[str, Any] = {
        "db.system.name": "mongodb",
        "db.namespace": dbname,
        "db.command.name": command_name,
        "db.query.summary": _build_query_summary(command_name, dbname, collection),
        "server.address": address[0],
        "server.port": address[1],
        "network.transport": "unix" if address[0].endswith(".sock") else "tcp",
        "db.mongodb.driver_connection_id": conn.id,
    }
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


def end_span_success(span: Optional[Span], reply: _DocumentOut) -> None:
    """Set the cursor id (if any) and end the span."""
    if span is None:
        return
    cursor = reply.get("cursor")
    if isinstance(cursor, Mapping) and "id" in cursor:
        span.set_attribute("db.mongodb.cursor_id", cursor["id"])
    span.end()


def end_span_failure(
    span: Optional[Span],
    failure: _DocumentOut,
    exc: Optional[BaseException],
) -> None:
    """Record the exception, set the error status, and end the span."""
    if span is None:
        return
    if exc is not None:
        span.record_exception(exc)
    code = failure.get("code")
    if code is not None:
        span.set_attribute("db.response.status_code", str(code))
    span.set_status(Status(StatusCode.ERROR, description=failure.get("errmsg")))
    span.end()
