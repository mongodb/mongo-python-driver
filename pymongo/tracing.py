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

"""OpenTelemetry tracing support for PyMongo.

This module provides optional OpenTelemetry tracing for MongoDB commands.
Tracing is disabled by default and requires the opentelemetry-api package.

To enable tracing, set the environment variable:
    OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED=true

.. versionadded:: 4.x
"""
from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Mapping, Optional

from pymongo.logger import _SENSITIVE_COMMANDS

try:
    from opentelemetry import trace
    from opentelemetry.trace import Span, SpanKind, Status, StatusCode

    _HAS_OPENTELEMETRY = True
except ImportError:
    _HAS_OPENTELEMETRY = False
    trace = None  # type: ignore[assignment]
    Span = None  # type: ignore[assignment, misc]
    SpanKind = None  # type: ignore[assignment, misc]
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer

# Environment variable names
_OTEL_ENABLED_ENV = "OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED"


def _is_tracing_enabled() -> bool:
    """Check if tracing is enabled via environment variable."""
    if not _HAS_OPENTELEMETRY:
        return False
    value = os.environ.get(_OTEL_ENABLED_ENV, "").lower()
    return value in ("1", "true")


def _get_tracer() -> Optional[Tracer]:
    """Get the PyMongo tracer instance."""
    if not _HAS_OPENTELEMETRY or not _is_tracing_enabled():
        return None
    from pymongo._version import __version__

    return trace.get_tracer("PyMongo", __version__)


def _is_sensitive_command(command_name: str) -> bool:
    """Check if a command is sensitive and should not be traced."""
    return command_name.lower() in _SENSITIVE_COMMANDS


def _build_query_summary(
    command_name: str,
    database_name: str,
    collection_name: Optional[str],
) -> str:
    """Build the db.query.summary attribute value."""
    if collection_name:
        return f"{command_name} {database_name}.{collection_name}"
    return f"{command_name} {database_name}"


def _extract_collection_name(spec: Mapping[str, Any]) -> Optional[str]:
    """Extract collection name from command spec if applicable."""
    if not spec:
        return None
    cmd_name = next(iter(spec)).lower()
    # Commands where the first value is the collection name
    if cmd_name in (
        "insert",
        "update",
        "delete",
        "find",
        "aggregate",
        "findandmodify",
        "count",
        "distinct",
        "create",
        "drop",
        "createindexes",
        "dropindexes",
        "listindexes",
    ):
        value = spec.get(next(iter(spec)))
        if isinstance(value, str):
            return value
    return None


def record_command_exception(
    span: Optional[Span],
    exception: BaseException,
    error_code: Optional[int] = None,
) -> None:
    """Record an exception on a command span."""
    if span is None or not _HAS_OPENTELEMETRY:
        return

    span.record_exception(exception)
    span.set_status(Status(StatusCode.ERROR, str(exception)))

    if error_code is not None:
        span.set_attribute("db.response.status_code", str(error_code))


def add_cursor_id(span: Optional[Span], cursor_id: int) -> None:
    """Add cursor ID attribute to span if present."""
    if span is None or cursor_id == 0:
        return
    span.set_attribute("db.mongodb.cursor_id", cursor_id)
