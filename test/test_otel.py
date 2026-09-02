# Copyright 2026-present MongoDB, Inc.
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

"""Test OpenTelemetry operation spans, excluding cursor getMores."""

from __future__ import annotations

import gc
import os
import subprocess
import sys
from typing import Optional
from unittest.mock import patch

sys.path[0:0] = [""]

import pytest

import pymongo._otel as _otel
from pymongo import _telemetry, common
from pymongo._telemetry import _OperationTelemetry
from pymongo.cursor_shared import CursorType
from pymongo.errors import (
    ClientBulkWriteException,
    ConfigurationError,
    ConnectionFailure,
    InvalidOperation,
    NetworkTimeout,
    OperationFailure,
    ServerSelectionTimeoutError,
)
from pymongo.logger import _HELLO_COMMANDS
from pymongo.operations import InsertOne
from pymongo.read_preferences import ReadPreference
from pymongo.typings import _Address
from test import IntegrationTest, client_context, unittest
from test.unified_format_shared import _shared_test_provider
from test.utils import wait_until

_HAS_OTEL_TEST_DEPS = False
if _otel._HAS_OPENTELEMETRY:
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
        from opentelemetry.trace import StatusCode

        _HAS_OTEL_TEST_DEPS = True
    except ImportError:
        pass

_IS_SYNC = True

pytestmark = pytest.mark.otel


def _tracing_opts() -> _otel.TracingOptions:
    """Return resolved tracing options with tracing on and ``db.query.text`` disabled."""
    return {"enabled": True, "query_text_max_length": 0}


class TestBuildQueryText(unittest.TestCase):
    def test_result_never_exceeds_max_length(self):
        cmd = {"find": "coll", "filter": {"x": 1}}
        for max_length in range(1, 10):
            text = _otel._build_query_text(cmd, max_length)
            self.assertLessEqual(len(text), max_length, (max_length, text))


def _qualified_name(exc_type: type) -> str:
    """Format an exception class the way the spans do: ``module.QualName``."""
    return f"{exc_type.__module__}.{exc_type.__qualname__}"


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOTelOperationSpanPrimitives(unittest.TestCase):
    """Unit tests for the pymongo._otel operation-span primitives."""

    @classmethod
    def setUpClass(cls):
        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    @classmethod
    def tearDownClass(cls):
        # The span processor can never be removed from the shared process-wide
        # TracerProvider, so without this the exporter keeps accumulating every
        # span from every client for the rest of the test run.
        cls.exporter.shutdown()

    def setUp(self):
        self.exporter.clear()

    def _finished_span(self, operation: str = "find", **kwargs):
        """Start an operation span, end it successfully, and return the one finished span."""
        handle = _otel.start_operation_span(_tracing_opts(), operation, None, **kwargs)
        self.assertIsNotNone(handle)
        _otel.end_operation_span_success(handle)
        (span,) = self.exporter.get_finished_spans()
        return span

    def test_start_operation_span_success_sets_provisional_attributes(self):
        span = self._finished_span()
        self.assertEqual(span.name, "find")
        self.assertEqual(span.attributes["db.system.name"], "mongodb")
        self.assertEqual(span.attributes["db.operation.name"], "find")
        self.assertEqual(span.status.status_code, StatusCode.UNSET)

    def test_start_operation_span_failure_records_exception(self):
        handle = _otel.start_operation_span(_tracing_opts(), "insert", None)
        _otel.end_operation_span_failure(handle, ValueError("boom"))
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(len(span.events), 1)
        self.assertEqual(span.events[0].name, "exception")

    def test_start_operation_span_with_parent(self):
        parent_handle = _otel.start_operation_span(_tracing_opts(), "transaction", None)
        handle = _otel.start_operation_span(_tracing_opts(), "insert", parent_handle.span)
        _otel.end_operation_span_success(handle)
        _otel.end_operation_span_success(parent_handle)
        child, parent = self.exporter.get_finished_spans()
        self.assertEqual(child.parent.span_id, parent.context.span_id)

    def test_current_operation_name_contextvar_scoped_correctly(self):
        self.assertIsNone(_otel._CURRENT_OPERATION_NAME.get())
        handle = _otel.start_operation_span(_tracing_opts(), "find", None)
        self.assertEqual(_otel._CURRENT_OPERATION_NAME.get(), "find")
        _otel.end_operation_span_success(handle)
        self.assertIsNone(_otel._CURRENT_OPERATION_NAME.get())

    def test_eager_dbname_and_collection_set_at_creation(self):
        span = self._finished_span(dbname="mydb", collection="mycoll")
        self.assertEqual(span.name, "find mydb.mycoll")
        self.assertEqual(span.attributes["db.namespace"], "mydb")
        self.assertEqual(span.attributes["db.collection.name"], "mycoll")
        self.assertEqual(span.attributes["db.operation.summary"], "find mydb.mycoll")
        self.assertEqual(span.attributes["db.operation.name"], "find")

    def test_eager_dbname_without_collection_omits_collection_attribute(self):
        span = self._finished_span("listCollections", dbname="mydb")
        self.assertEqual(span.name, "listCollections mydb")
        self.assertEqual(span.attributes["db.operation.summary"], "listCollections mydb")
        self.assertNotIn("db.collection.name", span.attributes)

    def test_no_eager_attributes_leaves_provisional_name(self):
        span = self._finished_span()
        self.assertEqual(span.name, "find")
        self.assertNotIn("db.namespace", span.attributes)
        # db.operation.summary is Required (unlike db.namespace, which is only
        # "Required if available"), so it always falls back to the bare
        # operation name when no dbname is given.
        self.assertEqual(span.attributes["db.operation.summary"], "find")

    def test_detached_span_is_not_current_until_used(self):
        handle = _otel.start_operation_span(_tracing_opts(), "find", None, set_current=False)
        self.assertIsNotNone(handle)
        # Not current, and the operation-name contextvar is untouched.
        self.assertIsNot(trace.get_current_span(), handle.span)
        self.assertIsNone(_otel._CURRENT_OPERATION_NAME.get())
        with _otel.use_operation_span(handle):
            self.assertIs(trace.get_current_span(), handle.span)
            self.assertEqual(_otel._CURRENT_OPERATION_NAME.get(), "find")
        # Restored afterwards, and the span is still open (not ended).
        self.assertIsNot(trace.get_current_span(), handle.span)
        self.assertIsNone(_otel._CURRENT_OPERATION_NAME.get())
        self.assertEqual(self.exporter.get_finished_spans(), ())
        _otel.end_operation_span_success(handle)
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "find")

    def test_detached_span_reused_across_multiple_use_blocks(self):
        handle = _otel.start_operation_span(_tracing_opts(), "find", None, set_current=False)
        for _ in range(3):
            with _otel.use_operation_span(handle):
                pass
        self.assertEqual(self.exporter.get_finished_spans(), ())
        _otel.end_operation_span_success(handle)
        self.assertEqual(len(self.exporter.get_finished_spans()), 1)

    def test_use_operation_span_with_none_handle_is_noop(self):
        with _otel.use_operation_span(None):
            pass
        self.assertEqual(self.exporter.get_finished_spans(), ())

    def test_detached_span_failure_inside_use_block_records_exception_once(self):
        # Regression test: use_span's record_exception/set_status_on_exception
        # default to True, so without disabling them an exception leaving the
        # block is recorded twice, here and by end_operation_span_failure.
        handle = _otel.start_operation_span(_tracing_opts(), "find", None, set_current=False)
        exc = ValueError("boom")
        try:
            with _otel.use_operation_span(handle):
                raise exc
        except ValueError:
            pass
        _otel.end_operation_span_failure(handle, exc)
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        exception_events = [e for e in span.events if e.name == "exception"]
        self.assertEqual(len(exception_events), 1)

    def test_detached_span_failure_without_use_block(self):
        handle = _otel.start_operation_span(_tracing_opts(), "find", None, set_current=False)
        _otel.end_operation_span_failure(handle, ValueError("boom"))
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        exception_events = [e for e in span.events if e.name == "exception"]
        self.assertEqual(len(exception_events), 1)


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOperationTelemetry(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    @classmethod
    def tearDownClass(cls):
        # The span processor can never be removed from the shared process-wide
        # TracerProvider, so without this the exporter keeps accumulating every
        # span from every client for the rest of the test run.
        cls.exporter.shutdown()

    def setUp(self):
        self.exporter.clear()

    def test_succeeded_with_no_session(self):
        telemetry = _telemetry._OperationTelemetry(_tracing_opts(), "find", None)
        telemetry.succeeded()
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "find")
        self.assertIsNone(span.parent)

    def test_failed_records_exception(self):
        telemetry = _telemetry._OperationTelemetry(_tracing_opts(), "insert", None)
        telemetry.failed(RuntimeError("nope"))
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)

    def test_disabled_is_a_no_op(self):
        telemetry = _telemetry._OperationTelemetry(None, "find", None)
        telemetry.succeeded()  # must not raise
        telemetry2 = _telemetry._OperationTelemetry(None, "find", None)
        telemetry2.failed(RuntimeError("x"))  # must not raise
        self.assertEqual(self.exporter.get_finished_spans(), ())

    def test_run_command_operation_name_override(self):
        # Per the spec, Database.command() produces a "runCommand" operation
        # span, not one named after the command actually sent.
        telemetry = _telemetry._OperationTelemetry(
            _tracing_opts(), "ping", None, is_run_command=True
        )
        telemetry.succeeded()
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "runCommand")
        self.assertEqual(span.attributes["db.operation.name"], "runCommand")


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOperationTelemetryContextManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    @classmethod
    def tearDownClass(cls):
        # The span processor can never be removed from the shared process-wide
        # TracerProvider, so without this the exporter keeps accumulating every
        # span from every client for the rest of the test run.
        cls.exporter.shutdown()

    def setUp(self):
        self.exporter.clear()

    def test_context_manager_success_ends_span(self):
        with _OperationTelemetry(
            _tracing_opts(), "killCursors", None, dbname="mydb", collection="c"
        ):
            pass
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "killCursors mydb.c")
        self.assertEqual(span.status.status_code, StatusCode.UNSET)

    def test_context_manager_failure_records_exception(self):
        with self.assertRaises(ValueError):
            with _OperationTelemetry(_tracing_opts(), "killCursors", None, dbname="mydb"):
                raise ValueError("boom")
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(span.attributes["exception.type"], "ValueError")

    def test_detached_telemetry_use_makes_span_current(self):
        telemetry = _OperationTelemetry(
            _tracing_opts(), "find", None, dbname="mydb", collection="c", set_current=False
        )
        self.assertIsNot(trace.get_current_span(), telemetry.handle.span)
        with telemetry.use():
            self.assertIs(trace.get_current_span(), telemetry.handle.span)
        self.assertEqual(self.exporter.get_finished_spans(), ())
        telemetry.succeeded()
        self.assertEqual(len(self.exporter.get_finished_spans()), 1)


_NO_OPENTELEMETRY_SCRIPT = """
import builtins
import sys

_real_import = builtins.__import__


def _blocked_import(name, *args, **kwargs):
    if name == "opentelemetry" or name.startswith("opentelemetry."):
        raise ImportError("blocked for test")
    return _real_import(name, *args, **kwargs)


builtins.__import__ = _blocked_import

import pymongo._otel as _otel

assert _otel._HAS_OPENTELEMETRY is False, "expected opentelemetry to be unavailable"

from pymongo import MongoClient

client = MongoClient(sys.argv[1], tracing={"enabled": True})
client.admin.command("ping")
client.close()
print("SUBPROCESS_OK")
"""


class TestOTelWithoutOpenTelemetryInstalled(IntegrationTest):
    # Blocks the opentelemetry import at the interpreter level, so this covers
    # the no-op path regardless of whether opentelemetry-sdk is installed here.
    @client_context.require_sync
    def test_no_op_when_opentelemetry_is_unimportable(self):
        result = subprocess.run(
            [sys.executable, "-c", _NO_OPENTELEMETRY_SCRIPT, client_context.uri],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("SUBPROCESS_OK", result.stdout)


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOTelSpans(IntegrationTest):
    """Operation and command spans for a single round trip."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    @classmethod
    def tearDownClass(cls):
        # A span processor cannot be removed from the shared process-wide
        # TracerProvider, so without this shutdown() the exporter accumulates
        # every span from every client for the rest of the run.
        cls.exporter.shutdown()
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        self.exporter.clear()

    def spans(self, name: str | None = None):
        finished = self.exporter.get_finished_spans()
        if name is None:
            return list(finished)
        return [s for s in finished if s.name == name]

    @staticmethod
    def operation_spans(finished, operation: str):
        """Return the operation spans for ``operation``, excluding command spans.

        Only command spans carry db.command.name, so its absence is what tells
        the two kinds apart when both name the same operation.
        """
        return [
            s
            for s in finished
            if s.attributes.get("db.operation.name") == operation
            and "db.command.name" not in s.attributes
        ]

    @staticmethod
    def command_spans(finished, command: str):
        """Return the command spans for ``command``."""
        return [s for s in finished if s.attributes.get("db.command.name") == command]

    def ping_spans(self):
        """Return the spans belonging to a ``ping`` run through ``db.command()``.

        For tests asserting tracing produced *nothing*. An empty-exporter
        assertion would also catch unrelated spans, since a cursor abandoned
        earlier ends its span from a finalizer that runs at an unpredictable
        point on interpreters without reference counting.
        """
        return [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.command.name") == "ping"
            or s.attributes.get("db.operation.name") == "runCommand"
        ]

    def _aggregate_operation_span(self):
        matching = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.operation.name") == "aggregate"
        ]
        self.assertEqual(len(matching), 1)
        return matching[0]

    def test_operation_span_records_failure(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test
        self.exporter.clear()
        with self.assertRaises(OperationFailure):
            coll.find_one({"$invalidOperator": 1})
        matching = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.operation.name") == "find"
        ]
        self.assertEqual(len(matching), 1)
        op_span = matching[0]
        self.assertEqual(op_span.status.status_code, StatusCode.ERROR)
        # The spec requires exception.type/message/stacktrace as *attributes*
        # on the operation span, not just the event record_exception attaches.
        self.assertTrue(any(event.name == "exception" for event in op_span.events))
        self.assertIn("exception.type", op_span.attributes)
        self.assertIn("exception.message", op_span.attributes)
        self.assertIn("exception.stacktrace", op_span.attributes)

    def test_explain_retains_collection_name(self):
        # explain wraps the real command ({"explain": {"find": "coll", ...}}), the
        # same shape as getMore's indirection, so it needs the same handling.
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        client[self.db.name].command("explain", {"find": "test_otel", "filter": {}})

        spans = self.spans("explain")
        self.assertEqual(len(spans), 1)
        attrs = spans[0].attributes
        self.assertEqual(attrs["db.collection.name"], "test_otel")
        self.assertEqual(attrs["db.query.summary"], f"explain {self.db.name}.test_otel")

    def test_server_port_omitted_for_unix_socket(self):
        class _FakeUnixConn:
            id = 1
            server_connection_id: Optional[int] = None
            address: _Address = ("/tmp/fake-otel-test.sock", None)
            service_id = None

        self.exporter.clear()
        span = _otel.start_command_span(
            {"enabled": True, "query_text_max_length": 0},
            _FakeUnixConn(),
            {"ping": 1},
            "admin",
            "ping",
            False,
        )
        _otel.end_command_span_success(span, {"ok": 1})

        spans = self.spans("ping")
        self.assertEqual(len(spans), 1)
        attrs = spans[0].attributes
        self.assertNotIn("server.port", attrs)
        self.assertEqual(attrs["network.transport"], "unix")

    def test_sensitive_command_produces_no_span(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        with self.assertRaises(OperationFailure):
            client.admin.command("saslStart", mechanism="SCRAM-SHA-256", payload=b"")

        # The inner command span must stay fully suppressed for sensitive commands.
        command_span_names = [s.name for s in self.spans() if "db.command.name" in s.attributes]
        self.assertNotIn("saslStart", command_span_names)

        # The operation span still carries namespace and summary, backfilled
        # before start_command_span's sensitive-command return.
        finished = self.exporter.get_finished_spans()
        operation_names = [s.attributes.get("db.operation.name") for s in finished]
        self.assertNotIn("saslStart", operation_names)
        self.assertIn("runCommand", operation_names)
        op_span = next(s for s in finished if s.attributes.get("db.operation.name") == "runCommand")
        self.assertEqual(op_span.name, "runCommand admin")
        self.assertEqual(op_span.attributes["db.namespace"], "admin")
        self.assertEqual(op_span.attributes["db.operation.summary"], "runCommand admin")

    def test_admin_command_omits_collection_name(self):
        # usersInfo's command value is a username string, not a collection, and
        # it always runs against admin; querying a nonexistent user is a no-op.
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        client.admin.command("usersInfo", "pymongo_otel_nonexistent_user")

        spans = self.spans("usersInfo")
        self.assertEqual(len(spans), 1)
        attrs = spans[0].attributes
        self.assertEqual(attrs["db.namespace"], "admin")
        self.assertNotIn("db.collection.name", attrs)
        self.assertEqual(attrs["db.query.summary"], "usersInfo admin")

    def test_database_command_produces_run_command_operation_span(self):
        # The spec names anything reached through Database.command()
        # "runCommand", so admin.command("ping") yields an operation span named
        # "runCommand admin", not "ping admin".
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        client.admin.command("ping")

        finished = self.exporter.get_finished_spans()
        matching = [s for s in finished if s.attributes.get("db.operation.name") == "runCommand"]
        self.assertEqual(len(matching), 1)
        op_span = matching[0]
        self.assertEqual(op_span.name, "runCommand admin")
        self.assertEqual(op_span.attributes["db.namespace"], "admin")
        # The wire-level command span is unaffected: it's still named/attributed
        # after the actual command sent.
        cmd_spans = [s for s in finished if s.attributes.get("db.command.name") == "ping"]
        self.assertEqual(len(cmd_spans), 1)
        self.assertEqual(cmd_spans[0].name, "ping")

    def test_failure_records_exception_and_status_code(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        with self.assertRaises(OperationFailure):
            client[self.db.name].command("thisCommandDoesNotExist")

        # This also produces an ERROR operation span, so narrow to the command
        # span, which alone carries db.response.status_code.
        spans = [s for s in self.spans() if "db.response.status_code" in s.attributes]
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertEqual(span.status.status_code, trace.StatusCode.ERROR)
        self.assertIn("db.response.status_code", span.attributes)
        # For a server error the spec has error.type mirror the status code.
        self.assertEqual(span.attributes["error.type"], span.attributes["db.response.status_code"])
        self.assertTrue(any(event.name == "exception" for event in span.events))

    @client_context.require_failCommand_fail_point
    def test_error_type_is_exception_class_name_for_connection_failure(self):
        # A closed connection produces no server reply, so error.type uses the class name.
        client = self.rs_or_single_client(tracing={"enabled": True}, retryReads=False)
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {"failCommands": ["find"], "closeConnection": True},
        }
        with self.fail_point(fail_command):
            self.exporter.clear()
            with self.assertRaises(ConnectionFailure) as ctx:
                client[self.db.name].test.find_one({})

        spans = [s for s in self.spans() if s.attributes.get("db.command.name") == "find"]
        self.assertEqual(len(spans), 1)
        attrs = spans[0].attributes
        self.assertNotIn("db.response.status_code", attrs)
        self.assertEqual(attrs["error.type"], _qualified_name(type(ctx.exception)))
        self.assertEqual(attrs["error.type"], attrs["exception.type"])

    @client_context.require_failCommand_blockConnection
    def test_error_type_is_exception_class_name_for_network_timeout(self):
        # socketTimeoutMS trips before any reply, so there is no server error code.
        client = self.rs_or_single_client(
            tracing={"enabled": True}, socketTimeoutMS=200, retryReads=False
        )
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {
                "failCommands": ["find"],
                "blockConnection": True,
                "blockTimeMS": 1000,
            },
        }
        with self.fail_point(fail_command):
            self.exporter.clear()
            with self.assertRaises(NetworkTimeout) as ctx:
                client[self.db.name].test.find_one({})

        spans = [s for s in self.spans() if s.attributes.get("db.command.name") == "find"]
        self.assertEqual(len(spans), 1)
        attrs = spans[0].attributes
        self.assertNotIn("db.response.status_code", attrs)
        self.assertEqual(attrs["error.type"], _qualified_name(NetworkTimeout))
        self.assertIsInstance(ctx.exception, NetworkTimeout)

    def test_tracing_disabled_by_default(self):
        client = self.rs_or_single_client()
        self.exporter.clear()
        client.admin.command("ping")
        self.assertEqual(self.ping_spans(), [])

    def test_prose_1_tracing_enable_disable_via_env_var(self):
        """Prose Test 1: Tracing Enable/Disable via Environment Variable."""
        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "false"}):
            client = self.rs_or_single_client()
            self.exporter.clear()
            client.admin.command("ping")
        # When tracing is disabled we must suppress both the operation span and
        # the command span it wraps: db.command() routes through _retry_internal
        # same as any CRUD call, so both would exist if tracing weren't fully off.
        self.assertEqual(self.ping_spans(), [])

        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true"}):
            client = self.rs_or_single_client()
            self.exporter.clear()
            client.admin.command("ping")
        finished = self.exporter.get_finished_spans()
        # start_command_span renames the operation span in place, so span.name
        # cannot tell the two apart; db.command.name and db.operation.name can.
        # The operation span reads "runCommand" rather than "ping".
        self.assertIn("ping", [s.attributes.get("db.command.name") for s in finished])
        self.assertIn("runCommand", [s.attributes.get("db.operation.name") for s in finished])

    def test_env_var_tracing_does_not_trace_monitor_commands(self):
        # Monitor and handshake connections have no client, so their tracing
        # options are None. Enablement must not fall back to the environment
        # variable there, or every hello would get a span the spec excludes.
        self.assertFalse(_otel._is_tracing_enabled(None))
        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true"}):
            self.assertFalse(_otel._is_tracing_enabled(None))
            client = self.rs_or_single_client()
            self.exporter.clear()
            client.admin.command("ping")

        finished = self.exporter.get_finished_spans()
        # The env var still enables tracing for the client's own commands...
        self.assertIn("ping", [s.attributes.get("db.command.name") for s in finished])
        # ...but nothing traces the monitors' hellos.
        hello_spans = [
            s
            for s in finished
            if s.attributes.get("db.command.name") in _HELLO_COMMANDS or s.name in _HELLO_COMMANDS
        ]
        self.assertEqual(hello_spans, [], [s.name for s in finished])

    def test_prose_2_command_payload_emission_via_env_var(self):
        """Prose Test 2: Command Payload Emission via Environment Variable."""

        def command_spans():
            # self.spans("find") would also match the outer operation span, so
            # filter on db.command.name to isolate the command span that
            # carries db.query.text.
            return [
                s
                for s in self.exporter.get_finished_spans()
                if s.attributes.get("db.command.name") == "find"
            ]

        env = {
            "OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true",
            "OTEL_PYTHON_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH": "1024",
        }
        with patch.dict(os.environ, env):
            client = self.rs_or_single_client()
            self.exporter.clear()
            client[self.db.name].test_otel.find({}).to_list()
        spans = command_spans()
        self.assertEqual(len(spans), 1)
        self.assertIn("db.query.text", spans[0].attributes)

        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true"}):
            client = self.rs_or_single_client()
            self.exporter.clear()
            client[self.db.name].test_otel.find({}).to_list()
        spans = command_spans()
        self.assertEqual(len(spans), 1)
        self.assertNotIn("db.query.text", spans[0].attributes)

    def test_explicit_query_text_max_length_zero_overrides_env_var(self):
        # An explicit client-side 0 must win over the environment variable, unlike
        # unset (which defers to it) - otherwise an app can't reliably opt out.
        env = {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH": "1024"}
        with patch.dict(os.environ, env):
            client = self.rs_or_single_client(tracing={"enabled": True, "query_text_max_length": 0})
            self.exporter.clear()
            client.admin.command("ping")

        spans = self.spans("ping")
        self.assertEqual(len(spans), 1)
        self.assertNotIn("db.query.text", spans[0].attributes)

    def test_query_text_truncation_shrinks_oversized_field_values(self):
        client = self.rs_or_single_client(tracing={"enabled": True, "query_text_max_length": 200})
        coll = client[self.db.name].test_otel
        coll.drop()
        self.exporter.clear()
        coll.insert_one({"x": "a" * 500})

        spans = self.spans("insert")
        self.assertEqual(len(spans), 1)
        query_text = spans[0].attributes["db.query.text"]
        # The oversized field value must be truncated at the field level (not
        # just a blind cut of the fully-serialized string), and the result must
        # never exceed the configured bound, even when a "..." marker is added.
        self.assertLessEqual(len(query_text), 200)
        self.assertNotIn("a" * 500, query_text)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_bulk_write_unacknowledged_gets_operation_span(self):
        client = self.rs_or_single_client(tracing={"enabled": True}, w=0)
        self.exporter.clear()
        client.bulk_write(
            [InsertOne(namespace=f"{self.db.name}.test", document={"x": 1})],
            ordered=False,
        )
        matching = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.operation.name") == "bulkWrite"
        ]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0].attributes["db.namespace"], "admin")

    def test_collection_bulk_write_unacknowledged_gets_operation_span(self):
        # The unacknowledged path skips _retryable_write, which is what creates
        # the operation span on the acknowledged path.
        client = self.rs_or_single_client(tracing={"enabled": True}, w=0)
        coll = client[self.db.name]["test"]
        self.exporter.clear()
        coll.bulk_write([InsertOne({"x": 1})], ordered=False)
        matching = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.operation.name") == "insert"
        ]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0].attributes["db.namespace"], self.db.name)
        self.assertEqual(matching[0].attributes["db.collection.name"], "test")

    def test_operation_span_falls_back_to_bare_name_when_no_command_is_sent(self):
        # Failing during server selection builds no command, so the backfill in
        # start_command_span never runs, and insert_one threads no namespace
        # eagerly. db.operation.summary (Required) falls back to the bare
        # operation name; db.namespace/db.collection.name are absent.
        client = self.rs_or_single_client(
            "mongodb://localhost:1/",
            tracing={"enabled": True},
            serverSelectionTimeoutMS=10,
            connect=False,
        )
        self.exporter.clear()
        with self.assertRaises(ServerSelectionTimeoutError):
            client.mydb.mycoll.insert_one({})
        (span,) = [s for s in self.exporter.get_finished_spans() if s.name == "insert"]
        self.assertEqual(span.attributes["db.operation.name"], "insert")
        self.assertEqual(span.attributes["db.operation.summary"], "insert")
        self.assertNotIn("db.namespace", span.attributes)
        self.assertNotIn("db.collection.name", span.attributes)
        self.assertEqual(span.status.status_code, StatusCode.ERROR)

    def test_operation_span_name_can_differ_from_command_name(self):
        # count_documents' operation span is named "count" but sends an
        # aggregate, so an operation span name is not the command beneath it.
        # count.json covers estimated_document_count, where the two coincide.
        client = self.rs_or_single_client(tracing={"enabled": True})
        db = client.pymongo_test
        db.mycoll.insert_one({"x": 1})
        self.exporter.clear()
        db.mycoll.count_documents({})

        (op_span,) = self.spans("count pymongo_test.mycoll")
        self.assertEqual(op_span.attributes["db.operation.name"], "count")
        self.assertEqual(op_span.attributes["db.namespace"], "pymongo_test")
        (cmd_span,) = self.spans("aggregate")
        self.assertEqual(cmd_span.attributes["db.command.name"], "aggregate")
        self.assertEqual(cmd_span.parent.span_id, op_span.context.span_id)

    def test_tailable_rollover_is_not_an_error_span(self):
        # A tailable cursor whose error code means "cursor closed" returns
        # normally instead of raising, so the operation span is not a failure.
        client = self.rs_or_single_client(tracing={"enabled": True})
        db = client.pymongo_test
        db.drop_collection("otel_tailable")
        db.create_collection("otel_tailable", capped=True, size=4096)
        db.otel_tailable.insert_one({"x": 1})
        self.addCleanup(db.drop_collection, "otel_tailable")

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 43},
            }
        ):
            self.exporter.clear()
            cursor = db.otel_tailable.find(cursor_type=CursorType.TAILABLE)
            self.assertEqual(cursor.to_list(), [])

        (op_span,) = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.operation.name") == "find"
        ]
        self.assertNotEqual(op_span.status.status_code, StatusCode.ERROR)
        self.assertEqual(op_span.events, ())

    def test_kill_cursors_gets_operation_span(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.kill_cursors_span
        coll.drop()
        coll.insert_many([{"i": i} for i in range(10)])
        cursor = coll.find({}, batch_size=2)
        cursor.next()
        self.exporter.clear()
        cursor.close()  # Sends killCursors, since batches remain.

        op_spans = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.operation.name") == "killCursors"
            and "db.command.name" not in s.attributes
        ]
        self.assertEqual(len(op_spans), 1, [s.name for s in self.exporter.get_finished_spans()])
        (op_span,) = op_spans
        self.assertEqual(op_span.name, "killCursors pymongo_test.kill_cursors_span")
        self.assertEqual(op_span.attributes["db.namespace"], "pymongo_test")
        self.assertEqual(op_span.attributes["db.collection.name"], "kill_cursors_span")

        cmd_spans = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.command.name") == "killCursors"
        ]
        self.assertEqual(len(cmd_spans), 1)
        self.assertEqual(cmd_spans[0].parent.span_id, op_span.context.span_id)

    def test_background_kill_cursors_span_is_a_trace_root(self):
        # create_task freezes the caller's context, so the executor has to open
        # inside coll.drop() (hence connect=False) and the tick has to run by
        # waking that task; _process_kill_cursors() here would use a clean one.
        client = self.rs_or_single_client(tracing={"enabled": True}, connect=False)
        coll = client.pymongo_test.bg_kill_cursors
        coll.drop()
        coll.insert_many([{"i": i} for i in range(10)])

        cursor = coll.find({}, batch_size=2)
        cursor.next()
        del cursor
        gc.collect()  # Queues a deferred killCursors.

        self.exporter.clear()

        def _kill_op_spans():
            return [
                s
                for s in self.exporter.get_finished_spans()
                if s.attributes.get("db.operation.name") == "killCursors"
                and "db.command.name" not in s.attributes
            ]

        executor = client._kill_cursors_executor
        executor.skip_sleep()
        executor.wake()
        wait_until(_kill_op_spans, "background killCursors span emitted")

        kill_spans = _kill_op_spans()
        self.assertEqual(len(kill_spans), 1, [s.name for s in self.exporter.get_finished_spans()])
        # The background tick must not inherit a parent from whatever span
        # happened to be current when the executor task was created.
        self.assertIsNone(kill_spans[0].parent)

    def test_end_sessions_gets_operation_span(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        client.pymongo_test.end_sessions_span.find_one({})  # Uses an implicit session.
        self.exporter.clear()
        client.close()  # Sends endSessions.

        op_spans = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.operation.name") == "endSessions"
            and "db.command.name" not in s.attributes
        ]
        self.assertEqual(len(op_spans), 1, [s.name for s in self.exporter.get_finished_spans()])
        (op_span,) = op_spans
        self.assertEqual(op_span.name, "endSessions admin")
        self.assertEqual(op_span.attributes["db.namespace"], "admin")
        self.assertNotIn("db.collection.name", op_span.attributes)

        cmd_spans = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.command.name") == "endSessions"
        ]
        self.assertEqual(len(cmd_spans), 1)
        self.assertEqual(cmd_spans[0].parent.span_id, op_span.context.span_id)


# These unit tests cover the validator's edge cases: the rejection paths and the
# explicit-zero vs unset distinction for query_text_max_length.


class TestValidateTracingOrNone(unittest.TestCase):
    def test_none(self):
        self.assertIsNone(common.validate_tracing_or_none("tracing", None))

    def test_defaults(self):
        self.assertEqual(
            common.validate_tracing_or_none("tracing", {}),
            {"enabled": False, "query_text_max_length": None},
        )

    def test_enabled_and_query_text_max_length(self):
        self.assertEqual(
            common.validate_tracing_or_none(
                "tracing", {"enabled": True, "query_text_max_length": 500}
            ),
            {"enabled": True, "query_text_max_length": 500},
        )

    def test_explicit_zero_query_text_max_length_preserved(self):
        # 0 must stay distinct from "unset" (None) so it can override the
        # environment variable instead of being treated as not configured.
        result = common.validate_tracing_or_none(
            "tracing", {"enabled": True, "query_text_max_length": 0}
        )
        self.assertEqual(result["query_text_max_length"], 0)

    def test_rejects_non_mapping(self):
        with self.assertRaises(TypeError):
            common.validate_tracing_or_none("tracing", "enabled")

    def test_rejects_unknown_option(self):
        with self.assertRaisesRegex(ConfigurationError, "Unknown tracing option"):
            common.validate_tracing_or_none("tracing", {"bogus": True})

    def test_rejects_non_boolean_enabled(self):
        with self.assertRaises(TypeError):
            common.validate_tracing_or_none("tracing", {"enabled": "yes"})

    def test_rejects_non_integer_query_text_max_length(self):
        with self.assertRaises(TypeError):
            common.validate_tracing_or_none("tracing", {"query_text_max_length": [1]})

    def test_rejects_negative_query_text_max_length(self):
        with self.assertRaises(ValueError):
            common.validate_tracing_or_none("tracing", {"query_text_max_length": -1})

    def test_coerces_numeric_string_query_text_max_length(self):
        result = common.validate_tracing_or_none("tracing", {"query_text_max_length": "100"})
        self.assertEqual(result["query_text_max_length"], 100)


class TestOTelTracerCaching(unittest.TestCase):
    """Regression test for the tracer caching in ``pymongo/_otel.py``.

    ``get_tracer()`` must be called once at import time, cached as
    ``_otel._TRACER``. Per command it would allocate, take a process-wide lock,
    and mutate the global ``warnings`` filter list even on a cache hit.
    """

    @unittest.skipUnless(_otel._HAS_OPENTELEMETRY, "opentelemetry is not installed")
    def test_start_command_span_does_not_call_get_tracer(self):
        class _FakeConn:
            id = 1
            server_connection_id: Optional[int] = None
            address: _Address = ("localhost", 27017)
            service_id = None

        with patch.object(_otel, "trace") as mock_trace:
            for _ in range(3):
                span = _otel.start_command_span(
                    {"enabled": True, "query_text_max_length": 0},
                    _FakeConn(),
                    {"ping": 1},
                    "admin",
                    "ping",
                    False,
                )
                self.assertIsNotNone(span)
                span.end()

        mock_trace.get_tracer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
