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

"""Test OpenTelemetry command-span support."""

from __future__ import annotations

import os
import sys
from typing import Optional
from unittest.mock import patch

sys.path[0:0] = [""]

import pytest

import pymongo._otel as _otel
from pymongo import _telemetry, common
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.typings import _Address
from test import IntegrationTest, unittest

_HAS_OTEL_TEST_DEPS = False
if _otel._HAS_OPENTELEMETRY:
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
        from opentelemetry.trace import StatusCode

        _HAS_OTEL_TEST_DEPS = True
    except ImportError:
        pass

_IS_SYNC = True

pytestmark = pytest.mark.otel


def _shared_test_provider() -> TracerProvider:
    """Return a process-wide SDK TracerProvider for tests to attach exporters to.

    ``trace.set_tracer_provider`` only takes effect once per process (later calls
    are silently ignored), so tests must share one provider and each register
    their own span processor rather than trying to install a fresh provider.
    """
    current = trace.get_tracer_provider()
    if isinstance(current, TracerProvider):
        return current
    provider = TracerProvider()
    trace.set_tracer_provider(provider)
    return provider


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOTelOperationSpanPrimitives(unittest.TestCase):
    """Unit tests for the pymongo._otel operation-span primitives (no live server needed)."""

    @classmethod
    def setUpClass(cls):
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    def setUp(self):
        self.exporter.clear()

    def test_start_operation_span_disabled_returns_none(self):
        handle = _otel.start_operation_span(None, "find", None)
        self.assertIsNone(handle)

    def test_start_operation_span_success_sets_provisional_attributes(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "find", None)
        self.assertIsNotNone(handle)
        _otel.end_operation_span_success(handle)
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "find")
        self.assertEqual(span.attributes["db.system.name"], "mongodb")
        self.assertEqual(span.attributes["db.operation.name"], "find")
        self.assertEqual(span.status.status_code, StatusCode.UNSET)

    def test_start_operation_span_failure_records_exception(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "insert", None)
        _otel.end_operation_span_failure(handle, ValueError("boom"))
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(len(span.events), 1)
        self.assertEqual(span.events[0].name, "exception")

    def test_start_operation_span_with_parent(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        parent_handle = _otel.start_operation_span(opts, "transaction", None)
        handle = _otel.start_operation_span(opts, "insert", parent_handle.span)
        _otel.end_operation_span_success(handle)
        _otel.end_operation_span_success(parent_handle)
        child, parent = self.exporter.get_finished_spans()
        self.assertEqual(child.parent.span_id, parent.context.span_id)

    def test_current_operation_name_contextvar_scoped_correctly(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        self.assertIsNone(_otel._CURRENT_OPERATION_NAME.get())
        handle = _otel.start_operation_span(opts, "find", None)
        self.assertEqual(_otel._CURRENT_OPERATION_NAME.get(), "find")
        _otel.end_operation_span_success(handle)
        self.assertIsNone(_otel._CURRENT_OPERATION_NAME.get())


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOTelTransactionSpanPrimitives(unittest.TestCase):
    """Unit tests for the pymongo._otel transaction-span primitives (no live server needed)."""

    @classmethod
    def setUpClass(cls):
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    def setUp(self):
        self.exporter.clear()

    def test_start_transaction_span_disabled_returns_none(self):
        self.assertIsNone(_otel.start_transaction_span(None))

    def test_start_transaction_span_has_only_one_attribute(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        span = _otel.start_transaction_span(opts)
        _otel.end_transaction_span(span)
        (finished,) = self.exporter.get_finished_spans()
        self.assertEqual(finished.name, "transaction")
        self.assertEqual(dict(finished.attributes), {"db.system.name": "mongodb"})

    def test_end_transaction_span_is_none_safe(self):
        _otel.end_transaction_span(None)  # must not raise


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOperationTelemetry(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    def setUp(self):
        self.exporter.clear()

    def test_succeeded_with_no_session(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        telemetry = _telemetry._OperationTelemetry(opts, "find", None)
        telemetry.succeeded()
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "find")
        self.assertIsNone(span.parent)

    def test_failed_records_exception(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        telemetry = _telemetry._OperationTelemetry(opts, "insert", None)
        telemetry.failed(RuntimeError("nope"))
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)

    def test_nests_under_active_transaction_span(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        txn_span = _otel.start_transaction_span(opts)

        class _FakeTransaction:
            span = txn_span

        class _FakeSession:
            in_transaction = True
            _transaction = _FakeTransaction()

        telemetry = _telemetry._OperationTelemetry(opts, "insert", _FakeSession())
        telemetry.succeeded()
        _otel.end_transaction_span(txn_span)
        child, parent = self.exporter.get_finished_spans()
        self.assertEqual(child.parent.span_id, parent.context.span_id)

    def test_disabled_is_a_no_op(self):
        telemetry = _telemetry._OperationTelemetry(None, "find", None)
        telemetry.succeeded()  # must not raise
        telemetry2 = _telemetry._OperationTelemetry(None, "find", None)
        telemetry2.failed(RuntimeError("x"))  # must not raise
        self.assertEqual(self.exporter.get_finished_spans(), ())


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOTelSpans(IntegrationTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    def setUp(self):
        super().setUp()
        self.exporter.clear()

    def spans(self, name: str | None = None):
        finished = self.exporter.get_finished_spans()
        if name is None:
            return list(finished)
        return [s for s in finished if s.name == name]

    # TODO(PYTHON-5947): once the unified test format runner supports
    # expectTracingMessages/operation spans, this is superseded by the spec's
    # find_without_query_text.yml and insert.yml.
    def test_span_created_for_insert_and_find(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test_otel
        coll.drop()
        self.exporter.clear()
        coll.insert_one({"x": 1})

        insert_spans = self.spans("insert")
        self.assertEqual(len(insert_spans), 1)
        attrs = insert_spans[0].attributes
        self.assertEqual(attrs["db.system.name"], "mongodb")
        self.assertEqual(attrs["db.namespace"], self.db.name)
        self.assertEqual(attrs["db.collection.name"], "test_otel")
        self.assertEqual(attrs["db.command.name"], "insert")
        self.assertEqual(attrs["db.query.summary"], f"insert {self.db.name}.test_otel")
        self.assertIn("server.address", attrs)
        self.assertIn("server.port", attrs)
        self.assertIn(attrs["network.transport"], ("tcp", "unix"))
        self.assertIn("db.mongodb.driver_connection_id", attrs)
        self.assertNotIn("db.query.text", attrs)

        self.exporter.clear()
        docs = coll.find({}).to_list()
        self.assertEqual(len(docs), 1)
        find_spans = self.spans("find")
        self.assertEqual(len(find_spans), 1)
        self.assertEqual(find_spans[0].attributes["db.command.name"], "find")

    def test_span_created_for_get_more(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test_otel_getmore
        coll.drop()
        coll.insert_many([{"x": i} for i in range(5)])
        self.exporter.clear()

        docs = coll.find({}, batch_size=2).to_list()
        self.assertEqual(len(docs), 5)

        get_more_spans = self.spans("getMore")
        self.assertGreater(len(get_more_spans), 0)
        for span in get_more_spans:
            self.assertEqual(span.attributes["db.collection.name"], "test_otel_getmore")
            self.assertEqual(span.attributes["db.command.name"], "getMore")

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
            {"enabled": True, "query_text_max_length": None},
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

        names = [s.name for s in self.spans()]
        self.assertNotIn("saslStart", names)

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

    def test_failure_records_exception_and_status_code(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        with self.assertRaises(OperationFailure):
            client[self.db.name].command("thisCommandDoesNotExist")

        spans = self.spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertEqual(span.status.status_code, trace.StatusCode.ERROR)
        self.assertIn("db.response.status_code", span.attributes)
        self.assertTrue(any(event.name == "exception" for event in span.events))

    def test_tracing_disabled_by_default(self):
        client = self.rs_or_single_client()
        self.exporter.clear()
        client.admin.command("ping")
        self.assertEqual(self.spans(), [])

    # TODO(PYTHON-5947): once operation spans exist, also assert that the
    # "ping" *operation* span (not just the command span) is absent/present
    # here, and that self.spans() counts both.
    def test_prose_1_tracing_enable_disable_via_env_var(self):
        """Prose Test 1: Tracing Enable/Disable via Environment Variable."""
        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "false"}):
            client = self.rs_or_single_client()
            self.exporter.clear()
            client.admin.command("ping")
        self.assertEqual(self.spans(), [])

        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true"}):
            client = self.rs_or_single_client()
            self.exporter.clear()
            client.admin.command("ping")
        self.assertIn("ping", [s.name for s in self.spans()])

    # TODO(PYTHON-5947): once operation spans exist, self.spans("find") will
    # also match the outer find *operation* span; disambiguate (e.g. by
    # db.command.name vs db.operation.name) so this only asserts on the
    # command span's db.query.text attribute.
    def test_prose_2_command_payload_emission_via_env_var(self):
        """Prose Test 2: Command Payload Emission via Environment Variable."""
        env = {
            "OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true",
            "OTEL_PYTHON_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH": "1024",
        }
        with patch.dict(os.environ, env):
            client = self.rs_or_single_client()
            self.exporter.clear()
            client[self.db.name].test_otel.find({}).to_list()
        spans = self.spans("find")
        self.assertEqual(len(spans), 1)
        self.assertIn("db.query.text", spans[0].attributes)

        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true"}):
            client = self.rs_or_single_client()
            self.exporter.clear()
            client[self.db.name].test_otel.find({}).to_list()
        spans = self.spans("find")
        self.assertEqual(len(spans), 1)
        self.assertNotIn("db.query.text", spans[0].attributes)

    # TODO(PYTHON-5947): once the unified test format runner supports
    # expectTracingMessages/operation spans, this is superseded by the spec's
    # find.yml (db.query.text assertion).
    def test_query_text_included_when_configured(self):
        client = self.rs_or_single_client(tracing={"enabled": True, "query_text_max_length": 1000})
        coll = client[self.db.name].test_otel
        coll.drop()
        self.exporter.clear()
        coll.insert_one({"x": 1})

        spans = self.spans("insert")
        self.assertEqual(len(spans), 1)
        self.assertIn("db.query.text", spans[0].attributes)
        self.assertNotIn("lsid", spans[0].attributes["db.query.text"])

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


# TODO(PYTHON-5947): superseded once the unified test format's
# expectTracingMessages/observeTracingMessages tests exercise this validator
# indirectly through real client construction; remove this class then.
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


class TestOTelTracerCaching(unittest.TestCase):
    """Regression test for the tracer-caching implementation in ``pymongo/_otel.py``.

    ``opentelemetry.trace.get_tracer()`` must only be called once, at import
    time (cached as module-level ``_otel._TRACER``). Calling it per command
    allocates two objects, takes a process-wide lock, and mutates the global
    ``warnings`` filter list on every call, even on a cache hit.
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
                    {"enabled": True, "query_text_max_length": None},
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
