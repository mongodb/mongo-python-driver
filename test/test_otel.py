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
from pymongo._telemetry import _OperationTelemetry
from pymongo.errors import (
    ClientBulkWriteException,
    ConfigurationError,
    InvalidOperation,
    OperationFailure,
    ServerSelectionTimeoutError,
)
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

    @classmethod
    def tearDownClass(cls):
        # See the matching comment in test/synchronous/unified_format.py's
        # UnifiedSpecTestMixinV1.tearDownClass: the span processor can never
        # be removed from the shared process-wide TracerProvider, so without
        # this shutdown() the exporter keeps accumulating every span from
        # every client for the rest of the test run.
        cls.exporter.shutdown()

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

    def test_eager_dbname_and_collection_set_at_creation(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "find", None, dbname="mydb", collection="mycoll")
        self.assertIsNotNone(handle)
        _otel.end_operation_span_success(handle)
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "find mydb.mycoll")
        self.assertEqual(span.attributes["db.namespace"], "mydb")
        self.assertEqual(span.attributes["db.collection.name"], "mycoll")
        self.assertEqual(span.attributes["db.operation.summary"], "find mydb.mycoll")
        self.assertEqual(span.attributes["db.operation.name"], "find")

    def test_eager_dbname_without_collection_omits_collection_attribute(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "listCollections", None, dbname="mydb")
        _otel.end_operation_span_success(handle)
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "listCollections mydb")
        self.assertEqual(span.attributes["db.operation.summary"], "listCollections mydb")
        self.assertNotIn("db.collection.name", span.attributes)

    def test_no_eager_attributes_leaves_provisional_name(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "find", None)
        _otel.end_operation_span_success(handle)
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "find")
        self.assertNotIn("db.namespace", span.attributes)

    def test_detached_span_is_not_current_until_used(self):
        from opentelemetry import trace

        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "find", None, set_current=False)
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
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "find", None, set_current=False)
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
        # Regression test: trace.use_span's record_exception/
        # set_status_on_exception default to True, so without explicitly
        # disabling them, an exception propagating out of a `with
        # use_operation_span(handle):` block gets auto-recorded there *and
        # again* by the caller's own end_operation_span_failure -- producing
        # two identical "exception" events on the finished span.
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "find", None, set_current=False)
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
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "find", None, set_current=False)
        _otel.end_operation_span_failure(handle, ValueError("boom"))
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        exception_events = [e for e in span.events if e.name == "exception"]
        self.assertEqual(len(exception_events), 1)


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

    @classmethod
    def tearDownClass(cls):
        # See the matching comment in test/synchronous/unified_format.py's
        # UnifiedSpecTestMixinV1.tearDownClass: the span processor can never
        # be removed from the shared process-wide TracerProvider, so without
        # this shutdown() the exporter keeps accumulating every span from
        # every client for the rest of the test run.
        cls.exporter.shutdown()

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

    @classmethod
    def tearDownClass(cls):
        # See the matching comment in test/synchronous/unified_format.py's
        # UnifiedSpecTestMixinV1.tearDownClass: the span processor can never
        # be removed from the shared process-wide TracerProvider, so without
        # this shutdown() the exporter keeps accumulating every span from
        # every client for the rest of the test run.
        cls.exporter.shutdown()

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

    def test_operation_name_normalizes_enum_operation(self):
        # Regression test for PYTHON-5947 Finding #1: most _retry_internal
        # call sites pass an `_Op` enum member (a `str`-mixin enum), not a
        # plain string, as the `operation` argument. Python 3.11 changed
        # `Enum.__format__` for `str`-mixin enums so that
        # f"{_Op.INSERT}"/str(_Op.INSERT) produce "_Op.INSERT" instead of
        # "insert" -- on 3.10 the same code happened to already produce the
        # bare value, which is why this bug wasn't caught there. This test is
        # meaningful (and must pass) on every supported Python version.
        from pymongo.operations import _Op

        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        telemetry = _telemetry._OperationTelemetry(opts, _Op.INSERT, None)
        telemetry.succeeded()
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "insert")
        self.assertEqual(span.attributes["db.operation.name"], "insert")
        self.assertIs(type(span.attributes["db.operation.name"]), str)

    def test_run_command_operation_name_override(self):
        # Regression test for PYTHON-5947 Finding #2: Database.command()
        # (is_run_command=True) must produce a "runCommand" operation span,
        # per the OTel driver spec's span-name rule and db.namespace/
        # db.collection.name examples, not one named after the specific
        # command sent.
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        telemetry = _telemetry._OperationTelemetry(opts, "ping", None, is_run_command=True)
        telemetry.succeeded()
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "runCommand")
        self.assertEqual(span.attributes["db.operation.name"], "runCommand")


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOperationTelemetryContextManager(unittest.TestCase):
    """Unit tests for _OperationTelemetry's context-manager and detached modes."""

    @classmethod
    def setUpClass(cls):
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    @classmethod
    def tearDownClass(cls):
        # See the matching comment in test/synchronous/unified_format.py's
        # UnifiedSpecTestMixinV1.tearDownClass: the span processor can never
        # be removed from the shared process-wide TracerProvider, so without
        # this shutdown() the exporter keeps accumulating every span from
        # every client for the rest of the test run.
        cls.exporter.shutdown()

    def setUp(self):
        self.exporter.clear()

    def test_context_manager_success_ends_span(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        with _OperationTelemetry(opts, "killCursors", None, dbname="mydb", collection="c"):
            pass
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "killCursors mydb.c")
        self.assertEqual(span.status.status_code, StatusCode.UNSET)

    def test_context_manager_failure_records_exception(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        with self.assertRaises(ValueError):
            with _OperationTelemetry(opts, "killCursors", None, dbname="mydb"):
                raise ValueError("boom")
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(span.attributes["exception.type"], "ValueError")

    def test_context_manager_disabled_is_noop(self):
        with _OperationTelemetry(None, "killCursors", None, dbname="mydb"):
            pass
        self.assertEqual(self.exporter.get_finished_spans(), ())

    def test_detached_telemetry_use_makes_span_current(self):
        from opentelemetry import trace

        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        telemetry = _OperationTelemetry(
            opts, "find", None, dbname="mydb", collection="c", set_current=False
        )
        self.assertIsNot(trace.get_current_span(), telemetry.handle.span)
        with telemetry.use():
            self.assertIs(trace.get_current_span(), telemetry.handle.span)
        self.assertEqual(self.exporter.get_finished_spans(), ())
        telemetry.succeeded()
        self.assertEqual(len(self.exporter.get_finished_spans()), 1)


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOTelSpans(IntegrationTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    @classmethod
    def tearDownClass(cls):
        # See the matching comment in test/synchronous/unified_format.py's
        # UnifiedSpecTestMixinV1.tearDownClass: the span processor can never
        # be removed from the shared process-wide TracerProvider, so without
        # this shutdown() the exporter keeps accumulating every span from
        # every client for the rest of the test run.
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

    def test_operation_span_wraps_command_span_for_find(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test
        coll.insert_one({"x": 1})
        self.exporter.clear()
        coll.find_one({"x": 1})

        finished = self.exporter.get_finished_spans()
        # Identify the operation vs. command span by their distinguishing
        # attribute (db.operation.name / db.command.name) rather than by
        # span.name: start_command_span backfills the operation span's name
        # to a query summary (e.g. "find <db>.<coll>") once the first command
        # inside it runs, so only the command span still literally reads "find".
        matching = [s for s in finished if s.attributes.get("db.operation.name") == "find"]
        self.assertEqual(len(matching), 1)
        op_span = matching[0]
        self.assertEqual(op_span.attributes["db.namespace"], self.db.name)
        self.assertEqual(op_span.attributes["db.collection.name"], "test")
        cmd_spans = [s for s in finished if s.attributes.get("db.command.name") == "find"]
        self.assertEqual(len(cmd_spans), 1)
        cmd_span = cmd_spans[0]
        self.assertEqual(cmd_span.name, "find")
        self.assertIsNotNone(cmd_span.parent)
        self.assertEqual(cmd_span.parent.span_id, op_span.context.span_id)

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
        # The operation-span Exceptions section of the OTel spec requires the
        # same exception.type/message/stacktrace *attributes* as the command
        # span, not just the exception *event* that record_exception alone
        # attaches (PYTHON-5947 Finding #4).
        self.assertTrue(any(event.name == "exception" for event in op_span.events))
        self.assertIn("exception.type", op_span.attributes)
        self.assertIn("exception.message", op_span.attributes)
        self.assertIn("exception.stacktrace", op_span.attributes)

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

    def test_find_getmores_nest_under_one_operation_span(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.getmore_nesting
        coll.drop()
        coll.insert_many([{"i": i} for i in range(10)])
        self.exporter.clear()

        docs = coll.find({}, batch_size=2).to_list()
        self.assertEqual(len(docs), 10)

        finished = self.exporter.get_finished_spans()
        # Exactly one operation span for the whole cursor.
        find_op_spans = [
            s
            for s in finished
            if s.attributes.get("db.operation.name") == "find"
            and "db.command.name" not in s.attributes
        ]
        self.assertEqual(len(find_op_spans), 1, [s.name for s in finished])
        op_span = find_op_spans[0]
        self.assertEqual(op_span.name, "find pymongo_test.getmore_nesting")

        # No getMore *operation* spans at all.
        getmore_op_spans = [
            s
            for s in finished
            if s.attributes.get("db.operation.name") == "getMore"
            and "db.command.name" not in s.attributes
        ]
        self.assertEqual(getmore_op_spans, [])

        # Every getMore *command* span is a child of that one operation span.
        getmore_cmd_spans = [
            s for s in finished if s.attributes.get("db.command.name") == "getMore"
        ]
        self.assertGreater(len(getmore_cmd_spans), 1)
        for cmd_span in getmore_cmd_spans:
            self.assertEqual(cmd_span.parent.span_id, op_span.context.span_id)

    def test_aggregate_getmores_nest_under_one_operation_span(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.agg_nesting
        coll.drop()
        coll.insert_many([{"i": i} for i in range(10)])
        self.exporter.clear()

        docs = (coll.aggregate([{"$match": {}}], batchSize=2)).to_list()
        self.assertEqual(len(docs), 10)

        finished = self.exporter.get_finished_spans()
        agg_op_spans = [
            s
            for s in finished
            if s.attributes.get("db.operation.name") == "aggregate"
            and "db.command.name" not in s.attributes
        ]
        self.assertEqual(len(agg_op_spans), 1, [s.name for s in finished])
        op_span = agg_op_spans[0]

        getmore_op_spans = [
            s
            for s in finished
            if s.attributes.get("db.operation.name") == "getMore"
            and "db.command.name" not in s.attributes
        ]
        self.assertEqual(getmore_op_spans, [])

        getmore_cmd_spans = [
            s for s in finished if s.attributes.get("db.command.name") == "getMore"
        ]
        self.assertGreater(len(getmore_cmd_spans), 1)
        for cmd_span in getmore_cmd_spans:
            self.assertEqual(cmd_span.parent.span_id, op_span.context.span_id)

    def test_abandoned_cursor_still_ends_operation_span(self):
        import gc

        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.getmore_abandoned
        coll.drop()
        coll.insert_many([{"i": i} for i in range(10)])
        self.exporter.clear()

        cursor = coll.find({}, batch_size=2)
        cursor.next()  # Leaves the cursor open with batches pending.
        del cursor
        gc.collect()

        find_op_spans = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.operation.name") == "find"
            and "db.command.name" not in s.attributes
        ]
        self.assertEqual(len(find_op_spans), 1)

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

        # The inner command span must stay fully suppressed for sensitive commands.
        command_span_names = [s.name for s in self.spans() if "db.command.name" in s.attributes]
        self.assertNotIn("saslStart", command_span_names)

        # The sensitive command name must never leak onto the wrapping
        # *operation* span either: Database.command() always runs with
        # is_run_command=True, so the operation span is named/attributed
        # "runCommand" regardless of the actual (sensitive) command sent --
        # the bare "saslStart" name never appears anywhere. The operation
        # span must still carry its Required db.namespace/db.operation.summary
        # attributes (backfilled before start_command_span's sensitive-command
        # early return), even though the command itself produced no span.
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
        # Regression test for PYTHON-5947 Finding #2 (live): the OTel driver
        # spec names "runCommand" as the driver-operation name for any
        # operation reached via the generic Database.command() API, so
        # c.admin.command("ping") must produce an operation span named
        # "runCommand admin" with db.operation.name="runCommand" -- not one
        # named "ping"/"ping admin".
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        client.admin.command("ping")

        finished = self.exporter.get_finished_spans()
        matching = [s for s in finished if s.attributes.get("db.operation.name") == "runCommand"]
        self.assertEqual(len(matching), 1)
        op_span = matching[0]
        self.assertEqual(op_span.name, "runCommand admin")
        self.assertEqual(op_span.attributes["db.namespace"], "admin")
        # The wire-level command span is unaffected -- it's still named/attributed
        # after the actual command sent.
        cmd_spans = [s for s in finished if s.attributes.get("db.command.name") == "ping"]
        self.assertEqual(len(cmd_spans), 1)
        self.assertEqual(cmd_spans[0].name, "ping")

    def test_failure_records_exception_and_status_code(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        with self.assertRaises(OperationFailure):
            client[self.db.name].command("thisCommandDoesNotExist")

        # Operation spans wrap command spans, so this also produces an
        # ERROR-status operation span alongside the command span; narrow to
        # the command span specifically (it alone carries
        # db.response.status_code) rather than asserting there's only one span.
        spans = [s for s in self.spans() if "db.response.status_code" in s.attributes]
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

    def test_prose_1_tracing_enable_disable_via_env_var(self):
        """Prose Test 1: Tracing Enable/Disable via Environment Variable."""
        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "false"}):
            client = self.rs_or_single_client()
            self.exporter.clear()
            client.admin.command("ping")
        # Disabled must suppress both the operation span and the command span
        # it wraps -- db.command() routes through _retry_internal same as any
        # CRUD call, so both would exist if tracing weren't fully off.
        self.assertEqual(self.spans(), [])

        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true"}):
            client = self.rs_or_single_client()
            self.exporter.clear()
            client.admin.command("ping")
        finished = self.exporter.get_finished_spans()
        # Disambiguate the command span (db.command.name) from the operation
        # span (db.operation.name) that wraps it -- start_command_span renames
        # the operation span in place once the command runs, so span.name
        # alone can't tell them apart, but these attributes can. The operation
        # span reads "runCommand" (not "ping"): Database.command() always runs
        # with is_run_command=True, per the OTel spec's runCommand naming rule.
        self.assertIn("ping", [s.attributes.get("db.command.name") for s in finished])
        self.assertIn("runCommand", [s.attributes.get("db.operation.name") for s in finished])

    def test_prose_2_command_payload_emission_via_env_var(self):
        """Prose Test 2: Command Payload Emission via Environment Variable."""

        def command_spans():
            # self.spans("find") would also match the outer find *operation*
            # span (renamed to a "find <db>.<coll>" summary once the command
            # runs, not literally "find"); filter on db.command.name instead
            # to isolate the inner command span that carries db.query.text.
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

    @client_context.require_transactions
    def test_transaction_span_parents_operation_and_command_spans(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test
        coll.insert_one({"x": 1})
        self.exporter.clear()

        with client.start_session() as session:
            with session.start_transaction():
                coll.insert_one({"x": 2}, session=session)
                coll.insert_one({"x": 3}, session=session)

        finished = self.exporter.get_finished_spans()
        txn_span = next(s for s in finished if s.name == "transaction")
        self.assertEqual(dict(txn_span.attributes), {"db.system.name": "mongodb"})

        insert_op_spans = [s for s in finished if s.attributes.get("db.operation.name") == "insert"]
        self.assertEqual(len(insert_op_spans), 2)
        for op_span in insert_op_spans:
            self.assertEqual(op_span.parent.span_id, txn_span.context.span_id)

        commit_op_spans = [
            s for s in finished if s.attributes.get("db.operation.name") == "commitTransaction"
        ]
        self.assertEqual(len(commit_op_spans), 1)
        self.assertEqual(commit_op_spans[0].parent.span_id, txn_span.context.span_id)

    @client_context.require_transactions
    def test_aborted_transaction_still_ends_span(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test
        self.exporter.clear()

        with client.start_session() as session:
            with session.start_transaction():
                coll.insert_one({"x": 4}, session=session)
                session.abort_transaction()

        finished = self.exporter.get_finished_spans()
        txn_span = next(s for s in finished if s.name == "transaction")
        self.assertTrue(txn_span.end_time is not None)

    @client_context.require_transactions
    def test_committing_empty_transaction_ends_span(self):
        # No operation is ever run against the server, so commit_transaction
        # takes the STARTING/COMMITTED_EMPTY early-return path rather than
        # actually sending a commitTransaction command.
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()

        with client.start_session() as session:
            session.start_transaction()
            session.commit_transaction()

        finished = self.exporter.get_finished_spans()
        txn_span = next(s for s in finished if s.name == "transaction")
        self.assertTrue(txn_span.end_time is not None)

    @client_context.require_transactions
    def test_aborting_empty_transaction_ends_span(self):
        # No operation is ever run against the server, so abort_transaction
        # takes the STARTING early-return path rather than actually sending
        # an abortTransaction command.
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()

        with client.start_session() as session:
            session.start_transaction()
            session.abort_transaction()

        finished = self.exporter.get_finished_spans()
        txn_span = next(s for s in finished if s.name == "transaction")
        self.assertTrue(txn_span.end_time is not None)

    @client_context.require_transactions
    def test_direct_commit_retry_gives_each_span_its_own_end(self):
        # Explicitly retrying a successful commit moves the transaction state
        # COMMITTED -> IN_PROGRESS -> (back through the try/finally) ->
        # COMMITTED again. The prior attempt's span was already ended and
        # cleared, so the retry gets a fresh "transaction" span of its own
        # (this is the direct-API path, not with_transaction -- see
        # test_with_transaction_retry_reuses_one_transaction_span for the
        # with_transaction case, which shares a single span across retries
        # instead); each span's ending finally block must run exactly once
        # for its own span, never double-ending the same span and never
        # leaving one unended.
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test
        self.exporter.clear()

        with client.start_session() as session:
            with session.start_transaction():
                coll.insert_one({"x": 5}, session=session)
            # The transaction context manager already committed on clean
            # exit; retry the commit explicitly.
            session.commit_transaction()

        finished = self.exporter.get_finished_spans()
        txn_spans = [s for s in finished if s.name == "transaction"]
        self.assertEqual(len(txn_spans), 2)
        self.assertNotEqual(txn_spans[0].context.span_id, txn_spans[1].context.span_id)
        for txn_span in txn_spans:
            self.assertTrue(txn_span.end_time is not None)

    @client_context.require_transactions
    def test_with_transaction_retry_reuses_one_transaction_span(self):
        # A retried with_transaction() call must still produce exactly one
        # "transaction" span for the whole logical call -- not one sibling
        # span per full-transaction retry, and no separately-named wrapper
        # span either (the vendored transaction/convenient.json fixture
        # pins "transaction" itself as the trace root for withTransaction).
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.with_txn_spans
        coll.drop()
        client.pymongo_test.create_collection("with_txn_spans")

        attempts = []

        def callback(session):
            attempts.append(1)
            coll.insert_one({"n": len(attempts)}, session=session)
            if len(attempts) == 1:
                exc = OperationFailure("transient", 251)
                exc._add_error_label("TransientTransactionError")
                raise exc

        self.exporter.clear()
        with client.start_session() as session:
            session.with_transaction(callback)

        self.assertEqual(len(attempts), 2)
        finished = self.exporter.get_finished_spans()
        self.assertFalse(
            [s.name for s in finished if s.name.startswith("withTransaction")],
            [s.name for s in finished],
        )

        txn_spans = [s for s in finished if s.name == "transaction"]
        self.assertEqual(len(txn_spans), 1, [s.name for s in finished])
        self.assertTrue(txn_spans[0].end_time is not None)

        insert_op_spans = [s for s in finished if s.attributes.get("db.operation.name") == "insert"]
        self.assertEqual(len(insert_op_spans), 2)
        for op_span in insert_op_spans:
            self.assertEqual(op_span.parent.span_id, txn_spans[0].context.span_id)

    @client_context.require_transactions
    def test_reentrant_with_transaction_raises_and_does_not_leak_span(self):
        # A callback that illegally re-enters with_transaction() on the same
        # session must be rejected with a clear InvalidOperation, and the
        # outer call's "transaction" span must still end exactly once --
        # never leaked (created but never ended) and never double-ended.
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.reentrant_with_txn
        coll.drop()
        client.pymongo_test.create_collection("reentrant_with_txn")

        def inner_callback(session):
            coll.insert_one({"x": 1}, session=session)

        def outer_callback(session):
            coll.insert_one({"x": 2}, session=session)
            # Illegal: with_transaction() is not reentrant on one session.
            session.with_transaction(inner_callback)

        self.exporter.clear()
        with client.start_session() as session:
            with self.assertRaises(InvalidOperation):
                session.with_transaction(outer_callback)

        finished = self.exporter.get_finished_spans()
        txn_spans = [s for s in finished if s.name == "transaction"]
        # Only the outer call ever gets far enough to create a span -- the
        # guard rejects the inner call before it creates one of its own.
        self.assertEqual(len(txn_spans), 1, [s.name for s in finished])
        for txn_span in txn_spans:
            self.assertIsNotNone(txn_span.end_time)

    @client_context.require_transactions
    def test_with_transaction_while_direct_api_transaction_active_does_not_corrupt_span(
        self,
    ):
        # Calling with_transaction() while a transaction started via the
        # DIRECT API is already active on the same session is illegal --
        # start_transaction() inside with_transaction() raises "Transaction
        # already in progress" -- but the direct-API transaction's own
        # "transaction" span must survive that failure: with_transaction()'s
        # finally must not end/null it out from under the still-active
        # transaction (Important #1). Operations run on the session
        # afterwards must still parent to that span rather than becoming
        # trace roots, and the failed call must not leave behind a second,
        # spurious "transaction" span of its own.
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.direct_api_with_txn_conflict
        coll.drop()
        client.pymongo_test.create_collection("direct_api_with_txn_conflict")

        def callback(session):
            raise AssertionError("never reached -- start_transaction() raises first")

        self.exporter.clear()
        with client.start_session() as session:
            session.start_transaction()
            coll.insert_one({"x": 1}, session=session)

            with self.assertRaises(InvalidOperation):
                session.with_transaction(callback)

            # The original transaction is still active; this must still
            # nest under its span, not become a trace root.
            coll.insert_one({"x": 2}, session=session)
            session.commit_transaction()

        finished = self.exporter.get_finished_spans()
        txn_spans = [s for s in finished if s.name == "transaction"]
        self.assertEqual(len(txn_spans), 1, [s.name for s in finished])
        txn_span = txn_spans[0]
        self.assertIsNotNone(txn_span.end_time)

        insert_op_spans = [s for s in finished if s.attributes.get("db.operation.name") == "insert"]
        self.assertEqual(len(insert_op_spans), 2)
        for op_span in insert_op_spans:
            self.assertEqual(op_span.parent.span_id, txn_span.context.span_id)

    @client_context.require_transactions
    def test_retried_commit_has_a_transaction_span(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.retried_commit_spans
        coll.drop()
        client.pymongo_test.create_collection("retried_commit_spans")

        with client.start_session() as session:
            session.start_transaction()
            coll.insert_one({"x": 1}, session=session)
            session.commit_transaction()
            self.exporter.clear()
            # An explicit second commit re-enters the COMMITTED -> IN_PROGRESS
            # branch, which previously ran with no transaction span at all.
            session.commit_transaction()

        finished = self.exporter.get_finished_spans()
        txn_spans = [s for s in finished if s.name == "transaction"]
        self.assertEqual(len(txn_spans), 1, [s.name for s in finished])
        commit_cmd_spans = [
            s for s in finished if s.attributes.get("db.command.name") == "commitTransaction"
        ]
        self.assertGreaterEqual(len(commit_cmd_spans), 1)
        for cmd_span in commit_cmd_spans:
            self.assertIsNotNone(cmd_span.parent)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_bulk_write_acknowledged_gets_operation_span(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        client.bulk_write([InsertOne(namespace=f"{self.db.name}.test", document={"x": 1})])
        matching = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.operation.name") == "bulkWrite"
        ]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0].attributes["db.namespace"], "admin")
        self.assertNotIn("db.collection.name", matching[0].attributes)

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

    @client_context.require_version_min(8, 0)
    def test_client_bulk_write_results_cursor_getmores_nest_under_bulk_write(self):
        # A successful InsertOne's verbose result doc is tiny (~{"ok": 1, "idx":
        # i, "n": 1}) regardless of the inserted document's size, and the driver
        # never sends more than maxWriteBatchSize (100_000 by default) ops in one
        # bulkWrite command -- so plain successful inserts can never make the
        # results cursor's first batch exceed the 16MB per-batch limit, no
        # matter how many operations are given. Duplicate-key write errors,
        # whose result docs embed the offending key (here padded to 3000 bytes),
        # blow past that limit at a much smaller, fast-running operation count
        # while still exercising the exact same code path (a real
        # CommandCursor built and iterated by _process_results_cursor).
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.bulk_results_cursor
        coll.drop()
        coll.create_index("dup", unique=True)
        dup_value = "d" * 3000
        models = [
            InsertOne(namespace=coll.full_name, document={"dup": dup_value}) for _ in range(10000)
        ]
        self.exporter.clear()
        with self.assertRaises(ClientBulkWriteException):
            client.bulk_write(models, verbose_results=True, ordered=False)

        finished = self.exporter.get_finished_spans()
        # Exactly one operation span, for the bulkWrite itself.
        op_spans = [
            s
            for s in finished
            if "db.command.name" not in s.attributes
            and s.attributes.get("db.operation.name") is not None
        ]
        self.assertEqual(
            [s.attributes["db.operation.name"] for s in op_spans],
            ["bulkWrite"],
            [s.name for s in finished],
        )
        (op_span,) = op_spans

        # Any getMore command spans parent directly to the bulkWrite span.
        getmore_cmd_spans = [
            s for s in finished if s.attributes.get("db.command.name") == "getMore"
        ]
        self.assertGreater(len(getmore_cmd_spans), 0, "expected a multi-batch results cursor")
        for cmd_span in getmore_cmd_spans:
            self.assertEqual(cmd_span.parent.span_id, op_span.context.span_id)

    def test_operation_span_has_namespace_when_no_command_is_sent(self):
        # An operation that fails during server selection never builds a
        # command, so the lazy backfill never runs -- the eagerly-set
        # namespace/summary attributes are the only ones it will ever have.
        client = self.rs_or_single_client(
            "mongodb://localhost:1/",
            tracing={"enabled": True},
            serverSelectionTimeoutMS=10,
            connect=False,
        )
        self.exporter.clear()
        with self.assertRaises(ServerSelectionTimeoutError):
            client.mydb.mycoll.find_one({})
        (span,) = [s for s in self.exporter.get_finished_spans() if s.name.startswith("find")]
        self.assertEqual(span.name, "find mydb.mycoll")
        self.assertEqual(span.attributes["db.namespace"], "mydb")
        self.assertEqual(span.attributes["db.collection.name"], "mycoll")
        self.assertEqual(span.attributes["db.operation.summary"], "find mydb.mycoll")
        self.assertEqual(span.status.status_code, StatusCode.ERROR)

    def test_caller_owned_operation_telemetry_is_not_ended_by_retry_internal(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        telemetry = _OperationTelemetry(
            client.options.tracing,
            "find",
            None,
            dbname="mydb",
            collection="c",
            set_current=False,
        )
        self.exporter.clear()

        def _noop_read(_session, _server, _conn, _read_pref):
            return "ok"

        result = client._retryable_read(
            _noop_read,
            ReadPreference.PRIMARY,
            None,
            operation="find",
            operation_telemetry=telemetry,
        )
        self.assertEqual(result, "ok")
        # _retry_internal must not have ended the caller's span.
        self.assertEqual(
            [s for s in self.exporter.get_finished_spans() if s.name.startswith("find")], []
        )
        telemetry.succeeded()
        self.assertEqual(
            len([s for s in self.exporter.get_finished_spans() if s.name.startswith("find")]),
            1,
        )

    def test_eager_namespace_for_collection_and_database_operations(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        db = client.pymongo_test
        cases = [
            # (coroutine factory, expected span name)
            (lambda: db.mycoll.insert_one({"x": 1}), "insert pymongo_test.mycoll"),
            (lambda: db.mycoll.find_one({}), "find pymongo_test.mycoll"),
            (lambda: db.mycoll.count_documents({}), "count pymongo_test.mycoll"),
            (lambda: db.list_collection_names(), "listCollections pymongo_test"),
        ]
        for factory, expected_name in cases:
            with self.subTest(expected_name=expected_name):
                self.exporter.clear()
                factory()
                names = [s.name for s in self.exporter.get_finished_spans()]
                self.assertIn(expected_name, names)
                (span,) = [s for s in self.exporter.get_finished_spans() if s.name == expected_name]
                self.assertEqual(span.attributes["db.operation.summary"], expected_name)
                self.assertEqual(span.attributes["db.namespace"], "pymongo_test")

    def _aggregate_operation_span(self):
        matching = [
            s
            for s in self.exporter.get_finished_spans()
            if s.attributes.get("db.operation.name") == "aggregate"
        ]
        self.assertEqual(len(matching), 1)
        return matching[0]

    @client_context.require_version_min(4, 2, 0)
    @client_context.require_change_streams
    def test_change_stream_collection_level_operation_span_has_full_namespace(self):
        # ChangeStream._target_namespace must recognize a Collection
        # target via isinstance, not via attribute-probing: Database's
        # __getattr__ synthesizes a collection for any unknown attribute name
        # (including "database"), so a naive getattr(target, "database", None)
        # probe misidentifies a database/cluster target as a collection.
        client = self.rs_or_single_client(tracing={"enabled": True})
        db = client.pymongo_test
        coll = db.test_otel_change_stream_coll
        coll.drop()
        self.exporter.clear()
        with coll.watch():
            pass
        span = self._aggregate_operation_span()
        self.assertEqual(span.attributes["db.namespace"], "pymongo_test")
        self.assertEqual(span.attributes["db.collection.name"], "test_otel_change_stream_coll")

    @client_context.require_version_min(4, 2, 0)
    @client_context.require_change_streams
    def test_change_stream_database_level_operation_span_omits_collection_name(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        db = client.pymongo_test
        self.exporter.clear()
        with db.watch():
            pass
        span = self._aggregate_operation_span()
        self.assertEqual(span.attributes["db.namespace"], "pymongo_test")
        self.assertNotIn("db.collection.name", span.attributes)

    @client_context.require_version_min(4, 2, 0)
    @client_context.require_change_streams
    def test_change_stream_cluster_level_operation_span_targets_admin(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        with client.watch():
            pass
        span = self._aggregate_operation_span()
        self.assertEqual(span.attributes["db.namespace"], "admin")
        self.assertNotIn("db.collection.name", span.attributes)

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
        # Regression test for PYTHON-5947: asyncio.create_task freezes the
        # calling coroutine's contextvars.Context, and the kill-cursors
        # executor is opened lazily from inside the client's first traced
        # operation (_get_topology -> executor.open() -> create_task, reached
        # while that operation's _OperationTelemetry has already made its
        # span ambient-current). Without resetting the OTel context inside
        # PeriodicExecutor._run, every killCursors span the background
        # tick emits for the rest of the process's life gets parented under
        # that first, long-since-ended operation and shares its trace id.
        #
        # Calling client._process_kill_cursors() directly from this test
        # coroutine would NOT reproduce the bug: this coroutine's own
        # context is clean, so the span would come out parentless even with
        # the bug present. Instead we drive the *existing* kill-cursors
        # executor task -- the one whose context was frozen inside
        # find_one() below -- via wake()/skip_sleep(), so the tick actually
        # runs inside that frozen context, and poll (wait_until) for
        # the resulting span rather than sleeping a fixed amount.
        import gc

        # connect=False is essential here: the test helper's default
        # connect=True calls client._connect() -> _get_topology() right after
        # construction, *before* any traced operation runs and thus with no
        # span current -- which would open (and freeze) the kill-cursors
        # executor with a clean context and make this test pass regardless of
        # the bug. With connect=False, _get_topology() (and therefore the
        # executor's create_task) is only reached lazily, from inside the
        # find_one() call below, while that operation's span is current.
        client = self.rs_or_single_client(tracing={"enabled": True}, connect=False)
        coll = client.pymongo_test.bg_kill_cursors
        coll.drop()
        coll.insert_many([{"i": i} for i in range(10)])

        # This first traced operation is what opens the kill-cursors executor,
        # freezing its context while this operation's span is current.
        coll.find_one({})

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


# The unified test format's expectTracingMessages/observeTracingMessages
# tests (test_open_telemetry_unified.py) now exercise this validator
# indirectly through real client construction, but these direct unit tests
# are kept for the validator's edge cases (rejection paths, the explicit-zero
# vs. unset distinction for query_text_max_length) that aren't necessarily
# covered by the vendored fixtures.
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
