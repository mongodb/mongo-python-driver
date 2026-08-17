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

"""Test the OpenTelemetry transaction pseudo-span."""

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


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOTelTransactionSpanPrimitives(unittest.TestCase):
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

    def test_start_transaction_span_has_only_one_attribute(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        span = _otel.start_transaction_span(opts)
        _otel.end_transaction_span(span)
        (finished,) = self.exporter.get_finished_spans()
        self.assertEqual(finished.name, "transaction")
        self.assertEqual(dict(finished.attributes), {"db.system.name": "mongodb"})


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOperationTelemetryInTransaction(unittest.TestCase):
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


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOTelTransactionSpans(IntegrationTest):
    """Transaction spans and the operations nested under them."""

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

        For the tests that assert tracing produced *nothing*. Asserting the
        exporter is empty would also catch spans no test asked for: a cursor
        abandoned earlier in the class ends its operation span from a
        finalizer, and on an interpreter that does not reference count, that
        finalizer runs at an unpredictable point and lands in whichever test
        happens to be running. Naming the ping's own spans keeps the assertion
        about this client while staying immune to that.
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
        # (this is the direct-API path, not with_transaction; see
        # test_with_transaction_retry_reuses_one_transaction_span for the
        # with_transaction case, which shares a single span across retries
        # instead); each span's ending finally block must run exactly once
        # for its own span, never double-ending the same span and never
        # leaving one unended.
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test
        coll.drop()
        client[self.db.name].create_collection("test")
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
        # "transaction" span for the whole logical call, not one sibling
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
        # outer call's "transaction" span must still end exactly once,
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
        # Only the outer call ever gets far enough to create a span; the
        # guard rejects the inner call before it creates one of its own.
        self.assertEqual(len(txn_spans), 1, [s.name for s in finished])
        for txn_span in txn_spans:
            self.assertIsNotNone(txn_span.end_time)

    @client_context.require_transactions
    def test_nested_with_transaction_on_another_session_keeps_spans_separate(self):
        # Nesting with_transaction() is legal on a *different* session, unlike
        # the same-session case above. Each session's operations must parent to
        # its own transaction span, which holds because an operation span takes
        # its parent explicitly from session._transaction.span instead of from
        # ambient context.
        client = self.rs_or_single_client(tracing={"enabled": True})
        db = client.pymongo_test
        outer_coll = db.two_session_outer
        inner_coll = db.two_session_inner
        # Create both up front: creating a collection inside a transaction is
        # illegal before server 4.4.
        outer_coll.drop()
        inner_coll.drop()
        db.create_collection("two_session_outer")
        db.create_collection("two_session_inner")

        def inner_callback(inner_session):
            inner_coll.insert_one({"x": 1}, session=inner_session)

        def outer_callback(outer_session):
            outer_coll.insert_one({"x": 1}, session=outer_session)
            with client.start_session() as inner_session:
                inner_session.with_transaction(inner_callback)

        self.exporter.clear()
        with client.start_session() as outer_session:
            outer_session.with_transaction(outer_callback)

        finished = self.exporter.get_finished_spans()
        txn_spans = [s for s in finished if s.name == "transaction"]
        self.assertEqual(len(txn_spans), 2, [s.name for s in finished])
        for txn_span in txn_spans:
            self.assertIsNotNone(txn_span.end_time)
            # Transaction spans are never made current, so neither ends up
            # nested under the other.
            self.assertIsNone(txn_span.parent)

        def insert_parent_id(collname: str) -> int:
            (span,) = [
                s
                for s in finished
                if s.attributes.get("db.operation.name") == "insert"
                and s.attributes.get("db.collection.name") == collname
            ]
            return span.parent.span_id

        outer_parent = insert_parent_id("two_session_outer")
        inner_parent = insert_parent_id("two_session_inner")
        self.assertNotEqual(outer_parent, inner_parent)
        self.assertEqual({outer_parent, inner_parent}, {s.context.span_id for s in txn_spans})

    @client_context.require_transactions
    def test_with_transaction_while_direct_api_transaction_active_does_not_corrupt_span(
        self,
    ):
        # Calling with_transaction() while a transaction started with the
        # DIRECT API is already active on the same session is illegal:
        # start_transaction() inside with_transaction() raises "Transaction
        # already in progress", but the direct-API transaction's own
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
            raise AssertionError("never reached; start_transaction() raises first")

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
