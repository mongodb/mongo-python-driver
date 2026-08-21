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


def _tracing_opts() -> _otel.TracingOptions:
    """Return resolved tracing options with tracing on and ``db.query.text`` disabled."""
    return {"enabled": True, "query_text_max_length": 0}


@unittest.skipUnless(_HAS_OTEL_TEST_DEPS, "opentelemetry-sdk is not installed")
class TestOTelTransactionSpanPrimitives(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    @classmethod
    def tearDownClass(cls):
        # The span processor can never be removed from the shared process-wide
        # TracerProvider, so without this the exporter accumulates every span.
        cls.exporter.shutdown()

    def setUp(self):
        self.exporter.clear()

    def test_start_transaction_span_has_only_one_attribute(self):
        opts = _tracing_opts()
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
        # TracerProvider, so without this the exporter accumulates every span.
        cls.exporter.shutdown()

    def setUp(self):
        self.exporter.clear()

    def test_nests_under_active_transaction_span(self):
        opts = _tracing_opts()
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
        # The span processor can never be removed from the shared process-wide
        # TracerProvider, so without this the exporter accumulates every span.
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

        Only command spans carry db.command.name, so its absence separates them.
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

        For tests asserting tracing produced nothing. An empty-exporter assertion
        would also catch spans a finalizer flushes at an unpredictable point on
        interpreters without reference counting.
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
        # An explicit commit retry moves the state from COMMITTED back to
        # IN_PROGRESS, and the prior attempt's span was already ended and
        # cleared, so the retry must get a fresh span and end it exactly once.
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test
        coll.drop()
        client[self.db.name].create_collection("test")
        self.exporter.clear()

        with client.start_session() as session:
            with session.start_transaction():
                coll.insert_one({"x": 5}, session=session)
            # The context manager already committed on clean exit.
            session.commit_transaction()

        finished = self.exporter.get_finished_spans()
        txn_spans = [s for s in finished if s.name == "transaction"]
        self.assertEqual(len(txn_spans), 2)
        self.assertNotEqual(txn_spans[0].context.span_id, txn_spans[1].context.span_id)
        for txn_span in txn_spans:
            self.assertTrue(txn_span.end_time is not None)

    @client_context.require_transactions
    def test_with_transaction_retry_reuses_one_transaction_span(self):
        # A retried with_transaction() must produce exactly one "transaction"
        # span for the whole call, not one per retry and no wrapper span.
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
        # Re-entering with_transaction() on the same session must raise, and the
        # outer call's span must still end exactly once.
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
        # The guard rejects the inner call before it creates a span of its own.
        self.assertEqual(len(txn_spans), 1, [s.name for s in finished])
        for txn_span in txn_spans:
            self.assertIsNotNone(txn_span.end_time)

    @client_context.require_transactions
    def test_nested_with_transaction_on_another_session_keeps_spans_separate(self):
        # Nesting on a different session is legal, and each session's operations
        # must parent to its own transaction span.
        client = self.rs_or_single_client(tracing={"enabled": True})
        db = client.pymongo_test
        outer_coll = db.two_session_outer
        inner_coll = db.two_session_inner
        # Creating a collection inside a transaction is illegal before server 4.4.
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
            # Transaction spans are never made current, so neither nests under the other.
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
        # with_transaction() while a direct-API transaction is active on the same
        # session raises, and must leave that transaction's span open for later
        # operations to parent to, with no second span created.
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

            # The original transaction is still active, so this must nest under it.
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
            # An explicit second commit re-enters the branch that previously ran
            # with no transaction span at all.
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
