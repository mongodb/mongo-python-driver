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

"""Test OpenTelemetry operation spans for cursor getMores."""

from __future__ import annotations

import gc
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
class TestOTelGetMoreSpans(IntegrationTest):
    """getMore spans, cursor lifetime, and change streams."""

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

    def test_caller_driven_find_getmores_get_their_own_operation_spans(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.getmore_nesting
        coll.drop()
        coll.insert_many([{"i": i} for i in range(10)])
        self.exporter.clear()

        docs = coll.find({}, batch_size=2).to_list()
        self.assertEqual(len(docs), 10)

        finished = self.exporter.get_finished_spans()
        # One operation span for the query that created the cursor.
        find_op_spans = self.operation_spans(finished, "find")
        self.assertEqual(len(find_op_spans), 1, [s.name for s in finished])
        find_op_span = find_op_spans[0]
        self.assertEqual(find_op_span.name, "find pymongo_test.getmore_nesting")
        self.assertTrue(find_op_span.attributes["db.mongodb.cursor_id"])

        # One more, a sibling rather than a child, per getMore the caller drove.
        getmore_op_spans = self.operation_spans(finished, "getMore")
        self.assertGreater(len(getmore_op_spans), 1)
        for op_span in getmore_op_spans:
            self.assertEqual(op_span.name, "getMore pymongo_test.getmore_nesting")
            self.assertNotEqual(op_span.parent, find_op_span.context)
            self.assertEqual(
                op_span.attributes["db.mongodb.cursor_id"],
                find_op_span.attributes["db.mongodb.cursor_id"],
            )

        # Each getMore command span nests under its own operation span.
        getmore_cmd_spans = self.command_spans(finished, "getMore")
        self.assertEqual(len(getmore_cmd_spans), len(getmore_op_spans))
        parent_ids = {s.context.span_id for s in getmore_op_spans}
        for cmd_span in getmore_cmd_spans:
            self.assertIn(cmd_span.parent.span_id, parent_ids)

    def test_caller_driven_aggregate_getmores_get_their_own_operation_spans(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.agg_nesting
        coll.drop()
        coll.insert_many([{"i": i} for i in range(10)])
        self.exporter.clear()

        docs = (coll.aggregate([{"$match": {}}], batchSize=2)).to_list()
        self.assertEqual(len(docs), 10)

        finished = self.exporter.get_finished_spans()
        agg_op_spans = self.operation_spans(finished, "aggregate")
        self.assertEqual(len(agg_op_spans), 1, [s.name for s in finished])
        agg_op_span = agg_op_spans[0]

        getmore_op_spans = self.operation_spans(finished, "getMore")
        self.assertGreater(len(getmore_op_spans), 1)
        for op_span in getmore_op_spans:
            self.assertEqual(op_span.name, "getMore pymongo_test.agg_nesting")
            self.assertNotEqual(op_span.parent, agg_op_span.context)

        getmore_cmd_spans = self.command_spans(finished, "getMore")
        self.assertEqual(len(getmore_cmd_spans), len(getmore_op_spans))
        parent_ids = {s.context.span_id for s in getmore_op_spans}
        for cmd_span in getmore_cmd_spans:
            self.assertIn(cmd_span.parent.span_id, parent_ids)

    def test_internal_iteration_keeps_getmores_in_one_operation_span(self):
        # list_collection_names drains its own cursor, so the whole call is one
        # operation and its getMores get no operation spans of their own.
        client = self.rs_or_single_client(tracing={"enabled": True})
        db = client.pymongo_test_internal_iteration
        for i in range(6):
            db[f"coll{i}"].insert_one({})
        self.addCleanup(client.drop_database, db.name)
        self.exporter.clear()

        names = db.list_collection_names(cursor={"batchSize": 2})
        self.assertEqual(len(names), 6)

        finished = self.exporter.get_finished_spans()
        op_spans = self.operation_spans(finished, "listCollections")
        self.assertEqual(len(op_spans), 1, [s.name for s in finished])
        op_span = op_spans[0]
        self.assertEqual(self.operation_spans(finished, "getMore"), [])

        getmore_cmd_spans = self.command_spans(finished, "getMore")
        self.assertGreater(len(getmore_cmd_spans), 0)
        for cmd_span in getmore_cmd_spans:
            self.assertEqual(cmd_span.parent.span_id, op_span.context.span_id)

    def test_single_batch_aggregate_ends_span_promptly_not_at_gc(self):
        # A cursor exhausted by its first batch never calls close(), so without
        # explicit attachment only __del__ would end its span. Assert the span is
        # already ended while a reference to the cursor is still held.
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.agg_single_batch
        coll.drop()
        coll.insert_many([{"i": i} for i in range(3)])
        self.exporter.clear()

        cursor = coll.aggregate([{"$match": {}}])
        # Confirm this test actually exercises the single-batch path.
        self.assertTrue(cursor._killed)

        finished = self.exporter.get_finished_spans()
        agg_op_spans = [
            s
            for s in finished
            if s.attributes.get("db.operation.name") == "aggregate"
            and "db.command.name" not in s.attributes
        ]
        self.assertEqual(len(agg_op_spans), 1, [s.name for s in finished])
        self.assertIsNotNone(agg_op_spans[0].end_time)

        # Keeps the cursor alive: were __del__ ending the span, the assertion
        # above would not have seen it yet.
        self.assertIsNotNone(cursor)

    def test_abandoned_cursor_still_ends_operation_span(self):
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

    def test_prose_4_get_more_records_sent_cursor_id_not_returned_cursor_id(self):
        """Prose Test 4: getMore records the cursor id it sent, not the cursor id returned."""
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.prose4_getmore_cursor_id
        coll.drop()
        coll.insert_many([{"i": i} for i in range(3)])
        self.exporter.clear()

        cursor = coll.find({}, batch_size=2)
        # Drain the first batch without triggering a getMore, so the id read here
        # is the one about to be sent rather than the one its reply returns.
        cursor.next()
        cursor.next()
        sent_cursor_id = cursor.cursor_id
        self.assertIsNotNone(sent_cursor_id)
        self.assertNotEqual(sent_cursor_id, 0)

        # The last document sends one getMore, whose reply returns a cursor id of 0.
        remaining = cursor.to_list()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(cursor.cursor_id, 0)

        finished = self.exporter.get_finished_spans()
        getmore_op_spans = self.operation_spans(finished, "getMore")
        self.assertEqual(len(getmore_op_spans), 1, [s.name for s in finished])
        getmore_cmd_spans = self.command_spans(finished, "getMore")
        self.assertEqual(len(getmore_cmd_spans), 1, [s.name for s in finished])

        # Both spans must carry the id sent, never the 0 the reply returned.
        for span in (getmore_op_spans[0], getmore_cmd_spans[0]):
            self.assertIn("db.mongodb.cursor_id", span.attributes)
            self.assertNotEqual(span.attributes["db.mongodb.cursor_id"], 0)
            self.assertEqual(span.attributes["db.mongodb.cursor_id"], sent_cursor_id)

    @client_context.require_transactions
    def test_prose_3_get_more_in_transaction_nests_under_transaction_span(self):
        """Prose Test 3: getMore inside a transaction nests under the transaction span."""
        client = self.rs_or_single_client(tracing={"enabled": True})
        coll = client.pymongo_test.prose3_getmore_in_txn
        coll.drop()
        # Inserted outside the transaction, so the transaction below contains
        # only the find and getMore.
        coll.insert_many([{"i": i} for i in range(3)])

        def callback(session):
            docs = coll.find({}, batch_size=2, session=session).to_list()
            self.assertEqual(len(docs), 3)

        self.exporter.clear()
        with client.start_session() as session:
            session.with_transaction(callback)

        finished = self.exporter.get_finished_spans()
        txn_spans = [s for s in finished if s.name == "transaction"]
        self.assertEqual(len(txn_spans), 1, [s.name for s in finished])
        txn_span = txn_spans[0]

        find_op_spans = self.operation_spans(finished, "find")
        self.assertEqual(len(find_op_spans), 1, [s.name for s in finished])
        find_op_span = find_op_spans[0]

        getmore_op_spans = self.operation_spans(finished, "getMore")
        self.assertEqual(len(getmore_op_spans), 1, [s.name for s in finished])
        getmore_op_span = getmore_op_spans[0]

        # Both operation spans must nest directly under the transaction span...
        self.assertEqual(find_op_span.parent.span_id, txn_span.context.span_id)
        self.assertEqual(getmore_op_span.parent.span_id, txn_span.context.span_id)
        # ...as siblings of each other, not one nested under the other.
        self.assertNotEqual(getmore_op_span.parent.span_id, find_op_span.context.span_id)

    def test_getmore_over_a_command_namespace_omits_the_collection(self):
        """A cursor opened by a command targets no user collection.

        listCollections runs against "<db>.$cmd.listCollections", which its
        getMore carries as the command's collection. That names no user
        collection, so db.collection.name is omitted from the span.
        """
        client = self.rs_or_single_client(tracing={"enabled": True})
        db = client.pymongo_test_cmd_ns
        client.drop_database(db.name)
        # Two collections with a batch size of one guarantees exactly one getMore.
        db.coll_one.insert_one({"x": 1})
        db.coll_two.insert_one({"x": 1})
        self.exporter.clear()

        (db.list_collections(cursor={"batchSize": 1})).to_list()

        finished = self.exporter.get_finished_spans()
        getmore_op_spans = self.operation_spans(finished, "getMore")
        self.assertGreaterEqual(len(getmore_op_spans), 1, [s.name for s in finished])
        getmore_cmd_spans = self.command_spans(finished, "getMore")
        self.assertGreaterEqual(len(getmore_cmd_spans), 1, [s.name for s in finished])

        for span in getmore_op_spans + getmore_cmd_spans:
            self.assertNotIn("db.collection.name", span.attributes)
        # With no collection targeted the operation span is named "<operation>
        # <db>", while the command span is named after the command alone.
        for span in getmore_op_spans:
            self.assertEqual(span.name, f"getMore {db.name}")
            self.assertEqual(span.attributes["db.operation.summary"], f"getMore {db.name}")
        for span in getmore_cmd_spans:
            self.assertEqual(span.name, "getMore")
            self.assertEqual(span.attributes["db.query.summary"], f"getMore {db.name}")

        client.drop_database(db.name)

    @client_context.require_version_min(8, 0)
    def test_client_bulk_write_results_cursor_getmores_nest_under_bulk_write(self):
        # Successful inserts have tiny result docs, so no operation count within
        # maxWriteBatchSize overflows the first batch and forces a getMore.
        # Duplicate-key errors embed the offending key, so a padded one overflows
        # at a small, fast operation count on the same code path.
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

    @client_context.require_version_min(4, 2, 0)
    @client_context.require_change_streams
    def test_change_stream_collection_level_operation_span_has_full_namespace(self):
        # A collection-level change stream's span carries both the database and
        # the collection; the broader cases below must omit db.collection.name.
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
