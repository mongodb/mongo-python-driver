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
from unittest.mock import patch

sys.path[0:0] = [""]

import pymongo._otel as _otel
from pymongo.errors import OperationFailure
from test import IntegrationTest, unittest

if _otel._HAS_OPENTELEMETRY:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_IS_SYNC = True


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


@unittest.skipUnless(_otel._HAS_OPENTELEMETRY, "opentelemetry is not installed")
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

    def test_sensitive_command_produces_no_span(self):
        client = self.rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        with self.assertRaises(OperationFailure):
            client.admin.command("saslStart", mechanism="SCRAM-SHA-256", payload=b"")

        names = [s.name for s in self.spans()]
        self.assertNotIn("saslStart", names)

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

    def test_env_var_enables_tracing(self):
        client = self.rs_or_single_client()
        self.exporter.clear()
        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true"}):
            client.admin.command("ping")

        self.assertIn("ping", [s.name for s in self.spans()])

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


if __name__ == "__main__":
    unittest.main()
