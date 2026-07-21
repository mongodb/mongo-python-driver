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
from pymongo.errors import OperationFailure
from pymongo.typings import _Address
from test.asynchronous import AsyncIntegrationTest, unittest

if _otel._HAS_OPENTELEMETRY:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_IS_SYNC = False

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


@unittest.skipUnless(_otel._HAS_OPENTELEMETRY, "opentelemetry is not installed")
class TestOTelSpans(AsyncIntegrationTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.exporter.clear()

    def spans(self, name: str | None = None):
        finished = self.exporter.get_finished_spans()
        if name is None:
            return list(finished)
        return [s for s in finished if s.name == name]

    # TODO(PYTHON-5947): once the unified test format runner supports
    # expectTracingMessages/operation spans, this is superseded by the spec's
    # find_without_query_text.yml and insert.yml.
    async def test_span_created_for_insert_and_find(self):
        client = await self.async_rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test_otel
        await coll.drop()
        self.exporter.clear()
        await coll.insert_one({"x": 1})

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
        docs = await coll.find({}).to_list()
        self.assertEqual(len(docs), 1)
        find_spans = self.spans("find")
        self.assertEqual(len(find_spans), 1)
        self.assertEqual(find_spans[0].attributes["db.command.name"], "find")

    async def test_span_created_for_get_more(self):
        client = await self.async_rs_or_single_client(tracing={"enabled": True})
        coll = client[self.db.name].test_otel_getmore
        await coll.drop()
        await coll.insert_many([{"x": i} for i in range(5)])
        self.exporter.clear()

        docs = await coll.find({}, batch_size=2).to_list()
        self.assertEqual(len(docs), 5)

        get_more_spans = self.spans("getMore")
        self.assertGreater(len(get_more_spans), 0)
        for span in get_more_spans:
            self.assertEqual(span.attributes["db.collection.name"], "test_otel_getmore")
            self.assertEqual(span.attributes["db.command.name"], "getMore")

    async def test_explain_retains_collection_name(self):
        # explain wraps the real command ({"explain": {"find": "coll", ...}}), the
        # same shape as getMore's indirection, so it needs the same handling.
        client = await self.async_rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        await client[self.db.name].command("explain", {"find": "test_otel", "filter": {}})

        spans = self.spans("explain")
        self.assertEqual(len(spans), 1)
        attrs = spans[0].attributes
        self.assertEqual(attrs["db.collection.name"], "test_otel")
        self.assertEqual(attrs["db.query.summary"], f"explain {self.db.name}.test_otel")

    async def test_server_port_omitted_for_unix_socket(self):
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

    async def test_sensitive_command_produces_no_span(self):
        client = await self.async_rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        with self.assertRaises(OperationFailure):
            await client.admin.command("saslStart", mechanism="SCRAM-SHA-256", payload=b"")

        names = [s.name for s in self.spans()]
        self.assertNotIn("saslStart", names)

    async def test_admin_command_omits_collection_name(self):
        # usersInfo's command value is a username string, not a collection, and
        # it always runs against admin; querying a nonexistent user is a no-op.
        client = await self.async_rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        await client.admin.command("usersInfo", "pymongo_otel_nonexistent_user")

        spans = self.spans("usersInfo")
        self.assertEqual(len(spans), 1)
        attrs = spans[0].attributes
        self.assertEqual(attrs["db.namespace"], "admin")
        self.assertNotIn("db.collection.name", attrs)
        self.assertEqual(attrs["db.query.summary"], "usersInfo admin")

    async def test_failure_records_exception_and_status_code(self):
        client = await self.async_rs_or_single_client(tracing={"enabled": True})
        self.exporter.clear()
        with self.assertRaises(OperationFailure):
            await client[self.db.name].command("thisCommandDoesNotExist")

        spans = self.spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertEqual(span.status.status_code, trace.StatusCode.ERROR)
        self.assertIn("db.response.status_code", span.attributes)
        self.assertTrue(any(event.name == "exception" for event in span.events))

    async def test_tracing_disabled_by_default(self):
        client = await self.async_rs_or_single_client()
        self.exporter.clear()
        await client.admin.command("ping")
        self.assertEqual(self.spans(), [])

    async def test_prose_1_tracing_enable_disable_via_env_var(self):
        """Prose Test 1: Tracing Enable/Disable via Environment Variable."""
        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "false"}):
            client = await self.async_rs_or_single_client()
            self.exporter.clear()
            await client.admin.command("ping")
        self.assertEqual(self.spans(), [])

        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true"}):
            client = await self.async_rs_or_single_client()
            self.exporter.clear()
            await client.admin.command("ping")
        self.assertIn("ping", [s.name for s in self.spans()])

    async def test_prose_2_command_payload_emission_via_env_var(self):
        """Prose Test 2: Command Payload Emission via Environment Variable."""
        env = {
            "OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true",
            "OTEL_PYTHON_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH": "1024",
        }
        with patch.dict(os.environ, env):
            client = await self.async_rs_or_single_client()
            self.exporter.clear()
            await client[self.db.name].test_otel.find({}).to_list()
        spans = self.spans("find")
        self.assertEqual(len(spans), 1)
        self.assertIn("db.query.text", spans[0].attributes)

        with patch.dict(os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true"}):
            client = await self.async_rs_or_single_client()
            self.exporter.clear()
            await client[self.db.name].test_otel.find({}).to_list()
        spans = self.spans("find")
        self.assertEqual(len(spans), 1)
        self.assertNotIn("db.query.text", spans[0].attributes)

    # TODO(PYTHON-5947): once the unified test format runner supports
    # expectTracingMessages/operation spans, this is superseded by the spec's
    # find.yml (db.query.text assertion).
    async def test_query_text_included_when_configured(self):
        client = await self.async_rs_or_single_client(
            tracing={"enabled": True, "query_text_max_length": 1000}
        )
        coll = client[self.db.name].test_otel
        await coll.drop()
        self.exporter.clear()
        await coll.insert_one({"x": 1})

        spans = self.spans("insert")
        self.assertEqual(len(spans), 1)
        self.assertIn("db.query.text", spans[0].attributes)
        self.assertNotIn("lsid", spans[0].attributes["db.query.text"])

    async def test_explicit_query_text_max_length_zero_overrides_env_var(self):
        # An explicit client-side 0 must win over the environment variable, unlike
        # unset (which defers to it) - otherwise an app can't reliably opt out.
        env = {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH": "1024"}
        with patch.dict(os.environ, env):
            client = await self.async_rs_or_single_client(
                tracing={"enabled": True, "query_text_max_length": 0}
            )
            self.exporter.clear()
            await client.admin.command("ping")

        spans = self.spans("ping")
        self.assertEqual(len(spans), 1)
        self.assertNotIn("db.query.text", spans[0].attributes)

    async def test_query_text_truncation_shrinks_oversized_field_values(self):
        client = await self.async_rs_or_single_client(
            tracing={"enabled": True, "query_text_max_length": 200}
        )
        coll = client[self.db.name].test_otel
        await coll.drop()
        self.exporter.clear()
        await coll.insert_one({"x": "a" * 500})

        spans = self.spans("insert")
        self.assertEqual(len(spans), 1)
        query_text = spans[0].attributes["db.query.text"]
        # The oversized field value must be truncated at the field level (not
        # just a blind cut of the fully-serialized string), keeping the result
        # close to the configured bound; a "..." marker signals the (possible)
        # safety-net cut, mirroring the driver's existing log-truncation approach.
        self.assertLessEqual(len(query_text), 200 + len("..."))
        self.assertNotIn("a" * 500, query_text)


if __name__ == "__main__":
    unittest.main()
