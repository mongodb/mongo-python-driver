# Copyright 2024-present MongoDB, Inc.
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

"""Unit tests for the Atlas Stream Processing driver module.

All tests run offline — no live workspace is required.  The wire layer is
stubbed out by replacing ``_command`` on the client with a lightweight spy
that records calls and returns pre-configured responses.
"""

from __future__ import annotations

import inspect
import sys
import unittest
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from unittest.mock import MagicMock, patch

sys.path[0:0] = [""]

_IS_SYNC = True

import pymongo.synchronous.stream_processing
from bson import Timestamp
from pymongo import (
    CreateStreamProcessorOptions,
    GetStreamProcessorSamplesOptions,
    GetStreamProcessorSamplesResult,
    GetStreamProcessorStatsOptions,
    SampleCursor,
    StartStreamProcessorOptions,
    StreamProcessingClient,
    StreamProcessor,
    StreamProcessorInfo,
    StreamProcessors,
)
from pymongo.errors import ConfigurationError, InvalidOperation, OperationFailure

# ---------------------------------------------------------------------------
# Spy helper
# ---------------------------------------------------------------------------


def _spy_client(
    responses: Optional[list[Mapping[str, Any]]] = None,
    raises: Optional[Exception] = None,
) -> tuple[StreamProcessingClient, list[dict[str, Any]]]:
    """Return *(client, calls)* where ``client._command`` records every call.

    *responses* is consumed in order; when exhausted, ``{}`` is returned.
    If *raises* is set, the spy raises that exception on every call instead
    of returning a response.
    """
    calls: list[dict[str, Any]] = []
    resp_list: list[Mapping[str, Any]] = list(responses) if responses is not None else [{}]

    def _command(
        cmd: dict[str, Any],
        *,
        retryable_read: bool = False,
        session: Any = None,
    ) -> Mapping[str, Any]:
        calls.append({"cmd": dict(cmd), "retryable_read": retryable_read, "session": session})
        if raises is not None:
            raise raises
        return resp_list.pop(0) if resp_list else {}

    client = StreamProcessingClient.__new__(StreamProcessingClient)
    client._client = MagicMock()
    client._command = _command
    return client, calls


# ---------------------------------------------------------------------------
# Minimal info response fixture
# ---------------------------------------------------------------------------

_INFO_RESPONSE: dict[str, Any] = {
    "ok": 1,
    "id": "abc",
    "name": "demo",
    "state": "CREATED",
    "pipeline": [],
    "pipelineVersion": 1,
    "enableAutoScaling": False,
    "failoverEnabled": False,
    "activeRegion": "us-east-1",
    "hasStarted": False,
}


# ===========================================================================
# Test class 1 — client constructor validation
# ===========================================================================


class TestStreamProcessingClientConfig(unittest.TestCase):
    """Constructor-level validation for StreamProcessingClient."""

    def test_rejects_srv_uri(self) -> None:
        with self.assertRaises(ConfigurationError) as cm:
            StreamProcessingClient("mongodb+srv://example.com/")
        self.assertIn("mongodb+srv", str(cm.exception).lower())

    def test_rejects_tls_false_kwarg(self) -> None:
        with self.assertRaises(ConfigurationError):
            StreamProcessingClient("mongodb://example.com/", tls=False)

    def test_rejects_ssl_false_kwarg(self) -> None:
        with self.assertRaises(ConfigurationError):
            StreamProcessingClient("mongodb://example.com/", ssl=False)

    def test_rejects_tls_false_in_uri(self) -> None:
        with self.assertRaises(ConfigurationError):
            StreamProcessingClient("mongodb://example.com/?tls=false")

    @patch("pymongo.synchronous.stream_processing.MongoClient")
    def test_forces_tls_true(self, mock_client: MagicMock) -> None:
        mock_client.return_value = MagicMock()
        StreamProcessingClient("mongodb://example.com/")
        kwargs = mock_client.call_args.kwargs
        self.assertTrue(kwargs.get("tls"))

    @patch("pymongo.synchronous.stream_processing.MongoClient")
    def test_defaults_authsource_to_admin(self, mock_client: MagicMock) -> None:
        mock_client.return_value = MagicMock()
        StreamProcessingClient("mongodb://example.com/")
        kwargs = mock_client.call_args.kwargs
        self.assertEqual(kwargs.get("authSource"), "admin")

    @patch("pymongo.synchronous.stream_processing.MongoClient")
    def test_preserves_explicit_authsource_in_kwarg(self, mock_client: MagicMock) -> None:
        mock_client.return_value = MagicMock()
        StreamProcessingClient("mongodb://example.com/", authSource="other")
        kwargs = mock_client.call_args.kwargs
        # Our default injection must not overwrite a user-supplied value.
        self.assertEqual(kwargs.get("authSource"), "other")

    @patch("pymongo.synchronous.stream_processing.MongoClient")
    def test_preserves_explicit_authsource_in_uri(self, mock_client: MagicMock) -> None:
        mock_client.return_value = MagicMock()
        StreamProcessingClient("mongodb://example.com/?authSource=other")
        kwargs = mock_client.call_args.kwargs
        # authSource is already in the URI; we must not inject a duplicate kwarg.
        self.assertNotIn("authSource", kwargs)

    @patch("pymongo.synchronous.stream_processing.MongoClient")
    def test_drops_ssl_kwarg_to_avoid_duplicate(self, mock_client: MagicMock) -> None:
        mock_client.return_value = MagicMock()
        StreamProcessingClient("mongodb://example.com/", ssl=True)
        kwargs = mock_client.call_args.kwargs
        self.assertNotIn("ssl", kwargs)
        self.assertTrue(kwargs.get("tls"))

    def test_workspace_endpoint_detection(self) -> None:
        from pymongo.synchronous.stream_processing import _is_workspace_endpoint

        self.assertTrue(_is_workspace_endpoint("atlas-stream-foo.virginia-usa.a.query.mongodb.net"))
        self.assertTrue(_is_workspace_endpoint("something.a.query.mongodb.net"))
        self.assertFalse(_is_workspace_endpoint("cluster0.mongodb.net"))
        self.assertFalse(_is_workspace_endpoint("localhost"))


# ===========================================================================
# Test class 2 — options dataclass validation
# ===========================================================================


class TestStreamProcessingOptions(unittest.TestCase):
    """Dataclass validation in stream_processing_options."""

    def test_start_options_mutex_start_after_and_at_op_time(self) -> None:
        with self.assertRaises(InvalidOperation):
            StartStreamProcessorOptions(
                start_after={"_id": 1},
                start_at_operation_time=Timestamp(0, 0),
            )

    def test_start_options_invalid_tier(self) -> None:
        with self.assertRaises(InvalidOperation):
            StartStreamProcessorOptions(tier="SP1")
        for valid_tier in ("SP2", "SP5", "SP10", "SP30", "SP50"):
            StartStreamProcessorOptions(tier=valid_tier)  # must not raise

    def test_start_options_invalid_workers(self) -> None:
        with self.assertRaises(InvalidOperation):
            StartStreamProcessorOptions(workers=0)
        with self.assertRaises(InvalidOperation):
            StartStreamProcessorOptions(workers=-1)
        StartStreamProcessorOptions(workers=1)  # must not raise

    def test_stats_options_invalid_scale(self) -> None:
        with self.assertRaises(InvalidOperation):
            GetStreamProcessorStatsOptions(scale=0)
        with self.assertRaises(InvalidOperation):
            GetStreamProcessorStatsOptions(scale=-1)
        GetStreamProcessorStatsOptions(scale=1)
        GetStreamProcessorStatsOptions(scale=1024)

    def test_samples_options_invalid_cursor_id(self) -> None:
        with self.assertRaises(InvalidOperation):
            GetStreamProcessorSamplesOptions(cursor_id=-1)
        # cursor_id=0 is valid at construction; the method-level check rejects it.
        GetStreamProcessorSamplesOptions(cursor_id=0)
        GetStreamProcessorSamplesOptions(cursor_id=42)

    def test_samples_options_invalid_limit(self) -> None:
        with self.assertRaises(InvalidOperation):
            GetStreamProcessorSamplesOptions(limit=-1)
        GetStreamProcessorSamplesOptions(limit=0)
        GetStreamProcessorSamplesOptions(limit=10)

    def test_samples_options_invalid_batch_size(self) -> None:
        with self.assertRaises(InvalidOperation):
            GetStreamProcessorSamplesOptions(batch_size=-1)
        GetStreamProcessorSamplesOptions(batch_size=0)
        GetStreamProcessorSamplesOptions(batch_size=5)

    def test_create_options_all_optional(self) -> None:
        opts = CreateStreamProcessorOptions()
        self.assertIsNone(opts.dlq)
        self.assertIsNone(opts.stream_meta_field_name)
        self.assertIsNone(opts.tier)
        self.assertIsNone(opts.failover)

    def test_stream_processor_info_from_response_full(self) -> None:
        now = datetime.now(tz=timezone.utc)
        doc: dict[str, Any] = {
            "ok": 1,
            "id": "abc123",
            "name": "demo",
            "state": "STARTED",
            "pipeline": [{"$source": {}}],
            "pipelineVersion": 3,
            "tier": "SP10",
            "dlq": {"connectionName": "c1"},
            "streamMetaFieldName": "ts",
            "enableAutoScaling": True,
            "failoverEnabled": True,
            "activeRegion": "us-east-1",
            "lastModifiedAt": now,
            "modifiedBy": "user@example.com",
            "lastStateChange": now,
            "lastHeartbeat": now,
            "hasStarted": True,
            "stats": {"inputMessageCount": 42},
            "errorMsg": None,
            "errorCode": None,
            "errorRetryable": None,
        }
        info = StreamProcessorInfo.from_response(doc)
        self.assertEqual(info.id, "abc123")
        self.assertEqual(info.name, "demo")
        self.assertEqual(info.state, "STARTED")
        self.assertEqual(info.pipeline, [{"$source": {}}])
        self.assertEqual(info.pipeline_version, 3)
        self.assertEqual(info.tier, "SP10")
        self.assertEqual(info.dlq, {"connectionName": "c1"})
        self.assertEqual(info.stream_meta_field_name, "ts")
        self.assertTrue(info.enable_auto_scaling)
        self.assertTrue(info.failover_enabled)
        self.assertEqual(info.active_region, "us-east-1")
        self.assertTrue(info.has_started)
        self.assertEqual(info.stats, {"inputMessageCount": 42})
        self.assertEqual(info.raw, doc)

    def test_stream_processor_info_from_response_minimal(self) -> None:
        doc: dict[str, Any] = {
            "id": "x",
            "name": "p",
            "state": "CREATED",
            "pipeline": [],
            "pipelineVersion": 1,
        }
        info = StreamProcessorInfo.from_response(doc)
        self.assertEqual(info.id, "x")
        self.assertIsNone(info.tier)
        self.assertIsNone(info.dlq)
        self.assertFalse(info.enable_auto_scaling)
        self.assertFalse(info.has_started)
        self.assertEqual(info.raw, doc)

    def test_stream_processor_info_preserves_unknown_fields(self) -> None:
        doc: dict[str, Any] = {
            "id": "x",
            "name": "p",
            "state": "CREATED",
            "pipeline": [],
            "pipelineVersion": 1,
            "futureField": "someValue",
        }
        info = StreamProcessorInfo.from_response(doc)
        self.assertEqual(info.raw["futureField"], "someValue")

    def test_stream_processor_info_state_is_string(self) -> None:
        doc: dict[str, Any] = {
            "id": "x",
            "name": "p",
            "state": "FUTURE_UNKNOWN_STATE",
            "pipeline": [],
            "pipelineVersion": 1,
        }
        info = StreamProcessorInfo.from_response(doc)
        self.assertEqual(info.state, "FUTURE_UNKNOWN_STATE")


# ===========================================================================
# Test class 3 — lifecycle commands
# ===========================================================================


class TestStreamProcessorsCommands(unittest.TestCase):
    """Lifecycle commands on StreamProcessors and StreamProcessor."""

    def setUp(self) -> None:
        self.client, self.calls = _spy_client(responses=[{"ok": 1}] * 30)
        self.sps = StreamProcessors(self.client)
        self.proc = StreamProcessor(client=self.client, name="demo")

    def test_create_sends_correct_command(self) -> None:
        self.sps.create("demo", pipeline=[{"$source": {}}])
        entry = self.calls[-1]
        self.assertEqual(entry["cmd"].get("createStreamProcessor"), "demo")
        self.assertEqual(entry["cmd"].get("pipeline"), [{"$source": {}}])
        self.assertNotIn("options", entry["cmd"])
        self.assertFalse(entry["retryable_read"])

    def test_create_with_options_includes_them(self) -> None:
        opts = CreateStreamProcessorOptions(dlq={"connectionName": "c"}, tier="SP10")
        self.sps.create("demo", pipeline=[{"$source": {}}], options=opts)
        cmd = self.calls[-1]["cmd"]
        self.assertEqual(cmd["options"]["dlq"], {"connectionName": "c"})
        self.assertEqual(cmd["options"]["tier"], "SP10")

    def test_create_rejects_empty_name(self) -> None:
        with self.assertRaises(InvalidOperation):
            self.sps.create("", pipeline=[{"$x": 1}])
        self.assertEqual(len(self.calls), 0)

    def test_create_rejects_whitespace_name(self) -> None:
        with self.assertRaises(InvalidOperation):
            self.sps.create("   ", pipeline=[{"$x": 1}])
        self.assertEqual(len(self.calls), 0)

    def test_create_rejects_empty_pipeline(self) -> None:
        with self.assertRaises(InvalidOperation):
            self.sps.create("demo", pipeline=[])
        self.assertEqual(len(self.calls), 0)

    def test_get_returns_handle_without_calling_server(self) -> None:
        proc = self.sps.get("demo")
        self.assertEqual(len(self.calls), 0)
        self.assertEqual(proc.name, "demo")
        self.assertIsInstance(proc, StreamProcessor)

    def test_get_rejects_empty_name(self) -> None:
        with self.assertRaises(InvalidOperation):
            self.sps.get("")

    def test_get_info_sends_correct_command_and_decodes(self) -> None:
        client, calls = _spy_client(responses=[dict(_INFO_RESPONSE)])
        info = StreamProcessors(client).get_info("demo")
        self.assertEqual(calls[0]["cmd"].get("getStreamProcessor"), "demo")
        self.assertTrue(calls[0]["retryable_read"])
        self.assertIsInstance(info, StreamProcessorInfo)
        self.assertEqual(info.id, "abc")
        self.assertEqual(info.state, "CREATED")

    def test_get_info_preserves_unknown_response_fields(self) -> None:
        resp = dict(_INFO_RESPONSE, futureField="value")
        client, _ = _spy_client(responses=[resp])
        info = StreamProcessors(client).get_info("demo")
        self.assertEqual(info.raw["futureField"], "value")

    def test_start_sends_correct_command(self) -> None:
        self.proc.start()
        entry = self.calls[-1]
        self.assertEqual(entry["cmd"].get("startStreamProcessor"), "demo")
        self.assertNotIn("workers", entry["cmd"])
        self.assertNotIn("options", entry["cmd"])
        self.assertFalse(entry["retryable_read"])

    def test_start_with_options_includes_them(self) -> None:
        opts = StartStreamProcessorOptions(workers=2, clear_checkpoints=True, tier="SP10")
        self.proc.start(options=opts)
        cmd = self.calls[-1]["cmd"]
        self.assertEqual(cmd.get("workers"), 2)
        self.assertTrue(cmd["options"]["clearCheckpoints"])
        self.assertEqual(cmd["options"]["tier"], "SP10")

    def test_stop_sends_correct_command(self) -> None:
        self.proc.stop()
        entry = self.calls[-1]
        self.assertEqual(entry["cmd"].get("stopStreamProcessor"), "demo")
        self.assertFalse(entry["retryable_read"])

    def test_drop_sends_correct_command(self) -> None:
        self.proc.drop()
        entry = self.calls[-1]
        self.assertEqual(entry["cmd"].get("dropStreamProcessor"), "demo")
        self.assertFalse(entry["retryable_read"])

    def test_stats_sends_correct_command_and_returns_raw_dict(self) -> None:
        raw_resp: dict[str, Any] = {"ok": 1, "stats": {"inputMessageCount": 5}, "futureField": "x"}
        client, calls = _spy_client(responses=[raw_resp])
        result = StreamProcessor(client=client, name="demo").stats()
        self.assertEqual(calls[0]["cmd"].get("getStreamProcessorStats"), "demo")
        self.assertTrue(calls[0]["retryable_read"])
        self.assertEqual(result, raw_resp)

    def test_stats_with_options(self) -> None:
        client, calls = _spy_client(responses=[{"ok": 1}])
        opts = GetStreamProcessorStatsOptions(scale=1024, verbose=True)
        StreamProcessor(client=client, name="demo").stats(options=opts)
        self.assertEqual(calls[0]["cmd"]["options"]["scale"], 1024)
        self.assertTrue(calls[0]["cmd"]["options"]["verbose"])

    def test_session_propagates(self) -> None:
        fake_session = MagicMock()
        self.proc.start(session=fake_session)
        self.assertIs(self.calls[-1]["session"], fake_session)


# ===========================================================================
# Test class 4 — sample cursor
# ===========================================================================


class TestSampleCursor(unittest.TestCase):
    """Two-phase sample cursor protocol."""

    def test_get_samples_initial_call_sends_start_command(self) -> None:
        client, calls = _spy_client(responses=[{"cursorId": 42, "firstBatch": [{"x": 1}]}])
        proc = StreamProcessor(client=client, name="demo")
        result = proc.get_stream_processor_samples()
        cmd = calls[0]["cmd"]
        self.assertIn("startSampleStreamProcessor", cmd)
        self.assertNotIn("cursorId", cmd)
        self.assertNotIn("batchSize", cmd)
        self.assertFalse(calls[0]["retryable_read"])
        self.assertEqual(result.cursor_id, 42)
        self.assertEqual(result.documents, [{"x": 1}])

    def test_get_samples_initial_call_includes_limit(self) -> None:
        client, calls = _spy_client(responses=[{"cursorId": 1, "firstBatch": []}])
        proc = StreamProcessor(client=client, name="demo")
        proc.get_stream_processor_samples(GetStreamProcessorSamplesOptions(limit=100))
        self.assertEqual(calls[0]["cmd"].get("limit"), 100)
        self.assertNotIn("batchSize", calls[0]["cmd"])

    def test_get_samples_continuation_sends_get_more(self) -> None:
        client, calls = _spy_client(responses=[{"cursorId": 0, "nextBatch": [{"y": 2}]}])
        proc = StreamProcessor(client=client, name="demo")
        result = proc.get_stream_processor_samples(GetStreamProcessorSamplesOptions(cursor_id=42))
        cmd = calls[0]["cmd"]
        self.assertIn("getMoreSampleStreamProcessor", cmd)
        self.assertEqual(cmd.get("cursorId"), 42)
        self.assertNotIn("limit", cmd)
        self.assertEqual(result.cursor_id, 0)
        self.assertEqual(result.documents, [{"y": 2}])

    def test_get_samples_continuation_includes_batch_size(self) -> None:
        client, calls = _spy_client(responses=[{"cursorId": 0, "nextBatch": []}])
        proc = StreamProcessor(client=client, name="demo")
        proc.get_stream_processor_samples(
            GetStreamProcessorSamplesOptions(cursor_id=42, batch_size=5)
        )
        self.assertEqual(calls[0]["cmd"].get("batchSize"), 5)

    def test_get_samples_rejects_cursor_id_zero(self) -> None:
        client, calls = _spy_client()
        proc = StreamProcessor(client=client, name="demo")
        with self.assertRaises(InvalidOperation):
            proc.get_stream_processor_samples(GetStreamProcessorSamplesOptions(cursor_id=0))
        self.assertEqual(len(calls), 0)

    def test_sample_iterator_drains_single_batch(self) -> None:
        client, calls = _spy_client(
            responses=[{"cursorId": 0, "firstBatch": [{"a": 1}, {"a": 2}, {"a": 3}]}]
        )
        proc = StreamProcessor(client=client, name="demo")
        docs = []
        for doc in proc.sample():
            docs.append(doc)
        self.assertEqual(docs, [{"a": 1}, {"a": 2}, {"a": 3}])
        self.assertEqual(len(calls), 1)

    def test_sample_iterator_drains_multiple_batches(self) -> None:
        client, calls = _spy_client(
            responses=[
                {"cursorId": 11, "firstBatch": [{"a": 1}]},
                {"cursorId": 22, "nextBatch": [{"a": 2}]},
                {"cursorId": 0, "nextBatch": [{"a": 3}]},
            ]
        )
        proc = StreamProcessor(client=client, name="demo")
        docs = []
        for doc in proc.sample():
            docs.append(doc)
        self.assertEqual(docs, [{"a": 1}, {"a": 2}, {"a": 3}])
        self.assertEqual(len(calls), 3)

    def test_sample_iterator_handles_empty_batch_with_nonzero_cursor(self) -> None:
        client, calls = _spy_client(
            responses=[
                {"cursorId": 11, "firstBatch": []},
                {"cursorId": 22, "nextBatch": []},
                {"cursorId": 0, "nextBatch": [{"a": 1}]},
            ]
        )
        proc = StreamProcessor(client=client, name="demo")
        docs = []
        for doc in proc.sample():
            docs.append(doc)
        self.assertEqual(docs, [{"a": 1}])
        self.assertEqual(len(calls), 3)

    def test_sample_iterator_stops_after_cursor_id_zero(self) -> None:
        client, calls = _spy_client(responses=[{"cursorId": 0, "firstBatch": [{"a": 1}]}])
        proc = StreamProcessor(client=client, name="demo")
        cursor = proc.sample()
        first = cursor.__next__()
        self.assertEqual(first, {"a": 1})
        with self.assertRaises(StopIteration):
            cursor.__next__()
        # Only one wire call — the server returned cursorId:0 so we stopped.
        self.assertEqual(len(calls), 1)

    def test_sample_cursor_close_stops_iteration(self) -> None:
        client, calls = _spy_client()
        proc = StreamProcessor(client=client, name="demo")
        cursor = proc.sample()
        cursor.close()
        with self.assertRaises(StopIteration):
            cursor.__next__()
        self.assertEqual(len(calls), 0)

    def test_sample_cursor_alive_property(self) -> None:
        client, _ = _spy_client(responses=[{"cursorId": 0, "firstBatch": [{"a": 1}]}])
        proc = StreamProcessor(client=client, name="demo")
        cursor = proc.sample()
        self.assertTrue(cursor.alive)
        doc = cursor.__next__()
        self.assertEqual(doc, {"a": 1})
        # After exhaustion (cursorId:0 returned during refill), alive is False.
        self.assertFalse(cursor.alive)

    def test_sample_cursor_id_property(self) -> None:
        client, _ = _spy_client(responses=[{"cursorId": 42, "firstBatch": [{"a": 1}]}])
        proc = StreamProcessor(client=client, name="demo")
        cursor = proc.sample()
        self.assertIsNone(cursor.cursor_id)
        cursor.__next__()
        self.assertEqual(cursor.cursor_id, 42)

    def test_sample_cursor_async_context_manager(self) -> None:
        client, _ = _spy_client()
        proc = StreamProcessor(client=client, name="demo")
        with proc.sample() as cur:
            self.assertIsInstance(cur, SampleCursor)
        self.assertTrue(cur._closed)

    def test_sample_cursor_session_propagates_to_refill(self) -> None:
        client, calls = _spy_client(responses=[{"cursorId": 0, "firstBatch": [{"a": 1}]}])
        proc = StreamProcessor(client=client, name="demo")
        fake_session = MagicMock()
        for _ in proc.sample(session=fake_session):
            pass
        self.assertIs(calls[0]["session"], fake_session)

    def test_sample_cursor_does_not_inherit_from_cursor_base(self) -> None:
        self.assertEqual(SampleCursor.__bases__, (object,))


# ===========================================================================
# Test class 5 — error handling
# ===========================================================================


class TestErrorHandling(unittest.TestCase):
    """Server errors MUST propagate unchanged — no rewrapping, no filtering."""

    def _failure(self, code: int = 9) -> OperationFailure:
        return OperationFailure("test error", code=code, details={"errmsg": "test", "code": code})

    def _assert_propagates(self, fn: Any, expected_code: int) -> None:
        with self.assertRaises(OperationFailure) as cm:
            fn()
        self.assertEqual(cm.exception.code, expected_code)

    def test_create_propagates_operation_failure(self) -> None:
        client, _ = _spy_client(raises=self._failure(9))
        self._assert_propagates(
            lambda: StreamProcessors(client).create("p", pipeline=[{"$x": 1}]), 9
        )

    def test_start_propagates_operation_failure(self) -> None:
        client, _ = _spy_client(raises=self._failure(72))
        self._assert_propagates(lambda: StreamProcessor(client=client, name="p").start(), 72)

    def test_stop_propagates_operation_failure(self) -> None:
        client, _ = _spy_client(raises=self._failure(125))
        self._assert_propagates(lambda: StreamProcessor(client=client, name="p").stop(), 125)

    def test_drop_propagates_operation_failure(self) -> None:
        client, _ = _spy_client(raises=self._failure(1))
        self._assert_propagates(lambda: StreamProcessor(client=client, name="p").drop(), 1)

    def test_get_info_propagates_operation_failure(self) -> None:
        client, _ = _spy_client(raises=self._failure(9))
        self._assert_propagates(lambda: StreamProcessors(client).get_info("p"), 9)

    def test_stats_propagates_operation_failure(self) -> None:
        client, _ = _spy_client(raises=self._failure(72))
        self._assert_propagates(lambda: StreamProcessor(client=client, name="p").stats(), 72)

    def test_get_stream_processor_samples_propagates_operation_failure(self) -> None:
        client, _ = _spy_client(raises=self._failure(9))
        self._assert_propagates(
            lambda: StreamProcessor(client=client, name="p").get_stream_processor_samples(), 9
        )

    def test_unknown_error_code_propagates(self) -> None:
        # Code 999 is not in the documented list; it must still propagate.
        client, _ = _spy_client(raises=self._failure(999))
        self._assert_propagates(
            lambda: StreamProcessors(client).create("p", pipeline=[{"$x": 1}]), 999
        )

    def test_no_operation_failure_caught_in_module(self) -> None:
        """Structural: verify no try/except hides server errors in either module."""
        for mod in (pymongo.synchronous.stream_processing,):
            src = inspect.getsource(mod)
            self.assertNotIn("except OperationFailure", src, mod.__name__)
            self.assertNotIn("except PyMongoError", src, mod.__name__)
            self.assertNotIn("exc.code in", src, mod.__name__)


# ===========================================================================
# Test class 6 — spec compliance
# ===========================================================================


class TestSpecCompliance(unittest.TestCase):
    """Spec MUST-level compliance checks."""

    def test_retryability_classification(self) -> None:
        """Each command must route as retryable or non-retryable per the spec."""
        cases: list[tuple[str, Any, bool]] = [
            # (label, coroutine-factory, expected_retryable_read)
            ("create", lambda c: StreamProcessors(c).create("demo", pipeline=[{"$x": 1}]), False),
            ("start", lambda c: StreamProcessor(client=c, name="demo").start(), False),
            ("stop", lambda c: StreamProcessor(client=c, name="demo").stop(), False),
            ("drop", lambda c: StreamProcessor(client=c, name="demo").drop(), False),
            ("get_info", lambda c: StreamProcessors(c).get_info("demo"), True),
            ("stats", lambda c: StreamProcessor(client=c, name="demo").stats(), True),
            (
                "get_samples_initial",
                lambda c: StreamProcessor(client=c, name="demo").get_stream_processor_samples(),
                False,
            ),
            (
                "get_samples_continuation",
                lambda c: StreamProcessor(client=c, name="demo").get_stream_processor_samples(
                    GetStreamProcessorSamplesOptions(cursor_id=42)
                ),
                False,
            ),
        ]

        responses_for: dict[str, list[Mapping[str, Any]]] = {
            "get_info": [dict(_INFO_RESPONSE)],
            "get_samples_initial": [{"cursorId": 0, "firstBatch": []}],
            "get_samples_continuation": [{"cursorId": 0, "nextBatch": []}],
        }

        for label, make_coro, expected in cases:
            resp = responses_for.get(label, [{"ok": 1}])
            client, calls = _spy_client(responses=resp)
            make_coro(client)
            self.assertEqual(
                calls[-1]["retryable_read"],
                expected,
                f"retryability mismatch for '{label}'",
            )

    def test_async_sample_cursor_class_hierarchy(self) -> None:
        self.assertEqual(SampleCursor.__bases__, (object,))


if __name__ == "__main__":
    unittest.main()
