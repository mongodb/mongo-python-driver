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

"""Tests for pymongo.event_loggers."""
from __future__ import annotations

import sys
from unittest.mock import MagicMock

sys.path[0:0] = [""]

from test import unittest

from pymongo.event_loggers import (
    CommandLogger,
    ConnectionPoolLogger,
    HeartbeatLogger,
    ServerLogger,
    TopologyLogger,
)


def _make_command_started(command_name="find", request_id=1, connection_id=("localhost", 27017)):
    event = MagicMock()
    event.command_name = command_name
    event.request_id = request_id
    event.connection_id = connection_id
    return event


def _make_command_succeeded(
    command_name="find", request_id=1, connection_id=("localhost", 27017), duration_micros=100
):
    event = MagicMock()
    event.command_name = command_name
    event.request_id = request_id
    event.connection_id = connection_id
    event.duration_micros = duration_micros
    return event


def _make_command_failed(
    command_name="find", request_id=1, connection_id=("localhost", 27017), duration_micros=200
):
    event = MagicMock()
    event.command_name = command_name
    event.request_id = request_id
    event.connection_id = connection_id
    event.duration_micros = duration_micros
    return event


def _make_server_event(server_address=("localhost", 27017), topology_id="tid1"):
    event = MagicMock()
    event.server_address = server_address
    event.topology_id = topology_id
    return event


def _make_server_description_changed(
    server_address=("localhost", 27017),
    topology_id="tid1",
    previous_type=1,
    previous_type_name="Standalone",
    new_type=2,
    new_type_name="RSPrimary",
):
    event = MagicMock()
    event.server_address = server_address
    event.topology_id = topology_id
    event.previous_description.server_type = previous_type
    event.previous_description.server_type_name = previous_type_name
    event.new_description.server_type = new_type
    event.new_description.server_type_name = new_type_name
    return event


def _make_heartbeat_started(connection_id=("localhost", 27017)):
    event = MagicMock()
    event.connection_id = connection_id
    return event


def _make_heartbeat_succeeded(connection_id=("localhost", 27017), reply_document=None):
    event = MagicMock()
    event.connection_id = connection_id
    event.reply.document = reply_document or {"ok": 1}
    return event


def _make_heartbeat_failed(connection_id=("localhost", 27017), reply=None):
    event = MagicMock()
    event.connection_id = connection_id
    event.reply = reply or Exception("connection reset")
    return event


def _make_topology_event(topology_id="tid1"):
    event = MagicMock()
    event.topology_id = topology_id
    return event


def _make_topology_description_changed(
    topology_id="tid1",
    previous_type=0,
    previous_type_name="Unknown",
    new_type=1,
    new_type_name="Single",
    has_writable=True,
    has_readable=True,
):
    event = MagicMock()
    event.topology_id = topology_id
    event.previous_description.topology_type = previous_type
    event.previous_description.topology_type_name = previous_type_name
    event.new_description.topology_type = new_type
    event.new_description.topology_type_name = new_type_name
    event.new_description.has_writable_server.return_value = has_writable
    event.new_description.has_readable_server.return_value = has_readable
    return event


def _make_pool_event(address=("localhost", 27017)):
    event = MagicMock()
    event.address = address
    return event


def _make_connection_event(address=("localhost", 27017), connection_id=1):
    event = MagicMock()
    event.address = address
    event.connection_id = connection_id
    return event


def _make_connection_closed_event(address=("localhost", 27017), connection_id=1, reason="stale"):
    event = MagicMock()
    event.address = address
    event.connection_id = connection_id
    event.reason = reason
    return event


def _make_checkout_failed_event(address=("localhost", 27017), reason="timeout"):
    event = MagicMock()
    event.address = address
    event.reason = reason
    return event


class TestCommandLogger(unittest.TestCase):
    def setUp(self):
        self.logger = CommandLogger()

    def test_started_logs_info(self):
        event = _make_command_started(command_name="find", request_id=42)
        with self.assertLogs(level="INFO") as cm:
            self.logger.started(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("INFO", cm.output[0])
        self.assertIn("find", cm.output[0])
        self.assertIn("42", cm.output[0])
        self.assertIn("started", cm.output[0])

    def test_succeeded_logs_info(self):
        event = _make_command_succeeded(command_name="insert", request_id=7, duration_micros=500)
        with self.assertLogs(level="INFO") as cm:
            self.logger.succeeded(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("INFO", cm.output[0])
        self.assertIn("insert", cm.output[0])
        self.assertIn("7", cm.output[0])
        self.assertIn("500", cm.output[0])
        self.assertIn("succeeded", cm.output[0])

    def test_failed_logs_info(self):
        event = _make_command_failed(command_name="delete", request_id=3, duration_micros=300)
        with self.assertLogs(level="INFO") as cm:
            self.logger.failed(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("INFO", cm.output[0])
        self.assertIn("delete", cm.output[0])
        self.assertIn("3", cm.output[0])
        self.assertIn("300", cm.output[0])
        self.assertIn("failed", cm.output[0])

    def test_started_includes_connection_id(self):
        event = _make_command_started(connection_id=("db.example.com", 27018))
        with self.assertLogs(level="INFO") as cm:
            self.logger.started(event)
        self.assertIn("db.example.com", cm.output[0])

    def test_succeeded_includes_microseconds(self):
        event = _make_command_succeeded(duration_micros=12345)
        with self.assertLogs(level="INFO") as cm:
            self.logger.succeeded(event)
        self.assertIn("12345", cm.output[0])
        self.assertIn("microseconds", cm.output[0])

    def test_failed_includes_microseconds(self):
        event = _make_command_failed(duration_micros=9999)
        with self.assertLogs(level="INFO") as cm:
            self.logger.failed(event)
        self.assertIn("9999", cm.output[0])
        self.assertIn("microseconds", cm.output[0])


class TestServerLogger(unittest.TestCase):
    def setUp(self):
        self.logger = ServerLogger()

    def test_opened_logs_info(self):
        event = _make_server_event(server_address=("host1", 27017), topology_id="topology-abc")
        with self.assertLogs(level="INFO") as cm:
            self.logger.opened(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("INFO", cm.output[0])
        self.assertIn("host1", cm.output[0])
        self.assertIn("topology-abc", cm.output[0])

    def test_closed_logs_warning(self):
        event = _make_server_event(server_address=("host1", 27017), topology_id="topology-abc")
        with self.assertLogs(level="WARNING") as cm:
            self.logger.closed(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("WARNING", cm.output[0])
        self.assertIn("host1", cm.output[0])
        self.assertIn("topology-abc", cm.output[0])

    def test_description_changed_logs_when_type_changes(self):
        event = _make_server_description_changed(
            previous_type=1,
            previous_type_name="Unknown",
            new_type=2,
            new_type_name="Standalone",
        )
        with self.assertLogs(level="INFO") as cm:
            self.logger.description_changed(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("Unknown", cm.output[0])
        self.assertIn("Standalone", cm.output[0])

    def test_description_changed_no_log_when_type_same(self):
        event = _make_server_description_changed(
            previous_type=2,
            previous_type_name="Standalone",
            new_type=2,
            new_type_name="Standalone",
        )
        # No logs should be emitted when type is unchanged
        import logging

        root_logger = logging.getLogger()
        original_level = root_logger.level
        root_logger.setLevel(logging.DEBUG)
        try:
            with self.assertRaises(AssertionError):
                # assertLogs raises AssertionError if no logs are emitted
                with self.assertLogs(level="INFO"):
                    self.logger.description_changed(event)
        finally:
            root_logger.setLevel(original_level)


class TestHeartbeatLogger(unittest.TestCase):
    def setUp(self):
        self.logger = HeartbeatLogger()

    def test_started_logs_info(self):
        event = _make_heartbeat_started(connection_id=("mongo.host", 27017))
        with self.assertLogs(level="INFO") as cm:
            self.logger.started(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("INFO", cm.output[0])
        self.assertIn("mongo.host", cm.output[0])

    def test_succeeded_logs_info(self):
        event = _make_heartbeat_succeeded(
            connection_id=("mongo.host", 27017), reply_document={"ok": 1, "ismaster": True}
        )
        with self.assertLogs(level="INFO") as cm:
            self.logger.succeeded(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("INFO", cm.output[0])
        self.assertIn("mongo.host", cm.output[0])
        self.assertIn("succeeded", cm.output[0])

    def test_succeeded_includes_reply_document(self):
        reply_doc = {"ok": 1, "maxWireVersion": 17}
        event = _make_heartbeat_succeeded(reply_document=reply_doc)
        with self.assertLogs(level="INFO") as cm:
            self.logger.succeeded(event)
        self.assertIn(str(reply_doc), cm.output[0])

    def test_failed_logs_warning(self):
        error = ConnectionRefusedError("refused")
        event = _make_heartbeat_failed(connection_id=("mongo.host", 27017), reply=error)
        with self.assertLogs(level="WARNING") as cm:
            self.logger.failed(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("WARNING", cm.output[0])
        self.assertIn("mongo.host", cm.output[0])
        self.assertIn("failed", cm.output[0])

    def test_failed_includes_error(self):
        error = TimeoutError("timed out")
        event = _make_heartbeat_failed(reply=error)
        with self.assertLogs(level="WARNING") as cm:
            self.logger.failed(event)
        self.assertIn("timed out", cm.output[0])


class TestTopologyLogger(unittest.TestCase):
    def setUp(self):
        self.logger = TopologyLogger()

    def test_opened_logs_info(self):
        event = _make_topology_event(topology_id="topo-1")
        with self.assertLogs(level="INFO") as cm:
            self.logger.opened(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("INFO", cm.output[0])
        self.assertIn("topo-1", cm.output[0])
        self.assertIn("opened", cm.output[0])

    def test_closed_logs_info(self):
        event = _make_topology_event(topology_id="topo-1")
        with self.assertLogs(level="INFO") as cm:
            self.logger.closed(event)
        self.assertEqual(len(cm.output), 1)
        self.assertIn("INFO", cm.output[0])
        self.assertIn("topo-1", cm.output[0])
        self.assertIn("closed", cm.output[0])

    def test_description_changed_always_logs_update(self):
        event = _make_topology_description_changed(
            topology_id="topo-1",
            previous_type=1,
            new_type=1,
            has_writable=True,
            has_readable=True,
        )
        with self.assertLogs(level="INFO") as cm:
            self.logger.description_changed(event)
        self.assertTrue(any("updated" in msg for msg in cm.output))
        self.assertTrue(any("topo-1" in msg for msg in cm.output))

    def test_description_changed_logs_type_change(self):
        event = _make_topology_description_changed(
            topology_id="topo-2",
            previous_type=0,
            previous_type_name="Unknown",
            new_type=1,
            new_type_name="Single",
            has_writable=True,
            has_readable=True,
        )
        with self.assertLogs(level="INFO") as cm:
            self.logger.description_changed(event)
        type_change_logs = [m for m in cm.output if "Unknown" in m and "Single" in m]
        self.assertEqual(len(type_change_logs), 1)

    def test_description_changed_no_type_change_log_when_same(self):
        event = _make_topology_description_changed(
            previous_type=1,
            previous_type_name="Single",
            new_type=1,
            new_type_name="Single",
            has_writable=True,
            has_readable=True,
        )
        with self.assertLogs(level="INFO") as cm:
            self.logger.description_changed(event)
        # Only the general "updated" log, not a type-change log
        type_change_logs = [m for m in cm.output if "changed type" in m]
        self.assertEqual(len(type_change_logs), 0)

    def test_description_changed_warns_no_writable_server(self):
        event = _make_topology_description_changed(has_writable=False, has_readable=True)
        with self.assertLogs(level="WARNING") as cm:
            self.logger.description_changed(event)
        warning_logs = [m for m in cm.output if "WARNING" in m and "writable" in m]
        self.assertEqual(len(warning_logs), 1)

    def test_description_changed_warns_no_readable_server(self):
        event = _make_topology_description_changed(has_writable=True, has_readable=False)
        with self.assertLogs(level="WARNING") as cm:
            self.logger.description_changed(event)
        warning_logs = [m for m in cm.output if "WARNING" in m and "readable" in m]
        self.assertEqual(len(warning_logs), 1)

    def test_description_changed_warns_both_unavailable(self):
        event = _make_topology_description_changed(has_writable=False, has_readable=False)
        with self.assertLogs(level="WARNING") as cm:
            self.logger.description_changed(event)
        warning_logs = [m for m in cm.output if "WARNING" in m]
        self.assertEqual(len(warning_logs), 2)


class TestConnectionPoolLogger(unittest.TestCase):
    def setUp(self):
        self.logger = ConnectionPoolLogger()
        self.address = ("localhost", 27017)

    def test_pool_created(self):
        event = _make_pool_event(self.address)
        with self.assertLogs(level="INFO") as cm:
            self.logger.pool_created(event)
        self.assertIn("pool created", cm.output[0])
        self.assertIn("localhost", cm.output[0])

    def test_pool_ready(self):
        event = _make_pool_event(self.address)
        with self.assertLogs(level="INFO") as cm:
            self.logger.pool_ready(event)
        self.assertIn("pool ready", cm.output[0])
        self.assertIn("localhost", cm.output[0])

    def test_pool_cleared(self):
        event = _make_pool_event(self.address)
        with self.assertLogs(level="INFO") as cm:
            self.logger.pool_cleared(event)
        self.assertIn("pool cleared", cm.output[0])
        self.assertIn("localhost", cm.output[0])

    def test_pool_closed(self):
        event = _make_pool_event(self.address)
        with self.assertLogs(level="INFO") as cm:
            self.logger.pool_closed(event)
        self.assertIn("pool closed", cm.output[0])
        self.assertIn("localhost", cm.output[0])

    def test_connection_created(self):
        event = _make_connection_event(self.address, connection_id=5)
        with self.assertLogs(level="INFO") as cm:
            self.logger.connection_created(event)
        self.assertIn("connection created", cm.output[0])
        self.assertIn("5", cm.output[0])
        self.assertIn("localhost", cm.output[0])

    def test_connection_ready(self):
        event = _make_connection_event(self.address, connection_id=5)
        with self.assertLogs(level="INFO") as cm:
            self.logger.connection_ready(event)
        self.assertIn("connection setup succeeded", cm.output[0])
        self.assertIn("5", cm.output[0])

    def test_connection_closed(self):
        event = _make_connection_closed_event(self.address, connection_id=5, reason="stale")
        with self.assertLogs(level="INFO") as cm:
            self.logger.connection_closed(event)
        self.assertIn("connection closed", cm.output[0])
        self.assertIn("5", cm.output[0])
        self.assertIn("stale", cm.output[0])

    def test_connection_closed_reason_in_message(self):
        for reason in ("stale", "idle", "error", "poolClosed"):
            event = _make_connection_closed_event(self.address, reason=reason)
            with self.assertLogs(level="INFO") as cm:
                self.logger.connection_closed(event)
            self.assertIn(reason, cm.output[0])

    def test_connection_check_out_started(self):
        event = _make_pool_event(self.address)
        with self.assertLogs(level="INFO") as cm:
            self.logger.connection_check_out_started(event)
        self.assertIn("check out started", cm.output[0])
        self.assertIn("localhost", cm.output[0])

    def test_connection_check_out_failed(self):
        event = _make_checkout_failed_event(self.address, reason="timeout")
        with self.assertLogs(level="INFO") as cm:
            self.logger.connection_check_out_failed(event)
        self.assertIn("check out failed", cm.output[0])
        self.assertIn("timeout", cm.output[0])
        self.assertIn("localhost", cm.output[0])

    def test_connection_check_out_failed_reason_in_message(self):
        for reason in ("timeout", "poolClosed", "connectionError"):
            event = _make_checkout_failed_event(self.address, reason=reason)
            with self.assertLogs(level="INFO") as cm:
                self.logger.connection_check_out_failed(event)
            self.assertIn(reason, cm.output[0])

    def test_connection_checked_out(self):
        event = _make_connection_event(self.address, connection_id=3)
        with self.assertLogs(level="INFO") as cm:
            self.logger.connection_checked_out(event)
        self.assertIn("checked out", cm.output[0])
        self.assertIn("3", cm.output[0])
        self.assertIn("localhost", cm.output[0])

    def test_connection_checked_in(self):
        event = _make_connection_event(self.address, connection_id=3)
        with self.assertLogs(level="INFO") as cm:
            self.logger.connection_checked_in(event)
        self.assertIn("checked into", cm.output[0])
        self.assertIn("3", cm.output[0])
        self.assertIn("localhost", cm.output[0])


if __name__ == "__main__":
    unittest.main()
