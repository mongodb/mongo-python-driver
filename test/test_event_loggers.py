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
from unittest.mock import MagicMock, patch

sys.path[0:0] = [""]

from test import unittest

from pymongo.event_loggers import (
    CommandLogger,
    ConnectionPoolLogger,
    HeartbeatLogger,
    ServerLogger,
    TopologyLogger,
)


class TestCommandLogger(unittest.TestCase):
    def setUp(self):
        self.logger = CommandLogger()

    def test_started_logs_info(self):
        event = MagicMock()
        event.command_name = "find"
        event.request_id = 42
        event.connection_id = ("localhost", 27017)
        with self.assertLogs(level="INFO") as logs:
            self.logger.started(event)
        log = logs.records[0].getMessage()
        self.assertIn("find", log)
        self.assertIn("42", log)
        self.assertIn("started", log)

    def test_succeeded_logs_info(self):
        event = MagicMock()
        event.command_name = "insert"
        event.request_id = 7
        event.connection_id = ("localhost", 27017)
        event.duration_micros = 500
        with self.assertLogs(level="INFO") as logs:
            self.logger.succeeded(event)
        log = logs.records[0].getMessage()
        self.assertIn("insert", log)
        self.assertIn("7", log)
        self.assertIn("500", log)
        self.assertIn("microseconds", log)
        self.assertIn("succeeded", log)

    def test_failed_logs_info(self):
        event = MagicMock()
        event.command_name = "delete"
        event.request_id = 3
        event.connection_id = ("localhost", 27017)
        event.duration_micros = 300
        with self.assertLogs(level="INFO") as logs:
            self.logger.failed(event)
        log = logs.records[0].getMessage()
        self.assertIn("delete", log)
        self.assertIn("3", log)
        self.assertIn("300", log)
        self.assertIn("microseconds", log)
        self.assertIn("failed", log)


class TestServerLogger(unittest.TestCase):
    def setUp(self):
        self.logger = ServerLogger()

    def test_opened_logs_info(self):
        event = MagicMock()
        event.server_address = ("host1", 27017)
        event.topology_id = "topology-abc"
        with self.assertLogs(level="INFO") as logs:
            self.logger.opened(event)
        log = logs.records[0].getMessage()
        self.assertIn("host1", log)
        self.assertIn("topology-abc", log)

    def test_closed_logs_warning(self):
        event = MagicMock()
        event.server_address = ("host1", 27017)
        event.topology_id = "topology-abc"
        with self.assertLogs(level="WARNING") as logs:
            self.logger.closed(event)
        log = logs.records[0].getMessage()
        self.assertIn("host1", log)
        self.assertIn("topology-abc", log)

    def test_description_changed_logs_when_type_changes(self):
        event = MagicMock()
        event.server_address = ("host1", 27017)
        event.previous_description.server_type = 1
        event.previous_description.server_type_name = "Unknown"
        event.new_description.server_type = 2
        event.new_description.server_type_name = "Standalone"
        with self.assertLogs(level="INFO") as logs:
            self.logger.description_changed(event)
        log = logs.records[0].getMessage()
        self.assertIn("Unknown", log)
        self.assertIn("Standalone", log)

    def test_description_changed_no_log_when_type_same(self):
        event = MagicMock()
        event.previous_description.server_type = 2
        event.new_description.server_type = 2
        with patch("logging.info") as mock_info:
            self.logger.description_changed(event)
        mock_info.assert_not_called()


class TestHeartbeatLogger(unittest.TestCase):
    def setUp(self):
        self.logger = HeartbeatLogger()

    def test_started_logs_info(self):
        event = MagicMock()
        event.connection_id = ("mongo.host", 27017)
        with self.assertLogs(level="INFO") as logs:
            self.logger.started(event)
        log = logs.records[0].getMessage()
        self.assertIn("mongo.host", log)

    def test_succeeded_logs_info(self):
        event = MagicMock()
        event.connection_id = ("mongo.host", 27017)
        event.reply.document = {"ok": 1, "maxWireVersion": 17}
        with self.assertLogs(level="INFO") as logs:
            self.logger.succeeded(event)
        log = logs.records[0].getMessage()
        self.assertIn("mongo.host", log)
        self.assertIn("succeeded", log)
        self.assertIn("maxWireVersion", log)

    def test_failed_logs_warning(self):
        event = MagicMock()
        event.connection_id = ("mongo.host", 27017)
        event.reply = TimeoutError("timed out")
        with self.assertLogs(level="WARNING") as logs:
            self.logger.failed(event)
        log = logs.records[0].getMessage()
        self.assertIn("mongo.host", log)
        self.assertIn("failed", log)
        self.assertIn("timed out", log)


class TestTopologyLogger(unittest.TestCase):
    def setUp(self):
        self.logger = TopologyLogger()

    def test_opened_logs_info(self):
        event = MagicMock()
        event.topology_id = "topo-1"
        with self.assertLogs(level="INFO") as logs:
            self.logger.opened(event)
        log = logs.records[0].getMessage()
        self.assertIn("topo-1", log)
        self.assertIn("opened", log)

    def test_closed_logs_info(self):
        event = MagicMock()
        event.topology_id = "topo-1"
        with self.assertLogs(level="INFO") as logs:
            self.logger.closed(event)
        log = logs.records[0].getMessage()
        self.assertIn("topo-1", log)
        self.assertIn("closed", log)

    def test_description_changed_always_logs_update(self):
        event = MagicMock()
        event.topology_id = "topo-1"
        event.previous_description.topology_type = 1
        event.new_description.topology_type = 1
        event.new_description.has_writable_server.return_value = True
        event.new_description.has_readable_server.return_value = True
        with self.assertLogs(level="INFO") as logs:
            self.logger.description_changed(event)
        messages = [r.getMessage() for r in logs.records]
        self.assertTrue(any("updated" in m for m in messages))
        self.assertTrue(any("topo-1" in m for m in messages))

    def test_description_changed_logs_type_change(self):
        event = MagicMock()
        event.topology_id = "topo-2"
        event.previous_description.topology_type = 0
        event.previous_description.topology_type_name = "Unknown"
        event.new_description.topology_type = 1
        event.new_description.topology_type_name = "Single"
        event.new_description.has_writable_server.return_value = True
        event.new_description.has_readable_server.return_value = True
        with self.assertLogs(level="INFO") as logs:
            self.logger.description_changed(event)
        messages = [r.getMessage() for r in logs.records]
        self.assertTrue(any("Unknown" in m and "Single" in m for m in messages))

    def test_description_changed_no_type_change_log_when_same(self):
        event = MagicMock()
        event.topology_id = "topo-1"
        event.previous_description.topology_type = 1
        event.new_description.topology_type = 1
        event.new_description.has_writable_server.return_value = True
        event.new_description.has_readable_server.return_value = True
        with self.assertLogs(level="INFO") as logs:
            self.logger.description_changed(event)
        messages = [r.getMessage() for r in logs.records]
        self.assertFalse(any("changed type" in m for m in messages))

    def test_description_changed_warns_no_writable_server(self):
        event = MagicMock()
        event.previous_description.topology_type = 1
        event.new_description.topology_type = 1
        event.new_description.has_writable_server.return_value = False
        event.new_description.has_readable_server.return_value = True
        with self.assertLogs(level="WARNING") as logs:
            self.logger.description_changed(event)
        messages = [r.getMessage() for r in logs.records]
        self.assertTrue(any("writable" in m for m in messages))

    def test_description_changed_warns_no_readable_server(self):
        event = MagicMock()
        event.previous_description.topology_type = 1
        event.new_description.topology_type = 1
        event.new_description.has_writable_server.return_value = True
        event.new_description.has_readable_server.return_value = False
        with self.assertLogs(level="WARNING") as logs:
            self.logger.description_changed(event)
        messages = [r.getMessage() for r in logs.records]
        self.assertTrue(any("readable" in m for m in messages))

    def test_description_changed_warns_both_unavailable(self):
        event = MagicMock()
        event.previous_description.topology_type = 1
        event.new_description.topology_type = 1
        event.new_description.has_writable_server.return_value = False
        event.new_description.has_readable_server.return_value = False
        with self.assertLogs(level="WARNING") as logs:
            self.logger.description_changed(event)
        warning_messages = [r.getMessage() for r in logs.records if r.levelname == "WARNING"]
        self.assertEqual(len(warning_messages), 2)


class TestConnectionPoolLogger(unittest.TestCase):
    def setUp(self):
        self.logger = ConnectionPoolLogger()

    def test_pool_created(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        with self.assertLogs(level="INFO") as logs:
            self.logger.pool_created(event)
        log = logs.records[0].getMessage()
        self.assertIn("pool created", log)
        self.assertIn("localhost", log)

    def test_pool_ready(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        with self.assertLogs(level="INFO") as logs:
            self.logger.pool_ready(event)
        log = logs.records[0].getMessage()
        self.assertIn("pool ready", log)
        self.assertIn("localhost", log)

    def test_pool_cleared(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        with self.assertLogs(level="INFO") as logs:
            self.logger.pool_cleared(event)
        log = logs.records[0].getMessage()
        self.assertIn("pool cleared", log)
        self.assertIn("localhost", log)

    def test_pool_closed(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        with self.assertLogs(level="INFO") as logs:
            self.logger.pool_closed(event)
        log = logs.records[0].getMessage()
        self.assertIn("pool closed", log)
        self.assertIn("localhost", log)

    def test_connection_created(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        event.connection_id = 5
        with self.assertLogs(level="INFO") as logs:
            self.logger.connection_created(event)
        log = logs.records[0].getMessage()
        self.assertIn("connection created", log)
        self.assertIn("5", log)
        self.assertIn("localhost", log)

    def test_connection_ready(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        event.connection_id = 5
        with self.assertLogs(level="INFO") as logs:
            self.logger.connection_ready(event)
        log = logs.records[0].getMessage()
        self.assertIn("connection setup succeeded", log)
        self.assertIn("5", log)

    def test_connection_closed(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        event.connection_id = 5
        event.reason = "stale"
        with self.assertLogs(level="INFO") as logs:
            self.logger.connection_closed(event)
        log = logs.records[0].getMessage()
        self.assertIn("connection closed", log)
        self.assertIn("5", log)
        self.assertIn("stale", log)

    def test_connection_check_out_started(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        with self.assertLogs(level="INFO") as logs:
            self.logger.connection_check_out_started(event)
        log = logs.records[0].getMessage()
        self.assertIn("check out started", log)
        self.assertIn("localhost", log)

    def test_connection_check_out_failed(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        event.reason = "timeout"
        with self.assertLogs(level="INFO") as logs:
            self.logger.connection_check_out_failed(event)
        log = logs.records[0].getMessage()
        self.assertIn("check out failed", log)
        self.assertIn("timeout", log)
        self.assertIn("localhost", log)

    def test_connection_checked_out(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        event.connection_id = 3
        with self.assertLogs(level="INFO") as logs:
            self.logger.connection_checked_out(event)
        log = logs.records[0].getMessage()
        self.assertIn("checked out", log)
        self.assertIn("3", log)
        self.assertIn("localhost", log)

    def test_connection_checked_in(self):
        event = MagicMock()
        event.address = ("localhost", 27017)
        event.connection_id = 3
        with self.assertLogs(level="INFO") as logs:
            self.logger.connection_checked_in(event)
        log = logs.records[0].getMessage()
        self.assertIn("checked into", log)
        self.assertIn("3", log)
        self.assertIn("localhost", log)


if __name__ == "__main__":
    unittest.main()
