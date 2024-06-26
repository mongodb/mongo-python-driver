# Copyright 2023-present MongoDB, Inc.
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
from __future__ import annotations

import os
from test import unittest
from test.test_client import IntegrationTest
from test.utils import single_client
from unittest.mock import patch

from bson import json_util
from pymongo.errors import OperationFailure
from pymongo.logger import _DEFAULT_DOCUMENT_LENGTH


# https://github.com/mongodb/specifications/tree/master/source/command-logging-and-monitoring/tests#prose-tests
class TestLogger(IntegrationTest):
    def test_default_truncation_limit(self):
        docs = [{"x": "y"} for _ in range(100)]
        db = self.db

        with patch.dict("os.environ"):
            os.environ.pop("MONGOB_LOG_MAX_DOCUMENT_LENGTH", None)
            with self.assertLogs("pymongo.command", level="DEBUG") as cm:
                db.test.insert_many(docs)

                cmd_started_log = json_util.loads(cm.records[0].message)
                self.assertEqual(len(cmd_started_log["command"]), _DEFAULT_DOCUMENT_LENGTH + 3)

                cmd_succeeded_log = json_util.loads(cm.records[1].message)
                self.assertLessEqual(len(cmd_succeeded_log["reply"]), _DEFAULT_DOCUMENT_LENGTH + 3)

            with self.assertLogs("pymongo.command", level="DEBUG") as cm:
                list(db.test.find({}))
                cmd_succeeded_log = json_util.loads(cm.records[1].message)
                self.assertEqual(len(cmd_succeeded_log["reply"]), _DEFAULT_DOCUMENT_LENGTH + 3)

    def test_configured_truncation_limit(self):
        cmd = {"hello": True}
        db = self.db
        with patch.dict("os.environ", {"MONGOB_LOG_MAX_DOCUMENT_LENGTH": "5"}):
            with self.assertLogs("pymongo.command", level="DEBUG") as cm:
                db.command(cmd)

                cmd_started_log = json_util.loads(cm.records[0].message)
                self.assertEqual(len(cmd_started_log["command"]), 5 + 3)

                cmd_succeeded_log = json_util.loads(cm.records[1].message)
                self.assertLessEqual(len(cmd_succeeded_log["reply"]), 5 + 3)
                with self.assertRaises(OperationFailure):
                    db.command({"notARealCommand": True})
                cmd_failed_log = json_util.loads(cm.records[-1].message)
                self.assertEqual(len(cmd_failed_log["failure"]), 5 + 3)

    def test_truncation_multi_byte_codepoints(self):
        document_lengths = ["20000", "20001", "20002"]
        multi_byte_char_str_len = 50_000
        str_to_repeat = "界"

        multi_byte_char_str = ""
        for i in range(multi_byte_char_str_len):
            multi_byte_char_str += str_to_repeat

        for length in document_lengths:
            with patch.dict("os.environ", {"MONGOB_LOG_MAX_DOCUMENT_LENGTH": length}):
                with self.assertLogs("pymongo.command", level="DEBUG") as cm:
                    self.db.test.insert_one({"x": multi_byte_char_str})
                    cmd_started_log = json_util.loads(cm.records[0].message)["command"]

                    cmd_started_log = cmd_started_log[:-3]
                    last_3_bytes = cmd_started_log.encode()[-3:].decode()

                    self.assertEqual(last_3_bytes, str_to_repeat)

    def test_logging_without_listeners(self):
        c = single_client()
        self.assertEqual(len(c._event_listeners.event_listeners()), 0)
        with self.assertLogs("pymongo.connection", level="DEBUG") as cm:
            c.db.test.insert_one({"x": "1"})
            self.assertGreater(len(cm.records), 0)
        with self.assertLogs("pymongo.command", level="DEBUG") as cm:
            c.db.test.insert_one({"x": "1"})
            self.assertGreater(len(cm.records), 0)
        with self.assertLogs("pymongo.serverSelection", level="DEBUG") as cm:
            c.db.test.insert_one({"x": "1"})
            self.assertGreater(len(cm.records), 0)


if __name__ == "__main__":
    unittest.main()
