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

"""Test the sans-I/O SDAM monitor decision helpers.

These tests exercise pymongo._sdam_monitor directly. They perform no I/O and
require no MongoDB server.
"""

from __future__ import annotations

import sys

sys.path[0:0] = [""]

from pymongo._sdam_monitor import is_streaming_check
from test import unittest


class _FakeSD:
    """Minimal stand-in for ServerDescription; is_streaming_check only reads
    is_server_type_known and topology_version."""

    def __init__(self, is_server_type_known, topology_version):
        self.is_server_type_known = is_server_type_known
        self.topology_version = topology_version


_TV = {"processId": "p", "counter": 1}


class TestIsStreamingCheck(unittest.TestCase):
    def test_streaming_when_enabled_and_type_and_tv_known(self):
        self.assertTrue(is_streaming_check(True, _FakeSD(True, _TV)))

    def test_not_streaming_when_disabled(self):
        self.assertFalse(is_streaming_check(False, _FakeSD(True, _TV)))

    def test_not_streaming_when_type_unknown(self):
        self.assertFalse(is_streaming_check(True, _FakeSD(False, _TV)))

    def test_not_streaming_without_topology_version(self):
        self.assertFalse(is_streaming_check(True, _FakeSD(True, None)))

    def test_returns_bool(self):
        # Guard against leaking the truthy topology_version mapping.
        result = is_streaming_check(True, _FakeSD(True, _TV))
        self.assertIsInstance(result, bool)


if __name__ == "__main__":
    unittest.main()
