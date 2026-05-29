# Copyright 2025-present MongoDB, Inc.
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

"""Test that the asynchronous API detects event loop changes and fails correctly."""
from __future__ import annotations

import asyncio
import unittest

from pymongo import AsyncMongoClient


class TestClientLoopSafety(unittest.TestCase):
    def test_client_errors_on_different_loop(self):
        client = AsyncMongoClient()
        loop1 = asyncio.new_event_loop()
        loop1.run_until_complete(client.aconnect())
        loop2 = asyncio.new_event_loop()
        with self.assertRaisesRegex(
            RuntimeError, "Cannot use AsyncMongoClient in different event loop"
        ):
            loop2.run_until_complete(client.aconnect())
        loop1.run_until_complete(client.close())
        loop1.close()
        loop2.close()
