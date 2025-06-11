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

"""Test that AsyncPeriodicExecutors do not copy ContextVars from their parents."""
from __future__ import annotations

import asyncio
import sys
from test.asynchronous.utils import async_get_pool
from test.utils_shared import delay, one

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest


class TestAsyncContextVarsReset(AsyncIntegrationTest):
    async def test_context_vars_are_reset_in_executor(self):
        if sys.version_info < (3, 12):
            self.skipTest("Test requires asyncio.Task.get_context (added in Python 3.12)")

        await self.client.db.test.insert_one({"x": 1})
        for server in self.client._topology._servers.values():
            for context in [
                c
                for c in server._monitor._executor._task.get_context()
                if c.name in ["TIMEOUT", "RTT", "DEADLINE"]
            ]:
                self.assertIn(context.get(), [None, float("inf"), 0.0])
        await self.client.db.test.delete_many({})
