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

"""Test that the asynchronous API does not block the event loop."""
from __future__ import annotations

import asyncio
import time
from test.asynchronous import AsyncIntegrationTest

from pymongo.errors import ServerSelectionTimeoutError


class TestClientLoopUnblocked(AsyncIntegrationTest):
    async def test_client_does_not_block_loop(self):
        # Use an unreachable TEST-NET host to ensure that the client times out attempting to create a connection.
        client = self.simple_client("192.0.2.1", serverSelectionTimeoutMS=500)
        latencies = []

        # If the loop is being blocked, at least one iteration will have a latency much more than 0.1 seconds
        async def background_task():
            start = time.monotonic()
            try:
                while True:
                    start = time.monotonic()
                    await asyncio.sleep(0.1)
                    latencies.append(time.monotonic() - start)
            except asyncio.CancelledError:
                latencies.append(time.monotonic() - start)
                raise

        t = asyncio.create_task(background_task())

        with self.assertRaisesRegex(ServerSelectionTimeoutError, "No servers found yet"):
            await client.admin.command("ping")

        t.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await t

        self.assertLessEqual(
            sorted(latencies, reverse=True)[0],
            1.0,
            "Background task was blocked from running",
        )
