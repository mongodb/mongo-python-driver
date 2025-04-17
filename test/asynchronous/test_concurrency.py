# Copyright 2024-present MongoDB, Inc.
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

"""Tests to ensure that the async API is properly concurrent with asyncio."""
from __future__ import annotations

import asyncio
import time
from test.asynchronous import AsyncIntegrationTest, async_client_context
from test.utils_shared import delay

_IS_SYNC = False


class TestAsyncConcurrency(AsyncIntegrationTest):
    async def _task(self, client):
        await client.db.test.find_one({"$where": delay(0.20)})

    async def test_concurrency(self):
        tasks = []
        iterations = 5

        client = await self.async_single_client()
        await client.db.test.drop()
        await client.db.test.insert_one({"x": 1})

        start = time.time()

        for _ in range(iterations):
            await self._task(client)

        sequential_time = time.time() - start
        start = time.time()

        for i in range(iterations):
            tasks.append(self._task(client))

        await asyncio.gather(*tasks)
        concurrent_time = time.time() - start

        percent_faster = (sequential_time - concurrent_time) / concurrent_time * 100
        # We expect the concurrent tasks to be at least 50% faster on all platforms as a conservative benchmark
        self.assertGreaterEqual(percent_faster, 50)
