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

"""Async-only unit tests for AsyncPeriodicExecutor."""

from __future__ import annotations

import sys

sys.path[0:0] = [""]

from test.asynchronous import AsyncUnitTest, unittest

from pymongo.periodic_executor import AsyncPeriodicExecutor


class TestAsyncPeriodicExecutorExceptions(AsyncUnitTest):
    async def test_target_exception_stops_executor(self):
        call_count = 0

        async def target():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("error")

        executor = AsyncPeriodicExecutor(
            interval=30.0, min_interval=0.01, target=target, name="test"
        )
        executor.open()
        await executor.join(timeout=2)
        if executor._task is not None and executor._task.done():
            executor._task.exception()
        self.assertEqual(call_count, 1, "target should stop after exception")


if __name__ == "__main__":
    unittest.main()
