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

"""Unit tests for periodic_executor.py."""

from __future__ import annotations

import asyncio
import sys
import threading
import time

sys.path[0:0] = [""]

from test.asynchronous import AsyncUnitTest, unittest

from pymongo.periodic_executor import AsyncPeriodicExecutor

_IS_SYNC = False


def _make_executor(interval=30.0, min_interval=0.01, target=None, name="test"):
    if target is None:

        async def target():
            return True

    return AsyncPeriodicExecutor(
        interval=interval, min_interval=min_interval, target=target, name=name
    )


class AsyncPeriodicExecutorTestBase(AsyncUnitTest):
    async def asyncSetUp(self):
        self.executor = None

    async def asyncTearDown(self):
        if self.executor is not None:
            self.executor.close()
            await self.executor.join(timeout=2)


class TestAsyncPeriodicExecutor(AsyncPeriodicExecutorTestBase):
    async def test_repr_contains_class_and_name(self):
        executor = _make_executor(name="exec")
        executor_repr = repr(executor)
        self.assertIn("AsyncPeriodicExecutor", executor_repr)
        self.assertIn("exec", executor_repr)

    async def test_join_without_open_is_safe(self):
        self.executor = _make_executor()
        try:
            await self.executor.join(timeout=0.01)
        except Exception as e:
            self.fail(f"join() raised unexpected Exception {e}")

    async def test_target_returning_false_stops_executor(self):
        if _IS_SYNC:
            ran = threading.Event()
        else:
            ran = asyncio.Event()

        async def target():
            ran.set()
            return False

        self.executor = _make_executor(target=target)
        self.executor.open()
        await self.executor.join(timeout=2)
        self.assertTrue(ran.is_set(), "target never ran")

    async def test_target_exception_stops_executor(self):
        if _IS_SYNC:
            ran = threading.Event()
            captured_exc: list = []
            orig_excepthook = threading.excepthook

            def _capture_excepthook(args):
                captured_exc.append(args.exc_value)

            threading.excepthook = _capture_excepthook
            try:

                def target():
                    ran.set()
                    raise RuntimeError("error")

                self.executor = _make_executor(target=target)
                self.executor.open()
                self.executor.join(timeout=2)
                self.assertTrue(ran.is_set(), "target never ran")
            finally:
                threading.excepthook = orig_excepthook
            self.assertEqual(len(captured_exc), 1)
            self.assertIsInstance(captured_exc[0], RuntimeError)
        else:
            call_count = 0

            async def target():
                nonlocal call_count
                call_count += 1
                raise RuntimeError("error")

            self.executor = _make_executor(target=target)
            self.executor.open()
            await self.executor.join(timeout=2)
            self.assertEqual(call_count, 1, "target should stop after exception")

    async def test_skip_sleep_flag_skips_interval(self):
        call_times = []

        async def target():
            call_times.append(time.monotonic())
            if len(call_times) >= 2:
                return False
            return True

        self.executor = _make_executor(interval=30.0, min_interval=0.001, target=target)
        self.executor.skip_sleep()
        self.executor.open()
        await self.executor.join(timeout=3)
        self.assertGreaterEqual(len(call_times), 2)
        self.assertLess(call_times[1] - call_times[0], 5.0)

    async def test_wake_causes_early_run(self):
        call_count = 0
        if _IS_SYNC:
            woken = threading.Event()
        else:
            woken = asyncio.Event()

        async def target():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                woken.set()
            return call_count < 2

        self.executor = _make_executor(interval=30.0, min_interval=0.01, target=target)
        self.executor.open()
        if _IS_SYNC:
            woken.wait(timeout=2)
        else:
            await asyncio.wait_for(woken.wait(), timeout=2)
        self.executor.wake()
        await self.executor.join(timeout=3)
        self.assertGreaterEqual(call_count, 2)

    async def test_open_after_target_returns_false(self):
        called = 0

        async def target():
            nonlocal called
            called += 1
            return False

        self.executor = _make_executor(target=target)
        self.executor.open()
        await self.executor.join(timeout=2)
        self.executor.open()
        await self.executor.join(timeout=2)
        self.assertGreaterEqual(called, 2)


if __name__ == "__main__":
    unittest.main()
