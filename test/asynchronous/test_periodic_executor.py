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
import gc
import sys
import threading
import time

sys.path[0:0] = [""]

from test.asynchronous import AsyncUnitTest, unittest

from pymongo import periodic_executor
from pymongo.periodic_executor import (
    AsyncPeriodicExecutor,
    _register_executor,
    _shutdown_executors,
)

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
        self.executor = _make_executor()

    async def asyncTearDown(self):
        self.executor.close()
        await self.executor.join(timeout=2)


class TestAsyncPeriodicExecutorRepr(AsyncUnitTest):
    async def test_repr_contains_class_and_name(self):
        executor = _make_executor(name="exec")
        executor_repr = repr(executor)
        self.assertIn("AsyncPeriodicExecutor", executor_repr)
        self.assertIn("exec", executor_repr)


class TestAsyncPeriodicExecutorBasic(AsyncPeriodicExecutorTestBase):
    async def test_wake_sets_event(self):
        self.assertFalse(self.executor._event)
        self.executor.wake()
        self.assertTrue(self.executor._event)

    async def test_update_interval(self):
        self.executor.update_interval(60)
        self.assertEqual(self.executor._interval, 60)

    async def test_skip_sleep(self):
        self.assertFalse(self.executor._skip_sleep)
        self.executor.skip_sleep()
        self.assertTrue(self.executor._skip_sleep)


class TestAsyncPeriodicExecutorLifecycle(AsyncPeriodicExecutorTestBase):
    async def test_open_starts_worker(self):
        self.executor.open()
        if _IS_SYNC:
            self.assertIsNotNone(self.executor._thread)
            self.assertTrue(self.executor._thread.is_alive())
        else:
            self.assertIsNotNone(self.executor._task)

    async def test_close_sets_stopped(self):
        self.executor.open()
        self.executor.close()
        self.assertTrue(self.executor._stopped)
        await self.executor.join(timeout=1)

    async def test_join_without_open_is_safe(self):
        await self.executor.join(timeout=0.01)

    async def test_multiple_open_calls_have_no_effect(self):
        self.executor.open()
        if _IS_SYNC:
            worker_id = id(self.executor._thread)
        else:
            worker_id = id(self.executor._task)
        self.executor.open()
        if _IS_SYNC:
            self.assertEqual(worker_id, id(self.executor._thread))
        else:
            self.assertEqual(worker_id, id(self.executor._task))


class TestAsyncPeriodicExecutorTarget(AsyncPeriodicExecutorTestBase):
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
        if _IS_SYNC:
            self.assertTrue(ran.wait(timeout=2), "target never ran")
        else:
            await asyncio.wait_for(ran.wait(), timeout=2)
        await self.executor.join(timeout=2)
        self.assertTrue(self.executor._stopped)

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
                    raise RuntimeError("boom")

                self.executor = _make_executor(target=target)
                self.executor.open()
                self.assertTrue(ran.wait(timeout=2), "target never ran")
                self.executor.join(timeout=2)
            finally:
                threading.excepthook = orig_excepthook
            self.assertTrue(self.executor._stopped)
            self.assertEqual(len(captured_exc), 1)
            self.assertIsInstance(captured_exc[0], RuntimeError)
        else:
            ran = asyncio.Event()

            async def target():
                ran.set()
                raise RuntimeError("async boom")

            self.executor = _make_executor(target=target)
            self.executor.open()
            await asyncio.wait_for(ran.wait(), timeout=2)
            await self.executor.join(timeout=2)
            self.assertTrue(self.executor._stopped)
            if self.executor._task is not None and self.executor._task.done():
                self.executor._task.exception()

    async def test_skip_sleep_flag_skips_interval(self):
        call_times = []

        async def target():
            call_times.append(time.monotonic() if _IS_SYNC else asyncio.get_running_loop().time())
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
        call_count = [0]
        if _IS_SYNC:
            woken = threading.Event()
        else:
            woken = asyncio.Event()

        async def target():
            call_count[0] += 1
            if call_count[0] == 1:
                woken.set()
            if call_count[0] >= 2:
                return False
            return True

        self.executor = _make_executor(interval=30.0, min_interval=0.01, target=target)
        self.executor.open()
        if _IS_SYNC:
            woken.wait(timeout=2)
        else:
            await asyncio.wait_for(woken.wait(), timeout=2)
        self.executor.wake()
        await self.executor.join(timeout=3)
        self.assertGreaterEqual(call_count[0], 2)

    async def test_open_after_target_returns_false(self):
        called = [0]

        async def target():
            called[0] += 1
            return False

        self.executor = _make_executor(target=target)
        self.executor.open()
        await self.executor.join(timeout=2)
        self.assertTrue(self.executor._stopped)
        if not _IS_SYNC:
            first_task = self.executor._task
        self.executor.open()
        await self.executor.join(timeout=2)
        self.assertGreaterEqual(called[0], 2)
        if not _IS_SYNC:
            self.assertIsNot(self.executor._task, first_task)


class TestShouldStop(AsyncUnitTest):
    if _IS_SYNC:

        def test_returns_false_when_not_stopped(self):
            executor = _make_executor()
            self.assertFalse(executor._should_stop())
            self.assertFalse(executor._thread_will_exit)

        def test_returns_true_and_sets_thread_will_exit(self):
            executor = _make_executor()
            executor._stopped = True
            self.assertTrue(executor._should_stop())
            self.assertTrue(executor._thread_will_exit)


class TestRegisterExecutor(AsyncUnitTest):
    if _IS_SYNC:

        def setUp(self):
            self._orig = set(periodic_executor._EXECUTORS)

        def tearDown(self):
            periodic_executor._EXECUTORS.clear()
            periodic_executor._EXECUTORS.update(self._orig)

        def test_register_adds_weakref(self):
            executor = _make_executor()
            before = len(periodic_executor._EXECUTORS)
            _register_executor(executor)
            self.assertEqual(len(periodic_executor._EXECUTORS), before + 1)
            ref = next(r for r in periodic_executor._EXECUTORS if r() is executor)
            del executor
            gc.collect()
            self.assertNotIn(ref, periodic_executor._EXECUTORS)

        def test_shutdown_executors_stops_running_executors(self):
            ran = threading.Event()

            def target():
                ran.set()
                return True

            executor = _make_executor(target=target)
            executor.open()
            self.assertTrue(ran.wait(timeout=2), "target never ran")
            _shutdown_executors()
            executor.join(timeout=2)
            self.assertTrue(executor._stopped)

        def test_shutdown_executors_safe_when_empty(self):
            periodic_executor._EXECUTORS.clear()
            _shutdown_executors()


if __name__ == "__main__":
    unittest.main()
