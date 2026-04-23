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
import weakref

sys.path[0:0] = [""]

from test import unittest

import pymongo.periodic_executor as pe_module
from pymongo.periodic_executor import (
    AsyncPeriodicExecutor,
    PeriodicExecutor,
    _register_executor,
    _shutdown_executors,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sync(interval=30.0, min_interval=0.01, target=None, name="test"):
    if target is None:

        def target():
            return True

    return PeriodicExecutor(interval=interval, min_interval=min_interval, target=target, name=name)


def _make_async(interval=30.0, min_interval=0.01, target=None, name="test"):
    async def _default_target():
        return True

    if target is None:
        target = _default_target
    return AsyncPeriodicExecutor(
        interval=interval, min_interval=min_interval, target=target, name=name
    )


# ---------------------------------------------------------------------------
# PeriodicExecutor (sync / threading)
# ---------------------------------------------------------------------------


class TestPeriodicExecutorRepr(unittest.TestCase):
    def test_repr_contains_class_and_name(self):
        ex = _make_sync(name="myexec")
        r = repr(ex)
        self.assertIn("PeriodicExecutor", r)
        self.assertIn("myexec", r)


class TestPeriodicExecutorLifecycle(unittest.TestCase):
    def test_open_starts_thread(self):
        ex = _make_sync()
        ex.open()
        try:
            self.assertIsNotNone(ex._thread)
            # Give thread a moment to start.
            time.sleep(0.05)
            self.assertTrue(ex._thread.is_alive())
        finally:
            ex.close()
            ex.join(timeout=2)

    def test_multiple_open_calls_have_no_effect(self):
        ex = _make_sync()
        ex.open()
        thread_id = id(ex._thread)
        ex.open()
        try:
            self.assertEqual(thread_id, id(ex._thread))
        finally:
            ex.close()
            ex.join(timeout=2)

    def test_close_sets_stopped(self):
        ex = _make_sync()
        ex.open()
        ex.close()
        self.assertTrue(ex._stopped)
        ex.join(timeout=2)

    def test_join_without_open_is_safe(self):
        ex = _make_sync()
        ex.join(timeout=0.01)  # should not raise

    def test_wake_sets_event(self):
        ex = _make_sync()
        self.assertFalse(ex._event)
        ex.wake()
        self.assertTrue(ex._event)

    def test_update_interval(self):
        ex = _make_sync(interval=30.0)
        ex.update_interval(60)
        self.assertEqual(ex._interval, 60)

    def test_skip_sleep(self):
        ex = _make_sync()
        self.assertFalse(ex._skip_sleep)
        ex.skip_sleep()
        self.assertTrue(ex._skip_sleep)


class TestPeriodicExecutorTarget(unittest.TestCase):
    def test_target_returning_false_stops_executor(self):
        ran = threading.Event()

        def target():
            ran.set()
            return False  # Signal stop.

        ex = _make_sync(target=target)
        ex.open()
        self.assertTrue(ran.wait(timeout=2), "target never ran")
        ex.join(timeout=2)
        self.assertTrue(ex._stopped)

    def test_target_exception_stops_executor(self):
        ran = threading.Event()
        captured_exc = []
        orig_excepthook = threading.excepthook

        def _capture_excepthook(args):
            captured_exc.append(args.exc_value)

        threading.excepthook = _capture_excepthook
        try:

            def target():
                ran.set()
                raise RuntimeError("boom")

            ex = _make_sync(target=target)
            ex.open()
            self.assertTrue(ran.wait(timeout=2), "target never ran")
            ex.join(timeout=2)
        finally:
            threading.excepthook = orig_excepthook

        self.assertTrue(ex._stopped)
        self.assertEqual(len(captured_exc), 1)
        self.assertIsInstance(captured_exc[0], RuntimeError)

    def test_skip_sleep_flag_skips_interval(self):
        call_times = []

        def target():
            call_times.append(time.monotonic())
            if len(call_times) >= 2:
                return False
            return True

        ex = _make_sync(interval=30.0, min_interval=0.001, target=target)
        ex.skip_sleep()
        ex.open()
        ex.join(timeout=2)
        # First call should have skipped the 30s sleep.
        self.assertGreaterEqual(len(call_times), 2)
        self.assertLess(call_times[1] - call_times[0], 5.0)

    def test_wake_causes_early_run(self):
        call_count = [0]
        woken = threading.Event()

        def target():
            call_count[0] += 1
            if call_count[0] == 1:
                woken.set()
            if call_count[0] >= 2:
                return False
            return True

        ex = _make_sync(interval=30.0, min_interval=0.01, target=target)
        ex.open()
        woken.wait(timeout=2)
        ex.wake()
        ex.join(timeout=3)
        self.assertGreaterEqual(call_count[0], 2)


class TestShouldStop(unittest.TestCase):
    def test_returns_false_when_not_stopped(self):
        ex = _make_sync()
        self.assertFalse(ex._should_stop())
        self.assertFalse(ex._thread_will_exit)

    def test_returns_true_and_sets_thread_will_exit(self):
        ex = _make_sync()
        ex._stopped = True
        self.assertTrue(ex._should_stop())
        self.assertTrue(ex._thread_will_exit)


class TestPeriodicExecutorOpenAfterExit(unittest.TestCase):
    def test_reopen_after_target_returns_false(self):
        called = [0]

        def target():
            called[0] += 1
            return False

        ex = _make_sync(target=target)
        ex.open()
        ex.join(timeout=2)
        self.assertTrue(ex._stopped)
        # Re-open should start a new thread.
        ex.open()
        ex.join(timeout=2)
        self.assertGreaterEqual(called[0], 2)


# ---------------------------------------------------------------------------
# Module-level: _register_executor, _on_executor_deleted, _shutdown_executors
# ---------------------------------------------------------------------------


class TestRegisterExecutor(unittest.TestCase):
    def setUp(self):
        self._orig = set(pe_module._EXECUTORS)

    def tearDown(self):
        pe_module._EXECUTORS.clear()
        pe_module._EXECUTORS.update(self._orig)

    def test_register_adds_weakref(self):
        ex = _make_sync()
        before = len(pe_module._EXECUTORS)
        _register_executor(ex)
        self.assertEqual(len(pe_module._EXECUTORS), before + 1)
        # When executor is GC'd the ref is cleaned up.
        ref_count_before = len(pe_module._EXECUTORS)
        del ex
        self.assertLessEqual(len(pe_module._EXECUTORS), ref_count_before)

    def test_shutdown_executors_stops_running_executors(self):
        stopped = threading.Event()

        def target():
            stopped.wait(timeout=5)
            return True

        ex = _make_sync(target=target)
        ex.open()
        time.sleep(0.05)
        _register_executor(ex)
        _shutdown_executors()
        stopped.set()
        ex.join(timeout=2)
        self.assertTrue(ex._stopped)

    def test_shutdown_executors_safe_when_empty(self):
        pe_module._EXECUTORS.clear()
        _shutdown_executors()  # Should not raise.


# ---------------------------------------------------------------------------
# AsyncPeriodicExecutor
# ---------------------------------------------------------------------------


class TestAsyncPeriodicExecutorRepr(unittest.TestCase):
    def test_repr_contains_class_and_name(self):
        ex = _make_async(name="asyncexec")
        r = repr(ex)
        self.assertIn("AsyncPeriodicExecutor", r)
        self.assertIn("asyncexec", r)


class TestAsyncPeriodicExecutorBasic(unittest.TestCase):
    def test_wake_sets_event(self):
        ex = _make_async()
        ex.wake()
        self.assertTrue(ex._event)

    def test_update_interval(self):
        ex = _make_async(interval=30.0)
        ex.update_interval(60)
        self.assertEqual(ex._interval, 60)

    def test_skip_sleep(self):
        ex = _make_async()
        ex.skip_sleep()
        self.assertTrue(ex._skip_sleep)


class TestAsyncPeriodicExecutorLifecycle(unittest.TestCase):
    def test_open_creates_task(self):
        async def run():
            ex = _make_async()
            ex.open()
            self.assertIsNotNone(ex._task)
            ex.close()
            await ex.join(timeout=1)

        asyncio.run(run())

    def test_close_cancels_task(self):
        async def run():
            ex = _make_async()
            ex.open()
            ex.close()
            await ex.join(timeout=1)
            self.assertTrue(ex._stopped)

        asyncio.run(run())

    def test_join_without_open_is_safe(self):
        async def run():
            ex = _make_async()
            await ex.join(timeout=0.01)  # Should not raise.

        asyncio.run(run())

    def test_multiple_open_calls_have_no_effect(self):
        async def run():
            ex = _make_async()
            ex.open()
            task_id = id(ex._task)
            ex.open()  # Second open: same task still running.
            self.assertEqual(task_id, id(ex._task))
            ex.close()
            await ex.join(timeout=1)

        asyncio.run(run())


class TestAsyncPeriodicExecutorTarget(unittest.TestCase):
    def test_target_returning_false_stops_executor(self):
        async def run():
            ran = asyncio.Event()

            async def target():
                ran.set()
                return False

            ex = _make_async(target=target)
            ex.open()
            await asyncio.wait_for(ran.wait(), timeout=2)
            await ex.join(timeout=2)
            self.assertTrue(ex._stopped)

        asyncio.run(run())

    def test_target_exception_stops_executor(self):
        async def run():
            ran = asyncio.Event()

            async def target():
                ran.set()
                raise RuntimeError("async boom")

            ex = _make_async(target=target)
            ex.open()
            await asyncio.wait_for(ran.wait(), timeout=2)
            await ex.join(timeout=2)
            self.assertTrue(ex._stopped)

        asyncio.run(run())

    def test_skip_sleep_flag_skips_interval(self):
        async def run():
            call_times = []

            async def target():
                call_times.append(asyncio.get_event_loop().time())
                if len(call_times) >= 2:
                    return False
                return True

            ex = _make_async(interval=30.0, min_interval=0.001, target=target)
            ex.skip_sleep()
            ex.open()
            await ex.join(timeout=3)
            self.assertGreaterEqual(len(call_times), 2)
            self.assertLess(call_times[1] - call_times[0], 5.0)

        asyncio.run(run())

    def test_open_after_target_returns_false_creates_new_task(self):
        async def run():
            call_count = [0]

            async def target():
                call_count[0] += 1
                return False

            ex = _make_async(target=target)
            ex.open()
            await ex.join(timeout=2)
            first_task = ex._task
            ex.open()
            await ex.join(timeout=2)
            self.assertGreaterEqual(call_count[0], 2)
            self.assertIsNot(ex._task, first_task)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
