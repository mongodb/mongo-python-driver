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

from test import UnitTest, unittest

from pymongo import periodic_executor
from pymongo.periodic_executor import (
    PeriodicExecutor,
    _register_executor,
    _shutdown_executors,
)

_IS_SYNC = True


def _make_executor(interval=30.0, min_interval=0.01, target=None, name="test"):
    if target is None:

        def target():
            return True

    return PeriodicExecutor(interval=interval, min_interval=min_interval, target=target, name=name)


class PeriodicExecutorTestBase(UnitTest):
    def setUp(self):
        self.ex = _make_executor()

    def tearDown(self):
        self.ex.close()
        self.ex.join(timeout=2)


class TestPeriodicExecutorRepr(UnitTest):
    def test_repr_contains_class_and_name(self):
        ex = _make_executor(name="exec")
        r = repr(ex)
        self.assertIn("PeriodicExecutor", r)
        self.assertIn("exec", r)


class TestPeriodicExecutorBasic(PeriodicExecutorTestBase):
    def test_wake_sets_event(self):
        self.assertFalse(self.ex._event)
        self.ex.wake()
        self.assertTrue(self.ex._event)

    def test_update_interval(self):
        self.ex.update_interval(60)
        self.assertEqual(self.ex._interval, 60)

    def test_skip_sleep(self):
        self.assertFalse(self.ex._skip_sleep)
        self.ex.skip_sleep()
        self.assertTrue(self.ex._skip_sleep)


class TestPeriodicExecutorLifecycle(PeriodicExecutorTestBase):
    def test_open_starts_worker(self):
        self.ex.open()
        if _IS_SYNC:
            self.assertIsNotNone(self.ex._thread)
            self.assertTrue(self.ex._thread.is_alive())
        else:
            self.assertIsNotNone(self.ex._task)

    def test_close_sets_stopped(self):
        self.ex.open()
        self.ex.close()
        self.assertTrue(self.ex._stopped)
        self.ex.join(timeout=1)

    def test_join_without_open_is_safe(self):
        self.ex.join(timeout=0.01)

    def test_multiple_open_calls_have_no_effect(self):
        self.ex.open()
        if _IS_SYNC:
            worker_id = id(self.ex._thread)
        else:
            worker_id = id(self.ex._task)
        self.ex.open()
        if _IS_SYNC:
            self.assertEqual(worker_id, id(self.ex._thread))
        else:
            self.assertEqual(worker_id, id(self.ex._task))


class TestPeriodicExecutorTarget(PeriodicExecutorTestBase):
    def test_target_returning_false_stops_executor(self):
        if _IS_SYNC:
            ran = threading.Event()
        else:
            ran = asyncio.Event()

        def target():
            ran.set()
            return False

        self.ex = _make_executor(target=target)
        self.ex.open()
        if _IS_SYNC:
            self.assertTrue(ran.wait(timeout=2), "target never ran")
        else:
            asyncio.wait_for(ran.wait(), timeout=2)
        self.ex.join(timeout=2)
        self.assertTrue(self.ex._stopped)

    def test_target_exception_stops_executor(self):
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

                self.ex = _make_executor(target=target)
                self.ex.open()
                self.assertTrue(ran.wait(timeout=2), "target never ran")
                self.ex.join(timeout=2)
            finally:
                threading.excepthook = orig_excepthook
            self.assertTrue(self.ex._stopped)
            self.assertEqual(len(captured_exc), 1)
            self.assertIsInstance(captured_exc[0], RuntimeError)
        else:
            ran = asyncio.Event()

            def target():
                ran.set()
                raise RuntimeError("async boom")

            self.ex = _make_executor(target=target)
            self.ex.open()
            asyncio.wait_for(ran.wait(), timeout=2)
            self.ex.join(timeout=2)
            self.assertTrue(self.ex._stopped)
            if self.ex._task is not None and self.ex._task.done():
                self.ex._task.exception()

    def test_skip_sleep_flag_skips_interval(self):
        call_times = []

        def target():
            call_times.append(time.monotonic() if _IS_SYNC else asyncio.get_running_loop().time())
            if len(call_times) >= 2:
                return False
            return True

        self.ex = _make_executor(interval=30.0, min_interval=0.001, target=target)
        self.ex.skip_sleep()
        self.ex.open()
        self.ex.join(timeout=3)
        self.assertGreaterEqual(len(call_times), 2)
        self.assertLess(call_times[1] - call_times[0], 5.0)

    def test_wake_causes_early_run(self):
        call_count = [0]
        if _IS_SYNC:
            woken = threading.Event()
        else:
            woken = asyncio.Event()

        def target():
            call_count[0] += 1
            if call_count[0] == 1:
                woken.set()
            if call_count[0] >= 2:
                return False
            return True

        self.ex = _make_executor(interval=30.0, min_interval=0.01, target=target)
        self.ex.open()
        if _IS_SYNC:
            woken.wait(timeout=2)
        else:
            asyncio.wait_for(woken.wait(), timeout=2)
        self.ex.wake()
        self.ex.join(timeout=3)
        self.assertGreaterEqual(call_count[0], 2)

    def test_open_after_target_returns_false(self):
        called = [0]

        def target():
            called[0] += 1
            return False

        self.ex = _make_executor(target=target)
        self.ex.open()
        self.ex.join(timeout=2)
        self.assertTrue(self.ex._stopped)
        if not _IS_SYNC:
            first_task = self.ex._task
        self.ex.open()
        self.ex.join(timeout=2)
        self.assertGreaterEqual(called[0], 2)
        if not _IS_SYNC:
            self.assertIsNot(self.ex._task, first_task)


class TestShouldStop(UnitTest):
    if _IS_SYNC:

        def test_returns_false_when_not_stopped(self):
            ex = _make_executor()
            self.assertFalse(ex._should_stop())
            self.assertFalse(ex._thread_will_exit)

        def test_returns_true_and_sets_thread_will_exit(self):
            ex = _make_executor()
            ex._stopped = True
            self.assertTrue(ex._should_stop())
            self.assertTrue(ex._thread_will_exit)


class TestRegisterExecutor(UnitTest):
    if _IS_SYNC:

        def setUp(self):
            self._orig = set(periodic_executor._EXECUTORS)

        def tearDown(self):
            periodic_executor._EXECUTORS.clear()
            periodic_executor._EXECUTORS.update(self._orig)

        def test_register_adds_weakref(self):
            ex = _make_executor()
            before = len(periodic_executor._EXECUTORS)
            _register_executor(ex)
            self.assertEqual(len(periodic_executor._EXECUTORS), before + 1)
            ref = next(r for r in periodic_executor._EXECUTORS if r() is ex)
            del ex
            gc.collect()
            self.assertNotIn(ref, periodic_executor._EXECUTORS)

        def test_shutdown_executors_stops_running_executors(self):
            ran = threading.Event()

            def target():
                ran.set()
                return True

            ex = _make_executor(target=target)
            ex.open()
            self.assertTrue(ran.wait(timeout=2), "target never ran")
            _shutdown_executors()
            ex.join(timeout=2)
            self.assertTrue(ex._stopped)

        def test_shutdown_executors_safe_when_empty(self):
            periodic_executor._EXECUTORS.clear()
            _shutdown_executors()


if __name__ == "__main__":
    unittest.main()
