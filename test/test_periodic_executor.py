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
import importlib
import os
import sys
import textwrap
import threading
import time
from typing import Any
from unittest import mock

_interpreters: Any = None
if sys.version_info >= (3, 14):
    _interpreters = importlib.import_module("concurrent.interpreters")

sys.path[0:0] = [""]

import pymongo
from pymongo.periodic_executor import PeriodicExecutor
from test import UnitTest, unittest

_IS_SYNC = True

_PYMONGO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(pymongo.__file__)))


class TestPeriodicExecutor(UnitTest):
    def _make_executor(self, interval=30.0, min_interval=0.01, target=None, name="test"):
        if target is None:

            def target():
                return True

        executor = PeriodicExecutor(
            interval=interval, min_interval=min_interval, target=target, name=name
        )
        self.addCleanup(self._close_executor, executor)
        return executor

    def _close_executor(self, executor):
        executor.close()
        executor.join(timeout=2)

    def test_join_without_open_is_safe(self):
        executor = self._make_executor()
        try:
            executor.join(timeout=0.01)
        except Exception as e:
            self.fail(f"join() raised unexpected Exception {e}")

    def test_target_returning_false_stops_executor(self):
        if _IS_SYNC:
            ran = threading.Event()
        else:
            ran = asyncio.Event()

        def target():
            ran.set()
            return False

        executor = self._make_executor(target=target)
        executor.open()
        executor.join(timeout=2)
        self.assertTrue(ran.is_set(), "target never ran")

    def test_skip_sleep_flag_skips_interval(self):
        call_times = []

        def target():
            nonlocal call_times
            call_times.append(time.monotonic())
            if len(call_times) >= 2:
                return False
            return True

        executor = self._make_executor(interval=30.0, min_interval=0.001, target=target)
        executor.skip_sleep()
        executor.open()
        executor.join(timeout=3)
        self.assertGreaterEqual(len(call_times), 2)
        self.assertLess(call_times[1] - call_times[0], 5.0)

    def test_wake_causes_early_run(self):
        call_count = 0
        if _IS_SYNC:
            woken = threading.Event()
        else:
            woken = asyncio.Event()

        def target():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                woken.set()
            return call_count < 2

        executor = self._make_executor(interval=30.0, min_interval=0.01, target=target)
        executor.open()
        if _IS_SYNC:
            woken.wait(timeout=2)
        else:
            assert isinstance(woken, asyncio.Event)
            asyncio.wait_for(woken.wait(), timeout=2)
        executor.wake()
        executor.join(timeout=3)
        self.assertGreaterEqual(call_count, 2)

    def test_update_interval_changes_next_wait(self):
        call_times = []

        def target():
            nonlocal call_times
            call_times.append(time.monotonic())
            if len(call_times) == 1:
                # Shorten the interval from 30s so the next run happens promptly.
                executor.update_interval(0.05)
                return True
            return False

        executor = self._make_executor(interval=30.0, min_interval=0.01, target=target)
        executor.open()
        executor.join(timeout=3)
        self.assertGreaterEqual(len(call_times), 2)
        self.assertLess(call_times[1] - call_times[0], 5.0)

    def test_open_after_target_returns_false(self):
        called = 0

        def target():
            nonlocal called
            called += 1
            return False

        executor = self._make_executor(target=target)
        executor.open()
        executor.join(timeout=2)
        executor.open()
        executor.join(timeout=2)
        self.assertGreaterEqual(called, 2)

    def test_target_exception_stops_executor(self):
        call_count = 0

        def target():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("error")

        executor = self._make_executor(target=target)

        if _IS_SYNC:
            # The exception re-raises on the executor's background thread,
            # which would otherwise trigger threading.excepthook and print a
            # noisy traceback. Swap it for a no-op for the duration of the test.
            original_excepthook = threading.excepthook
            threading.excepthook = lambda args: None
            self.addCleanup(setattr, threading, "excepthook", original_excepthook)

        executor.open()
        executor.join(timeout=2)
        if not _IS_SYNC and executor._task is not None and executor._task.done():
            # Retrieve the exception to avoid "Task exception was never
            # retrieved" warnings when the task is garbage collected.
            executor._task.exception()
        self.assertEqual(call_count, 1, "target should stop after raising")

        # Re-opening after an exception restarts the executor. For the threaded
        # PeriodicExecutor this also exercises the _thread_will_exit join path
        # in open().
        executor.open()
        executor.join(timeout=2)
        if not _IS_SYNC and executor._task is not None and executor._task.done():
            executor._task.exception()
        self.assertEqual(call_count, 2, "executor should run again after re-open")

    def test_open_non_daemon_thread(self):
        # Subinterpreters disallow daemon threads; PeriodicExecutor.open()
        # must fall back to starting the monitor thread as a non-daemon thread.
        from pymongo.periodic_executor import PeriodicExecutor

        def set_daemon(self, value):
            raise RuntimeError("daemon threads are disallowed in subinterpreters")

        daemon = threading.Thread.daemon
        executor = PeriodicExecutor(
            interval=30.0, min_interval=0.01, target=lambda: True, name="non-daemon"
        )
        self.addCleanup(executor.join, 2)
        self.addCleanup(executor.close)
        with mock.patch.object(threading.Thread, "daemon", property(daemon.fget, set_daemon)):
            executor.open()
            thread = executor._thread
            assert thread is not None
            self.assertFalse(thread.daemon, "thread must not be a daemon thread")

    def test_subinterpreter_shutdown(self):
        if _interpreters is None:
            self.skipTest("concurrent.interpreters requires Python 3.14+")
            return

        root = _PYMONGO_ROOT
        # The async executor's open() starts a task, which requires a running
        # event loop; synchro translates the rest of the block for the sync suite.
        # Windows proactor loops cannot start in subinterpreters (set_wakeup_fd
        # is main-interpreter only), so use a selector loop there.
        run_stmt = (
            "main()"
            if _IS_SYNC
            else 'asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None)'
        )
        # The sync monitor signals its thread through the event; the async
        # monitor yields to its loop instead. Synchro drops the async yield.
        wait_stmt = "assert started.wait(10)" if _IS_SYNC else ""
        code = textwrap.dedent(
            f"""
            import asyncio
            import sys
            import threading
            sys.path.insert(0, {root!r})
            from pymongo.periodic_executor import PeriodicExecutor

            started = threading.Event()

            def target():
                started.set()
                return True

            def main():
                executor = PeriodicExecutor(
                    interval=30.0, min_interval=0.05, target=target, name="subinterp"
                )
                executor.open()
                {wait_stmt}

            {run_stmt}
            """
        )
        interp = _interpreters.create()
        try:
            interp.exec(code)
        finally:
            # Destroying the subinterpreter must not crash or hang.
            interp.close()


if __name__ == "__main__":
    unittest.main()
