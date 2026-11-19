# Copyright 2014-present MongoDB, Inc.
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

"""Test the monitor module."""

from __future__ import annotations

import asyncio
import gc
import subprocess
import sys
import time
import warnings
from functools import partial

sys.path[0:0] = [""]

from pymongo.periodic_executor import _EXECUTORS
from pymongo.synchronous.monitor import Monitor
from test import IntegrationTest, client_context, connected, unittest
from test.utils import (
    wait_until,
)
from test.utils import wait_until as sync_wait_until
from test.utils_shared import (
    ServerAndTopologyEventListener,
    gevent_monkey_patched,
    suppress_fork_deprecation,
)

_IS_SYNC = True


def unregistered(ref):
    gc.collect()
    return ref not in _EXECUTORS


def get_executors(client):
    executors = []
    for server in client._topology._servers.values():
        executors.append(server._monitor._executor)
        executors.append(server._monitor._rtt_monitor._executor)
    executors.append(client._kill_cursors_executor)
    executors.append(client._topology._events_executor)
    return [e for e in executors if e is not None]


class TestMonitor(IntegrationTest):
    def create_client(self):
        listener = ServerAndTopologyEventListener()
        client = self.unmanaged_single_client(event_listeners=[listener])
        connected(client)
        return client

    @unittest.skipIf(
        gevent_monkey_patched(), "PYTHON-5516 Resources are not cleared when using gevent"
    )
    def test_cleanup_executors_on_client_del(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            client = self.create_client()
            executors = get_executors(client)
            self.assertEqual(len(executors), 4)

            # Each executor stores a weakref to itself in _EXECUTORS.
            executor_refs = [(r, r()._name) for r in _EXECUTORS.copy() if r() in executors]

            del executors
            del client

            for ref, name in executor_refs:
                wait_until(partial(unregistered, ref), f"unregister executor: {name}", timeout=5)

            def resource_warning_caught():
                gc.collect()
                for warning in w:
                    if (
                        issubclass(warning.category, ResourceWarning)
                        and "Call MongoClient.close() to safely shut down "
                        "your client and free up resources."
                        in str(warning.message)
                    ):
                        return True
                return False

            wait_until(resource_warning_caught, "catch resource warning")

    def test_cleanup_executors_on_client_close(self):
        client = self.create_client()
        executors = get_executors(client)
        self.assertEqual(len(executors), 4)

        client.close()

        for executor in executors:
            wait_until(lambda: executor._stopped, f"closed executor: {executor._name}", timeout=5)

    @client_context.require_sync
    def test_close_stops_monitor_thread(self):
        """PYTHON-1634: close() joins the monitor thread before returning.

        Callers don't race it while it reads the socket.
        """
        client = self.create_client()
        server = next(iter(client._topology._servers.values()))
        monitor = server._monitor

        def thread_alive(executor):
            try:
                return executor._thread is not None and executor._thread.is_alive()
            except ReferenceError:
                return False

        sync_wait_until(lambda: thread_alive(monitor._executor), "monitor thread never started")

        client.close()

        # close() drains the deferred-join queue after releasing the lock.
        self.assertEqual(client._topology._monitor_tasks, [])
        self.assertFalse(thread_alive(monitor._executor))
        self.assertFalse(thread_alive(monitor._rtt_monitor._executor))

    @client_context.require_async
    def test_close_stops_monitor_task(self):
        """PYTHON-1634: the async counterpart of test_close_stops_monitor_thread."""
        client = self.create_client()
        server = next(iter(client._topology._servers.values()))
        monitor = server._monitor

        self.assertIsNotNone(monitor._executor._task)
        self.assertFalse(monitor._executor._task.done())

        client.close()

        self.assertEqual(client._topology._monitor_tasks, [])
        self.assertTrue(monitor._executor._task.done())
        self.assertTrue(monitor._rtt_monitor._executor._task.done())

    @client_context.require_sync
    def test_no_thread_start_runtime_err_on_shutdown(self):
        """Test we silence noisy runtime errors.

        The errors are fired when the MongoClient spawns a new thread on
        process shutdown.
        """
        command = [
            sys.executable,
            "-c",
            "from pymongo import MongoClient; c = MongoClient()",
        ]
        with suppress_fork_deprecation():
            completed_process: subprocess.CompletedProcess = subprocess.run(
                command, capture_output=True
            )

        self.assertFalse(completed_process.stderr)
        self.assertFalse(completed_process.stdout)


class _SlowExecutor:
    """Executor stand-in whose join takes a fixed time."""

    def __init__(self, delay: float) -> None:
        self._delay = delay

    def join(self, timeout: float | None = None) -> None:
        # Blocks with time.sleep on purpose: the sync mirror of this test
        # needs the same delay, and unasync does not translate asyncio.sleep.
        self._sleep()

    def _sleep(self) -> None:
        time.sleep(self._delay)


class _RecordingExecutor:
    """Executor stand-in that records the timeouts it is joined with."""

    def __init__(self) -> None:
        self.timeouts: list[float | None] = []

    def join(self, timeout: float | None = None) -> None:
        self.timeouts.append(timeout)


class TestMonitorJoin(unittest.TestCase):
    """Unit tests for the timeout budget in Monitor.join."""

    def test_rtt_join_gets_unspent_budget(self):
        # PYTHON-1634: after the heartbeat executor stops, the RTT monitor
        # gets whatever budget remains, never more.
        monitor = Monitor.__new__(Monitor)
        monitor._executor = _SlowExecutor(0.45)  # type: ignore[assignment]
        rtt = _RecordingExecutor()
        monitor._rtt_monitor = rtt  # type: ignore[assignment]

        monitor.join(1)
        rtt_timeout = rtt.timeouts[0]
        # Narrowing only; the verified assertion is assertLess below.
        assert isinstance(rtt_timeout, float)
        self.assertLess(rtt_timeout, 0.95)

    def test_join_without_timeout_waits_indefinitely(self):
        monitor = Monitor.__new__(Monitor)
        monitor._executor = _SlowExecutor(0)  # type: ignore[assignment]
        rtt = _RecordingExecutor()
        monitor._rtt_monitor = rtt  # type: ignore[assignment]

        monitor.join(None)
        self.assertEqual(rtt.timeouts, [None])


if __name__ == "__main__":
    unittest.main()
