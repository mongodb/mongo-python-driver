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
import warnings
from functools import partial

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, connected, unittest
from test.asynchronous.utils import (
    async_wait_until,
)
from test.utils_shared import ServerAndTopologyEventListener

from pymongo.periodic_executor import _EXECUTORS

_IS_SYNC = False


def unregistered(ref):
    gc.collect()
    return ref not in _EXECUTORS


def get_executors(client):
    executors = []
    for server in client._topology._servers.values():
        executors.append(server._monitor._executor)
        executors.append(server._monitor._rtt_monitor._executor)
    executors.append(client._kill_cursors_executor)
    executors.append(client._topology._Topology__events_executor)
    return [e for e in executors if e is not None]


class TestMonitor(AsyncIntegrationTest):
    async def create_client(self):
        listener = ServerAndTopologyEventListener()
        client = await self.unmanaged_async_single_client(event_listeners=[listener])
        await connected(client)
        return client

    @unittest.skipIf("PyPy" in sys.version, "PYTHON-5283 fails often on PyPy")
    async def test_cleanup_executors_on_client_del(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            client = await self.create_client()
            executors = get_executors(client)
            self.assertEqual(len(executors), 4)

            # Each executor stores a weakref to itself in _EXECUTORS.
            executor_refs = [(r, r()._name) for r in _EXECUTORS.copy() if r() in executors]

            del executors
            del client

            for ref, name in executor_refs:
                await async_wait_until(
                    partial(unregistered, ref), f"unregister executor: {name}", timeout=5
                )

            def resource_warning_caught():
                gc.collect()
                for warning in w:
                    if (
                        issubclass(warning.category, ResourceWarning)
                        and "Call AsyncMongoClient.close() to safely shut down your client and free up resources."
                        in str(warning.message)
                    ):
                        return True
                return False

            await async_wait_until(resource_warning_caught, "catch resource warning")

    async def test_cleanup_executors_on_client_close(self):
        client = await self.create_client()
        executors = get_executors(client)
        self.assertEqual(len(executors), 4)

        await client.close()

        for executor in executors:
            await async_wait_until(
                lambda: executor._stopped, f"closed executor: {executor._name}", timeout=5
            )

    @async_client_context.require_sync
    def test_no_thread_start_runtime_err_on_shutdown(self):
        """Test we silence noisy runtime errors fired when the AsyncMongoClient spawns a new thread
        on process shutdown."""
        command = [
            sys.executable,
            "-c",
            "from pymongo import AsyncMongoClient; c = AsyncMongoClient()",
        ]
        completed_process: subprocess.CompletedProcess = subprocess.run(
            command, capture_output=True
        )

        self.assertFalse(completed_process.stderr)
        self.assertFalse(completed_process.stdout)


if __name__ == "__main__":
    unittest.main()
