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

import gc
import sys
from functools import partial

sys.path[0:0] = [""]

from test import IntegrationTest, unittest
from test.utils import (
    ServerAndTopologyEventListener,
    connected,
    single_client,
    wait_until,
)

from pymongo.periodic_executor import _EXECUTORS


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


def create_client():
    listener = ServerAndTopologyEventListener()
    client = single_client(event_listeners=[listener])
    connected(client)
    return client


class TestMonitor(IntegrationTest):
    def test_cleanup_executors_on_client_del(self):
        client = create_client()
        executors = get_executors(client)
        self.assertEqual(len(executors), 4)

        # Each executor stores a weakref to itself in _EXECUTORS.
        executor_refs = [(r, r()._name) for r in _EXECUTORS.copy() if r() in executors]

        del executors
        del client

        for ref, name in executor_refs:
            wait_until(partial(unregistered, ref), f"unregister executor: {name}", timeout=5)

    def test_cleanup_executors_on_client_close(self):
        client = create_client()
        executors = get_executors(client)
        self.assertEqual(len(executors), 4)

        client.close()

        for executor in executors:
            wait_until(lambda: executor._stopped, f"closed executor: {executor._name}", timeout=5)


if __name__ == "__main__":
    unittest.main()
