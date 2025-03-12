# Copyright 2019-present MongoDB, Inc.
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

"""Test retryable reads spec."""
from __future__ import annotations

import os
import pprint
import sys
import threading
from test.utils import set_fail_point

from pymongo.errors import AutoReconnect

sys.path[0:0] = [""]

from test import (
    IntegrationTest,
    PyMongoTestCase,
    client_context,
    client_knobs,
    unittest,
)
from test.utils_shared import (
    CMAPListener,
    OvertCommandListener,
)

from pymongo.monitoring import (
    ConnectionCheckedOutEvent,
    ConnectionCheckOutFailedEvent,
    ConnectionCheckOutFailedReason,
    PoolClearedEvent,
)

_IS_SYNC = True


class TestClientOptions(PyMongoTestCase):
    def test_default(self):
        client = self.simple_client(connect=False)
        self.assertEqual(client.options.retry_reads, True)

    def test_kwargs(self):
        client = self.simple_client(retryReads=True, connect=False)
        self.assertEqual(client.options.retry_reads, True)
        client = self.simple_client(retryReads=False, connect=False)
        self.assertEqual(client.options.retry_reads, False)

    def test_uri(self):
        client = self.simple_client("mongodb://h/?retryReads=true", connect=False)
        self.assertEqual(client.options.retry_reads, True)
        client = self.simple_client("mongodb://h/?retryReads=false", connect=False)
        self.assertEqual(client.options.retry_reads, False)


class FindThread(threading.Thread):
    def __init__(self, collection):
        super().__init__()
        self.daemon = True
        self.collection = collection
        self.passed = False

    def run(self):
        self.collection.find_one({})
        self.passed = True


class TestPoolPausedError(IntegrationTest):
    # Pools don't get paused in load balanced mode.
    RUN_ON_LOAD_BALANCER = False
    RUN_ON_SERVERLESS = False

    @client_context.require_sync
    @client_context.require_failCommand_blockConnection
    @client_knobs(heartbeat_frequency=0.05, min_heartbeat_interval=0.05)
    def test_pool_paused_error_is_retryable(self):
        if "PyPy" in sys.version:
            # Tracked in PYTHON-3519
            self.skipTest("Test is flakey on PyPy")
        cmap_listener = CMAPListener()
        cmd_listener = OvertCommandListener()
        client = self.rs_or_single_client(
            maxPoolSize=1, event_listeners=[cmap_listener, cmd_listener]
        )
        for _ in range(10):
            cmap_listener.reset()
            cmd_listener.reset()
            threads = [FindThread(client.pymongo_test.test) for _ in range(2)]
            fail_command = {
                "mode": {"times": 1},
                "data": {
                    "failCommands": ["find"],
                    "blockConnection": True,
                    "blockTimeMS": 1000,
                    "errorCode": 91,
                },
            }
            with self.fail_point(fail_command):
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                for thread in threads:
                    self.assertTrue(thread.passed)

            # It's possible that SDAM can rediscover the server and mark the
            # pool ready before the thread in the wait queue has a chance
            # to run. Repeat the test until the thread actually encounters
            # a PoolClearedError.
            if cmap_listener.event_count(ConnectionCheckOutFailedEvent):
                break

        # Via CMAP monitoring, assert that the first check out succeeds.
        cmap_events = cmap_listener.events_by_type(
            (ConnectionCheckedOutEvent, ConnectionCheckOutFailedEvent, PoolClearedEvent)
        )
        msg = pprint.pformat(cmap_listener.events)
        self.assertIsInstance(cmap_events[0], ConnectionCheckedOutEvent, msg)
        self.assertIsInstance(cmap_events[1], PoolClearedEvent, msg)
        self.assertIsInstance(cmap_events[2], ConnectionCheckOutFailedEvent, msg)
        self.assertEqual(cmap_events[2].reason, ConnectionCheckOutFailedReason.CONN_ERROR, msg)
        self.assertIsInstance(cmap_events[3], ConnectionCheckedOutEvent, msg)

        # Connection check out failures are not reflected in command
        # monitoring because we only publish command events _after_ checking
        # out a connection.
        started = cmd_listener.started_events
        msg = pprint.pformat(cmd_listener.results)
        self.assertEqual(3, len(started), msg)
        succeeded = cmd_listener.succeeded_events
        self.assertEqual(2, len(succeeded), msg)
        failed = cmd_listener.failed_events
        self.assertEqual(1, len(failed), msg)


class TestRetryableReads(IntegrationTest):
    @client_context.require_multiple_mongoses
    @client_context.require_failCommand_fail_point
    def test_retryable_reads_in_sharded_cluster_multiple_available(self):
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {
                "failCommands": ["find"],
                "closeConnection": True,
                "appName": "retryableReadTest",
            },
        }

        mongos_clients = []

        for mongos in client_context.mongos_seeds().split(","):
            client = self.rs_or_single_client(mongos)
            set_fail_point(client, fail_command)
            mongos_clients.append(client)

        listener = OvertCommandListener()
        client = self.rs_or_single_client(
            client_context.mongos_seeds(),
            appName="retryableReadTest",
            event_listeners=[listener],
            retryReads=True,
        )

        with self.assertRaises(AutoReconnect):
            client.t.t.find_one({})

        # Disable failpoints on each mongos
        for client in mongos_clients:
            fail_command["mode"] = "off"
            set_fail_point(client, fail_command)

        self.assertEqual(len(listener.failed_events), 2)
        self.assertEqual(len(listener.succeeded_events), 0)


if __name__ == "__main__":
    unittest.main()
