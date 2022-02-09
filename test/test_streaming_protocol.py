# Copyright 2020-present MongoDB, Inc.
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

"""Test the database module."""

import sys
import time

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.utils import (
    HeartbeatEventListener,
    ServerEventListener,
    rs_or_single_client,
    single_client,
    wait_until,
)

from pymongo import monitoring
from pymongo.hello import HelloCompat


class TestStreamingProtocol(IntegrationTest):
    @client_context.require_failCommand_appName
    def test_failCommand_streaming(self):
        listener = ServerEventListener()
        hb_listener = HeartbeatEventListener()
        client = rs_or_single_client(
            event_listeners=[listener, hb_listener],
            heartbeatFrequencyMS=500,
            appName="failingHeartbeatTest",
        )
        self.addCleanup(client.close)
        # Force a connection.
        client.admin.command("ping")
        address = client.address
        listener.reset()

        fail_hello = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 4},
            "data": {
                "failCommands": [HelloCompat.LEGACY_CMD, "hello"],
                "closeConnection": False,
                "errorCode": 10107,
                "appName": "failingHeartbeatTest",
            },
        }
        with self.fail_point(fail_hello):

            def _marked_unknown(event):
                return (
                    event.server_address == address
                    and not event.new_description.is_server_type_known
                )

            def _discovered_node(event):
                return (
                    event.server_address == address
                    and not event.previous_description.is_server_type_known
                    and event.new_description.is_server_type_known
                )

            def marked_unknown():
                return len(listener.matching(_marked_unknown)) >= 1

            def rediscovered():
                return len(listener.matching(_discovered_node)) >= 1

            # Topology events are published asynchronously
            wait_until(marked_unknown, "mark node unknown")
            wait_until(rediscovered, "rediscover node")

        # Server should be selectable.
        client.admin.command("ping")

    @client_context.require_failCommand_appName
    def test_streaming_rtt(self):
        listener = ServerEventListener()
        hb_listener = HeartbeatEventListener()
        # On Windows, RTT can actually be 0.0 because time.time() only has
        # 1-15 millisecond resolution. We need to delay the initial hello
        # to ensure that RTT is never zero.
        name = "streamingRttTest"
        delay_hello: dict = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1000},
            "data": {
                "failCommands": [HelloCompat.LEGACY_CMD, "hello"],
                "blockConnection": True,
                "blockTimeMS": 20,
                # This can be uncommented after SERVER-49220 is fixed.
                # 'appName': name,
            },
        }
        with self.fail_point(delay_hello):
            client = rs_or_single_client(
                event_listeners=[listener, hb_listener], heartbeatFrequencyMS=500, appName=name
            )
            self.addCleanup(client.close)
            # Force a connection.
            client.admin.command("ping")
            address = client.address

        delay_hello["data"]["blockTimeMS"] = 500
        delay_hello["data"]["appName"] = name
        with self.fail_point(delay_hello):

            def rtt_exceeds_250_ms():
                # XXX: Add a public TopologyDescription getter to MongoClient?
                topology = client._topology
                sd = topology.description.server_descriptions()[address]
                return sd.round_trip_time > 0.250

            wait_until(rtt_exceeds_250_ms, "exceed 250ms RTT")

        # Server should be selectable.
        client.admin.command("ping")

        def changed_event(event):
            return event.server_address == address and isinstance(
                event, monitoring.ServerDescriptionChangedEvent
            )

        # There should only be one event published, for the initial discovery.
        events = listener.matching(changed_event)
        self.assertEqual(1, len(events))
        self.assertGreater(events[0].new_description.round_trip_time, 0)

    @client_context.require_version_min(4, 9, -1)
    @client_context.require_failCommand_appName
    def test_monitor_waits_after_server_check_error(self):
        # This test implements:
        # https://github.com/mongodb/specifications/blob/6c5b2ac/source/server-discovery-and-monitoring/server-discovery-and-monitoring-tests.rst#monitors-sleep-at-least-minheartbeatfreqencyms-between-checks
        fail_hello = {
            "mode": {"times": 5},
            "data": {
                "failCommands": [HelloCompat.LEGACY_CMD, "hello"],
                "errorCode": 1234,
                "appName": "SDAMMinHeartbeatFrequencyTest",
            },
        }
        with self.fail_point(fail_hello):
            start = time.time()
            client = single_client(
                appName="SDAMMinHeartbeatFrequencyTest", serverSelectionTimeoutMS=5000
            )
            self.addCleanup(client.close)
            # Force a connection.
            client.admin.command("ping")
            duration = time.time() - start
            # Explanation of the expected events:
            # 0ms: run configureFailPoint
            # 1ms: create MongoClient
            # 2ms: failed monitor handshake, 1
            # 502ms: failed monitor handshake, 2
            # 1002ms: failed monitor handshake, 3
            # 1502ms: failed monitor handshake, 4
            # 2002ms: failed monitor handshake, 5
            # 2502ms: monitor handshake succeeds
            # 2503ms: run awaitable hello
            # 2504ms: application handshake succeeds
            # 2505ms: ping command succeeds
            self.assertGreaterEqual(duration, 2)
            self.assertLessEqual(duration, 3.5)

    @client_context.require_failCommand_appName
    def test_heartbeat_awaited_flag(self):
        hb_listener = HeartbeatEventListener()
        client = single_client(
            event_listeners=[hb_listener],
            heartbeatFrequencyMS=500,
            appName="heartbeatEventAwaitedFlag",
        )
        self.addCleanup(client.close)
        # Force a connection.
        client.admin.command("ping")

        def hb_succeeded(event):
            return isinstance(event, monitoring.ServerHeartbeatSucceededEvent)

        def hb_failed(event):
            return isinstance(event, monitoring.ServerHeartbeatFailedEvent)

        fail_heartbeat = {
            "mode": {"times": 2},
            "data": {
                "failCommands": [HelloCompat.LEGACY_CMD, "hello"],
                "closeConnection": True,
                "appName": "heartbeatEventAwaitedFlag",
            },
        }
        with self.fail_point(fail_heartbeat):
            wait_until(lambda: hb_listener.matching(hb_failed), "published failed event")
        # Reconnect.
        client.admin.command("ping")

        hb_succeeded_events = hb_listener.matching(hb_succeeded)
        hb_failed_events = hb_listener.matching(hb_failed)
        self.assertFalse(hb_succeeded_events[0].awaited)
        self.assertTrue(hb_failed_events[0].awaited)
        # Depending on thread scheduling, the failed heartbeat could occur on
        # the second or third check.
        events = [type(e) for e in hb_listener.events[:4]]
        if events == [
            monitoring.ServerHeartbeatStartedEvent,
            monitoring.ServerHeartbeatSucceededEvent,
            monitoring.ServerHeartbeatStartedEvent,
            monitoring.ServerHeartbeatFailedEvent,
        ]:
            self.assertFalse(hb_succeeded_events[1].awaited)
        else:
            self.assertTrue(hb_succeeded_events[1].awaited)


if __name__ == "__main__":
    unittest.main()
