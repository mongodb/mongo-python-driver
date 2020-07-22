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

from pymongo import monitoring

from test import (client_context,
                  IntegrationTest,
                  unittest)
from test.utils import (HeartbeatEventListener,
                        rs_or_single_client,
                        single_client,
                        ServerEventListener,
                        wait_until)


class TestStreamingProtocol(IntegrationTest):
    @client_context.require_failCommand_appName
    def test_failCommand_streaming(self):
        listener = ServerEventListener()
        hb_listener = HeartbeatEventListener()
        client = rs_or_single_client(
            event_listeners=[listener, hb_listener], heartbeatFrequencyMS=500,
            appName='failingIsMasterTest')
        self.addCleanup(client.close)
        # Force a connection.
        client.admin.command('ping')
        address = client.address
        listener.reset()

        fail_ismaster = {
            'configureFailPoint': 'failCommand',
            'mode': {'times': 4},
            'data': {
                'failCommands': ['isMaster'],
                'closeConnection': False,
                'errorCode': 10107,
                'appName': 'failingIsMasterTest',
            },
        }
        with self.fail_point(fail_ismaster):
            def _marked_unknown(event):
                return (event.server_address == address
                        and not event.new_description.is_server_type_known)

            def _discovered_node(event):
                return (event.server_address == address
                        and not event.previous_description.is_server_type_known
                        and event.new_description.is_server_type_known)

            def marked_unknown():
                return len(listener.matching(_marked_unknown)) >= 1

            def rediscovered():
                return len(listener.matching(_discovered_node)) >= 1

            # Topology events are published asynchronously
            wait_until(marked_unknown, 'mark node unknown')
            wait_until(rediscovered, 'rediscover node')

        # Server should be selectable.
        client.admin.command('ping')

    @client_context.require_failCommand_appName
    def test_streaming_rtt(self):
        listener = ServerEventListener()
        hb_listener = HeartbeatEventListener()
        # On Windows, RTT can actually be 0.0 because time.time() only has
        # 1-15 millisecond resolution. We need to delay the initial isMaster
        # to ensure that RTT is never zero.
        name = 'streamingRttTest'
        delay_ismaster = {
            'configureFailPoint': 'failCommand',
            'mode': {'times': 1000},
            'data': {
                'failCommands': ['isMaster'],
                'blockConnection': True,
                'blockTimeMS': 20,
                # This can be uncommented after SERVER-49220 is fixed.
                # 'appName': name,
            },
        }
        with self.fail_point(delay_ismaster):
            client = rs_or_single_client(
                event_listeners=[listener, hb_listener],
                heartbeatFrequencyMS=500,
                appName=name)
            self.addCleanup(client.close)
            # Force a connection.
            client.admin.command('ping')
            address = client.address

        delay_ismaster['data']['blockTimeMS'] = 500
        delay_ismaster['data']['appName'] = name
        with self.fail_point(delay_ismaster):
            def rtt_exceeds_250_ms():
                # XXX: Add a public TopologyDescription getter to MongoClient?
                topology = client._topology
                sd = topology.description.server_descriptions()[address]
                return sd.round_trip_time > 0.250

            wait_until(rtt_exceeds_250_ms, 'exceed 250ms RTT')

        # Server should be selectable.
        client.admin.command('ping')

        def changed_event(event):
            return (event.server_address == address and isinstance(
                        event, monitoring.ServerDescriptionChangedEvent))

        # There should only be one event published, for the initial discovery.
        events = listener.matching(changed_event)
        self.assertEqual(1, len(events))
        self.assertGreater(events[0].new_description.round_trip_time, 0)

    @client_context.require_failCommand_appName
    def test_monitor_waits_after_server_check_error(self):
        hb_listener = HeartbeatEventListener()
        client = rs_or_single_client(
            event_listeners=[hb_listener], heartbeatFrequencyMS=500,
            appName='waitAfterErrorTest')
        self.addCleanup(client.close)
        # Force a connection.
        client.admin.command('ping')
        address = client.address

        fail_ismaster = {
            'mode': {'times': 50},
            'data': {
                'failCommands': ['isMaster'],
                'closeConnection': False,
                'errorCode': 91,
                # This can be uncommented after SERVER-49220 is fixed.
                # 'appName': 'waitAfterErrorTest',
            },
        }
        with self.fail_point(fail_ismaster):
            time.sleep(2)

        # Server should be selectable.
        client.admin.command('ping')

        def hb_started(event):
            return (isinstance(event, monitoring.ServerHeartbeatStartedEvent)
                    and event.connection_id == address)

        hb_started_events = hb_listener.matching(hb_started)
        # Explanation of the expected heartbeat events:
        # Time: event
        # 0ms: create MongoClient
        # 1ms: run monitor handshake, 1
        # 2ms: run awaitable isMaster, 2
        # 3ms: run configureFailPoint
        # 502ms: isMaster fails for the first time with command error
        # 1002ms: run monitor handshake, 3
        # 1502ms: run monitor handshake, 4
        # 2002ms: run monitor handshake, 5
        # 2003ms: disable configureFailPoint
        # 2004ms: isMaster succeeds, 6
        # 2004ms: awaitable isMaster, 7
        self.assertGreater(len(hb_started_events), 7)
        # This can be reduced to ~15 after SERVER-49220 is fixed.
        self.assertLess(len(hb_started_events), 40)

    @client_context.require_failCommand_appName
    def test_heartbeat_awaited_flag(self):
        hb_listener = HeartbeatEventListener()
        client = single_client(
            event_listeners=[hb_listener], heartbeatFrequencyMS=500,
            appName='heartbeatEventAwaitedFlag')
        self.addCleanup(client.close)
        # Force a connection.
        client.admin.command('ping')

        def hb_succeeded(event):
            return isinstance(event, monitoring.ServerHeartbeatSucceededEvent)

        def hb_failed(event):
            return isinstance(event, monitoring.ServerHeartbeatFailedEvent)

        fail_heartbeat = {
            'mode': {'times': 2},
            'data': {
                'failCommands': ['isMaster'],
                'closeConnection': True,
                'appName': 'heartbeatEventAwaitedFlag',
            },
        }
        with self.fail_point(fail_heartbeat):
            wait_until(lambda: hb_listener.matching(hb_failed),
                       "published failed event")
        # Reconnect.
        client.admin.command('ping')

        hb_succeeded_events = hb_listener.matching(hb_succeeded)
        hb_failed_events = hb_listener.matching(hb_failed)
        self.assertFalse(hb_succeeded_events[0].awaited)
        self.assertTrue(hb_failed_events[0].awaited)
        # Depending on thread scheduling, the failed heartbeat could occur on
        # the second or third check.
        events = [type(e) for e in hb_listener.results[:4]]
        if events == [monitoring.ServerHeartbeatStartedEvent,
                      monitoring.ServerHeartbeatSucceededEvent,
                      monitoring.ServerHeartbeatStartedEvent,
                      monitoring.ServerHeartbeatFailedEvent]:
            self.assertFalse(hb_succeeded_events[1].awaited)
        else:
            self.assertTrue(hb_succeeded_events[1].awaited)


if __name__ == "__main__":
    unittest.main()
