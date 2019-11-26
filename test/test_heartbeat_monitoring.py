# Copyright 2016-present MongoDB, Inc.
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

"""Test the monitoring of the server heartbeats."""

import sys
import threading

sys.path[0:0] = [""]

from pymongo.errors import ConnectionFailure
from pymongo.ismaster import IsMaster
from pymongo.monitor import Monitor
from test import unittest, client_knobs
from test.utils import (HeartbeatEventListener, MockPool, single_client,
                        wait_until)


class TestHeartbeatMonitoring(unittest.TestCase):

    def create_mock_monitor(self, responses, uri, expected_results):
        listener = HeartbeatEventListener()
        with client_knobs(heartbeat_frequency=0.1,
                          min_heartbeat_interval=0.1,
                          events_queue_frequency=0.1):
            class MockMonitor(Monitor):
                def _check_with_socket(self, *args, **kwargs):
                    if isinstance(responses[1], Exception):
                        raise responses[1]
                    return IsMaster(responses[1]), 99

            m = single_client(
                h=uri,
                event_listeners=(listener,),
                _monitor_class=MockMonitor,
                _pool_class=MockPool)

            expected_len = len(expected_results)
            # Wait for *at least* expected_len number of results. The
            # monitor thread may run multiple times during the execution
            # of this test.
            wait_until(
                lambda: len(listener.results) >= expected_len,
                "publish all events")

        try:
            # zip gives us len(expected_results) pairs.
            for expected, actual in zip(expected_results, listener.results):
                self.assertEqual(expected,
                                 actual.__class__.__name__)
                self.assertEqual(actual.connection_id,
                                 responses[0])
                if expected != 'ServerHeartbeatStartedEvent':
                    if isinstance(actual.reply, IsMaster):
                        self.assertEqual(actual.duration, 99)
                        self.assertEqual(actual.reply._doc, responses[1])
                    else:
                        self.assertEqual(actual.reply, responses[1])

        finally:
            m.close()

    def test_standalone(self):
        responses = (('a', 27017),
                     {
                         "ismaster": True,
                         "maxWireVersion": 4,
                         "minWireVersion": 0,
                         "ok": 1
                     })
        uri = "mongodb://a:27017"
        expected_results = ['ServerHeartbeatStartedEvent',
                            'ServerHeartbeatSucceededEvent']

        self.create_mock_monitor(responses, uri, expected_results)

    def test_standalone_error(self):
        responses = (('a', 27017),
                     ConnectionFailure("SPECIAL MESSAGE"))
        uri = "mongodb://a:27017"
        # _check_with_socket failing results in a second attempt.
        expected_results = ['ServerHeartbeatStartedEvent',
                            'ServerHeartbeatFailedEvent',
                            'ServerHeartbeatStartedEvent',
                            'ServerHeartbeatFailedEvent']

        self.create_mock_monitor(responses, uri, expected_results)


if __name__ == "__main__":
    unittest.main()
