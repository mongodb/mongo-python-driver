# Copyright 2016 MongoDB, Inc.
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

from pymongo import monitoring
from pymongo.errors import ConnectionFailure
from pymongo.ismaster import IsMaster
from pymongo.monitor import Monitor
from test import unittest, client_knobs
from test.utils import HeartbeatEventListener, single_client, wait_until

sys.path[0:0] = [""]


class MockSocketInfo(object):
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockPool(object):
    def __init__(self, *args, **kwargs):
        self.pool_id = 0
        self._lock = threading.Lock()

    def get_socket(self, all_credentials):
        return MockSocketInfo()

    def return_socket(self, _):
        pass

    def reset(self):
        with self._lock:
            self.pool_id += 1

    def remove_stale_sockets(self):
        pass


class TestHeartbeatMonitoring(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.all_listener = HeartbeatEventListener()
        cls.saved_listeners = monitoring._LISTENERS
        monitoring._LISTENERS = monitoring._Listeners([], [], [], [])

    @classmethod
    def tearDown(cls):
        monitoring._LISTENERS = cls.saved_listeners

    def create_mock_monitor(self, responses, uri, expected_results):
        with client_knobs(heartbeat_frequency=0.1,
                          min_heartbeat_interval=0.1,
                          events_queue_frequency=0.1):
            class MockMonitor(Monitor):
                def _check_with_socket(self, sock_info):
                    if isinstance(responses[1], Exception):
                        raise responses[1]
                    return IsMaster(responses[1]), 99

            m = single_client(h=uri,
                              event_listeners=(self.all_listener,),
                              _monitor_class=MockMonitor,
                              _pool_class=MockPool
                              )

            expected_len = len(expected_results)
            wait_until(lambda: len(self.all_listener.results) == expected_len,
                       "publish all events", timeout=15)

        try:
            for i in range(len(expected_results)):
                result = self.all_listener.results[i] if len(
                    self.all_listener.results) > i else None
                self.assertEqual(expected_results[i],
                                 result.__class__.__name__)
                self.assertEqual(result.connection_id,
                                 responses[0])
                if expected_results[i] != 'ServerHeartbeatStartedEvent':
                    if isinstance(result.reply, IsMaster):
                        self.assertEqual(result.duration, 99)
                        self.assertEqual(result.reply._doc, responses[1])
                    else:
                        self.assertEqual(result.reply, responses[1])

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
        expected_results = ['ServerHeartbeatStartedEvent',
                            'ServerHeartbeatFailedEvent']

        self.create_mock_monitor(responses, uri, expected_results)


if __name__ == "__main__":
    unittest.main()
