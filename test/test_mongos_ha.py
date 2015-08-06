# Copyright 2013-2015 MongoDB, Inc.
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

"""Test MongoClient's mongos high-availability features using a mock."""

import sys
import threading
import unittest
import warnings

sys.path[0:0] = [""]

from pymongo.errors import AutoReconnect
from test import skip_restricted_localhost
from test.pymongo_mocks import MockClient
from test.utils import catch_warnings


setUpModule = skip_restricted_localhost


class FindOne(threading.Thread):
    def __init__(self, client):
        super(FindOne, self).__init__()
        self.client = client
        self.passed = False

    def run(self):
        self.client.db.collection.find_one()
        self.passed = True  # No exception raised.


def do_find_one(client, nthreads):
    threads = [FindOne(client) for _ in range(nthreads)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    for t in threads:
        assert t.passed


class TestMongosHA(unittest.TestCase):
    def mock_client(self, connect):
        return MockClient(
            standalones=[],
            members=[],
            mongoses=['a:1', 'b:2', 'c:3'],
            host='a:1,b:2,c:3',
            _connect=connect)
        
    def test_lazy_connect(self):
        nthreads = 10
        client = self.mock_client(False)
        self.assertEqual(0, len(client.nodes))

        # Trigger initial connection.
        do_find_one(client, nthreads)
        self.assertEqual(3, len(client.nodes))

    def test_reconnect(self):
        nthreads = 10
        client = self.mock_client(True)
        self.assertEqual(3, len(client.nodes))

        # Trigger reconnect.
        client.close()
        do_find_one(client, nthreads)
        self.assertEqual(3, len(client.nodes))

    def test_failover(self):
        nthreads = 1

        # ['1:1', '2:2', '3:3', ...]
        mock_hosts = ['%d:%d' % (i, i) for i in range(50)]
        client = MockClient(
            standalones=[],
            members=[],
            mongoses=mock_hosts,
            host=','.join(mock_hosts))

        self.assertEqual(len(mock_hosts), len(client.nodes))

        # Our chosen mongos goes down.
        client.kill_host('%s:%s' % client.address)

        # Trigger failover. AutoReconnect should be raised exactly once.
        errors = []
        passed = []

        def f():
            try:
                client.db.collection.find_one()
            except AutoReconnect:
                errors.append(True)

                # Second attempt succeeds.
                client.db.collection.find_one()

            passed.append(True)

        threads = [threading.Thread(target=f) for _ in range(nthreads)]
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(1, len(errors))
        self.assertEqual(nthreads, len(passed))

        # Down host is still in list.
        self.assertEqual(len(mock_hosts), len(client.nodes))

    def test_acceptable_latency(self):
        client = MockClient(
            standalones=[],
            members=[],
            mongoses=['a:1', 'b:2', 'c:3'],
            host='a:1,b:2,c:3',
            localThresholdMS=7)

        self.assertEqual(7, client.secondary_acceptable_latency_ms)
        # No error
        client.db.collection.find_one()

        client = MockClient(
            standalones=[],
            members=[],
            mongoses=['a:1', 'b:2', 'c:3'],
            host='a:1,b:2,c:3',
            localThresholdMS=0)

        self.assertEqual(0, client.secondary_acceptable_latency_ms)
        # No error
        client.db.collection.find_one()
        # Our chosen mongos goes down.
        client.kill_host('%s:%s' % client.address)
        try:
            client.db.collection.find_one()
        except:
            pass
        # No error
        client.db.collection.find_one()

    def test_backport_localthresholdms_kwarg(self):
        # Test that localThresholdMS takes precedence over
        # secondaryAcceptableLatencyMS.
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            client = MockClient(
                standalones=[],
                members=[],
                mongoses=['a:1', 'b:2', 'c:3'],
                host='a:1,b:2,c:3',
                localThresholdMS=7,
                secondaryAcceptableLatencyMS=0)

            self.assertEqual(7, client.secondary_acceptable_latency_ms)
            self.assertEqual(7, client.local_threshold_ms)
            # No error
            client.db.collection.find_one()

            client = MockClient(
                standalones=[],
                members=[],
                mongoses=['a:1', 'b:2', 'c:3'],
                host='a:1,b:2,c:3',
                localThresholdMS=0,
                secondaryAcceptableLatencyMS=15)

            self.assertEqual(0, client.secondary_acceptable_latency_ms)
            self.assertEqual(0, client.local_threshold_ms)

            # Test that using localThresholdMS works in the same way as using
            # secondaryAcceptableLatencyMS.
            client.db.collection.find_one()
            # Our chosen mongos goes down.
            client.kill_host('%s:%s' % client.address)
            try:
                client.db.collection.find_one()
            except:
                pass
            # No error
            client.db.collection.find_one()
        finally:
            ctx.exit()

if __name__ == "__main__":
    unittest.main()
