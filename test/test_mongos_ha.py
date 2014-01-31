# Copyright 2013-2014 MongoDB, Inc.
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

sys.path[0:0] = [""]

from pymongo.errors import AutoReconnect
from test.pymongo_mocks import MockClient


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
        client.disconnect()
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
        client.kill_host('%s:%s' % (client.host, client.port))

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


if __name__ == "__main__":
    unittest.main()
