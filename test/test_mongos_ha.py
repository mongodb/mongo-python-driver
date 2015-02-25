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

sys.path[0:0] = [""]

from pymongo.errors import AutoReconnect
from test import unittest, client_context, SkipTest, MockClientTest
from test.pymongo_mocks import MockClient
from test.utils import connected, wait_until


@client_context.require_connection
def setUpModule():
    pass


class SimpleOp(threading.Thread):

    def __init__(self, client):
        super(SimpleOp, self).__init__()
        self.client = client
        self.passed = False

    def run(self):
        self.client.db.command('ismaster')
        self.passed = True  # No exception raised.


def do_simple_op(client, nthreads):
    threads = [SimpleOp(client) for _ in range(nthreads)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    for t in threads:
        assert t.passed


class TestMongosHA(MockClientTest):

    def mock_client(self):
        return MockClient(
            standalones=[],
            members=[],
            mongoses=['a:1', 'b:2', 'c:3'],
            host='a:1,b:2,c:3',
            connect=False)
        
    def test_lazy_connect(self):
        # TODO: Reimplement Mongos HA with PyMongo 3's MongoClient.
        raise SkipTest('Mongos HA must be reimplemented with 3.0 MongoClient')

        nthreads = 10
        client = self.mock_client()
        self.assertEqual(0, len(client.nodes))

        # Trigger initial connection.
        do_simple_op(client, nthreads)
        self.assertEqual(3, len(client.nodes))

    def test_reconnect(self):
        nthreads = 10
        client = connected(self.mock_client())

        # connected() ensures we've contacted at least one mongos. Wait for
        # all of them.
        wait_until(lambda: len(client.nodes) == 3, 'connect to all mongoses')

        # Trigger reconnect.
        client.close()
        do_simple_op(client, nthreads)

        wait_until(lambda: len(client.nodes) == 3,
                   'reconnect to all mongoses')

    def test_failover(self):
        # TODO: PyMongo 3's MongoClient currently picks a new Mongos at random
        #       for each operation (besides getMore). Need to "pin".
        raise SkipTest('Mongos HA must be reimplemented with 3.0 MongoClient')

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
                client.db.command('ismaster')
            except AutoReconnect:
                errors.append(True)

                # Second attempt succeeds.
                client.db.command('ismaster')

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
