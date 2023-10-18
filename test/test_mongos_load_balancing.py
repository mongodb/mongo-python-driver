# Copyright 2015-present MongoDB, Inc.
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

"""Test MongoClient's mongos load balancing using a mock."""
from __future__ import annotations

import sys
import threading

sys.path[0:0] = [""]

from test import MockClientTest, client_context, unittest
from test.pymongo_mocks import MockClient
from test.utils import connected, wait_until

from pymongo.errors import AutoReconnect, InvalidOperation
from pymongo.server_selectors import writable_server_selector
from pymongo.topology_description import TOPOLOGY_TYPE


@client_context.require_connection
@client_context.require_no_load_balancer
def setUpModule():
    pass


class SimpleOp(threading.Thread):
    def __init__(self, client):
        super().__init__()
        self.client = client
        self.passed = False

    def run(self):
        self.client.db.command("ping")
        self.passed = True  # No exception raised.


def do_simple_op(client, nthreads):
    threads = [SimpleOp(client) for _ in range(nthreads)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    for t in threads:
        assert t.passed


def writable_addresses(topology):
    return {
        server.description.address for server in topology.select_servers(writable_server_selector)
    }


class TestMongosLoadBalancing(MockClientTest):
    def mock_client(self, **kwargs):
        mock_client = MockClient(
            standalones=[],
            members=[],
            mongoses=["a:1", "b:2", "c:3"],
            host="a:1,b:2,c:3",
            connect=False,
            **kwargs,
        )
        self.addCleanup(mock_client.close)

        # Latencies in seconds.
        mock_client.mock_rtts["a:1"] = 0.020
        mock_client.mock_rtts["b:2"] = 0.025
        mock_client.mock_rtts["c:3"] = 0.045
        return mock_client

    def test_lazy_connect(self):
        # While connected() ensures we can trigger connection from the main
        # thread and wait for the monitors, this test triggers connection from
        # several threads at once to check for data races.
        nthreads = 10
        client = self.mock_client()
        self.assertEqual(0, len(client.nodes))

        # Trigger initial connection.
        do_simple_op(client, nthreads)
        wait_until(lambda: len(client.nodes) == 3, "connect to all mongoses")

    def test_failover(self):
        nthreads = 10
        client = connected(self.mock_client(localThresholdMS=0.001))
        wait_until(lambda: len(client.nodes) == 3, "connect to all mongoses")

        # Our chosen mongos goes down.
        client.kill_host("a:1")

        # Trigger failover to higher-latency nodes. AutoReconnect should be
        # raised at most once in each thread.
        passed = []

        def f():
            try:
                client.db.command("ping")
            except AutoReconnect:
                # Second attempt succeeds.
                client.db.command("ping")

            passed.append(True)

        threads = [threading.Thread(target=f) for _ in range(nthreads)]
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(nthreads, len(passed))

        # Down host removed from list.
        self.assertEqual(2, len(client.nodes))

    def test_local_threshold(self):
        client = connected(self.mock_client(localThresholdMS=30))
        self.assertEqual(30, client.options.local_threshold_ms)
        wait_until(lambda: len(client.nodes) == 3, "connect to all mongoses")
        topology = client._topology

        # All are within a 30-ms latency window, see self.mock_client().
        self.assertEqual({("a", 1), ("b", 2), ("c", 3)}, writable_addresses(topology))

        # No error
        client.admin.command("ping")

        client = connected(self.mock_client(localThresholdMS=0))
        self.assertEqual(0, client.options.local_threshold_ms)
        # No error
        client.db.command("ping")
        # Our chosen mongos goes down.
        client.kill_host("{}:{}".format(*next(iter(client.nodes))))
        try:
            client.db.command("ping")
        except:
            pass

        # We eventually connect to a new mongos.
        def connect_to_new_mongos():
            try:
                return client.db.command("ping")
            except AutoReconnect:
                pass

        wait_until(connect_to_new_mongos, "connect to a new mongos")

    def test_load_balancing(self):
        # Although the server selection JSON tests already prove that
        # select_servers works for sharded topologies, here we do an end-to-end
        # test of discovering servers' round trip times and configuring
        # localThresholdMS.
        client = connected(self.mock_client())
        wait_until(lambda: len(client.nodes) == 3, "connect to all mongoses")

        # Prohibited for topology type Sharded.
        with self.assertRaises(InvalidOperation):
            client.address

        topology = client._topology
        self.assertEqual(TOPOLOGY_TYPE.Sharded, topology.description.topology_type)

        # a and b are within the 15-ms latency window, see self.mock_client().
        self.assertEqual({("a", 1), ("b", 2)}, writable_addresses(topology))

        client.mock_rtts["a:1"] = 0.045

        # Discover only b is within latency window.
        wait_until(
            lambda: {("b", 2)} == writable_addresses(topology),
            'discover server "a" is too far',
        )


if __name__ == "__main__":
    unittest.main()
