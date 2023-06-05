# Copyright 2015 MongoDB, Inc.
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

import unittest

from mockupdb import Future, MockupDB, OpReply, going, wait_until

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.topology_description import TOPOLOGY_TYPE


class TestNetworkDisconnectPrimary(unittest.TestCase):
    def test_network_disconnect_primary(self):
        # Application operation fails against primary. Test that topology
        # type changes from ReplicaSetWithPrimary to ReplicaSetNoPrimary.
        # http://bit.ly/1B5ttuL
        primary, secondary = servers = [MockupDB() for _ in range(2)]
        for server in servers:
            server.run()
            self.addCleanup(server.stop)

        hosts = [server.address_string for server in servers]
        primary_response = OpReply(
            ismaster=True, setName="rs", hosts=hosts, minWireVersion=2, maxWireVersion=6
        )
        primary.autoresponds("ismaster", primary_response)
        secondary.autoresponds(
            "ismaster",
            ismaster=False,
            secondary=True,
            setName="rs",
            hosts=hosts,
            minWireVersion=2,
            maxWireVersion=6,
        )

        client = MongoClient(primary.uri, replicaSet="rs")
        self.addCleanup(client.close)
        wait_until(lambda: client.primary == primary.address, "discover primary")

        topology = client._topology
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary, topology.description.topology_type)

        # Open a socket in the application pool (calls ismaster).
        with going(client.db.command, "buildinfo"):
            primary.receives("buildinfo").ok()

        # The primary hangs replying to ismaster.
        ismaster_future = Future()
        primary.autoresponds("ismaster", lambda r: r.ok(ismaster_future.result()))

        # Network error on application operation.
        with self.assertRaises(ConnectionFailure):
            with going(client.db.command, "buildinfo"):
                primary.receives("buildinfo").hangup()

        # Topology type is updated.
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary, topology.description.topology_type)

        # Let ismasters through again.
        ismaster_future.set_result(primary_response)

        # Demand a primary.
        with going(client.db.command, "buildinfo"):
            wait_until(lambda: client.primary == primary.address, "rediscover primary")
            primary.receives("buildinfo").ok()

        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary, topology.description.topology_type)


if __name__ == "__main__":
    unittest.main()
