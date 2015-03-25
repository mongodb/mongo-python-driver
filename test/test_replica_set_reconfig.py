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

"""Test clients and replica set configuration changes, using mocks."""

import sys

sys.path[0:0] = [""]

from pymongo.errors import ConnectionFailure, AutoReconnect
from pymongo import ReadPreference
from test import unittest, client_context, client_knobs, MockClientTest
from test.pymongo_mocks import MockClient
from test.utils import wait_until


@client_context.require_connection
def setUpModule():
    pass


class TestSecondaryBecomesStandalone(MockClientTest):
    # An administrator removes a secondary from a 3-node set and
    # brings it back up as standalone, without updating the other
    # members' config. Verify we don't continue using it.
    def test_client(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='a:1,b:2,c:3',
            replicaSet='rs',
            serverSelectionTimeoutMS=100)

        # MongoClient connects to primary by default.
        wait_until(lambda: c.address is not None, 'connect to primary')
        self.assertEqual(c.address, ('a', 1))

        # C is brought up as a standalone.
        c.mock_members.remove('c:3')
        c.mock_standalones.append('c:3')

        # Fail over.
        c.kill_host('a:1')
        c.kill_host('b:2')

        # Force reconnect.
        c.close()

        with self.assertRaises(AutoReconnect):
            c.db.command('ismaster')

        self.assertEqual(c.address, None)

    def test_replica_set_client(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='a:1,b:2,c:3',
            replicaSet='rs')

        wait_until(lambda: ('b', 2) in c.secondaries,
                   'discover host "b"')

        wait_until(lambda: ('c', 3) in c.secondaries,
                   'discover host "c"')

        # C is brought up as a standalone.
        c.mock_members.remove('c:3')
        c.mock_standalones.append('c:3')

        wait_until(lambda: set([('b', 2)]) == c.secondaries,
                   'update the list of secondaries')

        self.assertEqual(('a', 1), c.primary)


class TestSecondaryRemoved(MockClientTest):
    # An administrator removes a secondary from a 3-node set *without*
    # restarting it as standalone.
    def test_replica_set_client(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='a:1,b:2,c:3',
            replicaSet='rs')

        wait_until(lambda: ('b', 2) in c.secondaries, 'discover host "b"')
        wait_until(lambda: ('c', 3) in c.secondaries, 'discover host "c"')

        # C is removed.
        c.mock_ismaster_hosts.remove('c:3')
        wait_until(lambda: set([('b', 2)]) == c.secondaries,
                   'update list of secondaries')

        self.assertEqual(('a', 1), c.primary)


class TestSocketError(MockClientTest):
    def test_socket_error_marks_member_down(self):
        # Disable background refresh.
        with client_knobs(heartbeat_frequency=999999):
            c = MockClient(
                standalones=[],
                members=['a:1', 'b:2'],
                mongoses=[],
                host='a:1',
                replicaSet='rs')

            wait_until(lambda: len(c.nodes) == 2, 'discover both nodes')

            # b now raises socket.error.
            c.mock_down_hosts.append('b:2')
            self.assertRaises(
                ConnectionFailure,
                c.db.collection.with_options(
                    read_preference=ReadPreference.SECONDARY).find_one)

            self.assertEqual(1, len(c.nodes))


class TestSecondaryAdded(MockClientTest):
    def test_client(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2'],
            mongoses=[],
            host='a:1',
            replicaSet='rs')

        wait_until(lambda: len(c.nodes) == 2, 'discover both nodes')

        # MongoClient connects to primary by default.
        self.assertEqual(c.address, ('a', 1))
        self.assertEqual(set([('a', 1), ('b', 2)]), c.nodes)

        # C is added.
        c.mock_members.append('c:3')
        c.mock_ismaster_hosts.append('c:3')

        c.close()
        c.db.command('ismaster')

        self.assertEqual(c.address, ('a', 1))

        wait_until(lambda: set([('a', 1), ('b', 2), ('c', 3)]) == c.nodes,
                   'reconnect to both secondaries')

    def test_replica_set_client(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2'],
            mongoses=[],
            host='a:1',
            replicaSet='rs')

        wait_until(lambda: ('a', 1) == c.primary, 'discover the primary')
        wait_until(lambda: set([('b', 2)]) == c.secondaries,
                   'discover the secondary')

        # C is added.
        c.mock_members.append('c:3')
        c.mock_ismaster_hosts.append('c:3')

        wait_until(lambda: set([('b', 2), ('c', 3)]) == c.secondaries,
                   'discover the new secondary')

        self.assertEqual(('a', 1), c.primary)


if __name__ == "__main__":
    unittest.main()
