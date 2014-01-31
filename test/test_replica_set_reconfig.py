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

"""Test clients and replica set configuration changes, using mocks."""

import sys
import unittest

sys.path[0:0] = [""]

from pymongo.errors import ConfigurationError, ConnectionFailure
from pymongo import ReadPreference
from test.pymongo_mocks import MockClient, MockReplicaSetClient


class TestSecondaryBecomesStandalone(unittest.TestCase):
    # An administrator removes a secondary from a 3-node set and
    # brings it back up as standalone, without updating the other
    # members' config. Verify we don't continue using it.
    def test_client(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='a:1,b:2,c:3',
            replicaSet='rs')

        # MongoClient connects to primary by default.
        self.assertEqual('a', c.host)
        self.assertEqual(1, c.port)

        # C is brought up as a standalone.
        c.mock_members.remove('c:3')
        c.mock_standalones.append('c:3')

        # Fail over.
        c.kill_host('a:1')
        c.kill_host('b:2')

        # Force reconnect.
        c.disconnect()

        try:
            c.db.collection.find_one()
        except ConfigurationError, e:
            self.assertTrue('not a member of replica set' in str(e))
        else:
            self.fail("MongoClient didn't raise AutoReconnect")

        self.assertEqual(None, c.host)
        self.assertEqual(None, c.port)

    def test_replica_set_client(self):
        c = MockReplicaSetClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='a:1,b:2,c:3',
            replicaSet='rs')

        self.assertTrue(('b', 2) in c.secondaries)
        self.assertTrue(('c', 3) in c.secondaries)

        # C is brought up as a standalone.
        c.mock_members.remove('c:3')
        c.mock_standalones.append('c:3')
        c.refresh()

        self.assertEqual(('a', 1), c.primary)
        self.assertEqual(set([('b', 2)]), c.secondaries)


class TestSecondaryRemoved(unittest.TestCase):
    # An administrator removes a secondary from a 3-node set *without*
    # restarting it as standalone.
    def test_replica_set_client(self):
        c = MockReplicaSetClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='a:1,b:2,c:3',
            replicaSet='rs')

        self.assertTrue(('b', 2) in c.secondaries)
        self.assertTrue(('c', 3) in c.secondaries)

        # C is removed.
        c.mock_ismaster_hosts.remove('c:3')
        c.refresh()

        self.assertEqual(('a', 1), c.primary)
        self.assertEqual(set([('b', 2)]), c.secondaries)


class TestSocketError(unittest.TestCase):
    def test_socket_error_marks_member_down(self):
        c = MockReplicaSetClient(
            standalones=[],
            members=['a:1', 'b:2'],
            mongoses=[],
            host='a:1',
            replicaSet='rs')

        self.assertEqual(2, len(c._MongoReplicaSetClient__rs_state.members))

        # b now raises socket.error.
        c.mock_down_hosts.append('b:2')
        self.assertRaises(
            ConnectionFailure,
            c.db.collection.find_one, read_preference=ReadPreference.SECONDARY)

        self.assertEqual(1, len(c._MongoReplicaSetClient__rs_state.members))


class TestSecondaryAdded(unittest.TestCase):
    def test_client(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2'],
            mongoses=[],
            host='a:1',
            replicaSet='rs')

        # MongoClient connects to primary by default.
        self.assertEqual('a', c.host)
        self.assertEqual(1, c.port)
        self.assertEqual(set([('a', 1), ('b', 2)]), c.nodes)

        # C is added.
        c.mock_members.append('c:3')
        c.mock_ismaster_hosts.append('c:3')

        c.disconnect()
        c.db.collection.find_one()

        self.assertEqual('a', c.host)
        self.assertEqual(1, c.port)
        self.assertEqual(set([('a', 1), ('b', 2), ('c', 3)]), c.nodes)

    def test_replica_set_client(self):
        c = MockReplicaSetClient(
            standalones=[],
            members=['a:1', 'b:2'],
            mongoses=[],
            host='a:1',
            replicaSet='rs')

        self.assertEqual(('a', 1), c.primary)
        self.assertEqual(set([('b', 2)]), c.secondaries)

        # C is added.
        c.mock_members.append('c:3')
        c.mock_ismaster_hosts.append('c:3')
        c.refresh()

        self.assertEqual(('a', 1), c.primary)
        self.assertEqual(set([('b', 2), ('c', 3)]), c.secondaries)


if __name__ == "__main__":
    unittest.main()
