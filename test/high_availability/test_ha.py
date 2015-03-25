# Copyright 2009-2015 MongoDB, Inc.
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

"""Test replica set operations and failures."""

# These test methods exuberantly violate the "one assert per test" rule, because
# each method requires running setUp, which takes about 30 seconds to bring up
# a replica set. Thus each method asserts everything we want to assert for a
# given replica-set configuration.

import time

import ha_tools
from pymongo import common
from pymongo.common import partition_node
from pymongo.errors import (AutoReconnect,
                            OperationFailure,
                            ConnectionFailure,
                            InvalidOperation,
                            WTimeoutError)
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import ReadPreference
from pymongo.server_description import ServerDescription
from pymongo.write_concern import WriteConcern
from test import unittest, utils, client_knobs
from test.utils import one, wait_until, connected



# To make the code terser, copy modes into module scope
PRIMARY = ReadPreference.PRIMARY
PRIMARY_PREFERRED = ReadPreference.PRIMARY_PREFERRED
SECONDARY = ReadPreference.SECONDARY
SECONDARY_PREFERRED = ReadPreference.SECONDARY_PREFERRED
NEAREST = ReadPreference.NEAREST


def partition_nodes(nodes):
    """Translate from ['host:port', ...] to [(host, port), ...]"""
    return [partition_node(node) for node in nodes]


class HATestCase(unittest.TestCase):
    """A test case for connections to replica sets or mongos."""
    
    # Override default 10-second interval for faster testing...
    heartbeat_frequency = 0.5
    
    # ... or disable it by setting "enable_heartbeat" to False.
    enable_heartbeat = True

    # Override this to speed up connection-failure tests.
    server_selection_timeout = common.SERVER_SELECTION_TIMEOUT

    def setUp(self):
        if self.enable_heartbeat:
            heartbeat_frequency = self.heartbeat_frequency
        else:
            # Disable periodic monitoring.
            heartbeat_frequency = 1e6

        self.knobs = client_knobs(heartbeat_frequency=heartbeat_frequency)

        self.knobs.enable()

    def tearDown(self):
        ha_tools.kill_all_members()
        ha_tools.nodes.clear()
        ha_tools.routers.clear()
        time.sleep(1)  # Let members really die.

        self.knobs.disable()


class TestDirectConnection(HATestCase):

    def setUp(self):
        super(TestDirectConnection, self).setUp()
        members = [{}, {}, {'arbiterOnly': True}]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

    def test_secondary_connection(self):
        self.c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        wait_until(lambda: len(self.c.secondaries), "discover secondary")
        # Wait for replication...
        w = len(self.c.secondaries) + 1
        db = self.c.get_database("pymongo_test",
                                 write_concern=WriteConcern(w=w))

        db.test.delete_many({})
        db.test.insert_one({'foo': 'bar'})

        # Test direct connection to a primary or secondary
        primary_host, primary_port = ha_tools.get_primary().split(':')
        primary_port = int(primary_port)
        (secondary_host,
         secondary_port) = ha_tools.get_secondaries()[0].split(':')
        secondary_port = int(secondary_port)
        arbiter_host, arbiter_port = ha_tools.get_arbiters()[0].split(':')
        arbiter_port = int(arbiter_port)

        # MongoClient succeeds no matter the read preference
        for kwargs in [
            {'read_preference': PRIMARY},
            {'read_preference': PRIMARY_PREFERRED},
            {'read_preference': SECONDARY},
            {'read_preference': SECONDARY_PREFERRED},
            {'read_preference': NEAREST},
        ]:
            client = MongoClient(
                primary_host,
                primary_port,
                serverSelectionTimeoutMS=self.server_selection_timeout,
                **kwargs)
            wait_until(lambda: primary_host == client.host,
                       "connect to primary")

            self.assertEqual(primary_port, client.port)
            self.assertTrue(client.is_primary)

            # Direct connection to primary can be queried with any read pref
            self.assertTrue(client.pymongo_test.test.find_one())

            client = MongoClient(
                secondary_host,
                secondary_port,
                serverSelectionTimeoutMS=self.server_selection_timeout,
                **kwargs)
            wait_until(lambda: secondary_host == client.host,
                       "connect to secondary")

            self.assertEqual(secondary_port, client.port)
            self.assertFalse(client.is_primary)

            # Direct connection to secondary can be queried with any read pref
            # but PRIMARY
            if kwargs.get('read_preference') != PRIMARY:
                self.assertTrue(client.pymongo_test.test.find_one())
            else:
                self.assertRaises(
                    AutoReconnect, client.pymongo_test.test.find_one)

            # Since an attempt at an acknowledged write to a secondary from a
            # direct connection raises AutoReconnect('not master'), MongoClient
            # should do the same for unacknowledged writes.
            try:
                client.get_database(
                    "pymongo_test",
                    write_concern=WriteConcern(w=0)).test.insert_one({})
            except AutoReconnect as e:
                self.assertEqual('not master', e.args[0])
            else:
                self.fail(
                    'Unacknowledged insert into secondary client %s should'
                    'have raised exception' % (client,))

            # Test direct connection to an arbiter
            client = MongoClient(
                arbiter_host,
                arbiter_port,
                serverSelectionTimeoutMS=self.server_selection_timeout,
                **kwargs)
            wait_until(lambda: arbiter_host == client.host,
                       "connect to arbiter")

            self.assertEqual(arbiter_port, client.port)
            self.assertFalse(client.is_primary)
            
            # See explanation above
            try:
                client.get_database(
                    "pymongo_test",
                    write_concern=WriteConcern(w=0)).test.insert_one({})
            except AutoReconnect as e:
                self.assertEqual('not master', e.args[0])
            else:
                self.fail(
                    'Unacknowledged insert into arbiter client %s should'
                    'have raised exception' % (client,))
        

class TestPassiveAndHidden(HATestCase):

    def setUp(self):
        super(TestPassiveAndHidden, self).setUp()

        members = [{},
                   {'priority': 0},
                   {'arbiterOnly': True},
                   {'priority': 0, 'hidden': True},
                   {'priority': 0, 'slaveDelay': 5}
        ]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

    def test_passive_and_hidden(self):
        self.c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)

        passives = ha_tools.get_passives()
        passives = partition_nodes(passives)
        self.assertEqual(self.c.secondaries, set(passives))

        for mode in SECONDARY, SECONDARY_PREFERRED:
            utils.assertReadFromAll(self, self.c, passives, mode)

        ha_tools.kill_members(ha_tools.get_passives(), 2)
        time.sleep(2 * self.heartbeat_frequency)
        utils.assertReadFrom(self, self.c, self.c.primary, SECONDARY_PREFERRED)


class TestMonitorRemovesRecoveringMember(HATestCase):
    # Members in STARTUP2 or RECOVERING states are shown in the primary's
    # isMaster response, but aren't secondaries and shouldn't be read from.
    # Verify that if a secondary goes into RECOVERING mode, the Monitor removes
    # it from the set of readers.

    def setUp(self):
        super(TestMonitorRemovesRecoveringMember, self).setUp()
        members = [{}, {'priority': 0}, {'priority': 0}]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

    def test_monitor_removes_recovering_member(self):
        self.c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)

        secondaries = ha_tools.get_secondaries()

        for mode in SECONDARY, SECONDARY_PREFERRED:
            partitioned_secondaries = partition_nodes(secondaries)
            utils.assertReadFromAll(self, self.c, partitioned_secondaries, mode)

        secondary, recovering_secondary = secondaries
        ha_tools.set_maintenance(recovering_secondary, True)
        time.sleep(2 * self.heartbeat_frequency)

        for mode in SECONDARY, SECONDARY_PREFERRED:
            # Don't read from recovering member
            utils.assertReadFrom(self, self.c, partition_node(secondary), mode)


class TestTriggeredRefresh(HATestCase):
    # Verify that if a secondary goes into RECOVERING mode or if the primary
    # changes, the next exception triggers an immediate refresh.
    
    enable_heartbeat = False

    def setUp(self):
        super(TestTriggeredRefresh, self).setUp()
        members = [{}, {}]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

    def test_recovering_member_triggers_refresh(self):
        # To test that find_one() and count() trigger immediate refreshes,
        # we'll create a separate client for each
        self.c_find_one, self.c_count = [
            MongoClient(
                self.seed,
                replicaSet=self.name,
                read_preference=SECONDARY,
                serverSelectionTimeoutMS=self.server_selection_timeout)
            for _ in xrange(2)]

        # We've started the primary and one secondary
        primary = ha_tools.get_primary()
        secondary = ha_tools.get_secondaries()[0]

        # Pre-condition: just make sure they all connected OK
        for c in self.c_find_one, self.c_count:
            wait_until(
                lambda: c.primary == partition_node(primary),
                'connect to the primary')

            wait_until(
                lambda: one(c.secondaries) == partition_node(secondary),
                'connect to the secondary')

        ha_tools.set_maintenance(secondary, True)

        # Trigger a refresh in various ways
        self.assertRaises(AutoReconnect, self.c_find_one.test.test.find_one)
        self.assertRaises(AutoReconnect, self.c_count.test.test.count)

        # Wait for the immediate refresh to complete - we're not waiting for
        # the periodic refresh, which has been disabled
        time.sleep(1)

        self.assertFalse(self.c_find_one.secondaries)
        self.assertEqual(partition_node(primary), self.c_find_one.primary)

        self.assertFalse(self.c_count.secondaries)
        self.assertEqual(partition_node(primary), self.c_count.primary)

    def test_stepdown_triggers_refresh(self):
        c_find_one = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        c_count = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)

        # We've started the primary and one secondary
        wait_until(lambda: len(c_find_one.secondaries), "discover secondary")
        wait_until(lambda: len(c_count.secondaries), "discover secondary")

        ha_tools.stepdown_primary()

        # Trigger a refresh, both with a cursor and a command.
        self.assertRaises(AutoReconnect, c_find_one.test.test.find_one)
        self.assertRaises(AutoReconnect, c_count.test.command, 'count')

        # Both clients detect the stepdown *AND* re-check the server
        # immediately, they don't just mark it Unknown. Wait for the
        # immediate refresh to complete - we're not waiting for the
        # periodic refresh, which has been disabled
        wait_until(lambda: len(c_find_one.secondaries) == 2,
                   "detect two secondaries")

        wait_until(lambda: len(c_count.secondaries) == 2,
                   "detect two secondaries")


class TestHealthMonitor(HATestCase):

    def setUp(self):
        super(TestHealthMonitor, self).setUp()
        res = ha_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    def test_primary_failure(self):
        c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        wait_until(lambda: c.primary, "discover primary")
        wait_until(lambda: len(c.secondaries) == 2, "discover secondaries")
        old_primary = c.primary
        old_secondaries = c.secondaries

        killed = ha_tools.kill_primary()
        self.assertTrue(bool(len(killed)))
        wait_until(lambda: c.primary and c.primary != old_primary,
                   "discover new primary",
                   timeout=30)

        wait_until(lambda: c.secondaries != old_secondaries,
                   "discover new secondaries",
                   timeout=30)

    def test_secondary_failure(self):
        c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        wait_until(lambda: c.primary, "discover primary")
        wait_until(lambda: len(c.secondaries) == 2, "discover secondaries")
        primary = c.primary
        old_secondaries = c.secondaries

        killed = ha_tools.kill_secondary()
        time.sleep(2 * self.heartbeat_frequency)
        self.assertTrue(bool(len(killed)))
        self.assertEqual(primary, c.primary)
        wait_until(lambda: c.secondaries != old_secondaries,
                   "discover new secondaries",
                   timeout=30)

        old_secondaries = c.secondaries
        ha_tools.restart_members([killed])
        self.assertEqual(primary, c.primary)
        wait_until(lambda: c.secondaries != old_secondaries,
                   "discover new secondaries",
                   timeout=30)

    def test_primary_stepdown(self):
        c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        wait_until(lambda: c.primary, "discover primary")
        wait_until(lambda: len(c.secondaries) == 2, "discover secondaries")

        ha_tools.stepdown_primary()

        # Wait for new primary.
        wait_until(lambda:
                   (ha_tools.get_primary()
                    and c.primary == partition_node(ha_tools.get_primary())),
                   "discover new primary",
                   timeout=30)

        wait_until(lambda: len(c.secondaries) == 2,
                   "discover new secondaries",
                   timeout=30)


class TestWritesWithFailover(HATestCase):

    enable_heartbeat = False

    def setUp(self):
        super(TestWritesWithFailover, self).setUp()
        res = ha_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    def test_writes_with_failover(self):
        c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        wait_until(lambda: c.primary, "discover primary")
        wait_until(lambda: len(c.secondaries) == 2, "discover secondaries")
        primary = c.primary
        w = len(c.secondaries) + 1
        db = c.get_database("pymongo_test",
                            write_concern=WriteConcern(w=w))
        db.test.delete_many({})
        db.test.insert_one({'foo': 'bar'})
        self.assertEqual('bar', db.test.find_one()['foo'])

        killed = ha_tools.kill_primary(9)
        self.assertTrue(bool(len(killed)))

        # Wait past pool's check interval, so it throws an error from
        # get_socket().
        time.sleep(1)

        # Verify that we only raise AutoReconnect, not some other error,
        # while we wait for new primary.
        for _ in xrange(10000):
            try:
                db.test.insert_one({'bar': 'baz'})

                # No error, found primary.
                break
            except AutoReconnect:
                time.sleep(.01)
        else:
            self.fail("Couldn't connect to new primary")

        # Found new primary.
        self.assertTrue(c.primary)
        self.assertTrue(primary != c.primary)
        self.assertEqual('baz', db.test.find_one({'bar': 'baz'})['bar'])


class TestReadWithFailover(HATestCase):

    def setUp(self):
        super(TestReadWithFailover, self).setUp()
        res = ha_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    def test_read_with_failover(self):
        c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        wait_until(lambda: c.primary, "discover primary")
        wait_until(lambda: len(c.secondaries) == 2, "discover secondaries")

        def iter_cursor(cursor):
            for _ in cursor:
                pass
            return True

        w = len(c.secondaries) + 1
        db = c.get_database("pymongo_test",
                            write_concern=WriteConcern(w=w))
        db.test.delete_many({})
        # Force replication
        db.test.insert_many([{'foo': i} for i in xrange(10)])
        self.assertEqual(10, db.test.count())

        db.read_preference = SECONDARY_PREFERRED
        cursor = db.test.find().batch_size(5)
        next(cursor)
        self.assertEqual(5, cursor._Cursor__retrieved)
        self.assertTrue(cursor.address in c.secondaries)
        ha_tools.kill_primary()
        # Primary failure shouldn't interrupt the cursor
        self.assertTrue(iter_cursor(cursor))
        self.assertEqual(10, cursor._Cursor__retrieved)


class TestReadPreference(HATestCase):

    # Speed up assertReadFrom() when no server is suitable.
    server_selection_timeout = 0.001

    def setUp(self):
        super(TestReadPreference, self).setUp()

        members = [
            # primary
            {'tags': {'dc': 'ny', 'name': 'primary'}},

            # secondary
            {'tags': {'dc': 'la', 'name': 'secondary'}, 'priority': 0},

            # other_secondary
            {'tags': {'dc': 'ny', 'name': 'other_secondary'}, 'priority': 0},
        ]

        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

        primary = ha_tools.get_primary()
        self.primary = partition_node(primary)
        self.primary_tags = ha_tools.get_tags(primary)
        # Make sure priority worked
        self.assertEqual('primary', self.primary_tags['name'])

        self.primary_dc = {'dc': self.primary_tags['dc']}

        secondaries = ha_tools.get_secondaries()

        (secondary, ) = [
            s for s in secondaries
            if ha_tools.get_tags(s)['name'] == 'secondary']

        self.secondary = partition_node(secondary)
        self.secondary_tags = ha_tools.get_tags(secondary)
        self.secondary_dc = {'dc': self.secondary_tags['dc']}

        (other_secondary, ) = [
            s for s in secondaries
            if ha_tools.get_tags(s)['name'] == 'other_secondary']

        self.other_secondary = partition_node(other_secondary)
        self.other_secondary_tags = ha_tools.get_tags(other_secondary)
        self.other_secondary_dc = {'dc': self.other_secondary_tags['dc']}

        self.c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        self.w = len(self.c.secondaries) + 1
        self.db = self.c.get_database("pymongo_test",
                                      write_concern=WriteConcern(w=self.w))
        self.db.test.delete_many({})
        self.db.test.insert_many([{'foo': i} for i in xrange(10)])

        self.clear_ping_times()

    def set_ping_time(self, host, ping_time_seconds):
        ServerDescription._host_to_round_trip_time[host] = ping_time_seconds

    def clear_ping_times(self):
        ServerDescription._host_to_round_trip_time.clear()

    def test_read_preference(self):
        # We pass through four states:
        #
        #       1. A primary and two secondaries
        #       2. Primary down
        #       3. Primary up, one secondary down
        #       4. Primary up, all secondaries down
        #
        # For each state, we verify the behavior of PRIMARY,
        # PRIMARY_PREFERRED, SECONDARY, SECONDARY_PREFERRED, and NEAREST
        c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        wait_until(lambda: c.primary, "discover primary")
        wait_until(lambda: len(c.secondaries) == 2, "discover secondaries")

        def assertReadFrom(member, *args, **kwargs):
            utils.assertReadFrom(self, c, member, *args, **kwargs)

        def assertReadFromAll(members, *args, **kwargs):
            utils.assertReadFromAll(self, c, members, *args, **kwargs)

        def unpartition_node(node):
            host, port = node
            return '%s:%s' % (host, port)

        # To make the code terser, copy hosts into local scope
        primary = self.primary
        secondary = self.secondary
        other_secondary = self.other_secondary

        bad_tag = {'bad': 'tag'}

        # 1. THREE MEMBERS UP -------------------------------------------------
        #       PRIMARY
        assertReadFrom(primary, PRIMARY)

        #       PRIMARY_PREFERRED
        # Trivial: mode and tags both match
        assertReadFrom(primary, PRIMARY_PREFERRED, self.primary_dc)

        # Secondary matches but not primary, choose primary
        assertReadFrom(primary, PRIMARY_PREFERRED, self.secondary_dc)

        # Chooses primary, ignoring tag sets
        assertReadFrom(primary, PRIMARY_PREFERRED, self.primary_dc)

        # Chooses primary, ignoring tag sets
        assertReadFrom(primary, PRIMARY_PREFERRED, bad_tag)
        assertReadFrom(primary, PRIMARY_PREFERRED, [bad_tag, {}])

        #       SECONDARY
        assertReadFromAll([secondary, other_secondary], SECONDARY)

        #       SECONDARY_PREFERRED
        assertReadFromAll([secondary, other_secondary], SECONDARY_PREFERRED)

        # Multiple tags
        assertReadFrom(secondary, SECONDARY_PREFERRED, self.secondary_tags)

        # Fall back to primary if it's the only one matching the tags
        assertReadFrom(primary, SECONDARY_PREFERRED, {'name': 'primary'})

        # No matching secondaries
        assertReadFrom(primary, SECONDARY_PREFERRED, bad_tag)

        # Fall back from non-matching tag set to matching set
        assertReadFromAll([secondary, other_secondary],
            SECONDARY_PREFERRED, [bad_tag, {}])

        assertReadFrom(other_secondary,
            SECONDARY_PREFERRED, [bad_tag, {'dc': 'ny'}])

        #       NEAREST
        self.clear_ping_times()

        assertReadFromAll([primary, secondary, other_secondary], NEAREST)

        assertReadFromAll([primary, other_secondary],
            NEAREST, [bad_tag, {'dc': 'ny'}])

        self.set_ping_time(primary, 0)
        self.set_ping_time(secondary, .03) # 30 ms
        self.set_ping_time(other_secondary, 10)

        # Nearest member, no tags
        assertReadFrom(primary, NEAREST)

        # Tags override nearness
        assertReadFrom(primary, NEAREST, {'name': 'primary'})
        assertReadFrom(secondary, NEAREST, self.secondary_dc)

        # Make secondary fast
        self.set_ping_time(primary, .03) # 30 ms
        self.set_ping_time(secondary, 0)

        assertReadFrom(secondary, NEAREST)

        # Other secondary fast
        self.set_ping_time(secondary, 10)
        self.set_ping_time(other_secondary, 0)

        assertReadFrom(other_secondary, NEAREST)

        self.clear_ping_times()

        assertReadFromAll([primary, other_secondary], NEAREST, [{'dc': 'ny'}])

        # 2. PRIMARY DOWN -----------------------------------------------------
        killed = ha_tools.kill_primary()

        # Let monitor notice primary's gone
        time.sleep(2 * self.heartbeat_frequency)

        #       PRIMARY
        assertReadFrom(None, PRIMARY)

        #       PRIMARY_PREFERRED
        # No primary, choose matching secondary
        assertReadFromAll([secondary, other_secondary], PRIMARY_PREFERRED)
        assertReadFrom(secondary, PRIMARY_PREFERRED, {'name': 'secondary'})

        # No primary or matching secondary
        assertReadFrom(None, PRIMARY_PREFERRED, bad_tag)

        #       SECONDARY
        assertReadFromAll([secondary, other_secondary], SECONDARY)

        # Only primary matches
        assertReadFrom(None, SECONDARY, {'name': 'primary'})

        # No matching secondaries
        assertReadFrom(None, SECONDARY, bad_tag)

        #       SECONDARY_PREFERRED
        assertReadFromAll([secondary, other_secondary], SECONDARY_PREFERRED)

        # Mode and tags both match
        assertReadFrom(secondary, SECONDARY_PREFERRED, {'name': 'secondary'})

        #       NEAREST
        self.clear_ping_times()

        assertReadFromAll([secondary, other_secondary], NEAREST)

        # 3. PRIMARY UP, ONE SECONDARY DOWN -----------------------------------
        ha_tools.restart_members([killed])
        ha_tools.wait_for_primary()

        ha_tools.kill_members([unpartition_node(secondary)], 2)
        time.sleep(5)
        ha_tools.wait_for_primary()
        time.sleep(2 * self.heartbeat_frequency)

        #       PRIMARY
        assertReadFrom(primary, PRIMARY)

        #       PRIMARY_PREFERRED
        assertReadFrom(primary, PRIMARY_PREFERRED)

        #       SECONDARY
        assertReadFrom(other_secondary, SECONDARY)
        assertReadFrom(other_secondary, SECONDARY, self.other_secondary_dc)

        # Only the down secondary matches
        assertReadFrom(None, SECONDARY, {'name': 'secondary'})

        #       SECONDARY_PREFERRED
        assertReadFrom(other_secondary, SECONDARY_PREFERRED)
        assertReadFrom(
            other_secondary, SECONDARY_PREFERRED, self.other_secondary_dc)

        # The secondary matching the tag is down, use primary
        assertReadFrom(primary, SECONDARY_PREFERRED, {'name': 'secondary'})

        #       NEAREST
        assertReadFromAll([primary, other_secondary], NEAREST)
        assertReadFrom(other_secondary, NEAREST, {'name': 'other_secondary'})
        assertReadFrom(primary, NEAREST, {'name': 'primary'})

        # 4. PRIMARY UP, ALL SECONDARIES DOWN ---------------------------------
        ha_tools.kill_members([unpartition_node(other_secondary)], 2)

        #       PRIMARY
        assertReadFrom(primary, PRIMARY)

        #       PRIMARY_PREFERRED
        assertReadFrom(primary, PRIMARY_PREFERRED)
        assertReadFrom(primary, PRIMARY_PREFERRED, self.secondary_dc)

        #       SECONDARY
        assertReadFrom(None, SECONDARY)
        assertReadFrom(None, SECONDARY, self.other_secondary_dc)
        assertReadFrom(None, SECONDARY, {'dc': 'ny'})

        #       SECONDARY_PREFERRED
        assertReadFrom(primary, SECONDARY_PREFERRED)
        assertReadFrom(primary, SECONDARY_PREFERRED, self.secondary_dc)
        assertReadFrom(primary, SECONDARY_PREFERRED, {'name': 'secondary'})
        assertReadFrom(primary, SECONDARY_PREFERRED, {'dc': 'ny'})

        #       NEAREST
        assertReadFrom(primary, NEAREST)
        assertReadFrom(None, NEAREST, self.secondary_dc)
        assertReadFrom(None, NEAREST, {'name': 'secondary'})

        # Even if primary's slow, still read from it
        self.set_ping_time(primary, 100)
        assertReadFrom(primary, NEAREST)
        assertReadFrom(None, NEAREST, self.secondary_dc)

        self.clear_ping_times()


class TestReplicaSetAuth(HATestCase):
    def setUp(self):
        super(TestReplicaSetAuth, self).setUp()

        members = [
            {},
            {'priority': 0},
            {'priority': 0},
        ]

        res = ha_tools.start_replica_set(members, auth=True)
        self.c = MongoClient(
            res[0],
            replicaSet=res[1],
            serverSelectionTimeoutMS=self.server_selection_timeout)

        # Add an admin user to enable auth
        self.c.admin.add_user('admin', 'adminpass')
        self.c.admin.authenticate('admin', 'adminpass')

        self.db = self.c.pymongo_ha_auth
        self.db.add_user('user', 'userpass')
        self.c.admin.logout()

    def test_auth_during_failover(self):
        self.assertTrue(self.db.authenticate('user', 'userpass'))
        db = self.db.client.get_database(
            self.db.name, write_concern=WriteConcern(w=3, wtimeout=3000))
        self.assertTrue(db.foo.insert_one({'foo': 'bar'}))
        self.db.logout()
        self.assertRaises(OperationFailure, self.db.foo.find_one)

        primary = self.c.primary
        ha_tools.kill_members(['%s:%d' % primary], 2)

        # Let monitor notice primary's gone
        time.sleep(2 * self.heartbeat_frequency)
        self.assertFalse(primary == self.c.primary)

        # Make sure we can still authenticate
        self.assertTrue(self.db.authenticate('user', 'userpass'))
        # And still query.
        self.db.read_preference = PRIMARY_PREFERRED
        self.assertEqual('bar', self.db.foo.find_one()['foo'])


class TestAlive(HATestCase):
    def setUp(self):
        super(TestAlive, self).setUp()

        members = [{}, {}]
        self.seed, self.name = ha_tools.start_replica_set(members)

    def test_alive(self):
        primary = ha_tools.get_primary()
        secondary = ha_tools.get_random_secondary()
        primary_cx = connected(
            MongoClient(
                primary,
                serverSelectionTimeoutMS=self.server_selection_timeout)),
        secondary_cx = connected(
            MongoClient(
                secondary,
                serverSelectionTimeoutMS=self.server_selection_timeout))
        rsc = connected(
            MongoClient(
                self.seed,
                replicaSet=self.name,
                serverSelectionTimeoutMS=self.server_selection_timeout))

        self.assertTrue(primary_cx.alive())
        self.assertTrue(secondary_cx.alive())
        self.assertTrue(rsc.alive())

        ha_tools.kill_primary()
        time.sleep(0.5)

        self.assertFalse(primary_cx.alive())
        self.assertTrue(secondary_cx.alive())
        self.assertFalse(rsc.alive())

        ha_tools.kill_members([secondary], 2)
        time.sleep(0.5)

        self.assertFalse(primary_cx.alive())
        self.assertFalse(secondary_cx.alive())
        self.assertFalse(rsc.alive())


class TestMongosLoadBalancing(HATestCase):
    def setUp(self):
        super(TestMongosLoadBalancing, self).setUp()
        seed_list = ha_tools.create_sharded_cluster()
        self.assertIsNotNone(seed_list)
        self.dbname = 'pymongo_mongos_ha'
        self.client = MongoClient(
            seed_list,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        self.client.drop_database(self.dbname)

    def test_mongos_load_balancing(self):
        wait_until(lambda: len(ha_tools.routers) == len(self.client.nodes),
                   'discover all mongoses')

        # Can't access "address" when load balancing.
        with self.assertRaises(InvalidOperation):
            self.client.address

        coll = self.client[self.dbname].test
        coll.insert_one({'foo': 'bar'})

        live_routers = list(ha_tools.routers)
        ha_tools.kill_mongos(live_routers.pop())
        while live_routers:
            try:
                self.assertEqual(1, coll.count())
            except ConnectionFailure:
                # If first attempt happened to select the dead mongos.
                self.assertEqual(1, coll.count())

            wait_until(lambda: len(live_routers) == len(self.client.nodes),
                       'remove dead mongos',
                       timeout=30)
            ha_tools.kill_mongos(live_routers.pop())

        # Make sure the last one's really dead.
        time.sleep(1)

        # I'm alone.
        self.assertRaises(ConnectionFailure, coll.count)
        wait_until(lambda: 0 == len(self.client.nodes),
                   'remove dead mongos',
                   timeout=30)

        ha_tools.restart_mongos(one(ha_tools.routers))

        # Find new mongos
        self.assertEqual(1, coll.count())


class TestLastErrorDefaults(HATestCase):

    def setUp(self):
        super(TestLastErrorDefaults, self).setUp()

        members = [{}, {}]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res
        self.c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)

    def test_get_last_error_defaults(self):
        replset = self.c.local.system.replset.find_one()
        settings = replset.get('settings', {})
        # This should cause a WTimeoutError for every write command
        settings['getLastErrorDefaults'] = {
            'w': 3,
            'wtimeout': 1
        }
        replset['settings'] = settings
        replset['version'] = replset.get("version", 1) + 1

        self.c.admin.command("replSetReconfig", replset)

        self.assertRaises(WTimeoutError, self.c.pymongo_test.test.insert_one,
                          {'_id': 0})
        self.assertRaises(WTimeoutError, self.c.pymongo_test.test.update_one,
                          {'_id': 0}, {"$set": {"a": 10}})
        self.assertRaises(WTimeoutError, self.c.pymongo_test.test.delete_one,
                          {'_id': 0})


class TestShipOfTheseus(HATestCase):
    # If all of a replica set's members are replaced with new ones, is it still
    # the same replica set, or a different one?
    def setUp(self):
        super(TestShipOfTheseus, self).setUp()
        res = ha_tools.start_replica_set([{}, {}])
        self.seed, self.name = res

    def test_ship_of_theseus(self):
        c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        db = c.get_database(
            "pymongo_test",
            write_concern=WriteConcern(w=len(c.secondaries) + 1))
        db.test.insert_one({})
        find_one = db.test.find_one

        primary = ha_tools.get_primary()
        secondary1 = ha_tools.get_random_secondary()

        new_hosts = []
        for i in range(3):
            new_hosts.append(ha_tools.add_member())

            # RS closes all connections after reconfig.
            for j in xrange(30):
                try:
                    if ha_tools.get_primary():
                        break
                except (ConnectionFailure, OperationFailure):
                    pass

                time.sleep(1)
            else:
                self.fail("Couldn't recover from reconfig")

        # Wait for new members to join.
        for _ in xrange(120):
            if ha_tools.get_primary() and len(ha_tools.get_secondaries()) == 4:
                break

            time.sleep(1)
        else:
            self.fail("New secondaries didn't join")

        ha_tools.kill_members([primary, secondary1], 9)
        time.sleep(5)

        wait_until(lambda: (ha_tools.get_primary()
                            and len(ha_tools.get_secondaries()) == 2),
                   "fail over",
                   timeout=30)

        time.sleep(2 * self.heartbeat_frequency)

        # No error.
        find_one()
        find_one(read_preference=SECONDARY)

        # All members down.
        ha_tools.kill_members(new_hosts, 9)
        self.assertRaises(
            ConnectionFailure,
            find_one, read_preference=SECONDARY)

        ha_tools.restart_members(new_hosts)

        # Should be able to reconnect to set even though original seed
        # list is useless. Use SECONDARY so we don't have to wait for
        # the election, merely for the client to detect members are up.
        time.sleep(2 * self.heartbeat_frequency)
        find_one(read_preference=SECONDARY)

        # Kill new members and switch back to original two members.
        ha_tools.kill_members(new_hosts, 9)
        self.assertRaises(
            ConnectionFailure,
            find_one, read_preference=SECONDARY)

        ha_tools.restart_members([primary, secondary1])

        # Wait for members to figure out they're secondaries.
        wait_until(lambda: len(ha_tools.get_secondaries()) == 2,
                   "detect two secondaries",
                   timeout=30)

        # Should be able to reconnect to set again.
        time.sleep(2 * self.heartbeat_frequency)
        find_one(read_preference=SECONDARY)


class TestLastError(HATestCase):
    # A "not master" error from Database.error() should refresh the server.
    enable_heartbeat = False

    def setUp(self):
        super(TestLastError, self).setUp()
        res = ha_tools.start_replica_set([{}, {}])
        self.seed, self.name = res

    def test_last_error(self):
        c = MongoClient(
            self.seed,
            replicaSet=self.name,
            serverSelectionTimeoutMS=self.server_selection_timeout)
        wait_until(lambda: c.primary, "discover primary")
        wait_until(lambda: c.secondaries, "discover secondary")
        ha_tools.stepdown_primary()
        db = c.get_database(
            "pymongo_test", write_concern=WriteConcern(w=0))

        db.test.insert_one({})
        response = db.error()
        self.assertTrue('err' in response and 'not master' in response['err'])
        wait_until(lambda: len(c.secondaries) == 2, "discover two secondaries")


if __name__ == '__main__':
    unittest.main()
