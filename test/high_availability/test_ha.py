# Copyright 2009-2012 10gen, Inc.
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

import time
import unittest

import ha_tools
from ha_tools import use_greenlets


from pymongo import (ReplicaSetConnection,
                     ReadPreference)
from pymongo.replica_set_connection import Member, Monitor
from pymongo.connection import Connection, _partition_node
from pymongo.errors import AutoReconnect, OperationFailure

from test import utils


# Override default 30-second interval for faster testing
Monitor._refresh_interval = MONITOR_INTERVAL = 0.5


class TestSecondaryConnection(unittest.TestCase):

    def setUp(self):
        members = [{}, {}, {'arbiterOnly': True}]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

    def test_secondary_connection(self):
        self.c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(self.c.secondaries)))
        db = self.c.pymongo_test
        db.test.remove({}, safe=True, w=len(self.c.secondaries))

        # Force replication...
        w = len(self.c.secondaries) + 1
        db.test.insert({'foo': 'bar'}, safe=True, w=w)

        # Test direct connection to a primary or secondary
        primary_host, primary_port = ha_tools.get_primary().split(':')
        primary_port = int(primary_port)
        (secondary_host,
         secondary_port) = ha_tools.get_secondaries()[0].split(':')
        secondary_port = int(secondary_port)

        self.assertTrue(Connection(
            primary_host, primary_port, use_greenlets=use_greenlets).is_primary)

        self.assertTrue(Connection(
            primary_host, primary_port, use_greenlets=use_greenlets,
            read_preference=ReadPreference.PRIMARY_PREFERRED).is_primary)

        self.assertTrue(Connection(
            primary_host, primary_port, use_greenlets=use_greenlets,
            read_preference=ReadPreference.SECONDARY_PREFERRED).is_primary)

        self.assertTrue(Connection(
            primary_host, primary_port, use_greenlets=use_greenlets,
            read_preference=ReadPreference.NEAREST).is_primary)

        self.assertTrue(Connection(
            primary_host, primary_port, use_greenlets=use_greenlets,
            read_preference=ReadPreference.SECONDARY).is_primary)

        for kwargs in [
            {'read_preference': ReadPreference.PRIMARY_PREFERRED},
            {'read_preference': ReadPreference.SECONDARY},
            {'read_preference': ReadPreference.SECONDARY_PREFERRED},
            {'read_preference': ReadPreference.NEAREST},
            {'slave_okay': True},
        ]:
            conn = Connection(secondary_host,
                              secondary_port,
                              use_greenlets=use_greenlets,
                              **kwargs)
            self.assertEqual(secondary_host, conn.host)
            self.assertEqual(secondary_port, conn.port)
            self.assertFalse(conn.is_primary)
            self.assert_(conn.pymongo_test.test.find_one())

        # Test direct connection to an arbiter
        secondary_host = ha_tools.get_arbiters()[0]
        host, port = ha_tools.get_arbiters()[0].split(':')
        port = int(port)
        conn = Connection(host, port)
        self.assertEqual(host, conn.host)
        self.assertEqual(port, conn.port)

    def tearDown(self):
        self.c.close()
        ha_tools.kill_all_members()


class TestPassiveAndHidden(unittest.TestCase):

    def setUp(self):
        members = [{},
                   {'priority': 0},
                   {'arbiterOnly': True},
                   {'priority': 0, 'hidden': True},
                   {'priority': 0, 'slaveDelay': 5}
        ]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

    def test_passive_and_hidden(self):
        self.c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        db = self.c.pymongo_test
        w = len(self.c.secondaries) + 1
        db.test.remove({}, safe=True, w=w)
        db.test.insert({'foo': 'bar'}, safe=True, w=w)

        passives = ha_tools.get_passives()
        passives = [_partition_node(member) for member in passives]
        hidden = ha_tools.get_hidden_members()
        hidden = [_partition_node(member) for member in hidden]
        self.assertEqual(self.c.secondaries, set(passives))

        for mode in (
            ReadPreference.SECONDARY, ReadPreference.SECONDARY_PREFERRED
        ):
            db.read_preference = mode
            for _ in xrange(10):
                cursor = db.test.find()
                cursor.next()
                self.assertTrue(cursor._Cursor__connection_id in passives)
                self.assertTrue(cursor._Cursor__connection_id not in hidden)

        ha_tools.kill_members(ha_tools.get_passives(), 2)
        sleep(2 * MONITOR_INTERVAL)
        db.read_preference = ReadPreference.SECONDARY_PREFERRED

        for _ in xrange(10):
            cursor = db.test.find()
            cursor.next()
            self.assertEqual(cursor._Cursor__connection_id, self.c.primary)

    def tearDown(self):
        self.c.close()
        ha_tools.kill_all_members()


class TestHealthMonitor(unittest.TestCase):

    def setUp(self):
        res = ha_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    def test_primary_failure(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries

        # Wait for new primary to be elected
        def primary_changed():
            for _ in xrange(30):
                if c.primary and c.primary != primary:
                    return True
                sleep(1)
            return False

        killed = ha_tools.kill_primary()
        self.assertTrue(bool(len(killed)))
        self.assertTrue(primary_changed())
        self.assertNotEqual(secondaries, c.secondaries)

    def test_secondary_failure(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries

        def readers_changed():
            for _ in xrange(20):
                if c.secondaries != secondaries:
                    return True

                sleep(1)
            return False

        killed = ha_tools.kill_secondary()
        sleep(2 * MONITOR_INTERVAL)
        self.assertTrue(bool(len(killed)))
        self.assertEqual(primary, c.primary)
        self.assertTrue(readers_changed())
        secondaries = c.secondaries

        ha_tools.restart_members([killed])
        self.assertEqual(primary, c.primary)
        self.assertTrue(readers_changed())

    def test_primary_stepdown(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries.copy()

        def primary_changed():
            for _ in xrange(30):
                if c.primary != primary:
                    return True
                sleep(1)
            return False

        ha_tools.stepdown_primary()
        self.assertTrue(primary_changed())

        # There can be a delay between finding the primary and updating
        # secondaries
        sleep(5)
        self.assertNotEqual(secondaries, c.secondaries)

    def tearDown(self):
        ha_tools.kill_all_members()


class TestWritesWithFailover(unittest.TestCase):

    def setUp(self):
        res = ha_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    def test_writes_with_failover(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        primary = c.primary
        db = c.pymongo_test
        w = len(c.secondaries) + 1
        db.test.remove({}, safe=True, w=w)
        db.test.insert({'foo': 'bar'}, safe=True, w=w)
        self.assertEqual('bar', db.test.find_one()['foo'])

        def try_write():
            for _ in xrange(30):
                try:
                    db.test.insert({'bar': 'baz'}, safe=True)
                    return True
                except AutoReconnect:
                    sleep(1)
            return False

        killed = ha_tools.kill_primary(9)
        self.assertTrue(bool(len(killed)))
        self.assertTrue(try_write())
        self.assertTrue(primary != c.primary)
        self.assertEqual('baz', db.test.find_one({'bar': 'baz'})['bar'])

    def tearDown(self):
        ha_tools.kill_all_members()


class TestReadWithFailover(unittest.TestCase):

    def setUp(self):
        res = ha_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    def test_read_with_failover(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets,
            auto_start_request=False)
        self.assertTrue(bool(len(c.secondaries)))

        def iter_cursor(cursor):
            for _ in cursor:
                pass
            return True

        db = c.pymongo_test
        w = len(c.secondaries) + 1
        db.test.remove({}, safe=True, w=w)
        # Force replication
        db.test.insert([{'foo': i} for i in xrange(10)], safe=True, w=w)
        self.assertEqual(10, db.test.count())

        db.read_preference = ReadPreference.SECONDARY_PREFERRED
        cursor = db.test.find().batch_size(5)
        cursor.next()
        self.assertEqual(5, cursor._Cursor__retrieved)
        self.assertTrue(cursor._Cursor__connection_id in c.secondaries)
        ha_tools.kill_primary()
        # Primary failure shouldn't interrupt the cursor
        self.assertTrue(iter_cursor(cursor))
        self.assertEqual(10, cursor._Cursor__retrieved)

    def tearDown(self):
        ha_tools.kill_all_members()


class TestReadPreference(unittest.TestCase):
    def setUp(self):
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
        self.primary = _partition_node(primary)
        self.primary_tags = ha_tools.get_tags(primary)
        # Make sure priority worked
        self.assertEqual('primary', self.primary_tags['name'])

        self.primary_dc = {'dc': self.primary_tags['dc']}

        secondaries = ha_tools.get_secondaries()

        (secondary, ) = [
            s for s in secondaries
            if ha_tools.get_tags(s)['name'] == 'secondary']

        self.secondary = _partition_node(secondary)
        self.secondary_tags = ha_tools.get_tags(secondary)
        self.secondary_dc = {'dc': self.secondary_tags['dc']}

        (other_secondary, ) = [
            s for s in secondaries
            if ha_tools.get_tags(s)['name'] == 'other_secondary']

        self.other_secondary = _partition_node(other_secondary)
        self.other_secondary_tags = ha_tools.get_tags(other_secondary)
        self.other_secondary_dc = {'dc': self.other_secondary_tags['dc']}

        self.c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.db = self.c.pymongo_test
        self.w = len(self.c.secondaries) + 1
        self.db.test.remove({}, safe=True, w=self.w)
        self.db.test.insert(
            [{'foo': i} for i in xrange(10)], safe=True, w=self.w)

        self.clear_ping_times()

    def set_ping_time(self, host, ping_time_seconds):
        Member._host_to_ping_time[host] = ping_time_seconds

    def clear_ping_times(self):
        Member._host_to_ping_time.clear()

    def test_read_preference(self):
        # This is long, but we put all the tests in one function to save time
        # on setUp, which takes about 30 seconds to bring up a replica set.
        # We pass through four states:
        #
        #       1. A primary and two secondaries
        #       2. Primary down
        #       3. Primary up, one secondary down
        #       4. Primary up, all secondaries down
        #
        # For each state, we verify the behavior of PRIMARY,
        # PRIMARY_PREFERRED, SECONDARY, SECONDARY_PREFERRED, and NEAREST
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets,
            auto_start_request=False)

        def assertReadFrom(member, *args, **kwargs):
            utils.assertReadFrom(self, c, member, *args, **kwargs)

        def assertReadFromAll(members, *args, **kwargs):
            utils.assertReadFromAll(self, c, members, *args, **kwargs)

        def unpartition_node(node):
            host, port = node
            return '%s:%s' % (host, port)

        # To make the code terser, copy modes and hosts into local scope
        PRIMARY = ReadPreference.PRIMARY
        PRIMARY_PREFERRED = ReadPreference.PRIMARY_PREFERRED
        SECONDARY = ReadPreference.SECONDARY
        SECONDARY_PREFERRED = ReadPreference.SECONDARY_PREFERRED
        NEAREST = ReadPreference.NEAREST
        
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

        # High secondaryAcceptableLatencyMS, should read from all members
        assertReadFromAll(
            [primary, secondary, other_secondary],
            NEAREST, secondary_acceptable_latency_ms=1000*1000)

        self.clear_ping_times()

        assertReadFromAll([primary, other_secondary], NEAREST, [{'dc': 'ny'}])

        # 2. PRIMARY DOWN -----------------------------------------------------
        killed = ha_tools.kill_primary()

        # Let monitor notice primary's gone
        sleep(2 * MONITOR_INTERVAL)

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

        for _ in range(30):
            if ha_tools.get_primary():
                break
            sleep(1)
        else:
            self.fail("Primary didn't come back up")

        ha_tools.kill_members([unpartition_node(secondary)], 2)
        self.assertTrue(Connection(
            unpartition_node(primary), use_greenlets=use_greenlets,
            slave_okay=True
        ).admin.command('ismaster')['ismaster'])

        sleep(2 * MONITOR_INTERVAL)
        
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
        self.assertTrue(Connection(
            unpartition_node(primary), use_greenlets=use_greenlets,
            slave_okay=True
        ).admin.command('ismaster')['ismaster'])

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

    def tearDown(self):
        self.c.close()
        ha_tools.kill_all_members()
        self.clear_ping_times()


class TestReplicaSetAuth(unittest.TestCase):
    def setUp(self):
        members = [
            {},
            {'priority': 0},
            {'priority': 0},
        ]

        res = ha_tools.start_replica_set(members, auth=True)
        self.c = ReplicaSetConnection(res[0], replicaSet=res[1],
                                      use_greenlets=use_greenlets)

        # Add an admin user to enable auth
        try:
            self.c.admin.add_user('admin', 'adminpass')
        except:
            # SERVER-4225
            pass
        self.c.admin.authenticate('admin', 'adminpass')

        self.db = self.c.pymongo_ha_auth
        self.db.add_user('user', 'userpass')
        self.c.admin.logout()

    def test_auth_during_failover(self):
        self.assertTrue(self.db.authenticate('user', 'userpass'))
        self.assertTrue(self.db.foo.insert({'foo': 'bar'},
                                           safe=True, w=3, wtimeout=1000))
        self.db.logout()
        self.assertRaises(OperationFailure, self.db.foo.find_one)

        primary = '%s:%d' % self.c.primary
        ha_tools.kill_members([primary], 2)

        # Let monitor notice primary's gone
        sleep(2 * MONITOR_INTERVAL)

        # Make sure we can still authenticate
        self.assertTrue(self.db.authenticate('user', 'userpass'))
        # And still query.
        self.db.read_preference = ReadPreference.PRIMARY_PREFERRED
        self.assertEqual('bar', self.db.foo.find_one()['foo'])

    def tearDown(self):
        self.c.close()
        ha_tools.kill_all_members()

class TestMongosHighAvailability(unittest.TestCase):
    def setUp(self):
        seed_list = ha_tools.create_sharded_cluster()
        self.dbname = 'pymongo_mongos_ha'
        self.conn = Connection(seed_list)
        self.conn.drop_database(self.dbname)

    def test_mongos_ha(self):
        coll = self.conn[self.dbname].test
        self.assertTrue(coll.insert({'foo': 'bar'}, safe=True))

        first = '%s:%d' % (self.conn.host, self.conn.port)
        ha_tools.kill_mongos(first)
        # Fail first attempt
        self.assertRaises(AutoReconnect, coll.count)
        # Find new mongos
        self.assertEqual(1, coll.count())

        second = '%s:%d' % (self.conn.host, self.conn.port)
        self.assertNotEqual(first, second)
        ha_tools.kill_mongos(second)
        # Fail first attempt
        self.assertRaises(AutoReconnect, coll.count)
        # Find new mongos
        self.assertEqual(1, coll.count())

        third = '%s:%d' % (self.conn.host, self.conn.port)
        self.assertNotEqual(second, third)
        ha_tools.kill_mongos(third)
        # Fail first attempt
        self.assertRaises(AutoReconnect, coll.count)

        # We've killed all three, restart one.
        ha_tools.restart_mongos(first)

        # Find new mongos
        self.assertEqual(1, coll.count())

    def tearDown(self):
        self.conn.drop_database(self.dbname)
        ha_tools.kill_all_members()


if __name__ == '__main__':
    if use_greenlets:
        print('Using Gevent')
        import gevent
        print('gevent version %s' % gevent.__version__)

        from gevent import monkey
        monkey.patch_socket()
        sleep = gevent.sleep
    else:
        sleep = time.sleep

    unittest.main()
