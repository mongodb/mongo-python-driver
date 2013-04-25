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

# These test methods exuberantly violate the "one assert per test" rule, because
# each method requires running setUp, which takes about 30 seconds to bring up
# a replica set. Thus each method asserts everything we want to assert for a
# given replica-set configuration.

import itertools
import time
import unittest

import ha_tools
from ha_tools import use_greenlets


from pymongo import ReadPreference
from pymongo.mongo_replica_set_client import (
    Member, Monitor, MongoReplicaSetClient)
from pymongo.mongo_client import _partition_node
from pymongo.mongo_client import MongoClient
from pymongo.errors import AutoReconnect, OperationFailure, ConnectionFailure
from pymongo.read_preferences import modes

from test import utils
from test.utils import one


# Override default 30-second interval for faster testing
Monitor._refresh_interval = MONITOR_INTERVAL = 0.5


# To make the code terser, copy modes into module scope
PRIMARY = ReadPreference.PRIMARY
PRIMARY_PREFERRED = ReadPreference.PRIMARY_PREFERRED
SECONDARY = ReadPreference.SECONDARY
SECONDARY_PREFERRED = ReadPreference.SECONDARY_PREFERRED
NEAREST = ReadPreference.NEAREST


class TestDirectConnection(unittest.TestCase):

    def setUp(self):
        members = [{}, {}, {'arbiterOnly': True}]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

    def test_secondary_connection(self):
        self.c = MongoReplicaSetClient(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(self.c.secondaries)))
        db = self.c.pymongo_test

        # Wait for replication...
        w = len(self.c.secondaries) + 1
        db.test.remove({}, w=w)
        db.test.insert({'foo': 'bar'}, w=w)

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
            {'slave_okay': True}
        ]:
            client = MongoClient(primary_host,
                                 primary_port,
                                 use_greenlets=use_greenlets,
                                 **kwargs)
            self.assertEqual(primary_host, client.host)
            self.assertEqual(primary_port, client.port)
            self.assertTrue(client.is_primary)

            # Direct connection to primary can be queried with any read pref
            self.assertTrue(client.pymongo_test.test.find_one())

            client = MongoClient(secondary_host,
                                 secondary_port,
                                 use_greenlets=use_greenlets,
                                 **kwargs)
            self.assertEqual(secondary_host, client.host)
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
                client.pymongo_test.test.insert({}, w=0)
            except AutoReconnect, e:
                self.assertEqual('not master', e.args[0])
            else:
                self.fail(
                    'Unacknowledged insert into secondary client %s should'
                    'have raised exception' % (client,))

            # Test direct connection to an arbiter
            client = MongoClient(arbiter_host, arbiter_port, **kwargs)
            self.assertEqual(arbiter_host, client.host)
            self.assertEqual(arbiter_port, client.port)
            self.assertFalse(client.is_primary)

            # See explanation above
            try:
                client.pymongo_test.test.insert({}, w=0)
            except AutoReconnect, e:
                self.assertEqual('not master', e.args[0])
            else:
                self.fail(
                    'Unacknowledged insert into arbiter client %s should'
                    'have raised exception' % (client,))

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
        self.c = MongoReplicaSetClient(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)

        passives = ha_tools.get_passives()
        passives = [_partition_node(member) for member in passives]
        self.assertEqual(self.c.secondaries, set(passives))

        for mode in SECONDARY, SECONDARY_PREFERRED:
            utils.assertReadFromAll(self, self.c, passives, mode)

        ha_tools.kill_members(ha_tools.get_passives(), 2)
        sleep(2 * MONITOR_INTERVAL)
        utils.assertReadFrom(self, self.c, self.c.primary, SECONDARY_PREFERRED)

    def tearDown(self):
        self.c.close()
        ha_tools.kill_all_members()


class TestMonitorRemovesRecoveringMember(unittest.TestCase):
    # Members in STARTUP2 or RECOVERING states are shown in the primary's
    # isMaster response, but aren't secondaries and shouldn't be read from.
    # Verify that if a secondary goes into RECOVERING mode, the Monitor removes
    # it from the set of readers.

    def setUp(self):
        members = [{}, {'priority': 0}, {'priority': 0}]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

    def test_monitor_removes_recovering_member(self):
        self.c = MongoReplicaSetClient(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)

        secondaries = ha_tools.get_secondaries()

        for mode in SECONDARY, SECONDARY_PREFERRED:
            partitioned_secondaries = [_partition_node(s) for s in secondaries]
            utils.assertReadFromAll(self, self.c, partitioned_secondaries, mode)

        secondary, recovering_secondary = secondaries
        ha_tools.set_maintenance(recovering_secondary, True)
        sleep(2 * MONITOR_INTERVAL)

        for mode in SECONDARY, SECONDARY_PREFERRED:
            # Don't read from recovering member
            utils.assertReadFrom(self, self.c, _partition_node(secondary), mode)

    def tearDown(self):
        self.c.close()
        ha_tools.kill_all_members()


class TestTriggeredRefresh(unittest.TestCase):
    # Verify that if a secondary goes into RECOVERING mode or if the primary
    # changes, the next exception triggers an immediate refresh.

    def setUp(self):
        members = [{}, {}]
        res = ha_tools.start_replica_set(members)
        self.seed, self.name = res

        # Disable periodic refresh
        Monitor._refresh_interval = 1e6

    def test_recovering_member_triggers_refresh(self):
        # To test that find_one() and count() trigger immediate refreshes,
        # we'll create a separate client for each
        self.c_find_one, self.c_count = [
            MongoReplicaSetClient(
                self.seed, replicaSet=self.name, use_greenlets=use_greenlets,
                read_preference=SECONDARY)
            for _ in xrange(2)]

        # We've started the primary and one secondary
        primary = ha_tools.get_primary()
        secondary = ha_tools.get_secondaries()[0]

        # Pre-condition: just make sure they all connected OK
        for c in self.c_find_one, self.c_count:
            self.assertEqual(one(c.secondaries), _partition_node(secondary))

        ha_tools.set_maintenance(secondary, True)

        # Trigger a refresh in various ways
        self.assertRaises(AutoReconnect, self.c_find_one.test.test.find_one)
        self.assertRaises(AutoReconnect, self.c_count.test.test.count)

        # Wait for the immediate refresh to complete - we're not waiting for
        # the periodic refresh, which has been disabled
        sleep(1)

        for c in self.c_find_one, self.c_count:
            self.assertFalse(c.secondaries)
            self.assertEqual(_partition_node(primary), c.primary)

    def test_stepdown_triggers_refresh(self):
        c_find_one = MongoReplicaSetClient(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)

        # We've started the primary and one secondary
        primary = ha_tools.get_primary()
        secondary = ha_tools.get_secondaries()[0]
        self.assertEqual(
            one(c_find_one.secondaries), _partition_node(secondary))

        ha_tools.stepdown_primary()

        # Make sure the stepdown completes
        sleep(1)

        # Trigger a refresh
        self.assertRaises(AutoReconnect, c_find_one.test.test.find_one)

        # Wait for the immediate refresh to complete - we're not waiting for
        # the periodic refresh, which has been disabled
        sleep(1)

        # We've detected the stepdown
        self.assertTrue(
            not c_find_one.primary
            or primary != _partition_node(c_find_one.primary))

    def tearDown(self):
        Monitor._refresh_interval = MONITOR_INTERVAL
        ha_tools.kill_all_members()


class TestHealthMonitor(unittest.TestCase):

    def setUp(self):
        res = ha_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    def test_primary_failure(self):
        c = MongoReplicaSetClient(
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
        c = MongoReplicaSetClient(
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
        c = MongoReplicaSetClient(
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
        c = MongoReplicaSetClient(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        primary = c.primary
        db = c.pymongo_test
        w = len(c.secondaries) + 1
        db.test.remove({}, w=w)
        db.test.insert({'foo': 'bar'}, w=w)
        self.assertEqual('bar', db.test.find_one()['foo'])

        def try_write():
            for _ in xrange(30):
                try:
                    db.test.insert({'bar': 'baz'})
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
        c = MongoReplicaSetClient(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(c.secondaries)))

        def iter_cursor(cursor):
            for _ in cursor:
                pass
            return True

        db = c.pymongo_test
        w = len(c.secondaries) + 1
        db.test.remove({}, w=w)
        # Force replication
        db.test.insert([{'foo': i} for i in xrange(10)], w=w)
        self.assertEqual(10, db.test.count())

        db.read_preference = SECONDARY_PREFERRED
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

        self.c = MongoReplicaSetClient(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.db = self.c.pymongo_test
        self.w = len(self.c.secondaries) + 1
        self.db.test.remove({}, w=self.w)
        self.db.test.insert(
            [{'foo': i} for i in xrange(10)], w=self.w)

        self.clear_ping_times()

    def set_ping_time(self, host, ping_time_seconds):
        Member._host_to_ping_time[host] = ping_time_seconds

    def clear_ping_times(self):
        Member._host_to_ping_time.clear()

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
        c = MongoReplicaSetClient(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)

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
        self.assertTrue(MongoClient(
            unpartition_node(primary), use_greenlets=use_greenlets,
            read_preference=PRIMARY_PREFERRED
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
        self.assertTrue(MongoClient(
            unpartition_node(primary), use_greenlets=use_greenlets,
            read_preference=PRIMARY_PREFERRED
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

    def test_pinning(self):
        # To make the code terser, copy modes into local scope
        PRIMARY = ReadPreference.PRIMARY
        PRIMARY_PREFERRED = ReadPreference.PRIMARY_PREFERRED
        SECONDARY = ReadPreference.SECONDARY
        SECONDARY_PREFERRED = ReadPreference.SECONDARY_PREFERRED
        NEAREST = ReadPreference.NEAREST

        c = MongoReplicaSetClient(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets,
            auto_start_request=True)

        # Verify that changing the mode unpins the member. We'll try it for
        # every relevant change of mode.
        for mode0, mode1 in itertools.permutations(
            (PRIMARY, SECONDARY, SECONDARY_PREFERRED, NEAREST), 2
        ):
            # Try reading and then changing modes and reading again, see if we
            # read from a different host
            for _ in range(1000):
                # pin to this host
                host = utils.read_from_which_host(c, mode0)
                # unpin?
                new_host = utils.read_from_which_host(c, mode1)
                if host != new_host:
                    # Reading with a different mode unpinned, hooray!
                    break
            else:
                self.fail(
                    "Changing from mode %s to mode %s never unpinned" % (
                        modes[mode0], modes[mode1]))

        # Now verify changing the tag_sets unpins the member.
        tags0 = [{'a': 'a'}, {}]
        tags1 = [{'a': 'x'}, {}]
        for _ in range(1000):
            host = utils.read_from_which_host(c, NEAREST, tags0)
            new_host = utils.read_from_which_host(c, NEAREST, tags1)
            if host != new_host:
                break
        else:
            self.fail(
                "Changing from tags %s to tags %s never unpinned" % (
                    tags0, tags1))

        # Finally, verify changing the secondary_acceptable_latency_ms unpins
        # the member.
        for _ in range(1000):
            host = utils.read_from_which_host(c, SECONDARY, None, 15)
            new_host = utils.read_from_which_host(c, SECONDARY, None, 20)
            if host != new_host:
                break
        else:
            self.fail(
                "Changing secondary_acceptable_latency_ms from 15 to 20"
                " never unpinned")

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
        self.c = MongoReplicaSetClient(res[0], replicaSet=res[1],
                                       use_greenlets=use_greenlets)

        # Add an admin user to enable auth
        self.c.admin.add_user('admin', 'adminpass')
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
        self.db.read_preference = PRIMARY_PREFERRED
        self.assertEqual('bar', self.db.foo.find_one()['foo'])

    def tearDown(self):
        self.c.close()
        ha_tools.kill_all_members()


class TestAlive(unittest.TestCase):
    def setUp(self):
        members = [{}, {}]
        self.seed, self.name = ha_tools.start_replica_set(members)

    def test_alive(self):
        primary = ha_tools.get_primary()
        secondary = ha_tools.get_random_secondary()
        primary_cx = MongoClient(primary, use_greenlets=use_greenlets)
        secondary_cx = MongoClient(secondary, use_greenlets=use_greenlets)
        rsc = MongoReplicaSetClient(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)

        try:
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
        finally:
            rsc.close()

    def tearDown(self):
        ha_tools.kill_all_members()


class TestMongosHighAvailability(unittest.TestCase):
    def setUp(self):
        seed_list = ha_tools.create_sharded_cluster()
        self.dbname = 'pymongo_mongos_ha'
        self.client = MongoClient(seed_list)
        self.client.drop_database(self.dbname)

    def test_mongos_ha(self):
        coll = self.client[self.dbname].test
        self.assertTrue(coll.insert({'foo': 'bar'}))

        first = '%s:%d' % (self.client.host, self.client.port)
        ha_tools.kill_mongos(first)
        # Fail first attempt
        self.assertRaises(AutoReconnect, coll.count)
        # Find new mongos
        self.assertEqual(1, coll.count())

        second = '%s:%d' % (self.client.host, self.client.port)
        self.assertNotEqual(first, second)
        ha_tools.kill_mongos(second)
        # Fail first attempt
        self.assertRaises(AutoReconnect, coll.count)
        # Find new mongos
        self.assertEqual(1, coll.count())

        third = '%s:%d' % (self.client.host, self.client.port)
        self.assertNotEqual(second, third)
        ha_tools.kill_mongos(third)
        # Fail first attempt
        self.assertRaises(AutoReconnect, coll.count)

        # We've killed all three, restart one.
        ha_tools.restart_mongos(first)

        # Find new mongos
        self.assertEqual(1, coll.count())

    def tearDown(self):
        self.client.drop_database(self.dbname)
        ha_tools.kill_all_members()


class TestReplicaSetRequest(unittest.TestCase):
    def setUp(self):
        members = [{}, {}, {'arbiterOnly': True}]
        res = ha_tools.start_replica_set(members)
        self.c = MongoReplicaSetClient(res[0], replicaSet=res[1],
                                       use_greenlets=use_greenlets,
                                       auto_start_request=True)

    def test_request_during_failover(self):
        primary = _partition_node(ha_tools.get_primary())
        secondary = _partition_node(ha_tools.get_random_secondary())

        self.assertTrue(self.c.auto_start_request)
        self.assertTrue(self.c.in_request())

        primary_pool = self.c._MongoReplicaSetClient__members[primary].pool
        secondary_pool = self.c._MongoReplicaSetClient__members[secondary].pool

        # Trigger start_request on primary pool
        utils.assertReadFrom(self, self.c, primary, PRIMARY)
        self.assertTrue(primary_pool.in_request())

        # Fail over
        ha_tools.kill_primary()
        patience_seconds = 60
        for _ in range(patience_seconds):
            sleep(1)
            try:
                if ha_tools.ha_tools_debug:
                    print 'Waiting for failover'
                if ha_tools.get_primary():
                    # We have a new primary
                    break
            except ConnectionFailure:
                pass
        else:
            self.fail("Problem with test: No new primary after %s seconds"
                % patience_seconds)

        try:
            # Trigger start_request on secondary_pool, which is becoming new
            # primary
            self.c.test.test.find_one()
        except AutoReconnect:
            # We've noticed the failover now
            pass

        # The old secondary is now primary
        utils.assertReadFrom(self, self.c, secondary, PRIMARY)
        self.assertTrue(self.c.in_request())
        self.assertTrue(secondary_pool.in_request())

    def tearDown(self):
        self.c.close()
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
