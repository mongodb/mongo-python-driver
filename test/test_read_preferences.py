# Copyright 2011-2012 10gen, Inc.
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

"""Test the replica_set_connection module."""
import random

import sys
import unittest

sys.path[0:0] = [""]

from bson.son import SON
from pymongo.replica_set_connection import ReplicaSetConnection
from pymongo.read_preferences import ReadPreference, modes, MovingAverage
from pymongo.errors import ConfigurationError

from test.test_replica_set_connection import TestConnectionReplicaSetBase
from test.test_connection import get_connection, host, port
from test import version, utils


class TestReadPreferencesBase(TestConnectionReplicaSetBase):
    def setUp(self):
        super(TestReadPreferencesBase, self).setUp()
        # Insert some data so we can use cursors in read_from_which_host
        c = self._get_connection()
        c.pymongo_test.test.drop()
        c.pymongo_test.test.insert([{'_id': i} for i in range(10)], w=self.w)

    def tearDown(self):
        super(TestReadPreferencesBase, self).tearDown()
        c = self._get_connection()
        c.pymongo_test.test.drop()

    def read_from_which_host(self, connection):
        """Do a find() on the connection and return which host was used
        """
        cursor = connection.pymongo_test.test.find()
        cursor.next()
        return cursor._Cursor__connection_id

    def read_from_which_kind(self, connection):
        """Do a find() on the connection and return 'primary' or 'secondary'
           depending on which the connection used.
        """
        connection_id = self.read_from_which_host(connection)
        if connection_id == connection.primary:
            return 'primary'
        elif connection_id in connection.secondaries:
            return 'secondary'
        else:
            self.fail(
                'Cursor used connection id %s, expected either primary '
                '%s or secondaries %s' % (
                    connection_id, connection.primary, connection.secondaries))

    def assertReadsFrom(self, expected, **kwargs):
        c = self._get_connection(**kwargs)
        used = self.read_from_which_kind(c)
        self.assertEqual(expected, used, 'Cursor used %s, expected %s' % (
            expected, used))


class TestReadPreferences(TestReadPreferencesBase):
    def test_primary(self):
        self.assertReadsFrom('primary',
            read_preference=ReadPreference.PRIMARY)

    def test_primary_with_tags(self):
        # Tags not allowed with PRIMARY
        self.assertRaises(ConfigurationError,
            self._get_connection, tag_sets=[{'dc': 'ny'}])

    def test_primary_preferred(self):
        self.assertReadsFrom('primary',
            read_preference=ReadPreference.PRIMARY_PREFERRED)

    def test_secondary(self):
        self.assertReadsFrom('secondary',
            read_preference=ReadPreference.SECONDARY)

    def test_secondary_preferred(self):
        self.assertReadsFrom('secondary',
            read_preference=ReadPreference.SECONDARY_PREFERRED)
        
    def test_secondary_only(self):
        # Test deprecated mode SECONDARY_ONLY, which is now a synonym for
        # SECONDARY
        self.assertEqual(
            ReadPreference.SECONDARY, ReadPreference.SECONDARY_ONLY)

    def test_nearest(self):
        # With high secondaryAcceptableLatencyMS, expect to read from any
        # member
        c = self._get_connection(
            read_preference=ReadPreference.NEAREST,
            secondaryAcceptableLatencyMS=1500,
            auto_start_request=False)

        used = set()
        for i in range(1000):
            host = self.read_from_which_host(c)
            used.add(host)

        data_members = set(self.hosts).difference(set(self.arbiters))
        not_used = data_members.difference(used)
        self.assertFalse(not_used,
            "Expected to use primary and all secondaries for mode NEAREST,"
            " but didn't use %s" % not_used)


class ReadPrefTester(ReplicaSetConnection):
    def __init__(self, *args, **kwargs):
        self.has_read_from = set()
        super(ReadPrefTester, self).__init__(*args, **kwargs)

    def _ReplicaSetConnection__send_and_receive(self, member, *args, **kwargs):
        self.has_read_from.add(member)
        rsc = super(ReadPrefTester, self)
        return rsc._ReplicaSetConnection__send_and_receive(
            member, *args, **kwargs)


class TestCommandAndReadPreference(TestConnectionReplicaSetBase):
    def setUp(self):
        super(TestCommandAndReadPreference, self).setUp()
        
        # Need auto_start_request False to avoid pinning members.
        self.c = ReadPrefTester(
            '%s:%s' % (host, port),
            replicaSet=self.name, auto_start_request=False,
            # Effectively ignore members' ping times so we can test the effect
            # of ReadPreference modes only
            secondary_acceptable_latency_ms=1000*1000)

    def tearDown(self):
        self.c.close()

        # We create a lot of collections and indexes in these tests, so drop
        # the database
        self._get_connection().drop_database('pymongo_test')
        super(TestCommandAndReadPreference, self).tearDown()

    def executed_on_which_member(self, connection, fn, *args, **kwargs):
        connection.has_read_from.clear()
        fn(*args, **kwargs)
        self.assertEqual(1, len(connection.has_read_from))
        member, = connection.has_read_from
        return member

    def assertExecutedOn(self, state, connection, fn, *args, **kwargs):
        member = self.executed_on_which_member(connection, fn, *args, **kwargs)
        if state == 'primary':
            self.assertTrue(member.is_primary)
        elif state == 'secondary':
            self.assertFalse(member.is_primary)
        else:
            self.fail("Bad state %s" % repr(state))

    def _test_fn(self, obedient, fn):
        if not obedient:
            for mode in modes:
                self.c.read_preference = mode

                # Run it a few times to make sure we don't just get lucky the
                # first time.
                for _ in range(10):
                    self.assertExecutedOn('primary', self.c, fn)
        else:
            for mode, expected_state in [
                (ReadPreference.PRIMARY, 'primary'),
                (ReadPreference.PRIMARY_PREFERRED, 'primary'),
                (ReadPreference.SECONDARY, 'secondary'),
                (ReadPreference.SECONDARY_PREFERRED, 'secondary'),
                (ReadPreference.NEAREST, 'any'),
            ]:
                self.c.read_preference = mode
                for _ in range(10):
                    if expected_state in ('primary', 'secondary'):
                        self.assertExecutedOn(expected_state, self.c, fn)
                    elif expected_state == 'any':
                        used = set()
                        for _ in range(1000):
                            member = self.executed_on_which_member(
                                self.c, fn)
                            used.add(member.host)
                            if len(used) == len(self.c.secondaries) + 1:
                                # Success
                                break

                        unused = self.c.secondaries.union(
                            set([self.c.primary])
                        ).difference(used)
                        if unused:
                            self.fail(
                                "Some members not used for NEAREST: %s" % (
                                    unused))

    def test_command(self):
        # Test generic 'command' method. Some commands obey read preference,
        # most don't.
        # Disobedient commands, always go to primary
        self._test_fn(False, lambda: self.c.pymongo_test.command('ping'))
        self._test_fn(False, lambda: self.c.admin.command('buildinfo'))

        # Obedient commands.
        self._test_fn(True, lambda: self.c.pymongo_test.command('group', {
            'ns': 'test', 'key': {'a': 1}, '$reduce': 'function(obj, prev) { }',
            'initial': {}}))

        self._test_fn(True, lambda: self.c.pymongo_test.command('dbStats'))

        # collStats fails if no collection
        self.c.pymongo_test.test.insert({}, w=self.w)
        self._test_fn(True, lambda: self.c.pymongo_test.command(
            'collStats', 'test'))

        # Count
        self._test_fn(True, lambda: self.c.pymongo_test.command(
            'count', 'test'))
        self._test_fn(True, lambda: self.c.pymongo_test.command(
            'count', 'test', query={'a': 1}))
        self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
            ('count', 'test'), ('query', {'a': 1})])))

        # Distinct
        self._test_fn(True, lambda: self.c.pymongo_test.command(
            'distinct', 'test', key={'a': 1}))
        self._test_fn(True, lambda: self.c.pymongo_test.command(
            'distinct', 'test', key={'a': 1}, query={'a': 1}))
        self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
            ('distinct', 'test'), ('key', {'a': 1}), ('query', {'a': 1})])))

        # Geo stuff. Make sure a 2d index is created and replicated
        self.c.pymongo_test.system.indexes.insert({
            'key' : { 'location' : '2d' }, 'ns' : 'pymongo_test.test',
            'name' : 'location_2d' }, w=self.w)

        self.c.pymongo_test.system.indexes.insert(SON([
            ('ns', 'pymongo_test.test'),
            ('key', SON([('location', 'geoHaystack'), ('key', 1)])),
            ('bucketSize', 100),
            ('name', 'location_geoHaystack'),
        ]), w=self.w)

        self._test_fn(True, lambda: self.c.pymongo_test.command(
            'geoNear', 'test', near=[0, 0]))
        self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
            ('geoNear', 'test'), ('near', [0, 0])])))

        self._test_fn(True, lambda: self.c.pymongo_test.command(
            'geoSearch', 'test', near=[33, 33], maxDistance=6,
            search={'type': 'restaurant' }, limit=30))
        
        self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
            ('geoSearch', 'test'), ('near', [33, 33]), ('maxDistance', 6),
            ('search', {'type': 'restaurant'}), ('limit', 30)])))

        if version.at_least(self.c, (2, 1, 0)):
            self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
                ('aggregate', 'test'),
                ('pipeline', [])
            ])))

    def test_map_reduce_command(self):
        # mapreduce fails if no collection
        self.c.pymongo_test.test.insert({}, w=self.w)

        # Non-inline mapreduce always goes to primary, doesn't obey read prefs.
        # Test with command in a SON and with kwargs
        self._test_fn(False, lambda: self.c.pymongo_test.command(SON([
            ('mapreduce', 'test'),
            ('map', 'function() { }'),
            ('reduce', 'function() { }'),
            ('out', 'mr_out')
        ])))

        self._test_fn(False, lambda: self.c.pymongo_test.command(
            'mapreduce', 'test', map='function() { }',
            reduce='function() { }', out='mr_out'))

        self._test_fn(False, lambda: self.c.pymongo_test.command(
            'mapreduce', 'test', map='function() { }',
            reduce='function() { }', out={'replace': 'some_collection'}))

        # Inline mapreduce obeys read prefs
        self._test_fn(True, lambda: self.c.pymongo_test.command(
            'mapreduce', 'test', map='function() { }',
            reduce='function() { }', out={'inline': True}))

        self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
            ('mapreduce', 'test'),
            ('map', 'function() { }'),
            ('reduce', 'function() { }'),
            ('out', {'inline': True})
        ])))

    def test_create_collection(self):
        # Collections should be created on primary, obviously
        self._test_fn(False, lambda: self.c.pymongo_test.command(
            'create', 'some_collection%s' % random.randint(0, sys.maxint)))

        self._test_fn(False, lambda: self.c.pymongo_test.create_collection(
            'some_collection%s' % random.randint(0, sys.maxint)))

    def test_drop_collection(self):
        self._test_fn(False, lambda: self.c.pymongo_test.drop_collection(
            'some_collection'))

        self._test_fn(False, lambda: self.c.pymongo_test.some_collection.drop())

    def test_group(self):
        self._test_fn(True, lambda: self.c.pymongo_test.test.group(
            {'a': 1}, {}, {}, 'function() { }'))

    def test_map_reduce(self):
        # mapreduce fails if no collection
        self.c.pymongo_test.test.insert({}, w=self.w)

        self._test_fn(False, lambda: self.c.pymongo_test.test.map_reduce(
            'function() { }', 'function() { }', 'mr_out'))

        self._test_fn(True, lambda: self.c.pymongo_test.test.map_reduce(
            'function() { }', 'function() { }', {'inline': 1}))

    def test_inline_map_reduce(self):
        # mapreduce fails if no collection
        self.c.pymongo_test.test.insert({}, w=self.w)

        self._test_fn(True, lambda: self.c.pymongo_test.test.inline_map_reduce(
            'function() { }', 'function() { }'))

        self._test_fn(True, lambda: self.c.pymongo_test.test.inline_map_reduce(
            'function() { }', 'function() { }', full_response=True))

    def test_count(self):
        self._test_fn(True, lambda: self.c.pymongo_test.test.count())
        self._test_fn(True, lambda: self.c.pymongo_test.test.find().count())

    def test_distinct(self):
        self._test_fn(True, lambda: self.c.pymongo_test.test.distinct('a'))
        self._test_fn(True,
            lambda: self.c.pymongo_test.test.find().distinct('a'))

    def test_aggregate(self):
        if version.at_least(self.c, (2, 1, 0)):
            self._test_fn(True, lambda: self.c.pymongo_test.test.aggregate([]))


class TestMovingAverage(unittest.TestCase):
    def test_empty_moving_average(self):
        avg = MovingAverage(0)
        self.assertEqual(None, avg.get())
        avg.update(10)
        self.assertEqual(None, avg.get())

    def test_trivial_moving_average(self):
        avg = MovingAverage(1)
        self.assertEqual(None, avg.get())
        avg.update(10)
        self.assertEqual(10, avg.get())
        avg.update(20)
        self.assertEqual(20, avg.get())
        avg.update(0)
        self.assertEqual(0, avg.get())

    def test_2_sample_moving_average(self):
        avg = MovingAverage(2)
        self.assertEqual(None, avg.get())
        avg.update(10)
        self.assertEqual(10, avg.get())
        avg.update(20)
        self.assertEqual(15, avg.get())
        avg.update(30)
        self.assertEqual(25, avg.get())
        avg.update(-100)
        self.assertEqual(-35, avg.get())

    def test_5_sample_moving_average(self):
        avg = MovingAverage(5)
        self.assertEqual(None, avg.get())
        avg.update(10)
        self.assertEqual(10, avg.get())
        avg.update(20)
        self.assertEqual(15, avg.get())
        avg.update(30)
        self.assertEqual(20, avg.get())
        avg.update(-100)
        self.assertEqual((10 + 20 + 30 - 100) / 4, avg.get())
        avg.update(17)
        self.assertEqual((10 + 20 + 30 - 100 + 17) / 5., avg.get())
        avg.update(43)
        self.assertEqual((20 + 30 - 100 + 17 + 43) / 5., avg.get())
        avg.update(-1111)
        self.assertEqual((30 - 100 + 17 + 43 - 1111) / 5., avg.get())


class TestMongosConnection(unittest.TestCase):
    def test_mongos_connection(self):
        c = get_connection()
        is_mongos = utils.is_mongos(c)

        # Test default mode, PRIMARY
        cursor = c.pymongo_test.test.find()
        if is_mongos:
            self.assertEqual(
                {'mode': 'primary'},
                cursor._Cursor__query_spec().get('$readPreference')
            )
        else:
            self.assertFalse(
                '$readPreference' in cursor._Cursor__query_spec())

        # Test non-PRIMARY modes which can be combined with tags
        for mode, mongos_mode in (
            (ReadPreference.PRIMARY_PREFERRED, 'primaryPreferred'),
            (ReadPreference.SECONDARY, 'secondary'),
            (ReadPreference.SECONDARY_PREFERRED, 'secondaryPreferred'),
            (ReadPreference.NEAREST, 'nearest'),
        ):
            for tag_sets in (
                None, [{}]
            ):
                c = get_connection(
                    read_preference=mode,
                    tag_sets=tag_sets)

                self.assertEqual(is_mongos, c.is_mongos)
                cursor = c.pymongo_test.test.find()
                if is_mongos:
                    self.assertEqual(
                        {'mode': mongos_mode},
                        cursor._Cursor__query_spec().get('$readPreference')
                    )
                else:
                    self.assertFalse(
                        '$readPreference' in cursor._Cursor__query_spec())

            for tag_sets in (
                [{'dc': 'la'}],
                [{'dc': 'la'}, {'dc': 'sf'}],
                [{'dc': 'la'}, {'dc': 'sf'}, {}],
            ):
                c = get_connection(
                    read_preference=mode,
                    tag_sets=tag_sets)

                self.assertEqual(is_mongos, c.is_mongos)
                cursor = c.pymongo_test.test.find()
                if is_mongos:
                    self.assertEqual(
                        {'mode': mongos_mode, 'tags': tag_sets},
                        cursor._Cursor__query_spec().get('$readPreference'))
                else:
                    self.assertFalse(
                        '$readPreference' in cursor._Cursor__query_spec())


if __name__ == "__main__":
    unittest.main()
