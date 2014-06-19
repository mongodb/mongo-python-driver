# Copyright 2011-2014 MongoDB, Inc.
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
import warnings

from nose.plugins.skip import SkipTest

sys.path[0:0] = [""]

from bson.son import SON
from pymongo.cursor import _QUERY_OPTIONS
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import (ReadPreference, modes, MovingAverage,
                                      secondary_ok_commands)
from pymongo.errors import ConfigurationError

from test.test_replica_set_client import TestReplicaSetClientBase
from test.test_client import get_client
from test import version, utils, host, port
from test.utils import catch_warnings


class TestReadPreferencesBase(TestReplicaSetClientBase):
    def setUp(self):
        super(TestReadPreferencesBase, self).setUp()
        # Insert some data so we can use cursors in read_from_which_host
        c = self._get_client()
        c.pymongo_test.test.drop()
        c.pymongo_test.test.insert([{'_id': i} for i in range(10)], w=self.w)

    def tearDown(self):
        super(TestReadPreferencesBase, self).tearDown()
        c = self._get_client()
        c.pymongo_test.test.drop()

    def read_from_which_host(self, client):
        """Do a find() on the client and return which host was used
        """
        cursor = client.pymongo_test.test.find()
        cursor.next()
        return cursor._Cursor__connection_id

    def read_from_which_kind(self, client):
        """Do a find() on the client and return 'primary' or 'secondary'
           depending on which the client used.
        """
        connection_id = self.read_from_which_host(client)
        if connection_id == client.primary:
            return 'primary'
        elif connection_id in client.secondaries:
            return 'secondary'
        else:
            self.fail(
                'Cursor used connection id %s, expected either primary '
                '%s or secondaries %s' % (
                    connection_id, client.primary, client.secondaries))

    def assertReadsFrom(self, expected, **kwargs):
        c = self._get_client(**kwargs)
        used = self.read_from_which_kind(c)
        self.assertEqual(expected, used, 'Cursor used %s, expected %s' % (
            expected, used))


class TestReadPreferences(TestReadPreferencesBase):
    def test_mode_validation(self):
        # 'modes' are imported from read_preferences.py
        for mode in modes:
            self.assertEqual(mode, self._get_client(
                read_preference=mode).read_preference)

        self.assertRaises(ConfigurationError, self._get_client,
            read_preference='foo')

    def test_tag_sets_validation(self):
        # Can't use tags with PRIMARY
        self.assertRaises(ConfigurationError, self._get_client,
            tag_sets=[{'k': 'v'}])

        # ... but empty tag sets are ok with PRIMARY
        self.assertEqual([{}], self._get_client(tag_sets=[{}]).tag_sets)

        S = ReadPreference.SECONDARY
        self.assertEqual([{}], self._get_client(read_preference=S).tag_sets)

        self.assertEqual([{'k': 'v'}], self._get_client(
            read_preference=S, tag_sets=[{'k': 'v'}]).tag_sets)

        self.assertEqual([{'k': 'v'}, {}], self._get_client(
            read_preference=S, tag_sets=[{'k': 'v'}, {}]).tag_sets)

        self.assertRaises(ConfigurationError, self._get_client,
            read_preference=S, tag_sets=[])

        # One dict not ok, must be a list of dicts
        self.assertRaises(ConfigurationError, self._get_client,
            read_preference=S, tag_sets={'k': 'v'})

        self.assertRaises(ConfigurationError, self._get_client,
            read_preference=S, tag_sets='foo')

        self.assertRaises(ConfigurationError, self._get_client,
            read_preference=S, tag_sets=['foo'])

    def test_latency_validation(self):
        self.assertEqual(17, self._get_client(
            secondary_acceptable_latency_ms=17
        ).secondary_acceptable_latency_ms)

        self.assertEqual(42, self._get_client(
            secondaryAcceptableLatencyMS=42
        ).secondary_acceptable_latency_ms)

        self.assertEqual(666, self._get_client(
            secondaryacceptablelatencyms=666
        ).secondary_acceptable_latency_ms)

    def test_primary(self):
        self.assertReadsFrom('primary',
            read_preference=ReadPreference.PRIMARY)

    def test_primary_with_tags(self):
        # Tags not allowed with PRIMARY
        self.assertRaises(ConfigurationError,
            self._get_client, tag_sets=[{'dc': 'ny'}])

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
        c = self._get_client(
            read_preference=ReadPreference.NEAREST,
            secondaryAcceptableLatencyMS=10000, # 10 seconds
            auto_start_request=False)

        data_members = set(self.hosts).difference(set(self.arbiters))

        # This is a probabilistic test; track which members we've read from so
        # far, and keep reading until we've used all the members or give up.
        # Chance of using only 2 of 3 members 10k times if there's no bug =
        # 3 * (2/3)**10000, very low.
        used = set()
        i = 0
        while data_members.difference(used) and i < 10000:
            host = self.read_from_which_host(c)
            used.add(host)
            i += 1

        not_used = data_members.difference(used)
        latencies = ', '.join(
            '%s: %dms' % (member.host, member.ping_time.get())
            for member in c._MongoReplicaSetClient__rs_state.members)

        self.assertFalse(not_used,
            "Expected to use primary and all secondaries for mode NEAREST,"
            " but didn't use %s\nlatencies: %s" % (not_used, latencies))


class ReadPrefTester(MongoReplicaSetClient):
    def __init__(self, *args, **kwargs):
        self.has_read_from = set()
        super(ReadPrefTester, self).__init__(*args, **kwargs)

    def _MongoReplicaSetClient__send_and_receive(self, member, *args, **kwargs):
        self.has_read_from.add(member)
        rsc = super(ReadPrefTester, self)
        return rsc._MongoReplicaSetClient__send_and_receive(
            member, *args, **kwargs)


class TestCommandAndReadPreference(TestReplicaSetClientBase):
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
        # We create a lot of collections and indexes in these tests, so drop
        # the database.
        self.c.drop_database('pymongo_test')
        self.c.close()
        self.c = None
        super(TestCommandAndReadPreference, self).tearDown()

    def executed_on_which_member(self, client, fn, *args, **kwargs):
        client.has_read_from.clear()
        fn(*args, **kwargs)
        self.assertEqual(1, len(client.has_read_from))
        member, = client.has_read_from
        return member

    def assertExecutedOn(self, state, client, fn, *args, **kwargs):
        member = self.executed_on_which_member(client, fn, *args, **kwargs)
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
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", UserWarning)
            self._test_fn(False, lambda: self.c.pymongo_test.command('ping'))
            self._test_fn(False, lambda: self.c.admin.command('buildinfo'))
        finally:
            ctx.exit()

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
            'distinct', 'test', key='a'))
        self._test_fn(True, lambda: self.c.pymongo_test.command(
            'distinct', 'test', key='a', query={'a': 1}))
        self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
            ('distinct', 'test'), ('key', 'a'), ('query', {'a': 1})])))

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

        # Text search.
        if version.at_least(self.c, (2, 3, 2)):
            ctx = catch_warnings()
            try:
                warnings.simplefilter("ignore", UserWarning)
                utils.enable_text_search(self.c)
            finally:
                ctx.exit()
            db = self.c.pymongo_test

            # Only way to create an index and wait for all members to build it.
            index = {
                'ns': 'pymongo_test.test',
                'name': 't_text',
                'key': {'t': 'text'}}

            db.system.indexes.insert(
                index, manipulate=False, check_keys=False, w=self.w)

            self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
                ('text', 'test'),
                ('search', 'foo')])))

            self.c.pymongo_test.test.drop_indexes()

    def test_map_reduce_command(self):
        # mapreduce fails if no collection
        self.c.pymongo_test.test.insert({}, w=self.w)

        # Non-inline mapreduce always goes to primary, doesn't obey read prefs.
        # Test with command in a SON and with kwargs
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", UserWarning)
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
        finally:
            ctx.exit()

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

    def test_aggregate_command_with_out(self):
        if not version.at_least(self.c, (2, 5, 2)):
            raise SkipTest("Aggregation with $out requires MongoDB >= 2.5.2")

        # Tests aggregate command when pipeline contains $out.
        self.c.pymongo_test.test.insert({"x": 1, "y": 1}, w=self.w)
        self.c.pymongo_test.test.insert({"x": 1, "y": 2}, w=self.w)
        self.c.pymongo_test.test.insert({"x": 2, "y": 1}, w=self.w)
        self.c.pymongo_test.test.insert({"x": 2, "y": 2}, w=self.w)

        # Aggregate with $out always goes to primary, doesn't obey read prefs.
        # Test aggregate command sent directly to db.command.
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", UserWarning)
            self._test_fn(False, lambda: self.c.pymongo_test.command(
                "aggregate", "test",
                pipeline=[{"$match": {"x": 1}}, {"$out": "agg_out"}]
            ))

            # Test aggregate when sent through the collection aggregate function.
            self._test_fn(False, lambda: self.c.pymongo_test.test.aggregate(
                [{"$match": {"x": 2}}, {"$out": "agg_out"}]
            ))
        finally:
            ctx.exit()

        self.c.pymongo_test.drop_collection("test")
        self.c.pymongo_test.drop_collection("agg_out")

    def test_create_collection(self):
        # Collections should be created on primary, obviously
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", UserWarning)
            self._test_fn(False, lambda: self.c.pymongo_test.command(
                'create', 'some_collection%s' % random.randint(0, sys.maxint)))

            self._test_fn(False, lambda: self.c.pymongo_test.create_collection(
                'some_collection%s' % random.randint(0, sys.maxint)))
        finally:
            ctx.exit()

    def test_drop_collection(self):
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", UserWarning)
            self._test_fn(False, lambda: self.c.pymongo_test.drop_collection(
                'some_collection'))

            self._test_fn(False, lambda: self.c.pymongo_test.some_collection.drop())
        finally:
            ctx.exit()

    def test_group(self):
        self._test_fn(True, lambda: self.c.pymongo_test.test.group(
            {'a': 1}, {}, {}, 'function() { }'))

    def test_map_reduce(self):
        # mapreduce fails if no collection
        self.c.pymongo_test.test.insert({}, w=self.w)

        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", UserWarning)
            self._test_fn(False, lambda: self.c.pymongo_test.test.map_reduce(
                'function() { }', 'function() { }', 'mr_out'))
        finally:
            ctx.exit()

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
    def test_empty_init(self):
        self.assertRaises(AssertionError, MovingAverage, [])

    def test_moving_average(self):
        avg = MovingAverage([10])
        self.assertEqual(10, avg.get())
        avg2 = avg.clone_with(20)
        self.assertEqual(15, avg2.get())
        avg3 = avg2.clone_with(30)
        self.assertEqual(20, avg3.get())
        avg4 = avg3.clone_with(-100)
        self.assertEqual((10 + 20 + 30 - 100) / 4., avg4.get())
        avg5 = avg4.clone_with(17)
        self.assertEqual((10 + 20 + 30 - 100 + 17) / 5., avg5.get())
        avg6 = avg5.clone_with(43)
        self.assertEqual((20 + 30 - 100 + 17 + 43) / 5., avg6.get())
        avg7 = avg6.clone_with(-1111)
        self.assertEqual((30 - 100 + 17 + 43 - 1111) / 5., avg7.get())


class TestMongosConnection(unittest.TestCase):
    def test_mongos_connection(self):
        c = get_client()
        is_mongos = utils.is_mongos(c)

        # Test default mode, PRIMARY
        cursor = c.pymongo_test.test.find()
        if is_mongos:
            # We only set $readPreference if it's something other than
            # PRIMARY to avoid problems with mongos versions that don't
            # support read preferences.
            self.assertEqual(
                None,
                cursor._Cursor__query_spec().get('$readPreference')
            )
        else:
            self.assertFalse(
                '$readPreference' in cursor._Cursor__query_spec())

        # Copy these constants for brevity
        PRIMARY_PREFERRED = ReadPreference.PRIMARY_PREFERRED
        SECONDARY = ReadPreference.SECONDARY
        SECONDARY_PREFERRED = ReadPreference.SECONDARY_PREFERRED
        NEAREST = ReadPreference.NEAREST
        SLAVE_OKAY = _QUERY_OPTIONS['slave_okay']

        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            # Test non-PRIMARY modes which can be combined with tags
            for kwarg, value, mongos_mode in (
                ('read_preference', PRIMARY_PREFERRED, 'primaryPreferred'),
                ('read_preference', SECONDARY, 'secondary'),
                ('read_preference', SECONDARY_PREFERRED, 'secondaryPreferred'),
                ('read_preference', NEAREST, 'nearest'),
                ('slave_okay', True, 'secondaryPreferred'),
                ('slave_okay', False, 'primary')
            ):
                for tag_sets in (
                    None, [{}]
                ):
                    # Create a client e.g. with read_preference=NEAREST or
                    # slave_okay=True
                    c = get_client(tag_sets=tag_sets, **{kwarg: value})

                    self.assertEqual(is_mongos, c.is_mongos)
                    cursor = c.pymongo_test.test.find()
                    if is_mongos:
                        # We don't set $readPreference for SECONDARY_PREFERRED
                        # unless tags are in use. slaveOkay has the same effect.
                        if mongos_mode == 'secondaryPreferred':
                            self.assertEqual(
                                None,
                                cursor._Cursor__query_spec().get('$readPreference'))

                            self.assertTrue(
                                cursor._Cursor__query_options() & SLAVE_OKAY)

                        # Don't send $readPreference for PRIMARY either
                        elif mongos_mode == 'primary':
                            self.assertEqual(
                                None,
                                cursor._Cursor__query_spec().get('$readPreference'))

                            self.assertFalse(
                                cursor._Cursor__query_options() & SLAVE_OKAY)
                        else:
                            self.assertEqual(
                                {'mode': mongos_mode},
                                cursor._Cursor__query_spec().get('$readPreference'))

                            self.assertTrue(
                                cursor._Cursor__query_options() & SLAVE_OKAY)
                    else:
                        self.assertFalse(
                            '$readPreference' in cursor._Cursor__query_spec())

                for tag_sets in (
                    [{'dc': 'la'}],
                    [{'dc': 'la'}, {'dc': 'sf'}],
                    [{'dc': 'la'}, {'dc': 'sf'}, {}],
                ):
                    if kwarg == 'slave_okay':
                        # Can't use tags with slave_okay True or False, need a
                        # real read preference
                        self.assertRaises(
                            ConfigurationError,
                            get_client, tag_sets=tag_sets, **{kwarg: value})

                        continue

                    c = get_client(tag_sets=tag_sets, **{kwarg: value})

                    self.assertEqual(is_mongos, c.is_mongos)
                    cursor = c.pymongo_test.test.find()
                    if is_mongos:
                        self.assertEqual(
                            {'mode': mongos_mode, 'tags': tag_sets},
                            cursor._Cursor__query_spec().get('$readPreference'))
                    else:
                        self.assertFalse(
                            '$readPreference' in cursor._Cursor__query_spec())
        finally:
            ctx.exit()

    def test_only_secondary_ok_commands_have_read_prefs(self):
        c = get_client(read_preference=ReadPreference.SECONDARY)
        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", UserWarning)
            is_mongos = utils.is_mongos(c)
        finally:
            ctx.exit()
        if not is_mongos:
            raise SkipTest("Only mongos have read_prefs added to the spec")

        # Ensure secondary_ok_commands have readPreference
        for cmd in secondary_ok_commands:
            if cmd == 'mapreduce':  # map reduce is a special case
                continue
            command = SON([(cmd, 1)])
            cursor = c.pymongo_test["$cmd"].find(command.copy())
            # White-listed commands also have to be wrapped in $query
            command = SON([('$query', command)])
            command['$readPreference'] = {'mode': 'secondary'}
            self.assertEqual(command, cursor._Cursor__query_spec())

        # map_reduce inline should have read prefs
        command = SON([('mapreduce', 'test'), ('out', {'inline': 1})])
        cursor = c.pymongo_test["$cmd"].find(command.copy())
        # White-listed commands also have to be wrapped in $query
        command = SON([('$query', command)])
        command['$readPreference'] = {'mode': 'secondary'}
        self.assertEqual(command, cursor._Cursor__query_spec())

        # map_reduce that outputs to a collection shouldn't have read prefs
        command = SON([('mapreduce', 'test'), ('out', {'mrtest': 1})])
        cursor = c.pymongo_test["$cmd"].find(command.copy())
        self.assertEqual(command, cursor._Cursor__query_spec())

        # Other commands shouldn't be changed
        for cmd in ('drop', 'create', 'any-future-cmd'):
            command = SON([(cmd, 1)])
            cursor = c.pymongo_test["$cmd"].find(command.copy())
            self.assertEqual(command, cursor._Cursor__query_spec())

if __name__ == "__main__":
    unittest.main()
