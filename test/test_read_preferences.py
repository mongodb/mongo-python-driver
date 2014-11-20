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
import warnings

sys.path[0:0] = [""]

from bson.py3compat import MAXSIZE
from bson.son import SON
from pymongo.cursor import _QUERY_OPTIONS
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import (ReadPreference, MovingAverage,
                                      Primary, PrimaryPreferred,
                                      Secondary, SecondaryPreferred,
                                      Nearest, ServerMode,
                                      SECONDARY_OK_COMMANDS)
from pymongo.server_selectors import any_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.errors import ConfigurationError

from test.test_replica_set_client import TestReplicaSetClientBase
from test import (client_context,
                  host,
                  port,
                  SkipTest,
                  unittest,
                  utils,
                  IntegrationTest,
                  db_user,
                  db_pwd)
from test.utils import connected, single_client, one, wait_until, rs_client
from test.version import Version


class TestReadPreferencesBase(TestReplicaSetClientBase):

    def setUp(self):
        super(TestReadPreferencesBase, self).setUp()
        # Insert some data so we can use cursors in read_from_which_host
        self.client.pymongo_test.test.drop()
        self.client.pymongo_test.test.insert(
            [{'_id': i} for i in range(10)], w=self.w)

    def tearDown(self):
        super(TestReadPreferencesBase, self).tearDown()
        self.client.pymongo_test.test.drop()

    def read_from_which_host(self, client):
        """Do a find() on the client and return which host was used
        """
        cursor = client.pymongo_test.test.find()
        next(cursor)
        return cursor.address

    def read_from_which_kind(self, client):
        """Do a find() on the client and return 'primary' or 'secondary'
           depending on which the client used.
        """
        address = self.read_from_which_host(client)
        if address == client.primary:
            return 'primary'
        elif address in client.secondaries:
            return 'secondary'
        else:
            self.fail(
                'Cursor used address %s, expected either primary '
                '%s or secondaries %s' % (
                    address, client.primary, client.secondaries))

    def assertReadsFrom(self, expected, **kwargs):
        c = rs_client(**kwargs)
        wait_until(
            lambda: len(c.nodes) == self.w,
            "discovered all nodes")

        used = self.read_from_which_kind(c)
        self.assertEqual(expected, used, 'Cursor used %s, expected %s' % (
            used, expected))


class TestReadPreferences(TestReadPreferencesBase):
    def test_mode_validation(self):
        for mode in ReadPreference:
            self.assertEqual(
                mode,
                rs_client(read_preference=mode).read_preference)

        self.assertRaises(
            ConfigurationError,
            rs_client, read_preference='foo')

    def test_tag_sets_validation(self):
        # Can't use tags with PRIMARY
        self.assertRaises(ConfigurationError, ServerMode,
                          0, tag_sets=[{'k': 'v'}])

        # ... but empty tag sets are ok with PRIMARY
        self.assertRaises(ConfigurationError, ServerMode,
                          0, tag_sets=[{}])

        S = Secondary(tag_sets=[{}])
        self.assertEqual(
            [{}],
            rs_client(read_preference=S).read_preference.tag_sets)

        S = Secondary(tag_sets=[{'k': 'v'}])
        self.assertEqual(
            [{'k': 'v'}],
            rs_client(read_preference=S).read_preference.tag_sets)

        S = Secondary(tag_sets=[{'k': 'v'}, {}])
        self.assertEqual(
            [{'k': 'v'}, {}],
            rs_client(read_preference=S).read_preference.tag_sets)

        self.assertRaises(ConfigurationError, Secondary, tag_sets=[])

        # One dict not ok, must be a list of dicts
        self.assertRaises(ConfigurationError, Secondary, tag_sets={'k': 'v'})

        self.assertRaises(ConfigurationError, Secondary, tag_sets='foo')

        self.assertRaises(ConfigurationError, Secondary, tag_sets=['foo'])

    def test_latency_validation(self):
        self.assertEqual(17, rs_client(
            latencyThresholdMS=17
        ).read_preference.latency_threshold_ms)

        self.assertEqual(42, rs_client(
            latencyThresholdMS=42
        ).read_preference.latency_threshold_ms)

        self.assertEqual(666, rs_client(
            latencythresholdms=666
        ).read_preference.latency_threshold_ms)

    def test_primary(self):
        self.assertReadsFrom('primary',
            read_preference=ReadPreference.PRIMARY)

    def test_primary_with_tags(self):
        # Tags not allowed with PRIMARY
        self.assertRaises(
            ConfigurationError,
            rs_client, tag_sets=[{'dc': 'ny'}])

    def test_primary_preferred(self):
        self.assertReadsFrom('primary',
            read_preference=ReadPreference.PRIMARY_PREFERRED)

    def test_secondary(self):
        self.assertReadsFrom('secondary',
            read_preference=ReadPreference.SECONDARY)

    def test_secondary_preferred(self):
        self.assertReadsFrom('secondary',
            read_preference=ReadPreference.SECONDARY_PREFERRED)

    def test_nearest(self):
        # With high latencyThresholdMS, expect to read from any
        # member
        c = rs_client(
            read_preference=ReadPreference.NEAREST,
            latencyThresholdMS=10000)  # 10 seconds

        data_members = set(self.hosts).difference(set(self.arbiters))

        # This is a probabilistic test; track which members we've read from so
        # far, and keep reading until we've used all the members or give up.
        # Chance of using only 2 of 3 members 10k times if there's no bug =
        # 3 * (2/3)**10000, very low.
        used = set()
        i = 0
        while data_members.difference(used) and i < 10000:
            address = self.read_from_which_host(c)
            used.add(address)
            i += 1

        not_used = data_members.difference(used)
        latencies = ', '.join(
            '%s: %dms' % (server.description.address,
                          server.description.round_trip_time)
            for server in c._get_topology().select_servers(any_server_selector))

        self.assertFalse(not_used,
            "Expected to use primary and all secondaries for mode NEAREST,"
            " but didn't use %s\nlatencies: %s" % (not_used, latencies))


class ReadPrefTester(MongoClient):
    def __init__(self, *args, **kwargs):
        self.has_read_from = set()
        super(ReadPrefTester, self).__init__(*args, **kwargs)

    def _reset_on_error(self, server, fn, *args, **kwargs):
        self.has_read_from.add(server)
        return super(ReadPrefTester, self)._reset_on_error(
            server, fn, *args, **kwargs)


class TestCommandAndReadPreference(TestReplicaSetClientBase):

    def setUp(self):
        super(TestCommandAndReadPreference, self).setUp()
        self.c = ReadPrefTester(
            '%s:%s' % (host, port),
            replicaSet=self.name,
            # Ignore round trip times, to test ReadPreference modes only.
            latencyThresholdMS=1000*1000)
        if client_context.auth_enabled:
            self.c.admin.authenticate(db_user, db_pwd)
        self.client_version = Version.from_client(self.c)

    def tearDown(self):
        # We create a lot of collections and indexes in these tests, so drop
        # the database.
        self.c.drop_database('pymongo_test')
        self.c.close()
        self.c = None
        super(TestCommandAndReadPreference, self).tearDown()

    def executed_on_which_server(self, client, fn, *args, **kwargs):
        """Execute fn(*args, **kwargs) and return the Server instance used."""
        client.has_read_from.clear()
        fn(*args, **kwargs)
        self.assertEqual(1, len(client.has_read_from))
        return one(client.has_read_from)

    def assertExecutedOn(self, server_type, client, fn, *args, **kwargs):
        server = self.executed_on_which_server(client, fn, *args, **kwargs)
        self.assertEqual(SERVER_TYPE._fields[server_type],
                         SERVER_TYPE._fields[server.description.server_type])

    def _test_fn(self, obedient, fn):
        if not obedient:
            for mode in ReadPreference:
                self.c.read_preference = mode

                # Run it a few times to make sure we don't just get lucky the
                # first time.
                for _ in range(10):
                    self.assertExecutedOn(SERVER_TYPE.RSPrimary, self.c, fn)
        else:
            for mode, server_type in [
                (Primary, SERVER_TYPE.RSPrimary),
                (PrimaryPreferred, SERVER_TYPE.RSPrimary),
                (Secondary, SERVER_TYPE.RSSecondary),
                (SecondaryPreferred, SERVER_TYPE.RSSecondary),
                (Nearest, 'any'),
            ]:
                self.c.read_preference = mode(latency_threshold_ms=1000*1000)
                for i in range(10):
                    if server_type == 'any':
                        used = set()
                        for j in range(1000):
                            server = self.executed_on_which_server(self.c, fn)
                            used.add(server.description.address)
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
                    else:
                        self.assertExecutedOn(server_type, self.c, fn)

    def test_command(self):
        # Test generic 'command' method. Some commands obey read preference,
        # most don't.
        # Disobedient commands, always go to primary
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
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
            'distinct', 'test', key='a'))
        self._test_fn(True, lambda: self.c.pymongo_test.command(
            'distinct', 'test', key='a', query={'a': 1}))
        self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
            ('distinct', 'test'), ('key', 'a'), ('query', {'a': 1})])))

        # Geo stuff.
        self.c.pymongo_test.test.create_index([('location', '2d')])

        self.c.pymongo_test.test.create_index([('location', 'geoHaystack'),
                                               ('key', 1)], bucketSize=100)

        # Attempt to await replication of indexes.
        self.c.pymongo_test.test2.insert({}, w=self.w)
        self.c.pymongo_test.test2.remove({}, w=self.w)

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

        if self.client_version.at_least(2, 1, 0):
            self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
                ('aggregate', 'test'),
                ('pipeline', [])
            ])))

        # Text search.
        if self.client_version.at_least(2, 3, 2):

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                utils.enable_text_search(self.c)
            db = self.c.pymongo_test

            # Only way to create an index and wait for all members to build it.
            index = {
                'ns': 'pymongo_test.test',
                'name': 't_text',
                'key': {'t': 'text'}}

            db.test.create_index([('t', 'text')])
            db.test.insert({}, w=self.w)
            db.test.remove({}, w=self.w)

            self._test_fn(True, lambda: self.c.pymongo_test.command(SON([
                ('text', 'test'),
                ('search', 'foo')])))

            self.c.pymongo_test.test.drop_indexes()

    def test_map_reduce_command(self):
        # mapreduce fails if no collection
        self.c.pymongo_test.test.insert({}, w=self.w)

        # Non-inline mapreduce always goes to primary, doesn't obey read prefs.
        # Test with command in a SON and with kwargs
        with warnings.catch_warnings():
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

    @client_context.require_version_min(2, 5, 2)
    def test_aggregate_command_with_out(self):
        # Tests aggregate command when pipeline contains $out.
        self.c.pymongo_test.test.insert({"x": 1, "y": 1}, w=self.w)
        self.c.pymongo_test.test.insert({"x": 1, "y": 2}, w=self.w)
        self.c.pymongo_test.test.insert({"x": 2, "y": 1}, w=self.w)
        self.c.pymongo_test.test.insert({"x": 2, "y": 2}, w=self.w)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            # Aggregate with $out always goes to primary, doesn't obey
            # read prefs.

            # Test aggregate command sent directly to db.command.
            self._test_fn(False, lambda: self.c.pymongo_test.command(
                "aggregate", "test",
                pipeline=[{"$match": {"x": 1}}, {"$out": "agg_out"}]
            ))

            # Test aggregate when sent through the collection aggregate
            # function.
            self._test_fn(False, lambda: self.c.pymongo_test.test.aggregate(
                [{"$match": {"x": 2}}, {"$out": "agg_out"}]
            ))

        self.c.pymongo_test.drop_collection("test")
        self.c.pymongo_test.drop_collection("agg_out")

    def test_create_collection(self):
        # Collections should be created on primary, obviously
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            self._test_fn(False, lambda: self.c.pymongo_test.command(
                'create', 'some_collection%s' % random.randint(0, MAXSIZE)))

            self._test_fn(False, lambda: self.c.pymongo_test.create_collection(
                'some_collection%s' % random.randint(0, MAXSIZE)))

    def test_drop_collection(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            self._test_fn(False, lambda: self.c.pymongo_test.drop_collection(
                'some_collection'))

            self._test_fn(False,
                          lambda: self.c.pymongo_test.some_collection.drop())

    def test_group(self):
        self._test_fn(True, lambda: self.c.pymongo_test.test.group(
            {'a': 1}, {}, {}, 'function() { }'))

    def test_map_reduce(self):
        # mapreduce fails if no collection
        self.c.pymongo_test.test.insert({}, w=self.w)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
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
        if self.client_version.at_least(2, 1, 0):
            self._test_fn(True,
                lambda: self.c.pymongo_test.test.aggregate(
                    [{'$project': {'_id': 1}}]))


class TestMovingAverage(unittest.TestCase):
    def test_moving_average(self):
        avg = MovingAverage()
        self.assertIsNone(avg.get())
        avg.add_sample(10)
        self.assertAlmostEqual(10, avg.get())
        avg.add_sample(20)
        self.assertAlmostEqual(12, avg.get())
        avg.add_sample(30)
        self.assertAlmostEqual(15.6, avg.get())


class TestMongosConnection(IntegrationTest):

    def test_mongos_connection(self):
        c = single_client(host, port)
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
        SLAVE_OKAY = _QUERY_OPTIONS['slave_okay']

        # Test non-PRIMARY modes which can be combined with tags
        for mode, mongos_mode in (
            (PrimaryPreferred, 'primaryPreferred'),
            (Secondary, 'secondary'),
            (SecondaryPreferred, 'secondaryPreferred'),
            (Nearest, 'nearest'),
        ):
            for tag_sets in (
                None, [{}]
            ):
                # Create a client e.g. with read_preference=NEAREST
                c = connected(single_client(
                    host, port, read_preference=mode(tag_sets=tag_sets)))

                self.assertEqual(is_mongos, c.is_mongos)
                cursor = c.pymongo_test.test.find()
                if is_mongos:
                    # We don't set $readPreference for SECONDARY_PREFERRED
                    # unless tags are in use.
                    if mongos_mode == 'secondaryPreferred':
                        self.assertEqual(
                            None,
                            cursor._Cursor__query_spec().get('$readPreference'))

                        self.assertTrue(
                            cursor._Cursor__query_flags & SLAVE_OKAY)

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
                            cursor._Cursor__query_flags & SLAVE_OKAY)
                else:
                    self.assertFalse(
                        '$readPreference' in cursor._Cursor__query_spec())

            for tag_sets in (
                [{'dc': 'la'}],
                [{'dc': 'la'}, {'dc': 'sf'}],
                [{'dc': 'la'}, {'dc': 'sf'}, {}],
            ):
                c = connected(single_client(
                    host, port, read_preference=mode(tag_sets=tag_sets)))

                self.assertEqual(is_mongos, c.is_mongos)
                cursor = c.pymongo_test.test.find()
                if is_mongos:
                    self.assertEqual(
                        {'mode': mongos_mode, 'tags': tag_sets},
                        cursor._Cursor__query_spec().get('$readPreference'))
                else:
                    self.assertFalse(
                        '$readPreference' in cursor._Cursor__query_spec())

    def test_only_secondary_ok_commands_have_read_prefs(self):
        c = single_client(host, port, read_preference=ReadPreference.SECONDARY)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            is_mongos = utils.is_mongos(c)

        if not is_mongos:
            raise SkipTest("Only mongos have read_prefs added to the spec")

        # Ensure secondary_ok_commands have readPreference
        for cmd in SECONDARY_OK_COMMANDS:
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
