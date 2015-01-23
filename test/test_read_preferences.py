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

sys.path[0:0] = [""]

from bson.py3compat import MAXSIZE
from pymongo.cursor import _QUERY_OPTIONS
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import (ReadPreference, MovingAverage,
                                      Primary, PrimaryPreferred,
                                      Secondary, SecondaryPreferred,
                                      Nearest, ServerMode)
from pymongo.server_selectors import any_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.errors import ConfigurationError

from test.test_replica_set_client import TestReplicaSetClientBase
from test import (client_context,
                  host,
                  port,
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

        self.addCleanup(self.client.pymongo_test.test.drop)

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

    def test_threshold_validation(self):
        self.assertEqual(17, rs_client(
            localThresholdMS=17
        ).local_threshold_ms)

        self.assertEqual(42, rs_client(
            localThresholdMS=42
        ).local_threshold_ms)

        self.assertEqual(666, rs_client(
            localthresholdms=666
        ).local_threshold_ms)

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
        # With high localThresholdMS, expect to read from any
        # member
        c = rs_client(
            read_preference=ReadPreference.NEAREST,
            localThresholdMS=10000)  # 10 seconds

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


_PREF_MAP = [
    (Primary, SERVER_TYPE.RSPrimary),
    (PrimaryPreferred, SERVER_TYPE.RSPrimary),
    (Secondary, SERVER_TYPE.RSSecondary),
    (SecondaryPreferred, SERVER_TYPE.RSSecondary),
    (Nearest, 'any')
]


class TestCommandAndReadPreference(TestReplicaSetClientBase):

    def setUp(self):
        super(TestCommandAndReadPreference, self).setUp()
        self.c = ReadPrefTester(
            '%s:%s' % (host, port),
            replicaSet=self.name,
            # Ignore round trip times, to test ReadPreference modes only.
            localThresholdMS=1000*1000)
        if client_context.auth_enabled:
            self.c.admin.authenticate(db_user, db_pwd)
        self.client_version = Version.from_client(self.c)
        self.addCleanup(self.c.drop_database, 'pymongo_test')

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

    def _test_fn(self, server_type, fn):
        for _ in range(10):
            if server_type == 'any':
                used = set()
                for _ in range(1000):
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

    def _test_primary_helper(self, func):
        # Helpers that ignore read preference.
        self._test_fn(SERVER_TYPE.RSPrimary, func)

    def _test_coll_helper(self, secondary_ok, coll, meth, *args, **kwargs):
        for mode, server_type in _PREF_MAP:
            new_coll = coll.with_options(read_preference=mode())
            func = lambda: getattr(new_coll, meth)(*args, **kwargs)
            if secondary_ok:
                self._test_fn(server_type, func)
            else:
                self._test_fn(SERVER_TYPE.RSPrimary, func)

    def test_command(self):
        # Test that the generic command helper obeys the read preference
        # passed to it.
        for mode, server_type in _PREF_MAP:
            func = lambda: self.c.pymongo_test.command('dbStats',
                                                       read_preference=mode())
            self._test_fn(server_type, func)

    @client_context.require_version_min(2, 5, 2)
    def test_aggregate_command_with_out(self):
        # Tests aggregate command when pipeline contains $out.
        self.c.pymongo_test.test.insert({"x": 1, "y": 1}, w=self.w)
        self.c.pymongo_test.test.insert({"x": 1, "y": 2}, w=self.w)
        self.c.pymongo_test.test.insert({"x": 2, "y": 1}, w=self.w)
        self.c.pymongo_test.test.insert({"x": 2, "y": 2}, w=self.w)

        # Test aggregate when sent through the collection aggregate
        # function. Aggregate with $out always goes to primary, doesn't obey
        # read prefs.
        self._test_coll_helper(False, self.c.pymongo_test.test, 'aggregate',
                               [{"$match": {"x": 2}}, {"$out": "agg_out"}])

        self.c.pymongo_test.drop_collection("test")
        self.c.pymongo_test.drop_collection("agg_out")

    def test_create_collection(self):
        # Collections should be created on primary, obviously
        self._test_primary_helper(
            lambda: self.c.pymongo_test.create_collection(
                'some_collection%s' % random.randint(0, MAXSIZE)))

    def test_drop_collection(self):
        self._test_primary_helper(
            lambda: self.c.pymongo_test.drop_collection('some_collection'))

        self._test_primary_helper(
            lambda: self.c.pymongo_test.some_collection.drop())

    def test_group(self):
        self._test_coll_helper(True, self.c.pymongo_test.test, 'group',
                               {'a': 1}, {}, {}, 'function() { }')

    def test_map_reduce(self):
        # mapreduce fails if no collection
        self.c.pymongo_test.test.insert({}, w=self.w)

        self._test_coll_helper(False, self.c.pymongo_test.test, 'map_reduce',
                               'function() { }', 'function() { }', 'mr_out')

        self._test_coll_helper(True, self.c.pymongo_test.test, 'map_reduce',
                               'function() { }', 'function() { }',
                               {'inline': 1})

    def test_inline_map_reduce(self):
        # mapreduce fails if no collection
        self.c.pymongo_test.test.insert({}, w=self.w)

        self._test_coll_helper(True, self.c.pymongo_test.test,
                               'inline_map_reduce',
                               'function() { }', 'function() { }')

    def test_count(self):
        self._test_coll_helper(True, self.c.pymongo_test.test, 'count')

    def test_distinct(self):
        self._test_coll_helper(True, self.c.pymongo_test.test, 'distinct', 'a')

    def test_aggregate(self):
        if self.client_version.at_least(2, 1, 0):
            self._test_coll_helper(True, self.c.pymongo_test.test,
                                   'aggregate',
                                   [{'$project': {'_id': 1}}])


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


if __name__ == "__main__":
    unittest.main()
