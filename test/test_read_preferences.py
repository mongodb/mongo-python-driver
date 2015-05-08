# Copyright 2011-2015 MongoDB, Inc.
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

import contextlib
import copy
import random
import sys
import pickle

sys.path[0:0] = [""]

from bson.py3compat import MAXSIZE
from bson.son import SON
from pymongo.errors import ConfigurationError
from pymongo.message import _maybe_add_read_preference
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import (ReadPreference, MovingAverage,
                                      Primary, PrimaryPreferred,
                                      Secondary, SecondaryPreferred,
                                      Nearest, _ServerMode)
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import readable_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.write_concern import WriteConcern

from test.test_replica_set_client import TestReplicaSetClientBase
from test import (SkipTest,
                  client_context,
                  host,
                  port,
                  unittest,
                  db_user,
                  db_pwd)
from test.utils import connected, single_client, one, wait_until, rs_client
from test.version import Version


class TestReadPreferenceObjects(unittest.TestCase):
    prefs = [Primary(), Secondary(), Nearest(tag_sets=[{'a': 1}, {'b': 2}])]

    def test_pickle(self):
        for pref in self.prefs:
            self.assertEqual(pref, pickle.loads(pickle.dumps(pref)))

    def test_copy(self):
        for pref in self.prefs:
            self.assertEqual(pref, copy.copy(pref))


class TestReadPreferencesBase(TestReplicaSetClientBase):

    def setUp(self):
        super(TestReadPreferencesBase, self).setUp()
        # Insert some data so we can use cursors in read_from_which_host
        self.client.pymongo_test.test.drop()
        self.client.get_database(
            "pymongo_test",
            write_concern=WriteConcern(w=self.w)).test.insert_many(
                [{'_id': i} for i in range(10)])

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
            lambda: len(c.nodes - c.arbiters) == self.w,
            "discovered all nodes")

        used = self.read_from_which_kind(c)
        self.assertEqual(expected, used, 'Cursor used %s, expected %s' % (
            used, expected))


class TestSingleSlaveOk(TestReadPreferencesBase):

    def test_reads_from_secondary(self):

        host, port = next(iter(self.client.secondaries))
        # Direct connection to a secondary.
        client = single_client(host, port)
        self.assertFalse(client.is_primary)

        # Regardless of read preference, we should be able to do
        # "reads" with a direct connection to a secondary.
        # See server-selection.rst#topology-type-single.
        self.assertEqual(client.read_preference, ReadPreference.PRIMARY)

        db = client.pymongo_test
        coll = db.test

        # Test find and find_one.
        self.assertIsNotNone(coll.find_one())
        self.assertEqual(10, len(list(coll.find())))

        # Test some database helpers.
        self.assertIsNotNone(db.collection_names())
        self.assertIsNotNone(db.validate_collection("test"))
        self.assertIsNotNone(db.command("count", "test"))

        # Test some collection helpers.
        self.assertEqual(10, coll.count())
        self.assertEqual(10, len(coll.distinct("_id")))
        self.assertIsNotNone(coll.aggregate([]))
        self.assertIsNotNone(coll.index_information())

        # Test some "magic" namespace helpers.
        self.assertIsNotNone(db.current_op())
        client.unlock()  # No error.


class TestReadPreferences(TestReadPreferencesBase):
    def test_mode_validation(self):
        for mode in (ReadPreference.PRIMARY,
                     ReadPreference.PRIMARY_PREFERRED,
                     ReadPreference.SECONDARY,
                     ReadPreference.SECONDARY_PREFERRED,
                     ReadPreference.NEAREST):
            self.assertEqual(
                mode,
                rs_client(read_preference=mode).read_preference)

        self.assertRaises(
            TypeError,
            rs_client, read_preference='foo')

    def test_tag_sets_validation(self):
        # Can't use tags with PRIMARY
        self.assertRaises(ConfigurationError, _ServerMode,
                          0, tag_sets=[{'k': 'v'}])

        # ... but empty tag sets are ok with PRIMARY
        self.assertRaises(ConfigurationError, _ServerMode,
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

        self.assertRaises(ValueError, Secondary, tag_sets=[])

        # One dict not ok, must be a list of dicts
        self.assertRaises(TypeError, Secondary, tag_sets={'k': 'v'})

        self.assertRaises(TypeError, Secondary, tag_sets='foo')

        self.assertRaises(TypeError, Secondary, tag_sets=['foo'])

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

        self.assertEqual(0, rs_client(
            localthresholdms=0
        ).local_threshold_ms)

        self.assertRaises(ValueError,
                          rs_client,
                          localthresholdms=-1)

    def test_zero_latency(self):
        ping_times = set()
        # Generate unique ping times.
        while len(ping_times) < len(self.client.nodes):
            ping_times.add(random.random())
        for ping_time, host in zip(ping_times, self.client.nodes):
            ServerDescription._host_to_round_trip_time[host] = ping_time
        try:
            client = connected(
                rs_client(readPreference='nearest', localThresholdMS=0))
            wait_until(
                lambda: client.nodes == self.client.nodes,
                "discovered all nodes")
            host = self.read_from_which_host(client)
            for _ in range(5):
                self.assertEqual(host, self.read_from_which_host(client))
        finally:
            ServerDescription._host_to_round_trip_time.clear()

    def test_primary(self):
        self.assertReadsFrom(
            'primary', read_preference=ReadPreference.PRIMARY)

    def test_primary_with_tags(self):
        # Tags not allowed with PRIMARY
        self.assertRaises(
            ConfigurationError,
            rs_client, tag_sets=[{'dc': 'ny'}])

    def test_primary_preferred(self):
        self.assertReadsFrom(
            'primary', read_preference=ReadPreference.PRIMARY_PREFERRED)

    def test_secondary(self):
        self.assertReadsFrom(
            'secondary', read_preference=ReadPreference.SECONDARY)

    def test_secondary_preferred(self):
        self.assertReadsFrom(
            'secondary', read_preference=ReadPreference.SECONDARY_PREFERRED)

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
            for server in c._get_topology().select_servers(
                readable_server_selector))

        self.assertFalse(
            not_used,
            "Expected to use primary and all secondaries for mode NEAREST,"
            " but didn't use %s\nlatencies: %s" % (not_used, latencies))


class ReadPrefTester(MongoClient):
    def __init__(self, *args, **kwargs):
        self.has_read_from = set()
        super(ReadPrefTester, self).__init__(*args, **kwargs)

    @contextlib.contextmanager
    def _socket_for_reads(self, read_preference):
        context = super(ReadPrefTester, self)._socket_for_reads(read_preference)
        with context as (sock_info, slave_ok):
            self.record_a_read(sock_info.address)
            yield sock_info, slave_ok

    def record_a_read(self, address):
        server = self._get_topology().select_server_by_address(address, 0)
        self.has_read_from.add(server)

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
        coll = self.c.pymongo_test.test.with_options(
            write_concern=WriteConcern(w=self.w))
        coll.insert_one({})

        self._test_coll_helper(False, self.c.pymongo_test.test, 'map_reduce',
                               'function() { }', 'function() { }', 'mr_out')

        self._test_coll_helper(False, self.c.pymongo_test.test, 'map_reduce',
                               'function() { }', 'function() { }',
                               {'inline': 1})

    def test_inline_map_reduce(self):
        # mapreduce fails if no collection
        coll = self.c.pymongo_test.test.with_options(
            write_concern=WriteConcern(w=self.w))
        coll.insert_one({})

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

class TestMongosAndReadPreference(unittest.TestCase):

    def test_maybe_add_read_preference(self):

        # Primary doesn't add $readPreference
        out = _maybe_add_read_preference({}, Primary())
        self.assertEqual(out, {})

        pref = PrimaryPreferred()
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(
            out, SON([("$query", {}), ("$readPreference", pref.document)]))
        pref = PrimaryPreferred(tag_sets=[{'dc': 'nyc'}])
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(
            out, SON([("$query", {}), ("$readPreference", pref.document)]))

        pref = Secondary()
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(
            out, SON([("$query", {}), ("$readPreference", pref.document)]))
        pref = Secondary(tag_sets=[{'dc': 'nyc'}])
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(
            out, SON([("$query", {}), ("$readPreference", pref.document)]))

        # SecondaryPreferred without tag_sets doesn't add $readPreference
        pref = SecondaryPreferred()
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(out, {})
        pref = SecondaryPreferred(tag_sets=[{'dc': 'nyc'}])
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(
            out, SON([("$query", {}), ("$readPreference", pref.document)]))

        pref = Nearest()
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(
            out, SON([("$query", {}), ("$readPreference", pref.document)]))
        pref = Nearest(tag_sets=[{'dc': 'nyc'}])
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(
            out, SON([("$query", {}), ("$readPreference", pref.document)]))

        criteria = SON([("$query", {}), ("$orderby", SON([("_id", 1)]))])
        pref = Nearest()
        out = _maybe_add_read_preference(criteria, pref)
        self.assertEqual(
            out,
            SON([("$query", {}),
                 ("$orderby", SON([("_id", 1)])),
                 ("$readPreference", pref.document)]))
        pref = Nearest(tag_sets=[{'dc': 'nyc'}])
        out = _maybe_add_read_preference(criteria, pref)
        self.assertEqual(
            out,
            SON([("$query", {}),
                 ("$orderby", SON([("_id", 1)])),
                 ("$readPreference", pref.document)]))

    @client_context.require_mongos
    def test_mongos(self):
        shard = client_context.client.config.shards.find_one()['host']
        num_members = shard.count(',') + 1
        if num_members == 1:
            raise SkipTest("Need a replica set shard to test.")
        coll = client_context.client.pymongo_test.get_collection(
            "test",
            write_concern=WriteConcern(w=num_members))
        coll.drop()
        res = coll.insert_many([{} for _ in range(5)])
        first_id = res.inserted_ids[0]
        last_id = res.inserted_ids[-1]

        # Note - this isn't a perfect test since there's no way to
        # tell what shard member a query ran on.
        for pref in (Primary(),
                     PrimaryPreferred(),
                     Secondary(),
                     SecondaryPreferred(),
                     Nearest()):
            qcoll = coll.with_options(read_preference=pref)
            results = list(qcoll.find().sort([("_id", 1)]))
            self.assertEqual(first_id, results[0]["_id"])
            self.assertEqual(last_id, results[-1]["_id"])
            results = list(qcoll.find().sort([("_id", -1)]))
            self.assertEqual(first_id, results[-1]["_id"])
            self.assertEqual(last_id, results[0]["_id"])


if __name__ == "__main__":
    unittest.main()
