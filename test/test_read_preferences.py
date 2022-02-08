# Copyright 2011-present MongoDB, Inc.
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
import pickle
import random
import sys

sys.path[0:0] = [""]

from test import IntegrationTest, SkipTest, client_context, unittest
from test.utils import (
    OvertCommandListener,
    connected,
    one,
    rs_client,
    single_client,
    wait_until,
)
from test.version import Version

from bson.son import SON
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.message import _maybe_add_read_preference
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import (
    MovingAverage,
    Nearest,
    Primary,
    PrimaryPreferred,
    ReadPreference,
    Secondary,
    SecondaryPreferred,
)
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import Selection, readable_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.write_concern import WriteConcern


class TestSelections(IntegrationTest):
    @client_context.require_connection
    def test_bool(self):
        client = single_client()

        wait_until(lambda: client.address, "discover primary")
        selection = Selection.from_topology_description(client._topology.description)

        self.assertTrue(selection)
        self.assertFalse(selection.with_server_descriptions([]))


class TestReadPreferenceObjects(unittest.TestCase):
    prefs = [
        Primary(),
        PrimaryPreferred(),
        Secondary(),
        Nearest(tag_sets=[{"a": 1}, {"b": 2}]),
        SecondaryPreferred(max_staleness=30),
    ]

    def test_pickle(self):
        for pref in self.prefs:
            self.assertEqual(pref, pickle.loads(pickle.dumps(pref)))

    def test_copy(self):
        for pref in self.prefs:
            self.assertEqual(pref, copy.copy(pref))

    def test_deepcopy(self):
        for pref in self.prefs:
            self.assertEqual(pref, copy.deepcopy(pref))


class TestReadPreferencesBase(IntegrationTest):
    @classmethod
    @client_context.require_secondaries_count(1)
    def setUpClass(cls):
        super(TestReadPreferencesBase, cls).setUpClass()

    def setUp(self):
        super(TestReadPreferencesBase, self).setUp()
        # Insert some data so we can use cursors in read_from_which_host
        self.client.pymongo_test.test.drop()
        self.client.get_database(
            "pymongo_test", write_concern=WriteConcern(w=client_context.w)
        ).test.insert_many([{"_id": i} for i in range(10)])

        self.addCleanup(self.client.pymongo_test.test.drop)

    def read_from_which_host(self, client):
        """Do a find() on the client and return which host was used"""
        cursor = client.pymongo_test.test.find()
        next(cursor)
        return cursor.address

    def read_from_which_kind(self, client):
        """Do a find() on the client and return 'primary' or 'secondary'
        depending on which the client used.
        """
        address = self.read_from_which_host(client)
        if address == client.primary:
            return "primary"
        elif address in client.secondaries:
            return "secondary"
        else:
            self.fail(
                "Cursor used address %s, expected either primary "
                "%s or secondaries %s" % (address, client.primary, client.secondaries)
            )

    def assertReadsFrom(self, expected, **kwargs):
        c = rs_client(**kwargs)
        wait_until(lambda: len(c.nodes - c.arbiters) == client_context.w, "discovered all nodes")

        used = self.read_from_which_kind(c)
        self.assertEqual(expected, used, "Cursor used %s, expected %s" % (used, expected))


class TestSingleSecondaryOk(TestReadPreferencesBase):
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
        self.assertIsNotNone(db.list_collection_names())
        self.assertIsNotNone(db.validate_collection("test"))
        self.assertIsNotNone(db.command("ping"))

        # Test some collection helpers.
        self.assertEqual(10, coll.count_documents({}))
        self.assertEqual(10, len(coll.distinct("_id")))
        self.assertIsNotNone(coll.aggregate([]))
        self.assertIsNotNone(coll.index_information())


class TestReadPreferences(TestReadPreferencesBase):
    def test_mode_validation(self):
        for mode in (
            ReadPreference.PRIMARY,
            ReadPreference.PRIMARY_PREFERRED,
            ReadPreference.SECONDARY,
            ReadPreference.SECONDARY_PREFERRED,
            ReadPreference.NEAREST,
        ):
            self.assertEqual(mode, rs_client(read_preference=mode).read_preference)

        self.assertRaises(TypeError, rs_client, read_preference="foo")

    def test_tag_sets_validation(self):
        S = Secondary(tag_sets=[{}])
        self.assertEqual([{}], rs_client(read_preference=S).read_preference.tag_sets)

        S = Secondary(tag_sets=[{"k": "v"}])
        self.assertEqual([{"k": "v"}], rs_client(read_preference=S).read_preference.tag_sets)

        S = Secondary(tag_sets=[{"k": "v"}, {}])
        self.assertEqual([{"k": "v"}, {}], rs_client(read_preference=S).read_preference.tag_sets)

        self.assertRaises(ValueError, Secondary, tag_sets=[])

        # One dict not ok, must be a list of dicts
        self.assertRaises(TypeError, Secondary, tag_sets={"k": "v"})

        self.assertRaises(TypeError, Secondary, tag_sets="foo")

        self.assertRaises(TypeError, Secondary, tag_sets=["foo"])

    def test_threshold_validation(self):
        self.assertEqual(
            17, rs_client(localThresholdMS=17, connect=False).options.local_threshold_ms
        )

        self.assertEqual(
            42, rs_client(localThresholdMS=42, connect=False).options.local_threshold_ms
        )

        self.assertEqual(
            666, rs_client(localThresholdMS=666, connect=False).options.local_threshold_ms
        )

        self.assertEqual(0, rs_client(localThresholdMS=0, connect=False).options.local_threshold_ms)

        self.assertRaises(ValueError, rs_client, localthresholdms=-1)

    def test_zero_latency(self):
        ping_times: set = set()
        # Generate unique ping times.
        while len(ping_times) < len(self.client.nodes):
            ping_times.add(random.random())
        for ping_time, host in zip(ping_times, self.client.nodes):
            ServerDescription._host_to_round_trip_time[host] = ping_time
        try:
            client = connected(rs_client(readPreference="nearest", localThresholdMS=0))
            wait_until(lambda: client.nodes == self.client.nodes, "discovered all nodes")
            host = self.read_from_which_host(client)
            for _ in range(5):
                self.assertEqual(host, self.read_from_which_host(client))
        finally:
            ServerDescription._host_to_round_trip_time.clear()

    def test_primary(self):
        self.assertReadsFrom("primary", read_preference=ReadPreference.PRIMARY)

    def test_primary_with_tags(self):
        # Tags not allowed with PRIMARY
        self.assertRaises(ConfigurationError, rs_client, tag_sets=[{"dc": "ny"}])

    def test_primary_preferred(self):
        self.assertReadsFrom("primary", read_preference=ReadPreference.PRIMARY_PREFERRED)

    def test_secondary(self):
        self.assertReadsFrom("secondary", read_preference=ReadPreference.SECONDARY)

    def test_secondary_preferred(self):
        self.assertReadsFrom("secondary", read_preference=ReadPreference.SECONDARY_PREFERRED)

    def test_nearest(self):
        # With high localThresholdMS, expect to read from any
        # member
        c = rs_client(read_preference=ReadPreference.NEAREST, localThresholdMS=10000)  # 10 seconds

        data_members = {self.client.primary} | self.client.secondaries

        # This is a probabilistic test; track which members we've read from so
        # far, and keep reading until we've used all the members or give up.
        # Chance of using only 2 of 3 members 10k times if there's no bug =
        # 3 * (2/3)**10000, very low.
        used: set = set()
        i = 0
        while data_members.difference(used) and i < 10000:
            address = self.read_from_which_host(c)
            used.add(address)
            i += 1

        not_used = data_members.difference(used)
        latencies = ", ".join(
            "%s: %dms" % (server.description.address, server.description.round_trip_time)
            for server in c._get_topology().select_servers(readable_server_selector)
        )

        self.assertFalse(
            not_used,
            "Expected to use primary and all secondaries for mode NEAREST,"
            " but didn't use %s\nlatencies: %s" % (not_used, latencies),
        )


class ReadPrefTester(MongoClient):
    def __init__(self, *args, **kwargs):
        self.has_read_from = set()
        client_options = client_context.client_options
        client_options.update(kwargs)
        super(ReadPrefTester, self).__init__(*args, **client_options)

    @contextlib.contextmanager
    def _socket_for_reads(self, read_preference, session):
        context = super(ReadPrefTester, self)._socket_for_reads(read_preference, session)
        with context as (sock_info, read_preference):
            self.record_a_read(sock_info.address)
            yield sock_info, read_preference

    @contextlib.contextmanager
    def _socket_from_server(self, read_preference, server, session):
        context = super(ReadPrefTester, self)._socket_from_server(read_preference, server, session)
        with context as (sock_info, read_preference):
            self.record_a_read(sock_info.address)
            yield sock_info, read_preference

    def record_a_read(self, address):
        server = self._get_topology().select_server_by_address(address, 0)
        self.has_read_from.add(server)


_PREF_MAP = [
    (Primary, SERVER_TYPE.RSPrimary),
    (PrimaryPreferred, SERVER_TYPE.RSPrimary),
    (Secondary, SERVER_TYPE.RSSecondary),
    (SecondaryPreferred, SERVER_TYPE.RSSecondary),
    (Nearest, "any"),
]


class TestCommandAndReadPreference(IntegrationTest):
    c: ReadPrefTester
    client_version: Version

    @classmethod
    @client_context.require_secondaries_count(1)
    def setUpClass(cls):
        super(TestCommandAndReadPreference, cls).setUpClass()
        cls.c = ReadPrefTester(
            client_context.pair,
            # Ignore round trip times, to test ReadPreference modes only.
            localThresholdMS=1000 * 1000,
        )
        cls.client_version = Version.from_client(cls.c)
        # mapReduce fails if the collection does not exist.
        coll = cls.c.pymongo_test.get_collection(
            "test", write_concern=WriteConcern(w=client_context.w)
        )
        coll.insert_one({})

    @classmethod
    def tearDownClass(cls):
        cls.c.drop_database("pymongo_test")
        cls.c.close()

    def executed_on_which_server(self, client, fn, *args, **kwargs):
        """Execute fn(*args, **kwargs) and return the Server instance used."""
        client.has_read_from.clear()
        fn(*args, **kwargs)
        self.assertEqual(1, len(client.has_read_from))
        return one(client.has_read_from)

    def assertExecutedOn(self, server_type, client, fn, *args, **kwargs):
        server = self.executed_on_which_server(client, fn, *args, **kwargs)
        self.assertEqual(
            SERVER_TYPE._fields[server_type], SERVER_TYPE._fields[server.description.server_type]
        )

    def _test_fn(self, server_type, fn):
        for _ in range(10):
            if server_type == "any":
                used = set()
                for _ in range(1000):
                    server = self.executed_on_which_server(self.c, fn)
                    used.add(server.description.address)
                    if len(used) == len(self.c.secondaries) + 1:
                        # Success
                        break

                assert self.c.primary is not None
                unused = self.c.secondaries.union(set([self.c.primary])).difference(used)
                if unused:
                    self.fail("Some members not used for NEAREST: %s" % (unused))
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
            func = lambda: self.c.pymongo_test.command("dbStats", read_preference=mode())
            self._test_fn(server_type, func)

    def test_create_collection(self):
        # create_collection runs listCollections on the primary to check if
        # the collection already exists.
        self._test_primary_helper(
            lambda: self.c.pymongo_test.create_collection(
                "some_collection%s" % random.randint(0, sys.maxsize)
            )
        )

    def test_count_documents(self):
        self._test_coll_helper(True, self.c.pymongo_test.test, "count_documents", {})

    def test_estimated_document_count(self):
        self._test_coll_helper(True, self.c.pymongo_test.test, "estimated_document_count")

    def test_distinct(self):
        self._test_coll_helper(True, self.c.pymongo_test.test, "distinct", "a")

    def test_aggregate(self):
        self._test_coll_helper(
            True, self.c.pymongo_test.test, "aggregate", [{"$project": {"_id": 1}}]
        )

    def test_aggregate_write(self):
        # 5.0 servers support $out on secondaries.
        secondary_ok = client_context.version.at_least(5, 0)
        self._test_coll_helper(
            secondary_ok,
            self.c.pymongo_test.test,
            "aggregate",
            [{"$project": {"_id": 1}}, {"$out": "agg_write_test"}],
        )


class TestMovingAverage(unittest.TestCase):
    def test_moving_average(self):
        avg = MovingAverage()
        self.assertIsNone(avg.get())
        avg.add_sample(10)
        self.assertAlmostEqual(10, avg.get())  # type: ignore
        avg.add_sample(20)
        self.assertAlmostEqual(12, avg.get())  # type: ignore
        avg.add_sample(30)
        self.assertAlmostEqual(15.6, avg.get())  # type: ignore


class TestMongosAndReadPreference(IntegrationTest):
    def test_read_preference_document(self):

        pref = Primary()
        self.assertEqual(pref.document, {"mode": "primary"})

        pref = PrimaryPreferred()
        self.assertEqual(pref.document, {"mode": "primaryPreferred"})
        pref = PrimaryPreferred(tag_sets=[{"dc": "sf"}])
        self.assertEqual(pref.document, {"mode": "primaryPreferred", "tags": [{"dc": "sf"}]})
        pref = PrimaryPreferred(tag_sets=[{"dc": "sf"}], max_staleness=30)
        self.assertEqual(
            pref.document,
            {"mode": "primaryPreferred", "tags": [{"dc": "sf"}], "maxStalenessSeconds": 30},
        )

        pref = Secondary()
        self.assertEqual(pref.document, {"mode": "secondary"})
        pref = Secondary(tag_sets=[{"dc": "sf"}])
        self.assertEqual(pref.document, {"mode": "secondary", "tags": [{"dc": "sf"}]})
        pref = Secondary(tag_sets=[{"dc": "sf"}], max_staleness=30)
        self.assertEqual(
            pref.document, {"mode": "secondary", "tags": [{"dc": "sf"}], "maxStalenessSeconds": 30}
        )

        pref = SecondaryPreferred()
        self.assertEqual(pref.document, {"mode": "secondaryPreferred"})
        pref = SecondaryPreferred(tag_sets=[{"dc": "sf"}])
        self.assertEqual(pref.document, {"mode": "secondaryPreferred", "tags": [{"dc": "sf"}]})
        pref = SecondaryPreferred(tag_sets=[{"dc": "sf"}], max_staleness=30)
        self.assertEqual(
            pref.document,
            {"mode": "secondaryPreferred", "tags": [{"dc": "sf"}], "maxStalenessSeconds": 30},
        )

        pref = Nearest()
        self.assertEqual(pref.document, {"mode": "nearest"})
        pref = Nearest(tag_sets=[{"dc": "sf"}])
        self.assertEqual(pref.document, {"mode": "nearest", "tags": [{"dc": "sf"}]})
        pref = Nearest(tag_sets=[{"dc": "sf"}], max_staleness=30)
        self.assertEqual(
            pref.document, {"mode": "nearest", "tags": [{"dc": "sf"}], "maxStalenessSeconds": 30}
        )

        with self.assertRaises(TypeError):
            # Float is prohibited.
            Nearest(max_staleness=1.5)  # type: ignore

        with self.assertRaises(ValueError):
            Nearest(max_staleness=0)

        with self.assertRaises(ValueError):
            Nearest(max_staleness=-2)

    def test_read_preference_document_hedge(self):
        cases = {
            "primaryPreferred": PrimaryPreferred,
            "secondary": Secondary,
            "secondaryPreferred": SecondaryPreferred,
            "nearest": Nearest,
        }
        for mode, cls in cases.items():
            with self.assertRaises(TypeError):
                cls(hedge=[])  # type: ignore

            pref = cls(hedge={})
            self.assertEqual(pref.document, {"mode": mode})
            out = _maybe_add_read_preference({}, pref)
            if cls == SecondaryPreferred:
                # SecondaryPreferred without hedge doesn't add $readPreference.
                self.assertEqual(out, {})
            else:
                self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))

            hedge = {"enabled": True}
            pref = cls(hedge=hedge)
            self.assertEqual(pref.document, {"mode": mode, "hedge": hedge})
            out = _maybe_add_read_preference({}, pref)
            self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))

            hedge = {"enabled": False}
            pref = cls(hedge=hedge)
            self.assertEqual(pref.document, {"mode": mode, "hedge": hedge})
            out = _maybe_add_read_preference({}, pref)
            self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))

            hedge = {"enabled": False, "extra": "option"}
            pref = cls(hedge=hedge)
            self.assertEqual(pref.document, {"mode": mode, "hedge": hedge})
            out = _maybe_add_read_preference({}, pref)
            self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))

    def test_send_hedge(self):
        cases = {
            "primaryPreferred": PrimaryPreferred,
            "secondaryPreferred": SecondaryPreferred,
            "nearest": Nearest,
        }
        if client_context.supports_secondary_read_pref:
            cases["secondary"] = Secondary
        listener = OvertCommandListener()
        client = rs_client(event_listeners=[listener])
        self.addCleanup(client.close)
        client.admin.command("ping")
        for mode, cls in cases.items():
            pref = cls(hedge={"enabled": True})
            coll = client.test.get_collection("test", read_preference=pref)
            listener.reset()
            coll.find_one()
            started = listener.results["started"]
            self.assertEqual(len(started), 1, started)
            cmd = started[0].command
            if client_context.is_rs or client_context.is_mongos:
                self.assertIn("$readPreference", cmd)
                self.assertEqual(cmd["$readPreference"], pref.document)
            else:
                self.assertNotIn("$readPreference", cmd)

    def test_maybe_add_read_preference(self):

        # Primary doesn't add $readPreference
        out = _maybe_add_read_preference({}, Primary())
        self.assertEqual(out, {})

        pref = PrimaryPreferred()
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))
        pref = PrimaryPreferred(tag_sets=[{"dc": "nyc"}])
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))

        pref = Secondary()
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))
        pref = Secondary(tag_sets=[{"dc": "nyc"}])
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))

        # SecondaryPreferred without tag_sets or max_staleness doesn't add
        # $readPreference
        pref = SecondaryPreferred()
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(out, {})
        pref = SecondaryPreferred(tag_sets=[{"dc": "nyc"}])
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))
        pref = SecondaryPreferred(max_staleness=120)
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))

        pref = Nearest()
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))
        pref = Nearest(tag_sets=[{"dc": "nyc"}])
        out = _maybe_add_read_preference({}, pref)
        self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))

        criteria = SON([("$query", {}), ("$orderby", SON([("_id", 1)]))])
        pref = Nearest()
        out = _maybe_add_read_preference(criteria, pref)
        self.assertEqual(
            out,
            SON(
                [
                    ("$query", {}),
                    ("$orderby", SON([("_id", 1)])),
                    ("$readPreference", pref.document),
                ]
            ),
        )
        pref = Nearest(tag_sets=[{"dc": "nyc"}])
        out = _maybe_add_read_preference(criteria, pref)
        self.assertEqual(
            out,
            SON(
                [
                    ("$query", {}),
                    ("$orderby", SON([("_id", 1)])),
                    ("$readPreference", pref.document),
                ]
            ),
        )

    @client_context.require_mongos
    def test_mongos(self):
        res = client_context.client.config.shards.find_one()
        assert res is not None
        shard = res["host"]
        num_members = shard.count(",") + 1
        if num_members == 1:
            raise SkipTest("Need a replica set shard to test.")
        coll = client_context.client.pymongo_test.get_collection(
            "test", write_concern=WriteConcern(w=num_members)
        )
        coll.drop()
        res = coll.insert_many([{} for _ in range(5)])
        first_id = res.inserted_ids[0]
        last_id = res.inserted_ids[-1]

        # Note - this isn't a perfect test since there's no way to
        # tell what shard member a query ran on.
        for pref in (Primary(), PrimaryPreferred(), Secondary(), SecondaryPreferred(), Nearest()):
            qcoll = coll.with_options(read_preference=pref)
            results = list(qcoll.find().sort([("_id", 1)]))
            self.assertEqual(first_id, results[0]["_id"])
            self.assertEqual(last_id, results[-1]["_id"])
            results = list(qcoll.find().sort([("_id", -1)]))
            self.assertEqual(first_id, results[-1]["_id"])
            self.assertEqual(last_id, results[0]["_id"])

    @client_context.require_mongos
    def test_mongos_max_staleness(self):
        # Sanity check that we're sending maxStalenessSeconds
        coll = client_context.client.pymongo_test.get_collection(
            "test", read_preference=SecondaryPreferred(max_staleness=120)
        )
        # No error
        coll.find_one()

        coll = client_context.client.pymongo_test.get_collection(
            "test", read_preference=SecondaryPreferred(max_staleness=10)
        )
        try:
            coll.find_one()
        except OperationFailure as exc:
            self.assertEqual(160, exc.code)
        else:
            self.fail("mongos accepted invalid staleness")

        coll = single_client(
            readPreference="secondaryPreferred", maxStalenessSeconds=120
        ).pymongo_test.test
        # No error
        coll.find_one()

        coll = single_client(
            readPreference="secondaryPreferred", maxStalenessSeconds=10
        ).pymongo_test.test
        try:
            coll.find_one()
        except OperationFailure as exc:
            self.assertEqual(160, exc.code)
        else:
            self.fail("mongos accepted invalid staleness")


if __name__ == "__main__":
    unittest.main()
