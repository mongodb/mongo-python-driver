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
from __future__ import annotations

import contextlib
import copy
import pickle
import random
import sys
from typing import Any

from pymongo.operations import _Op

sys.path[0:0] = [""]

from test.asynchronous import (
    AsyncIntegrationTest,
    SkipTest,
    async_client_context,
    connected,
    unittest,
)
from test.utils_shared import (
    OvertCommandListener,
    _ignore_deprecations,
    async_wait_until,
    one,
)
from test.version import Version

from bson.son import SON
from pymongo.asynchronous.helpers import anext
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.message import _maybe_add_read_preference
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

_IS_SYNC = False


class TestSelections(AsyncIntegrationTest):
    @async_client_context.require_connection
    async def test_bool(self):
        client = await self.async_single_client()

        async def predicate():
            return await client.address

        await async_wait_until(predicate, "discover primary")
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


class TestReadPreferencesBase(AsyncIntegrationTest):
    @async_client_context.require_secondaries_count(1)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        # Insert some data so we can use cursors in read_from_which_host
        await self.client.pymongo_test.test.drop()
        await self.client.get_database(
            "pymongo_test", write_concern=WriteConcern(w=async_client_context.w)
        ).test.insert_many([{"_id": i} for i in range(10)])

        self.addAsyncCleanup(self.client.pymongo_test.test.drop)

    async def read_from_which_host(self, client):
        """Do a find() on the client and return which host was used"""
        cursor = client.pymongo_test.test.find()
        await anext(cursor)
        return cursor.address

    async def read_from_which_kind(self, client):
        """Do a find() on the client and return 'primary' or 'secondary'
        depending on which the client used.
        """
        address = await self.read_from_which_host(client)
        if address == await client.primary:
            return "primary"
        elif address in await client.secondaries:
            return "secondary"
        else:
            self.fail(
                f"Cursor used address {address}, expected either primary "
                f"{client.primary} or secondaries {client.secondaries}"
            )

    async def assertReadsFrom(self, expected, **kwargs):
        c = await self.async_rs_client(**kwargs)

        async def predicate():
            return len(c.nodes - await c.arbiters) == async_client_context.w

        await async_wait_until(predicate, "discovered all nodes")

        used = await self.read_from_which_kind(c)
        self.assertEqual(expected, used, f"Cursor used {used}, expected {expected}")


class TestSingleSecondaryOk(TestReadPreferencesBase):
    async def test_reads_from_secondary(self):
        host, port = next(iter(await self.client.secondaries))
        # Direct connection to a secondary.
        client = await self.async_single_client(host, port)
        self.assertFalse(await client.is_primary)

        # Regardless of read preference, we should be able to do
        # "reads" with a direct connection to a secondary.
        # See server-selection.rst#topology-type-single.
        self.assertEqual(client.read_preference, ReadPreference.PRIMARY)

        db = client.pymongo_test
        coll = db.test

        # Test find and find_one.
        self.assertIsNotNone(await coll.find_one())
        self.assertEqual(10, len(await coll.find().to_list()))

        # Test some database helpers.
        self.assertIsNotNone(await db.list_collection_names())
        self.assertIsNotNone(await db.validate_collection("test"))
        self.assertIsNotNone(await db.command("ping"))

        # Test some collection helpers.
        self.assertEqual(10, await coll.count_documents({}))
        self.assertEqual(10, len(await coll.distinct("_id")))
        self.assertIsNotNone(await coll.aggregate([]))
        self.assertIsNotNone(await coll.index_information())


class TestReadPreferences(TestReadPreferencesBase):
    async def test_mode_validation(self):
        for mode in (
            ReadPreference.PRIMARY,
            ReadPreference.PRIMARY_PREFERRED,
            ReadPreference.SECONDARY,
            ReadPreference.SECONDARY_PREFERRED,
            ReadPreference.NEAREST,
        ):
            self.assertEqual(
                mode, (await self.async_rs_client(read_preference=mode)).read_preference
            )

        with self.assertRaises(TypeError):
            await self.async_rs_client(read_preference="foo")

    async def test_tag_sets_validation(self):
        S = Secondary(tag_sets=[{}])
        self.assertEqual(
            [{}], (await self.async_rs_client(read_preference=S)).read_preference.tag_sets
        )

        S = Secondary(tag_sets=[{"k": "v"}])
        self.assertEqual(
            [{"k": "v"}], (await self.async_rs_client(read_preference=S)).read_preference.tag_sets
        )

        S = Secondary(tag_sets=[{"k": "v"}, {}])
        self.assertEqual(
            [{"k": "v"}, {}],
            (await self.async_rs_client(read_preference=S)).read_preference.tag_sets,
        )

        self.assertRaises(ValueError, Secondary, tag_sets=[])

        # One dict not ok, must be a list of dicts
        self.assertRaises(TypeError, Secondary, tag_sets={"k": "v"})

        self.assertRaises(TypeError, Secondary, tag_sets="foo")

        self.assertRaises(TypeError, Secondary, tag_sets=["foo"])

    async def test_threshold_validation(self):
        self.assertEqual(
            17,
            (
                await self.async_rs_client(localThresholdMS=17, connect=False)
            ).options.local_threshold_ms,
        )

        self.assertEqual(
            42,
            (
                await self.async_rs_client(localThresholdMS=42, connect=False)
            ).options.local_threshold_ms,
        )

        self.assertEqual(
            666,
            (
                await self.async_rs_client(localThresholdMS=666, connect=False)
            ).options.local_threshold_ms,
        )

        self.assertEqual(
            0,
            (
                await self.async_rs_client(localThresholdMS=0, connect=False)
            ).options.local_threshold_ms,
        )

        with self.assertRaises(ValueError):
            await self.async_rs_client(localthresholdms=-1)

    async def test_zero_latency(self):
        ping_times: set = set()
        # Generate unique ping times.
        while len(ping_times) < len(self.client.nodes):
            ping_times.add(random.random())
        for ping_time, host in zip(ping_times, self.client.nodes):
            ServerDescription._host_to_round_trip_time[host] = ping_time
        try:
            client = await connected(
                await self.async_rs_client(readPreference="nearest", localThresholdMS=0)
            )
            await async_wait_until(
                lambda: client.nodes == self.client.nodes, "discovered all nodes"
            )
            host = await self.read_from_which_host(client)
            for _ in range(5):
                self.assertEqual(host, await self.read_from_which_host(client))
        finally:
            ServerDescription._host_to_round_trip_time.clear()

    async def test_primary(self):
        await self.assertReadsFrom("primary", read_preference=ReadPreference.PRIMARY)

    async def test_primary_with_tags(self):
        # Tags not allowed with PRIMARY
        with self.assertRaises(ConfigurationError):
            await self.async_rs_client(tag_sets=[{"dc": "ny"}])

    async def test_primary_preferred(self):
        await self.assertReadsFrom("primary", read_preference=ReadPreference.PRIMARY_PREFERRED)

    async def test_secondary(self):
        await self.assertReadsFrom("secondary", read_preference=ReadPreference.SECONDARY)

    async def test_secondary_preferred(self):
        await self.assertReadsFrom("secondary", read_preference=ReadPreference.SECONDARY_PREFERRED)

    async def test_nearest(self):
        # With high localThresholdMS, expect to read from any
        # member
        c = await self.async_rs_client(
            read_preference=ReadPreference.NEAREST, localThresholdMS=10000
        )  # 10 seconds

        data_members = {await self.client.primary} | await self.client.secondaries

        # This is a probabilistic test; track which members we've read from so
        # far, and keep reading until we've used all the members or give up.
        # Chance of using only 2 of 3 members 10k times if there's no bug =
        # 3 * (2/3)**10000, very low.
        used: set = set()
        i = 0
        while data_members.difference(used) and i < 10000:
            address = await self.read_from_which_host(c)
            used.add(address)
            i += 1

        not_used = data_members.difference(used)
        latencies = ", ".join(
            "%s: %sms" % (server.description.address, server.description.round_trip_time)
            for server in await (await c._get_topology()).select_servers(
                readable_server_selector, _Op.TEST
            )
        )

        self.assertFalse(
            not_used,
            "Expected to use primary and all secondaries for mode NEAREST,"
            f" but didn't use {not_used}\nlatencies: {latencies}",
        )


class ReadPrefTester(AsyncMongoClient):
    def __init__(self, *args, **kwargs):
        self.has_read_from = set()
        client_options = async_client_context.client_options
        client_options.update(kwargs)
        super().__init__(*args, **client_options)

    async def _conn_for_reads(self, read_preference, session, operation):
        context = await super()._conn_for_reads(read_preference, session, operation)
        return context

    @contextlib.asynccontextmanager
    async def _conn_from_server(self, read_preference, server, session):
        context = super()._conn_from_server(read_preference, server, session)
        async with context as (conn, read_preference):
            await self.record_a_read(conn.address)
            yield conn, read_preference

    async def record_a_read(self, address):
        server = await (await self._get_topology()).select_server_by_address(address, _Op.TEST, 0)
        self.has_read_from.add(server)


_PREF_MAP = [
    (Primary, SERVER_TYPE.RSPrimary),
    (PrimaryPreferred, SERVER_TYPE.RSPrimary),
    (Secondary, SERVER_TYPE.RSSecondary),
    (SecondaryPreferred, SERVER_TYPE.RSSecondary),
    (Nearest, "any"),
]


class TestCommandAndReadPreference(AsyncIntegrationTest):
    c: ReadPrefTester
    client_version: Version

    @async_client_context.require_secondaries_count(1)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.c = ReadPrefTester(
            # Ignore round trip times, to test ReadPreference modes only.
            localThresholdMS=1000 * 1000,
        )
        self.client_version = await Version.async_from_client(self.c)
        # mapReduce fails if the collection does not exist.
        coll = self.c.pymongo_test.get_collection(
            "test", write_concern=WriteConcern(w=async_client_context.w)
        )
        await coll.insert_one({})

    async def asyncTearDown(self):
        await self.c.drop_database("pymongo_test")
        await self.c.close()

    async def executed_on_which_server(self, client, fn, *args, **kwargs):
        """Execute fn(*args, **kwargs) and return the Server instance used."""
        client.has_read_from.clear()
        await fn(*args, **kwargs)
        self.assertEqual(1, len(client.has_read_from))
        return one(client.has_read_from)

    async def assertExecutedOn(self, server_type, client, fn, *args, **kwargs):
        server = await self.executed_on_which_server(client, fn, *args, **kwargs)
        self.assertEqual(
            SERVER_TYPE._fields[server_type], SERVER_TYPE._fields[server.description.server_type]
        )

    async def _test_fn(self, server_type, fn):
        for _ in range(10):
            if server_type == "any":
                used = set()
                for _ in range(1000):
                    server = await self.executed_on_which_server(self.c, fn)
                    used.add(server.description.address)
                    if len(used) == len(await self.c.secondaries) + 1:
                        # Success
                        break

                assert await self.c.primary is not None
                unused = (await self.c.secondaries).union({await self.c.primary}).difference(used)
                if unused:
                    self.fail("Some members not used for NEAREST: %s" % (unused))
            else:
                await self.assertExecutedOn(server_type, self.c, fn)

    async def _test_primary_helper(self, func):
        # Helpers that ignore read preference.
        await self._test_fn(SERVER_TYPE.RSPrimary, func)

    async def _test_coll_helper(self, secondary_ok, coll, meth, *args, **kwargs):
        for mode, server_type in _PREF_MAP:
            new_coll = coll.with_options(read_preference=mode())

            async def func():
                return await getattr(new_coll, meth)(*args, **kwargs)

            if secondary_ok:
                await self._test_fn(server_type, func)
            else:
                await self._test_fn(SERVER_TYPE.RSPrimary, func)

    async def test_command(self):
        # Test that the generic command helper obeys the read preference
        # passed to it.
        for mode, server_type in _PREF_MAP:

            async def func():
                return await self.c.pymongo_test.command("dbStats", read_preference=mode())

            await self._test_fn(server_type, func)

    async def test_create_collection(self):
        # create_collection runs listCollections on the primary to check if
        # the collection already exists.
        async def func():
            return await self.c.pymongo_test.create_collection(
                "some_collection%s" % random.randint(0, sys.maxsize)
            )

        await self._test_primary_helper(func)

    async def test_count_documents(self):
        await self._test_coll_helper(True, self.c.pymongo_test.test, "count_documents", {})

    async def test_estimated_document_count(self):
        await self._test_coll_helper(True, self.c.pymongo_test.test, "estimated_document_count")

    async def test_distinct(self):
        await self._test_coll_helper(True, self.c.pymongo_test.test, "distinct", "a")

    async def test_aggregate(self):
        await self._test_coll_helper(
            True, self.c.pymongo_test.test, "aggregate", [{"$project": {"_id": 1}}]
        )

    async def test_aggregate_write(self):
        # 5.0 servers support $out on secondaries.
        secondary_ok = async_client_context.version.at_least(5, 0)
        await self._test_coll_helper(
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


class TestMongosAndReadPreference(AsyncIntegrationTest):
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
            with _ignore_deprecations():
                pref = cls(hedge={})
                self.assertEqual(pref.document, {"mode": mode})
                out = _maybe_add_read_preference({}, pref)
                if cls == SecondaryPreferred:
                    # SecondaryPreferred without hedge doesn't add $readPreference.
                    self.assertEqual(out, {})
                else:
                    self.assertEqual(out, SON([("$query", {}), ("$readPreference", pref.document)]))

                hedge: dict[str, Any] = {"enabled": True}
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

    def test_read_preference_hedge_deprecated(self):
        cases = {
            "primaryPreferred": PrimaryPreferred,
            "secondary": Secondary,
            "secondaryPreferred": SecondaryPreferred,
            "nearest": Nearest,
        }
        for _, cls in cases.items():
            with self.assertRaises(DeprecationWarning):
                cls(hedge={"enabled": True})

    async def test_send_hedge(self):
        cases = {
            "primaryPreferred": PrimaryPreferred,
            "secondaryPreferred": SecondaryPreferred,
            "nearest": Nearest,
        }
        if await async_client_context.supports_secondary_read_pref:
            cases["secondary"] = Secondary
        listener = OvertCommandListener()
        client = await self.async_rs_client(event_listeners=[listener])
        await client.admin.command("ping")
        for _mode, cls in cases.items():
            with _ignore_deprecations():
                pref = cls(hedge={"enabled": True})
            coll = client.test.get_collection("test", read_preference=pref)
            listener.reset()
            await coll.find_one()
            started = listener.started_events
            self.assertEqual(len(started), 1, started)
            cmd = started[0].command
            if async_client_context.is_rs or async_client_context.is_mongos:
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

    @async_client_context.require_mongos
    async def test_mongos(self):
        res = await async_client_context.client.config.shards.find_one()
        assert res is not None
        shard = res["host"]
        num_members = shard.count(",") + 1
        if num_members == 1:
            raise SkipTest("Need a replica set shard to test.")
        coll = async_client_context.client.pymongo_test.get_collection(
            "test", write_concern=WriteConcern(w=num_members)
        )
        await coll.drop()
        res = await coll.insert_many([{} for _ in range(5)])
        first_id = res.inserted_ids[0]
        last_id = res.inserted_ids[-1]

        # Note - this isn't a perfect test since there's no way to
        # tell what shard member a query ran on.
        for pref in (Primary(), PrimaryPreferred(), Secondary(), SecondaryPreferred(), Nearest()):
            qcoll = coll.with_options(read_preference=pref)
            results = await qcoll.find().sort([("_id", 1)]).to_list()
            self.assertEqual(first_id, results[0]["_id"])
            self.assertEqual(last_id, results[-1]["_id"])
            results = await qcoll.find().sort([("_id", -1)]).to_list()
            self.assertEqual(first_id, results[-1]["_id"])
            self.assertEqual(last_id, results[0]["_id"])

    @async_client_context.require_mongos
    async def test_mongos_max_staleness(self):
        # Sanity check that we're sending maxStalenessSeconds
        coll = async_client_context.client.pymongo_test.get_collection(
            "test", read_preference=SecondaryPreferred(max_staleness=120)
        )
        # No error
        await coll.find_one()

        coll = async_client_context.client.pymongo_test.get_collection(
            "test", read_preference=SecondaryPreferred(max_staleness=10)
        )
        try:
            await coll.find_one()
        except OperationFailure as exc:
            self.assertEqual(160, exc.code)
        else:
            self.fail("mongos accepted invalid staleness")

        coll = (
            await self.async_single_client(
                readPreference="secondaryPreferred", maxStalenessSeconds=120
            )
        ).pymongo_test.test
        # No error
        await coll.find_one()

        coll = (
            await self.async_single_client(
                readPreference="secondaryPreferred", maxStalenessSeconds=10
            )
        ).pymongo_test.test
        try:
            await coll.find_one()
        except OperationFailure as exc:
            self.assertEqual(160, exc.code)
        else:
            self.fail("mongos accepted invalid staleness")


if __name__ == "__main__":
    unittest.main()
