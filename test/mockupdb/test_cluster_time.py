# Copyright 2017-present MongoDB, Inc.
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

"""Test $clusterTime handling."""
from __future__ import annotations

import unittest
from test import PyMongoTestCase

import pytest

try:
    from mockupdb import MockupDB, going

    _HAVE_MOCKUPDB = True
except ImportError:
    _HAVE_MOCKUPDB = False


from bson import Timestamp
from pymongo import DeleteMany, InsertOne, MongoClient, UpdateOne
from pymongo.common import MIN_SUPPORTED_WIRE_VERSION
from pymongo.errors import OperationFailure

pytestmark = pytest.mark.mockupdb


class TestClusterTime(PyMongoTestCase):
    def cluster_time_conversation(
        self, callback, replies, max_wire_version=MIN_SUPPORTED_WIRE_VERSION
    ):
        cluster_time = Timestamp(0, 0)
        server = MockupDB()

        # First test all commands include $clusterTime with max_wire_version.
        _ = server.autoresponds(
            "ismaster",
            {
                "minWireVersion": 0,
                "maxWireVersion": max_wire_version,
                "$clusterTime": {"clusterTime": cluster_time},
            },
        )

        server.run()
        self.addCleanup(server.stop)

        client = self.simple_client(server.uri)

        with going(callback, client):
            for reply in replies:
                request = server.receives()
                self.assertIn("$clusterTime", request)
                self.assertEqual(request["$clusterTime"]["clusterTime"], cluster_time)
                cluster_time = Timestamp(cluster_time.time, cluster_time.inc + 1)
                reply["$clusterTime"] = {"clusterTime": cluster_time}
                request.reply(reply)

    def test_command(self):
        def callback(client):
            client.db.command("ping")
            client.db.command("ping")

        self.cluster_time_conversation(callback, [{"ok": 1}] * 2)

    def test_bulk(self):
        def callback(client: MongoClient[dict]) -> None:
            client.db.collection.bulk_write(
                [InsertOne({}), InsertOne({}), UpdateOne({}, {"$inc": {"x": 1}}), DeleteMany({})]
            )

        self.cluster_time_conversation(
            callback,
            [{"ok": 1, "nInserted": 2}, {"ok": 1, "nModified": 1}, {"ok": 1, "nDeleted": 2}],
        )

    batches = [
        {"cursor": {"id": 123, "firstBatch": [{"a": 1}]}},
        {"cursor": {"id": 123, "nextBatch": [{"a": 2}]}},
        {"cursor": {"id": 0, "nextBatch": [{"a": 3}]}},
    ]

    def test_cursor(self):
        def callback(client):
            list(client.db.collection.find())

        self.cluster_time_conversation(callback, self.batches)

    def test_aggregate(self):
        def callback(client):
            list(client.db.collection.aggregate([]))

        self.cluster_time_conversation(callback, self.batches)

    def test_explain(self):
        def callback(client):
            client.db.collection.find().explain()

        self.cluster_time_conversation(callback, [{"ok": 1}])

    def test_monitor(self):
        cluster_time = Timestamp(0, 0)
        reply = {
            "minWireVersion": 0,
            "maxWireVersion": MIN_SUPPORTED_WIRE_VERSION,
            "$clusterTime": {"clusterTime": cluster_time},
        }

        server = MockupDB()
        server.run()
        self.addCleanup(server.stop)

        client = self.simple_client(server.uri, heartbeatFrequencyMS=500)

        for _ in range(3):
            request = server.receives("ismaster")
            # No $clusterTime in heartbeats or handshakes.
            self.assertNotIn("$clusterTime", request)
            request.ok(reply)
        client.close()

    def test_collection_bulk_error(self):
        def callback(client: MongoClient[dict]) -> None:
            with self.assertRaises(OperationFailure):
                client.db.collection.bulk_write([InsertOne({}), InsertOne({})])

        self.cluster_time_conversation(
            callback,
            [{"ok": 0, "errmsg": "mock error"}],
        )

    def test_client_bulk_error(self):
        def callback(client: MongoClient[dict]) -> None:
            with self.assertRaises(OperationFailure):
                client.bulk_write(
                    [
                        InsertOne({}, namespace="db.collection"),
                        InsertOne({}, namespace="db.collection"),
                    ]
                )

        self.cluster_time_conversation(
            callback, [{"ok": 0, "errmsg": "mock error"}], max_wire_version=25
        )


if __name__ == "__main__":
    unittest.main()
