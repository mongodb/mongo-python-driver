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

import unittest

from mockupdb import MockupDB, going

from bson import Timestamp
from pymongo import DeleteMany, InsertOne, MongoClient, UpdateOne


class TestClusterTime(unittest.TestCase):
    def cluster_time_conversation(self, callback, replies):
        cluster_time = Timestamp(0, 0)
        server = MockupDB()

        # First test all commands include $clusterTime with wire version 6.
        _ = server.autoresponds(
            "ismaster",
            {
                "minWireVersion": 0,
                "maxWireVersion": 6,
                "$clusterTime": {"clusterTime": cluster_time},
            },
        )

        server.run()
        self.addCleanup(server.stop)

        client = MongoClient(server.uri)
        self.addCleanup(client.close)

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
        def callback(client):
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
            "maxWireVersion": 6,
            "$clusterTime": {"clusterTime": cluster_time},
        }

        server = MockupDB()
        server.run()
        self.addCleanup(server.stop)

        client = MongoClient(server.uri, heartbeatFrequencyMS=500)
        self.addCleanup(client.close)

        request = server.receives("ismaster")
        # No $clusterTime in first ismaster, only in subsequent ones
        self.assertNotIn("$clusterTime", request)
        request.ok(reply)

        # Next exchange: client returns first clusterTime, we send the second.
        request = server.receives("ismaster")
        self.assertIn("$clusterTime", request)
        self.assertEqual(request["$clusterTime"]["clusterTime"], cluster_time)
        cluster_time = Timestamp(cluster_time.time, cluster_time.inc + 1)
        reply["$clusterTime"] = {"clusterTime": cluster_time}
        request.reply(reply)

        # Third exchange: client returns second clusterTime.
        request = server.receives("ismaster")
        self.assertEqual(request["$clusterTime"]["clusterTime"], cluster_time)

        # Return command error with a new clusterTime.
        cluster_time = Timestamp(cluster_time.time, cluster_time.inc + 1)
        error = {
            "ok": 0,
            "code": 211,
            "errmsg": "Cache Reader No keys found for HMAC ...",
            "$clusterTime": {"clusterTime": cluster_time},
        }
        request.reply(error)

        # PyMongo 3.11+ closes the monitoring connection on command errors.

        # Fourth exchange: the Monitor closes the connection and runs the
        # handshake on a new connection.
        request = server.receives("ismaster")
        # No $clusterTime in first ismaster, only in subsequent ones
        self.assertNotIn("$clusterTime", request)

        # Reply without $clusterTime.
        reply.pop("$clusterTime")
        request.reply(reply)

        # Fifth exchange: the Monitor attempt uses the clusterTime from
        # the previous isMaster error.
        request = server.receives("ismaster")
        self.assertEqual(request["$clusterTime"]["clusterTime"], cluster_time)
        request.reply(reply)
        client.close()


if __name__ == "__main__":
    unittest.main()
