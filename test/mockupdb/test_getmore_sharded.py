# Copyright 2015 MongoDB, Inc.
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

"""Test PyMongo cursor with a sharded cluster."""
import unittest
from queue import Queue

from mockupdb import MockupDB, going

from pymongo import MongoClient


class TestGetmoreSharded(unittest.TestCase):
    def test_getmore_sharded(self):
        servers = [MockupDB(), MockupDB()]

        # Collect queries to either server in one queue.
        q: Queue = Queue()
        for server in servers:
            server.subscribe(q.put)
            server.autoresponds(
                "ismaster", ismaster=True, msg="isdbgrid", minWireVersion=2, maxWireVersion=6
            )
            server.run()
            self.addCleanup(server.stop)

        client = MongoClient(
            "mongodb://%s:%d,%s:%d"
            % (servers[0].host, servers[0].port, servers[1].host, servers[1].port)
        )
        self.addCleanup(client.close)
        collection = client.db.collection
        cursor = collection.find()
        with going(next, cursor):
            query = q.get(timeout=1)
            query.replies({"cursor": {"id": 123, "firstBatch": [{}]}})

        # 10 batches, all getMores go to same server.
        for i in range(1, 10):
            with going(next, cursor):
                getmore = q.get(timeout=1)
                self.assertEqual(query.server, getmore.server)
                cursor_id = 123 if i < 9 else 0
                getmore.replies({"cursor": {"id": cursor_id, "nextBatch": [{}]}})


if __name__ == "__main__":
    unittest.main()
