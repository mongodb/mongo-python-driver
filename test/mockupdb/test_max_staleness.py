# Copyright 2016 MongoDB, Inc.
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

import unittest

from mockupdb import MockupDB, going

from pymongo import MongoClient


class TestMaxStalenessMongos(unittest.TestCase):
    def test_mongos(self):
        mongos = MockupDB()
        mongos.autoresponds("ismaster", maxWireVersion=6, ismaster=True, msg="isdbgrid")
        mongos.run()
        self.addCleanup(mongos.stop)

        # No maxStalenessSeconds.
        uri = "mongodb://localhost:%d/?readPreference=secondary" % mongos.port

        client = MongoClient(uri)
        self.addCleanup(client.close)
        with going(client.db.coll.find_one) as future:
            request = mongos.receives()
            self.assertNotIn("maxStalenessSeconds", request.doc["$readPreference"])

            self.assertTrue(request.slave_okay)
            request.ok(cursor={"firstBatch": [], "id": 0})

        # find_one succeeds with no result.
        self.assertIsNone(future())

        # Set maxStalenessSeconds to 1. Client has no minimum with mongos,
        # we let mongos enforce the 90-second minimum and return an error:
        # SERVER-27146.
        uri = (
            "mongodb://localhost:%d/?readPreference=secondary"
            "&maxStalenessSeconds=1" % mongos.port
        )

        client = MongoClient(uri)
        self.addCleanup(client.close)
        with going(client.db.coll.find_one) as future:
            request = mongos.receives()
            self.assertEqual(1, request.doc["$readPreference"]["maxStalenessSeconds"])

            self.assertTrue(request.slave_okay)
            request.ok(cursor={"firstBatch": [], "id": 0})

        self.assertIsNone(future())


if __name__ == "__main__":
    unittest.main()
