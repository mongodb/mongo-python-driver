# Copyright 2023-present MongoDB, Inc.
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

"""Test PyMongo cursor does not set exhaustAllowed automatically (PYTHON-4007)."""
from __future__ import annotations

import unittest

from mockupdb import MockupDB, OpMsg, going

from bson.objectid import ObjectId
from pymongo import MongoClient


class TestCursor(unittest.TestCase):
    def test_getmore_load_balanced(self):
        server = MockupDB()
        server.autoresponds(
            "hello",
            isWritablePrimary=True,
            msg="isdbgrid",
            minWireVersion=0,
            maxWireVersion=20,
            helloOk=True,
            serviceId=ObjectId(),
        )
        server.run()
        self.addCleanup(server.stop)

        client = MongoClient(server.uri, loadBalanced=True)
        self.addCleanup(client.close)
        collection = client.db.coll
        cursor = collection.find()
        with going(next, cursor):
            request = server.receives(OpMsg({"find": "coll"}))
            self.assertEqual(request.flags, 0, "exhaustAllowed should not be set")
            # Respond with a different namespace.
            request.reply({"cursor": {"id": 123, "firstBatch": [{}]}})

        # 3 batches, check exhaustAllowed on all getMores.
        for i in range(1, 3):
            with going(next, cursor):
                request = server.receives(OpMsg({"getMore": 123}))
                self.assertEqual(request.flags, 0, "exhaustAllowed should not be set")
                cursor_id = 123 if i < 2 else 0
                request.replies({"cursor": {"id": cursor_id, "nextBatch": [{}]}})


if __name__ == "__main__":
    unittest.main()
