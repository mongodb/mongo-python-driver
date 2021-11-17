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

"""Test that PyMongo ignores the startingFrom field, PYTHON-945."""

from mockupdb import going, MockupDB, OpGetMore, Command, OpMsg, go
from pymongo import MongoClient

import unittest


class TestStartingFromOverflow(unittest.TestCase):
    def test_query(self):
        server = MockupDB(auto_ismaster=True,
                          min_wire_version=0, max_wire_version=6)
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri)
        cursor = client.test.collection.find()
        docs = go(list, cursor)
        request = server.receives(OpMsg({"find": "collection", "filter": {}}))
        request.replies(cursor={"id": 123, "firstBatch":[{
            "a":1}]})
        request = server.receives(OpMsg({"getMore":123}))
        request.replies(cursor={"id": 0, "nextBatch":[{
            "a":2}]})
        self.assertEqual([{'a': 1}, {'a': 2}], docs())

    def test_aggregate(self):
        server = MockupDB(auto_ismaster={'maxWireVersion': 6})
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri)
        cursor = go(client.test.collection.aggregate, [])
        request = server.receives(OpMsg({}))
        request.replies({'cursor': {
            'id': 123,
            'firstBatch': [{'a': 1}]}})

        docs = go(list, cursor())
        request = server.receives(OpMsg({"getMore": 123}))
        request.replies(cursor={"id": 0, "nextBatch":[{
            "a":2}]})

        self.assertEqual([{'a': 1}, {'a': 2}], docs())


if __name__ == '__main__':
    unittest.main()
