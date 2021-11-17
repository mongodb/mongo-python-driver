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

from mockupdb import going, MockupDB, OpGetMore, OpQuery, Command
from pymongo import MongoClient

import unittest


class TestStartingFromOverflow(unittest.TestCase):
    def test_query(self):
        server = MockupDB(auto_ismaster=True,
                          min_wire_version=0, max_wire_version=3)
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri)
        cursor = client.test.collection.find()
        with going(list, cursor) as docs:
            request = server.receives(OpQuery)
            request.reply({'a': 1}, cursor_id=123, starting_from=-7)
            request = server.receives(OpGetMore, cursor_id=123)
            request.reply({'a': 2}, starting_from=-3, cursor_id=0)

        self.assertEqual([{'a': 1}, {'a': 2}], docs())

    def test_aggregate(self):
        server = MockupDB(auto_ismaster={'maxWireVersion': 3})
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri)
        with going(client.test.collection.aggregate, []) as cursor:
            request = server.receives(Command)
            request.reply({'cursor': {
                'id': 123,
                'firstBatch': [{'a': 1}]}})

        with going(list, cursor()) as docs:
            request = server.receives(OpGetMore, cursor_id=123)
            request.reply({'a': 2}, starting_from=-3, cursor_id=0)

        self.assertEqual([{'a': 1}, {'a': 2}], docs())

    def test_find_command(self):
        server = MockupDB(auto_ismaster={'maxWireVersion': 4})
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri)
        with going(list, client.test.collection.find()) as docs:
            server.receives(Command).reply({'cursor': {
                'id': 123,
                'firstBatch': [{'a': 1}]}})

            request = server.receives(Command("getMore", 123))
            request.reply({'cursor': {
                'id': 0,
                'nextBatch': [{'a': 2}]}},
                starting_from=-3)

        self.assertEqual([{'a': 1}, {'a': 2}], docs())


if __name__ == '__main__':
    unittest.main()
