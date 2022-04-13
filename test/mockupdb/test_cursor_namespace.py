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

"""Test list_indexes with more than one batch."""

import unittest

from mockupdb import MockupDB, going

from pymongo import MongoClient


class TestCursorNamespace(unittest.TestCase):
    server: MockupDB
    client: MongoClient

    @classmethod
    def setUpClass(cls):
        cls.server = MockupDB(auto_ismaster={"maxWireVersion": 6})
        cls.server.run()
        cls.client = MongoClient(cls.server.uri)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.server.stop()

    def _test_cursor_namespace(self, cursor_op, command):
        with going(cursor_op) as docs:
            request = self.server.receives(**{command: "collection", "namespace": "test"})
            # Respond with a different namespace.
            request.reply(
                {
                    "cursor": {
                        "firstBatch": [{"doc": 1}],
                        "id": 123,
                        "ns": "different_db.different.coll",
                    }
                }
            )
            # Client uses the namespace we returned.
            request = self.server.receives(
                getMore=123, namespace="different_db", collection="different.coll"
            )

            request.reply({"cursor": {"nextBatch": [{"doc": 2}], "id": 0}})

        self.assertEqual([{"doc": 1}, {"doc": 2}], docs())

    def test_aggregate_cursor(self):
        def op():
            return list(self.client.test.collection.aggregate([]))

        self._test_cursor_namespace(op, "aggregate")

    def test_find_cursor(self):
        def op():
            return list(self.client.test.collection.find())

        self._test_cursor_namespace(op, "find")

    def test_list_indexes(self):
        def op():
            return list(self.client.test.collection.list_indexes())

        self._test_cursor_namespace(op, "listIndexes")


class TestKillCursorsNamespace(unittest.TestCase):
    server: MockupDB
    client: MongoClient

    @classmethod
    def setUpClass(cls):
        cls.server = MockupDB(auto_ismaster={"maxWireVersion": 6})
        cls.server.run()
        cls.client = MongoClient(cls.server.uri)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.server.stop()

    def _test_killCursors_namespace(self, cursor_op, command):
        with going(cursor_op):
            request = self.server.receives(**{command: "collection", "namespace": "test"})
            # Respond with a different namespace.
            request.reply(
                {
                    "cursor": {
                        "firstBatch": [{"doc": 1}],
                        "id": 123,
                        "ns": "different_db.different.coll",
                    }
                }
            )
            # Client uses the namespace we returned for killCursors.
            request = self.server.receives(
                **{"killCursors": "different.coll", "cursors": [123], "$db": "different_db"}
            )
            request.reply(
                {
                    "ok": 1,
                    "cursorsKilled": [123],
                    "cursorsNotFound": [],
                    "cursorsAlive": [],
                    "cursorsUnknown": [],
                }
            )

    def test_aggregate_killCursor(self):
        def op():
            cursor = self.client.test.collection.aggregate([], batchSize=1)
            next(cursor)
            cursor.close()

        self._test_killCursors_namespace(op, "aggregate")

    def test_find_killCursor(self):
        def op():
            cursor = self.client.test.collection.find(batch_size=1)
            next(cursor)
            cursor.close()

        self._test_killCursors_namespace(op, "find")


if __name__ == "__main__":
    unittest.main()
