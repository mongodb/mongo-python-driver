# Copyright 2020-present MongoDB, Inc.
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

"""Test that each file in mypy_fails/ actually fails mypy, and test some
sample client code that uses PyMongo typings."""

import os
import sys
import unittest
from typing import Any, Dict, Iterable, List

try:
    from mypy import api
except ImportError:
    api = None

from bson.son import SON
from pymongo.collection import Collection
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.mongo_client import MongoClient
from pymongo.operations import InsertOne

TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "mypy_fails")


def get_tests() -> Iterable[str]:
    for dirpath, _, filenames in os.walk(TEST_PATH):
        for filename in filenames:
            yield os.path.join(dirpath, filename)


class TestMypyFails(unittest.TestCase):
    def ensure_mypy_fails(self, filename: str) -> None:
        if api is None:
            raise unittest.SkipTest("Mypy is not installed")
        stdout, stderr, exit_status = api.run([filename])
        self.assertTrue(exit_status, msg=stdout)

    def test_mypy_failures(self) -> None:
        for filename in get_tests():
            with self.subTest(filename=filename):
                self.ensure_mypy_fails(filename)


class TestPymongo(unittest.TestCase):
    client: MongoClient
    coll: Collection

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = MongoClient(serverSelectionTimeoutMS=250, directConnection=False)
        cls.coll = cls.client.test.test
        try:
            cls.client.admin.command("ping")
        except ServerSelectionTimeoutError as exc:
            raise unittest.SkipTest(f"Could not connect to MongoDB: {exc}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()

    def test_insert_find(self) -> None:
        doc = {"my": "doc"}
        coll2 = self.client.test.test2
        result = self.coll.insert_one(doc)
        self.assertEqual(result.inserted_id, doc["_id"])
        retreived = self.coll.find_one({"_id": doc["_id"]})
        if retreived:
            # Documents returned from find are mutable.
            retreived["new_field"] = 1
            result2 = coll2.insert_one(retreived)
            self.assertEqual(result2.inserted_id, result.inserted_id)

    def test_cursor_iterable(self) -> None:
        def to_list(iterable: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
            return list(iterable)

        self.coll.insert_one({})
        cursor = self.coll.find()
        docs = to_list(cursor)
        self.assertTrue(docs)

    def test_bulk_write(self) -> None:
        self.coll.insert_one({})
        requests = [InsertOne({})]
        result = self.coll.bulk_write(requests)
        self.assertTrue(result.acknowledged)

    def test_aggregate_pipeline(self) -> None:
        coll3 = self.client.test.test3
        coll3.insert_many(
            [
                {"x": 1, "tags": ["dog", "cat"]},
                {"x": 2, "tags": ["cat"]},
                {"x": 2, "tags": ["mouse", "cat", "dog"]},
                {"x": 3, "tags": []},
            ]
        )

        class mydict(Dict[str, Any]):
            pass

        result = coll3.aggregate(
            [
                mydict({"$unwind": "$tags"}),
                {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
                {"$sort": SON([("count", -1), ("_id", -1)])},
            ]
        )
        self.assertTrue(len(list(result)))


if __name__ == "__main__":
    unittest.main()
