# Copyright 2015-present MongoDB, Inc.
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

"""Test the collection module."""
from __future__ import annotations

import sys

sys.path[0:0] = [""]

from test import unittest

from pymongo.operations import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)


class TestWriteOpsComparison(unittest.TestCase):
    def test_InsertOneEquals(self):
        self.assertEqual(InsertOne({"foo": 42}), InsertOne({"foo": 42}))

    def test_InsertOneNotEquals(self):
        self.assertNotEqual(InsertOne({"foo": 42}), InsertOne({"foo": 23}))

    def test_DeleteOneEquals(self):
        self.assertEqual(DeleteOne({"foo": 42}), DeleteOne({"foo": 42}))
        self.assertEqual(
            DeleteOne({"foo": 42}, {"locale": "en_US"}), DeleteOne({"foo": 42}, {"locale": "en_US"})
        )
        self.assertEqual(
            DeleteOne({"foo": 42}, {"locale": "en_US"}, {"hint": 1}),
            DeleteOne({"foo": 42}, {"locale": "en_US"}, {"hint": 1}),
        )

    def test_DeleteOneNotEquals(self):
        self.assertNotEqual(DeleteOne({"foo": 42}), DeleteOne({"foo": 23}))
        self.assertNotEqual(
            DeleteOne({"foo": 42}, {"locale": "en_US"}), DeleteOne({"foo": 42}, {"locale": "en_GB"})
        )
        self.assertNotEqual(
            DeleteOne({"foo": 42}, {"locale": "en_US"}, {"hint": 1}),
            DeleteOne({"foo": 42}, {"locale": "en_US"}, {"hint": 2}),
        )

    def test_DeleteManyEquals(self):
        self.assertEqual(DeleteMany({"foo": 42}), DeleteMany({"foo": 42}))
        self.assertEqual(
            DeleteMany({"foo": 42}, {"locale": "en_US"}),
            DeleteMany({"foo": 42}, {"locale": "en_US"}),
        )
        self.assertEqual(
            DeleteMany({"foo": 42}, {"locale": "en_US"}, {"hint": 1}),
            DeleteMany({"foo": 42}, {"locale": "en_US"}, {"hint": 1}),
        )

    def test_DeleteManyNotEquals(self):
        self.assertNotEqual(DeleteMany({"foo": 42}), DeleteMany({"foo": 23}))
        self.assertNotEqual(
            DeleteMany({"foo": 42}, {"locale": "en_US"}),
            DeleteMany({"foo": 42}, {"locale": "en_GB"}),
        )
        self.assertNotEqual(
            DeleteMany({"foo": 42}, {"locale": "en_US"}, {"hint": 1}),
            DeleteMany({"foo": 42}, {"locale": "en_US"}, {"hint": 2}),
        )

    def test_DeleteOneNotEqualsDeleteMany(self):
        self.assertNotEqual(DeleteOne({"foo": 42}), DeleteMany({"foo": 42}))

    def test_ReplaceOneEquals(self):
        self.assertEqual(
            ReplaceOne({"foo": 42}, {"bar": 42}, upsert=False),
            ReplaceOne({"foo": 42}, {"bar": 42}, upsert=False),
        )

    def test_ReplaceOneNotEquals(self):
        self.assertNotEqual(
            ReplaceOne({"foo": 42}, {"bar": 42}, upsert=False),
            ReplaceOne({"foo": 42}, {"bar": 42}, upsert=True),
        )

    def test_UpdateOneEquals(self):
        self.assertEqual(
            UpdateOne({"foo": 42}, {"$set": {"bar": 42}}),
            UpdateOne({"foo": 42}, {"$set": {"bar": 42}}),
        )

    def test_UpdateOneNotEquals(self):
        self.assertNotEqual(
            UpdateOne({"foo": 42}, {"$set": {"bar": 42}}),
            UpdateOne({"foo": 42}, {"$set": {"bar": 23}}),
        )

    def test_UpdateManyEquals(self):
        self.assertEqual(
            UpdateMany({"foo": 42}, {"$set": {"bar": 42}}),
            UpdateMany({"foo": 42}, {"$set": {"bar": 42}}),
        )

    def test_UpdateManyNotEquals(self):
        self.assertNotEqual(
            UpdateMany({"foo": 42}, {"$set": {"bar": 42}}),
            UpdateMany({"foo": 42}, {"$set": {"bar": 23}}),
        )

    def test_UpdateOneNotEqualsUpdateMany(self):
        self.assertNotEqual(
            UpdateOne({"foo": 42}, {"$set": {"bar": 42}}),
            UpdateMany({"foo": 42}, {"$set": {"bar": 42}}),
        )


if __name__ == "__main__":
    unittest.main()
