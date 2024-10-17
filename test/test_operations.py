# Copyright 2024-present MongoDB, Inc.
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

"""Test the operations module."""
from __future__ import annotations

from test import UnitTest, unittest

from pymongo import ASCENDING, DESCENDING
from pymongo.collation import Collation
from pymongo.errors import OperationFailure
from pymongo.operations import IndexModel, SearchIndexModel


class TestOperationsBase(UnitTest):
    """Base class for testing operations module."""

    def assertRepr(self, obj):
        new_obj = eval(repr(obj))
        self.assertEqual(type(new_obj), type(obj))
        self.assertEqual(repr(new_obj), repr(obj))


class TestIndexModel(TestOperationsBase):
    """Test IndexModel features."""

    def test_repr(self):
        # Based on examples in test_collection.py
        self.assertRepr(IndexModel("hello"))
        self.assertRepr(IndexModel([("hello", DESCENDING), ("world", ASCENDING)]))
        self.assertRepr(
            IndexModel([("hello", DESCENDING), ("world", ASCENDING)], name="hello_world")
        )
        # Test all the kwargs
        self.assertRepr(IndexModel("name", name="name"))
        self.assertRepr(IndexModel("unique", unique=False))
        self.assertRepr(IndexModel("background", background=True))
        self.assertRepr(IndexModel("sparse", sparse=True))
        self.assertRepr(IndexModel("bucketSize", bucketSize=1))
        self.assertRepr(IndexModel("min", min=1))
        self.assertRepr(IndexModel("max", max=1))
        self.assertRepr(IndexModel("expireAfterSeconds", expireAfterSeconds=1))
        self.assertRepr(
            IndexModel("partialFilterExpression", partialFilterExpression={"hello": "world"})
        )
        self.assertRepr(IndexModel("collation", collation=Collation(locale="en_US")))
        self.assertRepr(IndexModel("wildcardProjection", wildcardProjection={"$**": 1}))
        self.assertRepr(IndexModel("hidden", hidden=False))
        # Test string literal
        self.assertEqual(repr(IndexModel("hello")), "IndexModel({'hello': 1}, name='hello_1')")
        self.assertEqual(
            repr(IndexModel({"hello": 1, "world": -1})),
            "IndexModel({'hello': 1, 'world': -1}, name='hello_1_world_-1')",
        )


class TestSearchIndexModel(TestOperationsBase):
    """Test SearchIndexModel features."""

    def test_repr(self):
        self.assertRepr(SearchIndexModel({"hello": "hello"}, key=1))
        self.assertEqual(
            repr(SearchIndexModel({"hello": "hello"}, key=1)),
            "SearchIndexModel(definition={'hello': 'hello'}, key=1)",
        )


if __name__ == "__main__":
    unittest.main()
