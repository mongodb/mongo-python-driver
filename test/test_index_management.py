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

"""Run the auth spec tests."""
from __future__ import annotations

import os
import sys
import time
import uuid
from typing import Any, Mapping

sys.path[0:0] = [""]

from test import IntegrationTest, unittest
from test.unified_format import generate_test_classes

from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pymongo.operations import SearchIndexModel

_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "index_management")

_NAME = "test-search-index"


class TestCreateSearchIndex(IntegrationTest):
    def test_inputs(self):
        if not os.environ.get("TEST_INDEX_MANAGEMENT"):
            raise unittest.SkipTest("Skipping index management tests")
        client = MongoClient()
        self.addCleanup(client.close)
        coll = client.test.test
        coll.drop()
        definition = dict(mappings=dict(dynamic=True))
        model_kwarg_list: list[Mapping[str, Any]] = [
            dict(definition=definition, name=None),
            dict(definition=definition, name="test"),
        ]
        for model_kwargs in model_kwarg_list:
            model = SearchIndexModel(**model_kwargs)
            with self.assertRaises(OperationFailure):
                coll.create_search_index(model)
            with self.assertRaises(OperationFailure):
                coll.create_search_index(model_kwargs)


class TestSearchIndexProse(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if not os.environ.get("TEST_INDEX_MANAGEMENT"):
            raise unittest.SkipTest("Skipping index management tests")
        url = os.environ.get("MONGODB_URI")
        username = os.environ["DB_USER"]
        password = os.environ["DB_PASSWORD"]
        cls.client = MongoClient(url, username=username, password=password)
        cls.client.drop_database(_NAME)
        cls.db = cls.client.test_search_index_prose

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(_NAME)
        cls.client.close()

    def wait_for_ready(self, coll, name=_NAME, predicate=None):
        """Wait for a search index to be ready."""
        indices: list[Mapping[str, Any]] = []
        if predicate is None:
            predicate = lambda index: index.get("queryable") is True

        while True:
            indices = list(coll.list_search_indexes(name))
            if len(indices) and predicate(indices[0]):
                return indices[0]
                break
            time.sleep(5)

    def test_case_1(self):
        """Driver can successfully create and list search indexes."""

        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]

        # Create a new search index on ``coll0`` with the ``createSearchIndex`` helper.  Use the following definition:
        model = {"name": _NAME, "definition": {"mappings": {"dynamic": False}}}
        coll0.insert_one({})
        resp = coll0.create_search_index(model)

        # Assert that the command returns the name of the index: ``"test-search-index"``.
        self.assertEqual(resp, _NAME)

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied and store the value in a variable ``index``:
        # An index with the ``name`` of ``test-search-index`` is present and the index has a field ``queryable`` with a value of ``true``.
        index = self.wait_for_ready(coll0)

        # . Assert that ``index`` has a property ``latestDefinition`` whose value is ``{ 'mappings': { 'dynamic': false } }``
        self.assertIn("latestDefinition", index)
        self.assertEqual(index["latestDefinition"], model["definition"])

    def test_case_2(self):
        """Driver can successfully create multiple indexes in batch."""

        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]
        coll0.insert_one({})

        # Create two new search indexes on ``coll0`` with the ``createSearchIndexes`` helper.
        name1 = "test-search-index-1"
        name2 = "test-search-index-2"
        definition = {"mappings": {"dynamic": False}}
        index_definitions: list[dict[str, Any]] = [
            {"name": name1, "definition": definition},
            {"name": name2, "definition": definition},
        ]
        coll0.create_search_indexes(
            [SearchIndexModel(i["definition"], i["name"]) for i in index_definitions]
        )

        # .Assert that the command returns an array containing the new indexes' names: ``["test-search-index-1", "test-search-index-2"]``.
        indices = list(coll0.list_search_indexes())
        names = [i["name"] for i in indices]
        self.assertIn(name1, names)
        self.assertIn(name2, names)

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied.
        # An index with the ``name`` of ``test-search-index-1`` is present and index has a field ``queryable`` with the value of ``true``. Store result in ``index1``.
        # An index with the ``name`` of ``test-search-index-2`` is present and index has a field ``queryable`` with the value of ``true``. Store result in ``index2``.
        index1 = self.wait_for_ready(coll0, name1)
        index2 = self.wait_for_ready(coll0, name2)

        # Assert that ``index1`` and ``index2`` have the property ``latestDefinition`` whose value is ``{ "mappings" : { "dynamic" : false } }``
        for index in [index1, index2]:
            self.assertIn("latestDefinition", index)
            self.assertEqual(index["latestDefinition"], definition)

    def test_case_3(self):
        """Driver can successfully drop search indexes."""

        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]
        coll0.insert_one({})

        # Create a new search index on ``coll0``.
        model = {"name": _NAME, "definition": {"mappings": {"dynamic": False}}}
        resp = coll0.create_search_index(model)

        # Assert that the command returns the name of the index: ``"test-search-index"``.
        self.assertEqual(resp, "test-search-index")

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied:
        #   An index with the ``name`` of ``test-search-index`` is present and index has a field ``queryable`` with the value of ``true``.
        self.wait_for_ready(coll0)

        # Run a ``dropSearchIndex`` on ``coll0``, using ``test-search-index`` for the name.
        coll0.drop_search_index(_NAME)

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until ``listSearchIndexes`` returns an empty array.
        t0 = time.time()
        while True:
            indices = list(coll0.list_search_indexes())
            if indices:
                break
            if (time.time() - t0) / 60 > 5:
                raise TimeoutError("Timed out waiting for index deletion")
            time.sleep(5)

    def test_case_4(self):
        """Driver can update a search index."""
        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]
        coll0.insert_one({})

        # Create a new search index on ``coll0``.
        model = {"name": _NAME, "definition": {"mappings": {"dynamic": False}}}
        resp = coll0.create_search_index(model)

        # Assert that the command returns the name of the index: ``"test-search-index"``.
        self.assertEqual(resp, _NAME)

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied:
        #  An index with the ``name`` of ``test-search-index`` is present and index has a field ``queryable`` with the value of ``true``.
        self.wait_for_ready(coll0)

        # Run a ``updateSearchIndex`` on ``coll0``.
        # Assert that the command does not error and the server responds with a success.
        model2: dict[str, Any] = {"name": _NAME, "definition": {"mappings": {"dynamic": True}}}
        coll0.update_search_index(_NAME, model2["definition"])

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied:
        #   An index with the ``name`` of ``test-search-index`` is present.  This index is referred to as ``index``.
        # The index has a field ``queryable`` with a value of ``true`` and has a field ``status`` with the value of ``READY``.
        predicate = lambda index: index.get("queryable") is True and index.get("status") == "READY"
        self.wait_for_ready(coll0, predicate=predicate)

        # Assert that an index is present with the name ``test-search-index`` and the definition has a property ``latestDefinition`` whose value is ``{ 'mappings': { 'dynamic': true } }``.
        index = list(coll0.list_search_indexes(_NAME))[0]
        self.assertIn("latestDefinition", index)
        self.assertEqual(index["latestDefinition"], model2["definition"])

    def test_case_5(self):
        """``dropSearchIndex`` suppresses namespace not found errors."""
        # Create a driver-side collection object for a randomly generated collection name.  Do not create this collection on the server.
        coll0 = self.db[f"col{uuid.uuid4()}"]

        # Run a ``dropSearchIndex`` command and assert that no error is thrown.
        coll0.drop_search_index("foo")


if os.environ.get("TEST_INDEX_MANAGEMENT"):
    globals().update(
        generate_test_classes(
            _TEST_PATH,
            module=__name__,
        )
    )
else:

    class TestIndexManagementUnifiedTests(unittest.TestCase):
        @classmethod
        def setUpClass(cls) -> None:
            raise unittest.SkipTest("Skipping index management pending PYTHON-3951")

        def test_placeholder(self):
            pass


if __name__ == "__main__":
    unittest.main()
