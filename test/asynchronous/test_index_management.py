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

import asyncio
import os
import pathlib
import sys
import time
import uuid
from typing import Any, Mapping

import pytest

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, AsyncPyMongoTestCase, unittest
from test.asynchronous.unified_format import generate_test_classes, get_test_path
from test.utils_shared import AllowListEventListener, OvertCommandListener

from pymongo.errors import OperationFailure
from pymongo.operations import SearchIndexModel
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

_IS_SYNC = False

pytestmark = pytest.mark.search_index

_NAME = "test-search-index"


class TestCreateSearchIndex(AsyncIntegrationTest):
    async def test_inputs(self):
        listener = AllowListEventListener("createSearchIndexes")
        client = self.simple_client(event_listeners=[listener])
        coll = client.test.test
        await coll.drop()
        definition = dict(mappings=dict(dynamic=True))
        model_kwarg_list: list[Mapping[str, Any]] = [
            dict(definition=definition, name=None),
            dict(definition=definition, name="test"),
        ]
        for model_kwargs in model_kwarg_list:
            model = SearchIndexModel(**model_kwargs)
            with self.assertRaises(OperationFailure):
                await coll.create_search_index(model)
            with self.assertRaises(OperationFailure):
                await coll.create_search_index(model_kwargs)

        listener.reset()
        with self.assertRaises(OperationFailure):
            await coll.create_search_index({"definition": definition, "arbitraryOption": 1})
        self.assertEqual(
            {"definition": definition, "arbitraryOption": 1},
            listener.events[0].command["indexes"][0],
        )

        listener.reset()
        with self.assertRaises(OperationFailure):
            await coll.create_search_index({"definition": definition, "type": "search"})
        self.assertEqual(
            {"definition": definition, "type": "search"}, listener.events[0].command["indexes"][0]
        )


class SearchIndexIntegrationBase(AsyncPyMongoTestCase):
    db_name = "test_search_index_base"

    @classmethod
    def setUpClass(cls) -> None:
        cls.url = os.environ.get("MONGODB_URI")
        cls.username = os.environ["DB_USER"]
        cls.password = os.environ["DB_PASSWORD"]
        cls.listener = OvertCommandListener()

    async def asyncSetUp(self) -> None:
        self.client = self.simple_client(
            self.url,
            username=self.username,
            password=self.password,
            event_listeners=[self.listener],
        )
        await self.client.drop_database(_NAME)
        self.db = self.client[self.db_name]

    async def asyncTearDown(self):
        await self.client.drop_database(_NAME)

    async def wait_for_ready(self, coll, name=_NAME, predicate=None):
        """Wait for a search index to be ready."""
        indices: list[Mapping[str, Any]] = []
        if predicate is None:
            predicate = lambda index: index.get("queryable") is True

        while True:
            indices = await (await coll.list_search_indexes(name)).to_list()
            if len(indices) and predicate(indices[0]):
                return indices[0]
            await asyncio.sleep(5)


class TestSearchIndexIntegration(SearchIndexIntegrationBase):
    db_name = "test_search_index"

    async def test_comment_field(self):
        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]
        await coll0.insert_one({})

        # Create a new search index on ``coll0`` that implicitly passes its type.
        search_definition = {"mappings": {"dynamic": False}}
        self.listener.reset()
        implicit_search_resp = await coll0.create_search_index(
            model={"name": _NAME + "-implicit", "definition": search_definition}, comment="foo"
        )
        event = self.listener.events[0]
        self.assertEqual(event.command["comment"], "foo")

        # Get the index definition.
        self.listener.reset()
        await (await coll0.list_search_indexes(name=implicit_search_resp, comment="foo")).next()
        event = self.listener.events[0]
        self.assertEqual(event.command["comment"], "foo")


class TestSearchIndexProse(SearchIndexIntegrationBase):
    db_name = "test_search_index_prose"

    async def test_case_1(self):
        """Driver can successfully create and list search indexes."""

        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]

        # Create a new search index on ``coll0`` with the ``createSearchIndex`` helper.  Use the following definition:
        model = {"name": _NAME, "definition": {"mappings": {"dynamic": False}}}
        await coll0.insert_one({})
        resp = await coll0.create_search_index(model)

        # Assert that the command returns the name of the index: ``"test-search-index"``.
        self.assertEqual(resp, _NAME)

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied and store the value in a variable ``index``:
        # An index with the ``name`` of ``test-search-index`` is present and the index has a field ``queryable`` with a value of ``true``.
        index = await self.wait_for_ready(coll0)

        # . Assert that ``index`` has a property ``latestDefinition`` whose value is ``{ 'mappings': { 'dynamic': false } }``
        self.assertIn("latestDefinition", index)
        self.assertEqual(index["latestDefinition"], model["definition"])

    async def test_case_2(self):
        """Driver can successfully create multiple indexes in batch."""

        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]
        await coll0.insert_one({})

        # Create two new search indexes on ``coll0`` with the ``createSearchIndexes`` helper.
        name1 = "test-search-index-1"
        name2 = "test-search-index-2"
        definition = {"mappings": {"dynamic": False}}
        index_definitions: list[dict[str, Any]] = [
            {"name": name1, "definition": definition},
            {"name": name2, "definition": definition},
        ]
        await coll0.create_search_indexes(
            [SearchIndexModel(i["definition"], i["name"]) for i in index_definitions]
        )

        # .Assert that the command returns an array containing the new indexes' names: ``["test-search-index-1", "test-search-index-2"]``.
        indices = await (await coll0.list_search_indexes()).to_list()
        names = [i["name"] for i in indices]
        self.assertIn(name1, names)
        self.assertIn(name2, names)

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied.
        # An index with the ``name`` of ``test-search-index-1`` is present and index has a field ``queryable`` with the value of ``true``. Store result in ``index1``.
        # An index with the ``name`` of ``test-search-index-2`` is present and index has a field ``queryable`` with the value of ``true``. Store result in ``index2``.
        index1 = await self.wait_for_ready(coll0, name1)
        index2 = await self.wait_for_ready(coll0, name2)

        # Assert that ``index1`` and ``index2`` have the property ``latestDefinition`` whose value is ``{ "mappings" : { "dynamic" : false } }``
        for index in [index1, index2]:
            self.assertIn("latestDefinition", index)
            self.assertEqual(index["latestDefinition"], definition)

    async def test_case_3(self):
        """Driver can successfully drop search indexes."""

        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]
        await coll0.insert_one({})

        # Create a new search index on ``coll0``.
        model = {"name": _NAME, "definition": {"mappings": {"dynamic": False}}}
        resp = await coll0.create_search_index(model)

        # Assert that the command returns the name of the index: ``"test-search-index"``.
        self.assertEqual(resp, "test-search-index")

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied:
        #   An index with the ``name`` of ``test-search-index`` is present and index has a field ``queryable`` with the value of ``true``.
        await self.wait_for_ready(coll0)

        # Run a ``dropSearchIndex`` on ``coll0``, using ``test-search-index`` for the name.
        await coll0.drop_search_index(_NAME)

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until ``listSearchIndexes`` returns an empty array.
        t0 = time.time()
        while True:
            indices = await (await coll0.list_search_indexes()).to_list()
            if indices:
                break
            if (time.time() - t0) / 60 > 5:
                raise TimeoutError("Timed out waiting for index deletion")
            await asyncio.sleep(5)

    async def test_case_4(self):
        """Driver can update a search index."""
        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]
        await coll0.insert_one({})

        # Create a new search index on ``coll0``.
        model = {"name": _NAME, "definition": {"mappings": {"dynamic": False}}}
        resp = await coll0.create_search_index(model)

        # Assert that the command returns the name of the index: ``"test-search-index"``.
        self.assertEqual(resp, _NAME)

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied:
        #  An index with the ``name`` of ``test-search-index`` is present and index has a field ``queryable`` with the value of ``true``.
        await self.wait_for_ready(coll0)

        # Run a ``updateSearchIndex`` on ``coll0``.
        # Assert that the command does not error and the server responds with a success.
        model2: dict[str, Any] = {"name": _NAME, "definition": {"mappings": {"dynamic": True}}}
        await coll0.update_search_index(_NAME, model2["definition"])

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied:
        #   An index with the ``name`` of ``test-search-index`` is present.  This index is referred to as ``index``.
        # The index has a field ``queryable`` with a value of ``true`` and has a field ``status`` with the value of ``READY``.
        predicate = lambda index: index.get("queryable") is True and index.get("status") == "READY"
        await self.wait_for_ready(coll0, predicate=predicate)

        # Assert that an index is present with the name ``test-search-index`` and the definition has a property ``latestDefinition`` whose value is ``{ 'mappings': { 'dynamic': true } }``.
        index = (await (await coll0.list_search_indexes(_NAME)).to_list())[0]
        self.assertIn("latestDefinition", index)
        self.assertEqual(index["latestDefinition"], model2["definition"])

    async def test_case_5(self):
        """``dropSearchIndex`` suppresses namespace not found errors."""
        # Create a driver-side collection object for a randomly generated collection name.  Do not create this collection on the server.
        coll0 = self.db[f"col{uuid.uuid4()}"]

        # Run a ``dropSearchIndex`` command and assert that no error is thrown.
        await coll0.drop_search_index("foo")

    async def test_case_6(self):
        """Driver can successfully create and list search indexes with non-default readConcern and writeConcern."""
        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]
        await coll0.insert_one({})

        # Apply a write concern ``WriteConcern(w=1)`` and a read concern with ``ReadConcern(level="majority")`` to ``coll0``.
        coll0 = coll0.with_options(
            write_concern=WriteConcern(w="1"), read_concern=ReadConcern(level="majority")
        )

        # Create a new search index on ``coll0`` with the ``createSearchIndex`` helper.
        name = "test-search-index-case6"
        model = {"name": name, "definition": {"mappings": {"dynamic": False}}}
        resp = await coll0.create_search_index(model)

        # Assert that the command returns the name of the index: ``"test-search-index-case6"``.
        self.assertEqual(resp, name)

        # Run ``coll0.listSearchIndexes()`` repeatedly every 5 seconds until the following condition is satisfied and store the value in a variable ``index``:
        # - An index with the ``name`` of ``test-search-index-case6`` is present and the index has a field ``queryable`` with a value of ``true``.
        index = await self.wait_for_ready(coll0, name)

        # Assert that ``index`` has a property ``latestDefinition`` whose value is ``{ 'mappings': { 'dynamic': false } }``
        self.assertIn("latestDefinition", index)
        self.assertEqual(index["latestDefinition"], model["definition"])

    async def test_case_7(self):
        """Driver handles index types."""

        # Create a collection with the "create" command using a randomly generated name (referred to as ``coll0``).
        coll0 = self.db[f"col{uuid.uuid4()}"]
        await coll0.insert_one({})

        # Use these search and vector search definitions for indexes.
        search_definition = {"mappings": {"dynamic": False}}
        vector_search_definition = {
            "fields": [
                {
                    "type": "vector",
                    "path": "plot_embedding",
                    "numDimensions": 1536,
                    "similarity": "euclidean",
                },
            ]
        }

        # Create a new search index on ``coll0`` that implicitly passes its type.
        implicit_search_resp = await coll0.create_search_index(
            model={"name": _NAME + "-implicit", "definition": search_definition}
        )

        # Get the index definition.
        resp = await (await coll0.list_search_indexes(name=implicit_search_resp)).next()

        # Assert that the index model contains the correct index type: ``"search"``.
        self.assertEqual(resp["type"], "search")

        # Create a new search index on ``coll0`` that explicitly passes its type.
        explicit_search_resp = await coll0.create_search_index(
            model={"name": _NAME + "-explicit", "type": "search", "definition": search_definition}
        )

        # Get the index definition.
        resp = await (await coll0.list_search_indexes(name=explicit_search_resp)).next()

        # Assert that the index model contains the correct index type: ``"search"``.
        self.assertEqual(resp["type"], "search")

        # Create a new vector search index on ``coll0`` that explicitly passes its type.
        explicit_vector_resp = await coll0.create_search_index(
            model={
                "name": _NAME + "-vector",
                "type": "vectorSearch",
                "definition": vector_search_definition,
            }
        )

        # Get the index definition.
        resp = await (await coll0.list_search_indexes(name=explicit_vector_resp)).next()

        # Assert that the index model contains the correct index type: ``"vectorSearch"``.
        self.assertEqual(resp["type"], "vectorSearch")

        # Catch the error raised when trying to create a vector search index without specifying the type
        with self.assertRaises(OperationFailure) as e:
            await coll0.create_search_index(
                model={"name": _NAME + "-error", "definition": vector_search_definition}
            )
        self.assertIn("Attribute mappings missing.", e.exception.details["errmsg"])


globals().update(
    generate_test_classes(
        get_test_path("index_management"),
        module=__name__,
    )
)

if __name__ == "__main__":
    unittest.main()
