# Copyright 2016-present MongoDB, Inc.
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

"""Test the collation module."""
from __future__ import annotations

import functools
import warnings
from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest
from test.utils_shared import EventListener, OvertCommandListener
from typing import Any

from pymongo.asynchronous.helpers import anext
from pymongo.collation import (
    Collation,
    CollationAlternate,
    CollationCaseFirst,
    CollationMaxVariable,
    CollationStrength,
)
from pymongo.errors import ConfigurationError
from pymongo.operations import (
    DeleteMany,
    DeleteOne,
    IndexModel,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)
from pymongo.write_concern import WriteConcern

_IS_SYNC = False


class TestCollationObject(unittest.TestCase):
    def test_constructor(self):
        self.assertRaises(TypeError, Collation, locale=42)
        # Fill in a locale to test the other options.
        _Collation = functools.partial(Collation, "en_US")
        # No error.
        _Collation(caseFirst=CollationCaseFirst.UPPER)
        self.assertRaises(TypeError, _Collation, caseLevel="true")
        self.assertRaises(ValueError, _Collation, strength="six")
        self.assertRaises(TypeError, _Collation, numericOrdering="true")
        self.assertRaises(TypeError, _Collation, alternate=5)
        self.assertRaises(TypeError, _Collation, maxVariable=2)
        self.assertRaises(TypeError, _Collation, normalization="false")
        self.assertRaises(TypeError, _Collation, backwards="true")

        # No errors.
        Collation("en_US", future_option="bar", another_option=42)
        collation = Collation(
            "en_US",
            caseLevel=True,
            caseFirst=CollationCaseFirst.UPPER,
            strength=CollationStrength.QUATERNARY,
            numericOrdering=True,
            alternate=CollationAlternate.SHIFTED,
            maxVariable=CollationMaxVariable.SPACE,
            normalization=True,
            backwards=True,
        )

        self.assertEqual(
            {
                "locale": "en_US",
                "caseLevel": True,
                "caseFirst": "upper",
                "strength": 4,
                "numericOrdering": True,
                "alternate": "shifted",
                "maxVariable": "space",
                "normalization": True,
                "backwards": True,
            },
            collation.document,
        )

        self.assertEqual(
            {"locale": "en_US", "backwards": True}, Collation("en_US", backwards=True).document
        )


class TestCollation(AsyncIntegrationTest):
    listener: EventListener
    warn_context: Any
    collation: Collation

    @async_client_context.require_connection
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.listener = OvertCommandListener()
        self.client = await self.async_rs_or_single_client(event_listeners=[self.listener])
        self.db = self.client.pymongo_test
        self.collation = Collation("en_US")
        self.warn_context = warnings.catch_warnings()
        self.warn_context.__enter__()

    async def asyncTearDown(self) -> None:
        self.warn_context.__exit__()
        self.warn_context = None
        self.listener.reset()
        await super().asyncTearDown()

    def last_command_started(self):
        return self.listener.started_events[-1].command

    def assertCollationInLastCommand(self):
        self.assertEqual(self.collation.document, self.last_command_started()["collation"])

    async def test_create_collection(self):
        await self.db.test.drop()
        await self.db.create_collection("test", collation=self.collation)
        self.assertCollationInLastCommand()

        # Test passing collation as a dict as well.
        await self.db.test.drop()
        self.listener.reset()
        await self.db.create_collection("test", collation=self.collation.document)
        self.assertCollationInLastCommand()

    def test_index_model(self):
        model = IndexModel([("a", 1), ("b", -1)], collation=self.collation)
        self.assertEqual(self.collation.document, model.document["collation"])

    async def test_create_index(self):
        await self.db.test.create_index("foo", collation=self.collation)
        ci_cmd = self.listener.started_events[0].command
        self.assertEqual(self.collation.document, ci_cmd["indexes"][0]["collation"])

    async def test_aggregate(self):
        await self.db.test.aggregate([{"$group": {"_id": 42}}], collation=self.collation)
        self.assertCollationInLastCommand()

    async def test_count_documents(self):
        await self.db.test.count_documents({}, collation=self.collation)
        self.assertCollationInLastCommand()

    async def test_distinct(self):
        await self.db.test.distinct("foo", collation=self.collation)
        self.assertCollationInLastCommand()

        self.listener.reset()
        await self.db.test.find(collation=self.collation).distinct("foo")
        self.assertCollationInLastCommand()

    async def test_find_command(self):
        await self.db.test.insert_one({"is this thing on?": True})
        self.listener.reset()
        await anext(self.db.test.find(collation=self.collation))
        self.assertCollationInLastCommand()

    async def test_explain_command(self):
        self.listener.reset()
        await self.db.test.find(collation=self.collation).explain()
        # The collation should be part of the explained command.
        self.assertEqual(
            self.collation.document, self.last_command_started()["explain"]["collation"]
        )

    async def test_delete(self):
        await self.db.test.delete_one({"foo": 42}, collation=self.collation)
        command = self.listener.started_events[0].command
        self.assertEqual(self.collation.document, command["deletes"][0]["collation"])

        self.listener.reset()
        await self.db.test.delete_many({"foo": 42}, collation=self.collation)
        command = self.listener.started_events[0].command
        self.assertEqual(self.collation.document, command["deletes"][0]["collation"])

    async def test_update(self):
        await self.db.test.replace_one({"foo": 42}, {"foo": 43}, collation=self.collation)
        command = self.listener.started_events[0].command
        self.assertEqual(self.collation.document, command["updates"][0]["collation"])

        self.listener.reset()
        await self.db.test.update_one({"foo": 42}, {"$set": {"foo": 43}}, collation=self.collation)
        command = self.listener.started_events[0].command
        self.assertEqual(self.collation.document, command["updates"][0]["collation"])

        self.listener.reset()
        await self.db.test.update_many({"foo": 42}, {"$set": {"foo": 43}}, collation=self.collation)
        command = self.listener.started_events[0].command
        self.assertEqual(self.collation.document, command["updates"][0]["collation"])

    async def test_find_and(self):
        await self.db.test.find_one_and_delete({"foo": 42}, collation=self.collation)
        self.assertCollationInLastCommand()

        self.listener.reset()
        await self.db.test.find_one_and_update(
            {"foo": 42}, {"$set": {"foo": 43}}, collation=self.collation
        )
        self.assertCollationInLastCommand()

        self.listener.reset()
        await self.db.test.find_one_and_replace({"foo": 42}, {"foo": 43}, collation=self.collation)
        self.assertCollationInLastCommand()

    async def test_bulk_write(self):
        await self.db.test.collection.bulk_write(
            [
                DeleteOne({"noCollation": 42}),
                DeleteMany({"noCollation": 42}),
                DeleteOne({"foo": 42}, collation=self.collation),
                DeleteMany({"foo": 42}, collation=self.collation),
                ReplaceOne({"noCollation": 24}, {"bar": 42}),
                UpdateOne({"noCollation": 84}, {"$set": {"bar": 10}}, upsert=True),
                UpdateMany({"noCollation": 45}, {"$set": {"bar": 42}}),
                ReplaceOne({"foo": 24}, {"foo": 42}, collation=self.collation),
                UpdateOne(
                    {"foo": 84}, {"$set": {"foo": 10}}, upsert=True, collation=self.collation
                ),
                UpdateMany({"foo": 45}, {"$set": {"foo": 42}}, collation=self.collation),
            ]
        )

        delete_cmd = self.listener.started_events[0].command
        update_cmd = self.listener.started_events[1].command

        def check_ops(ops):
            for op in ops:
                if "noCollation" in op["q"]:
                    self.assertNotIn("collation", op)
                else:
                    self.assertEqual(self.collation.document, op["collation"])

        check_ops(delete_cmd["deletes"])
        check_ops(update_cmd["updates"])

    async def test_indexes_same_keys_different_collations(self):
        await self.db.test.drop()
        usa_collation = Collation("en_US")
        ja_collation = Collation("ja")
        await self.db.test.create_indexes(
            [
                IndexModel("fieldname", collation=usa_collation),
                IndexModel("fieldname", name="japanese_version", collation=ja_collation),
                IndexModel("fieldname", name="simple"),
            ]
        )
        indexes = await self.db.test.index_information()
        self.assertEqual(
            usa_collation.document["locale"], indexes["fieldname_1"]["collation"]["locale"]
        )
        self.assertEqual(
            ja_collation.document["locale"], indexes["japanese_version"]["collation"]["locale"]
        )
        self.assertNotIn("collation", indexes["simple"])
        await self.db.test.drop_index("fieldname_1")
        indexes = await self.db.test.index_information()
        self.assertIn("japanese_version", indexes)
        self.assertIn("simple", indexes)
        self.assertNotIn("fieldname", indexes)

    async def test_unacknowledged_write(self):
        unacknowledged = WriteConcern(w=0)
        collection = self.db.get_collection("test", write_concern=unacknowledged)
        with self.assertRaises(ConfigurationError):
            await collection.update_one(
                {"hello": "world"}, {"$set": {"hello": "moon"}}, collation=self.collation
            )
        update_one = UpdateOne(
            {"hello": "world"}, {"$set": {"hello": "moon"}}, collation=self.collation
        )
        with self.assertRaises(ConfigurationError):
            await collection.bulk_write([update_one])

    async def test_cursor_collation(self):
        await self.db.test.insert_one({"hello": "world"})
        await anext(self.db.test.find().collation(self.collation))
        self.assertCollationInLastCommand()
