# Copyright 2011-present MongoDB, Inc.
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

"""Test the pymongo common module."""
from __future__ import annotations

import sys
import uuid

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, connected, unittest

from bson.binary import PYTHON_LEGACY, STANDARD, Binary, UuidRepresentation
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from pymongo.errors import OperationFailure
from pymongo.write_concern import WriteConcern

_IS_SYNC = False


class TestCommon(AsyncIntegrationTest):
    async def test_uuid_representation(self):
        coll = self.db.uuid
        await coll.drop()

        # Test property
        self.assertEqual(UuidRepresentation.UNSPECIFIED, coll.codec_options.uuid_representation)

        # Test basic query
        uu = uuid.uuid4()
        # Insert as binary subtype 3
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        legacy_opts = coll.codec_options
        await coll.insert_one({"uu": uu})
        self.assertEqual(uu, (await coll.find_one({"uu": uu}))["uu"])  # type: ignore
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=STANDARD))
        self.assertEqual(STANDARD, coll.codec_options.uuid_representation)
        self.assertEqual(None, await coll.find_one({"uu": uu}))
        uul = Binary.from_uuid(uu, PYTHON_LEGACY)
        self.assertEqual(uul, (await coll.find_one({"uu": uul}))["uu"])  # type: ignore

        # Test count_documents
        self.assertEqual(0, await coll.count_documents({"uu": uu}))
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual(1, await coll.count_documents({"uu": uu}))

        # Test delete
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=STANDARD))
        await coll.delete_one({"uu": uu})
        self.assertEqual(1, await coll.count_documents({}))
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        await coll.delete_one({"uu": uu})
        self.assertEqual(0, await coll.count_documents({}))

        # Test update_one
        await coll.insert_one({"_id": uu, "i": 1})
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=STANDARD))
        await coll.update_one({"_id": uu}, {"$set": {"i": 2}})
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual(1, (await coll.find_one({"_id": uu}))["i"])  # type: ignore
        await coll.update_one({"_id": uu}, {"$set": {"i": 2}})
        self.assertEqual(2, (await coll.find_one({"_id": uu}))["i"])  # type: ignore

        # Test Cursor.distinct
        self.assertEqual([2], await coll.find({"_id": uu}).distinct("i"))
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=STANDARD))
        self.assertEqual([], await coll.find({"_id": uu}).distinct("i"))

        # Test findAndModify
        self.assertEqual(None, await coll.find_one_and_update({"_id": uu}, {"$set": {"i": 5}}))
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual(2, (await coll.find_one_and_update({"_id": uu}, {"$set": {"i": 5}}))["i"])
        self.assertEqual(5, (await coll.find_one({"_id": uu}))["i"])  # type: ignore

        # Test command
        self.assertEqual(
            5,
            (
                await self.db.command(
                    "findAndModify",
                    "uuid",
                    update={"$set": {"i": 6}},
                    query={"_id": uu},
                    codec_options=legacy_opts,
                )
            )["value"]["i"],
        )
        self.assertEqual(
            6,
            (
                await self.db.command(
                    "findAndModify",
                    "uuid",
                    update={"$set": {"i": 7}},
                    query={"_id": Binary.from_uuid(uu, PYTHON_LEGACY)},
                )
            )["value"]["i"],
        )

    async def test_write_concern(self):
        c = await self.async_rs_or_single_client(connect=False)
        self.assertEqual(WriteConcern(), c.write_concern)

        c = await self.async_rs_or_single_client(connect=False, w=2, wTimeoutMS=1000)
        wc = WriteConcern(w=2, wtimeout=1000)
        self.assertEqual(wc, c.write_concern)

        # Can we override back to the server default?
        db = c.get_database("pymongo_test", write_concern=WriteConcern())
        self.assertEqual(db.write_concern, WriteConcern())

        db = c.pymongo_test
        self.assertEqual(wc, db.write_concern)
        coll = db.test
        self.assertEqual(wc, coll.write_concern)

        cwc = WriteConcern(j=True)
        coll = db.get_collection("test", write_concern=cwc)
        self.assertEqual(cwc, coll.write_concern)
        self.assertEqual(wc, db.write_concern)

    async def test_mongo_client(self):
        pair = await async_client_context.pair
        m = await self.async_rs_or_single_client(w=0)
        coll = m.pymongo_test.write_concern_test
        await coll.drop()
        doc = {"_id": ObjectId()}
        await coll.insert_one(doc)
        self.assertTrue(await coll.insert_one(doc))
        coll = coll.with_options(write_concern=WriteConcern(w=1))
        with self.assertRaises(OperationFailure):
            await coll.insert_one(doc)

        m = await self.async_rs_or_single_client()
        coll = m.pymongo_test.write_concern_test
        new_coll = coll.with_options(write_concern=WriteConcern(w=0))
        self.assertTrue(await new_coll.insert_one(doc))
        with self.assertRaises(OperationFailure):
            await coll.insert_one(doc)

        m = await self.async_rs_or_single_client(
            f"mongodb://{pair}/", replicaSet=async_client_context.replica_set_name
        )

        coll = m.pymongo_test.write_concern_test
        with self.assertRaises(OperationFailure):
            await coll.insert_one(doc)
        m = await self.async_rs_or_single_client(
            f"mongodb://{pair}/?w=0", replicaSet=async_client_context.replica_set_name
        )

        coll = m.pymongo_test.write_concern_test
        await coll.insert_one(doc)

        # Equality tests
        direct = await connected(await self.async_single_client(w=0))
        direct2 = await connected(
            await self.async_single_client(f"mongodb://{pair}/?w=0", **self.credentials)
        )
        self.assertEqual(direct, direct2)
        self.assertFalse(direct != direct2)

    async def test_validate_boolean(self):
        await self.db.test.update_one({}, {"$set": {"total": 1}}, upsert=True)
        with self.assertRaisesRegex(
            TypeError, "upsert must be True or False, was: upsert={'upsert': True}"
        ):
            await self.db.test.update_one({}, {"$set": {"total": 1}}, {"upsert": True})  # type: ignore


if __name__ == "__main__":
    unittest.main()
