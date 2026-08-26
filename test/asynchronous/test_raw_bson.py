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
from __future__ import annotations

import datetime
import sys
import uuid

sys.path[0:0] = [""]

from bson import decode, encode
from bson.binary import JAVA_LEGACY, Binary, UuidRepresentation
from bson.codec_options import CodecOptions
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument
from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest
from test.test_raw_bson_shared import TEST_RAW_BSON

_IS_SYNC = False


class TestRawBSONDocument(AsyncIntegrationTest):
    # {'_id': ObjectId('556df68b6e32ab21a95e0785'),
    #  'name': 'Sherlock',
    #  'addresses': [{'street': 'Baker Street'}]}
    bson_string = TEST_RAW_BSON
    document = RawBSONDocument(bson_string)

    async def asyncTearDown(self):
        if async_client_context.connected:
            await self.client.pymongo_test.test_raw.drop()

    @async_client_context.require_connection
    async def test_round_trip_view_backed_document(self):
        inner = {"payload": "x" * 8000, "marker": 1}
        subdoc = RawBSONDocument(encode({"big": inner}))["big"]
        self.assertIsInstance(subdoc.raw, memoryview)
        coll = self.client.pymongo_test.test_raw
        await coll.insert_one(subdoc)
        result = await coll.find_one({"marker": 1}, {"_id": False})
        self.assertEqual(inner, result)

    @async_client_context.require_connection
    async def test_round_trip(self):
        db = self.client.get_database(
            "pymongo_test", codec_options=CodecOptions(document_class=RawBSONDocument)
        )
        await db.test_raw.insert_one(self.document)
        result = await db.test_raw.find_one(self.document["_id"])
        assert result is not None
        self.assertIsInstance(result, RawBSONDocument)
        self.assertEqual(dict(self.document.items()), dict(result.items()))

    @async_client_context.require_connection
    async def test_round_trip_raw_uuid(self):
        coll = self.client.get_database("pymongo_test").test_raw
        uid = uuid.uuid4()
        doc = {"_id": 1, "bin4": Binary(uid.bytes, 4), "bin3": Binary(uid.bytes, 3)}
        raw = RawBSONDocument(encode(doc))
        await coll.insert_one(raw)
        self.assertEqual(await coll.find_one(), doc)
        uuid_coll = coll.with_options(
            codec_options=coll.codec_options.with_options(
                uuid_representation=UuidRepresentation.STANDARD
            )
        )
        self.assertEqual(
            await uuid_coll.find_one(), {"_id": 1, "bin4": uid, "bin3": Binary(uid.bytes, 3)}
        )

        # Test that the raw bytes haven't changed.
        raw_coll = coll.with_options(codec_options=DEFAULT_RAW_BSON_OPTIONS)
        self.assertEqual(await raw_coll.find_one(), raw)

    @async_client_context.require_connection
    async def test_round_trip_codec_options(self):
        doc = {
            "date": datetime.datetime(2015, 6, 3, 18, 40, 50, 826000),
            "_id": uuid.UUID("026fab8f-975f-4965-9fbf-85ad874c60ff"),
        }
        db = self.client.pymongo_test
        coll = db.get_collection(
            "test_raw", codec_options=CodecOptions(uuid_representation=JAVA_LEGACY)
        )
        await coll.insert_one(doc)
        raw_java_legacy = CodecOptions(
            uuid_representation=JAVA_LEGACY, document_class=RawBSONDocument
        )
        coll = db.get_collection("test_raw", codec_options=raw_java_legacy)
        self.assertEqual(
            RawBSONDocument(encode(doc, codec_options=raw_java_legacy)), await coll.find_one()
        )

    @async_client_context.require_connection
    async def test_raw_bson_document_embedded(self):
        doc = {"embedded": self.document}
        db = self.client.pymongo_test
        await db.test_raw.insert_one(doc)
        result = await db.test_raw.find_one()
        assert result is not None
        self.assertEqual(decode(self.document.raw), result["embedded"])

        # Make sure that CodecOptions are preserved.
        # {'embedded': [
        #   {'date': datetime.datetime(2015, 6, 3, 18, 40, 50, 826000),
        #    '_id': UUID('026fab8f-975f-4965-9fbf-85ad874c60ff')}
        # ]}
        # encoded with JAVA_LEGACY uuid representation.
        bson_string = (
            b"D\x00\x00\x00\x04embedded\x005\x00\x00\x00\x030\x00-\x00\x00\x00"
            b"\tdate\x00\x8a\xd6\xb9\xbaM\x01\x00\x00\x05_id\x00\x10\x00\x00"
            b"\x00\x03eI_\x97\x8f\xabo\x02\xff`L\x87\xad\x85\xbf\x9f\x00\x00"
            b"\x00"
        )
        rbd = RawBSONDocument(
            bson_string,
            codec_options=CodecOptions(
                uuid_representation=JAVA_LEGACY, document_class=RawBSONDocument
            ),
        )

        await db.test_raw.drop()
        await db.test_raw.insert_one(rbd)
        result = await db.get_collection(
            "test_raw", codec_options=CodecOptions(uuid_representation=JAVA_LEGACY)
        ).find_one()
        assert result is not None
        self.assertEqual(rbd["embedded"][0]["_id"], result["embedded"][0]["_id"])

    @async_client_context.require_connection
    async def test_write_response_raw_bson(self):
        coll = self.client.get_database(
            "pymongo_test", codec_options=CodecOptions(document_class=RawBSONDocument)
        ).test_raw

        # No Exceptions raised while handling write response.
        await coll.insert_one(self.document)
        await coll.delete_one(self.document)
        await coll.insert_many([self.document])
        await coll.delete_many(self.document)
        await coll.update_one(self.document, {"$set": {"a": "b"}}, upsert=True)
        await coll.update_many(self.document, {"$set": {"b": "c"}})


if __name__ == "__main__":
    unittest.main()
