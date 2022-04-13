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

import sys
import uuid

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.utils import connected, rs_or_single_client, single_client

from bson.binary import PYTHON_LEGACY, STANDARD, Binary, UuidRepresentation
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from pymongo.errors import OperationFailure
from pymongo.write_concern import WriteConcern


@client_context.require_connection
def setUpModule():
    pass


class TestCommon(IntegrationTest):
    def test_uuid_representation(self):
        coll = self.db.uuid
        coll.drop()

        # Test property
        self.assertEqual(UuidRepresentation.UNSPECIFIED, coll.codec_options.uuid_representation)

        # Test basic query
        uu = uuid.uuid4()
        # Insert as binary subtype 3
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        legacy_opts = coll.codec_options
        coll.insert_one({"uu": uu})
        self.assertEqual(uu, coll.find_one({"uu": uu})["uu"])  # type: ignore
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=STANDARD))
        self.assertEqual(STANDARD, coll.codec_options.uuid_representation)
        self.assertEqual(None, coll.find_one({"uu": uu}))
        uul = Binary.from_uuid(uu, PYTHON_LEGACY)
        self.assertEqual(uul, coll.find_one({"uu": uul})["uu"])  # type: ignore

        # Test count_documents
        self.assertEqual(0, coll.count_documents({"uu": uu}))
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual(1, coll.count_documents({"uu": uu}))

        # Test delete
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=STANDARD))
        coll.delete_one({"uu": uu})
        self.assertEqual(1, coll.count_documents({}))
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        coll.delete_one({"uu": uu})
        self.assertEqual(0, coll.count_documents({}))

        # Test update_one
        coll.insert_one({"_id": uu, "i": 1})
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=STANDARD))
        coll.update_one({"_id": uu}, {"$set": {"i": 2}})
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual(1, coll.find_one({"_id": uu})["i"])  # type: ignore
        coll.update_one({"_id": uu}, {"$set": {"i": 2}})
        self.assertEqual(2, coll.find_one({"_id": uu})["i"])  # type: ignore

        # Test Cursor.distinct
        self.assertEqual([2], coll.find({"_id": uu}).distinct("i"))
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=STANDARD))
        self.assertEqual([], coll.find({"_id": uu}).distinct("i"))

        # Test findAndModify
        self.assertEqual(None, coll.find_one_and_update({"_id": uu}, {"$set": {"i": 5}}))
        coll = self.db.get_collection("uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual(2, coll.find_one_and_update({"_id": uu}, {"$set": {"i": 5}})["i"])
        self.assertEqual(5, coll.find_one({"_id": uu})["i"])  # type: ignore

        # Test command
        self.assertEqual(
            5,
            self.db.command(
                "findAndModify",
                "uuid",
                update={"$set": {"i": 6}},
                query={"_id": uu},
                codec_options=legacy_opts,
            )["value"]["i"],
        )
        self.assertEqual(
            6,
            self.db.command(
                "findAndModify",
                "uuid",
                update={"$set": {"i": 7}},
                query={"_id": Binary.from_uuid(uu, PYTHON_LEGACY)},
            )["value"]["i"],
        )

    def test_write_concern(self):
        c = rs_or_single_client(connect=False)
        self.assertEqual(WriteConcern(), c.write_concern)

        c = rs_or_single_client(connect=False, w=2, wTimeoutMS=1000)
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

    def test_mongo_client(self):
        pair = client_context.pair
        m = rs_or_single_client(w=0)
        coll = m.pymongo_test.write_concern_test
        coll.drop()
        doc = {"_id": ObjectId()}
        coll.insert_one(doc)
        self.assertTrue(coll.insert_one(doc))
        coll = coll.with_options(write_concern=WriteConcern(w=1))
        self.assertRaises(OperationFailure, coll.insert_one, doc)

        m = rs_or_single_client()
        coll = m.pymongo_test.write_concern_test
        new_coll = coll.with_options(write_concern=WriteConcern(w=0))
        self.assertTrue(new_coll.insert_one(doc))
        self.assertRaises(OperationFailure, coll.insert_one, doc)

        m = rs_or_single_client(
            "mongodb://%s/" % (pair,), replicaSet=client_context.replica_set_name
        )

        coll = m.pymongo_test.write_concern_test
        self.assertRaises(OperationFailure, coll.insert_one, doc)
        m = rs_or_single_client(
            "mongodb://%s/?w=0" % (pair,), replicaSet=client_context.replica_set_name
        )

        coll = m.pymongo_test.write_concern_test
        coll.insert_one(doc)

        # Equality tests
        direct = connected(single_client(w=0))
        direct2 = connected(single_client("mongodb://%s/?w=0" % (pair,), **self.credentials))
        self.assertEqual(direct, direct2)
        self.assertFalse(direct != direct2)


if __name__ == "__main__":
    unittest.main()
