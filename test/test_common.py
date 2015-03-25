# Copyright 2011-2015 MongoDB, Inc.
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

from bson.binary import UUIDLegacy, PYTHON_LEGACY, STANDARD
from bson.code import Code
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from pymongo.mongo_client import MongoClient
from pymongo.errors import OperationFailure
from pymongo.write_concern import WriteConcern
from test import client_context, pair, unittest, IntegrationTest
from test.utils import connected, rs_or_single_client, single_client


@client_context.require_connection
def setUpModule():
    pass


class TestCommon(IntegrationTest):

    def test_uuid_representation(self):
        coll = self.db.uuid
        coll.drop()

        # Test property
        self.assertEqual(PYTHON_LEGACY,
                         coll.codec_options.uuid_representation)

        # Test basic query
        uu = uuid.uuid4()
        # Insert as binary subtype 3
        coll.insert_one({'uu': uu})
        self.assertEqual(uu, coll.find_one({'uu': uu})['uu'])
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=STANDARD))
        self.assertEqual(STANDARD, coll.codec_options.uuid_representation)
        self.assertEqual(None, coll.find_one({'uu': uu}))
        self.assertEqual(uu, coll.find_one({'uu': UUIDLegacy(uu)})['uu'])

        # Test Cursor.count
        self.assertEqual(0, coll.find({'uu': uu}).count())
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual(1, coll.find({'uu': uu}).count())

        # Test delete
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=STANDARD))
        coll.delete_one({'uu': uu})
        self.assertEqual(1, coll.count())
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        coll.delete_one({'uu': uu})
        self.assertEqual(0, coll.count())

        # Test update_one
        coll.insert_one({'_id': uu, 'i': 1})
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=STANDARD))
        coll.update_one({'_id': uu}, {'$set': {'i': 2}})
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual(1, coll.find_one({'_id': uu})['i'])
        coll.update_one({'_id': uu}, {'$set': {'i': 2}})
        self.assertEqual(2, coll.find_one({'_id': uu})['i'])

        # Test Cursor.distinct
        self.assertEqual([2], coll.find({'_id': uu}).distinct('i'))
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=STANDARD))
        self.assertEqual([], coll.find({'_id': uu}).distinct('i'))

        # Test findAndModify
        self.assertEqual(None, coll.find_one_and_update({'_id': uu},
                                                        {'$set': {'i': 5}}))
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual(2, coll.find_one_and_update({'_id': uu},
                                                     {'$set': {'i': 5}})['i'])
        self.assertEqual(5, coll.find_one({'_id': uu})['i'])

        # Test command
        self.assertEqual(5, self.db.command('findAndModify', 'uuid',
                                            update={'$set': {'i': 6}},
                                            query={'_id': uu})['value']['i'])
        self.assertEqual(6, self.db.command(
            'findAndModify', 'uuid',
            update={'$set': {'i': 7}},
            query={'_id': UUIDLegacy(uu)})['value']['i'])

        # Test (inline)_map_reduce
        coll.drop()
        coll.insert_one({"_id": uu, "x": 1, "tags": ["dog", "cat"]})
        coll.insert_one({"_id": uuid.uuid4(), "x": 3,
                         "tags": ["mouse", "cat", "dog"]})

        map = Code("function () {"
                   "  this.tags.forEach(function(z) {"
                   "    emit(z, 1);"
                   "  });"
                   "}")

        reduce = Code("function (key, values) {"
                      "  var total = 0;"
                      "  for (var i = 0; i < values.length; i++) {"
                      "    total += values[i];"
                      "  }"
                      "  return total;"
                      "}")

        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=STANDARD))
        q = {"_id": uu}
        result = coll.inline_map_reduce(map, reduce, query=q)
        self.assertEqual([], result)

        result = coll.map_reduce(map, reduce, "results", query=q)
        self.assertEqual(0, self.db.results.count())

        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        q = {"_id": uu}
        result = coll.inline_map_reduce(map, reduce, query=q)
        self.assertEqual(2, len(result))

        result = coll.map_reduce(map, reduce, "results", query=q)
        self.assertEqual(2, self.db.results.count())

        self.db.drop_collection("result")
        coll.drop()

        # Test group
        coll.insert_one({"_id": uu, "a": 2})
        coll.insert_one({"_id": uuid.uuid4(), "a": 1})

        reduce = "function (obj, prev) { prev.count++; }"
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=STANDARD))
        self.assertEqual([],
                         coll.group([], {"_id": uu},
                                    {"count": 0}, reduce))
        coll = self.db.get_collection(
            "uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        self.assertEqual([{"count": 1}],
                         coll.group([], {"_id": uu},
                                    {"count": 0}, reduce))

    def test_write_concern(self):
        c = MongoClient(connect=False)
        self.assertEqual(WriteConcern(), c.write_concern)

        c = MongoClient(connect=False, w=2, wtimeout=1000)
        wc = WriteConcern(w=2, wtimeout=1000)
        self.assertEqual(wc, c.write_concern)

        db = c.pymongo_test
        self.assertEqual(wc, db.write_concern)
        coll = db.test
        self.assertEqual(wc, coll.write_concern)

        cwc = WriteConcern(j=True)
        coll = db.get_collection('test', write_concern=cwc)
        self.assertEqual(cwc, coll.write_concern)
        self.assertEqual(wc, db.write_concern)

    def test_mongo_client(self):
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

        m = MongoClient("mongodb://%s/" % (pair,),
                        replicaSet=client_context.replica_set_name)

        coll = m.pymongo_test.write_concern_test
        self.assertRaises(OperationFailure, coll.insert_one, doc)
        m = MongoClient("mongodb://%s/?w=0" % (pair,),
                        replicaSet=client_context.replica_set_name)

        coll = m.pymongo_test.write_concern_test
        coll.insert_one(doc)

        # Equality tests
        direct = connected(single_client(w=0))
        self.assertEqual(direct,
                         connected(MongoClient("mongodb://%s/?w=0" % (pair,))))

        self.assertFalse(direct !=
                         connected(MongoClient("mongodb://%s/?w=0" % (pair,))))


if __name__ == "__main__":
    unittest.main()
