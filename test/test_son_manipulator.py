# Copyright 2009-2015 MongoDB, Inc.
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

"""Tests for SONManipulators.
"""

import sys
sys.path[0:0] = [""]

from bson.son import SON
from pymongo import MongoClient
from pymongo.son_manipulator import (NamespaceInjector,
                                     ObjectIdInjector,
                                     ObjectIdShuffler,
                                     SONManipulator)
from test import qcheck, unittest, host, port


class TestSONManipulator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        client = MongoClient(host, port, connect=False)
        cls.db = client.pymongo_test

    def test_basic(self):
        manip = SONManipulator()
        collection = self.db.test

        def incoming_is_identity(son):
            return son == manip.transform_incoming(son, collection)
        qcheck.check_unittest(self, incoming_is_identity,
                              qcheck.gen_mongo_dict(3))

        def outgoing_is_identity(son):
            return son == manip.transform_outgoing(son, collection)
        qcheck.check_unittest(self, outgoing_is_identity,
                              qcheck.gen_mongo_dict(3))

    def test_id_injection(self):
        manip = ObjectIdInjector()
        collection = self.db.test

        def incoming_adds_id(son):
            son = manip.transform_incoming(son, collection)
            assert "_id" in son
            return True
        qcheck.check_unittest(self, incoming_adds_id,
                              qcheck.gen_mongo_dict(3))

        def outgoing_is_identity(son):
            return son == manip.transform_outgoing(son, collection)
        qcheck.check_unittest(self, outgoing_is_identity,
                              qcheck.gen_mongo_dict(3))

    def test_id_shuffling(self):
        manip = ObjectIdShuffler()
        collection = self.db.test

        def incoming_moves_id(son_in):
            son = manip.transform_incoming(son_in, collection)
            if not "_id" in son:
                return True
            for (k, v) in son.items():
                self.assertEqual(k, "_id")
                break
            # Key order matters in SON equality test,
            # matching collections.OrderedDict
            if isinstance(son_in, SON):
                return son_in.to_dict() == son.to_dict()
            return son_in == son

        self.assertTrue(incoming_moves_id({}))
        self.assertTrue(incoming_moves_id({"_id": 12}))
        self.assertTrue(incoming_moves_id({"hello": "world", "_id": 12}))
        self.assertTrue(incoming_moves_id(SON([("hello", "world"),
                                               ("_id", 12)])))

        def outgoing_is_identity(son):
            return son == manip.transform_outgoing(son, collection)
        qcheck.check_unittest(self, outgoing_is_identity,
                              qcheck.gen_mongo_dict(3))

    def test_ns_injection(self):
        manip = NamespaceInjector()
        collection = self.db.test

        def incoming_adds_ns(son):
            son = manip.transform_incoming(son, collection)
            assert "_ns" in son
            return son["_ns"] == collection.name
        qcheck.check_unittest(self, incoming_adds_ns,
                              qcheck.gen_mongo_dict(3))

        def outgoing_is_identity(son):
            return son == manip.transform_outgoing(son, collection)
        qcheck.check_unittest(self, outgoing_is_identity,
                              qcheck.gen_mongo_dict(3))

if __name__ == "__main__":
    unittest.main()
