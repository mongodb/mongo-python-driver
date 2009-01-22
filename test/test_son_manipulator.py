# Copyright 2009 10gen, Inc.
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

import unittest

import qcheck
from test_connection import get_connection
from pymongo.objectid import ObjectId
from pymongo.son_manipulator import SONManipulator, ObjectIdInjector, NamespaceInjector
from pymongo.database import Database

class TestSONManipulator(unittest.TestCase):
    def setUp(self):
        self.db = Database(get_connection(), "test")

    def test_basic(self):
        manip = SONManipulator(self.db)
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
        manip = ObjectIdInjector(self.db)
        collection = self.db.test

        def incoming_adds_id(son):
            son = manip.transform_incoming(son, collection)
            assert "_id" in son
            assert isinstance(son["_id"], ObjectId)
            return True
        qcheck.check_unittest(self, incoming_adds_id,
                              qcheck.gen_mongo_dict(3))

        def outgoing_is_identity(son):
            return son == manip.transform_outgoing(son, collection)
        qcheck.check_unittest(self, outgoing_is_identity,
                              qcheck.gen_mongo_dict(3))

    def test_ns_injection(self):
        manip = NamespaceInjector(self.db)
        collection = self.db.test

        def incoming_adds_ns(son):
            son = manip.transform_incoming(son, collection)
            assert "_ns" in son
            return son["_ns"] == collection.name()
        qcheck.check_unittest(self, incoming_adds_ns,
                              qcheck.gen_mongo_dict(3))

        def outgoing_is_identity(son):
            return son == manip.transform_outgoing(son, collection)
        qcheck.check_unittest(self, outgoing_is_identity,
                              qcheck.gen_mongo_dict(3))

if __name__ == "__main__":
    unittest.main()
