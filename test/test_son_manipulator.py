"""Tests for SONManipulators.
"""

import unittest

import qcheck
from objectid import ObjectId
from son_manipulator import SONManipulator, ObjectIdInjector, NamespaceInjector
from database import Database
from test_connection import get_connection

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
