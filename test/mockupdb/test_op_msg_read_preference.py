# Copyright 2018-present MongoDB, Inc.
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

import copy
import itertools
import unittest
from typing import Any

from mockupdb import CommandBase, MockupDB, going
from operations import operations

from pymongo import MongoClient, ReadPreference
from pymongo.read_preferences import (
    _MONGOS_MODES,
    make_read_preference,
    read_pref_mode_from_name,
)


class OpMsgReadPrefBase(unittest.TestCase):
    single_mongod = False
    primary: MockupDB
    secondary: MockupDB

    @classmethod
    def setUpClass(cls):
        super(OpMsgReadPrefBase, cls).setUpClass()

    @classmethod
    def add_test(cls, mode, test_name, test):
        setattr(cls, test_name, test)

    def setup_client(self, read_preference):
        client = MongoClient(self.primary.uri, read_preference=read_preference)
        self.addCleanup(client.close)
        return client


class TestOpMsgMongos(OpMsgReadPrefBase):
    @classmethod
    def setUpClass(cls):
        super(TestOpMsgMongos, cls).setUpClass()
        auto_ismaster = {
            "ismaster": True,
            "msg": "isdbgrid",  # Mongos.
            "minWireVersion": 2,
            "maxWireVersion": 6,
        }
        cls.primary = MockupDB(auto_ismaster=auto_ismaster)
        cls.primary.run()
        cls.secondary = cls.primary

    @classmethod
    def tearDownClass(cls):
        cls.primary.stop()
        super(TestOpMsgMongos, cls).tearDownClass()


class TestOpMsgReplicaSet(OpMsgReadPrefBase):
    @classmethod
    def setUpClass(cls):
        super(TestOpMsgReplicaSet, cls).setUpClass()
        cls.primary, cls.secondary = MockupDB(), MockupDB()
        for server in cls.primary, cls.secondary:
            server.run()

        hosts = [server.address_string for server in (cls.primary, cls.secondary)]

        primary_ismaster = {
            "ismaster": True,
            "setName": "rs",
            "hosts": hosts,
            "minWireVersion": 2,
            "maxWireVersion": 6,
        }
        cls.primary.autoresponds(CommandBase("ismaster"), primary_ismaster)
        secondary_ismaster = copy.copy(primary_ismaster)
        secondary_ismaster["ismaster"] = False
        secondary_ismaster["secondary"] = True
        cls.secondary.autoresponds(CommandBase("ismaster"), secondary_ismaster)

    @classmethod
    def tearDownClass(cls):
        for server in cls.primary, cls.secondary:
            server.stop()
        super(TestOpMsgReplicaSet, cls).tearDownClass()

    @classmethod
    def add_test(cls, mode, test_name, test):
        # Skip nearest tests since we don't know if we will select the primary
        # or secondary.
        if mode != "nearest":
            setattr(cls, test_name, test)

    def setup_client(self, read_preference):
        client = MongoClient(self.primary.uri, replicaSet="rs", read_preference=read_preference)

        # Run a command on a secondary to discover the topology. This ensures
        # that secondaryPreferred commands will select the secondary.
        client.admin.command("ismaster", read_preference=ReadPreference.SECONDARY)
        self.addCleanup(client.close)
        return client


class TestOpMsgSingle(OpMsgReadPrefBase):
    single_mongod = True

    @classmethod
    def setUpClass(cls):
        super(TestOpMsgSingle, cls).setUpClass()
        auto_ismaster = {
            "ismaster": True,
            "minWireVersion": 2,
            "maxWireVersion": 6,
        }
        cls.primary = MockupDB(auto_ismaster=auto_ismaster)
        cls.primary.run()
        cls.secondary = cls.primary

    @classmethod
    def tearDownClass(cls):
        cls.primary.stop()
        super(TestOpMsgSingle, cls).tearDownClass()


def create_op_msg_read_mode_test(mode, operation):
    def test(self):
        pref = make_read_preference(read_pref_mode_from_name(mode), tag_sets=None)

        client = self.setup_client(read_preference=pref)
        expected_pref: Any
        if operation.op_type == "always-use-secondary":
            expected_server = self.secondary
            expected_pref = ReadPreference.SECONDARY
        elif operation.op_type == "must-use-primary":
            expected_server = self.primary
            expected_pref = None
        elif operation.op_type == "may-use-secondary":
            if mode == "primary":
                expected_server = self.primary
                expected_pref = None
            elif mode == "primaryPreferred":
                expected_server = self.primary
                expected_pref = pref
            else:
                expected_server = self.secondary
                expected_pref = pref
        else:
            self.fail("unrecognized op_type %r" % operation.op_type)
        # For single mongod we omit the read preference.
        if self.single_mongod:
            expected_pref = None
        with going(operation.function, client):
            request = expected_server.receive()
            request.reply(operation.reply)

        actual_pref = request.doc.get("$readPreference")
        if expected_pref:
            self.assertEqual(expected_pref.document, actual_pref)
        else:
            self.assertIsNone(actual_pref)
        self.assertNotIn("$query", request.doc)

    return test


def generate_op_msg_read_mode_tests():
    matrix = itertools.product(_MONGOS_MODES, operations)

    for entry in matrix:
        mode, operation = entry
        test = create_op_msg_read_mode_test(mode, operation)
        test_name = "test_%s_with_mode_%s" % (operation.name.replace(" ", "_"), mode)
        test.__name__ = test_name
        for cls in TestOpMsgMongos, TestOpMsgReplicaSet, TestOpMsgSingle:
            cls.add_test(mode, test_name, test)


generate_op_msg_read_mode_tests()


if __name__ == "__main__":
    unittest.main()
