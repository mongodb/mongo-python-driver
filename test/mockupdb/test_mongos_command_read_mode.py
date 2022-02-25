# Copyright 2015 MongoDB, Inc.
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

import itertools
import unittest

from mockupdb import MockupDB, OpMsg, going
from operations import operations

from pymongo import MongoClient, ReadPreference
from pymongo.read_preferences import (
    _MONGOS_MODES,
    make_read_preference,
    read_pref_mode_from_name,
)


class TestMongosCommandReadMode(unittest.TestCase):
    def test_aggregate(self):
        server = MockupDB()
        server.autoresponds(
            "ismaster", ismaster=True, msg="isdbgrid", minWireVersion=2, maxWireVersion=6
        )
        self.addCleanup(server.stop)
        server.run()

        client = MongoClient(server.uri)
        self.addCleanup(client.close)
        collection = client.test.collection
        with going(collection.aggregate, []):
            command = server.receives(aggregate="collection", pipeline=[])
            self.assertFalse(command.slave_ok, "SlaveOkay set")
            command.ok(result=[{}])

        secondary_collection = collection.with_options(read_preference=ReadPreference.SECONDARY)

        with going(secondary_collection.aggregate, []):

            command = server.receives(
                OpMsg(
                    {
                        "aggregate": "collection",
                        "pipeline": [],
                        "$readPreference": {"mode": "secondary"},
                    }
                )
            )
            command.ok(result=[{}])
            self.assertTrue(command.slave_ok, "SlaveOkay not set")


def create_mongos_read_mode_test(mode, operation):
    def test(self):
        server = MockupDB()
        self.addCleanup(server.stop)
        server.run()
        server.autoresponds(
            "ismaster", ismaster=True, msg="isdbgrid", minWireVersion=2, maxWireVersion=6
        )

        pref = make_read_preference(read_pref_mode_from_name(mode), tag_sets=None)

        client = MongoClient(server.uri, read_preference=pref)
        self.addCleanup(client.close)

        with going(operation.function, client):
            request = server.receive()
            request.reply(operation.reply)

        if operation.op_type == "always-use-secondary":
            self.assertEqual(ReadPreference.SECONDARY.document, request.doc.get("$readPreference"))
            slave_ok = mode != "primary"
        elif operation.op_type == "must-use-primary":
            slave_ok = False
        elif operation.op_type == "may-use-secondary":
            slave_ok = mode != "primary"
            actual_pref = request.doc.get("$readPreference")
            if mode == "primary":
                self.assertIsNone(actual_pref)
            else:
                self.assertEqual(pref.document, actual_pref)
        else:
            self.fail("unrecognized op_type %r" % operation.op_type)

        if slave_ok:
            self.assertTrue(request.slave_ok, "SlaveOkay not set")
        else:
            self.assertFalse(request.slave_ok, "SlaveOkay set")

    return test


def generate_mongos_read_mode_tests():
    matrix = itertools.product(_MONGOS_MODES, operations)

    for entry in matrix:
        mode, operation = entry
        if mode == "primary" and operation.op_type == "always-use-secondary":
            # Skip something like command('foo', read_preference=SECONDARY).
            continue
        test = create_mongos_read_mode_test(mode, operation)
        test_name = "test_%s_with_mode_%s" % (operation.name.replace(" ", "_"), mode)
        test.__name__ = test_name
        setattr(TestMongosCommandReadMode, test_name, test)


generate_mongos_read_mode_tests()


if __name__ == "__main__":
    unittest.main()
