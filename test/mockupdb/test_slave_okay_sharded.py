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

"""Test PyMongo's SlaveOkay with:

- A direct connection to a standalone.
- A direct connection to a slave.
- A direct connection to a mongos.
"""
import itertools
import unittest
from queue import Queue

from mockupdb import MockupDB, going
from operations import operations

from pymongo import MongoClient
from pymongo.read_preferences import make_read_preference, read_pref_mode_from_name


class TestSlaveOkaySharded(unittest.TestCase):
    def setup_server(self):
        self.mongos1, self.mongos2 = MockupDB(), MockupDB()

        # Collect queries to either server in one queue.
        self.q: Queue = Queue()
        for server in self.mongos1, self.mongos2:
            server.subscribe(self.q.put)
            server.run()
            self.addCleanup(server.stop)
            server.autoresponds(
                "ismaster", minWireVersion=2, maxWireVersion=6, ismaster=True, msg="isdbgrid"
            )

        self.mongoses_uri = "mongodb://%s,%s" % (
            self.mongos1.address_string,
            self.mongos2.address_string,
        )


def create_slave_ok_sharded_test(mode, operation):
    def test(self):
        self.setup_server()
        if operation.op_type == "always-use-secondary":
            slave_ok = True
        elif operation.op_type == "may-use-secondary":
            slave_ok = mode != "primary"
        elif operation.op_type == "must-use-primary":
            slave_ok = False
        else:
            assert False, "unrecognized op_type %r" % operation.op_type

        pref = make_read_preference(read_pref_mode_from_name(mode), tag_sets=None)

        client = MongoClient(self.mongoses_uri, read_preference=pref)
        self.addCleanup(client.close)
        with going(operation.function, client):
            request = self.q.get(timeout=1)
            request.reply(operation.reply)

        if slave_ok:
            self.assertTrue(request.slave_ok, "SlaveOkay not set")
        else:
            self.assertFalse(request.slave_ok, "SlaveOkay set")

    return test


def generate_slave_ok_sharded_tests():
    modes = "primary", "secondary", "nearest"
    matrix = itertools.product(modes, operations)

    for entry in matrix:
        mode, operation = entry
        test = create_slave_ok_sharded_test(mode, operation)
        test_name = "test_%s_with_mode_%s" % (operation.name.replace(" ", "_"), mode)

        test.__name__ = test_name
        setattr(TestSlaveOkaySharded, test_name, test)


generate_slave_ok_sharded_tests()

if __name__ == "__main__":
    unittest.main()
