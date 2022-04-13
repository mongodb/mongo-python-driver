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

"""Test PyMongo's SlaveOkay with a replica set connection.

Just make sure SlaveOkay is *not* set on primary reads.
"""

import unittest

from mockupdb import MockupDB, going
from operations import operations

from pymongo import MongoClient


class TestSlaveOkayRS(unittest.TestCase):
    def setup_server(self):
        self.primary, self.secondary = MockupDB(), MockupDB()
        for server in self.primary, self.secondary:
            server.run()
            self.addCleanup(server.stop)

        hosts = [server.address_string for server in (self.primary, self.secondary)]
        self.primary.autoresponds(
            "ismaster", ismaster=True, setName="rs", hosts=hosts, minWireVersion=2, maxWireVersion=6
        )
        self.secondary.autoresponds(
            "ismaster",
            ismaster=False,
            secondary=True,
            setName="rs",
            hosts=hosts,
            minWireVersion=2,
            maxWireVersion=6,
        )


def create_slave_ok_rs_test(operation):
    def test(self):
        self.setup_server()
        assert not operation.op_type == "always-use-secondary"

        client = MongoClient(self.primary.uri, replicaSet="rs")
        self.addCleanup(client.close)
        with going(operation.function, client):
            request = self.primary.receive()
            request.reply(operation.reply)

        self.assertFalse(request.slave_ok, 'SlaveOkay set read mode "primary"')

    return test


def generate_slave_ok_rs_tests():
    for operation in operations:
        # Don't test secondary operations with MockupDB, the server enforces the
        # SlaveOkay bit so integration tests prove we set it.
        if operation.op_type == "always-use-secondary":
            continue
        test = create_slave_ok_rs_test(operation)

        test_name = "test_%s" % operation.name.replace(" ", "_")
        test.__name__ = test_name
        setattr(TestSlaveOkayRS, test_name, test)


generate_slave_ok_rs_tests()


if __name__ == "__main__":
    unittest.main()
