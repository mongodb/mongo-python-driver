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

"""Test PyMongo with a mixed-version cluster."""

import time
import unittest
from queue import Queue

from mockupdb import MockupDB, go
from operations import upgrades

from pymongo import MongoClient


class TestMixedVersionSharded(unittest.TestCase):
    def setup_server(self, upgrade):
        self.mongos_old, self.mongos_new = MockupDB(), MockupDB()

        # Collect queries to either server in one queue.
        self.q: Queue = Queue()
        for server in self.mongos_old, self.mongos_new:
            server.subscribe(self.q.put)
            server.autoresponds("getlasterror")
            server.run()
            self.addCleanup(server.stop)

        # Max wire version is too old for the upgraded operation.
        self.mongos_old.autoresponds(
            "ismaster", ismaster=True, msg="isdbgrid", maxWireVersion=upgrade.wire_version - 1
        )

        # Up-to-date max wire version.
        self.mongos_new.autoresponds(
            "ismaster", ismaster=True, msg="isdbgrid", maxWireVersion=upgrade.wire_version
        )

        self.mongoses_uri = "mongodb://%s,%s" % (
            self.mongos_old.address_string,
            self.mongos_new.address_string,
        )

        self.client = MongoClient(self.mongoses_uri)

    def tearDown(self):
        if hasattr(self, "client") and self.client:
            self.client.close()


def create_mixed_version_sharded_test(upgrade):
    def test(self):
        self.setup_server(upgrade)
        start = time.time()
        servers_used: set = set()
        while len(servers_used) < 2:
            go(upgrade.function, self.client)
            request = self.q.get(timeout=1)
            servers_used.add(request.server)
            request.assert_matches(
                upgrade.old if request.server is self.mongos_old else upgrade.new
            )
            if time.time() > start + 10:
                self.fail("never used both mongoses")

    return test


def generate_mixed_version_sharded_tests():
    for upgrade in upgrades:
        test = create_mixed_version_sharded_test(upgrade)
        test_name = "test_%s" % upgrade.name.replace(" ", "_")
        test.__name__ = test_name
        setattr(TestMixedVersionSharded, test_name, test)


generate_mixed_version_sharded_tests()

if __name__ == "__main__":
    unittest.main()
