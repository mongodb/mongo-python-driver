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

import time
import unittest

from mockupdb import MockupDB, wait_until

from pymongo import MongoClient


class TestInitialIsMaster(unittest.TestCase):
    def test_initial_ismaster(self):
        server = MockupDB()
        server.run()
        self.addCleanup(server.stop)

        start = time.time()
        client = MongoClient(server.uri)
        self.addCleanup(client.close)

        # A single ismaster is enough for the client to be connected.
        self.assertFalse(client.nodes)
        server.receives("ismaster").ok(ismaster=True, minWireVersion=2, maxWireVersion=6)
        wait_until(lambda: client.nodes, "update nodes", timeout=1)

        # At least 10 seconds before next heartbeat.
        server.receives("ismaster").ok(ismaster=True, minWireVersion=2, maxWireVersion=6)
        self.assertGreaterEqual(time.time() - start, 10)


if __name__ == "__main__":
    unittest.main()
