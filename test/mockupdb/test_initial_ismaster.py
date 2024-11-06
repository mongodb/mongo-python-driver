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
from __future__ import annotations

import time
import unittest
from test import PyMongoTestCase

import pytest

try:
    from mockupdb import MockupDB, wait_until

    _HAVE_MOCKUPDB = True
except ImportError:
    _HAVE_MOCKUPDB = False


from pymongo.common import MIN_SUPPORTED_WIRE_VERSION

pytestmark = pytest.mark.mockupdb


class TestInitialIsMaster(PyMongoTestCase):
    def test_initial_ismaster(self):
        server = MockupDB()
        server.run()
        self.addCleanup(server.stop)

        start = time.time()
        client = self.simple_client(server.uri)

        # A single ismaster is enough for the client to be connected.
        self.assertFalse(client.nodes)
        server.receives("ismaster").ok(
            ismaster=True, minWireVersion=2, maxWireVersion=MIN_SUPPORTED_WIRE_VERSION
        )
        wait_until(lambda: client.nodes, "update nodes", timeout=1)

        # At least 10 seconds before next heartbeat.
        server.receives("ismaster").ok(
            ismaster=True, minWireVersion=2, maxWireVersion=MIN_SUPPORTED_WIRE_VERSION
        )
        self.assertGreaterEqual(time.time() - start, 10)


if __name__ == "__main__":
    unittest.main()
