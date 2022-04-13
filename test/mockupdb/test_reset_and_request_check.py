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
import time
import unittest

from mockupdb import MockupDB, going, wait_until
from operations import operations

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.server_type import SERVER_TYPE


class TestResetAndRequestCheck(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestResetAndRequestCheck, self).__init__(*args, **kwargs)
        self.ismaster_time = 0.0
        self.client = None
        self.server = None

    def setup_server(self):
        self.server = MockupDB()

        def responder(request):
            self.ismaster_time = time.time()
            return request.ok(ismaster=True, minWireVersion=2, maxWireVersion=6)

        self.server.autoresponds("ismaster", responder)
        self.server.run()
        self.addCleanup(self.server.stop)

        kwargs = {"socketTimeoutMS": 100}
        # Disable retryable reads when pymongo supports it.
        kwargs["retryReads"] = False
        self.client = MongoClient(self.server.uri, **kwargs)  # type: ignore
        wait_until(lambda: self.client.nodes, "connect to standalone")

    def tearDown(self):
        if hasattr(self, "client") and self.client:
            self.client.close()

    def _test_disconnect(self, operation):
        # Application operation fails. Test that client resets server
        # description and does *not* schedule immediate check.
        self.setup_server()
        assert self.server is not None
        assert self.client is not None

        # Network error on application operation.
        with self.assertRaises(ConnectionFailure):
            with going(operation.function, self.client):
                self.server.receives().hangup()

        # Server is Unknown.
        topology = self.client._topology
        with self.assertRaises(ConnectionFailure):
            topology.select_server_by_address(self.server.address, 0)

        time.sleep(0.5)
        after = time.time()

        # Demand a reconnect.
        with going(self.client.db.command, "buildinfo"):
            self.server.receives("buildinfo").ok()

        last = self.ismaster_time
        self.assertGreaterEqual(last, after, "called ismaster before needed")

    def _test_timeout(self, operation):
        # Application operation times out. Test that client does *not* reset
        # server description and does *not* schedule immediate check.
        self.setup_server()
        assert self.server is not None
        assert self.client is not None

        with self.assertRaises(ConnectionFailure):
            with going(operation.function, self.client):
                self.server.receives()
                before = self.ismaster_time
                time.sleep(0.5)

        # Server is *not* Unknown.
        topology = self.client._topology
        server = topology.select_server_by_address(self.server.address, 0)
        assert server is not None
        self.assertEqual(SERVER_TYPE.Standalone, server.description.server_type)

        after = self.ismaster_time
        self.assertEqual(after, before, "unneeded ismaster call")

    def _test_not_master(self, operation):
        # Application operation gets a "not master" error.
        self.setup_server()
        assert self.server is not None
        assert self.client is not None

        with self.assertRaises(ConnectionFailure):
            with going(operation.function, self.client):
                request = self.server.receives()
                before = self.ismaster_time
                request.replies(operation.not_master)
                time.sleep(1)

        # Server is rediscovered.
        topology = self.client._topology
        server = topology.select_server_by_address(self.server.address, 0)
        assert server is not None
        self.assertEqual(SERVER_TYPE.Standalone, server.description.server_type)

        after = self.ismaster_time
        self.assertGreater(after, before, "ismaster not called")


def create_reset_test(operation, test_method):
    def test(self):
        test_method(self, operation)

    return test


def generate_reset_tests():
    test_methods = [
        (TestResetAndRequestCheck._test_disconnect, "test_disconnect"),
        (TestResetAndRequestCheck._test_timeout, "test_timeout"),
        (TestResetAndRequestCheck._test_not_master, "test_not_master"),
    ]

    matrix = itertools.product(operations, test_methods)

    for entry in matrix:
        operation, (test_method, name) = entry
        test = create_reset_test(operation, test_method)
        test_name = "%s_%s" % (name, operation.name.replace(" ", "_"))
        test.__name__ = test_name
        setattr(TestResetAndRequestCheck, test_name, test)


generate_reset_tests()

if __name__ == "__main__":
    unittest.main()
