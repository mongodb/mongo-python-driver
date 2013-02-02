# Copyright 2009-2012 10gen, Inc.
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

"""Test pairing support.

These tests are skipped by nose by default (since they depend on having a
paired setup. To run the tests just run this file manually).

Left and right nodes will be $DB_IP:$DB_PORT and $DB_IP2:$DB_PORT2 or
localhost:27017 and localhost:27018 by default.
"""

import unittest
import os
import sys
sys.path[0:0] = [""]

from pymongo.errors import ConnectionFailure
from pymongo.mongo_client import MongoClient

skip_tests = True


class TestPaired(unittest.TestCase):

    def setUp(self):
        left_host = os.environ.get("DB_IP", "localhost")
        left_port = int(os.environ.get("DB_PORT", 27017))
        self.left = "%s:%s" % (left_host, left_port)
        right_host = os.environ.get("DB_IP2", "localhost")
        right_port = int(os.environ.get("DB_PORT2", 27018))
        self.right = "%s:%s" % (right_host, right_port)
        self.bad = "%s:%s" % ("somedomainthatdoesntexist.org", 12345)

    def tearDown(self):
        pass

    def skip(self):
        if skip_tests:
            from nose.plugins.skip import SkipTest
            raise SkipTest()

    def test_connect(self):
        self.skip()

        # Use a timeout so the test is reasonably fast
        self.assertRaises(ConnectionFailure, MongoClient,
                          [self.bad, self.bad], connectTimeoutMS=500)

        client = MongoClient([self.left, self.right])
        self.assertTrue(client)

        host = client.host
        port = client.port

        client = MongoClient([self.right, self.left], connectTimeoutMS=500)
        self.assertTrue(client)
        self.assertEqual(host, client.host)
        self.assertEqual(port, client.port)

        if self.left == '%s:%s' % (host, port):
            slave = self.right
        else:
            slave = self.left

        # Refuse to connect if master absent
        self.assertRaises(ConnectionFailure, MongoClient,
                          [slave, self.bad], connectTimeoutMS=500)
        self.assertRaises(ConnectionFailure, MongoClient,
                          [self.bad, slave], connectTimeoutMS=500)

        # No error
        MongoClient([self.left, self.bad], connectTimeoutMS=500)

    def test_basic(self):
        self.skip()
        client = MongoClient([self.left, self.right])

        db = client.pymongo_test

        db.drop_collection("test")
        a = {"x": 1}
        db.test.save(a)
        self.assertEqual(a, db.test.find_one())

    def test_end_request(self):
        self.skip()
        client = MongoClient([self.left, self.right])
        db = client.pymongo_test

        for _ in range(100):
            db.test.remove({})
            db.test.insert({})
            self.assertTrue(db.test.find_one())
            client.end_request()


if __name__ == "__main__":
    skip_tests = False
    unittest.main()
