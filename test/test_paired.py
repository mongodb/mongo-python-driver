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
import logging
import os
import sys
import warnings
sys.path[0:0] = [""]

from pymongo.errors import ConnectionFailure
from pymongo.connection import Connection

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
        self.assertRaises(ConnectionFailure, Connection,
                          [self.bad, self.bad])

        connection = Connection([self.left, self.right])
        self.assertTrue(connection)

        host = connection.host
        port = connection.port

        connection = Connection([self.right, self.left])
        self.assertTrue(connection)
        self.assertEqual(host, connection.host)
        self.assertEqual(port, connection.port)

        slave = self.left == (host, port) and self.right or self.left
        self.assertRaises(ConnectionFailure, Connection,
                          [slave, self.bad])
        self.assertRaises(ConnectionFailure, Connection,
                          [self.bad, slave])

    def test_repr(self):
        self.skip()
        connection = Connection([self.left, self.right])

        self.assertEqual(repr(connection),
                         "Connection(['%s', '%s'])" %
                         (self.left, self.right))

    def test_basic(self):
        self.skip()
        connection = Connection([self.left, self.right])

        db = connection.pymongo_test

        db.drop_collection("test")
        a = {"x": 1}
        db.test.save(a)
        self.assertEqual(a, db.test.find_one())

    def test_end_request(self):
        self.skip()
        connection = Connection([self.left, self.right])
        db = connection.pymongo_test

        for _ in range(100):
            db.test.remove({})
            db.test.insert({})
            self.assertTrue(db.test.find_one())
            connection.end_request()


if __name__ == "__main__":
    skip_tests = False
    unittest.main()
