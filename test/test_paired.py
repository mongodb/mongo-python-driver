# Copyright 2009-2010 10gen, Inc.
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
paired setup. To run the tests just run this file manually.

Left and right nodes will be $DB_IP:$DB_PORT and $DB_IP2:$DB_PORT2 or
localhost:27017 and localhost:27018 by default.
"""

import unittest
import logging
import os
import sys
import warnings
sys.path[0:0] = [""]

from pymongo.errors import ConnectionFailure, ConfigurationError
from pymongo.connection import Connection

skip_tests = True


class TestPaired(unittest.TestCase):

    def setUp(self):
        left_host = os.environ.get("DB_IP", "localhost")
        left_port = int(os.environ.get("DB_PORT", 27017))
        self.left = (left_host, left_port)
        right_host = os.environ.get("DB_IP2", "localhost")
        right_port = int(os.environ.get("DB_PORT2", 27018))
        self.right = (right_host, right_port)
        self.bad = ("somedomainthatdoesntexist.org", 12345)

    def tearDown(self):
        pass

    def skip(self):
        if skip_tests:
            from nose.plugins.skip import SkipTest
            raise SkipTest()

    def test_types(self):
        self.skip()
        self.assertRaises(TypeError, Connection.paired, 5)
        self.assertRaises(TypeError, Connection.paired, "localhost")
        self.assertRaises(TypeError, Connection.paired, None)
        self.assertRaises(TypeError, Connection.paired, 5, self.right)
        self.assertRaises(TypeError, Connection.paired,
                          "localhost", self.right)
        self.assertRaises(TypeError, Connection.paired, None, self.right)
        self.assertRaises(TypeError, Connection.paired, self.left, 5)
        self.assertRaises(TypeError, Connection.paired, self.left, "localhost")
        self.assertRaises(TypeError, Connection.paired, self.left, "localhost")

    def test_connect(self):
        self.skip()
        self.assertRaises(ConnectionFailure, Connection.paired,
                          self.bad, self.bad)

        connection = Connection.paired(self.left, self.right)
        self.assert_(connection)

        host = connection.host
        port = connection.port

        connection = Connection.paired(self.right, self.left)
        self.assert_(connection)
        self.assertEqual(host, connection.host)
        self.assertEqual(port, connection.port)

        slave = self.left == (host, port) and self.right or self.left
        self.assertRaises(ConfigurationError, Connection.paired,
                          slave, self.bad)
        self.assertRaises(ConfigurationError, Connection.paired,
                          self.bad, slave)

    def test_repr(self):
        self.skip()
        connection = Connection.paired(self.left, self.right)

        self.assertEqual(repr(connection),
                         "Connection.paired(('%s', %s), ('%s', %s))" %
                         (self.left[0],
                          self.left[1],
                          self.right[0],
                          self.right[1]))

    def test_basic(self):
        self.skip()
        connection = Connection.paired(self.left, self.right)

        db = connection.pymongo_test

        db.drop_collection("test")
        a = {"x": 1}
        db.test.save(a)
        self.assertEqual(a, db.test.find_one())

    def test_end_request(self):
        self.skip()
        connection = Connection.paired(self.left, self.right)
        db = connection.pymongo_test

        for _ in range(100):
            db.test.remove({})
            db.test.insert({})
            self.assert_(db.test.find_one())
            connection.end_request()


    def test_deprecation_warnings_paired_connections(self):
        warnings.simplefilter("error")
        try:
            self.assertRaises(DeprecationWarning, Connection.paired, 
                              self.left, self.right, timeout=3)
            self.assertRaises(DeprecationWarning, Connection.paired, 
                              self.left, self.right, auto_start_request=True)
            self.assertRaises(DeprecationWarning, Connection.paired, 
                              self.left, self.right, pool_size=20)
        finally:
            warnings.resetwarnings()
            warnings.simplefilter('ignore')


    def test_paired_connections_pass_individual_connargs(self):
        c = Connection.paired(self.left, self.right, slave_okay=True)
        self.assertTrue(c.__slave_okay)



if __name__ == "__main__":
    skip_tests = False
    unittest.main()
