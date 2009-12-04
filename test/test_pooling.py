# Copyright 2009 10gen, Inc.
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

"""Test built in connection-pooling."""

import unittest
import threading
import os
import random
import sys
sys.path[0:0] = [""]

from pymongo.connection import Connection


class TestPooling(unittest.TestCase):

    def setUp(self):
        self.host = os.environ.get("DB_IP", "localhost")
        self.port = int(os.environ.get("DB_PORT", 27017))
        default_connection = Connection(self.host, self.port)
        no_auto_connection = Connection(self.host, self.port,
                                        auto_start_request=False)
        pooled_connection = Connection(self.host, self.port,
                                       pool_size=10, timeout=-1)
        no_auto_pooled_connection = Connection(self.host, self.port,
                                               pool_size=10, timeout=-1,
                                               auto_start_request=False)
        self.default_db = default_connection["pymongo_test"]
        self.pooled_db = pooled_connection["pymongo_test"]
        self.no_auto_db = no_auto_connection["pymongo_test"]
        self.no_auto_pooled_db = no_auto_pooled_connection["pymongo_test"]

    def test_exceptions(self):
        self.assertRaises(TypeError, Connection, self.host, self.port,
                          pool_size="one")
        self.assertRaises(TypeError, Connection, self.host, self.port,
                          pool_size=[])
        self.assertRaises(ValueError, Connection, self.host, self.port,
                          pool_size=-10)
        self.assertRaises(ValueError, Connection, self.host, self.port,
                          pool_size=0)

    def test_constants(self):
        Connection.POOL_SIZE = -1
        self.assertRaises(ValueError, Connection, self.host, self.port)

        Connection.POOL_SIZE = 1
        self.assert_(Connection(self.host, self.port))


    # NOTE this test is non-deterministic
    def test_end_request(self):
        count = 0
        for _ in range(100):
            self.default_db.test.remove({})
            self.default_db.test.insert({})
            if not self.default_db.test.find_one():
                count += 1
        self.assertEqual(0, count)

        count = 0
        for _ in range(100):
            self.default_db.test.remove({})
            self.default_db.test.insert({})
            self.default_db.connection.end_request()
            if not self.default_db.test.find_one():
                count += 1
        self.assertEqual(0, count)

        count = 0
        for _ in range(100):
            self.pooled_db.test.remove({})
            self.pooled_db.test.insert({})
            if not self.pooled_db.test.find_one():
                count += 1
        self.assertEqual(0, count)

# TODO better way to test this?
#         count = 0
#         for _ in range(6000):
#             self.pooled_db.test.remove({})
#             self.pooled_db.test.insert({})
#             self.pooled_db.connection.end_request()
#             if not self.pooled_db.test.find_one():
#                 count += 1
#         self.assertNotEqual(0, count)

    # NOTE this test is non-deterministic
    def test_no_auto_start_request(self):
        count = 0
        for _ in range(100):
            self.no_auto_db.test.remove({})
            self.no_auto_db.test.insert({})
            if not self.no_auto_db.test.find_one():
                count += 1
        self.assertEqual(0, count)

# TODO better way to test this?
#         count = 0
#         for _ in range(6000):
#             self.no_auto_pooled_db.test.remove({})
#             self.no_auto_pooled_db.test.insert({})
#             if not self.no_auto_pooled_db.test.find_one():
#                 count += 1
#         self.assertNotEqual(0, count)

        count = 0
        for _ in range(100):
            self.no_auto_db.connection.start_request()
            self.no_auto_db.test.remove({})
            self.no_auto_db.test.insert({})
            if not self.no_auto_db.test.find_one():
                count += 1
            self.no_auto_db.connection.end_request()
        self.assertEqual(0, count)

        count = 0
        for _ in range(100):
            self.no_auto_pooled_db.connection.start_request()
            self.no_auto_pooled_db.test.remove({})
            self.no_auto_pooled_db.test.insert({})
            if not self.no_auto_pooled_db.test.find_one():
                count += 1
            self.no_auto_pooled_db.connection.end_request()
        self.assertEqual(0, count)

    def test_multithread(self):
        self.pooled_db.mt_test.remove({})
        for _ in range(20):
            t = SaveAndFind(self.pooled_db)
            t.start()


class SaveAndFind(threading.Thread):

    def __init__(self, database):
        threading.Thread.__init__(self)
        self.database = database

    def run(self):
        for _ in xrange(100):
            rand = random.randint(0, 100)
            id = self.database.mt_test.save({"x": rand})
            assert self.database.mt_test.find_one(id)["x"] == rand
            self.database.connection.end_request()

if __name__ == "__main__":
    unittest.main()
