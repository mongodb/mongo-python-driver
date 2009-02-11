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

from pymongo.connection import Connection

class TestPooling(unittest.TestCase):
    def setUp(self):
        self.host = os.environ.get("DB_IP", "localhost")
        self.port = int(os.environ.get("DB_PORT", 27017))
        default_connection = Connection(self.host, self.port)
        pooled_connection = Connection(self.host, self.port, 10)
        self.default_db = default_connection["pymongo_test"]
        self.pooled_db = pooled_connection["pymongo_test"]

    def test_exceptions(self):
        self.assertRaises(TypeError, Connection, self.host, self.port, None)
        self.assertRaises(TypeError, Connection, self.host, self.port, "one")
        self.assertRaises(TypeError, Connection, self.host, self.port, [])
        self.assertRaises(ValueError, Connection, self.host, self.port, -10)
        self.assertRaises(ValueError, Connection, self.host, self.port, 0)

# TODO more tests for this!
# test auth support
# test request support
# test pools of size 1 and large pools
# test multithreaded stuff

if __name__ == "__main__":
    unittest.main()
