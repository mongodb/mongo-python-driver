# Copyright 2009-2014 MongoDB, Inc.
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

"""Test the pymongo module itself."""

import unittest
import os
import sys
sys.path[0:0] = [""]

import pymongo
from test import host, port

class TestPyMongo(unittest.TestCase):
    def test_mongo_client_alias(self):
        # Testing that pymongo module imports mongo_client.MongoClient
        c = pymongo.MongoClient(host, port)
        self.assertEqual(c.host, host)
        self.assertEqual(c.port, port)


if __name__ == "__main__":
    unittest.main()
