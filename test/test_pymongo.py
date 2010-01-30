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

"""Test the pymongo module itself."""

import unittest
import os
import sys
sys.path[0:0] = [""]

import pymongo


class TestPyMongo(unittest.TestCase):

    def setUp(self):
        self.host = os.environ.get("DB_IP", "localhost")
        self.port = int(os.environ.get("DB_PORT", 27017))

    def test_connection_alias(self):
        c = pymongo.Connection(self.host, self.port)
        self.assert_(c)
        self.assertEqual(c.host, self.host)
        self.assertEqual(c.port, self.port)

if __name__ == "__main__":
    unittest.main()
