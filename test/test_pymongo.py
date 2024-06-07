# Copyright 2009-2015 MongoDB, Inc.
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
from __future__ import annotations

import sys

sys.path[0:0] = [""]

from test import unittest

import pymongo
from pymongo._version import get_version_tuple


class TestPyMongo(unittest.TestCase):
    def test_mongo_client_alias(self):
        # Testing that pymongo module imports mongo_client.MongoClient
        self.assertEqual(pymongo.MongoClient, pymongo.mongo_client.MongoClient)

    def test_get_version_tuple(self):
        self.assertEqual(get_version_tuple("4.8.0.dev1"), (4, 8, 0, ".dev1"))
        self.assertEqual(get_version_tuple("4.8.1"), (4, 8, 1))
        self.assertEqual(get_version_tuple("5.0.0rc1"), (5, 0, 0, "rc1"))
        self.assertEqual(get_version_tuple("5.0"), (5, 0))
        with self.assertRaises(ValueError):
            get_version_tuple("5")


if __name__ == "__main__":
    unittest.main()
