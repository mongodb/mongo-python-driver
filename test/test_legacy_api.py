# Copyright 2015-present MongoDB, Inc.
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

"""Test various legacy / deprecated API features."""

import sys

sys.path[0:0] = [""]

from pymongo import ASCENDING, GEOHAYSTACK
from pymongo.operations import IndexModel
from test import unittest
from test.test_client import IntegrationTest
from test.utils import DeprecationFilter


class TestDeprecations(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        super(TestDeprecations, cls).setUpClass()
        cls.deprecation_filter = DeprecationFilter("error")

    @classmethod
    def tearDownClass(cls):
        cls.deprecation_filter.stop()

    def test_geoHaystack_deprecation(self):
        self.addCleanup(self.db.test.drop)
        keys = [("pos", GEOHAYSTACK), ("type", ASCENDING)]
        self.assertRaises(
            DeprecationWarning, self.db.test.create_index, keys, bucketSize=1)
        indexes = [IndexModel(keys, bucketSize=1)]
        self.assertRaises(
            DeprecationWarning, self.db.test.create_indexes, indexes)


if __name__ == "__main__":
    unittest.main()
