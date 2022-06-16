# Copyright 2022-present MongoDB, Inc.
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

"""Test the CSOT unified spec tests."""

import os
import sys

sys.path[0:0] = [""]

from test import IntegrationTest, unittest
from test.unified_format import generate_test_classes

import pymongo
from pymongo import _csot

# Location of JSON test specifications.
TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "csot")

# Generate unified tests.
globals().update(generate_test_classes(TEST_PATH, module=__name__))


class TestCSOT(IntegrationTest):
    def test_timeout_nested(self):
        coll = self.db.coll
        self.assertEqual(_csot.get_timeout(), None)
        self.assertEqual(_csot.get_deadline(), float("inf"))
        self.assertEqual(_csot.get_rtt(), 0.0)
        with pymongo.timeout(10):
            coll.find_one()
            self.assertEqual(_csot.get_timeout(), 10)
            deadline_10 = _csot.get_deadline()

            # Capped at the original 10 deadline.
            with pymongo.timeout(15):
                coll.find_one()
                self.assertEqual(_csot.get_timeout(), 15)
                self.assertEqual(_csot.get_deadline(), deadline_10)

            # Should be reset to previous values
            self.assertEqual(_csot.get_timeout(), 10)
            self.assertEqual(_csot.get_deadline(), deadline_10)
            coll.find_one()

            with pymongo.timeout(5):
                coll.find_one()
                self.assertEqual(_csot.get_timeout(), 5)
                self.assertLess(_csot.get_deadline(), deadline_10)

            # Should be reset to previous values
            self.assertEqual(_csot.get_timeout(), 10)
            self.assertEqual(_csot.get_deadline(), deadline_10)
            coll.find_one()

        # Should be reset to previous values
        self.assertEqual(_csot.get_timeout(), None)
        self.assertEqual(_csot.get_deadline(), float("inf"))
        self.assertEqual(_csot.get_rtt(), 0.0)


if __name__ == "__main__":
    unittest.main()
