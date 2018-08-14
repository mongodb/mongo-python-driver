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

"""Test the topology module's Server Selection Spec implementation."""

import os
import sys

from pymongo import MongoClient

sys.path[0:0] = [""]

from test import unittest
from test.utils_selection_tests import create_selection_tests


# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('server_selection', 'server_selection'))


class TestAllScenarios(create_selection_tests(_TEST_PATH)):
    pass


class TestCustomServerSelectorFunction(unittest.TestCase):
    def test_invalid_server_selector(self):
        _bad_server_selector = 1
        with self.assertRaisesRegex(ValueError, "must be a callable"):
            _ = MongoClient(
                connect=False, serverSelector=list()
            )


if __name__ == "__main__":
    unittest.main()
