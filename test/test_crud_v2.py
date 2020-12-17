# Copyright 2019-present MongoDB, Inc.
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

"""Test the collection module."""

import os
import sys

sys.path[0:0] = [""]

from test import unittest
from test.crud_v2_format import TestCrudV2
from test.utils import TestCreator


# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'crud', 'v2')


class TestSpec(TestCrudV2):
    # Default test database and collection names.
    TEST_DB = 'testdb'
    TEST_COLLECTION = 'testcollection'


def create_test(scenario_def, test, name):
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


test_creator = TestCreator(create_test, TestSpec, _TEST_PATH)
test_creator.create_tests()


if __name__ == "__main__":
    unittest.main()
