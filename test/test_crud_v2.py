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
from test.utils import TestCreator
from test.utils_spec_runner import SpecRunner


# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'crud', 'v2')

# Default test database and collection names.
TEST_DB = 'testdb'
TEST_COLLECTION = 'testcollection'


class TestSpec(SpecRunner):
    def get_scenario_db_name(self, scenario_def):
        """Crud spec says database_name is optional."""
        return scenario_def.get('database_name', TEST_DB)

    def get_scenario_coll_name(self, scenario_def):
        """Crud spec says collection_name is optional."""
        return scenario_def.get('collection_name', TEST_COLLECTION)

    def get_object_name(self, op):
        """Crud spec says object is optional and defaults to 'collection'."""
        return op.get('object', 'collection')

    def get_outcome_coll_name(self, outcome, collection):
        """Crud spec says outcome has an optional 'collection.name'."""
        return outcome['collection'].get('name', collection.name)

    def setup_scenario(self, scenario_def):
        """Allow specs to override a test's setup."""
        # PYTHON-1935 Only create the collection if there is data to insert.
        if scenario_def['data']:
            super(TestSpec, self).setup_scenario(scenario_def)


def create_test(scenario_def, test, name):
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


test_creator = TestCreator(create_test, TestSpec, _TEST_PATH)
test_creator.create_tests()


if __name__ == "__main__":
    unittest.main()
