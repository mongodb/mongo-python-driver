# Copyright 2020-present MongoDB, Inc.
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

"""Test Atlas Data Lake."""

import os
import sys

sys.path[0:0] = [""]

from test import client_context, unittest
from test.crud_v2_format import TestCrudV2
from test.utils import TestCreator


# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "data_lake")


class DataLakeTestSpec(TestCrudV2):
    # Default test database and collection names.
    TEST_DB = 'test'
    TEST_COLLECTION = 'driverdata'

    @classmethod
    @unittest.skipUnless(client_context.is_data_lake,
                         'Not connected to Atlas Data Lake')
    def setUpClass(cls):
        super(DataLakeTestSpec, cls).setUpClass()

    def setup_scenario(self, scenario_def):
        # Spec tests MUST NOT insert data/drop collection for
        # data lake testing.
        pass


def create_test(scenario_def, test, name):
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


TestCreator(create_test, DataLakeTestSpec, _TEST_PATH).create_tests()


if __name__ == "__main__":
    unittest.main()
