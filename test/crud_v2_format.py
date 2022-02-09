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

"""v2 format CRUD test runner.

https://github.com/mongodb/specifications/blob/master/source/crud/tests/README.rst
"""

from test.utils_spec_runner import SpecRunner


class TestCrudV2(SpecRunner):
    # Default test database and collection names.
    TEST_DB = None
    TEST_COLLECTION = None

    def allowable_errors(self, op):
        """Override expected error classes."""
        errors = super(TestCrudV2, self).allowable_errors(op)
        errors += (ValueError,)
        return errors

    def get_scenario_db_name(self, scenario_def):
        """Crud spec says database_name is optional."""
        return scenario_def.get("database_name", self.TEST_DB)

    def get_scenario_coll_name(self, scenario_def):
        """Crud spec says collection_name is optional."""
        return scenario_def.get("collection_name", self.TEST_COLLECTION)

    def get_object_name(self, op):
        """Crud spec says object is optional and defaults to 'collection'."""
        return op.get("object", "collection")

    def get_outcome_coll_name(self, outcome, collection):
        """Crud spec says outcome has an optional 'collection.name'."""
        return outcome["collection"].get("name", collection.name)

    def setup_scenario(self, scenario_def):
        """Allow specs to override a test's setup."""
        # PYTHON-1935 Only create the collection if there is data to insert.
        if scenario_def["data"]:
            super(TestCrudV2, self).setup_scenario(scenario_def)
