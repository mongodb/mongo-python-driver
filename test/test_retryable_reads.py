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

"""Test retryable reads spec."""

import os
import sys

sys.path[0:0] = [""]

from pymongo.mongo_client import MongoClient
from pymongo.write_concern import WriteConcern

from test import unittest, client_context, PyMongoTestCase
from test.utils import TestCreator
from test.utils_spec_runner import SpecRunner


# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'retryable_reads')


class TestClientOptions(PyMongoTestCase):
    def test_default(self):
        client = MongoClient(connect=False)
        self.assertEqual(client.retry_reads, True)

    def test_kwargs(self):
        client = MongoClient(retryReads=True, connect=False)
        self.assertEqual(client.retry_reads, True)
        client = MongoClient(retryReads=False, connect=False)
        self.assertEqual(client.retry_reads, False)

    def test_uri(self):
        client = MongoClient('mongodb://h/?retryReads=true', connect=False)
        self.assertEqual(client.retry_reads, True)
        client = MongoClient('mongodb://h/?retryReads=false', connect=False)
        self.assertEqual(client.retry_reads, False)


class TestSpec(SpecRunner):

    @classmethod
    @client_context.require_version_min(4, 0)
    # TODO: remove this once PYTHON-1948 is done.
    @client_context.require_no_mmap
    def setUpClass(cls):
        super(TestSpec, cls).setUpClass()
        if client_context.is_mongos and client_context.version[:2] <= (4, 0):
            raise unittest.SkipTest("4.0 mongos does not support failCommand")

    def maybe_skip_scenario(self, test):
        super(TestSpec, self).maybe_skip_scenario(test)
        skip_names = [
            'listCollectionObjects', 'listIndexNames', 'listDatabaseObjects']
        for name in skip_names:
            if name.lower() in test['description'].lower():
                self.skipTest('PyMongo does not support %s' % (name,))

        # Skip changeStream related tests on MMAPv1.
        test_name = self.id().rsplit('.')[-1]
        if ('changestream' in test_name.lower() and
                client_context.storage_engine == 'mmapv1'):
            self.skipTest("MMAPv1 does not support change streams.")

    def get_scenario_coll_name(self, scenario_def):
        """Override a test's collection name to support GridFS tests."""
        if 'bucket_name' in scenario_def:
            return scenario_def['bucket_name']
        return super(TestSpec, self).get_scenario_coll_name(scenario_def)

    def setup_scenario(self, scenario_def):
        """Override a test's setup to support GridFS tests."""
        if 'bucket_name' in scenario_def:
            db_name = self.get_scenario_db_name(scenario_def)
            db = client_context.client.get_database(
                db_name, write_concern=WriteConcern(w='majority'))
            # Create a bucket for the retryable reads GridFS tests.
            client_context.client.drop_database(db_name)
            if scenario_def['data']:
                data = scenario_def['data']
                # Load data.
                db['fs.chunks'].insert_many(data['fs.chunks'])
                db['fs.files'].insert_many(data['fs.files'])
        else:
            super(TestSpec, self).setup_scenario(scenario_def)


def create_test(scenario_def, test, name):
    @client_context.require_test_commands
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


test_creator = TestCreator(create_test, TestSpec, _TEST_PATH)
test_creator.create_tests()

if __name__ == "__main__":
    unittest.main()
