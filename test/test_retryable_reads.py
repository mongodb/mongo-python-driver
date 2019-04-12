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
                raise unittest.SkipTest(
                    'PyMongo does not support %s' % (name,))


def create_test(scenario_def, test, name):
    @client_context.require_test_commands
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


test_creator = TestCreator(create_test, TestSpec, _TEST_PATH)
test_creator.create_tests()

if __name__ == "__main__":
    unittest.main()
