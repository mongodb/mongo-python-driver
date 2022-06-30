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

"""Test MONGODB-AWS Authentication."""

import os
import sys
import unittest

sys.path[0:0] = [""]

from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pymongo.uri_parser import parse_uri


class TestAuthAWS(unittest.TestCase):
    uri: str

    @classmethod
    def setUpClass(cls):
        cls.uri = os.environ["MONGODB_URI"]

    def test_should_fail_without_credentials(self):
        if "@" not in self.uri:
            self.skipTest("MONGODB_URI already has no credentials")

        hosts = ["%s:%s" % addr for addr in parse_uri(self.uri)["nodelist"]]
        self.assertTrue(hosts)
        with MongoClient(hosts) as client:
            with self.assertRaises(OperationFailure):
                client.aws.test.find_one()

    def test_should_fail_incorrect_credentials(self):
        with MongoClient(
            self.uri, username="fake", password="fake", authMechanism="MONGODB-AWS"
        ) as client:
            with self.assertRaises(OperationFailure):
                client.get_database().test.find_one()

    def test_connect_uri(self):
        with MongoClient(self.uri) as client:
            client.get_database().test.find_one()


class TestAWSLambdaExamples(unittest.TestCase):
    def test_shared_client(self):
        # Start AWS Lambda Example 1
        import os

        from pymongo import MongoClient

        client = MongoClient(host=os.environ["MONGODB_URI"])

        def lambda_handler(event, context):
            return client.db.command("ping")

        # End AWS Lambda Example 1

    def test_IAM_auth(self):
        # Start AWS Lambda Example 2
        import os

        from pymongo import MongoClient

        client = MongoClient(
            host=os.environ["MONGODB_URI"],
            authSource="$external",
            authMechanism="MONGODB-AWS",
        )

        def lambda_handler(event, context):
            return client.db.command("ping")

        # End AWS Lambda Example 2


if __name__ == "__main__":
    unittest.main()
