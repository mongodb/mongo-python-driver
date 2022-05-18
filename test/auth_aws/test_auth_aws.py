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
from typing import Optional

sys.path[0:0] = [""]

from test.utils import get_pool

from pymongo import MongoClient, MongoCredential
from pymongo.errors import OperationFailure
from pymongo.uri_parser import parse_uri


class AuthProvider:
    """Auth provider that returns good credentials except for the
    third time it is called."""

    def __init__(self, uri):
        self.count = 0
        parts = parse_uri(uri)
        self.access_key = parts["username"]
        self.secret_access_key = parts["password"]
        auth_props = parts["options"].get("authMechanismProperties", {})
        self.session_token = auth_props.get("AWS_SESSION_TOKEN", None)

    def get_credential(self, credential: Optional[MongoCredential]) -> MongoCredential:
        self.count += 1
        if self.count == 3:
            return MongoCredential(
                username="fake", password="fake", source="$$external", mechanism="MONGODB-AWS"
            )
        mechanism_props = dict(AWS_SESSION_TOKEN=self.session_token, AWS_ROLE_ARN="test")
        return MongoCredential(
            username=self.access_key,
            password=self.secret_access_key,
            source="$$external",
            mechanism="MONGODB-AWS",
            mechanism_properties=mechanism_props,
        )


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

    def test_dynamic_credential_callback(self):
        callback = AuthProvider(self.uri).get_credential
        with MongoClient(self.uri, dynamic_credential_callback=callback) as client:
            client.get_database().test.find_one()
            # Reset the pool between each request to force an auth refresh.
            get_pool(client).reset()
            client.get_database().test.find_one()
            get_pool(client).reset()
            with self.assertRaises(OperationFailure):
                client.get_database().test.find_one()
            client.get_database().test.find_one()
            get_pool(client).reset()


if __name__ == "__main__":
    unittest.main()
