# Copyright 2018-present MongoDB, Inc.
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

"""Run the auth spec tests."""

import glob
import json
import os
import sys

sys.path[0:0] = [""]

from test import unittest

from pymongo import MongoClient

_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "auth")


class TestAuthSpec(unittest.TestCase):
    pass


def create_test(test_case):
    def run_test(self):
        uri = test_case["uri"]
        valid = test_case["valid"]
        credential = test_case.get("credential")

        if not valid:
            self.assertRaises(Exception, MongoClient, uri, connect=False)
        else:
            client = MongoClient(uri, connect=False)
            credentials = client.options.pool_options._credentials
            if credential is None:
                self.assertIsNone(credentials)
            else:
                self.assertIsNotNone(credentials)
                self.assertEqual(credentials.username, credential["username"])
                self.assertEqual(credentials.password, credential["password"])
                self.assertEqual(credentials.source, credential["source"])
                if credential["mechanism"] is not None:
                    self.assertEqual(credentials.mechanism, credential["mechanism"])
                else:
                    self.assertEqual(credentials.mechanism, "DEFAULT")
                expected = credential["mechanism_properties"]
                if expected is not None:
                    actual = credentials.mechanism_properties
                    for key, val in expected.items():
                        if "SERVICE_NAME" in expected:
                            self.assertEqual(actual.service_name, expected["SERVICE_NAME"])
                        elif "CANONICALIZE_HOST_NAME" in expected:
                            self.assertEqual(
                                actual.canonicalize_host_name, expected["CANONICALIZE_HOST_NAME"]
                            )
                        elif "SERVICE_REALM" in expected:
                            self.assertEqual(actual.service_realm, expected["SERVICE_REALM"])
                        elif "AWS_SESSION_TOKEN" in expected:
                            self.assertEqual(
                                actual.aws_session_token, expected["AWS_SESSION_TOKEN"]
                            )
                        else:
                            self.fail("Unhandled property: %s" % (key,))
                else:
                    if credential["mechanism"] == "MONGODB-AWS":
                        self.assertIsNone(credentials.mechanism_properties.aws_session_token)
                    else:
                        self.assertIsNone(credentials.mechanism_properties)

    return run_test


def create_tests():
    for filename in glob.glob(os.path.join(_TEST_PATH, "*.json")):
        test_suffix, _ = os.path.splitext(os.path.basename(filename))
        with open(filename) as auth_tests:
            test_cases = json.load(auth_tests)["tests"]
            for test_case in test_cases:
                if test_case.get("optional", False):
                    continue
                test_method = create_test(test_case)
                name = str(test_case["description"].lower().replace(" ", "_"))
                setattr(TestAuthSpec, "test_%s_%s" % (test_suffix, name), test_method)


create_tests()


if __name__ == "__main__":
    unittest.main()
