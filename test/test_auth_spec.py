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

from pymongo import MongoClient
from test import unittest


_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'auth')


class TestAuthSpec(unittest.TestCase):
    pass


def create_test(test_case):

    def run_test(self):
        uri = test_case['uri']
        valid = test_case['valid']
        auth = test_case['auth']
        options = test_case['options']

        if not valid:
            self.assertRaises(Exception, MongoClient, uri, connect=False)
        else:
            client = MongoClient(uri, connect=False)
            credentials = client._MongoClient__options.credentials
            if auth is not None:
                self.assertEqual(credentials.username, auth['username'])
                self.assertEqual(credentials.password, auth['password'])
                self.assertEqual(credentials.source, auth['db'])
            if options is not None:
                if 'authmechanism' in options:
                    self.assertEqual(
                        credentials.mechanism, options['authmechanism'])
                else:
                    self.assertEqual(credentials.mechanism, 'DEFAULT')
                if 'authmechanismproperties' in options:
                    expected = options['authmechanismproperties']
                    actual = credentials.mechanism_properties
                    if 'SERVICE_NAME' in expected:
                        self.assertEqual(
                            actual.service_name, expected['SERVICE_NAME'])
                    if 'CANONICALIZE_HOST_NAME' in expected:
                        self.assertEqual(
                            actual.canonicalize_host_name,
                            expected['CANONICALIZE_HOST_NAME'])
                    if 'SERVICE_REALM' in expected:
                        self.assertEqual(
                            actual.service_realm, expected['SERVICE_REALM'])

    return run_test


def create_tests():
    for filename in glob.glob(os.path.join(_TEST_PATH, '*.json')):
        test_suffix, _ = os.path.splitext(os.path.basename(filename))
        with open(filename) as auth_tests:
            test_cases = json.load(auth_tests)['tests']
            for test_case in test_cases:
                if test_case.get('optional', False):
                    continue
                test_method = create_test(test_case)
                name = str(test_case['description'].lower().replace(' ', '_'))
                setattr(
                    TestAuthSpec,
                    'test_%s_%s' % (test_suffix, name),
                    test_method)


create_tests()


if __name__ == "__main__":
    unittest.main()
