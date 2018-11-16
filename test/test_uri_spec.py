# Copyright 2011-2015 MongoDB, Inc.
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

"""Test the pymongo uri_parser module is up to spec."""
import json
import os
import sys
import warnings

sys.path[0:0] = [""]

from pymongo.uri_parser import parse_uri
from test import unittest


CONN_STRING_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('connection_string', 'test'))

URI_OPTIONS_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'uri_options')


class TestAllScenarios(unittest.TestCase):
    pass


def get_error_message_template(expected, artefact):
    return "%s %s for test '%s'" % (
        "Expected" if expected else "Unexpected", artefact, "%s")


def create_test(scenario_def):
    def run_scenario(self):
        self.assertTrue(scenario_def['tests'], "tests cannot be empty")
        for test in scenario_def['tests']:
            dsc = test['description']

            valid = True
            warning = False

            with warnings.catch_warnings():
                warnings.filterwarnings('error')
                try:
                    options = parse_uri(test['uri'], warn=True)
                except Warning:
                    warning = True
                except Exception:
                    valid = False

            expected_valid = test.get('valid', True)
            self.assertEqual(
                valid, expected_valid, get_error_message_template(
                    not expected_valid, "error") % dsc)

            if expected_valid:
                expected_warning = test.get('warning', False)
                self.assertEqual(
                    warning, expected_warning, get_error_message_template(
                        expected_warning, "warning") % dsc)

            # Redo in the case there were warnings that were not expected.
            if warning:
                options = parse_uri(test['uri'], warn=True)

            # Compare hosts and port.
            if test['hosts'] is not None:
                self.assertEqual(
                    len(test['hosts']), len(options['nodelist']),
                    "Incorrect number of hosts parsed from URI")

                for exp, actual in zip(test['hosts'],
                                       options['nodelist']):
                    self.assertEqual(exp['host'], actual[0],
                                     "Expected host %s but got %s"
                                     % (exp['host'], actual[0]))
                    if exp['port'] is not None:
                        self.assertEqual(exp['port'], actual[1],
                                         "Expected port %s but got %s"
                                         % (exp['port'], actual))

            # Compare auth options.
            auth = test['auth']
            if auth is not None:
                auth['database'] = auth.pop('db')  # db == database
                # Special case for PyMongo's collection parsing.
                if options.get('collection') is not None:
                    options['database'] += "." + options['collection']
                for elm in auth:
                    if auth[elm] is not None:
                        self.assertEqual(auth[elm], options[elm],
                                         "Expected %s but got %s"
                                         % (auth[elm], options[elm]))

            # Compare URI options.
            if test['options'] is not None:
                for opt in test['options']:
                    if options.get(opt) is not None:
                        self.assertEqual(
                            options[opt], test['options'][opt],
                            "For option %s expected %s but got %s"
                            % (opt, options[opt],
                               test['options'][opt]))

    return run_scenario


def create_tests(test_path):
    for dirpath, _, filenames in os.walk(test_path):
        dirname = os.path.split(dirpath)
        dirname = os.path.split(dirname[-2])[-1] + '_' + dirname[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(scenario_stream)
            # Construct test from scenario.
            new_test = create_test(scenario_def)
            test_name = 'test_%s_%s' % (
                dirname, os.path.splitext(filename)[0])
            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)


for test_path in [CONN_STRING_TEST_PATH, URI_OPTIONS_TEST_PATH]:
    create_tests(test_path)


if __name__ == "__main__":
    unittest.main()
