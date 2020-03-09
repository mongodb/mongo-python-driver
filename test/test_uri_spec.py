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

"""Test that the pymongo.uri_parser module is compliant with the connection
string and uri options specifications."""

import json
import os
import sys
import warnings

sys.path[0:0] = [""]

from pymongo.common import INTERNAL_URI_OPTION_NAME_MAP, validate
from pymongo.compression_support import _HAVE_SNAPPY
from pymongo.uri_parser import parse_uri
from test import clear_warning_registry, unittest


CONN_STRING_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('connection_string', 'test'))

URI_OPTIONS_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'uri_options')

TEST_DESC_SKIP_LIST = [
    "Valid options specific to single-threaded drivers are parsed correctly",
    "Invalid serverSelectionTryOnce causes a warning",
    "tlsDisableCertificateRevocationCheck can be set to true",
    "tlsDisableCertificateRevocationCheck can be set to false",
    "tlsAllowInvalidCertificates and tlsDisableCertificateRevocationCheck both present (and true) raises an error",
    "tlsAllowInvalidCertificates=true and tlsDisableCertificateRevocationCheck=false raises an error",
    "tlsAllowInvalidCertificates=false and tlsDisableCertificateRevocationCheck=true raises an error",
    "tlsAllowInvalidCertificates and tlsDisableCertificateRevocationCheck both present (and false) raises an error",
    "tlsDisableCertificateRevocationCheck and tlsAllowInvalidCertificates both present (and true) raises an error",
    "tlsDisableCertificateRevocationCheck=true and tlsAllowInvalidCertificates=false raises an error",
    "tlsDisableCertificateRevocationCheck=false and tlsAllowInvalidCertificates=true raises an error",
    "tlsDisableCertificateRevocationCheck and tlsAllowInvalidCertificates both present (and false) raises an error",
    "tlsInsecure and tlsDisableCertificateRevocationCheck both present (and true) raises an error",
    "tlsInsecure=true and tlsDisableCertificateRevocationCheck=false raises an error",
    "tlsInsecure=false and tlsDisableCertificateRevocationCheck=true raises an error",
    "tlsInsecure and tlsDisableCertificateRevocationCheck both present (and false) raises an error",
    "tlsDisableCertificateRevocationCheck and tlsInsecure both present (and true) raises an error",
    "tlsDisableCertificateRevocationCheck=true and tlsInsecure=false raises an error",
    "tlsDisableCertificateRevocationCheck=false and tlsInsecure=true raises an error",
    "tlsDisableCertificateRevocationCheck and tlsInsecure both present (and false) raises an error",
    "tlsDisableCertificateRevocationCheck and tlsDisableOCSPEndpointCheck both present (and true) raises an error",
    "tlsDisableCertificateRevocationCheck=true and tlsDisableOCSPEndpointCheck=false raises an error",
    "tlsDisableCertificateRevocationCheck=false and tlsDisableOCSPEndpointCheck=true raises an error",
    "tlsDisableCertificateRevocationCheck and tlsDisableOCSPEndpointCheck both present (and false) raises an error",
    "tlsDisableOCSPEndpointCheck and tlsDisableCertificateRevocationCheck both present (and true) raises an error",
    "tlsDisableOCSPEndpointCheck=true and tlsDisableCertificateRevocationCheck=false raises an error",
    "tlsDisableOCSPEndpointCheck=false and tlsDisableCertificateRevocationCheck=true raises an error",
    "tlsDisableOCSPEndpointCheck and tlsDisableCertificateRevocationCheck both present (and false) raises an error"]


class TestAllScenarios(unittest.TestCase):
    def setUp(self):
        clear_warning_registry()


def get_error_message_template(expected, artefact):
    return "%s %s for test '%s'" % (
        "Expected" if expected else "Unexpected", artefact, "%s")


def run_scenario_in_dir(target_workdir):
    def workdir_context_decorator(func):
        def modified_test_scenario(*args, **kwargs):
            original_workdir = os.getcwd()
            os.chdir(target_workdir)
            func(*args, **kwargs)
            os.chdir(original_workdir)
        return modified_test_scenario
    return workdir_context_decorator


def create_test(test, test_workdir):
    def run_scenario(self):
        compressors = (test.get('options') or {}).get('compressors', [])
        if 'snappy' in compressors and not _HAVE_SNAPPY:
            self.skipTest('This test needs the snappy module.')

        valid = True
        warning = False

        with warnings.catch_warnings(record=True) as ctx:
            warnings.simplefilter('always')
            try:
                options = parse_uri(test['uri'], warn=True)
            except Exception:
                valid = False
            else:
                warning = len(ctx) > 0

        expected_valid = test.get('valid', True)
        self.assertEqual(
            valid, expected_valid, get_error_message_template(
                not expected_valid, "error") % test['description'])

        if expected_valid:
            expected_warning = test.get('warning', False)
            self.assertEqual(
                warning, expected_warning, get_error_message_template(
                    expected_warning, "warning") % test['description'])

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
        err_msg = "For option %s expected %s but got %s"
        if test['options']:
            opts = options['options']
            for opt in test['options']:
                lopt = opt.lower()
                optname = INTERNAL_URI_OPTION_NAME_MAP.get(lopt, lopt)
                if opts.get(optname) is not None:
                    if opts[optname] == test['options'][opt]:
                        expected_value = test['options'][opt]
                    else:
                        expected_value = validate(
                            lopt, test['options'][opt])[1]
                    self.assertEqual(
                        opts[optname], expected_value,
                        err_msg % (opt, expected_value, opts[optname],))
                else:
                    self.fail(
                        "Missing expected option %s" % (opt,))

    return run_scenario_in_dir(test_workdir)(run_scenario)


def create_tests(test_path):
    for dirpath, _, filenames in os.walk(test_path):
        dirname = os.path.split(dirpath)
        dirname = os.path.split(dirname[-2])[-1] + '_' + dirname[-1]

        for filename in filenames:
            if not filename.endswith('.json'):
                # skip everything that is not a test specification
                continue
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(scenario_stream)

            for testcase in scenario_def['tests']:
                dsc = testcase['description']

                if dsc in TEST_DESC_SKIP_LIST:
                    print("Skipping test '%s'" % dsc)
                    continue

                testmethod = create_test(testcase, dirpath)
                testname = 'test_%s_%s_%s' % (
                    dirname, os.path.splitext(filename)[0],
                    str(dsc).replace(' ', '_'))
                testmethod.__name__ = testname
                setattr(TestAllScenarios, testmethod.__name__, testmethod)


for test_path in [CONN_STRING_TEST_PATH, URI_OPTIONS_TEST_PATH]:
    create_tests(test_path)


if __name__ == "__main__":
    unittest.main()
