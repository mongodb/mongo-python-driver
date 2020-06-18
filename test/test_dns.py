# Copyright 2017 MongoDB, Inc.
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

"""Run the SRV support tests."""

import glob
import json
import os
import sys

sys.path[0:0] = [""]

from pymongo.common import validate_read_preference_tags
from pymongo.srv_resolver import _HAVE_DNSPYTHON
from pymongo.errors import ConfigurationError
from pymongo.mongo_client import MongoClient
from pymongo.uri_parser import parse_uri, split_hosts
from test import client_context, unittest
from test.utils import wait_until


_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'srv_seedlist')

class TestDNS(unittest.TestCase):
    pass


def create_test(test_case):

    @client_context.require_replica_set
    @client_context.require_tls
    def run_test(self):
        if not _HAVE_DNSPYTHON:
            raise unittest.SkipTest("DNS tests require the dnspython module")
        uri = test_case['uri']
        seeds = test_case['seeds']
        hosts = test_case['hosts']
        options = test_case.get('options')
        if seeds:
            seeds = split_hosts(','.join(seeds))
        if hosts:
            hosts = frozenset(split_hosts(','.join(hosts)))

        if seeds:
            result = parse_uri(uri, validate=True)
            self.assertEqual(sorted(result['nodelist']), sorted(seeds))
            if options:
                opts = result['options']
                if 'readpreferencetags' in opts:
                    rpts = validate_read_preference_tags(
                        'readPreferenceTags', opts.pop('readpreferencetags'))
                    opts['readPreferenceTags'] = rpts
                self.assertEqual(result['options'], options)

            hostname = next(iter(client_context.client.nodes))[0]
            # The replica set members must be configured as 'localhost'.
            if hostname == 'localhost':
                copts = client_context.default_client_options.copy()
                if client_context.tls is True:
                    # Our test certs don't support the SRV hosts used in these tests.
                    copts['ssl_match_hostname'] = False

                client = MongoClient(uri, **copts)
                # Force server selection
                client.admin.command('ismaster')
                wait_until(
                    lambda: hosts == client.nodes,
                    'match test hosts to client nodes')
        else:
            try:
                parse_uri(uri)
            except (ConfigurationError, ValueError):
                pass
            else:
                self.fail("failed to raise an exception")

    return run_test


def create_tests():
    for filename in glob.glob(os.path.join(_TEST_PATH, '*.json')):
        test_suffix, _ = os.path.splitext(os.path.basename(filename))
        with open(filename) as dns_test_file:
            test_method = create_test(json.load(dns_test_file))
        setattr(TestDNS, 'test_' + test_suffix, test_method)


create_tests()

class TestParsingErrors(unittest.TestCase):

    @unittest.skipUnless(_HAVE_DNSPYTHON, "DNS tests require the dnspython module")
    def test_invalid_host(self):
        self.assertRaisesRegex(
            ConfigurationError,
            "Invalid URI host: mongodb",
            MongoClient, "mongodb+srv://mongodb")
        self.assertRaisesRegex(
            ConfigurationError,
            "Invalid URI host: mongodb.com",
            MongoClient, "mongodb+srv://mongodb.com")


if __name__ == '__main__':
    unittest.main()
