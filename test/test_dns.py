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
from __future__ import annotations

import glob
import json
import os
import pathlib
import sys

sys.path[0:0] = [""]

from test import (
    IntegrationTest,
    PyMongoTestCase,
    client_context,
    unittest,
)
from test.utils_shared import wait_until

from pymongo.common import validate_read_preference_tags
from pymongo.errors import ConfigurationError
from pymongo.synchronous.uri_parser import parse_uri
from pymongo.uri_parser_shared import split_hosts

_IS_SYNC = True


class TestDNSRepl(PyMongoTestCase):
    if _IS_SYNC:
        TEST_PATH = os.path.join(
            pathlib.Path(__file__).resolve().parent, "srv_seedlist", "replica-set"
        )
    else:
        TEST_PATH = os.path.join(
            pathlib.Path(__file__).resolve().parent.parent, "srv_seedlist", "replica-set"
        )
    load_balanced = False

    @client_context.require_replica_set
    def setUp(self):
        pass


class TestDNSLoadBalanced(PyMongoTestCase):
    if _IS_SYNC:
        TEST_PATH = os.path.join(
            pathlib.Path(__file__).resolve().parent, "srv_seedlist", "load-balanced"
        )
    else:
        TEST_PATH = os.path.join(
            pathlib.Path(__file__).resolve().parent.parent, "srv_seedlist", "load-balanced"
        )
    load_balanced = True

    @client_context.require_load_balancer
    def setUp(self):
        pass


class TestDNSSharded(PyMongoTestCase):
    if _IS_SYNC:
        TEST_PATH = os.path.join(pathlib.Path(__file__).resolve().parent, "srv_seedlist", "sharded")
    else:
        TEST_PATH = os.path.join(
            pathlib.Path(__file__).resolve().parent.parent, "srv_seedlist", "sharded"
        )
    load_balanced = False

    @client_context.require_mongos
    def setUp(self):
        pass


def create_test(test_case):
    def run_test(self):
        uri = test_case["uri"]
        seeds = test_case.get("seeds")
        num_seeds = test_case.get("numSeeds", len(seeds or []))
        hosts = test_case.get("hosts")
        num_hosts = test_case.get("numHosts", len(hosts or []))

        options = test_case.get("options", {})
        if "ssl" in options:
            options["tls"] = options.pop("ssl")
        parsed_options = test_case.get("parsed_options")
        # See DRIVERS-1324, unless tls is explicitly set to False we need TLS.
        needs_tls = not (options and (options.get("ssl") is False or options.get("tls") is False))
        if needs_tls and not client_context.tls:
            self.skipTest("this test requires a TLS cluster")
        if not needs_tls and client_context.tls:
            self.skipTest("this test requires a non-TLS cluster")

        if seeds:
            seeds = split_hosts(",".join(seeds))
        if hosts:
            hosts = frozenset(split_hosts(",".join(hosts)))

        if seeds or num_seeds:
            result = parse_uri(uri, validate=True)
            if seeds is not None:
                self.assertEqual(sorted(result["nodelist"]), sorted(seeds))
            if num_seeds is not None:
                self.assertEqual(len(result["nodelist"]), num_seeds)
            if options:
                opts = result["options"]
                if "readpreferencetags" in opts:
                    rpts = validate_read_preference_tags(
                        "readPreferenceTags", opts.pop("readpreferencetags")
                    )
                    opts["readPreferenceTags"] = rpts
                self.assertEqual(result["options"], options)
            if parsed_options:
                for opt, expected in parsed_options.items():
                    if opt == "user":
                        self.assertEqual(result["username"], expected)
                    elif opt == "password":
                        self.assertEqual(result["password"], expected)
                    elif opt == "auth_database" or opt == "db":
                        self.assertEqual(result["database"], expected)

            hostname = next(iter(client_context.client.nodes))[0]
            # The replica set members must be configured as 'localhost'.
            if hostname == "localhost":
                copts = client_context.default_client_options.copy()
                # Remove tls since SRV parsing should add it automatically.
                copts.pop("tls", None)
                if client_context.tls:
                    # Our test certs don't support the SRV hosts used in these
                    # tests.
                    copts["tlsAllowInvalidHostnames"] = True

                client = self.simple_client(uri, **copts)
                if client._options.connect:
                    client._connect()
                if num_seeds is not None:
                    self.assertEqual(len(client._topology_settings.seeds), num_seeds)
                if hosts is not None:
                    wait_until(lambda: hosts == client.nodes, "match test hosts to client nodes")
                if num_hosts is not None:
                    wait_until(
                        lambda: num_hosts == len(client.nodes), "wait to connect to num_hosts"
                    )
                if test_case.get("ping", True):
                    client.admin.command("ping")
                # XXX: we should block until SRV poller runs at least once
                # and re-run these assertions.
        else:
            try:
                parse_uri(uri)
            except (ConfigurationError, ValueError):
                pass
            else:
                self.fail("failed to raise an exception")

    return run_test


def create_tests(cls):
    for filename in glob.glob(os.path.join(cls.TEST_PATH, "*.json")):
        test_suffix, _ = os.path.splitext(os.path.basename(filename))
        with open(filename) as dns_test_file:
            test_method = create_test(json.load(dns_test_file))
        setattr(cls, "test_" + test_suffix, test_method)


create_tests(TestDNSRepl)
create_tests(TestDNSLoadBalanced)
create_tests(TestDNSSharded)


class TestParsingErrors(PyMongoTestCase):
    def test_invalid_host(self):
        with self.assertRaisesRegex(ConfigurationError, "Invalid URI host: an IP address is not"):
            client = self.simple_client("mongodb+srv://127.0.0.1")
            client._connect()
        with self.assertRaisesRegex(ConfigurationError, "Invalid URI host: an IP address is not"):
            client = self.simple_client("mongodb+srv://[::1]")
            client._connect()


class TestCaseInsensitive(IntegrationTest):
    def test_connect_case_insensitive(self):
        client = self.simple_client("mongodb+srv://TEST1.TEST.BUILD.10GEN.cc/")
        client._connect()
        self.assertGreater(len(client.topology_description.server_descriptions()), 1)


if __name__ == "__main__":
    unittest.main()
