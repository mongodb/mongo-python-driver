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

from test.asynchronous import (
    AsyncIntegrationTest,
    AsyncPyMongoTestCase,
    async_client_context,
    unittest,
)
from test.utils_shared import async_wait_until
from unittest.mock import MagicMock, patch

from pymongo.asynchronous.uri_parser import parse_uri
from pymongo.common import validate_read_preference_tags
from pymongo.errors import ConfigurationError
from pymongo.uri_parser_shared import split_hosts

_IS_SYNC = False


class TestDNSRepl(AsyncPyMongoTestCase):
    if _IS_SYNC:
        TEST_PATH = os.path.join(
            pathlib.Path(__file__).resolve().parent, "srv_seedlist", "replica-set"
        )
    else:
        TEST_PATH = os.path.join(
            pathlib.Path(__file__).resolve().parent.parent, "srv_seedlist", "replica-set"
        )
    load_balanced = False

    @async_client_context.require_replica_set
    def asyncSetUp(self):
        pass


class TestDNSLoadBalanced(AsyncPyMongoTestCase):
    if _IS_SYNC:
        TEST_PATH = os.path.join(
            pathlib.Path(__file__).resolve().parent, "srv_seedlist", "load-balanced"
        )
    else:
        TEST_PATH = os.path.join(
            pathlib.Path(__file__).resolve().parent.parent, "srv_seedlist", "load-balanced"
        )
    load_balanced = True

    @async_client_context.require_load_balancer
    def asyncSetUp(self):
        pass


class TestDNSSharded(AsyncPyMongoTestCase):
    if _IS_SYNC:
        TEST_PATH = os.path.join(pathlib.Path(__file__).resolve().parent, "srv_seedlist", "sharded")
    else:
        TEST_PATH = os.path.join(
            pathlib.Path(__file__).resolve().parent.parent, "srv_seedlist", "sharded"
        )
    load_balanced = False

    @async_client_context.require_mongos
    def asyncSetUp(self):
        pass


def create_test(test_case):
    async def run_test(self):
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
        if needs_tls and not async_client_context.tls:
            self.skipTest("this test requires a TLS cluster")
        if not needs_tls and async_client_context.tls:
            self.skipTest("this test requires a non-TLS cluster")

        if seeds:
            seeds = split_hosts(",".join(seeds))
        if hosts:
            hosts = frozenset(split_hosts(",".join(hosts)))

        if seeds or num_seeds:
            result = await parse_uri(uri, validate=True)
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

            hostname = next(iter(async_client_context.client.nodes))[0]
            # The replica set members must be configured as 'localhost'.
            if hostname == "localhost":
                copts = async_client_context.default_client_options.copy()
                # Remove tls since SRV parsing should add it automatically.
                copts.pop("tls", None)
                if async_client_context.tls:
                    # Our test certs don't support the SRV hosts used in these
                    # tests.
                    copts["tlsAllowInvalidHostnames"] = True

                client = self.simple_client(uri, **copts)
                if client._options.connect:
                    await client.aconnect()
                if num_seeds is not None:
                    self.assertEqual(len(client._topology_settings.seeds), num_seeds)
                if hosts is not None:
                    await async_wait_until(
                        lambda: hosts == client.nodes, "match test hosts to client nodes"
                    )
                if num_hosts is not None:
                    await async_wait_until(
                        lambda: num_hosts == len(client.nodes), "wait to connect to num_hosts"
                    )
                if test_case.get("ping", True):
                    await client.admin.command("ping")
                # XXX: we should block until SRV poller runs at least once
                # and re-run these assertions.
        else:
            try:
                await parse_uri(uri)
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


class TestParsingErrors(AsyncPyMongoTestCase):
    async def test_invalid_host(self):
        with self.assertRaisesRegex(ConfigurationError, "Invalid URI host: an IP address is not"):
            client = self.simple_client("mongodb+srv://127.0.0.1")
            await client.aconnect()
        with self.assertRaisesRegex(ConfigurationError, "Invalid URI host: an IP address is not"):
            client = self.simple_client("mongodb+srv://[::1]")
            await client.aconnect()


class IsolatedAsyncioTestCaseInsensitive(AsyncIntegrationTest):
    async def test_connect_case_insensitive(self):
        client = self.simple_client("mongodb+srv://TEST1.TEST.BUILD.10GEN.cc/")
        await client.aconnect()
        self.assertGreater(len(client.topology_description.server_descriptions()), 1)


class TestInitialDnsSeedlistDiscovery(AsyncPyMongoTestCase):
    """
    Initial DNS Seedlist Discovery prose tests
    https://github.com/mongodb/specifications/blob/0a7a8b5/source/initial-dns-seedlist-discovery/tests/README.md#prose-tests
    """

    async def run_initial_dns_seedlist_discovery_prose_tests(self, test_cases):
        for case in test_cases:
            with patch("dns.asyncresolver.resolve") as mock_resolver:

                async def mock_resolve(query, record_type, *args, **kwargs):
                    mock_srv = MagicMock()
                    mock_srv.target.to_text.return_value = case["mock_target"]
                    return [mock_srv]

                mock_resolver.side_effect = mock_resolve
                domain = case["query"].split("._tcp.")[1]
                connection_string = f"mongodb+srv://{domain}"
                if "expected_error" not in case:
                    await parse_uri(connection_string)
                else:
                    try:
                        await parse_uri(connection_string)
                    except ConfigurationError as e:
                        self.assertIn(case["expected_error"], str(e))
                    else:
                        self.fail(f"ConfigurationError was not raised for query: {case['query']}")

    async def test_1_allow_srv_hosts_with_fewer_than_three_dot_separated_parts(self):
        with patch("dns.asyncresolver.resolve"):
            await parse_uri("mongodb+srv://localhost/")
            await parse_uri("mongodb+srv://mongo.local/")

    async def test_2_throw_when_return_address_does_not_end_with_srv_domain(self):
        test_cases = [
            {
                "query": "_mongodb._tcp.localhost",
                "mock_target": "localhost.mongodb",
                "expected_error": "Invalid SRV host",
            },
            {
                "query": "_mongodb._tcp.blogs.mongodb.com",
                "mock_target": "blogs.evil.com",
                "expected_error": "Invalid SRV host",
            },
            {
                "query": "_mongodb._tcp.blogs.mongo.local",
                "mock_target": "test_1.evil.com",
                "expected_error": "Invalid SRV host",
            },
        ]
        await self.run_initial_dns_seedlist_discovery_prose_tests(test_cases)

    async def test_3_throw_when_return_address_is_identical_to_srv_hostname(self):
        test_cases = [
            {
                "query": "_mongodb._tcp.localhost",
                "mock_target": "localhost",
                "expected_error": "Invalid SRV host",
            },
            {
                "query": "_mongodb._tcp.mongo.local",
                "mock_target": "mongo.local",
                "expected_error": "Invalid SRV host",
            },
        ]
        await self.run_initial_dns_seedlist_discovery_prose_tests(test_cases)

    async def test_4_throw_when_return_address_does_not_contain_dot_separating_shared_part_of_domain(
        self
    ):
        test_cases = [
            {
                "query": "_mongodb._tcp.localhost",
                "mock_target": "test_1.cluster_1localhost",
                "expected_error": "Invalid SRV host",
            },
            {
                "query": "_mongodb._tcp.mongo.local",
                "mock_target": "test_1.my_hostmongo.local",
                "expected_error": "Invalid SRV host",
            },
            {
                "query": "_mongodb._tcp.blogs.mongodb.com",
                "mock_target": "cluster.testmongodb.com",
                "expected_error": "Invalid SRV host",
            },
        ]
        await self.run_initial_dns_seedlist_discovery_prose_tests(test_cases)

    async def test_5_when_srv_hostname_has_two_dot_separated_parts_it_is_valid_for_the_returned_hostname_to_be_identical(
        self
    ):
        test_cases = [
            {
                "query": "_mongodb._tcp.blogs.mongodb.com",
                "mock_target": "blogs.mongodb.com",
            },
        ]
        await self.run_initial_dns_seedlist_discovery_prose_tests(test_cases)


if __name__ == "__main__":
    unittest.main()
