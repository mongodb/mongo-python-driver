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

from pymongo._psl import is_public_suffix

sys.path[0:0] = [""]

from unittest.mock import MagicMock, patch

from pymongo.common import validate_read_preference_tags
from pymongo.errors import ConfigurationError
from pymongo.synchronous.uri_parser import parse_uri
from pymongo.uri_parser_shared import split_hosts
from test import (
    IntegrationTest,
    PyMongoTestCase,
    client_context,
    unittest,
)
from test.utils_shared import wait_until

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
                for opt in options:
                    self.assertIn(opt, result["options"])
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


class TestInitialDnsSeedlistDiscovery(PyMongoTestCase):
    """
    Initial DNS Seedlist Discovery prose tests
    https://github.com/mongodb/specifications/blob/5036f26/source/initial-dns-seedlist-discovery/tests/README.md#prose-tests

    Numbered tests correspond to the numbered prose tests in the spec. The
    unnumbered tests are PyMongo-specific additions with no spec counterpart.
    """

    def _parse(self, srv_hostname, mock_target, **kwargs):
        """Resolve mongodb+srv://<srv_hostname> with SRV records naming mock_target."""
        with patch("dns.resolver.resolve") as mock_resolver:

            def mock_resolve(query, record_type, *args, **kwargs):
                mock_srv = MagicMock()
                # Mirror dnspython: the wire form keeps the root label, and the
                # caller strips it via omit_final_dot.
                mock_srv.target.to_text.side_effect = lambda omit_final_dot=False: (
                    mock_target.rstrip(".") if omit_final_dot else mock_target
                )
                return [mock_srv]

            mock_resolver.side_effect = mock_resolve
            return parse_uri(f"mongodb+srv://{srv_hostname}", **kwargs)

    def run_initial_dns_seedlist_discovery_prose_tests(self, test_cases):
        for case in test_cases:
            domain = case["query"].split("._tcp.")[1]
            if "expected_error" not in case:
                self._parse(domain, case["mock_target"])
            else:
                try:
                    self._parse(domain, case["mock_target"])
                except ConfigurationError as e:
                    self.assertIn(case["expected_error"], str(e))
                else:
                    self.fail(f"ConfigurationError was not raised for query: {case['query']}")

    def test_1_allow_srv_hosts_with_fewer_than_three_dot_separated_parts(self):
        with patch("dns.resolver.resolve"):
            parse_uri("mongodb+srv://localhost/")
            parse_uri("mongodb+srv://mongo.local/")

    def test_2_throw_when_return_address_does_not_end_with_srv_domain(self):
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
                "query": "_mongodb._tcp.mongo.local",
                "mock_target": "test_1.evil.local",
                "expected_error": "Invalid SRV host",
            },
        ]
        self.run_initial_dns_seedlist_discovery_prose_tests(test_cases)

    def test_3_throw_when_return_address_is_identical_to_srv_hostname(self):
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
        self.run_initial_dns_seedlist_discovery_prose_tests(test_cases)

    def test_4_throw_when_return_address_does_not_contain_dot_separating_shared_part_of_domain(
        self,
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
        self.run_initial_dns_seedlist_discovery_prose_tests(test_cases)

    def test_5_srv_host_validator_accepts_a_host_the_default_verification_would_reject(self):
        # "blogs.evil.com" does not share a parent domain with the seed, so the
        # default check rejects it; the callback overrides that decision.
        res = self._parse(
            "blogs.mongodb.com", "blogs.evil.com", srv_host_validator=lambda host: True
        )
        self.assertEqual(["blogs.evil.com"], [node[0] for node in res["nodelist"]])

        # "mongo.local" does not add a domain level to an SRV hostname with fewer
        # than three "." separated parts, which the default check also rejects.
        res = self._parse("mongo.local", "mongo.local", srv_host_validator=lambda host: True)
        self.assertEqual(["mongo.local"], [node[0] for node in res["nodelist"]])

    def test_6_reject_a_host_the_default_verification_would_accept(self):
        with self.assertRaisesRegex(ConfigurationError, "rejected by srv_host_validator"):
            self._parse(
                "blogs.mongodb.com", "cluster.mongodb.com", srv_host_validator=lambda host: False
            )

    def test_7_the_validator_receives_the_normalized_host_name(self):
        seen = []

        def validator(host):
            seen.append(host)
            return True

        self._parse("blogs.mongodb.com", "CLUSTER.MONGODB.COM.", srv_host_validator=validator)
        self.assertEqual(["cluster.mongodb.com"], seen)

    def test_8_wrap_an_error_raised_by_the_validator(self):
        original_exc = Exception("validator_error")

        def validator(host):
            raise original_exc

        with self.assertRaisesRegex(
            ConfigurationError, "srv_host_validator raised an exception"
        ) as ctx:
            self._parse("blogs.mongodb.com", "cluster.mongodb.com", srv_host_validator=validator)
        # The wrapping error must retain the error raised by the validator.
        self.assertIs(original_exc, ctx.exception.__cause__)
        self.assertIn("validator_error", str(ctx.exception))

    def test_9_throw_when_both_srv_allowed_hosts_suffix_and_srv_host_validator_are_configured(
        self,
    ):
        # Rejected by the client
        with self.assertRaisesRegex(ConfigurationError, "Cannot specify both"):
            self.simple_client(
                "mongodb+srv://blogs.mongodb.com",
                srv_host_validator=lambda host: True,
                srvAllowedHostsSuffix=".mongodb.com",
                connect=False,
            )

        # Rejected by the resolver
        with self.assertRaisesRegex(ConfigurationError, "Cannot specify both"):
            self._parse(
                "blogs.mongodb.com",
                "cluster.mongodb.com",
                srv_host_validator=lambda host: True,
                srv_allowed_hosts_suffix=".mongodb.com",
            )

    def test_10_accept_a_mixed_case_returned_address_with_srv_allowed_hosts_suffix(self):
        # Returned addresses are normalized before the suffix comparison, so
        # the case DNS happens to use must not affect the result.
        res = self._parse(
            "blogs.mongodb.com", "CLUSTER.MONGODB.COM.", srv_allowed_hosts_suffix=".mongodb.com"
        )
        self.assertEqual(["cluster.mongodb.com"], [node[0] for node in res["nodelist"]])

    def test_11_throw_when_srv_host_validator_is_not_callable(self):
        with self.assertRaisesRegex(ValueError, "must be a callable"):
            self.simple_client("mongodb+srv://blogs.mongodb.com", srv_host_validator="notacallable")

    def test_12_accept_a_reserved_single_label_as_srv_allowed_hosts_suffix(self):
        # A single label is a public suffix under the Public Suffix List's "*"
        # rule, but the reserved names are accepted despite that.
        res = self._parse(
            "cluster.localhost", "db.cluster.localhost", srv_allowed_hosts_suffix="localhost"
        )
        self.assertEqual(["db.cluster.localhost"], [node[0] for node in res["nodelist"]])

    def test_13_throw_when_srv_host_validator_is_used_with_a_non_srv_uri(self):
        with self.assertRaisesRegex(
            ConfigurationError, "only allowed with 'mongodb\\+srv://' URIs"
        ):
            self.simple_client(
                "mongodb://localhost:27017",
                srv_host_validator=lambda host: True,
                connect=False,
            )

    def test_srv_hostname_with_three_or_more_parts_may_equal_the_returned_hostname(
        self,
    ):
        test_cases = [
            {
                "query": "_mongodb._tcp.blogs.mongodb.com",
                "mock_target": "blogs.mongodb.com",
            },
        ]
        self.run_initial_dns_seedlist_discovery_prose_tests(test_cases)


class TestPublicSuffixListParsing(unittest.TestCase):
    def test_1_multi_label_ordinary_rule(self):
        self.assertTrue(is_public_suffix("com.ac"))
        self.assertFalse(is_public_suffix("foo.com.ac"))

    def test_2_long_wildcard_rule(self):
        self.assertTrue(is_public_suffix("abc.nom.br"))
        self.assertFalse(is_public_suffix("x.abc.nom.br"))

    def test_3_wildcard_rule(self):
        self.assertTrue(is_public_suffix("b.ck"))
        self.assertFalse(is_public_suffix("a.b.ck"))

    def test_4_exception_rule(self):
        self.assertTrue(is_public_suffix("ck"))
        self.assertFalse(is_public_suffix("www.ck"))

    def test_5_no_rule_matches(self):
        self.assertTrue(is_public_suffix("nosuchtld"))
        self.assertFalse(is_public_suffix("foo.nosuchtld"))

    def test_6_internationalized_rule(self):
        self.assertTrue(is_public_suffix("xn--p1ai"))
        self.assertTrue(is_public_suffix("xn--55qx5d.cn"))
        self.assertFalse(is_public_suffix("example.xn--p1ai"))


if __name__ == "__main__":
    unittest.main()
