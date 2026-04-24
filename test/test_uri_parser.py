# Copyright 2011-present MongoDB, Inc.
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

"""Test the pymongo uri_parser module."""
from __future__ import annotations

import copy
import sys
import warnings
from typing import Any
from urllib.parse import quote_plus

sys.path[0:0] = [""]

from test import unittest
from unittest.mock import patch

from bson.binary import JAVA_LEGACY
from pymongo import ReadPreference
from pymongo.errors import ConfigurationError, InvalidURI
from pymongo.synchronous.uri_parser import parse_uri
from pymongo.uri_parser_shared import (
    _unquoted_percent,
    parse_host,
    parse_ipv6_literal_host,
    parse_userinfo,
    split_hosts,
    split_options,
)


class TestURI(unittest.TestCase):
    def test_validate_userinfo(self):
        self.assertRaises(InvalidURI, parse_userinfo, "foo@")
        self.assertRaises(InvalidURI, parse_userinfo, ":password")
        self.assertRaises(InvalidURI, parse_userinfo, "fo::o:p@ssword")
        self.assertRaises(InvalidURI, parse_userinfo, ":")
        self.assertTrue(parse_userinfo("user:password"))
        self.assertEqual(("us:r", "p@ssword"), parse_userinfo("us%3Ar:p%40ssword"))
        self.assertEqual(("us er", "p ssword"), parse_userinfo("us+er:p+ssword"))
        self.assertEqual(("us er", "p ssword"), parse_userinfo("us%20er:p%20ssword"))
        self.assertEqual(("us+er", "p+ssword"), parse_userinfo("us%2Ber:p%2Bssword"))
        self.assertEqual(("dev1@FOO.COM", ""), parse_userinfo("dev1%40FOO.COM"))
        self.assertEqual(("dev1@FOO.COM", ""), parse_userinfo("dev1%40FOO.COM:"))

    def test_split_hosts(self):
        self.assertRaises(ConfigurationError, split_hosts, "localhost:27017,")
        self.assertRaises(ConfigurationError, split_hosts, ",localhost:27017")
        self.assertRaises(ConfigurationError, split_hosts, "localhost:27017,,localhost:27018")
        self.assertEqual(
            [("localhost", 27017), ("example.com", 27017)], split_hosts("localhost,example.com")
        )
        self.assertEqual(
            [("localhost", 27018), ("example.com", 27019)],
            split_hosts("localhost:27018,example.com:27019"),
        )
        self.assertEqual(
            [("/tmp/mongodb-27017.sock", None)], split_hosts("/tmp/mongodb-27017.sock")
        )
        self.assertEqual(
            [("/tmp/mongodb-27017.sock", None), ("example.com", 27017)],
            split_hosts("/tmp/mongodb-27017.sock,example.com:27017"),
        )
        self.assertEqual(
            [("example.com", 27017), ("/tmp/mongodb-27017.sock", None)],
            split_hosts("example.com:27017,/tmp/mongodb-27017.sock"),
        )
        self.assertRaises(ValueError, split_hosts, "::1", 27017)
        self.assertRaises(ValueError, split_hosts, "[::1:27017")
        self.assertRaises(ValueError, split_hosts, "::1")
        self.assertRaises(ValueError, split_hosts, "::1]:27017")
        self.assertEqual([("::1", 27017)], split_hosts("[::1]:27017"))
        self.assertEqual([("::1", 27017)], split_hosts("[::1]"))

    def test_split_options(self):
        self.assertRaises(ConfigurationError, split_options, "foo")
        self.assertRaises(ConfigurationError, split_options, "foo=bar;foo")
        self.assertTrue(split_options("ssl=true"))
        self.assertTrue(split_options("connect=true"))
        self.assertTrue(split_options("tlsAllowInvalidHostnames=false"))

        # Test Invalid URI options that should throw warnings.
        with warnings.catch_warnings():
            warnings.filterwarnings("error")
            self.assertRaises(Warning, split_options, "foo=bar", warn=True)
            self.assertRaises(Warning, split_options, "socketTimeoutMS=foo", warn=True)
            self.assertRaises(Warning, split_options, "socketTimeoutMS=0.0", warn=True)
            self.assertRaises(Warning, split_options, "connectTimeoutMS=foo", warn=True)
            self.assertRaises(Warning, split_options, "connectTimeoutMS=0.0", warn=True)
            self.assertRaises(Warning, split_options, "connectTimeoutMS=1e100000", warn=True)
            self.assertRaises(Warning, split_options, "connectTimeoutMS=-1e100000", warn=True)
            self.assertRaises(Warning, split_options, "ssl=foo", warn=True)
            self.assertRaises(Warning, split_options, "connect=foo", warn=True)
            self.assertRaises(Warning, split_options, "tlsAllowInvalidHostnames=foo", warn=True)
            self.assertRaises(Warning, split_options, "connectTimeoutMS=inf", warn=True)
            self.assertRaises(Warning, split_options, "connectTimeoutMS=-inf", warn=True)
            self.assertRaises(Warning, split_options, "wtimeoutms=foo", warn=True)
            self.assertRaises(Warning, split_options, "wtimeoutms=5.5", warn=True)
            self.assertRaises(Warning, split_options, "fsync=foo", warn=True)
            self.assertRaises(Warning, split_options, "fsync=5.5", warn=True)
            self.assertRaises(Warning, split_options, "authMechanism=foo", warn=True)

        # Test invalid options with warn=False.
        self.assertRaises(ConfigurationError, split_options, "foo=bar")
        self.assertRaises(ValueError, split_options, "socketTimeoutMS=foo")
        self.assertRaises(ValueError, split_options, "socketTimeoutMS=0.0")
        self.assertRaises(ValueError, split_options, "connectTimeoutMS=foo")
        self.assertRaises(ValueError, split_options, "connectTimeoutMS=0.0")
        self.assertRaises(ValueError, split_options, "connectTimeoutMS=1e100000")
        self.assertRaises(ValueError, split_options, "connectTimeoutMS=-1e100000")
        self.assertRaises(ValueError, split_options, "ssl=foo")
        self.assertRaises(ValueError, split_options, "connect=foo")
        self.assertRaises(ValueError, split_options, "tlsAllowInvalidHostnames=foo")
        self.assertRaises(ValueError, split_options, "connectTimeoutMS=inf")
        self.assertRaises(ValueError, split_options, "connectTimeoutMS=-inf")
        self.assertRaises(ValueError, split_options, "wtimeoutms=foo")
        self.assertRaises(ValueError, split_options, "wtimeoutms=5.5")
        self.assertRaises(ValueError, split_options, "fsync=foo")
        self.assertRaises(ValueError, split_options, "fsync=5.5")
        self.assertRaises(ValueError, split_options, "authMechanism=foo")

        # Test splitting options works when valid.
        self.assertTrue(split_options("socketTimeoutMS=300"))
        self.assertTrue(split_options("connectTimeoutMS=300"))
        self.assertEqual({"sockettimeoutms": 0.3}, split_options("socketTimeoutMS=300"))
        self.assertEqual({"sockettimeoutms": 0.0001}, split_options("socketTimeoutMS=0.1"))
        self.assertEqual({"connecttimeoutms": 0.3}, split_options("connectTimeoutMS=300"))
        self.assertEqual({"connecttimeoutms": 0.0001}, split_options("connectTimeoutMS=0.1"))
        self.assertTrue(split_options("connectTimeoutMS=300"))
        self.assertIsInstance(split_options("w=5")["w"], int)
        self.assertIsInstance(split_options("w=5.5")["w"], str)
        self.assertTrue(split_options("w=foo"))
        self.assertTrue(split_options("w=majority"))
        self.assertTrue(split_options("wtimeoutms=500"))
        self.assertEqual({"fsync": True}, split_options("fsync=true"))
        self.assertEqual({"fsync": False}, split_options("fsync=false"))
        self.assertEqual({"authMechanism": "GSSAPI"}, split_options("authMechanism=GSSAPI"))
        self.assertEqual(
            {"authMechanism": "SCRAM-SHA-1"}, split_options("authMechanism=SCRAM-SHA-1")
        )
        self.assertEqual({"authsource": "foobar"}, split_options("authSource=foobar"))
        self.assertEqual({"maxpoolsize": 50}, split_options("maxpoolsize=50"))

        # Test suggestions given when invalid kwarg passed

        expected = r"Unknown option: auth. Did you mean one of \(authsource, authmechanism, timeoutms\) or maybe a camelCase version of one\? Refer to docstring."
        with self.assertRaisesRegex(ConfigurationError, expected):
            split_options("auth=GSSAPI")

    def test_parse_uri(self):
        self.assertRaises(InvalidURI, parse_uri, "http://foobar.com")
        self.assertRaises(InvalidURI, parse_uri, "http://foo@foobar.com")
        self.assertRaises(ValueError, parse_uri, "mongodb://::1", 27017)

        orig: dict = {
            "nodelist": [("localhost", 27017)],
            "username": None,
            "password": None,
            "database": None,
            "collection": None,
            "options": {},
            "fqdn": None,
        }

        res: dict = copy.deepcopy(orig)
        self.assertEqual(res, parse_uri("mongodb://localhost"))

        res.update({"username": "fred", "password": "foobar"})
        self.assertEqual(res, parse_uri("mongodb://fred:foobar@localhost"))

        res.update({"database": "baz"})
        self.assertEqual(res, parse_uri("mongodb://fred:foobar@localhost/baz"))

        res = copy.deepcopy(orig)
        res["nodelist"] = [("example1.com", 27017), ("example2.com", 27017)]
        self.assertEqual(res, parse_uri("mongodb://example1.com:27017,example2.com:27017"))

        res = copy.deepcopy(orig)
        res["nodelist"] = [("localhost", 27017), ("localhost", 27018), ("localhost", 27019)]
        self.assertEqual(res, parse_uri("mongodb://localhost,localhost:27018,localhost:27019"))

        res = copy.deepcopy(orig)
        res["database"] = "foo"
        self.assertEqual(res, parse_uri("mongodb://localhost/foo"))

        res = copy.deepcopy(orig)
        self.assertEqual(res, parse_uri("mongodb://localhost/"))

        res.update({"database": "test", "collection": "yield_historical.in"})
        self.assertEqual(res, parse_uri("mongodb://localhost/test.yield_historical.in"))

        res.update({"username": "fred", "password": "foobar"})
        self.assertEqual(res, parse_uri("mongodb://fred:foobar@localhost/test.yield_historical.in"))

        res = copy.deepcopy(orig)
        res["nodelist"] = [("example1.com", 27017), ("example2.com", 27017)]
        res.update({"database": "test", "collection": "yield_historical.in"})
        self.assertEqual(
            res,
            parse_uri("mongodb://example1.com:27017,example2.com:27017/test.yield_historical.in"),
        )

        # Test socket path without escaped characters.
        self.assertRaises(InvalidURI, parse_uri, "mongodb:///tmp/mongodb-27017.sock")

        # Test with escaped characters.
        res = copy.deepcopy(orig)
        res["nodelist"] = [("example2.com", 27017), ("/tmp/mongodb-27017.sock", None)]
        self.assertEqual(res, parse_uri("mongodb://example2.com,%2Ftmp%2Fmongodb-27017.sock"))

        res = copy.deepcopy(orig)
        res["nodelist"] = [("shoe.sock.pants.co.uk", 27017), ("/tmp/mongodb-27017.sock", None)]
        res["database"] = "nethers_db"
        self.assertEqual(
            res,
            parse_uri("mongodb://shoe.sock.pants.co.uk,%2Ftmp%2Fmongodb-27017.sock/nethers_db"),
        )

        res = copy.deepcopy(orig)
        res["nodelist"] = [("/tmp/mongodb-27017.sock", None), ("example2.com", 27017)]
        res.update({"database": "test", "collection": "yield_historical.in"})
        self.assertEqual(
            res,
            parse_uri(
                "mongodb://%2Ftmp%2Fmongodb-27017.sock,"
                "example2.com:27017"
                "/test.yield_historical.in"
            ),
        )

        res = copy.deepcopy(orig)
        res["nodelist"] = [("/tmp/mongodb-27017.sock", None), ("example2.com", 27017)]
        res.update({"database": "test", "collection": "yield_historical.sock"})
        self.assertEqual(
            res,
            parse_uri(
                "mongodb://%2Ftmp%2Fmongodb-27017.sock,"
                "example2.com:27017/test.yield_historical"
                ".sock"
            ),
        )

        res = copy.deepcopy(orig)
        res["nodelist"] = [("example2.com", 27017)]
        res.update({"database": "test", "collection": "yield_historical.sock"})
        self.assertEqual(res, parse_uri("mongodb://example2.com:27017/test.yield_historical.sock"))

        res = copy.deepcopy(orig)
        res["nodelist"] = [("/tmp/mongodb-27017.sock", None)]
        res.update({"database": "test", "collection": "mongodb-27017.sock"})
        self.assertEqual(
            res, parse_uri("mongodb://%2Ftmp%2Fmongodb-27017.sock/test.mongodb-27017.sock")
        )

        res = copy.deepcopy(orig)
        res["nodelist"] = [
            ("/tmp/mongodb-27020.sock", None),
            ("::1", 27017),
            ("2001:0db8:85a3:0000:0000:8a2e:0370:7334", 27018),
            ("192.168.0.212", 27019),
            ("localhost", 27018),
        ]
        self.assertEqual(
            res,
            parse_uri(
                "mongodb://%2Ftmp%2Fmongodb-27020.sock"
                ",[::1]:27017,[2001:0db8:"
                "85a3:0000:0000:8a2e:0370:7334],"
                "192.168.0.212:27019,localhost",
                27018,
            ),
        )

        res = copy.deepcopy(orig)
        res.update({"username": "fred", "password": "foobar"})
        res.update({"database": "test", "collection": "yield_historical.in"})
        self.assertEqual(res, parse_uri("mongodb://fred:foobar@localhost/test.yield_historical.in"))

        res = copy.deepcopy(orig)
        res["database"] = "test"
        res["collection"] = 'name/with "delimiters'
        self.assertEqual(res, parse_uri('mongodb://localhost/test.name/with "delimiters'))

        res = copy.deepcopy(orig)
        res["options"] = {"readPreference": ReadPreference.SECONDARY.mongos_mode}
        self.assertEqual(res, parse_uri("mongodb://localhost/?readPreference=secondary"))

        # Various authentication tests
        res = copy.deepcopy(orig)
        res["options"] = {"authMechanism": "SCRAM-SHA-256"}
        res["username"] = "user"
        res["password"] = "password"
        self.assertEqual(
            res, parse_uri("mongodb://user:password@localhost/?authMechanism=SCRAM-SHA-256")
        )

        res = copy.deepcopy(orig)
        res["options"] = {"authMechanism": "SCRAM-SHA-256", "authSource": "bar"}
        res["username"] = "user"
        res["password"] = "password"
        res["database"] = "foo"
        self.assertEqual(
            res,
            parse_uri(
                "mongodb://user:password@localhost/foo?authSource=bar;authMechanism=SCRAM-SHA-256"
            ),
        )

        res = copy.deepcopy(orig)
        res["options"] = {"authMechanism": "SCRAM-SHA-256"}
        res["username"] = "user"
        res["password"] = ""
        self.assertEqual(res, parse_uri("mongodb://user:@localhost/?authMechanism=SCRAM-SHA-256"))

        res = copy.deepcopy(orig)
        res["username"] = "user@domain.com"
        res["password"] = "password"
        res["database"] = "foo"
        self.assertEqual(res, parse_uri("mongodb://user%40domain.com:password@localhost/foo"))

        res = copy.deepcopy(orig)
        res["options"] = {"authMechanism": "GSSAPI"}
        res["username"] = "user@domain.com"
        res["password"] = "password"
        res["database"] = "foo"
        self.assertEqual(
            res,
            parse_uri("mongodb://user%40domain.com:password@localhost/foo?authMechanism=GSSAPI"),
        )

        res = copy.deepcopy(orig)
        res["options"] = {"authMechanism": "GSSAPI"}
        res["username"] = "user@domain.com"
        res["password"] = ""
        res["database"] = "foo"
        self.assertEqual(
            res, parse_uri("mongodb://user%40domain.com@localhost/foo?authMechanism=GSSAPI")
        )

        res = copy.deepcopy(orig)
        res["options"] = {
            "readPreference": ReadPreference.SECONDARY.mongos_mode,
            "readPreferenceTags": [
                {"dc": "west", "use": "website"},
                {"dc": "east", "use": "website"},
            ],
        }
        res["username"] = "user@domain.com"
        res["password"] = "password"
        res["database"] = "foo"
        self.assertEqual(
            res,
            parse_uri(
                "mongodb://user%40domain.com:password"
                "@localhost/foo?readpreference=secondary&"
                "readpreferencetags=dc:west,use:website&"
                "readpreferencetags=dc:east,use:website"
            ),
        )

        res = copy.deepcopy(orig)
        res["options"] = {
            "readPreference": ReadPreference.SECONDARY.mongos_mode,
            "readPreferenceTags": [
                {"dc": "west", "use": "website"},
                {"dc": "east", "use": "website"},
                {},
            ],
        }
        res["username"] = "user@domain.com"
        res["password"] = "password"
        res["database"] = "foo"
        self.assertEqual(
            res,
            parse_uri(
                "mongodb://user%40domain.com:password"
                "@localhost/foo?readpreference=secondary&"
                "readpreferencetags=dc:west,use:website&"
                "readpreferencetags=dc:east,use:website&"
                "readpreferencetags="
            ),
        )

        res = copy.deepcopy(orig)
        res["options"] = {"uuidrepresentation": JAVA_LEGACY}
        res["username"] = "user@domain.com"
        res["password"] = "password"
        res["database"] = "foo"
        self.assertEqual(
            res,
            parse_uri(
                "mongodb://user%40domain.com:password"
                "@localhost/foo?uuidrepresentation="
                "javaLegacy"
            ),
        )

        with warnings.catch_warnings():
            warnings.filterwarnings("error")
            self.assertRaises(
                Warning,
                parse_uri,
                "mongodb://user%40domain.com:password"
                "@localhost/foo?uuidrepresentation=notAnOption",
                warn=True,
            )
        self.assertRaises(
            ValueError,
            parse_uri,
            "mongodb://user%40domain.com:password@localhost/foo?uuidrepresentation=notAnOption",
        )

    def test_parse_ssl_paths(self):
        # Turn off "validate" since these paths don't exist on filesystem.
        self.assertEqual(
            {
                "collection": None,
                "database": None,
                "nodelist": [("/MongoDB.sock", None)],
                "options": {"tlsCertificateKeyFile": "/a/b"},
                "password": "foo/bar",
                "username": "jesse",
                "fqdn": None,
            },
            parse_uri(
                "mongodb://jesse:foo%2Fbar@%2FMongoDB.sock/?tlsCertificateKeyFile=/a/b",
                validate=False,
            ),
        )

        self.assertEqual(
            {
                "collection": None,
                "database": None,
                "nodelist": [("/MongoDB.sock", None)],
                "options": {"tlsCertificateKeyFile": "a/b"},
                "password": "foo/bar",
                "username": "jesse",
                "fqdn": None,
            },
            parse_uri(
                "mongodb://jesse:foo%2Fbar@%2FMongoDB.sock/?tlsCertificateKeyFile=a/b",
                validate=False,
            ),
        )

    def test_tlsinsecure_simple(self):
        # check that tlsInsecure is expanded correctly.
        self.maxDiff = None
        uri = "mongodb://example.com/?tlsInsecure=true"
        res = {
            "tlsAllowInvalidHostnames": True,
            "tlsAllowInvalidCertificates": True,
            "tlsInsecure": True,
            "tlsDisableOCSPEndpointCheck": True,
        }
        self.assertEqual(res, parse_uri(uri)["options"])

    def test_normalize_options(self):
        # check that options are converted to their internal names correctly.
        uri = "mongodb://example.com/?ssl=true&appname=myapp"
        res = {"tls": True, "appname": "myapp"}
        self.assertEqual(res, parse_uri(uri)["options"])

    def test_unquote_during_parsing(self):
        quoted_val = "val%21%40%23%24%25%5E%26%2A%28%29_%2B%3A+etc"
        unquoted_val = "val!@#$%^&*()_+: etc"
        uri = (
            "mongodb://user:password@localhost/?authMechanism=MONGODB-AWS"
            "&authMechanismProperties=AWS_SESSION_TOKEN:" + quoted_val
        )
        res = parse_uri(uri)
        options: dict[str, Any] = {
            "authMechanism": "MONGODB-AWS",
            "authMechanismProperties": {"AWS_SESSION_TOKEN": unquoted_val},
        }
        self.assertEqual(options, res["options"])

        uri = (
            "mongodb://localhost/foo?readpreference=secondary&"
            "readpreferencetags=dc:west," + quoted_val + ":" + quoted_val + "&"
            "readpreferencetags=dc:east,use:" + quoted_val
        )
        res = parse_uri(uri)
        options = {
            "readPreference": ReadPreference.SECONDARY.mongos_mode,
            "readPreferenceTags": [
                {"dc": "west", unquoted_val: unquoted_val},
                {"dc": "east", "use": unquoted_val},
            ],
        }
        self.assertEqual(options, res["options"])

    def test_redact_AWS_SESSION_TOKEN(self):
        token = "token"
        uri = (
            "mongodb://user:password@localhost/?authMechanism=MONGODB-AWS"
            "&authMechanismProperties=AWS_SESSION_TOKEN-" + token
        )
        with self.assertRaisesRegex(
            ValueError,
            "Malformed auth mechanism properties",
        ):
            parse_uri(uri)

    def test_handle_colon(self):
        token = "token:foo"
        uri = (
            "mongodb://user:password@localhost/?authMechanism=MONGODB-AWS"
            "&authMechanismProperties=AWS_SESSION_TOKEN:" + token
        )
        res = parse_uri(uri)
        options = {
            "authMechanism": "MONGODB-AWS",
            "authMechanismProperties": {"AWS_SESSION_TOKEN": token},
        }
        self.assertEqual(options, res["options"])

    def test_special_chars(self):
        user = "user@ /9+:?~!$&'()*+,;="
        pwd = "pwd@ /9+:?~!$&'()*+,;="
        uri = f"mongodb://{quote_plus(user)}:{quote_plus(pwd)}@localhost"
        res = parse_uri(uri)
        self.assertEqual(user, res["username"])
        self.assertEqual(pwd, res["password"])

    def test_do_not_include_password_in_port_message(self):
        with self.assertRaisesRegex(ValueError, "Port must be an integer between 0 and 65535"):
            parse_uri("mongodb://localhost:65536")
        with self.assertRaisesRegex(
            ValueError, "Port contains non-digit characters. Hint: username "
        ) as ctx:
            parse_uri("mongodb://user:PASS /@localhost:27017")
        self.assertNotIn("PASS", str(ctx.exception))

        # This "invalid" case is technically a valid URI:
        res = parse_uri("mongodb://user:1234/@localhost:27017")
        self.assertEqual([("user", 1234)], res["nodelist"])
        self.assertEqual("@localhost:27017", res["database"])

    def test_port_with_whitespace(self):
        with self.assertRaisesRegex(ValueError, "Port contains whitespace character: ' '"):
            parse_uri("mongodb://localhost:27017 ")
        with self.assertRaisesRegex(ValueError, "Port contains whitespace character: ' '"):
            parse_uri("mongodb://localhost: 27017")
        with self.assertRaisesRegex(ValueError, r"Port contains whitespace character: '\\n'"):
            parse_uri("mongodb://localhost:27\n017")

    def test_parse_uri_options_type(self):
        opts = parse_uri("mongodb://localhost:27017")["options"]
        self.assertIsInstance(opts, dict)

    def test_unquoted_percent(self):
        # Empty string and strings without percent signs are always safe.
        self.assertFalse(_unquoted_percent(""))
        self.assertFalse(_unquoted_percent("no_percent_here"))
        # Valid percent-encoded sequences are not flagged.
        self.assertFalse(_unquoted_percent("%25"))  # %25 decodes to literal "%"
        self.assertFalse(_unquoted_percent("%40"))  # %40 decodes to "@"
        self.assertFalse(_unquoted_percent("user%40domain.com"))
        self.assertFalse(_unquoted_percent("%2B"))  # %2B decodes to "+"
        self.assertFalse(_unquoted_percent("%E2%85%A8"))  # multi-byte sequence
        self.assertFalse(_unquoted_percent("%2525"))  # double-encoded: %25 -> %
        # Unescaped percent signs (invalid percent encodings) are flagged.
        self.assertTrue(_unquoted_percent("%foo"))  # 'o' is not a hex digit
        self.assertTrue(_unquoted_percent("50%off"))  # 'o' is not a hex digit
        self.assertTrue(_unquoted_percent("100%"))  # trailing bare %

    def test_parse_ipv6_literal_host_direct(self):
        # IPv6 without explicit port uses default_port.
        self.assertEqual(("::1", 27017), parse_ipv6_literal_host("[::1]", 27017))
        self.assertEqual(("::1", None), parse_ipv6_literal_host("[::1]", None))
        # IPv6 with explicit port returns port as a string (int conversion
        # happens later in parse_host).
        host, port = parse_ipv6_literal_host("[::1]:27018", 27017)
        self.assertEqual("::1", host)
        self.assertEqual("27018", port)
        # Full-form IPv6 address without port.
        full_ipv6 = "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
        host, port = parse_ipv6_literal_host(f"[{full_ipv6}]", 27017)
        self.assertEqual(full_ipv6, host)
        self.assertEqual(27017, port)
        # Missing closing bracket must raise.
        self.assertRaises(ValueError, parse_ipv6_literal_host, "[::1", 27017)

    def test_parse_host_case_normalization(self):
        # Hostnames are normalized to lowercase (RFC 4343).
        self.assertEqual(("localhost", 27017), parse_host("LOCALHOST:27017"))
        self.assertEqual(("example.com", 27017), parse_host("Example.COM"))
        self.assertEqual(("example.com", 27017), parse_host("EXAMPLE.COM:27017"))
        # IP addresses are unaffected but still lowercased (no-op for digits).
        self.assertEqual(("192.168.1.1", 27017), parse_host("192.168.1.1:27017"))
        # IPv6 literal addresses are lowercased.
        self.assertEqual(("::1", 27017), parse_host("[::1]:27017"))

    def test_parse_host_port_boundaries(self):
        # Ports 1-65535 are valid.
        self.assertEqual(("localhost", 1), parse_host("localhost:1"))
        self.assertEqual(("localhost", 65535), parse_host("localhost:65535"))
        # Port 0 is invalid.
        self.assertRaises(ValueError, parse_host, "localhost:0")
        # Port 65536 is invalid.
        self.assertRaises(ValueError, parse_host, "localhost:65536")

    def test_tls_option_conflicts(self):
        # tlsInsecure cannot coexist with any of the options it implicitly sets.
        self.assertRaises(
            InvalidURI, split_options, "tlsInsecure=true&tlsAllowInvalidCertificates=true"
        )
        # The conflict is based on presence, not value.
        self.assertRaises(
            InvalidURI, split_options, "tlsInsecure=true&tlsAllowInvalidHostnames=false"
        )
        self.assertRaises(
            InvalidURI, split_options, "tlsInsecure=true&tlsDisableOCSPEndpointCheck=true"
        )
        # tlsAllowInvalidCertificates and tlsDisableOCSPEndpointCheck are mutually exclusive.
        self.assertRaises(
            InvalidURI,
            split_options,
            "tlsAllowInvalidCertificates=true&tlsDisableOCSPEndpointCheck=true",
        )
        # ssl and tls must agree when both are present.
        self.assertRaises(InvalidURI, split_options, "ssl=true&tls=false")
        self.assertRaises(InvalidURI, split_options, "ssl=false&tls=true")
        # Matching ssl/tls values are allowed.
        self.assertIsNotNone(split_options("ssl=true&tls=true"))
        self.assertIsNotNone(split_options("ssl=false&tls=false"))

    def test_split_options_mixed_delimiters(self):
        # Mixing '&' and ';' as option separators is not permitted.
        self.assertRaises(InvalidURI, split_options, "ssl=true&tls=true;appname=foo")
        self.assertRaises(InvalidURI, split_options, "appname=foo;ssl=true&tls=true")

    def test_split_options_duplicate_warning(self):
        # Specifying the same option key more than once emits a warning.
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            split_options("appname=foo&appname=bar")
        self.assertEqual(1, len(w))
        self.assertIn("Duplicate URI option", str(w[0].message))

    def test_split_options_empty_authsource(self):
        # An empty authSource value must be rejected.
        self.assertRaises(InvalidURI, split_options, "authSource=")

    def test_check_options_conflicts(self):
        # directConnection=true is incompatible with multiple hosts.
        self.assertRaises(
            ConfigurationError,
            parse_uri,
            "mongodb://host1,host2/?directConnection=true",
        )
        # loadBalanced=true is incompatible with multiple hosts.
        self.assertRaises(
            ConfigurationError,
            parse_uri,
            "mongodb://host1,host2/?loadBalanced=true",
        )
        # directConnection=true and loadBalanced=true cannot coexist.
        self.assertRaises(
            ConfigurationError,
            parse_uri,
            "mongodb://localhost/?directConnection=true&loadBalanced=true",
        )
        # loadBalanced=true and replicaSet cannot coexist.
        self.assertRaises(
            ConfigurationError,
            parse_uri,
            "mongodb://localhost/?loadBalanced=true&replicaSet=rs0",
        )

    def test_validate_uri_edge_cases(self):
        # URI with nothing after the scheme is invalid.
        self.assertRaises(InvalidURI, parse_uri, "mongodb://")
        # srvServiceName is only valid with mongodb+srv://.
        self.assertRaises(
            ConfigurationError,
            parse_uri,
            "mongodb://localhost/?srvServiceName=myService",
        )
        # srvMaxHosts is only valid with mongodb+srv://.
        self.assertRaises(
            ConfigurationError,
            parse_uri,
            "mongodb://localhost/?srvMaxHosts=1",
        )
        # Dollar sign is a prohibited character in database names.
        self.assertRaises(InvalidURI, parse_uri, "mongodb://localhost/%24db")
        # Space is a prohibited character in database names.
        self.assertRaises(InvalidURI, parse_uri, "mongodb://localhost/my%20db")

    def test_validate_uri_srv_structure(self):
        # SRV URIs require exactly one hostname with no port number.
        with patch("pymongo.uri_parser_shared._have_dnspython", return_value=True):
            # Multiple hosts in an SRV URI are not allowed.
            self.assertRaises(
                InvalidURI,
                parse_uri,
                "mongodb+srv://host1.example.com,host2.example.com",
            )
            # A port number in a SRV URI is not allowed.
            self.assertRaises(
                InvalidURI,
                parse_uri,
                "mongodb+srv://host1.example.com:27017",
            )
            # directConnection=true is incompatible with SRV URIs.
            self.assertRaises(
                ConfigurationError,
                parse_uri,
                "mongodb+srv://host1.example.com/?directConnection=true",
            )
        # Without dnspython installed, SRV URIs raise ConfigurationError.
        with patch("pymongo.uri_parser_shared._have_dnspython", return_value=False):
            self.assertRaises(
                ConfigurationError,
                parse_uri,
                "mongodb+srv://host1.example.com",
            )


if __name__ == "__main__":
    unittest.main()
