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

"""Test connections to various Atlas cluster types."""

import os
import sys
import unittest
from collections import defaultdict

sys.path[0:0] = [""]

import pymongo
from pymongo.ssl_support import HAS_SNI

try:
    import dns  # noqa

    HAS_DNS = True
except ImportError:
    HAS_DNS = False


URIS = {
    "ATLAS_REPL": os.environ.get("ATLAS_REPL"),
    "ATLAS_SHRD": os.environ.get("ATLAS_SHRD"),
    "ATLAS_FREE": os.environ.get("ATLAS_FREE"),
    "ATLAS_TLS11": os.environ.get("ATLAS_TLS11"),
    "ATLAS_TLS12": os.environ.get("ATLAS_TLS12"),
    "ATLAS_SERVERLESS": os.environ.get("ATLAS_SERVERLESS"),
    "ATLAS_SRV_REPL": os.environ.get("ATLAS_SRV_REPL"),
    "ATLAS_SRV_SHRD": os.environ.get("ATLAS_SRV_SHRD"),
    "ATLAS_SRV_FREE": os.environ.get("ATLAS_SRV_FREE"),
    "ATLAS_SRV_TLS11": os.environ.get("ATLAS_SRV_TLS11"),
    "ATLAS_SRV_TLS12": os.environ.get("ATLAS_SRV_TLS12"),
    "ATLAS_SRV_SERVERLESS": os.environ.get("ATLAS_SRV_SERVERLESS"),
}

# Set this variable to true to run the SRV tests even when dnspython is not
# installed.
MUST_TEST_SRV = os.environ.get("MUST_TEST_SRV")


def connect(uri):
    if not uri:
        raise Exception("Must set env variable to test.")
    client = pymongo.MongoClient(uri)
    # No TLS error
    client.admin.command("ping")
    # No auth error
    client.test.test.count_documents({})


class TestAtlasConnect(unittest.TestCase):
    @unittest.skipUnless(HAS_SNI, "Free tier requires SNI support")
    def test_free_tier(self):
        connect(URIS["ATLAS_FREE"])

    def test_replica_set(self):
        connect(URIS["ATLAS_REPL"])

    def test_sharded_cluster(self):
        connect(URIS["ATLAS_SHRD"])

    def test_tls_11(self):
        connect(URIS["ATLAS_TLS11"])

    def test_tls_12(self):
        connect(URIS["ATLAS_TLS12"])

    def test_serverless(self):
        connect(URIS["ATLAS_SERVERLESS"])

    def connect_srv(self, uri):
        connect(uri)
        self.assertIn("mongodb+srv://", uri)

    @unittest.skipUnless(HAS_SNI, "Free tier requires SNI support")
    @unittest.skipUnless(HAS_DNS or MUST_TEST_SRV, "SRV requires dnspython")
    def test_srv_free_tier(self):
        self.connect_srv(URIS["ATLAS_SRV_FREE"])

    @unittest.skipUnless(HAS_DNS or MUST_TEST_SRV, "SRV requires dnspython")
    def test_srv_replica_set(self):
        self.connect_srv(URIS["ATLAS_SRV_REPL"])

    @unittest.skipUnless(HAS_DNS or MUST_TEST_SRV, "SRV requires dnspython")
    def test_srv_sharded_cluster(self):
        self.connect_srv(URIS["ATLAS_SRV_SHRD"])

    @unittest.skipUnless(HAS_DNS or MUST_TEST_SRV, "SRV requires dnspython")
    def test_srv_tls_11(self):
        self.connect_srv(URIS["ATLAS_SRV_TLS11"])

    @unittest.skipUnless(HAS_DNS or MUST_TEST_SRV, "SRV requires dnspython")
    def test_srv_tls_12(self):
        self.connect_srv(URIS["ATLAS_SRV_TLS12"])

    @unittest.skipUnless(HAS_DNS or MUST_TEST_SRV, "SRV requires dnspython")
    def test_srv_serverless(self):
        self.connect_srv(URIS["ATLAS_SRV_SERVERLESS"])

    def test_uniqueness(self):
        """Ensure that we don't accidentally duplicate the test URIs."""
        uri_to_names = defaultdict(list)
        for name, uri in URIS.items():
            if uri:
                uri_to_names[uri].append(name)
        duplicates = [names for names in uri_to_names.values() if len(names) > 1]
        self.assertFalse(
            duplicates,
            "Error: the following env variables have duplicate values: %s" % (duplicates,),
        )


if __name__ == "__main__":
    unittest.main()
