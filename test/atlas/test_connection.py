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

sys.path[0:0] = [""]

import pymongo
from pymongo.ssl_support import HAS_SNI

try:
    import dns
    HAS_DNS = True
except ImportError:
    HAS_DNS = False


ATLAS_REPL = os.environ.get("ATLAS_REPL")
ATLAS_SHRD = os.environ.get("ATLAS_SHRD")
ATLAS_FREE = os.environ.get("ATLAS_FREE")
ATLAS_TLS11 = os.environ.get("ATLAS_TLS11")
ATLAS_TLS12 = os.environ.get("ATLAS_TLS12")
SRV_ATLAS_REPL = os.environ.get("SRV_ATLAS_REPL")
SRV_ATLAS_SHRD = os.environ.get("SRV_ATLAS_SHRD")
SRV_ATLAS_FREE = os.environ.get("SRV_ATLAS_FREE")
SRV_ATLAS_TLS11 = os.environ.get("SRV_ATLAS_TLS11")
SRV_ATLAS_TLS12 = os.environ.get("SRV_ATLAS_TLS12")

PYMONGO_MUST_HAVE_DNS = os.environ.get("PYMONGO_MUST_HAVE_DNS")


def _connect(uri):
    if not uri:
        raise Exception("Must set env variable to test.")
    client = pymongo.MongoClient(uri)
    # No TLS error
    client.admin.command('ismaster')
    # No auth error
    client.test.test.count_documents({})


class TestAtlasConnect(unittest.TestCase):
    @unittest.skipUnless(HAS_SNI, 'Free tier requires SNI support')
    def test_free_tier(self):
        _connect(ATLAS_FREE)

    def test_replica_set(self):
        _connect(ATLAS_REPL)

    def test_sharded_cluster(self):
        _connect(ATLAS_SHRD)

    def test_tls_11(self):
        _connect(ATLAS_TLS11)

    def test_tls_12(self):
        _connect(ATLAS_TLS12)

    @unittest.skipUnless(HAS_DNS, 'SRV requires dnspython')
    @unittest.skipUnless(HAS_SNI, 'Free tier requires SNI support')
    def test_srv_free_tier(self):
        _connect(SRV_ATLAS_FREE)

    @unittest.skipUnless(HAS_DNS, 'SRV requires dnspython')
    def test_srv_replica_set(self):
        _connect(SRV_ATLAS_REPL)

    @unittest.skipUnless(HAS_DNS, 'SRV requires dnspython')
    def test_srv_sharded_cluster(self):
        _connect(SRV_ATLAS_SHRD)

    @unittest.skipUnless(HAS_DNS, 'SRV requires dnspython')
    def test_srv_tls_11(self):
        _connect(SRV_ATLAS_TLS11)

    @unittest.skipUnless(HAS_DNS, 'SRV requires dnspython')
    def test_srv_tls_12(self):
        _connect(SRV_ATLAS_TLS12)

    @unittest.skipUnless(PYMONGO_MUST_HAVE_DNS, 'dnspython is optional')
    def test_dnspython_was_installed(self):
        self.assertTrue(HAS_DNS, 'PYMONGO_MUST_HAVE_DNS is set but dns could '
                                 'not be imported')


if __name__ == '__main__':
    unittest.main()
