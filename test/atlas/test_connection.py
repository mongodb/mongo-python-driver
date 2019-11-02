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


_REPL = os.environ.get("ATLAS_REPL")
_SHRD = os.environ.get("ATLAS_SHRD")
_FREE = os.environ.get("ATLAS_FREE")
_TLS11 = os.environ.get("ATLAS_TLS11")
_TLS12 = os.environ.get("ATLAS_TLS12")


def _connect(uri):
    client = pymongo.MongoClient(uri)
    # No TLS error
    client.admin.command('ismaster')
    # No auth error
    client.test.test.count_documents({})


class TestAtlasConnect(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not all([_REPL, _SHRD, _FREE]):
            raise Exception(
                "Must set ATLAS_REPL/SHRD/FREE env variables to test.")

    def test_replica_set(self):
        _connect(_REPL)

    def test_sharded_cluster(self):
        _connect(_SHRD)

    def test_free_tier(self):
        if not HAS_SNI:
            raise unittest.SkipTest("Free tier requires SNI support.")
        _connect(_FREE)

    def test_tls_11(self):
        _connect(_TLS11)

    def test_tls_12(self):
        _connect(_TLS12)


if __name__ == '__main__':
    unittest.main()
