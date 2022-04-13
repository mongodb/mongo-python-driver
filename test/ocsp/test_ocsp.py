# Copyright 2020-present MongoDB, Inc.
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

"""Test OCSP."""

import logging
import os
import sys
import unittest

sys.path[0:0] = [""]

import pymongo
from pymongo.errors import ServerSelectionTimeoutError

CA_FILE = os.environ.get("CA_FILE")
OCSP_TLS_SHOULD_SUCCEED = os.environ.get("OCSP_TLS_SHOULD_SUCCEED") == "true"

# Enable logs in this format:
# 2020-06-08 23:49:35,982 DEBUG ocsp_support Peer did not staple an OCSP response
FORMAT = "%(asctime)s %(levelname)s %(module)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG)

if sys.platform == "win32":
    # The non-stapled OCSP endpoint check is slow on Windows.
    TIMEOUT_MS = 5000
else:
    TIMEOUT_MS = 500


def _connect(options):
    uri = ("mongodb://localhost:27017/?serverSelectionTimeoutMS=%s&tlsCAFile=%s&%s") % (
        TIMEOUT_MS,
        CA_FILE,
        options,
    )
    print(uri)
    client = pymongo.MongoClient(uri)
    client.admin.command("ping")


class TestOCSP(unittest.TestCase):
    def test_tls_insecure(self):
        # Should always succeed
        options = "tls=true&tlsInsecure=true"
        _connect(options)

    def test_allow_invalid_certificates(self):
        # Should always succeed
        options = "tls=true&tlsAllowInvalidCertificates=true"
        _connect(options)

    def test_tls(self):
        options = "tls=true"
        if not OCSP_TLS_SHOULD_SUCCEED:
            self.assertRaisesRegex(
                ServerSelectionTimeoutError, "invalid status response", _connect, options
            )
        else:
            _connect(options)


if __name__ == "__main__":
    unittest.main()
