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

"""Test the pymongo ocsp_support module."""

import random
import sys
from collections import namedtuple
from datetime import datetime, timedelta
from os import urandom
from time import sleep
from typing import Any

sys.path[0:0] = [""]

from test import unittest

from pymongo.ocsp_cache import _OCSPCache


class TestOcspCache(unittest.TestCase):
    MockHashAlgorithm: Any
    MockOcspRequest: Any
    MockOcspResponse: Any

    @classmethod
    def setUpClass(cls):
        cls.MockHashAlgorithm = namedtuple("MockHashAlgorithm", ["name"])  # type: ignore
        cls.MockOcspRequest = namedtuple(  # type: ignore
            "MockOcspRequest",
            ["hash_algorithm", "issuer_name_hash", "issuer_key_hash", "serial_number"],
        )
        cls.MockOcspResponse = namedtuple(  # type: ignore
            "MockOcspResponse", ["this_update", "next_update"]
        )

    def setUp(self):
        self.cache = _OCSPCache()

    def _create_mock_request(self):
        hash_algorithm = self.MockHashAlgorithm(random.choice(["sha1", "md5", "sha256"]))
        issuer_name_hash = urandom(8)
        issuer_key_hash = urandom(8)
        serial_number = random.randint(0, 10**10)
        return self.MockOcspRequest(
            hash_algorithm=hash_algorithm,
            issuer_name_hash=issuer_name_hash,
            issuer_key_hash=issuer_key_hash,
            serial_number=serial_number,
        )

    def _create_mock_response(self, this_update_delta_seconds, next_update_delta_seconds):
        now = datetime.utcnow()
        this_update = now + timedelta(seconds=this_update_delta_seconds)
        if next_update_delta_seconds is not None:
            next_update = now + timedelta(seconds=next_update_delta_seconds)
        else:
            next_update = None
        return self.MockOcspResponse(this_update=this_update, next_update=next_update)

    def _add_mock_cache_entry(self, mock_request, mock_response):
        key = self.cache._get_cache_key(mock_request)
        self.cache._data[key] = mock_response

    def test_simple(self):
        # Start with 1 valid entry in the cache.
        request = self._create_mock_request()
        response = self._create_mock_response(-10, +3600)
        self._add_mock_cache_entry(request, response)

        # Ensure entry can be retrieved.
        self.assertEqual(self.cache[request], response)

        # Valid entries with an earlier next_update have no effect.
        response_1 = self._create_mock_response(-20, +1800)
        self.cache[request] = response_1
        self.assertEqual(self.cache[request], response)

        # Invalid entries with a later this_update have no effect.
        response_2 = self._create_mock_response(+20, +1800)
        self.cache[request] = response_2
        self.assertEqual(self.cache[request], response)

        # Invalid entries with passed next_update have no effect.
        response_3 = self._create_mock_response(-10, -5)
        self.cache[request] = response_3
        self.assertEqual(self.cache[request], response)

        # Valid entries with a later next_update update the cache.
        response_new = self._create_mock_response(-5, +7200)
        self.cache[request] = response_new
        self.assertEqual(self.cache[request], response_new)

        # Entries with an unset next_update purge the cache.
        response_notset = self._create_mock_response(-5, None)
        self.cache[request] = response_notset
        with self.assertRaises(KeyError):
            _ = self.cache[request]

    def test_invalidate(self):
        # Start with 1 valid entry in the cache.
        request = self._create_mock_request()
        response = self._create_mock_response(-10, +0.25)
        self._add_mock_cache_entry(request, response)

        # Ensure entry can be retrieved.
        self.assertEqual(self.cache[request], response)

        # Wait for entry to become invalid and ensure KeyError is raised.
        sleep(0.5)
        with self.assertRaises(KeyError):
            _ = self.cache[request]

    def test_non_existent(self):
        # Start with 1 valid entry in the cache.
        request = self._create_mock_request()
        response = self._create_mock_response(-10, +10)
        self._add_mock_cache_entry(request, response)

        # Attempt to retrieve non-existent entry must raise KeyError.
        with self.assertRaises(KeyError):
            _ = self.cache[self._create_mock_request()]


if __name__ == "__main__":
    unittest.main()
