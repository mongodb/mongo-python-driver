# Copyright 2016 MongoDB, Inc.
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

"""Test maxStalenessSeconds support."""
from __future__ import annotations

import os
import sys
import time
import warnings

from pymongo.operations import _Op

sys.path[0:0] = [""]

from test import PyMongoTestCase, client_context, unittest
from test.utils_selection_tests import create_selection_tests

from pymongo import MongoClient
from pymongo.errors import ConfigurationError
from pymongo.server_selectors import writable_server_selector

# Location of JSON test specifications.
_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "max_staleness")


class TestAllScenarios(create_selection_tests(_TEST_PATH)):  # type: ignore
    pass


class TestMaxStaleness(PyMongoTestCase):
    def test_max_staleness(self):
        client = self.single_client()
        self.assertEqual(-1, client.read_preference.max_staleness)

        client = self.single_client("mongodb://a/?readPreference=secondary")
        self.assertEqual(-1, client.read_preference.max_staleness)

        # These tests are specified in max-staleness-tests.rst.
        with self.assertRaises(ConfigurationError):
            # Default read pref "primary" can't be used with max staleness.
            self.single_client("mongodb://a/?maxStalenessSeconds=120")

        with self.assertRaises(ConfigurationError):
            # Read pref "primary" can't be used with max staleness.
            self.single_client("mongodb://a/?readPreference=primary&maxStalenessSeconds=120")

        client = self.single_client("mongodb://host/?maxStalenessSeconds=-1")
        self.assertEqual(-1, client.read_preference.max_staleness)

        client = self.single_client("mongodb://host/?readPreference=primary&maxStalenessSeconds=-1")
        self.assertEqual(-1, client.read_preference.max_staleness)

        client = self.single_client(
            "mongodb://host/?readPreference=secondary&maxStalenessSeconds=120"
        )
        self.assertEqual(120, client.read_preference.max_staleness)

        client = self.single_client("mongodb://a/?readPreference=secondary&maxStalenessSeconds=1")
        self.assertEqual(1, client.read_preference.max_staleness)

        client = self.single_client("mongodb://a/?readPreference=secondary&maxStalenessSeconds=-1")
        self.assertEqual(-1, client.read_preference.max_staleness)

        client = self.single_client(maxStalenessSeconds=-1, readPreference="nearest")
        self.assertEqual(-1, client.read_preference.max_staleness)

        with self.assertRaises(TypeError):
            # Prohibit None.
            self.single_client(maxStalenessSeconds=None, readPreference="nearest")

    def test_max_staleness_float(self):
        with self.assertRaises(TypeError) as ctx:
            self.rs_or_single_client(maxStalenessSeconds=1.5, readPreference="nearest")

        self.assertIn("must be an integer", str(ctx.exception))

        with warnings.catch_warnings(record=True) as ctx:
            warnings.simplefilter("always")
            client = self.single_client(
                "mongodb://host/?maxStalenessSeconds=1.5&readPreference=nearest"
            )

            # Option was ignored.
            self.assertEqual(-1, client.read_preference.max_staleness)
            self.assertIn("must be an integer", str(ctx[0]))

    def test_max_staleness_zero(self):
        # Zero is too small.
        with self.assertRaises(ValueError) as ctx:
            self.rs_or_single_client(maxStalenessSeconds=0, readPreference="nearest")

        self.assertIn("must be a positive integer", str(ctx.exception))

        with warnings.catch_warnings(record=True) as ctx:
            warnings.simplefilter("always")
            client = self.single_client(
                "mongodb://host/?maxStalenessSeconds=0&readPreference=nearest"
            )

            # Option was ignored.
            self.assertEqual(-1, client.read_preference.max_staleness)
            self.assertIn("must be a positive integer", str(ctx[0]))

    @client_context.require_replica_set
    def test_last_write_date(self):
        # From max-staleness-tests.rst, "Parse lastWriteDate".
        client = self.rs_or_single_client(heartbeatFrequencyMS=500)
        client.pymongo_test.test.insert_one({})
        # Wait for the server description to be updated.
        time.sleep(1)
        server = client._topology.select_server(writable_server_selector, _Op.TEST)
        first = server.description.last_write_date
        self.assertTrue(first)
        # The first last_write_date may correspond to a internal server write,
        # sleep so that the next write does not occur within the same second.
        time.sleep(1)
        client.pymongo_test.test.insert_one({})
        # Wait for the server description to be updated.
        time.sleep(1)
        server = client._topology.select_server(writable_server_selector, _Op.TEST)
        second = server.description.last_write_date
        assert first is not None

        assert second is not None
        self.assertGreater(second, first)
        self.assertLess(second, first + 10)


if __name__ == "__main__":
    unittest.main()
