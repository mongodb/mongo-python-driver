# Copyright 2023-present MongoDB, Inc.
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

"""Test MONGODB-OIDC Authentication."""

import os
import sys
import unittest
from contextlib import contextmanager

sys.path[0:0] = [""]

from test.utils import EventListener

from bson import SON
from pymongo import MongoClient
from pymongo.auth import _AUTH_MAP
from pymongo.auth_oidc import _CACHE as _oidc_cache
from pymongo.auth_oidc import _authenticate_oidc
from pymongo.azure_helpers import _CACHE

# Force MONGODB-OIDC to be enabled.
_AUTH_MAP["MONGODB-OIDC"] = _authenticate_oidc  # type:ignore


class TestAuthOIDCAzure(unittest.TestCase):
    uri: str

    @classmethod
    def setUpClass(cls):
        cls.uri = os.environ["MONGODB_URI"]

    def setUp(self):
        _oidc_cache.clear()
        _CACHE.clear()

    @contextmanager
    def fail_point(self, command_args):
        cmd_on = SON([("configureFailPoint", "failCommand")])
        cmd_on.update(command_args)
        client = MongoClient(self.uri)
        client.admin.command(cmd_on)
        try:
            yield
        finally:
            client.admin.command("configureFailPoint", cmd_on["configureFailPoint"], mode="off")

    def test_connect(self):
        client = MongoClient(self.uri)
        client.test.test.find_one()
        client.close()

    def test_connect_allowed_hosts_ignored(self):
        client = MongoClient(self.uri)
        client.test.test.find_one()
        client.close()

    def test_main_cache_is_not_used(self):
        # Create a new client using the AZURE device workflow.
        # Ensure that a ``find`` operation does not add credentials to the cache.
        client = MongoClient(self.uri)
        client.test.test.find_one()
        client.close()

        # Ensure that the cache has been cleared.
        authenticator = list(_oidc_cache.values())[0]
        self.assertIsNone(authenticator.idp_info)

    def test_azure_cache_is_used(self):
        # Create a new client using the AZURE device workflow.
        # Ensure that a ``find`` operation does not add credentials to the cache.
        client = MongoClient(self.uri)
        client.test.test.find_one()
        client.close()

        assert len(_CACHE) == 1

    def test_reauthenticate_succeeds(self):
        listener = EventListener()

        client = MongoClient(self.uri, event_listeners=[listener])

        # Perform a find operation.
        client.test.test.find_one()

        # Assert that the refresh callback has not been called.
        # self.assertEqual(self.refresh_called, 0)

        listener.reset()

        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"failCommands": ["find"], "errorCode": 391},
            }
        ):
            # Perform a find operation.
            client.test.test.find_one()

        started_events = [
            i.command_name for i in listener.started_events if not i.command_name.startswith("sasl")
        ]
        succeeded_events = [
            i.command_name
            for i in listener.succeeded_events
            if not i.command_name.startswith("sasl")
        ]
        failed_events = [
            i.command_name for i in listener.failed_events if not i.command_name.startswith("sasl")
        ]

        self.assertEqual(
            started_events,
            [
                "find",
                "find",
            ],
        )
        self.assertEqual(succeeded_events, ["find"])
        self.assertEqual(failed_events, ["find"])

        # Assert that the refresh callback has been called.
        # self.assertEqual(self.refresh_called, 1)
        client.close()


if __name__ == "__main__":
    unittest.main()
