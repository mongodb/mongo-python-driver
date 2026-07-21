# Copyright 2026-present MongoDB, Inc.
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

"""Test connectivity and authentication through an Atlas Secure Frontend Processor (SFP/Monguard)."""

from __future__ import annotations

import os
import sys
import unittest
from typing import Any

import pytest

sys.path[0:0] = [""]

from bson import ObjectId
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.server_api import ServerApi
from test.asynchronous import AsyncPyMongoTestCase

_IS_SYNC = False

pytestmark = pytest.mark.sfp

# Each authenticated test must run under each of these variations:
# no additional configuration, a compressor enabled, and Server API v1.
VARIATIONS: dict[str, dict[str, Any]] = {
    "baseline": {},
    "compressor": {"compressors": "zlib"},
    "server_api": {"server_api": ServerApi("1")},
}


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise Exception(f"Must set {name} env variable to test.")
    return value


class TestAtlasSFP(AsyncPyMongoTestCase):
    async def assert_ping(self, client: AsyncMongoClient) -> None:
        response = await client.admin.command("ping")
        self.assertEqual(response["ok"], 1)

    async def assert_connection_status(self, client: AsyncMongoClient, authenticated: bool) -> None:
        response = await client.admin.command("connectionStatus")
        self.assertEqual(response["ok"], 1)
        users = response["authInfo"]["authenticatedUsers"]
        if authenticated:
            self.assertGreaterEqual(len(users), 1)
        else:
            self.assertEqual(users, [])

    async def assert_crud(self, client: AsyncMongoClient) -> None:
        # Use a unique collection name for each test run and drop it
        # afterward, regardless of test success or failure.
        collection = client.db[f"sfp_test_{ObjectId()}"]
        self.addAsyncCleanup(collection.drop)
        result = await collection.insert_one({"_id": 0})
        self.assertEqual(result.inserted_id, 0)
        document = await collection.find_one({"_id": 0})
        self.assertEqual(document, {"_id": 0})

    async def test_unauthenticated(self):
        client = self.simple_client(_require_env("SFP_ATLAS_URI"))
        await self.assert_ping(client)
        await self.assert_connection_status(client, authenticated=False)

    async def test_scram_sha_256(self):
        uri = _require_env("SFP_ATLAS_URI")
        username = _require_env("SFP_ATLAS_USER")
        password = _require_env("SFP_ATLAS_PASSWORD")
        for variation, kwargs in VARIATIONS.items():
            with self.subTest(variation=variation):
                client = self.simple_client(
                    uri,
                    username=username,
                    password=password,
                    authMechanism="SCRAM-SHA-256",
                    **kwargs,
                )
                await self.assert_ping(client)
                await self.assert_connection_status(client, authenticated=True)
                await self.assert_crud(client)

    async def test_x509(self):
        uri = _require_env("SFP_ATLAS_X509_URI")
        cert = _require_env("SFP_ATLAS_X509_CERT")
        for variation, kwargs in VARIATIONS.items():
            with self.subTest(variation=variation):
                client = self.simple_client(uri, tlsCertificateKeyFile=cert, **kwargs)
                await self.assert_ping(client)
                await self.assert_connection_status(client, authenticated=True)
                await self.assert_crud(client)


if __name__ == "__main__":
    unittest.main()
