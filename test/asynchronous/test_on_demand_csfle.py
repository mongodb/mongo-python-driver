# Copyright 2022-present MongoDB, Inc.
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

"""Test client side encryption with on demand credentials."""
from __future__ import annotations

import os
import sys
import unittest

import pytest

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context

from bson.codec_options import CodecOptions
from pymongo.asynchronous.encryption import (
    _HAVE_PYMONGOCRYPT,
    AsyncClientEncryption,
    EncryptionError,
)

_IS_SYNC = False

pytestmark = pytest.mark.kms


class TestonDemandGCPCredentials(AsyncIntegrationTest):
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    @async_client_context.require_version_min(4, 2, -1)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.master_key = {
            "projectId": "devprod-drivers",
            "location": "global",
            "keyRing": "key-ring-csfle",
            "keyName": "key-name-csfle",
        }

    @unittest.skipIf(not os.getenv("TEST_FLE_GCP_AUTO"), "Not testing FLE GCP auto")
    async def test_01_failure(self):
        if os.environ["SUCCESS"].lower() == "true":
            self.skipTest("Expecting success")
        self.client_encryption = AsyncClientEncryption(
            kms_providers={"gcp": {}},
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=async_client_context.client,
            codec_options=CodecOptions(),
        )
        with self.assertRaises(EncryptionError):
            await self.client_encryption.create_data_key("gcp", self.master_key)

    @unittest.skipIf(not os.getenv("TEST_FLE_GCP_AUTO"), "Not testing FLE GCP auto")
    async def test_02_success(self):
        if os.environ["SUCCESS"].lower() == "false":
            self.skipTest("Expecting failure")
        self.client_encryption = AsyncClientEncryption(
            kms_providers={"gcp": {}},
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=async_client_context.client,
            codec_options=CodecOptions(),
        )
        await self.client_encryption.create_data_key("gcp", self.master_key)


class TestonDemandAzureCredentials(AsyncIntegrationTest):
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    @async_client_context.require_version_min(4, 2, -1)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.master_key = {
            "keyVaultEndpoint": os.environ["KEY_VAULT_ENDPOINT"],
            "keyName": os.environ["KEY_NAME"],
        }

    @unittest.skipIf(not os.getenv("TEST_FLE_AZURE_AUTO"), "Not testing FLE Azure auto")
    async def test_01_failure(self):
        if os.environ["SUCCESS"].lower() == "true":
            self.skipTest("Expecting success")
        self.client_encryption = AsyncClientEncryption(
            kms_providers={"azure": {}},
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=async_client_context.client,
            codec_options=CodecOptions(),
        )
        with self.assertRaises(EncryptionError):
            await self.client_encryption.create_data_key("azure", self.master_key)

    @unittest.skipIf(not os.getenv("TEST_FLE_AZURE_AUTO"), "Not testing FLE Azure auto")
    async def test_02_success(self):
        if os.environ["SUCCESS"].lower() == "false":
            self.skipTest("Expecting failure")
        self.client_encryption = AsyncClientEncryption(
            kms_providers={"azure": {}},
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=async_client_context.client,
            codec_options=CodecOptions(),
        )
        await self.client_encryption.create_data_key("azure", self.master_key)


if __name__ == "__main__":
    unittest.main(verbosity=2)
