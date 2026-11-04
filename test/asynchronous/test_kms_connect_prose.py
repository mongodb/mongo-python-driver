"""Integration tests for the KMS connect callback and HTTP proxy support.

The unit tests live in ``test/test_kms_connect.py`` (a file that synchro
does not process). This module adds the integration tests, which run real
KMS traffic through a local proxy and are executed against both APIs via
the generated synchronous mirror.
"""

from __future__ import annotations

import asyncio
import http.client
import ssl
import unittest
from typing import Any

import pytest

from bson.binary import Binary
from pymongo.encryption_options import AsyncHTTPProxyKMSConnect, AutoEncryptionOpts
from pymongo.errors import EncryptionError
from test.asynchronous.test_encryption import OPTS, AsyncEncryptionIntegrationTest
from test.helpers_shared import AWS_CREDS, CA_PEM

_IS_SYNC = False

pytestmark = pytest.mark.encryption

KMS_PROXY_HOST = "127.0.0.1"
KMS_PROXY_PORT = 9004
KMS_TLS_PROXY_PORT = 9005

AWS_MASTER_KEY = {
    "region": "us-east-1",
    "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
}


class TestKmsConnectCallbackProse(AsyncEncryptionIntegrationTest):
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.callback_calls: list[Any] = []

    async def plain_callback(self, context):
        self.callback_calls.append(context)
        return await AsyncHTTPProxyKMSConnect(f"http://{KMS_PROXY_HOST}:{KMS_PROXY_PORT}")(context)

    def _proxy_tls_context(self):
        ctx = ssl.create_default_context(cafile=CA_PEM)
        ctx.check_hostname = False
        # PYTHON-5040 tracks re-enabling verification once the test CA cert
        # is fixed. The evergreen-tools CA lacks an Authority Key Identifier
        # that newer OpenSSL requires, so verification fails on Windows 3.14.
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    async def tls_callback(self, context):
        self.callback_calls.append(context)
        callback = AsyncHTTPProxyKMSConnect(
            f"https://{KMS_PROXY_HOST}:{KMS_TLS_PROXY_PORT}", self._proxy_tls_context()
        )
        return await callback(context)

    async def proxy_request(self, method, path, tls=False):
        """Call the proxy's control endpoints and return the body."""
        if _IS_SYNC:
            return self._proxy_request(method, path, tls)
        return await asyncio.get_running_loop().run_in_executor(
            None, self._proxy_request, method, path, tls
        )

    def _proxy_request(self, method, path, tls=False):
        if tls:
            conn = http.client.HTTPSConnection(
                f"{KMS_PROXY_HOST}:{KMS_TLS_PROXY_PORT}", context=self._proxy_tls_context()
            )
        else:
            conn = http.client.HTTPConnection(f"{KMS_PROXY_HOST}:{KMS_PROXY_PORT}")
        try:
            conn.request(method, path)
            return conn.getresponse().read().decode()
        finally:
            conn.close()

    async def connect_count(self, tls=False):
        body = await self.proxy_request("GET", "/metrics", tls=tls)
        # One "key value" per line. The server also emits connect_target.
        for line in body.splitlines():
            key, _, value = line.partition(" ")
            if key == "connect_count":
                return int(value)
        raise AssertionError(f"no connect_count in metrics body: {body!r}")

    async def test_01_plain_http_proxy(self):
        await self.proxy_request("POST", "/reset")
        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=self.plain_callback,
        )
        await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        self.assertGreaterEqual(await self.connect_count(), 1)

    async def test_02_https_proxy(self):
        await self.proxy_request("POST", "/reset", tls=True)
        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=self.tls_callback,
        )
        await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        self.assertGreaterEqual(await self.connect_count(tls=True), 1)

    async def test_03_auto_encryption_through_proxy(self):
        await self.client.keyvault.datakeys.drop()
        await self.client.db.coll.drop()

        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=self.plain_callback,
        )
        data_key_id = await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        schema = {
            "bsonType": "object",
            "properties": {
                "encrypted_string": {
                    "encrypt": {
                        "keyId": [data_key_id],
                        "bsonType": "string",
                        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    }
                }
            },
        }

        await self.proxy_request("POST", "/reset")
        opts = AutoEncryptionOpts(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            schema_map={"db.coll": schema},
            kms_connect_callback=self.plain_callback,
        )
        client_encrypted = await self.async_rs_or_single_client(auto_encryption_opts=opts)

        await client_encrypted.db.coll.insert_one({"_id": 1, "encrypted_string": "hello"})
        decrypted = await client_encrypted.db.coll.find_one({"_id": 1})
        self.assertEqual(decrypted["encrypted_string"], "hello")

        raw = await self.client.db.coll.find_one({"_id": 1})
        self.assertIsInstance(raw["encrypted_string"], Binary)

        # The decrypt reuses the cached key, so exactly one KMS request follows
        # the reset.
        self.assertEqual(await self.connect_count(), 1)

    async def test_04_callback_error(self):
        async def failing_callback(context):
            raise OSError("proxy is on fire")

        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=failing_callback,
        )
        with self.assertRaisesRegex(EncryptionError, "proxy is on fire"):
            await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)

    @unittest.skip(
        "PYTHON-6037 ClientEncryption does not support timeoutMS, so the "
        "callback always receives the default KMS connect timeout"
    )
    async def test_05_callback_receives_timeout(self):
        key_vault_client = await self.async_rs_or_single_client(timeoutMS=1000)
        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            key_vault_client,
            OPTS,
            kms_connect_callback=self.plain_callback,
        )
        await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)

        self.assertTrue(self.callback_calls, "callback was never invoked")
        for context in self.callback_calls:
            # Checks only the spec's non-zero requirement, which cannot fail.
            self.assertIsNotNone(context.timeout)
            self.assertGreater(context.timeout, 0)

    async def test_06_retry_after_network_error(self):
        state = {"calls": 0}

        async def flaky_callback(context):
            state["calls"] += 1
            if state["calls"] == 1:
                raise OSError("first attempt fails")
            return await AsyncHTTPProxyKMSConnect(f"http://{KMS_PROXY_HOST}:{KMS_PROXY_PORT}")(
                context
            )

        encryption = self.create_client_encryption(
            {"aws": AWS_CREDS},
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_connect_callback=flaky_callback,
        )
        await encryption.create_data_key("aws", master_key=AWS_MASTER_KEY)
        self.assertGreaterEqual(state["calls"], 2)
