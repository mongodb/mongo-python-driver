# Copyright 2019-present MongoDB, Inc.
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

"""Test client side encryption spec."""
from __future__ import annotations

import base64
import copy
import http.client
import json
import os
import pathlib
import re
import socket
import socketserver
import ssl
import sys
import textwrap
import traceback
import uuid
import warnings
from test.asynchronous import AsyncIntegrationTest, AsyncPyMongoTestCase, async_client_context
from test.asynchronous.test_bulk import AsyncBulkTestBase
from test.asynchronous.utils import flaky
from threading import Thread
from typing import Any, Dict, Mapping, Optional

import pytest

from pymongo.asynchronous.collection import AsyncCollection
from pymongo.daemon import _spawn_daemon
from pymongo.uri_parser_shared import _parse_kms_tls_options

try:
    from pymongo.pyopenssl_context import IS_PYOPENSSL
except ImportError:
    IS_PYOPENSSL = False

sys.path[0:0] = [""]

from test import (
    unittest,
)
from test.asynchronous.test_bulk import AsyncBulkTestBase
from test.asynchronous.unified_format import generate_test_classes, get_test_path
from test.helpers_shared import (
    ALL_KMS_PROVIDERS,
    AWS_CREDS,
    AWS_TEMP_CREDS,
    AZURE_CREDS,
    CA_PEM,
    CLIENT_PEM,
    DEFAULT_KMS_TLS,
    GCP_CREDS,
    KMIP_CREDS,
    LOCAL_MASTER_KEY,
)
from test.utils_shared import (
    AllowListEventListener,
    OvertCommandListener,
    TopologyEventListener,
    async_wait_until,
    camel_to_snake_args,
    is_greenthread_patched,
)

from bson import BSON, DatetimeMS, Decimal128, encode, json_util
from bson.binary import UUID_SUBTYPE, Binary, UuidRepresentation
from bson.codec_options import CodecOptions
from bson.errors import BSONError
from bson.json_util import JSONOptions
from bson.son import SON
from pymongo import ReadPreference
from pymongo.asynchronous import encryption
from pymongo.asynchronous.encryption import Algorithm, AsyncClientEncryption, QueryType
from pymongo.asynchronous.helpers import anext
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.cursor_shared import CursorType
from pymongo.encryption_options import _HAVE_PYMONGOCRYPT, AutoEncryptionOpts, RangeOpts, TextOpts
from pymongo.errors import (
    AutoReconnect,
    BulkWriteError,
    ConfigurationError,
    DuplicateKeyError,
    EncryptedCollectionError,
    EncryptionError,
    InvalidOperation,
    OperationFailure,
    PyMongoError,
    ServerSelectionTimeoutError,
    WriteError,
)
from pymongo.operations import InsertOne, ReplaceOne, UpdateOne
from pymongo.write_concern import WriteConcern

_IS_SYNC = False

pytestmark = pytest.mark.encryption

KMS_PROVIDERS = {"local": {"key": b"\x00" * 96}}


def get_client_opts(client):
    return client.options


class TestAutoEncryptionOpts(AsyncPyMongoTestCase):
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    @unittest.skipUnless(os.environ.get("TEST_CRYPT_SHARED"), "crypt_shared lib is not installed")
    async def test_crypt_shared(self):
        # Test that we can pick up crypt_shared lib automatically
        self.simple_client(
            auto_encryption_opts=AutoEncryptionOpts(
                KMS_PROVIDERS, "keyvault.datakeys", crypt_shared_lib_required=True
            ),
            connect=False,
        )

    @unittest.skipIf(_HAVE_PYMONGOCRYPT, "pymongocrypt is installed")
    def test_init_requires_pymongocrypt(self):
        with self.assertRaises(ConfigurationError):
            AutoEncryptionOpts({}, "keyvault.datakeys")

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    def test_init(self):
        opts = AutoEncryptionOpts({}, "keyvault.datakeys")
        self.assertEqual(opts._kms_providers, {})
        self.assertEqual(opts._key_vault_namespace, "keyvault.datakeys")
        self.assertEqual(opts._key_vault_client, None)
        self.assertEqual(opts._schema_map, None)
        self.assertEqual(opts._bypass_auto_encryption, False)
        self.assertEqual(opts._mongocryptd_uri, "mongodb://localhost:27020")
        self.assertEqual(opts._mongocryptd_bypass_spawn, False)
        self.assertEqual(opts._mongocryptd_spawn_path, "mongocryptd")
        self.assertEqual(opts._mongocryptd_spawn_args, ["--idleShutdownTimeoutSecs=60"])
        self.assertEqual(opts._kms_tls_options, None)

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    def test_init_spawn_args(self):
        # User can override idleShutdownTimeoutSecs
        opts = AutoEncryptionOpts(
            {}, "keyvault.datakeys", mongocryptd_spawn_args=["--idleShutdownTimeoutSecs=88"]
        )
        self.assertEqual(opts._mongocryptd_spawn_args, ["--idleShutdownTimeoutSecs=88"])

        # idleShutdownTimeoutSecs is added by default
        opts = AutoEncryptionOpts({}, "keyvault.datakeys", mongocryptd_spawn_args=[])
        self.assertEqual(opts._mongocryptd_spawn_args, ["--idleShutdownTimeoutSecs=60"])

        # Also added when other options are given
        opts = AutoEncryptionOpts(
            {}, "keyvault.datakeys", mongocryptd_spawn_args=["--quiet", "--port=27020"]
        )
        self.assertEqual(
            opts._mongocryptd_spawn_args,
            ["--quiet", "--port=27020", "--idleShutdownTimeoutSecs=60"],
        )

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    async def test_init_kms_tls_options(self):
        # Error cases:
        opts = AutoEncryptionOpts({}, "k.d", kms_tls_options={"kmip": 1})
        with self.assertRaisesRegex(TypeError, r'kms_tls_options\["kmip"\] must be a dict'):
            AsyncMongoClient(auto_encryption_opts=opts)

        tls_opts: Any
        for tls_opts in [
            {"kmip": {"tls": True, "tlsInsecure": True}},
            {"kmip": {"tls": True, "tlsAllowInvalidCertificates": True}},
            {"kmip": {"tls": True, "tlsAllowInvalidHostnames": True}},
        ]:
            opts = AutoEncryptionOpts({}, "k.d", kms_tls_options=tls_opts)
            with self.assertRaisesRegex(ConfigurationError, "Insecure TLS options prohibited"):
                AsyncMongoClient(auto_encryption_opts=opts)
        opts = AutoEncryptionOpts(
            {}, "k.d", kms_tls_options={"kmip": {"tlsCAFile": "does-not-exist"}}
        )
        with self.assertRaises(FileNotFoundError):
            AsyncMongoClient(auto_encryption_opts=opts)
        # Success cases:
        tls_opts: Any
        for tls_opts in [None, {}]:
            opts = AutoEncryptionOpts({}, "k.d", kms_tls_options=tls_opts)
            kms_tls_contexts = _parse_kms_tls_options(opts._kms_tls_options, _IS_SYNC)
            self.assertEqual(kms_tls_contexts, {})
        opts = AutoEncryptionOpts({}, "k.d", kms_tls_options={"kmip": {"tls": True}, "aws": {}})
        _kms_ssl_contexts = _parse_kms_tls_options(opts._kms_tls_options, _IS_SYNC)
        ctx = _kms_ssl_contexts["kmip"]
        self.assertEqual(ctx.check_hostname, True)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        ctx = _kms_ssl_contexts["aws"]
        self.assertEqual(ctx.check_hostname, True)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        opts = AutoEncryptionOpts(
            {},
            "k.d",
            kms_tls_options=DEFAULT_KMS_TLS,
        )
        _kms_ssl_contexts = _parse_kms_tls_options(opts._kms_tls_options, _IS_SYNC)
        ctx = _kms_ssl_contexts["kmip"]
        self.assertEqual(ctx.check_hostname, True)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)


class TestClientOptions(AsyncPyMongoTestCase):
    async def test_default(self):
        client = self.simple_client(connect=False)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, None)

        client = self.simple_client(auto_encryption_opts=None, connect=False)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, None)

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    async def test_kwargs(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = self.simple_client(auto_encryption_opts=opts, connect=False)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, opts)


class AsyncEncryptionIntegrationTest(AsyncIntegrationTest):
    """Base class for encryption integration tests."""

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    @async_client_context.require_version_min(4, 2, -1)  # type:ignore[untyped-decorator]
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()

    def assertEncrypted(self, val):
        self.assertIsInstance(val, Binary)
        self.assertEqual(val.subtype, 6)

    def assertBinaryUUID(self, val):
        self.assertIsInstance(val, Binary)
        self.assertEqual(val.subtype, UUID_SUBTYPE)

    def create_client_encryption(
        self,
        kms_providers: Mapping[str, Any],
        key_vault_namespace: str,
        key_vault_client: AsyncMongoClient,
        codec_options: CodecOptions,
        kms_tls_options: Optional[Mapping[str, Any]] = None,
    ):
        client_encryption = AsyncClientEncryption(
            kms_providers, key_vault_namespace, key_vault_client, codec_options, kms_tls_options
        )
        self.addAsyncCleanup(client_encryption.close)
        return client_encryption

    @classmethod
    def unmanaged_create_client_encryption(
        cls,
        kms_providers: Mapping[str, Any],
        key_vault_namespace: str,
        key_vault_client: AsyncMongoClient,
        codec_options: CodecOptions,
        kms_tls_options: Optional[Mapping[str, Any]] = None,
    ):
        client_encryption = AsyncClientEncryption(
            kms_providers, key_vault_namespace, key_vault_client, codec_options, kms_tls_options
        )
        return client_encryption


# Location of JSON test files.
BASE = get_test_path("client-side-encryption")
SPEC_PATH = os.path.join(BASE, "spec")

OPTS = CodecOptions()

# Use SON to preserve the order of fields while parsing json. Use tz_aware
# =False to match how CodecOptions decodes dates.
JSON_OPTS = JSONOptions(document_class=SON, tz_aware=False)


def read(*paths):
    with open(os.path.join(BASE, *paths)) as fp:
        return fp.read()


def json_data(*paths):
    return json_util.loads(read(*paths), json_options=JSON_OPTS)


def bson_data(*paths):
    return encode(json_data(*paths), codec_options=OPTS)


class TestClientSimple(AsyncEncryptionIntegrationTest):
    async def _test_auto_encrypt(self, opts):
        client = await self.async_rs_or_single_client(auto_encryption_opts=opts)

        # Create the encrypted field's data key.
        key_vault = await create_key_vault(
            self.client.keyvault.datakeys, json_data("custom", "key-document-local.json")
        )
        self.addAsyncCleanup(key_vault.drop)

        # Collection.insert_one/insert_many auto encrypts.
        docs = [
            {"_id": 0, "ssn": "000"},
            {"_id": 1, "ssn": "111"},
            {"_id": 2, "ssn": "222"},
            {"_id": 3, "ssn": "333"},
            {"_id": 4, "ssn": "444"},
            {"_id": 5, "ssn": "555"},
        ]
        encrypted_coll = client.pymongo_test.test
        await encrypted_coll.insert_one(docs[0])
        await encrypted_coll.insert_many(docs[1:3])
        unack = encrypted_coll.with_options(write_concern=WriteConcern(w=0))
        await unack.insert_one(docs[3])
        await unack.insert_many(docs[4:], ordered=False)

        async def count_documents():
            return await self.db.test.count_documents({}) == len(docs)

        await async_wait_until(count_documents, "insert documents with w=0")

        # Database.command auto decrypts.
        res = await client.pymongo_test.command("find", "test", filter={"ssn": "000"})
        decrypted_docs = res["cursor"]["firstBatch"]
        self.assertEqual(decrypted_docs, [{"_id": 0, "ssn": "000"}])

        # Collection.find auto decrypts.
        decrypted_docs = await encrypted_coll.find().to_list()
        self.assertEqual(decrypted_docs, docs)

        # Collection.find auto decrypts getMores.
        decrypted_docs = await encrypted_coll.find(batch_size=1).to_list()
        self.assertEqual(decrypted_docs, docs)

        # Collection.aggregate auto decrypts.
        decrypted_docs = await (await encrypted_coll.aggregate([])).to_list()
        self.assertEqual(decrypted_docs, docs)

        # Collection.aggregate auto decrypts getMores.
        decrypted_docs = await (await encrypted_coll.aggregate([], batchSize=1)).to_list()
        self.assertEqual(decrypted_docs, docs)

        # Collection.distinct auto decrypts.
        decrypted_ssns = await encrypted_coll.distinct("ssn")
        self.assertEqual(set(decrypted_ssns), {d["ssn"] for d in docs})

        # Make sure the field is actually encrypted.
        async for encrypted_doc in self.db.test.find():
            self.assertIsInstance(encrypted_doc["_id"], int)
            self.assertEncrypted(encrypted_doc["ssn"])

        # Attempt to encrypt an unencodable object.
        with self.assertRaises(BSONError):
            await encrypted_coll.insert_one({"unencodeable": object()})

    async def test_auto_encrypt(self):
        # Configure the encrypted field via jsonSchema.
        json_schema = json_data("custom", "schema.json")
        await create_with_schema(self.db.test, json_schema)
        self.addAsyncCleanup(self.db.test.drop)

        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        await self._test_auto_encrypt(opts)

    async def test_auto_encrypt_local_schema_map(self):
        # Configure the encrypted field via the local schema_map option.
        schemas = {"pymongo_test.test": json_data("custom", "schema.json")}
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys", schema_map=schemas)

        await self._test_auto_encrypt(opts)

    async def test_use_after_close(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = await self.async_rs_or_single_client(auto_encryption_opts=opts)

        await client.admin.command("ping")
        await client.aclose()
        with self.assertRaisesRegex(InvalidOperation, "Cannot use AsyncMongoClient after close"):
            await client.admin.command("ping")

    @unittest.skipIf(
        not hasattr(os, "register_at_fork"),
        "register_at_fork not available in this version of Python",
    )
    @unittest.skipIf(
        is_greenthread_patched(),
        "gevent does not support POSIX-style forking.",
    )
    @async_client_context.require_sync
    async def test_fork(self):
        self.skipTest("Test is flaky, PYTHON-4738")
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = await self.async_rs_or_single_client(auto_encryption_opts=opts)

        async def target():
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                await client.admin.command("ping")

        with self.fork(target):
            await target()


class TestEncryptedBulkWrite(AsyncBulkTestBase, AsyncEncryptionIntegrationTest):
    async def test_upsert_uuid_standard_encrypt(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = await self.async_rs_or_single_client(auto_encryption_opts=opts)

        options = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
        encrypted_coll = client.pymongo_test.test
        coll = encrypted_coll.with_options(codec_options=options)
        uuids = [uuid.uuid4() for _ in range(3)]
        result = await coll.bulk_write(
            [
                UpdateOne({"_id": uuids[0]}, {"$set": {"a": 0}}, upsert=True),
                ReplaceOne({"a": 1}, {"_id": uuids[1]}, upsert=True),
                # This is just here to make the counts right in all cases.
                ReplaceOne({"_id": uuids[2]}, {"_id": uuids[2]}, upsert=True),
            ]
        )
        self.assertEqualResponse(
            {
                "nMatched": 0,
                "nModified": 0,
                "nUpserted": 3,
                "nInserted": 0,
                "nRemoved": 0,
                "upserted": [
                    {"index": 0, "_id": uuids[0]},
                    {"index": 1, "_id": uuids[1]},
                    {"index": 2, "_id": uuids[2]},
                ],
            },
            result.bulk_api_result,
        )


class TestClientMaxWireVersion(AsyncIntegrationTest):
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    async def asyncSetUp(self):
        await super().asyncSetUp()

    async def test_raise_unsupported_error(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = await self.async_rs_or_single_client(auto_encryption_opts=opts)
        msg = "find_raw_batches does not support auto encryption"
        with self.assertRaisesRegex(InvalidOperation, msg):
            await client.test.test.find_raw_batches({})

        msg = "aggregate_raw_batches does not support auto encryption"
        with self.assertRaisesRegex(InvalidOperation, msg):
            await client.test.test.aggregate_raw_batches([])

        if async_client_context.is_mongos:
            msg = "Exhaust cursors are not supported by mongos"
        else:
            msg = "exhaust cursors do not support auto encryption"
        with self.assertRaisesRegex(InvalidOperation, msg):
            await anext(client.test.test.find(cursor_type=CursorType.EXHAUST))


class TestExplicitSimple(AsyncEncryptionIntegrationTest):
    async def test_encrypt_decrypt(self):
        client_encryption = self.create_client_encryption(
            KMS_PROVIDERS, "keyvault.datakeys", async_client_context.client, OPTS
        )
        # Use standard UUID representation.
        key_vault = async_client_context.client.keyvault.get_collection(
            "datakeys", codec_options=OPTS
        )
        self.addAsyncCleanup(key_vault.drop)

        # Create the encrypted field's data key.
        key_id = await client_encryption.create_data_key("local", key_alt_names=["name"])
        self.assertBinaryUUID(key_id)
        self.assertTrue(await key_vault.find_one({"_id": key_id}))

        # Create an unused data key to make sure filtering works.
        unused_key_id = await client_encryption.create_data_key("local", key_alt_names=["unused"])
        self.assertBinaryUUID(unused_key_id)
        self.assertTrue(await key_vault.find_one({"_id": unused_key_id}))

        doc = {"_id": 0, "ssn": "000"}
        encrypted_ssn = await client_encryption.encrypt(
            doc["ssn"], Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=key_id
        )

        # Ensure encryption via key_alt_name for the same key produces the
        # same output.
        encrypted_ssn2 = await client_encryption.encrypt(
            doc["ssn"], Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_alt_name="name"
        )
        self.assertEqual(encrypted_ssn, encrypted_ssn2)

        # Test encryption via UUID
        encrypted_ssn3 = await client_encryption.encrypt(
            doc["ssn"],
            Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_id=key_id.as_uuid(),
        )
        self.assertEqual(encrypted_ssn, encrypted_ssn3)

        # Test decryption.
        decrypted_ssn = await client_encryption.decrypt(encrypted_ssn)
        self.assertEqual(decrypted_ssn, doc["ssn"])

    async def test_validation(self):
        client_encryption = self.create_client_encryption(
            KMS_PROVIDERS, "keyvault.datakeys", async_client_context.client, OPTS
        )

        msg = "value to decrypt must be a bson.binary.Binary with subtype 6"
        with self.assertRaisesRegex(TypeError, msg):
            await client_encryption.decrypt("str")  # type: ignore[arg-type]
        with self.assertRaisesRegex(TypeError, msg):
            await client_encryption.decrypt(Binary(b"123"))

        msg = "key_id must be a bson.binary.Binary with subtype 4"
        algo = Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
        with self.assertRaisesRegex(TypeError, msg):
            await client_encryption.encrypt("str", algo, key_id=Binary(b"123"))

    async def test_bson_errors(self):
        client_encryption = self.create_client_encryption(
            KMS_PROVIDERS, "keyvault.datakeys", async_client_context.client, OPTS
        )

        # Attempt to encrypt an unencodable object.
        unencodable_value = object()
        with self.assertRaises(BSONError):
            await client_encryption.encrypt(
                unencodable_value,
                Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
                key_id=Binary.from_uuid(uuid.uuid4()),
            )

    async def test_codec_options(self):
        with self.assertRaisesRegex(TypeError, "codec_options must be"):
            self.create_client_encryption(
                KMS_PROVIDERS,
                "keyvault.datakeys",
                async_client_context.client,
                None,  # type: ignore[arg-type]
            )

        opts = CodecOptions(uuid_representation=UuidRepresentation.JAVA_LEGACY)
        client_encryption_legacy = self.create_client_encryption(
            KMS_PROVIDERS, "keyvault.datakeys", async_client_context.client, opts
        )

        # Create the encrypted field's data key.
        key_id = await client_encryption_legacy.create_data_key("local")

        # Encrypt a UUID with JAVA_LEGACY codec options.
        value = uuid.uuid4()
        encrypted_legacy = await client_encryption_legacy.encrypt(
            value, Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=key_id
        )
        decrypted_value_legacy = await client_encryption_legacy.decrypt(encrypted_legacy)
        self.assertEqual(decrypted_value_legacy, value)

        # Encrypt the same UUID with STANDARD codec options.
        opts = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
        client_encryption = self.create_client_encryption(
            KMS_PROVIDERS, "keyvault.datakeys", async_client_context.client, opts
        )
        encrypted_standard = await client_encryption.encrypt(
            value, Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=key_id
        )
        decrypted_standard = await client_encryption.decrypt(encrypted_standard)
        self.assertEqual(decrypted_standard, value)

        # Test that codec_options is applied during encryption.
        self.assertNotEqual(encrypted_standard, encrypted_legacy)
        # Test that codec_options is applied during decryption.
        self.assertEqual(
            await client_encryption_legacy.decrypt(encrypted_standard), Binary.from_uuid(value)
        )
        self.assertNotEqual(await client_encryption.decrypt(encrypted_legacy), value)

    async def test_close(self):
        client_encryption = self.create_client_encryption(
            KMS_PROVIDERS, "keyvault.datakeys", async_client_context.client, OPTS
        )
        await client_encryption.close()
        # Close can be called multiple times.
        await client_encryption.close()
        algo = Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
        msg = "Cannot use closed AsyncClientEncryption"
        with self.assertRaisesRegex(InvalidOperation, msg):
            await client_encryption.create_data_key("local")
        with self.assertRaisesRegex(InvalidOperation, msg):
            await client_encryption.encrypt("val", algo, key_alt_name="name")
        with self.assertRaisesRegex(InvalidOperation, msg):
            await client_encryption.decrypt(Binary(b"", 6))

    async def test_with_statement(self):
        async with self.create_client_encryption(
            KMS_PROVIDERS, "keyvault.datakeys", async_client_context.client, OPTS
        ) as client_encryption:
            pass
        with self.assertRaisesRegex(InvalidOperation, "Cannot use closed AsyncClientEncryption"):
            await client_encryption.create_data_key("local")


# Spec tests
AWS_TEMP_NO_SESSION_CREDS = {
    "accessKeyId": os.environ.get("CSFLE_AWS_TEMP_ACCESS_KEY_ID", ""),
    "secretAccessKey": os.environ.get("CSFLE_AWS_TEMP_SECRET_ACCESS_KEY", ""),
}


if _HAVE_PYMONGOCRYPT:
    globals().update(
        generate_test_classes(
            os.path.join(SPEC_PATH, "unified"), module=__name__, expected_failures=["mapReduce .*"]
        )
    )

# Prose Tests
LOCAL_KEY_ID = Binary(base64.b64decode(b"LOCALAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
AWS_KEY_ID = Binary(base64.b64decode(b"AWSAAAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
AZURE_KEY_ID = Binary(base64.b64decode(b"AZUREAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
GCP_KEY_ID = Binary(base64.b64decode(b"GCPAAAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
KMIP_KEY_ID = Binary(base64.b64decode(b"KMIPAAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)


async def create_with_schema(coll, json_schema):
    """Create and return a Collection with a jsonSchema."""
    await coll.with_options(write_concern=WriteConcern(w="majority")).drop()
    return await coll.database.create_collection(
        coll.name, validator={"$jsonSchema": json_schema}, codec_options=OPTS
    )


async def create_key_vault(vault, *data_keys):
    """Create the key vault collection with optional data keys."""
    vault = vault.with_options(write_concern=WriteConcern(w="majority"), codec_options=OPTS)
    await vault.drop()
    if data_keys:
        await vault.insert_many(data_keys)
    await vault.create_index(
        "keyAltNames",
        unique=True,
        partialFilterExpression={"keyAltNames": {"$exists": True}},
    )
    return vault


class TestDataKeyDoubleEncryption(AsyncEncryptionIntegrationTest):
    client_encrypted: AsyncMongoClient
    client_encryption: AsyncClientEncryption
    listener: OvertCommandListener
    vault: Any

    KMS_PROVIDERS = ALL_KMS_PROVIDERS

    MASTER_KEYS = {
        "aws": {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
        },
        "azure": {
            "keyVaultEndpoint": "key-vault-csfle.vault.azure.net",
            "keyName": "key-name-csfle",
        },
        "gcp": {
            "projectId": "devprod-drivers",
            "location": "global",
            "keyRing": "key-ring-csfle",
            "keyName": "key-name-csfle",
        },
        "kmip": {},
        "local": None,
    }

    @unittest.skipUnless(
        any([all(AWS_CREDS.values()), all(AZURE_CREDS.values()), all(GCP_CREDS.values())]),
        "No environment credentials are set",
    )
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.listener = OvertCommandListener()
        self.client = await self.async_rs_or_single_client(event_listeners=[self.listener])
        await self.client.db.coll.drop()
        self.vault = await create_key_vault(self.client.keyvault.datakeys)

        # Configure the encrypted field via the local schema_map option.
        schemas = {
            "db.coll": {
                "bsonType": "object",
                "properties": {
                    "encrypted_placeholder": {
                        "encrypt": {
                            "keyId": "/placeholder",
                            "bsonType": "string",
                            "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        }
                    }
                },
            }
        }
        opts = AutoEncryptionOpts(
            self.KMS_PROVIDERS,
            "keyvault.datakeys",
            schema_map=schemas,
            kms_tls_options=DEFAULT_KMS_TLS,
        )
        self.client_encrypted = await self.async_rs_or_single_client(
            auto_encryption_opts=opts, uuidRepresentation="standard"
        )
        self.client_encryption = self.create_client_encryption(
            self.KMS_PROVIDERS,
            "keyvault.datakeys",
            self.client,
            OPTS,
            kms_tls_options=DEFAULT_KMS_TLS,
        )
        self.listener.reset()

    async def asyncTearDown(self) -> None:
        await self.vault.drop()

    async def run_test(self, provider_name):
        # Create data key.
        master_key: Any = self.MASTER_KEYS[provider_name]
        datakey_id = await self.client_encryption.create_data_key(
            provider_name, master_key=master_key, key_alt_names=[f"{provider_name}_altname"]
        )
        self.assertBinaryUUID(datakey_id)
        cmd = self.listener.started_events[-1]
        self.assertEqual("insert", cmd.command_name)
        self.assertEqual({"w": "majority"}, cmd.command.get("writeConcern"))
        docs = await self.vault.find({"_id": datakey_id}).to_list()
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["masterKey"]["provider"], provider_name)

        # Encrypt by key_id.
        encrypted = await self.client_encryption.encrypt(
            f"hello {provider_name}",
            Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_id=datakey_id,
        )
        self.assertEncrypted(encrypted)
        await self.client_encrypted.db.coll.insert_one({"_id": provider_name, "value": encrypted})
        doc_decrypted = await self.client_encrypted.db.coll.find_one({"_id": provider_name})
        self.assertEqual(doc_decrypted["value"], f"hello {provider_name}")  # type: ignore

        # Encrypt by key_alt_name.
        encrypted_altname = await self.client_encryption.encrypt(
            f"hello {provider_name}",
            Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_alt_name=f"{provider_name}_altname",
        )
        self.assertEqual(encrypted_altname, encrypted)

        # Explicitly encrypting an auto encrypted field.
        with self.assertRaisesRegex(EncryptionError, r"encrypt element of type"):
            await self.client_encrypted.db.coll.insert_one({"encrypted_placeholder": encrypted})

    async def test_data_key_local(self):
        await self.run_test("local")

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def test_data_key_aws(self):
        await self.run_test("aws")

    @unittest.skipUnless(any(AZURE_CREDS.values()), "Azure environment credentials are not set")
    async def test_data_key_azure(self):
        await self.run_test("azure")

    @unittest.skipUnless(any(GCP_CREDS.values()), "GCP environment credentials are not set")
    async def test_data_key_gcp(self):
        await self.run_test("gcp")

    async def test_data_key_kmip(self):
        await self.run_test("kmip")


class TestExternalKeyVault(AsyncEncryptionIntegrationTest):
    @staticmethod
    def kms_providers():
        return {"local": {"key": LOCAL_MASTER_KEY}}

    async def _test_external_key_vault(self, with_external_key_vault):
        await self.client.db.coll.drop()
        vault = await create_key_vault(
            self.client.keyvault.datakeys,
            json_data("corpus", "corpus-key-local.json"),
            json_data("corpus", "corpus-key-aws.json"),
        )
        self.addAsyncCleanup(vault.drop)

        # Configure the encrypted field via the local schema_map option.
        schemas = {"db.coll": json_data("external", "external-schema.json")}
        if with_external_key_vault:
            key_vault_client = await self.async_rs_or_single_client(
                username="fake-user", password="fake-pwd"
            )
        else:
            key_vault_client = async_client_context.client
        opts = AutoEncryptionOpts(
            self.kms_providers(),
            "keyvault.datakeys",
            schema_map=schemas,
            key_vault_client=key_vault_client,
        )

        client_encrypted = await self.async_rs_or_single_client(
            auto_encryption_opts=opts, uuidRepresentation="standard"
        )

        client_encryption = self.create_client_encryption(
            self.kms_providers(), "keyvault.datakeys", key_vault_client, OPTS
        )

        if with_external_key_vault:
            # Authentication error.
            with self.assertRaises(EncryptionError) as ctx:
                await client_encrypted.db.coll.insert_one({"encrypted": "test"})
            # AuthenticationFailed error.
            self.assertIsInstance(ctx.exception.cause, OperationFailure)
            self.assertEqual(ctx.exception.cause.code, 18)
        else:
            await client_encrypted.db.coll.insert_one({"encrypted": "test"})

        if with_external_key_vault:
            # Authentication error.
            with self.assertRaises(EncryptionError) as ctx:
                await client_encryption.encrypt(
                    "test",
                    Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
                    key_id=LOCAL_KEY_ID,
                )
            # AuthenticationFailed error.
            self.assertIsInstance(ctx.exception.cause, OperationFailure)
            self.assertEqual(ctx.exception.cause.code, 18)
        else:
            await client_encryption.encrypt(
                "test", Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=LOCAL_KEY_ID
            )

    async def test_external_key_vault_1(self):
        await self._test_external_key_vault(True)

    async def test_external_key_vault_2(self):
        await self._test_external_key_vault(False)


class TestViews(AsyncEncryptionIntegrationTest):
    @staticmethod
    def kms_providers():
        return {"local": {"key": LOCAL_MASTER_KEY}}

    async def test_views_are_prohibited(self):
        await self.client.db.view.drop()
        await self.client.db.create_collection("view", viewOn="coll")
        self.addAsyncCleanup(self.client.db.view.drop)

        opts = AutoEncryptionOpts(self.kms_providers(), "keyvault.datakeys")
        client_encrypted = await self.async_rs_or_single_client(
            auto_encryption_opts=opts, uuidRepresentation="standard"
        )

        with self.assertRaisesRegex(EncryptionError, "cannot auto encrypt a view"):
            await client_encrypted.db.view.insert_one({})


class TestCorpus(AsyncEncryptionIntegrationTest):
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def asyncSetUp(self):
        await super().asyncSetUp()

    @staticmethod
    def kms_providers():
        return ALL_KMS_PROVIDERS

    @staticmethod
    def fix_up_schema(json_schema):
        """Remove deprecated symbol/dbPointer types from json schema."""
        for key in list(json_schema["properties"]):
            if "_symbol_" in key or "_dbPointer_" in key:
                del json_schema["properties"][key]
        return json_schema

    @staticmethod
    def fix_up_curpus(corpus):
        """Disallow deprecated symbol/dbPointer types from corpus test."""
        for key in corpus:
            if "_symbol_" in key or "_dbPointer_" in key:
                corpus[key]["allowed"] = False
        return corpus

    @staticmethod
    def fix_up_curpus_encrypted(corpus_encrypted, corpus):
        """Fix the expected values for deprecated symbol/dbPointer types."""
        for key in corpus_encrypted:
            if "_symbol_" in key or "_dbPointer_" in key:
                corpus_encrypted[key] = copy.deepcopy(corpus[key])
        return corpus_encrypted

    async def _test_corpus(self, opts):
        # Drop and create the collection 'db.coll' with jsonSchema.
        coll = await create_with_schema(
            self.client.db.coll, self.fix_up_schema(json_data("corpus", "corpus-schema.json"))
        )
        self.addAsyncCleanup(coll.drop)

        vault = await create_key_vault(
            self.client.keyvault.datakeys,
            json_data("corpus", "corpus-key-local.json"),
            json_data("corpus", "corpus-key-aws.json"),
            json_data("corpus", "corpus-key-azure.json"),
            json_data("corpus", "corpus-key-gcp.json"),
            json_data("corpus", "corpus-key-kmip.json"),
        )
        self.addAsyncCleanup(vault.drop)

        client_encrypted = await self.async_rs_or_single_client(auto_encryption_opts=opts)

        client_encryption = self.create_client_encryption(
            self.kms_providers(),
            "keyvault.datakeys",
            async_client_context.client,
            OPTS,
            kms_tls_options=DEFAULT_KMS_TLS,
        )

        corpus = self.fix_up_curpus(json_data("corpus", "corpus.json"))
        corpus_copied: SON = SON()
        for key, value in corpus.items():
            corpus_copied[key] = copy.deepcopy(value)
            if key in (
                "_id",
                "altname_aws",
                "altname_azure",
                "altname_gcp",
                "altname_local",
                "altname_kmip",
            ):
                continue
            if value["method"] == "auto":
                continue
            if value["method"] == "explicit":
                identifier = value["identifier"]
                self.assertIn(identifier, ("id", "altname"))
                kms = value["kms"]
                self.assertIn(kms, ("local", "aws", "azure", "gcp", "kmip"))
                if identifier == "id":
                    if kms == "local":
                        kwargs = {"key_id": LOCAL_KEY_ID}
                    elif kms == "aws":
                        kwargs = {"key_id": AWS_KEY_ID}
                    elif kms == "azure":
                        kwargs = {"key_id": AZURE_KEY_ID}
                    elif kms == "gcp":
                        kwargs = {"key_id": GCP_KEY_ID}
                    else:
                        kwargs = {"key_id": KMIP_KEY_ID}
                else:
                    kwargs = {"key_alt_name": kms}

                self.assertIn(value["algo"], ("det", "rand"))
                if value["algo"] == "det":
                    algo = Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
                else:
                    algo = Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Random

                try:
                    encrypted_val = await client_encryption.encrypt(
                        value["value"],
                        algo,
                        **kwargs,  # type: ignore[arg-type]
                    )
                    if not value["allowed"]:
                        self.fail(f"encrypt should have failed: {key!r}: {value!r}")
                    corpus_copied[key]["value"] = encrypted_val
                except Exception:
                    if value["allowed"]:
                        tb = traceback.format_exc()
                        self.fail(f"encrypt failed: {key!r}: {value!r}, traceback: {tb}")

        await client_encrypted.db.coll.insert_one(corpus_copied)
        corpus_decrypted = await client_encrypted.db.coll.find_one()
        self.assertEqual(corpus_decrypted, corpus)

        corpus_encrypted_expected = self.fix_up_curpus_encrypted(
            json_data("corpus", "corpus-encrypted.json"), corpus
        )
        corpus_encrypted_actual = await coll.find_one()
        for key, value in corpus_encrypted_actual.items():
            if key in (
                "_id",
                "altname_aws",
                "altname_azure",
                "altname_gcp",
                "altname_local",
                "altname_kmip",
            ):
                continue

            if value["algo"] == "det":
                self.assertEqual(value["value"], corpus_encrypted_expected[key]["value"], key)
            elif value["algo"] == "rand" and value["allowed"]:
                self.assertNotEqual(value["value"], corpus_encrypted_expected[key]["value"], key)

            if value["allowed"]:
                decrypt_actual = await client_encryption.decrypt(value["value"])
                decrypt_expected = await client_encryption.decrypt(
                    corpus_encrypted_expected[key]["value"]
                )
                self.assertEqual(decrypt_actual, decrypt_expected, key)
            else:
                self.assertEqual(value["value"], corpus[key]["value"], key)

    async def test_corpus(self):
        opts = AutoEncryptionOpts(
            self.kms_providers(), "keyvault.datakeys", kms_tls_options=DEFAULT_KMS_TLS
        )
        await self._test_corpus(opts)

    async def test_corpus_local_schema(self):
        # Configure the encrypted field via the local schema_map option.
        schemas = {"db.coll": self.fix_up_schema(json_data("corpus", "corpus-schema.json"))}
        opts = AutoEncryptionOpts(
            self.kms_providers(),
            "keyvault.datakeys",
            schema_map=schemas,
            kms_tls_options=DEFAULT_KMS_TLS,
        )
        await self._test_corpus(opts)


_2_MiB = 2097152
_16_MiB = 16777216


class TestBsonSizeBatches(AsyncEncryptionIntegrationTest):
    """Prose tests for BSON size limits and batch splitting."""

    coll: AsyncCollection
    coll_encrypted: AsyncCollection
    client_encrypted: AsyncMongoClient
    listener: OvertCommandListener

    async def asyncSetUp(self):
        await super().asyncSetUp()
        db = async_client_context.client.db
        self.coll = db.coll
        await self.coll.drop()
        # Configure the encrypted 'db.coll' collection via jsonSchema.
        json_schema = json_data("limits", "limits-schema.json")
        await db.create_collection(
            "coll",
            validator={"$jsonSchema": json_schema},
            codec_options=OPTS,
            write_concern=WriteConcern(w="majority"),
        )

        # Create the key vault.
        coll = async_client_context.client.get_database(
            "keyvault", write_concern=WriteConcern(w="majority"), codec_options=OPTS
        )["datakeys"]
        await coll.drop()
        await coll.insert_one(json_data("limits", "limits-key.json"))

        opts = AutoEncryptionOpts({"local": {"key": LOCAL_MASTER_KEY}}, "keyvault.datakeys")
        self.listener = OvertCommandListener()
        self.client_encrypted = await self.async_rs_or_single_client(
            auto_encryption_opts=opts, event_listeners=[self.listener]
        )
        self.coll_encrypted = self.client_encrypted.db.coll

    async def asyncTearDown(self) -> None:
        await self.coll_encrypted.drop()

    async def test_01_insert_succeeds_under_2MiB(self):
        doc = {"_id": "over_2mib_under_16mib", "unencrypted": "a" * _2_MiB}
        await self.coll_encrypted.insert_one(doc)

        # Same with bulk_write.
        doc["_id"] = "over_2mib_under_16mib_bulk"
        await self.coll_encrypted.bulk_write([InsertOne(doc)])

    async def test_02_insert_succeeds_over_2MiB_post_encryption(self):
        doc = {"_id": "encryption_exceeds_2mib", "unencrypted": "a" * ((2**21) - 2000)}
        doc.update(json_data("limits", "limits-doc.json"))
        await self.coll_encrypted.insert_one(doc)

        # Same with bulk_write.
        doc["_id"] = "encryption_exceeds_2mib_bulk"
        await self.coll_encrypted.bulk_write([InsertOne(doc)])

    async def test_03_bulk_batch_split(self):
        doc1 = {"_id": "over_2mib_1", "unencrypted": "a" * _2_MiB}
        doc2 = {"_id": "over_2mib_2", "unencrypted": "a" * _2_MiB}
        self.listener.reset()
        await self.coll_encrypted.bulk_write([InsertOne(doc1), InsertOne(doc2)])
        self.assertEqual(
            len([c for c in self.listener.started_command_names() if c == "insert"]), 2
        )

    async def test_04_bulk_batch_split(self):
        limits_doc = json_data("limits", "limits-doc.json")
        doc1 = {"_id": "encryption_exceeds_2mib_1", "unencrypted": "a" * (_2_MiB - 2000)}
        doc1.update(limits_doc)
        doc2 = {"_id": "encryption_exceeds_2mib_2", "unencrypted": "a" * (_2_MiB - 2000)}
        doc2.update(limits_doc)
        self.listener.reset()
        await self.coll_encrypted.bulk_write([InsertOne(doc1), InsertOne(doc2)])
        self.assertEqual(
            len([c for c in self.listener.started_command_names() if c == "insert"]), 2
        )

    async def test_05_insert_succeeds_just_under_16MiB(self):
        doc = {"_id": "under_16mib", "unencrypted": "a" * (_16_MiB - 2000)}
        await self.coll_encrypted.insert_one(doc)

        # Same with bulk_write.
        doc["_id"] = "under_16mib_bulk"
        await self.coll_encrypted.bulk_write([InsertOne(doc)])

    async def test_06_insert_fails_over_16MiB(self):
        limits_doc = json_data("limits", "limits-doc.json")
        doc = {"_id": "encryption_exceeds_16mib", "unencrypted": "a" * (_16_MiB - 2000)}
        doc.update(limits_doc)

        with self.assertRaisesRegex(WriteError, "object to insert too large"):
            await self.coll_encrypted.insert_one(doc)

        # Same with bulk_write.
        doc["_id"] = "encryption_exceeds_16mib_bulk"
        with self.assertRaises(BulkWriteError) as ctx:
            await self.coll_encrypted.bulk_write([InsertOne(doc)])
        err = ctx.exception.details["writeErrors"][0]
        self.assertIn(err["code"], [2, 10334])
        self.assertIn("object to insert too large", err["errmsg"])


class TestCustomEndpoint(AsyncEncryptionIntegrationTest):
    """Prose tests for creating data keys with a custom endpoint."""

    @unittest.skipUnless(
        any([all(AWS_CREDS.values()), all(AZURE_CREDS.values()), all(GCP_CREDS.values())]),
        "No environment credentials are set",
    )
    async def asyncSetUp(self):
        await super().asyncSetUp()
        kms_providers = {
            "aws": AWS_CREDS,
            "azure": AZURE_CREDS,
            "gcp": GCP_CREDS,
            "kmip": KMIP_CREDS,
        }
        self.client_encryption = self.create_client_encryption(
            kms_providers=kms_providers,
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=async_client_context.client,
            codec_options=OPTS,
            kms_tls_options=DEFAULT_KMS_TLS,
        )

        kms_providers_invalid = copy.deepcopy(kms_providers)
        kms_providers_invalid["azure"]["identityPlatformEndpoint"] = "doesnotexist.invalid:443"
        kms_providers_invalid["gcp"]["endpoint"] = "doesnotexist.invalid:443"
        kms_providers_invalid["kmip"]["endpoint"] = "doesnotexist.invalid:5698"
        self.client_encryption_invalid = self.create_client_encryption(
            kms_providers=kms_providers_invalid,
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=async_client_context.client,
            codec_options=OPTS,
            kms_tls_options=DEFAULT_KMS_TLS,
        )
        self._kmip_host_error = None
        self._invalid_host_error = None

    async def run_test_expected_success(self, provider_name, master_key):
        data_key_id = await self.client_encryption.create_data_key(
            provider_name, master_key=master_key
        )
        encrypted = await self.client_encryption.encrypt(
            "test", Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=data_key_id
        )
        self.assertEqual("test", await self.client_encryption.decrypt(encrypted))

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def test_01_aws_region_key(self):
        await self.run_test_expected_success(
            "aws",
            {
                "region": "us-east-1",
                "key": (
                    "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                ),
            },
        )

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def test_02_aws_region_key_endpoint(self):
        await self.run_test_expected_success(
            "aws",
            {
                "region": "us-east-1",
                "key": (
                    "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                ),
                "endpoint": "kms.us-east-1.amazonaws.com",
            },
        )

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def test_03_aws_region_key_endpoint_port(self):
        await self.run_test_expected_success(
            "aws",
            {
                "region": "us-east-1",
                "key": (
                    "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                ),
                "endpoint": "kms.us-east-1.amazonaws.com:443",
            },
        )

    async def test_04_kmip_endpoint_invalid_port(self):
        master_key = {"keyId": "1", "endpoint": "localhost:12345"}
        with self.assertRaisesRegex(EncryptionError, "localhost:12345"):
            await self.client_encryption.create_data_key("kmip", master_key=master_key)

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def test_05_aws_endpoint_wrong_region(self):
        master_key = {
            "region": "us-east-1",
            "key": ("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"),
            "endpoint": "kms.us-east-2.amazonaws.com",
        }
        # The full error should be something like:
        # "Credential should be scoped to a valid region, not 'us-east-1'"
        # but we only check for EncryptionError to avoid breaking on slight
        # changes to AWS' error message.
        with self.assertRaises(EncryptionError):
            await self.client_encryption.create_data_key("aws", master_key=master_key)

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def test_06_aws_endpoint_invalid_host(self):
        master_key = {
            "region": "us-east-1",
            "key": ("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"),
            "endpoint": "doesnotexist.invalid",
        }
        with self.assertRaisesRegex(EncryptionError, self.invalid_host_error):
            await self.client_encryption.create_data_key("aws", master_key=master_key)

    @unittest.skipUnless(any(AZURE_CREDS.values()), "Azure environment credentials are not set")
    async def test_07_azure(self):
        master_key = {
            "keyVaultEndpoint": "key-vault-csfle.vault.azure.net",
            "keyName": "key-name-csfle",
        }
        await self.run_test_expected_success("azure", master_key)

        # The full error should be something like:
        # "[Errno 8] nodename nor servname provided, or not known"
        with self.assertRaisesRegex(EncryptionError, self.invalid_host_error):
            await self.client_encryption_invalid.create_data_key("azure", master_key=master_key)

    @unittest.skipUnless(any(GCP_CREDS.values()), "GCP environment credentials are not set")
    async def test_08_gcp_valid_endpoint(self):
        master_key = {
            "projectId": "devprod-drivers",
            "location": "global",
            "keyRing": "key-ring-csfle",
            "keyName": "key-name-csfle",
            "endpoint": "cloudkms.googleapis.com:443",
        }
        await self.run_test_expected_success("gcp", master_key)

        # The full error should be something like:
        # "[Errno 8] nodename nor servname provided, or not known"
        with self.assertRaisesRegex(EncryptionError, self.invalid_host_error):
            await self.client_encryption_invalid.create_data_key("gcp", master_key=master_key)

    @unittest.skipUnless(any(GCP_CREDS.values()), "GCP environment credentials are not set")
    async def test_09_gcp_invalid_endpoint(self):
        master_key = {
            "projectId": "devprod-drivers",
            "location": "global",
            "keyRing": "key-ring-csfle",
            "keyName": "key-name-csfle",
            "endpoint": "doesnotexist.invalid:443",
        }

        # The full error should be something like:
        # "Invalid KMS response, no access_token returned. HTTP status=200"
        with self.assertRaisesRegex(EncryptionError, "Invalid KMS response"):
            await self.client_encryption.create_data_key("gcp", master_key=master_key)

    def dns_error(self, host, port):
        # The full error should be something like:
        # "[Errno 8] nodename nor servname provided, or not known"
        with self.assertRaises(Exception) as ctx:
            socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        return re.escape(str(ctx.exception))

    @property
    def invalid_host_error(self):
        if self._invalid_host_error is None:
            self._invalid_host_error = self.dns_error("doesnotexist.invalid", 443)
        return self._invalid_host_error

    @property
    def kmip_host_error(self):
        if self._kmip_host_error is None:
            self._kmip_host_error = self.dns_error("doesnotexist.local", 5698)
        return self._kmip_host_error

    async def test_10_kmip_invalid_endpoint(self):
        key = {"keyId": "1"}
        await self.run_test_expected_success("kmip", key)
        with self.assertRaisesRegex(EncryptionError, self.kmip_host_error):
            await self.client_encryption_invalid.create_data_key("kmip", key)

    async def test_11_kmip_master_key_endpoint(self):
        key = {"keyId": "1", "endpoint": KMIP_CREDS["endpoint"]}
        await self.run_test_expected_success("kmip", key)
        # Override invalid endpoint:
        data_key_id = await self.client_encryption_invalid.create_data_key("kmip", master_key=key)
        encrypted = await self.client_encryption_invalid.encrypt(
            "test", Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=data_key_id
        )
        self.assertEqual("test", await self.client_encryption_invalid.decrypt(encrypted))

    async def test_12_kmip_master_key_invalid_endpoint(self):
        key = {"keyId": "1", "endpoint": "doesnotexist.invalid:5698"}
        with self.assertRaisesRegex(EncryptionError, self.kmip_host_error):
            await self.client_encryption.create_data_key("kmip", key)


class AzureGCPEncryptionTestMixin(AsyncEncryptionIntegrationTest):
    DEK = None
    KMS_PROVIDER_MAP = None
    KEYVAULT_DB = "keyvault"
    KEYVAULT_COLL = "datakeys"
    client: AsyncMongoClient

    async def _setup(self):
        keyvault = self.client.get_database(self.KEYVAULT_DB).get_collection(self.KEYVAULT_COLL)
        await create_key_vault(keyvault, self.DEK)

    async def _test_explicit(self, expectation):
        await self._setup()
        client_encryption = self.create_client_encryption(
            self.KMS_PROVIDER_MAP,  # type: ignore[arg-type]
            ".".join([self.KEYVAULT_DB, self.KEYVAULT_COLL]),
            async_client_context.client,
            OPTS,
        )

        ciphertext = await client_encryption.encrypt(
            "string0",
            algorithm=Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_id=self.DEK["_id"],
        )

        self.assertEqual(bytes(ciphertext), base64.b64decode(expectation))
        self.assertEqual(await client_encryption.decrypt(ciphertext), "string0")

    async def _test_automatic(self, expectation_extjson, payload):
        await self._setup()
        encrypted_db = "db"
        encrypted_coll = "coll"
        keyvault_namespace = ".".join([self.KEYVAULT_DB, self.KEYVAULT_COLL])

        encryption_opts = AutoEncryptionOpts(
            self.KMS_PROVIDER_MAP,  # type: ignore[arg-type]
            keyvault_namespace,
            schema_map=self.SCHEMA_MAP,
        )

        insert_listener = AllowListEventListener("insert")
        client = await self.async_rs_or_single_client(
            auto_encryption_opts=encryption_opts, event_listeners=[insert_listener]
        )

        coll = client.get_database(encrypted_db).get_collection(
            encrypted_coll, codec_options=OPTS, write_concern=WriteConcern("majority")
        )
        await coll.drop()

        expected_document = json_util.loads(expectation_extjson, json_options=JSON_OPTS)

        await coll.insert_one(payload)
        event = insert_listener.started_events[0]
        inserted_doc = event.command["documents"][0]

        for key, value in expected_document.items():
            self.assertEqual(value, inserted_doc[key])

        output_doc = await coll.find_one({})
        for key, value in payload.items():
            self.assertEqual(output_doc[key], value)


class TestAzureEncryption(AzureGCPEncryptionTestMixin, AsyncEncryptionIntegrationTest):
    @unittest.skipUnless(any(AZURE_CREDS.values()), "Azure environment credentials are not set")
    async def asyncSetUp(self):
        self.KMS_PROVIDER_MAP = {"azure": AZURE_CREDS}
        self.DEK = json_data(BASE, "custom", "azure-dek.json")
        self.SCHEMA_MAP = json_data(BASE, "custom", "azure-gcp-schema.json")
        await super().asyncSetUp()

    async def test_explicit(self):
        return await self._test_explicit(
            "AQGVERPgAAAAAAAAAAAAAAAC5DbBSwPwfSlBrDtRuglvNvCXD1KzDuCKY2P+4bRFtHDjpTOE2XuytPAUaAbXf1orsPq59PVZmsbTZbt2CB8qaQ=="
        )

    async def test_automatic(self):
        expected_document_extjson = textwrap.dedent(
            """
        {"secret_azure": {
            "$binary": {
                "base64": "AQGVERPgAAAAAAAAAAAAAAAC5DbBSwPwfSlBrDtRuglvNvCXD1KzDuCKY2P+4bRFtHDjpTOE2XuytPAUaAbXf1orsPq59PVZmsbTZbt2CB8qaQ==",
                "subType": "06"}
        }}"""
        )
        return await self._test_automatic(expected_document_extjson, {"secret_azure": "string0"})


class TestGCPEncryption(AzureGCPEncryptionTestMixin, AsyncEncryptionIntegrationTest):
    @unittest.skipUnless(any(GCP_CREDS.values()), "GCP environment credentials are not set")
    async def asyncSetUp(self):
        self.KMS_PROVIDER_MAP = {"gcp": GCP_CREDS}
        self.DEK = json_data(BASE, "custom", "gcp-dek.json")
        self.SCHEMA_MAP = json_data(BASE, "custom", "azure-gcp-schema.json")
        await super().asyncSetUp()

    async def test_explicit(self):
        return await self._test_explicit(
            "ARgj/gAAAAAAAAAAAAAAAAACwFd+Y5Ojw45GUXNvbcIpN9YkRdoHDHkR4kssdn0tIMKlDQOLFkWFY9X07IRlXsxPD8DcTiKnl6XINK28vhcGlg=="
        )

    async def test_automatic(self):
        expected_document_extjson = textwrap.dedent(
            """
        {"secret_gcp": {
            "$binary": {
                "base64": "ARgj/gAAAAAAAAAAAAAAAAACwFd+Y5Ojw45GUXNvbcIpN9YkRdoHDHkR4kssdn0tIMKlDQOLFkWFY9X07IRlXsxPD8DcTiKnl6XINK28vhcGlg==",
                "subType": "06"}
        }}"""
        )
        return await self._test_automatic(expected_document_extjson, {"secret_gcp": "string0"})


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#deadlock-tests
class TestDeadlockProse(AsyncEncryptionIntegrationTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.client_test = await self.async_rs_or_single_client(
            maxPoolSize=1, readConcernLevel="majority", w="majority", uuidRepresentation="standard"
        )

        self.client_keyvault_listener = OvertCommandListener()
        self.client_keyvault = await self.async_rs_or_single_client(
            maxPoolSize=1,
            readConcernLevel="majority",
            w="majority",
            event_listeners=[self.client_keyvault_listener],
        )

        await self.client_test.keyvault.datakeys.drop()
        await self.client_test.db.coll.drop()
        await self.client_test.keyvault.datakeys.insert_one(
            json_data("external", "external-key.json")
        )
        _ = await self.client_test.db.create_collection(
            "coll",
            validator={"$jsonSchema": json_data("external", "external-schema.json")},
            codec_options=OPTS,
        )

        client_encryption = self.create_client_encryption(
            kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=self.client_test,
            codec_options=OPTS,
        )
        self.ciphertext = await client_encryption.encrypt(
            "string0", Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_alt_name="local"
        )

        self.client_listener = OvertCommandListener()
        self.topology_listener = TopologyEventListener()
        self.optargs = ({"local": {"key": LOCAL_MASTER_KEY}}, "keyvault.datakeys")

    async def _run_test(self, max_pool_size, auto_encryption_opts):
        client_encrypted = await self.async_rs_or_single_client(
            readConcernLevel="majority",
            w="majority",
            maxPoolSize=max_pool_size,
            auto_encryption_opts=auto_encryption_opts,
            event_listeners=[self.client_listener, self.topology_listener],
        )

        if auto_encryption_opts._bypass_auto_encryption is True:
            await self.client_test.db.coll.insert_one({"_id": 0, "encrypted": self.ciphertext})
        elif auto_encryption_opts._bypass_auto_encryption is False:
            await client_encrypted.db.coll.insert_one({"_id": 0, "encrypted": "string0"})
        else:
            raise RuntimeError("bypass_auto_encryption must be a bool")

        result = await client_encrypted.db.coll.find_one({"_id": 0})
        self.assertEqual(result, {"_id": 0, "encrypted": "string0"})

    async def test_case_1(self):
        await self._run_test(
            max_pool_size=1,
            auto_encryption_opts=AutoEncryptionOpts(
                *self.optargs, bypass_auto_encryption=False, key_vault_client=None
            ),
        )

        cev = self.client_listener.started_events
        self.assertEqual(len(cev), 4)
        self.assertEqual(cev[0].command_name, "listCollections")
        self.assertEqual(cev[0].database_name, "db")
        self.assertEqual(cev[1].command_name, "find")
        self.assertEqual(cev[1].database_name, "keyvault")
        self.assertEqual(cev[2].command_name, "insert")
        self.assertEqual(cev[2].database_name, "db")
        self.assertEqual(cev[3].command_name, "find")
        self.assertEqual(cev[3].database_name, "db")

        self.assertEqual(len(self.topology_listener.results["opened"]), 2)

    async def test_case_2(self):
        await self._run_test(
            max_pool_size=1,
            auto_encryption_opts=AutoEncryptionOpts(
                *self.optargs, bypass_auto_encryption=False, key_vault_client=self.client_keyvault
            ),
        )

        cev = self.client_listener.started_events
        self.assertEqual(len(cev), 3)
        self.assertEqual(cev[0].command_name, "listCollections")
        self.assertEqual(cev[0].database_name, "db")
        self.assertEqual(cev[1].command_name, "insert")
        self.assertEqual(cev[1].database_name, "db")
        self.assertEqual(cev[2].command_name, "find")
        self.assertEqual(cev[2].database_name, "db")

        cev = self.client_keyvault_listener.started_events
        self.assertEqual(len(cev), 1)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "keyvault")

        self.assertEqual(len(self.topology_listener.results["opened"]), 2)

    async def test_case_3(self):
        await self._run_test(
            max_pool_size=1,
            auto_encryption_opts=AutoEncryptionOpts(
                *self.optargs, bypass_auto_encryption=True, key_vault_client=None
            ),
        )

        cev = self.client_listener.started_events
        self.assertEqual(len(cev), 2)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "db")
        self.assertEqual(cev[1].command_name, "find")
        self.assertEqual(cev[1].database_name, "keyvault")

        self.assertEqual(len(self.topology_listener.results["opened"]), 2)

    async def test_case_4(self):
        await self._run_test(
            max_pool_size=1,
            auto_encryption_opts=AutoEncryptionOpts(
                *self.optargs, bypass_auto_encryption=True, key_vault_client=self.client_keyvault
            ),
        )

        cev = self.client_listener.started_events
        self.assertEqual(len(cev), 1)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "db")

        cev = self.client_keyvault_listener.started_events
        self.assertEqual(len(cev), 1)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "keyvault")

        self.assertEqual(len(self.topology_listener.results["opened"]), 1)

    async def test_case_5(self):
        await self._run_test(
            max_pool_size=None,
            auto_encryption_opts=AutoEncryptionOpts(
                *self.optargs, bypass_auto_encryption=False, key_vault_client=None
            ),
        )

        cev = self.client_listener.started_events
        self.assertEqual(len(cev), 5)
        self.assertEqual(cev[0].command_name, "listCollections")
        self.assertEqual(cev[0].database_name, "db")
        self.assertEqual(cev[1].command_name, "listCollections")
        self.assertEqual(cev[1].database_name, "keyvault")
        self.assertEqual(cev[2].command_name, "find")
        self.assertEqual(cev[2].database_name, "keyvault")
        self.assertEqual(cev[3].command_name, "insert")
        self.assertEqual(cev[3].database_name, "db")
        self.assertEqual(cev[4].command_name, "find")
        self.assertEqual(cev[4].database_name, "db")

        self.assertEqual(len(self.topology_listener.results["opened"]), 1)

    async def test_case_6(self):
        await self._run_test(
            max_pool_size=None,
            auto_encryption_opts=AutoEncryptionOpts(
                *self.optargs, bypass_auto_encryption=False, key_vault_client=self.client_keyvault
            ),
        )

        cev = self.client_listener.started_events
        self.assertEqual(len(cev), 3)
        self.assertEqual(cev[0].command_name, "listCollections")
        self.assertEqual(cev[0].database_name, "db")
        self.assertEqual(cev[1].command_name, "insert")
        self.assertEqual(cev[1].database_name, "db")
        self.assertEqual(cev[2].command_name, "find")
        self.assertEqual(cev[2].database_name, "db")

        cev = self.client_keyvault_listener.started_events
        self.assertEqual(len(cev), 1)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "keyvault")

        self.assertEqual(len(self.topology_listener.results["opened"]), 1)

    async def test_case_7(self):
        await self._run_test(
            max_pool_size=None,
            auto_encryption_opts=AutoEncryptionOpts(
                *self.optargs, bypass_auto_encryption=True, key_vault_client=None
            ),
        )

        cev = self.client_listener.started_events
        self.assertEqual(len(cev), 2)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "db")
        self.assertEqual(cev[1].command_name, "find")
        self.assertEqual(cev[1].database_name, "keyvault")

        self.assertEqual(len(self.topology_listener.results["opened"]), 1)

    async def test_case_8(self):
        await self._run_test(
            max_pool_size=None,
            auto_encryption_opts=AutoEncryptionOpts(
                *self.optargs, bypass_auto_encryption=True, key_vault_client=self.client_keyvault
            ),
        )

        cev = self.client_listener.started_events
        self.assertEqual(len(cev), 1)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "db")

        cev = self.client_keyvault_listener.started_events
        self.assertEqual(len(cev), 1)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "keyvault")

        self.assertEqual(len(self.topology_listener.results["opened"]), 1)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#14-decryption-events
class TestDecryptProse(AsyncEncryptionIntegrationTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.client = async_client_context.client
        await self.client.db.drop_collection("decryption_events")
        await create_key_vault(self.client.keyvault.datakeys)
        kms_providers_map = {"local": {"key": LOCAL_MASTER_KEY}}

        self.client_encryption = self.create_client_encryption(
            kms_providers_map, "keyvault.datakeys", self.client, CodecOptions()
        )
        keyID = await self.client_encryption.create_data_key("local")
        self.cipher_text = await self.client_encryption.encrypt(
            "hello", key_id=keyID, algorithm=Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
        )
        self.malformed_cipher_text = self.cipher_text[:-1] + (self.cipher_text[-1] ^ 1).to_bytes(
            1, "big"
        )
        self.malformed_cipher_text = Binary(self.malformed_cipher_text, 6)
        opts = AutoEncryptionOpts(
            key_vault_namespace="keyvault.datakeys", kms_providers=kms_providers_map
        )
        self.listener = AllowListEventListener("aggregate")
        self.encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=opts, retryReads=False, event_listeners=[self.listener]
        )

    async def test_01_command_error(self):
        async with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"errorCode": 123, "failCommands": ["aggregate"]},
            }
        ):
            with self.assertRaises(OperationFailure):
                await self.encrypted_client.db.decryption_events.aggregate([])
        self.assertEqual(len(self.listener.failed_events), 1)
        for event in self.listener.failed_events:
            self.assertEqual(event.failure["code"], 123)

    async def test_02_network_error(self):
        async with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"errorCode": 123, "closeConnection": True, "failCommands": ["aggregate"]},
            }
        ):
            with self.assertRaises(AutoReconnect):
                await self.encrypted_client.db.decryption_events.aggregate([])
        self.assertEqual(len(self.listener.failed_events), 1)
        self.assertEqual(self.listener.failed_events[0].command_name, "aggregate")

    async def test_03_decrypt_error(self):
        await self.encrypted_client.db.decryption_events.insert_one(
            {"encrypted": self.malformed_cipher_text}
        )
        with self.assertRaises(EncryptionError):
            await anext(await self.encrypted_client.db.decryption_events.aggregate([]))
        event = self.listener.succeeded_events[0]
        self.assertEqual(len(self.listener.failed_events), 0)
        self.assertEqual(
            event.reply["cursor"]["firstBatch"][0]["encrypted"], self.malformed_cipher_text
        )

    async def test_04_decrypt_success(self):
        await self.encrypted_client.db.decryption_events.insert_one({"encrypted": self.cipher_text})
        await anext(await self.encrypted_client.db.decryption_events.aggregate([]))
        event = self.listener.succeeded_events[0]
        self.assertEqual(len(self.listener.failed_events), 0)
        self.assertEqual(event.reply["cursor"]["firstBatch"][0]["encrypted"], self.cipher_text)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#bypass-spawning-mongocryptd
class TestBypassSpawningMongocryptdProse(AsyncEncryptionIntegrationTest):
    @unittest.skipIf(
        os.environ.get("TEST_CRYPT_SHARED"),
        "this prose test does not work when crypt_shared is on a system dynamic "
        "library search path.",
    )
    async def test_mongocryptd_bypass_spawn(self):
        # Lower the mongocryptd timeout to reduce the test run time.
        self._original_timeout = encryption._MONGOCRYPTD_TIMEOUT_MS
        encryption._MONGOCRYPTD_TIMEOUT_MS = 500

        def reset_timeout():
            encryption._MONGOCRYPTD_TIMEOUT_MS = self._original_timeout

        self.addCleanup(reset_timeout)

        # Configure the encrypted field via the local schema_map option.
        schemas = {"db.coll": json_data("external", "external-schema.json")}
        opts = AutoEncryptionOpts(
            {"local": {"key": LOCAL_MASTER_KEY}},
            "keyvault.datakeys",
            schema_map=schemas,
            mongocryptd_bypass_spawn=True,
            mongocryptd_uri="mongodb://localhost:27027/",
            mongocryptd_spawn_args=[
                "--pidfilepath=bypass-spawning-mongocryptd.pid",
                "--port=27027",
            ],
        )
        client_encrypted = await self.async_rs_or_single_client(auto_encryption_opts=opts)
        with self.assertRaisesRegex(EncryptionError, "Timeout"):
            await client_encrypted.db.coll.insert_one({"encrypted": "test"})

    async def test_bypassAutoEncryption(self):
        opts = AutoEncryptionOpts(
            {"local": {"key": LOCAL_MASTER_KEY}},
            "keyvault.datakeys",
            bypass_auto_encryption=True,
            mongocryptd_spawn_args=[
                "--pidfilepath=bypass-spawning-mongocryptd.pid",
                "--port=27027",
            ],
        )
        client_encrypted = await self.async_rs_or_single_client(auto_encryption_opts=opts)
        await client_encrypted.db.coll.insert_one({"unencrypted": "test"})
        # Validate that mongocryptd was not spawned:
        mongocryptd_client = self.simple_client(
            "mongodb://localhost:27027/?serverSelectionTimeoutMS=500"
        )
        with self.assertRaises(ServerSelectionTimeoutError):
            await mongocryptd_client.admin.command("ping")

    @unittest.skipUnless(os.environ.get("TEST_CRYPT_SHARED"), "crypt_shared lib is not installed")
    async def test_via_loading_shared_library(self):
        await create_key_vault(
            async_client_context.client.keyvault.datakeys,
            json_data("external", "external-key.json"),
        )
        schemas = {"db.coll": json_data("external", "external-schema.json")}
        opts = AutoEncryptionOpts(
            kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            key_vault_namespace="keyvault.datakeys",
            schema_map=schemas,
            mongocryptd_uri="mongodb://localhost:47021/db?serverSelectionTimeoutMS=1000",
            mongocryptd_spawn_args=[
                "--pidfilepath=bypass-spawning-mongocryptd.pid",
                "--port=47021",
            ],
            crypt_shared_lib_required=True,
        )
        client_encrypted = await self.async_rs_or_single_client(auto_encryption_opts=opts)
        await client_encrypted.db.coll.drop()
        await client_encrypted.db.coll.insert_one({"encrypted": "test"})
        self.assertEncrypted((await async_client_context.client.db.coll.find_one({}))["encrypted"])
        no_mongocryptd_client = self.simple_client(
            host="mongodb://localhost:47021/db?serverSelectionTimeoutMS=1000"
        )
        with self.assertRaises(ServerSelectionTimeoutError):
            await no_mongocryptd_client.db.command("ping")

    # https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#20-bypass-creating-mongocryptd-client-when-shared-library-is-loaded
    @unittest.skipUnless(os.environ.get("TEST_CRYPT_SHARED"), "crypt_shared lib is not installed")
    async def test_client_via_loading_shared_library(self):
        connection_established = False

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                nonlocal connection_established
                connection_established = True

        server = socketserver.TCPServer(("localhost", 47021), Handler)

        def listener():
            with server:
                server.serve_forever(poll_interval=0.05)  # Short poll timeout to speed up the test

        listener_t = Thread(target=listener)
        listener_t.start()
        await create_key_vault(
            async_client_context.client.keyvault.datakeys,
            json_data("external", "external-key.json"),
        )
        schemas = {"db.coll": json_data("external", "external-schema.json")}
        opts = AutoEncryptionOpts(
            kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            key_vault_namespace="keyvault.datakeys",
            schema_map=schemas,
            mongocryptd_uri="mongodb://localhost:47021",
            crypt_shared_lib_required=False,
        )
        client_encrypted = await self.async_rs_or_single_client(auto_encryption_opts=opts)
        await client_encrypted.db.coll.drop()
        await client_encrypted.db.coll.insert_one({"encrypted": "test"})
        server.shutdown()
        listener_t.join()
        self.assertFalse(connection_established, "a connection was established on port 47021")


# https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#kms-tls-tests
class TestKmsTLSProse(AsyncEncryptionIntegrationTest):
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.patch_system_certs(CA_PEM)
        self.client_encrypted = self.create_client_encryption(
            {"aws": AWS_CREDS}, "keyvault.datakeys", self.client, OPTS
        )

    async def test_invalid_kms_certificate_expired(self):
        key = {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
            "endpoint": "mongodb://127.0.0.1:9000",
        }
        # Some examples:
        # certificate verify failed: certificate has expired (_ssl.c:1129)
        # amazon1-2018 Python 3.6: certificate verify failed (_ssl.c:852)
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            await self.client_encrypted.create_data_key("aws", master_key=key)

    async def test_invalid_hostname_in_kms_certificate(self):
        key = {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
            "endpoint": "mongodb://127.0.0.1:9001",
        }
        # Some examples:
        # certificate verify failed: IP address mismatch, certificate is not valid for '127.0.0.1'. (_ssl.c:1129)"
        # hostname '127.0.0.1' doesn't match 'wronghost.com'
        # 127.0.0.1:9001: ('Certificate does not contain any `subjectAltName`s.',)
        with self.assertRaisesRegex(
            EncryptionError, "IP address mismatch|wronghost|IPAddressMismatch|Certificate"
        ):
            await self.client_encrypted.create_data_key("aws", master_key=key)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#kms-tls-options-tests
class TestKmsTLSOptions(AsyncEncryptionIntegrationTest):
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def asyncSetUp(self):
        await super().asyncSetUp()
        # 1, create client with only tlsCAFile.
        providers: dict = copy.deepcopy(ALL_KMS_PROVIDERS)
        providers["azure"]["identityPlatformEndpoint"] = "127.0.0.1:9002"
        providers["gcp"]["endpoint"] = "127.0.0.1:9002"
        kms_tls_opts_ca_only = {
            "aws": {"tlsCAFile": CA_PEM},
            "azure": {"tlsCAFile": CA_PEM},
            "gcp": {"tlsCAFile": CA_PEM},
            "kmip": {"tlsCAFile": CA_PEM},
        }
        self.client_encryption_no_client_cert = self.create_client_encryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts_ca_only
        )
        # 2, same providers as above but with tlsCertificateKeyFile.
        kms_tls_opts = copy.deepcopy(kms_tls_opts_ca_only)
        for p in kms_tls_opts:
            kms_tls_opts[p]["tlsCertificateKeyFile"] = CLIENT_PEM
        self.client_encryption_with_tls = self.create_client_encryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts
        )
        # 3, update endpoints to expired host.
        providers: dict = copy.deepcopy(providers)
        providers["azure"]["identityPlatformEndpoint"] = "127.0.0.1:9000"
        providers["gcp"]["endpoint"] = "127.0.0.1:9000"
        providers["kmip"]["endpoint"] = "127.0.0.1:9000"
        self.client_encryption_expired = self.create_client_encryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts_ca_only
        )
        # 3, update endpoints to invalid host.
        providers: dict = copy.deepcopy(providers)
        providers["azure"]["identityPlatformEndpoint"] = "127.0.0.1:9001"
        providers["gcp"]["endpoint"] = "127.0.0.1:9001"
        providers["kmip"]["endpoint"] = "127.0.0.1:9001"
        self.client_encryption_invalid_hostname = self.create_client_encryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts_ca_only
        )
        # Errors when client has no cert, some examples:
        # [SSL: TLSV13_ALERT_CERTIFICATE_REQUIRED] tlsv13 alert certificate required (_ssl.c:2623)
        self.cert_error = (
            "certificate required|SSL handshake failed|"
            "KMS connection closed|Connection reset by peer|ECONNRESET|EPIPE"
        )
        # On Python 3.10+ this error might be:
        # EOF occurred in violation of protocol (_ssl.c:2384)
        if sys.version_info[:2] >= (3, 10):
            self.cert_error += "|EOF"
        # On Windows this error might be:
        # [WinError 10054] An existing connection was forcibly closed by the remote host
        if sys.platform == "win32":
            self.cert_error += "|forcibly closed"
        # 4, Test named KMS providers.
        providers = {
            "aws:no_client_cert": AWS_CREDS,
            "azure:no_client_cert": {"identityPlatformEndpoint": "127.0.0.1:9002", **AZURE_CREDS},
            "gcp:no_client_cert": {"endpoint": "127.0.0.1:9002", **GCP_CREDS},
            "kmip:no_client_cert": KMIP_CREDS,
            "aws:with_tls": AWS_CREDS,
            "azure:with_tls": {"identityPlatformEndpoint": "127.0.0.1:9002", **AZURE_CREDS},
            "gcp:with_tls": {"endpoint": "127.0.0.1:9002", **GCP_CREDS},
            "kmip:with_tls": KMIP_CREDS,
        }
        no_cert = {"tlsCAFile": CA_PEM}
        with_cert = {"tlsCAFile": CA_PEM, "tlsCertificateKeyFile": CLIENT_PEM}
        kms_tls_opts_4 = {
            "aws:no_client_cert": no_cert,
            "azure:no_client_cert": no_cert,
            "gcp:no_client_cert": no_cert,
            "kmip:no_client_cert": no_cert,
            "aws:with_tls": with_cert,
            "azure:with_tls": with_cert,
            "gcp:with_tls": with_cert,
            "kmip:with_tls": with_cert,
        }
        self.client_encryption_with_names = self.create_client_encryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts_4
        )

    async def test_01_aws(self):
        key = {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
            "endpoint": "127.0.0.1:9002",
        }
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            await self.client_encryption_no_client_cert.create_data_key("aws", key)
        # "parse error" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "parse error"):
            await self.client_encryption_with_tls.create_data_key("aws", key)
        # Some examples:
        # certificate verify failed: certificate has expired (_ssl.c:1129)
        # amazon1-2018 Python 3.6: certificate verify failed (_ssl.c:852)
        key["endpoint"] = "127.0.0.1:9000"
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            await self.client_encryption_expired.create_data_key("aws", key)
        # Some examples:
        # certificate verify failed: IP address mismatch, certificate is not valid for '127.0.0.1'. (_ssl.c:1129)"
        # hostname '127.0.0.1' doesn't match 'wronghost.com'
        # 127.0.0.1:9001: ('Certificate does not contain any `subjectAltName`s.',)
        key["endpoint"] = "127.0.0.1:9001"
        with self.assertRaisesRegex(
            EncryptionError,
            "IP address mismatch|wronghost|IPAddressMismatch|Certificate|SSL handshake failed",
        ):
            await self.client_encryption_invalid_hostname.create_data_key("aws", key)

    async def test_02_azure(self):
        key = {"keyVaultEndpoint": "doesnotexist.invalid", "keyName": "foo"}
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            await self.client_encryption_no_client_cert.create_data_key("azure", key)
        # "HTTP status=404" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "HTTP status=404"):
            await self.client_encryption_with_tls.create_data_key("azure", key)
        # Expired cert error.
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            await self.client_encryption_expired.create_data_key("azure", key)
        # Invalid cert hostname error.
        with self.assertRaisesRegex(
            EncryptionError,
            "IP address mismatch|wronghost|IPAddressMismatch|Certificate|SSL handshake failed",
        ):
            await self.client_encryption_invalid_hostname.create_data_key("azure", key)

    async def test_03_gcp(self):
        key = {"projectId": "foo", "location": "bar", "keyRing": "baz", "keyName": "foo"}
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            await self.client_encryption_no_client_cert.create_data_key("gcp", key)
        # "HTTP status=404" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "HTTP status=404"):
            await self.client_encryption_with_tls.create_data_key("gcp", key)
        # Expired cert error.
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            await self.client_encryption_expired.create_data_key("gcp", key)
        # Invalid cert hostname error.
        with self.assertRaisesRegex(
            EncryptionError,
            "IP address mismatch|wronghost|IPAddressMismatch|Certificate|SSL handshake failed",
        ):
            await self.client_encryption_invalid_hostname.create_data_key("gcp", key)

    async def test_04_kmip(self):
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            await self.client_encryption_no_client_cert.create_data_key("kmip")
        await self.client_encryption_with_tls.create_data_key("kmip")
        # Expired cert error.
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            await self.client_encryption_expired.create_data_key("kmip")
        # Invalid cert hostname error.
        with self.assertRaisesRegex(
            EncryptionError,
            "IP address mismatch|wronghost|IPAddressMismatch|Certificate|SSL handshake failed",
        ):
            await self.client_encryption_invalid_hostname.create_data_key("kmip")

    async def test_05_tlsDisableOCSPEndpointCheck_is_permitted(self):
        providers = {"aws": {"accessKeyId": "foo", "secretAccessKey": "bar"}}
        options = {"aws": {"tlsDisableOCSPEndpointCheck": True}}
        encryption = self.create_client_encryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=options
        )
        ctx = encryption._io_callbacks._kms_ssl_contexts["aws"]
        if not hasattr(ctx, "check_ocsp_endpoint"):
            raise self.skipTest("OCSP not enabled")
        self.assertFalse(ctx.check_ocsp_endpoint)

    async def test_06_named_kms_providers_apply_tls_options_aws(self):
        key = {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
            "endpoint": "127.0.0.1:9002",
        }
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            await self.client_encryption_with_names.create_data_key("aws:no_client_cert", key)
        # "parse error" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "parse error"):
            await self.client_encryption_with_names.create_data_key("aws:with_tls", key)

    async def test_06_named_kms_providers_apply_tls_options_azure(self):
        key = {"keyVaultEndpoint": "doesnotexist.invalid", "keyName": "foo"}
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            await self.client_encryption_with_names.create_data_key("azure:no_client_cert", key)
        # "HTTP status=404" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "HTTP status=404"):
            await self.client_encryption_with_names.create_data_key("azure:with_tls", key)

    async def test_06_named_kms_providers_apply_tls_options_gcp(self):
        key = {"projectId": "foo", "location": "bar", "keyRing": "baz", "keyName": "foo"}
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            await self.client_encryption_with_names.create_data_key("gcp:no_client_cert", key)
        # "HTTP status=404" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "HTTP status=404"):
            await self.client_encryption_with_names.create_data_key("gcp:with_tls", key)

    async def test_06_named_kms_providers_apply_tls_options_kmip(self):
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            await self.client_encryption_with_names.create_data_key("kmip:no_client_cert")
        await self.client_encryption_with_names.create_data_key("kmip:with_tls")


# https://github.com/mongodb/specifications/blob/50e26fe/source/client-side-encryption/tests/README.md#unique-index-on-keyaltnames
class TestUniqueIndexOnKeyAltNamesProse(AsyncEncryptionIntegrationTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.client = async_client_context.client
        await create_key_vault(self.client.keyvault.datakeys)
        kms_providers_map = {"local": {"key": LOCAL_MASTER_KEY}}
        self.client_encryption = self.create_client_encryption(
            kms_providers_map, "keyvault.datakeys", self.client, CodecOptions()
        )
        self.def_key_id = await self.client_encryption.create_data_key(
            "local", key_alt_names=["def"]
        )

    async def test_01_create_key(self):
        await self.client_encryption.create_data_key("local", key_alt_names=["abc"])
        with self.assertRaisesRegex(EncryptionError, "E11000 duplicate key error collection"):
            await self.client_encryption.create_data_key("local", key_alt_names=["abc"])
        with self.assertRaisesRegex(EncryptionError, "E11000 duplicate key error collection"):
            await self.client_encryption.create_data_key("local", key_alt_names=["def"])

    async def test_02_add_key_alt_name(self):
        key_id = await self.client_encryption.create_data_key("local")
        await self.client_encryption.add_key_alt_name(key_id, "abc")
        key_doc = await self.client_encryption.add_key_alt_name(key_id, "abc")
        assert key_doc["keyAltNames"] == ["abc"]
        with self.assertRaisesRegex(DuplicateKeyError, "E11000 duplicate key error collection"):
            await self.client_encryption.add_key_alt_name(key_id, "def")
        key_doc = await self.client_encryption.add_key_alt_name(self.def_key_id, "def")
        assert key_doc["keyAltNames"] == ["def"]


# https://github.com/mongodb/specifications/blob/d4c9432/source/client-side-encryption/tests/README.md#explicit-encryption
class TestExplicitQueryableEncryption(AsyncEncryptionIntegrationTest):
    @async_client_context.require_no_standalone
    @async_client_context.require_version_min(7, 0, -1)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.encrypted_fields = json_data("etc", "data", "encryptedFields.json")
        self.key1_document = json_data("etc", "data", "keys", "key1-document.json")
        self.key1_id = self.key1_document["_id"]
        self.db = self.client.test_queryable_encryption
        await self.client.drop_database(self.db)
        await self.db.command(
            "create", "explicit_encryption", encryptedFields=self.encrypted_fields
        )
        key_vault = await create_key_vault(self.client.keyvault.datakeys, self.key1_document)
        self.addCleanup(key_vault.drop)
        self.key_vault_client = self.client
        self.client_encryption = self.create_client_encryption(
            {"local": {"key": LOCAL_MASTER_KEY}}, key_vault.full_name, self.key_vault_client, OPTS
        )
        opts = AutoEncryptionOpts(
            {"local": {"key": LOCAL_MASTER_KEY}},
            key_vault.full_name,
            bypass_query_analysis=True,
        )
        self.encrypted_client = await self.async_rs_or_single_client(auto_encryption_opts=opts)

    async def test_01_insert_encrypted_indexed_and_find(self):
        val = "encrypted indexed value"
        insert_payload = await self.client_encryption.encrypt(
            val, Algorithm.INDEXED, self.key1_id, contention_factor=0
        )
        await self.encrypted_client[self.db.name].explicit_encryption.insert_one(
            {"encryptedIndexed": insert_payload}
        )

        find_payload = await self.client_encryption.encrypt(
            val, Algorithm.INDEXED, self.key1_id, query_type=QueryType.EQUALITY, contention_factor=0
        )
        docs = (
            await self.encrypted_client[self.db.name]
            .explicit_encryption.find({"encryptedIndexed": find_payload})
            .to_list()
        )

        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["encryptedIndexed"], val)

    async def test_02_insert_encrypted_indexed_and_find_contention(self):
        val = "encrypted indexed value"
        contention = 10
        for _ in range(contention):
            insert_payload = await self.client_encryption.encrypt(
                val, Algorithm.INDEXED, self.key1_id, contention_factor=contention
            )
            await self.encrypted_client[self.db.name].explicit_encryption.insert_one(
                {"encryptedIndexed": insert_payload}
            )

        find_payload = await self.client_encryption.encrypt(
            val, Algorithm.INDEXED, self.key1_id, query_type=QueryType.EQUALITY, contention_factor=0
        )
        docs = (
            await self.encrypted_client[self.db.name]
            .explicit_encryption.find({"encryptedIndexed": find_payload})
            .to_list()
        )

        self.assertLessEqual(len(docs), 10)
        for doc in docs:
            self.assertEqual(doc["encryptedIndexed"], val)

        # Find with contention_factor will return all 10 documents.
        find_payload = await self.client_encryption.encrypt(
            val,
            Algorithm.INDEXED,
            self.key1_id,
            query_type=QueryType.EQUALITY,
            contention_factor=contention,
        )
        docs = (
            await self.encrypted_client[self.db.name]
            .explicit_encryption.find({"encryptedIndexed": find_payload})
            .to_list()
        )

        self.assertEqual(len(docs), 10)
        for doc in docs:
            self.assertEqual(doc["encryptedIndexed"], val)

    async def test_03_insert_encrypted_unindexed(self):
        val = "encrypted unindexed value"
        insert_payload = await self.client_encryption.encrypt(
            val, Algorithm.UNINDEXED, self.key1_id
        )
        await self.encrypted_client[self.db.name].explicit_encryption.insert_one(
            {"_id": 1, "encryptedUnindexed": insert_payload}
        )

        docs = (
            await self.encrypted_client[self.db.name].explicit_encryption.find({"_id": 1}).to_list()
        )
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["encryptedUnindexed"], val)

    async def test_04_roundtrip_encrypted_indexed(self):
        val = "encrypted indexed value"
        payload = await self.client_encryption.encrypt(
            val, Algorithm.INDEXED, self.key1_id, contention_factor=0
        )
        decrypted = await self.client_encryption.decrypt(payload)
        self.assertEqual(decrypted, val)

    async def test_05_roundtrip_encrypted_unindexed(self):
        val = "encrypted indexed value"
        payload = await self.client_encryption.encrypt(val, Algorithm.UNINDEXED, self.key1_id)
        decrypted = await self.client_encryption.decrypt(payload)
        self.assertEqual(decrypted, val)


# https://github.com/mongodb/specifications/blob/527e22d5090ec48bf1e144c45fc831de0f1935f6/source/client-side-encryption/tests/README.md#25-test-lookup
class TestLookupProse(AsyncEncryptionIntegrationTest):
    @async_client_context.require_no_standalone
    @async_client_context.require_version_min(7, 0, -1)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=AutoEncryptionOpts(
                key_vault_namespace="db.keyvault",
                kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            )
        )
        await encrypted_client.drop_database("db")

        key_doc = json_data("etc", "data", "lookup", "key-doc.json")
        await create_key_vault(encrypted_client.db.keyvault, key_doc)
        self.addAsyncCleanup(async_client_context.client.drop_database, "db")

        await encrypted_client.db.create_collection(
            "csfle",
            validator={"$jsonSchema": json_data("etc", "data", "lookup", "schema-csfle.json")},
        )
        await encrypted_client.db.create_collection(
            "csfle2",
            validator={"$jsonSchema": json_data("etc", "data", "lookup", "schema-csfle2.json")},
        )
        await encrypted_client.db.create_collection(
            "qe", encryptedFields=json_data("etc", "data", "lookup", "schema-qe.json")
        )
        await encrypted_client.db.create_collection(
            "qe2", encryptedFields=json_data("etc", "data", "lookup", "schema-qe2.json")
        )
        await encrypted_client.db.create_collection("no_schema")
        await encrypted_client.db.create_collection("no_schema2")

        unencrypted_client = await self.async_rs_or_single_client()

        await encrypted_client.db.csfle.insert_one({"csfle": "csfle"})
        doc = await unencrypted_client.db.csfle.find_one()
        self.assertIsInstance(doc["csfle"], Binary)
        await encrypted_client.db.csfle2.insert_one({"csfle2": "csfle2"})
        doc = await unencrypted_client.db.csfle2.find_one()
        self.assertIsInstance(doc["csfle2"], Binary)
        await encrypted_client.db.qe.insert_one({"qe": "qe"})
        doc = await unencrypted_client.db.qe.find_one()
        self.assertIsInstance(doc["qe"], Binary)
        await encrypted_client.db.qe2.insert_one({"qe2": "qe2"})
        doc = await unencrypted_client.db.qe2.find_one()
        self.assertIsInstance(doc["qe2"], Binary)
        await encrypted_client.db.no_schema.insert_one({"no_schema": "no_schema"})
        await encrypted_client.db.no_schema2.insert_one({"no_schema2": "no_schema2"})

        await encrypted_client.close()
        await unencrypted_client.close()

    @async_client_context.require_version_min(8, 1, -1)
    async def test_1_csfle_joins_no_schema(self):
        encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=AutoEncryptionOpts(
                key_vault_namespace="db.keyvault",
                kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            )
        )
        doc = await anext(
            await encrypted_client.db.csfle.aggregate(
                [
                    {"$match": {"csfle": "csfle"}},
                    {
                        "$lookup": {
                            "from": "no_schema",
                            "as": "matched",
                            "pipeline": [
                                {"$match": {"no_schema": "no_schema"}},
                                {"$project": {"_id": 0}},
                            ],
                        }
                    },
                    {"$project": {"_id": 0}},
                ]
            )
        )
        self.assertEqual(doc, {"csfle": "csfle", "matched": [{"no_schema": "no_schema"}]})

    @async_client_context.require_version_min(8, 1, -1)
    async def test_2_qe_joins_no_schema(self):
        encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=AutoEncryptionOpts(
                key_vault_namespace="db.keyvault",
                kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            )
        )
        doc = await anext(
            await encrypted_client.db.qe.aggregate(
                [
                    {"$match": {"qe": "qe"}},
                    {
                        "$lookup": {
                            "from": "no_schema",
                            "as": "matched",
                            "pipeline": [
                                {"$match": {"no_schema": "no_schema"}},
                                {"$project": {"_id": 0, "__safeContent__": 0}},
                            ],
                        }
                    },
                    {"$project": {"_id": 0, "__safeContent__": 0}},
                ]
            )
        )
        self.assertEqual(doc, {"qe": "qe", "matched": [{"no_schema": "no_schema"}]})

    @async_client_context.require_version_min(8, 1, -1)
    async def test_3_no_schema_joins_csfle(self):
        encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=AutoEncryptionOpts(
                key_vault_namespace="db.keyvault",
                kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            )
        )
        doc = await anext(
            await encrypted_client.db.no_schema.aggregate(
                [
                    {"$match": {"no_schema": "no_schema"}},
                    {
                        "$lookup": {
                            "from": "csfle",
                            "as": "matched",
                            "pipeline": [{"$match": {"csfle": "csfle"}}, {"$project": {"_id": 0}}],
                        }
                    },
                    {"$project": {"_id": 0}},
                ]
            )
        )
        self.assertEqual(doc, {"no_schema": "no_schema", "matched": [{"csfle": "csfle"}]})

    @async_client_context.require_version_min(8, 1, -1)
    async def test_4_no_schema_joins_qe(self):
        encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=AutoEncryptionOpts(
                key_vault_namespace="db.keyvault",
                kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            )
        )
        doc = await anext(
            await encrypted_client.db.no_schema.aggregate(
                [
                    {"$match": {"no_schema": "no_schema"}},
                    {
                        "$lookup": {
                            "from": "qe",
                            "as": "matched",
                            "pipeline": [
                                {"$match": {"qe": "qe"}},
                                {"$project": {"_id": 0, "__safeContent__": 0}},
                            ],
                        }
                    },
                    {"$project": {"_id": 0}},
                ]
            )
        )
        self.assertEqual(doc, {"no_schema": "no_schema", "matched": [{"qe": "qe"}]})

    @async_client_context.require_version_min(8, 1, -1)
    async def test_5_csfle_joins_csfle2(self):
        encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=AutoEncryptionOpts(
                key_vault_namespace="db.keyvault",
                kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            )
        )
        doc = await anext(
            await encrypted_client.db.csfle.aggregate(
                [
                    {"$match": {"csfle": "csfle"}},
                    {
                        "$lookup": {
                            "from": "csfle2",
                            "as": "matched",
                            "pipeline": [
                                {"$match": {"csfle2": "csfle2"}},
                                {"$project": {"_id": 0}},
                            ],
                        }
                    },
                    {"$project": {"_id": 0}},
                ]
            )
        )
        self.assertEqual(doc, {"csfle": "csfle", "matched": [{"csfle2": "csfle2"}]})

    @async_client_context.require_version_min(8, 1, -1)
    async def test_6_qe_joins_qe2(self):
        encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=AutoEncryptionOpts(
                key_vault_namespace="db.keyvault",
                kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            )
        )
        doc = await anext(
            await encrypted_client.db.qe.aggregate(
                [
                    {"$match": {"qe": "qe"}},
                    {
                        "$lookup": {
                            "from": "qe2",
                            "as": "matched",
                            "pipeline": [
                                {"$match": {"qe2": "qe2"}},
                                {"$project": {"_id": 0, "__safeContent__": 0}},
                            ],
                        }
                    },
                    {"$project": {"_id": 0, "__safeContent__": 0}},
                ]
            )
        )
        self.assertEqual(doc, {"qe": "qe", "matched": [{"qe2": "qe2"}]})

    @async_client_context.require_version_min(8, 1, -1)
    async def test_7_no_schema_joins_no_schema2(self):
        encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=AutoEncryptionOpts(
                key_vault_namespace="db.keyvault",
                kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            )
        )
        doc = await anext(
            await encrypted_client.db.no_schema.aggregate(
                [
                    {"$match": {"no_schema": "no_schema"}},
                    {
                        "$lookup": {
                            "from": "no_schema2",
                            "as": "matched",
                            "pipeline": [
                                {"$match": {"no_schema2": "no_schema2"}},
                                {"$project": {"_id": 0}},
                            ],
                        }
                    },
                    {"$project": {"_id": 0}},
                ]
            )
        )
        self.assertEqual(doc, {"no_schema": "no_schema", "matched": [{"no_schema2": "no_schema2"}]})

    @async_client_context.require_version_min(8, 1, -1)
    async def test_8_csfle_joins_qe(self):
        encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=AutoEncryptionOpts(
                key_vault_namespace="db.keyvault",
                kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            )
        )
        with self.assertRaises(PyMongoError) as exc:
            _ = await anext(
                await encrypted_client.db.csfle.aggregate(
                    [
                        {"$match": {"csfle": "qe"}},
                        {
                            "$lookup": {
                                "from": "qe",
                                "as": "matched",
                                "pipeline": [{"$match": {"qe": "qe"}}, {"$project": {"_id": 0}}],
                            }
                        },
                        {"$project": {"_id": 0}},
                    ]
                )
            )
            self.assertIn("not supported", str(exc))

    @async_client_context.require_version_max(8, 1, -1)
    async def test_9_error(self):
        encrypted_client = await self.async_rs_or_single_client(
            auto_encryption_opts=AutoEncryptionOpts(
                key_vault_namespace="db.keyvault",
                kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            )
        )
        with self.assertRaises(PyMongoError) as exc:
            _ = await anext(
                await encrypted_client.db.csfle.aggregate(
                    [
                        {"$match": {"csfle": "csfle"}},
                        {
                            "$lookup": {
                                "from": "no_schema",
                                "as": "matched",
                                "pipeline": [
                                    {"$match": {"no_schema": "no_schema"}},
                                    {"$project": {"_id": 0}},
                                ],
                            }
                        },
                        {"$project": {"_id": 0}},
                    ]
                )
            )
            self.assertIn("Upgrade", str(exc))


# https://github.com/mongodb/specifications/blob/072601/source/client-side-encryption/tests/README.md#rewrap
class TestRewrapWithSeparateClientEncryption(AsyncEncryptionIntegrationTest):
    MASTER_KEYS: Mapping[str, Mapping[str, Any]] = {
        "aws": {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
        },
        "azure": {
            "keyVaultEndpoint": "key-vault-csfle.vault.azure.net",
            "keyName": "key-name-csfle",
        },
        "gcp": {
            "projectId": "devprod-drivers",
            "location": "global",
            "keyRing": "key-ring-csfle",
            "keyName": "key-name-csfle",
        },
        "kmip": {},
        "local": {},
    }

    async def test_rewrap(self):
        for src_provider in self.MASTER_KEYS:
            for dst_provider in self.MASTER_KEYS:
                with self.subTest(src_provider=src_provider, dst_provider=dst_provider):
                    await self.run_test(src_provider, dst_provider)

    async def run_test(self, src_provider, dst_provider):
        # Step 1. Drop the collection ``keyvault.datakeys``.
        await self.client.keyvault.drop_collection("datakeys")

        # Step 2. Create a ``AsyncClientEncryption`` object named ``client_encryption1``
        client_encryption1 = self.create_client_encryption(
            key_vault_client=self.client,
            key_vault_namespace="keyvault.datakeys",
            kms_providers=ALL_KMS_PROVIDERS,
            kms_tls_options=DEFAULT_KMS_TLS,
            codec_options=OPTS,
        )

        # Step 3. Call ``client_encryption1.create_data_key`` with ``src_provider``.
        key_id = await client_encryption1.create_data_key(
            master_key=self.MASTER_KEYS[src_provider], kms_provider=src_provider
        )

        # Step 4. Call ``client_encryption1.encrypt`` with the value "test"
        cipher_text = await client_encryption1.encrypt(
            "test", key_id=key_id, algorithm=Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
        )

        # Step 5. Create a ``AsyncClientEncryption`` object named ``client_encryption2``
        client2 = await self.async_rs_or_single_client()
        client_encryption2 = self.create_client_encryption(
            key_vault_client=client2,
            key_vault_namespace="keyvault.datakeys",
            kms_providers=ALL_KMS_PROVIDERS,
            kms_tls_options=DEFAULT_KMS_TLS,
            codec_options=OPTS,
        )

        # Step 6. Call ``client_encryption2.rewrap_many_data_key`` with an empty ``filter``.
        rewrap_many_data_key_result = await client_encryption2.rewrap_many_data_key(
            {}, provider=dst_provider, master_key=self.MASTER_KEYS[dst_provider]
        )

        self.assertEqual(rewrap_many_data_key_result.bulk_write_result.modified_count, 1)

        # 7. Call ``client_encryption1.decrypt`` with the ``cipher_text``. Assert the return value is "test".
        decrypt_result1 = await client_encryption1.decrypt(cipher_text)
        self.assertEqual(decrypt_result1, "test")

        # 8. Call ``client_encryption2.decrypt`` with the ``cipher_text``. Assert the return value is "test".
        decrypt_result2 = await client_encryption2.decrypt(cipher_text)
        self.assertEqual(decrypt_result2, "test")

        # 8. Case 2. Provider is not optional when master_key is given.
        with self.assertRaises(ConfigurationError):
            rewrap_many_data_key_result = await client_encryption2.rewrap_many_data_key(
                {}, master_key=self.MASTER_KEYS[dst_provider]
            )


# https://github.com/mongodb/specifications/blob/5cf3ed/source/client-side-encryption/tests/README.md#on-demand-aws-credentials
class TestOnDemandAWSCredentials(AsyncEncryptionIntegrationTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.master_key = {
            "region": "us-east-1",
            "key": ("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"),
        }

    @unittest.skipIf(any(AWS_CREDS.values()), "AWS environment credentials are set")
    async def test_01_failure(self):
        self.client_encryption = self.create_client_encryption(
            kms_providers={"aws": {}},
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=async_client_context.client,
            codec_options=OPTS,
        )
        with self.assertRaises(EncryptionError):
            await self.client_encryption.create_data_key("aws", self.master_key)

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def test_02_success(self):
        self.client_encryption = self.create_client_encryption(
            kms_providers={"aws": {}},
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=async_client_context.client,
            codec_options=OPTS,
        )
        await self.client_encryption.create_data_key("aws", self.master_key)


class TestQueryableEncryptionDocsExample(AsyncEncryptionIntegrationTest):
    # Queryable Encryption is not supported on Standalone topology.
    @async_client_context.require_no_standalone
    @async_client_context.require_version_min(7, 0, -1)
    async def asyncSetUp(self):
        await super().asyncSetUp()

    async def test_queryable_encryption(self):
        # AsyncMongoClient to use in testing that handles auth/tls/etc,
        # and cleanup.
        async def AsyncMongoClient(**kwargs):
            c = await self.async_rs_or_single_client(**kwargs)
            return c

        # Drop data from prior test runs.
        await self.client.keyvault.datakeys.drop()
        await self.client.drop_database("docs_examples")

        kms_providers_map = {"local": {"key": LOCAL_MASTER_KEY}}

        # Create two data keys.
        key_vault_client = await AsyncMongoClient()
        client_encryption = self.create_client_encryption(
            kms_providers_map, "keyvault.datakeys", key_vault_client, CodecOptions()
        )
        key1_id = await client_encryption.create_data_key("local")
        key2_id = await client_encryption.create_data_key("local")

        # Create an encryptedFieldsMap.
        encrypted_fields_map = {
            "docs_examples.encrypted": {
                "fields": [
                    {
                        "path": "encrypted_indexed",
                        "bsonType": "string",
                        "keyId": key1_id,
                        "queries": [
                            {
                                "queryType": "equality",
                            },
                        ],
                    },
                    {
                        "path": "encrypted_unindexed",
                        "bsonType": "string",
                        "keyId": key2_id,
                    },
                ],
            },
        }

        # Create an Queryable Encryption collection.
        opts = AutoEncryptionOpts(
            kms_providers_map, "keyvault.datakeys", encrypted_fields_map=encrypted_fields_map
        )
        encrypted_client = await AsyncMongoClient(auto_encryption_opts=opts)

        # Create a Queryable Encryption collection "docs_examples.encrypted".
        # Because docs_examples.encrypted is in encrypted_fields_map, it is
        # created with Queryable Encryption support.
        db = encrypted_client.docs_examples
        encrypted_coll = await db.create_collection("encrypted")

        # Auto encrypt an insert and find.

        # Encrypt an insert.
        await encrypted_coll.insert_one(
            {
                "_id": 1,
                "encrypted_indexed": "indexed_value",
                "encrypted_unindexed": "unindexed_value",
            }
        )

        # Encrypt a find.
        res = await encrypted_coll.find_one({"encrypted_indexed": "indexed_value"})
        assert res is not None
        assert res["encrypted_indexed"] == "indexed_value"
        assert res["encrypted_unindexed"] == "unindexed_value"

        # Find documents without decryption.
        unencrypted_client = await AsyncMongoClient()
        unencrypted_coll = unencrypted_client.docs_examples.encrypted
        res = await unencrypted_coll.find_one({"_id": 1})
        assert res is not None
        assert isinstance(res["encrypted_indexed"], Binary)
        assert isinstance(res["encrypted_unindexed"], Binary)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#22-range-explicit-encryption
class TestRangeQueryProse(AsyncEncryptionIntegrationTest):
    @async_client_context.require_no_standalone
    @async_client_context.require_version_min(8, 0, -1)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.key1_document = json_data("etc", "data", "keys", "key1-document.json")
        self.key1_id = self.key1_document["_id"]
        await self.client.drop_database(self.db)
        key_vault = await create_key_vault(self.client.keyvault.datakeys, self.key1_document)
        self.addCleanup(key_vault.drop)
        self.key_vault_client = self.client
        self.client_encryption = self.create_client_encryption(
            {"local": {"key": LOCAL_MASTER_KEY}}, key_vault.full_name, self.key_vault_client, OPTS
        )
        opts = AutoEncryptionOpts(
            {"local": {"key": LOCAL_MASTER_KEY}},
            key_vault.full_name,
            bypass_query_analysis=True,
        )
        self.encrypted_client = await self.async_rs_or_single_client(auto_encryption_opts=opts)
        self.db = self.encrypted_client.db

    async def run_expression_find(
        self, name, expression, expected_elems, range_opts, use_expr=False, key_id=None
    ):
        find_payload = await self.client_encryption.encrypt_expression(
            expression=expression,
            key_id=key_id or self.key1_id,
            algorithm=Algorithm.RANGE,
            query_type=QueryType.RANGE,
            contention_factor=0,
            range_opts=range_opts,
        )
        if use_expr:
            find_payload = {"$expr": find_payload}
        sorted_find = sorted(
            await self.encrypted_client.db.explicit_encryption.find(find_payload).to_list(),
            key=lambda x: x["_id"],
        )
        for elem, expected in zip(sorted_find, expected_elems):
            self.assertEqual(elem[f"encrypted{name}"], expected)

    async def run_test_cases(self, name, range_opts, cast_func):
        encrypted_fields = json_data("etc", "data", f"range-encryptedFields-{name}.json")
        await self.db.drop_collection("explicit_encryption", encrypted_fields=encrypted_fields)
        await self.db.create_collection("explicit_encryption", encryptedFields=encrypted_fields)

        async def encrypt_and_cast(i):
            return await self.client_encryption.encrypt(
                cast_func(i),
                key_id=self.key1_id,
                algorithm=Algorithm.RANGE,
                contention_factor=0,
                range_opts=range_opts,
            )

        for elem in [{f"encrypted{name}": await encrypt_and_cast(i)} for i in [0, 6, 30, 200]]:
            await self.encrypted_client.db.explicit_encryption.insert_one(elem)

        # Case 1.
        insert_payload = await self.client_encryption.encrypt(
            cast_func(6),
            key_id=self.key1_id,
            algorithm=Algorithm.RANGE,
            contention_factor=0,
            range_opts=range_opts,
        )
        self.assertEqual(await self.client_encryption.decrypt(insert_payload), cast_func(6))

        # Case 2.
        expression = {
            "$and": [
                {f"encrypted{name}": {"$gte": cast_func(6)}},
                {f"encrypted{name}": {"$lte": cast_func(200)}},
            ]
        }
        await self.run_expression_find(
            name, expression, [cast_func(i) for i in [6, 30, 200]], range_opts
        )
        # Case 2, with UUID key_id
        await self.run_expression_find(
            name,
            expression,
            [cast_func(i) for i in [6, 30, 200]],
            range_opts,
            key_id=self.key1_id.as_uuid(),
        )

        # Case 3.
        await self.run_expression_find(
            name,
            {
                "$and": [
                    {f"encrypted{name}": {"$gte": cast_func(0)}},
                    {f"encrypted{name}": {"$lte": cast_func(6)}},
                ]
            },
            [cast_func(i) for i in [0, 6]],
            range_opts,
        )

        # Case 4.
        await self.run_expression_find(
            name,
            {
                "$and": [
                    {f"encrypted{name}": {"$gt": cast_func(30)}},
                ]
            },
            [cast_func(i) for i in [200]],
            range_opts,
        )

        # Case 5.
        await self.run_expression_find(
            name,
            {"$and": [{"$lt": [f"$encrypted{name}", cast_func(30)]}]},
            [cast_func(i) for i in [0, 6]],
            range_opts,
            use_expr=True,
        )

        # The spec says to skip the following tests for no precision decimal or double types.
        if name not in ("DoubleNoPrecision", "DecimalNoPrecision"):
            # Case 6.
            with self.assertRaisesRegex(
                EncryptionError,
                "greater than or equal to the minimum value and less than or equal to the maximum value",
            ):
                await self.client_encryption.encrypt(
                    cast_func(201),
                    key_id=self.key1_id,
                    algorithm=Algorithm.RANGE,
                    contention_factor=0,
                    range_opts=range_opts,
                )

            # Case 7.
            with self.assertRaisesRegex(
                EncryptionError, "expected matching 'min' and value type. Got range option"
            ):
                await self.client_encryption.encrypt(
                    6 if cast_func != int else float(6),
                    key_id=self.key1_id,
                    algorithm=Algorithm.RANGE,
                    contention_factor=0,
                    range_opts=range_opts,
                )

            # Case 8.
            # The spec says we must additionally not run this case with any precision type, not just the ones above.
            if "Precision" not in name:
                with self.assertRaisesRegex(
                    EncryptionError,
                    "expected 'precision' to be set with double or decimal128 index, but got:",
                ):
                    await self.client_encryption.encrypt(
                        cast_func(6),
                        key_id=self.key1_id,
                        algorithm=Algorithm.RANGE,
                        contention_factor=0,
                        range_opts=RangeOpts(
                            min=cast_func(0),
                            max=cast_func(200),
                            sparsity=1,
                            trim_factor=1,
                            precision=2,
                        ),
                    )

    async def test_double_no_precision(self):
        await self.run_test_cases("DoubleNoPrecision", RangeOpts(sparsity=1, trim_factor=1), float)

    async def test_double_precision(self):
        await self.run_test_cases(
            "DoublePrecision",
            RangeOpts(min=0.0, max=200.0, sparsity=1, trim_factor=1, precision=2),
            float,
        )

    async def test_decimal_no_precision(self):
        await self.run_test_cases(
            "DecimalNoPrecision", RangeOpts(sparsity=1, trim_factor=1), lambda x: Decimal128(str(x))
        )

    async def test_decimal_precision(self):
        await self.run_test_cases(
            "DecimalPrecision",
            RangeOpts(
                min=Decimal128("0.0"),
                max=Decimal128("200.0"),
                sparsity=1,
                trim_factor=1,
                precision=2,
            ),
            lambda x: Decimal128(str(x)),
        )

    async def test_datetime(self):
        await self.run_test_cases(
            "Date",
            RangeOpts(min=DatetimeMS(0), max=DatetimeMS(200), sparsity=1, trim_factor=1),
            lambda x: DatetimeMS(x).as_datetime(),
        )

    async def test_int(self):
        await self.run_test_cases("Int", RangeOpts(min=0, max=200, sparsity=1, trim_factor=1), int)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#23-range-explicit-encryption-applies-defaults
class TestRangeQueryDefaultsProse(AsyncEncryptionIntegrationTest):
    @async_client_context.require_no_standalone
    @async_client_context.require_version_min(8, 0, -1)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.client.drop_database(self.db)
        self.key_vault_client = self.client
        self.client_encryption = self.create_client_encryption(
            {"local": {"key": LOCAL_MASTER_KEY}}, "keyvault.datakeys", self.key_vault_client, OPTS
        )
        self.key_id = await self.client_encryption.create_data_key("local")
        opts = RangeOpts(min=0, max=1000)
        self.payload_defaults = await self.client_encryption.encrypt(
            123, "range", self.key_id, contention_factor=0, range_opts=opts
        )

    async def test_uses_libmongocrypt_defaults(self):
        opts = RangeOpts(min=0, max=1000, sparsity=2, trim_factor=6)
        payload = await self.client_encryption.encrypt(
            123, "range", self.key_id, contention_factor=0, range_opts=opts
        )
        assert len(payload) == len(self.payload_defaults)

    async def test_accepts_trim_factor_0(self):
        opts = RangeOpts(min=0, max=1000, trim_factor=0)
        payload = await self.client_encryption.encrypt(
            123, "range", self.key_id, contention_factor=0, range_opts=opts
        )
        assert len(payload) > len(self.payload_defaults)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#24-kms-retry-tests
class TestKmsRetryProse(AsyncEncryptionIntegrationTest):
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    async def asyncSetUp(self):
        await super().asyncSetUp()
        # 1, create client with only tlsCAFile.
        providers: dict = copy.deepcopy(ALL_KMS_PROVIDERS)
        providers["azure"]["identityPlatformEndpoint"] = "127.0.0.1:9003"
        providers["gcp"]["endpoint"] = "127.0.0.1:9003"
        kms_tls_opts = {
            p: {"tlsCAFile": CA_PEM, "tlsCertificateKeyFile": CLIENT_PEM} for p in providers
        }
        self.client_encryption = self.create_client_encryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts
        )

    async def http_post(self, path, data=None):
        # Note, the connection to the mock server needs to be closed after
        # each request because the server is single threaded.
        ctx = ssl.create_default_context(cafile=CA_PEM)
        ctx.load_cert_chain(CLIENT_PEM)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        conn = http.client.HTTPSConnection("127.0.0.1:9003", context=ctx)
        try:
            if data is not None:
                headers = {"Content-type": "application/json"}
                body = json.dumps(data)
            else:
                headers = {}
                body = None
            conn.request("POST", path, body, headers)
            res = conn.getresponse()
            res.read()
        finally:
            conn.close()

    async def _test(self, provider, master_key):
        await self.http_post("/reset")
        # Case 1: createDataKey and encrypt with TCP retry
        await self.http_post("/set_failpoint/network", {"count": 1})
        key_id = await self.client_encryption.create_data_key(provider, master_key=master_key)
        await self.http_post("/set_failpoint/network", {"count": 1})
        await self.client_encryption.encrypt(
            123, Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id
        )

        # Case 2: createDataKey and encrypt with HTTP retry
        await self.http_post("/set_failpoint/http", {"count": 1})
        key_id = await self.client_encryption.create_data_key(provider, master_key=master_key)
        await self.http_post("/set_failpoint/http", {"count": 1})
        await self.client_encryption.encrypt(
            123, Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id
        )

        # Case 3: createDataKey fails after too many retries
        await self.http_post("/set_failpoint/network", {"count": 4})
        with self.assertRaisesRegex(EncryptionError, "KMS request failed after"):
            await self.client_encryption.create_data_key(provider, master_key=master_key)

    async def test_kms_retry(self):
        if IS_PYOPENSSL:
            self.skipTest(
                "PyOpenSSL does not support a required method for this test, Connection.makefile"
            )
        await self._test("aws", {"region": "foo", "key": "bar", "endpoint": "127.0.0.1:9003"})
        await self._test("azure", {"keyVaultEndpoint": "127.0.0.1:9003", "keyName": "foo"})
        await self._test(
            "gcp",
            {
                "projectId": "foo",
                "location": "bar",
                "keyRing": "baz",
                "keyName": "qux",
                "endpoint": "127.0.0.1:9003",
            },
        )


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#automatic-data-encryption-keys
class TestAutomaticDecryptionKeys(AsyncEncryptionIntegrationTest):
    @async_client_context.require_no_standalone
    @async_client_context.require_version_min(7, 0, -1)
    @flaky(reason="PYTHON-4982")
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.key1_document = json_data("etc", "data", "keys", "key1-document.json")
        self.key1_id = self.key1_document["_id"]
        await self.client.drop_database(self.db)
        self.key_vault = await create_key_vault(self.client.keyvault.datakeys, self.key1_document)
        self.addAsyncCleanup(self.key_vault.drop)
        self.client_encryption = self.create_client_encryption(
            {"local": {"key": LOCAL_MASTER_KEY}},
            self.key_vault.full_name,
            self.client,
            OPTS,
        )

    async def test_01_simple_create(self):
        coll, _ = await self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            encrypted_fields={"fields": [{"path": "ssn", "bsonType": "string", "keyId": None}]},
            kms_provider="local",
        )
        with self.assertRaises(WriteError) as exc:
            await coll.insert_one({"ssn": "123-45-6789"})
        self.assertEqual(exc.exception.code, 121)

    async def test_02_no_fields(self):
        with self.assertRaisesRegex(
            TypeError,
            "create_encrypted_collection.* missing 1 required positional argument: 'encrypted_fields'",
        ):
            await self.client_encryption.create_encrypted_collection(  # type:ignore[call-arg]
                database=self.db,
                name="testing1",
            )

    async def test_03_invalid_keyid(self):
        # checkAuthForCreateCollection can be removed when SERVER-102101 is fixed.
        with self.assertRaisesRegex(
            EncryptedCollectionError,
            "(create|checkAuthForCreateCollection).encryptedFields.fields.keyId' is the wrong type 'bool', expected type 'binData",
        ):
            await self.client_encryption.create_encrypted_collection(
                database=self.db,
                name="testing1",
                encrypted_fields={
                    "fields": [{"path": "ssn", "bsonType": "string", "keyId": False}]
                },
                kms_provider="local",
            )

    async def test_04_insert_encrypted(self):
        coll, ef = await self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            encrypted_fields={"fields": [{"path": "ssn", "bsonType": "string", "keyId": None}]},
            kms_provider="local",
        )
        key1_id = ef["fields"][0]["keyId"]
        encrypted_value = await self.client_encryption.encrypt(
            "123-45-6789",
            key_id=key1_id,
            algorithm=Algorithm.UNINDEXED,
        )
        await coll.insert_one({"ssn": encrypted_value})

    async def test_copy_encrypted_fields(self):
        encrypted_fields = {
            "fields": [
                {
                    "path": "ssn",
                    "bsonType": "string",
                    "keyId": None,
                }
            ]
        }
        _, ef = await self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            kms_provider="local",
            encrypted_fields=encrypted_fields,
        )
        self.assertIsNotNone(ef["fields"][0]["keyId"])
        self.assertIsNone(encrypted_fields["fields"][0]["keyId"])

    async def test_options_forward(self):
        coll, ef = await self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            kms_provider="local",
            encrypted_fields={"fields": [{"path": "ssn", "bsonType": "string", "keyId": None}]},
            read_preference=ReadPreference.NEAREST,
        )
        self.assertEqual(coll.read_preference, ReadPreference.NEAREST)
        self.assertEqual(coll.name, "testing1")

    async def test_mixed_null_keyids(self):
        key = await self.client_encryption.create_data_key(kms_provider="local")
        coll, ef = await self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            encrypted_fields={
                "fields": [
                    {"path": "ssn", "bsonType": "string", "keyId": None},
                    {"path": "dob", "bsonType": "string", "keyId": key},
                    {"path": "secrets", "bsonType": "string"},
                    {"path": "address", "bsonType": "string", "keyId": None},
                ]
            },
            kms_provider="local",
        )
        encrypted_values = [
            await self.client_encryption.encrypt(
                val,
                key_id=key,
                algorithm=Algorithm.UNINDEXED,
            )
            for val, key in zip(
                ["123-45-6789", "11/22/1963", "My secret", "New Mexico, 87104"],
                [field["keyId"] for field in ef["fields"]],
            )
        ]
        await coll.insert_one(
            {
                "ssn": encrypted_values[0],
                "dob": encrypted_values[1],
                "secrets": encrypted_values[2],
                "address": encrypted_values[3],
            }
        )

    async def test_create_datakey_fails(self):
        key = await self.client_encryption.create_data_key(kms_provider="local")
        encrypted_fields = {
            "fields": [
                {"path": "address", "bsonType": "string", "keyId": key},
                {"path": "dob", "bsonType": "string", "keyId": None},
            ]
        }
        # Make sure the exception's encrypted_fields object includes the previous keys in the error message even when
        # generating keys fails.
        with self.assertRaises(
            EncryptedCollectionError,
        ) as exc:
            await self.client_encryption.create_encrypted_collection(
                database=self.db,
                name="testing1",
                encrypted_fields=encrypted_fields,
                kms_provider="does not exist",
            )
        self.assertEqual(exc.exception.encrypted_fields, encrypted_fields)

    async def test_create_failure(self):
        key = await self.client_encryption.create_data_key(kms_provider="local")
        # Make sure the exception's encrypted_fields object includes the previous keys in the error message even when
        # it is the creation of the collection that fails.
        with self.assertRaises(
            EncryptedCollectionError,
        ) as exc:
            await self.client_encryption.create_encrypted_collection(
                database=self.db,
                name=1,  # type:ignore[arg-type]
                encrypted_fields={
                    "fields": [
                        {"path": "address", "bsonType": "string", "keyId": key},
                        {"path": "dob", "bsonType": "string", "keyId": None},
                    ]
                },
                kms_provider="local",
            )
        for field in exc.exception.encrypted_fields["fields"]:
            self.assertIsInstance(field["keyId"], Binary)

    async def test_collection_name_collision(self):
        encrypted_fields = {
            "fields": [
                {"path": "address", "bsonType": "string", "keyId": None},
            ]
        }
        await self.db.create_collection("testing1")
        with self.assertRaises(
            EncryptedCollectionError,
        ) as exc:
            await self.client_encryption.create_encrypted_collection(
                database=self.db,
                name="testing1",
                encrypted_fields=encrypted_fields,
                kms_provider="local",
            )
        self.assertIsInstance(exc.exception.encrypted_fields["fields"][0]["keyId"], Binary)
        await self.db.drop_collection("testing1", encrypted_fields=encrypted_fields)
        await self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            encrypted_fields=encrypted_fields,
            kms_provider="local",
        )
        with self.assertRaises(
            EncryptedCollectionError,
        ) as exc:
            await self.client_encryption.create_encrypted_collection(
                database=self.db,
                name="testing1",
                encrypted_fields=encrypted_fields,
                kms_provider="local",
            )
        self.assertIsInstance(exc.exception.encrypted_fields["fields"][0]["keyId"], Binary)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#27-text-explicit-encryption
class TestExplicitTextEncryptionProse(AsyncEncryptionIntegrationTest):
    @async_client_context.require_no_standalone
    @async_client_context.require_version_min(8, 2, -1)
    @async_client_context.require_libmongocrypt_min(1, 15, 1)
    @async_client_context.require_pymongocrypt_min(1, 16, 0)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        # Load the file key1-document.json as key1Document.
        self.key1_document = json_data("etc", "data", "keys", "key1-document.json")
        # Read the "_id" field of key1Document as key1ID.
        self.key1_id = self.key1_document["_id"]
        # Drop and create the collection keyvault.datakeys.
        # Insert key1Document in keyvault.datakeys with majority write concern.
        self.key_vault = await create_key_vault(self.client.keyvault.datakeys, self.key1_document)
        self.addAsyncCleanup(self.key_vault.drop)
        # Create a ClientEncryption object named clientEncryption with these options.
        self.kms_providers = {"local": {"key": LOCAL_MASTER_KEY}}
        self.client_encryption = self.create_client_encryption(
            self.kms_providers,
            self.key_vault.full_name,
            self.client,
            OPTS,
        )
        # Create a MongoClient named encryptedClient with these AutoEncryptionOpts.
        opts = AutoEncryptionOpts(
            self.kms_providers,
            "keyvault.datakeys",
            bypass_query_analysis=True,
        )
        self.client_encrypted = await self.async_rs_or_single_client(auto_encryption_opts=opts)

        # Using QE CreateCollection() and Collection.Drop(), drop and create the following collections with majority write concern:
        # db.prefix-suffix using the encryptedFields option set to the contents of encryptedFields-prefix-suffix.json.
        db = self.client_encrypted.db
        await db.drop_collection("prefix-suffix")
        encrypted_fields = json_data("etc", "data", "encryptedFields-prefix-suffix.json")
        await self.client_encryption.create_encrypted_collection(
            db, "prefix-suffix", kms_provider="local", encrypted_fields=encrypted_fields
        )
        # db.substring using the encryptedFields option set to the contents of encryptedFields-substring.json.
        await db.drop_collection("substring")
        encrypted_fields = json_data("etc", "data", "encryptedFields-substring.json")
        await self.client_encryption.create_encrypted_collection(
            db, "substring", kms_provider="local", encrypted_fields=encrypted_fields
        )

        # Use clientEncryption to encrypt the string "foobarbaz" with the following EncryptOpts.
        text_opts = TextOpts(
            case_sensitive=True,
            diacritic_sensitive=True,
            prefix=dict(strMaxQueryLength=10, strMinQueryLength=2),
            suffix=dict(strMaxQueryLength=10, strMinQueryLength=2),
        )
        encrypted_value = await self.client_encryption.encrypt(
            "foobarbaz",
            key_id=self.key1_id,
            algorithm=Algorithm.TEXTPREVIEW,
            contention_factor=0,
            text_opts=text_opts,
        )
        # Use encryptedClient to insert the following document into db.prefix-suffix with majority write concern.
        coll = self.client_encrypted.db["prefix-suffix"].with_options(
            write_concern=WriteConcern(w="majority")
        )
        await coll.insert_one({"_id": 0, "encryptedText": encrypted_value})

        # Use clientEncryption to encrypt the string "foobarbaz" with the following EncryptOpts.
        text_opts = TextOpts(
            case_sensitive=True,
            diacritic_sensitive=True,
            substring=dict(strMaxLength=10, strMaxQueryLength=10, strMinQueryLength=2),
        )
        encrypted_value = await self.client_encryption.encrypt(
            "foobarbaz",
            key_id=self.key1_id,
            algorithm=Algorithm.TEXTPREVIEW,
            contention_factor=0,
            text_opts=text_opts,
        )
        # Use encryptedClient to insert the following document into db.substring with majority write concern.
        coll = self.client_encrypted.db["substring"].with_options(
            write_concern=WriteConcern(w="majority")
        )
        await coll.insert_one({"_id": 0, "encryptedText": encrypted_value})

    async def test_01_can_find_a_document_by_prefix(self):
        # Use clientEncryption.encrypt() to encrypt the string "foo" with the following EncryptOpts.
        text_opts = TextOpts(
            case_sensitive=True,
            diacritic_sensitive=True,
            prefix=dict(strMaxQueryLength=10, strMinQueryLength=2),
        )
        encrypted_value = await self.client_encryption.encrypt(
            "foo",
            key_id=self.key1_id,
            algorithm=Algorithm.TEXTPREVIEW,
            query_type=QueryType.PREFIXPREVIEW,
            contention_factor=0,
            text_opts=text_opts,
        )
        # Use encryptedClient to run a "find" operation on the db.prefix-suffix collection with the following filter.
        value = await self.client_encrypted.db["prefix-suffix"].find_one(
            {"$expr": {"$encStrStartsWith": {"input": "$encryptedText", "prefix": encrypted_value}}}
        )
        # Assert the following document is returned.
        expected = {"_id": 0, "encryptedText": "foobarbaz"}
        value.pop("__safeContent__", None)
        self.assertEqual(value, expected)

    async def test_02_can_find_a_document_by_suffix(self):
        # Use clientEncryption.encrypt() to encrypt the string "baz" with the following EncryptOpts:
        text_opts = TextOpts(
            case_sensitive=True,
            diacritic_sensitive=True,
            suffix=dict(strMaxQueryLength=10, strMinQueryLength=2),
        )
        encrypted_value = await self.client_encryption.encrypt(
            "baz",
            key_id=self.key1_id,
            algorithm=Algorithm.TEXTPREVIEW,
            query_type=QueryType.SUFFIXPREVIEW,
            contention_factor=0,
            text_opts=text_opts,
        )
        # Use encryptedClient to run a "find" operation on the db.prefix-suffix collection with the following filter:
        value = await self.client_encrypted.db["prefix-suffix"].find_one(
            {"$expr": {"$encStrEndsWith": {"input": "$encryptedText", "suffix": encrypted_value}}}
        )
        # Assert the following document is returned.
        expected = {"_id": 0, "encryptedText": "foobarbaz"}
        value.pop("__safeContent__", None)
        self.assertEqual(value, expected)

    async def test_03_no_document_found_by_prefix(self):
        # Use clientEncryption.encrypt() to encrypt the string "baz" with the following EncryptOpts:
        text_opts = TextOpts(
            case_sensitive=True,
            diacritic_sensitive=True,
            prefix=dict(strMaxQueryLength=10, strMinQueryLength=2),
        )
        encrypted_value = await self.client_encryption.encrypt(
            "baz",
            key_id=self.key1_id,
            algorithm=Algorithm.TEXTPREVIEW,
            query_type=QueryType.PREFIXPREVIEW,
            contention_factor=0,
            text_opts=text_opts,
        )
        # Use encryptedClient to run a "find" operation on the db.prefix-suffix collection with the following filter:
        value = await self.client_encrypted.db["prefix-suffix"].find_one(
            {"$expr": {"$encStrStartsWith": {"input": "$encryptedText", "prefix": encrypted_value}}}
        )
        # Assert that no documents are returned.
        self.assertIsNone(value)

    async def test_04_no_document_found_by_suffix(self):
        # Use clientEncryption.encrypt() to encrypt the string "foo" with the following EncryptOpts:
        text_opts = TextOpts(
            case_sensitive=True,
            diacritic_sensitive=True,
            suffix=dict(strMaxQueryLength=10, strMinQueryLength=2),
        )
        encrypted_value = await self.client_encryption.encrypt(
            "foo",
            key_id=self.key1_id,
            algorithm=Algorithm.TEXTPREVIEW,
            query_type=QueryType.SUFFIXPREVIEW,
            contention_factor=0,
            text_opts=text_opts,
        )
        # Use encryptedClient to run a "find" operation on the db.prefix-suffix collection with the following filter:
        value = await self.client_encrypted.db["prefix-suffix"].find_one(
            {"$expr": {"$encStrEndsWith": {"input": "$encryptedText", "suffix": encrypted_value}}}
        )
        # Assert that no documents are returned.
        self.assertIsNone(value)

    async def test_05_can_find_a_document_by_substring(self):
        # Use clientEncryption.encrypt() to encrypt the string "bar" with the following EncryptOpts:
        text_opts = TextOpts(
            case_sensitive=True,
            diacritic_sensitive=True,
            substring=dict(strMaxLength=10, strMaxQueryLength=10, strMinQueryLength=2),
        )
        encrypted_value = await self.client_encryption.encrypt(
            "bar",
            key_id=self.key1_id,
            algorithm=Algorithm.TEXTPREVIEW,
            query_type=QueryType.SUBSTRINGPREVIEW,
            contention_factor=0,
            text_opts=text_opts,
        )
        # Use encryptedClient to run a "find" operation on the db.substring collection with the following filter:
        value = await self.client_encrypted.db["substring"].find_one(
            {
                "$expr": {
                    "$encStrContains": {"input": "$encryptedText", "substring": encrypted_value}
                }
            }
        )
        # Assert the following document is returned:
        expected = {"_id": 0, "encryptedText": "foobarbaz"}
        value.pop("__safeContent__", None)
        self.assertEqual(value, expected)

    async def test_06_no_document_found_by_substring(self):
        # Use clientEncryption.encrypt() to encrypt the string "qux" with the following EncryptOpts:
        text_opts = TextOpts(
            case_sensitive=True,
            diacritic_sensitive=True,
            substring=dict(strMaxLength=10, strMaxQueryLength=10, strMinQueryLength=2),
        )
        encrypted_value = await self.client_encryption.encrypt(
            "qux",
            key_id=self.key1_id,
            algorithm=Algorithm.TEXTPREVIEW,
            query_type=QueryType.SUBSTRINGPREVIEW,
            contention_factor=0,
            text_opts=text_opts,
        )
        # Use encryptedClient to run a "find" operation on the db.substring collection with the following filter:
        value = await self.client_encrypted.db["substring"].find_one(
            {
                "$expr": {
                    "$encStrContains": {"input": "$encryptedText", "substring": encrypted_value}
                }
            }
        )
        # Assert that no documents are returned.
        self.assertIsNone(value)

    async def test_07_contentionFactor_is_required(self):
        from pymongocrypt.errors import MongoCryptError

        # Use clientEncryption.encrypt() to encrypt the string "foo" with the following EncryptOpts:
        text_opts = TextOpts(
            case_sensitive=True,
            diacritic_sensitive=True,
            prefix=dict(strMaxQueryLength=10, strMinQueryLength=2),
        )
        with self.assertRaises(EncryptionError) as ctx:
            await self.client_encryption.encrypt(
                "foo",
                key_id=self.key1_id,
                algorithm=Algorithm.TEXTPREVIEW,
                query_type=QueryType.PREFIXPREVIEW,
                text_opts=text_opts,
            )
        # Expect an error from libmongocrypt with a message containing the string: "contention factor is required for textPreview algorithm".
        self.assertIsInstance(ctx.exception.cause, MongoCryptError)
        self.assertEqual(
            str(ctx.exception), "contention factor is required for textPreview algorithm"
        )


def start_mongocryptd(port) -> None:
    args = ["mongocryptd", f"--port={port}", "--idleShutdownTimeoutSecs=60"]
    _spawn_daemon(args)


@unittest.skipIf(os.environ.get("TEST_CRYPT_SHARED"), "crypt_shared lib is installed")
class TestNoSessionsSupport(AsyncEncryptionIntegrationTest):
    mongocryptd_client: AsyncMongoClient
    MONGOCRYPTD_PORT = 27020

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        start_mongocryptd(self.MONGOCRYPTD_PORT)

        self.listener = OvertCommandListener()
        self.mongocryptd_client = self.simple_client(
            f"mongodb://localhost:{self.MONGOCRYPTD_PORT}", event_listeners=[self.listener]
        )

        hello = await self.mongocryptd_client.db.command("hello")
        self.assertNotIn("logicalSessionTimeoutMinutes", hello)

    async def test_implicit_session_ignored_when_unsupported(self):
        self.listener.reset()
        with self.assertRaises(OperationFailure):
            await self.mongocryptd_client.db.test.find_one()

        self.assertNotIn("lsid", self.listener.started_events[0].command)

        with self.assertRaises(OperationFailure):
            await self.mongocryptd_client.db.test.insert_one({"x": 1})

        self.assertNotIn("lsid", self.listener.started_events[1].command)

        await self.mongocryptd_client.close()

    async def test_explicit_session_errors_when_unsupported(self):
        self.listener.reset()
        async with self.mongocryptd_client.start_session() as s:
            with self.assertRaisesRegex(
                ConfigurationError, r"Sessions are not supported by this MongoDB deployment"
            ):
                await self.mongocryptd_client.db.test.find_one(session=s)
            with self.assertRaisesRegex(
                ConfigurationError, r"Sessions are not supported by this MongoDB deployment"
            ):
                await self.mongocryptd_client.db.test.insert_one({"x": 1}, session=s)

        await self.mongocryptd_client.close()


if __name__ == "__main__":
    unittest.main()
