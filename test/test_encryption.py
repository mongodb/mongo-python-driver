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
import os
import re
import socket
import socketserver
import ssl
import sys
import textwrap
import traceback
import uuid
import warnings
from threading import Thread
from typing import Any, Dict, Mapping

from pymongo.collection import Collection
from pymongo.daemon import _spawn_daemon

sys.path[0:0] = [""]

from test import (
    AWS_CREDS,
    AZURE_CREDS,
    CA_PEM,
    CLIENT_PEM,
    GCP_CREDS,
    KMIP_CREDS,
    LOCAL_MASTER_KEY,
    IntegrationTest,
    PyMongoTestCase,
    client_context,
    unittest,
)
from test.test_bulk import BulkTestBase
from test.unified_format import generate_test_classes
from test.utils import (
    AllowListEventListener,
    OvertCommandListener,
    SpecTestCreator,
    TopologyEventListener,
    camel_to_snake_args,
    is_greenthread_patched,
    rs_or_single_client,
    wait_until,
)
from test.utils_spec_runner import SpecRunner

from bson import DatetimeMS, Decimal128, encode, json_util
from bson.binary import UUID_SUBTYPE, Binary, UuidRepresentation
from bson.codec_options import CodecOptions
from bson.errors import BSONError
from bson.json_util import JSONOptions
from bson.son import SON
from pymongo import ReadPreference, encryption
from pymongo.cursor import CursorType
from pymongo.encryption import Algorithm, ClientEncryption, QueryType
from pymongo.encryption_options import _HAVE_PYMONGOCRYPT, AutoEncryptionOpts, RangeOpts
from pymongo.errors import (
    AutoReconnect,
    BulkWriteError,
    ConfigurationError,
    DuplicateKeyError,
    EncryptedCollectionError,
    EncryptionError,
    InvalidOperation,
    OperationFailure,
    ServerSelectionTimeoutError,
    WriteError,
)
from pymongo.mongo_client import MongoClient
from pymongo.operations import InsertOne, ReplaceOne, UpdateOne
from pymongo.write_concern import WriteConcern

KMS_PROVIDERS = {"local": {"key": b"\x00" * 96}}


def get_client_opts(client):
    return client._MongoClient__options


class TestAutoEncryptionOpts(PyMongoTestCase):
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    @unittest.skipUnless(os.environ.get("TEST_CRYPT_SHARED"), "crypt_shared lib is not installed")
    def test_crypt_shared(self):
        # Test that we can pick up crypt_shared lib automatically
        client = MongoClient(
            auto_encryption_opts=AutoEncryptionOpts(
                KMS_PROVIDERS, "keyvault.datakeys", crypt_shared_lib_required=True
            ),
            connect=False,
        )
        self.addCleanup(client.close)

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
        self.assertEqual(opts._kms_ssl_contexts, {})

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
    def test_init_kms_tls_options(self):
        # Error cases:
        with self.assertRaisesRegex(TypeError, r'kms_tls_options\["kmip"\] must be a dict'):
            AutoEncryptionOpts({}, "k.d", kms_tls_options={"kmip": 1})
        tls_opts: Any
        for tls_opts in [
            {"kmip": {"tls": True, "tlsInsecure": True}},
            {"kmip": {"tls": True, "tlsAllowInvalidCertificates": True}},
            {"kmip": {"tls": True, "tlsAllowInvalidHostnames": True}},
        ]:
            with self.assertRaisesRegex(ConfigurationError, "Insecure TLS options prohibited"):
                opts = AutoEncryptionOpts({}, "k.d", kms_tls_options=tls_opts)
        with self.assertRaises(FileNotFoundError):
            AutoEncryptionOpts({}, "k.d", kms_tls_options={"kmip": {"tlsCAFile": "does-not-exist"}})
        # Success cases:
        tls_opts: Any
        for tls_opts in [None, {}]:
            opts = AutoEncryptionOpts({}, "k.d", kms_tls_options=tls_opts)
            self.assertEqual(opts._kms_ssl_contexts, {})
        opts = AutoEncryptionOpts({}, "k.d", kms_tls_options={"kmip": {"tls": True}, "aws": {}})
        ctx = opts._kms_ssl_contexts["kmip"]
        self.assertEqual(ctx.check_hostname, True)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        ctx = opts._kms_ssl_contexts["aws"]
        self.assertEqual(ctx.check_hostname, True)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        opts = AutoEncryptionOpts(
            {},
            "k.d",
            kms_tls_options={"kmip": {"tlsCAFile": CA_PEM, "tlsCertificateKeyFile": CLIENT_PEM}},
        )
        ctx = opts._kms_ssl_contexts["kmip"]
        self.assertEqual(ctx.check_hostname, True)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)


class TestClientOptions(PyMongoTestCase):
    def test_default(self):
        client = MongoClient(connect=False)
        self.addCleanup(client.close)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, None)

        client = MongoClient(auto_encryption_opts=None, connect=False)
        self.addCleanup(client.close)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, None)

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    def test_kwargs(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = MongoClient(auto_encryption_opts=opts, connect=False)
        self.addCleanup(client.close)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, opts)


class EncryptionIntegrationTest(IntegrationTest):
    """Base class for encryption integration tests."""

    @classmethod
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    @client_context.require_version_min(4, 2, -1)
    def setUpClass(cls):
        super().setUpClass()

    def assertEncrypted(self, val):
        self.assertIsInstance(val, Binary)
        self.assertEqual(val.subtype, 6)

    def assertBinaryUUID(self, val):
        self.assertIsInstance(val, Binary)
        self.assertEqual(val.subtype, UUID_SUBTYPE)


# Location of JSON test files.
BASE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "client-side-encryption")
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


class TestClientSimple(EncryptionIntegrationTest):
    def _test_auto_encrypt(self, opts):
        client = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client.close)

        # Create the encrypted field's data key.
        key_vault = create_key_vault(
            self.client.keyvault.datakeys, json_data("custom", "key-document-local.json")
        )
        self.addCleanup(key_vault.drop)

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
        encrypted_coll.insert_one(docs[0])
        encrypted_coll.insert_many(docs[1:3])
        unack = encrypted_coll.with_options(write_concern=WriteConcern(w=0))
        unack.insert_one(docs[3])
        unack.insert_many(docs[4:], ordered=False)
        wait_until(
            lambda: self.db.test.count_documents({}) == len(docs), "insert documents with w=0"
        )

        # Database.command auto decrypts.
        res = client.pymongo_test.command("find", "test", filter={"ssn": "000"})
        decrypted_docs = res["cursor"]["firstBatch"]
        self.assertEqual(decrypted_docs, [{"_id": 0, "ssn": "000"}])

        # Collection.find auto decrypts.
        decrypted_docs = list(encrypted_coll.find())
        self.assertEqual(decrypted_docs, docs)

        # Collection.find auto decrypts getMores.
        decrypted_docs = list(encrypted_coll.find(batch_size=1))
        self.assertEqual(decrypted_docs, docs)

        # Collection.aggregate auto decrypts.
        decrypted_docs = list(encrypted_coll.aggregate([]))
        self.assertEqual(decrypted_docs, docs)

        # Collection.aggregate auto decrypts getMores.
        decrypted_docs = list(encrypted_coll.aggregate([], batchSize=1))
        self.assertEqual(decrypted_docs, docs)

        # Collection.distinct auto decrypts.
        decrypted_ssns = encrypted_coll.distinct("ssn")
        self.assertEqual(set(decrypted_ssns), {d["ssn"] for d in docs})

        # Make sure the field is actually encrypted.
        for encrypted_doc in self.db.test.find():
            self.assertIsInstance(encrypted_doc["_id"], int)
            self.assertEncrypted(encrypted_doc["ssn"])

        # Attempt to encrypt an unencodable object.
        with self.assertRaises(BSONError):
            encrypted_coll.insert_one({"unencodeable": object()})

    def test_auto_encrypt(self):
        # Configure the encrypted field via jsonSchema.
        json_schema = json_data("custom", "schema.json")
        create_with_schema(self.db.test, json_schema)
        self.addCleanup(self.db.test.drop)

        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        self._test_auto_encrypt(opts)

    def test_auto_encrypt_local_schema_map(self):
        # Configure the encrypted field via the local schema_map option.
        schemas = {"pymongo_test.test": json_data("custom", "schema.json")}
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys", schema_map=schemas)

        self._test_auto_encrypt(opts)

    def test_use_after_close(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client.close)

        client.admin.command("ping")
        client.close()
        with self.assertRaisesRegex(InvalidOperation, "Cannot use MongoClient after close"):
            client.admin.command("ping")

    @unittest.skipIf(
        not hasattr(os, "register_at_fork"),
        "register_at_fork not available in this version of Python",
    )
    @unittest.skipIf(
        is_greenthread_patched(),
        "gevent and eventlet do not support POSIX-style forking.",
    )
    def test_fork(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client.close)

        def target():
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                client.admin.command("ping")

        with self.fork(target):
            target()


class TestEncryptedBulkWrite(BulkTestBase, EncryptionIntegrationTest):
    def test_upsert_uuid_standard_encrypt(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client.close)

        options = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
        encrypted_coll = client.pymongo_test.test
        coll = encrypted_coll.with_options(codec_options=options)
        uuids = [uuid.uuid4() for _ in range(3)]
        result = coll.bulk_write(
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


class TestClientMaxWireVersion(IntegrationTest):
    @classmethod
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    def setUpClass(cls):
        super().setUpClass()

    @client_context.require_version_max(4, 0, 99)
    def test_raise_max_wire_version_error(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client.close)
        msg = "Auto-encryption requires a minimum MongoDB version of 4.2"
        with self.assertRaisesRegex(ConfigurationError, msg):
            client.test.test.insert_one({})
        with self.assertRaisesRegex(ConfigurationError, msg):
            client.admin.command("ping")
        with self.assertRaisesRegex(ConfigurationError, msg):
            client.test.test.find_one({})
        with self.assertRaisesRegex(ConfigurationError, msg):
            client.test.test.bulk_write([InsertOne({})])

    def test_raise_unsupported_error(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, "keyvault.datakeys")
        client = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client.close)
        msg = "find_raw_batches does not support auto encryption"
        with self.assertRaisesRegex(InvalidOperation, msg):
            client.test.test.find_raw_batches({})

        msg = "aggregate_raw_batches does not support auto encryption"
        with self.assertRaisesRegex(InvalidOperation, msg):
            client.test.test.aggregate_raw_batches([])

        if client_context.is_mongos:
            msg = "Exhaust cursors are not supported by mongos"
        else:
            msg = "exhaust cursors do not support auto encryption"
        with self.assertRaisesRegex(InvalidOperation, msg):
            next(client.test.test.find(cursor_type=CursorType.EXHAUST))


class TestExplicitSimple(EncryptionIntegrationTest):
    def test_encrypt_decrypt(self):
        client_encryption = ClientEncryption(
            KMS_PROVIDERS, "keyvault.datakeys", client_context.client, OPTS
        )
        self.addCleanup(client_encryption.close)
        # Use standard UUID representation.
        key_vault = client_context.client.keyvault.get_collection("datakeys", codec_options=OPTS)
        self.addCleanup(key_vault.drop)

        # Create the encrypted field's data key.
        key_id = client_encryption.create_data_key("local", key_alt_names=["name"])
        self.assertBinaryUUID(key_id)
        self.assertTrue(key_vault.find_one({"_id": key_id}))

        # Create an unused data key to make sure filtering works.
        unused_key_id = client_encryption.create_data_key("local", key_alt_names=["unused"])
        self.assertBinaryUUID(unused_key_id)
        self.assertTrue(key_vault.find_one({"_id": unused_key_id}))

        doc = {"_id": 0, "ssn": "000"}
        encrypted_ssn = client_encryption.encrypt(
            doc["ssn"], Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=key_id
        )

        # Ensure encryption via key_alt_name for the same key produces the
        # same output.
        encrypted_ssn2 = client_encryption.encrypt(
            doc["ssn"], Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_alt_name="name"
        )
        self.assertEqual(encrypted_ssn, encrypted_ssn2)

        # Test encryption via UUID
        encrypted_ssn3 = client_encryption.encrypt(
            doc["ssn"],
            Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_id=key_id.as_uuid(),
        )
        self.assertEqual(encrypted_ssn, encrypted_ssn3)

        # Test decryption.
        decrypted_ssn = client_encryption.decrypt(encrypted_ssn)
        self.assertEqual(decrypted_ssn, doc["ssn"])

    def test_validation(self):
        client_encryption = ClientEncryption(
            KMS_PROVIDERS, "keyvault.datakeys", client_context.client, OPTS
        )
        self.addCleanup(client_encryption.close)

        msg = "value to decrypt must be a bson.binary.Binary with subtype 6"
        with self.assertRaisesRegex(TypeError, msg):
            client_encryption.decrypt("str")  # type: ignore[arg-type]
        with self.assertRaisesRegex(TypeError, msg):
            client_encryption.decrypt(Binary(b"123"))

        msg = "key_id must be a bson.binary.Binary with subtype 4"
        algo = Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
        with self.assertRaisesRegex(TypeError, msg):
            client_encryption.encrypt("str", algo, key_id=Binary(b"123"))

    def test_bson_errors(self):
        client_encryption = ClientEncryption(
            KMS_PROVIDERS, "keyvault.datakeys", client_context.client, OPTS
        )
        self.addCleanup(client_encryption.close)

        # Attempt to encrypt an unencodable object.
        unencodable_value = object()
        with self.assertRaises(BSONError):
            client_encryption.encrypt(
                unencodable_value,
                Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
                key_id=Binary.from_uuid(uuid.uuid4()),
            )

    def test_codec_options(self):
        with self.assertRaisesRegex(TypeError, "codec_options must be"):
            ClientEncryption(
                KMS_PROVIDERS,
                "keyvault.datakeys",
                client_context.client,
                None,  # type: ignore[arg-type]
            )

        opts = CodecOptions(uuid_representation=UuidRepresentation.JAVA_LEGACY)
        client_encryption_legacy = ClientEncryption(
            KMS_PROVIDERS, "keyvault.datakeys", client_context.client, opts
        )
        self.addCleanup(client_encryption_legacy.close)

        # Create the encrypted field's data key.
        key_id = client_encryption_legacy.create_data_key("local")

        # Encrypt a UUID with JAVA_LEGACY codec options.
        value = uuid.uuid4()
        encrypted_legacy = client_encryption_legacy.encrypt(
            value, Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=key_id
        )
        decrypted_value_legacy = client_encryption_legacy.decrypt(encrypted_legacy)
        self.assertEqual(decrypted_value_legacy, value)

        # Encrypt the same UUID with STANDARD codec options.
        opts = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
        client_encryption = ClientEncryption(
            KMS_PROVIDERS, "keyvault.datakeys", client_context.client, opts
        )
        self.addCleanup(client_encryption.close)
        encrypted_standard = client_encryption.encrypt(
            value, Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=key_id
        )
        decrypted_standard = client_encryption.decrypt(encrypted_standard)
        self.assertEqual(decrypted_standard, value)

        # Test that codec_options is applied during encryption.
        self.assertNotEqual(encrypted_standard, encrypted_legacy)
        # Test that codec_options is applied during decryption.
        self.assertEqual(
            client_encryption_legacy.decrypt(encrypted_standard), Binary.from_uuid(value)
        )
        self.assertNotEqual(client_encryption.decrypt(encrypted_legacy), value)

    def test_close(self):
        client_encryption = ClientEncryption(
            KMS_PROVIDERS, "keyvault.datakeys", client_context.client, OPTS
        )
        client_encryption.close()
        # Close can be called multiple times.
        client_encryption.close()
        algo = Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
        msg = "Cannot use closed ClientEncryption"
        with self.assertRaisesRegex(InvalidOperation, msg):
            client_encryption.create_data_key("local")
        with self.assertRaisesRegex(InvalidOperation, msg):
            client_encryption.encrypt("val", algo, key_alt_name="name")
        with self.assertRaisesRegex(InvalidOperation, msg):
            client_encryption.decrypt(Binary(b"", 6))

    def test_with_statement(self):
        with ClientEncryption(
            KMS_PROVIDERS, "keyvault.datakeys", client_context.client, OPTS
        ) as client_encryption:
            pass
        with self.assertRaisesRegex(InvalidOperation, "Cannot use closed ClientEncryption"):
            client_encryption.create_data_key("local")


# Spec tests
AWS_TEMP_CREDS = {
    "accessKeyId": os.environ.get("CSFLE_AWS_TEMP_ACCESS_KEY_ID", ""),
    "secretAccessKey": os.environ.get("CSFLE_AWS_TEMP_SECRET_ACCESS_KEY", ""),
    "sessionToken": os.environ.get("CSFLE_AWS_TEMP_SESSION_TOKEN", ""),
}

AWS_TEMP_NO_SESSION_CREDS = {
    "accessKeyId": os.environ.get("CSFLE_AWS_TEMP_ACCESS_KEY_ID", ""),
    "secretAccessKey": os.environ.get("CSFLE_AWS_TEMP_SECRET_ACCESS_KEY", ""),
}
KMS_TLS_OPTS = {"kmip": {"tlsCAFile": CA_PEM, "tlsCertificateKeyFile": CLIENT_PEM}}


class TestSpec(SpecRunner):
    @classmethod
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    def setUpClass(cls):
        super().setUpClass()

    def parse_auto_encrypt_opts(self, opts):
        """Parse clientOptions.autoEncryptOpts."""
        opts = camel_to_snake_args(opts)
        kms_providers = opts["kms_providers"]
        if "aws" in kms_providers:
            kms_providers["aws"] = AWS_CREDS
            if not any(AWS_CREDS.values()):
                self.skipTest("AWS environment credentials are not set")
        if "awsTemporary" in kms_providers:
            kms_providers["aws"] = AWS_TEMP_CREDS
            del kms_providers["awsTemporary"]
            if not any(AWS_TEMP_CREDS.values()):
                self.skipTest("AWS Temp environment credentials are not set")
        if "awsTemporaryNoSessionToken" in kms_providers:
            kms_providers["aws"] = AWS_TEMP_NO_SESSION_CREDS
            del kms_providers["awsTemporaryNoSessionToken"]
            if not any(AWS_TEMP_NO_SESSION_CREDS.values()):
                self.skipTest("AWS Temp environment credentials are not set")
        if "azure" in kms_providers:
            kms_providers["azure"] = AZURE_CREDS
            if not any(AZURE_CREDS.values()):
                self.skipTest("Azure environment credentials are not set")
        if "gcp" in kms_providers:
            kms_providers["gcp"] = GCP_CREDS
            if not any(AZURE_CREDS.values()):
                self.skipTest("GCP environment credentials are not set")
        if "kmip" in kms_providers:
            kms_providers["kmip"] = KMIP_CREDS
            opts["kms_tls_options"] = KMS_TLS_OPTS
        if "key_vault_namespace" not in opts:
            opts["key_vault_namespace"] = "keyvault.datakeys"
        if "extra_options" in opts:
            opts.update(camel_to_snake_args(opts.pop("extra_options")))

        opts = dict(opts)
        return AutoEncryptionOpts(**opts)

    def parse_client_options(self, opts):
        """Override clientOptions parsing to support autoEncryptOpts."""
        encrypt_opts = opts.pop("autoEncryptOpts", None)
        if encrypt_opts:
            opts["auto_encryption_opts"] = self.parse_auto_encrypt_opts(encrypt_opts)

        return super().parse_client_options(opts)

    def get_object_name(self, op):
        """Default object is collection."""
        return op.get("object", "collection")

    def maybe_skip_scenario(self, test):
        super().maybe_skip_scenario(test)
        desc = test["description"].lower()
        if (
            "timeoutms applied to listcollections to get collection schema" in desc
            and sys.platform in ("win32", "darwin")
        ):
            self.skipTest("PYTHON-3706 flaky test on Windows/macOS")
        if "type=symbol" in desc:
            self.skipTest("PyMongo does not support the symbol type")

    def setup_scenario(self, scenario_def):
        """Override a test's setup."""
        key_vault_data = scenario_def["key_vault_data"]
        encrypted_fields = scenario_def["encrypted_fields"]
        json_schema = scenario_def["json_schema"]
        data = scenario_def["data"]
        coll = client_context.client.get_database("keyvault", codec_options=OPTS)["datakeys"]
        coll.delete_many({})
        if key_vault_data:
            coll.insert_many(key_vault_data)

        db_name = self.get_scenario_db_name(scenario_def)
        coll_name = self.get_scenario_coll_name(scenario_def)
        db = client_context.client.get_database(db_name, codec_options=OPTS)
        coll = db.drop_collection(coll_name, encrypted_fields=encrypted_fields)
        wc = WriteConcern(w="majority")
        kwargs: Dict[str, Any] = {}
        if json_schema:
            kwargs["validator"] = {"$jsonSchema": json_schema}
            kwargs["codec_options"] = OPTS
        if not data:
            kwargs["write_concern"] = wc
        if encrypted_fields:
            kwargs["encryptedFields"] = encrypted_fields
        db.create_collection(coll_name, **kwargs)
        coll = db[coll_name]
        if data:
            # Load data.
            coll.with_options(write_concern=wc).insert_many(scenario_def["data"])

    def allowable_errors(self, op):
        """Override expected error classes."""
        errors = super().allowable_errors(op)
        # An updateOne test expects encryption to error when no $ operator
        # appears but pymongo raises a client side ValueError in this case.
        if op["name"] == "updateOne":
            errors += (ValueError,)
        return errors


def create_test(scenario_def, test, name):
    @client_context.require_test_commands
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


test_creator = SpecTestCreator(create_test, TestSpec, os.path.join(SPEC_PATH, "legacy"))
test_creator.create_tests()


if _HAVE_PYMONGOCRYPT:
    globals().update(
        generate_test_classes(
            os.path.join(SPEC_PATH, "unified"),
            module=__name__,
        )
    )

# Prose Tests
ALL_KMS_PROVIDERS = {
    "aws": AWS_CREDS,
    "azure": AZURE_CREDS,
    "gcp": GCP_CREDS,
    "kmip": KMIP_CREDS,
    "local": {"key": LOCAL_MASTER_KEY},
}

LOCAL_KEY_ID = Binary(base64.b64decode(b"LOCALAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
AWS_KEY_ID = Binary(base64.b64decode(b"AWSAAAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
AZURE_KEY_ID = Binary(base64.b64decode(b"AZUREAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
GCP_KEY_ID = Binary(base64.b64decode(b"GCPAAAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
KMIP_KEY_ID = Binary(base64.b64decode(b"KMIPAAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)


def create_with_schema(coll, json_schema):
    """Create and return a Collection with a jsonSchema."""
    coll.with_options(write_concern=WriteConcern(w="majority")).drop()
    return coll.database.create_collection(
        coll.name, validator={"$jsonSchema": json_schema}, codec_options=OPTS
    )


def create_key_vault(vault, *data_keys):
    """Create the key vault collection with optional data keys."""
    vault = vault.with_options(write_concern=WriteConcern(w="majority"), codec_options=OPTS)
    vault.drop()
    if data_keys:
        vault.insert_many(data_keys)
    vault.create_index(
        "keyAltNames",
        unique=True,
        partialFilterExpression={"keyAltNames": {"$exists": True}},
    )
    return vault


class TestDataKeyDoubleEncryption(EncryptionIntegrationTest):
    client_encrypted: MongoClient
    client_encryption: ClientEncryption
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

    @classmethod
    @unittest.skipUnless(
        any([all(AWS_CREDS.values()), all(AZURE_CREDS.values()), all(GCP_CREDS.values())]),
        "No environment credentials are set",
    )
    def setUpClass(cls):
        super().setUpClass()
        cls.listener = OvertCommandListener()
        cls.client = rs_or_single_client(event_listeners=[cls.listener])
        cls.client.db.coll.drop()
        cls.vault = create_key_vault(cls.client.keyvault.datakeys)

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
            cls.KMS_PROVIDERS, "keyvault.datakeys", schema_map=schemas, kms_tls_options=KMS_TLS_OPTS
        )
        cls.client_encrypted = rs_or_single_client(
            auto_encryption_opts=opts, uuidRepresentation="standard"
        )
        cls.client_encryption = ClientEncryption(
            cls.KMS_PROVIDERS, "keyvault.datakeys", cls.client, OPTS, kms_tls_options=KMS_TLS_OPTS
        )

    @classmethod
    def tearDownClass(cls):
        cls.vault.drop()
        cls.client.close()
        cls.client_encrypted.close()
        cls.client_encryption.close()

    def setUp(self):
        self.listener.reset()

    def run_test(self, provider_name):
        # Create data key.
        master_key: Any = self.MASTER_KEYS[provider_name]
        datakey_id = self.client_encryption.create_data_key(
            provider_name, master_key=master_key, key_alt_names=[f"{provider_name}_altname"]
        )
        self.assertBinaryUUID(datakey_id)
        cmd = self.listener.started_events[-1]
        self.assertEqual("insert", cmd.command_name)
        self.assertEqual({"w": "majority"}, cmd.command.get("writeConcern"))
        docs = list(self.vault.find({"_id": datakey_id}))
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["masterKey"]["provider"], provider_name)

        # Encrypt by key_id.
        encrypted = self.client_encryption.encrypt(
            f"hello {provider_name}",
            Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_id=datakey_id,
        )
        self.assertEncrypted(encrypted)
        self.client_encrypted.db.coll.insert_one({"_id": provider_name, "value": encrypted})
        doc_decrypted = self.client_encrypted.db.coll.find_one({"_id": provider_name})
        self.assertEqual(doc_decrypted["value"], f"hello {provider_name}")  # type: ignore

        # Encrypt by key_alt_name.
        encrypted_altname = self.client_encryption.encrypt(
            f"hello {provider_name}",
            Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_alt_name=f"{provider_name}_altname",
        )
        self.assertEqual(encrypted_altname, encrypted)

        # Explicitly encrypting an auto encrypted field.
        with self.assertRaisesRegex(EncryptionError, r"encrypt element of type"):
            self.client_encrypted.db.coll.insert_one({"encrypted_placeholder": encrypted})

    def test_data_key_local(self):
        self.run_test("local")

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def test_data_key_aws(self):
        self.run_test("aws")

    @unittest.skipUnless(any(AZURE_CREDS.values()), "Azure environment credentials are not set")
    def test_data_key_azure(self):
        self.run_test("azure")

    @unittest.skipUnless(any(GCP_CREDS.values()), "GCP environment credentials are not set")
    def test_data_key_gcp(self):
        self.run_test("gcp")

    def test_data_key_kmip(self):
        self.run_test("kmip")


class TestExternalKeyVault(EncryptionIntegrationTest):
    @staticmethod
    def kms_providers():
        return {"local": {"key": LOCAL_MASTER_KEY}}

    def _test_external_key_vault(self, with_external_key_vault):
        self.client.db.coll.drop()
        vault = create_key_vault(
            self.client.keyvault.datakeys,
            json_data("corpus", "corpus-key-local.json"),
            json_data("corpus", "corpus-key-aws.json"),
        )
        self.addCleanup(vault.drop)

        # Configure the encrypted field via the local schema_map option.
        schemas = {"db.coll": json_data("external", "external-schema.json")}
        if with_external_key_vault:
            key_vault_client = rs_or_single_client(username="fake-user", password="fake-pwd")
            self.addCleanup(key_vault_client.close)
        else:
            key_vault_client = client_context.client
        opts = AutoEncryptionOpts(
            self.kms_providers(),
            "keyvault.datakeys",
            schema_map=schemas,
            key_vault_client=key_vault_client,
        )

        client_encrypted = rs_or_single_client(
            auto_encryption_opts=opts, uuidRepresentation="standard"
        )
        self.addCleanup(client_encrypted.close)

        client_encryption = ClientEncryption(
            self.kms_providers(), "keyvault.datakeys", key_vault_client, OPTS
        )
        self.addCleanup(client_encryption.close)

        if with_external_key_vault:
            # Authentication error.
            with self.assertRaises(EncryptionError) as ctx:
                client_encrypted.db.coll.insert_one({"encrypted": "test"})
            # AuthenticationFailed error.
            self.assertIsInstance(ctx.exception.cause, OperationFailure)
            self.assertEqual(ctx.exception.cause.code, 18)
        else:
            client_encrypted.db.coll.insert_one({"encrypted": "test"})

        if with_external_key_vault:
            # Authentication error.
            with self.assertRaises(EncryptionError) as ctx:
                client_encryption.encrypt(
                    "test",
                    Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
                    key_id=LOCAL_KEY_ID,
                )
            # AuthenticationFailed error.
            self.assertIsInstance(ctx.exception.cause, OperationFailure)
            self.assertEqual(ctx.exception.cause.code, 18)
        else:
            client_encryption.encrypt(
                "test", Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=LOCAL_KEY_ID
            )

    def test_external_key_vault_1(self):
        self._test_external_key_vault(True)

    def test_external_key_vault_2(self):
        self._test_external_key_vault(False)


class TestViews(EncryptionIntegrationTest):
    @staticmethod
    def kms_providers():
        return {"local": {"key": LOCAL_MASTER_KEY}}

    def test_views_are_prohibited(self):
        self.client.db.view.drop()
        self.client.db.create_collection("view", viewOn="coll")
        self.addCleanup(self.client.db.view.drop)

        opts = AutoEncryptionOpts(self.kms_providers(), "keyvault.datakeys")
        client_encrypted = rs_or_single_client(
            auto_encryption_opts=opts, uuidRepresentation="standard"
        )
        self.addCleanup(client_encrypted.close)

        with self.assertRaisesRegex(EncryptionError, "cannot auto encrypt a view"):
            client_encrypted.db.view.insert_one({})


class TestCorpus(EncryptionIntegrationTest):
    @classmethod
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def setUpClass(cls):
        super().setUpClass()

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

    def _test_corpus(self, opts):
        # Drop and create the collection 'db.coll' with jsonSchema.
        coll = create_with_schema(
            self.client.db.coll, self.fix_up_schema(json_data("corpus", "corpus-schema.json"))
        )
        self.addCleanup(coll.drop)

        vault = create_key_vault(
            self.client.keyvault.datakeys,
            json_data("corpus", "corpus-key-local.json"),
            json_data("corpus", "corpus-key-aws.json"),
            json_data("corpus", "corpus-key-azure.json"),
            json_data("corpus", "corpus-key-gcp.json"),
            json_data("corpus", "corpus-key-kmip.json"),
        )
        self.addCleanup(vault.drop)

        client_encrypted = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client_encrypted.close)

        client_encryption = ClientEncryption(
            self.kms_providers(),
            "keyvault.datakeys",
            client_context.client,
            OPTS,
            kms_tls_options=KMS_TLS_OPTS,
        )
        self.addCleanup(client_encryption.close)

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
                    encrypted_val = client_encryption.encrypt(
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

        client_encrypted.db.coll.insert_one(corpus_copied)
        corpus_decrypted = client_encrypted.db.coll.find_one()
        self.assertEqual(corpus_decrypted, corpus)

        corpus_encrypted_expected = self.fix_up_curpus_encrypted(
            json_data("corpus", "corpus-encrypted.json"), corpus
        )
        corpus_encrypted_actual = coll.find_one()
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
                decrypt_actual = client_encryption.decrypt(value["value"])
                decrypt_expected = client_encryption.decrypt(
                    corpus_encrypted_expected[key]["value"]
                )
                self.assertEqual(decrypt_actual, decrypt_expected, key)
            else:
                self.assertEqual(value["value"], corpus[key]["value"], key)

    def test_corpus(self):
        opts = AutoEncryptionOpts(
            self.kms_providers(), "keyvault.datakeys", kms_tls_options=KMS_TLS_OPTS
        )
        self._test_corpus(opts)

    def test_corpus_local_schema(self):
        # Configure the encrypted field via the local schema_map option.
        schemas = {"db.coll": self.fix_up_schema(json_data("corpus", "corpus-schema.json"))}
        opts = AutoEncryptionOpts(
            self.kms_providers(),
            "keyvault.datakeys",
            schema_map=schemas,
            kms_tls_options=KMS_TLS_OPTS,
        )
        self._test_corpus(opts)


_2_MiB = 2097152
_16_MiB = 16777216


class TestBsonSizeBatches(EncryptionIntegrationTest):
    """Prose tests for BSON size limits and batch splitting."""

    coll: Collection
    coll_encrypted: Collection
    client_encrypted: MongoClient
    listener: OvertCommandListener

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        db = client_context.client.db
        cls.coll = db.coll
        cls.coll.drop()
        # Configure the encrypted 'db.coll' collection via jsonSchema.
        json_schema = json_data("limits", "limits-schema.json")
        db.create_collection(
            "coll",
            validator={"$jsonSchema": json_schema},
            codec_options=OPTS,
            write_concern=WriteConcern(w="majority"),
        )

        # Create the key vault.
        coll = client_context.client.get_database(
            "keyvault", write_concern=WriteConcern(w="majority"), codec_options=OPTS
        )["datakeys"]
        coll.drop()
        coll.insert_one(json_data("limits", "limits-key.json"))

        opts = AutoEncryptionOpts({"local": {"key": LOCAL_MASTER_KEY}}, "keyvault.datakeys")
        cls.listener = OvertCommandListener()
        cls.client_encrypted = rs_or_single_client(
            auto_encryption_opts=opts, event_listeners=[cls.listener]
        )
        cls.coll_encrypted = cls.client_encrypted.db.coll

    @classmethod
    def tearDownClass(cls):
        cls.coll_encrypted.drop()
        cls.client_encrypted.close()
        super().tearDownClass()

    def test_01_insert_succeeds_under_2MiB(self):
        doc = {"_id": "over_2mib_under_16mib", "unencrypted": "a" * _2_MiB}
        self.coll_encrypted.insert_one(doc)

        # Same with bulk_write.
        doc["_id"] = "over_2mib_under_16mib_bulk"
        self.coll_encrypted.bulk_write([InsertOne(doc)])

    def test_02_insert_succeeds_over_2MiB_post_encryption(self):
        doc = {"_id": "encryption_exceeds_2mib", "unencrypted": "a" * ((2**21) - 2000)}
        doc.update(json_data("limits", "limits-doc.json"))
        self.coll_encrypted.insert_one(doc)

        # Same with bulk_write.
        doc["_id"] = "encryption_exceeds_2mib_bulk"
        self.coll_encrypted.bulk_write([InsertOne(doc)])

    def test_03_bulk_batch_split(self):
        doc1 = {"_id": "over_2mib_1", "unencrypted": "a" * _2_MiB}
        doc2 = {"_id": "over_2mib_2", "unencrypted": "a" * _2_MiB}
        self.listener.reset()
        self.coll_encrypted.bulk_write([InsertOne(doc1), InsertOne(doc2)])
        self.assertEqual(self.listener.started_command_names(), ["insert", "insert"])

    def test_04_bulk_batch_split(self):
        limits_doc = json_data("limits", "limits-doc.json")
        doc1 = {"_id": "encryption_exceeds_2mib_1", "unencrypted": "a" * (_2_MiB - 2000)}
        doc1.update(limits_doc)
        doc2 = {"_id": "encryption_exceeds_2mib_2", "unencrypted": "a" * (_2_MiB - 2000)}
        doc2.update(limits_doc)
        self.listener.reset()
        self.coll_encrypted.bulk_write([InsertOne(doc1), InsertOne(doc2)])
        self.assertEqual(self.listener.started_command_names(), ["insert", "insert"])

    def test_05_insert_succeeds_just_under_16MiB(self):
        doc = {"_id": "under_16mib", "unencrypted": "a" * (_16_MiB - 2000)}
        self.coll_encrypted.insert_one(doc)

        # Same with bulk_write.
        doc["_id"] = "under_16mib_bulk"
        self.coll_encrypted.bulk_write([InsertOne(doc)])

    def test_06_insert_fails_over_16MiB(self):
        limits_doc = json_data("limits", "limits-doc.json")
        doc = {"_id": "encryption_exceeds_16mib", "unencrypted": "a" * (_16_MiB - 2000)}
        doc.update(limits_doc)

        with self.assertRaisesRegex(WriteError, "object to insert too large"):
            self.coll_encrypted.insert_one(doc)

        # Same with bulk_write.
        doc["_id"] = "encryption_exceeds_16mib_bulk"
        with self.assertRaises(BulkWriteError) as ctx:
            self.coll_encrypted.bulk_write([InsertOne(doc)])
        err = ctx.exception.details["writeErrors"][0]
        self.assertEqual(2, err["code"])
        self.assertIn("object to insert too large", err["errmsg"])


class TestCustomEndpoint(EncryptionIntegrationTest):
    """Prose tests for creating data keys with a custom endpoint."""

    @classmethod
    @unittest.skipUnless(
        any([all(AWS_CREDS.values()), all(AZURE_CREDS.values()), all(GCP_CREDS.values())]),
        "No environment credentials are set",
    )
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        kms_providers = {
            "aws": AWS_CREDS,
            "azure": AZURE_CREDS,
            "gcp": GCP_CREDS,
            "kmip": KMIP_CREDS,
        }
        self.client_encryption = ClientEncryption(
            kms_providers=kms_providers,
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=client_context.client,
            codec_options=OPTS,
            kms_tls_options=KMS_TLS_OPTS,
        )

        kms_providers_invalid = copy.deepcopy(kms_providers)
        kms_providers_invalid["azure"]["identityPlatformEndpoint"] = "doesnotexist.invalid:443"
        kms_providers_invalid["gcp"]["endpoint"] = "doesnotexist.invalid:443"
        kms_providers_invalid["kmip"]["endpoint"] = "doesnotexist.local:5698"
        self.client_encryption_invalid = ClientEncryption(
            kms_providers=kms_providers_invalid,
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=client_context.client,
            codec_options=OPTS,
            kms_tls_options=KMS_TLS_OPTS,
        )
        self._kmip_host_error = None
        self._invalid_host_error = None

    def tearDown(self):
        self.client_encryption.close()
        self.client_encryption_invalid.close()

    def run_test_expected_success(self, provider_name, master_key):
        data_key_id = self.client_encryption.create_data_key(provider_name, master_key=master_key)
        encrypted = self.client_encryption.encrypt(
            "test", Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=data_key_id
        )
        self.assertEqual("test", self.client_encryption.decrypt(encrypted))

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def test_01_aws_region_key(self):
        self.run_test_expected_success(
            "aws",
            {
                "region": "us-east-1",
                "key": (
                    "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                ),
            },
        )

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def test_02_aws_region_key_endpoint(self):
        self.run_test_expected_success(
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
    def test_03_aws_region_key_endpoint_port(self):
        self.run_test_expected_success(
            "aws",
            {
                "region": "us-east-1",
                "key": (
                    "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                ),
                "endpoint": "kms.us-east-1.amazonaws.com:443",
            },
        )

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def test_04_aws_endpoint_invalid_port(self):
        master_key = {
            "region": "us-east-1",
            "key": ("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"),
            "endpoint": "kms.us-east-1.amazonaws.com:12345",
        }
        with self.assertRaisesRegex(EncryptionError, "kms.us-east-1.amazonaws.com:12345") as ctx:
            self.client_encryption.create_data_key("aws", master_key=master_key)
        self.assertIsInstance(ctx.exception.cause, AutoReconnect)

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def test_05_aws_endpoint_wrong_region(self):
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
            self.client_encryption.create_data_key("aws", master_key=master_key)

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def test_06_aws_endpoint_invalid_host(self):
        master_key = {
            "region": "us-east-1",
            "key": ("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"),
            "endpoint": "doesnotexist.invalid",
        }
        with self.assertRaisesRegex(EncryptionError, self.invalid_host_error):
            self.client_encryption.create_data_key("aws", master_key=master_key)

    @unittest.skipUnless(any(AZURE_CREDS.values()), "Azure environment credentials are not set")
    def test_07_azure(self):
        master_key = {
            "keyVaultEndpoint": "key-vault-csfle.vault.azure.net",
            "keyName": "key-name-csfle",
        }
        self.run_test_expected_success("azure", master_key)

        # The full error should be something like:
        # "[Errno 8] nodename nor servname provided, or not known"
        with self.assertRaisesRegex(EncryptionError, self.invalid_host_error):
            self.client_encryption_invalid.create_data_key("azure", master_key=master_key)

    @unittest.skipUnless(any(GCP_CREDS.values()), "GCP environment credentials are not set")
    def test_08_gcp_valid_endpoint(self):
        master_key = {
            "projectId": "devprod-drivers",
            "location": "global",
            "keyRing": "key-ring-csfle",
            "keyName": "key-name-csfle",
            "endpoint": "cloudkms.googleapis.com:443",
        }
        self.run_test_expected_success("gcp", master_key)

        # The full error should be something like:
        # "[Errno 8] nodename nor servname provided, or not known"
        with self.assertRaisesRegex(EncryptionError, self.invalid_host_error):
            self.client_encryption_invalid.create_data_key("gcp", master_key=master_key)

    @unittest.skipUnless(any(GCP_CREDS.values()), "GCP environment credentials are not set")
    def test_09_gcp_invalid_endpoint(self):
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
            self.client_encryption.create_data_key("gcp", master_key=master_key)

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

    def test_10_kmip_invalid_endpoint(self):
        key = {"keyId": "1"}
        self.run_test_expected_success("kmip", key)
        with self.assertRaisesRegex(EncryptionError, self.kmip_host_error):
            self.client_encryption_invalid.create_data_key("kmip", key)

    def test_11_kmip_master_key_endpoint(self):
        key = {"keyId": "1", "endpoint": KMIP_CREDS["endpoint"]}
        self.run_test_expected_success("kmip", key)
        # Override invalid endpoint:
        data_key_id = self.client_encryption_invalid.create_data_key("kmip", master_key=key)
        encrypted = self.client_encryption_invalid.encrypt(
            "test", Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_id=data_key_id
        )
        self.assertEqual("test", self.client_encryption_invalid.decrypt(encrypted))

    def test_12_kmip_master_key_invalid_endpoint(self):
        key = {"keyId": "1", "endpoint": "doesnotexist.local:5698"}
        with self.assertRaisesRegex(EncryptionError, self.kmip_host_error):
            self.client_encryption.create_data_key("kmip", key)


class AzureGCPEncryptionTestMixin:
    DEK = None
    KMS_PROVIDER_MAP = None
    KEYVAULT_DB = "keyvault"
    KEYVAULT_COLL = "datakeys"
    client: MongoClient

    def setUp(self):
        keyvault = self.client.get_database(self.KEYVAULT_DB).get_collection(self.KEYVAULT_COLL)
        create_key_vault(keyvault, self.DEK)

    def _test_explicit(self, expectation):
        client_encryption = ClientEncryption(
            self.KMS_PROVIDER_MAP,  # type: ignore[arg-type]
            ".".join([self.KEYVAULT_DB, self.KEYVAULT_COLL]),
            client_context.client,
            OPTS,
        )
        self.addCleanup(client_encryption.close)

        ciphertext = client_encryption.encrypt(
            "string0",
            algorithm=Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_id=self.DEK["_id"],
        )

        self.assertEqual(bytes(ciphertext), base64.b64decode(expectation))
        self.assertEqual(client_encryption.decrypt(ciphertext), "string0")

    def _test_automatic(self, expectation_extjson, payload):
        encrypted_db = "db"
        encrypted_coll = "coll"
        keyvault_namespace = ".".join([self.KEYVAULT_DB, self.KEYVAULT_COLL])

        encryption_opts = AutoEncryptionOpts(
            self.KMS_PROVIDER_MAP,  # type: ignore[arg-type]
            keyvault_namespace,
            schema_map=self.SCHEMA_MAP,
        )

        insert_listener = AllowListEventListener("insert")
        client = rs_or_single_client(
            auto_encryption_opts=encryption_opts, event_listeners=[insert_listener]
        )
        self.addCleanup(client.close)

        coll = client.get_database(encrypted_db).get_collection(
            encrypted_coll, codec_options=OPTS, write_concern=WriteConcern("majority")
        )
        coll.drop()

        expected_document = json_util.loads(expectation_extjson, json_options=JSON_OPTS)

        coll.insert_one(payload)
        event = insert_listener.started_events[0]
        inserted_doc = event.command["documents"][0]

        for key, value in expected_document.items():
            self.assertEqual(value, inserted_doc[key])

        output_doc = coll.find_one({})
        for key, value in payload.items():
            self.assertEqual(output_doc[key], value)


class TestAzureEncryption(AzureGCPEncryptionTestMixin, EncryptionIntegrationTest):
    @classmethod
    @unittest.skipUnless(any(AZURE_CREDS.values()), "Azure environment credentials are not set")
    def setUpClass(cls):
        cls.KMS_PROVIDER_MAP = {"azure": AZURE_CREDS}
        cls.DEK = json_data(BASE, "custom", "azure-dek.json")
        cls.SCHEMA_MAP = json_data(BASE, "custom", "azure-gcp-schema.json")
        super().setUpClass()

    def test_explicit(self):
        return self._test_explicit(
            "AQGVERPgAAAAAAAAAAAAAAAC5DbBSwPwfSlBrDtRuglvNvCXD1KzDuCKY2P+4bRFtHDjpTOE2XuytPAUaAbXf1orsPq59PVZmsbTZbt2CB8qaQ=="
        )

    def test_automatic(self):
        expected_document_extjson = textwrap.dedent(
            """
        {"secret_azure": {
            "$binary": {
                "base64": "AQGVERPgAAAAAAAAAAAAAAAC5DbBSwPwfSlBrDtRuglvNvCXD1KzDuCKY2P+4bRFtHDjpTOE2XuytPAUaAbXf1orsPq59PVZmsbTZbt2CB8qaQ==",
                "subType": "06"}
        }}"""
        )
        return self._test_automatic(expected_document_extjson, {"secret_azure": "string0"})


class TestGCPEncryption(AzureGCPEncryptionTestMixin, EncryptionIntegrationTest):
    @classmethod
    @unittest.skipUnless(any(GCP_CREDS.values()), "GCP environment credentials are not set")
    def setUpClass(cls):
        cls.KMS_PROVIDER_MAP = {"gcp": GCP_CREDS}
        cls.DEK = json_data(BASE, "custom", "gcp-dek.json")
        cls.SCHEMA_MAP = json_data(BASE, "custom", "azure-gcp-schema.json")
        super().setUpClass()

    def test_explicit(self):
        return self._test_explicit(
            "ARgj/gAAAAAAAAAAAAAAAAACwFd+Y5Ojw45GUXNvbcIpN9YkRdoHDHkR4kssdn0tIMKlDQOLFkWFY9X07IRlXsxPD8DcTiKnl6XINK28vhcGlg=="
        )

    def test_automatic(self):
        expected_document_extjson = textwrap.dedent(
            """
        {"secret_gcp": {
            "$binary": {
                "base64": "ARgj/gAAAAAAAAAAAAAAAAACwFd+Y5Ojw45GUXNvbcIpN9YkRdoHDHkR4kssdn0tIMKlDQOLFkWFY9X07IRlXsxPD8DcTiKnl6XINK28vhcGlg==",
                "subType": "06"}
        }}"""
        )
        return self._test_automatic(expected_document_extjson, {"secret_gcp": "string0"})


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.rst#deadlock-tests
class TestDeadlockProse(EncryptionIntegrationTest):
    def setUp(self):
        self.client_test = rs_or_single_client(
            maxPoolSize=1, readConcernLevel="majority", w="majority", uuidRepresentation="standard"
        )
        self.addCleanup(self.client_test.close)

        self.client_keyvault_listener = OvertCommandListener()
        self.client_keyvault = rs_or_single_client(
            maxPoolSize=1,
            readConcernLevel="majority",
            w="majority",
            event_listeners=[self.client_keyvault_listener],
        )
        self.addCleanup(self.client_keyvault.close)

        self.client_test.keyvault.datakeys.drop()
        self.client_test.db.coll.drop()
        self.client_test.keyvault.datakeys.insert_one(json_data("external", "external-key.json"))
        _ = self.client_test.db.create_collection(
            "coll",
            validator={"$jsonSchema": json_data("external", "external-schema.json")},
            codec_options=OPTS,
        )

        client_encryption = ClientEncryption(
            kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=self.client_test,
            codec_options=OPTS,
        )
        self.ciphertext = client_encryption.encrypt(
            "string0", Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic, key_alt_name="local"
        )
        client_encryption.close()

        self.client_listener = OvertCommandListener()
        self.topology_listener = TopologyEventListener()
        self.optargs = ({"local": {"key": LOCAL_MASTER_KEY}}, "keyvault.datakeys")

    def _run_test(self, max_pool_size, auto_encryption_opts):
        client_encrypted = rs_or_single_client(
            readConcernLevel="majority",
            w="majority",
            maxPoolSize=max_pool_size,
            auto_encryption_opts=auto_encryption_opts,
            event_listeners=[self.client_listener, self.topology_listener],
        )

        if auto_encryption_opts._bypass_auto_encryption is True:
            self.client_test.db.coll.insert_one({"_id": 0, "encrypted": self.ciphertext})
        elif auto_encryption_opts._bypass_auto_encryption is False:
            client_encrypted.db.coll.insert_one({"_id": 0, "encrypted": "string0"})
        else:
            raise RuntimeError("bypass_auto_encryption must be a bool")

        result = client_encrypted.db.coll.find_one({"_id": 0})
        self.assertEqual(result, {"_id": 0, "encrypted": "string0"})

        self.addCleanup(client_encrypted.close)

    def test_case_1(self):
        self._run_test(
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

    def test_case_2(self):
        self._run_test(
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

    def test_case_3(self):
        self._run_test(
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

    def test_case_4(self):
        self._run_test(
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

    def test_case_5(self):
        self._run_test(
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

    def test_case_6(self):
        self._run_test(
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

    def test_case_7(self):
        self._run_test(
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

    def test_case_8(self):
        self._run_test(
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


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.rst#14-decryption-events
class TestDecryptProse(EncryptionIntegrationTest):
    def setUp(self):
        self.client = client_context.client
        self.client.db.drop_collection("decryption_events")
        create_key_vault(self.client.keyvault.datakeys)
        kms_providers_map = {"local": {"key": LOCAL_MASTER_KEY}}

        self.client_encryption = ClientEncryption(
            kms_providers_map, "keyvault.datakeys", self.client, CodecOptions()
        )
        keyID = self.client_encryption.create_data_key("local")
        self.cipher_text = self.client_encryption.encrypt(
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
        self.encrypted_client = rs_or_single_client(
            auto_encryption_opts=opts, retryReads=False, event_listeners=[self.listener]
        )
        self.addCleanup(self.encrypted_client.close)

    def test_01_command_error(self):
        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"errorCode": 123, "failCommands": ["aggregate"]},
            }
        ):
            with self.assertRaises(OperationFailure):
                self.encrypted_client.db.decryption_events.aggregate([])
        self.assertEqual(len(self.listener.failed_events), 1)
        for event in self.listener.failed_events:
            self.assertEqual(event.failure["code"], 123)

    def test_02_network_error(self):
        with self.fail_point(
            {
                "mode": {"times": 1},
                "data": {"errorCode": 123, "closeConnection": True, "failCommands": ["aggregate"]},
            }
        ):
            with self.assertRaises(AutoReconnect):
                self.encrypted_client.db.decryption_events.aggregate([])
        self.assertEqual(len(self.listener.failed_events), 1)
        self.assertEqual(self.listener.failed_events[0].command_name, "aggregate")

    def test_03_decrypt_error(self):
        self.encrypted_client.db.decryption_events.insert_one(
            {"encrypted": self.malformed_cipher_text}
        )
        with self.assertRaises(EncryptionError):
            next(self.encrypted_client.db.decryption_events.aggregate([]))
        event = self.listener.succeeded_events[0]
        self.assertEqual(len(self.listener.failed_events), 0)
        self.assertEqual(
            event.reply["cursor"]["firstBatch"][0]["encrypted"], self.malformed_cipher_text
        )

    def test_04_decrypt_success(self):
        self.encrypted_client.db.decryption_events.insert_one({"encrypted": self.cipher_text})
        next(self.encrypted_client.db.decryption_events.aggregate([]))
        event = self.listener.succeeded_events[0]
        self.assertEqual(len(self.listener.failed_events), 0)
        self.assertEqual(event.reply["cursor"]["firstBatch"][0]["encrypted"], self.cipher_text)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.rst#bypass-spawning-mongocryptd
class TestBypassSpawningMongocryptdProse(EncryptionIntegrationTest):
    @unittest.skipIf(
        os.environ.get("TEST_CRYPT_SHARED"),
        "this prose test does not work when crypt_shared is on a system dynamic "
        "library search path.",
    )
    def test_mongocryptd_bypass_spawn(self):
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
        client_encrypted = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client_encrypted.close)
        with self.assertRaisesRegex(EncryptionError, "Timeout"):
            client_encrypted.db.coll.insert_one({"encrypted": "test"})

    def test_bypassAutoEncryption(self):
        opts = AutoEncryptionOpts(
            {"local": {"key": LOCAL_MASTER_KEY}},
            "keyvault.datakeys",
            bypass_auto_encryption=True,
            mongocryptd_spawn_args=[
                "--pidfilepath=bypass-spawning-mongocryptd.pid",
                "--port=27027",
            ],
        )
        client_encrypted = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client_encrypted.close)
        client_encrypted.db.coll.insert_one({"unencrypted": "test"})
        # Validate that mongocryptd was not spawned:
        mongocryptd_client = MongoClient("mongodb://localhost:27027/?serverSelectionTimeoutMS=500")
        with self.assertRaises(ServerSelectionTimeoutError):
            mongocryptd_client.admin.command("ping")

    @unittest.skipUnless(os.environ.get("TEST_CRYPT_SHARED"), "crypt_shared lib is not installed")
    def test_via_loading_shared_library(self):
        create_key_vault(
            client_context.client.keyvault.datakeys, json_data("external", "external-key.json")
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
        client_encrypted = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client_encrypted.close)
        client_encrypted.db.coll.drop()
        client_encrypted.db.coll.insert_one({"encrypted": "test"})
        self.assertEncrypted(client_context.client.db.coll.find_one({})["encrypted"])
        no_mongocryptd_client = MongoClient(
            host="mongodb://localhost:47021/db?serverSelectionTimeoutMS=1000"
        )
        self.addCleanup(no_mongocryptd_client.close)
        with self.assertRaises(ServerSelectionTimeoutError):
            no_mongocryptd_client.db.command("ping")

    # https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.rst#20-bypass-creating-mongocryptd-client-when-shared-library-is-loaded
    @unittest.skipUnless(os.environ.get("TEST_CRYPT_SHARED"), "crypt_shared lib is not installed")
    def test_client_via_loading_shared_library(self):
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
        create_key_vault(
            client_context.client.keyvault.datakeys, json_data("external", "external-key.json")
        )
        schemas = {"db.coll": json_data("external", "external-schema.json")}
        opts = AutoEncryptionOpts(
            kms_providers={"local": {"key": LOCAL_MASTER_KEY}},
            key_vault_namespace="keyvault.datakeys",
            schema_map=schemas,
            mongocryptd_uri="mongodb://localhost:47021",
            crypt_shared_lib_required=False,
        )
        client_encrypted = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client_encrypted.close)
        client_encrypted.db.coll.drop()
        client_encrypted.db.coll.insert_one({"encrypted": "test"})
        server.shutdown()
        listener_t.join()
        self.assertFalse(connection_established, "a connection was established on port 47021")


# https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#kms-tls-tests
class TestKmsTLSProse(EncryptionIntegrationTest):
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def setUp(self):
        super().setUp()
        self.patch_system_certs(CA_PEM)
        self.client_encrypted = ClientEncryption(
            {"aws": AWS_CREDS}, "keyvault.datakeys", self.client, OPTS
        )
        self.addCleanup(self.client_encrypted.close)

    def test_invalid_kms_certificate_expired(self):
        key = {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
            "endpoint": "mongodb://127.0.0.1:9000",
        }
        # Some examples:
        # certificate verify failed: certificate has expired (_ssl.c:1129)
        # amazon1-2018 Python 3.6: certificate verify failed (_ssl.c:852)
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            self.client_encrypted.create_data_key("aws", master_key=key)

    def test_invalid_hostname_in_kms_certificate(self):
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
            self.client_encrypted.create_data_key("aws", master_key=key)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.rst#kms-tls-options-tests
class TestKmsTLSOptions(EncryptionIntegrationTest):
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def setUp(self):
        super().setUp()
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
        self.client_encryption_no_client_cert = ClientEncryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts_ca_only
        )
        self.addCleanup(self.client_encryption_no_client_cert.close)
        # 2, same providers as above but with tlsCertificateKeyFile.
        kms_tls_opts = copy.deepcopy(kms_tls_opts_ca_only)
        for p in kms_tls_opts:
            kms_tls_opts[p]["tlsCertificateKeyFile"] = CLIENT_PEM
        self.client_encryption_with_tls = ClientEncryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts
        )
        self.addCleanup(self.client_encryption_with_tls.close)
        # 3, update endpoints to expired host.
        providers: dict = copy.deepcopy(providers)
        providers["azure"]["identityPlatformEndpoint"] = "127.0.0.1:9000"
        providers["gcp"]["endpoint"] = "127.0.0.1:9000"
        providers["kmip"]["endpoint"] = "127.0.0.1:9000"
        self.client_encryption_expired = ClientEncryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts_ca_only
        )
        self.addCleanup(self.client_encryption_expired.close)
        # 3, update endpoints to invalid host.
        providers: dict = copy.deepcopy(providers)
        providers["azure"]["identityPlatformEndpoint"] = "127.0.0.1:9001"
        providers["gcp"]["endpoint"] = "127.0.0.1:9001"
        providers["kmip"]["endpoint"] = "127.0.0.1:9001"
        self.client_encryption_invalid_hostname = ClientEncryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts_ca_only
        )
        self.addCleanup(self.client_encryption_invalid_hostname.close)
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
        self.client_encryption_with_names = ClientEncryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=kms_tls_opts_4
        )

    def test_01_aws(self):
        key = {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
            "endpoint": "127.0.0.1:9002",
        }
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            self.client_encryption_no_client_cert.create_data_key("aws", key)
        # "parse error" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "parse error"):
            self.client_encryption_with_tls.create_data_key("aws", key)
        # Some examples:
        # certificate verify failed: certificate has expired (_ssl.c:1129)
        # amazon1-2018 Python 3.6: certificate verify failed (_ssl.c:852)
        key["endpoint"] = "127.0.0.1:9000"
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            self.client_encryption_expired.create_data_key("aws", key)
        # Some examples:
        # certificate verify failed: IP address mismatch, certificate is not valid for '127.0.0.1'. (_ssl.c:1129)"
        # hostname '127.0.0.1' doesn't match 'wronghost.com'
        # 127.0.0.1:9001: ('Certificate does not contain any `subjectAltName`s.',)
        key["endpoint"] = "127.0.0.1:9001"
        with self.assertRaisesRegex(
            EncryptionError, "IP address mismatch|wronghost|IPAddressMismatch|Certificate"
        ):
            self.client_encryption_invalid_hostname.create_data_key("aws", key)

    def test_02_azure(self):
        key = {"keyVaultEndpoint": "doesnotexist.local", "keyName": "foo"}
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            self.client_encryption_no_client_cert.create_data_key("azure", key)
        # "HTTP status=404" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "HTTP status=404"):
            self.client_encryption_with_tls.create_data_key("azure", key)
        # Expired cert error.
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            self.client_encryption_expired.create_data_key("azure", key)
        # Invalid cert hostname error.
        with self.assertRaisesRegex(
            EncryptionError, "IP address mismatch|wronghost|IPAddressMismatch|Certificate"
        ):
            self.client_encryption_invalid_hostname.create_data_key("azure", key)

    def test_03_gcp(self):
        key = {"projectId": "foo", "location": "bar", "keyRing": "baz", "keyName": "foo"}
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            self.client_encryption_no_client_cert.create_data_key("gcp", key)
        # "HTTP status=404" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "HTTP status=404"):
            self.client_encryption_with_tls.create_data_key("gcp", key)
        # Expired cert error.
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            self.client_encryption_expired.create_data_key("gcp", key)
        # Invalid cert hostname error.
        with self.assertRaisesRegex(
            EncryptionError, "IP address mismatch|wronghost|IPAddressMismatch|Certificate"
        ):
            self.client_encryption_invalid_hostname.create_data_key("gcp", key)

    def test_04_kmip(self):
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            self.client_encryption_no_client_cert.create_data_key("kmip")
        self.client_encryption_with_tls.create_data_key("kmip")
        # Expired cert error.
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            self.client_encryption_expired.create_data_key("kmip")
        # Invalid cert hostname error.
        with self.assertRaisesRegex(
            EncryptionError, "IP address mismatch|wronghost|IPAddressMismatch|Certificate"
        ):
            self.client_encryption_invalid_hostname.create_data_key("kmip")

    def test_05_tlsDisableOCSPEndpointCheck_is_permitted(self):
        providers = {"aws": {"accessKeyId": "foo", "secretAccessKey": "bar"}}
        options = {"aws": {"tlsDisableOCSPEndpointCheck": True}}
        encryption = ClientEncryption(
            providers, "keyvault.datakeys", self.client, OPTS, kms_tls_options=options
        )
        self.addCleanup(encryption.close)
        ctx = encryption._io_callbacks.opts._kms_ssl_contexts["aws"]
        if not hasattr(ctx, "check_ocsp_endpoint"):
            raise self.skipTest("OCSP not enabled")
        self.assertFalse(ctx.check_ocsp_endpoint)

    def test_06_named_kms_providers_apply_tls_options_aws(self):
        key = {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
            "endpoint": "127.0.0.1:9002",
        }
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            self.client_encryption_with_names.create_data_key("aws:no_client_cert", key)
        # "parse error" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "parse error"):
            self.client_encryption_with_names.create_data_key("aws:with_tls", key)

    def test_06_named_kms_providers_apply_tls_options_azure(self):
        key = {"keyVaultEndpoint": "doesnotexist.local", "keyName": "foo"}
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            self.client_encryption_with_names.create_data_key("azure:no_client_cert", key)
        # "HTTP status=404" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "HTTP status=404"):
            self.client_encryption_with_names.create_data_key("azure:with_tls", key)

    def test_06_named_kms_providers_apply_tls_options_gcp(self):
        key = {"projectId": "foo", "location": "bar", "keyRing": "baz", "keyName": "foo"}
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            self.client_encryption_with_names.create_data_key("gcp:no_client_cert", key)
        # "HTTP status=404" here means that the TLS handshake succeeded.
        with self.assertRaisesRegex(EncryptionError, "HTTP status=404"):
            self.client_encryption_with_names.create_data_key("gcp:with_tls", key)

    def test_06_named_kms_providers_apply_tls_options_kmip(self):
        # Missing client cert error.
        with self.assertRaisesRegex(EncryptionError, self.cert_error):
            self.client_encryption_with_names.create_data_key("kmip:no_client_cert")
        self.client_encryption_with_names.create_data_key("kmip:with_tls")


# https://github.com/mongodb/specifications/blob/50e26fe/source/client-side-encryption/tests/README.rst#unique-index-on-keyaltnames
class TestUniqueIndexOnKeyAltNamesProse(EncryptionIntegrationTest):
    def setUp(self):
        self.client = client_context.client
        create_key_vault(self.client.keyvault.datakeys)
        kms_providers_map = {"local": {"key": LOCAL_MASTER_KEY}}
        self.client_encryption = ClientEncryption(
            kms_providers_map, "keyvault.datakeys", self.client, CodecOptions()
        )
        self.def_key_id = self.client_encryption.create_data_key("local", key_alt_names=["def"])

    def test_01_create_key(self):
        self.client_encryption.create_data_key("local", key_alt_names=["abc"])
        with self.assertRaisesRegex(EncryptionError, "E11000 duplicate key error collection"):
            self.client_encryption.create_data_key("local", key_alt_names=["abc"])
        with self.assertRaisesRegex(EncryptionError, "E11000 duplicate key error collection"):
            self.client_encryption.create_data_key("local", key_alt_names=["def"])

    def test_02_add_key_alt_name(self):
        key_id = self.client_encryption.create_data_key("local")
        self.client_encryption.add_key_alt_name(key_id, "abc")
        key_doc = self.client_encryption.add_key_alt_name(key_id, "abc")
        assert key_doc["keyAltNames"] == ["abc"]
        with self.assertRaisesRegex(DuplicateKeyError, "E11000 duplicate key error collection"):
            self.client_encryption.add_key_alt_name(key_id, "def")
        key_doc = self.client_encryption.add_key_alt_name(self.def_key_id, "def")
        assert key_doc["keyAltNames"] == ["def"]


# https://github.com/mongodb/specifications/blob/d4c9432/source/client-side-encryption/tests/README.rst#explicit-encryption
class TestExplicitQueryableEncryption(EncryptionIntegrationTest):
    @client_context.require_no_standalone
    @client_context.require_version_min(7, 0, -1)
    def setUp(self):
        super().setUp()
        self.encrypted_fields = json_data("etc", "data", "encryptedFields.json")
        self.key1_document = json_data("etc", "data", "keys", "key1-document.json")
        self.key1_id = self.key1_document["_id"]
        self.db = self.client.test_queryable_encryption
        self.client.drop_database(self.db)
        self.db.command("create", "explicit_encryption", encryptedFields=self.encrypted_fields)
        key_vault = create_key_vault(self.client.keyvault.datakeys, self.key1_document)
        self.addCleanup(key_vault.drop)
        self.key_vault_client = self.client
        self.client_encryption = ClientEncryption(
            {"local": {"key": LOCAL_MASTER_KEY}}, key_vault.full_name, self.key_vault_client, OPTS
        )
        self.addCleanup(self.client_encryption.close)
        opts = AutoEncryptionOpts(
            {"local": {"key": LOCAL_MASTER_KEY}},
            key_vault.full_name,
            bypass_query_analysis=True,
        )
        self.encrypted_client = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(self.encrypted_client.close)

    def test_01_insert_encrypted_indexed_and_find(self):
        val = "encrypted indexed value"
        insert_payload = self.client_encryption.encrypt(
            val, Algorithm.INDEXED, self.key1_id, contention_factor=0
        )
        self.encrypted_client[self.db.name].explicit_encryption.insert_one(
            {"encryptedIndexed": insert_payload}
        )

        find_payload = self.client_encryption.encrypt(
            val, Algorithm.INDEXED, self.key1_id, query_type=QueryType.EQUALITY, contention_factor=0
        )
        docs = list(
            self.encrypted_client[self.db.name].explicit_encryption.find(
                {"encryptedIndexed": find_payload}
            )
        )
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["encryptedIndexed"], val)

    def test_02_insert_encrypted_indexed_and_find_contention(self):
        val = "encrypted indexed value"
        contention = 10
        for _ in range(contention):
            insert_payload = self.client_encryption.encrypt(
                val, Algorithm.INDEXED, self.key1_id, contention_factor=contention
            )
            self.encrypted_client[self.db.name].explicit_encryption.insert_one(
                {"encryptedIndexed": insert_payload}
            )

        find_payload = self.client_encryption.encrypt(
            val, Algorithm.INDEXED, self.key1_id, query_type=QueryType.EQUALITY, contention_factor=0
        )
        docs = list(
            self.encrypted_client[self.db.name].explicit_encryption.find(
                {"encryptedIndexed": find_payload}
            )
        )
        self.assertLessEqual(len(docs), 10)
        for doc in docs:
            self.assertEqual(doc["encryptedIndexed"], val)

        # Find with contention_factor will return all 10 documents.
        find_payload = self.client_encryption.encrypt(
            val,
            Algorithm.INDEXED,
            self.key1_id,
            query_type=QueryType.EQUALITY,
            contention_factor=contention,
        )
        docs = list(
            self.encrypted_client[self.db.name].explicit_encryption.find(
                {"encryptedIndexed": find_payload}
            )
        )
        self.assertEqual(len(docs), 10)
        for doc in docs:
            self.assertEqual(doc["encryptedIndexed"], val)

    def test_03_insert_encrypted_unindexed(self):
        val = "encrypted unindexed value"
        insert_payload = self.client_encryption.encrypt(val, Algorithm.UNINDEXED, self.key1_id)
        self.encrypted_client[self.db.name].explicit_encryption.insert_one(
            {"_id": 1, "encryptedUnindexed": insert_payload}
        )

        docs = list(self.encrypted_client[self.db.name].explicit_encryption.find({"_id": 1}))
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["encryptedUnindexed"], val)

    def test_04_roundtrip_encrypted_indexed(self):
        val = "encrypted indexed value"
        payload = self.client_encryption.encrypt(
            val, Algorithm.INDEXED, self.key1_id, contention_factor=0
        )
        decrypted = self.client_encryption.decrypt(payload)
        self.assertEqual(decrypted, val)

    def test_05_roundtrip_encrypted_unindexed(self):
        val = "encrypted indexed value"
        payload = self.client_encryption.encrypt(val, Algorithm.UNINDEXED, self.key1_id)
        decrypted = self.client_encryption.decrypt(payload)
        self.assertEqual(decrypted, val)


# https://github.com/mongodb/specifications/blob/072601/source/client-side-encryption/tests/README.rst#rewrap
class TestRewrapWithSeparateClientEncryption(EncryptionIntegrationTest):
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

    def test_rewrap(self):
        for src_provider in self.MASTER_KEYS:
            for dst_provider in self.MASTER_KEYS:
                with self.subTest(src_provider=src_provider, dst_provider=dst_provider):
                    self.run_test(src_provider, dst_provider)

    def run_test(self, src_provider, dst_provider):
        # Step 1. Drop the collection ``keyvault.datakeys``.
        self.client.keyvault.drop_collection("datakeys")

        # Step 2. Create a ``ClientEncryption`` object named ``client_encryption1``
        client_encryption1 = ClientEncryption(
            key_vault_client=self.client,
            key_vault_namespace="keyvault.datakeys",
            kms_providers=ALL_KMS_PROVIDERS,
            kms_tls_options=KMS_TLS_OPTS,
            codec_options=OPTS,
        )
        self.addCleanup(client_encryption1.close)

        # Step 3. Call ``client_encryption1.create_data_key`` with ``src_provider``.
        key_id = client_encryption1.create_data_key(
            master_key=self.MASTER_KEYS[src_provider], kms_provider=src_provider
        )

        # Step 4. Call ``client_encryption1.encrypt`` with the value "test"
        cipher_text = client_encryption1.encrypt(
            "test", key_id=key_id, algorithm=Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
        )

        # Step 5. Create a ``ClientEncryption`` object named ``client_encryption2``
        client2 = rs_or_single_client()
        self.addCleanup(client2.close)
        client_encryption2 = ClientEncryption(
            key_vault_client=client2,
            key_vault_namespace="keyvault.datakeys",
            kms_providers=ALL_KMS_PROVIDERS,
            kms_tls_options=KMS_TLS_OPTS,
            codec_options=OPTS,
        )
        self.addCleanup(client_encryption1.close)

        # Step 6. Call ``client_encryption2.rewrap_many_data_key`` with an empty ``filter``.
        rewrap_many_data_key_result = client_encryption2.rewrap_many_data_key(
            {}, provider=dst_provider, master_key=self.MASTER_KEYS[dst_provider]
        )

        self.assertEqual(rewrap_many_data_key_result.bulk_write_result.modified_count, 1)

        # 7. Call ``client_encryption1.decrypt`` with the ``cipher_text``. Assert the return value is "test".
        decrypt_result1 = client_encryption1.decrypt(cipher_text)
        self.assertEqual(decrypt_result1, "test")

        # 8. Call ``client_encryption2.decrypt`` with the ``cipher_text``. Assert the return value is "test".
        decrypt_result2 = client_encryption2.decrypt(cipher_text)
        self.assertEqual(decrypt_result2, "test")

        # 8. Case 2. Provider is not optional when master_key is given.
        with self.assertRaises(ConfigurationError):
            rewrap_many_data_key_result = client_encryption2.rewrap_many_data_key(
                {}, master_key=self.MASTER_KEYS[dst_provider]
            )


# https://github.com/mongodb/specifications/blob/5cf3ed/source/client-side-encryption/tests/README.rst#on-demand-aws-credentials
class TestOnDemandAWSCredentials(EncryptionIntegrationTest):
    def setUp(self):
        super().setUp()
        self.master_key = {
            "region": "us-east-1",
            "key": ("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"),
        }

    @unittest.skipIf(any(AWS_CREDS.values()), "AWS environment credentials are set")
    def test_01_failure(self):
        self.client_encryption = ClientEncryption(
            kms_providers={"aws": {}},
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=client_context.client,
            codec_options=OPTS,
        )
        with self.assertRaises(EncryptionError):
            self.client_encryption.create_data_key("aws", self.master_key)

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def test_02_success(self):
        self.client_encryption = ClientEncryption(
            kms_providers={"aws": {}},
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=client_context.client,
            codec_options=OPTS,
        )
        self.client_encryption.create_data_key("aws", self.master_key)


class TestQueryableEncryptionDocsExample(EncryptionIntegrationTest):
    # Queryable Encryption is not supported on Standalone topology.
    @client_context.require_no_standalone
    @client_context.require_version_min(7, 0, -1)
    def setUp(self):
        super().setUp()

    def test_queryable_encryption(self):
        # MongoClient to use in testing that handles auth/tls/etc,
        # and cleanup.
        def MongoClient(**kwargs):
            c = rs_or_single_client(**kwargs)
            self.addCleanup(c.close)
            return c

        # Drop data from prior test runs.
        self.client.keyvault.datakeys.drop()
        self.client.drop_database("docs_examples")

        kms_providers_map = {"local": {"key": LOCAL_MASTER_KEY}}

        # Create two data keys.
        key_vault_client = MongoClient()
        client_encryption = ClientEncryption(
            kms_providers_map, "keyvault.datakeys", key_vault_client, CodecOptions()
        )
        key1_id = client_encryption.create_data_key("local")
        key2_id = client_encryption.create_data_key("local")

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
        encrypted_client = MongoClient(auto_encryption_opts=opts)

        # Create a Queryable Encryption collection "docs_examples.encrypted".
        # Because docs_examples.encrypted is in encrypted_fields_map, it is
        # created with Queryable Encryption support.
        db = encrypted_client.docs_examples
        encrypted_coll = db.create_collection("encrypted")

        # Auto encrypt an insert and find.

        # Encrypt an insert.
        encrypted_coll.insert_one(
            {
                "_id": 1,
                "encrypted_indexed": "indexed_value",
                "encrypted_unindexed": "unindexed_value",
            }
        )

        # Encrypt a find.
        res = encrypted_coll.find_one({"encrypted_indexed": "indexed_value"})
        assert res is not None
        assert res["encrypted_indexed"] == "indexed_value"
        assert res["encrypted_unindexed"] == "unindexed_value"

        # Find documents without decryption.
        unencrypted_client = MongoClient()
        unencrypted_coll = unencrypted_client.docs_examples.encrypted
        res = unencrypted_coll.find_one({"_id": 1})
        assert res is not None
        assert isinstance(res["encrypted_indexed"], Binary)
        assert isinstance(res["encrypted_unindexed"], Binary)

        client_encryption.close()


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.rst#range-explicit-encryption
class TestRangeQueryProse(EncryptionIntegrationTest):
    @client_context.require_no_standalone
    @client_context.require_version_min(7, 0, -1)
    @client_context.require_version_max(7, 9, 99)
    def setUp(self):
        super().setUp()
        self.key1_document = json_data("etc", "data", "keys", "key1-document.json")
        self.key1_id = self.key1_document["_id"]
        self.client.drop_database(self.db)
        key_vault = create_key_vault(self.client.keyvault.datakeys, self.key1_document)
        self.addCleanup(key_vault.drop)
        self.key_vault_client = self.client
        self.client_encryption = ClientEncryption(
            {"local": {"key": LOCAL_MASTER_KEY}}, key_vault.full_name, self.key_vault_client, OPTS
        )
        self.addCleanup(self.client_encryption.close)
        opts = AutoEncryptionOpts(
            {"local": {"key": LOCAL_MASTER_KEY}},
            key_vault.full_name,
            bypass_query_analysis=True,
        )
        self.encrypted_client = rs_or_single_client(auto_encryption_opts=opts)
        self.db = self.encrypted_client.db
        self.addCleanup(self.encrypted_client.close)

    def run_expression_find(
        self, name, expression, expected_elems, range_opts, use_expr=False, key_id=None
    ):
        find_payload = self.client_encryption.encrypt_expression(
            expression=expression,
            key_id=key_id or self.key1_id,
            algorithm=Algorithm.RANGEPREVIEW,
            query_type=QueryType.RANGEPREVIEW,
            contention_factor=0,
            range_opts=range_opts,
        )
        if use_expr:
            find_payload = {"$expr": find_payload}
        sorted_find = sorted(
            self.encrypted_client.db.explicit_encryption.find(find_payload), key=lambda x: x["_id"]
        )
        for elem, expected in zip(sorted_find, expected_elems):
            self.assertEqual(elem[f"encrypted{name}"], expected)

    def run_test_cases(self, name, range_opts, cast_func):
        encrypted_fields = json_data("etc", "data", f"range-encryptedFields-{name}.json")
        self.db.drop_collection("explicit_encryption", encrypted_fields=encrypted_fields)
        self.db.create_collection("explicit_encryption", encryptedFields=encrypted_fields)

        def encrypt_and_cast(i):
            return self.client_encryption.encrypt(
                cast_func(i),
                key_id=self.key1_id,
                algorithm=Algorithm.RANGEPREVIEW,
                contention_factor=0,
                range_opts=range_opts,
            )

        for elem in [{f"encrypted{name}": encrypt_and_cast(i)} for i in [0, 6, 30, 200]]:
            self.encrypted_client.db.explicit_encryption.insert_one(elem)

        # Case 1.
        insert_payload = self.client_encryption.encrypt(
            cast_func(6),
            key_id=self.key1_id,
            algorithm=Algorithm.RANGEPREVIEW,
            contention_factor=0,
            range_opts=range_opts,
        )
        self.assertEqual(self.client_encryption.decrypt(insert_payload), cast_func(6))

        # Case 2.
        expression = {
            "$and": [
                {f"encrypted{name}": {"$gte": cast_func(6)}},
                {f"encrypted{name}": {"$lte": cast_func(200)}},
            ]
        }
        self.run_expression_find(name, expression, [cast_func(i) for i in [6, 30, 200]], range_opts)
        # Case 2, with UUID key_id
        self.run_expression_find(
            name,
            expression,
            [cast_func(i) for i in [6, 30, 200]],
            range_opts,
            key_id=self.key1_id.as_uuid(),
        )

        # Case 3.
        self.run_expression_find(
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
        self.run_expression_find(
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
        self.run_expression_find(
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
                self.client_encryption.encrypt(
                    cast_func(201),
                    key_id=self.key1_id,
                    algorithm=Algorithm.RANGEPREVIEW,
                    contention_factor=0,
                    range_opts=range_opts,
                )

            # Case 7.
            with self.assertRaisesRegex(
                EncryptionError, "expected matching 'min' and value type. Got range option"
            ):
                self.client_encryption.encrypt(
                    6 if cast_func != int else float(6),
                    key_id=self.key1_id,
                    algorithm=Algorithm.RANGEPREVIEW,
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
                    self.client_encryption.encrypt(
                        cast_func(6),
                        key_id=self.key1_id,
                        algorithm=Algorithm.RANGEPREVIEW,
                        contention_factor=0,
                        range_opts=RangeOpts(
                            min=cast_func(0), max=cast_func(200), sparsity=1, precision=2
                        ),
                    )

    def test_double_no_precision(self):
        self.run_test_cases("DoubleNoPrecision", RangeOpts(sparsity=1), float)

    def test_double_precision(self):
        self.run_test_cases(
            "DoublePrecision",
            RangeOpts(min=0.0, max=200.0, sparsity=1, precision=2),
            float,
        )

    def test_decimal_no_precision(self):
        self.run_test_cases(
            "DecimalNoPrecision", RangeOpts(sparsity=1), lambda x: Decimal128(str(x))
        )

    def test_decimal_precision(self):
        self.run_test_cases(
            "DecimalPrecision",
            RangeOpts(min=Decimal128("0.0"), max=Decimal128("200.0"), sparsity=1, precision=2),
            lambda x: Decimal128(str(x)),
        )

    def test_datetime(self):
        self.run_test_cases(
            "Date",
            RangeOpts(min=DatetimeMS(0), max=DatetimeMS(200), sparsity=1),
            lambda x: DatetimeMS(x).as_datetime(),
        )

    def test_int(self):
        self.run_test_cases("Int", RangeOpts(min=0, max=200, sparsity=1), int)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.rst#automatic-data-encryption-keys
class TestAutomaticDecryptionKeys(EncryptionIntegrationTest):
    @client_context.require_no_standalone
    @client_context.require_version_min(7, 0, -1)
    def setUp(self):
        super().setUp()
        self.key1_document = json_data("etc", "data", "keys", "key1-document.json")
        self.key1_id = self.key1_document["_id"]
        self.client.drop_database(self.db)
        self.key_vault = create_key_vault(self.client.keyvault.datakeys, self.key1_document)
        self.addCleanup(self.key_vault.drop)
        self.client_encryption = ClientEncryption(
            {"local": {"key": LOCAL_MASTER_KEY}},
            self.key_vault.full_name,
            self.client,
            OPTS,
        )
        self.addCleanup(self.client_encryption.close)

    def test_01_simple_create(self):
        coll, _ = self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            encrypted_fields={"fields": [{"path": "ssn", "bsonType": "string", "keyId": None}]},
            kms_provider="local",
        )
        with self.assertRaises(WriteError) as exc:
            coll.insert_one({"ssn": "123-45-6789"})
        self.assertEqual(exc.exception.code, 121)

    def test_02_no_fields(self):
        with self.assertRaisesRegex(
            TypeError,
            "create_encrypted_collection.* missing 1 required positional argument: 'encrypted_fields'",
        ):
            self.client_encryption.create_encrypted_collection(  # type:ignore[call-arg]
                database=self.db,
                name="testing1",
            )

    def test_03_invalid_keyid(self):
        with self.assertRaisesRegex(
            EncryptedCollectionError,
            "create.encryptedFields.fields.keyId' is the wrong type 'bool', expected type 'binData",
        ):
            self.client_encryption.create_encrypted_collection(
                database=self.db,
                name="testing1",
                encrypted_fields={
                    "fields": [{"path": "ssn", "bsonType": "string", "keyId": False}]
                },
                kms_provider="local",
            )

    def test_04_insert_encrypted(self):
        coll, ef = self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            encrypted_fields={"fields": [{"path": "ssn", "bsonType": "string", "keyId": None}]},
            kms_provider="local",
        )
        key1_id = ef["fields"][0]["keyId"]
        encrypted_value = self.client_encryption.encrypt(
            "123-45-6789",
            key_id=key1_id,
            algorithm=Algorithm.UNINDEXED,
        )
        coll.insert_one({"ssn": encrypted_value})

    def test_copy_encrypted_fields(self):
        encrypted_fields = {
            "fields": [
                {
                    "path": "ssn",
                    "bsonType": "string",
                    "keyId": None,
                }
            ]
        }
        _, ef = self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            kms_provider="local",
            encrypted_fields=encrypted_fields,
        )
        self.assertIsNotNone(ef["fields"][0]["keyId"])
        self.assertIsNone(encrypted_fields["fields"][0]["keyId"])

    def test_options_forward(self):
        coll, ef = self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            kms_provider="local",
            encrypted_fields={"fields": [{"path": "ssn", "bsonType": "string", "keyId": None}]},
            read_preference=ReadPreference.NEAREST,
        )
        self.assertEqual(coll.read_preference, ReadPreference.NEAREST)
        self.assertEqual(coll.name, "testing1")

    def test_mixed_null_keyids(self):
        key = self.client_encryption.create_data_key(kms_provider="local")
        coll, ef = self.client_encryption.create_encrypted_collection(
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
            self.client_encryption.encrypt(
                val,
                key_id=key,
                algorithm=Algorithm.UNINDEXED,
            )
            for val, key in zip(
                ["123-45-6789", "11/22/1963", "My secret", "New Mexico, 87104"],
                [field["keyId"] for field in ef["fields"]],
            )
        ]
        coll.insert_one(
            {
                "ssn": encrypted_values[0],
                "dob": encrypted_values[1],
                "secrets": encrypted_values[2],
                "address": encrypted_values[3],
            }
        )

    def test_create_datakey_fails(self):
        key = self.client_encryption.create_data_key(kms_provider="local")
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
            self.client_encryption.create_encrypted_collection(
                database=self.db,
                name="testing1",
                encrypted_fields=encrypted_fields,
                kms_provider="does not exist",
            )
        self.assertEqual(exc.exception.encrypted_fields, encrypted_fields)

    def test_create_failure(self):
        key = self.client_encryption.create_data_key(kms_provider="local")
        # Make sure the exception's encrypted_fields object includes the previous keys in the error message even when
        # it is the creation of the collection that fails.
        with self.assertRaises(
            EncryptedCollectionError,
        ) as exc:
            self.client_encryption.create_encrypted_collection(
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

    def test_collection_name_collision(self):
        encrypted_fields = {
            "fields": [
                {"path": "address", "bsonType": "string", "keyId": None},
            ]
        }
        self.db.create_collection("testing1")
        with self.assertRaises(
            EncryptedCollectionError,
        ) as exc:
            self.client_encryption.create_encrypted_collection(
                database=self.db,
                name="testing1",
                encrypted_fields=encrypted_fields,
                kms_provider="local",
            )
        self.assertIsInstance(exc.exception.encrypted_fields["fields"][0]["keyId"], Binary)
        self.db.drop_collection("testing1", encrypted_fields=encrypted_fields)
        self.client_encryption.create_encrypted_collection(
            database=self.db,
            name="testing1",
            encrypted_fields=encrypted_fields,
            kms_provider="local",
        )
        with self.assertRaises(
            EncryptedCollectionError,
        ) as exc:
            self.client_encryption.create_encrypted_collection(
                database=self.db,
                name="testing1",
                encrypted_fields=encrypted_fields,
                kms_provider="local",
            )
        self.assertIsInstance(exc.exception.encrypted_fields["fields"][0]["keyId"], Binary)


def start_mongocryptd(port) -> None:
    args = ["mongocryptd", f"--port={port}", "--idleShutdownTimeoutSecs=60"]
    _spawn_daemon(args)


class TestNoSessionsSupport(EncryptionIntegrationTest):
    mongocryptd_client: MongoClient
    MONGOCRYPTD_PORT = 27020

    @classmethod
    @unittest.skipIf(os.environ.get("TEST_CRYPT_SHARED"), "crypt_shared lib is installed")
    def setUpClass(cls):
        super().setUpClass()
        start_mongocryptd(cls.MONGOCRYPTD_PORT)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def setUp(self) -> None:
        self.listener = OvertCommandListener()
        self.mongocryptd_client = MongoClient(
            f"mongodb://localhost:{self.MONGOCRYPTD_PORT}", event_listeners=[self.listener]
        )
        self.addCleanup(self.mongocryptd_client.close)

        hello = self.mongocryptd_client.db.command("hello")
        self.assertNotIn("logicalSessionTimeoutMinutes", hello)

    def test_implicit_session_ignored_when_unsupported(self):
        self.listener.reset()
        with self.assertRaises(OperationFailure):
            self.mongocryptd_client.db.test.find_one()

        self.assertNotIn("lsid", self.listener.started_events[0].command)

        with self.assertRaises(OperationFailure):
            self.mongocryptd_client.db.test.insert_one({"x": 1})

        self.assertNotIn("lsid", self.listener.started_events[1].command)

    def test_explicit_session_errors_when_unsupported(self):
        self.listener.reset()
        with self.mongocryptd_client.start_session() as s:
            with self.assertRaisesRegex(
                ConfigurationError, r"Sessions are not supported by this MongoDB deployment"
            ):
                self.mongocryptd_client.db.test.find_one(session=s)
            with self.assertRaisesRegex(
                ConfigurationError, r"Sessions are not supported by this MongoDB deployment"
            ):
                self.mongocryptd_client.db.test.insert_one({"x": 1}, session=s)


if __name__ == "__main__":
    unittest.main()
