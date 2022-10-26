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

import base64
import copy
import os
import re
import socket
import sys
import textwrap
import traceback
import uuid

sys.path[0:0] = [""]

from test import IntegrationTest, PyMongoTestCase, client_context, unittest
from test.test_bulk import BulkTestBase
from test.test_ssl import CA_PEM
from test.utils import (
    AllowListEventListener,
    OvertCommandListener,
    TestCreator,
    TopologyEventListener,
    camel_to_snake_args,
    rs_or_single_client,
    wait_until,
)
from test.utils_spec_runner import SpecRunner

from bson import encode, json_util
from bson.binary import JAVA_LEGACY, STANDARD, UUID_SUBTYPE, Binary, UuidRepresentation
from bson.codec_options import CodecOptions
from bson.errors import BSONError
from bson.json_util import JSONOptions
from bson.py3compat import _unicode
from bson.son import SON
from pymongo import encryption
from pymongo.cursor import CursorType
from pymongo.encryption import Algorithm, ClientEncryption
from pymongo.encryption_options import _HAVE_PYMONGOCRYPT, AutoEncryptionOpts
from pymongo.errors import (
    BulkWriteError,
    ConfigurationError,
    EncryptionError,
    InvalidOperation,
    OperationFailure,
    ServerSelectionTimeoutError,
    WriteError,
)
from pymongo.mongo_client import MongoClient
from pymongo.operations import InsertOne, ReplaceOne, UpdateOne
from pymongo.write_concern import WriteConcern


def get_client_opts(client):
    return client._MongoClient__options


KMS_PROVIDERS = {"local": {"key": b"\x00" * 96}}


class TestAutoEncryptionOpts(PyMongoTestCase):
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
        super(EncryptionIntegrationTest, cls).setUpClass()

    def assertEncrypted(self, val):
        self.assertIsInstance(val, Binary)
        self.assertEqual(val.subtype, 6)

    def assertBinaryUUID(self, val):
        self.assertIsInstance(val, Binary)
        self.assertEqual(val.subtype, UUID_SUBTYPE)


# Location of JSON test files.
BASE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "client-side-encryption")
SPEC_PATH = os.path.join(BASE, "spec")

OPTS = CodecOptions(uuid_representation=STANDARD)

# Use SON to preserve the order of fields while parsing json. Use tz_aware
# =False to match how CodecOptions decodes dates.
JSON_OPTS = JSONOptions(document_class=SON, uuid_representation=STANDARD, tz_aware=False)


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
        self.assertEqual(set(decrypted_ssns), set(d["ssn"] for d in docs))

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


class TestEncryptedBulkWrite(BulkTestBase, EncryptionIntegrationTest):
    def test_upsert_uuid_standard_encrypte(self):
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
        super(TestClientMaxWireVersion, cls).setUpClass()

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
            client_encryption.decrypt("str")
        with self.assertRaisesRegex(TypeError, msg):
            client_encryption.decrypt(Binary(b"123"))

        msg = "key_id must be a bson.binary.Binary with subtype 4"
        algo = Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
        with self.assertRaisesRegex(TypeError, msg):
            client_encryption.encrypt("str", algo, key_id=uuid.uuid4())
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
                key_id=Binary(uuid.uuid4().bytes, UUID_SUBTYPE),
            )

    def test_codec_options(self):
        with self.assertRaisesRegex(TypeError, "codec_options must be"):
            ClientEncryption(KMS_PROVIDERS, "keyvault.datakeys", client_context.client, None)

        opts = CodecOptions(uuid_representation=JAVA_LEGACY)
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
        client_encryption = ClientEncryption(
            KMS_PROVIDERS, "keyvault.datakeys", client_context.client, OPTS
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
        self.assertEqual(client_encryption_legacy.decrypt(encrypted_standard), value)
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
AWS_CREDS = {
    "accessKeyId": os.environ.get("FLE_AWS_KEY", ""),
    "secretAccessKey": os.environ.get("FLE_AWS_SECRET", ""),
}

AWS_TEMP_CREDS = {
    "accessKeyId": os.environ.get("CSFLE_AWS_TEMP_ACCESS_KEY_ID", ""),
    "secretAccessKey": os.environ.get("CSFLE_AWS_TEMP_SECRET_ACCESS_KEY", ""),
    "sessionToken": os.environ.get("CSFLE_AWS_TEMP_SESSION_TOKEN", ""),
}

AWS_TEMP_NO_SESSION_CREDS = {
    "accessKeyId": os.environ.get("CSFLE_AWS_TEMP_ACCESS_KEY_ID", ""),
    "secretAccessKey": os.environ.get("CSFLE_AWS_TEMP_SECRET_ACCESS_KEY", ""),
}

AZURE_CREDS = {
    "tenantId": os.environ.get("FLE_AZURE_TENANTID", ""),
    "clientId": os.environ.get("FLE_AZURE_CLIENTID", ""),
    "clientSecret": os.environ.get("FLE_AZURE_CLIENTSECRET", ""),
}

GCP_CREDS = {
    "email": os.environ.get("FLE_GCP_EMAIL", ""),
    "privateKey": _unicode(os.environ.get("FLE_GCP_PRIVATEKEY", "")),
}


class TestSpec(SpecRunner):
    @classmethod
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    @client_context.require_version_min(3, 6)  # SpecRunner requires sessions.
    def setUpClass(cls):
        super(TestSpec, cls).setUpClass()

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
        if "key_vault_namespace" not in opts:
            opts["key_vault_namespace"] = "keyvault.datakeys"
        if "extra_options" in opts:
            opts.update(camel_to_snake_args(opts.pop("extra_options")))

        opts = dict(opts)
        return AutoEncryptionOpts(**opts)

    def parse_client_options(self, opts):
        """Override clientOptions parsing to support autoEncryptOpts."""
        encrypt_opts = opts.pop("autoEncryptOpts")
        if encrypt_opts:
            opts["auto_encryption_opts"] = self.parse_auto_encrypt_opts(encrypt_opts)

        return super(TestSpec, self).parse_client_options(opts)

    def get_object_name(self, op):
        """Default object is collection."""
        return op.get("object", "collection")

    def maybe_skip_scenario(self, test):
        super(TestSpec, self).maybe_skip_scenario(test)
        desc = test["description"].lower()
        if "type=symbol" in desc:
            self.skipTest("PyMongo does not support the symbol type")
        if desc == "explain a find with deterministic encryption":
            # PyPy and Python 3.6+ have ordered dict.
            if sys.version_info[:2] < (3, 6) and "PyPy" not in sys.version:
                self.skipTest("explain test does not work without ordered dict")

    def setup_scenario(self, scenario_def):
        """Override a test's setup."""
        key_vault_data = scenario_def["key_vault_data"]
        if key_vault_data:
            coll = client_context.client.get_database(
                "keyvault", write_concern=WriteConcern(w="majority"), codec_options=OPTS
            )["datakeys"]
            coll.drop()
            coll.insert_many(key_vault_data)

        db_name = self.get_scenario_db_name(scenario_def)
        coll_name = self.get_scenario_coll_name(scenario_def)
        db = client_context.client.get_database(
            db_name, write_concern=WriteConcern(w="majority"), codec_options=OPTS
        )
        coll = db[coll_name]
        coll.drop()
        json_schema = scenario_def["json_schema"]
        if json_schema:
            db.create_collection(
                coll_name, validator={"$jsonSchema": json_schema}, codec_options=OPTS
            )
        else:
            db.create_collection(coll_name)

        if scenario_def["data"]:
            # Load data.
            coll.insert_many(scenario_def["data"])

    def allowable_errors(self, op):
        """Override expected error classes."""
        errors = super(TestSpec, self).allowable_errors(op)
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


test_creator = TestCreator(create_test, TestSpec, SPEC_PATH)
test_creator.create_tests()


# Prose Tests
LOCAL_MASTER_KEY = base64.b64decode(
    b"Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ"
    b"5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk"
)

LOCAL_KEY_ID = Binary(base64.b64decode(b"LOCALAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
AWS_KEY_ID = Binary(base64.b64decode(b"AWSAAAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
AZURE_KEY_ID = Binary(base64.b64decode(b"AZUREAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)
GCP_KEY_ID = Binary(base64.b64decode(b"GCPAAAAAAAAAAAAAAAAAAA=="), UUID_SUBTYPE)


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
    return vault


class TestDataKeyDoubleEncryption(EncryptionIntegrationTest):

    KMS_PROVIDERS = {
        "aws": AWS_CREDS,
        "azure": AZURE_CREDS,
        "gcp": GCP_CREDS,
        "local": {"key": LOCAL_MASTER_KEY},
    }

    MASTER_KEYS = {
        "aws": {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-" "4bd9-9f25-e30687b580d0",
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
        "local": None,
    }

    @classmethod
    @unittest.skipUnless(
        any([all(AWS_CREDS.values()), all(AZURE_CREDS.values()), all(GCP_CREDS.values())]),
        "No environment credentials are set",
    )
    def setUpClass(cls):
        super(TestDataKeyDoubleEncryption, cls).setUpClass()
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
        opts = AutoEncryptionOpts(cls.KMS_PROVIDERS, "keyvault.datakeys", schema_map=schemas)
        cls.client_encrypted = rs_or_single_client(
            auto_encryption_opts=opts, uuidRepresentation="standard"
        )
        cls.client_encryption = ClientEncryption(
            cls.KMS_PROVIDERS, "keyvault.datakeys", cls.client, OPTS
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
        master_key = self.MASTER_KEYS[provider_name]
        datakey_id = self.client_encryption.create_data_key(
            provider_name, master_key=master_key, key_alt_names=["%s_altname" % (provider_name,)]
        )
        self.assertBinaryUUID(datakey_id)
        cmd = self.listener.results["started"][-1]
        self.assertEqual("insert", cmd.command_name)
        self.assertEqual({"w": "majority"}, cmd.command.get("writeConcern"))
        docs = list(self.vault.find({"_id": datakey_id}))
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["masterKey"]["provider"], provider_name)

        # Encrypt by key_id.
        encrypted = self.client_encryption.encrypt(
            "hello %s" % (provider_name,),
            Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_id=datakey_id,
        )
        self.assertEncrypted(encrypted)
        self.client_encrypted.db.coll.insert_one({"_id": provider_name, "value": encrypted})
        doc_decrypted = self.client_encrypted.db.coll.find_one({"_id": provider_name})
        self.assertEqual(doc_decrypted["value"], "hello %s" % (provider_name,))

        # Encrypt by key_alt_name.
        encrypted_altname = self.client_encryption.encrypt(
            "hello %s" % (provider_name,),
            Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_alt_name="%s_altname" % (provider_name,),
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
        super(TestCorpus, cls).setUpClass()

    @staticmethod
    def kms_providers():
        return {
            "aws": AWS_CREDS,
            "azure": AZURE_CREDS,
            "gcp": GCP_CREDS,
            "local": {"key": LOCAL_MASTER_KEY},
        }

    @staticmethod
    def fix_up_schema(json_schema):
        """Remove deprecated symbol/dbPointer types from json schema."""
        for key in json_schema["properties"].keys():
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
        )
        self.addCleanup(vault.drop)

        client_encrypted = rs_or_single_client(
            auto_encryption_opts=opts, uuidRepresentation="standard"
        )
        self.addCleanup(client_encrypted.close)

        client_encryption = ClientEncryption(
            self.kms_providers(), "keyvault.datakeys", client_context.client, OPTS
        )
        self.addCleanup(client_encryption.close)

        corpus = self.fix_up_curpus(json_data("corpus", "corpus.json"))
        corpus_copied = SON()
        for key, value in corpus.items():
            corpus_copied[key] = copy.deepcopy(value)
            if key in ("_id", "altname_aws", "altname_azure", "altname_gcp", "altname_local"):
                continue
            if value["method"] == "auto":
                continue
            if value["method"] == "explicit":
                identifier = value["identifier"]
                self.assertIn(identifier, ("id", "altname"))
                kms = value["kms"]
                self.assertIn(kms, ("local", "aws", "azure", "gcp"))
                if identifier == "id":
                    if kms == "local":
                        kwargs = dict(key_id=LOCAL_KEY_ID)
                    elif kms == "aws":
                        kwargs = dict(key_id=AWS_KEY_ID)
                    elif kms == "azure":
                        kwargs = dict(key_id=AZURE_KEY_ID)
                    else:
                        kwargs = dict(key_id=GCP_KEY_ID)
                else:
                    kwargs = dict(key_alt_name=kms)

                self.assertIn(value["algo"], ("det", "rand"))
                if value["algo"] == "det":
                    algo = Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic
                else:
                    algo = Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Random

                try:
                    encrypted_val = client_encryption.encrypt(value["value"], algo, **kwargs)
                    if not value["allowed"]:
                        self.fail("encrypt should have failed: %r: %r" % (key, value))
                    corpus_copied[key]["value"] = encrypted_val
                except Exception:
                    if value["allowed"]:
                        tb = traceback.format_exc()
                        self.fail("encrypt failed: %r: %r, traceback: %s" % (key, value, tb))

        client_encrypted.db.coll.insert_one(corpus_copied)
        corpus_decrypted = client_encrypted.db.coll.find_one()
        self.assertEqual(corpus_decrypted, corpus)

        corpus_encrypted_expected = self.fix_up_curpus_encrypted(
            json_data("corpus", "corpus-encrypted.json"), corpus
        )
        corpus_encrypted_actual = coll.find_one()
        for key, value in corpus_encrypted_actual.items():
            if key in ("_id", "altname_aws", "altname_azure", "altname_gcp", "altname_local"):
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
        opts = AutoEncryptionOpts(self.kms_providers(), "keyvault.datakeys")
        self._test_corpus(opts)

    def test_corpus_local_schema(self):
        # Configure the encrypted field via the local schema_map option.
        schemas = {"db.coll": self.fix_up_schema(json_data("corpus", "corpus-schema.json"))}
        opts = AutoEncryptionOpts(self.kms_providers(), "keyvault.datakeys", schema_map=schemas)
        self._test_corpus(opts)


_2_MiB = 2097152
_16_MiB = 16777216


class TestBsonSizeBatches(EncryptionIntegrationTest):
    """Prose tests for BSON size limits and batch splitting."""

    @classmethod
    def setUpClass(cls):
        super(TestBsonSizeBatches, cls).setUpClass()
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
        super(TestBsonSizeBatches, cls).tearDownClass()

    def test_01_insert_succeeds_under_2MiB(self):
        doc = {"_id": "over_2mib_under_16mib", "unencrypted": "a" * _2_MiB}
        self.coll_encrypted.insert_one(doc)

        # Same with bulk_write.
        doc["_id"] = "over_2mib_under_16mib_bulk"
        self.coll_encrypted.bulk_write([InsertOne(doc)])

    def test_02_insert_succeeds_over_2MiB_post_encryption(self):
        doc = {"_id": "encryption_exceeds_2mib", "unencrypted": "a" * ((2 ** 21) - 2000)}
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
        super(TestCustomEndpoint, cls).setUpClass()

    def setUp(self):
        kms_providers = {"aws": AWS_CREDS, "azure": AZURE_CREDS, "gcp": GCP_CREDS}
        self.client_encryption = ClientEncryption(
            kms_providers=kms_providers,
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=client_context.client,
            codec_options=OPTS,
        )

        kms_providers_invalid = copy.deepcopy(kms_providers)

        kms_providers_invalid["azure"]["identityPlatformEndpoint"] = "doesnotexist.invalid:443"
        kms_providers_invalid["gcp"]["endpoint"] = "doesnotexist.invalid:443"
        self.client_encryption_invalid = ClientEncryption(
            kms_providers=kms_providers_invalid,
            key_vault_namespace="keyvault.datakeys",
            key_vault_client=client_context.client,
            codec_options=OPTS,
        )
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
                    "arn:aws:kms:us-east-1:579766882180:key/" "89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
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
                    "arn:aws:kms:us-east-1:579766882180:key/" "89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
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
                    "arn:aws:kms:us-east-1:579766882180:key/" "89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                ),
                "endpoint": "kms.us-east-1.amazonaws.com:443",
            },
        )

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def test_04_aws_endpoint_invalid_port(self):
        master_key = {
            "region": "us-east-1",
            "key": (
                "arn:aws:kms:us-east-1:579766882180:key/" "89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
            ),
            "endpoint": "kms.us-east-1.amazonaws.com:12345",
        }
        with self.assertRaises(EncryptionError) as ctx:
            self.client_encryption.create_data_key("aws", master_key=master_key)
        self.assertIsInstance(ctx.exception.cause, socket.error)

    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def test_05_aws_endpoint_wrong_region(self):
        master_key = {
            "region": "us-east-1",
            "key": (
                "arn:aws:kms:us-east-1:579766882180:key/" "89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
            ),
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
            "key": (
                "arn:aws:kms:us-east-1:579766882180:key/" "89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
            ),
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


class AzureGCPEncryptionTestMixin(object):
    DEK = None
    KMS_PROVIDER_MAP = None
    KEYVAULT_DB = "keyvault"
    KEYVAULT_COLL = "datakeys"

    def setUp(self):
        keyvault = self.client.get_database(self.KEYVAULT_DB).get_collection(self.KEYVAULT_COLL)
        create_key_vault(keyvault, self.DEK)

    def _test_explicit(self, expectation):
        client_encryption = ClientEncryption(
            self.KMS_PROVIDER_MAP,
            ".".join([self.KEYVAULT_DB, self.KEYVAULT_COLL]),
            client_context.client,
            OPTS,
        )
        self.addCleanup(client_encryption.close)

        ciphertext = client_encryption.encrypt(
            "string0",
            algorithm=Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
            key_id=Binary.from_uuid(self.DEK["_id"], STANDARD),
        )

        self.assertEqual(bytes(ciphertext), base64.b64decode(expectation))
        self.assertEqual(client_encryption.decrypt(ciphertext), "string0")

    def _test_automatic(self, expectation_extjson, payload):
        encrypted_db = "db"
        encrypted_coll = "coll"
        keyvault_namespace = ".".join([self.KEYVAULT_DB, self.KEYVAULT_COLL])

        encryption_opts = AutoEncryptionOpts(
            self.KMS_PROVIDER_MAP, keyvault_namespace, schema_map=self.SCHEMA_MAP
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
        event = insert_listener.results["started"][0]
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
        super(TestAzureEncryption, cls).setUpClass()

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
        super(TestGCPEncryption, cls).setUpClass()

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

        if auto_encryption_opts._bypass_auto_encryption == True:
            self.client_test.db.coll.insert_one({"_id": 0, "encrypted": self.ciphertext})
        elif auto_encryption_opts._bypass_auto_encryption == False:
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

        cev = self.client_listener.results["started"]
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

        cev = self.client_listener.results["started"]
        self.assertEqual(len(cev), 3)
        self.assertEqual(cev[0].command_name, "listCollections")
        self.assertEqual(cev[0].database_name, "db")
        self.assertEqual(cev[1].command_name, "insert")
        self.assertEqual(cev[1].database_name, "db")
        self.assertEqual(cev[2].command_name, "find")
        self.assertEqual(cev[2].database_name, "db")

        cev = self.client_keyvault_listener.results["started"]
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

        cev = self.client_listener.results["started"]
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

        cev = self.client_listener.results["started"]
        self.assertEqual(len(cev), 1)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "db")

        cev = self.client_keyvault_listener.results["started"]
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

        cev = self.client_listener.results["started"]
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

        cev = self.client_listener.results["started"]
        self.assertEqual(len(cev), 3)
        self.assertEqual(cev[0].command_name, "listCollections")
        self.assertEqual(cev[0].database_name, "db")
        self.assertEqual(cev[1].command_name, "insert")
        self.assertEqual(cev[1].database_name, "db")
        self.assertEqual(cev[2].command_name, "find")
        self.assertEqual(cev[2].database_name, "db")

        cev = self.client_keyvault_listener.results["started"]
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

        cev = self.client_listener.results["started"]
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

        cev = self.client_listener.results["started"]
        self.assertEqual(len(cev), 1)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "db")

        cev = self.client_keyvault_listener.results["started"]
        self.assertEqual(len(cev), 1)
        self.assertEqual(cev[0].command_name, "find")
        self.assertEqual(cev[0].database_name, "keyvault")

        self.assertEqual(len(self.topology_listener.results["opened"]), 1)


# https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.rst#bypass-spawning-mongocryptd
class TestBypassSpawningMongocryptdProse(EncryptionIntegrationTest):
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


# https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#kms-tls-tests
class TestKmsTLSProse(EncryptionIntegrationTest):
    @unittest.skipUnless(any(AWS_CREDS.values()), "AWS environment credentials are not set")
    def setUp(self):
        super(TestKmsTLSProse, self).setUp()
        self.patch_system_certs(CA_PEM)
        self.client_encrypted = ClientEncryption(
            {"aws": AWS_CREDS}, "keyvault.datakeys", self.client, OPTS
        )
        self.addCleanup(self.client_encrypted.close)

    def test_invalid_kms_certificate_expired(self):
        key = {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/" "89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
            "endpoint": "mongodb://127.0.0.1:8000",
        }
        # Some examples:
        # certificate verify failed: certificate has expired (_ssl.c:1129)
        # amazon1-2018 Python 3.6: certificate verify failed (_ssl.c:852)
        with self.assertRaisesRegex(EncryptionError, "expired|certificate verify failed"):
            self.client_encrypted.create_data_key("aws", master_key=key)

    def test_invalid_hostname_in_kms_certificate(self):
        key = {
            "region": "us-east-1",
            "key": "arn:aws:kms:us-east-1:579766882180:key/" "89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
            "endpoint": "mongodb://127.0.0.1:8001",
        }
        # Some examples:
        # certificate verify failed: IP address mismatch, certificate is not valid for '127.0.0.1'. (_ssl.c:1129)"
        # hostname '127.0.0.1' doesn't match 'wronghost.com'
        with self.assertRaisesRegex(EncryptionError, "IP address mismatch|wronghost"):
            self.client_encrypted.create_data_key("aws", master_key=key)


if __name__ == "__main__":
    unittest.main()
