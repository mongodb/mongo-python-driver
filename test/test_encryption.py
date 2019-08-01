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

import os
import socket
import sys

sys.path[0:0] = [""]

from bson import BSON, json_util
from bson.binary import STANDARD, Binary
from bson.codec_options import CodecOptions
from bson.json_util import JSONOptions
from bson.raw_bson import RawBSONDocument
from bson.son import SON

from pymongo.errors import ConfigurationError
from pymongo.mongo_client import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts, _HAVE_PYMONGOCRYPT
from pymongo.write_concern import WriteConcern

from test import unittest, IntegrationTest, PyMongoTestCase, client_context
from test.utils import wait_until


if _HAVE_PYMONGOCRYPT:
    # Load the mongocrypt library.
    from pymongocrypt.binding import init
    init(os.environ.get('MONGOCRYPT_LIB', 'mongocrypt'))


def get_client_opts(client):
    return client._MongoClient__options


KMS_PROVIDERS = {'local': {'key': b'\x00'*96}}


class TestAutoEncryptionOpts(PyMongoTestCase):
    @unittest.skipIf(_HAVE_PYMONGOCRYPT, 'pymongocrypt is installed')
    def test_init_requires_pymongocrypt(self):
        with self.assertRaises(ConfigurationError):
            AutoEncryptionOpts({}, 'admin.datakeys')

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, 'pymongocrypt is not installed')
    def test_init(self):
        opts = AutoEncryptionOpts({}, 'admin.datakeys')
        self.assertEqual(opts._kms_providers, {})
        self.assertEqual(opts._key_vault_namespace, 'admin.datakeys')
        self.assertEqual(opts._key_vault_client, None)
        self.assertEqual(opts._schema_map, None)
        self.assertEqual(opts._bypass_auto_encryption, False)

        if hasattr(socket, 'AF_UNIX'):
            self.assertEqual(
                opts._mongocryptd_uri, 'mongodb://%2Ftmp%2Fmongocryptd.sock')
        else:
            self.assertEqual(
                opts._mongocryptd_uri, 'mongodb://localhost:27020')

        self.assertEqual(opts._mongocryptd_bypass_spawn, False)
        self.assertEqual(opts._mongocryptd_spawn_path, 'mongocryptd')
        self.assertEqual(
            opts._mongocryptd_spawn_args, ['--idleShutdownTimeoutSecs=60'])

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, 'pymongocrypt is not installed')
    def test_init_spawn_args(self):
        # User can override idleShutdownTimeoutSecs
        opts = AutoEncryptionOpts(
            {}, 'admin.datakeys',
            mongocryptd_spawn_args=['--idleShutdownTimeoutSecs=88'])
        self.assertEqual(
            opts._mongocryptd_spawn_args, ['--idleShutdownTimeoutSecs=88'])

        # idleShutdownTimeoutSecs is added by default
        opts = AutoEncryptionOpts(
            {}, 'admin.datakeys', mongocryptd_spawn_args=[])
        self.assertEqual(
            opts._mongocryptd_spawn_args, ['--idleShutdownTimeoutSecs=60'])

        # Also added when other options are given
        opts = AutoEncryptionOpts(
            {}, 'admin.datakeys',
            mongocryptd_spawn_args=['--quiet', '--port=27020'])
        self.assertEqual(
            opts._mongocryptd_spawn_args,
            ['--quiet', '--port=27020', '--idleShutdownTimeoutSecs=60'])


class TestClientOptions(PyMongoTestCase):
    def test_default(self):
        client = MongoClient(connect=False)
        self.addCleanup(client.close)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, None)

        client = MongoClient(auto_encryption_opts=None, connect=False)
        self.addCleanup(client.close)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, None)

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, 'pymongocrypt is not installed')
    def test_kwargs(self):
        opts = AutoEncryptionOpts(KMS_PROVIDERS, 'admin.datakeys')
        client = MongoClient(auto_encryption_opts=opts, connect=False)
        self.addCleanup(client.close)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, opts)


class EncryptionIntegrationTest(IntegrationTest):
    """Base class for encryption integration tests."""

    @classmethod
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, 'pymongocrypt is not installed')
    @client_context.require_version_min(4, 2, -1)
    def setUpClass(cls):
        super(EncryptionIntegrationTest, cls).setUpClass()


# Location of JSON test files.
TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'client-side-encryption')

OPTS = CodecOptions(uuid_representation=STANDARD)

# Use SON to preserve the order of fields while parsing json.
JSON_OPTS = JSONOptions(document_class=SON, uuid_representation=STANDARD)


def read(filename):
    with open(os.path.join(TEST_PATH, filename)) as fp:
        return fp.read()


def json_data(filename):
    return json_util.loads(read(filename), json_options=JSON_OPTS)


def bson_data(filename):
    return BSON.encode(json_data(filename), codec_options=OPTS)


class TestClientSimple(EncryptionIntegrationTest):

    def _test_auto_encrypt(self, opts):
        client = MongoClient(auto_encryption_opts=opts)
        self.addCleanup(client.close)

        # Create the encrypted field's data key.
        key_vault = self.client.admin.get_collection(
            'datakeys', codec_options=OPTS)
        data_key = RawBSONDocument(bson_data('key-document-local.json'))
        key_vault.insert_one(data_key)
        self.addCleanup(key_vault.drop)

        # Collection.insert_one/insert_many auto encrypts.
        docs = [{'_id': 0, 'ssn': '000'},
                {'_id': 1, 'ssn': '111'},
                {'_id': 2, 'ssn': '222'},
                {'_id': 3, 'ssn': '333'},
                {'_id': 4, 'ssn': '444'},
                {'_id': 5, 'ssn': '555'}]
        encrypted_coll = client.pymongo_test.test
        encrypted_coll.insert_one(docs[0])
        encrypted_coll.insert_many(docs[1:3])
        unack = encrypted_coll.with_options(write_concern=WriteConcern(w=0))
        unack.insert_one(docs[3])
        unack.insert_many(docs[4:], ordered=False)
        wait_until(lambda: self.db.test.count_documents({}) == len(docs),
                   'insert documents with w=0')

        # Database.command auto decrypts.
        res = client.pymongo_test.command(
            'find', 'test', filter={'ssn': '000'})
        decrypted_docs = res['cursor']['firstBatch']
        self.assertEqual(decrypted_docs, [{'_id': 0, 'ssn': '000'}])

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
        decrypted_ssns = encrypted_coll.distinct('ssn')
        self.assertEqual(decrypted_ssns, [d['ssn'] for d in docs])

        # Make sure the field is actually encrypted.
        for encrypted_doc in self.db.test.find():
            self.assertIsInstance(encrypted_doc['_id'], int)
            self.assertIsInstance(encrypted_doc['ssn'], Binary)
            self.assertEqual(encrypted_doc['ssn'].subtype, 6)

    def test_auto_encrypt(self):
        # Configure the encrypted field via jsonSchema.
        json_schema = json_data('schema.json')
        coll = self.db.create_collection(
            'test', validator={'$jsonSchema': json_schema}, codec_options=OPTS)
        self.addCleanup(coll.drop)

        opts = AutoEncryptionOpts(KMS_PROVIDERS, 'admin.datakeys')
        self._test_auto_encrypt(opts)

    def test_auto_encrypt_local_schema_map(self):
        # Configure the encrypted field via the local schema_map option.
        schemas = {'pymongo_test.test': json_data('schema.json')}
        opts = AutoEncryptionOpts(
            KMS_PROVIDERS, 'admin.datakeys', schema_map=schemas)

        self._test_auto_encrypt(opts)


if __name__ == "__main__":
    unittest.main()
