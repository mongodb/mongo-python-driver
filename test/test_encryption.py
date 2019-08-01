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
from test.utils import TestCreator, camel_to_snake_args, wait_until
from test.utils_spec_runner import SpecRunner


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
BASE = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'client-side-encryption')
CUSTOM_PATH = os.path.join(BASE, 'custom')
SPEC_PATH = os.path.join(BASE, 'spec')

OPTS = CodecOptions(uuid_representation=STANDARD)

# Use SON to preserve the order of fields while parsing json.
JSON_OPTS = JSONOptions(document_class=SON, uuid_representation=STANDARD)


def read(filename):
    with open(os.path.join(CUSTOM_PATH, filename)) as fp:
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


# Spec tests

AWS_CREDS = {
    'accessKeyId': os.environ.get('FLE_AWS_KEY', ''),
    'secretAccessKey': os.environ.get('FLE_AWS_SECRET', '')
}


class TestSpec(SpecRunner):

    @classmethod
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, 'pymongocrypt is not installed')
    def setUpClass(cls):
        super(TestSpec, cls).setUpClass()

    def parse_auto_encrypt_opts(self, opts):
        """Parse clientOptions.autoEncryptOpts."""
        opts = camel_to_snake_args(opts)
        kms_providers = opts['kms_providers']
        if 'aws' in kms_providers:
            kms_providers['aws'] = AWS_CREDS
            if not any(AWS_CREDS.values()):
                self.skipTest('AWS environment credentials are not set')
        if 'key_vault_namespace' not in opts:
            opts['key_vault_namespace'] = 'admin.datakeys'
        opts = dict(opts)
        return AutoEncryptionOpts(**opts)

    def parse_client_options(self, opts):
        """Override clientOptions parsing to support autoEncryptOpts."""
        encrypt_opts = opts.pop('autoEncryptOpts')
        if encrypt_opts:
            opts['auto_encryption_opts'] = self.parse_auto_encrypt_opts(
                encrypt_opts)

        return super(TestSpec, self).parse_client_options(opts)

    def get_object_name(self, op):
        """Default object is collection."""
        return op.get('object', 'collection')

    def maybe_skip_scenario(self, test):
        super(TestSpec, self).maybe_skip_scenario(test)
        if 'type=symbol' in test['description'].lower():
            raise unittest.SkipTest(
                'PyMongo does not support the symbol type')

    def setup_scenario(self, scenario_def):
        """Override a test's setup."""
        key_vault_data = scenario_def['key_vault_data']
        if key_vault_data:
            coll = client_context.client.get_database(
                'admin',
                write_concern=WriteConcern(w='majority'),
                codec_options=OPTS)['datakeys']
            coll.drop()
            coll.insert_many(key_vault_data)

        db_name = self.get_scenario_db_name(scenario_def)
        coll_name = self.get_scenario_coll_name(scenario_def)
        db = client_context.client.get_database(
            db_name, write_concern=WriteConcern(w='majority'),
            codec_options=OPTS)
        coll = db[coll_name]
        coll.drop()
        json_schema = scenario_def['json_schema']
        if json_schema:
            db.create_collection(
                coll_name,
                validator={'$jsonSchema': json_schema}, codec_options=OPTS)
        else:
            db.create_collection(coll_name)

        if scenario_def['data']:
            # Load data.
            coll.insert_many(scenario_def['data'])

    def allowable_errors(self, op):
        """Override expected error classes."""
        errors = super(TestSpec, self).allowable_errors(op)
        # An updateOne test expects encryption to error when no $ operator
        # appears but pymongo raises a client side ValueError in this case.
        if op['name'] == 'updateOne':
            errors += (ValueError,)
        return errors


def create_test(scenario_def, test, name):
    @client_context.require_test_commands
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


test_creator = TestCreator(create_test, TestSpec, SPEC_PATH)
test_creator.create_tests()


if __name__ == "__main__":
    unittest.main()
