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

import socket
import sys

sys.path[0:0] = [""]

from pymongo.errors import ConfigurationError
from pymongo.mongo_client import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts, _HAVE_PYMONGOCRYPT

from test import unittest, PyMongoTestCase


def get_client_opts(client):
    return client._MongoClient__options


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
        self.assertEqual(get_client_opts(client).auto_encryption_opts, None)

        client = MongoClient(auto_encryption_opts=None, connect=False)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, None)

    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, 'pymongocrypt is not installed')
    def test_kwargs(self):
        opts = AutoEncryptionOpts({}, 'admin.datakeys')
        client = MongoClient(auto_encryption_opts=opts, connect=False)
        self.assertEqual(get_client_opts(client).auto_encryption_opts, opts)


if __name__ == "__main__":
    unittest.main()
