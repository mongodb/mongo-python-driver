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

"""Client side encryption implementation."""

import subprocess
import weakref

from pymongocrypt.auto_encrypter import AutoEncrypter
from pymongocrypt.errors import MongoCryptError
from pymongocrypt.mongocrypt import MongoCryptOptions
from pymongocrypt.state_machine import MongoCryptCallback

from bson import _bson_to_dict, _dict_to_bson
from bson.binary import STANDARD
from bson.codec_options import CodecOptions
from bson.raw_bson import (DEFAULT_RAW_BSON_OPTIONS,
                           RawBSONDocument,
                           _inflate_bson)
from bson.son import SON

from pymongo.errors import ServerSelectionTimeoutError
from pymongo.mongo_client import MongoClient
from pymongo.pool import _configured_socket, PoolOptions
from pymongo.ssl_support import get_ssl_context


_HTTPS_PORT = 443
_KMS_CONNECT_TIMEOUT = 10  # TODO: CDRIVER-3262 will define this value.
_MONGOCRYPTD_TIMEOUT_MS = 1000

_DATA_KEY_OPTS = CodecOptions(document_class=SON, uuid_representation=STANDARD)
# Use RawBSONDocument codec options to avoid needlessly decoding
# documents from the key vault.
_KEY_VAULT_OPTS = CodecOptions(document_class=RawBSONDocument,
                               uuid_representation=STANDARD)


class _EncryptionIO(MongoCryptCallback):
    def __init__(self, client, key_vault_coll, mongocryptd_client, opts):
        """Internal class to perform I/O on behalf of pymongocrypt."""
        # Use a weak ref to break reference cycle.
        self.client_ref = weakref.ref(client)
        self.key_vault_coll = key_vault_coll.with_options(
            codec_options=_KEY_VAULT_OPTS)
        self.mongocryptd_client = mongocryptd_client
        self.opts = opts
        self._spawned = False

    def kms_request(self, kms_context):
        """Complete a KMS request.

        :Parameters:
          - `kms_context`: A :class:`MongoCryptKmsContext`.

        :Returns:
          None
        """
        endpoint = kms_context.endpoint
        message = kms_context.message
        ctx = get_ssl_context(None, None, None, None, None, None, True)
        opts = PoolOptions(connect_timeout=_KMS_CONNECT_TIMEOUT,
                           socket_timeout=_KMS_CONNECT_TIMEOUT,
                           ssl_context=ctx)
        try:
            conn = _configured_socket((endpoint, _HTTPS_PORT), opts)
            conn.sendall(message)
        except Exception as exc:
            raise MongoCryptError(str(exc))

        while kms_context.bytes_needed > 0:
            data = conn.recv(kms_context.bytes_needed)
            kms_context.feed(data)

    def collection_info(self, database, filter):
        """Get the collection info for a namespace.

        The returned collection info is passed to libmongocrypt which reads
        the JSON schema.

        :Parameters:
          - `database`: The database on which to run listCollections.
          - `filter`: The filter to pass to listCollections.

        :Returns:
          The first document from the listCollections command response as BSON.
        """
        with self.client_ref()[database].list_collections(
                filter=RawBSONDocument(filter)) as cursor:
            for doc in cursor:
                return _dict_to_bson(doc, False, _DATA_KEY_OPTS)

    def spawn(self):
        """Spawn mongocryptd.

        Note this method is thread safe; at most one mongocryptd will start
        successfully.
        """
        self._spawned = True
        args = [self.opts._mongocryptd_spawn_path or 'mongocryptd']
        args.extend(self.opts._mongocryptd_spawn_args)
        subprocess.Popen(args)

    def mark_command(self, database, cmd):
        """Mark a command for encryption.

        :Parameters:
          - `database`: The database on which to run this command.
          - `cmd`: The BSON command to run.

        :Returns:
          The marked command response from mongocryptd.
        """
        if not self._spawned and not self.opts._mongocryptd_bypass_spawn:
            self.spawn()
        # Database.command only supports mutable mappings so we need to decode
        # the raw BSON command first.
        inflated_cmd = _inflate_bson(cmd, DEFAULT_RAW_BSON_OPTIONS)
        try:
            res = self.mongocryptd_client[database].command(
                inflated_cmd,
                codec_options=DEFAULT_RAW_BSON_OPTIONS)
        except ServerSelectionTimeoutError:
            if self.opts._mongocryptd_bypass_spawn:
                raise
            self.spawn()
            res = self.mongocryptd_client[database].command(
                inflated_cmd,
                codec_options=DEFAULT_RAW_BSON_OPTIONS)
        return res.raw

    def fetch_keys(self, filter):
        """Yields one or more keys from the key vault.

        :Parameters:
          - `filter`: The filter to pass to find.

        :Returns:
          A generator which yields the requested keys from the key vault.
        """
        with self.key_vault_coll.find(RawBSONDocument(filter)) as cursor:
            for key in cursor:
                yield key.raw

    def insert_data_key(self, data_key):
        """Insert a data key into the key vault.

        :Parameters:
          - `data_key`: The data key document to insert.

        :Returns:
          The _id of the inserted data key document.
        """
        # insert does not return the inserted _id when given a RawBSONDocument.
        doc = _bson_to_dict(data_key, _DATA_KEY_OPTS)
        res = self.key_vault_coll.insert_one(doc)
        return res.inserted_id

    def close(self):
        """Release resources.

        Note it is not safe to call this method from __del__ or any GC hooks.
        """
        self.client_ref = None
        self.key_vault_coll = None
        self.mongocryptd_client.close()
        self.mongocryptd_client = None


class _Encrypter(object):
    def __init__(self, io_callbacks, opts):
        """Encrypts and decrypts MongoDB commands.

        This class is used to support automatic encryption and decryption of
        MongoDB commands.

        :Parameters:
          - `io_callbacks`: A :class:`MongoCryptCallback`.
          - `opts`: The encrypted client's :class:`AutoEncryptionOpts`.
        """
        if opts._schema_map is None:
            schema_map = None
        else:
            schema_map = _dict_to_bson(opts._schema_map, False, _DATA_KEY_OPTS)
        self._auto_encrypter = AutoEncrypter(io_callbacks, MongoCryptOptions(
            opts._kms_providers, schema_map))
        self._bypass_auto_encryption = opts._bypass_auto_encryption

    def encrypt(self, database, cmd, check_keys, codec_options):
        """Encrypt a MongoDB command.

        :Parameters:
          - `database`: The database for this command.
          - `cmd`: A command document.
          - `check_keys`: If True, check `cmd` for invalid keys.
          - `codec_options`: The CodecOptions to use while encoding `cmd`.

        :Returns:
          The encrypted command to execute.
        """
        # Workaround for $clusterTime which is incompatible with check_keys.
        cluster_time = check_keys and cmd.pop('$clusterTime', None)
        encrypted_cmd = self._auto_encrypter.encrypt(
            database, _dict_to_bson(cmd, check_keys, codec_options))
        # TODO: PYTHON-1922 avoid decoding the encrypted_cmd.
        encrypt_cmd = _inflate_bson(encrypted_cmd, DEFAULT_RAW_BSON_OPTIONS)
        if cluster_time:
            encrypt_cmd['$clusterTime'] = cluster_time
        return encrypt_cmd

    def decrypt(self, response):
        """Decrypt a MongoDB command response.

        :Parameters:
          - `response`: A MongoDB command response as BSON.

        :Returns:
          The decrypted command response.
        """
        return self._auto_encrypter.decrypt(response)

    def close(self):
        """Cleanup resources."""
        self._auto_encrypter.close()

    @staticmethod
    def create(client, opts):
        """Create a _CommandEncyptor for a client.

        :Parameters:
          - `client`: The encrypted MongoClient.
          - `opts`: The encrypted client's :class:`AutoEncryptionOpts`.

        :Returns:
          A :class:`_CommandEncrypter` for this client.
        """
        key_vault_client = opts._key_vault_client or client
        db, coll = opts._key_vault_namespace.split('.', 1)
        key_vault_coll = key_vault_client[db][coll]

        mongocryptd_client = MongoClient(
            opts._mongocryptd_uri, connect=False,
            serverSelectionTimeoutMS=_MONGOCRYPTD_TIMEOUT_MS)

        io_callbacks = _EncryptionIO(
            client, key_vault_coll, mongocryptd_client, opts)
        return _Encrypter(io_callbacks, opts)
