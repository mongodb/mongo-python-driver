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

"""Support for explicit client-side field level encryption."""

import contextlib
import enum
import socket
import uuid
import weakref
from typing import Any, Iterable, Mapping, Optional, Sequence

try:
    from pymongocrypt.auto_encrypter import AutoEncrypter
    from pymongocrypt.errors import MongoCryptError  # noqa: F401
    from pymongocrypt.explicit_encrypter import ExplicitEncrypter
    from pymongocrypt.mongocrypt import MongoCryptOptions, RewrapManyDataKeyResult
    from pymongocrypt.state_machine import MongoCryptCallback

    _HAVE_PYMONGOCRYPT = True
except ImportError:
    _HAVE_PYMONGOCRYPT = False
    MongoCryptCallback = object

from bson import _dict_to_bson, decode, encode
from bson.binary import STANDARD, UUID_SUBTYPE, Binary
from bson.codec_options import CodecOptions
from bson.errors import BSONError
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument, _inflate_bson
from bson.son import SON
from pymongo import _csot
from pymongo.daemon import _spawn_daemon
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo.errors import (
    ConfigurationError,
    EncryptionError,
    InvalidOperation,
    ServerSelectionTimeoutError,
)
from pymongo.mongo_client import MongoClient
from pymongo.network import BLOCKING_IO_ERRORS
from pymongo.operations import ReplaceOne
from pymongo.pool import PoolOptions, _configured_socket
from pymongo.read_concern import ReadConcern
from pymongo.ssl_support import get_ssl_context
from pymongo.uri_parser import parse_host
from pymongo.write_concern import WriteConcern

_HTTPS_PORT = 443
_KMS_CONNECT_TIMEOUT = 10  # TODO: CDRIVER-3262 will define this value.
_MONGOCRYPTD_TIMEOUT_MS = 10000


# TODO: add key_material to _DATA_KEY_OPTS
# keyMaterial is used to encrypt data. If omitted, keyMaterial is generated from
# a cryptographically secure random source.
# keyMaterial: Optional<Array[byte]>


_DATA_KEY_OPTS: CodecOptions = CodecOptions(document_class=SON, uuid_representation=STANDARD)
# Use RawBSONDocument codec options to avoid needlessly decoding
# documents from the key vault.
_KEY_VAULT_OPTS = CodecOptions(document_class=RawBSONDocument, uuid_representation=STANDARD)


@contextlib.contextmanager
def _wrap_encryption_errors():
    """Context manager to wrap encryption related errors."""
    try:
        yield
    except BSONError:
        # BSON encoding/decoding errors are unrelated to encryption so
        # we should propagate them unchanged.
        raise
    except Exception as exc:
        raise EncryptionError(exc)


class _EncryptionIO(MongoCryptCallback):  # type: ignore
    def __init__(self, client, key_vault_coll, mongocryptd_client, opts):
        """Internal class to perform I/O on behalf of pymongocrypt."""
        self.client_ref: Any
        # Use a weak ref to break reference cycle.
        if client is not None:
            self.client_ref = weakref.ref(client)
        else:
            self.client_ref = None
        self.key_vault_coll = key_vault_coll.with_options(
            codec_options=_KEY_VAULT_OPTS,
            read_concern=ReadConcern(level="majority"),
            write_concern=WriteConcern(w="majority"),
        )
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
        provider = kms_context.kms_provider
        ctx = self.opts._kms_ssl_contexts.get(provider)
        if ctx is None:
            # Enable strict certificate verification, OCSP, match hostname, and
            # SNI using the system default CA certificates.
            ctx = get_ssl_context(
                None,  # certfile
                None,  # passphrase
                None,  # ca_certs
                None,  # crlfile
                False,  # allow_invalid_certificates
                False,  # allow_invalid_hostnames
                False,
            )  # disable_ocsp_endpoint_check
        # CSOT: set timeout for socket creation.
        connect_timeout = max(_csot.clamp_remaining(_KMS_CONNECT_TIMEOUT), 0.001)
        opts = PoolOptions(
            connect_timeout=connect_timeout,
            socket_timeout=connect_timeout,
            ssl_context=ctx,
        )
        host, port = parse_host(endpoint, _HTTPS_PORT)
        conn = _configured_socket((host, port), opts)
        try:
            conn.sendall(message)
            while kms_context.bytes_needed > 0:
                # CSOT: update timeout.
                conn.settimeout(max(_csot.clamp_remaining(_KMS_CONNECT_TIMEOUT), 0))
                data = conn.recv(kms_context.bytes_needed)
                if not data:
                    raise OSError("KMS connection closed")
                kms_context.feed(data)
        except BLOCKING_IO_ERRORS:
            raise socket.timeout("timed out")
        finally:
            conn.close()

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
        with self.client_ref()[database].list_collections(filter=RawBSONDocument(filter)) as cursor:
            for doc in cursor:
                return _dict_to_bson(doc, False, _DATA_KEY_OPTS)

    def spawn(self):
        """Spawn mongocryptd.

        Note this method is thread safe; at most one mongocryptd will start
        successfully.
        """
        self._spawned = True
        args = [self.opts._mongocryptd_spawn_path or "mongocryptd"]
        args.extend(self.opts._mongocryptd_spawn_args)
        _spawn_daemon(args)

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
                inflated_cmd, codec_options=DEFAULT_RAW_BSON_OPTIONS
            )
        except ServerSelectionTimeoutError:
            if self.opts._mongocryptd_bypass_spawn:
                raise
            self.spawn()
            res = self.mongocryptd_client[database].command(
                inflated_cmd, codec_options=DEFAULT_RAW_BSON_OPTIONS
            )
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
        raw_doc = RawBSONDocument(data_key, _KEY_VAULT_OPTS)
        data_key_id = raw_doc.get("_id")
        if not isinstance(data_key_id, uuid.UUID):
            raise TypeError("data_key _id must be a UUID")

        self.key_vault_coll.insert_one(raw_doc)
        return Binary(data_key_id.bytes, subtype=UUID_SUBTYPE)

    def rewrap_many_data_key(self, data_keys):
        """**Experimental** Decrypts multiple data keys and (re-)encrypts them.

        :Parameters:
            `data_keys`: The data keys to rewrap.

        :Returns:
           A :class:`RewrapManyDataKeyResult`.
        """
        replacements = []
        for key in data_keys:
            op = ReplaceOne({"_id": key.id}, key)
            replacements.append(op)
        result = self.key_vault_coll.bulk_write(replacements)
        return RewrapManyDataKeyResult(result)

    def bson_encode(self, doc):
        """Encode a document to BSON.

        A document can be any mapping type (like :class:`dict`).

        :Parameters:
          - `doc`: mapping type representing a document

        :Returns:
          The encoded BSON bytes.
        """
        return encode(doc)

    def close(self):
        """Release resources.

        Note it is not safe to call this method from __del__ or any GC hooks.
        """
        self.client_ref = None
        self.key_vault_coll = None
        if self.mongocryptd_client:
            self.mongocryptd_client.close()
            self.mongocryptd_client = None


class _Encrypter(object):
    """Encrypts and decrypts MongoDB commands.

    This class is used to support automatic encryption and decryption of
    MongoDB commands."""

    def __init__(self, client, opts):
        """Create a _Encrypter for a client.

        :Parameters:
          - `client`: The encrypted MongoClient.
          - `opts`: The encrypted client's :class:`AutoEncryptionOpts`.
        """
        if opts._schema_map is None:
            schema_map = None
        else:
            schema_map = _dict_to_bson(opts._schema_map, False, _DATA_KEY_OPTS)

        if opts._encrypted_fields_map is None:
            encrypted_fields_map = None
        else:
            encrypted_fields_map = _dict_to_bson(opts._encrypted_fields_map, False, _DATA_KEY_OPTS)
        self._bypass_auto_encryption = opts._bypass_auto_encryption
        self._internal_client = None

        def _get_internal_client(encrypter, mongo_client):
            if mongo_client.options.pool_options.max_pool_size is None:
                # Unlimited pool size, use the same client.
                return mongo_client
            # Else - limited pool size, use an internal client.
            if encrypter._internal_client is not None:
                return encrypter._internal_client
            internal_client = mongo_client._duplicate(minPoolSize=0, auto_encryption_opts=None)
            encrypter._internal_client = internal_client
            return internal_client

        if opts._key_vault_client is not None:
            key_vault_client = opts._key_vault_client
        else:
            key_vault_client = _get_internal_client(self, client)

        if opts._bypass_auto_encryption:
            metadata_client = None
        else:
            metadata_client = _get_internal_client(self, client)

        db, coll = opts._key_vault_namespace.split(".", 1)
        key_vault_coll = key_vault_client[db][coll]

        mongocryptd_client: MongoClient = MongoClient(
            opts._mongocryptd_uri, connect=False, serverSelectionTimeoutMS=_MONGOCRYPTD_TIMEOUT_MS
        )

        io_callbacks = _EncryptionIO(metadata_client, key_vault_coll, mongocryptd_client, opts)
        self._auto_encrypter = AutoEncrypter(
            io_callbacks,
            MongoCryptOptions(
                opts._kms_providers,
                schema_map,
                crypt_shared_lib_path=opts._crypt_shared_lib_path,
                crypt_shared_lib_required=opts._crypt_shared_lib_required,
                bypass_encryption=opts._bypass_auto_encryption,
                encrypted_fields_map=encrypted_fields_map,
                bypass_query_analysis=opts._bypass_query_analysis,
            ),
        )
        self._closed = False

    def encrypt(self, database, cmd, codec_options):
        """Encrypt a MongoDB command.

        :Parameters:
          - `database`: The database for this command.
          - `cmd`: A command document.
          - `codec_options`: The CodecOptions to use while encoding `cmd`.

        :Returns:
          The encrypted command to execute.
        """
        self._check_closed()
        encoded_cmd = _dict_to_bson(cmd, False, codec_options)
        with _wrap_encryption_errors():
            encrypted_cmd = self._auto_encrypter.encrypt(database, encoded_cmd)
            # TODO: PYTHON-1922 avoid decoding the encrypted_cmd.
            encrypt_cmd = _inflate_bson(encrypted_cmd, DEFAULT_RAW_BSON_OPTIONS)
            return encrypt_cmd

    def decrypt(self, response):
        """Decrypt a MongoDB command response.

        :Parameters:
          - `response`: A MongoDB command response as BSON.

        :Returns:
          The decrypted command response.
        """
        self._check_closed()
        with _wrap_encryption_errors():
            return self._auto_encrypter.decrypt(response)

    def _check_closed(self):
        if self._closed:
            raise InvalidOperation("Cannot use MongoClient after close")

    def close(self):
        """Cleanup resources."""
        self._closed = True
        self._auto_encrypter.close()
        if self._internal_client:
            self._internal_client.close()
            self._internal_client = None


class Algorithm(str, enum.Enum):
    """An enum that defines the supported encryption algorithms."""

    AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
    """AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic."""
    AEAD_AES_256_CBC_HMAC_SHA_512_Random = "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
    """AEAD_AES_256_CBC_HMAC_SHA_512_Random."""
    INDEXED = "Indexed"
    """Indexed.

    .. note:: Support for Queryable Encryption is in beta.
       Backwards-breaking changes may be made before the final release.

    .. versionadded:: 4.2
    """
    UNINDEXED = "Unindexed"
    """Unindexed.

    .. note:: Support for Queryable Encryption is in beta.
       Backwards-breaking changes may be made before the final release.

    .. versionadded:: 4.2
    """


class QueryType(enum.IntEnum):
    """**(BETA)** An enum that defines the supported values for explicit encryption query_type.

    .. note:: Support for Queryable Encryption is in beta.
       Backwards-breaking changes may be made before the final release.

    .. versionadded:: 4.2
    """

    EQUALITY = 1
    """Used to encrypt a value for an equality query."""


class ClientEncryption(object):
    """Explicit client-side field level encryption."""

    def __init__(
        self,
        kms_providers: Mapping[str, Any],
        key_vault_namespace: str,
        key_vault_client: MongoClient,
        codec_options: CodecOptions,
        kms_tls_options: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Explicit client-side field level encryption.

        The ClientEncryption class encapsulates explicit operations on a key
        vault collection that cannot be done directly on a MongoClient. Similar
        to configuring auto encryption on a MongoClient, it is constructed with
        a MongoClient (to a MongoDB cluster containing the key vault
        collection), KMS provider configuration, and keyVaultNamespace. It
        provides an API for explicitly encrypting and decrypting values, and
        creating data keys. It does not provide an API to query keys from the
        key vault collection, as this can be done directly on the MongoClient.

        See :ref:`explicit-client-side-encryption` for an example.

        :Parameters:
          - `kms_providers`: Map of KMS provider options. The `kms_providers`
            map values differ by provider:

              - `aws`: Map with "accessKeyId" and "secretAccessKey" as strings.
                These are the AWS access key ID and AWS secret access key used
                to generate KMS messages. An optional "sessionToken" may be
                included to support temporary AWS credentials.
              - `azure`: Map with "tenantId", "clientId", and "clientSecret" as
                strings. Additionally, "identityPlatformEndpoint" may also be
                specified as a string (defaults to 'login.microsoftonline.com').
                These are the Azure Active Directory credentials used to
                generate Azure Key Vault messages.
              - `gcp`: Map with "email" as a string and "privateKey"
                as `bytes` or a base64 encoded string.
                Additionally, "endpoint" may also be specified as a string
                (defaults to 'oauth2.googleapis.com'). These are the
                credentials used to generate Google Cloud KMS messages.
              - `kmip`: Map with "endpoint" as a host with required port.
                For example: ``{"endpoint": "example.com:443"}``.
              - `local`: Map with "key" as `bytes` (96 bytes in length) or
                a base64 encoded string which decodes
                to 96 bytes. "key" is the master key used to encrypt/decrypt
                data keys. This key should be generated and stored as securely
                as possible.

          - `key_vault_namespace`: The namespace for the key vault collection.
            The key vault collection contains all data keys used for encryption
            and decryption. Data keys are stored as documents in this MongoDB
            collection. Data keys are protected with encryption by a KMS
            provider.
          - `key_vault_client`: A MongoClient connected to a MongoDB cluster
            containing the `key_vault_namespace` collection.
          - `codec_options`: An instance of
            :class:`~bson.codec_options.CodecOptions` to use when encoding a
            value for encryption and decoding the decrypted BSON value. This
            should be the same CodecOptions instance configured on the
            MongoClient, Database, or Collection used to access application
            data.
          - `kms_tls_options` (optional): A map of KMS provider names to TLS
            options to use when creating secure connections to KMS providers.
            Accepts the same TLS options as
            :class:`pymongo.mongo_client.MongoClient`. For example, to
            override the system default CA file::

              kms_tls_options={'kmip': {'tlsCAFile': certifi.where()}}

            Or to supply a client certificate::

              kms_tls_options={'kmip': {'tlsCertificateKeyFile': 'client.pem'}}

        .. versionchanged:: 4.0
           Added the `kms_tls_options` parameter and the "kmip" KMS provider.

        .. versionadded:: 3.9
        """
        if not _HAVE_PYMONGOCRYPT:
            raise ConfigurationError(
                "client-side field level encryption requires the pymongocrypt "
                "library: install a compatible version with: "
                "python -m pip install 'pymongo[encryption]'"
            )

        if not isinstance(codec_options, CodecOptions):
            raise TypeError("codec_options must be an instance of bson.codec_options.CodecOptions")

        self._kms_providers = kms_providers
        self._key_vault_namespace = key_vault_namespace
        self._key_vault_client = key_vault_client
        self._codec_options = codec_options

        db, coll = key_vault_namespace.split(".", 1)
        key_vault_coll = key_vault_client[db][coll]

        opts = AutoEncryptionOpts(
            kms_providers, key_vault_namespace, kms_tls_options=kms_tls_options
        )
        self._io_callbacks: Optional[_EncryptionIO] = _EncryptionIO(
            None, key_vault_coll, None, opts
        )
        self._encryption = ExplicitEncrypter(
            self._io_callbacks, MongoCryptOptions(kms_providers, None)
        )
        self._key_vault_coll = self._io_callbacks.key_vault_coll.with_options(
            codec_options=DEFAULT_RAW_BSON_OPTIONS,
        )

    def create_data_key(
        self,
        kms_provider: str,
        master_key: Optional[Mapping[str, Any]] = None,
        key_alt_names: Optional[Sequence[str]] = None,
    ) -> Binary:
        """Create and insert a new data key into the key vault collection.

        :Parameters:
          - `kms_provider`: The KMS provider to use. Supported values are
            "aws", "azure", "gcp", "kmip", and "local".
          - `master_key`: Identifies a KMS-specific key used to encrypt the
            new data key. If the kmsProvider is "local" the `master_key` is
            not applicable and may be omitted.

            If the `kms_provider` is "aws" it is required and has the
            following fields::

              - `region` (string): Required. The AWS region, e.g. "us-east-1".
              - `key` (string): Required. The Amazon Resource Name (ARN) to
                 the AWS customer.
              - `endpoint` (string): Optional. An alternate host to send KMS
                requests to. May include port number, e.g.
                "kms.us-east-1.amazonaws.com:443".

            If the `kms_provider` is "azure" it is required and has the
            following fields::

              - `keyVaultEndpoint` (string): Required. Host with optional
                 port, e.g. "example.vault.azure.net".
              - `keyName` (string): Required. Key name in the key vault.
              - `keyVersion` (string): Optional. Version of the key to use.

            If the `kms_provider` is "gcp" it is required and has the
            following fields::

              - `projectId` (string): Required. The Google cloud project ID.
              - `location` (string): Required. The GCP location, e.g. "us-east1".
              - `keyRing` (string): Required. Name of the key ring that contains
                the key to use.
              - `keyName` (string): Required. Name of the key to use.
              - `keyVersion` (string): Optional. Version of the key to use.
              - `endpoint` (string): Optional. Host with optional port.
                Defaults to "cloudkms.googleapis.com".

            If the `kms_provider` is "kmip" it is optional and has the
            following fields::

              - `keyId` (string): Optional. `keyId` is the KMIP Unique
                Identifier to a 96 byte KMIP Secret Data managed object. If
                keyId is omitted, the driver creates a random 96 byte KMIP
                Secret Data managed object.
              - `endpoint` (string): Optional. Host with optional
                 port, e.g. "example.vault.azure.net:".

          - `key_alt_names` (optional): An optional list of string alternate
            names used to reference a key. If a key is created with alternate
            names, then encryption may refer to the key by the unique alternate
            name instead of by ``key_id``. The following example shows creating
            and referring to a data key by alternate name::

              client_encryption.create_data_key("local", keyAltNames=["name1"])
              # reference the key with the alternate name
              client_encryption.encrypt("457-55-5462", keyAltName="name1",
                                        algorithm=Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Random)

        :Returns:
          The ``_id`` of the created data key document as a
          :class:`~bson.binary.Binary` with subtype
          :data:`~bson.binary.UUID_SUBTYPE`.
        """
        self._check_closed()
        with _wrap_encryption_errors():
            return self._encryption.create_data_key(
                kms_provider, master_key=master_key, key_alt_names=key_alt_names
            )

    def create_key(
        self,
        kms_provider: str,
        master_key: Optional[Mapping[str, Any]] = None,
        key_alt_names: Optional[Sequence[str]] = None,
    ) -> Binary:
        """Create and insert a new key into the key vault collection.

        :Parameters:
          - `kms_provider`: The KMS provider to use. Supported values are
            "aws", "azure", "gcp", "kmip", and "local".
          - `master_key`: Identifies a KMS-specific key used to encrypt the
            new key. If the kmsProvider is "local" the `master_key` is
            not applicable and may be omitted.

            If the `kms_provider` is "aws" it is required and has the
            following fields::

              - `region` (string): Required. The AWS region, e.g. "us-east-1".
              - `key` (string): Required. The Amazon Resource Name (ARN) to
                 the AWS customer.
              - `endpoint` (string): Optional. An alternate host to send KMS
                requests to. May include port number, e.g.
                "kms.us-east-1.amazonaws.com:443".

            If the `kms_provider` is "azure" it is required and has the
            following fields::

              - `keyVaultEndpoint` (string): Required. Host with optional
                 port, e.g. "example.vault.azure.net".
              - `keyName` (string): Required. Key name in the key vault.
              - `keyVersion` (string): Optional. Version of the key to use.

            If the `kms_provider` is "gcp" it is required and has the
            following fields::

              - `projectId` (string): Required. The Google cloud project ID.
              - `location` (string): Required. The GCP location, e.g. "us-east1".
              - `keyRing` (string): Required. Name of the key ring that contains
                the key to use.
              - `keyName` (string): Required. Name of the key to use.
              - `keyVersion` (string): Optional. Version of the key to use.
              - `endpoint` (string): Optional. Host with optional port.
                Defaults to "cloudkms.googleapis.com".

            If the `kms_provider` is "kmip" it is optional and has the
            following fields::

              - `keyId` (string): Optional. `keyId` is the KMIP Unique
                Identifier to a 96 byte KMIP Secret Data managed object. If
                keyId is omitted, the driver creates a random 96 byte KMIP
                Secret Data managed object.
              - `endpoint` (string): Optional. Host with optional
                 port, e.g. "example.vault.azure.net:".

          - `key_alt_names` (optional): An optional list of string alternate
            names used to reference a key. If a key is created with alternate
            names, then encryption may refer to the key by the unique alternate
            name instead of by ``key_id``. The following example shows creating
            and referring to a key by alternate name::

              client_encryption.create_key("local", keyAltNames=["name1"])
              # reference the key with the alternate name
              client_encryption.encrypt("457-55-5462", keyAltName="name1",
                                        algorithm=Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Random)

        :Returns:
          The ``_id`` of the created key document as a
          :class:`~bson.binary.Binary` with subtype
          :data:`~bson.binary.UUID_SUBTYPE`.

        .. versionadded:: 4.2
            Added create_key as an alias to create_data_key to support
            queryable encryption API.
        """
        return self.create_data_key(
            kms_provider, master_key=master_key, key_alt_names=key_alt_names
        )

    def encrypt(
        self,
        value: Any,
        algorithm: str,
        key_id: Optional[Binary] = None,
        key_alt_name: Optional[str] = None,
        index_key_id: Optional[Binary] = None,
        query_type: Optional[int] = None,
        contention_factor: Optional[int] = None,
    ) -> Binary:
        """Encrypt a BSON value with a given key and algorithm.

        Note that exactly one of ``key_id`` or  ``key_alt_name`` must be
        provided.

        :Parameters:
          - `value`: The BSON value to encrypt.
          - `algorithm` (string): The encryption algorithm to use. See
            :class:`Algorithm` for some valid options.
          - `key_id`: Identifies a data key by ``_id`` which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).
          - `key_alt_name`: Identifies a key vault document by 'keyAltName'.
          - `index_key_id`: **(BETA)** The index key id to use for Queryable Encryption. Must be
            a :class:`~bson.binary.Binary` with subtype 4 (:attr:`~bson.binary.UUID_SUBTYPE`).
          - `query_type` (int): **(BETA)** The query type to execute. See
            :class:`QueryType` for valid options.
          - `contention_factor` (int): **(BETA)** The contention factor to use
            when the algorithm is :attr:`Algorithm.INDEXED`.

        .. note:: `index_key_id`, `query_type`, and `contention_factor` are part of the
           Queryable Encryption beta. Backwards-breaking changes may be made before the
           final release.

        :Returns:
          The encrypted value, a :class:`~bson.binary.Binary` with subtype 6.

        .. versionchanged:: 4.2
           Added the `index_key_id`, `query_type`, and `contention_factor` parameters.
        """
        self._check_closed()
        if key_id is not None and not (
            isinstance(key_id, Binary) and key_id.subtype == UUID_SUBTYPE
        ):
            raise TypeError("key_id must be a bson.binary.Binary with subtype 4")
        if index_key_id is not None and not (
            isinstance(index_key_id, Binary) and index_key_id.subtype == UUID_SUBTYPE
        ):
            raise TypeError("index_key_id must be a bson.binary.Binary with subtype 4")

        doc = encode({"v": value}, codec_options=self._codec_options)
        with _wrap_encryption_errors():
            encrypted_doc = self._encryption.encrypt(
                doc,
                algorithm,
                key_id=key_id,
                key_alt_name=key_alt_name,
                index_key_id=index_key_id,
                query_type=query_type,
                contention_factor=contention_factor,
            )
            return decode(encrypted_doc)["v"]  # type: ignore[index]

    def decrypt(self, value: Binary) -> Any:
        """Decrypt an encrypted value.

        :Parameters:
          - `value` (Binary): The encrypted value, a
            :class:`~bson.binary.Binary` with subtype 6.

        :Returns:
          The decrypted BSON value.
        """
        self._check_closed()
        if not (isinstance(value, Binary) and value.subtype == 6):
            raise TypeError("value to decrypt must be a bson.binary.Binary with subtype 6")

        with _wrap_encryption_errors():
            doc = encode({"v": value})
            decrypted_doc = self._encryption.decrypt(doc)
            return decode(decrypted_doc, codec_options=self._codec_options)["v"]

    def get_key(self, id):
        """Get a data key by id.

        :Parameters:
          - `id` (Binary): The UUID of a key a which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).

        :Returns:
          The key document.
        """
        return self._key_vault_coll.find_one({"_id": id})

    def delete_key(self, id: Binary) -> Any:
        """Delete a key document in the key vault collection that has the given ``key_id``.

        :Parameters:
          - `id` (Binary): The UUID of a key a which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).

        :Returns:
          The delete result.
        """
        result = self._key_vault_coll.delete_one({"_id": id})
        raw_result = result.raw_result
        raw_result.update(
            {"deletedCount": result.deleted_count, "acknowledged": result.acknowledged}
        )
        return raw_result

    def add_key_alt_name(self, id: Binary, key_alt_name: str) -> Any:
        """Add ``key_alt_name`` to the set of alternate names in the key document with UUID ``key_id``.

        :Parameters:
          - ``id``: The UUID of a key a which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).
          - ``key_alt_name``: The key alternate name to add.

        :Returns:
          The key document.
        """
        update = {"$addToSet": {"keyAltNames": key_alt_name}}
        return self._key_vault_coll.find_one_and_update({"_id": id}, update)

    def remove_key_alt_name(self, id: Binary, key_alt_name: str) -> Any:
        """Remove ``key_alt_name`` from the set of keyAltNames in the key document with UUID ``id``.

        Also removes the ``keyAltNames`` field from the key document if it would otherwise be empty.

        :Parameters:
          - ``id``: The UUID of a key a which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).
          - ``key_alt_name``: The key alternate name to remove.

        :Returns:
          The removal result.
        """
        update = {"$pull": {"keyAltNames": key_alt_name}}
        reply = self._key_vault_coll.find_one_and_update({"_id": id}, update)
        # Ensure keyAltNames field is removed if it would otherwise be empty.
        if reply:
            pass

    def get_key_by_alt_name(self, key_alt_name: str) -> Any:
        """Get a key document in the key vault collection that has the given ``key_alt_name``.

        :Parameters:
          - `key_alt_name`: (str): The key alternate name of the key to get.

        :Returns:
          The key document.
        """
        return self._key_vault_coll.find_one({"keyAltNames": key_alt_name})

    def get_keys(self):
        """Get all of the data keys.

        :Returns:
          An iterable of all the data keys.
        """
        return list(self._key_vault_coll.find({}))

    def __enter__(self) -> "ClientEncryption":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _check_closed(self):
        if self._encryption is None:
            raise InvalidOperation("Cannot use closed ClientEncryption")

    def close(self) -> None:
        """Release resources.

        Note that using this class in a with-statement will automatically call
        :meth:`close`::

            with ClientEncryption(...) as client_encryption:
                encrypted = client_encryption.encrypt(value, ...)
                decrypted = client_encryption.decrypt(encrypted)

        """
        if self._io_callbacks:
            self._io_callbacks.close()
            self._encryption.close()
            self._io_callbacks = None
            self._encryption = None
