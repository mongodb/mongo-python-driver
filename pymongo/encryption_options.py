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

"""Support for automatic client-side field level encryption."""

import copy

try:
    import pymongocrypt
    _HAVE_PYMONGOCRYPT = True
except ImportError:
    _HAVE_PYMONGOCRYPT = False

from pymongo.errors import ConfigurationError
from pymongo.uri_parser import _parse_kms_tls_options


class AutoEncryptionOpts(object):
    """Options to configure automatic client-side field level encryption."""

    def __init__(self, kms_providers, key_vault_namespace,
                 key_vault_client=None,
                 schema_map=None,
                 bypass_auto_encryption=False,
                 mongocryptd_uri='mongodb://localhost:27020',
                 mongocryptd_bypass_spawn=False,
                 mongocryptd_spawn_path='mongocryptd',
                 mongocryptd_spawn_args=None,
                 kms_tls_options=None):
        """Options to configure automatic client-side field level encryption.

        Automatic client-side field level encryption requires MongoDB 4.2
        enterprise or a MongoDB 4.2 Atlas cluster. Automatic encryption is not
        supported for operations on a database or view and will result in
        error.

        Although automatic encryption requires MongoDB 4.2 enterprise or a
        MongoDB 4.2 Atlas cluster, automatic *decryption* is supported for all
        users. To configure automatic *decryption* without automatic
        *encryption* set ``bypass_auto_encryption=True``. Explicit
        encryption and explicit decryption is also supported for all users
        with the :class:`~pymongo.encryption.ClientEncryption` class.

        See :ref:`automatic-client-side-encryption` for an example.

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
          - `key_vault_client` (optional): By default the key vault collection
            is assumed to reside in the same MongoDB cluster as the encrypted
            MongoClient. Use this option to route data key queries to a
            separate MongoDB cluster.
          - `schema_map` (optional): Map of collection namespace ("db.coll") to
            JSON Schema.  By default, a collection's JSONSchema is periodically
            polled with the listCollections command. But a JSONSchema may be
            specified locally with the schemaMap option.

            **Supplying a `schema_map` provides more security than relying on
            JSON Schemas obtained from the server. It protects against a
            malicious server advertising a false JSON Schema, which could trick
            the client into sending unencrypted data that should be
            encrypted.**

            Schemas supplied in the schemaMap only apply to configuring
            automatic encryption for client side encryption. Other validation
            rules in the JSON schema will not be enforced by the driver and
            will result in an error.
          - `bypass_auto_encryption` (optional): If ``True``, automatic
            encryption will be disabled but automatic decryption will still be
            enabled. Defaults to ``False``.
          - `mongocryptd_uri` (optional): The MongoDB URI used to connect
            to the *local* mongocryptd process. Defaults to
            ``'mongodb://localhost:27020'``.
          - `mongocryptd_bypass_spawn` (optional): If ``True``, the encrypted
            MongoClient will not attempt to spawn the mongocryptd process.
            Defaults to ``False``.
          - `mongocryptd_spawn_path` (optional): Used for spawning the
            mongocryptd process. Defaults to ``'mongocryptd'`` and spawns
            mongocryptd from the system path.
          - `mongocryptd_spawn_args` (optional): A list of string arguments to
            use when spawning the mongocryptd process. Defaults to
            ``['--idleShutdownTimeoutSecs=60']``. If the list does not include
            the ``idleShutdownTimeoutSecs`` option then
            ``'--idleShutdownTimeoutSecs=60'`` will be added.
          - `kms_tls_options` (optional):  A map of KMS provider names to TLS
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
                "client side encryption requires the pymongocrypt library: "
                "install a compatible version with: "
                "python -m pip install 'pymongo[encryption]'")

        self._kms_providers = kms_providers
        self._key_vault_namespace = key_vault_namespace
        self._key_vault_client = key_vault_client
        self._schema_map = schema_map
        self._bypass_auto_encryption = bypass_auto_encryption
        self._mongocryptd_uri = mongocryptd_uri
        self._mongocryptd_bypass_spawn = mongocryptd_bypass_spawn
        self._mongocryptd_spawn_path = mongocryptd_spawn_path
        self._mongocryptd_spawn_args = (copy.copy(mongocryptd_spawn_args) or
                                        ['--idleShutdownTimeoutSecs=60'])
        if not isinstance(self._mongocryptd_spawn_args, list):
            raise TypeError('mongocryptd_spawn_args must be a list')
        if not any('idleShutdownTimeoutSecs' in s
                   for s in self._mongocryptd_spawn_args):
            self._mongocryptd_spawn_args.append('--idleShutdownTimeoutSecs=60')
        # Maps KMS provider name to a SSLContext.
        self._kms_ssl_contexts = _parse_kms_tls_options(kms_tls_options)
