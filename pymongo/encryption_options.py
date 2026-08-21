# Copyright 2019-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Support for automatic client-side field level encryption.

.. seealso:: This module is compatible with both the synchronous and asynchronous PyMongo APIs.
"""

from __future__ import annotations

import asyncio
import functools
import socket
import ssl
import threading
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Optional, TypedDict

from pymongo.uri_parser_shared import _parse_kms_tls_options

try:
    import pymongocrypt  # type:ignore[import-untyped]  # noqa: F401

    # Check for pymongocrypt>=1.10.
    from pymongocrypt import synchronous as _  # noqa: F401

    _HAVE_PYMONGOCRYPT = True
except ImportError:
    _HAVE_PYMONGOCRYPT = False
from bson import int64
from pymongo.common import check_for_min_version, validate_is_mapping
from pymongo.errors import ConfigurationError

if TYPE_CHECKING:
    from pymongo.pyopenssl_context import SSLContext
    from pymongo.typings import _AgnosticMongoClient


def check_min_pymongocrypt() -> None:
    """Raise an appropriate error if the min pymongocrypt is not installed."""
    pymongocrypt_version, required_version, is_valid = check_for_min_version("pymongocrypt")
    if not is_valid:
        raise ConfigurationError(
            f"client side encryption requires pymongocrypt>={required_version}, "
            f"found version {pymongocrypt_version}. "
            "Install a compatible version with: "
            "python -m pip install 'pymongo[encryption]'"
        )


@dataclass(frozen=True)
class KMSConnectContext:
    """Information about a pending KMS connection.

    Passed to ``kms_connect_callback``, which must return a plain, unwrapped
    :class:`socket.socket`. The driver performs the KMS TLS handshake over it
    against ``host``, not the peer actually reached, which is what makes
    proxying safe.

    Prefer :class:`HTTPProxyKMSConnect` or :class:`AsyncHTTPProxyKMSConnect`
    over writing a callback.

    :param host: Hostname of the KMS server, and the TLS verification target.
    :param port: Port of the KMS server.
    :param timeout: Seconds left in the timeout budget, else the default KMS
        connect timeout. Never ``None``.

    .. note:: ``timeoutMS`` does not constrain KMS requests for explicit
       encryption, so ``timeout`` is always the default there. Automatic
       encryption passes the remaining budget. This deviates from the Client
       Side Operations Timeout specification; see PYTHON-6037.

    .. versionadded:: 4.18
    """

    host: str
    port: int
    timeout: Optional[float]


# A callback that opens a connection to a KMS host.
AsyncKMSConnectCallback = Callable[[KMSConnectContext], Awaitable[socket.socket]]
KMSConnectCallback = Callable[[KMSConnectContext], socket.socket]


class HTTPProxyKMSConnect:
    """Route KMS connections through an HTTP proxy, for the synchronous API.

    Pass an instance as ``kms_connect_callback`` to reach KMS hosts through a
    forward proxy that speaks HTTP ``CONNECT``::

      from pymongo.encryption_options import HTTPProxyKMSConnect

      opts = AutoEncryptionOpts(
          kms_providers={"aws": aws_creds},
          key_vault_namespace="keyvault.datakeys",
          kms_connect_callback=HTTPProxyKMSConnect("proxy.example.com", 8080),
      )

    To reach the proxy over TLS, pass an :class:`ssl.SSLContext`. It applies
    only to the proxy connection; KMS TLS is still negotiated end to end::

      import ssl

      proxy_tls = ssl.create_default_context(cafile="proxy-ca.pem")
      callback = HTTPProxyKMSConnect("proxy.example.com", 8443, proxy_tls)

    Use :class:`AsyncHTTPProxyKMSConnect` with the asynchronous API.

    :param host: Hostname of the proxy.
    :param port: Port of the proxy.
    :param ssl_context: Optional :class:`ssl.SSLContext` for connecting to the
        proxy over TLS. Defaults to ``None``, meaning a plain connection.

    .. versionadded:: 4.18
    """

    def __init__(self, host: str, port: int, ssl_context: Optional[ssl.SSLContext] = None):
        self.host = host
        self.port = port
        self.ssl_context = ssl_context

    def _tunnel(self, sock: socket.socket, context: KMSConnectContext) -> None:
        target = f"{context.host}:{context.port}"
        sock.sendall(f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n".encode())
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = sock.recv(4096)
            if not chunk:
                raise OSError(f"proxy closed the connection while tunneling to {target}")
            response += chunk
        status = response.split(b"\r\n", 1)[0]
        if not status.startswith(b"HTTP/1.1 200"):
            raise OSError(f"proxy refused CONNECT to {target}: {status!r}")

    def _bridge(self, proxy: socket.socket) -> socket.socket:
        """Relay a TLS proxy connection through a socketpair.

        Python cannot layer TLS over an :class:`ssl.SSLSocket`, so return the
        plain end of a pair. Threads rather than tasks, even in
        :class:`AsyncHTTPProxyKMSConnect`, because the event loop cannot read
        an :class:`ssl.SSLSocket`.
        """
        driver_side, relay_side = socket.socketpair()

        def relay(src: socket.socket, dst: socket.socket) -> None:
            try:
                while True:
                    buf = src.recv(16384)
                    if not buf:
                        break
                    dst.sendall(buf)
            except OSError:
                pass
            finally:
                # EOF the peer instead of closing a socket it may be reading.
                try:
                    dst.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                src.close()

        for pair in ((relay_side, proxy), (proxy, relay_side)):
            threading.Thread(target=relay, args=pair, daemon=True).start()
        return driver_side

    def __call__(self, context: KMSConnectContext) -> socket.socket:
        sock = socket.create_connection((self.host, self.port), timeout=context.timeout)
        try:
            if self.ssl_context is not None:
                sock = self.ssl_context.wrap_socket(sock, server_hostname=self.host)
            self._tunnel(sock, context)
        except BaseException:
            sock.close()
            raise
        if self.ssl_context is None:
            return sock
        return self._bridge(sock)


class AsyncHTTPProxyKMSConnect(HTTPProxyKMSConnect):
    """Route KMS connections through an HTTP proxy, for the asynchronous API.

    Behaves exactly like :class:`HTTPProxyKMSConnect`, but is a coroutine
    callable and runs the blocking connect in a thread so the event loop stays
    free.

    .. versionadded:: 4.18
    """

    async def __call__(self, context: KMSConnectContext) -> socket.socket:  # type: ignore[override]
        # run_in_executor, as auth_oidc.py does for user callbacks.
        connect = functools.partial(super().__call__, context)
        return await asyncio.get_running_loop().run_in_executor(None, connect)


class AutoEncryptionOpts:
    """Options to configure automatic client-side field level encryption."""

    def __init__(
        self,
        kms_providers: Mapping[str, Any],
        key_vault_namespace: str,
        key_vault_client: Optional[_AgnosticMongoClient] = None,
        schema_map: Optional[Mapping[str, Any]] = None,
        bypass_auto_encryption: bool = False,
        mongocryptd_uri: str = "mongodb://localhost:27020",
        mongocryptd_bypass_spawn: bool = False,
        mongocryptd_spawn_path: str = "mongocryptd",
        mongocryptd_spawn_args: Optional[list[str]] = None,
        kms_tls_options: Optional[Mapping[str, Any]] = None,
        crypt_shared_lib_path: Optional[str] = None,
        crypt_shared_lib_required: bool = False,
        bypass_query_analysis: bool = False,
        encrypted_fields_map: Optional[Mapping[str, Any]] = None,
        key_expiration_ms: Optional[int] = None,
        kms_connect_callback: Optional[Callable[[KMSConnectContext], Any]] = None,
    ) -> None:
        """Options to configure automatic client-side field level encryption.

        Automatic client-side field level encryption requires MongoDB >=4.2
        enterprise or a MongoDB >=4.2 Atlas cluster. Automatic encryption is not
        supported for operations on a database or view and will result in
        error.

        Although automatic encryption requires MongoDB >=4.2 enterprise or a
        MongoDB >=4.2 Atlas cluster, automatic *decryption* is supported for all
        users. To configure automatic *decryption* without automatic
        *encryption* set ``bypass_auto_encryption=True``. Explicit
        encryption and explicit decryption is also supported for all users
        with the :class:`~pymongo.asynchronous.encryption.AsyncClientEncryption` and :class:`~pymongo.encryption.ClientEncryption` classes.

        See `client-side field level encryption <https://www.mongodb.com/docs/languages/python/pymongo-driver/current/security/in-use-encryption/#client-side-field-level-encryption>`_ for an example.

        :param kms_providers: Map of KMS provider options. The `kms_providers`
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

            KMS providers may be specified with an optional name suffix
            separated by a colon, for example "kmip:name" or "aws:name".
            Named KMS providers do not support `CSFLE on-demand credentials <https://www.mongodb.com/docs/manual/core/csfle/tutorials/aws/aws-automatic/?interface=driver&language=python#use-automatic-client-side-field-level-encryption-with-aws>`_.
            Named KMS providers enables more than one of each KMS provider type to be configured.
            For example, to configure multiple local KMS providers::

              kms_providers = {
                  "local": {"key": local_kek1},        # Unnamed KMS provider.
                  "local:myname": {"key": local_kek2}, # Named KMS provider with name "myname".
              }

        :param key_vault_namespace: The namespace for the key vault collection.
            The key vault collection contains all data keys used for encryption
            and decryption. Data keys are stored as documents in this MongoDB
            collection. Data keys are protected with encryption by a KMS
            provider.
        :param key_vault_client: By default, the key vault collection
            is assumed to reside in the same MongoDB cluster as the encrypted
            AsyncMongoClient/MongoClient. Use this option to route data key queries to a
            separate MongoDB cluster.
        :param schema_map: Map of collection namespace ("db.coll") to
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
        :param bypass_auto_encryption: If ``True``, automatic
            encryption will be disabled but automatic decryption will still be
            enabled. Defaults to ``False``.
        :param mongocryptd_uri: The MongoDB URI used to connect
            to the *local* mongocryptd process. Defaults to
            ``'mongodb://localhost:27020'``.
        :param mongocryptd_bypass_spawn: If ``True``, the encrypted
            AsyncMongoClient/MongoClient will not attempt to spawn the mongocryptd process.
            Defaults to ``False``.
        :param mongocryptd_spawn_path: Used for spawning the
            mongocryptd process. Defaults to ``'mongocryptd'`` and spawns
            mongocryptd from the system path.
        :param mongocryptd_spawn_args: A list of string arguments to
            use when spawning the mongocryptd process. Defaults to
            ``['--idleShutdownTimeoutSecs=60']``. If the list does not include
            the ``idleShutdownTimeoutSecs`` option then
            ``'--idleShutdownTimeoutSecs=60'`` will be added.
        :param kms_tls_options:  A map of KMS provider names to TLS
            options to use when creating secure connections to KMS providers.
            Accepts the same TLS options as
            :class:`pymongo.mongo_client.AsyncMongoClient` and :class:`pymongo.mongo_client.MongoClient`. For example, to
            override the system default CA file::

              kms_tls_options={'kmip': {'tlsCAFile': certifi.where()}}

            Or to supply a client certificate::

              kms_tls_options={'kmip': {'tlsCertificateKeyFile': 'client.pem'}}
        :param crypt_shared_lib_path: Override the path to load the crypt_shared library.
        :param crypt_shared_lib_required: If True, raise an error if libmongocrypt is
            unable to load the crypt_shared library.
        :param bypass_query_analysis: If ``True``, disable automatic analysis
            of outgoing commands. Set `bypass_query_analysis` to use explicit
            encryption on indexed fields without the MongoDB Enterprise Advanced
            licensed crypt_shared library.
        :param encrypted_fields_map: Map of collection namespace ("db.coll") to documents
            that described the encrypted fields for Queryable Encryption. For example::

                {
                  "db.encryptedCollection": {
                      "escCollection": "enxcol_.encryptedCollection.esc",
                      "ecocCollection": "enxcol_.encryptedCollection.ecoc",
                      "fields": [
                          {
                              "path": "firstName",
                              "keyId": Binary.from_uuid(UUID('00000000-0000-0000-0000-000000000000')),
                              "bsonType": "string",
                              "queries": {"queryType": "equality"}
                          },
                          {
                              "path": "ssn",
                              "keyId": Binary.from_uuid(UUID('04104104-1041-0410-4104-104104104104')),
                              "bsonType": "string"
                          }
                      ]
                  }
                }
        :param key_expiration_ms: The cache expiration time for data encryption keys.
            Defaults to ``None`` which defers to libmongocrypt's default which is currently 60000.
            Set to 0 to disable key expiration.
        :param kms_connect_callback: A callable that opens the connection to a
            KMS host, used to route KMS requests through an HTTP proxy. It
            receives a :class:`KMSConnectContext` and returns a connected,
            unwrapped :class:`socket.socket`; the driver then performs the KMS
            TLS handshake over it. Must be a coroutine function for
            :class:`~pymongo.asynchronous.mongo_client.AsyncMongoClient` and a
            regular function for
            :class:`~pymongo.synchronous.mongo_client.MongoClient`. For an
            ordinary HTTP proxy, pass :class:`HTTPProxyKMSConnect` or
            :class:`AsyncHTTPProxyKMSConnect`. Defaults to ``None``, meaning
            the driver connects to KMS hosts directly.

        .. versionchanged:: 4.18
           Added the `kms_connect_callback` parameter.
        .. versionchanged:: 4.12
           Added the `key_expiration_ms` parameter.
        .. versionchanged:: 4.2
           Added the `encrypted_fields_map`, `crypt_shared_lib_path`, `crypt_shared_lib_required`,
           and `bypass_query_analysis` parameters.

        .. versionchanged:: 4.0
           Added the `kms_tls_options` parameter and the "kmip" KMS provider.

        .. versionadded:: 3.9
        """
        if not _HAVE_PYMONGOCRYPT:
            raise ConfigurationError(
                "client side encryption requires the pymongocrypt library: "
                "install a compatible version with: "
                "python -m pip install 'pymongo[encryption]'"
            )
        check_min_pymongocrypt()
        if encrypted_fields_map:
            validate_is_mapping("encrypted_fields_map", encrypted_fields_map)
        self._encrypted_fields_map = encrypted_fields_map
        self._crypt_shared_lib_path = crypt_shared_lib_path
        self._crypt_shared_lib_required = crypt_shared_lib_required
        self._kms_providers = kms_providers
        self._key_vault_namespace = key_vault_namespace
        self._key_vault_client = key_vault_client
        self._schema_map = schema_map
        self._bypass_auto_encryption = bypass_auto_encryption
        self._mongocryptd_uri = mongocryptd_uri
        self._mongocryptd_bypass_spawn = mongocryptd_bypass_spawn
        self._mongocryptd_spawn_path = mongocryptd_spawn_path
        if mongocryptd_spawn_args is None:
            mongocryptd_spawn_args = ["--idleShutdownTimeoutSecs=60"]
        self._mongocryptd_spawn_args = mongocryptd_spawn_args
        if not isinstance(self._mongocryptd_spawn_args, list):
            raise TypeError(
                f"mongocryptd_spawn_args must be a list, not {type(self._mongocryptd_spawn_args)}"
            )
        if not any("idleShutdownTimeoutSecs" in s for s in self._mongocryptd_spawn_args):
            self._mongocryptd_spawn_args.append("--idleShutdownTimeoutSecs=60")
        # Maps KMS provider name to a SSLContext.
        self._kms_tls_options = kms_tls_options
        self._sync_kms_ssl_contexts: Optional[dict[str, SSLContext]] = None
        self._async_kms_ssl_contexts: Optional[dict[str, SSLContext]] = None
        self._bypass_query_analysis = bypass_query_analysis
        self._key_expiration_ms = key_expiration_ms
        if kms_connect_callback is not None and not callable(kms_connect_callback):
            raise TypeError(
                f"kms_connect_callback must be callable, not {type(kms_connect_callback)}"
            )
        self._kms_connect_callback = kms_connect_callback

    def _kms_ssl_contexts(self, is_sync: bool) -> dict[str, SSLContext]:
        if is_sync:
            if self._sync_kms_ssl_contexts is None:
                self._sync_kms_ssl_contexts = _parse_kms_tls_options(self._kms_tls_options, True)
            return self._sync_kms_ssl_contexts
        else:
            if self._async_kms_ssl_contexts is None:
                self._async_kms_ssl_contexts = _parse_kms_tls_options(self._kms_tls_options, False)
            return self._async_kms_ssl_contexts


class RangeOpts:
    """Options to configure encrypted queries using the range algorithm."""

    def __init__(
        self,
        sparsity: Optional[int] = None,
        trim_factor: Optional[int] = None,
        min: Optional[Any] = None,
        max: Optional[Any] = None,
        precision: Optional[int] = None,
    ) -> None:
        """Options to configure encrypted queries using the range algorithm.

        :param sparsity: An integer.
        :param trim_factor: An integer.
        :param min: A BSON scalar value corresponding to the type being queried.
        :param max: A BSON scalar value corresponding to the type being queried.
        :param precision: An integer, may only be set for double or decimal128 types.

        .. versionadded:: 4.4
        """
        self.min = min
        self.max = max
        self.sparsity = sparsity
        self.trim_factor = trim_factor
        self.precision = precision

    @property
    def document(self) -> dict[str, Any]:
        doc = {}
        for k, v in [
            ("sparsity", int64.Int64(self.sparsity) if self.sparsity else None),
            ("trimFactor", self.trim_factor),
            ("precision", self.precision),
            ("min", self.min),
            ("max", self.max),
        ]:
            if v is not None:
                doc[k] = v
        return doc


class TextOpts:
    """**BETA** Options to configure encrypted queries using the text algorithm.

    TextOpts is currently unstable API and subject to backwards breaking changes."""

    def __init__(
        self,
        substring: Optional[SubstringOpts] = None,
        prefix: Optional[PrefixOpts] = None,
        suffix: Optional[SuffixOpts] = None,
        case_sensitive: Optional[bool] = None,
        diacritic_sensitive: Optional[bool] = None,
    ) -> None:
        """Options to configure encrypted queries using the text algorithm.

        :param substring: Further options to support substring queries.
        :param prefix: Further options to support prefix queries.
        :param suffix: Further options to support suffix queries.
        :param case_sensitive: Whether text indexes for this field are case sensitive.
        :param diacritic_sensitive: Whether text indexes for this field are diacritic sensitive.

        .. versionadded:: 4.15
        """
        self.substring = substring
        self.prefix = prefix
        self.suffix = suffix
        self.case_sensitive = case_sensitive
        self.diacritic_sensitive = diacritic_sensitive

    @property
    def document(self) -> dict[str, Any]:
        doc = {}
        for k, v in [
            ("substring", self.substring),
            ("prefix", self.prefix),
            ("suffix", self.suffix),
            ("caseSensitive", self.case_sensitive),
            ("diacriticSensitive", self.diacritic_sensitive),
        ]:
            if v is not None:
                doc[k] = v
        return doc


class SubstringOpts(TypedDict):
    """**BETA** Options for substring text queries.

    SubstringOpts is currently unstable API and subject to backwards breaking changes.
    """

    # strMaxLength is the maximum allowed length to insert. Inserting longer strings will error.
    strMaxLength: int
    # strMinQueryLength is the minimum allowed query length. Querying with a shorter string will error.
    strMinQueryLength: int
    # strMaxQueryLength is the maximum allowed query length. Querying with a longer string will error.
    strMaxQueryLength: int


class PrefixOpts(TypedDict):
    """**BETA** Options for prefix text queries.

    PrefixOpts is currently unstable API and subject to backwards breaking changes.
    """

    # strMinQueryLength is the minimum allowed query length. Querying with a shorter string will error.
    strMinQueryLength: int
    # strMaxQueryLength is the maximum allowed query length. Querying with a longer string will error.
    strMaxQueryLength: int


class SuffixOpts(TypedDict):
    """**BETA** Options for suffix text queries.

    SuffixOpts is currently unstable API and subject to backwards breaking changes.
    """

    # strMinQueryLength is the minimum allowed query length. Querying with a shorter string will error.
    strMinQueryLength: int
    # strMaxQueryLength is the maximum allowed query length. Querying with a longer string will error.
    strMaxQueryLength: int
