# Copyright 2011-present MongoDB, Inc.
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

"""Tests for SSL support."""
from __future__ import annotations

import os
import pathlib
import socket
import sys

sys.path[0:0] = [""]

from test.asynchronous import (
    HAVE_IPADDRESS,
    AsyncIntegrationTest,
    AsyncPyMongoTestCase,
    SkipTest,
    async_client_context,
    connected,
    remove_all_users,
    unittest,
)
from test.utils_shared import (
    EventListener,
    OvertCommandListener,
    cat_files,
    ignore_deprecations,
)
from urllib.parse import quote_plus

from pymongo import AsyncMongoClient, ssl_support
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure
from pymongo.hello import HelloCompat
from pymongo.ssl_support import HAVE_PYSSL, HAVE_SSL, _ssl, get_ssl_context
from pymongo.write_concern import WriteConcern

_HAVE_PYOPENSSL = False
try:
    # All of these must be available to use PyOpenSSL
    import OpenSSL
    import requests
    import service_identity

    # Ensure service_identity>=18.1 is installed
    from service_identity.pyopenssl import verify_ip_address

    from pymongo.ocsp_support import _load_trusted_ca_certs

    _HAVE_PYOPENSSL = True
except ImportError:
    _load_trusted_ca_certs = None  # type: ignore


if HAVE_SSL:
    import ssl

_IS_SYNC = False

if _IS_SYNC:
    CERT_PATH = os.path.join(pathlib.Path(__file__).resolve().parent, "certificates")
else:
    CERT_PATH = os.path.join(pathlib.Path(__file__).resolve().parent.parent, "certificates")

CLIENT_PEM = os.path.join(CERT_PATH, "client.pem")
CLIENT_ENCRYPTED_PEM = os.path.join(CERT_PATH, "password_protected.pem")
CA_PEM = os.path.join(CERT_PATH, "ca.pem")
CA_BUNDLE_PEM = os.path.join(CERT_PATH, "trusted-ca.pem")
CRL_PEM = os.path.join(CERT_PATH, "crl.pem")
MONGODB_X509_USERNAME = "C=US,ST=New York,L=New York City,O=MDB,OU=Drivers,CN=client"

# To fully test this start a mongod instance (built with SSL support) like so:
# mongod --dbpath /path/to/data/directory --sslOnNormalPorts \
# --sslPEMKeyFile /path/to/pymongo/test/certificates/server.pem \
# --sslCAFile /path/to/pymongo/test/certificates/ca.pem \
# --sslWeakCertificateValidation
# Also, make sure you have 'server' as an alias for localhost in /etc/hosts
#
# Note: For all replica set tests to pass, the replica set configuration must
# use 'localhost' for the hostname of all hosts.


class TestClientSSL(AsyncPyMongoTestCase):
    @unittest.skipIf(HAVE_SSL, "The ssl module is available, can't test what happens without it.")
    def test_no_ssl_module(self):
        # Explicit
        self.assertRaises(ConfigurationError, self.simple_client, ssl=True)

        # Implied
        self.assertRaises(ConfigurationError, self.simple_client, tlsCertificateKeyFile=CLIENT_PEM)

    @unittest.skipUnless(HAVE_SSL, "The ssl module is not available.")
    @ignore_deprecations
    def test_config_ssl(self):
        # Tests various ssl configurations
        self.assertRaises(ValueError, self.simple_client, ssl="foo")
        self.assertRaises(
            ConfigurationError, self.simple_client, tls=False, tlsCertificateKeyFile=CLIENT_PEM
        )
        self.assertRaises(TypeError, self.simple_client, ssl=0)
        self.assertRaises(TypeError, self.simple_client, ssl=5.5)
        self.assertRaises(TypeError, self.simple_client, ssl=[])

        self.assertRaises(IOError, self.simple_client, tlsCertificateKeyFile="NoSuchFile")
        self.assertRaises(TypeError, self.simple_client, tlsCertificateKeyFile=True)
        self.assertRaises(TypeError, self.simple_client, tlsCertificateKeyFile=[])

        # Test invalid combinations
        self.assertRaises(
            ConfigurationError, self.simple_client, tls=False, tlsCertificateKeyFile=CLIENT_PEM
        )
        self.assertRaises(ConfigurationError, self.simple_client, tls=False, tlsCAFile=CA_PEM)
        self.assertRaises(ConfigurationError, self.simple_client, tls=False, tlsCRLFile=CRL_PEM)
        self.assertRaises(
            ConfigurationError, self.simple_client, tls=False, tlsAllowInvalidCertificates=False
        )
        self.assertRaises(
            ConfigurationError, self.simple_client, tls=False, tlsAllowInvalidHostnames=False
        )
        self.assertRaises(
            ConfigurationError, self.simple_client, tls=False, tlsDisableOCSPEndpointCheck=False
        )

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_use_pyopenssl_when_available(self):
        self.assertTrue(HAVE_PYSSL)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "Cannot test without PyOpenSSL")
    def test_load_trusted_ca_certs(self):
        trusted_ca_certs = _load_trusted_ca_certs(CA_BUNDLE_PEM)
        self.assertEqual(2, len(trusted_ca_certs))


class TestSSL(AsyncIntegrationTest):
    saved_port: int

    async def assertClientWorks(self, client):
        coll = client.pymongo_test.ssl_test.with_options(
            write_concern=WriteConcern(w=async_client_context.w)
        )
        await coll.drop()
        await coll.insert_one({"ssl": True})
        self.assertTrue((await coll.find_one())["ssl"])
        await coll.drop()

    @unittest.skipUnless(HAVE_SSL, "The ssl module is not available.")
    async def asyncSetUp(self):
        await super().asyncSetUp()
        # MongoClient should connect to the primary by default.
        self.saved_port = AsyncMongoClient.PORT
        AsyncMongoClient.PORT = await async_client_context.port

    async def asyncTearDown(self):
        AsyncMongoClient.PORT = self.saved_port

    @async_client_context.require_tls
    async def test_simple_ssl(self):
        if "PyPy" in sys.version:
            self.skipTest("Test is flaky on PyPy")
        # Expects the server to be running with ssl and with
        # no --sslPEMKeyFile or with --sslWeakCertificateValidation
        await self.assertClientWorks(self.client)

    @async_client_context.require_tlsCertificateKeyFile
    @async_client_context.require_no_api_version
    @ignore_deprecations
    async def test_tlsCertificateKeyFilePassword(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        if not hasattr(ssl, "SSLContext") and not HAVE_PYSSL:
            self.assertRaises(
                ConfigurationError,
                self.simple_client,
                "localhost",
                ssl=True,
                tlsCertificateKeyFile=CLIENT_ENCRYPTED_PEM,
                tlsCertificateKeyFilePassword="qwerty",
                tlsCAFile=CA_PEM,
                serverSelectionTimeoutMS=1000,
            )
        else:
            await connected(
                self.simple_client(
                    "localhost",
                    ssl=True,
                    tlsCertificateKeyFile=CLIENT_ENCRYPTED_PEM,
                    tlsCertificateKeyFilePassword="qwerty",
                    tlsCAFile=CA_PEM,
                    serverSelectionTimeoutMS=5000,
                    **self.credentials,  # type: ignore[arg-type]
                )
            )

            uri_fmt = (
                "mongodb://localhost/?ssl=true"
                "&tlsCertificateKeyFile=%s&tlsCertificateKeyFilePassword=qwerty"
                "&tlsCAFile=%s&serverSelectionTimeoutMS=5000"
            )
            await connected(
                self.simple_client(uri_fmt % (CLIENT_ENCRYPTED_PEM, CA_PEM), **self.credentials)  # type: ignore[arg-type]
            )

    @async_client_context.require_tlsCertificateKeyFile
    @async_client_context.require_no_auth
    @ignore_deprecations
    async def test_cert_ssl_implicitly_set(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #

        # test that setting tlsCertificateKeyFile causes ssl to be set to True
        client = self.simple_client(
            await async_client_context.host,
            await async_client_context.port,
            tlsAllowInvalidCertificates=True,
            tlsCertificateKeyFile=CLIENT_PEM,
        )
        response = await client.admin.command(HelloCompat.LEGACY_CMD)
        if "setName" in response:
            client = self.simple_client(
                await async_client_context.pair,
                replicaSet=response["setName"],
                w=len(response["hosts"]),
                tlsAllowInvalidCertificates=True,
                tlsCertificateKeyFile=CLIENT_PEM,
            )

        await self.assertClientWorks(client)

    @async_client_context.require_tlsCertificateKeyFile
    @async_client_context.require_no_auth
    @ignore_deprecations
    async def test_cert_ssl_validation(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #
        client = self.simple_client(
            "localhost",
            ssl=True,
            tlsCertificateKeyFile=CLIENT_PEM,
            tlsAllowInvalidCertificates=False,
            tlsCAFile=CA_PEM,
        )
        response = await client.admin.command(HelloCompat.LEGACY_CMD)
        if "setName" in response:
            if response["primary"].split(":")[0] != "localhost":
                raise SkipTest(
                    "No hosts in the replicaset for 'localhost'. "
                    "Cannot validate hostname in the certificate"
                )

            client = self.simple_client(
                "localhost",
                replicaSet=response["setName"],
                w=len(response["hosts"]),
                ssl=True,
                tlsCertificateKeyFile=CLIENT_PEM,
                tlsAllowInvalidCertificates=False,
                tlsCAFile=CA_PEM,
            )

        await self.assertClientWorks(client)

        if HAVE_IPADDRESS:
            client = self.simple_client(
                "127.0.0.1",
                ssl=True,
                tlsCertificateKeyFile=CLIENT_PEM,
                tlsAllowInvalidCertificates=False,
                tlsCAFile=CA_PEM,
            )
            await self.assertClientWorks(client)

    @async_client_context.require_tlsCertificateKeyFile
    @async_client_context.require_no_auth
    @ignore_deprecations
    async def test_cert_ssl_uri_support(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #
        uri_fmt = (
            "mongodb://localhost/?ssl=true&tlsCertificateKeyFile=%s&tlsAllowInvalidCertificates"
            "=%s&tlsCAFile=%s&tlsAllowInvalidHostnames=false"
        )
        client = self.simple_client(uri_fmt % (CLIENT_PEM, "true", CA_PEM))
        await self.assertClientWorks(client)

    @unittest.skipIf(
        "PyPy" in sys.version and not _IS_SYNC,
        "https://github.com/pypy/pypy/issues/5131 flaky on async PyPy due to SSL EOF",
    )
    @async_client_context.require_tlsCertificateKeyFile
    @async_client_context.require_server_resolvable
    @async_client_context.require_no_api_version
    @ignore_deprecations
    async def test_cert_ssl_validation_hostname_matching(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        ctx = get_ssl_context(None, None, None, None, True, True, False, _IS_SYNC)
        self.assertFalse(ctx.check_hostname)
        ctx = get_ssl_context(None, None, None, None, True, False, False, _IS_SYNC)
        self.assertFalse(ctx.check_hostname)
        ctx = get_ssl_context(None, None, None, None, False, True, False, _IS_SYNC)
        self.assertFalse(ctx.check_hostname)
        ctx = get_ssl_context(None, None, None, None, False, False, False, _IS_SYNC)
        self.assertTrue(ctx.check_hostname)

        response = await self.client.admin.command(HelloCompat.LEGACY_CMD)

        with self.assertRaises(ConnectionFailure) as cm:
            await connected(
                self.simple_client(
                    "server",
                    ssl=True,
                    tlsCertificateKeyFile=CLIENT_PEM,
                    tlsAllowInvalidCertificates=False,
                    tlsCAFile=CA_PEM,
                    serverSelectionTimeoutMS=500,
                    **self.credentials,  # type: ignore[arg-type]
                )
            )
        # PYTHON-5414 Check for "module service_identity has no attribute SICertificateError"
        self.assertNotIn("has no attribute", str(cm.exception))

        await connected(
            self.simple_client(
                "server",
                ssl=True,
                tlsCertificateKeyFile=CLIENT_PEM,
                tlsAllowInvalidCertificates=False,
                tlsCAFile=CA_PEM,
                tlsAllowInvalidHostnames=True,
                serverSelectionTimeoutMS=500,
                **self.credentials,  # type: ignore[arg-type]
            )
        )

        if "setName" in response:
            with self.assertRaises(ConnectionFailure):
                await connected(
                    self.simple_client(
                        "server",
                        replicaSet=response["setName"],
                        ssl=True,
                        tlsCertificateKeyFile=CLIENT_PEM,
                        tlsAllowInvalidCertificates=False,
                        tlsCAFile=CA_PEM,
                        serverSelectionTimeoutMS=500,
                        **self.credentials,  # type: ignore[arg-type]
                    )
                )

            await connected(
                self.simple_client(
                    "server",
                    replicaSet=response["setName"],
                    ssl=True,
                    tlsCertificateKeyFile=CLIENT_PEM,
                    tlsAllowInvalidCertificates=False,
                    tlsCAFile=CA_PEM,
                    tlsAllowInvalidHostnames=True,
                    serverSelectionTimeoutMS=500,
                    **self.credentials,  # type: ignore[arg-type]
                )
            )

    @async_client_context.require_tlsCertificateKeyFile
    @async_client_context.require_sync
    @async_client_context.require_no_api_version
    @ignore_deprecations
    async def test_tlsCRLFile_support(self):
        if not hasattr(ssl, "VERIFY_CRL_CHECK_LEAF") or HAVE_PYSSL:
            self.assertRaises(
                ConfigurationError,
                self.simple_client,
                "localhost",
                ssl=True,
                tlsCAFile=CA_PEM,
                tlsCRLFile=CRL_PEM,
                serverSelectionTimeoutMS=1000,
            )
        else:
            await connected(
                self.simple_client(
                    "localhost",
                    ssl=True,
                    tlsCAFile=CA_PEM,
                    serverSelectionTimeoutMS=1000,
                    **self.credentials,  # type: ignore[arg-type]
                )
            )

            with self.assertRaises(ConnectionFailure):
                await connected(
                    self.simple_client(
                        "localhost",
                        ssl=True,
                        tlsCAFile=CA_PEM,
                        tlsCRLFile=CRL_PEM,
                        serverSelectionTimeoutMS=1000,
                        **self.credentials,  # type: ignore[arg-type]
                    )
                )

            uri_fmt = "mongodb://localhost/?ssl=true&tlsCAFile=%s&serverSelectionTimeoutMS=1000"
            await connected(self.simple_client(uri_fmt % (CA_PEM,), **self.credentials))  # type: ignore

            uri_fmt = (
                "mongodb://localhost/?ssl=true&tlsCRLFile=%s"
                "&tlsCAFile=%s&serverSelectionTimeoutMS=1000"
            )
            with self.assertRaises(ConnectionFailure):
                await connected(
                    self.simple_client(uri_fmt % (CRL_PEM, CA_PEM), **self.credentials)  # type: ignore[arg-type]
                )

    @unittest.skipIf(
        "PyPy" in sys.version and not _IS_SYNC,
        "https://github.com/pypy/pypy/issues/5131 flaky on async PyPy due to SSL EOF",
    )
    @async_client_context.require_tlsCertificateKeyFile
    @async_client_context.require_server_resolvable
    @async_client_context.require_no_api_version
    @ignore_deprecations
    async def test_validation_with_system_ca_certs(self):
        # Expects the server to be running with server.pem and ca.pem.
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #   --sslWeakCertificateValidation
        #
        self.patch_system_certs(CA_PEM)
        with self.assertRaises(ConnectionFailure):
            # Server cert is verified but hostname matching fails
            await connected(
                self.simple_client(
                    "server", ssl=True, serverSelectionTimeoutMS=1000, **self.credentials
                )  # type: ignore[arg-type]
            )

        # Server cert is verified. Disable hostname matching.
        await connected(
            self.simple_client(
                "server",
                ssl=True,
                tlsAllowInvalidHostnames=True,
                serverSelectionTimeoutMS=1000,
                **self.credentials,  # type: ignore[arg-type]
            )
        )

        # Server cert and hostname are verified.
        await connected(
            self.simple_client(
                "localhost", ssl=True, serverSelectionTimeoutMS=1000, **self.credentials
            )  # type: ignore[arg-type]
        )

        # Server cert and hostname are verified.
        await connected(
            self.simple_client(
                "mongodb://localhost/?ssl=true&serverSelectionTimeoutMS=1000",
                **self.credentials,  # type: ignore[arg-type]
            )
        )

    def test_system_certs_config_error(self):
        ctx = get_ssl_context(None, None, None, None, True, True, False, _IS_SYNC)
        if (sys.platform != "win32" and hasattr(ctx, "set_default_verify_paths")) or hasattr(
            ctx, "load_default_certs"
        ):
            raise SkipTest("Can't test when system CA certificates are loadable.")

        have_certifi = ssl_support.HAVE_CERTIFI
        have_wincertstore = ssl_support.HAVE_WINCERTSTORE
        # Force the test regardless of environment.
        ssl_support.HAVE_CERTIFI = False
        ssl_support.HAVE_WINCERTSTORE = False
        try:
            with self.assertRaises(ConfigurationError):
                self.simple_client("mongodb://localhost/?ssl=true")
        finally:
            ssl_support.HAVE_CERTIFI = have_certifi
            ssl_support.HAVE_WINCERTSTORE = have_wincertstore

    def test_certifi_support(self):
        if hasattr(ssl, "SSLContext"):
            # SSLSocket doesn't provide ca_certs attribute on pythons
            # with SSLContext and SSLContext provides no information
            # about ca_certs.
            raise SkipTest("Can't test when SSLContext available.")
        if not ssl_support.HAVE_CERTIFI:
            raise SkipTest("Need certifi to test certifi support.")

        have_wincertstore = ssl_support.HAVE_WINCERTSTORE
        # Force the test on Windows, regardless of environment.
        ssl_support.HAVE_WINCERTSTORE = False
        try:
            ctx = get_ssl_context(None, None, CA_PEM, None, False, False, False, _IS_SYNC)
            ssl_sock = ctx.wrap_socket(socket.socket())
            self.assertEqual(ssl_sock.ca_certs, CA_PEM)

            ctx = get_ssl_context(None, None, None, None, False, False, False, _IS_SYNC)
            ssl_sock = ctx.wrap_socket(socket.socket())
            self.assertEqual(ssl_sock.ca_certs, ssl_support.certifi.where())
        finally:
            ssl_support.HAVE_WINCERTSTORE = have_wincertstore

    def test_wincertstore(self):
        if sys.platform != "win32":
            raise SkipTest("Only valid on Windows.")
        if hasattr(ssl, "SSLContext"):
            # SSLSocket doesn't provide ca_certs attribute on pythons
            # with SSLContext and SSLContext provides no information
            # about ca_certs.
            raise SkipTest("Can't test when SSLContext available.")
        if not ssl_support.HAVE_WINCERTSTORE:
            raise SkipTest("Need wincertstore to test wincertstore.")

        ctx = get_ssl_context(None, None, CA_PEM, None, False, False, False, _IS_SYNC)
        ssl_sock = ctx.wrap_socket(socket.socket())
        self.assertEqual(ssl_sock.ca_certs, CA_PEM)

        ctx = get_ssl_context(None, None, None, None, False, False, False, _IS_SYNC)
        ssl_sock = ctx.wrap_socket(socket.socket())
        self.assertEqual(ssl_sock.ca_certs, ssl_support._WINCERTS.name)

    @async_client_context.require_auth
    @async_client_context.require_tlsCertificateKeyFile
    @async_client_context.require_no_api_version
    @ignore_deprecations
    async def test_mongodb_x509_auth(self):
        host, port = await async_client_context.host, await async_client_context.port
        self.addAsyncCleanup(remove_all_users, async_client_context.client["$external"])

        # Give x509 user all necessary privileges.
        await async_client_context.create_user(
            "$external",
            MONGODB_X509_USERNAME,
            roles=[
                {"role": "readWriteAnyDatabase", "db": "admin"},
                {"role": "userAdminAnyDatabase", "db": "admin"},
            ],
        )

        noauth = self.simple_client(
            await async_client_context.pair,
            ssl=True,
            tlsAllowInvalidCertificates=True,
            tlsCertificateKeyFile=CLIENT_PEM,
        )

        with self.assertRaises(OperationFailure):
            await noauth.pymongo_test.test.find_one()

        listener = EventListener()
        auth = self.simple_client(
            await async_client_context.pair,
            authMechanism="MONGODB-X509",
            ssl=True,
            tlsAllowInvalidCertificates=True,
            tlsCertificateKeyFile=CLIENT_PEM,
            event_listeners=[listener],
        )

        # No error
        await auth.pymongo_test.test.find_one()
        names = listener.started_command_names()
        if async_client_context.version.at_least(4, 4, -1):
            # Speculative auth skips the authenticate command.
            self.assertEqual(names, ["find"])
        else:
            self.assertEqual(names, ["authenticate", "find"])

        uri = "mongodb://%s@%s:%d/?authMechanism=MONGODB-X509" % (
            quote_plus(MONGODB_X509_USERNAME),
            host,
            port,
        )
        client = self.simple_client(
            uri, ssl=True, tlsAllowInvalidCertificates=True, tlsCertificateKeyFile=CLIENT_PEM
        )
        # No error
        await client.pymongo_test.test.find_one()

        uri = "mongodb://%s:%d/?authMechanism=MONGODB-X509" % (host, port)
        client = self.simple_client(
            uri, ssl=True, tlsAllowInvalidCertificates=True, tlsCertificateKeyFile=CLIENT_PEM
        )
        # No error
        await client.pymongo_test.test.find_one()
        # Auth should fail if username and certificate do not match
        uri = "mongodb://%s@%s:%d/?authMechanism=MONGODB-X509" % (
            quote_plus("not the username"),
            host,
            port,
        )

        bad_client = self.simple_client(
            uri, ssl=True, tlsAllowInvalidCertificates=True, tlsCertificateKeyFile=CLIENT_PEM
        )

        with self.assertRaises(OperationFailure):
            await bad_client.pymongo_test.test.find_one()

        bad_client = self.simple_client(
            await async_client_context.pair,
            username="not the username",
            authMechanism="MONGODB-X509",
            ssl=True,
            tlsAllowInvalidCertificates=True,
            tlsCertificateKeyFile=CLIENT_PEM,
        )

        with self.assertRaises(OperationFailure):
            await bad_client.pymongo_test.test.find_one()

        # Invalid certificate (using CA certificate as client certificate)
        uri = "mongodb://%s@%s:%d/?authMechanism=MONGODB-X509" % (
            quote_plus(MONGODB_X509_USERNAME),
            host,
            port,
        )
        try:
            await connected(
                self.simple_client(
                    uri,
                    ssl=True,
                    tlsAllowInvalidCertificates=True,
                    tlsCertificateKeyFile=CA_PEM,
                    serverSelectionTimeoutMS=1000,
                )
            )
        except (ConnectionFailure, ConfigurationError):
            pass
        else:
            self.fail("Invalid certificate accepted.")

    @async_client_context.require_tlsCertificateKeyFile
    @async_client_context.require_no_api_version
    @ignore_deprecations
    async def test_connect_with_ca_bundle(self):
        def remove(path):
            try:
                os.remove(path)
            except OSError:
                pass

        temp_ca_bundle = os.path.join(CERT_PATH, "trusted-ca-bundle.pem")
        self.addCleanup(remove, temp_ca_bundle)
        # Add the CA cert file to the bundle.
        cat_files(temp_ca_bundle, CA_BUNDLE_PEM, CA_PEM)
        async with self.simple_client(
            "localhost", tls=True, tlsCertificateKeyFile=CLIENT_PEM, tlsCAFile=temp_ca_bundle
        ) as client:
            self.assertTrue(await client.admin.command("ping"))

    @async_client_context.require_async
    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    @unittest.skipUnless(HAVE_SSL, "The ssl module is not available.")
    async def test_pyopenssl_ignored_in_async(self):
        client = AsyncMongoClient(
            "mongodb://localhost:27017?tls=true&tlsAllowInvalidCertificates=true"
        )
        await client.admin.command("ping")  # command doesn't matter, just needs it to connect
        await client.close()


if __name__ == "__main__":
    unittest.main()
