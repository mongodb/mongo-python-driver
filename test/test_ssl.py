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

import os
import socket
import sys
from typing import Any

sys.path[0:0] = [""]

from test import HAVE_IPADDRESS, IntegrationTest, SkipTest, client_context, unittest
from test.utils import (
    EventListener,
    cat_files,
    connected,
    ignore_deprecations,
    remove_all_users,
)
from urllib.parse import quote_plus

from pymongo import MongoClient, ssl_support
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure
from pymongo.hello import HelloCompat
from pymongo.ssl_support import HAVE_SSL, _ssl, get_ssl_context
from pymongo.write_concern import WriteConcern

_HAVE_PYOPENSSL = False
try:
    # All of these must be available to use PyOpenSSL
    import OpenSSL  # noqa
    import requests  # noqa
    import service_identity  # noqa

    # Ensure service_identity>=18.1 is installed
    from service_identity.pyopenssl import verify_ip_address  # noqa

    from pymongo.ocsp_support import _load_trusted_ca_certs

    _HAVE_PYOPENSSL = True
except ImportError:
    _load_trusted_ca_certs = None  # type: ignore


if HAVE_SSL:
    import ssl

CERT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "certificates")
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


class TestClientSSL(unittest.TestCase):
    @unittest.skipIf(HAVE_SSL, "The ssl module is available, can't test what happens without it.")
    def test_no_ssl_module(self):
        # Explicit
        self.assertRaises(ConfigurationError, MongoClient, ssl=True)

        # Implied
        self.assertRaises(ConfigurationError, MongoClient, tlsCertificateKeyFile=CLIENT_PEM)

    @unittest.skipUnless(HAVE_SSL, "The ssl module is not available.")
    @ignore_deprecations
    def test_config_ssl(self):
        # Tests various ssl configurations
        self.assertRaises(ValueError, MongoClient, ssl="foo")
        self.assertRaises(
            ConfigurationError, MongoClient, tls=False, tlsCertificateKeyFile=CLIENT_PEM
        )
        self.assertRaises(TypeError, MongoClient, ssl=0)
        self.assertRaises(TypeError, MongoClient, ssl=5.5)
        self.assertRaises(TypeError, MongoClient, ssl=[])

        self.assertRaises(IOError, MongoClient, tlsCertificateKeyFile="NoSuchFile")
        self.assertRaises(TypeError, MongoClient, tlsCertificateKeyFile=True)
        self.assertRaises(TypeError, MongoClient, tlsCertificateKeyFile=[])

        # Test invalid combinations
        self.assertRaises(
            ConfigurationError, MongoClient, tls=False, tlsCertificateKeyFile=CLIENT_PEM
        )
        self.assertRaises(ConfigurationError, MongoClient, tls=False, tlsCAFile=CA_PEM)
        self.assertRaises(ConfigurationError, MongoClient, tls=False, tlsCRLFile=CRL_PEM)
        self.assertRaises(
            ConfigurationError, MongoClient, tls=False, tlsAllowInvalidCertificates=False
        )
        self.assertRaises(
            ConfigurationError, MongoClient, tls=False, tlsAllowInvalidHostnames=False
        )
        self.assertRaises(
            ConfigurationError, MongoClient, tls=False, tlsDisableOCSPEndpointCheck=False
        )

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_use_pyopenssl_when_available(self):
        self.assertTrue(_ssl.IS_PYOPENSSL)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "Cannot test without PyOpenSSL")
    def test_load_trusted_ca_certs(self):
        trusted_ca_certs = _load_trusted_ca_certs(CA_BUNDLE_PEM)
        self.assertEqual(2, len(trusted_ca_certs))


class TestSSL(IntegrationTest):
    saved_port: int

    def assertClientWorks(self, client):
        coll = client.pymongo_test.ssl_test.with_options(
            write_concern=WriteConcern(w=client_context.w)
        )
        coll.drop()
        coll.insert_one({"ssl": True})
        self.assertTrue(coll.find_one()["ssl"])
        coll.drop()

    @classmethod
    @unittest.skipUnless(HAVE_SSL, "The ssl module is not available.")
    def setUpClass(cls):
        super(TestSSL, cls).setUpClass()
        # MongoClient should connect to the primary by default.
        cls.saved_port = MongoClient.PORT
        MongoClient.PORT = client_context.port

    @classmethod
    def tearDownClass(cls):
        MongoClient.PORT = cls.saved_port
        super(TestSSL, cls).tearDownClass()

    @client_context.require_tls
    def test_simple_ssl(self):
        # Expects the server to be running with ssl and with
        # no --sslPEMKeyFile or with --sslWeakCertificateValidation
        self.assertClientWorks(self.client)

    @client_context.require_tlsCertificateKeyFile
    @ignore_deprecations
    def test_tlsCertificateKeyFilePassword(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        if not hasattr(ssl, "SSLContext") and not _ssl.IS_PYOPENSSL:
            self.assertRaises(
                ConfigurationError,
                MongoClient,
                "localhost",
                ssl=True,
                tlsCertificateKeyFile=CLIENT_ENCRYPTED_PEM,
                tlsCertificateKeyFilePassword="qwerty",
                tlsCAFile=CA_PEM,
                serverSelectionTimeoutMS=100,
            )
        else:
            connected(
                MongoClient(
                    "localhost",
                    ssl=True,
                    tlsCertificateKeyFile=CLIENT_ENCRYPTED_PEM,
                    tlsCertificateKeyFilePassword="qwerty",
                    tlsCAFile=CA_PEM,
                    serverSelectionTimeoutMS=5000,
                    **self.credentials  # type: ignore[arg-type]
                )
            )

            uri_fmt = (
                "mongodb://localhost/?ssl=true"
                "&tlsCertificateKeyFile=%s&tlsCertificateKeyFilePassword=qwerty"
                "&tlsCAFile=%s&serverSelectionTimeoutMS=5000"
            )
            connected(
                MongoClient(uri_fmt % (CLIENT_ENCRYPTED_PEM, CA_PEM), **self.credentials)  # type: ignore[arg-type]
            )

    @client_context.require_tlsCertificateKeyFile
    @client_context.require_no_auth
    @ignore_deprecations
    def test_cert_ssl_implicitly_set(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #

        # test that setting tlsCertificateKeyFile causes ssl to be set to True
        client = MongoClient(
            client_context.host,
            client_context.port,
            tlsAllowInvalidCertificates=True,
            tlsCertificateKeyFile=CLIENT_PEM,
        )
        response = client.admin.command(HelloCompat.LEGACY_CMD)
        if "setName" in response:
            client = MongoClient(
                client_context.pair,
                replicaSet=response["setName"],
                w=len(response["hosts"]),
                tlsAllowInvalidCertificates=True,
                tlsCertificateKeyFile=CLIENT_PEM,
            )

        self.assertClientWorks(client)

    @client_context.require_tlsCertificateKeyFile
    @client_context.require_no_auth
    @ignore_deprecations
    def test_cert_ssl_validation(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #
        client = MongoClient(
            "localhost",
            ssl=True,
            tlsCertificateKeyFile=CLIENT_PEM,
            tlsAllowInvalidCertificates=False,
            tlsCAFile=CA_PEM,
        )
        response = client.admin.command(HelloCompat.LEGACY_CMD)
        if "setName" in response:
            if response["primary"].split(":")[0] != "localhost":
                raise SkipTest(
                    "No hosts in the replicaset for 'localhost'. "
                    "Cannot validate hostname in the certificate"
                )

            client = MongoClient(
                "localhost",
                replicaSet=response["setName"],
                w=len(response["hosts"]),
                ssl=True,
                tlsCertificateKeyFile=CLIENT_PEM,
                tlsAllowInvalidCertificates=False,
                tlsCAFile=CA_PEM,
            )

        self.assertClientWorks(client)

        if HAVE_IPADDRESS:
            client = MongoClient(
                "127.0.0.1",
                ssl=True,
                tlsCertificateKeyFile=CLIENT_PEM,
                tlsAllowInvalidCertificates=False,
                tlsCAFile=CA_PEM,
            )
            self.assertClientWorks(client)

    @client_context.require_tlsCertificateKeyFile
    @client_context.require_no_auth
    @ignore_deprecations
    def test_cert_ssl_uri_support(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #
        uri_fmt = (
            "mongodb://localhost/?ssl=true&tlsCertificateKeyFile=%s&tlsAllowInvalidCertificates"
            "=%s&tlsCAFile=%s&tlsAllowInvalidHostnames=false"
        )
        client = MongoClient(uri_fmt % (CLIENT_PEM, "true", CA_PEM))
        self.assertClientWorks(client)

    @client_context.require_tlsCertificateKeyFile
    @client_context.require_server_resolvable
    @ignore_deprecations
    def test_cert_ssl_validation_hostname_matching(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        ctx = get_ssl_context(None, None, None, None, True, True, False)
        self.assertFalse(ctx.check_hostname)
        ctx = get_ssl_context(None, None, None, None, True, False, False)
        self.assertFalse(ctx.check_hostname)
        ctx = get_ssl_context(None, None, None, None, False, True, False)
        self.assertFalse(ctx.check_hostname)
        ctx = get_ssl_context(None, None, None, None, False, False, False)
        self.assertTrue(ctx.check_hostname)

        response = self.client.admin.command(HelloCompat.LEGACY_CMD)

        with self.assertRaises(ConnectionFailure):
            connected(
                MongoClient(
                    "server",
                    ssl=True,
                    tlsCertificateKeyFile=CLIENT_PEM,
                    tlsAllowInvalidCertificates=False,
                    tlsCAFile=CA_PEM,
                    serverSelectionTimeoutMS=500,
                    **self.credentials  # type: ignore[arg-type]
                )
            )

        connected(
            MongoClient(
                "server",
                ssl=True,
                tlsCertificateKeyFile=CLIENT_PEM,
                tlsAllowInvalidCertificates=False,
                tlsCAFile=CA_PEM,
                tlsAllowInvalidHostnames=True,
                serverSelectionTimeoutMS=500,
                **self.credentials  # type: ignore[arg-type]
            )
        )

        if "setName" in response:
            with self.assertRaises(ConnectionFailure):
                connected(
                    MongoClient(
                        "server",
                        replicaSet=response["setName"],
                        ssl=True,
                        tlsCertificateKeyFile=CLIENT_PEM,
                        tlsAllowInvalidCertificates=False,
                        tlsCAFile=CA_PEM,
                        serverSelectionTimeoutMS=500,
                        **self.credentials  # type: ignore[arg-type]
                    )
                )

            connected(
                MongoClient(
                    "server",
                    replicaSet=response["setName"],
                    ssl=True,
                    tlsCertificateKeyFile=CLIENT_PEM,
                    tlsAllowInvalidCertificates=False,
                    tlsCAFile=CA_PEM,
                    tlsAllowInvalidHostnames=True,
                    serverSelectionTimeoutMS=500,
                    **self.credentials  # type: ignore[arg-type]
                )
            )

    @client_context.require_tlsCertificateKeyFile
    @ignore_deprecations
    def test_tlsCRLFile_support(self):
        if not hasattr(ssl, "VERIFY_CRL_CHECK_LEAF") or _ssl.IS_PYOPENSSL:
            self.assertRaises(
                ConfigurationError,
                MongoClient,
                "localhost",
                ssl=True,
                tlsCAFile=CA_PEM,
                tlsCRLFile=CRL_PEM,
                serverSelectionTimeoutMS=100,
            )
        else:
            connected(
                MongoClient(
                    "localhost",
                    ssl=True,
                    tlsCAFile=CA_PEM,
                    serverSelectionTimeoutMS=100,
                    **self.credentials  # type: ignore[arg-type]
                )
            )

            with self.assertRaises(ConnectionFailure):
                connected(
                    MongoClient(
                        "localhost",
                        ssl=True,
                        tlsCAFile=CA_PEM,
                        tlsCRLFile=CRL_PEM,
                        serverSelectionTimeoutMS=100,
                        **self.credentials  # type: ignore[arg-type]
                    )
                )

            uri_fmt = "mongodb://localhost/?ssl=true&tlsCAFile=%s&serverSelectionTimeoutMS=100"
            connected(MongoClient(uri_fmt % (CA_PEM,), **self.credentials))  # type: ignore

            uri_fmt = (
                "mongodb://localhost/?ssl=true&tlsCRLFile=%s"
                "&tlsCAFile=%s&serverSelectionTimeoutMS=100"
            )
            with self.assertRaises(ConnectionFailure):
                connected(
                    MongoClient(uri_fmt % (CRL_PEM, CA_PEM), **self.credentials)  # type: ignore[arg-type]
                )

    @client_context.require_tlsCertificateKeyFile
    @client_context.require_server_resolvable
    @ignore_deprecations
    def test_validation_with_system_ca_certs(self):
        # Expects the server to be running with server.pem and ca.pem.
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #   --sslWeakCertificateValidation
        #
        self.patch_system_certs(CA_PEM)
        with self.assertRaises(ConnectionFailure):
            # Server cert is verified but hostname matching fails
            connected(
                MongoClient("server", ssl=True, serverSelectionTimeoutMS=100, **self.credentials)  # type: ignore[arg-type]
            )

        # Server cert is verified. Disable hostname matching.
        connected(
            MongoClient(
                "server",
                ssl=True,
                tlsAllowInvalidHostnames=True,
                serverSelectionTimeoutMS=100,
                **self.credentials  # type: ignore[arg-type]
            )
        )

        # Server cert and hostname are verified.
        connected(
            MongoClient("localhost", ssl=True, serverSelectionTimeoutMS=100, **self.credentials)  # type: ignore[arg-type]
        )

        # Server cert and hostname are verified.
        connected(
            MongoClient(
                "mongodb://localhost/?ssl=true&serverSelectionTimeoutMS=100", **self.credentials  # type: ignore[arg-type]
            )
        )

    def test_system_certs_config_error(self):
        ctx = get_ssl_context(None, None, None, None, True, True, False)
        if (sys.platform != "win32" and hasattr(ctx, "set_default_verify_paths")) or hasattr(
            ctx, "load_default_certs"
        ):
            raise SkipTest("Can't test when system CA certificates are loadable.")

        ssl_support: Any
        have_certifi = ssl_support.HAVE_CERTIFI
        have_wincertstore = ssl_support.HAVE_WINCERTSTORE
        # Force the test regardless of environment.
        ssl_support.HAVE_CERTIFI = False
        ssl_support.HAVE_WINCERTSTORE = False
        try:
            with self.assertRaises(ConfigurationError):
                MongoClient("mongodb://localhost/?ssl=true")
        finally:
            ssl_support.HAVE_CERTIFI = have_certifi
            ssl_support.HAVE_WINCERTSTORE = have_wincertstore

    def test_certifi_support(self):
        if hasattr(ssl, "SSLContext"):
            # SSLSocket doesn't provide ca_certs attribute on pythons
            # with SSLContext and SSLContext provides no information
            # about ca_certs.
            raise SkipTest("Can't test when SSLContext available.")
        ssl_support: Any
        if not ssl_support.HAVE_CERTIFI:
            raise SkipTest("Need certifi to test certifi support.")

        have_wincertstore = ssl_support.HAVE_WINCERTSTORE
        # Force the test on Windows, regardless of environment.
        ssl_support.HAVE_WINCERTSTORE = False
        try:
            ctx = get_ssl_context(None, None, CA_PEM, None, False, False, False)
            ssl_sock = ctx.wrap_socket(socket.socket())
            self.assertEqual(ssl_sock.ca_certs, CA_PEM)

            ctx = get_ssl_context(None, None, None, None, False, False, False)
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

        ctx = get_ssl_context(None, None, CA_PEM, None, False, False, False)
        ssl_sock = ctx.wrap_socket(socket.socket())
        self.assertEqual(ssl_sock.ca_certs, CA_PEM)

        ctx = get_ssl_context(None, None, None, None, False, False, False)
        ssl_sock = ctx.wrap_socket(socket.socket())
        self.assertEqual(ssl_sock.ca_certs, ssl_support._WINCERTS.name)

    @client_context.require_auth
    @client_context.require_tlsCertificateKeyFile
    @ignore_deprecations
    def test_mongodb_x509_auth(self):
        host, port = client_context.host, client_context.port
        self.addCleanup(remove_all_users, client_context.client["$external"])

        # Give x509 user all necessary privileges.
        client_context.create_user(
            "$external",
            MONGODB_X509_USERNAME,
            roles=[
                {"role": "readWriteAnyDatabase", "db": "admin"},
                {"role": "userAdminAnyDatabase", "db": "admin"},
            ],
        )

        noauth = MongoClient(
            client_context.pair,
            ssl=True,
            tlsAllowInvalidCertificates=True,
            tlsCertificateKeyFile=CLIENT_PEM,
        )
        self.addCleanup(noauth.close)

        with self.assertRaises(OperationFailure):
            noauth.pymongo_test.test.find_one()

        listener = EventListener()
        auth = MongoClient(
            client_context.pair,
            authMechanism="MONGODB-X509",
            ssl=True,
            tlsAllowInvalidCertificates=True,
            tlsCertificateKeyFile=CLIENT_PEM,
            event_listeners=[listener],
        )
        self.addCleanup(auth.close)

        # No error
        auth.pymongo_test.test.find_one()
        names = listener.started_command_names()
        if client_context.version.at_least(4, 4, -1):
            # Speculative auth skips the authenticate command.
            self.assertEqual(names, ["find"])
        else:
            self.assertEqual(names, ["authenticate", "find"])

        uri = "mongodb://%s@%s:%d/?authMechanism=MONGODB-X509" % (
            quote_plus(MONGODB_X509_USERNAME),
            host,
            port,
        )
        client = MongoClient(
            uri, ssl=True, tlsAllowInvalidCertificates=True, tlsCertificateKeyFile=CLIENT_PEM
        )
        self.addCleanup(client.close)
        # No error
        client.pymongo_test.test.find_one()

        uri = "mongodb://%s:%d/?authMechanism=MONGODB-X509" % (host, port)
        client = MongoClient(
            uri, ssl=True, tlsAllowInvalidCertificates=True, tlsCertificateKeyFile=CLIENT_PEM
        )
        self.addCleanup(client.close)
        # No error
        client.pymongo_test.test.find_one()
        # Auth should fail if username and certificate do not match
        uri = "mongodb://%s@%s:%d/?authMechanism=MONGODB-X509" % (
            quote_plus("not the username"),
            host,
            port,
        )

        bad_client = MongoClient(
            uri, ssl=True, tlsAllowInvalidCertificates=True, tlsCertificateKeyFile=CLIENT_PEM
        )
        self.addCleanup(bad_client.close)

        with self.assertRaises(OperationFailure):
            bad_client.pymongo_test.test.find_one()

        bad_client = MongoClient(
            client_context.pair,
            username="not the username",
            authMechanism="MONGODB-X509",
            ssl=True,
            tlsAllowInvalidCertificates=True,
            tlsCertificateKeyFile=CLIENT_PEM,
        )
        self.addCleanup(bad_client.close)

        with self.assertRaises(OperationFailure):
            bad_client.pymongo_test.test.find_one()

        # Invalid certificate (using CA certificate as client certificate)
        uri = "mongodb://%s@%s:%d/?authMechanism=MONGODB-X509" % (
            quote_plus(MONGODB_X509_USERNAME),
            host,
            port,
        )
        try:
            connected(
                MongoClient(
                    uri,
                    ssl=True,
                    tlsAllowInvalidCertificates=True,
                    tlsCertificateKeyFile=CA_PEM,
                    serverSelectionTimeoutMS=100,
                )
            )
        except (ConnectionFailure, ConfigurationError):
            pass
        else:
            self.fail("Invalid certificate accepted.")

    @client_context.require_tlsCertificateKeyFile
    @ignore_deprecations
    def test_connect_with_ca_bundle(self):
        def remove(path):
            try:
                os.remove(path)
            except OSError:
                pass

        temp_ca_bundle = os.path.join(CERT_PATH, "trusted-ca-bundle.pem")
        self.addCleanup(remove, temp_ca_bundle)
        # Add the CA cert file to the bundle.
        cat_files(temp_ca_bundle, CA_BUNDLE_PEM, CA_PEM)
        with MongoClient(
            "localhost", tls=True, tlsCertificateKeyFile=CLIENT_PEM, tlsCAFile=temp_ca_bundle
        ) as client:
            self.assertTrue(client.admin.command("ping"))


if __name__ == "__main__":
    unittest.main()
