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

sys.path[0:0] = [""]

try:
    from urllib.parse import quote_plus
except ImportError:
    # Python 2
    from urllib import quote_plus

from pymongo import MongoClient, ssl_support
from pymongo.errors import (ConfigurationError,
                            ConnectionFailure,
                            OperationFailure)
from pymongo.ssl_support import HAVE_SSL, get_ssl_context, validate_cert_reqs, _ssl
from pymongo.write_concern import WriteConcern
from test import (IntegrationTest,
                  client_context,
                  db_pwd,
                  db_user,
                  SkipTest,
                  unittest,
                  HAVE_IPADDRESS)
from test.utils import (EventListener,
                        cat_files,
                        connected,
                        remove_all_users)


_HAVE_PYOPENSSL = False
try:
    # All of these must be available to use PyOpenSSL
    import OpenSSL
    import requests
    import service_identity
    _HAVE_PYOPENSSL = True
except ImportError:
    pass

if _HAVE_PYOPENSSL:
    from pymongo.ocsp_support import _load_trusted_ca_certs
else:
    _load_trusted_ca_certs = None

if HAVE_SSL:
    import ssl

CERT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         'certificates')
CLIENT_PEM = os.path.join(CERT_PATH, 'client.pem')
CLIENT_ENCRYPTED_PEM = os.path.join(CERT_PATH, 'password_protected.pem')
CA_PEM = os.path.join(CERT_PATH, 'ca.pem')
CA_BUNDLE_PEM = os.path.join(CERT_PATH, 'trusted-ca.pem')
CRL_PEM = os.path.join(CERT_PATH, 'crl.pem')
MONGODB_X509_USERNAME = (
    "C=US,ST=New York,L=New York City,O=MDB,OU=Drivers,CN=client")

_PY37PLUS = sys.version_info[:2] >= (3, 7)

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

    @unittest.skipIf(HAVE_SSL, "The ssl module is available, can't test what "
                               "happens without it.")
    def test_no_ssl_module(self):
        # Explicit
        self.assertRaises(ConfigurationError,
                          MongoClient, ssl=True)

        # Implied
        self.assertRaises(ConfigurationError,
                          MongoClient, ssl_certfile=CLIENT_PEM)

    @unittest.skipUnless(HAVE_SSL, "The ssl module is not available.")
    def test_config_ssl(self):
        # Tests various ssl configurations
        self.assertRaises(ValueError, MongoClient, ssl='foo')
        self.assertRaises(ConfigurationError,
                          MongoClient,
                          ssl=False,
                          ssl_certfile=CLIENT_PEM)
        self.assertRaises(TypeError, MongoClient, ssl=0)
        self.assertRaises(TypeError, MongoClient, ssl=5.5)
        self.assertRaises(TypeError, MongoClient, ssl=[])

        self.assertRaises(IOError, MongoClient, ssl_certfile="NoSuchFile")
        self.assertRaises(TypeError, MongoClient, ssl_certfile=True)
        self.assertRaises(TypeError, MongoClient, ssl_certfile=[])
        self.assertRaises(IOError, MongoClient, ssl_keyfile="NoSuchFile")
        self.assertRaises(TypeError, MongoClient, ssl_keyfile=True)
        self.assertRaises(TypeError, MongoClient, ssl_keyfile=[])

        # Test invalid combinations
        self.assertRaises(ConfigurationError,
                          MongoClient,
                          ssl=False,
                          ssl_keyfile=CLIENT_PEM)
        self.assertRaises(ConfigurationError,
                          MongoClient,
                          ssl=False,
                          ssl_certfile=CLIENT_PEM)
        self.assertRaises(ConfigurationError,
                          MongoClient,
                          ssl=False,
                          ssl_keyfile=CLIENT_PEM,
                          ssl_certfile=CLIENT_PEM)

        self.assertRaises(
            ValueError, validate_cert_reqs, 'ssl_cert_reqs', 3)
        self.assertRaises(
            ValueError, validate_cert_reqs, 'ssl_cert_reqs', -1)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', None), None)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', ssl.CERT_NONE),
            ssl.CERT_NONE)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', ssl.CERT_OPTIONAL),
            ssl.CERT_OPTIONAL)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', ssl.CERT_REQUIRED),
            ssl.CERT_REQUIRED)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 0), ssl.CERT_NONE)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 1), ssl.CERT_OPTIONAL)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 2), ssl.CERT_REQUIRED)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 'CERT_NONE'), ssl.CERT_NONE)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 'CERT_OPTIONAL'),
            ssl.CERT_OPTIONAL)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 'CERT_REQUIRED'),
            ssl.CERT_REQUIRED)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "PyOpenSSL is not available.")
    def test_use_openssl_when_available(self):
        self.assertTrue(_ssl.IS_PYOPENSSL)

    @unittest.skipUnless(_HAVE_PYOPENSSL, "Cannot test without PyOpenSSL")
    def test_load_trusted_ca_certs(self):
        trusted_ca_certs = _load_trusted_ca_certs(CA_BUNDLE_PEM)
        self.assertEqual(2, len(trusted_ca_certs))


class TestSSL(IntegrationTest):

    def assertClientWorks(self, client):
        coll = client.pymongo_test.ssl_test.with_options(
            write_concern=WriteConcern(w=client_context.w))
        coll.drop()
        coll.insert_one({'ssl': True})
        self.assertTrue(coll.find_one()['ssl'])
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

    @client_context.require_ssl_certfile
    def test_ssl_pem_passphrase(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        if not hasattr(ssl, 'SSLContext') and not _ssl.IS_PYOPENSSL:
            self.assertRaises(
                ConfigurationError,
                MongoClient,
                'localhost',
                ssl=True,
                ssl_certfile=CLIENT_ENCRYPTED_PEM,
                ssl_pem_passphrase="qwerty",
                ssl_ca_certs=CA_PEM,
                serverSelectionTimeoutMS=100)
        else:
            connected(MongoClient('localhost',
                                  ssl=True,
                                  ssl_certfile=CLIENT_ENCRYPTED_PEM,
                                  ssl_pem_passphrase="qwerty",
                                  ssl_ca_certs=CA_PEM,
                                  serverSelectionTimeoutMS=5000,
                                  **self.credentials))

            uri_fmt = ("mongodb://localhost/?ssl=true"
                       "&ssl_certfile=%s&ssl_pem_passphrase=qwerty"
                       "&ssl_ca_certs=%s&serverSelectionTimeoutMS=5000")
            connected(MongoClient(uri_fmt % (CLIENT_ENCRYPTED_PEM, CA_PEM),
                                  **self.credentials))

    @client_context.require_ssl_certfile
    @client_context.require_no_auth
    def test_cert_ssl_implicitly_set(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #

        # test that setting ssl_certfile causes ssl to be set to True
        client = MongoClient(client_context.host, client_context.port,
                             ssl_cert_reqs=ssl.CERT_NONE,
                             ssl_certfile=CLIENT_PEM)
        response = client.admin.command('ismaster')
        if 'setName' in response:
            client = MongoClient(client_context.pair,
                                 replicaSet=response['setName'],
                                 w=len(response['hosts']),
                                 ssl_cert_reqs=ssl.CERT_NONE,
                                 ssl_certfile=CLIENT_PEM)

        self.assertClientWorks(client)

    @client_context.require_ssl_certfile
    @client_context.require_no_auth
    def test_cert_ssl_validation(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #
        client = MongoClient('localhost',
                             ssl=True,
                             ssl_certfile=CLIENT_PEM,
                             ssl_cert_reqs=ssl.CERT_REQUIRED,
                             ssl_ca_certs=CA_PEM)
        response = client.admin.command('ismaster')
        if 'setName' in response:
            if response['primary'].split(":")[0] != 'localhost':
                raise SkipTest("No hosts in the replicaset for 'localhost'. "
                               "Cannot validate hostname in the certificate")

            client = MongoClient('localhost',
                                 replicaSet=response['setName'],
                                 w=len(response['hosts']),
                                 ssl=True,
                                 ssl_certfile=CLIENT_PEM,
                                 ssl_cert_reqs=ssl.CERT_REQUIRED,
                                 ssl_ca_certs=CA_PEM)

        self.assertClientWorks(client)

        if HAVE_IPADDRESS:
            client = MongoClient('127.0.0.1',
                                 ssl=True,
                                 ssl_certfile=CLIENT_PEM,
                                 ssl_cert_reqs=ssl.CERT_REQUIRED,
                                 ssl_ca_certs=CA_PEM)
            self.assertClientWorks(client)

    @client_context.require_ssl_certfile
    @client_context.require_no_auth
    def test_cert_ssl_uri_support(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #
        uri_fmt = ("mongodb://localhost/?ssl=true&ssl_certfile=%s&ssl_cert_reqs"
                   "=%s&ssl_ca_certs=%s&ssl_match_hostname=true")
        client = MongoClient(uri_fmt % (CLIENT_PEM, 'CERT_REQUIRED', CA_PEM))
        self.assertClientWorks(client)

    @client_context.require_ssl_certfile
    @client_context.require_no_auth
    def test_cert_ssl_validation_optional(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #
        client = MongoClient('localhost',
                             ssl=True,
                             ssl_certfile=CLIENT_PEM,
                             ssl_cert_reqs=ssl.CERT_OPTIONAL,
                             ssl_ca_certs=CA_PEM)

        response = client.admin.command('ismaster')
        if 'setName' in response:
            if response['primary'].split(":")[0] != 'localhost':
                raise SkipTest("No hosts in the replicaset for 'localhost'. "
                               "Cannot validate hostname in the certificate")

            client = MongoClient('localhost',
                                 replicaSet=response['setName'],
                                 w=len(response['hosts']),
                                 ssl=True,
                                 ssl_certfile=CLIENT_PEM,
                                 ssl_cert_reqs=ssl.CERT_OPTIONAL,
                                 ssl_ca_certs=CA_PEM)

        self.assertClientWorks(client)

    @client_context.require_ssl_certfile
    @client_context.require_server_resolvable
    def test_cert_ssl_validation_hostname_matching(self):
        # Expects the server to be running with server.pem and ca.pem
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem

        # Python > 2.7.9. If SSLContext doesn't have load_default_certs
        # it also doesn't have check_hostname.
        ctx = get_ssl_context(
            None, None, None, None, ssl.CERT_NONE, None, False, True)
        if hasattr(ctx, 'load_default_certs'):
            self.assertFalse(ctx.check_hostname)
            ctx = get_ssl_context(
                None, None, None, None, ssl.CERT_NONE, None, True, True)
            self.assertFalse(ctx.check_hostname)
            ctx = get_ssl_context(
                None, None, None, None, ssl.CERT_REQUIRED, None, False, True)
            self.assertFalse(ctx.check_hostname)
            ctx = get_ssl_context(
                None, None, None, None, ssl.CERT_REQUIRED, None, True, True)
            if _PY37PLUS:
                self.assertTrue(ctx.check_hostname)
            else:
                self.assertFalse(ctx.check_hostname)

        response = self.client.admin.command('ismaster')

        with self.assertRaises(ConnectionFailure):
            connected(MongoClient('server',
                                  ssl=True,
                                  ssl_certfile=CLIENT_PEM,
                                  ssl_cert_reqs=ssl.CERT_REQUIRED,
                                  ssl_ca_certs=CA_PEM,
                                  serverSelectionTimeoutMS=500,
                                  **self.credentials))

        connected(MongoClient('server',
                              ssl=True,
                              ssl_certfile=CLIENT_PEM,
                              ssl_cert_reqs=ssl.CERT_REQUIRED,
                              ssl_ca_certs=CA_PEM,
                              ssl_match_hostname=False,
                              serverSelectionTimeoutMS=500,
                              **self.credentials))

        if 'setName' in response:
            with self.assertRaises(ConnectionFailure):
                connected(MongoClient('server',
                                      replicaSet=response['setName'],
                                      ssl=True,
                                      ssl_certfile=CLIENT_PEM,
                                      ssl_cert_reqs=ssl.CERT_REQUIRED,
                                      ssl_ca_certs=CA_PEM,
                                      serverSelectionTimeoutMS=500,
                                      **self.credentials))

            connected(MongoClient('server',
                                  replicaSet=response['setName'],
                                  ssl=True,
                                  ssl_certfile=CLIENT_PEM,
                                  ssl_cert_reqs=ssl.CERT_REQUIRED,
                                  ssl_ca_certs=CA_PEM,
                                  ssl_match_hostname=False,
                                  serverSelectionTimeoutMS=500,
                                  **self.credentials))

    @client_context.require_ssl_certfile
    def test_ssl_crlfile_support(self):
        if not hasattr(ssl, 'VERIFY_CRL_CHECK_LEAF') or _ssl.IS_PYOPENSSL:
            self.assertRaises(
                ConfigurationError,
                MongoClient,
                'localhost',
                ssl=True,
                ssl_ca_certs=CA_PEM,
                ssl_crlfile=CRL_PEM,
                serverSelectionTimeoutMS=100)
        else:
            connected(MongoClient('localhost',
                                  ssl=True,
                                  ssl_ca_certs=CA_PEM,
                                  serverSelectionTimeoutMS=100,
                                  **self.credentials))

            with self.assertRaises(ConnectionFailure):
                connected(MongoClient('localhost',
                                      ssl=True,
                                      ssl_ca_certs=CA_PEM,
                                      ssl_crlfile=CRL_PEM,
                                      serverSelectionTimeoutMS=100,
                                      **self.credentials))

            uri_fmt = ("mongodb://localhost/?ssl=true&"
                       "ssl_ca_certs=%s&serverSelectionTimeoutMS=100")
            connected(MongoClient(uri_fmt % (CA_PEM,),
                                  **self.credentials))

            uri_fmt = ("mongodb://localhost/?ssl=true&ssl_crlfile=%s"
                       "&ssl_ca_certs=%s&serverSelectionTimeoutMS=100")
            with self.assertRaises(ConnectionFailure):
                connected(MongoClient(uri_fmt % (CRL_PEM, CA_PEM),
                                      **self.credentials))

    @client_context.require_ssl_certfile
    @client_context.require_server_resolvable
    def test_validation_with_system_ca_certs(self):
        # Expects the server to be running with server.pem and ca.pem.
        #
        #   --sslPEMKeyFile=/path/to/pymongo/test/certificates/server.pem
        #   --sslCAFile=/path/to/pymongo/test/certificates/ca.pem
        #   --sslWeakCertificateValidation
        #
        if sys.platform == "win32":
            raise SkipTest("Can't test system ca certs on Windows.")

        if sys.version_info < (2, 7, 9):
            raise SkipTest("Can't load system CA certificates.")

        if (ssl.OPENSSL_VERSION.lower().startswith('libressl') and
                sys.platform == 'darwin' and not _ssl.IS_PYOPENSSL):
            raise SkipTest(
                "LibreSSL on OSX doesn't support setting CA certificates "
                "using SSL_CERT_FILE environment variable.")

        # Tell OpenSSL where CA certificates live.
        os.environ['SSL_CERT_FILE'] = CA_PEM
        try:
            with self.assertRaises(ConnectionFailure):
                # Server cert is verified but hostname matching fails
                connected(MongoClient('server',
                                      ssl=True,
                                      serverSelectionTimeoutMS=100,
                                      **self.credentials))

            # Server cert is verified. Disable hostname matching.
            connected(MongoClient('server',
                                  ssl=True,
                                  ssl_match_hostname=False,
                                  serverSelectionTimeoutMS=100,
                                  **self.credentials))

            # Server cert and hostname are verified.
            connected(MongoClient('localhost',
                                  ssl=True,
                                  serverSelectionTimeoutMS=100,
                                  **self.credentials))

            # Server cert and hostname are verified.
            connected(
                MongoClient(
                    'mongodb://localhost/?ssl=true&serverSelectionTimeoutMS=100',
                    **self.credentials))
        finally:
            os.environ.pop('SSL_CERT_FILE')

    def test_system_certs_config_error(self):
        ctx = get_ssl_context(
            None, None, None, None, ssl.CERT_NONE, None, False, True)
        if ((sys.platform != "win32"
             and hasattr(ctx, "set_default_verify_paths"))
                or hasattr(ctx, "load_default_certs")):
            raise SkipTest(
                "Can't test when system CA certificates are loadable.")

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
        if not ssl_support.HAVE_CERTIFI:
            raise SkipTest("Need certifi to test certifi support.")

        have_wincertstore = ssl_support.HAVE_WINCERTSTORE
        # Force the test on Windows, regardless of environment.
        ssl_support.HAVE_WINCERTSTORE = False
        try:
            ctx = get_ssl_context(
                None, None, None, CA_PEM, ssl.CERT_REQUIRED, None, True, True)
            ssl_sock = ctx.wrap_socket(socket.socket())
            self.assertEqual(ssl_sock.ca_certs, CA_PEM)

            ctx = get_ssl_context(None, None, None, None, None, None, True, True)
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

        ctx = get_ssl_context(
            None, None, None, CA_PEM, ssl.CERT_REQUIRED, None, True, True)
        ssl_sock = ctx.wrap_socket(socket.socket())
        self.assertEqual(ssl_sock.ca_certs, CA_PEM)

        ctx = get_ssl_context(None, None, None, None, None, None, True, True)
        ssl_sock = ctx.wrap_socket(socket.socket())
        self.assertEqual(ssl_sock.ca_certs, ssl_support._WINCERTS.name)

    @client_context.require_auth
    @client_context.require_ssl_certfile
    def test_mongodb_x509_auth(self):
        host, port = client_context.host, client_context.port
        ssl_client = MongoClient(
            client_context.pair,
            ssl=True,
            ssl_cert_reqs=ssl.CERT_NONE,
            ssl_certfile=CLIENT_PEM)
        self.addCleanup(remove_all_users, ssl_client['$external'])

        ssl_client.admin.authenticate(db_user, db_pwd)

        # Give x509 user all necessary privileges.
        client_context.create_user('$external', MONGODB_X509_USERNAME, roles=[
            {'role': 'readWriteAnyDatabase', 'db': 'admin'},
            {'role': 'userAdminAnyDatabase', 'db': 'admin'}])

        noauth = MongoClient(
            client_context.pair,
            ssl=True,
            ssl_cert_reqs=ssl.CERT_NONE,
            ssl_certfile=CLIENT_PEM)

        self.assertRaises(OperationFailure, noauth.pymongo_test.test.count)

        listener = EventListener()
        auth = MongoClient(
            client_context.pair,
            authMechanism='MONGODB-X509',
            ssl=True,
            ssl_cert_reqs=ssl.CERT_NONE,
            ssl_certfile=CLIENT_PEM,
            event_listeners=[listener])

        if client_context.version.at_least(3, 3, 12):
            # No error
            auth.pymongo_test.test.find_one()
            names = listener.started_command_names()
            if client_context.version.at_least(4, 4, -1):
                # Speculative auth skips the authenticate command.
                self.assertEqual(names, ['find'])
            else:
                self.assertEqual(names, ['authenticate', 'find'])
        else:
            # Should require a username
            with self.assertRaises(ConfigurationError):
                auth.pymongo_test.test.find_one()

        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'MONGODB-X509' % (
                   quote_plus(MONGODB_X509_USERNAME), host, port))
        client = MongoClient(uri,
                             ssl=True,
                             ssl_cert_reqs=ssl.CERT_NONE,
                             ssl_certfile=CLIENT_PEM)
        # No error
        client.pymongo_test.test.find_one()

        uri = 'mongodb://%s:%d/?authMechanism=MONGODB-X509' % (host, port)
        client = MongoClient(uri,
                             ssl=True,
                             ssl_cert_reqs=ssl.CERT_NONE,
                             ssl_certfile=CLIENT_PEM)
        if client_context.version.at_least(3, 3, 12):
            # No error
            client.pymongo_test.test.find_one()
        else:
            # Should require a username
            with self.assertRaises(ConfigurationError):
                client.pymongo_test.test.find_one()

        # Auth should fail if username and certificate do not match
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'MONGODB-X509' % (
                   quote_plus("not the username"), host, port))

        bad_client = MongoClient(
            uri, ssl=True, ssl_cert_reqs="CERT_NONE", ssl_certfile=CLIENT_PEM)

        with self.assertRaises(OperationFailure):
            bad_client.pymongo_test.test.find_one()

        bad_client = MongoClient(
                client_context.pair,
                username="not the username",
                authMechanism='MONGODB-X509',
                ssl=True,
                ssl_cert_reqs=ssl.CERT_NONE,
                ssl_certfile=CLIENT_PEM)

        with self.assertRaises(OperationFailure):
            bad_client.pymongo_test.test.find_one()

        # Invalid certificate (using CA certificate as client certificate)
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'MONGODB-X509' % (
                   quote_plus(MONGODB_X509_USERNAME), host, port))
        try:
            connected(MongoClient(uri,
                                  ssl=True,
                                  ssl_cert_reqs=ssl.CERT_NONE,
                                  ssl_certfile=CA_PEM,
                                  serverSelectionTimeoutMS=100))
        except (ConnectionFailure, ConfigurationError):
            pass
        else:
            self.fail("Invalid certificate accepted.")

    @client_context.require_ssl_certfile
    def test_connect_with_ca_bundle(self):
        def remove(path):
            try:
                os.remove(path)
            except OSError:
                pass

        temp_ca_bundle = os.path.join(CERT_PATH, 'trusted-ca-bundle.pem')
        self.addCleanup(remove, temp_ca_bundle)
        # Add the CA cert file to the bundle.
        cat_files(temp_ca_bundle, CA_BUNDLE_PEM, CA_PEM)
        with MongoClient('localhost',
                         tls=True,
                         tlsCertificateKeyFile=CLIENT_PEM,
                         tlsCAFile=temp_ca_bundle) as client:
            self.assertTrue(client.admin.command('ismaster'))


if __name__ == "__main__":
    unittest.main()
