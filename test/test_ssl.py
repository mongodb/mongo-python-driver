# Copyright 2011-2012 10gen, Inc.
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
import unittest
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo import MongoClient, MongoReplicaSetClient
from pymongo.common import HAS_SSL
from pymongo.errors import ConfigurationError, ConnectionFailure
from test import host, port, pair


CERT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         'certificates')
CLIENT_PEM = os.path.join(CERT_PATH, 'client.pem')
CA_PEM = os.path.join(CERT_PATH, 'ca.pem')
SIMPLE_SSL = False
CERT_SSL = False
SERVER_IS_RESOLVABLE = False

# To fully test this start a mongod instance (built with SSL support) like so:
# mongod --dbpath /path/to/data/directory --sslOnNormalPorts \
# --sslPEMKeyFile /path/to/mongo/jstests/libs/server.pem \
# --sslCAFile /path/to/mongo/jstests/libs/ca.pem \
# --sslCRLFile /path/to/mongo/jstests/libs/crl.pem \
# --sslWeakCertificateValidation
# Also, make sure you have 'server' as an alias for localhost in /etc/hosts
#
# Note: For all tests to pass with MongoReplicaSetConnection the replica
# set configuration must use 'server' for the hostname of all hosts.

def is_server_resolvable():
    """Returns True if 'server' is resolvable."""
    socket_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(1)
    try:
        try:
            socket.gethostbyname('server')
            return True
        except socket.error:
            return False
    finally:
        socket.setdefaulttimeout(socket_timeout)


if HAS_SSL:
    import ssl

    # Check this all once instead of before every test method below.

    # Is MongoDB configured for SSL?
    try:
        MongoClient(host, port, connectTimeoutMS=100, ssl=True)
        SIMPLE_SSL = True
    except ConnectionFailure:
        pass

    if SIMPLE_SSL:
        # Is MongoDB configured with server.pem, ca.pem, and crl.pem from
        # mongodb jstests/lib?
        try:
            MongoClient(host, port, connectTimeoutMS=100, ssl=True,
                        ssl_certfile=CLIENT_PEM)
            CERT_SSL = True
        except ConnectionFailure:
            pass

        if CERT_SSL:
            SERVER_IS_RESOLVABLE = is_server_resolvable()


class TestClientSSL(unittest.TestCase):

    def test_no_ssl_module(self):
        # Test that ConfigurationError is raised if the ssl
        # module isn't available.
        if HAS_SSL:
            raise SkipTest(
                "The ssl module is available, can't test what happens "
                "without it."
            )

        # Explicit
        self.assertRaises(ConfigurationError,
                          MongoClient, ssl=True)
        self.assertRaises(ConfigurationError,
                          MongoReplicaSetClient, ssl=True)
        # Implied
        self.assertRaises(ConfigurationError,
                          MongoClient, ssl_certfile=CLIENT_PEM)
        self.assertRaises(ConfigurationError,
                          MongoReplicaSetClient, ssl_certfile=CLIENT_PEM)

    def test_config_ssl(self):
        """Tests various ssl configurations"""
        self.assertRaises(ConfigurationError, MongoClient, ssl='foo')
        self.assertRaises(ConfigurationError,
                          MongoClient,
                          ssl=False,
                          ssl_certfile=CLIENT_PEM)
        self.assertRaises(TypeError, MongoClient, ssl=0)
        self.assertRaises(TypeError, MongoClient, ssl=5.5)
        self.assertRaises(TypeError, MongoClient, ssl=[])

        self.assertRaises(ConfigurationError, MongoReplicaSetClient, ssl='foo')
        self.assertRaises(ConfigurationError,
                          MongoReplicaSetClient,
                          ssl=False,
                          ssl_certfile=CLIENT_PEM)
        self.assertRaises(TypeError, MongoReplicaSetClient, ssl=0)
        self.assertRaises(TypeError, MongoReplicaSetClient, ssl=5.5)
        self.assertRaises(TypeError, MongoReplicaSetClient, ssl=[])

        self.assertRaises(IOError, MongoClient, ssl_certfile="NoSuchFile")
        self.assertRaises(TypeError, MongoClient, ssl_certfile=True)
        self.assertRaises(TypeError, MongoClient, ssl_certfile=[])
        self.assertRaises(IOError, MongoClient, ssl_keyfile="NoSuchFile")
        self.assertRaises(TypeError, MongoClient, ssl_keyfile=True)
        self.assertRaises(TypeError, MongoClient, ssl_keyfile=[])

        self.assertRaises(IOError,
                          MongoReplicaSetClient,
                          ssl_keyfile="NoSuchFile")
        self.assertRaises(IOError,
                          MongoReplicaSetClient,
                          ssl_certfile="NoSuchFile")
        self.assertRaises(TypeError, MongoReplicaSetClient, ssl_certfile=True)

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

        self.assertRaises(ConfigurationError,
                          MongoReplicaSetClient,
                          ssl=False,
                          ssl_keyfile=CLIENT_PEM)
        self.assertRaises(ConfigurationError,
                          MongoReplicaSetClient,
                          ssl=False,
                          ssl_certfile=CLIENT_PEM)
        self.assertRaises(ConfigurationError,
                          MongoReplicaSetClient,
                          ssl=False,
                          ssl_keyfile=CLIENT_PEM,
                          ssl_certfile=CLIENT_PEM)


class TestSSL(unittest.TestCase):

    def setUp(self):
        if not HAS_SSL:
            raise SkipTest("The ssl module is not available.")

        if sys.version.startswith('3.0'):
            raise SkipTest("Python 3.0.x has problems "
                           "with SSL and socket timeouts.")

        if not SIMPLE_SSL:
            raise SkipTest("No simple mongod available over SSL")

    def test_simple_ssl(self):
        # Expects the server to be running with ssl and with
        # no --sslPEMKeyFile or with --sslWeakCertificateValidation
        client = MongoClient(host, port, ssl=True)
        response = client.admin.command('ismaster')
        if 'setName' in response:
            client = MongoReplicaSetClient(pair,
                                           replicaSet=response['setName'],
                                           w=len(response['hosts']),
                                           ssl=True)

        db = client.pymongo_ssl_test
        db.test.drop()
        self.assertTrue(db.test.insert({'ssl': True}))
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    def test_cert_ssl(self):
        # Expects the server to be running with the the server.pem, ca.pem
        # and crl.pem provided in mongodb and the server tests eg:
        #
        #   --sslPEMKeyFile=jstests/libs/server.pem
        #   --sslCAFile=jstests/libs/ca.pem
        #   --sslCRLFile=jstests/libs/crl.pem
        #
        # Also requires an /etc/hosts entry where "server" is resolvable
        if not CERT_SSL:
            raise SkipTest("No mongod available over SSL with certs")

        client = MongoClient(host, port, ssl=True, ssl_certfile=CLIENT_PEM)
        response = client.admin.command('ismaster')
        if 'setName' in response:
            client = MongoReplicaSetClient(pair,
                                           replicaSet=response['setName'],
                                           w=len(response['hosts']),
                                           ssl=True, ssl_certfile=CLIENT_PEM)

        db = client.pymongo_ssl_test
        db.test.drop()
        self.assertTrue(db.test.insert({'ssl': True}))
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    def test_cert_ssl_implicitly_set(self):
        # Expects the server to be running with the the server.pem, ca.pem
        # and crl.pem provided in mongodb and the server tests eg:
        #
        #   --sslPEMKeyFile=jstests/libs/server.pem
        #   --sslCAFile=jstests/libs/ca.pem
        #   --sslCRLFile=jstests/libs/crl.pem
        #
        # Also requires an /etc/hosts entry where "server" is resolvable
        if not CERT_SSL:
            raise SkipTest("No mongod available over SSL with certs")

        client = MongoClient(host, port, ssl_certfile=CLIENT_PEM)
        response = client.admin.command('ismaster')
        if 'setName' in response:
            client = MongoReplicaSetClient(pair,
                                           replicaSet=response['setName'],
                                           w=len(response['hosts']),
                                           ssl_certfile=CLIENT_PEM)

        db = client.pymongo_ssl_test
        db.test.drop()
        self.assertTrue(db.test.insert({'ssl': True}))
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    def test_cert_ssl_validation(self):
        # Expects the server to be running with the the server.pem, ca.pem
        # and crl.pem provided in mongodb and the server tests eg:
        #
        #   --sslPEMKeyFile=jstests/libs/server.pem
        #   --sslCAFile=jstests/libs/ca.pem
        #   --sslCRLFile=jstests/libs/crl.pem
        #
        # Also requires an /etc/hosts entry where "server" is resolvable
        if not CERT_SSL:
            raise SkipTest("No mongod available over SSL with certs")

        if not SERVER_IS_RESOLVABLE:
            raise SkipTest("No hosts entry for 'server'. Cannot validate "
                           "hostname in the certificate")

        client = MongoClient('server',
                             ssl=True,
                             ssl_certfile=CLIENT_PEM,
                             ssl_cert_reqs=ssl.CERT_REQUIRED,
                             ssl_ca_certs=CA_PEM)
        response = client.admin.command('ismaster')
        if 'setName' in response:
            if response['primary'].split(":")[0] != 'server':
                raise SkipTest("No hosts in the replicaset for 'server'. "
                               "Cannot validate hostname in the certificate")

            client = MongoReplicaSetClient('server',
                                           replicaSet=response['setName'],
                                           w=len(response['hosts']),
                                           ssl=True,
                                           ssl_certfile=CLIENT_PEM,
                                           ssl_cert_reqs=ssl.CERT_REQUIRED,
                                           ssl_ca_certs=CA_PEM)

        db = client.pymongo_ssl_test
        db.test.drop()
        self.assertTrue(db.test.insert({'ssl': True}))
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    def test_cert_ssl_validation_optional(self):
        # Expects the server to be running with the the server.pem, ca.pem
        # and crl.pem provided in mongodb and the server tests eg:
        #
        #   --sslPEMKeyFile=jstests/libs/server.pem
        #   --sslCAFile=jstests/libs/ca.pem
        #   --sslCRLFile=jstests/libs/crl.pem
        #
        # Also requires an /etc/hosts entry where "server" is resolvable
        if not CERT_SSL:
            raise SkipTest("No mongod available over SSL with certs")

        if not SERVER_IS_RESOLVABLE:
            raise SkipTest("No hosts entry for 'server'. Cannot validate "
                           "hostname in the certificate")

        client = MongoClient('server',
                             ssl=True,
                             ssl_certfile=CLIENT_PEM,
                             ssl_cert_reqs=ssl.CERT_OPTIONAL,
                             ssl_ca_certs=CA_PEM)

        response = client.admin.command('ismaster')
        if 'setName' in response:
            if response['primary'].split(":")[0] != 'server':
                raise SkipTest("No hosts in the replicaset for 'server'. "
                               "Cannot validate hostname in the certificate")

            client = MongoReplicaSetClient('server',
                                           replicaSet=response['setName'],
                                           w=len(response['hosts']),
                                           ssl=True,
                                           ssl_certfile=CLIENT_PEM,
                                           ssl_cert_reqs=ssl.CERT_OPTIONAL,
                                           ssl_ca_certs=CA_PEM)

        db = client.pymongo_ssl_test
        db.test.drop()
        self.assertTrue(db.test.insert({'ssl': True}))
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    def test_cert_ssl_validation_hostname_fail(self):
        # Expects the server to be running with the the server.pem, ca.pem
        # and crl.pem provided in mongodb and the server tests eg:
        #
        #   --sslPEMKeyFile=jstests/libs/server.pem
        #   --sslCAFile=jstests/libs/ca.pem
        #   --sslCRLFile=jstests/libs/crl.pem
        if not CERT_SSL:
            raise SkipTest("No mongod available over SSL with certs")

        client = MongoClient(ssl=True, ssl_certfile=CLIENT_PEM)
        response = client.admin.command('ismaster')
        single_server = 'setName' not in response

        if single_server:
            try:
                MongoClient(pair,
                            ssl=True,
                            ssl_certfile=CLIENT_PEM,
                            ssl_cert_reqs=ssl.CERT_REQUIRED,
                            ssl_ca_certs=CA_PEM)
                self.fail("Invalid hostname should have failed")
            except:
                pass
        else:
            try:
                MongoReplicaSetClient(pair,
                                      replicaSet=response['setName'],
                                      w=len(response['hosts']),
                                      ssl=True,
                                      ssl_certfile=CLIENT_PEM,
                                      ssl_cert_reqs=ssl.CERT_OPTIONAL,
                                      ssl_ca_certs=CA_PEM)
                self.fail("Invalid hostname should have failed")
            except:
                pass


if __name__ == "__main__":
    unittest.main()
