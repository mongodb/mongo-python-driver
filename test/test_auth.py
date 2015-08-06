# Copyright 2013-2015 MongoDB, Inc.
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

"""Authentication Tests."""

import os
import socket
import sys
import threading
import unittest
import warnings

from urllib import quote_plus

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo import (MongoClient,
                     MongoReplicaSetClient,
                     auth)
from pymongo.auth import HAVE_KERBEROS
from pymongo.errors import (OperationFailure,
                            ConfigurationError,
                            ConnectionFailure,
                            AutoReconnect)
from pymongo.read_preferences import ReadPreference
from test import version, host, port, pair, auth_context, db_user, db_pwd
from test.test_bulk import BulkTestBase
from test.test_client import get_client
from test.test_pooling_base import get_pool
from test.test_replica_set_client import TestReplicaSetClientBase
from test.test_threads import AutoAuthenticateThreads
from test.utils import (is_mongos,
                        remove_all_users,
                        assertRaisesExactly,
                        one,
                        catch_warnings,
                        TestRequestMixin,
                        joinall,
                        get_command_line)

# YOU MUST RUN KINIT BEFORE RUNNING GSSAPI TESTS.
GSSAPI_HOST = os.environ.get('GSSAPI_HOST')
GSSAPI_PORT = int(os.environ.get('GSSAPI_PORT', '27017'))
PRINCIPAL = os.environ.get('PRINCIPAL')

SASL_HOST = os.environ.get('SASL_HOST')
SASL_PORT = int(os.environ.get('SASL_PORT', '27017'))
SASL_USER = os.environ.get('SASL_USER')
SASL_PASS = os.environ.get('SASL_PASS')
SASL_DB   = os.environ.get('SASL_DB', '$external')


def setUpModule():
    if not auth_context.auth_enabled:
        raise SkipTest("Server not started with --auth.")
    if (is_mongos(auth_context.client) and
            not version.at_least(auth_context.client, (2, 0, 0))):
        raise SkipTest("Auth with sharding requires MongoDB >= 2.0.0")
    auth_context.add_user_and_log_in()


def tearDownModule():
    if auth_context.auth_enabled:
        auth_context.remove_user_and_log_out()


class AutoAuthenticateThread(threading.Thread):
    """Used in testing threaded authentication.
    """

    def __init__(self, database):
        super(AutoAuthenticateThread, self).__init__()
        self.database = database
        self.success = True

    def run(self):
        try:
            self.database.command('dbstats')
        except OperationFailure:
            self.success = False


class TestGSSAPI(unittest.TestCase):

    def setUp(self):
        if not HAVE_KERBEROS:
            raise SkipTest('Kerberos module not available.')
        if not GSSAPI_HOST or not PRINCIPAL:
            raise SkipTest('Must set GSSAPI_HOST and PRINCIPAL to test GSSAPI')

    def test_gssapi_simple(self):

        client = MongoClient(GSSAPI_HOST, GSSAPI_PORT)
        # Without gssapiServiceName
        self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                 mechanism='GSSAPI'))
        client.database_names()
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'GSSAPI' % (quote_plus(PRINCIPAL), GSSAPI_HOST, GSSAPI_PORT))
        client = MongoClient(uri)
        client.database_names()

        # With gssapiServiceName
        self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                 mechanism='GSSAPI',
                                                 gssapiServiceName='mongodb'))
        client.database_names()
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'GSSAPI;gssapiServiceName=mongodb' % (quote_plus(PRINCIPAL),
                                                     GSSAPI_HOST, GSSAPI_PORT))
        client = MongoClient(uri)
        client.database_names()
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'GSSAPI;authMechanismProperties=SERVICE_NAME:mongodb' % (
               quote_plus(PRINCIPAL), GSSAPI_HOST, GSSAPI_PORT))
        client = MongoClient(uri)
        client.database_names()

        set_name = client.admin.command('ismaster').get('setName')
        if set_name:
            client = MongoReplicaSetClient(GSSAPI_HOST,
                                           port=GSSAPI_PORT,
                                           replicaSet=set_name)
            # Without gssapiServiceName
            self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                     mechanism='GSSAPI'))
            client.database_names()
            uri = ('mongodb://%s@%s:%d/?authMechanism=GSSAPI;replicaSet'
                   '=%s' % (quote_plus(PRINCIPAL),
                            GSSAPI_HOST, GSSAPI_PORT, str(set_name)))
            client = MongoReplicaSetClient(uri)
            client.database_names()

            # With gssapiServiceName
            self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                     mechanism='GSSAPI',
                                                     gssapiServiceName='mongodb'))
            client.database_names()
            uri = ('mongodb://%s@%s:%d/?authMechanism=GSSAPI;replicaSet'
                   '=%s;gssapiServiceName=mongodb' % (quote_plus(PRINCIPAL),
                                                      GSSAPI_HOST,
                                                      GSSAPI_PORT,
                                                      str(set_name)))
            client = MongoReplicaSetClient(uri)
            client.database_names()
            uri = ('mongodb://%s@%s:%d/?authMechanism=GSSAPI;replicaSet=%s;'
                   'authMechanismProperties=SERVICE_NAME:mongodb' % (
                   quote_plus(PRINCIPAL),
                   GSSAPI_HOST, GSSAPI_PORT, str(set_name)))
            client = MongoReplicaSetClient(uri)
            client.database_names()

    def test_gssapi_threaded(self):

        # Use auto_start_request=True to make sure each thread
        # uses a different socket.
        client = MongoClient(GSSAPI_HOST, auto_start_request=True)
        self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                 mechanism='GSSAPI'))

        threads = []
        for _ in xrange(4):
            threads.append(AutoAuthenticateThread(client.foo))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
            self.assertTrue(thread.success)

        set_name = client.admin.command('ismaster').get('setName')
        if set_name:
            preference = ReadPreference.SECONDARY
            client = MongoReplicaSetClient(GSSAPI_HOST,
                                           replicaSet=set_name,
                                           read_preference=preference)
            self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                     mechanism='GSSAPI'))
            self.assertTrue(client.foo.command('dbstats'))

            threads = []
            for _ in xrange(4):
                threads.append(AutoAuthenticateThread(client.foo))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
                self.assertTrue(thread.success)


class TestSASLPlain(unittest.TestCase):

    def setUp(self):
        if not SASL_HOST or not SASL_USER or not SASL_PASS:
            raise SkipTest('Must set SASL_HOST, '
                           'SASL_USER, and SASL_PASS to test SASL')

    def test_sasl_plain(self):

        client = MongoClient(SASL_HOST, SASL_PORT)
        self.assertTrue(client.ldap.authenticate(SASL_USER, SASL_PASS,
                                                 SASL_DB, 'PLAIN'))
        client.ldap.test.find_one()

        uri = ('mongodb://%s:%s@%s:%d/?authMechanism=PLAIN;'
               'authSource=%s' % (quote_plus(SASL_USER),
                                  quote_plus(SASL_PASS),
                                  SASL_HOST, SASL_PORT, SASL_DB))
        client = MongoClient(uri)
        client.ldap.test.find_one()

        set_name = client.admin.command('ismaster').get('setName')
        if set_name:
            client = MongoReplicaSetClient(SASL_HOST,
                                           port=SASL_PORT,
                                           replicaSet=set_name)
            self.assertTrue(client.ldap.authenticate(SASL_USER, SASL_PASS,
                                                     SASL_DB, 'PLAIN'))
            client.ldap.test.find_one()

            uri = ('mongodb://%s:%s@%s:%d/?authMechanism=PLAIN;'
                   'authSource=%s;replicaSet=%s' % (quote_plus(SASL_USER),
                                                    quote_plus(SASL_PASS),
                                                    SASL_HOST, SASL_PORT,
                                                    SASL_DB, str(set_name)))
            client = MongoReplicaSetClient(uri)
            client.ldap.test.find_one()

    def test_sasl_plain_bad_credentials(self):

        client = MongoClient(SASL_HOST, SASL_PORT)

        # Bad username
        self.assertRaises(OperationFailure, client.ldap.authenticate,
                          'not-user', SASL_PASS, SASL_DB, 'PLAIN')
        self.assertRaises(OperationFailure, client.ldap.test.find_one)
        self.assertRaises(OperationFailure, client.ldap.test.insert,
                          {"failed": True})

        # Bad password
        self.assertRaises(OperationFailure, client.ldap.authenticate,
                          SASL_USER, 'not-pwd', SASL_DB, 'PLAIN')
        self.assertRaises(OperationFailure, client.ldap.test.find_one)
        self.assertRaises(OperationFailure, client.ldap.test.insert,
                          {"failed": True})

        def auth_string(user, password):
            uri = ('mongodb://%s:%s@%s:%d/?authMechanism=PLAIN;'
                   'authSource=%s' % (quote_plus(user),
                                      quote_plus(password),
                                      SASL_HOST, SASL_PORT, SASL_DB))
            return uri

        # Just assert that we raise the right exception
        self.assertRaises(ConfigurationError, MongoClient,
                          auth_string('not-user', SASL_PASS))
        self.assertRaises(ConfigurationError, MongoClient,
                          auth_string(SASL_USER, 'not-pwd'))


class TestSCRAMSHA1(unittest.TestCase):

    def setUp(self):
        client = auth_context.client
        if not version.at_least(client, (2, 7, 2)):
            raise SkipTest("SCRAM-SHA-1 requires MongoDB >= 2.7.2")
        ismaster = client.admin.command('ismaster')
        self.is_mongos = ismaster.get('msg') == 'isdbgrid'
        self.set_name = ismaster.get('setName')

        # SCRAM-SHA-1 is always enabled beginning in 2.7.8.
        if not version.at_least(client, (2, 7, 8)):
            cmd_line = get_command_line(client)
            if 'SCRAM-SHA-1' not in cmd_line.get(
                'parsed', {}).get('setParameter',
                                  {}).get('authenticationMechanisms', ''):
                raise SkipTest('SCRAM-SHA-1 mechanism not enabled')

        if self.set_name:
            client.pymongo_test.add_user('user', 'pass',
                roles=['userAdmin', 'readWrite'],
                writeConcern={'w': len(ismaster['hosts'])})
        else:
            client.pymongo_test.add_user(
                'user', 'pass', roles=['userAdmin', 'readWrite'])

    def test_scram_sha1(self):
        client = MongoClient(host, port)
        self.assertTrue(client.pymongo_test.authenticate(
            'user', 'pass', mechanism='SCRAM-SHA-1'))
        client.pymongo_test.command('dbstats')

        client = MongoClient('mongodb://user:pass@%s:%d/pymongo_test'
                             '?authMechanism=SCRAM-SHA-1' % (host, port))
        client.pymongo_test.command('dbstats')

        if self.set_name:
            client = MongoReplicaSetClient(
                'mongodb://localhost:%d/?replicaSet=%s' % (port, self.set_name))
            self.assertTrue(client.pymongo_test.authenticate(
                'user', 'pass', mechanism='SCRAM-SHA-1'))
            client.pymongo_test.command('dbstats')

            uri = ('mongodb://user:pass'
                   '@%s:%d/pymongo_test?authMechanism=SCRAM-SHA-1'
                   '&replicaSet=%s' % (host, port, self.set_name))
            client = MongoReplicaSetClient(uri)
            client.pymongo_test.command('dbstats')
            client.read_preference = ReadPreference.SECONDARY
            client.pymongo_test.command('dbstats')

    def tearDown(self):
        auth_context.client.pymongo_test.remove_user('user')


class TestAuthURIOptions(unittest.TestCase):

    def setUp(self):
        client = MongoClient(host, port)
        response = client.admin.command('ismaster')
        self.set_name = str(response.get('setName', ''))
        auth_context.client.admin.add_user('admin', 'pass',
                                           roles=['userAdminAnyDatabase',
                                                  'dbAdminAnyDatabase',
                                                  'readWriteAnyDatabase',
                                                  'clusterAdmin'])
        client.admin.authenticate('admin', 'pass')
        client.pymongo_test.add_user('user', 'pass',
                                     roles=['userAdmin', 'readWrite'])

        if self.set_name:
            # GLE requires authentication.
            client.admin.authenticate('admin', 'pass')
            # Make sure the admin user is replicated after calling add_user
            # above. This avoids a race in the MRSC tests below. Adding a
            # user is just an insert into system.users.
            client.admin.command('getLastError', w=len(response['hosts']))
        self.client = client

    def tearDown(self):
        self.client.admin.authenticate('admin', 'pass')
        self.client.pymongo_test.remove_user('user')
        self.client.admin.remove_user('admin')
        self.client.pymongo_test.logout()
        self.client.admin.logout()
        self.client = None

    def test_uri_options(self):
        # Test default to admin
        client = MongoClient('mongodb://admin:pass@%s:%d' % (host, port))
        self.assertTrue(client.admin.command('dbstats'))

        if self.set_name:
            uri = ('mongodb://admin:pass'
                   '@%s:%d/?replicaSet=%s' % (host, port, self.set_name))
            client = MongoReplicaSetClient(uri)
            self.assertTrue(client.admin.command('dbstats'))
            client.read_preference = ReadPreference.SECONDARY
            self.assertTrue(client.admin.command('dbstats'))

        # Test explicit database
        uri = 'mongodb://user:pass@%s:%d/pymongo_test' % (host, port)
        client = MongoClient(uri)
        self.assertRaises(OperationFailure, client.admin.command, 'dbstats')
        self.assertTrue(client.pymongo_test.command('dbstats'))

        if self.set_name:
            uri = ('mongodb://user:pass@%s:%d'
                   '/pymongo_test?replicaSet=%s' % (host, port, self.set_name))
            client = MongoReplicaSetClient(uri)
            self.assertRaises(OperationFailure,
                              client.admin.command, 'dbstats')
            self.assertTrue(client.pymongo_test.command('dbstats'))
            client.read_preference = ReadPreference.SECONDARY
            self.assertTrue(client.pymongo_test.command('dbstats'))

        # Test authSource
        uri = ('mongodb://user:pass@%s:%d'
               '/pymongo_test2?authSource=pymongo_test' % (host, port))
        client = MongoClient(uri)
        self.assertRaises(OperationFailure,
                          client.pymongo_test2.command, 'dbstats')
        self.assertTrue(client.pymongo_test.command('dbstats'))

        if self.set_name:
            uri = ('mongodb://user:pass@%s:%d/pymongo_test2?replicaSet='
                   '%s;authSource=pymongo_test' % (host, port, self.set_name))
            client = MongoReplicaSetClient(uri)
            self.assertRaises(OperationFailure,
                              client.pymongo_test2.command, 'dbstats')
            self.assertTrue(client.pymongo_test.command('dbstats'))
            client.read_preference = ReadPreference.SECONDARY
            self.assertTrue(client.pymongo_test.command('dbstats'))


class TestDelegatedAuth(unittest.TestCase):

    def setUp(self):
        self.client = MongoClient(host, port)
        authed_client = auth_context.client
        if not version.at_least(authed_client, (2, 4, 0)):
            raise SkipTest('Delegated authentication requires MongoDB >= 2.4.0')
        if version.at_least(authed_client, (2, 5, 3, -1)):
            raise SkipTest('Delegated auth does not exist in MongoDB >= 2.5.3')
        # Give admin all privileges.
        authed_client.admin.add_user('admin', 'pass',
                                     roles=['readAnyDatabase',
                                            'readWriteAnyDatabase',
                                            'userAdminAnyDatabase',
                                            'dbAdminAnyDatabase',
                                            'clusterAdmin'])

    def tearDown(self):
        self.client.admin.authenticate('admin', 'pass')
        self.client.pymongo_test.remove_user('user')
        self.client.pymongo_test2.remove_user('user')
        self.client.pymongo_test2.foo.remove()
        self.client.admin.remove_user('admin')
        self.client.admin.logout()
        self.client = None

    def test_delegated_auth(self):
        self.client.admin.authenticate('admin', 'pass')
        self.client.pymongo_test2.foo.remove()
        self.client.pymongo_test2.foo.insert({})
        # User definition with no roles in pymongo_test.
        self.client.pymongo_test.add_user('user', 'pass', roles=[])
        # Delegate auth to pymongo_test.
        self.client.pymongo_test2.add_user('user',
                                           userSource='pymongo_test',
                                           roles=['read'])
        self.client.admin.logout()
        self.assertRaises(OperationFailure,
                          self.client.pymongo_test2.foo.find_one)
        # Auth must occur on the db where the user is defined.
        self.assertRaises(OperationFailure,
                          self.client.pymongo_test2.authenticate,
                          'user', 'pass')
        # Auth directly
        self.assertTrue(self.client.pymongo_test.authenticate('user', 'pass'))
        self.assertTrue(self.client.pymongo_test2.foo.find_one())
        self.client.pymongo_test.logout()
        self.assertRaises(OperationFailure,
                          self.client.pymongo_test2.foo.find_one)
        # Auth using source
        self.assertTrue(self.client.pymongo_test2.authenticate(
            'user', 'pass', source='pymongo_test'))
        self.assertTrue(self.client.pymongo_test2.foo.find_one())
        # Must logout from the db authenticate was called on.
        self.client.pymongo_test2.logout()
        self.assertRaises(OperationFailure,
                          self.client.pymongo_test2.foo.find_one)


class TestClientAuth(unittest.TestCase):

    def test_copy_db(self):
        authed_client = auth_context.client
        if is_mongos(authed_client):
            raise SkipTest("SERVER-6427")

        c = MongoClient(host, port)

        authed_client.admin.add_user("admin", "password")
        c.admin.authenticate("admin", "password")
        c.drop_database("pymongo_test")
        c.drop_database("pymongo_test1")
        c.pymongo_test.test.insert({"foo": "bar"})

        try:
            c.pymongo_test.add_user("mike", "password")

            self.assertRaises(OperationFailure, c.copy_database,
                              "pymongo_test", "pymongo_test1",
                              username="foo", password="bar")
            self.assertFalse("pymongo_test1" in c.database_names())

            self.assertRaises(OperationFailure, c.copy_database,
                              "pymongo_test", "pymongo_test1",
                              username="mike", password="bar")
            self.assertFalse("pymongo_test1" in c.database_names())

            c.copy_database("pymongo_test", "pymongo_test1",
                            username="mike", password="password")
            self.assertTrue("pymongo_test1" in c.database_names())
            self.assertEqual("bar", c.pymongo_test1.test.find_one()["foo"])
        finally:
            # Cleanup
            remove_all_users(c.pymongo_test)
            c.admin.remove_user("admin")
            c.disconnect()

    def test_auth_from_uri(self):
        c = MongoClient(host, port)
        auth_context.client.admin.add_user("admin", "pass")
        c.admin.authenticate("admin", "pass")
        try:
            c.pymongo_test.add_user("user", "pass",
                                    roles=['userAdmin', 'readWrite'])

            self.assertRaises(ConfigurationError, MongoClient,
                              "mongodb://foo:bar@%s:%d" % (host, port))
            self.assertRaises(ConfigurationError, MongoClient,
                              "mongodb://admin:bar@%s:%d" % (host, port))
            self.assertRaises(ConfigurationError, MongoClient,
                              "mongodb://user:pass@%s:%d" % (host, port))
            MongoClient("mongodb://admin:pass@%s:%d" % (host, port))

            self.assertRaises(ConfigurationError, MongoClient,
                              "mongodb://admin:pass@%s:%d/pymongo_test" %
                              (host, port))
            self.assertRaises(ConfigurationError, MongoClient,
                              "mongodb://user:foo@%s:%d/pymongo_test" %
                              (host, port))
            MongoClient("mongodb://user:pass@%s:%d/pymongo_test" %
                       (host, port))

            # Auth with lazy connection.
            MongoClient(
                "mongodb://user:pass@%s:%d/pymongo_test" % (host, port),
                _connect=False).pymongo_test.test.find_one()

            # Wrong password.
            bad_client = MongoClient(
                "mongodb://user:wrong@%s:%d/pymongo_test" % (host, port),
                _connect=False)

            self.assertRaises(OperationFailure,
                              bad_client.pymongo_test.test.find_one)

        finally:
            # Clean up.
            remove_all_users(c.pymongo_test)
            c.admin.remove_user('admin')

    def test_lazy_auth_raises_operation_failure(self):
        lazy_client = MongoClient(
            "mongodb://user:wrong@%s:%d/pymongo_test" % (host, port),
            _connect=False)

        assertRaisesExactly(
            OperationFailure, lazy_client.test.collection.find_one)

    def test_unix_socket(self):
        authed_client = auth_context.client
        if not hasattr(socket, "AF_UNIX"):
            raise SkipTest("UNIX-sockets are not supported on this system")
        if (sys.platform == 'darwin' and
                not version.at_least(authed_client, (2, 7, 1))):
            raise SkipTest("SERVER-8492")

        mongodb_socket = '/tmp/mongodb-27017.sock'
        if not os.access(mongodb_socket, os.R_OK):
            raise SkipTest("Socket file is not accessable")

        self.assertTrue(MongoClient("mongodb://%s" % mongodb_socket))

        authed_client.admin.add_user('admin', 'pass')

        try:
            client = MongoClient("mongodb://%s" % mongodb_socket)
            client.admin.authenticate('admin', 'pass')
            client.pymongo_test.test.save({"dummy": "object"})

            # Confirm we can read via the socket
            dbs = client.database_names()
            self.assertTrue("pymongo_test" in dbs)

            # Confirm it fails with a missing socket
            self.assertRaises(ConnectionFailure, MongoClient,
                              "mongodb:///tmp/none-existent.sock")
        finally:
            authed_client.admin.remove_user('admin')

    def test_auth_network_error(self):
        # Make sure there's no semaphore leak if we get a network error
        # when authenticating a new socket with cached credentials.
        auth_client = get_client()

        auth_context.client.admin.add_user('admin', 'password')
        auth_client.admin.authenticate('admin', 'password')
        try:
            # Get a client with one socket so we detect if it's leaked.
            c = get_client(max_pool_size=1, waitQueueTimeoutMS=1)

            # Simulate an authenticate() call on a different socket.
            credentials = auth._build_credentials_tuple(
                'DEFAULT', 'admin',
                unicode('admin'), unicode('password'),
                {})

            c._cache_credentials('test', credentials, connect=False)

            # Cause a network error on the actual socket.
            pool = get_pool(c)
            socket_info = one(pool.sockets)
            socket_info.sock.close()

            # In __check_auth, the client authenticates its socket with the
            # new credential, but gets a socket.error. Should be reraised as
            # AutoReconnect.
            self.assertRaises(AutoReconnect, c.test.collection.find_one)

            # No semaphore leak, the pool is allowed to make a new socket.
            c.test.collection.find_one()
        finally:
            auth_client.admin.remove_user('admin')


class TestDatabaseAuth(unittest.TestCase):
    def setUp(self):
        self.client = MongoClient(host, port)

    def test_authenticate_add_remove_user(self):
        authed_client = auth_context.client
        db = authed_client.pymongo_test

        # Configuration errors
        self.assertRaises(ValueError, db.add_user, "user", '')
        self.assertRaises(TypeError, db.add_user, "user", 'password', 15)
        self.assertRaises(ConfigurationError, db.add_user,
                          "user", 'password', 'True')
        self.assertRaises(ConfigurationError, db.add_user,
                          "user", 'password', True, roles=['read'])

        if version.at_least(authed_client, (2, 5, 3, -1)):
            ctx = catch_warnings()
            try:
                warnings.simplefilter("error", DeprecationWarning)
                self.assertRaises(DeprecationWarning, db.add_user,
                                  "user", "password")
                self.assertRaises(DeprecationWarning, db.add_user,
                                  "user", "password", True)
            finally:
                ctx.exit()

            self.assertRaises(ConfigurationError, db.add_user,
                              "user", "password", digestPassword=True)

        authed_client.admin.add_user("admin", "password")
        self.client.admin.authenticate("admin", "password")
        db = self.client.pymongo_test

        try:
            # Add / authenticate / remove
            db.add_user("mike", "password")
            self.assertRaises(TypeError, db.authenticate, 5, "password")
            self.assertRaises(TypeError, db.authenticate, "mike", 5)
            self.assertRaises(OperationFailure,
                              db.authenticate, "mike", "not a real password")
            self.assertRaises(OperationFailure,
                              db.authenticate, "faker", "password")
            self.assertTrue(db.authenticate("mike", "password"))
            db.logout()
            self.assertTrue(db.authenticate(u"mike", u"password"))
            db.remove_user("mike")
            db.logout()

            self.assertRaises(OperationFailure,
                              db.authenticate, "mike", "password")

            # Add / authenticate / change password
            self.assertRaises(OperationFailure,
                              db.authenticate, "Gustave", u"Dor\xe9")
            db.add_user("Gustave", u"Dor\xe9")
            self.assertTrue(db.authenticate("Gustave", u"Dor\xe9"))
            db.add_user("Gustave", "password")
            db.logout()
            self.assertRaises(OperationFailure,
                              db.authenticate, "Gustave", u"Dor\xe9")
            self.assertTrue(db.authenticate("Gustave", u"password"))

            if not version.at_least(authed_client, (2, 5, 3, -1)):
                # Add a readOnly user
                db.add_user("Ross", "password", read_only=True)
                db.logout()
                self.assertTrue(db.authenticate("Ross", u"password"))
                self.assertTrue(
                    db.system.users.find({"readOnly": True}).count())
                db.logout()

        # Cleanup
        finally:
            remove_all_users(db)
            self.client.admin.remove_user("admin")
            self.client.admin.logout()

    def test_make_user_readonly(self):
        admin = self.client.admin
        auth_context.client.admin.add_user('admin', 'pw')
        admin.authenticate('admin', 'pw')

        db = self.client.pymongo_test

        try:
            # Make a read-write user.
            db.add_user('jesse', 'pw')
            admin.logout()

            # Check that we're read-write by default.
            db.authenticate('jesse', 'pw')
            db.collection.insert({})
            db.logout()

            # Make the user read-only.
            admin.authenticate('admin', 'pw')
            db.add_user('jesse', 'pw', read_only=True)
            admin.logout()

            db.authenticate('jesse', 'pw')
            self.assertRaises(OperationFailure, db.collection.insert, {})
        finally:
            # Cleanup
            admin.authenticate('admin', 'pw')
            remove_all_users(db)
            admin.remove_user("admin")
            admin.logout()

    def test_default_roles(self):
        authed_client = auth_context.client
        if not version.at_least(authed_client, (2, 5, 3, -1)):
            raise SkipTest("Default roles only exist in MongoDB >= 2.5.3")

        # "Admin" user
        db = self.client.admin
        authed_client.admin.add_user('admin', 'pass')
        try:
            db.authenticate('admin', 'pass')
            info = db.command('usersInfo', 'admin')['users'][0]
            self.assertEqual("root", info['roles'][0]['role'])

            # Read only "admin" user
            db.add_user('ro-admin', 'pass', read_only=True)
            db.logout()
            db.authenticate('ro-admin', 'pass')
            info = db.command('usersInfo', 'ro-admin')['users'][0]
            self.assertEqual("readAnyDatabase", info['roles'][0]['role'])
            db.logout()

        # Cleanup
        finally:
            db.authenticate('admin', 'pass')
            db.remove_user('ro-admin')
            db.remove_user('admin')
            db.logout()

        db.connection.disconnect()

        # "Non-admin" user
        db = self.client.pymongo_test
        authed_client.pymongo_test.add_user('user', 'pass')
        try:
            db.authenticate('user', 'pass')
            info = db.command('usersInfo', 'user')['users'][0]
            self.assertEqual("dbOwner", info['roles'][0]['role'])

            # Read only "Non-admin" user
            db.add_user('ro-user', 'pass', read_only=True)
            db.logout()
            db.authenticate('ro-user', 'pass')
            info = db.command('usersInfo', 'ro-user')['users'][0]
            self.assertEqual("read", info['roles'][0]['role'])
            db.logout()

        # Cleanup
        finally:
            db.authenticate('user', 'pass')
            remove_all_users(db)
            db.logout()

    def test_new_user_cmds(self):
        authed_client = auth_context.client
        if not version.at_least(authed_client, (2, 5, 3, -1)):
            raise SkipTest("User manipulation through commands "
                           "requires MongoDB >= 2.5.3")

        db = self.client.pymongo_test
        authed_client.pymongo_test.add_user("amalia", "password",
                                            roles=["userAdmin"])
        db.authenticate("amalia", "password")
        try:
            # This tests the ability to update user attributes.
            db.add_user("amalia", "new_password",
                        customData={"secret": "koalas"})

            user_info = db.command("usersInfo", "amalia")
            self.assertTrue(user_info["users"])
            amalia_user = user_info["users"][0]
            self.assertEqual(amalia_user["user"], "amalia")
            self.assertEqual(amalia_user["customData"], {"secret": "koalas"})
        finally:
            db.remove_user("amalia")
            db.logout()

    def test_authenticate_and_safe(self):
        db = auth_context.client.auth_test

        db.add_user("bernie", "password",
                    roles=["userAdmin", "dbAdmin", "readWrite"])
        db.authenticate("bernie", "password")
        try:
            db.test.remove({})
            self.assertTrue(db.test.insert({"bim": "baz"}))
            self.assertEqual(1, db.test.count())

            self.assertEqual(1,
                             db.test.update({"bim": "baz"},
                                            {"$set": {"bim": "bar"}}).get('n'))

            self.assertEqual(1,
                             db.test.remove({}).get('n'))

            self.assertEqual(0, db.test.count())
        finally:
            db.remove_user("bernie")
            db.logout()

    def test_authenticate_and_request(self):
        # Database.authenticate() needs to be in a request - check that it
        # always runs in a request, and that it restores the request state
        # (in or not in a request) properly when it's finished.
        self.assertFalse(self.client.auto_start_request)
        db = self.client.pymongo_test
        auth_context.client.pymongo_test.add_user(
            "mike", "password",
            roles=["userAdmin", "dbAdmin", "readWrite"])
        try:
            self.assertFalse(self.client.in_request())
            self.assertTrue(db.authenticate("mike", "password"))
            self.assertFalse(self.client.in_request())

            request_cx = get_client(auto_start_request=True)
            request_db = request_cx.pymongo_test
            self.assertTrue(request_db.authenticate("mike", "password"))
            self.assertTrue(request_cx.in_request())
        finally:
            db.authenticate("mike", "password")
            db.remove_user("mike")
            db.logout()
            request_db.logout()

    def test_authenticate_multiple(self):
        client = get_client()
        authed_client = auth_context.client
        if (is_mongos(authed_client) and not
                version.at_least(authed_client, (2, 2, 0))):
            raise SkipTest("Need mongos >= 2.2.0")

        # Setup
        authed_client.pymongo_test.test.drop()
        authed_client.pymongo_test1.test.drop()
        users_db = client.pymongo_test
        admin_db = client.admin
        other_db = client.pymongo_test1

        authed_client.admin.add_user('admin', 'pass',
                                     roles=["userAdminAnyDatabase", "dbAdmin",
                                            "clusterAdmin", "readWrite"])
        try:
            self.assertTrue(admin_db.authenticate('admin', 'pass'))

            if version.at_least(self.client, (2, 5, 3, -1)):
                admin_db.add_user('ro-admin', 'pass',
                                  roles=["userAdmin", "readAnyDatabase"])
            else:
                admin_db.add_user('ro-admin', 'pass', read_only=True)

            users_db.add_user('user', 'pass',
                              roles=["userAdmin", "readWrite"])

            admin_db.logout()
            self.assertRaises(OperationFailure, users_db.test.find_one)

            # Regular user should be able to query its own db, but
            # no other.
            users_db.authenticate('user', 'pass')
            self.assertEqual(0, users_db.test.count())
            self.assertRaises(OperationFailure, other_db.test.find_one)

            # Admin read-only user should be able to query any db,
            # but not write.
            admin_db.authenticate('ro-admin', 'pass')
            self.assertEqual(0, other_db.test.count())
            self.assertRaises(OperationFailure,
                              other_db.test.insert, {})

            # Force close all sockets
            client.disconnect()

            # We should still be able to write to the regular user's db
            self.assertTrue(users_db.test.remove())
            # And read from other dbs...
            self.assertEqual(0, other_db.test.count())
            # But still not write to other dbs...
            self.assertRaises(OperationFailure,
                              other_db.test.insert, {})

        # Cleanup
        finally:
            admin_db.logout()
            users_db.logout()
            admin_db.authenticate('admin', 'pass')
            remove_all_users(users_db)
            admin_db.remove_user('ro-admin')
            admin_db.remove_user('admin')


class TestReplicaSetClientAuth(TestReplicaSetClientBase, TestRequestMixin):

    def test_init_disconnected_with_auth_failure(self):
        c = MongoReplicaSetClient(
            "mongodb://user:pass@somedomainthatdoesntexist", replicaSet="rs",
            connectTimeoutMS=1, _connect=False)

        self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

    def test_init_disconnected_with_auth(self):
        c = self._get_client()

        auth_context.client.admin.add_user("admin", "pass")
        c.admin.authenticate("admin", "pass")
        try:
            c.pymongo_test.add_user("user", "pass",
                                    roles=['readWrite', 'userAdmin'])

            # Auth with lazy connection.
            host = one(self.hosts)
            uri = "mongodb://user:pass@%s:%d/pymongo_test?replicaSet=%s" % (
                host[0], host[1], self.name)

            authenticated_client = MongoReplicaSetClient(uri, _connect=False)
            authenticated_client.pymongo_test.test.find_one()

            # Wrong password.
            bad_uri = ("mongodb://user:wrong@%s:%d/pymongo_test?replicaSet=%s"
                       % (host[0], host[1], self.name))

            bad_client = MongoReplicaSetClient(bad_uri, _connect=False)
            self.assertRaises(
                OperationFailure, bad_client.pymongo_test.test.find_one)

        finally:
            # Clean up.
            remove_all_users(c.pymongo_test)
            c.admin.remove_user('admin')

    def test_lazy_auth_raises_operation_failure(self):
        lazy_client = MongoReplicaSetClient(
            "mongodb://user:wrong@%s/pymongo_test" % pair,
            replicaSet=self.name,
            _connect=False)

        assertRaisesExactly(
            OperationFailure, lazy_client.test.collection.find_one)

    def test_copy_db(self):
        authed_client = auth_context.client

        authed_client.admin.add_user("admin", "password")
        c = self._get_client()
        c.admin.authenticate("admin", "password")
        c.drop_database("pymongo_test1")
        c.pymongo_test.test.insert({"foo": "bar"})

        try:
            c.pymongo_test.add_user("mike", "password")

            self.assertRaises(OperationFailure, c.copy_database,
                              "pymongo_test", "pymongo_test1",
                              username="foo", password="bar")
            self.assertFalse("pymongo_test1" in c.database_names())

            self.assertRaises(OperationFailure, c.copy_database,
                              "pymongo_test", "pymongo_test1",
                              username="mike", password="bar")
            self.assertFalse("pymongo_test1" in c.database_names())

            c.copy_database("pymongo_test", "pymongo_test1",
                            username="mike", password="password")
            self.assertTrue("pymongo_test1" in c.database_names())
            res = c.pymongo_test1.test.find_one(_must_use_master=True)
            self.assertEqual("bar", res["foo"])
        finally:
            # Cleanup
            remove_all_users(c.pymongo_test)
            c.admin.remove_user("admin")
        c.close()

    def test_auth_network_error(self):
        # Make sure there's no semaphore leak if we get a network error
        # when authenticating a new socket with cached credentials.
        # Get a client with one socket so we detect if it's leaked.

        # Generous wait queue timeout in case the main thread contends
        # with the monitor, though -- a semaphore leak will be detected
        # eventually, even with a long timeout.
        c = self._get_client(max_pool_size=1, waitQueueTimeoutMS=10000)

        # Simulate an authenticate() call on a different socket.
        credentials = auth._build_credentials_tuple(
            'DEFAULT', 'admin',
            unicode(db_user), unicode(db_pwd),
            {})

        c._cache_credentials('test', credentials, connect=False)

        # Cause a network error on the actual socket.
        pool = get_pool(c)
        socket_info = one(pool.sockets)
        socket_info.sock.close()

        # In __check_auth, the client authenticates its socket with the
        # new credential, but gets a socket.error. Reraised as AutoReconnect,
        # unless periodic monitoring or Pool._check prevent the error.
        try:
            c.test.collection.find_one()
        except AutoReconnect:
            pass

        # No semaphore leak, the pool is allowed to make a new socket.
        c.test.collection.find_one()


class TestBulkAuthorization(BulkTestBase):

    def setUp(self):
        super(TestBulkAuthorization, self).setUp()
        self.client = client = get_client()
        authed_client = auth_context.client
        if not version.at_least(authed_client, (2, 5, 3)):
            raise SkipTest('Need at least MongoDB 2.5.3 with auth')

        db = client.pymongo_test
        self.coll = db.test

        authed_client.pymongo_test.test.drop()
        authed_client.pymongo_test.add_user('dbOwner', 'pw', roles=['dbOwner'])
        db.authenticate('dbOwner', 'pw')
        db.add_user('readonly', 'pw', roles=['read'])
        db.command(
            'createRole', 'noremove',
            privileges=[{
                'actions': ['insert', 'update', 'find'],
                'resource': {'db': 'pymongo_test', 'collection': 'test'}
            }],
            roles=[])

        db.add_user('noremove', 'pw', roles=['noremove'])
        db.logout()

    def test_readonly(self):
        # We test that an authorization failure aborts the batch and is raised
        # as OperationFailure.
        db = self.client.pymongo_test
        db.authenticate('readonly', 'pw')
        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.insert({'x': 1})
        self.assertRaises(OperationFailure, bulk.execute)

    def test_no_remove(self):
        # We test that an authorization failure aborts the batch and is raised
        # as OperationFailure.
        db = self.client.pymongo_test
        db.authenticate('noremove', 'pw')
        bulk = self.coll.initialize_ordered_bulk_op()
        bulk.insert({'x': 1})
        bulk.find({'x': 2}).upsert().replace_one({'x': 2})
        bulk.find({}).remove()  # Prohibited.
        bulk.insert({'x': 3})   # Never attempted.
        self.assertRaises(OperationFailure, bulk.execute)
        self.assertEqual(set([1, 2]), set(self.coll.distinct('x')))

    def tearDown(self):
        db = self.client.pymongo_test
        db.logout()
        db.authenticate('dbOwner', 'pw')
        db.command('dropRole', 'noremove')
        remove_all_users(db)
        db.logout()


class BaseTestThreadsAuth(object):
    """
    Base test class for TestThreadsAuth and TestThreadsAuthReplicaSet. (This is
    not itself a unittest.TestCase, otherwise it'd be run twice -- once when
    nose imports this module, and once when nose imports
    test_threads_replica_set_connection.py, which imports this module.)
    """
    def _get_client(self):
        """
        Intended for overriding in TestThreadsAuthReplicaSet. This method
        returns a MongoClient here, and a MongoReplicaSetClient in
        test_threads_replica_set_connection.py.
        """
        # Regular test client
        return get_client()

    def setUp(self):
        client = self._get_client()
        self.client = client
        auth_context.client.admin.add_user('admin-user', 'password',
                                           roles=['clusterAdmin',
                                                  'dbAdminAnyDatabase',
                                                  'readWriteAnyDatabase',
                                                  'userAdminAnyDatabase'])
        self.client.admin.authenticate("admin-user", "password")
        self.client.auth_test.add_user("test-user", "password",
                                       roles=['readWrite'])

    def tearDown(self):
        # Remove auth users from databases
        self.client.admin.authenticate("admin-user", "password")
        self.client.drop_database('auth_test')
        remove_all_users(self.client.auth_test)
        self.client.admin.remove_user('admin-user')
        # Clear client reference so that RSC's monitor thread
        # dies.
        self.client = None

    def test_auto_auth_login(self):
        client = self._get_client()
        self.assertRaises(OperationFailure, client.auth_test.test.find_one)

        # Admin auth
        client = self._get_client()
        client.admin.authenticate("admin-user", "password")

        nthreads = 10
        threads = []
        for _ in xrange(nthreads):
            t = AutoAuthenticateThreads(client.auth_test.test, 100)
            t.start()
            threads.append(t)

        joinall(threads)

        for t in threads:
            self.assertTrue(t.success)

        # Database-specific auth
        client = self._get_client()
        client.auth_test.authenticate("test-user", "password")

        threads = []
        for _ in xrange(nthreads):
            t = AutoAuthenticateThreads(client.auth_test.test, 100)
            t.start()
            threads.append(t)

        joinall(threads)

        for t in threads:
            self.assertTrue(t.success)


class TestThreadsAuth(BaseTestThreadsAuth, unittest.TestCase):
    pass


class TestThreadsAuthReplicaSet(TestReplicaSetClientBase, BaseTestThreadsAuth):

    def setUp(self):
        """
        Prepare to test all the same things that TestThreads tests, but do it
        with a replica-set client
        """
        TestReplicaSetClientBase.setUp(self)
        BaseTestThreadsAuth.setUp(self)

    def tearDown(self):
        TestReplicaSetClientBase.tearDown(self)
        BaseTestThreadsAuth.tearDown(self)

    def _get_client(self):
        """
        Override TestThreadsAuth, so its tests run on a MongoReplicaSetClient
        instead of a regular MongoClient.
        """
        return MongoReplicaSetClient(pair, replicaSet=self.name)


if __name__ == "__main__":
    unittest.main()
