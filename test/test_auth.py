# Copyright 2013-2014 MongoDB, Inc.
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
import sys
import threading

try:
    from urllib.parse import quote_plus
except ImportError:
    # Python 2
    from urllib import quote_plus

sys.path[0:0] = [""]

from pymongo import MongoClient
from pymongo.auth import HAVE_KERBEROS, _build_credentials_tuple
from pymongo.errors import OperationFailure, ConfigurationError
from pymongo.read_preferences import ReadPreference
from test import client_context, host, port, SkipTest, unittest

# YOU MUST RUN KINIT BEFORE RUNNING GSSAPI TESTS.
GSSAPI_HOST = os.environ.get('GSSAPI_HOST')
GSSAPI_PORT = int(os.environ.get('GSSAPI_PORT', '27017'))
PRINCIPAL = os.environ.get('PRINCIPAL')

SASL_HOST = os.environ.get('SASL_HOST')
SASL_PORT = int(os.environ.get('SASL_PORT', '27017'))
SASL_USER = os.environ.get('SASL_USER')
SASL_PASS = os.environ.get('SASL_PASS')
SASL_DB   = os.environ.get('SASL_DB', '$external')


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

    @classmethod
    def setUpClass(cls):
        if not HAVE_KERBEROS:
            raise SkipTest('Kerberos module not available.')
        if not GSSAPI_HOST or not PRINCIPAL:
            raise SkipTest('Must set GSSAPI_HOST and PRINCIPAL to test GSSAPI')

    def test_credentials_hashing(self):
        # GSSAPI credentials are properly hashed.
        creds0 = _build_credentials_tuple(
            'GSSAPI', '', 'user', 'pass', {})

        creds1 = _build_credentials_tuple(
            'GSSAPI', '', 'user', 'pass', {'gssapiservicename': 'A'})

        creds2 = _build_credentials_tuple(
            'GSSAPI', '', 'user', 'pass', {'gssapiservicename': 'A'})

        creds3 = _build_credentials_tuple(
            'GSSAPI', '', 'user', 'pass', {'gssapiservicename': 'B'})

        self.assertEqual(1, len(set([creds1, creds2])))
        self.assertEqual(3, len(set([creds0, creds1, creds2, creds3])))

    def test_gssapi_simple(self):
        # Call authenticate() without gssapiServiceName.
        client = MongoClient(GSSAPI_HOST, GSSAPI_PORT)
        self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                 mechanism='GSSAPI'))
        client.test.collection.find_one()

        # Log in using URI, without gssapiServiceName.
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'GSSAPI' % (quote_plus(PRINCIPAL), GSSAPI_HOST, GSSAPI_PORT))
        client = MongoClient(uri)
        client.test.collection.find_one()

        # Call authenticate() with gssapiServiceName.
        self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                 mechanism='GSSAPI',
                                                 gssapiServiceName='mongodb'))
        client.test.collection.find_one()

        # Log in using URI, with gssapiServiceName.
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'GSSAPI;gssapiServiceName=mongodb' % (quote_plus(PRINCIPAL),
                                                     GSSAPI_HOST, GSSAPI_PORT))
        client = MongoClient(uri)
        client.test.collection.find_one()

        set_name = client.admin.command('ismaster').get('setName')
        if set_name:
            client = MongoClient(GSSAPI_HOST,
                                 port=GSSAPI_PORT,
                                 replicaSet=set_name)
            # Without gssapiServiceName
            self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                     mechanism='GSSAPI'))
            self.assertTrue(client.database_names())
            uri = ('mongodb://%s@%s:%d/?authMechanism=GSSAPI;replicaSet'
                   '=%s' % (quote_plus(PRINCIPAL),
                            GSSAPI_HOST, GSSAPI_PORT, str(set_name)))
            client = MongoClient(uri)
            self.assertTrue(client.database_names())

            # With gssapiServiceName
            self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                     mechanism='GSSAPI',
                                                     gssapiServiceName='mongodb'))
            self.assertTrue(client.database_names())
            uri = ('mongodb://%s@%s:%d/?authMechanism=GSSAPI;replicaSet'
                   '=%s;gssapiServiceName=mongodb' % (quote_plus(PRINCIPAL),
                                                      GSSAPI_HOST,
                                                      GSSAPI_PORT,
                                                      str(set_name)))
            client = MongoClient(uri)
            self.assertTrue(client.database_names())

    def test_gssapi_threaded(self):

        client = MongoClient(GSSAPI_HOST)
        # Make sure each thread uses a different socket.
        client.start_request()
        self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                 mechanism='GSSAPI'))

        threads = []
        for _ in range(4):
            threads.append(AutoAuthenticateThread(client.test))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
            self.assertTrue(thread.success)

        set_name = client.admin.command('ismaster').get('setName')
        if set_name:
            preference = ReadPreference.SECONDARY
            client = MongoClient(GSSAPI_HOST,
                                 replicaSet=set_name,
                                 read_preference=preference)
            self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                     mechanism='GSSAPI'))
            self.assertTrue(client.test.command('dbstats'))

            threads = []
            for _ in range(4):
                threads.append(AutoAuthenticateThread(client.foo))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
                self.assertTrue(thread.success)


class TestSASLPlain(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
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
            client = MongoClient(SASL_HOST,
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
            client = MongoClient(uri)
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

    @client_context.require_auth
    @client_context.require_version_min(2, 7, 2)
    def setUp(self):
        self.set_name = client_context.setname

        cmd_line = client_context.cmd_line
        if 'SCRAM-SHA-1' not in cmd_line.get(
            'parsed', {}).get('setParameter',
                            {}).get('authenticationMechanisms', ''):
            raise SkipTest('SCRAM-SHA-1 mechanism not enabled')

        client = client_context.client
        if self.set_name:
            client.pymongo_test.add_user('user', 'pass',
                roles=['userAdmin', 'readWrite'],
                writeConcern={'w': client_context.w})
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
            client = MongoClient(host, port,
                                 replicaSet='%s' % (self.set_name,))
            self.assertTrue(client.pymongo_test.authenticate(
                'user', 'pass', mechanism='SCRAM-SHA-1'))
            client.pymongo_test.command('dbstats')

            uri = ('mongodb://user:pass'
                   '@%s:%d/pymongo_test?authMechanism=SCRAM-SHA-1'
                   '&replicaSet=%s' % (host, port, self.set_name))
            client = MongoClient(uri)
            client.pymongo_test.command('dbstats')
            client.read_preference = ReadPreference.SECONDARY
            client.pymongo_test.command('dbstats')

    def tearDown(self):
        client_context.client.pymongo_test.remove_user('user')


class TestAuthURIOptions(unittest.TestCase):

    @client_context.require_auth
    def setUp(self):
        client = MongoClient(host, port)
        response = client.admin.command('ismaster')
        self.set_name = str(response.get('setName', ''))
        client_context.client.admin.add_user('admin', 'pass',
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
            # above. This avoids a race in the replica set tests below.
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
            client = MongoClient(uri)
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
            client = MongoClient(uri)
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
            client = MongoClient(uri)
            self.assertRaises(OperationFailure,
                              client.pymongo_test2.command, 'dbstats')
            self.assertTrue(client.pymongo_test.command('dbstats'))
            client.read_preference = ReadPreference.SECONDARY
            self.assertTrue(client.pymongo_test.command('dbstats'))


class TestDelegatedAuth(unittest.TestCase):

    @client_context.require_auth
    @client_context.require_version_max(2, 5, 3)
    @client_context.require_version_min(2, 4, 0)
    def setUp(self):
        self.client = client_context.client

    def tearDown(self):
        self.client.pymongo_test.remove_user('user')
        self.client.pymongo_test2.remove_user('user')
        self.client.pymongo_test2.foo.drop()

    def test_delegated_auth(self):
        self.client.pymongo_test2.foo.remove()
        self.client.pymongo_test2.foo.insert({})
        # User definition with no roles in pymongo_test.
        self.client.pymongo_test.add_user('user', 'pass', roles=[])
        # Delegate auth to pymongo_test.
        self.client.pymongo_test2.add_user('user',
                                           userSource='pymongo_test',
                                           roles=['read'])
        auth_c = MongoClient(host, port)
        self.assertRaises(OperationFailure,
                          auth_c.pymongo_test2.foo.find_one)
        # Auth must occur on the db where the user is defined.
        self.assertRaises(OperationFailure,
                          auth_c.pymongo_test2.authenticate,
                          'user', 'pass')
        # Auth directly
        self.assertTrue(auth_c.pymongo_test.authenticate('user', 'pass'))
        self.assertTrue(auth_c.pymongo_test2.foo.find_one())
        auth_c.pymongo_test.logout()
        self.assertRaises(OperationFailure,
                          auth_c.pymongo_test2.foo.find_one)
        # Auth using source
        self.assertTrue(auth_c.pymongo_test2.authenticate(
            'user', 'pass', source='pymongo_test'))
        self.assertTrue(auth_c.pymongo_test2.foo.find_one())
        # Must logout from the db authenticate was called on.
        auth_c.pymongo_test2.logout()
        self.assertRaises(OperationFailure,
                          auth_c.pymongo_test2.foo.find_one)


if __name__ == "__main__":
    unittest.main()
