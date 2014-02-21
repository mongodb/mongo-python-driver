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
import unittest

from urllib import quote_plus

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo import MongoClient, MongoReplicaSetClient
from pymongo.auth import HAVE_KERBEROS
from pymongo.errors import OperationFailure, ConfigurationError
from pymongo.read_preferences import ReadPreference
from test import version, host, port
from test.utils import is_mongos, server_started_with_auth

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
        self.assertTrue(client.database_names())
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'GSSAPI' % (quote_plus(PRINCIPAL), GSSAPI_HOST, GSSAPI_PORT))
        client = MongoClient(uri)
        self.assertTrue(client.database_names())

        # With gssapiServiceName
        self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                 mechanism='GSSAPI',
                                                 gssapiServiceName='mongodb'))
        self.assertTrue(client.database_names())
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'GSSAPI;gssapiServiceName=mongodb' % (quote_plus(PRINCIPAL),
                                                     GSSAPI_HOST, GSSAPI_PORT))
        client = MongoClient(uri)
        self.assertTrue(client.database_names())

        set_name = client.admin.command('ismaster').get('setName')
        if set_name:
            client = MongoReplicaSetClient(GSSAPI_HOST,
                                           port=GSSAPI_PORT,
                                           replicaSet=set_name)
            # Without gssapiServiceName
            self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                     mechanism='GSSAPI'))
            self.assertTrue(client.database_names())
            uri = ('mongodb://%s@%s:%d/?authMechanism=GSSAPI;replicaSet'
                   '=%s' % (quote_plus(PRINCIPAL),
                            GSSAPI_HOST, GSSAPI_PORT, str(set_name)))
            client = MongoReplicaSetClient(uri)
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
            client = MongoReplicaSetClient(uri)
            self.assertTrue(client.database_names())

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


class TestSASL(unittest.TestCase):

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


class TestAuthURIOptions(unittest.TestCase):

    def setUp(self):
        client = MongoClient(host, port)
        # Sharded auth not supported before MongoDB 2.0
        if is_mongos(client) and not version.at_least(client, (2, 0, 0)):
            raise SkipTest("Auth with sharding requires MongoDB >= 2.0.0")
        if not server_started_with_auth(client):
            raise SkipTest('Authentication is not enabled on server')
        response = client.admin.command('ismaster')
        self.set_name = str(response.get('setName', ''))
        client.admin.add_user('admin', 'pass', roles=['userAdminAnyDatabase',
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
        if not version.at_least(self.client, (2, 4, 0)):
            raise SkipTest('Delegated authentication requires MongoDB >= 2.4.0')
        if not server_started_with_auth(self.client):
            raise SkipTest('Authentication is not enabled on server')
        if version.at_least(self.client, (2, 5, 3, -1)):
            raise SkipTest('Delegated auth does not exist in MongoDB >= 2.5.3')
        # Give admin all privileges.
        self.client.admin.add_user('admin', 'pass',
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


if __name__ == "__main__":
    unittest.main()
