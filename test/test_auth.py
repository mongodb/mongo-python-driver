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
from pymongo.errors import OperationFailure
from pymongo.read_preferences import ReadPreference
from test import client_context, SkipTest, unittest, Version
from test.utils import (delay,
                        ignore_deprecations,
                        rs_or_single_client_noauth,
                        single_client_noauth)

# YOU MUST RUN KINIT BEFORE RUNNING GSSAPI TESTS ON UNIX.
GSSAPI_HOST = os.environ.get('GSSAPI_HOST')
GSSAPI_PORT = int(os.environ.get('GSSAPI_PORT', '27017'))
GSSAPI_PRINCIPAL = os.environ.get('GSSAPI_PRINCIPAL')
GSSAPI_SERVICE_NAME = os.environ.get('GSSAPI_SERVICE_NAME', 'mongodb')
GSSAPI_CANONICALIZE = os.environ.get('GSSAPI_CANONICALIZE', 'false')
GSSAPI_SERVICE_REALM = os.environ.get('GSSAPI_SERVICE_REALM')
GSSAPI_PASS = os.environ.get('GSSAPI_PASS')
GSSAPI_DB = os.environ.get('GSSAPI_DB', 'test')

SASL_HOST = os.environ.get('SASL_HOST')
SASL_PORT = int(os.environ.get('SASL_PORT', '27017'))
SASL_USER = os.environ.get('SASL_USER')
SASL_PASS = os.environ.get('SASL_PASS')
SASL_DB   = os.environ.get('SASL_DB', '$external')


class AutoAuthenticateThread(threading.Thread):
    """Used in testing threaded authentication.

    This does collection.find_one() with a 1-second delay to ensure it must
    check out and authenticate multiple sockets from the pool concurrently.

    :Parameters:
      `collection`: An auth-protected collection containing one document.
    """

    def __init__(self, collection):
        super(AutoAuthenticateThread, self).__init__()
        self.collection = collection
        self.success = False

    def run(self):
        assert self.collection.find_one({'$where': delay(1)}) is not None
        self.success = True


class TestGSSAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not HAVE_KERBEROS:
            raise SkipTest('Kerberos module not available.')
        if not GSSAPI_HOST or not GSSAPI_PRINCIPAL:
            raise SkipTest(
               'Must set GSSAPI_HOST and GSSAPI_PRINCIPAL to test GSSAPI')
        cls.service_realm_required = (
            GSSAPI_SERVICE_REALM is not None and
            GSSAPI_SERVICE_REALM not in GSSAPI_PRINCIPAL)
        mech_properties = 'SERVICE_NAME:%s' % (GSSAPI_SERVICE_NAME,)
        mech_properties += (
            ',CANONICALIZE_HOST_NAME:%s' % (GSSAPI_CANONICALIZE,))
        if GSSAPI_SERVICE_REALM is not None:
            mech_properties += ',SERVICE_REALM:%s' % (GSSAPI_SERVICE_REALM,)
        cls.mech_properties = mech_properties

    def test_credentials_hashing(self):
        # GSSAPI credentials are properly hashed.
        creds0 = _build_credentials_tuple(
            'GSSAPI', '', 'user', 'pass', {})

        creds1 = _build_credentials_tuple(
            'GSSAPI', '', 'user', 'pass',
            {'authmechanismproperties': {'SERVICE_NAME': 'A'}})

        creds2 = _build_credentials_tuple(
            'GSSAPI', '', 'user', 'pass',
            {'authmechanismproperties': {'SERVICE_NAME': 'A'}})

        creds3 = _build_credentials_tuple(
            'GSSAPI', '', 'user', 'pass',
            {'authmechanismproperties': {'SERVICE_NAME': 'B'}})

        self.assertEqual(1, len(set([creds1, creds2])))
        self.assertEqual(3, len(set([creds0, creds1, creds2, creds3])))

    @ignore_deprecations
    def test_gssapi_simple(self):
        if GSSAPI_PASS is not None:
            uri = ('mongodb://%s:%s@%s:%d/?authMechanism='
                   'GSSAPI' % (quote_plus(GSSAPI_PRINCIPAL),
                               GSSAPI_PASS,
                               GSSAPI_HOST,
                               GSSAPI_PORT))
        else:
            uri = ('mongodb://%s@%s:%d/?authMechanism='
                   'GSSAPI' % (quote_plus(GSSAPI_PRINCIPAL),
                               GSSAPI_HOST,
                               GSSAPI_PORT))

        if not self.service_realm_required:
            # Without authMechanismProperties.
            client = MongoClient(GSSAPI_HOST,
                                 GSSAPI_PORT,
                                 username=GSSAPI_PRINCIPAL,
                                 password=GSSAPI_PASS,
                                 authMechanism='GSSAPI')

            client[GSSAPI_DB].collection.find_one()

            # Log in using URI, without authMechanismProperties.
            client = MongoClient(uri)
            client[GSSAPI_DB].collection.find_one()

        # Authenticate with authMechanismProperties.
        client = MongoClient(GSSAPI_HOST,
                             GSSAPI_PORT,
                             username=GSSAPI_PRINCIPAL,
                             password=GSSAPI_PASS,
                             authMechanism='GSSAPI',
                             authMechanismProperties=self.mech_properties)

        client[GSSAPI_DB].collection.find_one()

        # Log in using URI, with authMechanismProperties.
        mech_uri = uri + '&authMechanismProperties=%s' % (self.mech_properties,)
        client = MongoClient(mech_uri)
        client[GSSAPI_DB].collection.find_one()

        set_name = client.admin.command('ismaster').get('setName')
        if set_name:
            if not self.service_realm_required:
                # Without authMechanismProperties
                client = MongoClient(GSSAPI_HOST,
                                     GSSAPI_PORT,
                                     username=GSSAPI_PRINCIPAL,
                                     password=GSSAPI_PASS,
                                     authMechanism='GSSAPI',
                                     replicaSet=set_name)

                client[GSSAPI_DB].collection_names()

                uri = uri + '&replicaSet=%s' % (str(set_name),)
                client = MongoClient(uri)
                client[GSSAPI_DB].collection_names()

            # With authMechanismProperties
            client = MongoClient(GSSAPI_HOST,
                                 GSSAPI_PORT,
                                 username=GSSAPI_PRINCIPAL,
                                 password=GSSAPI_PASS,
                                 authMechanism='GSSAPI',
                                 authMechanismProperties=self.mech_properties,
                                 replicaSet=set_name)

            client[GSSAPI_DB].collection_names()

            mech_uri = mech_uri + '&replicaSet=%s' % (str(set_name),)
            client = MongoClient(mech_uri)
            client[GSSAPI_DB].collection_names()

    @ignore_deprecations
    def test_gssapi_threaded(self):
        client = MongoClient(GSSAPI_HOST,
                             GSSAPI_PORT,
                             username=GSSAPI_PRINCIPAL,
                             password=GSSAPI_PASS,
                             authMechanism='GSSAPI',
                             authMechanismProperties=self.mech_properties)

        # Authentication succeeded?
        client.server_info()
        db = client[GSSAPI_DB]

        # Need one document in the collection. AutoAuthenticateThread does
        # collection.find_one with a 1-second delay, forcing it to check out
        # multiple sockets from the pool concurrently, proving that
        # auto-authentication works with GSSAPI.
        collection = db.test
        if collection.count() == 0:
            try:
                collection.drop()
                collection.insert_one({'_id': 1})
            except OperationFailure:
                raise SkipTest("User must be able to write.")

        threads = []
        for _ in range(4):
            threads.append(AutoAuthenticateThread(collection))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
            self.assertTrue(thread.success)

        set_name = client.admin.command('ismaster').get('setName')
        if set_name:
            client = MongoClient(GSSAPI_HOST,
                                 GSSAPI_PORT,
                                 username=GSSAPI_PRINCIPAL,
                                 password=GSSAPI_PASS,
                                 authMechanism='GSSAPI',
                                 authMechanismProperties=self.mech_properties,
                                 replicaSet=set_name)

            # Succeeded?
            client.server_info()

            threads = []
            for _ in range(4):
                threads.append(AutoAuthenticateThread(collection))
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

        client = MongoClient(SASL_HOST,
                             SASL_PORT,
                             username=SASL_USER,
                             password=SASL_PASS,
                             authSource=SASL_DB,
                             authMechanism='PLAIN')
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
                                 SASL_PORT,
                                 replicaSet=set_name,
                                 username=SASL_USER,
                                 password=SASL_PASS,
                                 authSource=SASL_DB,
                                 authMechanism='PLAIN')
            client.ldap.test.find_one()

            uri = ('mongodb://%s:%s@%s:%d/?authMechanism=PLAIN;'
                   'authSource=%s;replicaSet=%s' % (quote_plus(SASL_USER),
                                                    quote_plus(SASL_PASS),
                                                    SASL_HOST, SASL_PORT,
                                                    SASL_DB, str(set_name)))
            client = MongoClient(uri)
            client.ldap.test.find_one()

    def test_sasl_plain_bad_credentials(self):

        with ignore_deprecations():
            client = MongoClient(SASL_HOST, SASL_PORT)

            # Bad username
            self.assertRaises(OperationFailure, client.ldap.authenticate,
                              'not-user', SASL_PASS, SASL_DB, 'PLAIN')
            self.assertRaises(OperationFailure, client.ldap.test.find_one)
            self.assertRaises(OperationFailure, client.ldap.test.insert_one,
                              {"failed": True})

            # Bad password
            self.assertRaises(OperationFailure, client.ldap.authenticate,
                              SASL_USER, 'not-pwd', SASL_DB, 'PLAIN')
            self.assertRaises(OperationFailure, client.ldap.test.find_one)
            self.assertRaises(OperationFailure, client.ldap.test.insert_one,
                              {"failed": True})

        def auth_string(user, password):
            uri = ('mongodb://%s:%s@%s:%d/?authMechanism=PLAIN;'
                   'authSource=%s' % (quote_plus(user),
                                      quote_plus(password),
                                      SASL_HOST, SASL_PORT, SASL_DB))
            return uri

        bad_user = MongoClient(auth_string('not-user', SASL_PASS))
        bad_pwd = MongoClient(auth_string(SASL_USER, 'not-pwd'))
        # OperationFailure raised upon connecting.
        self.assertRaises(OperationFailure, bad_user.admin.command, 'ismaster')
        self.assertRaises(OperationFailure, bad_pwd.admin.command, 'ismaster')


class TestSCRAMSHA1(unittest.TestCase):

    @client_context.require_auth
    @client_context.require_version_min(2, 7, 2)
    def setUp(self):
        # Before 2.7.7, SCRAM-SHA-1 had to be enabled from the command line.
        if client_context.version < Version(2, 7, 7):
            cmd_line = client_context.cmd_line
            if 'SCRAM-SHA-1' not in cmd_line.get(
                    'parsed', {}).get('setParameter',
                    {}).get('authenticationMechanisms', ''):
                raise SkipTest('SCRAM-SHA-1 mechanism not enabled')

        client = client_context.client
        client.pymongo_test.add_user(
            'user', 'pass',
            roles=['userAdmin', 'readWrite'],
            writeConcern={'w': client_context.w})

    def test_scram_sha1(self):
        host, port = client_context.host, client_context.port

        with ignore_deprecations():
            client = rs_or_single_client_noauth()
            self.assertTrue(client.pymongo_test.authenticate(
                'user', 'pass', mechanism='SCRAM-SHA-1'))
            client.pymongo_test.command('dbstats')

        client = rs_or_single_client_noauth(
            'mongodb://user:pass@%s:%d/pymongo_test?authMechanism=SCRAM-SHA-1'
            % (host, port))
        client.pymongo_test.command('dbstats')

        if client_context.is_rs:
            uri = ('mongodb://user:pass'
                   '@%s:%d/pymongo_test?authMechanism=SCRAM-SHA-1'
                   '&replicaSet=%s' % (host, port,
                                       client_context.replica_set_name))
            client = single_client_noauth(uri)
            client.pymongo_test.command('dbstats')
            db = client.get_database(
                'pymongo_test', read_preference=ReadPreference.SECONDARY)
            db.command('dbstats')

    def tearDown(self):
        client_context.client.pymongo_test.remove_user('user')


class TestAuthURIOptions(unittest.TestCase):

    @client_context.require_auth
    def setUp(self):
        client_context.client.admin.add_user('admin', 'pass',
                                             roles=['userAdminAnyDatabase',
                                                    'dbAdminAnyDatabase',
                                                    'readWriteAnyDatabase',
                                                    'clusterAdmin'])
        client = rs_or_single_client_noauth(username='admin', password='pass')
        client.pymongo_test.add_user('user', 'pass',
                                     roles=['userAdmin', 'readWrite'])

        if client_context.is_rs:
            # Make sure the admin user is replicated after calling add_user
            # above. This avoids a race in the replica set tests below.
            client.admin.command('getLastError', w=client_context.w)
        self.client = client

    def tearDown(self):
        self.client.pymongo_test.remove_user('user')
        self.client.admin.remove_user('admin')
        self.client = None

    def test_uri_options(self):
        # Test default to admin
        host, port = client_context.host, client_context.port
        client = rs_or_single_client_noauth(
            'mongodb://admin:pass@%s:%d' % (host, port))
        self.assertTrue(client.admin.command('dbstats'))

        if client_context.is_rs:
            uri = ('mongodb://admin:pass@%s:%d/?replicaSet=%s' % (
                host, port, client_context.replica_set_name))
            client = single_client_noauth(uri)
            self.assertTrue(client.admin.command('dbstats'))
            db = client.get_database(
                'admin', read_preference=ReadPreference.SECONDARY)
            self.assertTrue(db.command('dbstats'))

        # Test explicit database
        uri = 'mongodb://user:pass@%s:%d/pymongo_test' % (host, port)
        client = rs_or_single_client_noauth(uri)
        self.assertRaises(OperationFailure, client.admin.command, 'dbstats')
        self.assertTrue(client.pymongo_test.command('dbstats'))

        if client_context.is_rs:
            uri = ('mongodb://user:pass@%s:%d/pymongo_test?replicaSet=%s' % (
                host, port, client_context.replica_set_name))
            client = single_client_noauth(uri)
            self.assertRaises(OperationFailure,
                              client.admin.command, 'dbstats')
            self.assertTrue(client.pymongo_test.command('dbstats'))
            db = client.get_database(
                'pymongo_test', read_preference=ReadPreference.SECONDARY)
            self.assertTrue(db.command('dbstats'))

        # Test authSource
        uri = ('mongodb://user:pass@%s:%d'
               '/pymongo_test2?authSource=pymongo_test' % (host, port))
        client = rs_or_single_client_noauth(uri)
        self.assertRaises(OperationFailure,
                          client.pymongo_test2.command, 'dbstats')
        self.assertTrue(client.pymongo_test.command('dbstats'))

        if client_context.is_rs:
            uri = ('mongodb://user:pass@%s:%d/pymongo_test2?replicaSet='
                   '%s;authSource=pymongo_test' % (
                host, port, client_context.replica_set_name))
            client = single_client_noauth(uri)
            self.assertRaises(OperationFailure,
                              client.pymongo_test2.command, 'dbstats')
            self.assertTrue(client.pymongo_test.command('dbstats'))
            db = client.get_database(
                'pymongo_test', read_preference=ReadPreference.SECONDARY)
            self.assertTrue(db.command('dbstats'))


if __name__ == "__main__":
    unittest.main()
