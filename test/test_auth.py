# Copyright 2013 10gen, Inc.
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
from pymongo.errors import OperationFailure
from pymongo.read_preferences import ReadPreference
from test import version, host, port
from test.utils import is_mongos, server_started_with_auth

# YOU MUST RUN KINIT BEFORE RUNNING GSSAPI TESTS.
GSSAPI_HOST = os.environ.get('GSSAPI_HOST')
GSSAPI_PORT = int(os.environ.get('GSSAPI_PORT', '27017'))
PRINCIPAL = os.environ.get('PRINCIPAL')

class TestGSSAPI(unittest.TestCase):

    def setUp(self):
        if not HAVE_KERBEROS:
            raise SkipTest('Kerberos module not available.')
        if not GSSAPI_HOST:
            raise SkipTest('Must set GSSAPI_HOST and PRINCIPAL to test GSSAPI')

    def test_gssapi_simple(self):

        client = MongoClient(GSSAPI_HOST, GSSAPI_PORT)
        self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                 mechanism='GSSAPI'))
        # Just test that we can run a simple command.
        self.assertTrue(client.database_names())

        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'GSSAPI' % (quote_plus(PRINCIPAL), GSSAPI_HOST, GSSAPI_PORT))
        client = MongoClient(uri)
        self.assertTrue(client.database_names())

        set_name = client.admin.command('ismaster').get('setName')
        if set_name:
            client = MongoReplicaSetClient(GSSAPI_HOST,
                                           port=GSSAPI_PORT,
                                           replicaSet=set_name)
            self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                     mechanism='GSSAPI'))
            self.assertTrue(client.database_names())
            uri = ('mongodb://%s@%s:%d/?authMechanism=GSSAPI;replicaSet'
                   '=%s' % (quote_plus(PRINCIPAL),
                            GSSAPI_HOST, GSSAPI_PORT, str(set_name)))
            client = MongoReplicaSetClient(uri)
            self.assertTrue(client.database_names())

    def test_gssapi_threaded(self):

        # Use auto_start_request=True to make sure each thread
        # uses a different socket.
        client = MongoClient(GSSAPI_HOST, auto_start_request=True)
        self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                 mechanism='GSSAPI'))

        result = True
        def try_command():
            try:
                client.foo.command('dbstats')
            except OperationFailure:
                result = False

        threads = []
        for _ in xrange(2):
            threads.append(threading.Thread(target=try_command))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertTrue(result)

        set_name = client.admin.command('ismaster').get('setName')
        if set_name:
            result = True
            preference = ReadPreference.SECONDARY
            client = MongoReplicaSetClient(GSSAPI_HOST,
                                           replicaSet=set_name,
                                           read_preference=preference)
            self.assertTrue(client.test.authenticate(PRINCIPAL,
                                                     mechanism='GSSAPI'))
            self.assertTrue(client.foo.command('dbstats'))

            threads = []
            for _ in xrange(2):
                threads.append(threading.Thread(target=try_command))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertTrue(result)


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
        client.pymongo_test.add_user('user', 'pass')
        client.admin.add_user('admin', 'pass')
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
        self.client.pymongo_test.system.users.remove()
        self.client.admin.system.users.remove()
        self.client.admin.logout()

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


if __name__ == "__main__":
    unittest.main()
