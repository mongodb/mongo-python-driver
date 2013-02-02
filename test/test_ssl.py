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

import unittest
import sys
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo import MongoClient, MongoReplicaSetClient
from pymongo.errors import ConfigurationError, ConnectionFailure

have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False


class TestSSL(unittest.TestCase):

    def setUp(self):
        if sys.version.startswith('3.0'):
            raise SkipTest("Python 3.0.x has problems "
                           "with SSL and socket timeouts.")

    def test_config_ssl(self):
        self.assertRaises(ConfigurationError, MongoClient, ssl='foo')
        self.assertRaises(TypeError, MongoClient, ssl=0)
        self.assertRaises(TypeError, MongoClient, ssl=5.5)
        self.assertRaises(TypeError, MongoClient, ssl=[])

        self.assertRaises(ConfigurationError, MongoReplicaSetClient, ssl='foo')
        self.assertRaises(TypeError, MongoReplicaSetClient, ssl=0)
        self.assertRaises(TypeError, MongoReplicaSetClient, ssl=5.5)
        self.assertRaises(TypeError, MongoReplicaSetClient, ssl=[])

    def test_no_ssl(self):
        if have_ssl:
            raise SkipTest(
                "The ssl module is available, can't test what happens "
                "without it."
            )

        self.assertRaises(ConfigurationError,
                          MongoClient, ssl=True)
        self.assertRaises(ConfigurationError,
                          MongoReplicaSetClient, ssl=True)

    def test_simple_ops(self):
        if not have_ssl:
            raise SkipTest("The ssl module is not available.")

        try:
            client = MongoClient(connectTimeoutMS=100, ssl=True)
        # MongoDB not configured for SSL?
        except ConnectionFailure:
            raise SkipTest("No mongod available over SSL")
        response = client.admin.command('ismaster')
        if 'setName' in response:
            client = MongoReplicaSetClient(replicaSet=response['setName'],
                                        w=len(response['hosts']),
                                        ssl=True)

        db = client.pymongo_ssl_test
        db.test.drop()
        self.assertTrue(db.test.insert({'ssl': True}))
        self.assertTrue(db.test.find_one()['ssl'])


if __name__ == "__main__":
    unittest.main()
