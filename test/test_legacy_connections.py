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

"""Test deprecated client classes Connection and ReplicaSetConnection."""


import sys
import unittest
import warnings

sys.path[0:0] = [""]

from bson import ObjectId

import pymongo
from pymongo.connection import Connection
from pymongo.replica_set_connection import ReplicaSetConnection
from pymongo.errors import ConfigurationError
from test import host, port, pair
from test.test_replica_set_client import TestReplicaSetClientBase
from test.utils import catch_warnings, get_pool


class TestConnection(unittest.TestCase):
    def test_connection(self):
        c = Connection(host, port)

        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            self.assertTrue(c.auto_start_request)
            self.assertEqual(None, c.max_pool_size)
            self.assertFalse(c.slave_okay)
            self.assertFalse(c.safe)
            self.assertEqual({}, c.get_lasterror_options())

            # Connection's writes are unacknowledged by default
            doc = {"_id": ObjectId()}
            coll = c.pymongo_test.write_concern_test
            coll.drop()
            coll.insert(doc)
            coll.insert(doc)

            c = Connection("mongodb://%s:%s/?safe=true" % (host, port))
            self.assertTrue(c.safe)
        finally:
            ctx.exit()

        # To preserve legacy Connection's behavior, max_size should be None.
        # Pool should handle this without error.
        self.assertEqual(None, get_pool(c).max_size)
        c.end_request()

        # Connection's network_timeout argument is translated into
        # socketTimeoutMS
        self.assertEqual(123, Connection(
            host, port, network_timeout=123)._MongoClient__net_timeout)

        for network_timeout in 'foo', 0, -1:
            self.assertRaises(
                ConfigurationError,
                Connection, host, port, network_timeout=network_timeout)

    def test_connection_alias(self):
        # Testing that pymongo module imports connection.Connection
        self.assertEqual(Connection, pymongo.Connection)


class TestReplicaSetConnection(TestReplicaSetClientBase):
    def test_replica_set_connection(self):
        c = ReplicaSetConnection(pair, replicaSet=self.name)

        ctx = catch_warnings()
        try:
            warnings.simplefilter("ignore", DeprecationWarning)
            self.assertTrue(c.auto_start_request)
            self.assertEqual(None, c.max_pool_size)
            self.assertFalse(c.slave_okay)
            self.assertFalse(c.safe)
            self.assertEqual({}, c.get_lasterror_options())

            # ReplicaSetConnection's writes are unacknowledged by default
            doc = {"_id": ObjectId()}
            coll = c.pymongo_test.write_concern_test
            coll.drop()
            coll.insert(doc)
            coll.insert(doc)

            c = ReplicaSetConnection("mongodb://%s:%s/?replicaSet=%s&safe=true" % (
                host, port, self.name))

            self.assertTrue(c.safe)
        finally:
            ctx.exit()

        # To preserve legacy ReplicaSetConnection's behavior, max_size should
        # be None. Pool should handle this without error.
        pool = get_pool(c)
        self.assertEqual(None, pool.max_size)
        c.end_request()

        # ReplicaSetConnection's network_timeout argument is translated into
        # socketTimeoutMS
        self.assertEqual(123, ReplicaSetConnection(
            pair, replicaSet=self.name, network_timeout=123
        )._MongoReplicaSetClient__net_timeout)

        for network_timeout in 'foo', 0, -1:
            self.assertRaises(
                ConfigurationError,
                ReplicaSetConnection, pair, replicaSet=self.name,
                network_timeout=network_timeout)

    def test_replica_set_connection_alias(self):
        # Testing that pymongo module imports ReplicaSetConnection
        self.assertEqual(ReplicaSetConnection, pymongo.ReplicaSetConnection)

if __name__ == "__main__":
    unittest.main()
