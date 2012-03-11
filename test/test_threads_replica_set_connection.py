# Copyright 2009-2010 10gen, Inc.
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

"""Test that pymongo is thread safe."""

import unittest
import os

from nose.plugins.skip import SkipTest
from pymongo.replica_set_connection import ReplicaSetConnection
from test.test_threads import (IgnoreAutoReconnect,
                               BaseTestThreads,
                               BaseTestThreadsAuth)

from test.test_replica_set_connection import (TestConnectionReplicaSetBase,
                                              pair)

from pymongo.errors import AutoReconnect

class TestThreadsReplicaSet(TestConnectionReplicaSetBase, BaseTestThreads):
    def setUp(self):
        """
        Prepare to test all the same things that TestThreads tests, but do it
        with a replica-set connection
        """
        TestConnectionReplicaSetBase.setUp(self)
        BaseTestThreads.setUp(self)

    def tearDown(self):
        TestConnectionReplicaSetBase.tearDown(self)
        BaseTestThreads.tearDown(self)

    def _get_connection(self):
        """
        Override TestThreads, so its tests run on a ReplicaSetConnection
        instead of a regular Connection.
        """
        return ReplicaSetConnection(pair, replicaSet=self.name)

    def test_low_network_timeout(self):
        """
        Override the similar test as TestThreads.test_low_connect_timeout(),
        but use the newer connectTimeoutMS and socketTimeoutMS arguments instead
        of the soon-to-be-deprecated network_timeout.
        """
        db = None
        n = 10

        # First test connection timeout
        i = 0
        while db is None and i < n:
            try:
                db = ReplicaSetConnection(
                    pair,
                    replicaSet=self.name,
                    connectTimeoutMS=1
                ).pymongo_test
            except AutoReconnect:
                i += 1
        if i == n:
            raise SkipTest()

        # ... then test socket timeout
        i = 0
        while db is None and i < n:
            try:
                db = ReplicaSetConnection(
                    pair,
                    replicaSet=self.name,
                    socketTimeoutMS=1
                ).pymongo_test
            except AutoReconnect:
                i += 1
        if i == n:
            raise SkipTest()

        threads = []
        for _ in range(4):
            t = IgnoreAutoReconnect(db.test, 100)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()


class TestThreadsAuthReplicaSet(TestConnectionReplicaSetBase, BaseTestThreadsAuth):

    def setUp(self):
        """
        Prepare to test all the same things that TestThreads tests, but do it
        with a replica-set connection
        """
        TestConnectionReplicaSetBase.setUp(self)
        BaseTestThreadsAuth.setUp(self)

    def tearDown(self):
        TestConnectionReplicaSetBase.tearDown(self)
        BaseTestThreadsAuth.tearDown(self)

    def _get_connection(self):
        """
        Override TestThreadsAuth, so its tests run on a ReplicaSetConnection
        instead of a regular Connection.
        """
        return ReplicaSetConnection(pair, replicaSet=self.name)

if __name__ == "__main__":
    suite = unittest.TestSuite([
        unittest.makeSuite(TestThreadsReplicaSet),
        unittest.makeSuite(TestThreadsAuthReplicaSet)
    ])
    unittest.TextTestRunner(verbosity=2).run(suite)
