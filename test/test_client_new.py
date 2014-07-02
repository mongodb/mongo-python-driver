# Copyright 2009-2014 MongoDB, Inc.
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

"""Test the mongo_client module."""

import sys

sys.path[0:0] = [""]

from pymongo import ReadPreference
from pymongo.mongo_client_new import MongoClientNew
from test import host, port, unittest, IntegrationTest, client_context
from test.utils import one


class TestClientNew(IntegrationTest):
    def test_buildinfo(self):
        c = MongoClientNew(host, port)
        assert 'version' in c.admin.command('buildinfo')

    def test_ismaster(self):
        c = MongoClientNew(['%s:%d' % (host, port)])
        response = c.admin.command('ismaster')
        self.assertTrue('ismaster' in response)

    @client_context.require_replica_set
    def test_ismaster_primary(self):
        primary_pair = client_context.rs_client.primary
        c = MongoClientNew(['%s:%d' % primary_pair],
                           replicaSet=client_context.setname)

        response = c.admin.command('ismaster')
        self.assertTrue(response['ismaster'])

    @client_context.require_replica_set
    def test_ismaster_secondary(self):
        secondary_pair = one(client_context.rs_client.secondaries)
        c = MongoClientNew(['%s:%d' % secondary_pair],
                           replicaSet=client_context.setname)

        # 'ismaster' does not obey read preference, so hack it.
        secondary_response = c.admin['$cmd'].find_one(
            {'ismaster': 1},
            read_preference=ReadPreference.SECONDARY)

        # Make sure we really executed ismaster on a secondary.
        self.assertTrue(secondary_response.get('secondary'))

    def test_find(self):
        client_context.client.pymongo_test.test_collection.insert({'_id': 1})
        c = MongoClientNew(host, port)
        docs = list(c.pymongo_test.test_collection.find())
        self.assertEqual([{'_id': 1}], docs)


if __name__ == "__main__":
    unittest.main()
