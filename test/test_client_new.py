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

from pymongo import MongoClient, ReadPreference
from pymongo.mongo_client_new import MongoClientNew
from test import host, port, unittest


class TestClientNew(unittest.TestCase):
    def test_buildinfo(self):
        c = MongoClientNew(host, port)
        assert 'version' in c.admin.command('buildinfo')

    def test_ismaster(self):
        legacy = MongoClient(host, port)
        name = legacy.admin.command('ismaster').get('setName')
        c = MongoClientNew(
            ['localhost:27017', 'localhost:27018'],
            replicaSet=name)

        primary_response = c.admin.command('ismaster')
        self.assertTrue(primary_response['ismaster'])

        # 'ismaster' does not obey read preference, so hack it.
        secondary_response = c.admin['$cmd'].find_one(
            {'ismaster': 1},
            read_preference=ReadPreference.SECONDARY)

        # Make sure we really executed ismaster on a secondary.
        self.assertTrue(secondary_response.get('secondary'))

    def test_find(self):
        legacy = MongoClient(host, port)
        legacy.test.test.drop()
        legacy.test.test.insert({'_id': 1})
        c = MongoClientNew(host, port)
        docs = list(c.test.test.find())
        self.assertEqual([{'_id': 1}], docs)


if __name__ == "__main__":
    unittest.main()
