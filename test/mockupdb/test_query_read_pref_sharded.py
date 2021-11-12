# Copyright 2015 MongoDB, Inc.
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

"""Test PyMongo query and read preference with a sharded cluster."""

from bson import SON
from pymongo import MongoClient, version_tuple
from pymongo.read_preferences import (Primary,
                                      PrimaryPreferred,
                                      Secondary,
                                      SecondaryPreferred,
                                      Nearest)
from mockupdb import MockupDB, going, Command, OpMsg

from tests import unittest


class TestQueryAndReadModeSharded(unittest.TestCase):
    def test_query_and_read_mode_sharded_op_query(self):
        server = MockupDB()
        server.autoresponds('ismaster', ismaster=True, msg='isdbgrid',
                            minWireVersion=2, maxWireVersion=5)
        server.run()
        self.addCleanup(server.stop)

        client = MongoClient(server.uri)
        self.addCleanup(client.close)

        modes_without_query = (
            Primary(),
            SecondaryPreferred(),)

        modes_with_query = (
            PrimaryPreferred(),
            Secondary(),
            Nearest(),
            SecondaryPreferred([{'tag': 'value'}]),)

        find_command = SON([('find', 'test'), ('filter', {'a': 1})])
        for query in ({'a': 1}, {'$query': {'a': 1}},):
            for mode in modes_with_query + modes_without_query:
                collection = client.db.get_collection('test',
                                                      read_preference=mode)
                cursor = collection.find(query.copy())
                with going(next, cursor):
                    request = server.receives()
                    if mode in modes_without_query:
                        # Filter is hoisted out of $query.
                        request.assert_matches(Command(find_command))
                        self.assertFalse('$readPreference' in request)
                    else:
                        # Command is nested in $query.
                        request.assert_matches(Command(
                            SON([('$query', find_command),
                                 ('$readPreference', mode.document)])))

                    request.replies({'cursor': {'id': 0, 'firstBatch': [{}]}})

    @unittest.skipUnless(version_tuple >= (3, 7), "requires PyMongo 3.7")
    def test_query_and_read_mode_sharded_op_msg(self):
        """Test OP_MSG sends non-primary $readPreference and never $query."""
        server = MockupDB()
        server.autoresponds('ismaster', ismaster=True, msg='isdbgrid',
                            minWireVersion=2, maxWireVersion=6)
        server.run()
        self.addCleanup(server.stop)

        client = MongoClient(server.uri)
        self.addCleanup(client.close)

        read_prefs = (
            Primary(),
            SecondaryPreferred(),
            PrimaryPreferred(),
            Secondary(),
            Nearest(),
            SecondaryPreferred([{'tag': 'value'}]),)

        for query in ({'a': 1}, {'$query': {'a': 1}},):
            for mode in read_prefs:
                collection = client.db.get_collection('test',
                                                      read_preference=mode)
                cursor = collection.find(query.copy())
                with going(next, cursor):
                    request = server.receives()
                    # Command is not nested in $query.
                    request.assert_matches(OpMsg(
                        SON([('find', 'test'),
                             ('filter', {'a': 1}),
                             ('$readPreference', mode.document)])))

                    request.replies({'cursor': {'id': 0, 'firstBatch': [{}]}})


if __name__ == '__main__':
    unittest.main()
