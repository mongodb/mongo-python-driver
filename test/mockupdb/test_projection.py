# Copyright 2018-present MongoDB, Inc.
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

"""PyMongo shouldn't append projection fields to "find" command, PYTHON-1479."""

from bson import SON
from mockupdb import Command, MockupDB, OpQuery, going
from pymongo import MongoClient

import unittest


class TestProjection(unittest.TestCase):
    def test_projection(self):
        q = {}
        fields = {'foo': True}

        # OP_QUERY,
        server = MockupDB(auto_ismaster=True,
                          min_wire_version=0, max_wire_version=3)
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri)
        cursor = client.test.collection.find(q, fields)
        with going(next, cursor):
            request = server.receives(OpQuery(q, fields=fields))
            request.reply([], cursor_id=0)

        # "find" command.
        server = MockupDB(auto_ismaster=True,
                          min_wire_version=0, max_wire_version=4)
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri)
        cursor = client.test.collection.find(q, fields)
        cmd = Command(SON([('find', 'collection'), ('filter', q),
                           ('projection', fields)]))

        with going(next, cursor):
            request = server.receives(cmd)
            request.ok(cursor={'id': 0, 'firstBatch': []})


if __name__ == '__main__':
    unittest.main()
