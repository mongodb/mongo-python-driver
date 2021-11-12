# Copyright 2017 MongoDB, Inc.
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

from bson.son import SON
from mockupdb import (MockupDB, going, OpInsert, OpMsg, absent, Command,
                      OP_MSG_FLAGS)
from pymongo import MongoClient, WriteConcern, version_tuple

from tests import unittest


class TestLegacyCRUD(unittest.TestCase):
    def test_op_insert_manipulate_false(self):
        # Test three aspects of legacy insert with manipulate=False:
        #   1. The return value is None, [None], or [None, None] as appropriate.
        #   2. _id is not set on the passed-in document object.
        #   3. _id is not sent to server.
        server = MockupDB(auto_ismaster=True)
        server.run()
        self.addCleanup(server.stop)

        client = MongoClient(server.uri)
        self.addCleanup(client.close)

        coll = client.db.get_collection('coll', write_concern=WriteConcern(w=0))
        doc = {}
        with going(coll.insert, doc, manipulate=False) as future:
            if version_tuple >= (3, 7):
                server.receives(OpMsg(SON([
                    ("insert", coll.name),
                    ("ordered", True),
                    ("writeConcern", {"w": 0}),
                    ("documents", [{}])]), flags=OP_MSG_FLAGS['moreToCome']))
            else:
                server.receives(OpInsert({'_id': absent}))

        self.assertFalse('_id' in doc)
        self.assertIsNone(future())

        docs = [{}]  # One doc in a list.
        with going(coll.insert, docs, manipulate=False) as future:
            if version_tuple >= (3, 7):
                # PyMongo 3.7 ordered bulk w:0 writes use implicit w:1.
                request = server.receives()
                request.assert_matches(OpMsg(SON([
                    ("insert", coll.name),
                    ("ordered", True),
                    ("documents", [{}])]), flags=0))
                request.reply({"n": 1})
            else:
                server.receives(OpInsert({'_id': absent}))

        self.assertFalse('_id' in docs[0])
        self.assertEqual(future(), [None])

        docs = [{}, {}]  # Two docs.
        with going(coll.insert, docs, manipulate=False) as future:
            if version_tuple >= (3, 7):
                # PyMongo 3.7 ordered bulk w:0 writes use implicit w:1.
                request = server.receives()
                request.assert_matches(OpMsg(SON([
                    ("insert", coll.name),
                    ("ordered", True),
                    ("documents", [{}, {}])]), flags=0))
                request.reply({"n": 2})
            else:
                server.receives(OpInsert({'_id': absent}, {'_id': absent}))

        self.assertFalse('_id' in docs[0])
        self.assertFalse('_id' in docs[1])
        self.assertEqual(future(), [None, None])

    def test_insert_command_manipulate_false(self):
        # Test same three aspects as test_op_insert_manipulate_false does,
        # with the "insert" command.
        server = MockupDB(auto_ismaster={'maxWireVersion': 2})
        server.run()
        self.addCleanup(server.stop)

        client = MongoClient(server.uri)
        self.addCleanup(client.close)

        doc = {}
        with going(client.db.coll.insert, doc, manipulate=False) as future:
            r = server.receives(Command("insert", "coll", documents=[{}]))
            # MockupDB doesn't understand "absent" in subdocuments yet.
            self.assertFalse('_id' in r.doc['documents'][0])
            r.ok()

        self.assertFalse('_id' in doc)
        self.assertIsNone(future())

        docs = [{}]  # One doc in a list.
        with going(client.db.coll.insert, docs, manipulate=False) as future:
            r = server.receives(Command("insert", "coll", documents=[{}]))
            self.assertFalse('_id' in r.doc['documents'][0])
            r.ok()

        self.assertFalse('_id' in docs[0])
        self.assertEqual(future(), [None])

        docs = [{}, {}]  # Two docs.
        with going(client.db.coll.insert, docs, manipulate=False) as future:
            r = server.receives(Command("insert", "coll", documents=[{}, {}]))
            self.assertFalse('_id' in r.doc['documents'][0])
            self.assertFalse('_id' in r.doc['documents'][1])
            r.ok()

        self.assertFalse('_id' in docs[0])
        self.assertFalse('_id' in docs[1])
        self.assertEqual(future(), [None, None])


if __name__ == '__main__':
    unittest.main()
