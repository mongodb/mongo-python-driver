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

import unittest
from collections import namedtuple

from mockupdb import OP_MSG_FLAGS, MockupDB, OpMsg, OpMsgReply, going

from pymongo import MongoClient, WriteConcern
from pymongo.cursor import CursorType
from pymongo.operations import DeleteOne, InsertOne, UpdateOne

Operation = namedtuple("Operation", ["name", "function", "request", "reply"])

operations = [
    Operation(
        "find_one",
        lambda coll: coll.find_one({}),
        request=OpMsg({"find": "coll"}, flags=0),
        reply={"ok": 1, "cursor": {"firstBatch": [], "id": 0}},
    ),
    Operation(
        "aggregate",
        lambda coll: coll.aggregate([]),
        request=OpMsg({"aggregate": "coll"}, flags=0),
        reply={"ok": 1, "cursor": {"firstBatch": [], "id": 0}},
    ),
    Operation(
        "insert_one",
        lambda coll: coll.insert_one({}),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={"ok": 1, "n": 1},
    ),
    Operation(
        "insert_one-w0",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).insert_one({}),
        request=OpMsg({"insert": "coll"}, flags=OP_MSG_FLAGS["moreToCome"]),
        reply=None,
    ),
    Operation(
        "insert_many",
        lambda coll: coll.insert_many([{}, {}, {}]),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={"ok": 1, "n": 3},
    ),
    Operation(
        "insert_many-w0",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).insert_many([{}, {}, {}]),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={"ok": 1, "n": 3},
    ),
    Operation(
        "insert_many-w0-unordered",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).insert_many(
            [{}, {}, {}], ordered=False
        ),
        request=OpMsg({"insert": "coll"}, flags=OP_MSG_FLAGS["moreToCome"]),
        reply=None,
    ),
    Operation(
        "replace_one",
        lambda coll: coll.replace_one({"_id": 1}, {"new": 1}),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={"ok": 1, "n": 1, "nModified": 1},
    ),
    Operation(
        "replace_one-w0",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).replace_one(
            {"_id": 1}, {"new": 1}
        ),
        request=OpMsg({"update": "coll"}, flags=OP_MSG_FLAGS["moreToCome"]),
        reply=None,
    ),
    Operation(
        "update_one",
        lambda coll: coll.update_one({"_id": 1}, {"$set": {"new": 1}}),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={"ok": 1, "n": 1, "nModified": 1},
    ),
    Operation(
        "replace_one-w0",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).update_one(
            {"_id": 1}, {"$set": {"new": 1}}
        ),
        request=OpMsg({"update": "coll"}, flags=OP_MSG_FLAGS["moreToCome"]),
        reply=None,
    ),
    Operation(
        "update_many",
        lambda coll: coll.update_many({"_id": 1}, {"$set": {"new": 1}}),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={"ok": 1, "n": 1, "nModified": 1},
    ),
    Operation(
        "update_many-w0",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).update_many(
            {"_id": 1}, {"$set": {"new": 1}}
        ),
        request=OpMsg({"update": "coll"}, flags=OP_MSG_FLAGS["moreToCome"]),
        reply=None,
    ),
    Operation(
        "delete_one",
        lambda coll: coll.delete_one({"a": 1}),
        request=OpMsg({"delete": "coll"}, flags=0),
        reply={"ok": 1, "n": 1},
    ),
    Operation(
        "delete_one-w0",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).delete_one({"a": 1}),
        request=OpMsg({"delete": "coll"}, flags=OP_MSG_FLAGS["moreToCome"]),
        reply=None,
    ),
    Operation(
        "delete_many",
        lambda coll: coll.delete_many({"a": 1}),
        request=OpMsg({"delete": "coll"}, flags=0),
        reply={"ok": 1, "n": 1},
    ),
    Operation(
        "delete_many-w0",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).delete_many({"a": 1}),
        request=OpMsg({"delete": "coll"}, flags=OP_MSG_FLAGS["moreToCome"]),
        reply=None,
    ),
    # Legacy methods
    Operation(
        "bulk_write_insert",
        lambda coll: coll.bulk_write([InsertOne({}), InsertOne({})]),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={"ok": 1, "n": 2},
    ),
    Operation(
        "bulk_write_insert-w0",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).bulk_write(
            [InsertOne({}), InsertOne({})]
        ),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={"ok": 1, "n": 2},
    ),
    Operation(
        "bulk_write_insert-w0-unordered",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).bulk_write(
            [InsertOne({}), InsertOne({})], ordered=False
        ),
        request=OpMsg({"insert": "coll"}, flags=OP_MSG_FLAGS["moreToCome"]),
        reply=None,
    ),
    Operation(
        "bulk_write_update",
        lambda coll: coll.bulk_write(
            [
                UpdateOne({"_id": 1}, {"$set": {"new": 1}}),
                UpdateOne({"_id": 2}, {"$set": {"new": 1}}),
            ]
        ),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={"ok": 1, "n": 2, "nModified": 2},
    ),
    Operation(
        "bulk_write_update-w0",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).bulk_write(
            [
                UpdateOne({"_id": 1}, {"$set": {"new": 1}}),
                UpdateOne({"_id": 2}, {"$set": {"new": 1}}),
            ]
        ),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={"ok": 1, "n": 2, "nModified": 2},
    ),
    Operation(
        "bulk_write_update-w0-unordered",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).bulk_write(
            [
                UpdateOne({"_id": 1}, {"$set": {"new": 1}}),
                UpdateOne({"_id": 2}, {"$set": {"new": 1}}),
            ],
            ordered=False,
        ),
        request=OpMsg({"update": "coll"}, flags=OP_MSG_FLAGS["moreToCome"]),
        reply=None,
    ),
    Operation(
        "bulk_write_delete",
        lambda coll: coll.bulk_write([DeleteOne({"_id": 1}), DeleteOne({"_id": 2})]),
        request=OpMsg({"delete": "coll"}, flags=0),
        reply={"ok": 1, "n": 2},
    ),
    Operation(
        "bulk_write_delete-w0",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).bulk_write(
            [DeleteOne({"_id": 1}), DeleteOne({"_id": 2})]
        ),
        request=OpMsg({"delete": "coll"}, flags=0),
        reply={"ok": 1, "n": 2},
    ),
    Operation(
        "bulk_write_delete-w0-unordered",
        lambda coll: coll.with_options(write_concern=WriteConcern(w=0)).bulk_write(
            [DeleteOne({"_id": 1}), DeleteOne({"_id": 2})], ordered=False
        ),
        request=OpMsg({"delete": "coll"}, flags=OP_MSG_FLAGS["moreToCome"]),
        reply=None,
    ),
]

operations_312 = [
    Operation(
        "find_raw_batches",
        lambda coll: list(coll.find_raw_batches({})),
        request=[
            OpMsg({"find": "coll"}, flags=0),
            OpMsg({"getMore": 7}, flags=0),
        ],
        reply=[
            {"ok": 1, "cursor": {"firstBatch": [{}], "id": 7}},
            {"ok": 1, "cursor": {"nextBatch": [{}], "id": 0}},
        ],
    ),
    Operation(
        "aggregate_raw_batches",
        lambda coll: list(coll.aggregate_raw_batches([])),
        request=[
            OpMsg({"aggregate": "coll"}, flags=0),
            OpMsg({"getMore": 7}, flags=0),
        ],
        reply=[
            {"ok": 1, "cursor": {"firstBatch": [], "id": 7}},
            {"ok": 1, "cursor": {"nextBatch": [{}], "id": 0}},
        ],
    ),
    Operation(
        "find_exhaust_cursor",
        lambda coll: list(coll.find({}, cursor_type=CursorType.EXHAUST)),
        request=[
            OpMsg({"find": "coll"}, flags=0),
            OpMsg({"getMore": 7}, flags=1 << 16),
        ],
        reply=[
            OpMsgReply({"ok": 1, "cursor": {"firstBatch": [{}], "id": 7}}, flags=0),
            OpMsgReply({"ok": 1, "cursor": {"nextBatch": [{}], "id": 7}}, flags=2),
            OpMsgReply({"ok": 1, "cursor": {"nextBatch": [{}], "id": 7}}, flags=2),
            OpMsgReply({"ok": 1, "cursor": {"nextBatch": [{}], "id": 0}}, flags=0),
        ],
    ),
]


class TestOpMsg(unittest.TestCase):
    server: MockupDB
    client: MongoClient

    @classmethod
    def setUpClass(cls):
        cls.server = MockupDB(auto_ismaster=True, max_wire_version=8)
        cls.server.run()
        cls.client = MongoClient(cls.server.uri)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        cls.client.close()

    def _test_operation(self, op):
        coll = self.client.db.coll
        with going(op.function, coll) as future:
            expected_requests = op.request
            replies = op.reply
            if not isinstance(op.request, list):
                expected_requests = [op.request]
                replies = [op.reply]

            for expected_request in expected_requests:
                request = self.server.receives(expected_request)
                reply = None
                if replies:
                    reply = replies.pop(0)
                if reply is not None:
                    request.reply(reply)
            for reply in replies:
                if reply is not None:
                    request.reply(reply)

        future()  # No error.


def operation_test(op):
    def test(self):
        self._test_operation(op)

    return test


def create_tests(ops):
    for op in ops:
        test_name = "test_op_msg_%s" % (op.name,)
        setattr(TestOpMsg, test_name, operation_test(op))


create_tests(operations)

create_tests(operations_312)

if __name__ == "__main__":
    unittest.main()
