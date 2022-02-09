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

import unittest

from mockupdb import MockupDB, OpMsg, going

from bson import SON
from pymongo import MongoClient
from pymongo.read_preferences import (
    Nearest,
    Primary,
    PrimaryPreferred,
    Secondary,
    SecondaryPreferred,
)


class TestQueryAndReadModeSharded(unittest.TestCase):
    def test_query_and_read_mode_sharded_op_msg(self):
        """Test OP_MSG sends non-primary $readPreference and never $query."""
        server = MockupDB()
        server.autoresponds(
            "ismaster", ismaster=True, msg="isdbgrid", minWireVersion=2, maxWireVersion=6
        )
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
            SecondaryPreferred([{"tag": "value"}]),
        )

        for query in (
            {"a": 1},
            {"$query": {"a": 1}},
        ):
            for pref in read_prefs:
                collection = client.db.get_collection("test", read_preference=pref)
                cursor = collection.find(query.copy())
                with going(next, cursor):
                    request = server.receives()
                    # Command is not nested in $query.
                    expected_cmd = SON([("find", "test"), ("filter", {"a": 1})])
                    if pref.mode:
                        expected_cmd["$readPreference"] = pref.document
                    request.assert_matches(OpMsg(expected_cmd))

                    request.replies({"cursor": {"id": 0, "firstBatch": [{}]}})


if __name__ == "__main__":
    unittest.main()
