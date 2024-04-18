# Copyright 2024-present MongoDB, Inc.
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

"""Test errors that come from a standalone shard."""
from __future__ import annotations

import unittest

from mockupdb import MockupDB, going

from pymongo import MongoClient
from pymongo.errors import OperationFailure


class TestStandaloneShard(unittest.TestCase):
    # See PYTHON-2048 and SERVER-44591.
    def test_bulk_txn_error_message(self):
        server = MockupDB(auto_ismaster={"maxWireVersion": 8})
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri)
        self.addCleanup(client.close)

        with self.assertRaisesRegex(
            OperationFailure, "This MongoDB deployment does not support retryable writes"
        ):
            with going(client.db.collection.insert_many, [{}, {}]):
                request = server.receives()
                request.reply(
                    {
                        "n": 0,
                        "ok": 1.0,
                        "writeErrors": [
                            {
                                "code": 20,
                                "codeName": "IllegalOperation",
                                "errmsg": "Transaction numbers are only allowed on a replica set member or mongos",
                                "index": 0,
                            }
                        ],
                    }
                )


if __name__ == "__main__":
    unittest.main()
