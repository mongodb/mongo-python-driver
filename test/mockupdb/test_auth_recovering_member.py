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

import unittest

from mockupdb import MockupDB

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


class TestAuthRecoveringMember(unittest.TestCase):
    def test_auth_recovering_member(self):
        # Test that we don't attempt auth against a recovering RS member.
        server = MockupDB()
        server.autoresponds(
            "ismaster",
            {
                "minWireVersion": 2,
                "maxWireVersion": 6,
                "ismaster": False,
                "secondary": False,
                "setName": "rs",
            },
        )

        server.run()
        self.addCleanup(server.stop)

        client = MongoClient(
            server.uri, replicaSet="rs", serverSelectionTimeoutMS=100, socketTimeoutMS=100
        )

        self.addCleanup(client.close)

        # Should see there's no primary or secondary and raise selection timeout
        # error. If it raises AutoReconnect we know it actually tried the
        # server, and that's wrong.
        with self.assertRaises(ServerSelectionTimeoutError):
            client.db.command("ping")


if __name__ == "__main__":
    unittest.main()
