# Copyright 2021-present MongoDB, Inc.
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

"""Test connections to RSGhost nodes."""

import datetime
import unittest

from mockupdb import MockupDB, going

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


class TestRSGhost(unittest.TestCase):
    def test_rsghost(self):
        rsother_response = {
            "ok": 1.0,
            "ismaster": False,
            "secondary": False,
            "info": "Does not have a valid replica set config",
            "isreplicaset": True,
            "maxBsonObjectSize": 16777216,
            "maxMessageSizeBytes": 48000000,
            "maxWriteBatchSize": 100000,
            "localTime": datetime.datetime(2021, 11, 30, 0, 53, 4, 99000),
            "logicalSessionTimeoutMinutes": 30,
            "connectionId": 3,
            "minWireVersion": 0,
            "maxWireVersion": 15,
            "readOnly": False,
        }
        server = MockupDB(auto_ismaster=rsother_response)
        server.run()
        self.addCleanup(server.stop)
        # Default auto discovery yields a server selection timeout.
        with MongoClient(server.uri, serverSelectionTimeoutMS=250) as client:
            with self.assertRaises(ServerSelectionTimeoutError):
                client.test.command("ping")
        # Direct connection succeeds.
        with MongoClient(server.uri, directConnection=True) as client:
            with going(client.test.command, "ping"):
                request = server.receives(ping=1)
                request.reply()


if __name__ == "__main__":
    unittest.main()
