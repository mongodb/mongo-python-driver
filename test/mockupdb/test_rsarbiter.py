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

"""Test connections to RSArbiter nodes."""

import datetime

from mockupdb import going, MockupDB
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

import unittest


class TestRSArbiter(unittest.TestCase):

    def test_rsarbiter_auth(self):
        rsarbiter_response = {
            'ok': 1.0, 'ismaster': False, 'secondary': False,  'setName': 'rs',
            'arbiterOnly': True, 'maxBsonObjectSize': 16777216,
            'maxMessageSizeBytes': 48000000, 'maxWriteBatchSize': 100000,
            'localTime': datetime.datetime(2021, 11, 30, 0, 53, 4, 99000),
            'logicalSessionTimeoutMinutes': 30, 'connectionId': 3,
            'minWireVersion': 0, 'maxWireVersion': 15, 'readOnly': False}
        server = MockupDB(auto_ismaster=rsarbiter_response)
        server.autoresponds('authenticate', ok=0, code=13,
                            errmsg='authenticate should not be called')
        server.autoresponds('saslStart', ok=0, code=13,
                            errmsg='saslStart should not be called')
        server.autoresponds('ping')
        server.run()
        self.addCleanup(server.stop)
        # Direct connection succeeds and skips auth step.
        with MongoClient(server.uri, directConnection=True,
                         username='invalid', password='invalid') as client:
            client.test.command('ping')


if __name__ == '__main__':
    unittest.main()
