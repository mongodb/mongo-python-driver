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

"""Test list_indexes with more than one batch."""
from __future__ import annotations

import unittest
from test import PyMongoTestCase

import pytest

try:
    from mockupdb import MockupDB, going

    _HAVE_MOCKUPDB = True
except ImportError:
    _HAVE_MOCKUPDB = False


from bson import SON
from pymongo.common import MIN_SUPPORTED_WIRE_VERSION

pytestmark = pytest.mark.mockupdb


class TestListIndexes(PyMongoTestCase):
    def test_list_indexes_command(self):
        server = MockupDB(auto_ismaster={"maxWireVersion": MIN_SUPPORTED_WIRE_VERSION})
        server.run()
        self.addCleanup(server.stop)
        client = self.simple_client(server.uri)
        with going(client.test.collection.list_indexes) as cursor:
            request = server.receives(listIndexes="collection", namespace="test")
            request.reply({"cursor": {"firstBatch": [{"name": "index_0"}], "id": 123}})

        with going(list, cursor()) as indexes:
            request = server.receives(getMore=123, namespace="test", collection="collection")

            request.reply({"cursor": {"nextBatch": [{"name": "index_1"}], "id": 0}})

        self.assertEqual([{"name": "index_0"}, {"name": "index_1"}], indexes())
        for index_info in indexes():
            self.assertIsInstance(index_info, SON)


if __name__ == "__main__":
    unittest.main()
