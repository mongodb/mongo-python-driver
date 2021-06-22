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

"""Test the Load Balancer unified spec tests."""

import os
import sys

sys.path[0:0] = [""]

from test import unittest, IntegrationTest, client_context
from test.utils import get_pool
from test.unified_format import generate_test_classes

# Location of JSON test specifications.
TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'load_balancer', 'unified')

# Generate unified tests.
globals().update(generate_test_classes(TEST_PATH, module=__name__))


class TestLB(IntegrationTest):

    def test_connections_are_only_returned_once(self):
        pool = get_pool(self.client)
        nconns = len(pool.sockets)
        self.db.test.find_one({})
        self.assertEqual(len(pool.sockets), nconns)
        self.db.test.aggregate([{'$limit': 1}])
        self.assertEqual(len(pool.sockets), nconns)

    @client_context.require_load_balancer
    def test_unpin_committed_transaction(self):
        pool = get_pool(self.client)
        with self.client.start_session() as session:
            with session.start_transaction():
                self.assertEqual(pool.active_sockets, 0)
                self.db.test.insert_one({}, session=session)
                self.assertEqual(pool.active_sockets, 1)  # Pinned.
            self.assertEqual(pool.active_sockets, 1)  # Still pinned.
        self.assertEqual(pool.active_sockets, 0)  # Unpinned.

    def test_client_can_be_reopened(self):
        self.client.close()
        self.db.test.find_one({})


if __name__ == "__main__":
    unittest.main()
