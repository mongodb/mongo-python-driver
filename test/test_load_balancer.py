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

import gc
import os
import sys
import threading

sys.path[0:0] = [""]

from test import unittest, IntegrationTest, client_context
from test.utils import get_pool, wait_until, ExceptionCatchingThread
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

    def test_cursor_gc(self):
        def create_resource(coll):
            cursor = coll.find({}, batch_size=3)
            next(cursor)
            return cursor
        self._test_no_gc_deadlock(create_resource)

    def test_command_cursor_gc(self):
        def create_resource(coll):
            cursor = coll.aggregate([], batchSize=3)
            next(cursor)
            return cursor
        self._test_no_gc_deadlock(create_resource)

    def _test_no_gc_deadlock(self, create_resource):
        pool = get_pool(self.client)
        self.db.test.insert_many([{} for _ in range(10)])
        # Cause the initial find attempt to fail to induce a reference cycle.
        args = {
            "mode": {
              "times": 1
            },
            "data": {
              "failCommands": [
                "find", "aggregate"
              ],
              "errorCode": 91,
              "closeConnection": True,
            }
        }
        with self.fail_point(args):
            resource = create_resource(self.db.test)
            if client_context.load_balancer:
                self.assertEqual(pool.active_sockets, 1)  # Pinned.

        thread = PoolLocker(pool)
        thread.start()
        self.assertTrue(thread.locked.wait(5), 'timed out')
        # Garbage collect the resource while the pool is locked to ensure we
        # don't deadlock.
        del resource
        gc.collect()
        thread.unlock.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(thread.exc)

        wait_until(lambda: pool.active_sockets == 0, 'return socket')
        # Run another operation to ensure the socket still works.
        self.db.test.delete_many({})


class PoolLocker(ExceptionCatchingThread):
    def __init__(self, pool):
        super(PoolLocker, self).__init__(target=self.lock_pool)
        self.pool = pool
        self.daemon = True
        self.locked = threading.Event()
        self.unlock = threading.Event()

    def lock_pool(self):
        with self.pool.lock:
            self.locked.set()
            # Wait for the unlock flag.
            unlock_pool = self.unlock.wait(10)
            if not unlock_pool:
                raise Exception('timed out waiting for unlock signal:'
                                ' deadlock?')


if __name__ == "__main__":
    unittest.main()
