# Copyright 2022-present MongoDB, Inc.
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

"""Test that pymongo resets its own locks after a fork."""
from __future__ import annotations

import os
import sys
import unittest
import warnings
from multiprocessing import Pipe

sys.path[0:0] = [""]

from test import IntegrationTest
from test.utils import is_greenthread_patched

from bson.objectid import ObjectId


@unittest.skipIf(
    not hasattr(os, "register_at_fork"), "register_at_fork not available in this version of Python"
)
@unittest.skipIf(
    is_greenthread_patched(),
    "gevent and eventlet do not support POSIX-style forking.",
)
class TestFork(IntegrationTest):
    def test_lock_client(self):
        # Forks the client with some items locked.
        # Parent => All locks should be as before the fork.
        # Child => All locks should be reset.
        with self.client._MongoClient__lock:

            def target():
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    self.client.admin.command("ping")

            with self.fork(target):
                pass
        self.client.admin.command("ping")

    def test_lock_object_id(self):
        # Forks the client with ObjectId's _inc_lock locked.
        # Parent => _inc_lock should remain locked.
        # Child => _inc_lock should be unlocked.
        with ObjectId._inc_lock:

            def target():
                self.assertFalse(ObjectId._inc_lock.locked())
                self.assertTrue(ObjectId())

            with self.fork(target):
                pass

    def test_topology_reset(self):
        # Tests that topologies are different from each other.
        # Cannot use ID because virtual memory addresses may be the same.
        # Cannot reinstantiate ObjectId in the topology settings.
        # Relies on difference in PID when opened again.
        parent_conn, child_conn = Pipe()
        init_id = self.client._topology._pid
        parent_cursor_exc = self.client._kill_cursors_executor

        def target():
            # Catch the fork warning and send to the parent for assertion.
            with warnings.catch_warnings(record=True) as ctx:
                warnings.simplefilter("always")
                self.client.admin.command("ping")
                child_conn.send(str(ctx[0]))
            child_conn.send(self.client._topology._pid)
            child_conn.send(
                (
                    parent_cursor_exc != self.client._kill_cursors_executor,
                    "client._kill_cursors_executor was not reinitialized",
                )
            )

        with self.fork(target):
            self.assertEqual(self.client._topology._pid, init_id)
            fork_warning = parent_conn.recv()
            self.assertIn("MongoClient opened before fork", fork_warning)
            child_id = parent_conn.recv()
            self.assertNotEqual(child_id, init_id)
            passed, msg = parent_conn.recv()
            self.assertTrue(passed, msg)


if __name__ == "__main__":
    unittest.main()
