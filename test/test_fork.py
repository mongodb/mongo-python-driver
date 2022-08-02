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

"""Test that pymongo is fork safe."""

import os
import threading
from multiprocessing import Pipe
from test import IntegrationTest, client_context
from typing import Any, Callable
from unittest import skipIf
from unittest.mock import patch

from bson.objectid import ObjectId
from pymongo.lock import _create_lock, _insertion_lock


@client_context.require_connection
def setUpModule():
    pass


# Not available for versions of Python without "register_at_fork"
@skipIf(
    not hasattr(os, "register_at_fork"), "register_at_fork not available in this version of Python"
)
class TestFork(IntegrationTest):
    def test_lock_client(self):
        """
        Forks the client with some items locked.
        Parent => All locks should be as before the fork.
        Child => All locks should be reset.
        """

        def exit_cond():
            with _insertion_lock:
                return 0  # success

        with self.client._MongoClient__lock:
            # Call _get_topology, will launch a thread to fork upon __enter__ing
            # the with region.
            lock_pid = os.fork()
            # The POSIX standard states only the forking thread is cloned.
            # In the parent, it'll return here.
            # In the child, it'll end with the calling thread.
            if lock_pid == 0:
                os._exit(exit_cond())
            else:
                self.assertEqual(0, os.waitpid(lock_pid, 0)[1] >> 8)

    def test_lock_object_id(self):
        """
        Forks the client with ObjectId's _inc_lock locked.
        Parent => _inc_lock should remain locked.
        Child => _inc_lock should be unlocked.
        """
        with ObjectId._inc_lock:
            lock_pid: int = os.fork()

            if lock_pid == 0:
                os._exit(int(ObjectId._inc_lock.locked()))
            else:
                self.assertEqual(0, os.waitpid(lock_pid, 0)[1] >> 8)

    def test_topology_reset(self):
        """
        Tests that topologies are different from each other.
        Cannot use ID because virtual memory addresses may be the same.
        Cannot reinstantiate ObjectId in the topology settings.
        Relies on difference in PID when opened again.
        """
        parent_conn, child_conn = Pipe()
        init_id = self.client._topology._pid
        parent_cursor_exc = self.client._kill_cursors_executor
        lock_pid: int = os.fork()

        if lock_pid == 0:  # Child
            self.client.admin.command("ping")
            child_conn.send(self.client._topology._pid)
            child_conn.send(
                (
                    parent_cursor_exc != self.client._kill_cursors_executor,
                    "client._kill_cursors_executor was not reinitialized",
                )
            )
            os._exit(0)
        else:  # Parent
            self.assertEqual(0, os.waitpid(lock_pid, 0)[1] >> 8)
            self.assertEqual(self.client._topology._pid, init_id)
            child_id = parent_conn.recv()
            self.assertNotEqual(child_id, init_id)
            passed, msg = parent_conn.recv()
            self.assertTrue(passed, msg)
