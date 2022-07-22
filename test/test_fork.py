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


class ForkThread(threading.Thread):
    def __init__(self, exit_cond: Callable[[], int] = lambda: 0):
        super().__init__()
        self.pid: int = -1  # Indicate we haven't started.
        self.exit_cond = exit_cond

    def run(self):
        if self.pid < 0:
            self.pid = os.fork()
            if self.pid == 0:
                os._exit(self.exit_cond())


class LockWrapper:
    def __init__(self, fork_thread: ForkThread, lock_type: Any = _create_lock):
        self.__lock = lock_type()
        self.fork_thread = fork_thread

    def __enter__(self):
        self.__lock.__enter__()
        if not self.fork_thread.is_alive():
            self.fork_thread.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.fork_thread.join()
        self.__lock.__exit__(exc_type, exc_value, traceback)

    def __getattr__(self, item):
        return getattr(self.__lock, item)


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
            # Checking all the locks after forking this way is highly
            # probabilistic as more locking behavior may ensue. Instead we
            # check that we can acquire _insertion_lock, meaning we have
            # successfully completed.
            with _insertion_lock:
                return 0  # success

        fork_thread = ForkThread()
        fork_thread.exit_cond = exit_cond
        with patch.object(
            self.db.client, "_MongoClient__lock", LockWrapper(fork_thread=fork_thread)
        ):
            # Call _get_topology, will launch a thread to fork upon __enter__ing
            # the with region.
            self.db.client._get_topology()
            lock_pid: int = self.db.client._MongoClient__lock.fork_thread.pid
            # The POSIX standard states only the forking thread is cloned.
            # In the parent, it'll return here.
            # In the child, it'll end with the calling thread.
            self.assertNotEqual(lock_pid, 0)
            self.assertEqual(0, os.waitpid(lock_pid, 0)[1] >> 8)

    def test_lock_object_id(self):
        """
        Forks the client with ObjectId's _inc_lock locked.
        Will fork upon __enter__, waits for child to return.
        Parent => _inc_lock should remain locked.
        Child => _inc_lock should be unlocked.
        Must use threading.Lock as ObjectId uses this.
        """
        fork_thread = ForkThread()
        fork_thread.exit_cond = lambda: 0 if not ObjectId._inc_lock.locked() else 1
        with patch.object(
            ObjectId,
            "_inc_lock",
            LockWrapper(fork_thread=fork_thread, lock_type=threading.Lock),
        ):
            # Generate the ObjectId, will generate a thread to fork
            # upon __enter__ing the with region.

            ObjectId()
            lock_pid: int = ObjectId._inc_lock.fork_thread.pid

            self.assertNotEqual(lock_pid, 0)
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
        lock_pid: int = os.fork()

        if lock_pid == 0:  # Child
            self.client.admin.command("ping")
            child_conn.send(self.client._topology._pid)
            os._exit(0)
        else:  # Parent
            self.assertEqual(self.client._topology._pid, init_id)
            child_id = parent_conn.recv()
            self.assertNotEqual(child_id, init_id)
