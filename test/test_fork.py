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
import platform
import threading
from multiprocessing import Pipe
from test import IntegrationTest, client_context
from typing import Any, Callable
from unittest import skipIf
from unittest.mock import patch

from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.lock import MongoClientLock


@client_context.require_connection
def setUpModule():
    pass


# Not available for versions of Python without "register_at_fork"
@skipIf(
    not hasattr(os, "register_at_fork"), "register_at_fork not available in this version of Python"
)
class TestFork(IntegrationTest):
    def setUp(self):
        self.db = self.client.pymongo_test

    class LockWrapper:
        def __init__(self, _lock_type: Any = MongoClientLock, _after_enter: Callable = None):
            self.__lock = _lock_type()
            self._after_enter = _after_enter

        def __enter__(self):
            self.__lock.__enter__()
            self._after_enter()

        def __exit__(self, exc_type, exc_value, traceback):
            self.__lock.__exit__(exc_type, exc_value, traceback)

        def __getattr__(self, item):
            return getattr(self.__lock, item)

    def test_lock_client(self):
        """
        Forks the client with some items locked.
        Parent => All locks should be as before the fork.
        Child => All locks should be reset.
        """
        lock_pid: int = -1

        def _fork():
            nonlocal lock_pid
            lock_pid = os.fork()

        with patch.object(
            self.db.client, "_MongoClient__lock", TestFork.LockWrapper(_after_enter=_fork)
        ):
            # Call _get_topology, will fork upon __enter__ing
            # the with region.
            self.db.client._get_topology()

            if lock_pid == 0:  # Child
                os._exit(0 if not self.db.client._MongoClient__lock.locked() else 1)
            else:  # Parent
                self.assertEqual(0, os.waitpid(lock_pid, 0)[1] >> 8)

    def test_lock_object_id(self):
        """
        Forks the client with ObjectId's _inc_lock locked.
        Will fork upon __enter__, waits for child to return.
        Parent => _inc_lock should remain locked.
        Child => _inc_lock should be unlocked.
        Must use threading.Lock as ObjectId uses this.
        """

        lock_pid: int = -1

        def _fork():
            nonlocal lock_pid
            lock_pid = os.fork()

        with patch.object(
            ObjectId,
            "_inc_lock",
            TestFork.LockWrapper(_lock_type=threading.Lock, _after_enter=_fork),
        ):
            # Generate the ObjectId, will fork upon __enter__ing
            # the with region.
            ObjectId()
            if lock_pid == 0:  # Child
                os._exit(0 if not ObjectId._inc_lock.locked() else 1)
            else:  # Parent
                self.assertEqual(0, os.waitpid(lock_pid, 0)[1] >> 8)

    @skipIf(
        platform.python_implementation() != "CPython", "Depends on CPython implementation of id"
    )
    def test_topology_reset(self):
        """
        Tests that topologies are different from each other.
        Cannot use ID because virtual memory addresses may be the same.
        Cannot reinstantiate ObjectId in the topology settings.
        Relies on difference in PID when opened again.
        """
        parent_conn, child_conn = Pipe()
        cl_test = MongoClient()
        init_id = cl_test._topology._pid
        lock_pid: int = os.fork()

        if lock_pid == 0:  # Child
            cl_test._topology.open()
            child_conn.send(cl_test._topology._pid)
            os._exit(0)
        else:  # Parent
            self.assertEqual(cl_test._topology._pid, init_id)
            child_id = parent_conn.recv()
            self.assertNotEqual(child_id, init_id)
