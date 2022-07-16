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

import threading
import weakref


class _ForkLock:
    """
    Represents a lock that is tracked upon instantiation using a WeakSet and
    reset by pymongo upon forking.
    """

    _locks: weakref.WeakSet = weakref.WeakSet()  # References to instances of _ForkLock

    _insertion_lock = threading.Lock()

    def __init__(self):
        self._lock = threading.Lock()
        with _ForkLock._insertion_lock:
            _ForkLock._locks.add(self)

    def __getattr__(self, item):
        return getattr(self._lock, item)

    def __enter__(self):
        self._lock.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.__exit__(exc_type, exc_val, exc_tb)

    @classmethod
    def _release_locks(cls, child: bool):
        # Completed the fork, reset all the locks in the child.
        if child:
            for lock in _ForkLock._locks:
                if lock._lock.locked():
                    lock._lock.release()
        _ForkLock._insertion_lock.release()

    @classmethod
    def _acquire_locks(cls):
        _ForkLock._insertion_lock.acquire()
