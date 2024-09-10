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
from __future__ import annotations

import asyncio
import os
import threading
import time
import weakref
from typing import Any, Callable, Optional

_HAS_REGISTER_AT_FORK = hasattr(os, "register_at_fork")

# References to instances of _create_lock
_forkable_locks: weakref.WeakSet[threading.Lock] = weakref.WeakSet()


def _create_lock() -> threading.Lock:
    """Represents a lock that is tracked upon instantiation using a WeakSet and
    reset by pymongo upon forking.
    """
    lock = threading.Lock()
    if _HAS_REGISTER_AT_FORK:
        _forkable_locks.add(lock)
    return lock


def _release_locks() -> None:
    # Completed the fork, reset all the locks in the child.
    for lock in _forkable_locks:
        if lock.locked():
            lock.release()


class _ALock:
    def __init__(self, lock: threading.Lock) -> None:
        self._lock = lock

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        return self._lock.acquire(blocking=blocking, timeout=timeout)

    async def a_acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if timeout > 0:
            tstart = time.monotonic()
        while True:
            acquired = self._lock.acquire(blocking=False)
            if acquired:
                return True
            if timeout > 0 and (time.monotonic() - tstart) > timeout:
                return False
            if not blocking:
                return False
            await asyncio.sleep(0)

    def release(self) -> None:
        self._lock.release()

    async def __aenter__(self) -> _ALock:
        await self.a_acquire()
        return self

    def __enter__(self) -> _ALock:
        self._lock.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()


class _ACondition:
    def __init__(self, condition: threading.Condition) -> None:
        self._condition = condition

    async def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if timeout > 0:
            tstart = time.monotonic()
        while True:
            acquired = self._condition.acquire(blocking=False)
            if acquired:
                return True
            if timeout > 0 and (time.monotonic() - tstart) > timeout:
                return False
            if not blocking:
                return False
            await asyncio.sleep(0)

    async def wait(self, timeout: Optional[float] = None) -> bool:
        if timeout is not None:
            tstart = time.monotonic()
        while True:
            notified = self._condition.wait(0.001)
            if notified:
                return True
            if timeout is not None and (time.monotonic() - tstart) > timeout:
                return False

    async def wait_for(self, predicate: Callable, timeout: Optional[float] = None) -> bool:
        if timeout is not None:
            tstart = time.monotonic()
        while True:
            notified = self._condition.wait_for(predicate, 0.001)
            if notified:
                return True
            if timeout is not None and (time.monotonic() - tstart) > timeout:
                return False

    def notify(self, n: int = 1) -> None:
        self._condition.notify(n)

    def notify_all(self) -> None:
        self._condition.notify_all()

    def release(self) -> None:
        self._condition.release()

    async def __aenter__(self) -> _ACondition:
        await self.acquire()
        return self

    def __enter__(self) -> _ACondition:
        self._condition.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()
