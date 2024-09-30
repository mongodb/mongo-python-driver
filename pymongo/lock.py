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
import collections
import os
import threading
import time
import weakref
from typing import Any, Callable, Optional, TypeVar

_HAS_REGISTER_AT_FORK = hasattr(os, "register_at_fork")

# References to instances of _create_lock
_forkable_locks: weakref.WeakSet[threading.Lock] = weakref.WeakSet()

_T = TypeVar("_T")


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


# Needed only for synchro.py compat.
def _Lock(lock: threading.Lock) -> threading.Lock:
    return lock


class _ALock:
    __slots__ = ("_lock",)

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


def _safe_set_result(fut: asyncio.Future) -> None:
    # Ensure the future hasn't been cancelled before calling set_result.
    if not fut.done():
        fut.set_result(False)


class _ACondition:
    __slots__ = ("_condition", "_waiters")

    def __init__(self, condition: threading.Condition) -> None:
        self._condition = condition
        self._waiters: collections.deque = collections.deque()

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
        """Wait until notified.

        If the calling task has not acquired the lock when this
        method is called, a RuntimeError is raised.

        This method releases the underlying lock, and then blocks
        until it is awakened by a notify() or notify_all() call for
        the same condition variable in another task.  Once
        awakened, it re-acquires the lock and returns True.

        This method may return spuriously,
        which is why the caller should always
        re-check the state and be prepared to wait() again.
        """
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._waiters.append((loop, fut))
        self.release()
        try:
            try:
                try:
                    await asyncio.wait_for(fut, timeout)
                    return True
                except asyncio.TimeoutError:
                    return False  # Return false on timeout for sync pool compat.
            finally:
                # Must re-acquire lock even if wait is cancelled.
                # We only catch CancelledError here, since we don't want any
                # other (fatal) errors with the future to cause us to spin.
                err = None
                while True:
                    try:
                        await self.acquire()
                        break
                    except asyncio.exceptions.CancelledError as e:
                        err = e

                self._waiters.remove((loop, fut))
                if err is not None:
                    try:
                        raise err  # Re-raise most recent exception instance.
                    finally:
                        err = None  # Break reference cycles.
        except BaseException:
            # Any error raised out of here _may_ have occurred after this Task
            # believed to have been successfully notified.
            # Make sure to notify another Task instead.  This may result
            # in a "spurious wakeup", which is allowed as part of the
            # Condition Variable protocol.
            self.notify(1)
            raise

    async def wait_for(self, predicate: Callable[[], _T]) -> _T:
        """Wait until a predicate becomes true.

        The predicate should be a callable whose result will be
        interpreted as a boolean value.  The method will repeatedly
        wait() until it evaluates to true.  The final predicate value is
        the return value.
        """
        result = predicate()
        while not result:
            await self.wait()
            result = predicate()
        return result

    def notify(self, n: int = 1) -> None:
        """By default, wake up one coroutine waiting on this condition, if any.
        If the calling coroutine has not acquired the lock when this method
        is called, a RuntimeError is raised.

        This method wakes up at most n of the coroutines waiting for the
        condition variable; it is a no-op if no coroutines are waiting.

        Note: an awakened coroutine does not actually return from its
        wait() call until it can reacquire the lock. Since notify() does
        not release the lock, its caller should.
        """
        idx = 0
        to_remove = []
        for loop, fut in self._waiters:
            if idx >= n:
                break

            if fut.done():
                continue

            try:
                loop.call_soon_threadsafe(_safe_set_result, fut)
            except RuntimeError:
                # Loop was closed, ignore.
                to_remove.append((loop, fut))
                continue

            idx += 1

        for waiter in to_remove:
            self._waiters.remove(waiter)

    def notify_all(self) -> None:
        """Wake up all threads waiting on this condition. This method acts
        like notify(), but wakes up all waiting threads instead of one. If the
        calling thread has not acquired the lock when this method is called,
        a RuntimeError is raised.
        """
        self.notify(len(self._waiters))

    def locked(self) -> bool:
        """Only needed for tests in test_locks."""
        return self._condition._lock.locked()  # type: ignore[attr-defined]

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
