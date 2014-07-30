# Copyright 2012-2014 MongoDB, Inc.
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

"""Utilities for multi-threading support."""

import threading
import sys
import weakref
try:
    from time import monotonic as _time
except ImportError:
    from time import time as _time


from pymongo.errors import ExceededMaxWaiters


# Do we have to work around http://bugs.python.org/issue1868?
issue1868 = (sys.version_info[:3] <= (2, 7, 0))


class DummyLock(object):
    def acquire(self):
        pass

    def release(self):
        pass


class ThreadIdent(object):
    def __init__(self):
        self._refs = {}
        self._local = threading.local()
        if issue1868:
            self._lock = threading.Lock()
        else:
            self._lock = DummyLock()

    def watching(self):
        """Is the current thread being watched for death?"""
        return self.get() in self._refs

    def unwatch(self, tid):
        self._refs.pop(tid, None)

    def get(self):
        return id(self._make_vigil())

    def watch(self, callback):
        vigil = self._make_vigil()
        self._refs[id(vigil)] = weakref.ref(vigil, callback)

    # We watch for thread-death using a weakref callback to a thread local.
    # Weakrefs are permitted on subclasses of object but not object() itself.
    class ThreadVigil(object):
        pass

    def _make_vigil(self):
        # Threadlocals in Python <= 2.7.0 have race conditions when setting
        # attributes and possibly when getting them, too, leading to weakref
        # callbacks not getting called later.
        self._lock.acquire()
        try:
            vigil = getattr(self._local, 'vigil', None)
            if not vigil:
                self._local.vigil = vigil = ThreadIdent.ThreadVigil()
        finally:
            self._lock.release()

        return vigil


class Counter(object):
    """A thread-local counter."""
    def __init__(self):
        self.ident = ThreadIdent()
        self._counters = {}

    def inc(self):
        # Copy these references so on_thread_died needn't close over self
        ident = self.ident
        _counters = self._counters

        tid = ident.get()
        _counters.setdefault(tid, 0)
        _counters[tid] += 1

        if not ident.watching():
            # Before the tid is possibly reused, remove it from _counters
            def on_thread_died(ref):
                ident.unwatch(tid)
                _counters.pop(tid, None)

            ident.watch(on_thread_died)

        return _counters[tid]

    def dec(self):
        tid = self.ident.get()
        if self._counters.get(tid, 0) > 0:
            self._counters[tid] -= 1
            return self._counters[tid]
        else:
            return 0

    def get(self):
        return self._counters.get(self.ident.get(), 0)


class Future(object):
    """Minimal backport of concurrent.futures.Future.

    event_class makes this Future adaptable for async clients.
    """
    def __init__(self, event_class):
        self._event = event_class()
        self._result = None
        self._exception = None

    def set_result(self, result):
        self._result = result
        self._event.set()

    def set_exception(self, exc):
        if hasattr(exc, 'with_traceback'):
            # Python 3: avoid potential reference cycle.
            self._exception = exc.with_traceback(None)
        else:
            self._exception = exc
        self._event.set()

    def result(self):
        self._event.wait()
        if self._exception:
            raise self._exception
        else:
            return self._result


### Begin backport from CPython 3.2 for timeout support for Semaphore.acquire
class Semaphore:

    # After Tim Peters' semaphore class, but not quite the same (no maximum)

    def __init__(self, value=1):
        if value < 0:
            raise ValueError("semaphore initial value must be >= 0")
        self._cond = threading.Condition(threading.Lock())
        self._value = value

    def acquire(self, blocking=True, timeout=None):
        if not blocking and timeout is not None:
            raise ValueError("can't specify timeout for non-blocking acquire")
        rc = False
        endtime = None
        self._cond.acquire()
        while self._value == 0:
            if not blocking:
                break
            if timeout is not None:
                if endtime is None:
                    endtime = _time() + timeout
                else:
                    timeout = endtime - _time()
                    if timeout <= 0:
                        break
            self._cond.wait(timeout)
        else:
            self._value = self._value - 1
            rc = True
        self._cond.release()
        return rc

    __enter__ = acquire

    def release(self):
        self._cond.acquire()
        self._value = self._value + 1
        self._cond.notify()
        self._cond.release()

    def __exit__(self, t, v, tb):
        self.release()

    @property
    def counter(self):
        return self._value


class BoundedSemaphore(Semaphore):
    """Semaphore that checks that # releases is <= # acquires"""
    def __init__(self, value=1):
        Semaphore.__init__(self, value)
        self._initial_value = value

    def release(self):
        if self._value >= self._initial_value:
            raise ValueError("Semaphore released too many times")
        return Semaphore.release(self)
### End backport from CPython 3.2


class DummySemaphore(object):
    def __init__(self, value=None):
        pass

    def acquire(self, blocking=True, timeout=None):
        return True

    def release(self):
        pass


class MaxWaitersBoundedSemaphore(object):
    def __init__(self, semaphore_class, value=1, max_waiters=1):
        self.waiter_semaphore = semaphore_class(max_waiters)
        self.semaphore = semaphore_class(value)

    def acquire(self, blocking=True, timeout=None):
        if not self.waiter_semaphore.acquire(False):
            raise ExceededMaxWaiters()
        try:
            return self.semaphore.acquire(blocking, timeout)
        finally:
            self.waiter_semaphore.release()

    def __getattr__(self, name):
        return getattr(self.semaphore, name)


class MaxWaitersBoundedSemaphoreThread(MaxWaitersBoundedSemaphore):
    def __init__(self, value=1, max_waiters=1):
        MaxWaitersBoundedSemaphore.__init__(
            self, BoundedSemaphore, value, max_waiters)


def create_semaphore(max_size, max_waiters):
    if max_size is None:
        return DummySemaphore()
    else:
        if max_waiters is None:
            return BoundedSemaphore(max_size)
        else:
            return MaxWaitersBoundedSemaphoreThread(max_size, max_waiters)


class Event(object):
    """Copy of standard threading.Event, but uses a custom condition class.

    Allows async frameworks to override monitors' synchronization behavior
    with ClusterSettings.condition_class.

    Copied from CPython's threading.py at hash c7960cc9.
    """

    def __init__(self, condition_class):
        self._cond = condition_class(threading.Lock())
        self._flag = False

    def is_set(self):
        """Return true if and only if the internal flag is true."""
        return self._flag

    isSet = is_set

    def set(self):
        """Set the internal flag to true.

        All threads waiting for it to become true are awakened. Threads
        that call wait() once the flag is true will not block at all.

        """
        self._cond.acquire()
        try:
            self._flag = True
            self._cond.notify_all()
        finally:
            self._cond.release()

    def clear(self):
        """Reset the internal flag to false.

        Subsequently, threads calling wait() will block until set() is called to
        set the internal flag to true again.

        """
        self._cond.acquire()
        try:
            self._flag = False
        finally:
            self._cond.release()

    def wait(self, timeout=None):
        """Block until the internal flag is true.

        If the internal flag is true on entry, return immediately. Otherwise,
        block until another thread calls set() to set the flag to true, or until
        the optional timeout occurs.

        When the timeout argument is present and not None, it should be a
        floating point number specifying a timeout for the operation in seconds
        (or fractions thereof).

        This method returns the internal flag on exit, so it will always return
        True except if a timeout is given and the operation times out.

        """
        self._cond.acquire()
        try:
            signaled = self._flag
            if not signaled:
                signaled = self._cond.wait(timeout)
            return signaled
        finally:
            self._cond.release()
