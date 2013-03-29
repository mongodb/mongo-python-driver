# Copyright 2012 10gen, Inc.
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

"""Utilities to abstract the differences between threads and greenlets."""

import threading
import weakref
try:
    from time import monotonic as _time
except ImportError:
    from time import time as _time

have_greenlet = True
try:
    import greenlet
except ImportError:
    have_greenlet = False


class Ident(object):
    def __init__(self):
        self._refs = {}

    def watching(self):
        """Is the current thread or greenlet being watched for death?"""
        return self.get() in self._refs

    def unwatch(self):
        self._refs.pop(self.get(), None)

    def get(self):
        """An id for this thread or greenlet"""
        raise NotImplementedError

    def watch(self, callback):
        """Run callback when this thread or greenlet dies. callback takes
        one meaningless argument.
        """
        raise NotImplementedError


class ThreadIdent(Ident):
    def __init__(self):
        super(ThreadIdent, self).__init__()
        self._local = threading.local()

    # We watch for thread-death using a weakref callback to a thread local.
    # Weakrefs are permitted on subclasses of object but not object() itself.
    class ThreadVigil(object):
        pass

    def get(self):
        if not hasattr(self._local, 'vigil'):
            self._local.vigil = ThreadIdent.ThreadVigil()
        return id(self._local.vigil)

    def watch(self, callback):
        tid = self.get()
        self._refs[tid] = weakref.ref(self._local.vigil, callback)


class GreenletIdent(Ident):
    def get(self):
        return id(greenlet.getcurrent())

    def watch(self, callback):
        current = greenlet.getcurrent()
        tid = self.get()

        if hasattr(current, 'link'):
            # This is a Gevent Greenlet (capital G), which inherits from
            # greenlet and provides a 'link' method to detect when the
            # Greenlet exits.
            current.link(callback)
            self._refs[tid] = None
        else:
            # This is a non-Gevent greenlet (small g), or it's the main
            # greenlet.
            self._refs[tid] = weakref.ref(current, callback)


def create_ident(use_greenlets):
    if use_greenlets:
        return GreenletIdent()
    else:
        return ThreadIdent()


class Counter(object):
    """A thread- or greenlet-local counter.
    """
    def __init__(self, use_greenlets):
        self.ident = create_ident(use_greenlets)
        self._counters = {}

    def inc(self):
        tid = self.ident.get()
        self._counters.setdefault(tid, 0)
        self._counters[tid] += 1
        return self._counters[tid]

    def dec(self):
        tid = self.ident.get()
        if self._counters.get(tid, 0) > 0:
            self._counters[tid] -= 1
            return self._counters[tid]
        else:
            return 0

    def get(self):
        return self._counters.get(self.ident.get(), 0)


class SynchronizedCounter(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.__value = 0

    def inc(self):
        try:
            self.lock.acquire()
            self.__value += 1
        finally:
            self.lock.release()

    def dec(self):
        try:
            self.lock.acquire()
            self.__value -= 1
        finally:
            self.lock.release()

    def get(self):
        # NOTE(reversefold): I don't believe a simple return of a value needs
        # to use the lock. The copy of __value for the return should be atomic.
        return self.__value


### Begin backport from CPython 3.2 for timeout support for acquire
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


class NoopSemaphore(object):
    def __init__(self, value=None):
        pass

    def acquire(self, blocking=True, timeout=None):
        return True

    def release(self):
        pass
