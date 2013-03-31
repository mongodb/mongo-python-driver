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

import thread
import threading
import weakref
try:
    from time import monotonic as _time
except ImportError:
    from time import time as _time
from time import sleep as _sleep
try:
    import gevent.coros
    import gevent.thread
except ImportError:
    pass

have_greenlet = True
try:
    import greenlet
except ImportError:
    have_greenlet = False

lock_acquire_has_timeout = False


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


### Begin backport from CPython 3.2 for timeout support for acquire
class Condition:

    def __init__(self, lock=None, use_greenlets=False):
        self.use_greenlets = use_greenlets
        if lock is None:
            if use_greenlets:
                lock = gevent.coros.RLock()
            else:
                lock = threading.RLock()
        self._lock = lock
        # Export the lock's acquire() and release() methods
        self.acquire = lock.acquire
        self.release = lock.release
        # If the lock defines _release_save() and/or _acquire_restore(),
        # these override the default implementations (which just call
        # release() and acquire() on the lock).  Ditto for _is_owned().
        try:
            self._release_save = lock._release_save
        except AttributeError:
            pass
        try:
            self._acquire_restore = lock._acquire_restore
        except AttributeError:
            pass
        try:
            self._is_owned = lock._is_owned
        except AttributeError:
            pass
        self._waiters = []

    def __enter__(self):
        return self._lock.__enter__()

    def __exit__(self, *args):
        return self._lock.__exit__(*args)

    def __repr__(self):
        return "<Condition(%s, %d)>" % (self._lock, len(self._waiters))

    def _release_save(self):
        self._lock.release()           # No state to save

    def _acquire_restore(self, x):
        self._lock.acquire()           # Ignore saved state

    def _is_owned(self):
        # Return True if lock is owned by current_thread.
        # This method is called only if __lock doesn't have _is_owned().
        if self._lock.acquire(0):
            self._lock.release()
            return False
        else:
            return True

    def wait(self, timeout=None):
        if not self._is_owned():
            raise RuntimeError("cannot wait on un-acquired lock")
        if self.use_greenlets:
            waiter = gevent.thread.allocate_lock()
        else:
            waiter = thread.allocate_lock()
        waiter.acquire()
        self._waiters.append(waiter)
        saved_state = self._release_save()
        try:    # restore state no matter what (e.g., KeyboardInterrupt)
            if timeout is None:
                waiter.acquire()
                gotit = True
            else:
                if lock_acquire_has_timeout:
                    if timeout > 0:
                        gotit = waiter.acquire(True, timeout)
                    else:
                        gotit = waiter.acquire(False)
                    if not gotit:
                        try:
                            self._waiters.remove(waiter)
                        except ValueError:
                            pass
                else:
                ### BEGIN forward-port from Python 2.x to support locks
                ### without timeouts
                    # Balancing act:  We can't afford a pure busy loop, so we
                    # have to sleep; but if we sleep the whole timeout time,
                    # we'll be unresponsive.  The scheme here sleeps very
                    # little at first, longer as time goes on, but never longer
                    # than 20 times per second (or the timeout time remaining).
                    endtime = _time() + timeout
                    delay = 0.0005 # 500 us -> initial delay of 1 ms
                    while True:
                        gotit = waiter.acquire(0)
                        if gotit:
                            break
                        remaining = endtime - _time()
                        if remaining <= 0:
                            break
                        delay = min(delay * 2, remaining, .05)
                        _sleep(delay)
                    if not gotit:
                        try:
                            self._waiters.remove(waiter)
                        except ValueError:
                            pass
                ### END forward-port
            return gotit
        finally:
            self._acquire_restore(saved_state)

    def wait_for(self, predicate, timeout=None):
        endtime = None
        waittime = timeout
        result = predicate()
        while not result:
            if waittime is not None:
                if endtime is None:
                    endtime = _time() + waittime
                else:
                    waittime = endtime - _time()
                    if waittime <= 0:
                        break
            self.wait(waittime)
            result = predicate()
        return result

    def notify(self, n=1):
        if not self._is_owned():
            raise RuntimeError("cannot notify on un-acquired lock")
        __waiters = self._waiters
        waiters = __waiters[:n]
        if not waiters:
            return
        for waiter in waiters:
            waiter.release()
            try:
                __waiters.remove(waiter)
            except ValueError:
                pass

    def notify_all(self):
        self.notify(len(self._waiters))

    notifyAll = notify_all


class Semaphore:

    # After Tim Peters' semaphore class, but not quite the same (no maximum)

    def __init__(self, value=1, use_greenlets=False):
        if value < 0:
            raise ValueError("semaphore initial value must be >= 0")
        if use_greenlets:
            self._cond = Condition(gevent.thread.allocate_lock(), use_greenlets)
        else:
            self._cond = Condition(threading.Lock(), use_greenlets)
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
    def __init__(self, value=1, use_greenlets=False):
        Semaphore.__init__(self, value, use_greenlets)
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
