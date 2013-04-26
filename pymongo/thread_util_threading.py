import sys
import threading
from threading import local
try:
    from time import monotonic as _time
except ImportError:
    from time import time as _time
import weakref

from pymongo import thread_util
from pymongo import mongo_replica_set_client


# Do we have to work around http://bugs.python.org/issue1868?
issue1868 = (sys.version_info[:3] <= (2, 7, 0))


class Ident(thread_util.IdentBase):
    class _DummyLock(object):
        def acquire(self):
            pass

        def release(self):
            pass

    def __init__(self):
        super(Ident, self).__init__()
        self._local = threading.local()
        if issue1868:
            self._lock = threading.Lock()
        else:
            self._lock = Ident._DummyLock()

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
                self._local.vigil = vigil = Ident.ThreadVigil()
        finally:
            self._lock.release()

        return vigil

    def get(self):
        return id(self._make_vigil())

    def watch(self, callback):
        vigil = self._make_vigil()
        self._refs[id(vigil)] = weakref.ref(vigil, callback)

    def watching(self):
        """Is the current thread being watched for death?"""
        tid = self.get()
        if tid not in self._refs:
            return False
        # Check that the weakref is active, if not the thread has died
        # This fixes the case where a thread id gets reused
        return self._refs[tid]()


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


class MaxWaitersBoundedSemaphore(thread_util.MaxWaitersBoundedSemaphore):
    def __init__(self, value=1, max_waiters=1):
        thread_util.MaxWaitersBoundedSemaphore.__init__(
            self, BoundedSemaphore, value, max_waiters)


class ReplSetMonitor(mongo_replica_set_client.Monitor, threading.Thread):
    """Thread based replica set monitor.
    """
    def __init__(self, rsc):
        mongo_replica_set_client.Monitor.__init__(self, rsc, threading.Event)
        threading.Thread.__init__(self)
        self.setName("ReplicaSetMonitorThread")
        self.setDaemon(True)

    def run(self):
        """Override Thread's run method.
        """
        self.monitor()
