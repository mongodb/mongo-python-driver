import threading
try:
    from time import monotonic as _time
except ImportError:
    from time import time as _time

from pymongo import thread_util


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
