import os
import threading
import weakref


class _ForkLock:
    """
    Represents a lock that is tracked upon instantiation using a WeakSet and
    reset by pymongo upon forking.
    """

    _locks: weakref.WeakSet = weakref.WeakSet()  # References to instances of _ForkLock
    _global_lock = threading.Lock()
    _global_locked_counter = 0
    _global_locked_cv = threading.Condition()

    def __init__(self):
        self._lock = threading.Lock()
        _ForkLock._locks.add(self)

    def __getattr__(self, item):
        return getattr(self._lock, item)

    def __enter__(self):
        _ForkLock._global_lock.acquire()
        self._lock.__enter__()
        _ForkLock._global_locked_counter += 1
        _ForkLock._global_lock.release()

    def __exit__(self, exc_type, exc_val, exc_tb):
        _ForkLock._global_lock.acquire()
        self._lock.__exit__(exc_type, exc_val, exc_tb)
        _ForkLock._global_locked_counter -= 1
        with _ForkLock._global_locked_cv:
            _ForkLock._global_locked_cv.notify()
        _ForkLock._global_lock.release()

    @classmethod
    def _release_locks(cls):
        # _ForkLock._global_locked_cv.wait_for(
        #    lambda: _ForkLock._global_locked_counter == 0)
        with _ForkLock._global_lock:
            for lock in cls._locks:
                lock._lock.release()

    @classmethod
    def _acquire_locks(cls):
        # Needs to lock all locks without causing deadlocks elsewhere.
        # So we wait for all locks to be released, and then lock _global_lock
        # to prevent more locks while we fork.

        with _ForkLock._global_locked_cv:
            while not _ForkLock._global_locked_counter == 0:
                _ForkLock._global_locked_cv.wait()
        with _ForkLock._global_lock:
            for lock in cls._locks:
                lock._lock.acquire()
