import inspect
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
    _global_locked_cv = threading.Condition()
    _atomic_counter = 0
    _atomic_counter_lock = threading.Lock()

    def __init__(self):
        self._lock = threading.Lock()
        _ForkLock._locks.add(self)

    def __getattr__(self, item):
        return getattr(self._lock, item)

    def __enter__(self):
        # frame = inspect.getouterframes(inspect.currentframe(), 2)
        # print(
        #    f"{frame[1][:3]}:"
        #    f"{self._lock.locked()} "
        #    f"{self._global_lock.locked()} "
        #    f" {_ForkLock._atomic_counter} "
        # )

        if _ForkLock._atomic_counter > 0:
            self._lock.__enter__()  # Just enter
            with _ForkLock._atomic_counter_lock:
                _ForkLock._atomic_counter += 1
        else:
            with _ForkLock._global_locked_cv:
                while _ForkLock._global_lock.locked():  # About to fork?
                    _ForkLock._global_locked_cv.wait()  # Wait until we finish.
                    # Parent => Will continue as before
                    # Child => Caller will die anyways.
                with _ForkLock._atomic_counter_lock:
                    _ForkLock._atomic_counter += 1
                self._lock.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.__exit__(exc_type, exc_val, exc_tb)
        with _ForkLock._atomic_counter_lock:
            _ForkLock._atomic_counter -= 1

    @classmethod
    def _release_locks(cls):
        # _ForkLock._global_locked_cv.wait_for(
        #    lambda: _ForkLock._global_locked_counter == 0)
        for lock in cls._locks:
            lock._lock.release()
        _ForkLock._global_lock.release()  # Release our lock
        with _ForkLock._global_locked_cv:
            _ForkLock._global_locked_cv.notify_all()  # Notify all waiting threads.

    @classmethod
    def _acquire_locks(cls):
        # Needs to lock all locks without causing deadlocks elsewhere.
        # So we wait for all locks to be released, and then lock _global_lock
        # to prevent more locks while we fork.

        # Issues
        # - Cannot cause deadlocks by grabbing a lock when there are nested
        # locks.
        # - Needs to grab all locks before another thread can.
        # - Needs to deal with fact only calling thread will be alive in
        # child process.
        # - Needs to have minimal performance impact.

        # Solution
        # When forking
        # - Lower a Condition indicating no other locks should be acquired.
        # Other locks may not acquire **unless 1+ is already alive.**
        # - Upon 0 locks being held, lock all of them.
        # - Fork
        # - Unlock all locks.
        # - Release Condition and notify_all.
        # When locking
        # - Is Condition lowered and are no locks active? => Wait and block
        # - Is Condition lowered and are locks active? => Continue (likely a
        # dependent lock)
        # - Is Condition not lowered and are locks active => Continue (not
        # forking)
        # - Atomic increment lock counter. (done first and last to ensure
        # correct state)
        # - Acquire lock.
        # - Critical region (and any nested critical regions)
        # - Release lock.
        # - Decrement lock counter.

        _ForkLock._global_lock.acquire()
        while _ForkLock._atomic_counter > 0:
            pass
        for lock in cls._locks:
            lock._lock.acquire()
