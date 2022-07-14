import platform
import threading
import weakref


class _ForkLock:
    """
    Represents a lock that is tracked upon instantiation using a WeakSet and
    reset by pymongo upon forking.
    """

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

    if platform.python_implementation() == "CPython":
        _locks: weakref.WeakSet = weakref.WeakSet()  # References to instances of _ForkLock
        _global_lock = threading.Lock()  # Used for indicating we're about to fork.
        _global_locked_cv = threading.Condition()  # Used for notifying locks.

        # Atomic counter for locked locks.
        _atomic_counter = 0
        _atomic_counter_lock = threading.Lock()

        _insertion_lock = threading.Lock()

        def __init__(self):
            self._lock = threading.Lock()
            with _ForkLock._insertion_lock:
                _ForkLock._locks.add(self)

        def __getattr__(self, item):
            return getattr(self._lock, item)

        def __enter__(self):
            # Decide whether we're about to fork.
            if _ForkLock._atomic_counter > 0:  # Possible dependent lock, keep going
                with _ForkLock._atomic_counter_lock:  # Increment first.
                    _ForkLock._atomic_counter += 1
                self._lock.__enter__()
            else:  # Possible we might fork, there's no other locks.
                with _ForkLock._global_locked_cv:
                    while _ForkLock._global_lock.locked():  # About to fork?
                        _ForkLock._global_locked_cv.wait()  # Wait until we finish.
                        # Parent => Will continue as before
                        # Child => Caller will die anyways.
                    with _ForkLock._atomic_counter_lock:  # Increment first.
                        _ForkLock._atomic_counter += 1
                    self._lock.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self._lock.__exit__(exc_type, exc_val, exc_tb)
            with _ForkLock._atomic_counter_lock:  # Make sure the counter is updated
                _ForkLock._atomic_counter -= 1

        @classmethod
        def _release_locks(cls):
            # Completed the fork, reset all the locks.
            for lock in cls._locks:
                lock._lock.release()
            _ForkLock._global_lock.release()  # Release our lock
            with _ForkLock._global_locked_cv:
                _ForkLock._global_locked_cv.notify_all()
                # Notify all waiting threads in the parent.
                # In the child, there should be no waiting threads as only the
                # calling thread gets forked.

        @classmethod
        def _acquire_locks(cls):
            _ForkLock._global_lock.acquire()  # Indicate we're about to fork.
            while _ForkLock._atomic_counter > 0:
                pass  # Spin while the other threads finish up.
            for lock in cls._locks:  # Acquire them all.
                lock._lock.acquire()
            # From here, we can fork.

    else:
        # Simple wrapper.
        def __init__(self):
            self._lock = threading.Lock()

        def __getattr__(self, item):
            return getattr(self._lock, item)

        def __enter__(self):
            self._lock.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self._lock.__exit__(exc_type, exc_val, exc_tb)

        @classmethod
        def _release_locks(cls):
            pass

        @classmethod
        def _acquire_locks(cls):
            pass
