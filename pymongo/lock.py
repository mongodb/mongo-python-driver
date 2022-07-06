import threading
import weakref


class MongoClientLock:
    """
    Represents a lock that is tracked upon instantiation using a WeakSet and
    reset by pymongo upon forking.
    """

    _locks: weakref.WeakSet = weakref.WeakSet()  # References to instances of MongoClientLock

    def __init__(self):
        self._lock = threading.Lock()
        MongoClientLock._locks.add(self)

    def __getattr__(self, item):
        return getattr(self._lock, item)

    def __enter__(self):
        self._lock.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._lock.locked():
            self._lock.__exit__(exc_type, exc_val, exc_tb)

    @classmethod
    def _reset_locks(cls):
        for lock in cls._locks:
            lock._lock = threading.Lock()
