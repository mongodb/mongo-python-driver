import threading
import weakref


class MongoClientLock:
    """
    Represents a lock that can be tracked with a single instance of
    """

    _locks = weakref.WeakSet()  # References to instances of MongoClientLock

    def __init__(self):
        self.__lock = threading.Lock()
        MongoClientLock._locks.add(self)

    def __getattr__(self, item):
        return getattr(self.__lock, item)

    def __enter__(self):
        self.__lock.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__lock.__exit__(exc_type, exc_val, exc_tb)

    @classmethod
    def _reset_locks(cls):
        for lock in cls._locks:
            lock.__lock = threading.Lock()
