from gevent import Greenlet
from gevent.coros import BoundedSemaphore

from pymongo import thread_util


class MaxWaitersBoundedSemaphore(thread_util.MaxWaitersBoundedSemaphore):
    def __init__(self, value=1, max_waiters=1):
        thread_util.MaxWaitersBoundedSemaphore.__init__(
            self, BoundedSemaphore, value, max_waiters)
