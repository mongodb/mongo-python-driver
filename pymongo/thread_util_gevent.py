import greenlet
from gevent import Greenlet
from gevent.coros import BoundedSemaphore
from gevent.event import Event
from gevent.local import local
import weakref

from pymongo import thread_util
from pymongo import mongo_replica_set_client


class Ident(thread_util.IdentBase):
    def get(self):
        return id(greenlet.getcurrent())

    def watch(self, callback):
        current = greenlet.getcurrent()
        tid = self.get()

        if hasattr(current, 'link'):
            # This is a Gevent Greenlet (capital G) or eventlet GreenThread,
            # which inherits from greenlet and provides a 'link' method to
            # detect when the Greenlet/GreenThread exits.
            current.link(callback)
            self._refs[tid] = None
        else:
            # This is a non-Gevent greenlet (small g), or it's the main
            # greenlet.
            self._refs[tid] = weakref.ref(current, callback)


class MaxWaitersBoundedSemaphore(thread_util.MaxWaitersBoundedSemaphore):
    def __init__(self, value=1, max_waiters=1):
        thread_util.MaxWaitersBoundedSemaphore.__init__(
            self, BoundedSemaphore, value, max_waiters)


class ReplSetMonitor(mongo_replica_set_client.Monitor, Greenlet):
    """Greenlet based replica set monitor.
    """
    def __init__(self, rsc):
        mongo_replica_set_client.Monitor.__init__(self, rsc, Event)
        Greenlet.__init__(self)

    # Don't override `run` in a Greenlet. Add _run instead.
    # Refer to gevent's Greenlet docs and source for more
    # information.
    def _run(self):
        """Define Greenlet's _run method.
        """
        self.monitor()
