import contextlib
from eventlet.api import timeout as eventlet_timeout, TimeoutError
from eventlet.greenthread import GreenThread as Greenlet
from eventlet.semaphore import BoundedSemaphore


@contextlib.contextmanager
def noopcontext():
    yield


class EventletBoundedSemaphore(BoundedSemaphore):
    def acquire(self, blocking=True, timeout=None):
        if not blocking and timeout is not None:
            raise ValueError("can't specify timeout for non-blocking acquire")
        if timeout is None:
            context = noopcontext()
        else:
            context = eventlet_timeout(timeout)
        try:
            with context:
                return super(EventletBoundedSemaphore, self).acquire(blocking)
        except TimeoutError:
            return False
