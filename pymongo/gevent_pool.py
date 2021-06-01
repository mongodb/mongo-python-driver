
import os
import threading

from pymongo.monitoring import (ConnectionCheckOutFailedReason,
                                ConnectionClosedReason)
from pymongo.pool import Pool, _PoolClosedError


class GeventPool(Pool):
    """
    The regular pool's exception handler may be interrupted by gevent.Timeout
    This runs the exception handling on a new greenlet, which won't be interrupted.
    I use the threading library, as gevent automatically patches it, so that
    pymongo does not need to introduce a dependency onto gevent.
    """
    def _safe_destruct(self, sock_info, incremented):
        """
        Place this into it's own greenlet, so it can't be interrupted
        """
        try:
            if sock_info:
                # We checked out a socket but authentication failed.
                sock_info.close_socket(ConnectionClosedReason.ERROR)
            self._socket_semaphore.release()

            if incremented:
                with self.lock:
                    self.active_sockets -= 1

            if self.enabled_for_cmap:
                self.opts.event_listeners.publish_connection_check_out_failed(
                    self.address, ConnectionCheckOutFailedReason.CONN_ERROR)
        except:
            exlog("Safe destruction of socket failed, pool may have leaked")

    def _get_socket(self, all_credentials):
        """Get or create a SocketInfo. Can raise ConnectionFailure."""
        # We use the pid here to avoid issues with fork / multiprocessing.
        # See test.test_client:TestClient.test_fork for an example of
        # what could go wrong otherwise
        if self.pid != os.getpid():
            self.reset()

        if self.closed:
            if self.enabled_for_cmap:
                self.opts.event_listeners.publish_connection_check_out_failed(
                    self.address, ConnectionCheckOutFailedReason.POOL_CLOSED)
            raise _PoolClosedError(
                'Attempted to check out a connection from closed connection '
                'pool')

        # Get a free socket or create one.
        if not self._socket_semaphore.acquire(
                True, self.opts.wait_queue_timeout):
            self._raise_wait_queue_timeout()

        # We've now acquired the semaphore and must release it on error.
        sock_info = None
        incremented = False
        try:
            with self.lock:
                self.active_sockets += 1
                incremented = True
            while sock_info is None:
                try:
                    with self.lock:
                        sock_info = self.sockets.popleft()
                except IndexError:
                    # Can raise ConnectionFailure or CertificateError.
                    sock_info = self.connect(all_credentials)
                else:
                    if self._perished(sock_info):
                        sock_info = None
            sock_info.check_auth(all_credentials)
        except:
            threading.Thread(target=GeventPool._safe_destruct, args=(self, sock_info, incremented)).start()
            raise

        return sock_info