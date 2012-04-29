# Copyright 2011-2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

import os
import socket
import sys
import time
import threading
import weakref

from pymongo.errors import ConnectionFailure


have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False


# PyMongo does not use greenlet-aware connection pools by default, but it will
# attempt to do so if you pass use_greenlets=True to Connection or
# ReplicaSetConnection
have_greenlet = True
try:
    import greenlet
except ImportError:
    have_greenlet = False


NO_REQUEST    = None
NO_SOCKET_YET = -1


if sys.platform.startswith('java'):
    from select import cpython_compatible_select as select
else:
    from select import select


def _closed(sock):
    """Return True if we know socket has been closed, False otherwise.
    """
    try:
        rd, _, _ = select([sock], [], [], 0)
    # Any exception here is equally bad (select.error, ValueError, etc.).
    except:
        return True
    return len(rd) > 0


class SocketInfo(object):
    """Store a socket with some metadata
    """
    def __init__(self, sock, poolref):
        self.sock = sock

        # We can't strongly reference the Pool, because the Pool
        # references this SocketInfo as long as it's in pool
        self.poolref = poolref

        self.authset = set()
        self.closed = False
        self.last_checkout = time.time()
        self.pool_id = poolref().pool_id

    def close(self):
        self.closed = True
        # Avoid exceptions on interpreter shutdown.
        try:
            self.sock.close()
        except:
            pass

    def __del__(self):
        if not self.closed:
            # This socket was given out, but not explicitly returned. Perhaps
            # the socket was assigned to a thread local for a request, but the
            # request wasn't ended before the thread died. Reclaim the socket
            # for the pool.
            pool = self.poolref()
            if pool:
                # Return a copy of self rather than self -- the Python docs
                # discourage postponing deletion by adding a reference to self.
                copy = SocketInfo(self.sock, self.poolref)
                copy.authset = self.authset
                pool.return_socket(copy)
            else:
                # Close socket now rather than awaiting garbage collector
                self.close()

    def __eq__(self, other):
        return hasattr(other, 'sock') and self.sock == other.sock

    def __hash__(self):
        return hash(self.sock)

    def __repr__(self):
        return "SocketInfo(%s, %s)%s at %s" % (
            repr(self.sock), repr(self.poolref()),
            self.closed and " CLOSED" or "",
            id(self)
        )


class BasePool(object):
    def __init__(self, pair, max_size, net_timeout, conn_timeout, use_ssl):
        """
        :Parameters:
          - `pair`: a (hostname, port) tuple
          - `max_size`: approximate number of idle connections to keep open
          - `net_timeout`: timeout in seconds for operations on open connection
          - `conn_timeout`: timeout in seconds for establishing connection
          - `use_ssl`: bool, if True use an encrypted connection
        """
        self.sockets = set()
        self.lock = threading.Lock()

        # Keep track of resets, so we notice sockets created before the most
        # recent reset and close them.
        self.pool_id = 0
        self.pid = os.getpid()
        self.pair = pair
        self.max_size = max_size
        self.net_timeout = net_timeout
        self.conn_timeout = conn_timeout
        self.use_ssl = use_ssl

    def reset(self):
        # Ignore this race condition -- if many threads are resetting at once,
        # the pool_id will definitely change, which is all we care about.
        self.pool_id += 1

        request_state = self._get_request_state()
        self.pid = os.getpid()

        # Close sockets before deleting them, otherwise they'll come
        # running back.
        if request_state not in (NO_REQUEST, NO_SOCKET_YET):
            # request_state is a SocketInfo for this request
            request_state.close()

        sockets = None
        try:
            # Swapping variables is not atomic. We need to ensure no other
            # thread is modifying self.sockets, or replacing it, in this
            # critical section.
            self.lock.acquire()
            sockets, self.sockets = self.sockets, set()
        finally:
            self.lock.release()

        for sock_info in sockets: sock_info.close()

        # Reset subclass's data structures
        self._reset()

        # If we were in a request before the reset, then delete the request
        # socket, but resume the request with a new socket the next time
        # get_socket() is called.
        if request_state != NO_REQUEST:
            self._set_request_state(NO_SOCKET_YET)

    def create_connection(self, pair):
        """Connect to *pair* and return the socket object.

        This is a modified version of create_connection from
        CPython >=2.6.
        """
        # Don't try IPv6 if we don't support it.
        family = socket.AF_INET
        if socket.has_ipv6:
            family = socket.AF_UNSPEC

        host, port = pair or self.pair
        err = None
        for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
            af, socktype, proto, dummy, sa = res
            sock = None
            try:
                sock = socket.socket(af, socktype, proto)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.settimeout(self.conn_timeout or 20.0)
                sock.connect(sa)
                return sock
            except socket.error, e:
                err = e
                if sock is not None:
                    sock.close()

        if err is not None:
            raise err
        else:
            # This likely means we tried to connect to an IPv6 only
            # host with an OS/kernel or Python interpeter that doesn't
            # support IPv6. The test case is Jython2.5.1 which doesn't
            # support IPv6 at all.
            raise socket.error('getaddrinfo failed')

    def connect(self, pair):
        """Connect to Mongo and return a new (connected) socket. Note that the
           pool does not keep a reference to the socket -- you must call
           return_socket() when you're done with it.
        """
        sock = self.create_connection(pair)

        if self.use_ssl:
            try:
                sock = ssl.wrap_socket(sock)
            except ssl.SSLError:
                sock.close()
                raise ConnectionFailure("SSL handshake failed. MongoDB may "
                                        "not be configured with SSL support.")

        sock.settimeout(self.net_timeout)
        return SocketInfo(sock, weakref.ref(self))

    def get_socket(self, pair=None):
        """Get a socket from the pool.

        Returns a :class:`SocketInfo` object wrapping a connected
        :class:`socket.socket`, and a bool saying whether the socket was from
        the pool or freshly created.

        :Parameters:
          - `pair`: optional (hostname, port) tuple
        """
        # We use the pid here to avoid issues with fork / multiprocessing.
        # See test.test_connection:TestConnection.test_fork for an example of
        # what could go wrong otherwise
        if self.pid != os.getpid():
            self.reset()

        # Have we opened a socket for this request?
        req_state = self._get_request_state()
        if req_state not in (NO_SOCKET_YET, NO_REQUEST):
            # There's a socket for this request, check it and return it
            checked_sock = self._check(req_state, pair)
            if checked_sock != req_state:
                self._set_request_state(checked_sock)

            checked_sock.last_checkout = time.time()
            return checked_sock

        # We're not in a request, just get any free socket or create one
        sock_info, from_pool = None, None
        try:
            try:
                # set.pop() isn't atomic in Jython, see
                # http://bugs.jython.org/issue1854
                self.lock.acquire()
                sock_info, from_pool = self.sockets.pop(), True
            finally:
                self.lock.release()
        except KeyError:
            sock_info, from_pool = self.connect(pair), False

        if from_pool:
            sock_info = self._check(sock_info, pair)

        if req_state == NO_SOCKET_YET:
            # start_request has been called but we haven't assigned a socket to
            # the request yet. Let's use this socket for this request until
            # end_request.
            self._set_request_state(sock_info)

        sock_info.last_checkout = time.time()
        return sock_info

    def start_request(self):
        if self._get_request_state() == NO_REQUEST:
            # Add a placeholder value so we know we're in a request, but we
            # have no socket assigned to the request yet.
            self._set_request_state(NO_SOCKET_YET)

    def in_request(self):
        return self._get_request_state() != NO_REQUEST

    def end_request(self):
        sock_info = self._get_request_state()
        self._set_request_state(NO_REQUEST)
        self.return_socket(sock_info)

    def discard_socket(self, sock_info):
        """Close and discard the active socket.
        """
        if sock_info:
            sock_info.close()

            if sock_info == self._get_request_state():
                self._set_request_state(NO_SOCKET_YET)

    def return_socket(self, sock_info):
        """Return the socket currently in use to the pool. If the
        pool is full the socket will be discarded.
        """
        if self.pid != os.getpid():
            self.reset()
        elif sock_info not in (NO_REQUEST, NO_SOCKET_YET):
            if sock_info.closed:
                return

            if sock_info != self._get_request_state():
                added = False
                try:
                    self.lock.acquire()
                    if len(self.sockets) < self.max_size:
                        self.sockets.add(sock_info)
                        added = True
                finally:
                    self.lock.release()

                if not added:
                    self.discard_socket(sock_info)

    def _check(self, sock_info, pair):
        """This side-effecty function checks if this pool has been reset since
        the last time this socket was used, or if the socket has been closed by
        some external network error if it's been > 1 second since the last time
        we used it, and if so, attempts to create a new socket. If this
        connection attempt fails we reset the pool and reraise the error.

        Checking sockets lets us avoid seeing *some*
        :class:`~pymongo.errors.AutoReconnect` exceptions on server
        hiccups, etc. We only do this if it's been > 1 second since
        the last socket checkout, to keep performance reasonable - we
        can't avoid AutoReconnects completely anyway.
        """
        error = False

        if self.pool_id != sock_info.pool_id:
            self.discard_socket(sock_info)
            error = True

        elif time.time() - sock_info.last_checkout > 1:
            if _closed(sock_info.sock):
                self.discard_socket(sock_info)
                error = True

        if not error:
            return sock_info
        else:
            try:
                return self.connect(pair)
            except socket.error:
                self.reset()
                raise

    # Overridable methods for Pools. These methods must simply set and get an
    # arbitrary value associated with the execution context (thread, greenlet,
    # Tornado StackContext, ...) in which we want to use a single socket.
    def _set_request_state(self, sock_info):
        raise NotImplementedError

    def _get_request_state(self):
        raise NotImplementedError

    def _reset(self):
        pass


# This thread-local will hold a Pool's per-thread request state. sock_info
# defaults to NO_REQUEST each time it's accessed from a new thread. It's
# much simpler to make a separate thread-local class rather than having Pool
# inherit both from BasePool and threading.local.
class _Local(threading.local):
    sock_info = NO_REQUEST


class Pool(BasePool):
    """A simple connection pool.

    Calling start_request() acquires a thread-local socket, which is returned
    to the pool when the thread calls end_request() or dies.
    """
    def __init__(self, *args, **kwargs):
        self.local = _Local()
        super(Pool, self).__init__(*args, **kwargs)

    def _set_request_state(self, sock_info):
        self.local.sock_info = sock_info

    def _get_request_state(self):
        return self.local.sock_info

    def _reset(self):
        self.local.sock_info = NO_REQUEST


class GreenletPool(BasePool):
    """A simple connection pool.

    Calling start_request() acquires a greenlet-local socket, which is returned
    to the pool when the greenlet calls end_request() or dies.
    """
    def __init__(self, *args, **kwargs):
        self._gr_id_to_sock = {}

        # Weakrefs to non-Gevent greenlets
        self._refs = {}
        super(GreenletPool, self).__init__(*args, **kwargs)

    # Overrides
    def _set_request_state(self, sock_info):
        current = greenlet.getcurrent()
        gr_id = id(current)

        if sock_info == NO_REQUEST:
            self._refs.pop(gr_id, None)
            self._gr_id_to_sock.pop(gr_id, None)
        else:
            self._gr_id_to_sock[gr_id] = sock_info

            def delete_callback(dummy):
                # End the request
                self._refs.pop(gr_id, None)
                request_sock = self._gr_id_to_sock.pop(gr_id, None)
                self.return_socket(request_sock)

            if gr_id not in self._refs:
                if hasattr(current, 'link'):
                    # This is a Gevent Greenlet (capital G), which inherits from
                    # greenlet and provides a 'link' method to detect when the
                    # Greenlet exits
                    current.link(delete_callback)
                    self._refs[gr_id] = None
                else:
                    # This is a non-Gevent greenlet (small g), or it's the main
                    # greenlet. Since there's no link() method, we use a weakref
                    # to detect when the greenlet is garbage-collected. Garbage-
                    # collection is a later-firing and less reliable event than
                    # Greenlet.link() so we prefer link() if available.
                    self._refs[gr_id] = weakref.ref(current, delete_callback)

    def _get_request_state(self):
        gr_id = id(greenlet.getcurrent())
        return self._gr_id_to_sock.get(gr_id, NO_REQUEST)

    def _reset(self):
        self._gr_id_to_sock.clear()
        self._refs.clear()


class Request(object):
    """
    A context manager returned by Connection.start_request(), so you can do
    `with connection.start_request(): do_something()` in Python 2.5+.
    """
    def __init__(self, connection):
        self.connection = connection

    def end(self):
        self.connection.end_request()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end()
        # Returning False means, "Don't suppress exceptions if any were
        # thrown within the block"
        return False
