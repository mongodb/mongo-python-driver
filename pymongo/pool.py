# Copyright 2009-2011 10gen, Inc.
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
import thread
import threading

from pymongo.errors import ConnectionFailure, OperationFailure
import weakref


have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False


NO_REQUEST    = None
NO_SOCKET_YET = -1


class SocketInfo(object):
    """Store a socket with some metadata
    """
    def __init__(self, sock):
        self.sock = sock
        self.authset = set()


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
        self.reset()
        self.pair = pair
        self.max_size = max_size
        self.net_timeout = net_timeout
        self.conn_timeout = conn_timeout
        self.use_ssl = use_ssl

    def reset(self):
        in_request = self.in_request()
        self.pid = os.getpid()
        self.sockets = set()

        # Reset subclass's data structures
        self._reset()

        # If we were in a request before the reset, then delete the request
        # socket, but resume the request with a new socket the next time
        # get_socket() is called.
        if in_request:
            self._set_request_socket(NO_SOCKET_YET)

    def _check_pair_arg(self, pair):
        if not (self.pair or pair):
            raise OperationFailure("Must configure host and port on Pool")

        if self.pair and pair:
            raise OperationFailure(
                "Attempt to specify pair %s on Pool already configured with "
                "pair %s" % (pair, self.pair)
            )

    def connect(self, pair):
        """Connect to Mongo and return a new (connected) socket. Note that the
           pool does not keep a reference to the socket -- you must call
           return_socket() when you're done with it.
        """
        self._check_pair_arg(pair)

        # Prefer IPv4. If there is demand for an option
        # to specify one or the other we can add it later.
        socket_types = (socket.AF_INET, socket.AF_INET6)
        for socket_type in socket_types:
            try:
                s = socket.socket(socket_type)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                s.settimeout(self.conn_timeout or 20.0)
                s.connect(self.pair or pair)
                break
            except socket.gaierror:
                # If that fails try IPv6
                continue
        else:
            # None of the socket types worked
            raise

        if self.use_ssl:
            try:
                s = ssl.wrap_socket(s)
            except ssl.SSLError:
                s.close()
                raise ConnectionFailure("SSL handshake failed. MongoDB may "
                                        "not be configured with SSL support.")

        s.settimeout(self.net_timeout)
        return SocketInfo(s)

    def get_socket(self, pair=None):
        """Get a socket from the pool.

        Returns a (socket_info, from_pool): a SocketInfo object wrapping a
        connected :class:`socket.socket`, and a bool saying whether the socket
        was from the pool or freshly created.

        :Parameters:
          - `pair`: optional (hostname, port) tuple
        """
        self._check_pair_arg(pair)

        # We use the pid here to avoid issues with fork / multiprocessing.
        # See test.test_connection:TestConnection.test_fork for an example of
        # what could go wrong otherwise
        if self.pid != os.getpid():
            self.reset()

        # Have we opened a socket for this request?
        sock_info = self._get_request_socket()
        if sock_info not in (NO_SOCKET_YET, NO_REQUEST):
            return sock_info, True

        # We're not in a request, just get any free socket or create one
        try:
            sock_info, from_pool = self.sockets.pop(), True
        except KeyError:
            sock_info, from_pool = self.connect(pair), False

        if self._get_request_socket() == NO_SOCKET_YET:
            # start_request has been called but we haven't assigned a socket to
            # the request yet. Let's use this socket for this request until
            # end_request.
            self._set_request_socket(sock_info)

        return sock_info, from_pool

    def start_request(self):
        if self._get_request_socket() == NO_REQUEST:
            # Add a placeholder value so we know we're in a request, but we
            # have no socket assigned to the request yet.
            self._set_request_socket(NO_SOCKET_YET)

    def in_request(self):
        return self._get_request_socket() != NO_REQUEST

    def end_request(self):
        sock = self._get_request_socket()
        self._set_request_socket(NO_REQUEST)
        self.return_socket(sock)

    def discard_socket(self, sock_info):
        """Close and discard the active socket.
        """
        if sock_info:
            if sock_info == self._get_request_socket():
                # We're ending the request
                self._set_request_socket(NO_REQUEST)

            sock_info.sock.close()

    def return_socket(self, sock_info):
        """Return the socket currently in use to the pool. If the
        pool is full the socket will be discarded.
        """
        if self.pid != os.getpid():
            self.reset()
        elif sock_info:
            # Don't really return a socket if we're in a request
            if sock_info is not self._get_request_socket():
                # There's a race condition here, but we deliberately
                # ignore it.  It means that if the pool_size is 10 we
                # might actually keep slightly more than that.
                if len(self.sockets) < self.max_size:
                    self.sockets.add(sock_info)
                else:
                    self.discard_socket(sock_info)

    # Overridable methods for Pools. These methods must simply set and get an
    # arbitrary value associated with the execution context (thread, greenlet,
    # Tornado StackContext, ...) in which we want to use a single socket.
    def _set_request_socket(self, sock_info):
        raise NotImplementedError

    def _get_request_socket(self):
        raise NotImplementedError

    def _reset(self):
        raise NotImplementedError


def try_pop(dct, key):
    try:
        return dct.pop(key)
    except KeyError:
        return None


class Pool(BasePool):
    """A simple connection pool.

    Calling start_request() acquires a thread-local socket, which is returned
    to the pool when the thread calls end_request() or dies.
    """
    def __init__(self, *args, **kwargs):
        self.tid_to_socket = {}
        self.threadref_to_tid = {}
        super(Pool, self).__init__(*args, **kwargs)

    def _set_request_socket(self, sock_info):
        tid = thread.get_ident()
        self.tid_to_socket[tid] = sock_info

        if isinstance(sock_info, SocketInfo):
            # We're binding a socket to a thread - this thread will use this
            # socket until it calls end_request() or dies.  We need some way to
            # know if this thread dies but doesn't call end_request(), so we
            # can return this socket to the pool.  Make a weakref to the Thread
            # object and set the weakref's callback to self.on_thread_dying.
            current_thread = threading.currentThread()
            threadref = weakref.ref(current_thread, self.on_thread_dying)
            self.threadref_to_tid[threadref] = tid
        else:
            try_pop(self.threadref_to_tid, tid)

    def _get_request_socket(self):
        tid = thread.get_ident()
        return self.tid_to_socket.get(tid, NO_REQUEST)

    def _reset(self):
        self.tid_to_socket = {}
        self.threadref_to_tid = {}

    def on_thread_dying(self, threadref):
        # We're probably on the main thread here, not the thread to which
        # threadref refers. Get its thread_id.
        tid = try_pop(self.threadref_to_tid, threadref)
        if tid is None:
            # This thread didn't call start_request without a matching
            # end_request.
            return

        # End the request
        request_sock = try_pop(self.tid_to_socket, tid)
        if request_sock:
            self.return_socket(request_sock)


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
