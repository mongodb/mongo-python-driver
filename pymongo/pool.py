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
import threading

from pymongo.errors import ConnectionFailure, OperationFailure


have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False


NO_REQUEST    =  None
NO_SOCKET_YET = -1


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
        self._reset()
        self.pair = pair
        self.max_size = max_size
        self.net_timeout = net_timeout
        self.conn_timeout = conn_timeout
        self.use_ssl = use_ssl

    def _reset(self):
        self.pid = os.getpid()
        self.sockets = set()

        # Map socket -> set of (host, port, database) triples for which this
        # socket has been authorized
        self.authsets = {}

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
        return s

    def get_socket(self, pair=None):
        """Get a socket from the pool. Returns a new socket if the pool is
        empty.

        Returns (sock, from_pool), a connected :class:`socket.socket` and a bool
        saying whether the socket was from the pool or freshly created.

        :Parameters:
          - `pair`: optional (hostname, port) tuple
        """
        self._check_pair_arg(pair)

        # We use the pid here to avoid issues with fork / multiprocessing.
        # See test.test_connection:TestConnection.test_fork for an example of
        # what could go wrong otherwise
        if self.pid != os.getpid():
            self._reset()

        # Have we opened a socket for this request?
        sock = self._get_request_socket()
        if sock not in (NO_SOCKET_YET, NO_REQUEST):
            return sock, True

        # We're not in a request, just get any free socket or create one
        try:
            sock, from_pool = self.sockets.pop(), True
        except KeyError:
            sock, from_pool = self.connect(pair), False

        if self._get_request_socket() == NO_SOCKET_YET:
            # start_request has been called but we haven't assigned a socket to
            # the request yet. Let's use this socket for this request until
            # end_request.
            self._set_request_socket(sock)

        return sock, from_pool

    def start_request(self):
        if self._get_request_socket() is NO_REQUEST:
            # Add a placeholder value so we know we're in a request, but we
            # have no socket assigned to the request yet.
            self._set_request_socket(NO_SOCKET_YET)

    def end_request(self):
        sock = self._get_request_socket()
        self._set_request_socket(NO_REQUEST)
        self.return_socket(sock)

    def discard_socket(self, sock):
        """Close and discard the active socket.
        """
        if sock:
            if sock in self.authsets:
                del self.authsets[sock]

            if sock == self._get_request_socket():
                # We're ending the request
                self._set_request_socket(NO_REQUEST)

            sock.close()

    def return_socket(self, sock):
        """Return the socket currently in use to the pool. If the
        pool is full the socket will be discarded.
        """
        if self.pid != os.getpid():
            self._reset()
        elif sock:
            # Don't really return a socket if we're in a request
            if sock is not self._get_request_socket():
                # There's a race condition here, but we deliberately
                # ignore it.  It means that if the pool_size is 10 we
                # might actually keep slightly more than that.
                if len(self.sockets) < self.max_size:
                    self.sockets.add(sock)
                else:
                    self.discard_socket(sock)

    def get_authset(self, sock):
        """Set of database names for which this socket has been authenticated
        """
        return self.authsets.setdefault(sock, set())

    # Overridable methods for Pools. These methods must simply set and get an
    # arbitrary value associated with the execution context (thread, greenlet,
    # Tornado StackContext, ...) in which we want to use a single socket.
    def _set_request_socket(self, sock):
        raise NotImplementedError

    def _get_request_socket(self):
        raise NotImplementedError


class Pool(BasePool):
    """A simple connection pool.

    Calling start_request() acquires a thread-local socket, which is returned
    to the pool when the thread calls end_request() or dies.
    """
    def __init__(self, *args, **kwargs):
        self.thread_local = threading.local()
        super(Pool, self).__init__(*args, **kwargs)

    def _reset(self):
        # End request if we're in one
        self.thread_local.sock = NO_REQUEST
        super(Pool, self)._reset()

    def _set_request_socket(self, sock):
        self.thread_local.sock = sock

    def _get_request_socket(self):
        return getattr(self.thread_local, 'sock', NO_REQUEST)


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
