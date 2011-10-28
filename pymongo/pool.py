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

from pymongo.errors import ConnectionFailure

have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False


class Pool(threading.local):
    """A simple connection pool.

    Uses thread-local socket per thread. By calling return_socket() a
    thread can return a socket to the pool.
    """

    # Non thread-locals
    __slots__ = ["pid", "host", "max_size",
                 "net_timeout", "conn_timeout", "use_ssl", "sockets"]

    # thread-local default
    sock = None

    def __init__(self, host, max_size, net_timeout, conn_timeout, use_ssl):
        self.pid = os.getpid()
        self.host = host
        self.max_size = max_size
        self.net_timeout = net_timeout
        self.conn_timeout = conn_timeout
        self.use_ssl = use_ssl
        if not hasattr(self, "sockets"):
            self.sockets = []

    def connect(self):
        """Connect to Mongo and return a new (connected) socket.
        """
        try:
            # Prefer IPv4. If there is demand for an option
            # to specify one or the other we can add it later.
            s = socket.socket(socket.AF_INET)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(self.conn_timeout or 20.0)
            s.connect(self.host)
        except socket.gaierror:
            # If that fails try IPv6
            s = socket.socket(socket.AF_INET6)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(self.conn_timeout or 20.0)
            s.connect(self.host)

        if self.use_ssl:
            try:
                s = ssl.wrap_socket(s)
            except ssl.SSLError:
                s.close()
                raise ConnectionFailure("SSL handshake failed. MongoDB may "
                                        "not be configured with SSL support.")

        s.settimeout(self.net_timeout)

        return s

    def get_socket(self):
        """Get a socket from the pool. Returns a new socket if the
        pool is empty.
        """
        # We use the pid here to avoid issues with fork / multiprocessing.
        # See test.test_connection:TestConnection.test_fork for an example of
        # what could go wrong otherwise
        pid = os.getpid()

        if pid != self.pid:
            self.sock = None
            self.sockets = []
            self.pid = pid

        if self.sock is not None and self.sock[0] == pid:
            return (self.sock[1], self.sock[2])

        try:
            sock, auth = self.sockets.pop()
            self.sock = (pid, sock, auth)
        except IndexError:
            self.sock = (pid, self.connect(), set())
        return (self.sock[1], self.sock[2])

    def discard_socket(self):
        """Close and discard the active socket.
        """
        if self.sock:
            self.sock[1].close()
            self.sock = None

    def return_socket(self):
        """Return the socket currently in use to the pool. If the
        pool is full the socket will be discarded.
        """
        if self.sock is not None and self.sock[0] == os.getpid():
            # There's a race condition here, but we deliberately
            # ignore it.  It means that if the pool_size is 10 we
            # might actually keep slightly more than that.
            if len(self.sockets) < self.max_size:
                self.sockets.append((self.sock[1], self.sock[2]))
            else:
                self.sock[1].close()
        self.sock = None
