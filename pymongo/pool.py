# Copyright 2011-2014 MongoDB, Inc.
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

import contextlib
import os
import socket
import threading

from bson.py3compat import u, itervalues
from pymongo import auth, thread_util
from pymongo.errors import ConnectionFailure
from pymongo.ismaster import IsMaster
from pymongo.monotonic import time as _time
from pymongo.network import (command,
                             receive_message,
                             socket_closed)


# If the first getaddrinfo call of this interpreter's life is on a thread,
# while the main thread holds the import lock, getaddrinfo deadlocks trying
# to import the IDNA codec. Import it here, where presumably we're on the
# main thread, to avoid the deadlock. See PYTHON-607.
u('foo').encode('idna')

try:
    from ssl import match_hostname, CertificateError
except ImportError:
    # These don't require the ssl module
    from pymongo.ssl_match_hostname import match_hostname, CertificateError


class PoolOptions(object):

    __slots__ = ('__max_pool_size', '__connect_timeout', '__socket_timeout',
                 '__wait_queue_timeout', '__wait_queue_multiple',
                 '__ssl_context', '__ssl_match_hostname', '__socket_keepalive')

    def __init__(self, max_pool_size=100, connect_timeout=None,
                 socket_timeout=None, wait_queue_timeout=None,
                 wait_queue_multiple=None, ssl_context=None,
                 ssl_match_hostname=True, socket_keepalive=False):

        self.__max_pool_size = max_pool_size
        self.__connect_timeout = connect_timeout
        self.__socket_timeout = socket_timeout
        self.__wait_queue_timeout = wait_queue_timeout
        self.__wait_queue_multiple = wait_queue_multiple
        self.__ssl_context = ssl_context
        self.__ssl_match_hostname = ssl_match_hostname
        self.__socket_keepalive = socket_keepalive

    @property
    def max_pool_size(self):
        """The maximum number of connections that the pool will open
        simultaneously. If this is set, operations will block if there
        are `max_pool_size` outstanding connections.
        """
        return self.__max_pool_size

    @property
    def connect_timeout(self):
        """How long a connection can take to be opened before timing out.
        """
        return self.__connect_timeout

    @property
    def socket_timeout(self):
        """How long a send or receive on a socket can take before timing out.
        """
        return self.__socket_timeout

    @property
    def wait_queue_timeout(self):
        """How long a thread will wait for a socket from the pool if the pool
        has no free sockets.
        """
        return self.__wait_queue_timeout

    @property
    def wait_queue_multiple(self):
        """Multiplied by max_pool_size to give the number of threads allowed
        to wait for a socket at one time.
        """
        return self.__wait_queue_multiple

    @property
    def ssl_context(self):
        """An SSLContext instance or None.
        """
        return self.__ssl_context

    @property
    def ssl_match_hostname(self):
        """Call ssl.match_hostname if cert_reqs is not ssl.CERT_NONE.
        """
        return self.__ssl_match_hostname

    @property
    def socket_keepalive(self):
        """Whether to send periodic messages to determine if a connection
        is closed.
        """
        return self.__socket_keepalive


class SocketInfo(object):
    """Store a socket with some metadata.

    :Parameters:
      - `sock`: a raw socket object
      - `pool`: a Pool instance
      - `ismaster`: an IsMaster instance, response to ismaster call on `sock`
      - `host`: a string, the server's hostname (without port)
    """
    def __init__(self, sock, pool, ismaster, host):
        self.sock = sock
        self.host = host
        self.authset = set()
        self.closed = False
        self.last_checkout = _time()
        self.ismaster = ismaster

        # The pool's pool_id changes with each reset() so we can close sockets
        # created before the last reset.
        self.pool_id = pool.pool_id

    def command(self, dbname, spec):
        """Execute a command or raise socket.error or OperationFailure.

        :Parameters:
          - `dbname`: name of the database on which to run the command
          - `spec`: a command document as a dict, SON, or mapping object
        """
        try:
            return command(self.sock, dbname, spec)
        except socket.error:
            self.close()
            raise

    def send_message(self, message):
        """Send a raw BSON message or raise socket.error.

        If a network exception is raised, the socket is closed.
        """
        try:
            self.sock.sendall(message)
        except:
            self.close()
            raise

    def receive_message(self, operation, request_id):
        """Receive a raw BSON message or raise socket.error.

        If any exception is raised, the socket is closed.
        """
        try:
            return receive_message(self.sock, operation, request_id)
        except:
            self.close()
            raise

    def check_auth(self, all_credentials):
        """Update this socket's authentication.

        Log in or out to bring this socket's credentials up to date with
        those provided. Can raise socket.error or OperationFailure.

        :Parameters:
          - `all_credentials`: dict, maps auth source to MongoCredential.
        """
        if all_credentials or self.authset:
            cached = set(itervalues(all_credentials))
            authset = self.authset.copy()

            # Logout any credentials that no longer exist in the cache.
            for credentials in authset - cached:
                auth.logout(credentials.source, self)
                self.authset.discard(credentials)

            for credentials in cached - authset:
                auth.authenticate(credentials, self)
                self.authset.add(credentials)

    def authenticate(self, credentials):
        """Log in to the server and store these credentials in `authset`.

        :Parameters:
          - `credentials`: A MongoCredential.
        """
        auth.authenticate(credentials, self)
        self.authset.add(credentials)

    def close(self):
        self.closed = True
        # Avoid exceptions on interpreter shutdown.
        try:
            self.sock.close()
        except:
            pass
        
    @property
    def min_wire_version(self):
        return self.ismaster.min_wire_version
        
    @property
    def max_wire_version(self):
        return self.ismaster.max_wire_version

    def __eq__(self, other):
        return self.sock == other.sock

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.sock)

    def __repr__(self):
        return "SocketInfo(%s)%s at %s" % (
            repr(self.sock),
            self.closed and " CLOSED" or "",
            id(self)
        )


def _create_connection(address, options):
    """Given (host, port) and PoolOptions, connect and return a socket object.

    This is a modified version of create_connection from CPython >= 2.6.
    """
    host, port = address

    # Check if dealing with a unix domain socket
    if host.endswith('.sock'):
        if not hasattr(socket, "AF_UNIX"):
            raise ConnectionFailure("UNIX-sockets are not supported "
                                    "on this system")
        sock = socket.socket(socket.AF_UNIX)
        try:
            sock.connect(host)
            return sock
        except socket.error as e:
            sock.close()
            raise e

    # Don't try IPv6 if we don't support it. Also skip it if host
    # is 'localhost' (::1 is fine). Avoids slow connect issues
    # like PYTHON-356.
    family = socket.AF_INET
    if socket.has_ipv6 and host != 'localhost':
        family = socket.AF_UNSPEC

    err = None
    for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
        af, socktype, proto, dummy, sa = res
        sock = socket.socket(af, socktype, proto)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(options.connect_timeout)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE,
                            options.socket_keepalive)
            sock.connect(sa)
            return sock
        except socket.error as e:
            err = e
            sock.close()

    if err is not None:
        raise err
    else:
        # This likely means we tried to connect to an IPv6 only
        # host with an OS/kernel or Python interpreter that doesn't
        # support IPv6. The test case is Jython2.5.1 which doesn't
        # support IPv6 at all.
        raise socket.error('getaddrinfo failed')


def _configured_socket(address, options):
    """Given (host, port) and PoolOptions, return a configured socket.

    Sets socket's SSL and timeout options.
    """
    sock = _create_connection(address, options)
    ssl_context = options.ssl_context

    if ssl_context is not None:
        try:
            sock = ssl_context.wrap_socket(sock)
        except IOError:
            sock.close()
            raise ConnectionFailure("SSL handshake failed. MongoDB may "
                                    "not be configured with SSL support.")
        if ssl_context.verify_mode and options.ssl_match_hostname:
            try:
                match_hostname(sock.getpeercert(), hostname=address[0])
            except CertificateError:
                sock.close()
                raise

    sock.settimeout(options.socket_timeout)
    return sock


# Do *not* explicitly inherit from object or Jython won't call __del__
# http://bugs.jython.org/issue1057
class Pool:
    def __init__(self, address, options):
        """
        :Parameters:
          - `address`: a (hostname, port) tuple
          - `options`: a PoolOptions instance
        """
        # Check a socket's health with socket_closed() every once in a while.
        # Can override for testing: 0 to always check, None to never check.
        self._check_interval_seconds = 1

        self.sockets = set()
        self.lock = threading.Lock()

        # Keep track of resets, so we notice sockets created before the most
        # recent reset and close them.
        self.pool_id = 0
        self.pid = os.getpid()
        self.address = address
        self.opts = options

        if (self.opts.wait_queue_multiple is None or
                self.opts.max_pool_size is None):
            max_waiters = None
        else:
            max_waiters = (
                self.opts.max_pool_size * self.opts.wait_queue_multiple)

        self._socket_semaphore = thread_util.create_semaphore(
            self.opts.max_pool_size, max_waiters)

    def reset(self):
        with self.lock:
            self.pool_id += 1
            self.pid = os.getpid()
            sockets, self.sockets = self.sockets, set()

        for sock_info in sockets:
            sock_info.close()

    def connect(self):
        """Connect to Mongo and return a new (connected) socket. Note that the
           pool does not keep a reference to the socket -- you must call
           return_socket() when you're done with it.
        """
        sock = _configured_socket(self.address, self.opts)
        try:
            ismaster = IsMaster(command(sock, 'admin', {'ismaster': 1}))
        except:
            sock.close()
            raise

        return SocketInfo(sock, self, ismaster, host=self.address[0])

    @contextlib.contextmanager
    def get_socket(self, all_credentials, checkout=False):
        """Get a socket from the pool. Use with a "with" statement.

        Returns a :class:`SocketInfo` object wrapping a connected
        :class:`socket.socket`.

        This method should always be used in a with-statement::

            with pool.get_socket(credentials, checkout) as socket_info:
                socket_info.send_message(msg)
                data = socket_info.receive_message(op_code, request_id)

        The socket is logged in or out as needed to match ``all_credentials``
        using the correct authentication mechanism for the server's wire
        protocol version.

        :Parameters:
          - `all_credentials`: dict, maps auth source to MongoCredential.
          - `checkout` (optional): keep socket checked out.
        """
        # First get a socket, then attempt authentication. Simplifies
        # semaphore management in the face of network errors during auth.
        sock_info = self._get_socket_no_auth()
        try:
            sock_info.check_auth(all_credentials)
            yield sock_info
        except (socket.error, ConnectionFailure):
            sock_info.close()

            # Decrement semaphore.
            self.return_socket(sock_info)
            raise
        except:
            self.return_socket(sock_info)
            raise
        else:
            if not checkout:
                self.return_socket(sock_info)

    def _get_socket_no_auth(self):
        # We use the pid here to avoid issues with fork / multiprocessing.
        # See test.test_client:TestClient.test_fork for an example of
        # what could go wrong otherwise
        if self.pid != os.getpid():
            self.reset()

        # Get a free socket or create one.
        if not self._socket_semaphore.acquire(
                True, self.opts.wait_queue_timeout):
            self._raise_wait_queue_timeout()

        # We've now acquired the semaphore and must release it on error.
        try:
            try:
                # set.pop() isn't atomic in Jython less than 2.7, see
                # http://bugs.jython.org/issue1854
                with self.lock:
                    sock_info, from_pool = self.sockets.pop(), True
            except KeyError:
                sock_info, from_pool = self.connect(), False

            if from_pool:
                sock_info = self._check(sock_info)

        except:
            self._socket_semaphore.release()
            raise

        sock_info.last_checkout = _time()
        return sock_info

    def return_socket(self, sock_info):
        """Return the socket to the pool, or if it's closed discard it."""
        if self.pid != os.getpid():
            self.reset()
        else:
            if sock_info.pool_id != self.pool_id:
                sock_info.close()
            elif not sock_info.closed:
                with self.lock:
                    self.sockets.add(sock_info)

        self._socket_semaphore.release()

    def _check(self, sock_info):
        """This side-effecty function checks if this pool has been reset since
        the last time this socket was used, or if the socket has been closed by
        some external network error, and if so, attempts to create a new socket.
        If this connection attempt fails we reset the pool and reraise the
        error.

        Checking sockets lets us avoid seeing *some*
        :class:`~pymongo.errors.AutoReconnect` exceptions on server
        hiccups, etc. We only do this if it's been > 1 second since
        the last socket checkout, to keep performance reasonable - we
        can't avoid AutoReconnects completely anyway.
        """
        error = False

        # How long since socket was last checked out.
        age = _time() - sock_info.last_checkout
        if (self._check_interval_seconds is not None
                and (
                    0 == self._check_interval_seconds
                    or age > self._check_interval_seconds)):
            if socket_closed(sock_info.sock):
                sock_info.close()
                error = True

        if not error:
            return sock_info
        else:
            return self.connect()

    def _raise_wait_queue_timeout(self):
        raise ConnectionFailure(
            'Timed out waiting for socket from pool with max_size %r and'
            ' wait_queue_timeout %r' % (
                self.opts.max_pool_size, self.opts.wait_queue_timeout))

    def __del__(self):
        # Avoid ResourceWarnings in Python 3
        for sock_info in self.sockets:
            sock_info.close()
