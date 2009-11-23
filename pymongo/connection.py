# Copyright 2009 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tools for connecting to MongoDB.

To connect to a single instance of MongoDB use :class:`Connection`. To connect
to a replica pair use :meth:`~Connection.paired`.

.. seealso:: Module :mod:`~pymongo.master_slave_connection` for connecting to
   master-slave clusters.

To get a :class:`~pymongo.database.Database` instance from a
:class:`Connection` use either dictionary-style or attribute-style access::

  >>> from pymongo import Connection
  >>> c = Connection()
  >>> c.test_database
  Database(Connection('localhost', 27017), u'test_database')
  >>> c['test-database']
  Database(Connection('localhost', 27017), u'test-database')
"""

import sys
import socket
import struct
import types
import logging
import threading
import random
import errno
import datetime

from errors import ConnectionFailure, ConfigurationError, AutoReconnect
from errors import OperationFailure
from database import Database
from cursor_manager import CursorManager
from thread_util import TimeoutableLock
import bson
import message

_logger = logging.getLogger("pymongo.connection")
_logger.addHandler(logging.StreamHandler())
_logger.setLevel(logging.INFO)

_CONNECT_TIMEOUT = 20.0


class Connection(object): # TODO support auth for pooling
    """Connection to MongoDB.
    """

    HOST = "localhost"
    PORT = 27017
    POOL_SIZE = 1
    AUTO_START_REQUEST = True
    TIMEOUT = 1.0

    def __init__(self, host=None, port=None, pool_size=None,
                 auto_start_request=None, timeout=None, slave_okay=False,
                 network_timeout=None, _connect=True):
        """Create a new connection to a single MongoDB instance at *host:port*.

        The resultant connection object has connection-pooling built in. It
        also performs auto-reconnection when necessary. If an operation fails
        because of a connection error,
        :class:`~pymongo.errors.ConnectionFailure` is raised. If
        auto-reconnection will be performed,
        :class:`~pymongo.errors.AutoReconnect` will be raised. Application code
        should handle this exception (recognizing that the operation failed)
        and then continue to execute.

        Raises :class:`TypeError` if host is not an instance of string or port
        is not an instance of ``int``. Raises
        :class:`~pymongo.errors.ConnectionFailure` if the connection cannot be
        made. Raises :class:`TypeError` if `pool_size` is not an instance of
        ``int``. Raises :class:`ValueError` if `pool_size` is not greater than
        or equal to one.

        .. warning:: Connection pooling is not compatible with auth (yet).
           Please do not set the `pool_size` parameter to anything other than 1
           if auth is in use.

        :Parameters:
          - `host` (optional): hostname or IPv4 address of the instance to
            connect to
          - `port` (optional): port number on which to connect
          - `pool_size` (optional): maximum size of the built in
            connection-pool
          - `auto_start_request` (optional): automatically start a request
            on every operation - see :meth:`start_request`
          - `slave_okay` (optional): is it okay to connect directly to and
            perform queries on a slave instance
          - `timeout` (optional): max time to wait when attempting to acquire a
            connection from the connection pool before raising an exception -
            can be set to -1 to wait indefinitely
          - `network_timeout` (optional): timeout (in seconds) to use for socket
            operations - default is no timeout
        """
        if host is None:
            host = self.HOST
        if port is None:
            port = self.PORT
        if pool_size is None:
            pool_size = self.POOL_SIZE
        if auto_start_request is None:
            auto_start_request = self.AUTO_START_REQUEST
        if timeout is None:
            timeout = self.TIMEOUT
        if timeout == -1:
            timeout = None

        if not isinstance(host, types.StringTypes):
            raise TypeError("host must be an instance of (str, unicode)")
        if not isinstance(port, types.IntType):
            raise TypeError("port must be an instance of int")
        if not isinstance(pool_size, types.IntType):
            raise TypeError("pool_size must be an instance of int")
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")

        self.__host = None
        self.__port = None

        self.__nodes = [(host, port)]
        self.__slave_okay = slave_okay

        self.__cursor_manager = CursorManager(self)

        self.__pool_size = pool_size
        self.__auto_start_request = auto_start_request
        # map from threads to sockets
        self.__thread_map = {}
        # count of how many threads are mapped to each socket
        self.__thread_count = [0 for _ in range(self.__pool_size)]
        self.__acquire_timeout = timeout
        self.__locks = [TimeoutableLock() for _ in range(self.__pool_size)]
        self.__sockets = [None for _ in range(self.__pool_size)]
        self.__currently_resetting = False

        self.__network_timeout = network_timeout

        # cache of existing indexes used by ensure_index ops
        self.__index_cache = {}

        if _connect:
            self.__find_master()

    def __pair_with(self, host, port):
        """Pair this connection with a Mongo instance running on host:port.

        Raises TypeError if host is not an instance of string or port is not an
        instance of int. Raises ConnectionFailure if the connection cannot be
        made.

        :Parameters:
          - `host`: the hostname or IPv4 address of the instance to
            pair with
          - `port`: the port number on which to connect
        """
        if not isinstance(host, types.StringType):
            raise TypeError("host must be an instance of str")
        if not isinstance(port, types.IntType):
            raise TypeError("port must be an instance of int")
        self.__nodes.append((host, port))

        self.__find_master()

    def paired(cls, left, right=None,
               pool_size=None, auto_start_request=None):
        """Open a new paired connection to Mongo.

        Raises :class:`TypeError` if either `left` or `right` is not a tuple of
        the form ``(host, port)``. Raises :class:`~pymongo.ConnectionFailure`
        if the connection cannot be made.

        :Parameters:
          - `left`: ``(host, port)`` pair for the left MongoDB instance
          - `right` (optional): ``(host, port)`` pair for the right MongoDB
            instance
          - `pool_size` (optional): same as argument to :class:`Connection`
          - `auto_start_request` (optional): same as argument to
            :class:`Connection`
        """
        if right is None:
            right = (cls.HOST, cls.PORT)
        if pool_size is None:
            pool_size = cls.POOL_SIZE
        if auto_start_request is None:
            auto_start_request = cls.AUTO_START_REQUEST

        connection = cls(left[0], left[1], pool_size, auto_start_request,
                         _connect=False)
        connection.__pair_with(*right)
        return connection
    paired = classmethod(paired)

    def __master(self, sock):
        """Get the hostname and port of the master Mongo instance.

        Return a tuple (host, port).
        """
        result = self["admin"]._command({"ismaster": 1}, sock=sock)

        if result["ismaster"] == 1:
            return True
        else:
            if "remote" not in result:
                return False

            strings = result["remote"].split(":", 1)
            if len(strings) == 1:
                port = self.PORT
            else:
                port = int(strings[1])
            return (strings[0], port)

    def _cache_index(self, database, collection, index, ttl):
        """Add an index to the index cache for ensure_index operations.

        Return ``True`` if the index has been newly cached or if the index had
        expired and is being re-cached.

        Return ``False`` if the index exists and is valid.
        """
        now = datetime.datetime.utcnow()
        expire = datetime.timedelta(seconds=ttl) + now

        if database not in self.__index_cache:
            self.__index_cache[database] = {}
            self.__index_cache[database][collection] = {}
            self.__index_cache[database][collection][index] = expire
            return True

        if collection not in self.__index_cache[database]:
            self.__index_cache[database][collection] = {}
            self.__index_cache[database][collection][index] = expire
            return True

        if index in self.__index_cache[database][collection]:
            if now < self.__index_cache[database][collection][index]:
                return False

        self.__index_cache[database][collection][index] = expire
        return True

    def _purge_index(self, database_name,
                     collection_name=None, index_name=None):
        """Purge an index from the index cache.

        If `index_name` is None purge an entire collection.

        If `collection_name` is None purge an entire database.
        """
        if not database_name in self.__index_cache:
            return

        if collection_name is None:
            del self.__index_cache[database_name]
            return

        if not collection_name in self.__index_cache[database_name]:
            return

        if index_name is None:
            del self.__index_cache[database_name][collection_name]
            return

        if index_name in self.__index_cache[database_name][collection_name]:
            del self.__index_cache[database_name][collection_name][index_name]

    # TODO these really should be properties... Could be ugly to make that
    # backwards compatible though...
    def host(self):
        """Get the connection's current host.
        """
        return self.__host

    def port(self):
        """Get the connection's current port.
        """
        return self.__port

    def slave_okay(self):
        """Is it okay for this connection to connect directly to a slave?
        """
        return self.__slave_okay
    slave_okay = property(slave_okay)

    def __find_master(self):
        """Create a new socket and use it to figure out who the master is.

        Sets __host and __port so that `host()` and `port()` will return the
        address of the master.
        """
        _logger.debug("finding master")
        self.__host = None
        self.__port = None
        sock = None
        for (host, port) in self.__nodes:
            _logger.debug("trying %r:%r" % (host, port))
            try:
                try:
                    sock = socket.socket()
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    sock.settimeout(_CONNECT_TIMEOUT)
                    sock.connect((host, port))
                    sock.settimeout(self.__network_timeout)
                    try:
                        master = self.__master(sock)
                    except ConnectionFailure, e:
                        raise AutoReconnect(str(e))
                    if master is True:
                        self.__host = host
                        self.__port = port
                        _logger.debug("found master")
                        return
                    if not master:
                        if self.__slave_okay:
                            self.__host = host
                            self.__port = port
                            _logger.debug("connecting to slave (slave_okay mode)")
                            return

                        raise ConfigurationError("trying to connect directly to"
                                                 " slave %s:%r - must specify "
                                                 "slave_okay to connect to "
                                                 "slaves" % (host, port))
                    if master not in self.__nodes:
                        raise ConfigurationError(
                            "%r claims master is %r, "
                            "but that's not configured" %
                            ((host, port), master))
                    _logger.debug("not master, master is (%r, %r)" % master)
                except socket.error, e:
                    exctype, value = sys.exc_info()[:2]
                    _logger.debug("could not connect, got: %s %s" %
                                  (exctype, value))
                    if len(self.__nodes) == 1:
                        raise ConnectionFailure(e)
                    continue
            finally:
                if sock is not None:
                    sock.close()
        raise AutoReconnect("could not find master")

    def __connect(self, socket_number):
        """(Re-)connect to Mongo.

        Connect to the master if this is a paired connection.
        """
        if self.host() is None or self.port() is None:
            self.__find_master()
        _logger.debug("connecting socket %s..." % socket_number)

        assert self.__sockets[socket_number] is None

        try:
            self.__sockets[socket_number] = socket.socket()
            self.__sockets[socket_number].setsockopt(socket.IPPROTO_TCP,
                                                     socket.TCP_NODELAY, 1)
            sock = self.__sockets[socket_number]
            sock.settimeout(_CONNECT_TIMEOUT)
            sock.connect((self.host(), self.port()))
            sock.settimeout(self.__network_timeout)
            _logger.debug("connected")
            return
        except socket.error:
            raise ConnectionFailure("could not connect to %r" % self.__nodes)

    def _reset(self):
        """Reset everything and start connecting again.

        Closes all open sockets and resets them to None. Re-finds the master.

        This should be done in case of a connection failure or a "not master"
        error.
        """
        if self.__currently_resetting:
            return
        self.__currently_resetting = True

        for i in range(self.__pool_size):
            # prevent all operations during the reset
            if not self.__locks[i].acquire(timeout=self.__acquire_timeout):
                raise ConnectionFailure("timed out before acquiring "
                                        "a connection from the pool")
            if self.__sockets[i] is not None:
                self.__sockets[i].close()
                self.__sockets[i] = None

        try:
            self.__find_master()
        finally:
            self.__currently_resetting = False
            for i in range(self.__pool_size):
                self.__locks[i].release()

    def set_cursor_manager(self, manager_class):
        """Set this connection's cursor manager.

        Raises :class:`TypeError` if `manager_class` is not a subclass of
        :class:`~pymongo.cursor_manager.CursorManager`. A cursor manager handles
        closing cursors. Different managers can implement different policies in
        terms of when to actually kill a cursor that has been closed.

        :Parameters:
          - `manager_class`: cursor manager to use
        """
        manager = manager_class(self)
        if not isinstance(manager, CursorManager):
            raise TypeError("manager_class must be a subclass of "
                            "CursorManager")

        self.__cursor_manager = manager

    def __pick_and_acquire_socket(self):
        """Acquire a socket to use for synchronous send and receive operations.
        """
        choices = range(self.__pool_size)
        random.shuffle(choices)
        choices.sort(lambda x, y: cmp(self.__thread_count[x],
                                      self.__thread_count[y]))

        for choice in choices:
            if self.__locks[choice].acquire(False):
                return choice

        if not self.__locks[choices[0]].acquire(timeout=
                                                self.__acquire_timeout):
            raise ConnectionFailure("timed out before acquiring "
                                    "a connection from the pool")
        return choices[0]

    def __get_socket(self):
        thread = threading.currentThread()
        if self.__thread_map.get(thread, -1) >= 0:
            sock = self.__thread_map[thread]
            if not self.__locks[sock].acquire(timeout=self.__acquire_timeout):
                raise ConnectionFailure("timed out before acquiring "
                                        "a connection from the pool")
        else:
            sock = self.__pick_and_acquire_socket()
            if self.__auto_start_request or thread in self.__thread_map:
                self.__thread_map[thread] = sock
                self.__thread_count[sock] += 1

        try:
            if not self.__sockets[sock]:
                self.__connect(sock)
        except ConnectionFailure, e:
            self.__sockets[sock].close()
            self.__sockets[sock] = None
            raise AutoReconnect(str(e))
        return sock

    # TODO use static methods for a bunch of these
    def __send_data_on_socket(self, data, sock):
        """Lowest level send operation.

        Takes data to send as a byte string and a socket instance and
        sends all of the data, raising ConnectionFailure on error.
        """
        total_sent = 0
        while total_sent < len(data):
            try:
                sent = sock.send(data[total_sent:])
            except socket.error, e:
                if e[0] == errno.EAGAIN:
                    continue
                raise ConnectionFailure("connection closed, resetting")
            if sent == 0:
                raise ConnectionFailure("connection closed, resetting")
            total_sent += sent

    # TODO does this really belong here?
    def _unpack_response(response, cursor_id=None):
        """Unpack a response from the database.

        Check the response for errors and unpack, returning a dictionary
        containing the response data.

        :Parameters:
          - `response`: byte string as returned from the database
          - `cursor_id` (optional): cursor_id we sent to get this response -
            used for raising an informative exception when we get cursor id not
            valid at server response
        """
        response_flag = struct.unpack("<i", response[:4])[0]
        if response_flag == 1:
            # Shouldn't get this response if we aren't doing a getMore
            assert cursor_id is not None

            raise OperationFailure("cursor id '%s' not valid at server" %
                                   cursor_id)
        elif response_flag == 2:
            error_object = bson.BSON(response[20:]).to_dict()
            if error_object["$err"] == "not master":
                db.connection()._reset()
                raise AutoReconnect("master has changed")
            raise OperationFailure("database error: %s" %
                                   error_object["$err"])
        else:
            assert response_flag == 0

        result = {}
        result["cursor_id"] = struct.unpack("<q", response[4:12])[0]
        result["starting_from"] = struct.unpack("<i", response[12:16])[0]
        result["number_returned"] = struct.unpack("<i", response[16:20])[0]
        result["data"] = bson._to_dicts(response[20:])
        assert len(result["data"]) == result["number_returned"]
        return result

    _unpack_response = staticmethod(_unpack_response)

    def __check_response_to_last_error(self, response):
        """Check a response to a lastError message for errors.

        `response` is a byte string representing a response to the message.
        If it represents an error response we raise OperationFailure.
        """
        response = Connection._unpack_response(response)

        assert response["number_returned"] == 1
        error = response["data"][0]

        # TODO unify logic with database.error method
        if error.get("err", 0) is None:
            return
        if error["err"] == "not master":
            self._reset()
        raise OperationFailure(error["err"])

    def _send_message(self, message, with_last_error=False):
        """Say something to Mongo.

        Raises ConnectionFailure if the message cannot be sent. Raises
        OperationFailure if `with_last_error` is ``True`` and the response to
        the getLastError call returns an error.

        :Parameters:
          - `message`: message to send
          - `with_last_error`: check getLastError status after sending the
            message
        """
        sock_number = self.__get_socket()
        sock = self.__sockets[sock_number]
        try:
            try:
                (request_id, data) = message
                self.__send_data_on_socket(data, sock)
                # Safe mode. We pack the message together with a lastError
                # message and send both. We then get the response (to the
                # lastError) and raise OperationFailure if it is an error
                # response.
                if with_last_error:
                    response = self.__receive_message_on_socket(1, request_id, sock)
                    self.__check_response_to_last_error(response)
            except ConnectionFailure, e:
                self.__sockets[sock_number].close()
                self.__sockets[sock_number] = None
                raise AutoReconnect(str(e))
        finally:
            self.__locks[sock_number].release()

    def __receive_data_on_socket(self, length, sock):
        """Lowest level receive operation.

        Takes length to receive and repeatedly calls recv until able to
        return a buffer of that length, raising ConnectionFailure on error.
        """
        message = ""
        while len(message) < length:
            try:
                chunk = sock.recv(length - len(message))
            except socket.error, e:
                raise ConnectionFailure(e)
            if chunk == "":
                raise ConnectionFailure("connection closed")
            message += chunk
        return message

    def __receive_message_on_socket(self, operation, request_id, sock):
        """Receive a message in response to `request_id` on `sock`.

        Returns the response data with the header removed.
        """
        header = self.__receive_data_on_socket(16, sock)
        length = struct.unpack("<i", header[:4])[0]
        assert request_id == struct.unpack("<i", header[8:12])[0], \
            "ids don't match %r %r" % (request_id,
                                       struct.unpack("<i", header[8:12])[0])
        assert operation == struct.unpack("<i", header[12:])[0]

        return self.__receive_data_on_socket(length - 16, sock)

    def __send_and_receive(self, message, sock):
        """Send a message on the given socket and return the response data.
        """
        (request_id, data) = message
        self.__send_data_on_socket(data, sock)
        return self.__receive_message_on_socket(1, request_id, sock)

    __hack_socket_lock = threading.Lock()
    # we just ignore _must_use_master here: it's only relavant for
    # MasterSlaveConnection instances.
    def _send_message_with_response(self, message,
                                    _sock=None, _must_use_master=False):
        """Send a message to Mongo and return the response.

        Sends the given message and returns the response.

        :Parameters:
          - `message`: (request_id, data) pair making up the message to send
        """
        # hack so we can do find_master on a specific socket...
        if _sock:
            self.__hack_socket_lock.acquire()
            try:
                return self.__send_and_receive(message, _sock)
            finally:
                self.__hack_socket_lock.release()

        sock_number = self.__get_socket()
        sock = self.__sockets[sock_number]
        try:
            try:
                return self.__send_and_receive(message, sock)
            except ConnectionFailure, e:
                self.__sockets[sock_number].close()
                self.__sockets[sock_number] = None
                raise AutoReconnect(str(e))
        finally:
            self.__locks[sock_number].release()

    def start_request(self):
        """Start a "request".

        A "request" is a group of operations in which order matters. Examples
        include inserting a document and then performing a query which expects
        that document to have been inserted, or performing an operation and
        then using :meth:`database.Database.error()` to perform error-checking
        on that operation. When a thread performs operations in a "request", the
        connection will perform all operations on the same socket, so Mongo
        will order them correctly.

        This method is only relevant when the current :class:`Connection` has a
        ``pool_size`` greater than one. Otherwise only a single socket will be
        used for *all* operations, so there is no need to group operations into
        requests.

        This method only needs to be used if the ``auto_start_request`` option
        is set to ``False``. If ``auto_start_request`` is ``True``, a request
        will be started (if necessary) on every operation.
        """
        if not self.__auto_start_request:
            self.end_request()
            self.__thread_map[threading.currentThread()] = -1

    def end_request(self):
        """End the current "request", if this thread is in one.

        Judicious use of this method can lead to performance gains when
        connection-pooling is being used. By ending a request when it is safe
        to do so the connection is allowed to pick a new socket from the pool
        for that thread on the next operation. This could prevent an imbalance
        of threads trying to connect on the same socket. Care should be taken,
        however, to make sure that :meth:`end_request` isn't called in the
        middle of a sequence of operations in which ordering is important. This
        could lead to unexpected results.

        :meth:`end_request` is useful even (especially) if
        ``auto_start_request`` is ``True``.

        .. seealso:: :meth:`start_request` for more information on what
           a "request" is and when one should be used.
        """
        thread = threading.currentThread()
        if self.__thread_map.get(thread, -1) >= 0:
            sock_number = self.__thread_map.pop(thread)
            self.__thread_count[sock_number] -= 1

    def __cmp__(self, other):
        if isinstance(other, Connection):
            return cmp((self.__host, self.__port),
                       (other.__host, other.__port))
        return NotImplemented

    def __repr__(self):
        if len(self.__nodes) == 1:
            return "Connection(%r, %r)" % (self.__host, self.__port)
        elif len(self.__nodes) == 2:
            return ("Connection.paired((%r, %r), (%r, %r))" %
                    (self.__nodes[0][0],
                     self.__nodes[0][1],
                     self.__nodes[1][0],
                     self.__nodes[1][1]))

    def __getattr__(self, name):
        """Get a database by name.

        Raises InvalidName if an invalid database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return Database(self, name)

    def __getitem__(self, name):
        """Get a database by name.

        Raises InvalidName if an invalid database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return self.__getattr__(name)

    def close_cursor(self, cursor_id):
        """Close a single database cursor.

        Raises :class:`TypeError` if `cursor_id` is not an instance of ``(int, long)``. What closing the cursor actually means depends on this connection's cursor manager.

        :Parameters:
          - `cursor_id`: id of cursor to close

        .. seealso:: :meth:`set_cursor_manager` and
           the :mod:`~pymongo.cursor_manager` module
        """
        if not isinstance(cursor_id, (types.IntType, types.LongType)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self.__cursor_manager.close(cursor_id)

    def kill_cursors(self, cursor_ids):
        """Send a kill cursors message with the given ids.

        Raises :class:`TypeError` if `cursor_ids` is not an instance of
        ``list``.

        :Parameters:
          - `cursor_ids`: list of cursor ids to kill
        """
        if not isinstance(cursor_ids, types.ListType):
            raise TypeError("cursor_ids must be a list")
        return self._send_message(message.kill_cursors(cursor_ids))

    def __database_info(self):
        """Get a dictionary of (database_name: size_on_disk).
        """
        result = self["admin"]._command({"listDatabases": 1})
        info = result["databases"]
        return dict([(db["name"], db["sizeOnDisk"]) for db in info])

    def server_info(self):
        """Get information about the MongoDB server we're connected to.
        """
        return self["admin"]._command({"buildinfo": 1})

    def database_names(self):
        """Get a list of the names of all databases on the connected server.
        """
        return self.__database_info().keys()

    def drop_database(self, name_or_database):
        """Drop a database.

        Raises :class:`TypeError` if `name_or_database` is not an instance of
        ``(str, unicode, Database)``

        :Parameters:
          - `name_or_database`: the name of a database to drop, or a
            :class:`~pymongo.database.Database` instance representing the
            database to drop
        """
        name = name_or_database
        if isinstance(name, Database):
            name = name.name()

        if not isinstance(name, types.StringTypes):
            raise TypeError("name_or_database must be an instance of "
                            "(Database, str, unicode)")

        self._purge_index(name)
        self[name]._command({"dropDatabase": 1})

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Connection' object is not iterable")
