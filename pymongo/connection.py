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

"""Low level connection to Mongo."""

import sys
import socket
import struct
import types
import logging
import threading
import random
import errno

from errors import ConnectionFailure, InvalidName, OperationFailure, ConfigurationError
from database import Database
from cursor_manager import CursorManager

_logger = logging.getLogger("pymongo.connection")
_logger.addHandler(logging.StreamHandler())
# _logger.setLevel(logging.DEBUG)

_TIMEOUT = 20.0

# TODO support auth for pooling
class Connection(object):
    """A connection to Mongo.
    """
    def __init__(self, host="localhost", port=27017,
                 pool_size=1, auto_start_request=True, _connect=True):
        """Open a new connection to a Mongo instance at host:port.

        The resultant connection object has connection-pooling built in. It also
        performs auto-reconnection when necessary. If an operation fails because
        of a connection error, `pymongo.errors.ConnectionFailure` is raised. Auto-
        reconnection is also performed. Application code should handle this
        exception (recognizing that the operation failed) and then continue to
        execute.

        Raises TypeError if host is not an instance of string or port is not an
        instance of int. Raises ConnectionFailure if the connection cannot be
        made. Raises TypeError if `pool_size` is not an instance of int. Raises
        ValueError if `pool_size` is not greater than or equal to one.

        NOTE: Connection pooling is not compatible with auth (yet). Please do
        not set the "pool_size" to anything other than 1 if auth is in use.

        :Parameters:
          - `host` (optional): hostname or IPv4 address of the instance to
            connect to
          - `port` (optional): port number on which to connect
          - `pool_size` (optional): maximum size of the built in connection-pool
          - `auto_start_request` (optional): automatically start a request
            on every operation - see documentation for `start_request`.
        """
        if not isinstance(host, types.StringType):
            raise TypeError("host must be an instance of str")
        if not isinstance(port, types.IntType):
            raise TypeError("port must be an instance of int")
        if not isinstance(pool_size, types.IntType):
            raise TypeError("pool_size must be an instance of int")
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")

        self.__host = None
        self.__port = None

        self.__nodes = [(host, port)]

        # current request_id
        self.__id = 1
        self.__id_lock = threading.Lock()

        self.__cursor_manager = CursorManager(self)

        self.__pool_size = pool_size
        self.__auto_start_request = auto_start_request
        # map from threads to sockets
        self.__thread_map = {}
        # count of how many threads are mapped to each socket
        self.__thread_count = [0 for _ in range(self.__pool_size)]
        self.__locks = [threading.Lock() for _ in range(self.__pool_size)]
        self.__sockets = [None for _ in range(self.__pool_size)]
        self.__currently_resetting = False

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

    def paired(cls, left, right=("localhost", 27017),
               pool_size=1, auto_start_request=True):
        """Open a new paired connection to Mongo.

        Raises TypeError if either `left` or `right` is not a tuple of the form
        (host, port). Raises ConnectionFailure if the connection cannot be made.

        :Parameters:
          - `left`: (host, port) pair for the left Mongo instance
          - `right` (optional): (host, port) pair for the right Mongo instance
          - `pool_size` (optional): same as argument to `__init__`
          - `auto_start_request` (optional): same as argument to `__init__`
        """
        connection = cls(left[0], left[1], pool_size, auto_start_request,
                         _connect=False)
        connection.__pair_with(*right)
        return connection
    paired = classmethod(paired)

    def __increment_id(self):
        self.__id_lock.acquire()
        result = self.__id
        self.__id += 1
        self.__id_lock.release()
        return result

    def __master(self, sock):
        """Get the hostname and port of the master Mongo instance.

        Return a tuple (host, port).
        """
        result = self["admin"]._command({"ismaster": 1}, sock=sock)

        if result["ismaster"] == 1:
            return True
        else:
            strings = result["remote"].split(":", 1)
            if len(strings) == 1:
                port = 27017
            else:
                port = int(strings[1])
            return (strings[0], port)

    def host(self):
        """Get the connection's current host.
        """
        return self.__host

    def port(self):
        """Get the connection's current port.
        """
        return self.__port

    def __find_master(self):
        """Create a new socket and use it to figure out who the master is.

        Sets __host and __port so that `host()` and `port()` will return the
        address of the master.
        """
        _logger.debug("finding master")
        self.__host = None
        self.__port = None
        for (host, port) in self.__nodes:
            _logger.debug("trying %r:%r" % (host, port))
            try:
                try:
                    sock = socket.socket()
                    sock.settimeout(_TIMEOUT)
                    sock.connect((host, port))
                    master = self.__master(sock)
                    if master is True:
                        self.__host = host
                        self.__port = port
                        _logger.debug("found master")
                        return
                    if master not in self.__nodes:
                        raise ConfigurationError(
                            "%r claims master is %r, but that's not configured" %
                            ((host, port), master))
                    _logger.debug("not master, master is (%r, %r)" % master)
                except socket.error:
                    exctype, value = sys.exc_info()[:2]
                    _logger.debug("could not connect, got: %s %s" %
                                  (exctype, value))
                    continue
            finally:
                sock.close()
        raise ConnectionFailure("could not find master")

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
            sock = self.__sockets[socket_number]
            sock.settimeout(_TIMEOUT)
            sock.connect((self.host(), self.port()))
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
            self.__locks[i].acquire()
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

        Raises TypeError if manager_class is not a subclass of CursorManager. A
        cursor manager handles closing cursors. Different managers can implement
        different policies in terms of when to actually kill a cursor that has
        been closed.

        :Parameters:
          - `manager_class`: cursor manager to use
        """
        manager = manager_class(self)
        if not isinstance(manager, CursorManager):
            raise TypeError("manager_class must be a subclass of CursorManager")

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

        self.__locks[choices[0]].acquire()
        return choices[0]

    def __get_socket(self):
        thread = threading.currentThread()
        if self.__thread_map.get(thread, -1) >= 0:
            sock = self.__thread_map[thread]
            self.__locks[sock].acquire()
        else:
            sock = self.__pick_and_acquire_socket()
            if self.__auto_start_request or thread in self.__thread_map:
                self.__thread_map[thread] = sock
                self.__thread_count[sock] += 1

        try:
            if not self.__sockets[sock]:
                self.__connect(sock)
        except ConnectionFailure:
            self.__locks[sock].release()
            self._reset()
            raise
        return sock

    def __send_message_on_socket(self, operation, data, sock):
        # header
        request_id = self.__increment_id()
        to_send = struct.pack("<i", 16 + len(data))
        to_send += struct.pack("<i", request_id)
        to_send += struct.pack("<i", 0) # responseTo
        to_send += struct.pack("<i", operation)

        to_send += data

        total_sent = 0
        while total_sent < len(to_send):
            try:
                sent = sock.send(to_send[total_sent:])
            except socket.error, e:
                if e[0] == errno.EAGAIN:
                    continue
                raise ConnectionFailure("connection closed, resetting")
            if sent == 0:
                raise ConnectionFailure("connection closed, resetting")
            total_sent += sent

        return request_id

    def _send_message(self, operation, data):
        """Say something to Mongo.

        Raises ConnectionFailure if the message cannot be sent. Returns the
        request id of the sent message.

        :Parameters:
          - `operation`: opcode of the message
          - `data`: data to send
        """
        sock_number = self.__get_socket()
        sock = self.__sockets[sock_number]
        try:
            self.__send_message_on_socket(operation, data, sock)
            self.__locks[sock_number].release()
        except ConnectionFailure:
            self.__locks[sock_number].release()
            self._reset()
            raise

    def __receive_message_on_socket(self, operation, request_id, sock):
        def receive(length):
            message = ""
            while len(message) < length:
                try:
                    chunk = sock.recv(length - len(message))
                except socket.error:
                    raise ConnectionFailure("connection closed")
                if chunk == "":
                    raise ConnectionFailure("connection closed")
                message += chunk
            return message

        header = receive(16)
        length = struct.unpack("<i", header[:4])[0]
        assert request_id == struct.unpack("<i", header[8:12])[0]
        assert operation == struct.unpack("<i", header[12:])[0]

        return receive(length - 16)

    __hack_socket_lock = threading.Lock()
    def _receive_message(self, operation, data, _sock=None):
        """Receive a message from Mongo.

        Sends the given message and returns the response.

        :Parameters:
          - `operation`: opcode of the message to send
          - `data`: data to send
        """
        # hack so we can do find_master on a specific socket...
        if _sock:
            self.__hack_socket_lock.acquire()
            try:
                request_id = self.__send_message_on_socket(operation, data, _sock)
                result = self.__receive_message_on_socket(1, request_id, _sock)
                return result
            finally:
                self.__hack_socket_lock.release()

        sock_number = self.__get_socket()
        sock = self.__sockets[sock_number]
        try:
            request_id = self.__send_message_on_socket(operation, data, sock)
            result = self.__receive_message_on_socket(1, request_id, sock)
            self.__locks[sock_number].release()
            return result
        except ConnectionFailure:
            self.__locks[sock_number].release()
            self._reset()
            raise

    def start_request(self):
        """Start a "request".

        A "request" is a group of operations in which order matters. Examples
        include inserting a document and then performing a query which expects
        that document to have been inserted, or performing an operation and
        then using `database.Database.error()` to perform error-checking on
        that operation. When a thread performs operations in a "request", the
        connection will perform all operations on the same socket, so Mongo will
        order them correctly.

        This method is only relevant when the current Connection has a
        "pool_size" greater than one. Otherwise only a single socket will be
        used for *all* operations, so there is no need to group operations into
        requests.

        This method only needs to be used if the "auto_start_request" option
        is set to False. If "auto_start_request" is True, a request will be
        started (if necessary) on every operation.
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
        however, to make sure that `end_request` isn't called in the middle
        of a sequence of operations in which ordering is important. This could
        lead to unexpected results.

        `end_request` is useful even (especially) if "auto_start_request" is
        True.

        See the documentation for `start_request` for more information on what
        a "request" is and when one should be used.
        """
        thread = threading.currentThread()
        if self.__thread_map.get(thread, -1) >= 0:
            sock_number = self.__thread_map.pop(thread)
            self.__thread_count[sock_number] -= 1

    def __cmp__(self, other):
        if isinstance(other, Connection):
            return cmp((self.__host, self.__port), (other.__host, other.__port))
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

        Raises TypeError if cursor_id is not an instance of (int, long). What
        closing the cursor actually means depends on this connection's cursor
        manager.

        :Parameters:
          - `cursor_id`: cursor id to close
        """
        if not isinstance(cursor_id, (types.IntType, types.LongType)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self.__cursor_manager.close(cursor_id)

    def kill_cursors(self, cursor_ids):
        """Kill database cursors with the given ids.

        Raises TypeError if cursor_ids is not an instance of list.

        :Parameters:
          - `cursor_ids`: list of cursor ids to kill
        """
        if not isinstance(cursor_ids, types.ListType):
            raise TypeError("cursor_ids must be a list")
        message = "\x00\x00\x00\x00"
        message += struct.pack("<i", len(cursor_ids))
        for cursor_id in cursor_ids:
            message += struct.pack("<q", cursor_id)
        self._send_message(2007, message)

    def __database_info(self):
        """Get a dictionary of (database_name: size_on_disk).
        """
        result = self["admin"]._command({"listDatabases": 1})
        info = result["databases"]
        return dict([(db["name"], db["sizeOnDisk"]) for db in info])

    def database_names(self):
        """Get a list of all database names.
        """
        return self.__database_info().keys()

    def drop_database(self, name_or_database):
        """Drop a database.

        :Parameters:
          - `name_or_database`: the name of a database to drop or the object
            itself
        """
        name = name_or_database
        if isinstance(name, Database):
            name = name.name()

        if not isinstance(name, types.StringTypes):
            raise TypeError("name_or_database must be an instance of (Database, str, unicode)")

        self[name]._command({"dropDatabase": 1})

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Connection' object is not iterable")
