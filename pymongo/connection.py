# Copyright 2009-2010 10gen, Inc.
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

"""Tools for connecting to MongoDB.

.. seealso:: Module :mod:`~pymongo.master_slave_connection` for
   connecting to master-slave clusters, and
   :doc:`/examples/replica_set` for an example of how to connect to a
   replica set.

To get a :class:`~pymongo.database.Database` instance from a
:class:`Connection` use either dictionary-style or attribute-style
access:

.. doctest::

  >>> from pymongo import Connection
  >>> c = Connection()
  >>> c.test_database
  Database(Connection('localhost', 27017), u'test_database')
  >>> c['test-database']
  Database(Connection('localhost', 27017), u'test-database')
"""

import datetime
import os
import select
import socket
import struct
import thread
import time
import warnings

from pymongo import (common,
                     database,
                     helpers,
                     message,
                     uri_parser)
from pymongo.cursor_manager import CursorManager
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            DuplicateKeyError,
                            InvalidDocument,
                            InvalidURI,
                            OperationFailure)


_CONNECT_TIMEOUT = 20.0


try:
    from greenlet import greenlet
except ImportError:
    def _thread_identifier():
        """Return the identifier of the current thread-of-execution."""
        return os.getpid(), thread.get_ident()
else:
    def _thread_identifier():
        """Return the identifier of the current thread-of-execution.
        Supports greenlets.
        """
        return os.getpid(), thread.get_ident(), greenlet.getcurrent()



def _closed(sock):
    """Return True if we know socket has been closed, False otherwise.
    """
    rd, _, _ = select.select([sock], [], [], 0)
    try:
        return len(rd) and sock.recv() == ""
    except:
        return True


def _partition_node(node):
    """Split a host:port string returned from mongod/s into
    a (host, int(port)) pair needed for socket.connect().
    """
    host = node
    port = 27017
    idx = node.rfind(':')
    if idx != -1:
        host, port = node[:idx], int(node[idx + 1:])
    if host.startswith('['):
        host = host[1:-1]
    return host, port



class _Pool(object):
    """A simple connection pool.

    Uses thread-local socket per thread (including greenlets).
    By calling return_socket() a thread can return a socket to the pool.
    """

    def __init__(self, pool_size, network_timeout):
        self.pid = os.getpid()
        self.pool_size = pool_size
        self.network_timeout = network_timeout
        self.sockets = []
        self.active_sockets = {}

    def connect(self, host, port):
        """Connect to Mongo and return a new (connected) socket.
        """
        try:
            # Prefer IPv4. If there is demand for an option
            # to specify one or the other we can add it later.
            s = socket.socket(socket.AF_INET)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(self.network_timeout or _CONNECT_TIMEOUT)
            s.connect((host, port))
            s.settimeout(self.network_timeout)
            return s
        except socket.gaierror:
            # If that fails try IPv6
            s = socket.socket(socket.AF_INET6)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(self.network_timeout or _CONNECT_TIMEOUT)
            s.connect((host, port))
            s.settimeout(self.network_timeout)
            return s

    def get_socket(self, host, port):
        # We use the _thread_identifier here to avoid issues with multiple
        # threads of execution (processes, proper threads, greenlets)
        # See test.test_connection:TestConnection.test_fork for an example of
        # what could go wrong otherwise
        sock_id = _thread_identifier()

        try:
            sock = self.active_sockets[sock_id]
        except KeyError:
            try:
                sock = self.sockets.pop()
            except IndexError:
                sock = self.connect(host, port)
            self.active_sockets[sock_id] = sock
        return sock

    def return_socket(self):
        sock = self.active_sockets.pop(_thread_identifier(), None)
        if sock is not None:
            # There's a race condition here, but we deliberately
            # ignore it.  It means that if the pool_size is 10 we
            # might actually keep slightly more than that.
            if len(self.sockets) < self.pool_size:
                self.sockets.append(sock)


class Connection(common.BaseObject):  # TODO support auth for pooling
    """Connection to MongoDB.
    """

    HOST = "localhost"
    PORT = 27017

    __max_bson_size = 4 * 1024 * 1024

    def __init__(self, host=None, port=None, max_pool_size=10,
                 network_timeout=None, document_class=dict,
                 tz_aware=False, _connect=True, **kwargs):
        """Create a new connection to a single MongoDB instance at *host:port*.

        The resultant connection object has connection-pooling built
        in. It also performs auto-reconnection when necessary. If an
        operation fails because of a connection error,
        :class:`~pymongo.errors.ConnectionFailure` is raised. If
        auto-reconnection will be performed,
        :class:`~pymongo.errors.AutoReconnect` will be
        raised. Application code should handle this exception
        (recognizing that the operation failed) and then continue to
        execute.

        Raises :class:`TypeError` if port is not an instance of
        ``int``. Raises :class:`~pymongo.errors.ConnectionFailure` if
        the connection cannot be made.

        The `host` parameter can be a full `mongodb URI
        <http://dochub.mongodb.org/core/connections>`_, in addition to
        a simple hostname. It can also be a list of hostnames or
        URIs. Any port specified in the host string(s) will override
        the `port` parameter. If multiple mongodb URIs containing
        database or auth information are passed, the last database,
        username, and password present will be used.

        :Parameters:
          - `host` (optional): hostname or IP address of the
            instance to connect to, or a mongodb URI, or a list of
            hostnames / mongodb URIs. If `host` is an IPv6 literal
            it must be enclosed in '[' and ']' characters following
            the RFC2732 URL syntax (e.g. '[::1]' for localhost)
          - `port` (optional): port number on which to connect
          - `max_pool_size` (optional): The maximum size limit for
            the connection pool.
          - `network_timeout` (optional): timeout (in seconds) to use
            for socket operations - default is no timeout
          - `document_class` (optional): default class to use for
            documents returned from queries on this connection
          - `tz_aware` (optional): if ``True``,
            :class:`~datetime.datetime` instances returned as values
            in a document by this :class:`Connection` will be timezone
            aware (otherwise they will be naive)

          Other optional parameters can be passed as keyword arguments:

          - `slave_okay` or `slaveok`: Is it OK to perform queries if
            this connection is to a secondary?
          - `safe`: Use getlasterror for each write operation?
          - `j`: Block until write operations have been commited to the
            journal. Ignored if the server is running without journaling.
            Implies safe=True.
          - `w`: If this is a replica set the server won't return until
            write operations have replicated to this many set members.
            Implies safe=True.
          - `wtimeout`: Used in conjunction with `j` and/or `w`. Wait this many
            milliseconds for journal acknowledgement and/or write replication.
            Implies safe=True.
          - `fsync`: Force the database to fsync all files before returning
            When used with `j` the server awaits the next group commit before
            returning.
            Implies safe=True.

        .. seealso:: :meth:`end_request`
        .. versionchanged:: 1.11+
           `slave_okay` is a pure keyword argument. Added support for safe,
           and getlasterror options as keyword arguments.
        .. versionchanged:: 1.11
           Added `max_pool_size`. Completely removed previously deprecated
           `pool_size`, `auto_start_request` and `timeout` parameters.
        .. versionchanged:: 1.8
           The `host` parameter can now be a full `mongodb URI
           <http://dochub.mongodb.org/core/connections>`_, in addition
           to a simple hostname. It can also be a list of hostnames or
           URIs.
        .. versionadded:: 1.8
           The `tz_aware` parameter.
        .. versionadded:: 1.7
           The `document_class` parameter.
        .. versionchanged:: 1.4
           DEPRECATED The `pool_size`, `auto_start_request`, and `timeout`
           parameters.
        .. versionadded:: 1.1
           The `network_timeout` parameter.

        .. mongodoc:: connections
        """
        super(Connection, self).__init__(**kwargs)

        if host is None:
            host = self.HOST
        if isinstance(host, basestring):
            host = [host]
        if port is None:
            port = self.PORT
        if not isinstance(port, int):
            raise TypeError("port must be an instance of int")

        nodes = set()
        username = None
        password = None
        db = None
        coll = None
        options = {}
        for entity in host:
            if "://" in entity:
                if entity.startswith("mongodb://"):
                    res = uri_parser.parse_uri(entity, port)
                    nodes.update(res["nodelist"])
                    username = res["username"] or username
                    password = res["password"] or password
                    db = res["database"] or db
                    coll = res["collection"] or coll
                    options = res["options"]
                else:
                    idx = entity.find("://")
                    raise InvalidURI("Invalid URI scheme: "
                                     "%s" % (entity[:idx],))
            else:
                nodes.update(uri_parser.split_hosts(entity, port))
        if not nodes:
            raise ConfigurationError("need to specify at least one host")
        self.__nodes = nodes

        self.__host = None
        self.__port = None

        if options:
            super(Connection, self)._set_options(**options)

        assert isinstance(max_pool_size, int), "max_pool_size must be an int"
        self.__max_pool_size = options.get("maxpoolsize") or max_pool_size
        if self.__max_pool_size < 0:
            raise ValueError("the maximum pool size must be >= 0")

        # TODO - Support using other options like w and fsync from URI
        self.__options = options
        # TODO - Support setting the collection from URI like the Java driver
        self.__collection = coll

        self.__cursor_manager = CursorManager(self)

        self.__network_timeout = network_timeout
        self.__pool = _Pool(self.__max_pool_size, self.__network_timeout)
        self.__last_checkout = time.time()

        self.__document_class = document_class
        self.__tz_aware = tz_aware

        # cache of existing indexes used by ensure_index ops
        self.__index_cache = {}

        if _connect:
            self.__find_node()

        if db and username is None:
            warnings.warn("must provide a username and password "
                          "to authenticate to %s" % (db,))
        if username:
            db = db or "admin"
            if not self[db].authenticate(username, password):
                raise ConfigurationError("authentication failed")

    @classmethod
    def from_uri(cls, uri="mongodb://localhost", **connection_args):
        """DEPRECATED Can pass a mongodb URI directly to Connection() instead.

        .. versionchanged:: 1.8
           DEPRECATED
        .. versionadded:: 1.5
        """
        warnings.warn("Connection.from_uri is deprecated - can pass "
                      "URIs to Connection() now", DeprecationWarning)
        return cls(uri, **connection_args)

    @classmethod
    def paired(cls, left, right=None, **connection_args):
        """DEPRECATED Can pass a list of hostnames to Connection() instead.

        .. versionchanged:: 1.8
           DEPRECATED
        """
        warnings.warn("Connection.paired is deprecated - can pass multiple "
                      "hostnames to Connection() now", DeprecationWarning)
        if isinstance(left, str) or isinstance(right, str):
            raise TypeError("arguments to paired must be tuples")
        if right is None:
            right = (cls.HOST, cls.PORT)
        return cls([":".join(map(str, left)), ":".join(map(str, right))],
                   **connection_args)

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

    @property
    def host(self):
        """Current connected host.

        .. versionchanged:: 1.3
           ``host`` is now a property rather than a method.
        """
        return self.__host

    @property
    def port(self):
        """Current connected port.

        .. versionchanged:: 1.3
           ``port`` is now a property rather than a method.
        """
        return self.__port

    @property
    def max_pool_size(self):
        """The maximum pool size limit set for this connection.

        .. versionadded:: 1.11
        """
        return self.__max_pool_size

    @property
    def nodes(self):
        """List of all known nodes.

        Includes both nodes specified when the :class:`Connection` was
        created, as well as nodes discovered through the replica set
        discovery mechanism.

        .. versionadded:: 1.8
        """
        return self.__nodes

    def get_document_class(self):
        return self.__document_class

    def set_document_class(self, klass):
        self.__document_class = klass

    document_class = property(get_document_class, set_document_class,
                              doc="""Default class to use for documents
                              returned on this connection.

                              .. versionadded:: 1.7
                              """)

    @property
    def tz_aware(self):
        """Does this connection return timezone-aware datetimes?

        See the `tz_aware` parameter to :meth:`Connection`.

        .. versionadded:: 1.8
        """
        return self.__tz_aware

    @property
    def max_bson_size(self):
        """Return the maximum size BSON object the connected server
        accepts in bytes. Defaults to 4MB in server < 1.7.4.

        .. versionadded:: 1.10
        """
        return self.__max_bson_size

    def __try_node(self, node):
        """Try to connect to this node and see if it works
        for our connection type.

        :Parameters:
         - `node`: The (host, port) pair to try.
        """
        self.disconnect()
        self.__host, self.__port = node
        try:
            response = self.admin.command("ismaster")
        except:
            return None

        self.end_request()

        if "maxBsonObjectSize" in response:
            self.__max_bson_size = response["maxBsonObjectSize"]

        # Replica Set?
        if len(self.__nodes) > 1:
            if "hosts" in response:
                self.__nodes.update([_partition_node(h)
                                     for h in response["hosts"]])
            if response["ismaster"]:
                return node
            elif "primary" in response:
                candidate = _partition_node(response["primary"])
                return self.__try_node(candidate)
            return None

        # Direct connection
        if response.get("arbiterOnly", False):
            return None
        return node

    def __find_node(self):
        """Find a host, port pair suitable for our connection type.

        If only one host was supplied to __init__ see if we can connect
        to it. Don't check if the host is a master/primary so we can make
        a direct connection to read from a slave.

        If more than one host was supplied treat them as a seed list for
        connecting to a replica set. Try to find the primary and fail if
        we can't. Possibly updates any replSet information on success.

        If the list of hosts is not a seed list for a replica set the
        behavior is still the same. We iterate through the list trying
        to find a host we can send write operations to.

        In either case a connection to an arbiter will never succeed.

        Sets __host and __port so that :attr:`host` and :attr:`port`
        will return the address of the connected host.
        """
        for candidate in self.__nodes:
            node = self.__try_node(candidate)
            if node:
                return node
        self.disconnect()
        raise AutoReconnect("could not find master/primary")

    def __socket(self):
        """Get a socket from the pool.

        If it's been > 1 second since the last time we checked out a
        socket, we also check to see if the socket has been closed -
        this let's us avoid seeing *some*
        :class:`~pymongo.errors.AutoReconnect` exceptions on server
        hiccups, etc. We only do this if it's been > 1 second since
        the last socket checkout, to keep performance reasonable - we
        can't avoid those completely anyway.
        """
        host, port = (self.__host, self.__port)
        if host is None or port is None:
            host, port = self.__find_node()

        try:
            sock = self.__pool.get_socket(host, port)
        except socket.error:
            self.disconnect()
            raise AutoReconnect("could not connect to %s:%d" % (host, port))
        t = time.time()
        if t - self.__last_checkout > 1:
            if _closed(sock):
                self.disconnect()
                sock = self.__pool.get_socket(host, port)
        self.__last_checkout = t
        return sock

    def disconnect(self):
        """Disconnect from MongoDB.

        Disconnecting will close all underlying sockets in the
        connection pool. If the :class:`Connection` is used again it
        will be automatically re-opened. Care should be taken to make
        sure that :meth:`disconnect` is not called in the middle of a
        sequence of operations in which ordering is important. This
        could lead to unexpected results.

        .. seealso:: :meth:`end_request`
        .. versionadded:: 1.3
        """
        self.__pool = _Pool(self.__max_pool_size, self.__network_timeout)
        self.__host = None
        self.__port = None

    def set_cursor_manager(self, manager_class):
        """Set this connection's cursor manager.

        Raises :class:`TypeError` if `manager_class` is not a subclass of
        :class:`~pymongo.cursor_manager.CursorManager`. A cursor manager
        handles closing cursors. Different managers can implement different
        policies in terms of when to actually kill a cursor that has
        been closed.

        :Parameters:
          - `manager_class`: cursor manager to use
        """
        manager = manager_class(self)
        if not isinstance(manager, CursorManager):
            raise TypeError("manager_class must be a subclass of "
                            "CursorManager")

        self.__cursor_manager = manager

    def __check_response_to_last_error(self, response):
        """Check a response to a lastError message for errors.

        `response` is a byte string representing a response to the message.
        If it represents an error response we raise OperationFailure.

        Return the response as a document.
        """
        response = helpers._unpack_response(response)

        assert response["number_returned"] == 1
        error = response["data"][0]

        helpers._check_command_response(error, self.disconnect)

        # TODO unify logic with database.error method
        if error.get("err") is None:
            return error
        if error["err"] == "not master":
            self.disconnect()
            raise AutoReconnect("not master")

        if "code" in error:
            if error["code"] in [11000, 11001, 12582]:
                raise DuplicateKeyError(error["err"])
            else:
                raise OperationFailure(error["err"], error["code"])
        else:
            raise OperationFailure(error["err"])

    def __check_bson_size(self, message):
        """Make sure the message doesn't include BSON documents larger
        than the connected server will accept.

        :Parameters:
          - `message`: message to check
        """
        if len(message) == 3:
            (request_id, data, max_doc_size) = message
            if max_doc_size > self.__max_bson_size:
                raise InvalidDocument("BSON document too large (%d bytes)"
                                      " - the connected server supports"
                                      " BSON document sizes up to %d"
                                      " bytes." %
                                      (max_doc_size, self.__max_bson_size))
            return (request_id, data)
        else:
            # get_more and kill_cursors messages
            # don't include BSON documents.
            return message

    def _send_message(self, message, with_last_error=False):
        """Say something to Mongo.

        Raises ConnectionFailure if the message cannot be sent. Raises
        OperationFailure if `with_last_error` is ``True`` and the
        response to the getLastError call returns an error. Return the
        response from lastError, or ``None`` if `with_last_error`
        is ``False``.

        :Parameters:
          - `message`: message to send
          - `with_last_error`: check getLastError status after sending the
            message
        """
        sock = self.__socket()
        try:
            (request_id, data) = self.__check_bson_size(message)
            sock.sendall(data)
            # Safe mode. We pack the message together with a lastError
            # message and send both. We then get the response (to the
            # lastError) and raise OperationFailure if it is an error
            # response.
            if with_last_error:
                response = self.__receive_message_on_socket(1, request_id,
                                                            sock)
                return self.__check_response_to_last_error(response)
            return None
        except (ConnectionFailure, socket.error), e:
            self.disconnect()
            raise AutoReconnect(str(e))

    def __receive_data_on_socket(self, length, sock):
        """Lowest level receive operation.

        Takes length to receive and repeatedly calls recv until able to
        return a buffer of that length, raising ConnectionFailure on error.
        """
        message = ""
        while len(message) < length:
            chunk = sock.recv(length - len(message))
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
        (request_id, data) = self.__check_bson_size(message)
        sock.sendall(data)
        return self.__receive_message_on_socket(1, request_id, sock)

    # we just ignore _must_use_master here: it's only relevant for
    # MasterSlaveConnection instances.
    def _send_message_with_response(self, message,
                                    _must_use_master=False, **kwargs):
        """Send a message to Mongo and return the response.

        Sends the given message and returns the response.

        :Parameters:
          - `message`: (request_id, data) pair making up the message to send
        """
        sock = self.__socket()

        try:
            try:
                if "network_timeout" in kwargs:
                    sock.settimeout(kwargs["network_timeout"])
                return self.__send_and_receive(message, sock)
            except (ConnectionFailure, socket.error), e:
                self.disconnect()
                raise AutoReconnect(str(e))
        finally:
            if "network_timeout" in kwargs:
                sock.settimeout(self.__network_timeout)

    def start_request(self):
        """DEPRECATED all operations will start a request.

        .. versionchanged:: 1.4
           DEPRECATED
        """
        warnings.warn("the Connection.start_request method is deprecated",
                      DeprecationWarning)

    def end_request(self):
        """Allow this thread's connection to return to the pool.

        Calling :meth:`end_request` allows the :class:`~socket.socket`
        that has been reserved for this thread to be returned to the
        pool. Other threads will then be able to re-use that
        :class:`~socket.socket`. If your application uses many
        threads, or has long-running threads that infrequently perform
        MongoDB operations, then judicious use of this method can lead
        to performance gains. Care should be taken, however, to make
        sure that :meth:`end_request` is not called in the middle of a
        sequence of operations in which ordering is important. This
        could lead to unexpected results.

        One important case is when a thread is dying permanently. It
        is best to call :meth:`end_request` when you know a thread is
        finished, as otherwise its :class:`~socket.socket` will not be
        reclaimed.
        """
        self.__pool.return_socket()

    def __cmp__(self, other):
        if isinstance(other, Connection):
            return cmp((self.__host, self.__port),
                       (other.__host, other.__port))
        return NotImplemented

    def __repr__(self):
        if len(self.__nodes) == 1:
            return "Connection(%r, %r)" % (self.__host, self.__port)
        else:
            return "Connection(%r)" % ["%s:%d" % n for n in self.__nodes]

    def __getattr__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return database.Database(self, name)

    def __getitem__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return self.__getattr__(name)

    def close_cursor(self, cursor_id):
        """Close a single database cursor.

        Raises :class:`TypeError` if `cursor_id` is not an instance of
        ``(int, long)``. What closing the cursor actually means
        depends on this connection's cursor manager.

        :Parameters:
          - `cursor_id`: id of cursor to close

        .. seealso:: :meth:`set_cursor_manager` and
           the :mod:`~pymongo.cursor_manager` module
        """
        if not isinstance(cursor_id, (int, long)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self.__cursor_manager.close(cursor_id)

    def kill_cursors(self, cursor_ids):
        """Send a kill cursors message with the given ids.

        Raises :class:`TypeError` if `cursor_ids` is not an instance of
        ``list``.

        :Parameters:
          - `cursor_ids`: list of cursor ids to kill
        """
        if not isinstance(cursor_ids, list):
            raise TypeError("cursor_ids must be a list")
        return self._send_message(message.kill_cursors(cursor_ids))

    def server_info(self):
        """Get information about the MongoDB server we're connected to.
        """
        return self.admin.command("buildinfo")

    def database_names(self):
        """Get a list of the names of all databases on the connected server.
        """
        return [db["name"] for db in
                self.admin.command("listDatabases")["databases"]]

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
        if isinstance(name, database.Database):
            name = name.name

        if not isinstance(name, basestring):
            raise TypeError("name_or_database must be an instance of "
                            "(Database, str, unicode)")

        self._purge_index(name)
        self[name].command("dropDatabase")

    def copy_database(self, from_name, to_name,
                      from_host=None, username=None, password=None):
        """Copy a database, potentially from another host.

        Raises :class:`TypeError` if `from_name` or `to_name` is not
        an instance of :class:`basestring`. Raises
        :class:`~pymongo.errors.InvalidName` if `to_name` is not a
        valid database name.

        If `from_host` is ``None`` the current host is used as the
        source. Otherwise the database is copied from `from_host`.

        If the source database requires authentication, `username` and
        `password` must be specified.

        :Parameters:
          - `from_name`: the name of the source database
          - `to_name`: the name of the target database
          - `from_host` (optional): host name to copy from
          - `username` (optional): username for source database
          - `password` (optional): password for source database

        .. note:: Specifying `username` and `password` requires server
           version **>= 1.3.3+**.

        .. versionadded:: 1.5
        """
        if not isinstance(from_name, basestring):
            raise TypeError("from_name must be an instance of basestring")
        if not isinstance(to_name, basestring):
            raise TypeError("to_name must be an instance of basestring")

        database._check_name(to_name)

        command = {"fromdb": from_name, "todb": to_name}

        if from_host is not None:
            command["fromhost"] = from_host

        if username is not None:
            nonce = self.admin.command("copydbgetnonce",
                                       fromhost=from_host)["nonce"]
            command["username"] = username
            command["nonce"] = nonce
            command["key"] = helpers._auth_key(nonce, username, password)

        return self.admin.command("copydb", **command)

    @property
    def is_locked(self):
        """Is this server locked? While locked, all write operations
        are blocked, although read operations are still allowed.
        Use :meth:`~pymongo.connection.Connection.unlock` to unlock.

        .. versionadded:: 1.11+
        """
        ops = self.admin.current_op()
        return bool(ops.get('fsyncLock', 0))

    def fsync(self, **kwargs):
        """Flush all pending writes to datafiles.

        :Parameters:

            Optional parameters can be passed as keyword arguments:

            - `lock`: If True lock the server to disallow writes.
            - `async`: If True don't block while synchronizing.

            .. warning:: `async` and `lock` can not be used together.

            .. warning:: `async` is not supported on Windows and will
                         raise an exception on that platform.

        .. versionadded:: 1.11+
        """
        self.admin.command("fsync", **kwargs)

    def unlock(self):
        """Unlock a previously locked server.

        .. versionadded:: 1.11+
        """
        self.admin['$cmd'].sys.unlock.find_one()

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Connection' object is not iterable")
