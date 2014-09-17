# Copyright 2009-2014 MongoDB, Inc.
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

.. seealso:: :doc:`/examples/high_availability` for an example of how to
   connect to a replica set, or specify a list of mongos instances for
   automatic failover.

To get a :class:`~pymongo.database.Database` instance from a
:class:`MongoClient` use either dictionary-style or attribute-style
access:

.. doctest::

  >>> from pymongo import MongoClient
  >>> c = MongoClient()
  >>> c.test_database
  Database(MongoClient('localhost', 27017), u'test_database')
  >>> c['test-database']
  Database(MongoClient('localhost', 27017), u'test-database')
"""

import datetime
import socket
import warnings

from bson.py3compat import (integer_types,
                            string_type)
from bson.son import SON
from pymongo import (auth,
                     common,
                     database,
                     helpers,
                     message,
                     pool,
                     thread_util,
                     uri_parser)
from pymongo.client_options import ClientOptions
from pymongo.topology_description import TOPOLOGY_TYPE
from pymongo.cursor_manager import CursorManager
from pymongo.topology import Topology
from pymongo.errors import (ConfigurationError,
                            ConnectionFailure,
                            InvalidURI, AutoReconnect, OperationFailure,
                            DuplicateKeyError, InvalidOperation,
                            NetworkTimeout,
                            NotMasterError)
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import (any_server_selector,
                                      writable_preferred_server_selector,
                                      writable_server_selector)
from pymongo.server_type import SERVER_TYPE
from pymongo.settings import TopologySettings


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


class MongoClient(common.BaseObject):
    HOST = "localhost"
    PORT = 27017

    def __init__(
            self,
            host=None,
            port=None,
            max_pool_size=100,
            document_class=dict,
            tz_aware=False,
            connect=True,
            **kwargs):
        """Client for a MongoDB instance, a replica set, or a set of mongoses.

        The client object is thread-safe and has connection-pooling built in.
        If an operation fails because of a network error,
        :class:`~pymongo.errors.ConnectionFailure` is raised and the client
        reconnects in the background. Application code should handle this
        exception (recognizing that the operation failed) and then continue to
        execute.

        The `host` parameter can be a full `mongodb URI
        <http://dochub.mongodb.org/core/connections>`_, in addition to
        a simple hostname. It can also be a list of hostnames or
        URIs. Any port specified in the host string(s) will override
        the `port` parameter. If multiple mongodb URIs containing
        database or auth information are passed, the last database,
        username, and password present will be used.  For username and
        passwords reserved characters like ':', '/', '+' and '@' must be
        escaped following RFC 2396.

        :Parameters:
          - `host` (optional): hostname or IP address of the
            instance to connect to, or a mongodb URI, or a list of
            hostnames / mongodb URIs. If `host` is an IPv6 literal
            it must be enclosed in '[' and ']' characters following
            the RFC2732 URL syntax (e.g. '[::1]' for localhost)
          - `port` (optional): port number on which to connect
          - `max_pool_size` (optional): The maximum number of connections
            that the pool will open simultaneously. If this is set, operations
            will block if there are `max_pool_size` outstanding connections
            from the pool. Defaults to 100.
          - `document_class` (optional): default class to use for
            documents returned from queries on this client
          - `tz_aware` (optional): if ``True``,
            :class:`~datetime.datetime` instances returned as values
            in a document by this :class:`MongoClient` will be timezone
            aware (otherwise they will be naive)
          - `connect` (optional): if ``True`` (the default), immediately
            begin connecting to MongoDB in the background. Otherwise connect
            on the first operation.

          | **Other optional parameters can be passed as keyword arguments:**

          - `socketTimeoutMS`: (integer) How long (in milliseconds) a send or
            receive on a socket can take before timing out. Defaults to ``None``
            (no timeout).
          - `connectTimeoutMS`: (integer) How long (in milliseconds) a
            connection can take to be opened before timing out. Defaults to
            ``20000``.
          - `waitQueueTimeoutMS`: (integer) How long (in milliseconds) a
            thread will wait for a socket from the pool if the pool has no
            free sockets. Defaults to ``None`` (no timeout).
          - `waitQueueMultiple`: (integer) Multiplied by max_pool_size to give
            the number of threads allowed to wait for a socket at one time.
            Defaults to ``None`` (no waiters).
          - `socketKeepAlive`: (boolean) Whether to send periodic keep-alive
            packets on connected sockets. Defaults to ``False`` (do not send
            keep-alive packets).

          | **Write Concern options:**

          - `w`: (integer or string) If this is a replica set, write operations
            will block until they have been replicated to the specified number
            or tagged set of servers. `w=<int>` always includes the replica set
            primary (e.g. w=3 means write to the primary and wait until
            replicated to **two** secondaries). Passing w=0 **disables write
            acknowledgement** and all other write concern options.
          - `wtimeout`: (integer) Used in conjunction with `w`. Specify a value
            in milliseconds to control how long to wait for write propagation
            to complete. If replication does not complete in the given
            timeframe, a timeout exception is raised.
          - `j`: If ``True`` block until write operations have been committed
            to the journal. Cannot be used in combination with `fsync`. Prior
            to MongoDB 2.6 this option was ignored if the server was running
            without journaling. Starting with MongoDB 2.6 write operations will
            fail with an exception if this option is used when the server is
            running without journaling.
          - `fsync`: If ``True`` and the server is running without journaling,
            blocks until the server has synced all data files to disk. If the
            server is running with journaling, this acts the same as the `j`
            option, blocking until write operations have been committed to the
            journal. Cannot be used in combination with `j`.

          | **Replica set keyword arguments for connecting with a replica set
            - either directly or via a mongos:**

          - `replicaSet`: (string) The name of the replica set to connect to.
            The driver will verify that all servers it connects to match
            this name. Implies that the hosts specified are a seed list and the
            driver should attempt to find all members of the set.
          - `read_preference`: The read preference for this client. If
            connecting directly to a secondary then a read preference mode
            *other* than PRIMARY is required - otherwise all queries will throw
            :class:`~pymongo.errors.AutoReconnect` "not master".
            See :class:`~pymongo.read_preferences.ReadPreference` for all
            available read preference options.

          | **SSL configuration:**

          - `ssl`: If ``True``, create the connection to the server using SSL.
          - `ssl_keyfile`: The private keyfile used to identify the local
            connection against mongod.  If included with the ``certfile`` then
            only the ``ssl_certfile`` is needed.  Implies ``ssl=True``.
          - `ssl_certfile`: The certificate file used to identify the local
            connection against mongod. Implies ``ssl=True``.
          - `ssl_cert_reqs`: Specifies whether a certificate is required from
            the other side of the connection, and whether it will be validated
            if provided. It must be one of the three values ``ssl.CERT_NONE``
            (certificates ignored), ``ssl.CERT_OPTIONAL``
            (not required, but validated if provided), or ``ssl.CERT_REQUIRED``
            (required and validated). If the value of this parameter is not
            ``ssl.CERT_NONE``, then the ``ssl_ca_certs`` parameter must point
            to a file of CA certificates. Implies ``ssl=True``.
          - `ssl_ca_certs`: The ca_certs file contains a set of concatenated
            "certification authority" certificates, which are used to validate
            certificates passed from the other end of the connection.
            Implies ``ssl=True``.

        .. mongodoc:: connections

        .. versionchanged:: 3.0
           :class:`~pymongo.mongo_client.MongoClient` is now the one and only
           client class for a standalone server, mongos, or replica set.
           It includes the functionality that had been split into
           :class:`~pymongo.mongo_client.MongoReplicaSetClient`: it can connect
           to a replica set, discover all its members, and monitor the set for
           stepdowns, elections, and reconfigs.

           The :class:`~pymongo.mongo_client.MongoClient` constructor no
           longer blocks while connecting to the server or servers, and it no
           longer raises :class:`~pymongo.errors.ConnectionFailure` if they
           are unavailable, nor :class:`~pymongo.errors.ConfigurationError`
           if the user's credentials are wrong. Instead, the constructor
           returns immediately and launches the connection process on
           background threads.

           In PyMongo 2.x, :class:`~pymongo.MongoClient` accepted a list of
           standalone MongoDB servers and used the first it could connect to::

               MongoClient(['host1.com:27017', 'host2.com:27017'])

           A list of multiple standalones is no longer supported; if multiple
           servers are listed they must be members of the same replica set, or
           mongoses in the same sharded cluster.

           The ``connect`` option is added and ``auto_start_request`` is
           removed.
        """
        if host is None:
            host = self.HOST
        if isinstance(host, string_type):
            host = [host]
        if port is None:
            port = self.PORT
        if not isinstance(port, int):
            raise TypeError("port must be an instance of int")

        seeds = set()
        username = None
        password = None
        dbase = None
        opts = {}
        for entity in host:
            if "://" in entity:
                if entity.startswith("mongodb://"):
                    res = uri_parser.parse_uri(entity, port, False)
                    seeds.update(res["nodelist"])
                    username = res["username"] or username
                    password = res["password"] or password
                    dbase = res["database"] or dbase
                    opts = res["options"]
                else:
                    idx = entity.find("://")
                    raise InvalidURI("Invalid URI scheme: "
                                     "%s" % (entity[:idx],))
            else:
                seeds.update(uri_parser.split_hosts(entity, port))
        if not seeds:
            raise ConfigurationError("need to specify at least one host")

        # _pool_class, _monitor_class, and _condition_class are for deep
        # customization of PyMongo, e.g. Motor.
        pool_class = kwargs.pop('_pool_class', None)
        monitor_class = kwargs.pop('_monitor_class', None)
        condition_class = kwargs.pop('_condition_class', None)

        kwargs['max_pool_size'] = max_pool_size
        opts.update(kwargs)
        self.__options = options = ClientOptions(
            username, password, dbase, opts)

        self.__default_database_name = dbase
        self.__cursor_manager = None
        self.__document_class = document_class
        self.__tz_aware = common.validate_boolean('tz_aware', tz_aware)

        # Cache of existing indexes used by ensure_index ops.
        self.__index_cache = {}

        super(MongoClient, self).__init__(options.read_preference,
                                          options.uuid_subtype,
                                          options.write_concern.document)

        self.__request_counter = thread_util.Counter()
        self.__all_credentials = {}
        creds = options.credentials
        if creds:
            self._cache_credentials(creds.source, creds)

        self._topology_settings = TopologySettings(
            seeds=seeds,
            set_name=options.replica_set_name,
            pool_class=pool_class,
            pool_options=options.pool_options,
            monitor_class=monitor_class,
            condition_class=condition_class)

        self._topology = Topology(self._topology_settings)
        if connect:
            self._topology.open()

    def _cache_credentials(self, source, credentials, connect=False):
        """Save a set of authentication credentials.

        The credentials are used to login a socket whenever one is created.
        If `connect` is True, verify the credentials on the server first.
        """
        # Don't let other threads affect this call's data.
        all_credentials = self.__all_credentials.copy()

        if source in all_credentials:
            # Nothing to do if we already have these credentials.
            if credentials == all_credentials[source]:
                return
            raise OperationFailure('Another user is already authenticated '
                                   'to this database. You must logout first.')

        if connect:
            server = self._get_topology().select_server(
                writable_preferred_server_selector)

            # get_socket() logs out of the database if logged in with old
            # credentials, and logs in with new ones.
            with server.pool.get_socket(all_credentials) as sock_info:
                sock_info.authenticate(credentials)

        # If several threads run _cache_credentials at once, last one wins.
        self.__all_credentials[source] = credentials

    def _purge_credentials(self, source):
        """Purge credentials from the authentication cache."""
        self.__all_credentials.pop(source, None)

    def _cached(self, dbname, coll, index):
        """Test if `index` is cached."""
        cache = self.__index_cache
        now = datetime.datetime.utcnow()
        return (dbname in cache and
                coll in cache[dbname] and
                index in cache[dbname][coll] and
                now < cache[dbname][coll][index])

    def _cache_index(self, database, collection, index, cache_for):
        """Add an index to the index cache for ensure_index operations."""
        now = datetime.datetime.utcnow()
        expire = datetime.timedelta(seconds=cache_for) + now

        if database not in self.__index_cache:
            self.__index_cache[database] = {}
            self.__index_cache[database][collection] = {}
            self.__index_cache[database][collection][index] = expire

        elif collection not in self.__index_cache[database]:
            self.__index_cache[database][collection] = {}
            self.__index_cache[database][collection][index] = expire

        else:
            self.__index_cache[database][collection][index] = expire

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

    def _server_property(self, attr_name, default=None):
        """An attribute of the current server's description.

        Returns "default" while there is no current server, primary, or mongos.

        Not threadsafe if used multiple times in a single method, since
        the server may change. In such cases, store a local reference to a
        ServerDescription first, then use its properties.
        """
        try:
            server = self._topology.select_server(
                writable_server_selector, server_wait_time=0)

            return getattr(server.description, attr_name)
        except ConnectionFailure:
            return default

    @property
    def host(self):
        """Hostname of the standalone, primary, or mongos currently in use.

        .. warning:: An application that accesses :attr:`host` and :attr:`port`
           is vulnerable to a race condition, if the client switches to a
           new primary or mongos in between. Use :attr:`address` instead.
        """
        address = self.address
        return address[0] if address else None

    @property
    def port(self):
        """Port of the standalone, primary, or mongos currently in use.

        .. warning:: An application that accesses :attr:`host` and :attr:`port`
           is vulnerable to a race condition, if the client switches to a
           new primary or mongos in between. Use :attr:`address` instead.
        """
        address = self.address
        return address[1] if address else None

    @property
    def address(self):
        """(host, port) of the current standalone, primary, or mongos, or None.

        .. versionadded:: 3.0
        """
        return self._server_property('address')

    @property
    def primary(self):
        """The (host, port) of the current primary of the replica set.

        Returns None if there is no primary.

        .. versionadded:: 3.0
           MongoClient gained this property in version 3.0 when
           MongoReplicaSetClient's functionality was merged in.
        """
        return self._topology.get_primary()

    @property
    def secondaries(self):
        """The secondary members known to this client.

        A sequence of (host, port) pairs.

        .. versionadded:: 3.0
           MongoClient gained this property in version 3.0 when
           MongoReplicaSetClient's functionality was merged in.
        """
        return self._topology.get_secondaries()

    @property
    def arbiters(self):
        """Arbiters in the replica set.

        A sequence of (host, port) pairs. Empty if this client is not
        connected to a replica set.
        """
        return self._topology.get_arbiters()

    @property
    def is_primary(self):
        """If the current server can accept writes.

        True if the current server is a standalone, mongos, or the primary of
        a replica set.
        """
        return self._server_property('is_writable', False)

    @property
    def is_mongos(self):
        """If this client is connected to mongos.
        """
        return self._server_property('server_type') == SERVER_TYPE.Mongos

    @property
    def max_pool_size(self):
        """The maximum number of sockets the pool will open concurrently.

        When the pool has reached `max_pool_size`, operations block waiting for
        a socket to be returned to the pool. If ``waitQueueTimeoutMS`` is set,
        a blocked operation will raise :exc:`~pymongo.errors.ConnectionFailure`
        after a timeout. By default ``waitQueueTimeoutMS`` is not set.
        """
        return self.__options.pool_options.max_pool_size

    @property
    def nodes(self):
        """List of all connected servers.

        Nodes are either specified when this instance was created,
        or discovered through the replica set discovery mechanism.
        """
        description = self._topology.description
        return frozenset(s.address for s in description.known_servers)

    @property
    def document_class(self):
        """Default class to use for documents returned from this client.

        .. versionchanged:: 3.0
           Now read-only.
        """
        return self.__document_class

    def get_document_class(self):
        """Default class to use for documents returned from this client.

        Deprecated; use the document_class property instead.
        """
        warnings.warn('get_document_class() is deprecated, use the'
                      ' document_class property',
                      DeprecationWarning, stacklevel=2)

        return self.__document_class

    @property
    def tz_aware(self):
        """Does this client return timezone-aware datetimes?
        """
        return self.__tz_aware

    @property
    def max_bson_size(self):
        """The largest BSON object the connected server accepts in bytes.

        Defaults to 16MB if not connected to a server.
        """
        return self._server_property('max_bson_size', common.MAX_BSON_SIZE)

    @property
    def max_message_size(self):
        """The largest message the connected server accepts in bytes.

        Defaults to 32MB if not connected to a server.
        """
        return self._server_property(
            'max_message_size', common.MAX_MESSAGE_SIZE)

    @property
    def min_wire_version(self):
        """The minWireVersion reported by the server.

        Returns ``0`` when connected to server versions prior to MongoDB 2.6.
        """
        return self._server_property(
            'min_wire_version', common.MIN_WIRE_VERSION)

    @property
    def max_wire_version(self):
        """The maxWireVersion reported by the server.

        Returns ``0`` when connected to server versions prior to MongoDB 2.6.
        """
        return self._server_property(
            'max_wire_version', common.MAX_WIRE_VERSION)

    @property
    def max_write_batch_size(self):
        """The maxWriteBatchSize reported by the server.

        Returns a default value when connected to server versions prior to
        MongoDB 2.6.
        """
        return self._server_property(
            'max_write_batch_size', common.MAX_WRITE_BATCH_SIZE)

    def _writable_max_wire_version(self):
        """Connect to a writable server and get its max wire protocol version.

        Can raise ConnectionFailure.
        """
        topology = self._get_topology()  # Starts monitors if necessary.
        server = topology.select_server(writable_server_selector)
        return server.description.max_wire_version

    def _is_writable(self):
        """Attempt to connect to a writable server, or return False.
        """
        topology = self._get_topology()  # Starts monitors if necessary.
        try:
            s = topology.select_server(writable_server_selector)

            # When directly connected to a secondary, arbiter, etc.,
            # select_server returns it, whatever the selector. Check
            # again if the server is writable.
            return s.description.is_writable
        except ConnectionFailure:
            return False

    def disconnect(self):
        """Disconnect from MongoDB.

        Disconnecting will close all underlying sockets in the connection
        pools. If this instance is used again it will be automatically
        re-opened.
        """
        self._topology.reset()

    def close(self):
        """Alias for :meth:`disconnect`

        Disconnecting will close all underlying sockets in the connection
        pools. If this instance is used again it will be automatically
        re-opened.
        """
        self.disconnect()

    def alive(self):
        """Return ``False`` if there has been an error communicating with the
        server, else ``True``.

        This method attempts to check the status of the server (the standalone,
        replica set primary, or the mongos currently in use) with minimal I/O.
        Retrieves a socket from the pool and checks whether calling `select`_
        on it raises an error. If there are currently no idle sockets,
        :meth:`alive` attempts to actually connect to the server.

        A more certain way to determine server availability is::

            client.admin.command('ping')

        .. _select: http://docs.python.org/2/library/select.html#select.select
        """
        # In the common case, a socket is available and was used recently, so
        # calling select() on it is a reasonable attempt to see if the OS has
        # reported an error.
        try:
            # TODO: Mongos pinning.
            server = self._topology.select_server(
                writable_server_selector,
                server_wait_time=0)

            # Avoid race when other threads log in or out.
            all_credentials = self.__all_credentials.copy()
            with server.pool.get_socket(all_credentials) as sock_info:
                return not pool._closed(sock_info.sock)

        except (socket.error, ConnectionFailure):
            return False

    def set_cursor_manager(self, manager_class):
        """Set this client's cursor manager.

        Raises :class:`TypeError` if `manager_class` is not a subclass of
        :class:`~pymongo.cursor_manager.CursorManager`. A cursor manager
        handles closing cursors. Different managers can implement different
        policies in terms of when to actually kill a cursor that has
        been closed.

        :Parameters:
          - `manager_class`: cursor manager to use

        .. versionchanged:: 3.0
           Undeprecated.
        """
        manager = manager_class(self)
        if not isinstance(manager, CursorManager):
            raise TypeError("manager_class must be a subclass of "
                            "CursorManager")

        self.__cursor_manager = manager

    def _get_topology(self):
        """Get the internal :class:`~pymongo.topology.Topology` object.

        If this client was created with "connect=False", calling _get_topology
        launches the connection process in the background.
        """
        self._topology.open()
        return self._topology

    def __check_gle_response(self, response, is_command):
        """Check a response to a lastError message for errors.

        `response` is a byte string representing a response to the message.
        If it represents an error response we raise OperationFailure.

        Return the response as a document.
        """
        response = helpers._unpack_response(response)

        assert response["number_returned"] == 1
        result = response["data"][0]

        # Raises NotMasterError or OperationFailure.
        helpers._check_command_response(result)

        # write commands - skip getLastError checking
        if is_command:
            return result

        # getLastError
        error_msg = result.get("err", "")
        if error_msg is None:
            return result
        if error_msg.startswith("not master"):
            raise NotMasterError(error_msg)

        details = result
        # mongos returns the error code in an error object
        # for some errors.
        if "errObjects" in result:
            for errobj in result["errObjects"]:
                if errobj.get("err") == error_msg:
                    details = errobj
                    break

        code = details.get("code")
        if code in (11000, 11001, 12582):
            raise DuplicateKeyError(details["err"], code, result)
        raise OperationFailure(details["err"], code, result)

    def _send_message(
            self, message, with_last_error=False, command=False,
            check_primary=True, address=None):
        """Send a message to MongoDB, optionally returning response as a dict.

        Raises ConnectionFailure if the message cannot be sent. Raises
        OperationFailure if `with_last_error` is ``True`` and the
        response to the getLastError call returns an error. Return the
        response from lastError, or ``None`` if `with_last_error`
        is ``False``.

        :Parameters:
          - `message`: (request_id, data).
          - `with_last_error` (optional): check getLastError status after
            sending the message.
          - `check_primary` (optional): don't try to write to a non-primary;
            see kill_cursors for an exception to this rule.
          - `command` (optional): True for a write command.
          - `address` (optional): Optional address when sending a getMore or
            killCursors to a specific server.
        """
        topology = self._get_topology()
        if address:
            assert not check_primary, "Can't use check_primary with address"
            server = topology.get_server_by_address(address)
            if not server:
                raise AutoReconnect('server %s:%d no longer available'
                                    % address)
        else:
            server = topology.select_server(writable_server_selector)

        is_writable = server.description.is_writable
        if check_primary and not with_last_error and not is_writable:
            # When directly connected to a single server, we select it even
            # if it isn't writable. The write won't succeed, so bail as if
            # we'd done a getLastError.
            raise AutoReconnect("not master")

        if self.in_request() and not server.in_request():
            server.start_request()

        if with_last_error or command:
            response = self._reset_on_error(
                server,
                server.send_message_with_response,
                message,
                self.__all_credentials)

            try:
                return self.__check_gle_response(response.data, command)
            except NotMasterError:
                address = server.description.address
                self._reset_server_and_request_check(address)
                raise

        else:
            # Send the message. No response.
            self._reset_on_error(
                server,
                server.send_message,
                message,
                self.__all_credentials)

    def _send_message_with_response(
            self, message, read_preference=None, exhaust=False, address=None):
        """Send a message to MongoDB and return a Response.

        :Parameters:
          - `message`: (request_id, data, max_doc_size) or (request_id, data).
          - `read_preference` (optional): A ReadPreference.
          - `exhaust` (optional): If True, the socket used stays checked out.
            It is returned along with its Pool in the Response.
        """
        topology = self._get_topology()
        if address:
            server = topology.get_server_by_address(address)
            if not server:
                raise AutoReconnect('server %s:%d no longer available'
                                    % address)
        else:
            if read_preference:
                selector = read_preference.select_servers
            else:
                selector = writable_server_selector

            server = topology.select_server(selector)

        if self.in_request() and not server.in_request():
            server.start_request()

        return self._reset_on_error(
            server,
            server.send_message_with_response,
            message,
            self.__all_credentials,
            exhaust)

    def _reset_on_error(self, server, fn, *args, **kwargs):
        """Execute an operation. Reset the server on network error.

        Returns fn()'s return value on success. On error, clears the server's
        pool and marks the server Unknown.

        Re-raises any exception thrown by fn().
        """
        try:
            return fn(*args, **kwargs)
        except NetworkTimeout:
            # The socket has been closed. Don't reset the server.
            raise
        except ConnectionFailure:
            self.__reset_server(server.description.address)
            raise

    def __reset_server(self, address):
        """Clear our connection pool for a server and mark it Unknown."""
        self._topology.reset_server(address)

    def _reset_server_and_request_check(self, address):
        """Clear our pool for a server, mark it Unknown, and check it soon."""
        self._topology.reset_server(address)
        server = self._topology.get_server_by_address(address)

        # "server" is None if another thread removed it from the topology.
        if server:
            server.request_check()

    def start_request(self):
        """Ensure the current thread always uses the same socket
        until it calls :meth:`end_request`. This ensures consistent reads,
        even if you read after an unacknowledged write.

        :meth:`start_request` can be used as a context manager:

        >>> client = pymongo.MongoClient()
        >>> db = client.test
        >>> _id = db.test_collection.insert({})
        >>> with client.start_request():
        ...     for i in range(100):
        ...         db.test_collection.update({'_id': _id}, {'$set': {'i':i}})
        ...
        ...     # Definitely read the document after the final update completes
        ...     print db.test_collection.find({'_id': _id})

        If a thread calls start_request multiple times, an equal
        number of calls to :meth:`end_request` is required to end the request.
        """
        # TODO: Remove implicit threadlocal requests, use explicit requests.
        # TODO: Start / end replica set member pinning?
        if 1 == self.__request_counter.inc():
            # Start requests on all existing pools. New pools created while
            # this thread is in a request will have start_request() called
            # lazily. These greedy calls are to make PyMongo 2.x's request
            # tests pass.
            try:
                servers = self._topology.select_servers(any_server_selector,
                                                       server_wait_time=0)

                for s in servers:
                    s.start_request()
            except AutoReconnect:
                # No servers available.
                pass

        return pool.Request(self)

    def in_request(self):
        """True if this thread is in a request, meaning it has a socket
        reserved for its exclusive use.
        """
        return bool(self.__request_counter.get())

    def end_request(self):
        """Undo :meth:`start_request`. If :meth:`end_request` is called as many
        times as :meth:`start_request`, the request is over and this thread's
        connection returns to the pool. Extra calls to :meth:`end_request` have
        no effect.
        """
        if 0 == self.__request_counter.dec():
            try:
                servers = self._topology.select_servers(any_server_selector,
                                                       server_wait_time=0)

                for s in servers:
                    s.end_request()
            except ConnectionFailure:
                # No servers, we've disconnected.
                pass

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.address == other.address
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        server_descriptions = self._topology.description.server_descriptions()
        if len(server_descriptions) == 1:
            description, = server_descriptions.values()
            return "MongoClient(%r, %r)" % description.address
        else:
            return "MongoClient(%r)" % [
                "%s:%d" % address for address in server_descriptions]

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

    def close_cursor(self, cursor_id, address=None):
        """Close a single database cursor.

        Raises :class:`TypeError` if `cursor_id` is not an instance of
        ``(int, long)``. What closing the cursor actually means
        depends on this client's cursor manager.

        :Parameters:
          - `cursor_id`: id of cursor to close
          - `address` (optional): (host, port) pair of the cursor's server

        .. versionchanged:: 3.0
           Added ``address`` parameter.
        """
        if not isinstance(cursor_id, integer_types):
            raise TypeError("cursor_id must be an instance of (int, long)")

        # TODO: update this, pass address to cursor_manager.close().
        # PyMongo 2.x introduced a configurable CursorManager which sends
        # OP_KILLCURSORS to the server. The API doesn't handle a multi-server
        # topology, where we must pass the address of the server that receives
        # the message. Support CursorManager for backwards compatibility, but
        # only for single servers.
        if self.__cursor_manager:
            topology_type = self._topology.description.topology_type
            if topology_type not in (TOPOLOGY_TYPE.Single, TOPOLOGY_TYPE.Sharded):
                raise InvalidOperation(
                    "Can't use custom CursorManager with topology type %s" %
                    TOPOLOGY_TYPE._fields[topology_type])

            self.__cursor_manager.close(cursor_id)
        else:
            return self._send_message(
                message.kill_cursors([cursor_id]),
                check_primary=False,
                address=address)

    def kill_cursors(self, cursor_ids):
        """Send a kill cursors message with the given ids to the primary.

        Raises :class:`TypeError` if `cursor_ids` is not an instance of
        ``list``.

        :Parameters:
          - `cursor_ids`: list of cursor ids to kill
        """
        if not isinstance(cursor_ids, list):
            raise TypeError("cursor_ids must be a list")
        return self._send_message(
            message.kill_cursors(cursor_ids), check_primary=False)

    def server_info(self):
        """Get information about the MongoDB server we're connected to."""
        return self.admin.command("buildinfo",
                                  read_preference=ReadPreference.PRIMARY)

    def database_names(self):
        """Get a list of the names of all databases on the connected server."""
        return [db["name"] for db in
                self.admin.command("listDatabases",
                read_preference=ReadPreference.PRIMARY)["databases"]]

    def drop_database(self, name_or_database):
        """Drop a database.

        Raises :class:`TypeError` if `name_or_database` is not an instance of
        :class:`basestring` (:class:`str` in python 3) or
        :class:`~pymongo.database.Database`.

        :Parameters:
          - `name_or_database`: the name of a database to drop, or a
            :class:`~pymongo.database.Database` instance representing the
            database to drop
        """
        name = name_or_database
        if isinstance(name, database.Database):
            name = name.name

        if not isinstance(name, string_type):
            raise TypeError("name_or_database must be an instance "
                            "of %s or a Database" % (string_type.__name__,))

        self._purge_index(name)
        self[name].command("dropDatabase",
                           read_preference=ReadPreference.PRIMARY)

    def copy_database(self, from_name, to_name,
                      from_host=None, username=None, password=None):
        """Copy a database, potentially from another host.

        Raises :class:`TypeError` if `from_name` or `to_name` is not
        an instance of :class:`basestring` (:class:`str` in python 3).
        Raises :class:`~pymongo.errors.InvalidName` if `to_name` is
        not a valid database name.

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
        """
        if not isinstance(from_name, string_type):
            raise TypeError("from_name must be an "
                            "instance of %s" % (string_type.__name__,))
        if not isinstance(to_name, string_type):
            raise TypeError("to_name must be an "
                            "instance of %s" % (string_type.__name__,))

        database._check_name(to_name)

        command = SON([
            ("copydb", 1), ("fromdb", from_name), ("todb", to_name)])

        if from_host is not None:
            command["fromhost"] = from_host

        # _get_topology() starts connecting, if we initialized with
        # connect=False.
        server = self._get_topology().select_server(
            writable_server_selector)

        if self.in_request() and not server.in_request():
            server.start_request()

        # Avoid race when other threads log in or out.
        all_credentials = self.__all_credentials.copy()
        with server.pool.get_socket(all_credentials) as sock:
            if username is not None:
                get_nonce_cmd = SON([("copydbgetnonce", 1),
                                     ("fromhost", from_host)])

                nonce = sock.command("admin", get_nonce_cmd)["nonce"]

                command["username"] = username
                command["nonce"] = nonce
                command["key"] = auth._auth_key(nonce, username, password)

            return sock.command("admin", command)

    def get_default_database(self):
        """Get the database named in the MongoDB connection URI.

        >>> uri = 'mongodb://host/my_database'
        >>> client = MongoClient(uri)
        >>> db = client.get_default_database()
        >>> assert db.name == 'my_database'

        Useful in scripts where you want to choose which database to use
        based only on the URI in a configuration file.
        """
        if self.__default_database_name is None:
            raise ConfigurationError('No default database defined')

        return self[self.__default_database_name]

    @property
    def is_locked(self):
        """Is this server locked? While locked, all write operations
        are blocked, although read operations may still be allowed.
        Use :meth:`unlock` to unlock.
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

            .. warning:: MongoDB does not support the `async` option
                         on Windows and will raise an exception on that
                         platform.
        """
        self.admin.command("fsync",
                           read_preference=ReadPreference.PRIMARY, **kwargs)

    def unlock(self):
        """Unlock a previously locked server.
        """
        self.admin['$cmd'].sys.unlock.find_one()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'MongoClient' object is not iterable")

    __next__ = next
