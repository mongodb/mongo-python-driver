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
import threading
import warnings
import weakref
from collections import defaultdict

from bson.py3compat import (integer_types,
                            string_type)
from pymongo import (common,
                     database,
                     helpers,
                     message,
                     periodic_executor,
                     pool,
                     uri_parser)
from pymongo.client_options import ClientOptions
from pymongo.cursor_manager import CursorManager
from pymongo.network import socket_closed
from pymongo.topology import Topology
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            DuplicateKeyError,
                            InvalidURI,
                            NetworkTimeout,
                            NotMasterError,
                            OperationFailure)
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import (writable_preferred_server_selector,
                                      writable_server_selector)
from pymongo.server_type import SERVER_TYPE
from pymongo.settings import TopologySettings


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

          - `socketTimeoutMS`: (integer or None) How long (in milliseconds) a
            send or receive on a socket can take before timing out. Defaults to
            ``None`` (no timeout).
          - `connectTimeoutMS`: (integer or None) How long (in milliseconds) a
            connection can take to be opened before timing out. Defaults to
            ``20000``.
          - `waitQueueTimeoutMS`: (integer or None) How long (in milliseconds)
            a thread will wait for a socket from the pool if the pool has no
            free sockets. Defaults to ``None`` (no timeout).
          - `waitQueueMultiple`: (integer or None) Multiplied by max_pool_size
            to give the number of threads allowed to wait for a socket at one
            time. Defaults to ``None`` (no waiters).
          - `socketKeepAlive`: (boolean) Whether to send periodic keep-alive
            packets on connected sockets. Defaults to ``False`` (do not send
            keep-alive packets).

          | **Write Concern options:**
          | (Only set if passed. No default values.)

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

          - `replicaSet`: (string or None) The name of the replica set to
            connect to. The driver will verify that all servers it connects to
            match this name. Implies that the hosts specified are a seed list
            and the driver should attempt to find all members of the set.
            Defaults to ``None``.
          - `read_preference`: The read preference for this client. If
            connecting directly to a secondary then a read preference mode
            *other* than PRIMARY is required - otherwise all queries will throw
            :class:`~pymongo.errors.AutoReconnect` "not master".
            See :class:`~pymongo.read_preferences.ReadPreference` for all
            available read preference options. Defaults to ``PRIMARY``.

          | **SSL configuration:**

          - `ssl`: If ``True``, create the connection to the server using SSL.
            Defaults to ``False``.
          - `ssl_keyfile`: The private keyfile used to identify the local
            connection against mongod.  If included with the ``certfile`` then
            only the ``ssl_certfile`` is needed.  Implies ``ssl=True``.
            Defaults to ``None``.
          - `ssl_certfile`: The certificate file used to identify the local
            connection against mongod. Implies ``ssl=True``. Defaults to
            ``None``.
          - `ssl_cert_reqs`: Specifies whether a certificate is required from
            the other side of the connection, and whether it will be validated
            if provided. It must be one of the three values ``ssl.CERT_NONE``
            (certificates ignored), ``ssl.CERT_OPTIONAL``
            (not required, but validated if provided), or ``ssl.CERT_REQUIRED``
            (required and validated). If the value of this parameter is not
            ``ssl.CERT_NONE``, then the ``ssl_ca_certs`` parameter must point
            to a file of CA certificates. Implies ``ssl=True``. Defaults to
            ``ssl.CERT_NONE``.
          - `ssl_ca_certs`: The ca_certs file contains a set of concatenated
            "certification authority" certificates, which are used to validate
            certificates passed from the other end of the connection.
            Implies ``ssl=True``. Defaults to ``None``.

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

           The ``connect`` option is added.

           The ``start_request``, ``in_request``, and ``end_request`` methods
           are removed, as well as the ``auto_start_request`` option.

           The ``copy_database`` method is removed, see the
           :doc:`copy_database examples </examples/copydb>` for alternatives.

           :class:`~pymongo.mongo_client.MongoClient` no longer returns an
           instance of :class:`~pymongo.database.Database` for attribute names
           with leading underscores. You must use dict-style lookups instead::

               client['__my_database__']

           Not:

               client.__my_database__
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
        kwargs['document_class'] = document_class
        kwargs['tz_aware'] = tz_aware
        opts.update(kwargs)
        self.__options = options = ClientOptions(
            username, password, dbase, opts)

        self.__default_database_name = dbase
        self.__lock = threading.Lock()
        self.__cursor_manager = CursorManager(self)
        self.__kill_cursors_queue = []

        # Cache of existing indexes used by ensure_index ops.
        self.__index_cache = {}

        super(MongoClient, self).__init__(options.codec_options,
                                          options.read_preference,
                                          options.write_concern)

        self.__all_credentials = {}
        creds = options.credentials
        if creds:
            self._cache_credentials(creds.source, creds)

        self._topology_settings = TopologySettings(
            seeds=seeds,
            replica_set_name=options.replica_set_name,
            pool_class=pool_class,
            pool_options=options.pool_options,
            monitor_class=monitor_class,
            condition_class=condition_class,
            local_threshold_ms=options.local_threshold_ms)

        self._topology = Topology(self._topology_settings)
        if connect:
            self._topology.open()

        # We strongly reference the executor and it weakly references us via
        # this closure. When the client is freed, stop the executor.
        self_ref = weakref.ref(self)

        def target():
            client = self_ref()
            if client is None:
                return False  # Stop the executor.
            MongoClient._process_kill_cursors_queue(client)
            return True

        self._kill_cursors_executor = periodic_executor.PeriodicExecutor(
            condition_class=self._topology_settings.condition_class,
            interval=common.KILL_CURSOR_FREQUENCY,
            min_interval=0,
            target=target)

        self._kill_cursors_executor.open()

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
            with server.get_socket(all_credentials) as sock_info:
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
        return self.__options.codec_options.as_class

    def get_document_class(self):
        """Default class to use for documents returned from this client.

        Deprecated; use the document_class property instead.
        """
        warnings.warn('get_document_class() is deprecated, use the'
                      ' document_class property',
                      DeprecationWarning, stacklevel=2)

        return self.__options.codec_options.as_class

    @property
    def tz_aware(self):
        """Does this client return timezone-aware datetimes?
        """
        return self.__options.codec_options.tz_aware

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

    @property
    def local_threshold_ms(self):
        """The local threshold for this instance."""
        return self.__options.local_threshold_ms

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
        self._topology.close()

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
            with server.get_socket(all_credentials) as sock_info:
                return not socket_closed(sock_info.sock)

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
          - `address` (optional): Optional address when sending a message
            to a specific server, used for killCursors.
        """
        with self.__lock:
            # If needed, restart kill-cursors thread after a fork.
            self._kill_cursors_executor.open()

        topology = self._get_topology()
        if address:
            assert not check_primary, "Can't use check_primary with address"
            server = topology.select_server_by_address(address)
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
            raise NotMasterError("not master")

        if with_last_error or command:
            response = self._reset_on_error(
                server,
                server.send_message_with_response,
                message,
                self.__all_credentials)

            try:
                return self.__check_gle_response(response.data, command)
            except NotMasterError:
                self._reset_server_and_request_check(
                    server.description.address)

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
          - `address` (optional): Optional address when sending a message
            to a specific server, used for getMore.
        """
        with self.__lock:
            # If needed, restart kill-cursors thread after a fork.
            self._kill_cursors_executor.open()

        topology = self._get_topology()
        if address:
            server = topology.select_server_by_address(address)
            if not server:
                raise AutoReconnect('server %s:%d no longer available'
                                    % address)
        else:
            selector = read_preference or writable_server_selector
            server = topology.select_server(selector)

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
        self._topology.reset_server_and_request_check(address)

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
        if name.startswith('_'):
            raise AttributeError(
                "MongoClient has no attribute %r. To access the %s"
                " database, use client[%r]." % (name, name, name))
        return self.__getitem__(name)

    def __getitem__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return database.Database(self, name)

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

        self.__cursor_manager.close(cursor_id, address)

    def kill_cursors(self, cursor_ids, address=None):
        """Send a kill cursors message soon with the given ids.

        Raises :class:`TypeError` if `cursor_ids` is not an instance of
        ``list``.

        This method may be called from a :class:`~pymongo.cursor.Cursor`
        destructor during garbage collection, so it isn't safe to take a
        lock or do network I/O. Instead, we schedule the cursor to be closed
        soon on a background thread.

        :Parameters:
          - `cursor_ids`: list of cursor ids to kill
          - `address` (optional): (host, port) of server to send message to

        .. versionchanged:: 3.0
           Now accepts an `address` argument. Schedules the cursors to be
           closed on a background thread instead of sending the message
           immediately.
        """
        if not isinstance(cursor_ids, list):
            raise TypeError("cursor_ids must be a list")

        # "Atomic", needs no lock.
        self.__kill_cursors_queue.append((address, cursor_ids))

    # This method is run periodically by a background thread.
    def _process_kill_cursors_queue(self):
        address_to_cursor_ids = defaultdict(list)

        # Other threads or the GC may append to the queue concurrently.
        while True:
            try:
                address, cursor_ids = self.__kill_cursors_queue.pop()
            except IndexError:
                break

            address_to_cursor_ids[address].extend(cursor_ids)

        for address, cursor_ids in address_to_cursor_ids.items():
            try:
                self._send_message(
                    message.kill_cursors(cursor_ids),
                    address=address,
                    check_primary=False)
            except ConnectionFailure as exc:
                warnings.warn("couldn't close cursor on %s: %s"
                              % (address, exc))

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

    def get_database(self, name, codec_options=None,
                     read_preference=None, write_concern=None):
        """Get a :class:`~pymongo.database.Database` with the given name and
        options.

        :Parameters:
          - `name`: The name of the database - a string.
          - `codec_options` (optional): An instance of
            :class:`~pymongo.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`MongoClient` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`MongoClient` is used.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`MongoClient` is
            used.
        """
        return database.Database(
            self, name, codec_options, read_preference, write_concern)

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
