# Copyright 2009-2015 MongoDB, Inc.
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

.. seealso:: :doc:`/examples/high_availability` for examples of connecting
   to replica sets or sets of mongos servers.

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

import contextlib
import datetime
import threading
import warnings
import weakref
from collections import defaultdict

from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.py3compat import (integer_types,
                            string_type)
from bson.son import SON
from pymongo import (common,
                     database,
                     helpers,
                     message,
                     periodic_executor,
                     uri_parser)
from pymongo.client_options import ClientOptions
from pymongo.cursor_manager import CursorManager
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            InvalidOperation,
                            InvalidURI,
                            NetworkTimeout,
                            NotMasterError,
                            OperationFailure)
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import (writable_preferred_server_selector,
                                      writable_server_selector)
from pymongo.server_type import SERVER_TYPE
from pymongo.topology import Topology
from pymongo.topology_description import TOPOLOGY_TYPE
from pymongo.settings import TopologySettings
from pymongo.write_concern import WriteConcern


class MongoClient(common.BaseObject):
    HOST = "localhost"
    PORT = 27017
    # Define order to retrieve options from ClientOptions for __repr__.
    # No host/port; these are retrieved from TopologySettings.
    _constructor_args = ('document_class', 'tz_aware', 'connect')

    def __init__(
            self,
            host=None,
            port=None,
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

        .. warning:: When using PyMongo in a multiprocessing context, please
          read :ref:`multiprocessing` first.

        :Parameters:
          - `host` (optional): hostname or IP address of the
            instance to connect to, or a mongodb URI, or a list of
            hostnames / mongodb URIs. If `host` is an IPv6 literal
            it must be enclosed in '[' and ']' characters following
            the RFC2732 URL syntax (e.g. '[::1]' for localhost)
          - `port` (optional): port number on which to connect
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

          - `maxPoolSize` (optional): The maximum number of connections
            that the pool will open simultaneously. If this is set, operations
            will block if there are `maxPoolSize` outstanding connections
            from the pool. Defaults to 100. Cannot be 0.
          - `socketTimeoutMS`: (integer or None) Controls how long (in
            milliseconds) the driver will wait for a response after sending an
            ordinary (non-monitoring) database operation before concluding that
            a network error has occurred. Defaults to ``None`` (no timeout).
          - `connectTimeoutMS`: (integer or None) Controls how long (in
            milliseconds) the driver will wait during server monitoring when
            connecting a new socket to a server before concluding the server
            is unavailable. Defaults to ``20000`` (20 seconds).
          - `serverSelectionTimeoutMS`: (integer) Controls how long (in
            milliseconds) the driver will wait to find an available,
            appropriate server to carry out a database operation; while it is
            waiting, multiple server monitoring operations may be carried out,
            each controlled by `connectTimeoutMS`. Defaults to ``30000`` (30
            seconds).
          - `waitQueueTimeoutMS`: (integer or None) How long (in milliseconds)
            a thread will wait for a socket from the pool if the pool has no
            free sockets. Defaults to ``None`` (no timeout).
          - `waitQueueMultiple`: (integer or None) Multiplied by maxPoolSize
            to give the number of threads allowed to wait for a socket at one
            time. Defaults to ``None`` (no limit).
          - `socketKeepAlive`: (boolean) Whether to send periodic keep-alive
            packets on connected sockets. Defaults to ``False`` (do not send
            keep-alive packets).
          - `event_listeners`: a list or tuple of event listeners. See
            :mod:`~pymongo.monitoring` for details.

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
          - `read_preference`: The read preference for this client.
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
            ``ssl.CERT_NONE`` and a value is not provided for ``ssl_ca_certs``
            PyMongo will attempt to load system provided CA certificates.
            If the python version in use does not support loading system CA
            certificates then the ``ssl_ca_certs`` parameter must point
            to a file of CA certificates. Implies ``ssl=True``. Defaults to
            ``ssl.CERT_REQUIRED`` if not provided and ``ssl=True``.
          - `ssl_ca_certs`: The ca_certs file contains a set of concatenated
            "certification authority" certificates, which are used to validate
            certificates passed from the other end of the connection.
            Implies ``ssl=True``. Defaults to ``None``.
          - `ssl_match_hostname`: If ``True`` (the default), and
            `ssl_cert_reqs` is not ``ssl.CERT_NONE``, enables hostname
            verification using the :func:`~ssl.match_hostname` function from
            python's :mod:`~ssl` module. Think very carefully before setting
            this to ``False`` as that could make your application vulnerable to
            man-in-the-middle attacks.

          | **Read Concern options:**
          | (If not set explicitly, this will use the server default)

          - `readConcernLevel`: (string) The read concern level specifies the
            level of isolation for read operations.  For example, a read
            operation using a read concern level of ``majority`` will only
            return data that has been written to a majority of nodes. If the
            level is left unspecified, the server default will be used.

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

           Therefore the ``alive`` method is removed since it no longer
           provides meaningful information; even if the client is disconnected,
           it may discover a server in time to fulfill the next operation.

           In PyMongo 2.x, :class:`~pymongo.MongoClient` accepted a list of
           standalone MongoDB servers and used the first it could connect to::

               MongoClient(['host1.com:27017', 'host2.com:27017'])

           A list of multiple standalones is no longer supported; if multiple
           servers are listed they must be members of the same replica set, or
           mongoses in the same sharded cluster.

           The behavior for a list of mongoses is changed from "high
           availability" to "load balancing". Before, the client connected to
           the lowest-latency mongos in the list, and used it until a network
           error prompted it to re-evaluate all mongoses' latencies and
           reconnect to one of them. In PyMongo 3, the client monitors its
           network latency to all the mongoses continuously, and distributes
           operations evenly among those with the lowest latency. See
           :ref:`mongos-load-balancing` for more information.

           The ``connect`` option is added.

           The ``start_request``, ``in_request``, and ``end_request`` methods
           are removed, as well as the ``auto_start_request`` option.

           The ``copy_database`` method is removed, see the
           :doc:`copy_database examples </examples/copydb>` for alternatives.

           The :meth:`MongoClient.disconnect` method is removed; it was a
           synonym for :meth:`~pymongo.MongoClient.close`.

           :class:`~pymongo.mongo_client.MongoClient` no longer returns an
           instance of :class:`~pymongo.database.Database` for attribute names
           with leading underscores. You must use dict-style lookups instead::

               client['__my_database__']

           Not::

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
                    res = uri_parser.parse_uri(entity, port, warn=True)
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

        keyword_opts = kwargs
        keyword_opts['document_class'] = document_class
        keyword_opts['tz_aware'] = tz_aware
        keyword_opts['connect'] = connect
        # Validate all keyword options.
        keyword_opts = dict(common.validate(k, v)
                            for k, v in keyword_opts.items())
        opts.update(keyword_opts)
        self.__options = options = ClientOptions(
            username, password, dbase, opts)

        self.__default_database_name = dbase
        self.__lock = threading.Lock()
        self.__cursor_manager = CursorManager(self)
        self.__kill_cursors_queue = []

        self._event_listeners = options.pool_options.event_listeners

        # Cache of existing indexes used by ensure_index ops.
        self.__index_cache = {}
        self.__index_cache_lock = threading.Lock()

        super(MongoClient, self).__init__(options.codec_options,
                                          options.read_preference,
                                          options.write_concern,
                                          options.read_concern)

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
            local_threshold_ms=options.local_threshold_ms,
            server_selection_timeout=options.server_selection_timeout)

        self._topology = Topology(self._topology_settings)
        if connect:
            self._topology.open()

        def target():
            client = self_ref()
            if client is None:
                return False  # Stop the executor.
            MongoClient._process_kill_cursors_queue(client)
            return True

        executor = periodic_executor.PeriodicExecutor(
            interval=common.KILL_CURSOR_FREQUENCY,
            min_interval=0.5,
            target=target,
            name="pymongo_kill_cursors_thread")

        # We strongly reference the executor and it weakly references us via
        # this closure. When the client is freed, stop the executor soon.
        self_ref = weakref.ref(self, executor.close)
        self._kill_cursors_executor = executor
        executor.open()

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
        with self.__index_cache_lock:
            return (dbname in cache and
                    coll in cache[dbname] and
                    index in cache[dbname][coll] and
                    now < cache[dbname][coll][index])

    def _cache_index(self, dbname, collection, index, cache_for):
        """Add an index to the index cache for ensure_index operations."""
        now = datetime.datetime.utcnow()
        expire = datetime.timedelta(seconds=cache_for) + now

        with self.__index_cache_lock:
            if database not in self.__index_cache:
                self.__index_cache[dbname] = {}
                self.__index_cache[dbname][collection] = {}
                self.__index_cache[dbname][collection][index] = expire

            elif collection not in self.__index_cache[dbname]:
                self.__index_cache[dbname][collection] = {}
                self.__index_cache[dbname][collection][index] = expire

            else:
                self.__index_cache[dbname][collection][index] = expire

    def _purge_index(self, database_name,
                     collection_name=None, index_name=None):
        """Purge an index from the index cache.

        If `index_name` is None purge an entire collection.

        If `collection_name` is None purge an entire database.
        """
        with self.__index_cache_lock:
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

    def _server_property(self, attr_name):
        """An attribute of the current server's description.

        If the client is not connected, this will block until a connection is
        established or raise ServerSelectionTimeoutError if no server is
        available.

        Not threadsafe if used multiple times in a single method, since
        the server may change. In such cases, store a local reference to a
        ServerDescription first, then use its properties.
        """
        server = self._topology.select_server(
            writable_server_selector)

        return getattr(server.description, attr_name)

    @property
    def event_listeners(self):
        """The event listeners registered for this client.

        See :mod:`~pymongo.monitoring` for details.
        """
        return self._event_listeners.event_listeners

    @property
    def address(self):
        """(host, port) of the current standalone, primary, or mongos, or None.

        Accessing :attr:`address` raises :exc:`~.errors.InvalidOperation` if
        the client is load-balancing among mongoses, since there is no single
        address. Use :attr:`nodes` instead.

        If the client is not connected, this will block until a connection is
        established or raise ServerSelectionTimeoutError if no server is
        available.

        .. versionadded:: 3.0
        """
        topology_type = self._topology._description.topology_type
        if topology_type == TOPOLOGY_TYPE.Sharded:
            raise InvalidOperation(
                'Cannot use "address" property when load balancing among'
                ' mongoses, use "nodes" instead.')
        if topology_type not in (TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                                 TOPOLOGY_TYPE.Single):
            return None
        return self._server_property('address')

    @property
    def primary(self):
        """The (host, port) of the current primary of the replica set.

        Returns ``None`` if this client is not connected to a replica set,
        there is no primary, or this client was created without the
        `replicaSet` option.

        .. versionadded:: 3.0
           MongoClient gained this property in version 3.0 when
           MongoReplicaSetClient's functionality was merged in.
        """
        return self._topology.get_primary()

    @property
    def secondaries(self):
        """The secondary members known to this client.

        A sequence of (host, port) pairs. Empty if this client is not
        connected to a replica set, there are no visible secondaries, or this
        client was created without the `replicaSet` option.

        .. versionadded:: 3.0
           MongoClient gained this property in version 3.0 when
           MongoReplicaSetClient's functionality was merged in.
        """
        return self._topology.get_secondaries()

    @property
    def arbiters(self):
        """Arbiters in the replica set.

        A sequence of (host, port) pairs. Empty if this client is not
        connected to a replica set, there are no arbiters, or this client was
        created without the `replicaSet` option.
        """
        return self._topology.get_arbiters()

    @property
    def is_primary(self):
        """If this client is connected to a server that can accept writes.

        True if the current server is a standalone, mongos, or the primary of
        a replica set. If the client is not connected, this will block until a
        connection is established or raise ServerSelectionTimeoutError if no
        server is available.
        """
        return self._server_property('is_writable')

    @property
    def is_mongos(self):
        """If this client is connected to mongos. If the client is not
        connected, this will block until a connection is established or raise
        ServerSelectionTimeoutError if no server is available..
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
        """Set of all currently connected servers.

        .. warning:: When connected to a replica set the value of :attr:`nodes`
          can change over time as :class:`MongoClient`'s view of the replica
          set changes. :attr:`nodes` can also be an empty set when
          :class:`MongoClient` is first instantiated and hasn't yet connected
          to any servers, or a network partition causes it to lose connection
          to all servers.
        """
        description = self._topology.description
        return frozenset(s.address for s in description.known_servers)

    @property
    def max_bson_size(self):
        """The largest BSON object the connected server accepts in bytes.

        If the client is not connected, this will block until a connection is
        established or raise ServerSelectionTimeoutError if no server is
        available.
        """
        return self._server_property('max_bson_size')

    @property
    def max_message_size(self):
        """The largest message the connected server accepts in bytes.

        If the client is not connected, this will block until a connection is
        established or raise ServerSelectionTimeoutError if no server is
        available.
        """
        return self._server_property('max_message_size')

    @property
    def max_write_batch_size(self):
        """The maxWriteBatchSize reported by the server.

        If the client is not connected, this will block until a connection is
        established or raise ServerSelectionTimeoutError if no server is
        available.

        Returns a default value when connected to server versions prior to
        MongoDB 2.6.
        """
        return self._server_property('max_write_batch_size')

    @property
    def local_threshold_ms(self):
        """The local threshold for this instance."""
        return self.__options.local_threshold_ms

    @property
    def server_selection_timeout(self):
        """The server selection timeout for this instance in seconds."""
        return self.__options.server_selection_timeout

    def _is_writable(self):
        """Attempt to connect to a writable server, or return False.
        """
        topology = self._get_topology()  # Starts monitors if necessary.
        try:
            svr = topology.select_server(writable_server_selector)

            # When directly connected to a secondary, arbiter, etc.,
            # select_server returns it, whatever the selector. Check
            # again if the server is writable.
            return svr.description.is_writable
        except ConnectionFailure:
            return False

    def close(self):
        """Disconnect from MongoDB.

        Close all sockets in the connection pools and stop the monitor threads.
        If this instance is used again it will be automatically re-opened and
        the threads restarted.
        """
        self._topology.close()

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

    @contextlib.contextmanager
    def _get_socket(self, selector):
        server = self._get_topology().select_server(selector)
        try:
            with server.get_socket(self.__all_credentials) as sock_info:
                yield sock_info
        except NetworkTimeout:
            # The socket has been closed. Don't reset the server.
            # Server Discovery And Monitoring Spec: "When an application
            # operation fails because of any network error besides a socket
            # timeout...."
            raise
        except NotMasterError:
            # "When the client sees a "not master" error it MUST replace the
            # server's description with type Unknown. It MUST request an
            # immediate check of the server."
            self._reset_server_and_request_check(server.description.address)
            raise
        except ConnectionFailure:
            # "Client MUST replace the server's description with type Unknown
            # ... MUST NOT request an immediate check of the server."
            self.__reset_server(server.description.address)
            raise

    def _socket_for_writes(self):
        return self._get_socket(writable_server_selector)

    @contextlib.contextmanager
    def _socket_for_reads(self, read_preference):
        preference = read_preference or ReadPreference.PRIMARY
        # Get a socket for a server matching the read preference, and yield
        # sock_info, slave_ok. Server Selection Spec: "slaveOK must be sent to
        # mongods with topology type Single. If the server type is Mongos,
        # follow the rules for passing read preference to mongos, even for
        # topology type Single."
        # Thread safe: if the type is single it cannot change.
        topology = self._get_topology()
        single = topology.description.topology_type == TOPOLOGY_TYPE.Single
        with self._get_socket(read_preference) as sock_info:
            slave_ok = (single and not sock_info.is_mongos) or (
                preference != ReadPreference.PRIMARY)
            yield sock_info, slave_ok

    def _send_message_with_response(self, operation, read_preference=None,
                                    exhaust=False, address=None):
        """Send a message to MongoDB and return a Response.

        :Parameters:
          - `operation`: a _Query or _GetMore object.
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

        # A _Query's slaveOk bit is already set for queries with non-primary
        # read preference. If this is a direct connection to a mongod, override
        # and *always* set the slaveOk bit. See bullet point 2 in
        # server-selection.rst#topology-type-single.
        set_slave_ok = (
            topology.description.topology_type == TOPOLOGY_TYPE.Single
            and server.description.server_type != SERVER_TYPE.Mongos)

        return self._reset_on_error(
            server,
            server.send_message_with_response,
            operation,
            set_slave_ok,
            self.__all_credentials,
            self._event_listeners,
            exhaust)

    def _reset_on_error(self, server, func, *args, **kwargs):
        """Execute an operation. Reset the server on network error.

        Returns fn()'s return value on success. On error, clears the server's
        pool and marks the server Unknown.

        Re-raises any exception thrown by fn().
        """
        try:
            return func(*args, **kwargs)
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

    def _repr_helper(self):
        def option_repr(option, value):
            """Fix options whose __repr__ isn't usable in a constructor."""
            if option == 'document_class':
                if value is dict:
                    return 'document_class=dict'
                else:
                    return 'document_class=%s.%s' % (value.__module__,
                                                     value.__name__)
            if "ms" in option:
                return "%s='%s'" % (option, int(value * 1000))

            return '%s=%r' % (option, value)

        # Host first...
        options = ['host=%r' % [
            '%s:%d' % (host, port)
            for host, port in self._topology_settings.seeds]]
        # ... then everything in self._constructor_args...
        options.extend(
            option_repr(key, self.__options._options[key])
            for key in self._constructor_args)
        # ... then everything else.
        options.extend(
            option_repr(key, self.__options._options[key])
            for key in self.__options._options
            if key not in set(self._constructor_args))
        return ', '.join(options)

    def __repr__(self):
        return ("MongoClient(%s)" % (self._repr_helper(),))

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
          - `address` (optional): (host, port) pair of the cursor's server.
            If it is not provided, the client attempts to close the cursor on
            the primary or standalone, or a mongos server.

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
          - `address` (optional): (host, port) pair of the cursor's server.
            If it is not provided, the client attempts to close the cursor on
            the primary or standalone, or a mongos server.

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
        """Process any pending kill cursors requests."""
        address_to_cursor_ids = defaultdict(list)

        # Other threads or the GC may append to the queue concurrently.
        while True:
            try:
                address, cursor_ids = self.__kill_cursors_queue.pop()
            except IndexError:
                break

            address_to_cursor_ids[address].extend(cursor_ids)

        # Don't re-open topology if it's closed and there's no pending cursors.
        if address_to_cursor_ids:
            listeners = self._event_listeners
            publish = listeners.enabled_for_commands
            topology = self._get_topology()
            for address, cursor_ids in address_to_cursor_ids.items():
                try:
                    if address:
                        # address could be a tuple or _CursorAddress, but
                        # select_server_by_address needs (host, port).
                        server = topology.select_server_by_address(
                            tuple(address))
                    else:
                        # Application called close_cursor() with no address.
                        server = topology.select_server(
                            writable_server_selector)

                    try:
                        namespace = address.namespace
                        db, coll = namespace.split('.', 1)
                    except AttributeError:
                        namespace = None
                        db = coll = "OP_KILL_CURSORS"

                    spec = SON([('killCursors', coll),
                                ('cursors', cursor_ids)])
                    with server.get_socket(self.__all_credentials) as sock_info:
                        if (sock_info.max_wire_version >= 4 and
                                namespace is not None):
                            sock_info.command(db, spec)
                        else:
                            if publish:
                                start = datetime.datetime.now()
                            request_id, msg = message.kill_cursors(cursor_ids)
                            if publish:
                                duration = datetime.datetime.now() - start
                                listeners.publish_command_start(
                                    spec, db, request_id, address)
                                start = datetime.datetime.now()

                            try:
                                sock_info.send_message(msg, 0)
                            except Exception as exc:
                                if publish:
                                    dur = ((datetime.datetime.now() - start)
                                           + duration)
                                    listeners.publish_command_failure(
                                        dur, message._convert_exception(exc),
                                        'killCursors', request_id, address)
                                raise

                            if publish:
                                duration = ((datetime.datetime.now() - start)
                                            + duration)
                                # OP_KILL_CURSORS returns no reply, fake one.
                                reply = {'cursorsUnknown': cursor_ids, 'ok': 1}
                                listeners.publish_command_success(
                                    duration, reply, 'killCursors', request_id,
                                    address)

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
                self._database_default_options('admin').command(
                    "listDatabases")["databases"]]

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

    def get_database(self, name, codec_options=None, read_preference=None,
                     write_concern=None, read_concern=None):
        """Get a :class:`~pymongo.database.Database` with the given name and
        options.

        Useful for creating a :class:`~pymongo.database.Database` with
        different codec options, read preference, and/or write concern from
        this :class:`MongoClient`.

          >>> client.read_preference
          Primary()
          >>> db1 = client.test
          >>> db1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> db2 = client.get_database(
          ...     'test', read_preference=ReadPreference.SECONDARY)
          >>> db2.read_preference
          Secondary(tag_sets=None)

        :Parameters:
          - `name`: The name of the database - a string.
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`MongoClient` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`MongoClient` is used. See :mod:`~pymongo.read_preferences`
            for options.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`MongoClient` is
            used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`MongoClient` is
            used.
        """
        return database.Database(
            self, name, codec_options, read_preference,
            write_concern, read_concern)

    def _database_default_options(self, name):
        """Get a Database instance with the default settings."""
        return self.get_database(
            name, codec_options=DEFAULT_CODEC_OPTIONS,
            read_preference=ReadPreference.PRIMARY,
            write_concern=WriteConcern())

    @property
    def is_locked(self):
        """Is this server locked? While locked, all write operations
        are blocked, although read operations may still be allowed.
        Use :meth:`unlock` to unlock.
        """
        ops = self._database_default_options('admin').current_op()
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
        cmd = {"fsyncUnlock": 1}
        with self._socket_for_writes() as sock_info:
            if sock_info.max_wire_version >= 4:
                try:
                    sock_info.command("admin", cmd)
                except OperationFailure as exc:
                    # Ignore "DB not locked" to replicate old behavior
                    if exc.code != 125:
                        raise
            else:
                helpers._first_batch(sock_info, "admin", "$cmd.sys.unlock",
                    {}, -1, True, self.codec_options,
                    ReadPreference.PRIMARY, cmd, self._event_listeners)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        raise TypeError("'MongoClient' object is not iterable")

    next = __next__
