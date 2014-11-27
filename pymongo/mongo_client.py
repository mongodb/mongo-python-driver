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

.. seealso:: Module :mod:`~pymongo.master_slave_connection` for
   connecting to master-slave clusters, and
   :doc:`/examples/high_availability` for an example of how to connect
   to a replica set, or specify a list of mongos instances for automatic
   failover.

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
import random
import socket
import struct
import threading
import time
import warnings

from bson.py3compat import b
from pymongo import (auth,
                     common,
                     database,
                     helpers,
                     message,
                     pool,
                     thread_util,
                     uri_parser)
from pymongo.common import HAS_SSL
from pymongo.cursor_manager import CursorManager
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            DocumentTooLarge,
                            DuplicateKeyError,
                            InvalidURI,
                            OperationFailure)
from pymongo.member import Member
from pymongo.read_preferences import ReadPreference


EMPTY = b("")


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
    """Connection to MongoDB.
    """

    HOST = "localhost"
    PORT = 27017

    def __init__(self, host=None, port=None, max_pool_size=100,
                 document_class=dict, tz_aware=False, _connect=True,
                 **kwargs):
        """Create a new connection to a single MongoDB instance at *host:port*.

        The resultant client object has connection-pooling built
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
          - `auto_start_request`: If ``True``, each thread that accesses
            this :class:`MongoClient` has a socket allocated to it for the
            thread's lifetime.  This ensures consistent reads, even if you
            read after an unacknowledged write. Defaults to ``False``
          - `use_greenlets`: If ``True``, :meth:`start_request()` will ensure
            that the current greenlet uses the same socket for all
            operations until :meth:`end_request()`

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
          | (ignored by standalone mongod instances)

          - `replicaSet`: (string) The name of the replica set to connect to.
            The driver will verify that the replica set it connects to matches
            this name. Implies that the hosts specified are a seed list and the
            driver should attempt to find all members of the set. *Ignored by
            mongos*.
          - `read_preference`: The read preference for this client. If
            connecting to a secondary then a read preference mode *other* than
            PRIMARY is required - otherwise all queries will throw
            :class:`~pymongo.errors.AutoReconnect` "not master".
            See :class:`~pymongo.read_preferences.ReadPreference` for all
            available read preference options.
          - `tag_sets`: Ignored unless connecting to a replica set via mongos.
            Specify a priority-order for tag sets, provide a list of
            tag sets: ``[{'dc': 'ny'}, {'dc': 'la'}, {}]``. A final, empty tag
            set, ``{}``, means "read from any member that matches the mode,
            ignoring tags.

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

        .. seealso:: :meth:`end_request`

        .. mongodoc:: connections

        .. versionchanged:: 2.5
           Added additional ssl options
        .. versionadded:: 2.4
        """
        if host is None:
            host = self.HOST
        if isinstance(host, basestring):
            host = [host]
        if port is None:
            port = self.PORT
        if not isinstance(port, int):
            raise TypeError("port must be an instance of int")

        seeds = set()
        username = None
        password = None
        self.__default_database_name = None
        opts = {}
        for entity in host:
            if "://" in entity:
                if entity.startswith("mongodb://"):
                    res = uri_parser.parse_uri(entity, port)
                    seeds.update(res["nodelist"])
                    username = res["username"] or username
                    password = res["password"] or password
                    self.__default_database_name = (
                        res["database"] or self.__default_database_name)

                    opts = res["options"]
                else:
                    idx = entity.find("://")
                    raise InvalidURI("Invalid URI scheme: "
                                     "%s" % (entity[:idx],))
            else:
                seeds.update(uri_parser.split_hosts(entity, port))
        if not seeds:
            raise ConfigurationError("need to specify at least one host")

        # Seeds are only used before first connection attempt; nodes are then
        # used for any reconnects. Nodes are set to all replica set members
        # if connecting to a replica set (besides arbiters), or to all
        # available mongoses from the seed list, or to the one standalone
        # mongod.
        self.__seeds = frozenset(seeds)
        self.__nodes = frozenset()
        self.__member = None  # TODO: Rename to __server.

        # _pool_class and _event_class are for deep customization of PyMongo,
        # e.g. Motor. SHOULD NOT BE USED BY THIRD-PARTY DEVELOPERS.
        pool_class = kwargs.pop('_pool_class', pool.Pool)
        event_class = kwargs.pop('_event_class', None)

        options = {}
        for option, value in kwargs.iteritems():
            option, value = common.validate(option, value)
            options[option] = value
        options.update(opts)

        self.__max_pool_size = common.validate_positive_integer_or_none(
            'max_pool_size', max_pool_size)

        self.__cursor_manager = CursorManager(self)

        self.__repl = options.get('replicaset')
        self.__direct = len(seeds) == 1 and not self.__repl

        self.__net_timeout = options.get('sockettimeoutms')
        self.__conn_timeout = options.get('connecttimeoutms')
        self.__wait_queue_timeout = options.get('waitqueuetimeoutms')
        self.__wait_queue_multiple = options.get('waitqueuemultiple')

        self.__use_ssl = options.get('ssl', None)
        self.__ssl_keyfile = options.get('ssl_keyfile', None)
        self.__ssl_certfile = options.get('ssl_certfile', None)
        self.__ssl_cert_reqs = options.get('ssl_cert_reqs', None)
        self.__ssl_ca_certs = options.get('ssl_ca_certs', None)

        ssl_kwarg_keys = [k for k in kwargs.keys() if k.startswith('ssl_')]
        if self.__use_ssl == False and ssl_kwarg_keys:
            raise ConfigurationError("ssl has not been enabled but the "
                                     "following ssl parameters have been set: "
                                     "%s. Please set `ssl=True` or remove."
                                     % ', '.join(ssl_kwarg_keys))

        if self.__ssl_cert_reqs and not self.__ssl_ca_certs:
            raise ConfigurationError("If `ssl_cert_reqs` is not "
                                     "`ssl.CERT_NONE` then you must "
                                     "include `ssl_ca_certs` to be able "
                                     "to validate the server.")

        if ssl_kwarg_keys and self.__use_ssl is None:
            # ssl options imply ssl = True
            self.__use_ssl = True

        if self.__use_ssl and not HAS_SSL:
            raise ConfigurationError("The ssl module is not available. If you "
                                     "are using a python version previous to "
                                     "2.6 you must install the ssl package "
                                     "from PyPI.")

        self.__use_greenlets = options.get('use_greenlets', False)
        self.__pool_class = pool_class

        self.__connecting = False
        if self.__use_greenlets:
            # Greenlets don't need to lock around access to the Member;
            # they're only interrupted when they do I/O.
            self.__connecting_lock = thread_util.DummyLock()
        else:
            self.__connecting_lock = threading.Lock()

        if event_class:
            self.__event_class = event_class
        else:
            # Prevent a cycle; this lambda shouldn't refer to self.
            g = self.__use_greenlets
            event_class = lambda: thread_util.create_event(g)
            self.__event_class = event_class

        self.__future_member = None
        self.__document_class = document_class
        self.__tz_aware = common.validate_boolean('tz_aware', tz_aware)
        self.__auto_start_request = options.get('auto_start_request', False)

        # cache of existing indexes used by ensure_index ops
        self.__index_cache = {}
        self.__auth_credentials = {}

        super(MongoClient, self).__init__(**options)
        if self.slave_okay:
            warnings.warn("slave_okay is deprecated. Please "
                          "use read_preference instead.", DeprecationWarning,
                          stacklevel=2)

        if _connect:
            try:
                self._ensure_connected(True)
            except AutoReconnect, e:
                # ConnectionFailure makes more sense here than AutoReconnect
                raise ConnectionFailure(str(e))

        if username:
            mechanism = options.get('authmechanism', 'MONGODB-CR')
            source = (
                options.get('authsource')
                or self.__default_database_name
                or 'admin')

            credentials = auth._build_credentials_tuple(mechanism,
                                                        source,
                                                        unicode(username),
                                                        unicode(password),
                                                        options)
            try:
                self._cache_credentials(source, credentials, _connect)
            except OperationFailure, exc:
                raise ConfigurationError(str(exc))

    def _cached(self, dbname, coll, index):
        """Test if `index` is cached.
        """
        cache = self.__index_cache
        now = datetime.datetime.utcnow()
        return (dbname in cache and
                coll in cache[dbname] and
                index in cache[dbname][coll] and
                now < cache[dbname][coll][index])

    def _cache_index(self, database, collection, index, cache_for):
        """Add an index to the index cache for ensure_index operations.
        """
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

    def _cache_credentials(self, source, credentials, connect=True):
        """Add credentials to the database authentication cache
        for automatic login when a socket is created. If `connect` is True,
        verify the credentials on the server first.
        """
        if source in self.__auth_credentials:
            # Nothing to do if we already have these credentials.
            if credentials == self.__auth_credentials[source]:
                return
            raise OperationFailure('Another user is already authenticated '
                                   'to this database. You must logout first.')

        if connect:
            member = self.__ensure_member()
            sock_info = self.__socket(member)
            try:
                # Since __check_auth was called in __socket
                # there is no need to call it here.
                auth.authenticate(credentials, sock_info, self.__simple_command)
                sock_info.authset.add(credentials)
            finally:
                member.pool.maybe_return_socket(sock_info)

        self.__auth_credentials[source] = credentials

    def _purge_credentials(self, source):
        """Purge credentials from the database authentication cache.
        """
        if source in self.__auth_credentials:
            del self.__auth_credentials[source]

    def __create_pool(self, pair):
        return self.__pool_class(
            pair,
            self.__max_pool_size,
            self.__net_timeout,
            self.__conn_timeout,
            self.__use_ssl,
            use_greenlets=self.__use_greenlets,
            ssl_keyfile=self.__ssl_keyfile,
            ssl_certfile=self.__ssl_certfile,
            ssl_cert_reqs=self.__ssl_cert_reqs,
            ssl_ca_certs=self.__ssl_ca_certs,
            wait_queue_timeout=self.__wait_queue_timeout,
            wait_queue_multiple=self.__wait_queue_multiple)

    def __check_auth(self, sock_info):
        """Authenticate using cached database credentials.
        """
        if self.__auth_credentials or sock_info.authset:
            cached = set(self.__auth_credentials.itervalues())

            authset = sock_info.authset.copy()

            # Logout any credentials that no longer exist in the cache.
            for credentials in authset - cached:
                self.__simple_command(sock_info, credentials[1], {'logout': 1})
                sock_info.authset.discard(credentials)

            for credentials in cached - authset:
                auth.authenticate(credentials,
                                  sock_info, self.__simple_command)
                sock_info.authset.add(credentials)

    def __member_property(self, attr_name, default=None):
        member = self.__member
        if member:
            return getattr(member, attr_name)

        return default

    @property
    def host(self):
        """Current connected host.

        .. versionchanged:: 1.3
           ``host`` is now a property rather than a method.
        """
        member = self.__member
        if member:
            return member.host[0]

        return None

    @property
    def port(self):
        """Current connected port.

        .. versionchanged:: 1.3
           ``port`` is now a property rather than a method.
        """
        member = self.__member
        if member:
            return member.host[1]

        return None
    @property
    def is_primary(self):
        """If this instance is connected to a standalone, a replica set
        primary, or the master of a master-slave set.

        .. versionadded:: 2.3
        """
        return self.__member_property('is_primary', False)

    @property
    def is_mongos(self):
        """If this instance is connected to mongos.

        .. versionadded:: 2.3
        """
        return self.__member_property('is_mongos', False)

    @property
    def max_pool_size(self):
        """The maximum number of sockets the pool will open concurrently.

        When the pool has reached `max_pool_size`, operations block waiting for
        a socket to be returned to the pool. If ``waitQueueTimeoutMS`` is set,
        a blocked operation will raise :exc:`~pymongo.errors.ConnectionFailure`
        after a timeout. By default ``waitQueueTimeoutMS`` is not set.

        .. warning:: SIGNIFICANT BEHAVIOR CHANGE in 2.6. Previously, this
          parameter would limit only the idle sockets the pool would hold
          onto, not the number of open sockets. The default has also changed
          to 100.

        .. versionchanged:: 2.6
        .. versionadded:: 1.11
        """
        return self.__max_pool_size

    @property
    def use_greenlets(self):
        """Whether calling :meth:`start_request` assigns greenlet-local,
        rather than thread-local, sockets.

        .. versionadded:: 2.4.2
        """
        return self.__use_greenlets

    @property
    def nodes(self):
        """List of all known nodes.

        Nodes are either specified when this instance was created,
        or discovered through the replica set discovery mechanism.

        .. versionadded:: 1.8
        """
        return self.__nodes

    @property
    def auto_start_request(self):
        """Is auto_start_request enabled?
        """
        return self.__auto_start_request

    def get_document_class(self):
        return self.__document_class

    def set_document_class(self, klass):
        self.__document_class = klass

    document_class = property(get_document_class, set_document_class,
                              doc="""Default class to use for documents
                              returned from this client.

                              .. versionadded:: 1.7
                              """)

    @property
    def tz_aware(self):
        """Does this client return timezone-aware datetimes?

        .. versionadded:: 1.8
        """
        return self.__tz_aware

    @property
    def max_bson_size(self):
        """Return the maximum size BSON object the connected server
        accepts in bytes. Defaults to 16MB if not connected to a
        server.

        .. versionadded:: 1.10
        """
        return self.__member_property('max_bson_size', common.MAX_BSON_SIZE)

    @property
    def max_message_size(self):
        """Return the maximum message size the connected server
        accepts in bytes. Defaults to 32MB if not connected to a
        server.

        .. versionadded:: 2.6
        """
        return self.__member_property(
            'max_message_size', common.MAX_MESSAGE_SIZE)

    @property
    def min_wire_version(self):
        """The minWireVersion reported by the server.

        Returns ``0`` when connected to server versions prior to MongoDB 2.6.

        .. versionadded:: 2.7
        """
        return self.__member_property(
            'min_wire_version', common.MIN_WIRE_VERSION)

    @property
    def max_wire_version(self):
        """The maxWireVersion reported by the server.

        Returns ``0`` when connected to server versions prior to MongoDB 2.6.

        .. versionadded:: 2.7
        """
        return self.__member_property(
            'max_wire_version', common.MAX_WIRE_VERSION)

    @property
    def max_write_batch_size(self):
        """The maxWriteBatchSize reported by the server.

        Returns a default value when connected to server versions prior to
        MongoDB 2.6.

        .. versionadded:: 2.7
        """
        return self.__member_property(
            'max_write_batch_size', common.MAX_WRITE_BATCH_SIZE)

    def __simple_command(self, sock_info, dbname, spec):
        """Send a command to the server. May raise AutoReconnect.
        """
        rqst_id, msg, _ = message.query(0, dbname + '.$cmd', 0, -1, spec)
        start = time.time()
        try:
            sock_info.sock.sendall(msg)
            response = self.__receive_message_on_socket(1, rqst_id, sock_info)
        except socket.error, e:
            sock_info.close()
            raise AutoReconnect(e)
        except:
            sock_info.close()
            raise

        end = time.time()
        response = helpers._unpack_response(response)['data'][0]
        msg = "command %r failed: %%s" % spec
        helpers._check_command_response(response, None, msg)
        return response, end - start

    def __try_node(self, node):
        """Try to connect to this node and see if it works for our connection
        type. Returns a Member and set of hosts (including this one). Doesn't
        modify state.

        :Parameters:
         - `node`: The (host, port) pair to try.
        """
        # Call 'ismaster' directly so we can get a response time.
        connection_pool = self.__create_pool(node)
        sock_info = connection_pool.get_socket()
        try:
            response, res_time = self.__simple_command(sock_info,
                                                       'admin',
                                                       {'ismaster': 1})
        finally:
            connection_pool.maybe_return_socket(sock_info)

        member = Member(
            node,
            connection_pool,
            response,
            res_time)

        nodes = frozenset([node])

        # Replica Set?
        if not self.__direct:
            # Check that this host is part of the given replica set.
            if self.__repl and member.set_name != self.__repl:
                raise ConfigurationError("%s:%d is not a member of "
                                         "replica set %s"
                                         % (node[0], node[1], self.__repl))

            if "hosts" in response:
                nodes = frozenset([
                    _partition_node(h) for h in response["hosts"]])

            if member.is_primary:
                return member, nodes

            elif "primary" in response:
                # Shortcut: a secondary usually tells us who the primary is.
                candidate = _partition_node(response["primary"])
                return self.__try_node(candidate)

            # Explain why we aren't using this connection.
            raise AutoReconnect('%s:%d is not primary or master' % node)

        # Direct connection
        if member.is_arbiter and not self.__direct:
            raise ConfigurationError("%s:%d is an arbiter" % node)

        return member, nodes

    def __pick_nearest(self, candidates):
        """Return the 'nearest' Member instance based on response time.

        Doesn't modify state.
        """
        latency = self.secondary_acceptable_latency_ms
        # Only used for mongos high availability, ping_time is in seconds.
        fastest = min([
            member.ping_time for member in candidates])

        near_candidates = [
            member for member in candidates
            if member.ping_time - fastest < latency / 1000.0]

        return random.choice(near_candidates)

    def __ensure_member(self):
        """Connect and return a Member instance, or raise AutoReconnect."""
        # If `connecting` is False, no thread is in __find_node(),
        # and `future_member` is resolved. `member` may be None if the
        # last __find_node() attempt failed, otherwise it is in `nodes`.
        #
        # If `connecting` is True, a thread is in __find_node(),
        # `member` is None, and `future_member` is pending.
        #
        # To violate these invariants temporarily, acquire the lock.
        # Note that disconnect() interacts with this method.
        self.__connecting_lock.acquire()
        if self.__member:
            member = self.__member
            self.__connecting_lock.release()
            return member

        elif self.__connecting:
            # A thread is in __find_node(). Wait.
            future = self.__future_member
            self.__connecting_lock.release()
            return future.result()

        else:
            self.__connecting = True
            future = self.__future_member = thread_util.Future(
                self.__event_class)

            self.__connecting_lock.release()

            member = None
            nodes = None
            exc = None

            try:
                try:
                    member, nodes = self.__find_node()
                    return member
                except Exception, e:
                    exc = e
                    raise
            finally:
                # We're either returning a Member or raising an error.
                # Propagate either outcome to waiting threads.
                self.__connecting_lock.acquire()
                self.__member = member
                self.__connecting = False

                # If we discovered a set of nodes, use them from now on;
                # otherwise we're raising an error. Stick with the last
                # known good set of nodes.
                if nodes:
                    self.__nodes = nodes

                if member:
                    # Unblock waiting threads.
                    future.set_result(member)
                else:
                    # Raise exception in waiting threads.
                    future.set_exception(exc)

                self.__connecting_lock.release()

    def __find_node(self):
        """Find a server suitable for our connection type.

        Returns a Member and a set of nodes. Doesn't modify state.

        If only one host was supplied to __init__ see if we can connect
        to it. Don't check if the host is a master/primary so we can make
        a direct connection to read from a secondary or send commands to
        an arbiter.

        If more than one host was supplied treat them as a seed list for
        connecting to a replica set or to support high availability for
        mongos. If connecting to a replica set try to find the primary,
        and set `nodes` to list of all members.

        If a mongos seed list was provided find the "nearest" mongos and
        return it, setting `nodes` to all mongoses in the seed list that
        are up.

        Otherwise we iterate through the list trying to find a host we can
        send write operations to.
        """
        assert not self.__member, \
            "__find_node unexpectedly running with a non-null Member"

        errors = []
        mongos_candidates = []
        candidates = self.__nodes or self.__seeds
        chosen_member = None
        discovered_nodes = None

        for candidate in candidates:
            try:
                member, nodes = self.__try_node(candidate)
                if member.is_mongos and not self.__direct:
                    mongos_candidates.append(member)

                    # We intend to find all the mongoses; keep trying nodes.
                    continue
                elif len(mongos_candidates):
                    raise ConfigurationError("Seed list cannot contain a mix "
                                             "of mongod and mongos instances.")

                # We've found a suitable node.
                chosen_member = member
                discovered_nodes = nodes
                break
            except (OperationFailure, ConfigurationError, ValueError):
                # The server is available but something failed, e.g. auth,
                # wrong replica set name, or incompatible wire protocol.
                raise
            except Exception, why:
                errors.append(str(why))

        if len(mongos_candidates):
            # If we have a mongos seed list, pick the "nearest" member.
            chosen_member = self.__pick_nearest(mongos_candidates)
            mongoses = frozenset(m.host for m in mongos_candidates)

            # The first time, __nodes is empty and mongoses becomes nodes.
            return chosen_member, self.__nodes or mongoses

        if not chosen_member:
            # Couldn't find a suitable host.
            raise AutoReconnect(', '.join(errors))

        return chosen_member, discovered_nodes

    def __socket(self, member):
        """Get a SocketInfo.

        Calls disconnect() on error.
        """
        connection_pool = member.pool
        try:
            if self.auto_start_request and not connection_pool.in_request():
                connection_pool.start_request()

            sock_info = connection_pool.get_socket()
        except socket.error, why:
            self.disconnect()

            # Check if a unix domain socket
            host, port = member.host
            if host.endswith('.sock'):
                host_details = "%s:" % host
            else:
                host_details = "%s:%d:" % (host, port)
            raise AutoReconnect("could not connect to "
                                "%s %s" % (host_details, str(why)))
        try:
            self.__check_auth(sock_info)
        except:
            connection_pool.maybe_return_socket(sock_info)
            raise
        return sock_info

    def _ensure_connected(self, sync=False):
        """Ensure this client instance is connected to a mongod/s.
        """
        self.__ensure_member()

    def disconnect(self):
        """Disconnect from MongoDB.

        Disconnecting will close all underlying sockets in the connection
        pool. If this instance is used again it will be automatically
        re-opened. Care should be taken to make sure that :meth:`disconnect`
        is not called in the middle of a sequence of operations in which
        ordering is important. This could lead to unexpected results.

        .. seealso:: :meth:`end_request`
        .. versionadded:: 1.3
        """
        self.__connecting_lock.acquire()
        member, self.__member = self.__member, None
        self.__connecting_lock.release()

        # Close sockets promptly.
        if member:
            member.pool.reset()

    def close(self):
        """Alias for :meth:`disconnect`

        Disconnecting will close all underlying sockets in the connection
        pool. If this instance is used again it will be automatically
        re-opened. Care should be taken to make sure that :meth:`disconnect`
        is not called in the middle of a sequence of operations in which
        ordering is important. This could lead to unexpected results.

        .. seealso:: :meth:`end_request`
        .. versionadded:: 2.1
        """
        self.disconnect()

    def alive(self):
        """Return ``False`` if there has been an error communicating with the
        server, else ``True``.

        This method attempts to check the status of the server with minimal I/O.
        The current thread / greenlet retrieves a socket from the pool (its
        request socket if it's in a request, or a random idle socket if it's not
        in a request) and checks whether calling `select`_ on it raises an
        error. If there are currently no idle sockets, :meth:`alive` will
        attempt to actually connect to the server.

        A more certain way to determine server availability is::

            client.admin.command('ping')

        .. _select: http://docs.python.org/2/library/select.html#select.select
        """
        # In the common case, a socket is available and was used recently, so
        # calling select() on it is a reasonable attempt to see if the OS has
        # reported an error.
        self.__connecting_lock.acquire()
        member = self.__member
        self.__connecting_lock.release()
        if not member:
            return False
        else:
            sock_info = None
            try:
                try:
                    sock_info = member.pool.get_socket()
                    return not pool._closed(sock_info.sock)
                except (socket.error, ConnectionFailure):
                    return False
            finally:
                member.pool.maybe_return_socket(sock_info)

    def set_cursor_manager(self, manager_class):
        """Set this client's cursor manager.

        Raises :class:`TypeError` if `manager_class` is not a subclass of
        :class:`~pymongo.cursor_manager.CursorManager`. A cursor manager
        handles closing cursors. Different managers can implement different
        policies in terms of when to actually kill a cursor that has
        been closed.

        :Parameters:
          - `manager_class`: cursor manager to use

        .. versionchanged:: 2.1+
           Deprecated support for external cursor managers.
        """
        warnings.warn("Support for external cursor managers is deprecated "
                      "and will be removed in PyMongo 3.0.",
                      DeprecationWarning, stacklevel=2)
        manager = manager_class(self)
        if not isinstance(manager, CursorManager):
            raise TypeError("manager_class must be a subclass of "
                            "CursorManager")

        self.__cursor_manager = manager

    def __check_response_to_last_error(self, response, is_command):
        """Check a response to a lastError message for errors.

        `response` is a byte string representing a response to the message.
        If it represents an error response we raise OperationFailure.

        Return the response as a document.
        """
        response = helpers._unpack_response(response)

        assert response["number_returned"] == 1
        result = response["data"][0]

        helpers._check_command_response(result, self.disconnect)

        # write commands - skip getLastError checking
        if is_command:
            return result

        # getLastError
        error_msg = result.get("err", "")
        if error_msg is None:
            return result
        if error_msg.startswith("not master"):
            self.disconnect()
            raise AutoReconnect(error_msg)

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

    def __check_bson_size(self, message):
        """Make sure the message doesn't include BSON documents larger
        than the connected server will accept.

        :Parameters:
          - `message`: message to check
        """
        if len(message) == 3:
            (request_id, data, max_doc_size) = message
            if max_doc_size > self.max_bson_size:
                raise DocumentTooLarge("BSON document too large (%d bytes)"
                                       " - the connected server supports"
                                       " BSON document sizes up to %d"
                                       " bytes." %
                                       (max_doc_size, self.max_bson_size))
            return (request_id, data)
        else:
            # get_more and kill_cursors messages
            # don't include BSON documents.
            return message

    def _send_message(self, message,
                      with_last_error=False, command=False, check_primary=True):
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
          - `check_primary`: don't try to write to a non-primary; see
            kill_cursors for an exception to this rule
        """
        member = self.__ensure_member()
        if check_primary and not with_last_error and not self.is_primary:
            # The write won't succeed, bail as if we'd done a getLastError
            raise AutoReconnect("not master")

        sock_info = self.__socket(member)
        try:
            try:
                (request_id, data) = self.__check_bson_size(message)
                sock_info.sock.sendall(data)
                # Safe mode. We pack the message together with a lastError
                # message and send both. We then get the response (to the
                # lastError) and raise OperationFailure if it is an error
                # response.
                rv = None
                if with_last_error:
                    response = self.__receive_message_on_socket(1, request_id,
                                                                sock_info)
                    rv = self.__check_response_to_last_error(response, command)

                return rv
            except OperationFailure:
                raise
            except (ConnectionFailure, socket.error), e:
                self.disconnect()
                raise AutoReconnect(str(e))
            except:
                sock_info.close()
                raise
        finally:
            member.pool.maybe_return_socket(sock_info)

    def __receive_data_on_socket(self, length, sock_info):
        """Lowest level receive operation.

        Takes length to receive and repeatedly calls recv until able to
        return a buffer of that length, raising ConnectionFailure on error.
        """
        message = EMPTY
        while length:
            chunk = sock_info.sock.recv(length)
            if chunk == EMPTY:
                raise ConnectionFailure("connection closed")
            length -= len(chunk)
            message += chunk
        return message

    def __receive_message_on_socket(self, operation, rqst_id, sock_info):
        """Receive a message in response to `rqst_id` on `sock`.

        Returns the response data with the header removed.
        """
        header = self.__receive_data_on_socket(16, sock_info)
        length = struct.unpack("<i", header[:4])[0]
        # No rqst_id for exhaust cursor "getMore".
        if rqst_id is not None:
            resp_id = struct.unpack("<i", header[8:12])[0]
            assert rqst_id == resp_id, "ids don't match %r %r" % (rqst_id,
                                                                  resp_id)
        assert operation == struct.unpack("<i", header[12:])[0]

        return self.__receive_data_on_socket(length - 16, sock_info)

    def __send_and_receive(self, message, sock_info):
        """Send a message on the given socket and return the response data.
        """
        (request_id, data) = self.__check_bson_size(message)
        try:
            sock_info.sock.sendall(data)
            return self.__receive_message_on_socket(1, request_id, sock_info)
        except:
            sock_info.close()
            raise

    # we just ignore _must_use_master here: it's only relevant for
    # MasterSlaveConnection instances.
    def _send_message_with_response(self, message,
                                    _must_use_master=False, **kwargs):
        """Send a message to Mongo and return the response.

        Sends the given message and returns the response.

        :Parameters:
          - `message`: (request_id, data) pair making up the message to send
        """
        member = self.__ensure_member()
        sock_info = self.__socket(member)
        exhaust = kwargs.get('exhaust')
        try:
            if not exhaust and "network_timeout" in kwargs:
                sock_info.sock.settimeout(kwargs["network_timeout"])

            response = self.__send_and_receive(message, sock_info)

            if not exhaust:
                if "network_timeout" in kwargs:
                    sock_info.sock.settimeout(self.__net_timeout)

                member.pool.maybe_return_socket(sock_info)

            return (None, (response, sock_info, member.pool))
        except (ConnectionFailure, socket.error), e:
            self.disconnect()
            member.pool.maybe_return_socket(sock_info)
            raise AutoReconnect(str(e))
        except:
            member.pool.maybe_return_socket(sock_info)
            raise

    def _exhaust_next(self, sock_info):
        """Used with exhaust cursors to get the next batch off the socket.

        Can raise AutoReconnect.
        """
        try:
            return self.__receive_message_on_socket(1, None, sock_info)
        except socket.error, e:
            raise AutoReconnect(str(e))

    def start_request(self):
        """Ensure the current thread or greenlet always uses the same socket
        until it calls :meth:`end_request`. This ensures consistent reads,
        even if you read after an unacknowledged write.

        In Python 2.6 and above, or in Python 2.5 with
        "from __future__ import with_statement", :meth:`start_request` can be
        used as a context manager:

        >>> client = pymongo.MongoClient(auto_start_request=False)
        >>> db = client.test
        >>> _id = db.test_collection.insert({})
        >>> with client.start_request():
        ...     for i in range(100):
        ...         db.test_collection.update({'_id': _id}, {'$set': {'i':i}})
        ...
        ...     # Definitely read the document after the final update completes
        ...     print db.test_collection.find({'_id': _id})

        If a thread or greenlet calls start_request multiple times, an equal
        number of calls to :meth:`end_request` is required to end the request.

        .. versionchanged:: 2.4
           Now counts the number of calls to start_request and doesn't end
           request until an equal number of calls to end_request.

        .. versionadded:: 2.2
           The :class:`~pymongo.pool.Request` return value.
           :meth:`start_request` previously returned None
        """
        member = self.__ensure_member()
        member.pool.start_request()
        return pool.Request(self)

    def in_request(self):
        """True if this thread is in a request, meaning it has a socket
        reserved for its exclusive use.
        """
        member = self.__member  # Don't try to connect if disconnected.
        return member and member.pool.in_request()

    def end_request(self):
        """Undo :meth:`start_request`. If :meth:`end_request` is called as many
        times as :meth:`start_request`, the request is over and this thread's
        connection returns to the pool. Extra calls to :meth:`end_request` have
        no effect.

        Ending a request allows the :class:`~socket.socket` that has
        been reserved for this thread by :meth:`start_request` to be returned to
        the pool. Other threads will then be able to re-use that
        :class:`~socket.socket`. If your application uses many threads, or has
        long-running threads that infrequently perform MongoDB operations, then
        judicious use of this method can lead to performance gains. Care should
        be taken, however, to make sure that :meth:`end_request` is not called
        in the middle of a sequence of operations in which ordering is
        important. This could lead to unexpected results.
        """
        member = self.__member  # Don't try to connect if disconnected.
        if member:
            member.pool.end_request()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.host == other.host and self.port == other.port
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        if len(self.__nodes) == 1:
            return "MongoClient(%r, %r)" % (self.host, self.port)
        else:
            return "MongoClient(%r)" % ["%s:%d" % n for n in self.__nodes]

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
        depends on this client's cursor manager.

        :Parameters:
          - `cursor_id`: id of cursor to close
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
        return self._send_message(
            message.kill_cursors(cursor_ids), check_primary=False)

    def server_info(self):
        """Get information about the MongoDB server we're connected to.
        """
        return self.admin.command("buildinfo",
                                  read_preference=ReadPreference.PRIMARY)

    def database_names(self):
        """Get a list of the names of all databases on the connected server.
        """
        return [db["name"] for db in
                self.admin.command("listDatabases",
                    read_preference=ReadPreference.PRIMARY)["databases"]]

    def drop_database(self, name_or_database):
        """Drop a database.

        Raises :class:`TypeError` if `name_or_database` is not an instance of
        :class:`basestring` (:class:`str` in python 3) or Database.

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
                            "%s or Database" % (basestring.__name__,))

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

        .. note:: Specifying `username` and `password` requires server
           version **>= 1.3.3+**.

        .. versionadded:: 1.5
        """
        if not isinstance(from_name, basestring):
            raise TypeError("from_name must be an instance "
                            "of %s" % (basestring.__name__,))
        if not isinstance(to_name, basestring):
            raise TypeError("to_name must be an instance "
                            "of %s" % (basestring.__name__,))

        database._check_name(to_name)

        command = {"fromdb": from_name, "todb": to_name}

        if from_host is not None:
            command["fromhost"] = from_host

        try:
            self.start_request()

            if username is not None:
                nonce = self.admin.command("copydbgetnonce",
                    read_preference=ReadPreference.PRIMARY,
                    fromhost=from_host)["nonce"]
                command["username"] = username
                command["nonce"] = nonce
                command["key"] = auth._auth_key(nonce, username, password)

            return self.admin.command("copydb",
                                      read_preference=ReadPreference.PRIMARY,
                                      **command)
        finally:
            self.end_request()

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

        .. versionadded:: 2.0
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

        .. versionadded:: 2.0
        """
        self.admin.command("fsync",
                           read_preference=ReadPreference.PRIMARY, **kwargs)

    def unlock(self):
        """Unlock a previously locked server.

        .. versionadded:: 2.0
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
