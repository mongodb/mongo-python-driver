# Copyright 2009-2012 10gen, Inc.
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
import random
import socket
import struct
import time
import warnings

from bson.py3compat import b
from bson.son import SON
from pymongo import (common,
                     database,
                     helpers,
                     message,
                     pool,
                     uri_parser)
from pymongo.cursor_manager import CursorManager
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            DuplicateKeyError,
                            InvalidDocument,
                            InvalidURI,
                            OperationFailure)

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


class Connection(common.BaseObject):
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

          - `safe`: Use getlasterror for each write operation?
          - `j` or `journal`: Block until write operations have been committed
            to the journal. Ignored if the server is running without journaling.
            Implies safe=True.
          - `w`: (integer or string) If this is a replica set write operations
            won't return until they have been replicated to the specified
            number or tagged set of servers.
            Implies safe=True.
          - `wtimeout`: Used in conjunction with `j` and/or `w`. Wait this many
            milliseconds for journal acknowledgement and/or write replication.
            Implies safe=True.
          - `fsync`: Force the database to fsync all files before returning
            When used with `j` the server awaits the next group commit before
            returning.
            Implies safe=True.
          - `replicaSet`: The name of the replica set to connect to. The driver
            will verify that the replica set it connects to matches this name.
            Implies that the hosts specified are a seed list and the driver should
            attempt to find all members of the set.
          - `socketTimeoutMS`: How long a send or receive on a socket can take
            before timing out.
          - `connectTimeoutMS`: How long a connection can take to be opened
            before timing out.
          - `ssl`: If True, create the connection to the server using SSL.
          - `read_preference`: The read preference for this connection.
            See :class:`~pymongo.read_preferences.ReadPreference` for available
            options.
          - `auto_start_request`: If True (the default), each thread that
            accesses this Connection has a socket allocated to it for the
            thread's lifetime.  This ensures consistent reads, even if you read
            after an unsafe write.
          - `use_greenlets` (optional): if ``True``, :meth:`start_request()`
            will ensure that the current greenlet uses the same socket for all
            operations until :meth:`end_request()`
          - `slave_okay` or `slaveOk` (deprecated): Use `read_preference`
            instead.

        .. seealso:: :meth:`end_request`
        .. versionchanged:: 2.3
           Added support for failover between mongos seed list members.
        .. versionchanged:: 2.2
           Added `auto_start_request` option back. Added `use_greenlets`
           option.
        .. versionchanged:: 2.1
           Support `w` = integer or string.
           Added `ssl` option.
           DEPRECATED slave_okay/slaveOk.
        .. versionchanged:: 2.0
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
        .. versionadded:: 1.1
           The `network_timeout` parameter.

        .. mongodoc:: connections
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
        db = None
        options = {}
        for entity in host:
            if "://" in entity:
                if entity.startswith("mongodb://"):
                    res = uri_parser.parse_uri(entity, port)
                    seeds.update(res["nodelist"])
                    username = res["username"] or username
                    password = res["password"] or password
                    db = res["database"] or db
                    options = res["options"]
                else:
                    idx = entity.find("://")
                    raise InvalidURI("Invalid URI scheme: "
                                     "%s" % (entity[:idx],))
            else:
                seeds.update(uri_parser.split_hosts(entity, port))
        if not seeds:
            raise ConfigurationError("need to specify at least one host")

        self.__nodes = seeds
        self.__host = None
        self.__port = None
        self.__is_primary = False
        self.__is_mongos = False

        for option, value in kwargs.iteritems():
            option, value = common.validate(option, value)
            options[option] = value

        self.__max_pool_size = common.validate_positive_integer(
                                                'max_pool_size', max_pool_size)

        self.__cursor_manager = CursorManager(self)

        self.__repl = options.get('replicaset')
        if len(seeds) == 1 and not self.__repl:
            self.__direct = True
        else:
            self.__direct = False
            self.__nodes = set()

        if network_timeout is not None:
            if (not isinstance(network_timeout, (int, float)) or
                network_timeout <= 0):
                raise ConfigurationError("network_timeout must "
                                         "be a positive integer")
        self.__net_timeout = (network_timeout or
                              options.get('sockettimeoutms'))
        self.__conn_timeout = options.get('connecttimeoutms')
        self.__use_ssl = options.get('ssl', False)
        if self.__use_ssl and not pool.have_ssl:
            raise ConfigurationError("The ssl module is not available. If you "
                                     "are using a python version previous to "
                                     "2.6 you must install the ssl package "
                                     "from PyPI.")

        if options.get('use_greenlets', False):
            if not pool.have_greenlet:
                raise ConfigurationError(
                    "The greenlet module is not available. "
                    "Install the greenlet package from PyPI."
                )
            self.pool_class = pool.GreenletPool
        else:
            self.pool_class = pool.Pool

        self.__pool = self.pool_class(
            None,
            self.__max_pool_size,
            self.__net_timeout,
            self.__conn_timeout,
            self.__use_ssl
        )

        self.__document_class = document_class
        self.__tz_aware = common.validate_boolean('tz_aware', tz_aware)
        self.__auto_start_request = options.get('auto_start_request', True)

        # cache of existing indexes used by ensure_index ops
        self.__index_cache = {}
        self.__auth_credentials = {}

        super(Connection, self).__init__(**options)
        if self.slave_okay:
            warnings.warn("slave_okay is deprecated. Please "
                          "use read_preference instead.", DeprecationWarning)

        if _connect:
            self.__find_node(seeds)

        if db and username is None:
            warnings.warn("must provide a username and password "
                          "to authenticate to %s" % (db,))
        if username:
            db = db or "admin"
            if not self[db].authenticate(username, password):
                raise ConfigurationError("authentication failed")

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

    def _cache_credentials(self, db_name, username, password):
        """Add credentials to the database authentication cache
        for automatic login when a socket is created.

        If credentials are already cached for `db_name` they
        will be replaced.
        """
        self.__auth_credentials[db_name] = (username, password)

    def _purge_credentials(self, db_name=None):
        """Purge credentials from the database authentication cache.

        If `db_name` is None purge credentials for all databases.
        """
        if db_name is None:
            self.__auth_credentials.clear()
        elif db_name in self.__auth_credentials:
            del self.__auth_credentials[db_name]

    def __check_auth(self, sock_info):
        """Authenticate using cached database credentials.

        If credentials for the 'admin' database are available only
        this database is authenticated, since this gives global access.
        """
        authset = sock_info.authset
        names = set(self.__auth_credentials.iterkeys())

        # Logout from any databases no longer listed in the credentials cache.
        for dbname in authset - names:
            try:
                self.__simple_command(sock_info, dbname, {'logout': 1})
            # TODO: We used this socket to logout. Fix logout so we don't
            # have to catch this.
            except OperationFailure:
                pass
            authset.discard(dbname)

        # Once logged into the admin database we can access anything.
        if "admin" in authset:
            return

        if "admin" in self.__auth_credentials:
            username, password = self.__auth_credentials["admin"]
            self.__auth(sock_info, 'admin', username, password)
            authset.add('admin')
        else:
            for db_name in names - authset:
                user, pwd = self.__auth_credentials[db_name]
                self.__auth(sock_info, db_name, user, pwd)
                authset.add(db_name)

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
    def is_primary(self):
        """If this Connection is connected to a standalone, a replica-set
           primary, or the master of a master-slave set.

        .. versionadded:: 2.3
        """
        return self.__is_primary

    @property
    def is_mongos(self):
        """If this Connection is connected to mongos.

        .. versionadded:: 2.3
        """
        return self.__is_mongos

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

    @property
    def auto_start_request(self):
        return self.__auto_start_request

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

    def __simple_command(self, sock_info, dbname, spec):
        """Send a command to the server.
        """
        rqst_id, msg, _ = message.query(0, dbname + '.$cmd', 0, -1, spec)
        start = time.time()
        sock_info.sock.sendall(msg)
        response = self.__receive_message_on_socket(1, rqst_id, sock_info)
        end = time.time()
        response = helpers._unpack_response(response)['data'][0]
        msg = "command %r failed: %%s" % spec
        helpers._check_command_response(response, None, msg)
        return response, end - start

    def __auth(self, sock_info, dbname, user, passwd):
        """Authenticate socket against database `dbname`.
        """
        # Get a nonce
        response, _ = self.__simple_command(sock_info, dbname, {'getnonce': 1})
        nonce = response['nonce']
        key = helpers._auth_key(nonce, user, passwd)

        # Actually authenticate
        query = SON([('authenticate', 1),
            ('user', user), ('nonce', nonce), ('key', key)])
        self.__simple_command(sock_info, dbname, query)

    def __try_node(self, node):
        """Try to connect to this node and see if it works for our connection
        type. Returns ((host, port), ismaster, isdbgrid, res_time).

        :Parameters:
         - `node`: The (host, port) pair to try.
        """
        self.disconnect()
        self.__host, self.__port = node

        # Call 'ismaster' directly so we can get a response time.
        sock_info = self.__socket()
        response, res_time = self.__simple_command(sock_info,
                                                   'admin',
                                                   {'ismaster': 1})
        self.__pool.maybe_return_socket(sock_info)

        # Are we talking to a mongos?
        isdbgrid = response.get('msg', '') == 'isdbgrid'

        if "maxBsonObjectSize" in response:
            self.__max_bson_size = response["maxBsonObjectSize"]

        # Replica Set?
        if not self.__direct:
            # Check that this host is part of the given replica set.
            if self.__repl:
                set_name = response.get('setName')
                # The 'setName' field isn't returned by mongod before 1.6.2
                # so we can't assume that if it's missing this host isn't in
                # the specified set.
                if set_name and set_name != self.__repl:
                    raise ConfigurationError("%s:%d is not a member of "
                                             "replica set %s"
                                             % (node[0], node[1], self.__repl))
            if "hosts" in response:
                self.__nodes = set([_partition_node(h)
                                    for h in response["hosts"]])
            else:
                # The user passed a seed list of standalone or
                # mongos instances.
                self.__nodes.add(node)
            if response["ismaster"]:
                return node, True, isdbgrid, res_time
            elif "primary" in response:
                candidate = _partition_node(response["primary"])
                return self.__try_node(candidate)

            # Explain why we aren't using this connection.
            raise AutoReconnect('%s:%d is not primary or master' % node)

        # Direct connection
        if response.get("arbiterOnly", False) and not self.__direct:
            raise ConfigurationError("%s:%d is an arbiter" % node)
        return node, response['ismaster'], isdbgrid, res_time

    def __pick_nearest(self, candidates):
        """Return the 'nearest' candidate based on response time.
        """
        latency = self.secondary_acceptable_latency_ms
        # Only used for mongos high availability, res_time is in seconds.
        fastest = min([res_time for candidate, res_time in candidates])
        near_candidates = [
            candidate for candidate, res_time in candidates
            if res_time - fastest < latency / 1000.0
        ]

        node = random.choice(near_candidates)
        # Clear the pool from the last choice.
        self.disconnect()
        self.__host, self.__port = node
        return node

    def __find_node(self, seeds=None):
        """Find a host, port pair suitable for our connection type.

        If only one host was supplied to __init__ see if we can connect
        to it. Don't check if the host is a master/primary so we can make
        a direct connection to read from a secondary or send commands to
        an arbiter.

        If more than one host was supplied treat them as a seed list for
        connecting to a replica set or to support high availability for
        mongos. If connecting to a replica set try to find the primary
        and fail if we can't, possibly updating any replSet information
        on success. If a mongos seed list was provided find the "nearest"
        mongos and return it.

        Otherwise we iterate through the list trying to find a host we can
        send write operations to.

        Sets __host and __port so that :attr:`host` and :attr:`port`
        will return the address of the connected host. Sets __is_primary to
        True if this is a primary or master, else False. Sets __is_mongos
        to True if the connection is to a mongos.
        """
        errors = []
        mongos_candidates = []
        candidates = seeds or self.__nodes.copy()
        for candidate in candidates:
            try:
                node, ismaster, isdbgrid, res_time = self.__try_node(candidate)
                self.__is_primary = ismaster
                self.__is_mongos = isdbgrid
                # No need to calculate nearest if we only have one mongos.
                if isdbgrid and not self.__direct:
                    mongos_candidates.append((node, res_time))
                    continue
                elif len(mongos_candidates):
                    raise ConfigurationError("Seed list cannot contain a mix "
                                             "of mongod and mongos instances.")
                return node
            except Exception, why:
                errors.append(str(why))

        # If we have a mongos seed list, pick the "nearest" member.
        if len(mongos_candidates):
            self.__is_mongos = True
            return self.__pick_nearest(mongos_candidates)

        # Otherwise, try any hosts we discovered that were not in the seed list.
        for candidate in self.__nodes - candidates:
            try:
                node, ismaster, isdbgrid, _ = self.__try_node(candidate)
                self.__is_primary = ismaster
                self.__is_mongos = isdbgrid
                return node
            except Exception, why:
                errors.append(str(why))
        # Couldn't find a suitable host.
        self.disconnect()
        raise AutoReconnect(', '.join(errors))

    def __socket(self):
        """Get a SocketInfo from the pool.
        """
        host, port = (self.__host, self.__port)
        if host is None or port is None:
            host, port = self.__find_node()

        try:
            if self.__auto_start_request:
                # No effect if a request already started
                self.start_request()

            sock_info = self.__pool.get_socket((host, port))
        except socket.error, why:
            self.disconnect()
            raise AutoReconnect("could not connect to "
                                "%s:%d: %s" % (host, port, str(why)))
        if self.__auth_credentials:
            self.__check_auth(sock_info)
        return sock_info

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
        self.__pool.reset()
        self.__host = None
        self.__port = None

    def close(self):
        """Alias for :meth:`disconnect`

        Disconnecting will close all underlying sockets in the
        connection pool. If the :class:`Connection` is used again it
        will be automatically re-opened. Care should be taken to make
        sure that :meth:`disconnect` is not called in the middle of a
        sequence of operations in which ordering is important. This
        could lead to unexpected results.

        .. seealso:: :meth:`end_request`
        .. versionadded:: 2.1
        """
        self.disconnect()

    def set_cursor_manager(self, manager_class):
        """Set this connection's cursor manager.

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
                      "and will be removed in PyMongo 3.0.", DeprecationWarning)
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

        error_msg = error.get("err", "")
        if error_msg is None:
            return error
        if error_msg.startswith("not master"):
            self.disconnect()
            raise AutoReconnect(error_msg)

        details = error
        # mongos returns the error code in an error object
        # for some errors.
        if "errObjects" in error:
            for errobj in error["errObjects"]:
                if errobj["err"] == error_msg:
                    details = errobj
                    break

        if "code" in details:
            if details["code"] in [11000, 11001, 12582]:
                raise DuplicateKeyError(details["err"])
            else:
                raise OperationFailure(details["err"], details["code"])
        else:
            raise OperationFailure(details["err"])

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
        sock_info = self.__socket()
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
                rv = self.__check_response_to_last_error(response)

            self.__pool.maybe_return_socket(sock_info)
            return rv
        except (ConnectionFailure, socket.error), e:
            self.disconnect()
            raise AutoReconnect(str(e))

    def __receive_data_on_socket(self, length, sock_info):
        """Lowest level receive operation.

        Takes length to receive and repeatedly calls recv until able to
        return a buffer of that length, raising ConnectionFailure on error.
        """
        chunks = []
        while length:
            try:
                chunk = sock_info.sock.recv(length)
            except:
                # recv was interrupted
                self.__pool.discard_socket(sock_info)
                raise
            if chunk == EMPTY:
                raise ConnectionFailure("connection closed")
            length -= len(chunk)
            chunks.append(chunk)
        return EMPTY.join(chunks)

    def __receive_message_on_socket(self, operation, request_id, sock_info):
        """Receive a message in response to `request_id` on `sock`.

        Returns the response data with the header removed.
        """
        header = self.__receive_data_on_socket(16, sock_info)
        length = struct.unpack("<i", header[:4])[0]
        assert request_id == struct.unpack("<i", header[8:12])[0], \
            "ids don't match %r %r" % (request_id,
                                       struct.unpack("<i", header[8:12])[0])
        assert operation == struct.unpack("<i", header[12:])[0]

        return self.__receive_data_on_socket(length - 16, sock_info)

    def __send_and_receive(self, message, sock_info):
        """Send a message on the given socket and return the response data.
        """
        (request_id, data) = self.__check_bson_size(message)
        sock_info.sock.sendall(data)
        return self.__receive_message_on_socket(1, request_id, sock_info)

    # we just ignore _must_use_master here: it's only relevant for
    # MasterSlaveConnection instances.
    def _send_message_with_response(self, message,
                                    _must_use_master=False, **kwargs):
        """Send a message to Mongo and return the response.

        Sends the given message and returns the response.

        :Parameters:
          - `message`: (request_id, data) pair making up the message to send
        """
        sock_info = self.__socket()

        try:
            try:
                if "network_timeout" in kwargs:
                    sock_info.sock.settimeout(kwargs["network_timeout"])
                return self.__send_and_receive(message, sock_info)
            except (ConnectionFailure, socket.error), e:
                self.disconnect()
                raise AutoReconnect(str(e))
        finally:
            if "network_timeout" in kwargs:
                try:
                    # Restore the socket's original timeout and return it to
                    # the pool
                    sock_info.sock.settimeout(self.__net_timeout)
                    self.__pool.maybe_return_socket(sock_info)
                except socket.error:
                    # There was an exception and we've closed the socket
                    pass
            else:
                self.__pool.maybe_return_socket(sock_info)

    def start_request(self):
        """Ensure the current thread or greenlet always uses the same socket
        until it calls :meth:`end_request`. This ensures consistent reads,
        even if you read after an unsafe write.

        In Python 2.6 and above, or in Python 2.5 with
        "from __future__ import with_statement", :meth:`start_request` can be
        used as a context manager:

        >>> connection = pymongo.Connection(auto_start_request=False)
        >>> db = connection.test
        >>> _id = db.test_collection.insert({}, safe=True)
        >>> with connection.start_request():
        ...     for i in range(100):
        ...         db.test_collection.update({'_id': _id}, {'$set': {'i':i}})
        ...
        ...     # Definitely read the document after the final update completes
        ...     print db.test_collection.find({'_id': _id})

        .. versionadded:: 2.2
           The :class:`~pymongo.pool.Request` return value.
           :meth:`start_request` previously returned None
        """
        self.__pool.start_request()
        return pool.Request(self)

    def in_request(self):
        """True if :meth:`start_request` has been called, but not
        :meth:`end_request`, or if `auto_start_request` is True and
        :meth:`end_request` has not been called in this thread or greenlet.
        """
        return self.__pool.in_request()

    def end_request(self):
        """Undo :meth:`start_request` and allow this thread's connection to
        return to the pool.

        Calling :meth:`end_request` allows the :class:`~socket.socket` that has
        been reserved for this thread by :meth:`start_request` to be returned to
        the pool. Other threads will then be able to re-use that
        :class:`~socket.socket`. If your application uses many threads, or has
        long-running threads that infrequently perform MongoDB operations, then
        judicious use of this method can lead to performance gains. Care should
        be taken, however, to make sure that :meth:`end_request` is not called
        in the middle of a sequence of operations in which ordering is
        important. This could lead to unexpected results.
        """
        self.__pool.end_request()

    def __eq__(self, other):
        if isinstance(other, Connection):
            us = (self.__host, self.__port)
            them = (other.__host, other.__port)
            return us == them
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
        self[name].command("dropDatabase")

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

        in_request = self.in_request()
        try:
            if not in_request:
                self.start_request()

            if username is not None:
                nonce = self.admin.command("copydbgetnonce",
                                           fromhost=from_host)["nonce"]
                command["username"] = username
                command["nonce"] = nonce
                command["key"] = helpers._auth_key(nonce, username, password)

            return self.admin.command("copydb", **command)
        finally:
            if not in_request:
                self.end_request()

    @property
    def is_locked(self):
        """Is this server locked? While locked, all write operations
        are blocked, although read operations may still be allowed.
        Use :meth:`~pymongo.connection.Connection.unlock` to unlock.

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
        self.admin.command("fsync", **kwargs)

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
        raise TypeError("'Connection' object is not iterable")
