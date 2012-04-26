# Copyright 2011-2012 10gen, Inc.
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

"""Tools for connecting to a MongoDB replica set.

.. seealso:: :doc:`/examples/replica_set` for more examples of how to
   connect to a replica set.

To get a :class:`~pymongo.database.Database` instance from a
:class:`ReplicaSetConnection` use either dictionary-style or
attribute-style access:

.. doctest::

  >>> from pymongo import ReplicaSetConnection
  >>> c = ReplicaSetConnection('localhost:27017', replicaSet='repl0')
  >>> c.test_database
  Database(ReplicaSetConnection([u'...', u'...']), u'test_database')
  >>> c['test_database']
  Database(ReplicaSetConnection([u'...', u'...']), u'test_database')
"""

import datetime
import socket
import struct
import threading
import time
import warnings
import weakref

from bson.py3compat import b
from bson.son import SON
from pymongo import (common,
                     database,
                     helpers,
                     message,
                     pool,
                     uri_parser,
                     ReadPreference)
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            DuplicateKeyError,
                            InvalidDocument,
                            OperationFailure)

EMPTY = b("")
MAX_BSON_SIZE = 4 * 1024 * 1024


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


have_gevent = False
try:
    import gevent
    from gevent import Greenlet
    have_gevent = True

    class GreenletMonitor(Greenlet):
        def __init__(self, obj, interval=5):
            Greenlet.__init__(self)
            self.obj = weakref.proxy(obj)
            self.interval = interval

        def _run(self):
            while True:
                try:
                    self.obj.refresh()
                # The connection object has been
                # collected so we should die.
                except ReferenceError:
                    break
                except:
                    pass
                gevent.sleep(self.interval)

except ImportError:
    pass


class ReplicaSetConnection(common.BaseObject):
    """Connection to a MongoDB replica set.
    """

    def __init__(self, hosts_or_uri=None, max_pool_size=10,
                 document_class=dict, tz_aware=False, **kwargs):
        """Create a new connection to a MongoDB replica set.

        The resultant connection object has connection-pooling built
        in. It also performs auto-reconnection when necessary. If an
        operation fails because of a connection error,
        :class:`~pymongo.errors.ConnectionFailure` is raised. If
        auto-reconnection will be performed,
        :class:`~pymongo.errors.AutoReconnect` will be
        raised. Application code should handle this exception
        (recognizing that the operation failed) and then continue to
        execute.

        Raises :class:`~pymongo.errors.ConnectionFailure` if
        the connection cannot be made.

        The `hosts_or_uri` parameter can be a full `mongodb URI
        <http://dochub.mongodb.org/core/connections>`_, in addition to
        a string of `host:port` pairs (e.g. 'host1:port1,host2:port2').
        If `hosts_or_uri` is None 'localhost:27017' will be used.

        :Parameters:
          - `hosts_or_uri` (optional): A MongoDB URI or string of `host:port`
            pairs. If a host is an IPv6 literal it must be enclosed in '[' and
            ']' characters following the RFC2732 URL syntax (e.g. '[::1]' for
            localhost)
          - `max_pool_size` (optional): The maximum size limit for
            each connection pool.
          - `document_class` (optional): default class to use for
            documents returned from queries on this connection
          - `tz_aware` (optional): if ``True``,
            :class:`~datetime.datetime` instances returned as values
            in a document by this :class:`ReplicaSetConnection` will be timezone
            aware (otherwise they will be naive)
          - `replicaSet`: (required) The name of the replica set to connect to.
            The driver will verify that each host it connects to is a member of
            this replica set. Can be passed as a keyword argument or as a
            MongoDB URI option.

          Other optional parameters can be passed as keyword arguments:

          - `safe`: Use getlasterror for each write operation?
          - `j` or `journal`: Block until write operations have been commited
            to the journal. Ignored if the server is running without
            journaling. Implies safe=True.
          - `w`: (integer or string) If this is a replica set write operations
            won't return until they have been replicated to the specified
            number or tagged set of servers.
            Implies safe=True.
          - `wtimeoutMS`: Used in conjunction with `j` and/or `w`. Wait this
            many milliseconds for journal acknowledgement and/or write
            replication. Implies safe=True.
          - `fsync`: Force the database to fsync all files before returning
            When used with `j` the server awaits the next group commit before
            returning. Implies safe=True.
          - `socketTimeoutMS`: How long a send or receive on a socket can take
            before timing out.
          - `connectTimeoutMS`: How long a connection can take to be opened
            before timing out.
          - `ssl`: If True, create the connection to the servers using SSL.
          - `read_preference`: The read preference for this connection.
            See :class:`~pymongo.ReadPreference` for available options.
          - `auto_start_request`: If True (the default), each thread that
            accesses this :class:`ReplicaSetConnection` has a socket allocated
            to it for the thread's lifetime, for each member of the set. For
            :class:`~pymongo.ReadPreference` PRIMARY, auto_start_request=True
            ensures consistent reads, even if you read after an unsafe
            write. For read preferences other than PRIMARY, there are no
            consistency guarantees. (The semantics of auto_start_request,
            :class:`~pymongo.ReadPreference`, and :class:`ReplicaSetConnection`
            may change in future releases of PyMongo.)
          - `use_greenlets` (optional): if ``True``, use a background Greenlet
            instead of a background thread to monitor state of replica set.
            :meth:`start_request()` will ensure that the current greenlet uses
            the same socket for all operations until :meth:`end_request()`.
            `use_greenlets` with ReplicaSetConnection requires `Gevent
            <http://gevent.org/>`_ to be installed.
          - `slave_okay` or `slaveOk` (deprecated): Use `read_preference`
            instead.
          - `host`: For compatibility with connection.Connection. If both
            `host` and `hosts_or_uri` are specified `host` takes precedence.
          - `port`: For compatibility with connection.Connection. The default
            port number to use for hosts.
          - `network_timeout`: For compatibility with connection.Connection.
            The timeout (in seconds) to use for socket operations - default
            is no timeout. If both `network_timeout` and `socketTimeoutMS` are
            are specified `network_timeout` takes precedence, matching
            connection.Connection.


        .. versionchanged:: 2.2
           Added `auto_start_request` and `use_greenlets` options.
           Added support for `host`, `port`, and `network_timeout` keyword
           arguments for compatibility with connection.Connection.
        .. versionadded:: 2.1
        """
        self.__opts = {}
        self.__seeds = set()
        self.__hosts = None
        self.__arbiters = set()
        self.__writer = None
        self.__readers = []
        self.__pools = {}
        self.__index_cache = {}
        self.__auth_credentials = {}
        self.__done = False

        self.__max_pool_size = common.validate_positive_integer(
                                        'max_pool_size', max_pool_size)
        self.__tz_aware = common.validate_boolean('tz_aware', tz_aware)
        self.__document_class = document_class

        # Compatibility with connection.Connection
        host = kwargs.pop('host', hosts_or_uri)

        port = kwargs.pop('port', 27017)
        if not isinstance(port, int):
            raise TypeError("port must be an instance of int")

        network_timeout = kwargs.pop('network_timeout', None)
        if network_timeout is not None:
            if (not isinstance(network_timeout, (int, float)) or
                network_timeout <= 0):
                raise ConfigurationError("network_timeout must "
                                         "be a positive integer")

        username = None
        db_name = None
        if host is None:
            self.__seeds.add(('localhost', port))
        elif '://' in host:
            res = uri_parser.parse_uri(host, port)
            self.__seeds.update(res['nodelist'])
            username = res['username']
            password = res['password']
            db_name = res['database']
            self.__opts = res['options']
        else:
            self.__seeds.update(uri_parser.split_hosts(host, port))

        for option, value in kwargs.iteritems():
            option, value = common.validate(option, value)
            self.__opts[option] = value

        if self.__opts.get('use_greenlets', False):
            if not have_gevent:
                raise ConfigurationError(
                    "The gevent module is not available. "
                    "Install the gevent package from PyPI."
                )
            self.pool_class = pool.GreenletPool
        else:
            self.pool_class = pool.Pool

        self.__auto_start_request = self.__opts.get('auto_start_request', True)
        self.__in_request = self.__auto_start_request
        self.__name = self.__opts.get('replicaset')
        if not self.__name:
            raise ConfigurationError("the replicaSet "
                                     "keyword parameter is required.")


        self.__net_timeout = (network_timeout or
                              self.__opts.get('sockettimeoutms'))
        self.__conn_timeout = self.__opts.get('connecttimeoutms')
        self.__use_ssl = self.__opts.get('ssl', False)
        if self.__use_ssl and not pool.have_ssl:
            raise ConfigurationError("The ssl module is not available. If you "
                                     "are using a python version previous to "
                                     "2.6 you must install the ssl package "
                                     "from PyPI.")

        super(ReplicaSetConnection, self).__init__(**self.__opts)
        if self.slave_okay:
            warnings.warn("slave_okay is deprecated. Please "
                          "use read_preference instead.", DeprecationWarning)

        self.refresh()

        if self.__opts.get('use_greenlets', False):
            monitor = GreenletMonitor(self)
        else:
            monitor = threading.Thread(target=self.__refresh_loop)
            monitor.setName("ReplicaSetMonitorThread")
            monitor.setDaemon(True)
        monitor.start()

        if db_name and username is None:
            warnings.warn("must provide a username and password "
                          "to authenticate to %s" % (db_name,))
        if username:
            db_name = db_name or 'admin'
            if not self[db_name].authenticate(username, password):
                raise ConfigurationError("authentication failed")

    def __del__(self):
        """Shutdown the monitor thread.
        """
        self.__done = True

    def __refresh_loop(self):
        """Refresh loop used in the standard monitor thread.
        """
        while True:
            if not self.__done:
                try:
                    self.refresh()
                # Catch literally everything here to avoid
                # exceptions when the interpreter shuts down.
                except:
                    pass
                if time:
                    time.sleep(5)

    def _cached(self, dbname, coll, index):
        """Test if `index` is cached.
        """
        cache = self.__index_cache
        now = datetime.datetime.utcnow()
        return (dbname in cache and
                coll in cache[dbname] and
                index in cache[dbname][coll] and
                now < cache[dbname][coll][index])

    def _cache_index(self, dbase, collection, index, ttl):
        """Add an index to the index cache for ensure_index operations.
        """
        now = datetime.datetime.utcnow()
        expire = datetime.timedelta(seconds=ttl) + now

        if dbase not in self.__index_cache:
            self.__index_cache[dbase] = {}
            self.__index_cache[dbase][collection] = {}
            self.__index_cache[dbase][collection][index] = expire

        elif collection not in self.__index_cache[dbase]:
            self.__index_cache[dbase][collection] = {}
            self.__index_cache[dbase][collection][index] = expire

        else:
            self.__index_cache[dbase][collection][index] = expire

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
    def seeds(self):
        """The seed list used to connect to this replica set.
        """
        return self.__seeds

    @property
    def hosts(self):
        """All active and passive (priority 0) replica set
        members known to this connection. This does not include
        hidden or slaveDelay members, or arbiters.
        """
        return self.__hosts

    @property
    def primary(self):
        """The current primary of the replica set.

        Returns None if there is no primary.
        """
        return self.__writer

    @property
    def secondaries(self):
        """The secondary members known to this connection.
        """
        return set(self.__readers)

    @property
    def arbiters(self):
        """The arbiters known to this connection.
        """
        return self.__arbiters

    @property
    def max_pool_size(self):
        """The maximum pool size limit set for this connection.
        """
        return self.__max_pool_size

    def get_document_class(self):
        """document_class getter"""
        return self.__document_class

    def set_document_class(self, klass):
        """document_class setter"""
        self.__document_class = klass

    document_class = property(get_document_class, set_document_class,
                              doc="""Default class to use for documents
                              returned on this connection.
                              """)

    @property
    def tz_aware(self):
        """Does this connection return timezone-aware datetimes?
        """
        return self.__tz_aware

    @property
    def max_bson_size(self):
        """Returns the maximum size BSON object the connected primary
        accepts in bytes. Defaults to 4MB in server < 1.7.4. Returns
        0 if no primary is available.
        """
        if self.__writer:
            return self.__pools[self.__writer]['max_bson_size']
        return 0

    @property
    def auto_start_request(self):
        return self.__auto_start_request

    def __simple_command(self, sock_info, dbname, spec):
        """Send a command to the server.
        """
        rqst_id, msg, _ = message.query(0, dbname + '.$cmd', 0, -1, spec)
        sock_info.sock.sendall(msg)
        response = self.__recv_msg(1, rqst_id, sock_info)
        response = helpers._unpack_response(response)['data'][0]
        msg = "command %r failed: %%s" % spec
        helpers._check_command_response(response, None, msg)
        return response

    def __auth(self, sock_info, dbname, user, passwd):
        """Authenticate socket against database `dbname`.
        """
        # Get a nonce
        response = self.__simple_command(sock_info, dbname, {'getnonce': 1})
        nonce = response['nonce']
        key = helpers._auth_key(nonce, user, passwd)

        # Actually authenticate
        query = SON([('authenticate', 1),
                     ('user', user), ('nonce', nonce), ('key', key)])
        self.__simple_command(sock_info, dbname, query)

    def __is_master(self, host):
        """Directly call ismaster.
        """
        pool = self.pool_class(host, self.__max_pool_size,
                               self.__net_timeout, self.__conn_timeout,
                               self.__use_ssl)
        sock_info = pool.get_socket()
        response = self.__simple_command(
            sock_info, 'admin', {'ismaster': 1}
        )

        pool.return_socket(sock_info)
        return response, pool

    def __update_pools(self):
        """Update the mapping of (host, port) pairs to connection pools.
        """
        secondaries = []
        for host in self.__hosts:
            mongo, sock_info = None, None
            try:
                if host in self.__pools:
                    mongo = self.__pools[host]
                    sock_info = self.__socket(mongo)
                    res = self.__simple_command(sock_info, 'admin', {'ismaster': 1})
                    mongo['pool'].return_socket(sock_info)
                else:
                    res, conn = self.__is_master(host)
                    bson_max = res.get('maxBsonObjectSize', MAX_BSON_SIZE)
                    self.__pools[host] = {'pool': conn,
                                          'last_checkout': time.time(),
                                          'max_bson_size': bson_max}
            except (ConnectionFailure, socket.error):
                if mongo and sock_info:
                    mongo['pool'].discard_socket(sock_info)
                continue
            # Only use hosts that are currently in 'secondary' state
            # as readers.
            if res['secondary']:
                secondaries.append(host)
            elif res['ismaster']:
                self.__writer = host
        self.__readers = secondaries

    def refresh(self):
        """Iterate through the existing host list, or possibly the
        seed list, to update the list of hosts and arbiters in this
        replica set.
        """
        errors = []
        nodes = self.__hosts or self.__seeds
        hosts = set()

        for node in nodes:
            mongo, sock_info = None, None
            try:
                if node in self.__pools:
                    mongo = self.__pools[node]
                    sock_info = self.__socket(mongo)
                    response = self.__simple_command(sock_info, 'admin',
                                                     {'ismaster': 1})
                    mongo['pool'].return_socket(sock_info)
                else:
                    response, conn = self.__is_master(node)

                # Check that this host is part of the given replica set.
                set_name = response.get('setName')
                # The 'setName' field isn't returned by mongod before 1.6.2
                # so we can't assume that if it's missing this host isn't in
                # the specified set.
                if set_name and set_name != self.__name:
                    host, port = node
                    raise ConfigurationError("%s:%d is not a member of "
                                             "replica set %s"
                                             % (host, port, self.__name))
                if "arbiters" in response:
                    self.__arbiters = set([_partition_node(h)
                                           for h in response["arbiters"]])
                if "hosts" in response:
                    hosts.update([_partition_node(h)
                                  for h in response["hosts"]])
                if "passives" in response:
                    hosts.update([_partition_node(h)
                                  for h in response["passives"]])
            except (ConnectionFailure, socket.error), why:
                if mongo and sock_info:
                    mongo['pool'].discard_socket(sock_info)
                errors.append("%s:%d: %s" % (node[0], node[1], str(why)))
            if hosts:
                self.__hosts = hosts
                break
        else:
            if errors:
                raise AutoReconnect(', '.join(errors))
            raise ConfigurationError('No suitable hosts found')

        self.__update_pools()

    def __check_is_primary(self, host):
        """Checks if this host is the primary for the replica set.
        """
        mongo, sock_info = None, None
        try:
            if host in self.__pools:
                mongo = self.__pools[host]
                sock_info = self.__socket(mongo)
                res = self.__simple_command(
                    sock_info, 'admin', {'ismaster': 1}
                )
            else:
                res, conn = self.__is_master(host)
                bson_max = res.get('maxBsonObjectSize', MAX_BSON_SIZE)
                self.__pools[host] = {'pool': conn,
                                      'last_checkout': time.time(),
                                      'max_bson_size': bson_max}
        except (ConnectionFailure, socket.error), why:
            if mongo and sock_info:
                mongo['pool'].discard_socket(sock_info)
            raise ConnectionFailure("%s:%d: %s" % (host[0], host[1], str(why)))
        
        if mongo and sock_info:
            mongo['pool'].return_socket(sock_info)

        if res["ismaster"]:
            return host
        elif "primary" in res:
            candidate = _partition_node(res["primary"])
            # Don't report the same connect failure multiple times.
            try:
                return self.__check_is_primary(candidate)
            except (ConnectionFailure, socket.error):
                pass
        raise AutoReconnect('%s:%d: not primary' % host)

    def __find_primary(self):
        """Returns a connection to the primary of this replica set,
        if one exists.
        """
        if self.__writer:
            return self.__pools[self.__writer]

        # This is either the first connection or we had a failover.
        self.refresh()

        errors = []
        for candidate in self.__hosts:
            try:
                self.__writer = self.__check_is_primary(candidate)
                return self.__pools[self.__writer]
            except (ConnectionFailure, socket.error), why:
                errors.append(str(why))
        # Couldn't find the primary.
        raise AutoReconnect(', '.join(errors))

    def __socket(self, mongo):
        """Get a SocketInfo from the pool.
        """
        pool = mongo['pool']
        if self.__auto_start_request:
            # No effect if a request already started
            self.start_request()

        sock_info = pool.get_socket()

        if self.__auth_credentials:
            self.__check_auth(sock_info)
        return sock_info

    def disconnect(self):
        """Disconnect from the replica set primary.
        """
        mongo = self.__pools.get(self.__writer)
        if mongo and 'pool' in mongo:
            mongo['pool'].reset()
        self.__writer = None

    def close(self):
        """Disconnect from all set members.
        """
        self.__writer = None
        self.__pools = {}

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

        if "code" in error:
            if error["code"] in [11000, 11001, 12582]:
                raise DuplicateKeyError(error["err"])
            else:
                raise OperationFailure(error["err"], error["code"])
        else:
            raise OperationFailure(error["err"])

    def __recv_data(self, length, sock_info):
        """Lowest level receive operation.

        Takes length to receive and repeatedly calls recv until able to
        return a buffer of that length, raising ConnectionFailure on error.
        """
        chunks = []
        while length:
            chunk = sock_info.sock.recv(length)
            if chunk == EMPTY:
                raise ConnectionFailure("connection closed")
            length -= len(chunk)
            chunks.append(chunk)
        return EMPTY.join(chunks)

    def __recv_msg(self, operation, request_id, sock):
        """Receive a message in response to `request_id` on `sock`.

        Returns the response data with the header removed.
        """
        header = self.__recv_data(16, sock)
        length = struct.unpack("<i", header[:4])[0]
        resp_id = struct.unpack("<i", header[8:12])[0]
        assert resp_id == request_id, "ids don't match %r %r" % (resp_id,
                                                                 request_id)
        assert operation == struct.unpack("<i", header[12:])[0]

        return self.__recv_data(length - 16, sock)

    def __check_bson_size(self, msg, max_size):
        """Make sure the message doesn't include BSON documents larger
        than the connected server will accept.

        :Parameters:
          - `msg`: message to check
        """
        if len(msg) == 3:
            request_id, data, max_doc_size = msg
            if max_doc_size > max_size:
                raise InvalidDocument("BSON document too large (%d bytes)"
                                      " - the connected server supports"
                                      " BSON document sizes up to %d"
                                      " bytes." %
                                      (max_doc_size, max_size))
            return (request_id, data)
        # get_more and kill_cursors messages
        # don't include BSON documents.
        return msg

    def _send_message(self, msg, safe=False, _connection_to_use=None):
        """Say something to Mongo.

        Raises ConnectionFailure if the message cannot be sent. Raises
        OperationFailure if `safe` is ``True`` and the
        response to the getLastError call returns an error. Return the
        response from lastError, or ``None`` if `safe` is ``False``.

        :Parameters:
          - `msg`: message to send
          - `safe`: check getLastError status after sending the message
        """
        if _connection_to_use in (None, -1):
            mongo = self.__find_primary()
        else:
            mongo = self.__pools[_connection_to_use]

        sock_info = None
        try:
            sock_info = self.__socket(mongo)
            rqst_id, data = self.__check_bson_size(msg,
                                                   mongo['max_bson_size'])
            sock_info.sock.sendall(data)
            # Safe mode. We pack the message together with a lastError
            # message and send both. We then get the response (to the
            # lastError) and raise OperationFailure if it is an error
            # response.
            rv = None
            if safe:
                response = self.__recv_msg(1, rqst_id, sock_info)
                rv = self.__check_response_to_last_error(response)
            mongo['pool'].return_socket(sock_info)
            return rv
        except(ConnectionFailure, socket.error), why:
            mongo['pool'].discard_socket(sock_info)
            if _connection_to_use in (None, -1):
                self.disconnect()
            raise AutoReconnect(str(why))
        except:
            mongo['pool'].discard_socket(sock_info)
            raise

    def __send_and_receive(self, mongo, msg, **kwargs):
        """Send a message on the given socket and return the response data.
        """
        sock_info = None
        try:
            sock_info = self.__socket(mongo)

            if "network_timeout" in kwargs:
                sock_info.sock.settimeout(kwargs['network_timeout'])

            rqst_id, data = self.__check_bson_size(msg,
                                                   mongo['max_bson_size'])
            sock_info.sock.sendall(data)
            response = self.__recv_msg(1, rqst_id, sock_info)

            if "network_timeout" in kwargs:
                sock_info.sock.settimeout(self.__net_timeout)
            mongo['pool'].return_socket(sock_info)

            return response
        except (ConnectionFailure, socket.error), why:
            host, port = mongo['pool'].pair
            mongo['pool'].discard_socket(sock_info)
            raise AutoReconnect("%s:%d: %s" % (host, port, str(why)))
        except:
            mongo['pool'].discard_socket(sock_info)
            raise

    def _send_message_with_response(self, msg, _connection_to_use=None,
                                    _must_use_master=False, **kwargs):
        """Send a message to Mongo and return the response.

        Sends the given message and returns the response.

        :Parameters:
          - `msg`: (request_id, data) pair making up the message to send
        """
        read_pref = kwargs.get('read_preference', ReadPreference.PRIMARY)
        mongo = None
        try:
            if _connection_to_use is not None:
                if _connection_to_use == -1:
                    mongo = self.__find_primary()
                else:
                    mongo = self.__pools[_connection_to_use]
                return mongo['pool'].pair, self.__send_and_receive(mongo,
                                                                   msg,
                                                                   **kwargs)
            elif _must_use_master or not read_pref:
                mongo = self.__find_primary()
                return mongo['pool'].pair, self.__send_and_receive(mongo,
                                                                   msg,
                                                                   **kwargs)
        except AutoReconnect:
            if mongo == self.__pools.get(self.__writer):
                self.disconnect()
            raise

        errors = []
        for host in helpers.shuffled(self.__readers):
            try:
                mongo = self.__pools[host]
                return host, self.__send_and_receive(mongo, msg, **kwargs)
            except AutoReconnect, why:
                errors.append(str(why))
        # Fallback to primary
        if read_pref == ReadPreference.SECONDARY:
            try:
                mongo = self.__find_primary()
                return mongo['pool'].pair, self.__send_and_receive(mongo,
                                                                   msg,
                                                                   **kwargs)
            except AutoReconnect, why:
                self.disconnect()
                errors.append(str(why))
        raise AutoReconnect(', '.join(errors))

    def start_request(self):
        """Ensure the current thread or greenlet always uses the same socket
        until it calls :meth:`end_request`. For
        :class:`~pymongo.ReadPreference` PRIMARY, auto_start_request=True
        ensures consistent reads, even if you read after an unsafe write. For
        read preferences other than PRIMARY, there are no consistency
        guarantees.

        In Python 2.6 and above, or in Python 2.5 with
        "from __future__ import with_statement", :meth:`start_request` can be
        used as a context manager:

        >>> connection = pymongo.ReplicaSetConnection(auto_start_request=False)
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
        for mongo in self.__pools.values():
            if 'pool' in mongo:
                mongo['pool'].start_request()

        self.__in_request = True
        return pool.Request(self)

    def in_request(self):
        """True if :meth:`start_request` has been called, but not
        :meth:`end_request`, or if `auto_start_request` is True and
        :meth:`end_request` has not been called in this thread or greenlet.
        """
        return self.__in_request

    def end_request(self):
        """Undo :meth:`start_request` and allow this thread's connections to
        replica set members to return to the pool.

        Calling :meth:`end_request` allows the :class:`~socket.socket` that has
        been reserved for this thread by :meth:`start_request` to be returned
        to the pool. Other threads will then be able to re-use that
        :class:`~socket.socket`. If your application uses many threads, or has
        long-running threads that infrequently perform MongoDB operations, then
        judicious use of this method can lead to performance gains. Care should
        be taken, however, to make sure that :meth:`end_request` is not called
        in the middle of a sequence of operations in which ordering is
        important. This could lead to unexpected results.
        """
        for mongo in self.__pools.values():
            if 'pool' in mongo:
                mongo['pool'].end_request()

        self.__in_request = False

    def __eq__(self, other):
        # XXX: Implement this?
        return NotImplemented

    def __repr__(self):
        return "ReplicaSetConnection(%r)" % (["%s:%d" % n
                                              for n in self.__hosts],)

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

    def close_cursor(self, cursor_id, _conn_id):
        """Close a single database cursor.

        Raises :class:`TypeError` if `cursor_id` is not an instance of
        ``(int, long)``. What closing the cursor actually means
        depends on this connection's cursor manager.

        :Parameters:
          - `cursor_id`: id of cursor to close
        """
        if not isinstance(cursor_id, (int, long)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self._send_message(message.kill_cursors([cursor_id]),
                           _connection_to_use=_conn_id)

    def server_info(self):
        """Get information about the MongoDB primary we're connected to.
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
        :class:`basestring` (:class:`str` in python 3) or Database

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
