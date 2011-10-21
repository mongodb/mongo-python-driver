# Copyright 2009-2011 10gen, Inc.
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

THIS IS NOT READY FOR PRODUCTION USE AND IS NOT
YET SUPPORTED BY 10GEN. USE AT YOUR OWN RISK!!!!

To get a :class:`~pymongo.database.Database` instance from a
:class:`ReplicaSetConnection` use either dictionary-style or
attribute-style access:

.. doctest::

  >>> from pymongo import ReplicaSetConnection
  >>> c = ReplicaSetConnection('localhost:31017', replicaSet='repl0')
  >>> c.test_database
  Database(ReplicaSetConnection([u'...', u'...']), u'test_database')
  >>> c['test_database']
  Database(ReplicaSetConnection([u'...', u'...']), u'test_database')
"""

import datetime
import random
import socket
import struct
import sys
import time
import warnings

from bson.son import SON
from pymongo import (common,
                     database,
                     helpers,
                     message,
                     pool,
                     uri_parser,
                     PRIMARY,
                     SECONDARY,
                     SECONDARY_ONLY)
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            DuplicateKeyError,
                            InvalidDocument,
                            OperationFailure)


if sys.platform.startswith('java'):
    from select import cpython_compatible_select as select
else:
    from select import select


MAX_BSON_SIZE = 4 * 1024 * 1024


def _closed(sock):
    """Return True if we know socket has been closed, False otherwise.
    """
    try:
        readers, _, _ = select([sock], [], [], 0)
    # Any exception here is equally bad (select.error, ValueError, etc.).
    except Exception:
        return True
    return len(readers) > 0


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
            in a document by this :class:`Connection` will be timezone
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

        .. versionadded:: 2.0.1+
        """
        warnings.warn("ReplicaSetConnection IS NOT READY "
                      "FOR PRODUCTION USE. USE AT YOUR OWN RISK!")

        self.__max_pool_size = max_pool_size
        self.__document_class = document_class
        self.__tz_aware = tz_aware
        self.__opts = {}
        self.__seeds = set()
        self.__hosts = None
        self.__arbiters = set()
        self.__writer = None
        self.__readers = []
        self.__reader_pools = {}
        self.__index_cache = {}
        self.__auth_credentials = {}
        self.__read_pref = SECONDARY
        username = None
        db_name = None
        if hosts_or_uri is None:
            self.__seeds.add(('localhost', 27017))
        elif '://' in hosts_or_uri:
            res = uri_parser.parse_uri(hosts_or_uri)
            self.__seeds.update(res['nodelist'])
            username = res['username']
            password = res['password']
            db_name = res['database']
            self.__opts = res['options']
        else:
            self.__seeds.update(uri_parser.split_hosts(hosts_or_uri))

        for option, value in kwargs.iteritems():
            option, value = common.validate(option, value)
            self.__opts[option] = value

        self.__name = self.__opts.get('replicaset')
        if not self.__name:
            raise ConfigurationError("the replicaSet "
                                     "keyword parameter is required.")
        self.__net_timeout = self.__opts.get('sockettimeoutms')
        self.__conn_timeout = self.__opts.get('connecttimeoutms')

        super(ReplicaSetConnection, self).__init__(**self.__opts)

        # TODO: Connect with no primary.
        self.__find_primary()

        if db_name and username is None:
            warnings.warn("must provide a username and password "
                          "to authenticate to %s" % (db_name,))
        if username:
            db_name = db_name or 'admin'
            if not self[db_name].authenticate(username, password):
                raise ConfigurationError("authentication failed")

    def _cache_index(self, dbase, collection, index, ttl):
        """Add an index to the index cache for ensure_index operations.

        Return ``True`` if the index has been newly cached or if the index had
        expired and is being re-cached.

        Return ``False`` if the index exists and is valid.
        """
        now = datetime.datetime.utcnow()
        expire = datetime.timedelta(seconds=ttl) + now

        if dbase not in self.__index_cache:
            self.__index_cache[dbase] = {}
            self.__index_cache[dbase][collection] = {}
            self.__index_cache[dbase][collection][index] = expire
            return True

        if collection not in self.__index_cache[dbase]:
            self.__index_cache[dbase][collection] = {}
            self.__index_cache[dbase][collection][index] = expire
            return True

        if index in self.__index_cache[dbase][collection]:
            if now < self.__index_cache[dbase][collection][index]:
                return False

        self.__index_cache[dbase][collection][index] = expire
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

    def __check_auth(self, sock, authset):
        """Authenticate using cached database credentials.

        If credentials for the 'admin' database are available only
        this database is authenticated, since this gives global access.
        """
        names = set(self.__auth_credentials.iterkeys())

        # Logout from any databases no longer listed in the credentials cache.
        for dbname in authset - names:
            try:
                self.__simple_command(sock, dbname, {'logout': 1})
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
            self.__auth(sock, 'admin', username, password)
            authset.add('admin')
        else:
            for db_name in names - authset:
                user, pwd = self.__auth_credentials[db_name]
                self.__auth(sock, db_name, user, pwd)
                authset.add(db_name)


    @property
    def seeds(self):
        """Seed list used to connect to this replica set.
        """
        return self.__seeds

    @property
    def hosts(self):
        """All active hosts known to this replica set.
        """
        return self.__hosts

    @property
    def arbiters(self):
        """All arbiters known to this replica set.
        """
        return self.__arbiters

    @property
    def slave_okay(self):
        """Is it OK to perform queries on a secondary? This is
        always True for an instance of ReplicaSetConnection.
        """
        return True

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

    def get_read_preference(self):
        """read_pref getter"""
        return self.__read_pref

    def set_read_preference(self, pref):
        """read_pref setter"""
        if pref not in (PRIMARY, SECONDARY, SECONDARY_ONLY):
            raise ConfigurationError("Not a valid read preference.")
        self.__read_pref = pref
        self.__update_readers()

    read_preference = property(get_read_preference, set_read_preference,
                              doc="""The read preference setting for
                              this ReplicaSetConnection.
                              """)

    @property
    def tz_aware(self):
        """Does this connection return timezone-aware datetimes?
        """
        return self.__tz_aware

    def __simple_command(self, sock, dbname, spec):
        """Send a command to the server.
        """
        rqst_id, msg, _ = message.query(0, dbname + '.$cmd', 0, -1, spec)
        sock.sendall(msg)
        response = self.__recv_msg(1, rqst_id, sock)
        response = helpers._unpack_response(response)['data'][0]
        msg = "command %r failed: %%s" % spec
        helpers._check_command_response(response, None, msg)
        return response

    def __auth(self, sock, dbname, user, passwd):
        """Authenticate socket `sock` against database `dbname`.
        """
        # Get a nonce
        response = self.__simple_command(sock, dbname, {'getnonce': 1})
        nonce = response['nonce']
        key = helpers._auth_key(nonce, user, passwd)

        # Actually authenticate
        query = SON([('authenticate', 1),
                     ('user', user), ('nonce', nonce), ('key', key)])
        self.__simple_command(sock, dbname, query)

    def __is_master(self, host):
        """Directly call ismaster.
        """
        mongo = pool.Pool(host, self.__max_pool_size,
                          self.__net_timeout, self.__conn_timeout)
        sock = mongo.get_socket()[0]
        response = self.__simple_command(sock, 'admin', {'ismaster': 1})
        return response, mongo

    def __update_readers(self):
        """Update the list of hosts we can send read requests to.
        """
        readers = []
        reader_pools = {}
        if self.__read_pref != PRIMARY:
            for host in self.__hosts:
                try:
                    response, mongo = self.__is_master(host)
                    if (self.__read_pref == SECONDARY_ONLY and
                        response['ismaster']):
                        continue
                    bson_max = response.get('max_bson_size') or MAX_BSON_SIZE
                    readers.append(host)
                    reader_pools[host] = {'pool': mongo,
                                          'last_checkout': time.time(),
                                          'max_bson_size': bson_max}
                except:
                    continue
        self.__readers = readers
        self.__reader_pools = reader_pools

    def __refresh_hosts(self):
        """Iterate through the existing host list, or possibly the
        seed list, to update the list of hosts and arbiters in this
        replica set.
        """
        errors = []
        nodes = self.__hosts or self.__seeds

        for node in nodes:
            try:
                response, _ = self.__is_master(node)

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
                    self.__hosts = set([_partition_node(h)
                                        for h in response["hosts"]])
                    break
            except Exception, why:
                errors.append("%s:%d: %s" % (node[0], node[1], str(why)))
        else:
            # Couldn't find a suitable host.
            raise AutoReconnect(', '.join(errors))

    def __check_is_primary(self, host):
        """Checks if this host is the primary for the replica set.
        """
        try:
            response, mongo = self.__is_master(host)
        except Exception, why:
            raise AutoReconnect("%s:%d: %s" % (host[0], host[1], str(why)))
        bson_max = response.get('max_bson_size') or MAX_BSON_SIZE
        if response["ismaster"]:
            self.__writer = {'pool': mongo,
                             'last_checkout': time.time(),
                             'max_bson_size': bson_max}
            return True
        elif "primary" in response:
            candidate = _partition_node(response["primary"])
            # Don't report the same connect failure multiple times.
            try:
                return self.__check_is_primary(candidate)
            except:
                pass
        raise AutoReconnect('%s:%d: not primary' % host)

    def __find_primary(self):
        """Returns a connection to the primary of this replica set,
        if one exists.
        """
        if self.__writer:
            return self.__writer

        # This is either the first connection or we had a failover.
        self.__refresh_hosts()
        self.__update_readers()

        errors = []
        for candidate in self.__hosts:
            try:
                if self.__check_is_primary(candidate):
                    return self.__writer
            except Exception, why:
                errors.append(str(why))
        # Couldn't find the primary.
        raise AutoReconnect(', '.join(errors))

    def __socket(self, mongo):
        """Get a socket from the pool.

        If it's been > 1 second since the last time we checked out a
        socket, we also check to see if the socket has been closed -
        this let's us avoid seeing *some*
        :class:`~pymongo.errors.AutoReconnect` exceptions on server
        hiccups, etc. We only do this if it's been > 1 second since
        the last socket checkout, to keep performance reasonable - we
        can't avoid those completely anyway.
        """
        try:
            sock, authset = mongo['pool'].get_socket()

            now = time.time()
            if now - mongo['last_checkout'] > 1:
                if _closed(sock):
                    host = mongo['pool'].host
                    mongo['pool'] = pool.Pool(host,
                                              self.__max_pool_size,
                                              self.__net_timeout,
                                              self.__conn_timeout)
                    sock, authset = mongo['pool'].get_socket()
            mongo['last_checkout'] = now
        except socket.error, why:
            host, port = mongo['pool'].host
            raise AutoReconnect("could not connect to "
                                "%s:%d: %s" % (host, port, str(why)))
        if self.__auth_credentials or authset:
            self.__check_auth(sock, authset)
        return sock

    def disconnect(self):
        """Disconnect from MongoDB.

        Disconnecting will close all underlying sockets in the primary's
        connection pool. If the primary is used again a new connection will
        be made. Care should be taken to make sure that :meth:`disconnect`
        is not called in the middle of a sequence of operations in which
        ordering is important. This could lead to unexpected results.
        """
        self.__writer = None

    def close(self):
        """Disconnect from all set members.
        """
        self.__writer = None
        self.__readers = []
        self.__reader_pools = {}

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

    def __recv_data(self, length, sock):
        """Lowest level receive operation.

        Takes length to receive and repeatedly calls recv until able to
        return a buffer of that length, raising ConnectionFailure on error.
        """
        chunks = []
        while length:
            chunk = sock.recv(length)
            if chunk == "":
                raise ConnectionFailure("connection closed")
            length -= len(chunk)
            chunks.append(chunk)
        return "".join(chunks)

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
            host = self.__readers[_connection_to_use]
            mongo = self.__reader_pools[host]
        try:
            try:
                rqst_id, data = self.__check_bson_size(msg,
                                                       mongo['max_bson_size'])
                sock = self.__socket(mongo)
                sock.sendall(data)
                # Safe mode. We pack the message together with a lastError
                # message and send both. We then get the response (to the
                # lastError) and raise OperationFailure if it is an error
                # response.
                if safe:
                    response = self.__recv_msg(1, rqst_id, sock)
                    return self.__check_response_to_last_error(response)
                return None
            except(ConnectionFailure, socket.error), why:
                if _connection_to_use in (None, -1):
                    self.disconnect()
                raise AutoReconnect(str(why))
        finally:
            mongo['pool'].return_socket()

    def __send_and_receive(self, mongo, msg, timeout):
        """Send a message on the given socket and return the response data.
        """
        try:
            sock = self.__socket(mongo)
            if timeout:
                sock.settimeout(timeout)
            rqst_id, data = self.__check_bson_size(msg, mongo['max_bson_size'])
            sock.sendall(data)
            return self.__recv_msg(1, rqst_id, sock)
        finally:
            if timeout:
                sock.settimeout(self.__net_timeout)
            mongo['pool'].return_socket()

    def __iter_secondaries(self, msg, timeout):
        """Iterate through the readers attempting to return a read
        result.
        """
        errors = []
        for conn_id in random.sample(range(0, len(self.__readers)),
                                     len(self.__readers)):
            try:
                host = self.__readers[conn_id]
                mongo = self.__reader_pools[host]
                return conn_id, self.__send_and_receive(mongo, msg, timeout)
            except Exception, why:
                errors.append(str(why))
        else:
            if (self.__read_pref == SECONDARY_ONLY or
                not self.__writer):
                raise AutoReconnect(', '.join(errors))
            # Fall back to primary...
            try:
                return -1, self.__send_and_receive(self.__writer, msg, timeout)
            except Exception, why:
                self.disconnect()
                errors.append(str(why))
                raise AutoReconnect(', '.join(errors))

    def _send_message_with_response(self, msg, _connection_to_use=None,
                                    _must_use_master=False, **kwargs):
        """Send a message to Mongo and return the response.

        Sends the given message and returns the response.

        :Parameters:
          - `msg`: (request_id, data) pair making up the message to send
        """
        timeout = kwargs.get("network_timeout")
        if _connection_to_use is not None:
            if _connection_to_use == -1:
                mongo = self.__find_primary()
            else:
                host = self.__readers[_connection_to_use]
                mongo = self.__reader_pools[host]
        elif _must_use_master or self.__read_pref == PRIMARY:
            _connection_to_use = -1
            mongo = self.__find_primary()
        else:
            return self.__iter_secondaries(msg, timeout)

        try:
            return _connection_to_use, self.__send_and_receive(mongo,
                                                               msg, timeout)
        except(ConnectionFailure, socket.error), why:
            if _connection_to_use == -1:
                self.disconnect()
            raise AutoReconnect(str(why))

    def __cmp__(self, other):
        # XXX: Implement this?
        return NotImplemented

    def __repr__(self):
        hosts = self.__hosts.copy()
        if self.__writer:
            hosts.update([self.__writer['pool'].host])
        return "ReplicaSetConnection(%r)" % (["%s:%d" % n for n in hosts],)

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

    def close_cursor(self, cursor_id, conn_id):
        """Close a single database cursor.

        Raises :class:`TypeError` if `cursor_id` is not an instance of
        ``(int, long)``. What closing the cursor actually means
        depends on this connection's cursor manager.

        :Parameters:
          - `cursor_id`: id of cursor to close
        """
        if not isinstance(cursor_id, (int, long)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self._send_message(message.kill_cursors([cursor_id]), conn_id)

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
