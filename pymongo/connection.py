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

.. warning::
   **DEPRECATED:** Please use :mod:`~pymongo.mongo_client` instead.

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
from pymongo.mongo_client import MongoClient
from pymongo.errors import ConfigurationError


class Connection(MongoClient):
    """Connection to MongoDB.
    """

    def __init__(self, host=None, port=None, max_pool_size=None,
                 network_timeout=None, document_class=dict,
                 tz_aware=False, _connect=True, **kwargs):
        """Create a new connection to a single MongoDB instance at *host:port*.

        .. warning::
           **DEPRECATED:** :class:`Connection` is deprecated. Please
           use :class:`~pymongo.mongo_client.MongoClient` instead.

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
          - `max_pool_size` (optional): The maximum number of connections
            that the pool will open simultaneously. If this is set, operations
            will block if there are `max_pool_size` outstanding connections
            from the pool. By default the pool size is unlimited.
          - `network_timeout` (optional): timeout (in seconds) to use
            for socket operations - default is no timeout
          - `document_class` (optional): default class to use for
            documents returned from queries on this connection
          - `tz_aware` (optional): if ``True``,
            :class:`~datetime.datetime` instances returned as values
            in a document by this :class:`Connection` will be timezone
            aware (otherwise they will be naive)

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
          - `auto_start_request`: If ``True`` (the default), each thread that
            accesses this Connection has a socket allocated to it for the
            thread's lifetime, or until :meth:`end_request` is called.
          - `use_greenlets`: if ``True``, :meth:`start_request()` will ensure
            that the current greenlet uses the same socket for all operations
            until :meth:`end_request()`. Defaults to ``False``.

          | **Write Concern options:**

          - `safe`: :class:`Connection` **disables** acknowledgement of write
            operations. Use ``safe=True`` to enable write acknowledgement.
          - `w`: (integer or string) If this is a replica set, write operations
            will block until they have been replicated to the specified number
            or tagged set of servers. `w=<int>` always includes the replica set
            primary (e.g. w=3 means write to the primary and wait until
            replicated to **two** secondaries). Implies safe=True.
          - `wtimeout`: (integer) Used in conjunction with `w`. Specify a value
            in milliseconds to control how long to wait for write propagation
            to complete. If replication does not complete in the given
            timeframe, a timeout exception is raised. Implies safe=True.
          - `j`: If ``True`` block until write operations have been committed
            to the journal. Cannot be used in combination with `fsync`. Prior
            to MongoDB 2.6 this option was ignored if the server was running
            without journaling. Starting with MongoDB 2.6 write operations will
            fail with an exception if this option is used when the server is
            running without journaling. Implies safe=True.
          - `fsync`: If ``True`` and the server is running without journaling,
            blocks until the server has synced all data files to disk. If the
            server is running with journaling, this acts the same as the `j`
            option, blocking until write operations have been committed to the
            journal. Cannot be used in combination with `j`. Implies safe=True.

          | **Replica-set keyword arguments for connecting with a replica-set
            - either directly or via a mongos:**
          | (ignored by standalone mongod instances)

          - `slave_okay` or `slaveOk` (deprecated): Use `read_preference`
            instead.
          - `replicaSet`: (string) The name of the replica-set to connect to.
            The driver will verify that the replica-set it connects to matches
            this name. Implies that the hosts specified are a seed list and the
            driver should attempt to find all members of the set. *Ignored by
            mongos*. Defaults to ``None``.
          - `read_preference`: The read preference for this client. If
            connecting to a secondary then a read preference mode *other* than
            PRIMARY is required - otherwise all queries will throw a
            :class:`~pymongo.errors.AutoReconnect` "not master" error.
            See :class:`~pymongo.read_preferences.ReadPreference` for all
            available read preference options. Defaults to ``PRIMARY``.
          - `tag_sets`: Ignored unless connecting to a replica-set via mongos.
            Specify a priority-order for tag sets, provide a list of
            tag sets: ``[{'dc': 'ny'}, {'dc': 'la'}, {}]``. A final, empty tag
            set, ``{}``, means "read from any member that matches the mode,
            ignoring tags. Defaults to ``[{}]``, meaning "ignore members'
            tags."

          | **SSL configuration:**

          - `ssl`: If ``True``, create the connection to the server using SSL.
            Defaults to ``False``.
          - `ssl_keyfile`: The private keyfile used to identify the local
            connection against mongod.  If included with the ``certfile` then
            only the ``ssl_certfile`` is needed.  Implies ``ssl=True``.
            Defaults to ``None``.
          - `ssl_certfile`: The certificate file used to identify the local
            connection against mongod. Implies ``ssl=True``. Defaults to
            ``None``.
          - `ssl_cert_reqs`: The parameter cert_reqs specifies whether a
            certificate is required from the other side of the connection,
            and whether it will be validated if provided. It must be one of the
            three values ``ssl.CERT_NONE`` (certificates ignored),
            ``ssl.CERT_OPTIONAL`` (not required, but validated if provided), or
            ``ssl.CERT_REQUIRED`` (required and validated). If the value of
            this parameter is not ``ssl.CERT_NONE``, then the ``ssl_ca_certs``
            parameter must point to a file of CA certificates.
            Implies ``ssl=True``. Defaults to ``ssl.CERT_NONE``.
          - `ssl_ca_certs`: The ca_certs file contains a set of concatenated
            "certification authority" certificates, which are used to validate
            certificates passed from the other end of the connection.
            Implies ``ssl=True``. Defaults to ``None``.

        .. seealso:: :meth:`end_request`
        .. versionchanged:: 2.5
           Added additional ssl options
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
        if network_timeout is not None:
            if (not isinstance(network_timeout, (int, float)) or
                network_timeout <= 0):
                raise ConfigurationError("network_timeout must "
                                         "be a positive integer")
            kwargs['socketTimeoutMS'] = network_timeout * 1000

        kwargs['auto_start_request'] = kwargs.get('auto_start_request', True)
        kwargs['safe'] = kwargs.get('safe', False)

        super(Connection, self).__init__(host, port,
                max_pool_size, document_class, tz_aware, _connect, **kwargs)

    def __repr__(self):
        if len(self.nodes) == 1:
            return "Connection(%r, %r)" % (self.host, self.port)
        else:
            return "Connection(%r)" % ["%s:%d" % n for n in self.nodes]

    def next(self):
        raise TypeError("'Connection' object is not iterable")
