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

.. warning::
   **DEPRECATED:** Please use :mod:`~pymongo.mongo_replica_set_client` instead.

.. seealso:: :doc:`/examples/high_availability` for more examples of
   how to connect to a replica set.

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
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.errors import ConfigurationError


class ReplicaSetConnection(MongoReplicaSetClient):
    """Connection to a MongoDB replica set.
    """

    def __init__(self, hosts_or_uri=None, max_pool_size=10,
                 document_class=dict, tz_aware=False, **kwargs):
        """Create a new connection to a MongoDB replica set.

        .. warning::
           **DEPRECATED:** :class:`ReplicaSetConnection` is deprecated. Please
           use :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`
           instead

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

        .. note:: Instances of :class:`~ReplicaSetConnection` start a
           background task to monitor the state of the replica set. This allows
           it to quickly respond to changes in replica set configuration.
           Before discarding an instance of :class:`~ReplicaSetConnection` make
           sure you call :meth:`~close` to ensure that the monitor task is
           cleanly shut down.

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

          | **Other optional parameters can be passed as keyword arguments:**

          - `host`: For compatibility with connection.Connection. If both
            `host` and `hosts_or_uri` are specified `host` takes precedence.
          - `port`: For compatibility with connection.Connection. The default
            port number to use for hosts.
          - `network_timeout`: For compatibility with connection.Connection.
            The timeout (in seconds) to use for socket operations - default
            is no timeout. If both `network_timeout` and `socketTimeoutMS` are
            are specified `network_timeout` takes precedence, matching
            connection.Connection.
          - `socketTimeoutMS`: (integer) How long (in milliseconds) a send or
            receive on a socket can take before timing out.
          - `connectTimeoutMS`: (integer) How long (in milliseconds) a
            connection can take to be opened before timing out.
          - `auto_start_request`: If ``True`` (the default), each thread that
            accesses this :class:`ReplicaSetConnection` has a socket allocated
            to it for the thread's lifetime, for each member of the set. For
            :class:`~pymongo.read_preferences.ReadPreference` PRIMARY,
            auto_start_request=True ensures consistent reads, even if you read
            after an unsafe write. For read preferences other than PRIMARY,
            there are no consistency guarantees.
          - `use_greenlets`: if ``True``, use a background Greenlet instead of
            a background thread to monitor state of replica set. Additionally,
            :meth:`start_request()` will ensure that the current greenlet uses
            the same socket for all operations until :meth:`end_request()`.
            `use_greenlets` with ReplicaSetConnection requires `Gevent
            <http://gevent.org/>`_ to be installed.

          | **Write Concern options:**

          - `safe`: :class:`ReplicaSetConnection` **disables** acknowledgement
            of write operations. Use ``safe=True`` to enable write
            acknowledgement.
          - `w`: (integer or string) Write operations will block until they have
            been replicated to the specified number or tagged set of servers.
            `w=<int>` always includes the replica set primary (e.g. w=3 means
            write to the primary and wait until replicated to **two**
            secondaries). Implies safe=True.
          - `wtimeout`: (integer) Used in conjunction with `w`. Specify a value
            in milliseconds to control how long to wait for write propagation
            to complete. If replication does not complete in the given
            timeframe, a timeout exception is raised. Implies safe=True.
          - `j`: If ``True`` block until write operations have been committed
            to the journal. Ignored if the server is running without journaling.
            Implies safe=True.
          - `fsync`: If ``True`` force the database to fsync all files before
            returning. When used with `j` the server awaits the next group
            commit before returning. Implies safe=True.

          | **Read preference options:**

          - `slave_okay` or `slaveOk` (deprecated): Use `read_preference`
            instead.
          - `read_preference`: The read preference for this connection.
            See :class:`~pymongo.read_preferences.ReadPreference` for available
          - `tag_sets`: Read from replica-set members with these tags.
            To specify a priority-order for tag sets, provide a list of
            tag sets: ``[{'dc': 'ny'}, {'dc': 'la'}, {}]``. A final, empty tag
            set, ``{}``, means "read from any member that matches the mode,
            ignoring tags." :class:`MongoReplicaSetClient` tries each set of
            tags in turn until it finds a set of tags with at least one matching
            member.
          - `secondary_acceptable_latency_ms`: (integer) Any replica-set member
            whose ping time is within secondary_acceptable_latency_ms of the
            nearest member may accept reads. Default 15 milliseconds.
            **Ignored by mongos** and must be configured on the command line.
            See the localThreshold_ option for more information.

          | **SSL configuration:**

          - `ssl`: If ``True``, create the connection to the servers using SSL.
          - `ssl_keyfile`: The private keyfile used to identify the local
            connection against mongod.  If included with the ``certfile` then
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

        .. versionchanged:: 2.5
           Added addtional ssl options
        .. versionchanged:: 2.3
           Added `tag_sets` and `secondary_acceptable_latency_ms` options.
        .. versionchanged:: 2.2
           Added `auto_start_request` and `use_greenlets` options.
           Added support for `host`, `port`, and `network_timeout` keyword
           arguments for compatibility with connection.Connection.
        .. versionadded:: 2.1

        .. _localThreshold: http://docs.mongodb.org/manual/reference/mongos/#cmdoption-mongos--localThreshold
        """
        network_timeout = kwargs.pop('network_timeout', None)
        if network_timeout is not None:
            if (not isinstance(network_timeout, (int, float)) or
                network_timeout <= 0):
                raise ConfigurationError("network_timeout must "
                                         "be a positive integer")
            kwargs['socketTimeoutMS'] = network_timeout * 1000

        kwargs['auto_start_request'] = kwargs.get('auto_start_request', True)
        kwargs['safe'] = kwargs.get('safe', False)

        super(ReplicaSetConnection, self).__init__(
            hosts_or_uri, max_pool_size, document_class, tz_aware, **kwargs)

    def __repr__(self):
        return "ReplicaSetConnection(%r)" % (["%s:%d" % n
                                              for n in self.hosts],)
