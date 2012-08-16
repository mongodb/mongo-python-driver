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

import datetime
import socket
import struct
import sys
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
                     uri_parser)
from pymongo.read_preferences import (
    ReadPreference, select_member, modes, MovingAverage)
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            DuplicateKeyError,
                            InvalidDocument,
                            OperationFailure)

EMPTY = b("")
MAX_BSON_SIZE = 4 * 1024 * 1024
MAX_RETRY = 3

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


class Monitor(object):
    """Base class for replica set monitors.
    """
    _refresh_interval = 30
    def __init__(self, rsc, event_class):
        self.rsc = weakref.proxy(rsc, self.shutdown)
        self.event = event_class()
        self.stopped = False

    def shutdown(self, dummy):
        """Signal the monitor to shutdown.
        """
        self.stopped = True
        self.event.set()

    def schedule_refresh(self):
        """Refresh immediately
        """
        self.event.set()

    def monitor(self):
        """Run until the RSC is collected or an
        unexpected error occurs.
        """
        while True:
            self.event.wait(Monitor._refresh_interval)
            if self.stopped:
                break
            self.event.clear()
            try:
                self.rsc.refresh()
            except AutoReconnect:
                pass
            # RSC has been collected or there
            # was an unexpected error.
            except:
                break


class MonitorThread(Monitor, threading.Thread):
    """Thread based replica set monitor.
    """
    def __init__(self, rsc):
        Monitor.__init__(self, rsc, threading.Event)
        threading.Thread.__init__(self)
        self.setName("ReplicaSetMonitorThread")

    def run(self):
        """Override Thread's run method.
        """
        self.monitor()


have_gevent = False
try:
    from gevent import Greenlet
    from gevent.event import Event

    # Used by ReplicaSetConnection
    from gevent.local import local as gevent_local
    have_gevent = True

    class MonitorGreenlet(Monitor, Greenlet):
        """Greenlet based replica set monitor.
        """
        def __init__(self, rsc):
            Monitor.__init__(self, rsc, Event)
            Greenlet.__init__(self)

        # Don't override `run` in a Greenlet. Add _run instead.
        # Refer to gevent's Greenlet docs and source for more
        # information.
        def _run(self):
            """Define Greenlet's _run method.
            """
            self.monitor()

except ImportError:
    pass


class Member(object):
    """Represent one member of a replica set
    """
    # For unittesting only. Use under no circumstances!
    _host_to_ping_time = {}

    def __init__(self, host, ismaster_response, ping_time, connection_pool):
        self.host = host
        self.pool = connection_pool
        self.ping_time = MovingAverage(5)
        self.update(ismaster_response, ping_time)

    def update(self, ismaster_response, ping_time):
        self.is_primary = ismaster_response['ismaster']
        self.max_bson_size = ismaster_response.get(
            'maxBsonObjectSize', MAX_BSON_SIZE)
        self.tags = ismaster_response.get('tags', {})
        self.record_ping_time(ping_time)
        self.up = True

    def get_avg_ping_time(self):
        """Get a moving average of this member's ping times
        """
        if self.host in Member._host_to_ping_time:
            # Simulate ping times for unittesting
            return Member._host_to_ping_time[self.host]

        return self.ping_time.get()

    def record_ping_time(self, ping_time):
        self.ping_time.update(ping_time)

    def matches_mode(self, mode):
        if mode == ReadPreference.PRIMARY and not self.is_primary:
            return False

        if mode == ReadPreference.SECONDARY and self.is_primary:
            return False

        return True

    def matches_tags(self, tags):
        """Return True if this member's tags are a superset of the passed-in
           tags. E.g., if this member is tagged {'dc': 'ny', 'rack': '1'},
           then it matches {'dc': 'ny'}.
        """
        for key, value in tags.items():
            if key not in self.tags or self.tags[key] != value:
                return False

        return True

    def matches_tag_sets(self, tag_sets):
        """Return True if this member matches any of the tag sets, e.g.
           [{'dc': 'ny'}, {'dc': 'la'}, {}]
        """
        for tags in tag_sets:
            if self.matches_tags(tags):
                return True

        return False


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

          Other optional parameters can be passed as keyword arguments:

          - `safe`: Use getlasterror for each write operation?
          - `j` or `journal`: Block until write operations have been committed
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
            See :class:`~pymongo.read_preferences.ReadPreference` for available
          - `tag_sets`: Read from replica-set members with these tags.
            To specify a priority-order for tag sets, provide a list of
            tag sets: ``[{'dc': 'ny'}, {'dc': 'la'}, {}]``. A final, empty tag
            set, ``{}``, means "read from any member that matches the mode,
            ignoring tags." ReplicaSetConnection tries each set of tags in turn
            until it finds a set of tags with at least one matching member.
          - `secondary_acceptable_latency_ms`: Any replica-set member whose
            ping time is within secondary_acceptable_latency_ms of the nearest
            member may accept reads. Default 15 milliseconds.
          - `auto_start_request`: If True (the default), each thread that
            accesses this :class:`ReplicaSetConnection` has a socket allocated
            to it for the thread's lifetime, for each member of the set. For
            :class:`~pymongo.read_preferences.ReadPreference` PRIMARY,
            auto_start_request=True ensures consistent reads, even if you read
            after an unsafe write. For read preferences other than PRIMARY,
            there are no consistency guarantees.
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


        .. versionchanged:: 2.3
           Added `tag_sets` and `secondary_acceptable_latency_ms` options.
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
        self.__members = {}
        self.__index_cache = {}
        self.__auth_credentials = {}

        self.__max_pool_size = common.validate_positive_integer(
                                        'max_pool_size', max_pool_size)
        self.__tz_aware = common.validate_boolean('tz_aware', tz_aware)
        self.__document_class = document_class
        self.__monitor = None

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
        self.__reset_pinned_hosts()
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

        if db_name and username is None:
            warnings.warn("must provide a username and password "
                          "to authenticate to %s" % (db_name,))
        if username:
            db_name = db_name or 'admin'
            if not self[db_name].authenticate(username, password):
                raise ConfigurationError("authentication failed")

        # Start the monitor after we know the configuration is correct.
        if self.__opts.get('use_greenlets', False):
            self.__monitor = MonitorGreenlet(self)
        else:
            # NOTE: Don't ever make this a daemon thread in CPython. Daemon
            # threads in CPython cause serious issues when the interpreter is
            # torn down. Symptoms range from random exceptions to the
            # interpreter dumping core.
            self.__monitor = MonitorThread(self)
            # Sadly, weakrefs aren't totally reliable in PyPy and Jython
            # so use a daemon thread there.
            if (sys.platform.startswith('java') or
                'PyPy' in sys.version):
                self.__monitor.setDaemon(True)
        self.__monitor.start()

    def _cached(self, dbname, coll, index):
        """Test if `index` is cached.
        """
        cache = self.__index_cache
        now = datetime.datetime.utcnow()
        return (dbname in cache and
                coll in cache[dbname] and
                index in cache[dbname][coll] and
                now < cache[dbname][coll][index])

    def _cache_index(self, dbase, collection, index, cache_for):
        """Add an index to the index cache for ensure_index operations.
        """
        now = datetime.datetime.utcnow()
        expire = datetime.timedelta(seconds=cache_for) + now

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
    def is_mongos(self):
        """If this ReplicaSetConnection is connected to mongos (always False)

        .. versionadded:: 2.3
        """
        return False

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
            return self.__members[self.__writer].max_bson_size
        return 0

    @property
    def auto_start_request(self):
        return self.__auto_start_request

    def __simple_command(self, sock_info, dbname, spec):
        """Send a command to the server.
           Returns (response, ping_time in seconds).
        """
        rqst_id, msg, _ = message.query(0, dbname + '.$cmd', 0, -1, spec)
        start = time.time()
        sock_info.sock.sendall(msg)
        response = self.__recv_msg(1, rqst_id, sock_info)
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

    def __is_master(self, host):
        """Directly call ismaster.
           Returns (response, connection_pool, ping_time in seconds).
        """
        connection_pool = self.pool_class(
            host, self.__max_pool_size, self.__net_timeout, self.__conn_timeout,
            self.__use_ssl)

        sock_info = connection_pool.get_socket()
        try:
            response, ping_time = self.__simple_command(
                sock_info, 'admin', {'ismaster': 1}
            )

            connection_pool.maybe_return_socket(sock_info)
            return response, connection_pool, ping_time
        except (ConnectionFailure, socket.error):
            connection_pool.discard_socket(sock_info)
            raise

    def __update_pools(self):
        """Update the mapping of (host, port) pairs to connection pools.
        """
        primary = None
        secondaries = []
        for host in self.__hosts:
            member, sock_info = None, None
            try:
                if host in self.__members:
                    member = self.__members[host]
                    sock_info = self.__socket(member)
                    res, ping_time = self.__simple_command(
                        sock_info, 'admin', {'ismaster': 1})
                    member.pool.maybe_return_socket(sock_info)
                    member.update(res, ping_time)
                else:
                    res, connection_pool, ping_time = self.__is_master(host)
                    self.__members[host] = Member(
                        host=host,
                        ismaster_response=res,
                        ping_time=ping_time,
                        connection_pool=connection_pool)
            except (ConnectionFailure, socket.error):
                if member:
                    member.pool.discard_socket(sock_info)
                    self.__members.pop(member.host, None)
                continue
            # Only use hosts that are currently in 'secondary' state
            # as readers.
            if res['secondary']:
                secondaries.append(host)
            elif res['ismaster']:
                primary = host

        if primary != self.__writer:
            self.__reset_pinned_hosts()

        self.__writer = primary
        self.__readers = secondaries

    def __schedule_refresh(self):
        self.__monitor.schedule_refresh()

    def __pin_host(self, host):
        # After first successful read in a request, continue reading from same
        # member until read preferences change, host goes down, or
        # end_request(). This offers a small assurance that reads won't jump
        # around in time.
        self.__threadlocal.host = host

    def __pinned_host(self):
        return getattr(self.__threadlocal, 'host', None)

    def __unpin_host(self):
        self.__threadlocal.host = None

    def __reset_pinned_hosts(self):
        if self.__opts.get('use_greenlets', False):
            self.__threadlocal = gevent_local()
        else:
            self.__threadlocal = threading.local()

    def refresh(self):
        """Iterate through the existing host list, or possibly the
        seed list, to update the list of hosts and arbiters in this
        replica set.
        """
        errors = []
        nodes = self.__hosts or self.__seeds
        hosts = set()

        for node in nodes:
            member, sock_info = None, None
            try:
                if node in self.__members:
                    member = self.__members[node]
                    sock_info = self.__socket(member)
                    response, _ = self.__simple_command(
                        sock_info, 'admin', {'ismaster': 1})
                    member.pool.maybe_return_socket(sock_info)
                else:
                    response, _, _ = self.__is_master(node)

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
                if member:
                    member.pool.discard_socket(sock_info)
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
        member, sock_info = None, None
        try:
            if host in self.__members:
                member = self.__members[host]
                sock_info = self.__socket(member)
                res, ping_time = self.__simple_command(
                    sock_info, 'admin', {'ismaster': 1}
                )
            else:
                res, connection_pool, ping_time = self.__is_master(host)
                self.__members[host] = Member(
                    host=host,
                    ismaster_response=res,
                    ping_time=ping_time,
                    connection_pool=connection_pool)
        except (ConnectionFailure, socket.error), why:
            if member:
                member.pool.discard_socket(sock_info)
            raise ConnectionFailure("%s:%d: %s" % (host[0], host[1], str(why)))

        if member and sock_info:
            member.pool.maybe_return_socket(sock_info)

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
            primary = self.__members[self.__writer]
            if primary.up:
                return primary

        # This is either the first connection or we had a failover.
        self.refresh()

        errors = []
        for candidate in self.__hosts:
            try:
                self.__writer = self.__check_is_primary(candidate)
                return self.__members[self.__writer]
            except (ConnectionFailure, socket.error), why:
                errors.append(str(why))
        # Couldn't find the primary.
        raise AutoReconnect(', '.join(errors))

    def __socket(self, member):
        """Get a SocketInfo from the pool.
        """
        if self.__auto_start_request:
            # No effect if a request already started
            self.start_request()

        sock_info = member.pool.get_socket()

        if self.__auth_credentials:
            self.__check_auth(sock_info)
        return sock_info

    def disconnect(self):
        """Disconnect from the replica set primary.
        """
        member = self.__members.get(self.__writer)
        if member:
            member.pool.reset()
        self.__writer = None

    def close(self):
        """Close this ReplicaSetConnection.

        This method first terminates the replica set monitor, then disconnects
        from all members of the replica set. Once called this instance of
        ReplicaSetConnection should not be reused.

        .. versionchanged:: 2.2.1
           The :meth:`~close` method now terminates the replica set monitor.
        """
        if self.__monitor:
            self.__monitor.shutdown(None)
            # Use a reasonable timeout.
            self.__monitor.join(1.0)
            self.__monitor = None
        self.__writer = None
        self.__members = {}

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
            member = self.__find_primary()
        else:
            member = self.__members[_connection_to_use]

        sock_info = None
        try:
            sock_info = self.__socket(member)
            rqst_id, data = self.__check_bson_size(msg, member.max_bson_size)
            sock_info.sock.sendall(data)
            # Safe mode. We pack the message together with a lastError
            # message and send both. We then get the response (to the
            # lastError) and raise OperationFailure if it is an error
            # response.
            rv = None
            if safe:
                response = self.__recv_msg(1, rqst_id, sock_info)
                rv = self.__check_response_to_last_error(response)
            member.pool.maybe_return_socket(sock_info)
            return rv
        except(ConnectionFailure, socket.error), why:
            member.pool.discard_socket(sock_info)
            if _connection_to_use in (None, -1):
                self.disconnect()
            raise AutoReconnect(str(why))
        except:
            member.pool.discard_socket(sock_info)
            raise

    def __send_and_receive(self, member, msg, **kwargs):
        """Send a message on the given socket and return the response data.
        """
        sock_info = None
        try:
            sock_info = self.__socket(member)

            if "network_timeout" in kwargs:
                sock_info.sock.settimeout(kwargs['network_timeout'])

            rqst_id, data = self.__check_bson_size(msg, member.max_bson_size)
            sock_info.sock.sendall(data)
            response = self.__recv_msg(1, rqst_id, sock_info)

            if "network_timeout" in kwargs:
                sock_info.sock.settimeout(self.__net_timeout)
            member.pool.maybe_return_socket(sock_info)

            return response
        except (ConnectionFailure, socket.error), why:
            host, port = member.pool.pair
            member.pool.discard_socket(sock_info)
            raise AutoReconnect("%s:%d: %s" % (host, port, str(why)))
        except:
            member.pool.discard_socket(sock_info)
            raise

    def __try_read(self, member, msg, **kwargs):
        """Attempt a read from a member; on failure mark the member "down" and
           wake up the monitor thread to refresh as soon as possible.
        """
        try:
            return self.__send_and_receive(member, msg, **kwargs)
        except AutoReconnect:
            member.up = False
            self.__schedule_refresh()
            raise

    def _send_message_with_response(self, msg, _connection_to_use=None,
                                    _must_use_master=False, **kwargs):
        """Send a message to Mongo and return the response.

        Sends the given message and returns (host used, response).

        :Parameters:
          - `msg`: (request_id, data) pair making up the message to send
        """

        # If we've disconnected since last read, trigger refresh
        try:
            self.__find_primary()
        except AutoReconnect:
            # We'll throw an error later
            pass

        tag_sets = kwargs.get('tag_sets', [{}])
        mode = kwargs.get('read_preference', ReadPreference.PRIMARY)
        if _must_use_master:
            mode = ReadPreference.PRIMARY
            tag_sets = [{}]

        secondary_acceptable_latency_ms = kwargs.get(
            'secondary_acceptable_latency_ms',
            self.secondary_acceptable_latency_ms)

        member = None
        try:
            if _connection_to_use is not None:
                if _connection_to_use == -1:
                    member = self.__find_primary()
                else:
                    member = self.__members[_connection_to_use]
                return member.pool.pair, self.__try_read(
                    member, msg, **kwargs)
        except AutoReconnect:
            if member == self.__members.get(self.__writer):
                self.disconnect()
            raise

        errors = []
        pinned_member = self.__members.get(self.__pinned_host())
        if (pinned_member
            and pinned_member.matches_mode(mode)
            and pinned_member.matches_tag_sets(tag_sets)
            and pinned_member.up
        ):
            try:
                return (
                    pinned_member.host,
                    self.__try_read(pinned_member, msg, **kwargs))
            except AutoReconnect, why:
                if _must_use_master or mode == ReadPreference.PRIMARY:
                    self.disconnect()
                    raise
                else:
                    errors.append(str(why))

        # No pinned member, or pinned member down or doesn't match read pref
        self.__unpin_host()

        members = self.__members.copy().values()

        while len(errors) < MAX_RETRY:
            member = select_member(
                members=members,
                mode=mode,
                tag_sets=tag_sets,
                latency=secondary_acceptable_latency_ms)

            if not member:
                # Ran out of members to try
                break

            try:
                # Sets member.up False on failure, so select_member won't try
                # it again.
                response = self.__try_read(member, msg, **kwargs)

                # Success
                if self.in_request():
                    # Keep reading from this member in this thread / greenlet
                    self.__pin_host(member.host)
                return member.host, response
            except AutoReconnect, why:
                errors.append(str(why))
                members.remove(member)

        # Ran out of tries
        if mode == ReadPreference.PRIMARY:
            msg = "No replica set primary available for query"
        elif mode == ReadPreference.SECONDARY:
            msg = "No replica set secondary available for query"
        else:
            msg = "No replica set members available for query"

        msg += " with ReadPreference %s" % modes[mode]

        if tag_sets != [{}]:
            msg += " and tags " + repr(tag_sets)

        raise AutoReconnect(msg, errors)

    def start_request(self):
        """Ensure the current thread or greenlet always uses the same socket
        until it calls :meth:`end_request`. For
        :class:`~pymongo.read_preferences.ReadPreference` PRIMARY,
        auto_start_request=True ensures consistent reads, even if you read
        after an unsafe write. For read preferences other than PRIMARY, there
        are no consistency guarantees.

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
        for member in self.__members.values():
            member.pool.start_request()

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
        for member in self.__members.values():
            member.pool.end_request()

        self.__in_request = False
        self.__unpin_host()

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
