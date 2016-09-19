# Copyright 2011-2015 MongoDB, Inc.
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
:class:`MongoReplicaSetClient` use either dictionary-style or
attribute-style access:

.. doctest::

  >>> from pymongo import MongoReplicaSetClient
  >>> c = MongoReplicaSetClient('localhost:27017', replicaSet='repl0')
  >>> c.test_database
  Database(MongoReplicaSetClient([u'...', u'...']), u'test_database')
  >>> c['test_database']
  Database(MongoReplicaSetClient([u'...', u'...']), u'test_database')
"""

import atexit
import datetime
import socket
import struct
import threading
import time
import warnings
import weakref

from bson.binary import PYTHON_LEGACY
from bson.codec_options import CodecOptions
from bson.py3compat import b
from pymongo import (auth,
                     common,
                     database,
                     helpers,
                     message,
                     pool,
                     thread_util,
                     uri_parser)
from pymongo.member import Member
from pymongo.read_preferences import (
    ReadPreference, select_member, modes, MovingAverage, _ServerMode)
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            DocumentTooLarge,
                            DuplicateKeyError,
                            OperationFailure,
                            InvalidOperation)
from pymongo.thread_util import DummyLock

EMPTY = b("")
MAX_RETRY = 3

MONITORS = set()

def register_monitor(monitor):
    ref = weakref.ref(monitor, _on_monitor_deleted)
    MONITORS.add(ref)

def _on_monitor_deleted(ref):
    """Remove the weakreference from the set
    of active MONITORS. We no longer
    care about keeping track of it
    """
    MONITORS.remove(ref)

def shutdown_monitors():
    # Keep a local copy of MONITORS as
    # shutting down threads has a side effect
    # of removing them from the MONITORS set()
    monitors = list(MONITORS)
    for ref in monitors:
        monitor = ref()
        if monitor:
            monitor.shutdown()
            monitor.join(10)
atexit.register(shutdown_monitors)

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


# Concurrency notes: A MongoReplicaSetClient keeps its view of the replica-set
# state in an RSState instance. RSStates are immutable, except for
# host-pinning. Pools, which are internally thread / greenlet safe, can be
# copied from old to new RSStates safely. The client updates its view of the
# set's state not by modifying its RSState but by replacing it with an updated
# copy.

# In __init__, MongoReplicaSetClient gets a list of potential members called
# 'seeds' from its initial parameters, and calls refresh(). refresh() iterates
# over the the seeds in arbitrary order looking for a member it can connect to.
# Once it finds one, it calls 'ismaster' and sets self.__hosts to the list of
# members in the response, and connects to the rest of the members. refresh()
# sets the MongoReplicaSetClient's RSState. Finally, __init__ launches the
# replica-set monitor.

# The monitor calls refresh() every 30 seconds, or whenever the client has
# encountered an error that prompts it to wake the monitor.

# Every method that accesses the RSState multiple times within the method makes
# a local reference first and uses that throughout, so it's isolated from a
# concurrent method replacing the RSState with an updated copy. This technique
# avoids the need to lock around accesses to the RSState.


class RSState(object):
    def __init__(
            self, threadlocal, hosts=None, host_to_member=None, arbiters=None,
            writer=None, error_message='No primary available', exc=None,
            initial=False):
        """An immutable snapshot of the client's view of the replica set state.

        Stores Member instances for all members we're connected to, and a
        list of (host, port) pairs for all the hosts and arbiters listed
        in the most recent ismaster response.

        :Parameters:
          - `threadlocal`: Thread- or greenlet-local storage
          - `hosts`: Sequence of (host, port) pairs
          - `host_to_member`: Optional dict: (host, port) -> Member instance
          - `arbiters`: Optional sequence of arbiters as (host, port)
          - `writer`: Optional (host, port) of primary
          - `error_message`: Optional error if `writer` is None
          - `exc`: Optional error if state is unusable
          - `initial`: Whether this is the initial client state
        """
        self._threadlocal = threadlocal  # threading.local or gevent local
        self._arbiters = frozenset(arbiters or [])  # set of (host, port)
        self._writer = writer  # (host, port) of the primary, or None
        self._error_message = error_message
        self._host_to_member = host_to_member or {}
        self._hosts = frozenset(hosts or [])
        self._members = frozenset(self._host_to_member.values())
        self._exc = exc
        self._initial = initial
        self._primary_member = self.get(writer)

    def clone_with_host_down(self, host, error_message):
        """Get a clone, marking as "down" the member with the given (host, port)
        """
        members = self._host_to_member.copy()
        members.pop(host, None)

        if host == self.writer:
            # The primary went down; record the error message.
            return RSState(
                self._threadlocal,
                self._hosts,
                members,
                self._arbiters,
                None,
                error_message,
                self._exc)
        else:
            # Some other host went down. Keep our current primary or, if it's
            # already down, keep our current error message.
            return RSState(
                self._threadlocal,
                self._hosts,
                members,
                self._arbiters,
                self._writer,
                self._error_message,
                self._exc)

    def clone_without_writer(self, threadlocal):
        """Get a clone without a primary. Unpins all threads.

        :Parameters:
          - `threadlocal`: Thread- or greenlet-local storage
        """
        return RSState(
            threadlocal,
            self._hosts,
            self._host_to_member,
            self._arbiters)

    def clone_with_error(self, exc):
        return RSState(
            self._threadlocal,
            self._hosts,
            self._host_to_member.copy(),
            self._arbiters,
            self._writer,
            self._error_message,
            exc)

    @property
    def arbiters(self):
        """(host, port) pairs from the last ismaster response's arbiter list.
        """
        return self._arbiters

    @property
    def writer(self):
        """(host, port) of primary, or None."""
        return self._writer

    @property
    def primary_member(self):
        return self._primary_member

    @property
    def hosts(self):
        """(host, port) pairs from the last ismaster response's host list."""
        return self._hosts

    @property
    def members(self):
        """Set of Member instances."""
        return self._members

    @property
    def error_message(self):
        """The error, if any, raised when trying to connect to the primary"""
        return self._error_message

    @property
    def secondaries(self):
        """Set of (host, port) pairs, secondaries we're connected to."""
        # Unlike the other properties, this isn't cached because it isn't used
        # in regular operations.
        return set([
            host for host, member in self._host_to_member.items()
            if member.is_secondary])

    @property
    def exc(self):
        """Reason RSState is unusable, or None."""
        return self._exc

    @property
    def initial(self):
        """Whether this is the initial client state."""
        return self._initial

    def get(self, host):
        """Return a Member instance or None for the given (host, port)."""
        return self._host_to_member.get(host)

    def pin_host(self, host, mode, tag_sets, latency):
        """Pin this thread / greenlet to a member.

        `host` is a (host, port) pair. The remaining parameters are a read
        preference.
        """
        # Fun fact: Unlike in thread_util.ThreadIdent, we needn't lock around
        # assignment here. Assignment to a threadlocal is only unsafe if it
        # can cause other Python code to run implicitly.
        self._threadlocal.host = host
        self._threadlocal.read_preference = (mode, tag_sets, latency)

    def keep_pinned_host(self, mode, tag_sets, latency):
        """Does a read pref match the last used by this thread / greenlet?"""
        return self._threadlocal.read_preference == (mode, tag_sets, latency)

    @property
    def pinned_host(self):
        """The (host, port) last used by this thread / greenlet, or None."""
        return getattr(self._threadlocal, 'host', None)

    def unpin_host(self):
        """Forget this thread / greenlet's last used member."""
        self._threadlocal.host = self._threadlocal.read_preference = None

    @property
    def threadlocal(self):
        return self._threadlocal

    def __str__(self):
        return '<RSState [%s] writer="%s">' % (
            ', '.join(str(member) for member in self._host_to_member.itervalues()),
            self.writer and '%s:%s' % self.writer or None)


class Monitor(object):
    """Base class for replica set monitors.
    """
    _refresh_interval = 30

    def __init__(self, rsc, event_class):
        self.rsc = weakref.proxy(rsc, self.shutdown)
        self.timer = event_class()
        self.refreshed = event_class()
        self.started_event = event_class()
        self.stopped = False

    def start_sync(self):
        """Start the Monitor and block until it's really started.
        """
        # start() can return before the thread is fully bootstrapped,
        # so a fork can leave the thread thinking it's alive in a child
        # process when it's really dead:
        # http://bugs.python.org/issue18418.
        self.start()  # Implemented in subclasses.
        self.started_event.wait(5)

    def shutdown(self, dummy=None):
        """Signal the monitor to shutdown.
        """
        self.stopped = True
        self.timer.set()

    def schedule_refresh(self):
        """Refresh immediately
        """
        if not self.isAlive():
            # Checks in RS client should prevent this.
            raise AssertionError("schedule_refresh called with dead monitor")
        self.refreshed.clear()
        self.timer.set()

    def wait_for_refresh(self, timeout_seconds):
        """Block until a scheduled refresh completes
        """
        self.refreshed.wait(timeout_seconds)

    def monitor(self):
        """Run until the RSC is collected or an
        unexpected error occurs.
        """
        self.started_event.set()
        while True:
            self.timer.wait(Monitor._refresh_interval)
            if self.stopped:
                break
            self.timer.clear()

            try:
                try:
                    self.rsc.refresh()
                finally:
                    self.refreshed.set()
            except (AutoReconnect, OperationFailure):
                pass

            # RSC has been collected or there
            # was an unexpected error.
            except:
                break

    def isAlive(self):
        raise NotImplementedError()


class MonitorThread(threading.Thread, Monitor):
    """Thread based replica set monitor.
    """
    def __init__(self, rsc):
        Monitor.__init__(self, rsc, threading.Event)
        threading.Thread.__init__(self)
        self.setName("ReplicaSetMonitorThread")
        self.setDaemon(True)

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
            self.monitor_greenlet_alive = False
            Monitor.__init__(self, rsc, Event)
            Greenlet.__init__(self)

        def start_sync(self):
            self.monitor_greenlet_alive = True

            # Call superclass.
            Monitor.start_sync(self)

        # Don't override `run` in a Greenlet. Add _run instead.
        # Refer to gevent's Greenlet docs and source for more
        # information.
        def _run(self):
            """Define Greenlet's _run method.
            """
            self.monitor()

        def isAlive(self):
            # bool(self) isn't immediately True after someone calls start(),
            # but isAlive() is. Thus it's safe for greenlets to do:
            # "if not monitor.isAlive(): monitor.start()"
            # ... and be guaranteed only one greenlet starts the monitor.
            return self.monitor_greenlet_alive

except ImportError:
    pass


class MongoReplicaSetClient(common.BaseObject):
    """Connection to a MongoDB replica set.
    """

    # For tests.
    _refresh_timeout_sec = 5
    _rs_client = True

    def __init__(self, hosts_or_uri=None, max_pool_size=100,
                 document_class=dict, tz_aware=False, **kwargs):
        """Create a new connection to a MongoDB replica set.

        The resultant client object has connection-pooling built
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

        .. note:: Instances of :class:`MongoReplicaSetClient` start a
           background task to monitor the state of the replica set. This allows
           it to quickly respond to changes in replica set configuration.
           Before discarding an instance of :class:`MongoReplicaSetClient` make
           sure you call :meth:`~close` to ensure that the monitor task is
           cleanly shut down.

        :Parameters:
          - `hosts_or_uri` (optional): A MongoDB URI or string of `host:port`
            pairs. If a host is an IPv6 literal it must be enclosed in '[' and
            ']' characters following the RFC2732 URL syntax (e.g. '[::1]' for
            localhost)
          - `max_pool_size` (optional): The maximum number of connections
            each pool will open simultaneously. If this is set, operations
            will block if there are `max_pool_size` outstanding connections
            from the pool. Defaults to 100. Cannot be 0.
          - `document_class` (optional): default class to use for
            documents returned from queries on this client
          - `tz_aware` (optional): if ``True``,
            :class:`~datetime.datetime` instances returned as values
            in a document by this :class:`MongoReplicaSetClient` will be timezone
            aware (otherwise they will be naive)
          - `replicaSet`: (required) The name of the replica set to connect to.
            The driver will verify that each host it connects to is a member of
            this replica set. Can be passed as a keyword argument or as a
            MongoDB URI option.

          | **Other optional parameters can be passed as keyword arguments:**

          - `host`: For compatibility with :class:`~mongo_client.MongoClient`.
            If both `host` and `hosts_or_uri` are specified `host` takes
            precedence.
          - `port`: For compatibility with :class:`~mongo_client.MongoClient`.
            The default port number to use for hosts.
          - `maxpoolsize` (optional): Alias for max_pool_size. Takes
            precendence over max_pool_size.
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
          - `connect`: if True (the default), immediately connect to MongoDB
            in the foreground. Otherwise connect on the first operation.

          | **Write Concern options:**
          | (Only set if passed. No default values.)

          - `w`: (integer or string) Write operations will block until they have
            been replicated to the specified number or tagged set of servers.
            `w=<int>` always includes the replica set primary (e.g. w=3 means
            write to the primary and wait until replicated to **two**
            secondaries). Passing w=0 **disables write acknowledgement** and all
            other write concern options.
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

          | **Read preference options:**

          - `read_preference`: The read preference for this client.
            See :mod:`~pymongo.read_preferences` for available
            options. Defaults to ``ReadPreference.PRIMARY``.
          - `localThresholdMS`: (integer) Any replica-set member
            whose ping time is within localThresholdMS of the
            nearest member may accept reads. Default 15 milliseconds.
            **Ignored by mongos** and must be configured on the command line.
            See the localThreshold_ option for more information.

          | **SSL configuration:**

            See :doc:`/examples/tls` for examples.

          - `ssl`: If ``True``, create the connection to the servers using SSL.
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
          - `ssl_match_hostname`: If ``True`` (the default), and
            `ssl_cert_reqs` is not ``ssl.CERT_NONE``, enables hostname
            verification using the :func:`~ssl.match_hostname` function from
            python's :mod:`~ssl` module. Think very carefully before setting
            this to ``False`` as that could make your application vulnerable to
            man-in-the-middle attacks.

        .. versionchanged:: 2.5
           Added additional ssl options
        .. versionadded:: 2.4
        """
        self.__opts = {}
        self.__seeds = set()
        self.__index_cache = {}
        self.__index_cache_lock = threading.Lock()
        self.__auth_credentials = {}

        self.__monitor = None
        self.__closed = False

        # Compatibility with mongo_client.MongoClient
        host = kwargs.pop('host', hosts_or_uri)

        port = kwargs.pop('port', 27017)
        if not isinstance(port, int):
            raise TypeError("port must be an instance of int")

        username = None
        password = None
        self.__default_database_name = None
        options = {}
        if host is None:
            self.__seeds.add(('localhost', port))
        elif '://' in host:
            res = uri_parser.parse_uri(host, port)
            self.__seeds.update(res['nodelist'])
            username = res['username']
            password = res['password']
            self.__default_database_name = res['database']
            options = res['options']
        else:
            self.__seeds.update(uri_parser.split_hosts(host, port))

        # _pool_class and _monitor_class are for deep customization of PyMongo,
        # e.g. Motor. SHOULD NOT BE USED BY DEVELOPERS EXTERNAL TO MONGODB.
        self.pool_class = kwargs.pop('_pool_class', pool.Pool)
        self.__monitor_class = kwargs.pop('_monitor_class', None)

        # tag_sets is only supported through kwargs since it must be a list.
        if "tag_sets" in kwargs:
            warnings.warn("tag_sets is deprecated in this version of PyMongo "
                          "and removed in PyMongo 3. Pass a read preference "
                          "object as read_preference instead",
                          DeprecationWarning, stacklevel=2)

        # Support _ServerMode through kwargs
        pref = kwargs.get("read_preference")
        if isinstance(pref, _ServerMode):
            kwargs["read_preference"] = pref.mode
            kwargs["tag_sets"] = pref.tag_sets

        # URI overrides kwargs.
        for option, value in kwargs.iteritems():
            option, value = common.validate(option, value)
            self.__opts[option] = value
        self.__opts.update(options)

        # Both of these work in kwargs and URI...
        if ("secondary_acceptable_latency_ms" in self.__opts or
                "secondaryacceptablelatencyms" in self.__opts):
            warnings.warn("secondary_acceptable_latency_ms and "
                          "secondaryAcceptableLatencyMS are deprecated. Use "
                          "localThresholdMS instead",
                          DeprecationWarning, stacklevel=2)

        self.__max_pool_size = self.__opts.get(
            'maxpoolsize',
            common.validate_positive_integer_or_none(
                'max_pool_size', max_pool_size))

        common.validate_boolean('tz_aware', tz_aware)
        uuid_representation = self.__opts.pop('uuidrepresentation',
                                              PYTHON_LEGACY)

        self.__opts['codec_options'] = CodecOptions(
            document_class, tz_aware, uuid_representation)

        self.__use_greenlets = self.__opts.get('use_greenlets', False)
        if self.__use_greenlets:
            if not have_gevent:
                raise ConfigurationError(
                    "The gevent module is not available. "
                    "Install the gevent package from PyPI.")
            warnings.warn("use_greenlets is deprecated in this version of "
                          "PyMongo and removed in PyMongo 3. Use Gevent's "
                          "monkey.patch_all() instead.",
                          DeprecationWarning, stacklevel=2)


        self.__rs_state = RSState(self.__make_threadlocal(), initial=True)

        self.__request_counter = thread_util.Counter(self.__use_greenlets)

        self.__auto_start_request = self.__opts.get('auto_start_request', False)
        if self.__auto_start_request:
            self.start_request()

        self.__name = self.__opts.get('replicaset')
        if not self.__name:
            raise ConfigurationError("the replicaSet "
                                     "keyword parameter is required.")

        self.__net_timeout = self.__opts.get('sockettimeoutms')
        self.__conn_timeout = self.__opts.get('connecttimeoutms', 20.0)
        self.__wait_queue_timeout = self.__opts.get('waitqueuetimeoutms')
        self.__wait_queue_multiple = self.__opts.get('waitqueuemultiple')
        self.__socket_keepalive = self.__opts.get('socketkeepalive', False)
        self.__use_ssl = self.__opts.get('ssl')
        self.__ssl_keyfile = self.__opts.get('ssl_keyfile')
        self.__ssl_certfile = self.__opts.get('ssl_certfile')
        self.__ssl_cert_reqs = self.__opts.get('ssl_cert_reqs')
        self.__ssl_ca_certs = self.__opts.get('ssl_ca_certs')
        self.__ssl_match_hostname = self.__opts.get('ssl_match_hostname', True)

        ssl_kwarg_keys = [k for k in kwargs.keys()
                          if k.startswith('ssl_') and kwargs[k]]
        if self.__use_ssl is False and ssl_kwarg_keys:
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

        if self.__use_ssl and not common.HAS_SSL:
            raise ConfigurationError("The ssl module is not available. If you "
                                     "are using a python version previous to "
                                     "2.6 you must install the ssl package "
                                     "from PyPI.")

        # localThresholdMS takes precedence over secondaryAcceptableLatencyMS
        if "localthresholdms" in self.__opts:
            self.__opts.pop("secondaryacceptablelatencyms", None)
            self.__opts.pop("secondary_acceptable_latency_ms", None)
            self.__opts["secondaryacceptablelatencyms"] = (
                self.__opts["localthresholdms"])

        super(MongoReplicaSetClient, self).__init__(**self.__opts)
        if self.slave_okay:
            warnings.warn("slave_okay is deprecated in this version of "
                          "PyMongo and removed in PyMongo 3. Use "
                          "secondaryPreferred read preference instead.",
                          DeprecationWarning, stacklevel=2)

        _connect = self.__opts.get('_connect', self.__opts.get('connect', True))
        if _connect:
            try:
                self.refresh(initial=True)
            except AutoReconnect, e:
                # ConnectionFailure makes more sense here than AutoReconnect
                raise ConnectionFailure(str(e))

        if username:
            mechanism = options.get('authmechanism', 'DEFAULT')
            source = (
                options.get('authsource')
                or self.__default_database_name
                or 'admin')

            credentials = auth._build_credentials_tuple(mechanism,
                                                        source,
                                                        username,
                                                        password,
                                                        options)
            try:
                self._cache_credentials(source, credentials, _connect)
            except OperationFailure, exc:
                raise ConfigurationError(str(exc))

        # Start the monitor after we know the configuration is correct.
        if not self.__monitor_class:
            if self.__use_greenlets:
                self.__monitor_class = MonitorGreenlet
            else:
                # Common case: monitor RS with a background thread.
                self.__monitor_class = MonitorThread

        if self.__use_greenlets:
            # Greenlets don't need to lock around access to the monitor.
            # A Greenlet can safely do:
            # "if not self.__monitor: self.__monitor = monitor_class()"
            # because it won't be interrupted between the check and the
            # assignment.
            self.__monitor_lock = DummyLock()
        else:
            self.__monitor_lock = threading.Lock()

        if _connect:
            self.__ensure_monitor()

    def _cached(self, dbname, coll, index):
        """Test if `index` is cached.
        """
        cache = self.__index_cache
        now = datetime.datetime.utcnow()
        self.__index_cache_lock.acquire()
        try:
            return (dbname in cache and
                    coll in cache[dbname] and
                    index in cache[dbname][coll] and
                    now < cache[dbname][coll][index])
        finally:
            self.__index_cache_lock.release()

    def _cache_index(self, dbase, collection, index, cache_for):
        """Add an index to the index cache for ensure_index operations.
        """
        now = datetime.datetime.utcnow()
        expire = datetime.timedelta(seconds=cache_for) + now

        self.__index_cache_lock.acquire()
        try:
            if dbase not in self.__index_cache:
                self.__index_cache[dbase] = {}
                self.__index_cache[dbase][collection] = {}
                self.__index_cache[dbase][collection][index] = expire

            elif collection not in self.__index_cache[dbase]:
                self.__index_cache[dbase][collection] = {}
                self.__index_cache[dbase][collection][index] = expire

            else:
                self.__index_cache[dbase][collection][index] = expire
        finally:
            self.__index_cache_lock.release()

    def _purge_index(self, database_name,
                     collection_name=None, index_name=None):
        """Purge an index from the index cache.

        If `index_name` is None purge an entire collection.

        If `collection_name` is None purge an entire database.
        """
        self.__index_cache_lock.acquire()
        try:
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
        finally:
            self.__index_cache_lock.release()

    def _cache_credentials(self, source, credentials, connect=True):
        """Add credentials to the database authentication cache
        for automatic login when a socket is created. If `connect` is True,
        verify the credentials on the server first.

        Raises OperationFailure if other credentials are already stored for
        this source.
        """
        if source in self.__auth_credentials:
            # Nothing to do if we already have these credentials.
            if credentials == self.__auth_credentials[source]:
                return
            raise OperationFailure('Another user is already authenticated '
                                   'to this database. You must logout first.')

        if connect:
            # Try to authenticate even during failover.
            member = select_member(
                self.__rs_state.members, ReadPreference.PRIMARY_PREFERRED)

            if not member:
                raise AutoReconnect(
                    "No replica set members available for authentication")

            sock_info = self.__socket(member)
            try:
                # Since __check_auth was called in __socket
                # there is no need to call it here.
                auth.authenticate(credentials, sock_info, self.__simple_command)
                sock_info.authset.add(credentials)
            finally:
                member.maybe_return_socket(sock_info)

        self.__auth_credentials[source] = credentials

    def _purge_credentials(self, source):
        """Purge credentials from the database authentication cache.
        """
        if source in self.__auth_credentials:
            del self.__auth_credentials[source]

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

    @property
    def seeds(self):
        """The seed list used to connect to this replica set.

        A sequence of (host, port) pairs.
        """
        return self.__seeds

    @property
    def hosts(self):
        """All active and passive (priority 0) replica set
        members known to this client. This does not include
        hidden or slaveDelay members, or arbiters.

        A sequence of (host, port) pairs.
        """
        return self.__rs_state.hosts

    @property
    def address(self):
        """The (host, port) of the current primary of the replica set.

        Returns None if there is no primary.

        .. versionadded:: 2.9
        """
        return self.__rs_state.writer

    @property
    def primary(self):
        """The (host, port) of the current primary of the replica set.

        Returns None if there is no primary.
        """
        return self.__rs_state.writer

    @property
    def secondaries(self):
        """The secondary members known to this client.

        A sequence of (host, port) pairs.
        """
        return self.__rs_state.secondaries

    @property
    def arbiters(self):
        """The arbiters known to this client.

        A sequence of (host, port) pairs.
        """
        return self.__rs_state.arbiters

    @property
    def is_mongos(self):
        """If this instance is connected to mongos (always False).

        .. versionadded:: 2.3
        """
        return False

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
        """
        return self.__max_pool_size

    @property
    def use_greenlets(self):
        """**DEPRECATED** Whether calling :meth:`start_request` assigns
        greenlet-local, rather than thread-local, sockets.

        .. warning:: :attr:`use_greenlets` is deprecated in this version of
          PyMongo and removed in PyMongo 3. Use Gevent's monkey.patch_all()
          instead.

        .. versionchanged:: 2.9
          Deprecated use_greenlets.
        .. versionadded:: 2.4.2
        """
        warnings.warn("use_greenlets is deprecated in this version of PyMongo "
                      "and removed in PyMongo 3. Use Gevent's "
                      "monkey.patch_all() instead",
                      DeprecationWarning, stacklevel=2)
        return self.__use_greenlets

    def get_document_class(self):
        """**DEPRECATED** Default class to use for documents returned from this
        client.

        .. warning:: :attr:`document_class` is deprecated in this version of
          PyMongo and removed in PyMongo 3. Use
          :class:`~bson.codec_options.CodecOptions`
          with :meth:`~pymongo.mongo_client.MongoClient.get_database`,
          :meth:`~pymongo.database.Database.get_collection`,
          or :meth:`~pymongo.collection.Collection.with_options` instead.
          See the :doc:`/migrate-to-pymongo3` for examples.

        .. versionchanged:: 2.9
          Deprecated document_class.
        """
        warnings.warn("document_class is deprecated in this version of "
                      "PyMongo and removed in PyMongo 3. See the "
                      "document_class docstring for more information.",
                      DeprecationWarning, stacklevel=2)
        return self._codec_options.document_class

    def set_document_class(self, klass):
        """document_class setter"""
        warnings.warn("document_class is deprecated in this version of "
                      "PyMongo and removed in PyMongo 3. See the "
                      "document_class docstring for more information.",
                      DeprecationWarning, stacklevel=2)
        tz_aware = self._codec_options.tz_aware
        uuid_rep = self._codec_options.uuid_representation
        self._codec_options = CodecOptions(klass, tz_aware, uuid_rep)

    document_class = property(get_document_class, set_document_class)

    @property
    def tz_aware(self):
        """**DEPRECATED** Does this client return timezone-aware datetimes?

        .. warning:: :attr:`tz_aware` is deprecated in this version of PyMongo
          and removed in PyMongo 3. See :attr:`codec_options` instead.

        .. versionchanged:: 2.9
          Deprecated tz_aware.
        """
        warnings.warn("tz_aware is deprecated in this version of PyMongo and "
                      "removed in PyMongo 3. Use codec_options instead.",
                      DeprecationWarning, stacklevel=2)
        return self._codec_options.tz_aware

    @property
    def max_bson_size(self):
        """Returns the maximum size BSON object the connected primary
        accepts in bytes. Defaults to 16MB if not connected to a
        primary.
        """
        rs_state = self.__rs_state
        if rs_state.primary_member:
            return rs_state.primary_member.max_bson_size
        return common.MAX_BSON_SIZE

    @property
    def max_message_size(self):
        """Returns the maximum message size the connected primary
        accepts in bytes. Defaults to 32MB if not connected to a
        primary.
        """
        rs_state = self.__rs_state
        if rs_state.primary_member:
            return rs_state.primary_member.max_message_size
        return common.MAX_MESSAGE_SIZE

    @property
    def min_wire_version(self):
        """The minWireVersion reported by the server.

        Returns ``0`` when connected to server versions prior to MongoDB 2.6.

        .. versionadded:: 2.7
        """
        rs_state = self.__rs_state
        if rs_state.primary_member:
            return rs_state.primary_member.min_wire_version
        return common.MIN_WIRE_VERSION

    @property
    def max_wire_version(self):
        """The maxWireVersion reported by the server.

        Returns ``0`` when connected to server versions prior to MongoDB 2.6.

        .. versionadded:: 2.7
        """
        rs_state = self.__rs_state
        if rs_state.primary_member:
            return rs_state.primary_member.max_wire_version
        return common.MAX_WIRE_VERSION

    @property
    def max_write_batch_size(self):
        """The maxWriteBatchSize reported by the server.

        Returns a default value when connected to server versions prior to
        MongoDB 2.6.

        .. versionadded:: 2.7
        """
        rs_state = self.__rs_state
        if rs_state.primary_member:
            return rs_state.primary_member.max_write_batch_size
        return common.MAX_WRITE_BATCH_SIZE

    @property
    def auto_start_request(self):
        """**DEPRECATED** Is auto_start_request enabled?

        .. versionchanged:: 2.8
          Deprecated auto_start_request.
        """
        warnings.warn("auto_start_request is deprecated in this version of "
                      "PyMongo and removed in PyMongo 3.",
                      DeprecationWarning, stacklevel=2)
        return self.__auto_start_request

    @property
    def local_threshold_ms(self):
        """Any replica set member whose ping time is within
        :attr:`local_threshold_ms` of the nearest member may accept reads.
        Defaults to 15 milliseconds.

        .. versionadded:: 2.9
        """
        return self._secondary_acceptable_latency_ms

    def __simple_command(self, sock_info, dbname, spec):
        """Send a command to the server.
           Returns (response, ping_time in seconds).
        """
        ns = dbname + '.$cmd'
        rqst_id, msg, _ = message.query(0, ns, 0, -1, spec)
        start = time.time()
        try:
            sock_info.sock.sendall(msg)
            response = self.__recv_msg(1, rqst_id, sock_info)
        except:
            sock_info.close()
            raise

        end = time.time()
        response = helpers._unpack_response(response)['data'][0]
        helpers._check_command_response(response, None)
        return response, end - start

    def __is_master(self, host):
        """Directly call ismaster.
           Returns (response, connection_pool, ping_time in seconds).
        """
        connection_pool = self.pool_class(
            host,
            self.__max_pool_size,
            self.__net_timeout,
            self.__conn_timeout,
            self.__use_ssl,
            wait_queue_timeout=self.__wait_queue_timeout,
            wait_queue_multiple=self.__wait_queue_multiple,
            socket_keepalive=self.__socket_keepalive,
            use_greenlets=self.__use_greenlets,
            ssl_keyfile=self.__ssl_keyfile,
            ssl_certfile=self.__ssl_certfile,
            ssl_cert_reqs=self.__ssl_cert_reqs,
            ssl_ca_certs=self.__ssl_ca_certs,
            ssl_match_hostname=self.__ssl_match_hostname)

        if self.in_request():
            connection_pool.start_request()

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

    def __schedule_refresh(self, sync=False):
        """Awake the monitor to update our view of the replica set's state.

        If `sync` is True, block until the refresh completes.

        If multiple application threads call __schedule_refresh while refresh
        is in progress, the work of refreshing the state is only performed
        once.
        """
        if self.__closed:
            raise InvalidOperation('MongoReplicaSetClient has been closed')

        monitor = self.__ensure_monitor()
        monitor.schedule_refresh()
        if sync:
            monitor.wait_for_refresh(timeout_seconds=self._refresh_timeout_sec)

    def __ensure_monitor(self):
        """Ensure the monitor is started, and return it."""
        self.__monitor_lock.acquire()
        try:
            # Another thread can start the monitor while we wait for the lock.
            if self.__monitor is not None and self.__monitor.isAlive():
                return self.__monitor

            monitor = self.__monitor = self.__monitor_class(self)
            register_monitor(monitor)
            monitor.start_sync()
            return monitor
        finally:
            self.__monitor_lock.release()

    def __make_threadlocal(self):
        if self.__use_greenlets:
            return gevent_local()
        else:
            return threading.local()

    def refresh(self, initial=False):
        """Iterate through the existing host list, or possibly the
        seed list, to update the list of hosts and arbiters in this
        replica set.
        """
        # Only one thread / greenlet calls refresh() at a time: the one
        # running __init__() or the monitor. We won't modify the state, only
        # replace it.
        rs_state = self.__rs_state
        try:
            self.__rs_state = self.__create_rs_state(rs_state, initial)
        except ConfigurationError, e:
            self.__rs_state = rs_state.clone_with_error(e)
            raise

    def __create_rs_state(self, rs_state, initial):
        errors = []
        if rs_state.hosts:
            # Try first those hosts we think are up, then the down ones.
            nodes = sorted(
                rs_state.hosts,
                key=lambda host: bool(rs_state.get(host)),
                reverse=True)
        else:
            nodes = self.__seeds

        hosts = set()

        # This will become the new RSState.
        members = {}
        arbiters = set()
        writer = None

        # Look for first member from which we can get a list of all members.
        for node in nodes:
            member, sock_info = rs_state.get(node), None
            try:
                if member:
                    sock_info = self.__socket(member, force=True)
                    response, ping_time = self.__simple_command(
                        sock_info, 'admin', {'ismaster': 1})
                    member.maybe_return_socket(sock_info)
                    new_member = member.clone_with(response, ping_time)
                else:
                    response, pool, ping_time = self.__is_master(node)
                    new_member = Member(
                        node, pool, response, MovingAverage([ping_time]))

                # Check that this host is part of the given replica set.
                # Fail fast if we find a bad seed during __init__.
                # Regular refreshes keep searching for valid nodes.
                if response.get('setName') != self.__name:
                    if initial:
                        host, port = node
                        raise ConfigurationError("%s:%d is not a member of "
                                                 "replica set %s"
                                                 % (host, port, self.__name))
                    else:
                        continue

                if "arbiters" in response:
                    arbiters = set([
                        _partition_node(h) for h in response["arbiters"]])
                if "hosts" in response:
                    hosts.update([_partition_node(h)
                                  for h in response["hosts"]])
                if "passives" in response:
                    hosts.update([_partition_node(h)
                                  for h in response["passives"]])

                # Start off the new 'members' dict with this member
                # but don't add seed list members.
                if node in hosts:
                    members[node] = new_member
                    if response['ismaster']:
                        writer = node

            except (ConnectionFailure, socket.error, OperationFailure), why:
                # Member unreachable, or transient auth failure while member
                # is resyncing credentials.
                if member:
                    member.discard_socket(sock_info)
                errors.append("%s:%d: %s" % (node[0], node[1], str(why)))
            if hosts:
                break
        else:
            # We've changed nothing. On the next refresh, we'll try the same
            # list of hosts: rs_state.hosts or self.__seeds.
            if errors:
                raise AutoReconnect(', '.join(errors))
            raise ConfigurationError('No suitable hosts found')

        # Ensure we have a pool for each member, and find the primary.
        for host in hosts:
            if host in members:
                # This member was the first we connected to, in the loop above.
                continue

            member, sock_info = rs_state.get(host), None
            try:
                if member:
                    sock_info = self.__socket(member, force=True)
                    res, ping_time = self.__simple_command(
                        sock_info, 'admin', {'ismaster': 1})

                    if res.get('setName') != self.__name:
                        # Not a member of this set.
                        continue

                    member.maybe_return_socket(sock_info)
                    new_member = member.clone_with(res, ping_time)
                else:
                    res, connection_pool, ping_time = self.__is_master(host)
                    if res.get('setName') != self.__name:
                        # Not a member of this set.
                        continue

                    new_member = Member(
                        host, connection_pool, res, MovingAverage([ping_time]))

                members[host] = new_member

            except (ConnectionFailure, socket.error, OperationFailure):
                # Member unreachable, or transient auth failure while member
                # is resyncing credentials.
                if member:
                    member.discard_socket(sock_info)
                continue

            if res['ismaster']:
                writer = host

        if not members:
            # In the first loop, we connected to a member in the seed list
            # and got a host list, but couldn't reach any members in that
            # list.
            raise AutoReconnect(
                "Couldn't reach any hosts in %s. Replica set is"
                " configured with internal hostnames or IPs?"
                % list(hosts))

        if writer == rs_state.writer:
            threadlocal = self.__rs_state.threadlocal
        else:
            # We unpin threads from members if the primary has changed, since
            # no monotonic consistency can be promised now anyway.
            threadlocal = self.__make_threadlocal()

        # Get list of hosts in the RS config, including unreachable ones.
        # Prefer the primary's list, otherwise any member's list.
        if writer:
            response = members[writer].ismaster_response
        elif members:
            response = members.values()[0].ismaster_response
        else:
            response = {}

        final_host_list = (
            response.get('hosts', [])
            + response.get('passives', []))

        # Replace old state with new.
        return RSState(
            threadlocal,
            [_partition_node(h) for h in final_host_list],
            members,
            arbiters,
            writer)

    def __get_rs_state(self):
        rs_state = self.__rs_state
        if rs_state.exc:
            raise rs_state.exc

        return rs_state

    def __find_primary(self):
        """Returns a connection to the primary of this replica set,
        if one exists, or raises AutoReconnect.
        """
        rs_state = self.__get_rs_state()
        primary = rs_state.primary_member
        if primary:
            return primary

        # We had a failover.
        self.__schedule_refresh(sync=True)

        # Try again. This time copy the RSState reference so we're guaranteed
        # primary_member and error_message are from the same state.
        rs_state = self.__get_rs_state()
        if rs_state.primary_member:
            return rs_state.primary_member

        # Couldn't find the primary.
        raise AutoReconnect(rs_state.error_message)

    def __socket(self, member, force=False):
        """Get a SocketInfo from the pool.
        """
        if self.__auto_start_request and not self.in_request():
            self.start_request()

        sock_info = member.get_socket(force=force)

        try:
            self.__check_auth(sock_info)
        except:
            # No matter whether an auth failure or network error, increment
            # the pool's semaphore by returning the socket.
            member.maybe_return_socket(sock_info)
            raise
        return sock_info

    def _ensure_connected(self, sync=False):
        """Ensure this client instance is connected to a primary.
        """
        # This may be the first time we're connecting to the set.
        self.__ensure_monitor()

        if sync:
            rs_state = self.__rs_state
            if rs_state.exc or not rs_state.primary_member:
                self.__schedule_refresh(sync)

    def disconnect(self):
        """Disconnect from the replica set primary, unpin all members, and
        refresh our view of the replica set.
        """
        self._disconnect()

    def _disconnect(self):
        """Internal disconnect helper."""
        rs_state = self.__rs_state
        if rs_state.primary_member:
            rs_state.primary_member.reset()

        threadlocal = self.__make_threadlocal()
        self.__rs_state = rs_state.clone_without_writer(threadlocal)
        self.__schedule_refresh()

    def close(self):
        """Close this client instance.

        This method first terminates the replica set monitor, then disconnects
        from all members of the replica set. No further operations are
        permitted on this client.

        .. warning:: This method stops the replica set monitor task. The
           replica set monitor is required to properly handle replica set
           configuration changes, including a failure of the primary.
           Once :meth:`~close` is called this client instance must not be
           reused.

        .. versionchanged:: 2.2.1
           The :meth:`close` method now terminates the replica set monitor.
        """
        self.__closed = True
        self.__rs_state = RSState(self.__make_threadlocal())

        monitor, self.__monitor = self.__monitor, None
        if monitor:
            monitor.shutdown()
            # Use a reasonable timeout.
            monitor.join(1.0)

    def alive(self):
        """**DEPRECATED** Return ``False`` if there has been an error
        communicating with the primary, else ``True``.

        This method attempts to check the status of the primary with minimal
        I/O. The current thread / greenlet retrieves a socket from the
        primary's connection pool and checks whether calling select_ on it
        raises an error. If there are currently no idle sockets,
        :meth:`alive` attempts to connect a new socket.

        A more certain way to determine primary availability is to ping it::

            client.admin.command('ping')

        .. _select: http://docs.python.org/2/library/select.html#select.select
        """
        warnings.warn("alive is deprecated in this version of PyMongo and "
                      "removed in PyMongo 3.",
                      DeprecationWarning, stacklevel=2)
        # In the common case, a socket is available and was used recently, so
        # calling select() on it is a reasonable attempt to see if the OS has
        # reported an error.
        primary, sock_info = None, None
        try:
            try:
                rs_state = self.__get_rs_state()
                primary = rs_state.primary_member
                if not primary:
                    return False
                else:
                    sock_info = self.__socket(primary)
                    return not pool._closed(sock_info.sock)
            except (socket.error, ConnectionFailure):
                return False
        finally:
            if primary:
                primary.maybe_return_socket(sock_info)

    def __check_response_to_last_error(self, response, is_command):
        """Check a response to a lastError message for errors.

        `response` is a byte string representing a response to the message.
        If it represents an error response we raise OperationFailure.

        Return the response as a document.
        """
        response = helpers._unpack_response(response)

        assert response["number_returned"] == 1
        result = response["data"][0]

        helpers._check_command_response(result, self._disconnect)

        # write commands - skip getLastError checking
        if is_command:
            return result

        # getLastError
        error_msg = result.get("err", "")
        if error_msg is None:
            return result
        if error_msg.startswith("not master"):
            self._disconnect()
            raise AutoReconnect(error_msg)

        code = result.get("code")
        if code in (11000, 11001, 12582):
            raise DuplicateKeyError(result["err"], code, result)
        raise OperationFailure(result["err"], code, result)

    def __recv_data(self, length, sock_info):
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

    def __recv_msg(self, operation, rqst_id, sock):
        """Receive a message in response to `rqst_id` on `sock`.

        Returns the response data with the header removed.
        """
        header = self.__recv_data(16, sock)
        length = struct.unpack("<i", header[:4])[0]

        actual_op = struct.unpack("<i", header[12:])[0]
        assert actual_op == operation, (
            "wire protocol error: unknown opcode %r" % (actual_op,))

        # No rqst_id for exhaust cursor "getMore".
        if rqst_id is not None:
            resp_id = struct.unpack("<i", header[8:12])[0]
            assert rqst_id == resp_id, (
                "wire protocol error: got response id %r but expected %r"
                % (resp_id, rqst_id))

        assert length > 16, ("wire protocol error: message length is shorter"
                             " than standard message header: %r" % (length,))

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
                raise DocumentTooLarge("BSON document too large (%d bytes)"
                                       " - the connected server supports"
                                       " BSON document sizes up to %d"
                                       " bytes." %
                                       (max_doc_size, max_size))
            return (request_id, data)
        # get_more and kill_cursors messages
        # don't include BSON documents.
        return msg

    def _send_message(self, msg, with_last_error=False,
                      command=False, _connection_to_use=None):
        """Say something to Mongo.

        Raises ConnectionFailure if the message cannot be sent. Raises
        OperationFailure if `with_last_error` is ``True`` and the
        response to the getLastError call returns an error. Return the
        response from lastError, or ``None`` if `with_last_error` is
        ``False``.

        :Parameters:
          - `msg`: message to send
          - `with_last_error`: check getLastError status after sending the
            message
        """
        self._ensure_connected()

        if _connection_to_use in (None, -1):
            member = self.__find_primary()
        else:
            member = self.__get_rs_state().get(_connection_to_use)

        sock_info = None
        try:
            try:
                sock_info = self.__socket(member)
                rqst_id, data = self.__check_bson_size(
                    msg, member.max_bson_size)

                sock_info.sock.sendall(data)
                # Safe mode. We pack the message together with a lastError
                # message and send both. We then get the response (to the
                # lastError) and raise OperationFailure if it is an error
                # response.
                rv = None
                if with_last_error:
                    response = self.__recv_msg(1, rqst_id, sock_info)
                    rv = self.__check_response_to_last_error(response, command)
                return rv
            except OperationFailure:
                raise
            except(ConnectionFailure, socket.error), why:
                member.discard_socket(sock_info)
                if _connection_to_use in (None, -1):
                    self._disconnect()
                raise AutoReconnect(str(why))
            except:
                sock_info.close()
                raise
        finally:
            member.maybe_return_socket(sock_info)

    def __send_and_receive(self, member, msg, **kwargs):
        """Send a message on the given socket and return the response data.

        Can raise socket.error.
        """
        sock_info = None
        exhaust = kwargs.get('exhaust')
        rqst_id, data = self.__check_bson_size(msg, member.max_bson_size)
        try:
            sock_info = self.__socket(member)

            if not exhaust and "network_timeout" in kwargs:
                sock_info.sock.settimeout(kwargs['network_timeout'])

            sock_info.sock.sendall(data)
            response = self.__recv_msg(1, rqst_id, sock_info)

            if not exhaust:
                if "network_timeout" in kwargs:
                    sock_info.sock.settimeout(self.__net_timeout)
                member.maybe_return_socket(sock_info)

            return response, sock_info, member.pool
        except:
            if sock_info is not None:
                sock_info.close()
                member.maybe_return_socket(sock_info)
            raise

    def __try_read(self, member, msg, **kwargs):
        """Attempt a read from a member; on failure mark the member "down" and
           wake up the monitor thread to refresh as soon as possible.
        """
        try:
            return self.__send_and_receive(member, msg, **kwargs)
        except socket.timeout, e:
            # Could be one slow query, don't refresh.
            host, port = member.host
            raise AutoReconnect("%s:%d: %s" % (host, port, e))
        except (socket.error, ConnectionFailure), why:
            # Try to replace our RSState with a clone where this member is
            # marked "down", to reduce exceptions on other threads, or repeated
            # exceptions on this thread. We accept that there's a race
            # condition (another thread could be replacing our state with a
            # different version concurrently) but this approach is simple and
            # lock-free.
            self.__rs_state = self.__rs_state.clone_with_host_down(
                member.host, str(why))

            self.__schedule_refresh()
            host, port = member.host
            raise AutoReconnect("%s:%d: %s" % (host, port, why))

    def _send_message_with_response(self, msg, _connection_to_use=None,
                                    _must_use_master=False, **kwargs):
        """Send a message to Mongo and return the response.

        Sends the given message and returns (host used, response).

        :Parameters:
          - `msg`: (request_id, data) pair making up the message to send
          - `_connection_to_use`: Optional (host, port) of member for message,
            used by Cursor for getMore and killCursors messages.
          - `_must_use_master`: If True, send to primary.
        """
        self._ensure_connected()

        rs_state = self.__get_rs_state()
        tag_sets = kwargs.get('tag_sets', [{}])
        mode = kwargs.get('read_preference', ReadPreference.PRIMARY)
        if _must_use_master:
            mode = ReadPreference.PRIMARY
            tag_sets = [{}]

        if not rs_state.primary_member:
            # If we were initialized with _connect=False then connect now.
            # Otherwise, the primary was down last we checked. Start a refresh
            # if one is not already in progress. If caller requested the
            # primary, wait to see if it's up, otherwise continue with
            # known-good members.
            sync = (rs_state.initial or mode == ReadPreference.PRIMARY)
            self.__schedule_refresh(sync=sync)
            rs_state = self.__rs_state

        latency = kwargs.get(
            'secondary_acceptable_latency_ms',
            self.secondary_acceptable_latency_ms)

        try:
            if _connection_to_use is not None:
                if _connection_to_use == -1:
                    member = rs_state.primary_member
                    error_message = rs_state.error_message
                else:
                    member = rs_state.get(_connection_to_use)
                    error_message = '%s:%s not available' % _connection_to_use

                if not member:
                    raise AutoReconnect(error_message)

                return member.pool.pair, self.__try_read(
                    member, msg, **kwargs)
        except AutoReconnect:
            if _connection_to_use in (-1, rs_state.writer):
                # Primary's down. Refresh.
                self._disconnect()
            raise

        # To provide some monotonic consistency, we use the same member as
        # long as this thread is in a request and all reads use the same
        # mode, tags, and latency. The member gets unpinned if pref changes,
        # if member changes state, if we detect a failover, or if this thread
        # calls end_request().
        errors = []

        pinned_host = rs_state.pinned_host
        pinned_member = rs_state.get(pinned_host)
        if (pinned_member
                and pinned_member.matches_mode(mode)
                and pinned_member.matches_tag_sets(tag_sets)  # TODO: REMOVE?
                and rs_state.keep_pinned_host(mode, tag_sets, latency)):
            try:
                return (
                    pinned_member.host,
                    self.__try_read(pinned_member, msg, **kwargs))
            except AutoReconnect, why:
                if _must_use_master or mode == ReadPreference.PRIMARY:
                    self._disconnect()
                    raise
                else:
                    errors.append(str(why))

        # No pinned member, or pinned member down or doesn't match read pref
        rs_state.unpin_host()

        members = list(rs_state.members)
        while len(errors) < MAX_RETRY:
            member = select_member(
                members=members,
                mode=mode,
                tag_sets=tag_sets,
                latency=latency)

            if not member:
                # Ran out of members to try
                break

            try:
                # Removes member on failure, so select_member won't retry it.
                response = self.__try_read(member, msg, **kwargs)

                # Success
                if self.in_request():
                    # Keep reading from this member in this thread / greenlet
                    # unless read preference changes
                    rs_state.pin_host(member.host, mode, tag_sets, latency)
                return member.host, response
            except AutoReconnect, why:
                if mode == ReadPreference.PRIMARY:
                    raise

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

        # Format a message like:
        # 'No replica set secondary available for query with ReadPreference
        # SECONDARY. host:27018: timed out, host:27019: timed out'.
        if errors:
            msg += ". " + ', '.join(errors)

        raise AutoReconnect(msg, errors)

    def _exhaust_next(self, sock_info):
        """Used with exhaust cursors to get the next batch off the socket.

        Can raise AutoReconnect.
        """
        try:
            return self.__recv_msg(1, None, sock_info)
        except socket.error, e:
            raise AutoReconnect(str(e))

    def start_request(self):
        """**DEPRECATED**: start_request is removed in PyMongo 3.0.

        When doing w=0 writes to MongoDB 2.4 or earlier, :meth:`start_request`
        was sometimes useful to ensure the current thread always used the same
        socket until it called :meth:`end_request`. This made consistent reads
        more likely after an unacknowledged write. Requests are no longer
        useful in modern MongoDB applications, see
        `PYTHON-785 <https://jira.mongodb.org/browse/PYTHON-785>`_.

        .. warning:: :meth:`start_request`, :meth:`in_request`,
          and :meth:`end_request` are deprecated, and removed in PyMongo 3.
          See the :doc:`/migrate-to-pymongo3` for more information.

        .. versionchanged:: 2.8
           Deprecated start_request.

        .. versionadded:: 2.2
           The :class:`~pymongo.pool.Request` return value.
           :meth:`start_request` previously returned None
        """
        # We increment our request counter's thread- or greenlet-local value
        # for every call to start_request; however, we only call each pool's
        # start_request once to start a request, and call each pool's
        # end_request once to end it. We don't let pools' request counters
        # exceed 1. This keeps things sane when we create and delete pools
        # within a request.
        if 1 == self.__request_counter.inc():
            for member in self.__rs_state.members:
                member.start_request()

        return pool.Request(self)

    def in_request(self):
        """**DEPRECATED**: True if :meth:`start_request` has been called, but
        not :meth:`end_request`, or if `auto_start_request` is True and
        :meth:`end_request` has not been called in this thread or greenlet.

        .. warning:: :meth:`start_request`, :meth:`in_request`,
          and :meth:`end_request` are deprecated in this version of PyMongo
          and removed in PyMongo 3. See the :doc:`/migrate-to-pymongo3` for more
          information.

        .. versionchanged:: 2.8
          Deprecated in_request.
        """
        return bool(self.__request_counter.get())

    def end_request(self):
        """**DEPRECATED**: Undo :meth:`start_request`. If :meth:`end_request`
        is called as many times as :meth:`start_request`, the request is over
        and this thread's connection returns to the pool. Extra calls to
        :meth:`end_request` have no effect.

        Ending a request allows the :class:`~socket.socket` that has
        been reserved for this thread by :meth:`start_request` to be returned to
        the pool. Other threads will then be able to re-use that
        :class:`~socket.socket`. If your application uses many threads, or has
        long-running threads that infrequently perform MongoDB operations, then
        judicious use of this method can lead to performance gains. Care should
        be taken, however, to make sure that :meth:`end_request` is not called
        in the middle of a sequence of operations in which ordering is
        important. This could lead to unexpected results.

        .. warning:: :meth:`start_request`, :meth:`in_request`,
          and :meth:`end_request` are deprecated in this version of PyMongo and
          removed in PyMongo 3. See the :doc:`/migrate-to-pymongo3` for more
          information.

        .. versionchanged:: 2.8
          Deprecated end_request.
        """
        rs_state = self.__rs_state
        if 0 == self.__request_counter.dec():
            for member in rs_state.members:
                # No effect if not in a request
                member.end_request()

            rs_state.unpin_host()

    def __eq__(self, other):
        # XXX: Implement this?
        return NotImplemented

    def __ne__(self, other):
        return NotImplemented

    def __repr__(self):
        return "MongoReplicaSetClient(%r)" % (["%s:%d" % n
                                               for n in self.hosts],)

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
        ``(int, long)``.

        :Parameters:
          - `cursor_id`: id of cursor to close
        """
        if not isinstance(cursor_id, (int, long)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        member = self.__get_rs_state().get(_conn_id)

        # We can't risk taking the lock to reconnect if we're being called
        # from Cursor.__del__, see PYTHON-799.
        if not member:
            warnings.warn("not connected, couldn't close cursor",
                          stacklevel=2)
            return

        _, kill_cursors_msg = message.kill_cursors([cursor_id])
        sock_info = self.__socket(member)
        try:
            try:
                sock_info.sock.sendall(kill_cursors_msg)
            except:
                sock_info.close()
                raise
        finally:
            member.maybe_return_socket(sock_info)

    def server_info(self):
        """Get information about the MongoDB primary we're connected to.
        """
        return self.admin.command("buildinfo",
                                  read_preference=ReadPreference.PRIMARY)

    def database_names(self):
        """Get a list of the names of all databases on the connected server.
        """
        return [db["name"] for db in
                self.admin.command(
                    "listDatabases",
                    read_preference=ReadPreference.PRIMARY)["databases"]]

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
        self[name].command("dropDatabase",
                           read_preference=ReadPreference.PRIMARY)

    def copy_database(self, from_name, to_name,
                      from_host=None, username=None, password=None,
                      mechanism='DEFAULT'):
        """**DEPRECATED**: Copy a database, potentially from another host.

        Raises :class:`TypeError` if `from_name` or `to_name` is not
        an instance of :class:`basestring` (:class:`str` in python 3).
        Raises :class:`~pymongo.errors.InvalidName` if `to_name` is
        not a valid database name.

        If `from_host` is ``None`` the current host is used as the
        source. Otherwise the database is copied from `from_host`.

        If the source database requires authentication, `username` and
        `password` must be specified. By default, use SCRAM-SHA-1 with
        MongoDB 3.0 and later, MONGODB-CR (MongoDB Challenge Response
        protocol) for older servers.

        .. warning:: :meth:`copy_database` is removed in PyMongo 3.0. See the
          :doc:`copy_database examples </examples/copydb>` for alternatives.

        :Parameters:
          - `from_name`: the name of the source database
          - `to_name`: the name of the target database
          - `from_host` (optional): host name to copy from
          - `username` (optional): username for source database
          - `password` (optional): password for source database
          - `mechanism` (optional): auth method, 'MONGODB-CR' or 'SCRAM-SHA-1'

        .. seealso:: The :doc:`copy_database examples </examples/copydb>`.

        .. versionchanged:: 2.8
           Deprecated copy_database, and added SCRAM-SHA-1 support.
        """
        member = self.__find_primary()
        sock_info = self.__socket(member)
        try:
            helpers._copy_database(
                fromdb=from_name,
                todb=to_name,
                fromhost=from_host,
                mechanism=mechanism,
                username=username,
                password=password,
                sock_info=sock_info,
                cmd_func=self.__simple_command)
        finally:
            member.pool.maybe_return_socket(sock_info)

    def get_default_database(self):
        """Get the database named in the MongoDB connection URI.

        >>> uri = 'mongodb://host/my_database'
        >>> client = MongoReplicaSetClient(uri)
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

        Useful for creating a :class:`~pymongo.database.Database` with
        different codec options, read preference, and/or write concern from
        this :class:`MongoClient`.

          >>> from pymongo import ReadPreference
          >>> client.read_preference == ReadPreference.PRIMARY
          True
          >>> db1 = client.test
          >>> db1.read_preference == ReadPreference.PRIMARY
          True
          >>> db2 = client.get_database(
          ...     'test', read_preference=ReadPreference.SECONDARY)
          >>> db2.read_preference == ReadPreference.SECONDARY
          True

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

        .. versionadded:: 2.9
        """
        return database.Database(
            self, name, codec_options, read_preference, write_concern)
