# Copyright 2014-present MongoDB, Inc.
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

"""Class to monitor a MongoDB server on a background thread."""

import atexit
import threading
import time
import weakref

from pymongo import common, periodic_executor
from pymongo.errors import NotPrimaryError, OperationFailure, _OperationCancelled
from pymongo.hello import Hello
from pymongo.periodic_executor import _shutdown_executors
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription
from pymongo.srv_resolver import _SrvResolver


def _sanitize(error):
    """PYTHON-2433 Clear error traceback info."""
    error.__traceback__ = None
    error.__context__ = None
    error.__cause__ = None


class MonitorBase(object):
    def __init__(self, topology, name, interval, min_interval):
        """Base class to do periodic work on a background thread.

        The the background thread is signaled to stop when the Topology or
        this instance is freed.
        """
        # We strongly reference the executor and it weakly references us via
        # this closure. When the monitor is freed, stop the executor soon.
        def target():
            monitor = self_ref()
            if monitor is None:
                return False  # Stop the executor.
            monitor._run()
            return True

        executor = periodic_executor.PeriodicExecutor(
            interval=interval, min_interval=min_interval, target=target, name=name
        )

        self._executor = executor

        def _on_topology_gc(dummy=None):
            # This prevents GC from waiting 10 seconds for hello to complete
            # See test_cleanup_executors_on_client_del.
            monitor = self_ref()
            if monitor:
                monitor.gc_safe_close()

        # Avoid cycles. When self or topology is freed, stop executor soon.
        self_ref = weakref.ref(self, executor.close)
        self._topology = weakref.proxy(topology, _on_topology_gc)
        _register(self)

    def open(self):
        """Start monitoring, or restart after a fork.

        Multiple calls have no effect.
        """
        self._executor.open()

    def gc_safe_close(self):
        """GC safe close."""
        self._executor.close()

    def close(self):
        """Close and stop monitoring.

        open() restarts the monitor after closing.
        """
        self.gc_safe_close()

    def join(self, timeout=None):
        """Wait for the monitor to stop."""
        self._executor.join(timeout)

    def request_check(self):
        """If the monitor is sleeping, wake it soon."""
        self._executor.wake()


class Monitor(MonitorBase):
    def __init__(self, server_description, topology, pool, topology_settings):
        """Class to monitor a MongoDB server on a background thread.

        Pass an initial ServerDescription, a Topology, a Pool, and
        TopologySettings.

        The Topology is weakly referenced. The Pool must be exclusive to this
        Monitor.
        """
        super(Monitor, self).__init__(
            topology,
            "pymongo_server_monitor_thread",
            topology_settings.heartbeat_frequency,
            common.MIN_HEARTBEAT_INTERVAL,
        )
        self._server_description = server_description
        self._pool = pool
        self._settings = topology_settings
        self._listeners = self._settings._pool_options._event_listeners
        pub = self._listeners is not None
        self._publish = pub and self._listeners.enabled_for_server_heartbeat
        self._cancel_context = None
        self._rtt_monitor = _RttMonitor(
            topology,
            topology_settings,
            topology._create_pool_for_monitor(server_description.address),
        )
        self.heartbeater = None

    def cancel_check(self):
        """Cancel any concurrent hello check.

        Note: this is called from a weakref.proxy callback and MUST NOT take
        any locks.
        """
        context = self._cancel_context
        if context:
            # Note: we cannot close the socket because doing so may cause
            # concurrent reads/writes to hang until a timeout occurs
            # (depending on the platform).
            context.cancel()

    def _start_rtt_monitor(self):
        """Start an _RttMonitor that periodically runs ping."""
        # If this monitor is closed directly before (or during) this open()
        # call, the _RttMonitor will not be closed. Checking if this monitor
        # was closed directly after resolves the race.
        self._rtt_monitor.open()
        if self._executor._stopped:
            self._rtt_monitor.close()

    def gc_safe_close(self):
        self._executor.close()
        self._rtt_monitor.gc_safe_close()
        self.cancel_check()

    def close(self):
        self.gc_safe_close()
        self._rtt_monitor.close()
        # Increment the generation and maybe close the socket. If the executor
        # thread has the socket checked out, it will be closed when checked in.
        self._reset_connection()

    def _reset_connection(self):
        # Clear our pooled connection.
        self._pool.reset()

    def _run(self):
        try:
            prev_sd = self._server_description
            try:
                self._server_description = self._check_server()
            except _OperationCancelled as exc:
                _sanitize(exc)
                # Already closed the connection, wait for the next check.
                self._server_description = ServerDescription(
                    self._server_description.address, error=exc
                )
                if prev_sd.is_server_type_known:
                    # Immediately retry since we've already waited 500ms to
                    # discover that we've been cancelled.
                    self._executor.skip_sleep()
                return

            # Update the Topology and clear the server pool on error.
            self._topology.on_change(
                self._server_description, reset_pool=self._server_description.error
            )

            if (
                self._server_description.is_server_type_known
                and self._server_description.topology_version
            ):
                self._start_rtt_monitor()
                # Immediately check for the next streaming response.
                self._executor.skip_sleep()

            if self._server_description.error and prev_sd.is_server_type_known:
                # Immediately retry on network errors.
                self._executor.skip_sleep()
        except ReferenceError:
            # Topology was garbage-collected.
            self.close()

    def _check_server(self):
        """Call hello or read the next streaming response.

        Returns a ServerDescription.
        """
        start = time.monotonic()
        try:
            try:
                return self._check_once()
            except (OperationFailure, NotPrimaryError) as exc:
                # Update max cluster time even when hello fails.
                self._topology.receive_cluster_time(exc.details.get("$clusterTime"))
                raise
        except ReferenceError:
            raise
        except Exception as error:
            _sanitize(error)
            sd = self._server_description
            address = sd.address
            duration = time.monotonic() - start
            if self._publish:
                awaited = sd.is_server_type_known and sd.topology_version
                self._listeners.publish_server_heartbeat_failed(address, duration, error, awaited)
            self._reset_connection()
            if isinstance(error, _OperationCancelled):
                raise
            self._rtt_monitor.reset()
            # Server type defaults to Unknown.
            return ServerDescription(address, error=error)

    def _check_once(self):
        """A single attempt to call hello.

        Returns a ServerDescription, or raises an exception.
        """
        address = self._server_description.address
        if self._publish:
            self._listeners.publish_server_heartbeat_started(address)

        if self._cancel_context and self._cancel_context.cancelled:
            self._reset_connection()
        with self._pool.get_socket({}) as sock_info:
            self._cancel_context = sock_info.cancel_context
            response, round_trip_time = self._check_with_socket(sock_info)
            if not response.awaitable:
                self._rtt_monitor.add_sample(round_trip_time)

            sd = ServerDescription(address, response, self._rtt_monitor.average())
            if self._publish:
                self._listeners.publish_server_heartbeat_succeeded(
                    address, round_trip_time, response, response.awaitable
                )
            return sd

    def _check_with_socket(self, conn):
        """Return (Hello, round_trip_time).

        Can raise ConnectionFailure or OperationFailure.
        """
        cluster_time = self._topology.max_cluster_time()
        start = time.monotonic()
        if conn.more_to_come:
            # Read the next streaming hello (MongoDB 4.4+).
            response = Hello(conn._next_reply(), awaitable=True)
        elif conn.performed_handshake and self._server_description.topology_version:
            # Initiate streaming hello (MongoDB 4.4+).
            response = conn._hello(
                cluster_time,
                self._server_description.topology_version,
                self._settings.heartbeat_frequency,
                None,
            )
        else:
            # New connection handshake or polling hello (MongoDB <4.4).
            response = conn._hello(cluster_time, None, None, None)
        return response, time.monotonic() - start


class SrvMonitor(MonitorBase):
    def __init__(self, topology, topology_settings):
        """Class to poll SRV records on a background thread.

        Pass a Topology and a TopologySettings.

        The Topology is weakly referenced.
        """
        super(SrvMonitor, self).__init__(
            topology,
            "pymongo_srv_polling_thread",
            common.MIN_SRV_RESCAN_INTERVAL,
            topology_settings.heartbeat_frequency,
        )
        self._settings = topology_settings
        self._seedlist = self._settings._seeds
        self._fqdn = self._settings.fqdn

    def _run(self):
        seedlist = self._get_seedlist()
        if seedlist:
            self._seedlist = seedlist
            try:
                self._topology.on_srv_update(self._seedlist)
            except ReferenceError:
                # Topology was garbage-collected.
                self.close()

    def _get_seedlist(self):
        """Poll SRV records for a seedlist.

        Returns a list of ServerDescriptions.
        """
        try:
            resolver = _SrvResolver(
                self._fqdn,
                self._settings.pool_options.connect_timeout,
                self._settings.srv_service_name,
            )
            seedlist, ttl = resolver.get_hosts_and_min_ttl()
            if len(seedlist) == 0:
                # As per the spec: this should be treated as a failure.
                raise Exception
        except Exception:
            # As per the spec, upon encountering an error:
            # - An error must not be raised
            # - SRV records must be rescanned every heartbeatFrequencyMS
            # - Topology must be left unchanged
            self.request_check()
            return None
        else:
            self._executor.update_interval(max(ttl, common.MIN_SRV_RESCAN_INTERVAL))
            return seedlist


class _RttMonitor(MonitorBase):
    def __init__(self, topology, topology_settings, pool):
        """Maintain round trip times for a server.

        The Topology is weakly referenced.
        """
        super(_RttMonitor, self).__init__(
            topology,
            "pymongo_server_rtt_thread",
            topology_settings.heartbeat_frequency,
            common.MIN_HEARTBEAT_INTERVAL,
        )

        self._pool = pool
        self._moving_average = MovingAverage()
        self._lock = threading.Lock()

    def close(self):
        self.gc_safe_close()
        # Increment the generation and maybe close the socket. If the executor
        # thread has the socket checked out, it will be closed when checked in.
        self._pool.reset()

    def add_sample(self, sample):
        """Add a RTT sample."""
        with self._lock:
            self._moving_average.add_sample(sample)

    def average(self):
        """Get the calculated average, or None if no samples yet."""
        with self._lock:
            return self._moving_average.get()

    def reset(self):
        """Reset the average RTT."""
        with self._lock:
            return self._moving_average.reset()

    def _run(self):
        try:
            # NOTE: This thread is only run when when using the streaming
            # heartbeat protocol (MongoDB 4.4+).
            # XXX: Skip check if the server is unknown?
            rtt = self._ping()
            self.add_sample(rtt)
        except ReferenceError:
            # Topology was garbage-collected.
            self.close()
        except Exception:
            self._pool.reset()

    def _ping(self):
        """Run a "hello" command and return the RTT."""
        with self._pool.get_socket({}) as sock_info:
            if self._executor._stopped:
                raise Exception("_RttMonitor closed")
            start = time.monotonic()
            sock_info.hello()
            return time.monotonic() - start


# Close monitors to cancel any in progress streaming checks before joining
# executor threads. For an explanation of how this works see the comment
# about _EXECUTORS in periodic_executor.py.
_MONITORS = set()


def _register(monitor):
    ref = weakref.ref(monitor, _unregister)
    _MONITORS.add(ref)


def _unregister(monitor_ref):
    _MONITORS.remove(monitor_ref)


def _shutdown_monitors():
    if _MONITORS is None:
        return

    # Copy the set. Closing monitors removes them.
    monitors = list(_MONITORS)

    # Close all monitors.
    for ref in monitors:
        monitor = ref()
        if monitor:
            monitor.gc_safe_close()

    monitor = None


def _shutdown_resources():
    # _shutdown_monitors/_shutdown_executors may already be GC'd at shutdown.
    shutdown = _shutdown_monitors
    if shutdown:
        shutdown()
    shutdown = _shutdown_executors
    if shutdown:
        shutdown()


atexit.register(_shutdown_resources)
