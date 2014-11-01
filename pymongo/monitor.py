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

"""Class to monitor a MongoDB server on a background thread."""

import atexit
import socket
import threading
import time
import weakref

from pymongo import common, helpers, message, thread_util
from pymongo.server_type import SERVER_TYPE
from pymongo.ismaster import IsMaster
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription


class Monitor(object):
    def __init__(
            self,
            server_description,
            topology,
            pool,
            topology_settings):
        """Class to monitor a MongoDB server on a background thread.

        Pass an initial ServerDescription, a Topology, a Pool, and
        TopologySettings.

        The Topology is weakly referenced. The Pool must be exclusive to this
        Monitor.
        """
        super(Monitor, self).__init__()
        self._server_description = server_description

        # A weakref callback, takes ref to the dead topology as its parameter.
        def close(dummy):
            self.close()

        self._topology = weakref.proxy(topology, close)
        self._pool = pool
        self._settings = topology_settings
        self._stopped = False
        self._event = thread_util.Event(self._settings.condition_class)
        self._thread = None

    def open(self):
        """Start monitoring, or restart after a fork.

        Multiple calls have no effect.
        """
        self._stopped = False
        started = False
        try:
            started = self._thread and self._thread.is_alive()
        except ReferenceError:
            # Thread terminated.
            pass

        if not started:
            thread = threading.Thread(target=self.run)
            thread.daemon = True
            self._thread = weakref.proxy(thread)
            register_monitor(self)
            thread.start()

    def close(self):
        """Disconnect and stop monitoring.

        open() restarts the monitor after closing.
        """
        self._stopped = True
        self._pool.reset()

        # Wake the thread so it notices that _stopped is True.
        self.request_check()

    def join(self, timeout=None):
        if self._thread is not None:
            try:
                self._thread.join(timeout)
            except ReferenceError:
                # Thread already terminated.
                pass

    def request_check(self):
        """If the monitor is sleeping, wake and check the server soon."""
        self._event.set()

    def run(self):
        while not self._stopped:
            try:
                self._server_description = self._check_with_retry()
                self._topology.on_change(self._server_description)
            except ReferenceError:
                # Topology was garbage-collected.
                self.close()
            else:
                start = time.time()  # TODO: monotonic.
                self._event.wait(common.HEARTBEAT_FREQUENCY)
                self._event.clear()
                wait_time = time.time() - start
                if wait_time < common.MIN_HEARTBEAT_INTERVAL:
                    # request_check() was called before min_wait passed.
                    time.sleep(common.MIN_HEARTBEAT_INTERVAL - wait_time)

    def _check_with_retry(self):
        """Call ismaster once or twice. Reset server's pool on error.

        Returns a ServerDescription.
        """
        # According to the spec, if an ismaster call fails we reset the
        # server's pool. If a server was once connected, change its type
        # to Unknown only after retrying once.
        retry = self._server_description.server_type != SERVER_TYPE.Unknown
        new_server_description = self._check_once()
        if new_server_description:
            return new_server_description
        else:
            self._topology.reset_pool(self._server_description.address)
            if retry:
                server_description = self._check_once()
                if server_description:
                    return server_description

        # Server type defaults to Unknown.
        return ServerDescription(self._server_description.address)

    def _check_once(self):
        """A single attempt to call ismaster.

        Returns a ServerDescription, or None on error.
        """
        try:
            with self._pool.get_socket({}, 0, 0) as sock_info:
                response, round_trip_time = self._check_with_socket(sock_info)
                old_rtts = self._server_description.round_trip_times
                if old_rtts:
                    new_rtts = old_rtts.clone_with(round_trip_time)
                else:
                    new_rtts = MovingAverage([round_trip_time])

                sd = ServerDescription(
                    self._server_description.address, response, new_rtts)

                return sd
        except socket.error:
            return None
        except Exception:
            # TODO: This is unexpected. Log.
            return None

    def _check_with_socket(self, sock_info):
        """Return (IsMaster, round_trip_time).

        Can raise socket.error or PyMongoError.
        """
        # TODO: monotonic time.
        start = time.time()
        request_id, msg, _ = message.query(
            0, 'admin.$cmd', 0, -1, {'ismaster': 1})

        sock_info.send_message(msg)
        raw_response = sock_info.receive_message(1, request_id)
        result = helpers._unpack_response(raw_response)
        return IsMaster(result['data'][0]), time.time() - start


# MONITORS has a weakref to each running Monitor. A Monitor is kept alive by
# a strong reference from its Server and its Thread. Once both are destroyed
# the Monitor is garbage-collected and removed from MONITORS. If, however,
# any threads are still running when the interpreter begins to shut down,
# we attempt to halt and join them to avoid spurious errors.
MONITORS = set()


def register_monitor(monitor):
    ref = weakref.ref(monitor, _on_monitor_deleted)
    MONITORS.add(ref)


def _on_monitor_deleted(ref):
    MONITORS.remove(ref)


def shutdown_monitors():
    # Keep a local copy of MONITORS as
    # shutting down threads has a side effect
    # of removing them from the MONITORS set()
    monitors = list(MONITORS)
    for ref in monitors:
        monitor = ref()
        if monitor:
            monitor.close()
            monitor.join(10)

atexit.register(shutdown_monitors)
