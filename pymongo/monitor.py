# Copyright 2014-2015 MongoDB, Inc.
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

import weakref

from bson.codec_options import DEFAULT_CODEC_OPTIONS
from pymongo import common, helpers, message, periodic_executor
from pymongo.server_type import SERVER_TYPE
from pymongo.ismaster import IsMaster
from pymongo.monotonic import time as _time
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
        self._server_description = server_description
        self._pool = pool
        self._settings = topology_settings
        self._avg_round_trip_time = MovingAverage()

        # We strongly reference the executor and it weakly references us via
        # this closure. When the monitor is freed, stop the executor soon.
        def target():
            monitor = self_ref()
            if monitor is None:
                return False  # Stop the executor.
            Monitor._run(monitor)
            return True

        executor = periodic_executor.PeriodicExecutor(
            condition_class=self._settings.condition_class,
            interval=common.HEARTBEAT_FREQUENCY,
            min_interval=common.MIN_HEARTBEAT_INTERVAL,
            target=target)

        self._executor = executor
        
        # Avoid cycles. When self or topology is freed, stop executor soon.
        self_ref = weakref.ref(self, executor.close)
        self._topology = weakref.proxy(topology, executor.close)

    def open(self):
        """Start monitoring, or restart after a fork.

        Multiple calls have no effect.
        """
        self._executor.open()

    def close(self):
        """Close and stop monitoring.

        open() restarts the monitor after closing.
        """
        self._executor.close()

        # Increment the pool_id and maybe close the socket. If the executor
        # thread has the socket checked out, it will be closed when checked in.
        self._pool.reset()

    def join(self, timeout=None):
        self._executor.join(timeout)

    def request_check(self):
        """If the monitor is sleeping, wake and check the server soon."""
        self._executor.wake()

    def _run(self):
        try:
            self._server_description = self._check_with_retry()
            self._topology.on_change(self._server_description)
        except ReferenceError:
            # Topology was garbage-collected.
            self.close()

    def _check_with_retry(self):
        """Call ismaster once or twice. Reset server's pool on error.

        Returns a ServerDescription.
        """
        # According to the spec, if an ismaster call fails we reset the
        # server's pool. If a server was once connected, change its type
        # to Unknown only after retrying once.
        address = self._server_description.address
        retry = self._server_description.server_type != SERVER_TYPE.Unknown

        try:
            return self._check_once()
        except ReferenceError:
            raise
        except Exception as error:
            self._topology.reset_pool(address)
            default = ServerDescription(address, error=error)
            if not retry:
                self._avg_round_trip_time.reset()
                # Server type defaults to Unknown.
                return default

            # Try a second and final time. If it fails return original error.
            try:
                return self._check_once()
            except ReferenceError:
                raise
            except Exception:
                self._avg_round_trip_time.reset()
                return default

    def _check_once(self):
        """A single attempt to call ismaster.

        Returns a ServerDescription, or raises an exception.
        """
        with self._pool.get_socket({}) as sock_info:
            response, round_trip_time = self._check_with_socket(sock_info)
            self._avg_round_trip_time.add_sample(round_trip_time)
            sd = ServerDescription(
                address=self._server_description.address,
                ismaster=response,
                round_trip_time=self._avg_round_trip_time.get())

            return sd

    def _check_with_socket(self, sock_info):
        """Return (IsMaster, round_trip_time).

        Can raise ConnectionFailure or OperationFailure.
        """
        start = _time()
        request_id, msg, max_doc_size = message.query(
            0, 'admin.$cmd', 0, -1, {'ismaster': 1},
            None, DEFAULT_CODEC_OPTIONS)

        # TODO: use sock_info.command()
        sock_info.send_message(msg, max_doc_size)
        raw_response = sock_info.receive_message(1, request_id)
        result = helpers._unpack_response(raw_response)
        return IsMaster(result['data'].popleft().decode()), _time() - start
