# Copyright 2014-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Represent one server the driver is connected to."""

from bson import EPOCH_NAIVE
from pymongo.ismaster import IsMaster
from pymongo.monotonic import time as _time
from pymongo.server_type import SERVER_TYPE


class ServerDescription(object):
    """Immutable representation of one server.

    :Parameters:
      - `address`: A (host, port) pair
      - `ismaster`: Optional Hello instance
      - `round_trip_time`: Optional float
      - `error`: Optional, the last error attempting to connect to the server

    .. warning:: The `ismaster` parameter will be renamed to `hello` in PyMongo
      4.0.
    """

    __slots__ = (
        "_address",
        "_server_type",
        "_all_hosts",
        "_tags",
        "_replica_set_name",
        "_primary",
        "_max_bson_size",
        "_max_message_size",
        "_max_write_batch_size",
        "_min_wire_version",
        "_max_wire_version",
        "_round_trip_time",
        "_me",
        "_is_writable",
        "_is_readable",
        "_ls_timeout_minutes",
        "_error",
        "_set_version",
        "_election_id",
        "_cluster_time",
        "_last_write_date",
        "_last_update_time",
        "_topology_version",
    )

    def __init__(self, address, ismaster=None, round_trip_time=None, error=None):
        self._address = address
        hello = ismaster or IsMaster({})

        self._server_type = hello.server_type
        self._all_hosts = hello.all_hosts
        self._tags = hello.tags
        self._replica_set_name = hello.replica_set_name
        self._primary = hello.primary
        self._max_bson_size = hello.max_bson_size
        self._max_message_size = hello.max_message_size
        self._max_write_batch_size = hello.max_write_batch_size
        self._min_wire_version = hello.min_wire_version
        self._max_wire_version = hello.max_wire_version
        self._set_version = hello.set_version
        self._election_id = hello.election_id
        self._cluster_time = hello.cluster_time
        self._is_writable = hello.is_writable
        self._is_readable = hello.is_readable
        self._ls_timeout_minutes = hello.logical_session_timeout_minutes
        self._round_trip_time = round_trip_time
        self._me = hello.me
        self._last_update_time = _time()
        self._error = error
        self._topology_version = hello.topology_version
        if error:
            if hasattr(error, "details") and isinstance(error.details, dict):
                self._topology_version = error.details.get("topologyVersion")

        if hello.last_write_date:
            # Convert from datetime to seconds.
            delta = hello.last_write_date - EPOCH_NAIVE
            self._last_write_date = delta.total_seconds()
        else:
            self._last_write_date = None

    @property
    def address(self):
        """The address (host, port) of this server."""
        return self._address

    @property
    def server_type(self):
        """The type of this server."""
        return self._server_type

    @property
    def server_type_name(self):
        """The server type as a human readable string.

        .. versionadded:: 3.4
        """
        return SERVER_TYPE._fields[self._server_type]

    @property
    def all_hosts(self):
        """List of hosts, passives, and arbiters known to this server."""
        return self._all_hosts

    @property
    def tags(self):
        return self._tags

    @property
    def replica_set_name(self):
        """Replica set name or None."""
        return self._replica_set_name

    @property
    def primary(self):
        """This server's opinion about who the primary is, or None."""
        return self._primary

    @property
    def max_bson_size(self):
        return self._max_bson_size

    @property
    def max_message_size(self):
        return self._max_message_size

    @property
    def max_write_batch_size(self):
        return self._max_write_batch_size

    @property
    def min_wire_version(self):
        return self._min_wire_version

    @property
    def max_wire_version(self):
        return self._max_wire_version

    @property
    def set_version(self):
        return self._set_version

    @property
    def election_id(self):
        return self._election_id

    @property
    def cluster_time(self):
        return self._cluster_time

    @property
    def election_tuple(self):
        return self._set_version, self._election_id

    @property
    def me(self):
        return self._me

    @property
    def logical_session_timeout_minutes(self):
        return self._ls_timeout_minutes

    @property
    def last_write_date(self):
        return self._last_write_date

    @property
    def last_update_time(self):
        return self._last_update_time

    @property
    def round_trip_time(self):
        """The current average latency or None."""
        # This override is for unittesting only!
        if self._address in self._host_to_round_trip_time:
            return self._host_to_round_trip_time[self._address]

        return self._round_trip_time

    @property
    def error(self):
        """The last error attempting to connect to the server, or None."""
        return self._error

    @property
    def is_writable(self):
        return self._is_writable

    @property
    def is_readable(self):
        return self._is_readable

    @property
    def mongos(self):
        return self._server_type == SERVER_TYPE.Mongos

    @property
    def is_server_type_known(self):
        return self.server_type != SERVER_TYPE.Unknown

    @property
    def retryable_writes_supported(self):
        """Checks if this server supports retryable writes."""
        return (
            self._ls_timeout_minutes is not None
            and self._server_type in (SERVER_TYPE.Mongos, SERVER_TYPE.RSPrimary)
        ) or self._server_type == SERVER_TYPE.LoadBalancer

    @property
    def retryable_reads_supported(self):
        """Checks if this server supports retryable writes."""
        return self._max_wire_version >= 6

    @property
    def topology_version(self):
        return self._topology_version

    def to_unknown(self, error=None):
        unknown = ServerDescription(self.address, error=error)
        unknown._topology_version = self.topology_version
        return unknown

    def __eq__(self, other):
        if isinstance(other, ServerDescription):
            return (
                (self._address == other.address)
                and (self._server_type == other.server_type)
                and (self._min_wire_version == other.min_wire_version)
                and (self._max_wire_version == other.max_wire_version)
                and (self._me == other.me)
                and (self._all_hosts == other.all_hosts)
                and (self._tags == other.tags)
                and (self._replica_set_name == other.replica_set_name)
                and (self._set_version == other.set_version)
                and (self._election_id == other.election_id)
                and (self._primary == other.primary)
                and (self._ls_timeout_minutes == other.logical_session_timeout_minutes)
                and (self._error == other.error)
            )

        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        errmsg = ""
        if self.error:
            errmsg = ", error=%r" % (self.error,)
        return "<%s %s server_type: %s, rtt: %s%s>" % (
            self.__class__.__name__,
            self.address,
            self.server_type_name,
            self.round_trip_time,
            errmsg,
        )

    # For unittesting only. Use under no circumstances!
    _host_to_round_trip_time = {}
