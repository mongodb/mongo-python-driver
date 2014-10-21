# Copyright 2013-2014 MongoDB, Inc.
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

"""Represent a mongod / mongos instance"""

from pymongo import common
from pymongo.errors import ConfigurationError
from pymongo.read_preferences import ReadPreference

# Member states
PRIMARY = 1
SECONDARY = 2
ARBITER = 3
OTHER = 4


# TODO: rename 'Server' or 'ServerDescription'.
class Member(object):
    """Immutable representation of one server.

    :Parameters:
      - `host`: A (host, port) pair
      - `connection_pool`: A Pool instance
      - `ismaster_response`: A dict, MongoDB's ismaster response
      - `ping_time`: A MovingAverage instance
    """
    # For unittesting only. Use under no circumstances!
    _host_to_ping_time = {}

    def __init__(self, host, connection_pool, ismaster_response, ping_time):
        self.host = host
        self.pool = connection_pool
        self.ismaster_response = ismaster_response
        self.ping_time = ping_time
        self.is_mongos = (ismaster_response.get('msg') == 'isdbgrid')

        if ismaster_response['ismaster']:
            self.state = PRIMARY
        elif ismaster_response.get('secondary'):
            self.state = SECONDARY
        elif ismaster_response.get('arbiterOnly'):
            self.state = ARBITER
        else:
            self.state = OTHER

        self.set_name = ismaster_response.get('setName')
        self.tags = ismaster_response.get('tags', {})
        self.max_bson_size = ismaster_response.get(
            'maxBsonObjectSize', common.MAX_BSON_SIZE)
        self.max_message_size = ismaster_response.get(
            'maxMessageSizeBytes', 2 * self.max_bson_size)
        self.min_wire_version = ismaster_response.get(
            'minWireVersion', common.MIN_WIRE_VERSION)
        self.max_wire_version = ismaster_response.get(
            'maxWireVersion', common.MAX_WIRE_VERSION)
        self.max_write_batch_size = ismaster_response.get(
            'maxWriteBatchSize', common.MAX_WRITE_BATCH_SIZE)

        # self.min/max_wire_version is the server's wire protocol.
        # MIN/MAX_SUPPORTED_WIRE_VERSION is what PyMongo supports.
        if (
            # Server too new.
            common.MAX_SUPPORTED_WIRE_VERSION < self.min_wire_version
            # Server too old.
            or common.MIN_SUPPORTED_WIRE_VERSION > self.max_wire_version
        ):
            raise ConfigurationError(
                "Server at %s:%d uses wire protocol versions %d through %d, "
                "but PyMongo only supports %d through %d"
                % (self.host[0], self.host[1],
                   self.min_wire_version, self.max_wire_version,
                   common.MIN_SUPPORTED_WIRE_VERSION,
                   common.MAX_SUPPORTED_WIRE_VERSION))

    def clone_with(self, ismaster_response, ping_time_sample):
        """Get a clone updated with ismaster response and a single ping time.
        """
        ping_time = self.ping_time.clone_with(ping_time_sample)
        return Member(self.host, self.pool, ismaster_response, ping_time)

    @property
    def is_primary(self):
        return self.state == PRIMARY

    @property
    def is_secondary(self):
        return self.state == SECONDARY

    @property
    def is_arbiter(self):
        return self.state == ARBITER

    def get_avg_ping_time(self):
        """Get a moving average of this member's ping times.
        """
        if self.host in Member._host_to_ping_time:
            # Simulate ping times for unittesting
            return Member._host_to_ping_time[self.host]

        return self.ping_time.get()

    def matches_mode(self, mode):
        assert not self.is_mongos, \
            "Tried to match read preference mode on a mongos Member"

        if mode == ReadPreference.PRIMARY and not self.is_primary:
            return False

        if mode == ReadPreference.SECONDARY and not self.is_secondary:
            return False

        # If we're not primary or secondary, then we're in a state like
        # RECOVERING and we don't match any mode
        return self.is_primary or self.is_secondary

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

    def get_socket(self, force=False):
        sock_info = self.pool.get_socket(force)
        sock_info.set_wire_version_range(self.min_wire_version,
                                         self.max_wire_version)

        return sock_info

    def maybe_return_socket(self, sock_info):
        self.pool.maybe_return_socket(sock_info)

    def discard_socket(self, sock_info):
        self.pool.discard_socket(sock_info)

    def start_request(self):
        self.pool.start_request()

    def in_request(self):
        return self.pool.in_request()

    def end_request(self):
        self.pool.end_request()

    def reset(self):
        self.pool.reset()

    def __str__(self):
        return '<Member "%s:%s" primary=%r>' % (
            self.host[0], self.host[1], self.is_primary)
