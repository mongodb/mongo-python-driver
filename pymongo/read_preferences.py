# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License",
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

"""Utilities for choosing which member of a replica set to read from."""

import random
from collections import deque

from pymongo.errors import ConfigurationError


class ReadPreference:
    """An enum that defines the read preferences supported by PyMongo. Used in
    three cases:

    :class:`~pymongo.connection.Connection` to a single host:

    * `PRIMARY`: Queries are allowed if the connection is to the replica set
      primary.
    * `PRIMARY_PREFERRED`: Queries are allowed if the connection is to the
      primary or a secondary.
    * `SECONDARY`: Queries are allowed if the connection is to a secondary.
    * `SECONDARY_PREFERRED`: Same as `PRIMARY_PREFERRED`.
    * `NEAREST`: Same as `PRIMARY_PREFERRED`.

    :class:`~pymongo.connection.Connection` to a mongos, with a sharded cluster
    of replica sets:

    * `PRIMARY`: Queries are sent to the primary of a shard.
    * `PRIMARY_PREFERRED`: Queries are sent to the primary if available,
      otherwise a secondary.
    * `SECONDARY`: Queries are distributed among shard secondaries. An error
      is raised if no secondaries are available.
    * `SECONDARY_PREFERRED`: Queries are distributed among shard secondaries,
      or the primary if no secondary is available.
    * `NEAREST`: Queries are distributed among all members of a shard.

    :class:`~pymongo.replica_set_connection.ReplicaSetConnection`:

    * `PRIMARY`: Queries are sent to the primary of the replica set.
    * `PRIMARY_PREFERRED`: Queries are sent to the primary if available,
      otherwise a secondary.
    * `SECONDARY`: Queries are distributed among secondaries. An error
      is raised if no secondaries are available.
    * `SECONDARY_PREFERRED`: Queries are distributed among secondaries,
      or the primary if no secondary is available.
    * `NEAREST`: Queries are distributed among all members.
    """

    PRIMARY = 0
    PRIMARY_PREFERRED = 1
    SECONDARY = 2
    SECONDARY_ONLY = 2
    SECONDARY_PREFERRED = 3
    NEAREST = 4

# For formatting error messages
modes = {
    ReadPreference.PRIMARY:             'PRIMARY',
    ReadPreference.PRIMARY_PREFERRED:   'PRIMARY_PREFERRED',
    ReadPreference.SECONDARY:           'SECONDARY',
    ReadPreference.SECONDARY_PREFERRED: 'SECONDARY_PREFERRED',
    ReadPreference.NEAREST:             'NEAREST',
}

def select_primary(members):
    for member in members:
        if member.is_primary:
            if member.up:
                return member
            else:
                return None

    return None


def select_member_with_tags(members, tags, secondary_only, latency):
    candidates = []

    for candidate in members:
        if not candidate.up:
            continue

        if secondary_only and candidate.is_primary:
            continue

        if candidate.matches_tags(tags):
            candidates.append(candidate)

    if not candidates:
        return None

    # ping_time is in seconds
    fastest = min([candidate.get_avg_ping_time() for candidate in candidates])
    near_candidates = [
        candidate for candidate in candidates
        if candidate.get_avg_ping_time() - fastest < latency / 1000.]

    return random.choice(near_candidates)


def select_member(
    members,
    mode=ReadPreference.PRIMARY,
    tag_sets=None,
    latency=15
):
    """Return a Member or None.
    """
    if tag_sets is None:
        tag_sets = [{}]

    # For brevity
    PRIMARY             = ReadPreference.PRIMARY
    PRIMARY_PREFERRED   = ReadPreference.PRIMARY_PREFERRED
    SECONDARY           = ReadPreference.SECONDARY
    SECONDARY_PREFERRED = ReadPreference.SECONDARY_PREFERRED
    NEAREST             = ReadPreference.NEAREST
        
    if mode == PRIMARY:
        if tag_sets != [{}]:
            raise ConfigurationError("PRIMARY cannot be combined with tags")
        return select_primary(members)

    elif mode == PRIMARY_PREFERRED:
        candidate_primary = select_member(members, PRIMARY, [{}], latency)
        if candidate_primary:
            return candidate_primary
        else:
            return select_member(members, SECONDARY, tag_sets, latency)

    elif mode == SECONDARY:
        for tags in tag_sets:
            candidate = select_member_with_tags(members, tags, True, latency)
            if candidate:
                return candidate

        return None

    elif mode == SECONDARY_PREFERRED:
        candidate_secondary = select_member(
            members, SECONDARY, tag_sets, latency)
        if candidate_secondary:
            return candidate_secondary
        else:
            return select_member(members, PRIMARY, [{}], latency)

    elif mode == NEAREST:
        for tags in tag_sets:
            candidate = select_member_with_tags(members, tags, False, latency)
            if candidate:
                return candidate

        # Ran out of tags.
        return None

    else:
        raise ConfigurationError("Invalid mode %s" % repr(mode))


"""Commands that may be sent to replica-set secondaries, depending on
   ReadPreference and tags. All other commands are always run on the primary.
"""
secondary_ok_commands = frozenset([
    "group", "aggregate", "collstats", "dbstats", "count", "distinct",
    "geonear", "geosearch", "geowalk", "mapreduce", "getnonce", "authenticate",
])


class MovingAverage(object):
    """Tracks a moving average. Not thread-safe.
    """
    def __init__(self, window_sz):
        self.window_sz = window_sz
        self.samples = deque()
        self.total = 0

    def update(self, sample):
        self.samples.append(sample)
        self.total += sample
        if len(self.samples) > self.window_sz:
            self.total -= self.samples.popleft()

    def get(self):
        if self.samples:
            return self.total / float(len(self.samples))
        else:
            return None

def mongos_mode(mode):
    return {
        ReadPreference.PRIMARY:             'primary',
        ReadPreference.PRIMARY_PREFERRED:   'primaryPreferred',
        ReadPreference.SECONDARY:           'secondary',
        ReadPreference.SECONDARY_PREFERRED: 'secondaryPreferred',
        ReadPreference.NEAREST:             'nearest',
    }[mode]
