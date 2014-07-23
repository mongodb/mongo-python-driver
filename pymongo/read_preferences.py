# Copyright 2012-2014 MongoDB, Inc.
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

from collections import Mapping, namedtuple

from pymongo.errors import ConfigurationError


_PRIMARY = 0
_PRIMARY_PREFERRED = 1
_SECONDARY = 2
_SECONDARY_PREFERRED = 3
_NEAREST = 4


_MONGOS_MODES = (
    'primary',
    'primaryPreferred',
    'secondary',
    'secondaryPreferred',
    'nearest',
)


def _validate_tag_sets(tag_sets):
    """Validate tag sets for a MongoReplicaSetClient.
    """
    if tag_sets is None:
        return tag_sets

    if not isinstance(tag_sets, list):
        raise ConfigurationError((
            "Tag sets %r invalid, must be a list") % (tag_sets,))
    if len(tag_sets) == 0:
        raise ConfigurationError((
            "Tag sets %r invalid, must be None or contain at least one set of"
            " tags") % (tag_sets,))

    for tags in tag_sets:
        if not isinstance(tags, Mapping):
            raise ConfigurationError(
                "Tag set %r invalid, must be a mapping." % (tags,))

    return tag_sets


class ServerMode(object):
    """Base class for all read preferences.
    """

    __slots__ = ("__mongos_mode", "__mode", "__latency", "__tag_sets")

    def __init__(self, mode, latency_threshold_ms=15, tag_sets=None):
        if mode == _PRIMARY and tag_sets is not None:
            raise ConfigurationError("Read preference primary "
                                     "cannot be combined with tags")
        self.__mongos_mode = _MONGOS_MODES[mode]
        self.__mode = mode
        self.__tag_sets = _validate_tag_sets(tag_sets)
        try:
            self.__latency = float(latency_threshold_ms)
        except (ValueError, TypeError):
            raise ConfigurationError("latency_threshold_ms must "
                                     "be a positive integer or float")

    @property
    def name(self):
        """The name of this read preference.
        """
        return self.__class__.__name__

    @property
    def document(self):
        """Read preference as a document.
        """
        if self.__tag_sets in (None, [{}]):
            return {'mode': self.__mongos_mode}
        return {'mode': self.__mongos_mode, 'tags': self.__tag_sets}

    @property
    def mode(self):
        """The mode of this read preference instance.
        """
        return self.__mode

    @property
    def latency_threshold_ms(self):
        """int - Any replica-set member whose ping time is within
        `~latency_threshold_ms` of the nearest member may accept reads.
        When used with mongos high availability, any mongos whose ping
        time is within `~latency_threshold_ms` of the nearest mongos
        may be chosen as the new mongos during a failover. Default 15
        milliseconds.
        """
        return self.__latency

    @property
    def tag_sets(self):
        """Set ``tag_sets`` to a list of dictionaries like [{'dc': 'ny'}] to
        read only from members whose ``dc`` tag has the value ``"ny"``.
        To specify a priority-order for tag sets, provide a list of
        tag sets: ``[{'dc': 'ny'}, {'dc': 'la'}, {}]``. A final, empty tag
        set, ``{}``, means "read from any member that matches the mode,
        ignoring tags." MongoReplicaSetClient tries each set of tags in turn
        until it finds a set of tags with at least one matching member.

           .. seealso:: `Data-Center Awareness
               <http://www.mongodb.org/display/DOCS/Data+Center+Awareness>`_
        """
        return self.__tag_sets or [{}]

    def __repr__(self):
        return "%s(latency_threshold_ms=%d, tag_sets=%r)" % (
            self.name, self.__latency, self.__tag_sets)

    def __eq__(self, other):
        return (self.mode == other.mode and
                self.latency_threshold_ms == other.latency_threshold_ms and
                self.tag_sets == other.tag_sets)

    def __ne__(self, other):
        return not self == other


class Primary(ServerMode):
    """Primary read preference.

    * When directly connected to one mongod queries are allowed if the server
      is standalone or a replica set primary.
    * When connected to a mongos queries are sent to the primary of a shard.
    * When connected to a replica set queries are sent to the primary of
      the replica set.

    :Parameters:
      - `latency_threshold_ms`: Used for mongos high availability. The
        :attr:`~latency_threshold_ms` when selecting a failover mongos.
    """

    def __init__(self, latency_threshold_ms=15):
        super(Primary, self).__init__(_PRIMARY, latency_threshold_ms)

    def __repr__(self):
        return "Primary(latency_threshold_ms=%d)" % self.latency_threshold_ms


class PrimaryPreferred(ServerMode):
    """PrimaryPreferred read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are sent to the primary of a shard if
      available, otherwise a shard secondary.
    * When connected to a replica set queries are sent to the primary if
      available, otherwise a secondary.

    :Parameters:
      - `latency_threshold_ms`: The :attr:`~latency_threshold_ms` when
        selecting a secondary.
      - `tag_sets`: The :attr:`~tag_sets` to use if the primary is not
        available.
    """

    def __init__(self, latency_threshold_ms=15, tag_sets=None):
        super(PrimaryPreferred, self).__init__(
            _PRIMARY_PREFERRED, latency_threshold_ms, tag_sets)


class Secondary(ServerMode):
    """Secondary read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among shard
      secondaries. An error is raised if no secondaries are available.
    * When connected to a replica set queries are distributed among
      secondaries. An error is raised if no secondaries are available.

    :Parameters:
      - `latency_threshold_ms`: The :attr:`~latency_threshold_ms` when
        selecting a secondary.
      - `tag_sets`: The :attr:`~tag_sets` to use with this read_preference
    """

    def __init__(self, latency_threshold_ms=15, tag_sets=None):
        super(Secondary, self).__init__(
            _SECONDARY, latency_threshold_ms, tag_sets)


class SecondaryPreferred(ServerMode):
    """SecondaryPreferred read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among shard
      secondaries, or the shard primary if no secondary is available.
    * When connected to a replica set queries are distributed among
      secondaries, or the primary if no secondary is available.

    :Parameters:
      - `latency_threshold_ms`: The :attr:`~latency_threshold_ms` when
        selecting a secondary.
      - `tag_sets`: The :attr:`~tag_sets` to use with this read_preference
    """

    def __init__(self, latency_threshold_ms=15, tag_sets=None):
        super(SecondaryPreferred, self).__init__(
            _SECONDARY_PREFERRED, latency_threshold_ms, tag_sets)


class Nearest(ServerMode):
    """Nearest read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among all members of
      a shard.
    * When connected to a replica set queries are distributed among all
      members.

    :Parameters:
      - `latency_threshold_ms`: The :attr:`~latency_threshold_ms` when
        selecting a secondary.
      - `tag_sets`: The :attr:`~tag_sets` to use with this read_preference
    """

    def __init__(self, latency_threshold_ms=15, tag_sets=None):
        super(Nearest, self).__init__(
            _NEAREST, latency_threshold_ms, tag_sets)


_ALL_READ_PREFERENCES = (Primary, PrimaryPreferred,
                         Secondary, SecondaryPreferred, Nearest)

def make_read_preference(mode, latency_threshold_ms, tag_sets):
    if mode == _PRIMARY:
        if tag_sets not in (None, [{}]):
            raise ConfigurationError("Read preference primary "
                                     "cannot be combined with tags")
        return Primary(latency_threshold_ms)
    return _ALL_READ_PREFERENCES[mode](latency_threshold_ms, tag_sets)


_MODES = (
    'PRIMARY',
    'PRIMARY_PREFERRED',
    'SECONDARY',
    'SECONDARY_PREFERRED',
    'NEAREST',
)


ReadPreference = namedtuple("ReadPreference", _MODES)(
    Primary(), PrimaryPreferred(), Secondary(), SecondaryPreferred(), Nearest())
"""An enum that defines the read preference modes supported by PyMongo.
Used in three cases:

:class:`~pymongo.mongo_client.MongoClient` connected to a single mongod:

* `PRIMARY`: Queries are allowed if the server is standalone or a replica
  set primary.
* All other modes allow queries to standalone servers, to a replica set
  primary, or to replica set secondaries.

:class:`~pymongo.mongo_client.MongoClient` connected to a mongos, with a
sharded cluster of replica sets:

* `PRIMARY`: Queries are sent to the primary of a shard.
* `PRIMARY_PREFERRED`: Queries are sent to the shard primary if available,
  otherwise a shard secondary.
* `SECONDARY`: Queries are distributed among shard secondaries. An error
  is raised if no secondaries are available.
* `SECONDARY_PREFERRED`: Queries are distributed among shard secondaries,
  or the shard primary if no secondary is available.
* `NEAREST`: Queries are distributed among all members of a shard.

:class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`:

* `PRIMARY`: Queries are sent to the primary of the replica set.
* `PRIMARY_PREFERRED`: Queries are sent to the primary if available,
  otherwise a secondary.
* `SECONDARY`: Queries are distributed among secondaries. An error
  is raised if no secondaries are available.
* `SECONDARY_PREFERRED`: Queries are distributed among secondaries,
  or the primary if no secondary is available.
* `NEAREST`: Queries are distributed among all members.
"""


def read_pref_mode_from_name(name):
    """Get the read preference mode from mongos/uri name.
    """
    return _MONGOS_MODES.index(name)


def _select_primary(members):
    """Get the primary member.
    """
    for member in members:
        if member.is_primary:
            return member

    return None


def _select_member_with_tags(members, tags, secondary_only, latency):
    """Get the member matching the given tags, and acceptable latency.
    """
    candidates = []

    for candidate in members:
        if secondary_only and candidate.is_primary:
            continue

        if not (candidate.is_primary or candidate.is_secondary):
            # In RECOVERING or similar state
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


def select_member(members, mode, tag_sets=[{}], latency=15):
    """Return a Member or None.
    """
    if mode == _PRIMARY:
        return _select_primary(members)

    elif mode == _PRIMARY_PREFERRED:
        # Recurse.
        candidate_primary = select_member(members, _PRIMARY, [{}], latency)
        if candidate_primary:
            return candidate_primary
        else:
            return select_member(members, _SECONDARY, tag_sets, latency)

    elif mode == _SECONDARY:
        for tags in tag_sets:
            candidate = _select_member_with_tags(members, tags, True, latency)
            if candidate:
                return candidate

        return None

    elif mode == _SECONDARY_PREFERRED:
        # Recurse.
        candidate_secondary = select_member(
            members, _SECONDARY, tag_sets, latency)
        if candidate_secondary:
            return candidate_secondary
        else:
            return select_member(members, _PRIMARY, [{}], latency)

    elif mode == _NEAREST:
        for tags in tag_sets:
            candidate = _select_member_with_tags(members, tags, False, latency)
            if candidate:
                return candidate

        # Ran out of tags.
        return None

    else:
        raise ConfigurationError("Invalid mode %d" % (mode,))


SECONDARY_OK_COMMANDS = frozenset([
    "group", "aggregate", "collstats", "dbstats", "count", "distinct",
    "geonear", "geosearch", "geowalk", "mapreduce", "getnonce", "authenticate",
    "text", "parallelcollectionscan"
])
"""Commands that may be sent to replica-set secondaries, depending on
   ReadPreference and tags. All other commands are always run on the primary.
"""


class MovingAverage(object):
    """Immutable structure to track a 5-sample moving average.
    """
    def __init__(self, samples):
        self.samples = samples[-5:]
        assert self.samples
        self.average = sum(self.samples) / float(len(self.samples))

    def clone_with(self, sample):
        """Get a copy of this instance plus a new sample"""
        return MovingAverage(self.samples + [sample])

    def get(self):
        """Get the calculated average.
        """
        return self.average
