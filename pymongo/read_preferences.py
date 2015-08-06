# Copyright 2012-2015 MongoDB, Inc.
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

from pymongo.errors import ConfigurationError


_PRIMARY = 0
_PRIMARY_PREFERRED = 1
_SECONDARY = 2
_SECONDARY_PREFERRED = 3
_NEAREST = 4


class ReadPreference:
    """An enum that defines the read preference modes supported by PyMongo.
    Used in three cases:

    :class:`~pymongo.mongo_client.MongoClient` connected to a single host:

    * `PRIMARY`: Queries are allowed if the host is standalone or the replica
      set primary.
    * All other modes allow queries to standalone servers, to the primary, or
      to secondaries.

    :class:`~pymongo.mongo_client.MongoClient` connected to a mongos, with a
    sharded cluster of replica sets:

    * `PRIMARY`: Queries are sent to the primary of a shard.
    * `PRIMARY_PREFERRED`: Queries are sent to the primary if available,
      otherwise a secondary.
    * `SECONDARY`: Queries are distributed among shard secondaries. An error
      is raised if no secondaries are available.
    * `SECONDARY_PREFERRED`: Queries are distributed among shard secondaries,
      or the primary if no secondary is available.
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

    PRIMARY = _PRIMARY
    PRIMARY_PREFERRED = _PRIMARY_PREFERRED
    SECONDARY = _SECONDARY
    SECONDARY_ONLY = _SECONDARY
    SECONDARY_PREFERRED = _SECONDARY_PREFERRED
    NEAREST = _NEAREST

# For formatting error messages
modes = {
    _PRIMARY:             'PRIMARY',
    _PRIMARY_PREFERRED:   'PRIMARY_PREFERRED',
    _SECONDARY:           'SECONDARY',
    _SECONDARY_PREFERRED: 'SECONDARY_PREFERRED',
    _NEAREST:             'NEAREST',
}

_mongos_modes = [
    'primary',
    'primaryPreferred',
    'secondary',
    'secondaryPreferred',
    'nearest',
]

def mongos_mode(mode):
    return _mongos_modes[mode]

def mongos_enum(enum):
    return _mongos_modes.index(enum)

def select_primary(members):
    for member in members:
        if member.is_primary:
            return member

    return None


def select_member_with_tags(members, tags, secondary_only, latency):
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
        if candidate.get_avg_ping_time() - fastest <= latency / 1000.]

    return random.choice(near_candidates)


def select_member(members,
                  mode=ReadPreference.PRIMARY,
                  tag_sets=None,
                  latency=15):
    """Return a Member or None.
    """
    if tag_sets is None:
        tag_sets = [{}]

    if mode == _PRIMARY:
        if tag_sets != [{}]:
            raise ConfigurationError("PRIMARY cannot be combined with tags")
        return select_primary(members)

    elif mode == _PRIMARY_PREFERRED:
        # Recurse.
        candidate_primary = select_member(members, _PRIMARY, [{}], latency)
        if candidate_primary:
            return candidate_primary
        else:
            return select_member(members, _SECONDARY, tag_sets, latency)

    elif mode == _SECONDARY:
        for tags in tag_sets:
            candidate = select_member_with_tags(members, tags, True, latency)
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
    "text", "parallelcollectionscan"
])


class MovingAverage(object):
    def __init__(self, samples):
        """Immutable structure to track a 5-sample moving average.
        """
        self.samples = samples[-5:]
        assert self.samples
        self.average = sum(self.samples) / float(len(self.samples))

    def clone_with(self, sample):
        """Get a copy of this instance plus a new sample"""
        return MovingAverage(self.samples + [sample])

    def get(self):
        return self.average


def _validate_tag_sets(tag_sets):
    """Validate tag sets for a MongoReplicaSetClient.
    """
    if tag_sets is None:
        return tag_sets

    if not isinstance(tag_sets, list):
        raise TypeError((
            "Tag sets %r invalid, must be a list") % (tag_sets,))
    if len(tag_sets) == 0:
        raise ValueError((
            "Tag sets %r invalid, must be None or contain at least one set of"
            " tags") % (tag_sets,))

    for tags in tag_sets:
        if not isinstance(tags, dict):
            raise TypeError(
                "Tag set %r invalid, must be an instance of dict, or"
                "bson.son.SON" % (tags,))

    return tag_sets


class _ServerMode(object):
    """Base class for all read preferences.
    """

    __slots__ = ("__mongos_mode", "__mode", "__tag_sets")

    def __init__(self, mode, tag_sets=None):
        if mode == _PRIMARY and tag_sets is not None:
            raise ConfigurationError("Read preference primary "
                                     "cannot be combined with tags")
        self.__mongos_mode = _mongos_modes[mode]
        self.__mode = mode
        self.__tag_sets = _validate_tag_sets(tag_sets)

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
        if self.__tag_sets:
            return list(self.__tag_sets)
        return [{}]

    def __repr__(self):
        return "%s(tag_sets=%r)" % (
            self.name, self.__tag_sets)

    def __eq__(self, other):
        if isinstance(other, _ServerMode):
            return (self.mode == other.mode and
                    self.tag_sets == other.tag_sets)
        raise NotImplementedError

    def __ne__(self, other):
        return not self == other

    def __getstate__(self):
        """Return value of object for pickling.
        Needed explicitly because __slots__() defined.
        """
        return {'mode': self.__mode, 'tag_sets': self.__tag_sets}

    def __setstate__(self, value):
        """Restore from pickling."""
        self.__mode = value['mode']
        self.__mongos_mode = _mongos_modes[self.__mode]
        self.__tag_sets = _validate_tag_sets(value['tag_sets'])


class Primary(_ServerMode):
    """Primary read preference.

    * When directly connected to one mongod queries are allowed if the server
      is standalone or a replica set primary.
    * When connected to a mongos queries are sent to the primary of a shard.
    * When connected to a replica set queries are sent to the primary of
      the replica set.

    .. versionadded:: 2.9
    """

    def __init__(self):
        super(Primary, self).__init__(_PRIMARY)

    def __repr__(self):
        return "Primary()"

    def __eq__(self, other):
        if isinstance(other, _ServerMode):
            return other.mode == _PRIMARY
        raise NotImplementedError


class PrimaryPreferred(_ServerMode):
    """PrimaryPreferred read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are sent to the primary of a shard if
      available, otherwise a shard secondary.
    * When connected to a replica set queries are sent to the primary if
      available, otherwise a secondary.

    :Parameters:
      - `tag_sets`: The :attr:`~tag_sets` to use if the primary is not
        available.

    .. versionadded:: 2.9
    """

    def __init__(self, tag_sets=None):
        super(PrimaryPreferred, self).__init__(_PRIMARY_PREFERRED, tag_sets)


class Secondary(_ServerMode):
    """Secondary read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among shard
      secondaries. An error is raised if no secondaries are available.
    * When connected to a replica set queries are distributed among
      secondaries. An error is raised if no secondaries are available.

    :Parameters:
      - `tag_sets`: The :attr:`~tag_sets` to use with this read_preference

    .. versionadded:: 2.9
    """

    def __init__(self, tag_sets=None):
        super(Secondary, self).__init__(_SECONDARY, tag_sets)


class SecondaryPreferred(_ServerMode):
    """SecondaryPreferred read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among shard
      secondaries, or the shard primary if no secondary is available.
    * When connected to a replica set queries are distributed among
      secondaries, or the primary if no secondary is available.

    :Parameters:
      - `tag_sets`: The :attr:`~tag_sets` to use with this read_preference

    .. versionadded:: 2.9
    """

    def __init__(self, tag_sets=None):
        super(SecondaryPreferred, self).__init__(_SECONDARY_PREFERRED, tag_sets)


class Nearest(_ServerMode):
    """Nearest read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among all members of
      a shard.
    * When connected to a replica set queries are distributed among all
      members.

    :Parameters:
      - `tag_sets`: The :attr:`~tag_sets` to use with this read_preference

    .. versionadded:: 2.9
    """

    def __init__(self, tag_sets=None):
        super(Nearest, self).__init__(_NEAREST, tag_sets)
