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

"""Represent the topology of servers."""

from collections import namedtuple

from pymongo import common
from pymongo.server_type import SERVER_TYPE
from pymongo.errors import ConfigurationError
from pymongo.server_description import ServerDescription


TOPOLOGY_TYPE = namedtuple('TopologyType', ['Single', 'ReplicaSetNoPrimary',
                                            'ReplicaSetWithPrimary', 'Sharded',
                                            'Unknown'])(*range(5))


class TopologyDescription(object):
    def __init__(
            self,
            topology_type,
            server_descriptions,
            replica_set_name,
            max_election_id):
        """Represent a topology of servers.

        :Parameters:
          - `topology_type`: initial type
          - `server_descriptions`: dict of (address, ServerDescription) for
            all seeds
          - `replica_set_name`: replica set name or None
          - `max_election_id`: greatest electionId seen from a primary, or None
        """
        self._topology_type = topology_type
        self._replica_set_name = replica_set_name
        self._server_descriptions = server_descriptions
        self._max_election_id = max_election_id

        # Is PyMongo compatible with all servers' wire protocols?
        self._incompatible_err = None

        for s in self._server_descriptions.values():
            # s.min/max_wire_version is the server's wire protocol.
            # MIN/MAX_SUPPORTED_WIRE_VERSION is what PyMongo supports.
            server_too_new = (
                # Server too new.
                s.min_wire_version is not None
                and s.min_wire_version > common.MAX_SUPPORTED_WIRE_VERSION)

            server_too_old = (
                # Server too old.
                s.max_wire_version is not None
                and s.max_wire_version < common.MIN_SUPPORTED_WIRE_VERSION)

            if server_too_new or server_too_old:
                self._incompatible_err = (
                    "Server at %s:%d "
                    "uses wire protocol versions %d through %d, "
                    "but PyMongo only supports %d through %d"
                    % (s.address[0], s.address[1],
                       s.min_wire_version, s.max_wire_version,
                       common.MIN_SUPPORTED_WIRE_VERSION,
                       common.MAX_SUPPORTED_WIRE_VERSION))

                break

    def check_compatible(self):
        """Raise ConfigurationError if any server is incompatible.

        A server is incompatible if its wire protocol version range does not
        overlap with PyMongo's.
        """
        if self._incompatible_err:
            raise ConfigurationError(self._incompatible_err)

    def has_server(self, address):
        return address in self._server_descriptions

    def reset_server(self, address):
        """A copy of this description, with one server marked Unknown."""
        return updated_topology_description(self, ServerDescription(address))

    def reset(self):
        """A copy of this description, with all servers marked Unknown."""
        if self._topology_type == TOPOLOGY_TYPE.ReplicaSetWithPrimary:
            topology_type = TOPOLOGY_TYPE.ReplicaSetNoPrimary
        else:
            topology_type = self._topology_type

        # The default ServerDescription's type is Unknown.
        sds = dict((address, ServerDescription(address))
                   for address in self._server_descriptions)

        return TopologyDescription(
            topology_type,
            sds,
            self._replica_set_name,
            self._max_election_id)

    def server_descriptions(self):
        """Dict of (address, ServerDescription)."""
        return self._server_descriptions.copy()

    @property
    def topology_type(self):
        return self._topology_type

    @property
    def replica_set_name(self):
        """The replica set name."""
        return self._replica_set_name

    @property
    def max_election_id(self):
        """Greatest electionId seen from a primary, or None."""
        return self._max_election_id

    @property
    def known_servers(self):
        """List of Servers of types besides Unknown."""
        return [s for s in self._server_descriptions.values()
                if s.is_server_type_known]


# If topology type is Unknown and we receive an ismaster response, what should
# the new topology type be?
_SERVER_TYPE_TO_TOPOLOGY_TYPE = {
    SERVER_TYPE.Mongos: TOPOLOGY_TYPE.Sharded,
    SERVER_TYPE.RSPrimary: TOPOLOGY_TYPE.ReplicaSetWithPrimary,
    SERVER_TYPE.RSSecondary: TOPOLOGY_TYPE.ReplicaSetNoPrimary,
    SERVER_TYPE.RSArbiter: TOPOLOGY_TYPE.ReplicaSetNoPrimary,
    SERVER_TYPE.RSOther: TOPOLOGY_TYPE.ReplicaSetNoPrimary,
}


def updated_topology_description(topology_description, server_description):
    """Return an updated copy of a TopologyDescription.

    :Parameters:
      - `topology_description`: the current TopologyDescription
      - `server_description`: a new ServerDescription that resulted from
        an ismaster call

    Called after attempting (successfully or not) to call ismaster on the
    server at server_description.address. Does not modify topology_description.
    """
    address = server_description.address

    # These values will be updated, if necessary, to form the new
    # TopologyDescription.
    topology_type = topology_description.topology_type
    set_name = topology_description.replica_set_name
    max_election_id = topology_description.max_election_id
    server_type = server_description.server_type

    # Don't mutate the original dict of server descriptions; copy it.
    sds = topology_description.server_descriptions()

    # Replace this server's description with the new one.
    sds[address] = server_description

    if topology_type == TOPOLOGY_TYPE.Single:
        # Single type never changes.
        return TopologyDescription(
            TOPOLOGY_TYPE.Single,
            sds,
            set_name,
            max_election_id)

    if topology_type == TOPOLOGY_TYPE.Unknown:
        if server_type == SERVER_TYPE.Standalone:
            sds.pop(address)

        elif server_type not in (SERVER_TYPE.Unknown, SERVER_TYPE.RSGhost):
            topology_type = _SERVER_TYPE_TO_TOPOLOGY_TYPE[server_type]

    if topology_type == TOPOLOGY_TYPE.Sharded:
        if server_type not in (SERVER_TYPE.Mongos, SERVER_TYPE.Unknown):
            sds.pop(address)

    elif topology_type == TOPOLOGY_TYPE.ReplicaSetNoPrimary:
        if server_type in (SERVER_TYPE.Standalone, SERVER_TYPE.Mongos):
            sds.pop(address)

        elif server_type == SERVER_TYPE.RSPrimary:
            topology_type, set_name, max_election_id = _update_rs_from_primary(
                sds, set_name, server_description, max_election_id)

        elif server_type in (
                SERVER_TYPE.RSSecondary,
                SERVER_TYPE.RSArbiter,
                SERVER_TYPE.RSOther):
            topology_type, set_name = _update_rs_no_primary_from_member(
                sds, set_name, server_description)

    elif topology_type == TOPOLOGY_TYPE.ReplicaSetWithPrimary:
        if server_type in (SERVER_TYPE.Standalone, SERVER_TYPE.Mongos):
            sds.pop(address)
            topology_type = _check_has_primary(sds)

        elif server_type == SERVER_TYPE.RSPrimary:
            topology_type, set_name, max_election_id = _update_rs_from_primary(
                sds, set_name, server_description, max_election_id)

        elif server_type in (
                SERVER_TYPE.RSSecondary,
                SERVER_TYPE.RSArbiter,
                SERVER_TYPE.RSOther):
            topology_type = _update_rs_with_primary_from_member(
                sds, set_name, server_description)

        else:
            # Server type is Unknown or RSGhost: did we just lose the primary?
            topology_type = _check_has_primary(sds)

    # Return updated copy.
    return TopologyDescription(topology_type, sds, set_name, max_election_id)


def _update_rs_from_primary(
        sds,
        replica_set_name,
        server_description,
        max_election_id):
    """Update topology description from a primary's ismaster response.

    Pass in a dict of ServerDescriptions, current replica set name, the
    ServerDescription we are processing, and the TopologyDescription's
    max_election_id if any.

    Returns (new topology type, new replica_set_name, new max_election_id).
    """
    if replica_set_name is None:
        replica_set_name = server_description.replica_set_name

    elif replica_set_name != server_description.replica_set_name:
        # We found a primary but it doesn't have the replica_set_name
        # provided by the user.
        sds.pop(server_description.address)
        return _check_has_primary(sds), replica_set_name, max_election_id

    if server_description.election_id is not None:
        if max_election_id and max_election_id > server_description.election_id:
            # Stale primary, set to type Unknown.
            address = server_description.address
            sds[address] = ServerDescription(address)
            return _check_has_primary(sds), replica_set_name, max_election_id

        max_election_id = server_description.election_id

    # We've heard from the primary. Is it the same primary as before?
    for server in sds.values():
        if (server.server_type is SERVER_TYPE.RSPrimary
                and server.address != server_description.address):

            # Reset old primary's type to Unknown.
            sds[server.address] = ServerDescription(server.address)

            # There can be only one prior primary.
            break

    # Discover new hosts from this primary's response.
    for new_address in server_description.all_hosts:
        if new_address not in sds:
            sds[new_address] = ServerDescription(new_address)

    # Remove hosts not in the response.
    for addr in set(sds) - server_description.all_hosts:
        sds.pop(addr)

    # If the host list differs from the seed list, we may not have a primary
    # after all.
    return _check_has_primary(sds), replica_set_name, max_election_id


def _update_rs_with_primary_from_member(
        sds,
        replica_set_name,
        server_description):
    """RS with known primary. Process a response from a non-primary.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns new topology type.
    """
    assert replica_set_name is not None

    if replica_set_name != server_description.replica_set_name:
        sds.pop(server_description.address)

    # Had this member been the primary?
    return _check_has_primary(sds)


def _update_rs_no_primary_from_member(
        sds,
        replica_set_name,
        server_description):
    """RS without known primary. Update from a non-primary's response.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns (new topology type, new replica_set_name).
    """
    topology_type = TOPOLOGY_TYPE.ReplicaSetNoPrimary
    if replica_set_name is None:
        replica_set_name = server_description.replica_set_name

    elif replica_set_name != server_description.replica_set_name:
        sds.pop(server_description.address)
        return topology_type, replica_set_name

    # This isn't the primary's response, so don't remove any servers
    # it doesn't report. Only add new servers.
    for address in server_description.all_hosts:
        if address not in sds:
            sds[address] = ServerDescription(address)

    return topology_type, replica_set_name


def _check_has_primary(sds):
    """Current topology type is ReplicaSetWithPrimary. Is primary still known?

    Pass in a dict of ServerDescriptions.

    Returns new topology type.
    """
    for s in sds.values():
        if s.server_type == SERVER_TYPE.RSPrimary:
            return TOPOLOGY_TYPE.ReplicaSetWithPrimary
    else:
        return TOPOLOGY_TYPE.ReplicaSetNoPrimary
