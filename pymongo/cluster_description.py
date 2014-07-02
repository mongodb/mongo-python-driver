# Copyright 2014 MongoDB, Inc.
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

"""Represent the cluster of servers."""

from collections import namedtuple

from pymongo import common
from pymongo.errors import ConfigurationError
from pymongo.server_description import ServerDescription, SERVER_TYPE


CLUSTER_TYPE = namedtuple('ClusterType', ['Single', 'ReplicaSetNoPrimary',
                                          'ReplicaSetWithPrimary', 'Sharded',
                                          'Unknown'])(*range(5))


class ClusterDescription(object):
    def __init__(self, cluster_type, server_descriptions, set_name):
        """Represent a cluster of servers.

        :Parameters:
          - `cluster_type`: initial type
          - `server_descriptions`: dict of (address, ServerDescription) for
            all seeds
          - `set_name`: replica set name or None
        """
        self._cluster_type = cluster_type
        self._set_name = set_name
        self._server_descriptions = server_descriptions

    def has_server(self, address):
        return address in self._server_descriptions

    def check_compatible(self):
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
                raise ConfigurationError(
                    "Server at %s:%d "
                    "uses wire protocol versions %d through %d, "
                    "but PyMongo only supports %d through %d"
                    % (s.address[0], s.address[1],
                       s.min_wire_version, s.max_wire_version,
                       common.MIN_SUPPORTED_WIRE_VERSION,
                       common.MAX_SUPPORTED_WIRE_VERSION))

    def server_descriptions(self):
        """Dict of (address, ServerDescription)."""
        return self._server_descriptions.copy()

    @property
    def cluster_type(self):
        return self._cluster_type

    @property
    def set_name(self):
        """The replica set name."""
        return self._set_name

    @property
    def known_servers(self):
        return [s for s in self._server_descriptions.values()
                if s.is_server_type_known]


# If cluster type is Unknown and we receive an ismaster response, what should
# the new cluster type be?
_SERVER_TYPE_TO_CLUSTER_TYPE = {
    SERVER_TYPE.Mongos: CLUSTER_TYPE.Sharded,
    SERVER_TYPE.RSPrimary: CLUSTER_TYPE.ReplicaSetWithPrimary,
    SERVER_TYPE.RSSecondary: CLUSTER_TYPE.ReplicaSetNoPrimary,
    SERVER_TYPE.RSArbiter: CLUSTER_TYPE.ReplicaSetNoPrimary,
    SERVER_TYPE.RSOther: CLUSTER_TYPE.ReplicaSetNoPrimary,
}


def updated_cluster_description(cluster_description, server_description):
    """Return an updated copy of a ClusterDescription.

    :Parameters:
      - `cluster_description`: the current ClusterDescription
      - `server_description`: a new ServerDescription that resulted from
        an ismaster call

    Called after attempting (successfully or not) to call ismaster on the
    server at server_description.address. Does not modify cluster_description.
    """
    address = server_description.address

    # These values will be updated, if necessary, to form the new
    # ClusterDescription.
    cluster_type = cluster_description.cluster_type
    set_name = cluster_description.set_name
    server_type = server_description.server_type

    # Don't mutate the original dict of server descriptions; copy it.
    sds = cluster_description.server_descriptions()

    # Replace this server's description with the new one.
    sds[address] = server_description

    if cluster_type == CLUSTER_TYPE.Single:
        # Single type never changes.
        return ClusterDescription(CLUSTER_TYPE.Single, sds, set_name)

    if cluster_type == CLUSTER_TYPE.Unknown:
        if server_type == SERVER_TYPE.Standalone:
            sds.pop(address)

        elif server_type not in (SERVER_TYPE.Unknown, SERVER_TYPE.RSGhost):
            cluster_type = _SERVER_TYPE_TO_CLUSTER_TYPE[server_type]

    if cluster_type == CLUSTER_TYPE.Sharded:
        if server_type != SERVER_TYPE.Mongos:
            sds.pop(address)

    elif cluster_type == CLUSTER_TYPE.ReplicaSetNoPrimary:
        if server_type in (SERVER_TYPE.Standalone, SERVER_TYPE.Mongos):
            sds.pop(address)

        elif server_type == SERVER_TYPE.RSPrimary:
            cluster_type, set_name = _update_rs_from_primary(
                sds, set_name, server_description)

        elif server_type in (
                SERVER_TYPE.RSSecondary,
                SERVER_TYPE.RSArbiter,
                SERVER_TYPE.RSOther):
            cluster_type, set_name = _update_rs_no_primary_from_member(
                sds, set_name, server_description)

    elif cluster_type == CLUSTER_TYPE.ReplicaSetWithPrimary:
        if server_type in (SERVER_TYPE.Standalone, SERVER_TYPE.Mongos):
            sds.pop(address)
            cluster_type = _check_has_primary(sds)

        elif server_type == SERVER_TYPE.RSPrimary:
            cluster_type = _update_rs_from_primary(
                sds, set_name, server_description)

        elif server_type in (
                SERVER_TYPE.RSSecondary,
                SERVER_TYPE.RSArbiter,
                SERVER_TYPE.RSOther):
            cluster_type = _update_rs_with_primary_from_member(
                sds, set_name, server_description)

        else:
            # Server type is Unknown or RSGhost: did we just lose the primary?
            cluster_type = _check_has_primary(sds)

    # Return updated copy.
    return ClusterDescription(cluster_type, sds, set_name)


def _update_rs_from_primary(sds, set_name, server_description):
    """Update cluster description from a primary's ismaster response.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns (new cluster type, new set_name).
    """
    cluster_type = CLUSTER_TYPE.ReplicaSetWithPrimary
    if set_name is None:
        set_name = server_description.set_name

    elif set_name != server_description.set_name:
        # We found a primary but it doesn't have the set_name
        # provided by the user.
        sds.pop(server_description.address)
        cluster_type = CLUSTER_TYPE.ReplicaSetNoPrimary
        return cluster_type, set_name

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
    all_hosts = set(server_description.all_hosts)
    for old_sd in sds.values():
        if old_sd.address not in all_hosts:
            sds.pop(old_sd.address)

    return cluster_type, set_name


def _update_rs_with_primary_from_member(sds, set_name, server_description):
    """RS with known primary. Process a response from a non-primary.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns new cluster type.
    """
    assert set_name is not None

    if set_name != server_description.set_name:
        sds.pop(server_description.address)

    # Had this member been the primary?
    return _check_has_primary(sds)


def _update_rs_no_primary_from_member(sds, set_name, server_description):
    """RS without known primary. Update from a non-primary's response.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns (new cluster type, new set_name).
    """
    cluster_type = CLUSTER_TYPE.ReplicaSetNoPrimary
    if set_name is None:
        set_name = server_description.set_name

    elif set_name != server_description.set_name:
        sds.pop(server_description.address)
        return cluster_type, set_name

    # This isn't the primary's response, so don't remove any servers
    # it doesn't report. Only add new servers.
    for address in server_description.all_hosts:
        if address not in sds:
            sds[address] = ServerDescription(address)

    return cluster_type, set_name


def _check_has_primary(sds):
    """Current cluster type is ReplicaSetWithPrimary. Is primary still known?

    Pass in a dict of ServerDescriptions.

    Returns new cluster type.
    """
    for s in sds.values():
        if s.server_type == SERVER_TYPE.RSPrimary:
            return CLUSTER_TYPE.ReplicaSetWithPrimary
    else:
        return CLUSTER_TYPE.ReplicaSetNoPrimary
