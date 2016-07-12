# Copyright 2016 MongoDB, Inc.
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

"""Criteria to select ServerDescriptions based on maxStalenessMS.

The Max Staleness Spec says: When there is a known primary P,
a secondary S's staleness is estimated with this formula:

  (S.lastUpdateTime - S.lastWriteDate) - (P.lastUpdateTime - P.lastWriteDate)
  + heartbeatFrequencyMS

When there is no known primary, a secondary S's staleness is estimated with:

  SMax.lastWriteDate - S.lastWriteDate + heartbeatFrequencyMS

where "SMax" is the secondary with the greatest lastWriteDate.
"""

from pymongo.errors import ConfigurationError
from pymongo.server_type import SERVER_TYPE


def _with_primary(max_staleness, selection):
    """Apply max_staleness, in seconds, to a Selection with a known primary."""
    primary = selection.primary
    sds = []

    for s in selection.server_descriptions:
        if s.server_type == SERVER_TYPE.RSSecondary:
            # See max-staleness.rst for explanation of this formula.
            staleness = (
                (s.last_update_time - s.last_write_date) -
                (primary.last_update_time - primary.last_write_date) +
                selection.heartbeat_frequency)

            if staleness <= max_staleness:
                sds.append(s)
        else:
            sds.append(s)

    return selection.with_server_descriptions(sds)


def _no_primary(max_staleness, selection):
    """Apply max_staleness, in seconds, to a Selection with no known primary."""
    smax = selection.secondary_with_max_last_write_date()
    sds = []

    for s in selection.server_descriptions:
        if s.server_type == SERVER_TYPE.RSSecondary:
            # See max-staleness.rst for explanation of this formula.
            staleness = (smax.last_write_date -
                         s.last_write_date +
                         selection.heartbeat_frequency)

            if staleness <= max_staleness:
                sds.append(s)
        else:
            sds.append(s)

    return selection.with_server_descriptions(sds)


def select(max_staleness, selection):
    """Apply max_staleness, in seconds, to a Selection."""
    if not max_staleness:
        return selection

    # Server Selection Spec: "A driver MUST raise an error if the
    # TopologyType is ReplicaSetWithPrimary or ReplicaSetNoPrimary and
    # maxStalenessMS is less than twice heartbeatFrequencyMS."
    if max_staleness < 2 * selection.heartbeat_frequency:
        raise ConfigurationError(
            "maxStalenessMS must be twice heartbeatFrequencyMS")

    if selection.primary:
        return _with_primary(max_staleness, selection)
    else:
        return _no_primary(max_staleness, selection)
