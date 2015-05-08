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

"""Criteria to select some ServerDescriptions out of a list."""

from pymongo.server_type import SERVER_TYPE


def any_server_selector(server_descriptions):
    return server_descriptions


def readable_server_selector(server_descriptions):
    return [s for s in server_descriptions if s.is_readable]


def writable_server_selector(server_descriptions):
    return [s for s in server_descriptions if s.is_writable]


def secondary_server_selector(server_descriptions):
    return [s for s in server_descriptions
            if s.server_type == SERVER_TYPE.RSSecondary]


def arbiter_server_selector(server_descriptions):
    return [s for s in server_descriptions
            if s.server_type == SERVER_TYPE.RSArbiter]


def writable_preferred_server_selector(server_descriptions):
    """Like PrimaryPreferred but doesn't use tags or latency."""
    return (
        writable_server_selector(server_descriptions) or
        secondary_server_selector(server_descriptions))


def single_tag_set_server_selector(tag_set, server_descriptions):
    """All servers matching one tag set.

    A tag set is a dict. A server matches if its tags are a superset:
    A server tagged {'a': '1', 'b': '2'} matches the tag set {'a': '1'}.

    The empty tag set {} matches any server.

    The `server_descriptions` passed to this function should have
    non-readable servers (e.g. RSGhost, RSArbiter, Unknown) filtered
    out (e.g. by readable_server_selector or secondary_server_selector)
    first.
    """
    def tags_match(server_tags):
        for key, value in tag_set.items():
            if key not in server_tags or server_tags[key] != value:
                return False

        return True

    return [s for s in server_descriptions if tags_match(s.tags)]


def tag_sets_server_selector(tag_sets, server_descriptions):
    """All servers match a list of tag sets.

    tag_sets is a list of dicts. The empty tag set {} matches any server,
    and may be provided at the end of the list as a fallback. So
    [{'a': 'value'}, {}] expresses a preference for servers tagged
    {'a': 'value'}, but accepts any server if none matches the first
    preference.

    The `server_descriptions` passed to this function should have
    non-readable servers (e.g. RSGhost, RSArbiter, Unknown) filtered
    out (e.g. by readable_server_selector or secondary_server_selector)
    first.
    """
    for tag_set in tag_sets:
        selected = single_tag_set_server_selector(tag_set, server_descriptions)
        if selected:
            return selected

    return []


def apply_local_threshold(latency_ms, server_descriptions):
    """All servers with round trip times within latency_ms of the fastest one.

    No ServerDescription's round_trip_time can be None.

    The `server_descriptions` passed to this function should have
    non-readable servers (e.g. RSGhost, RSArbiter, Unknown) filtered
    out (e.g. by readable_server_selector or secondary_server_selector)
    first.
    """
    if not server_descriptions:
        # Avoid ValueError from min() with empty sequence.
        return []

    # round_trip_time is in seconds.
    if any(s for s in server_descriptions if s.round_trip_time is None):
        raise ValueError("Not all servers' round trip times are known")

    fastest = min(s.round_trip_time for s in server_descriptions)
    return [
        s for s in server_descriptions
        if (s.round_trip_time - fastest) <= latency_ms / 1000.]


def secondary_with_tags_server_selector(tag_sets, server_descriptions):
    """All near-enough secondaries matching the tag sets."""
    return tag_sets_server_selector(
        tag_sets, secondary_server_selector(server_descriptions))


def member_with_tags_server_selector(tag_sets, server_descriptions):
    """All near-enough members matching the tag sets."""
    return tag_sets_server_selector(
        tag_sets, readable_server_selector(server_descriptions))
