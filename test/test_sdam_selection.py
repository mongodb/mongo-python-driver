# Copyright 2026-present MongoDB, Inc.
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

"""Test the sans-I/O server-selection helpers. No server required."""

from __future__ import annotations

import sys

sys.path[0:0] = [""]

from pymongo._sdam_selection import (
    data_bearing_servers,
    format_selection_error,
    select_least_loaded,
)
from pymongo.errors import ConnectionFailure
from pymongo.hello import Hello
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import any_server_selector, writable_server_selector
from pymongo.settings import TopologySettings
from pymongo.topology_description import TOPOLOGY_TYPE, TopologyDescription
from test import unittest


def _standalone_sd(address):
    hello = Hello({"ok": 1, "isWritablePrimary": True, "maxWireVersion": 21})
    return ServerDescription(address, hello, 0)


def _mongos_sd(address):
    hello = Hello({"ok": 1, "msg": "isdbgrid", "maxWireVersion": 21})
    return ServerDescription(address, hello, 0)


def _td(topology_type, servers):
    """Build a real TopologyDescription from {address: ServerDescription}."""
    settings = TopologySettings(seeds=list(servers) or [("a", 27017)])
    return TopologyDescription(topology_type, dict(servers), None, None, None, settings)


_A = ("a", 27017)
_B = ("b", 27017)


class TestSelectLeastLoaded(unittest.TestCase):
    def test_single_candidate_returned_directly(self):
        # A lone candidate is returned without sampling.
        def boom(_seq, _k):
            raise AssertionError("sample must not be called for one candidate")

        self.assertEqual(select_least_loaded(["a"], lambda s: 0, sample=boom), "a")

    def test_picks_lower_load_of_the_two_sampled(self):
        load = {"a": 5, "b": 2, "c": 9}
        # Force the sampler to pick a and c; c is more loaded, so a wins.
        chosen = select_least_loaded(
            ["a", "b", "c"], load.__getitem__, sample=lambda seq, k: ["a", "c"]
        )
        self.assertEqual(chosen, "a")

    def test_ties_prefer_first_sampled(self):
        chosen = select_least_loaded(["a", "b"], lambda s: 7, sample=lambda seq, k: ["b", "a"])
        self.assertEqual(chosen, "b")

    def test_default_sampler_returns_a_candidate(self):
        # With the real random sampler, the result is always one of the inputs.
        candidates = ["a", "b", "c", "d"]
        for _ in range(50):
            self.assertIn(select_least_loaded(candidates, lambda s: 1), candidates)


class TestFormatSelectionError(unittest.TestCase):
    def test_no_servers_available(self):
        td = _td(TOPOLOGY_TYPE.Unknown, {})
        msg = format_selection_error(td, any_server_selector, None, [])
        self.assertEqual(msg, "No servers available")

    def test_no_replica_set_members_for_set_name(self):
        td = _td(TOPOLOGY_TYPE.ReplicaSetNoPrimary, {})
        msg = format_selection_error(td, any_server_selector, "rs0", [])
        self.assertEqual(msg, 'No replica set members available for replica set name "rs0"')

    def test_still_discovering(self):
        # A single Unknown server with no error yet.
        td = _td(TOPOLOGY_TYPE.Unknown, {_A: ServerDescription(_A)})
        msg = format_selection_error(td, any_server_selector, None, [_A])
        self.assertEqual(msg, "No servers found yet")

    def test_all_servers_share_one_error(self):
        err = ConnectionFailure("boom")
        td = _td(TOPOLOGY_TYPE.Unknown, {_A: ServerDescription(_A, error=err)})
        msg = format_selection_error(td, any_server_selector, None, [_A])
        self.assertEqual(msg, "boom")

    def test_distinct_errors_are_joined(self):
        td = _td(
            TOPOLOGY_TYPE.Unknown,
            {
                _A: ServerDescription(_A, error=ConnectionFailure("err-a")),
                _B: ServerDescription(_B, error=ConnectionFailure("err-b")),
            },
        )
        msg = format_selection_error(td, any_server_selector, None, [_A, _B])
        self.assertEqual(sorted(msg.split(",")), ["err-a", "err-b"])

    def test_replica_set_seeds_unreachable(self):
        # Servers whose addresses do not intersect the original seeds.
        err = ConnectionFailure("unreachable")
        td = _td(TOPOLOGY_TYPE.ReplicaSetNoPrimary, {_A: ServerDescription(_A, error=err)})
        msg = format_selection_error(td, writable_server_selector, "rs0", seed_addresses=[_B])
        self.assertIn("Could not reach any servers", msg)
        self.assertIn("internal hostnames or IPs", msg)


class TestDataBearingServers(unittest.TestCase):
    def test_single_returns_known_servers(self):
        # For a Single topology, any known server is data-bearing.
        td = _td(TOPOLOGY_TYPE.Single, {_A: _standalone_sd(_A)})
        result = data_bearing_servers(td)
        self.assertEqual([sd.address for sd in result], [_A])

    def test_single_excludes_unknown_servers(self):
        td = _td(TOPOLOGY_TYPE.Single, {_A: ServerDescription(_A)})
        self.assertEqual(data_bearing_servers(td), [])

    def test_sharded_returns_readable_servers(self):
        td = _td(TOPOLOGY_TYPE.Sharded, {_A: _mongos_sd(_A), _B: _mongos_sd(_B)})
        result = data_bearing_servers(td)
        self.assertEqual({sd.address for sd in result}, {_A, _B})

    def test_sharded_excludes_unknown_servers(self):
        td = _td(TOPOLOGY_TYPE.Sharded, {_A: _mongos_sd(_A), _B: ServerDescription(_B)})
        result = data_bearing_servers(td)
        self.assertEqual({sd.address for sd in result}, {_A})


if __name__ == "__main__":
    unittest.main()
