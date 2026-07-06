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

"""Test the sans-I/O server-selection tie-breaking. No server required."""

from __future__ import annotations

import sys

sys.path[0:0] = [""]

from pymongo._sdam_selection import select_least_loaded
from test import unittest


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


if __name__ == "__main__":
    unittest.main()
