# Copyright 2015 MongoDB, Inc.
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

"""Test the topology module."""

import json
import os
import sys

sys.path[0:0] = [""]

from test import unittest
from pymongo.read_preferences import MovingAverage

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'server_selection/rtt')


class TestAllScenarios(unittest.TestCase):
    pass


def create_test(scenario_def):
    def run_scenario(self):
        moving_average = MovingAverage()

        if scenario_def['avg_rtt_ms'] != "NULL":
            moving_average.add_sample(scenario_def['avg_rtt_ms'])

        if scenario_def['new_rtt_ms'] != "NULL":
            moving_average.add_sample(scenario_def['new_rtt_ms'])

        self.assertAlmostEqual(moving_average.get(),
                               scenario_def['new_avg_rtt'])

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(scenario_stream)

            # Construct test from scenario.
            new_test = create_test(scenario_def)
            test_name = 'test_%s_%s' % (
                dirname, os.path.splitext(filename)[0])

            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()
