# Copyright 2021-present MongoDB, Inc.
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
import unittest
import time

from test.unified_format import UnifiedSpecTestMixinV1


class TestCreateEntities(unittest.TestCase):

    def setUp(self):
        self.scenario_runner = UnifiedSpecTestMixinV1()

    def test_store_events_as_entities(self):
        spec = {
                  "description": "blank",
                  "schemaVersion": "1.2",
                  "createEntities": [
                    {
                      "client": {
                        "id": "client0",
                        "storeEventsAsEntities": [
                          {
                            "id": "events1",
                            "events": [
                              "PoolCreatedEvent",
                            ]
                          }
                        ]
                      }
                    },
                  ],
                  "tests": [
                    {
                      "description": "foo",
                      "operations": []
                    }
                  ]
                }
        self.scenario_runner.TEST_SPEC = spec
        self.scenario_runner.setUp()
        self.scenario_runner.run_scenario(spec["tests"][0])
        final_entity_map = self.scenario_runner.entity_map._entities
        self.assertIn("events1", final_entity_map)
        self.assertGreater(len(final_entity_map["events1"]), 0)
