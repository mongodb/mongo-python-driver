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
from test.unified_format import UnifiedSpecTestMixinV1


class TestCreateEntities(unittest.TestCase):
    def test_store_events_as_entities(self):
        self.scenario_runner = UnifiedSpecTestMixinV1()
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
                                ],
                            }
                        ],
                    }
                },
            ],
            "tests": [{"description": "foo", "operations": []}],
        }
        self.scenario_runner.TEST_SPEC = spec
        self.scenario_runner.setUp()
        self.scenario_runner.run_scenario(spec["tests"][0])
        self.scenario_runner.entity_map["client0"].close()
        final_entity_map = self.scenario_runner.entity_map
        self.assertIn("events1", final_entity_map)
        self.assertGreater(len(final_entity_map["events1"]), 0)
        for event in final_entity_map["events1"]:
            self.assertIn("PoolCreatedEvent", event["name"])

    def test_store_all_others_as_entities(self):
        self.scenario_runner = UnifiedSpecTestMixinV1()
        spec = {
            "description": "Find",
            "schemaVersion": "1.2",
            "createEntities": [
                {
                    "client": {
                        "id": "client0",
                        "uriOptions": {"retryReads": True},
                    }
                },
                {"database": {"id": "database0", "client": "client0", "databaseName": "dat"}},
                {
                    "collection": {
                        "id": "collection0",
                        "database": "database0",
                        "collectionName": "dat",
                    }
                },
            ],
            "tests": [
                {
                    "description": "test loops",
                    "operations": [
                        {
                            "name": "loop",
                            "object": "testRunner",
                            "arguments": {
                                "storeIterationsAsEntity": "iterations",
                                "storeSuccessesAsEntity": "successes",
                                "storeFailuresAsEntity": "failures",
                                "storeErrorsAsEntity": "errors",
                                "numIterations": 5,
                                "operations": [
                                    {
                                        "name": "insertOne",
                                        "object": "collection0",
                                        "arguments": {"document": {"_id": 1, "x": 44}},
                                    },
                                    {
                                        "name": "insertOne",
                                        "object": "collection0",
                                        "arguments": {"document": {"_id": 1, "x": 44}},
                                    },
                                ],
                            },
                        }
                    ],
                }
            ],
        }

        self.scenario_runner.TEST_SPEC = spec
        self.scenario_runner.setUp()
        self.scenario_runner.run_scenario(spec["tests"][0])
        self.scenario_runner.entity_map["client0"].close()
        final_entity_map = self.scenario_runner.entity_map
        for entity in ["errors", "failures"]:
            self.assertIn(entity, final_entity_map)
            self.assertGreaterEqual(len(final_entity_map[entity]), 0)
            self.assertEqual(type(final_entity_map[entity]), list)
        for entity in ["successes", "iterations"]:
            self.assertIn(entity, final_entity_map)
            self.assertEqual(type(final_entity_map[entity]), int)
