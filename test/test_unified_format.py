# Copyright 2020-present MongoDB, Inc.
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

import os
import sys

sys.path[0:0] = [""]

from test import unittest
from test.unified_format import MatchEvaluatorUtil, generate_test_classes

from bson import ObjectId

_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "unified-test-format")


globals().update(
    generate_test_classes(
        os.path.join(_TEST_PATH, "valid-pass"),
        module=__name__,
        class_name_prefix="UnifiedTestFormat",
        expected_failures=[
            "Client side error in command starting transaction",  # PYTHON-1894
        ],
        RUN_ON_SERVERLESS=False,
    )
)


globals().update(
    generate_test_classes(
        os.path.join(_TEST_PATH, "valid-fail"),
        module=__name__,
        class_name_prefix="UnifiedTestFormat",
        bypass_test_generation_errors=True,
        expected_failures=[
            ".*",  # All tests expected to fail
        ],
        RUN_ON_SERVERLESS=False,
    )
)


class TestMatchEvaluatorUtil(unittest.TestCase):
    def setUp(self):
        self.match_evaluator = MatchEvaluatorUtil(self)

    def test_unsetOrMatches(self):
        spec = {"$$unsetOrMatches": {"y": {"$$unsetOrMatches": 2}}}
        for actual in [{}, {"y": 2}, None]:
            self.match_evaluator.match_result(spec, actual)

        spec = {"x": {"$$unsetOrMatches": {"y": {"$$unsetOrMatches": 2}}}}
        for actual in [{}, {"x": {}}, {"x": {"y": 2}}]:
            self.match_evaluator.match_result(spec, actual)

        spec = {"y": {"$$unsetOrMatches": {"$$exists": True}}}
        self.match_evaluator.match_result(spec, {})
        self.match_evaluator.match_result(spec, {"y": 2})
        self.match_evaluator.match_result(spec, {"x": 1})
        self.match_evaluator.match_result(spec, {"y": {}})

    def test_type(self):
        self.match_evaluator.match_result(
            {
                "operationType": "insert",
                "ns": {"db": "change-stream-tests", "coll": "test"},
                "fullDocument": {"_id": {"$$type": "objectId"}, "x": 1},
            },
            {
                "operationType": "insert",
                "fullDocument": {"_id": ObjectId("5fc93511ac93941052098f0c"), "x": 1},
                "ns": {"db": "change-stream-tests", "coll": "test"},
            },
        )


if __name__ == "__main__":
    unittest.main()
