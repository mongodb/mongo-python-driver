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
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

sys.path[0:0] = [""]

from bson import ObjectId
from test import UnitTest, unittest
from test.unified_format import (
    MatchEvaluatorUtil,
    UnifiedSpecTestMixinV1,
    generate_test_classes,
    get_test_path,
)
from test.version import Version

_IS_SYNC = True

# Location of JSON test specifications.
TEST_PATH = get_test_path("unified-test-format")

globals().update(
    generate_test_classes(
        os.path.join(TEST_PATH, "valid-pass"),
        module=__name__,
        class_name_prefix="UnifiedTestFormat",
    )
)


globals().update(
    generate_test_classes(
        os.path.join(TEST_PATH, "valid-fail"),
        module=__name__,
        class_name_prefix="UnifiedTestFormat",
        bypass_test_generation_errors=True,
        expected_failures=[
            ".*",  # All tests expected to fail
        ],
    )
)


class TestSchemaVersionCoverage(UnitTest):
    def test_all_spec_files_are_supported(self):
        # PYTHON-6293: a spec resync that bumps a file's schemaVersion past
        # the runner's SCHEMA_VERSION otherwise only surfaces as a runtime
        # failure in setUp. Every consumed spec file must declare a
        # schema version the runner supports.
        # test/unified-test-format/invalid/ is not consumed by the runner and
        # valid-fail/ files (including schemaVersion-unsupported.json) are
        # deliberately invalid, so both are skipped.
        runner_version = UnifiedSpecTestMixinV1.SCHEMA_VERSION
        root = get_test_path()
        skipped = (
            os.path.join("unified-test-format", "invalid"),
            os.path.join("unified-test-format", "valid-fail"),
        )
        unsupported = []
        for dirpath, dirnames, filenames in os.walk(root):
            rel = os.path.relpath(dirpath, root)
            if any(rel == prefix or rel.startswith(prefix + os.sep) for prefix in skipped):
                continue
            for filename in sorted(filenames):
                if not filename.endswith(".json"):
                    continue
                with open(os.path.join(dirpath, filename)) as fpath:
                    spec = json.load(fpath)
                version = spec.get("schemaVersion") if isinstance(spec, dict) else None
                if not isinstance(version, str):
                    continue
                parsed = Version.from_string(version)
                if parsed[0] != runner_version[0] or parsed > runner_version:
                    unsupported.append((os.path.join(rel, filename), version))

        self.assertEqual(unsupported, [])


class TestMatchEvaluatorUtil(UnitTest):
    def setUp(self):
        self.match_evaluator = MatchEvaluatorUtil(self)

    def test_unsetOrMatches(self):
        spec: dict[str, Any] = {"$$unsetOrMatches": {"y": {"$$unsetOrMatches": 2}}}
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

    def test_lte(self):
        # Added in schema version 1.9.
        self.match_evaluator.match_result({"x": {"$$lte": 5}}, {"x": 5})
        self.match_evaluator.match_result({"x": {"$$lte": 5}}, {"x": 3})
        with self.assertRaises(AssertionError):
            self.match_evaluator.match_result({"x": {"$$lte": 5}}, {"x": 6})

    def test_gte(self):
        # Added in schema version 1.29.
        self.match_evaluator.match_result({"x": {"$$gte": 5}}, {"x": 5})
        self.match_evaluator.match_result({"x": {"$$gte": 5}}, {"x": 6})
        with self.assertRaises(AssertionError):
            self.match_evaluator.match_result({"x": {"$$gte": 5}}, {"x": 3})


if __name__ == "__main__":
    unittest.main()
