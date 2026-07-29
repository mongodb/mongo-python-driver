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

"""Run the OpenTelemetry unified format spec tests."""

from __future__ import annotations

import os
import sys

sys.path[0:0] = [""]

import pytest

from bson import json_util
from test import client_context, unittest
from test.unified_format import generate_test_classes, get_test_path

_IS_SYNC = True

pytestmark = pytest.mark.otel


def _otel_fixture_database_names() -> set[str]:
    """Collect every databaseName declared in the vendored OTel unified-test
    fixtures' createEntities blocks, so the cleanup fixture below never goes
    stale as fixtures are added, renamed, or removed."""
    names: set[str] = set()
    for dirpath, _, filenames in os.walk(get_test_path("open_telemetry")):
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json_util.loads(scenario_stream.read())
            for entity in scenario_def.get("createEntities", []):
                database = entity.get("database")
                if database is not None:
                    names.add(database["databaseName"])
    return names


@pytest.fixture(scope="module", autouse=True)
def _drop_otel_fixture_databases():
    # Some vendored OTel fixtures (e.g. operation/create_collection.json)
    # are not idempotent: they create a collection and rely on the unified
    # runner's insert_initial_data step to drop it beforehand, but that step
    # only runs when the fixture declares a non-null `initialData` -- this
    # one declares none, so nothing ever cleans the collection up between
    # runs. That's invisible upstream, where each fixture runs once per
    # process. PyMongo, however, runs every fixture *twice* per process: once
    # from this async module and again from its synchro-generated mirror,
    # test/test_open_telemetry_unified.py. The second pass then collides with
    # the first pass's leftover collection (NamespaceExists), which every
    # server version CI actually tests against (unlike our local 8.2+ server)
    # treats as a hard error rather than a no-op. Dropping the fixture
    # databases once before the module's tests run guarantees both passes
    # start from a clean slate.
    for name in _otel_fixture_database_names():
        client_context.client.drop_database(name)
    yield


globals().update(
    generate_test_classes(
        get_test_path("open_telemetry"),
        module=__name__,
    )
)

if __name__ == "__main__":
    unittest.main()
