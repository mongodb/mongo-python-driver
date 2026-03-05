# Copyright 2019-present MongoDB, Inc.
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

"""Utilities for testing driver specs."""
from __future__ import annotations

import asyncio
import os
import time
from test import client_context
from test.helpers import ConcurrentRunner
from test.utils_shared import ScenarioDict

from bson import json_util
from pymongo.lock import _cond_wait, _create_condition, _create_lock

_IS_SYNC = True


class SpecRunnerThread(ConcurrentRunner):
    def __init__(self, name):
        super().__init__(name=name)
        self.exc = None
        self.daemon = True
        self.cond = _create_condition(_create_lock())
        self.ops = []

    def schedule(self, work):
        self.ops.append(work)
        with self.cond:
            self.cond.notify()

    def stop(self):
        self.stopped = True
        with self.cond:
            self.cond.notify()

    def run(self):
        while not self.stopped or self.ops:
            if not self.ops:
                with self.cond:
                    _cond_wait(self.cond, 10)
            if self.ops:
                try:
                    work = self.ops.pop(0)
                    work()
                except Exception as exc:
                    self.exc = exc
                    self.stop()


class SpecTestCreator:
    """Class to create test cases from specifications."""

    def __init__(self, create_test, test_class, test_path):
        """Create a TestCreator object.

        :Parameters:
          - `create_test`: callback that returns a test case. The callback
            must accept the following arguments - a dictionary containing the
            entire test specification (the `scenario_def`), a dictionary
            containing the specification for which the test case will be
            generated (the `test_def`).
          - `test_class`: the unittest.TestCase class in which to create the
            test case.
          - `test_path`: path to the directory containing the JSON files with
            the test specifications.
        """
        self._create_test = create_test
        self._test_class = test_class
        self.test_path = test_path

    def _ensure_min_max_server_version(self, scenario_def, method):
        """Test modifier that enforces a version range for the server on a
        test case.
        """
        if "minServerVersion" in scenario_def:
            min_ver = tuple(int(elt) for elt in scenario_def["minServerVersion"].split("."))
            if min_ver is not None:
                method = client_context.require_version_min(*min_ver)(method)

        if "maxServerVersion" in scenario_def:
            max_ver = tuple(int(elt) for elt in scenario_def["maxServerVersion"].split("."))
            if max_ver is not None:
                method = client_context.require_version_max(*max_ver)(method)

        return method

    @staticmethod
    def valid_topology(run_on_req):
        return client_context.is_topology_type(
            run_on_req.get("topology", ["single", "replicaset", "sharded", "load-balanced"])
        )

    @staticmethod
    def min_server_version(run_on_req):
        version = run_on_req.get("minServerVersion")
        if version:
            min_ver = tuple(int(elt) for elt in version.split("."))
            return client_context.version >= min_ver
        return True

    @staticmethod
    def max_server_version(run_on_req):
        version = run_on_req.get("maxServerVersion")
        if version:
            max_ver = tuple(int(elt) for elt in version.split("."))
            return client_context.version <= max_ver
        return True

    @staticmethod
    def valid_auth_enabled(run_on_req):
        if "authEnabled" in run_on_req:
            if run_on_req["authEnabled"]:
                return client_context.auth_enabled
            return not client_context.auth_enabled
        return True

    def should_run_on(self, scenario_def):
        run_on = scenario_def.get("runOn", [])
        if not run_on:
            # Always run these tests.
            return True

        for req in run_on:
            if (
                self.valid_topology(req)
                and self.min_server_version(req)
                and self.max_server_version(req)
                and self.valid_auth_enabled(req)
            ):
                return True
        return False

    def ensure_run_on(self, scenario_def, method):
        """Test modifier that enforces a 'runOn' on a test case."""

        def predicate():
            return self.should_run_on(scenario_def)

        return client_context._require(predicate, "runOn not satisfied", method)

    def tests(self, scenario_def):
        """Allow CMAP spec test to override the location of test."""
        return scenario_def["tests"]

    def _create_tests(self):
        for dirpath, _, filenames in os.walk(self.test_path):
            dirname = os.path.split(dirpath)[-1]

            for filename in filenames:
                with open(os.path.join(dirpath, filename)) as scenario_stream:  # noqa: ASYNC101, RUF100
                    # Use tz_aware=False to match how CodecOptions decodes
                    # dates.
                    opts = json_util.JSONOptions(tz_aware=False)
                    scenario_def = ScenarioDict(
                        json_util.loads(scenario_stream.read(), json_options=opts)
                    )

                test_type = os.path.splitext(filename)[0]

                # Construct test from scenario.
                for test_def in self.tests(scenario_def):
                    test_name = "test_{}_{}_{}".format(
                        dirname,
                        test_type.replace("-", "_").replace(".", "_"),
                        str(test_def["description"].replace(" ", "_").replace(".", "_")),
                    )

                    new_test = self._create_test(scenario_def, test_def, test_name)
                    new_test = self._ensure_min_max_server_version(scenario_def, new_test)
                    new_test = self.ensure_run_on(scenario_def, new_test)

                    new_test.__name__ = test_name
                    setattr(self._test_class, new_test.__name__, new_test)

    def create_tests(self):
        if _IS_SYNC:
            self._create_tests()
        else:
            asyncio.run(self._create_tests())
