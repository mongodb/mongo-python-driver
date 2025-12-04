# Copyright 2025-present MongoDB, Inc.
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

import os
import pathlib
import sys
from time import perf_counter
from unittest.mock import patch

from pymongo.errors import OperationFailure

sys.path[0:0] = [""]

from test import (
    IntegrationTest,
    client_context,
    unittest,
)
from test.unified_format import generate_test_classes
from test.utils_shared import EventListener, OvertCommandListener

_IS_SYNC = True


class TestClientBackpressure(IntegrationTest):
    listener: EventListener

    @classmethod
    def setUpClass(cls) -> None:
        cls.listener = OvertCommandListener()

    @client_context.require_connection
    def setUp(self) -> None:
        super().setUp()
        self.listener.reset()
        self.app_name = self.__class__.__name__.lower()
        self.client = self.rs_or_single_client(
            event_listeners=[self.listener], retryWrites=False, appName=self.app_name
        )

    @patch("random.random")
    @client_context.require_failCommand_appName
    def test_01_operation_retry_uses_exponential_backoff(self, random_func):
        # Drivers should test that retries do not occur immediately when a SystemOverloadedError is encountered.

        # 1. let `client` be a `MongoClient`
        client = self.client

        # 2. let `collection` be a collection
        collection = client.test.test

        # 3. Now, run transactions without backoff:

        # a. Configure the random number generator used for jitter to always return `0` -- this effectively disables backoff.
        random_func.return_value = 0

        # b. Configure the following failPoint:
        fail_point = dict(
            mode="alwaysOn",
            data=dict(
                failCommands=["insert"],
                errorCode=2,
                errorLabels=["SystemOverloadedError", "RetryableError"],
                appName=self.app_name,
            ),
        )
        with self.fail_point(fail_point):
            # c. Execute the following command. Expect that the command errors. Measure the duration of the command execution.
            start0 = perf_counter()
            with self.assertRaises(OperationFailure):
                collection.insert_one({"a": 1})
            end0 = perf_counter()

            # d. Configure the random number generator used for jitter to always return `1`.
            random_func.return_value = 1

            # e. Execute step c again.
            start1 = perf_counter()
            with self.assertRaises(OperationFailure):
                collection.insert_one({"a": 1})
            end1 = perf_counter()

            # f. Compare the two time between the two runs.
            # The sum of 5 backoffs is 3.1 seconds. There is a 1-second window to account for potential variance between the two
            # runs.
            self.assertTrue(abs((end1 - start1) - (end0 - start0 + 3.1)) < 1)


# Location of JSON test specifications.
if _IS_SYNC:
    _TEST_PATH = os.path.join(pathlib.Path(__file__).resolve().parent, "client-backpressure")
else:
    _TEST_PATH = os.path.join(pathlib.Path(__file__).resolve().parent.parent, "client-backpressure")

globals().update(
    generate_test_classes(
        _TEST_PATH,
        module=__name__,
    )
)

if __name__ == "__main__":
    unittest.main()
