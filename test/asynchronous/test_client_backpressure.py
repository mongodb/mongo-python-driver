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

"""Test Client Backpressure spec."""

from __future__ import annotations

import os
import pathlib
import sys
from time import perf_counter
from unittest.mock import patch

from pymongo.common import MAX_ADAPTIVE_RETRIES

sys.path[0:0] = [""]

from pymongo.errors import OperationFailure, PyMongoError
from test.asynchronous import (
    AsyncIntegrationTest,
    async_client_context,
    unittest,
)
from test.asynchronous.unified_format import generate_test_classes
from test.utils_shared import EventListener, OvertCommandListener

_IS_SYNC = False

# Mock a system overload error.
mock_overload_error = {
    "configureFailPoint": "failCommand",
    "mode": {"times": 1},
    "data": {
        "failCommands": ["find", "insert", "update"],
        "errorCode": 462,  # IngressRequestRateLimitExceeded
        "errorLabels": ["RetryableError", "SystemOverloadedError"],
    },
}


def get_mock_overload_error(times: int):
    error = mock_overload_error.copy()
    error["mode"] = {"times": times}
    return error


class TestBackpressure(AsyncIntegrationTest):
    RUN_ON_LOAD_BALANCER = True

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_command(self):
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = get_mock_overload_error(MAX_ADAPTIVE_RETRIES)
        async with self.fail_point(fail_many):
            await self.db.command("find", "t")

        # Ensure command stops retrying after MAX_ADAPTIVE_RETRIES.
        fail_too_many = get_mock_overload_error(MAX_ADAPTIVE_RETRIES + 1)
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await self.db.command("find", "t")

        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_find(self):
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = get_mock_overload_error(MAX_ADAPTIVE_RETRIES)
        async with self.fail_point(fail_many):
            await self.db.t.find_one()

        # Ensure command stops retrying after MAX_ADAPTIVE_RETRIES.
        fail_too_many = get_mock_overload_error(MAX_ADAPTIVE_RETRIES + 1)
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await self.db.t.find_one()

        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_insert_one(self):
        # Ensure command is retried on overload error.
        fail_many = get_mock_overload_error(MAX_ADAPTIVE_RETRIES)
        async with self.fail_point(fail_many):
            await self.db.t.insert_one({"x": 1})

        # Ensure command stops retrying after MAX_ADAPTIVE_RETRIES.
        fail_too_many = get_mock_overload_error(MAX_ADAPTIVE_RETRIES + 1)
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await self.db.t.insert_one({"x": 1})

        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_update_many(self):
        # Even though update_many is not a retryable write operation, it will
        # still be retried via the "RetryableError" error label.
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = get_mock_overload_error(MAX_ADAPTIVE_RETRIES)
        async with self.fail_point(fail_many):
            await self.db.t.update_many({}, {"$set": {"x": 2}})

        # Ensure command stops retrying after MAX_ADAPTIVE_RETRIES.
        fail_too_many = get_mock_overload_error(MAX_ADAPTIVE_RETRIES + 1)
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await self.db.t.update_many({}, {"$set": {"x": 2}})

        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_getMore(self):
        coll = self.db.t
        await coll.insert_many([{"x": 1} for _ in range(10)])

        # Ensure command is retried on overload error.
        fail_many = {
            "configureFailPoint": "failCommand",
            "mode": {"times": MAX_ADAPTIVE_RETRIES},
            "data": {
                "failCommands": ["getMore"],
                "errorCode": 462,  # IngressRequestRateLimitExceeded
                "errorLabels": ["RetryableError", "SystemOverloadedError"],
            },
        }
        cursor = coll.find(batch_size=2)
        await cursor.next()
        async with self.fail_point(fail_many):
            await cursor.to_list()

        # Ensure command stops retrying after MAX_ADAPTIVE_RETRIES.
        fail_too_many = fail_many.copy()
        fail_too_many["mode"] = {"times": MAX_ADAPTIVE_RETRIES + 1}
        cursor = coll.find(batch_size=2)
        await cursor.next()
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await cursor.to_list()

        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))


# Prose tests.
class AsyncTestClientBackpressure(AsyncIntegrationTest):
    listener: EventListener

    @classmethod
    def setUpClass(cls) -> None:
        cls.listener = OvertCommandListener()

    @async_client_context.require_connection
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.listener.reset()
        self.app_name = self.__class__.__name__.lower()
        self.client = await self.async_rs_or_single_client(
            event_listeners=[self.listener], appName=self.app_name
        )

    @patch("random.random")
    @async_client_context.require_failCommand_appName
    async def test_01_operation_retry_uses_exponential_backoff(self, random_func):
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
        async with self.fail_point(fail_point):
            # c. Execute the following command. Expect that the command errors. Measure the duration of the command execution.
            start0 = perf_counter()
            with self.assertRaises(OperationFailure):
                await collection.insert_one({"a": 1})
            end0 = perf_counter()

            # d. Configure the random number generator used for jitter to always return `1`.
            random_func.return_value = 1

            # e. Execute step c again.
            start1 = perf_counter()
            with self.assertRaises(OperationFailure):
                await collection.insert_one({"a": 1})
            end1 = perf_counter()

            # f. Compare the times between the two runs.
            # The sum of 2 backoffs is 0.6 seconds. There is a 0.6-second window to account for potential variance between the two
            # runs.
            self.assertTrue(abs((end1 - start1) - (end0 - start0 + 0.6)) < 0.6)

    @async_client_context.require_failCommand_appName
    async def test_03_overload_retries_limited(self):
        # Drivers should test that overload errors are retried a maximum of two times.

        # 1. Let `client` be a `MongoClient`.
        client = self.client
        # 2. Let `coll` be a collection.
        coll = client.pymongo_test.coll

        # 3. Configure the following failpoint:
        failpoint = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {
                "failCommands": ["find"],
                "errorCode": 462,  # IngressRequestRateLimitExceeded
                "errorLabels": ["RetryableError", "SystemOverloadedError"],
            },
        }

        # 4. Perform a find operation with `coll` that fails.
        async with self.fail_point(failpoint):
            with self.assertRaises(PyMongoError) as error:
                await coll.find_one({})

        # 5. Assert that the raised error contains both the `RetryableError` and `SystemOverloadedError` error labels.
        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

        # 6. Assert that the total number of started commands is MAX_ADAPTIVE_RETRIES + 1.
        self.assertEqual(len(self.listener.started_events), MAX_ADAPTIVE_RETRIES + 1)

    @async_client_context.require_failCommand_appName
    async def test_04_overload_retries_limited_configured(self):
        # Drivers should test that overload errors are retried a maximum of maxAdaptiveRetries times.
        max_retries = 1

        # 1. Let `client` be a `MongoClient` with `maxAdaptiveRetries=1` and command event monitoring enabled.
        client = await self.async_single_client(
            maxAdaptiveRetries=max_retries, event_listeners=[self.listener]
        )
        # 2. Let `coll` be a collection.
        coll = client.pymongo_test.coll

        # 3. Configure the following failpoint:
        failpoint = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {
                "failCommands": ["find"],
                "errorCode": 462,  # IngressRequestRateLimitExceeded
                "errorLabels": ["RetryableError", "SystemOverloadedError"],
            },
        }

        # 4. Perform a find operation with `coll` that fails.
        async with self.fail_point(failpoint):
            with self.assertRaises(PyMongoError) as error:
                await coll.find_one({})

        # 5. Assert that the raised error contains both the `RetryableError` and `SystemOverloadedError` error labels.
        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

        # 6. Assert that the total number of started commands is max_retries + 1.
        self.assertEqual(len(self.listener.started_events), max_retries + 1)

    @unittest.skipIf(
        sys.platform == "darwin",
        "externalClientBaseBackoffMS is not supported on macOS",
    )
    @patch("random.random")
    @async_client_context.require_version_min(9, 0, 0, -1)
    @async_client_context.require_failCommand_appName
    async def test_05_overload_errors_with_basebackoffms_override_backoff(self, random_func):
        # Drivers should test that overload errors with `baseBackoffMS` override the default backoff duration.

        # 1. Let `client` be a `MongoClient`.
        client = self.client

        # 2. Let `coll` be a collection.
        coll = client.test.test

        # 3. Configure the random number generator used for exponential backoff jitter to always return a number as
        # close as possible to `1`.
        random_func.return_value = 1

        # 4. Configure the following failPoint:
        fail_point = dict(
            mode="alwaysOn",
            data=dict(
                failCommands=["insert"],
                errorCode=462,
                errorLabels=["SystemOverloadedError", "RetryableError"],
                appName=self.app_name,
            ),
        )
        async with self.fail_point(fail_point):
            # 5. Insert the document `{ a: 1 }`. Expect that the command errors. Measure the duration of the command
            # execution.
            start0 = perf_counter()
            with self.assertRaises(OperationFailure):
                await coll.insert_one({"a": 1})
            end0 = perf_counter()
            exponential_backoff_time = end0 - start0

            # 6. Run the following command to set up `baseBackoffMS` on overload errors.
            try:
                await client.admin.command("setParameter", 1, externalClientBaseBackoffMS=50)

                # 7. Execute step 5 again.
                start1 = perf_counter()
                with self.assertRaises(OperationFailure) as ctx:
                    await coll.insert_one({"a": 1})
                end1 = perf_counter()
                with_base_backoff_ms_time = end1 - start1

                # 8. Assert the server attached `baseBackoffMS` to the error and the driver parsed it.
                self.assertEqual(ctx.exception._base_backoff_ms, 50)
            finally:
                # 9. Run the following command to disable `baseBackoffMS` on overload errors.
                await client.admin.command("setParameter", 1, externalClientBaseBackoffMS=0)

        # 10. Assert absolute bounds on each run's duration.
        # A run can never be faster than the sum of its backoffs.
        # With jitter pinned to 1, the default backoffs are 0.2 + 0.4 = 0.6s
        # and the baseBackoffMS=50 backoffs are 0.1 + 0.2 = 0.3s.
        self.assertGreaterEqual(exponential_backoff_time, 0.6)
        self.assertGreaterEqual(with_base_backoff_ms_time, 0.3)
        self.assertLess(with_base_backoff_ms_time, 0.6)


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
