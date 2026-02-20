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

sys.path[0:0] = [""]

from test import (
    IntegrationTest,
    PyMongoTestCase,
    client_context,
    unittest,
)
from test.unified_format import generate_test_classes
from test.utils_shared import EventListener, OvertCommandListener

import pymongo
from pymongo.errors import OperationFailure, PyMongoError
from pymongo.synchronous import helpers
from pymongo.synchronous.helpers import _MAX_RETRIES, _RetryPolicy, _TokenBucket

_IS_SYNC = True

# Mock an system overload error.
mock_overload_error = {
    "configureFailPoint": "failCommand",
    "mode": {"times": 1},
    "data": {
        "failCommands": ["find", "insert", "update"],
        "errorCode": 462,  # IngressRequestRateLimitExceeded
        "errorLabels": ["RetryableError", "SystemOverloadedError"],
    },
}


class TestBackpressure(IntegrationTest):
    RUN_ON_LOAD_BALANCER = True

    @client_context.require_failCommand_appName
    def test_retry_overload_error_command(self):
        self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = mock_overload_error.copy()
        fail_many["mode"] = {"times": _MAX_RETRIES}
        with self.fail_point(fail_many):
            self.db.command("find", "t")

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_too_many = mock_overload_error.copy()
        fail_too_many["mode"] = {"times": _MAX_RETRIES + 1}
        with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                self.db.command("find", "t")

        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

    @client_context.require_failCommand_appName
    def test_retry_overload_error_find(self):
        self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = mock_overload_error.copy()
        fail_many["mode"] = {"times": _MAX_RETRIES}
        with self.fail_point(fail_many):
            self.db.t.find_one()

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_too_many = mock_overload_error.copy()
        fail_too_many["mode"] = {"times": _MAX_RETRIES + 1}
        with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                self.db.t.find_one()

        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

    @client_context.require_failCommand_appName
    def test_retry_overload_error_insert_one(self):
        self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = mock_overload_error.copy()
        fail_many["mode"] = {"times": _MAX_RETRIES}
        with self.fail_point(fail_many):
            self.db.t.find_one()

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_too_many = mock_overload_error.copy()
        fail_too_many["mode"] = {"times": _MAX_RETRIES + 1}
        with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                self.db.t.find_one()

        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

    @client_context.require_failCommand_appName
    def test_retry_overload_error_update_many(self):
        # Even though update_many is not a retryable write operation, it will
        # still be retried via the "RetryableError" error label.
        self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = mock_overload_error.copy()
        fail_many["mode"] = {"times": _MAX_RETRIES}
        with self.fail_point(fail_many):
            self.db.t.update_many({}, {"$set": {"x": 2}})

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_too_many = mock_overload_error.copy()
        fail_too_many["mode"] = {"times": _MAX_RETRIES + 1}
        with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                self.db.t.update_many({}, {"$set": {"x": 2}})

        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

    @client_context.require_failCommand_appName
    def test_retry_overload_error_getMore(self):
        coll = self.db.t
        coll.insert_many([{"x": 1} for _ in range(10)])

        # Ensure command is retried on overload error.
        fail_many = {
            "configureFailPoint": "failCommand",
            "mode": {"times": _MAX_RETRIES},
            "data": {
                "failCommands": ["getMore"],
                "errorCode": 462,  # IngressRequestRateLimitExceeded
                "errorLabels": ["RetryableError", "SystemOverloadedError"],
            },
        }
        cursor = coll.find(batch_size=2)
        cursor.next()
        with self.fail_point(fail_many):
            cursor.to_list()

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_too_many = fail_many.copy()
        fail_too_many["mode"] = {"times": _MAX_RETRIES + 1}
        cursor = coll.find(batch_size=2)
        cursor.next()
        with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                cursor.to_list()

        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))


class TestRetryPolicy(PyMongoTestCase):
    def test_retry_policy(self):
        capacity = 10
        retry_policy = _RetryPolicy(_TokenBucket(capacity=capacity), adaptive_retry=True)
        self.assertEqual(retry_policy.attempts, helpers._MAX_RETRIES)
        self.assertEqual(retry_policy.backoff_initial, helpers._BACKOFF_INITIAL)
        self.assertEqual(retry_policy.backoff_max, helpers._BACKOFF_MAX)
        for i in range(1, helpers._MAX_RETRIES + 1):
            self.assertTrue(retry_policy.should_retry(i, 0))
        self.assertFalse(retry_policy.should_retry(helpers._MAX_RETRIES + 1, 0))
        for i in range(capacity - helpers._MAX_RETRIES):
            self.assertTrue(retry_policy.should_retry(1, 0))
        # No tokens left, should not retry.
        self.assertFalse(retry_policy.should_retry(1, 0))
        self.assertEqual(retry_policy.token_bucket.tokens, 0)

        # record_success should generate tokens.
        for _ in range(int(2 / helpers.DEFAULT_RETRY_TOKEN_RETURN)):
            retry_policy.record_success(retry=False)
        self.assertAlmostEqual(retry_policy.token_bucket.tokens, 2)
        for i in range(2):
            self.assertTrue(retry_policy.should_retry(1, 0))
        self.assertFalse(retry_policy.should_retry(1, 0))

        # Recording a successful retry should return 1 additional token.
        retry_policy.record_success(retry=True)
        self.assertAlmostEqual(
            retry_policy.token_bucket.tokens, 1 + helpers.DEFAULT_RETRY_TOKEN_RETURN
        )
        self.assertTrue(retry_policy.should_retry(1, 0))
        self.assertFalse(retry_policy.should_retry(1, 0))
        self.assertAlmostEqual(retry_policy.token_bucket.tokens, helpers.DEFAULT_RETRY_TOKEN_RETURN)

    def test_retry_policy_csot(self):
        retry_policy = _RetryPolicy(_TokenBucket())
        self.assertTrue(retry_policy.should_retry(1, 0.5))
        with pymongo.timeout(0.5):
            self.assertTrue(retry_policy.should_retry(1, 0))
            self.assertTrue(retry_policy.should_retry(1, 0.1))
            # Would exceed the timeout, should not retry.
            self.assertFalse(retry_policy.should_retry(1, 1.0))
        self.assertTrue(retry_policy.should_retry(1, 1.0))


# Prose tests.
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

    @client_context.require_failCommand_appName
    def test_03_overload_retries_limited(self):
        # Drivers should test that without adaptive retries enabled, overload errors are retried a maximum of five times.

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
        with self.fail_point(failpoint):
            with self.assertRaises(PyMongoError) as error:
                coll.find_one({})

        # 5. Assert that the raised error contains both the `RetryableError` and `SystemOverLoadedError` error labels.
        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

        # 6. Assert that the total number of started commands is MAX_RETRIES + 1.
        self.assertEqual(len(self.listener.started_events), _MAX_RETRIES + 1)

    @client_context.require_failCommand_appName
    def test_03_adaptive_retries_limited_by_tokens(self):
        # Drivers should test that when enabled, adaptive retries are limited by the number of tokens in the bucket.

        # 1. Let `client` be a `MongoClient` with adaptiveRetries=True.
        client = self.rs_or_single_client(adaptive_retries=True, event_listeners=[self.listener])
        # 2. Set `client`'s retry token bucket to have 2 tokens.
        client._retry_policy.token_bucket.tokens = 2
        # 3. Let `coll` be a collection.
        coll = client.pymongo_test.coll

        # 4. Configure the following failpoint:
        failpoint = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 3},
            "data": {
                "failCommands": ["find"],
                "errorCode": 462,  # IngressRequestRateLimitExceeded
                "errorLabels": ["RetryableError", "SystemOverloadedError"],
            },
        }

        # 5. Perform a find operation with `coll` that fails.
        with self.fail_point(failpoint):
            with self.assertRaises(PyMongoError) as error:
                coll.find_one({})

        # 6. Assert that the raised error contains both the `RetryableError` and `SystemOverLoadedError` error labels.
        self.assertIn("RetryableError", str(error.exception))
        self.assertIn("SystemOverloadedError", str(error.exception))

        # 7. Assert that the total number of started commands is 3: one for the initial attempt and two for the retries.
        self.assertEqual(len(self.listener.started_events), 3)


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
