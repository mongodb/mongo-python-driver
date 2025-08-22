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

import asyncio
import sys

import pymongo

sys.path[0:0] = [""]

from test.asynchronous import (
    AsyncIntegrationTest,
    AsyncPyMongoTestCase,
    async_client_context,
    unittest,
)

from pymongo.asynchronous import helpers
from pymongo.asynchronous.helpers import _MAX_RETRIES, _RetryPolicy, _TokenBucket
from pymongo.errors import PyMongoError

_IS_SYNC = False

# Mock an system overload error.
mock_overload_error = {
    "configureFailPoint": "failCommand",
    "mode": {"times": 1},
    "data": {
        "failCommands": ["find", "insert", "update"],
        "errorCode": 462,  # IngressRequestRateLimitExceeded
        "errorLabels": ["RetryableError"],
    },
}


class TestBackpressure(AsyncIntegrationTest):
    RUN_ON_LOAD_BALANCER = True

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_command(self):
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = mock_overload_error.copy()
        fail_many["mode"] = {"times": _MAX_RETRIES}
        async with self.fail_point(fail_many):
            await self.db.command("find", "t")

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_too_many = mock_overload_error.copy()
        fail_too_many["mode"] = {"times": _MAX_RETRIES + 1}
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await self.db.command("find", "t")

        self.assertIn("RetryableError", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_find(self):
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = mock_overload_error.copy()
        fail_many["mode"] = {"times": _MAX_RETRIES}
        async with self.fail_point(fail_many):
            await self.db.t.find_one()

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_too_many = mock_overload_error.copy()
        fail_too_many["mode"] = {"times": _MAX_RETRIES + 1}
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await self.db.t.find_one()

        self.assertIn("RetryableError", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_insert_one(self):
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = mock_overload_error.copy()
        fail_many["mode"] = {"times": _MAX_RETRIES}
        async with self.fail_point(fail_many):
            await self.db.t.find_one()

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_too_many = mock_overload_error.copy()
        fail_too_many["mode"] = {"times": _MAX_RETRIES + 1}
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await self.db.t.find_one()

        self.assertIn("RetryableError", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_update_many(self):
        # Even though update_many is not a retryable write operation, it will
        # still be retried via the "RetryableError" error label.
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_many = mock_overload_error.copy()
        fail_many["mode"] = {"times": _MAX_RETRIES}
        async with self.fail_point(fail_many):
            await self.db.t.update_many({}, {"$set": {"x": 2}})

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_too_many = mock_overload_error.copy()
        fail_too_many["mode"] = {"times": _MAX_RETRIES + 1}
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await self.db.t.update_many({}, {"$set": {"x": 2}})

        self.assertIn("RetryableError", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_getMore(self):
        coll = self.db.t
        await coll.insert_many([{"x": 1} for _ in range(10)])

        # Ensure command is retried on overload error.
        fail_many = {
            "configureFailPoint": "failCommand",
            "mode": {"times": _MAX_RETRIES},
            "data": {
                "failCommands": ["getMore"],
                "errorCode": 462,  # IngressRequestRateLimitExceeded
                "errorLabels": ["RetryableError"],
            },
        }
        cursor = coll.find(batch_size=2)
        await cursor.next()
        async with self.fail_point(fail_many):
            await cursor.to_list()

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_too_many = fail_many.copy()
        fail_too_many["mode"] = {"times": _MAX_RETRIES + 1}
        cursor = coll.find(batch_size=2)
        await cursor.next()
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await cursor.to_list()

        self.assertIn("RetryableError", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_limit_retry_command(self):
        client = await self.async_rs_or_single_client()
        client._retry_policy.token_bucket.tokens = 1
        db = client.pymongo_test
        await db.t.insert_one({"x": 1})

        # Ensure command is retried once overload error.
        fail_many = mock_overload_error.copy()
        fail_many["mode"] = {"times": 1}
        async with self.fail_point(fail_many):
            await db.command("find", "t")

        # Ensure command stops retrying when there are no tokens left.
        fail_too_many = mock_overload_error.copy()
        fail_too_many["mode"] = {"times": 2}
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await db.command("find", "t")

        self.assertIn("RetryableError", str(error.exception))


class TestRetryPolicy(AsyncPyMongoTestCase):
    async def test_retry_policy(self):
        capacity = 10
        retry_policy = _RetryPolicy(_TokenBucket(capacity=capacity))
        self.assertEqual(retry_policy.attempts, helpers._MAX_RETRIES)
        self.assertEqual(retry_policy.backoff_initial, helpers._BACKOFF_INITIAL)
        self.assertEqual(retry_policy.backoff_max, helpers._BACKOFF_MAX)
        for i in range(1, helpers._MAX_RETRIES + 1):
            self.assertTrue(await retry_policy.should_retry(i, 0))
        self.assertFalse(await retry_policy.should_retry(helpers._MAX_RETRIES + 1, 0))
        for i in range(capacity - helpers._MAX_RETRIES):
            self.assertTrue(await retry_policy.should_retry(1, 0))
        # No tokens left, should not retry.
        self.assertFalse(await retry_policy.should_retry(1, 0))
        self.assertEqual(retry_policy.token_bucket.tokens, 0)

        # record_success should generate tokens.
        for _ in range(int(2 / helpers.DEFAULT_RETRY_TOKEN_RETURN)):
            await retry_policy.record_success(retry=False)
        self.assertAlmostEqual(retry_policy.token_bucket.tokens, 2)
        for i in range(2):
            self.assertTrue(await retry_policy.should_retry(1, 0))
        self.assertFalse(await retry_policy.should_retry(1, 0))

        # Recording a successful retry should return 1 additional token.
        await retry_policy.record_success(retry=True)
        self.assertAlmostEqual(
            retry_policy.token_bucket.tokens, 1 + helpers.DEFAULT_RETRY_TOKEN_RETURN
        )
        self.assertTrue(await retry_policy.should_retry(1, 0))
        self.assertFalse(await retry_policy.should_retry(1, 0))
        self.assertAlmostEqual(retry_policy.token_bucket.tokens, helpers.DEFAULT_RETRY_TOKEN_RETURN)

    async def test_retry_policy_csot(self):
        retry_policy = _RetryPolicy(_TokenBucket())
        self.assertTrue(await retry_policy.should_retry(1, 0.5))
        with pymongo.timeout(0.5):
            self.assertTrue(await retry_policy.should_retry(1, 0))
            self.assertTrue(await retry_policy.should_retry(1, 0.1))
            # Would exceed the timeout, should not retry.
            self.assertFalse(await retry_policy.should_retry(1, 1.0))
        self.assertTrue(await retry_policy.should_retry(1, 1.0))

    @async_client_context.require_failCommand_appName
    async def test_limit_retry_command(self):
        client = await self.async_rs_or_single_client()
        client._retry_policy.token_bucket.tokens = 1
        db = client.pymongo_test
        await db.t.insert_one({"x": 1})

        # Ensure command is retried once overload error.
        fail_many = mock_overload_error.copy()
        fail_many["mode"] = {"times": 1}
        async with self.fail_point(fail_many):
            await db.command("find", "t")

        # Ensure command stops retrying when there are no tokens left.
        fail_too_many = mock_overload_error.copy()
        fail_too_many["mode"] = {"times": 2}
        async with self.fail_point(fail_too_many):
            with self.assertRaises(PyMongoError) as error:
                await db.command("find", "t")

        self.assertIn("Retryable", str(error.exception))


class TestRetryPolicy(AsyncPyMongoTestCase):
    async def test_retry_policy(self):
        capacity = 10
        retry_policy = _RetryPolicy(_TokenBucket(capacity=capacity))
        self.assertEqual(retry_policy.attempts, helpers._MAX_RETRIES)
        self.assertEqual(retry_policy.backoff_initial, helpers._BACKOFF_INITIAL)
        self.assertEqual(retry_policy.backoff_max, helpers._BACKOFF_MAX)
        for i in range(1, helpers._MAX_RETRIES + 1):
            self.assertTrue(await retry_policy.should_retry(i, 0))
        self.assertFalse(await retry_policy.should_retry(helpers._MAX_RETRIES + 1, 0))
        for i in range(capacity - helpers._MAX_RETRIES):
            self.assertTrue(await retry_policy.should_retry(1, 0))
        # No tokens left, should not retry.
        self.assertFalse(await retry_policy.should_retry(1, 0))
        self.assertEqual(retry_policy.token_bucket.tokens, 0)

        # record_success should generate tokens.
        for _ in range(int(2 / helpers.DEFAULT_RETRY_TOKEN_RETURN)):
            await retry_policy.record_success(retry=False)
        self.assertAlmostEqual(retry_policy.token_bucket.tokens, 2)
        for i in range(2):
            self.assertTrue(await retry_policy.should_retry(1, 0))
        self.assertFalse(await retry_policy.should_retry(1, 0))

        # Recording a successful retry should return 1 additional token.
        await retry_policy.record_success(retry=True)
        self.assertAlmostEqual(
            retry_policy.token_bucket.tokens, 1 + helpers.DEFAULT_RETRY_TOKEN_RETURN
        )
        self.assertTrue(await retry_policy.should_retry(1, 0))
        self.assertFalse(await retry_policy.should_retry(1, 0))
        self.assertAlmostEqual(retry_policy.token_bucket.tokens, helpers.DEFAULT_RETRY_TOKEN_RETURN)

    async def test_retry_policy_csot(self):
        retry_policy = _RetryPolicy(_TokenBucket())
        self.assertTrue(await retry_policy.should_retry(1, 0.5))
        with pymongo.timeout(0.5):
            self.assertTrue(await retry_policy.should_retry(1, 0))
            self.assertTrue(await retry_policy.should_retry(1, 0.1))
            # Would exceed the timeout, should not retry.
            self.assertFalse(await retry_policy.should_retry(1, 1.0))
        self.assertTrue(await retry_policy.should_retry(1, 1.0))


if __name__ == "__main__":
    unittest.main()
