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

import sys

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest

from pymongo.asynchronous.helpers import _MAX_RETRIES
from pymongo.errors import PyMongoError

_IS_SYNC = False

# Mock an system overload error.
mock_overload_error = {
    "configureFailPoint": "failCommand",
    "mode": {"times": 1},
    "data": {
        "failCommands": ["find", "insert", "update"],
        "errorCode": 462,  # IngressRequestRateLimitExceeded
        "errorLabels": ["Retryable"],
    },
}


class TestBackpressure(AsyncIntegrationTest):
    RUN_ON_LOAD_BALANCER = True

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_command(self):
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_once = mock_overload_error.copy()
        fail_once["mode"] = {"times": _MAX_RETRIES}
        async with self.fail_point(fail_once):
            await self.db.command("find", "t")

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_many_times = mock_overload_error.copy()
        fail_many_times["mode"] = {"times": _MAX_RETRIES + 1}
        async with self.fail_point(fail_many_times):
            with self.assertRaises(PyMongoError) as error:
                await self.db.command("find", "t")

        self.assertIn("Retryable", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_find(self):
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_once = mock_overload_error.copy()
        fail_once["mode"] = {"times": _MAX_RETRIES}
        async with self.fail_point(fail_once):
            await self.db.t.find_one()

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_many_times = mock_overload_error.copy()
        fail_many_times["mode"] = {"times": _MAX_RETRIES + 1}
        async with self.fail_point(fail_many_times):
            with self.assertRaises(PyMongoError) as error:
                await self.db.t.find_one()

        self.assertIn("Retryable", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_insert_one(self):
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_once = mock_overload_error.copy()
        fail_once["mode"] = {"times": _MAX_RETRIES}
        async with self.fail_point(fail_once):
            await self.db.t.find_one()

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_many_times = mock_overload_error.copy()
        fail_many_times["mode"] = {"times": _MAX_RETRIES + 1}
        async with self.fail_point(fail_many_times):
            with self.assertRaises(PyMongoError) as error:
                await self.db.t.find_one()

        self.assertIn("Retryable", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_update_many(self):
        # Even though update_many is not a retryable write operation, it will
        # still be retried via the "Retryable" error label.
        await self.db.t.insert_one({"x": 1})

        # Ensure command is retried on overload error.
        fail_once = mock_overload_error.copy()
        fail_once["mode"] = {"times": _MAX_RETRIES}
        async with self.fail_point(fail_once):
            await self.db.t.update_many({}, {"$set": {"x": 2}})

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_many_times = mock_overload_error.copy()
        fail_many_times["mode"] = {"times": _MAX_RETRIES + 1}
        async with self.fail_point(fail_many_times):
            with self.assertRaises(PyMongoError) as error:
                await self.db.t.update_many({}, {"$set": {"x": 2}})

        self.assertIn("Retryable", str(error.exception))

    @async_client_context.require_failCommand_appName
    async def test_retry_overload_error_getMore(self):
        coll = self.db.t
        await coll.insert_many([{"x": 1} for _ in range(10)])

        # Ensure command is retried on overload error.
        fail_once = {
            "configureFailPoint": "failCommand",
            "mode": {"times": _MAX_RETRIES},
            "data": {
                "failCommands": ["getMore"],
                "errorCode": 462,  # IngressRequestRateLimitExceeded
                "errorLabels": ["Retryable"],
            },
        }
        cursor = coll.find(batch_size=2)
        await cursor.next()
        async with self.fail_point(fail_once):
            await cursor.to_list()

        # Ensure command stops retrying after _MAX_RETRIES.
        fail_many_times = fail_once.copy()
        fail_many_times["mode"] = {"times": _MAX_RETRIES + 1}
        cursor = coll.find(batch_size=2)
        await cursor.next()
        async with self.fail_point(fail_many_times):
            with self.assertRaises(PyMongoError) as error:
                await cursor.to_list()

        self.assertIn("Retryable", str(error.exception))


if __name__ == "__main__":
    unittest.main()
