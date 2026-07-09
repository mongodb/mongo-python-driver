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

"""Test that retry attempts reuse a single stable CommandStartedEvent.operation_id."""

from __future__ import annotations

import sys

sys.path[0:0] = [""]

import pymongo
from pymongo.errors import ConnectionFailure
from pymongo.operations import InsertOne
from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest
from test.utils_shared import AllowListEventListener

_IS_SYNC = False

_APP_NAME = "operationIdRetryTest"

_RETRYABLE_WRITES = [
    ("insert", lambda c: c.insert_one({"_id": 100})),
    ("update", lambda c: c.update_one({"_id": 1}, {"$set": {"y": 1}})),
    ("update", lambda c: c.replace_one({"_id": 2}, {"x": 9})),
    ("delete", lambda c: c.delete_one({"_id": 3})),
    ("findAndModify", lambda c: c.find_one_and_update({"_id": 4}, {"$set": {"y": 2}})),
    ("insert", lambda c: c.bulk_write([InsertOne({"_id": 200}), InsertOne({"_id": 201})])),
]


_RETRYABLE_READS = [
    ("find", lambda c: c.find({"x": 1}).to_list()),
    ("find", lambda c: c.find_one({"_id": 1})),
    ("aggregate", lambda c: _agg(c)),
    ("aggregate", lambda c: c.count_documents({"x": 1})),
    ("distinct", lambda c: c.distinct("x")),
    ("listIndexes", lambda c: _list_indexes(c)),
]

_COMMANDS = {name for name, _ in _RETRYABLE_WRITES + _RETRYABLE_READS}


async def _agg(coll):
    cursor = await coll.aggregate([{"$match": {"x": 1}}])
    return await cursor.to_list()


async def _list_indexes(coll):
    cursor = await coll.list_indexes()
    return await cursor.to_list()


class TestOperationIdRetry(AsyncIntegrationTest):
    RETRIES = 2  # fail this many attempts; the (RETRIES + 1)th succeeds.

    @async_client_context.require_failCommand_fail_point
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.listener = AllowListEventListener(*_COMMANDS)
        self.client = await self.async_rs_or_single_client(
            event_listeners=[self.listener], appname=_APP_NAME
        )
        self.coll = self.client.pymongo_test.test_operation_id_retry
        await self.coll.drop()
        await self.coll.insert_many([{"_id": i, "x": i % 3} for i in range(5)])
        await self.coll.create_index("x")

    async def _run_under_failpoint(self, command_name, action, times, expected_error=None):
        """Force ``times`` closeConnection failures of ``command_name``, run
        ``action``, and return its ``(started, failed, succeeded)`` events."""
        self.listener.reset()
        fail_point = {
            "mode": {"times": times},
            "data": {
                "failCommands": [command_name],
                "closeConnection": True,
                "appName": _APP_NAME,
            },
        }
        async with self.fail_point(fail_point):
            # A CSOT timeout lets a single operation retry more than once.
            with pymongo.timeout(60):
                if expected_error is not None:
                    with self.assertRaises(expected_error):
                        await action(self.coll)
                else:
                    await action(self.coll)

        def of(events):
            return [e for e in events if e.command_name == command_name]

        return (
            of(self.listener.started_events),
            of(self.listener.failed_events),
            of(self.listener.succeeded_events),
        )

    async def _check_stable_operation_id(self, command_name, action, retries):
        """Assert every command event for ``command_name`` shares one integer operation_id."""
        started, failed, succeeded = await self._run_under_failpoint(command_name, action, retries)
        op_ids = [e.operation_id for e in started + failed + succeeded]

        self.assertEqual(len(started), retries + 1, "expected one started event per attempt")
        self.assertEqual(len(failed), retries)
        self.assertEqual(len(succeeded), 1)
        self.assertTrue(all(isinstance(op, int) for op in op_ids))
        self.assertEqual(
            len(set(op_ids)),
            1,
            f"operation_id not stable across retries for {command_name}: {op_ids}",
        )

    @async_client_context.require_no_standalone
    async def test_retryable_writes_reuse_operation_id(self):
        for command_name, action in _RETRYABLE_WRITES:
            with self.subTest(command=command_name):
                await self._check_stable_operation_id(command_name, action, self.RETRIES)

    async def test_retryable_reads_reuse_operation_id(self):
        for command_name, action in _RETRYABLE_READS:
            with self.subTest(command=command_name):
                await self._check_stable_operation_id(command_name, action, self.RETRIES)

    @async_client_context.require_no_standalone
    async def test_non_retryable_write_is_not_retried(self):
        # Multi-document writes are not retryable: a single network error must
        # surface immediately, with exactly one attempt.
        for command_name, action in [
            ("update", lambda c: c.update_many({"x": 1}, {"$set": {"z": 1}})),
            ("delete", lambda c: c.delete_many({"x": 2})),
        ]:
            with self.subTest(command=command_name):
                started, _, _ = await self._run_under_failpoint(
                    command_name, action, times=1, expected_error=ConnectionFailure
                )
                self.assertEqual(len(started), 1, "must not retry")
                self.assertIsInstance(started[0].operation_id, int)


if __name__ == "__main__":
    unittest.main()
