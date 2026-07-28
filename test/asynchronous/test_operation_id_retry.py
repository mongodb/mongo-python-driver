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

import logging
import sys
from unittest.mock import patch

sys.path[0:0] = [""]

import pymongo
from bson.codec_options import DEFAULT_CODEC_OPTIONS
from pymongo import _op_id
from pymongo._telemetry import _CommandTelemetry
from pymongo.asynchronous import mongo_client
from pymongo.asynchronous.encryption import _Encrypter
from pymongo.asynchronous.helpers import _handle_reauth
from pymongo.asynchronous.pool import AsyncConnection
from pymongo.errors import OperationFailure
from pymongo.helpers_shared import _REAUTHENTICATION_REQUIRED_CODE
from pymongo.logger import _COMMAND_LOGGER, _SERVER_SELECTION_LOGGER
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


async def _agg(coll):
    cursor = await coll.aggregate([{"$match": {"x": 1}}])
    return await cursor.to_list()


async def _list_indexes(coll):
    cursor = await coll.list_indexes()
    return await cursor.to_list()


class TestOperationIdRetry(AsyncIntegrationTest):
    RETRIES = 2

    @async_client_context.require_failCommand_fail_point
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.listener = AllowListEventListener(
            *{name for name, _ in _RETRYABLE_WRITES + _RETRYABLE_READS}
        )
        self.client = await self.async_rs_or_single_client(
            event_listeners=[self.listener], appname=_APP_NAME
        )
        self.coll = self.client.pymongo_test.test_operation_id_retry
        await self.coll.drop()
        await self.coll.insert_many([{"_id": i, "x": i % 3} for i in range(5)])
        await self.coll.create_index("x")

    async def _run_under_failpoint(self, name, f, times):
        """Set a failpoint for the given command and return the corresponding events published during its execution."""
        self.listener.reset()
        fail_point = {
            "mode": {"times": times},
            "data": {
                "failCommands": [name],
                "closeConnection": True,
                "appName": _APP_NAME,
            },
        }
        async with self.fail_point(fail_point):
            # A CSOT timeout lets a single operation retry more than once.
            with pymongo.timeout(60):
                await f(self.coll)

        def matching(events):
            return [e for e in events if e.command_name == name]

        return (
            matching(self.listener.started_events),
            matching(self.listener.failed_events),
            matching(self.listener.succeeded_events),
        )

    async def _check_stable_operation_id(self, name, f, retries):
        """Assert every command event for ``name`` shares one integer operation_id."""
        started, failed, succeeded = await self._run_under_failpoint(name, f, retries)
        op_ids = [e.operation_id for e in started + failed + succeeded]

        self.assertEqual(len(started), retries + 1, "expected one started event per attempt")
        self.assertEqual(len(failed), retries)
        self.assertEqual(len(succeeded), 1)
        self.assertTrue(all(isinstance(op, int) for op in op_ids))
        self.assertEqual(
            len(set(op_ids)),
            1,
            f"operation_id not stable across retries for {name}: {op_ids}",
        )

    @async_client_context.require_no_standalone
    async def test_retryable_writes_reuse_operation_id(self):
        for i, (name, f) in enumerate(_RETRYABLE_WRITES):
            with self.subTest(command=name, index=i):
                await self._check_stable_operation_id(name, f, self.RETRIES)

    async def test_retryable_reads_reuse_operation_id(self):
        for i, (name, f) in enumerate(_RETRYABLE_READS):
            with self.subTest(command=name, index=i):
                await self._check_stable_operation_id(name, f, self.RETRIES)

    async def test_retry_without_listeners_or_logging_creates_no_operation_id(self):
        appname = _APP_NAME + "noapm"
        client = await self.async_rs_or_single_client(appname=appname)

        # Make sure APM and logging are disabled
        for logger in (_COMMAND_LOGGER, _SERVER_SELECTION_LOGGER):
            self.assertFalse(logger.isEnabledFor(logging.DEBUG))
        self.assertFalse(client._event_listeners.enabled_for_commands)

        find_op_ids = []
        original_init = _CommandTelemetry.__init__

        def recording_init(self, topology_id, conn, listeners, cmd, dbname, request_id, op_id):
            if next(iter(cmd)) == "find":
                find_op_ids.append(op_id)
            original_init(self, topology_id, conn, listeners, cmd, dbname, request_id, op_id)

        fail_point = {
            "mode": {"times": 1},
            "data": {
                "failCommands": ["find"],
                "closeConnection": True,
                "appName": appname,
            },
        }
        async with self.fail_point(fail_point):
            with (
                patch.object(mongo_client, "_randint") as randint,
                patch.object(_CommandTelemetry, "__init__", recording_init),
            ):
                self.assertIsNotNone(
                    await client.pymongo_test.test_operation_id_retry.find_one({"_id": 1})
                )

        self.assertEqual(
            randint.call_count, 0, "generated an operation id without APM/logging enabled"
        )
        self.assertEqual(
            find_op_ids,
            [None, None],
            "expected two attempts, neither carrying a shared operation id",
        )

    async def test_reauth_does_not_reuse_operation_id(self):
        class FakeConnection(AsyncConnection):
            def __init__(self):
                self.auth_op_ids = []

            async def authenticate(self, reauthenticate=False):
                self.auth_op_ids.append(_op_id.OP_ID.get())

        conn = FakeConnection()
        attempt_op_ids = []

        @_handle_reauth
        async def func(conn):
            attempt_op_ids.append(_op_id.OP_ID.get())
            if len(attempt_op_ids) == 1:
                raise OperationFailure("reauth required", _REAUTHENTICATION_REQUIRED_CODE)

        op_id = 42
        with _op_id._OpIdContext(op_id):
            await func(conn)
        # Reauth's auth commands must not inherit the in-flight op's id.
        self.assertEqual(conn.auth_op_ids, [None])
        # The op's id is restored for the retried command after reauth.
        self.assertEqual(attempt_op_ids, [op_id, op_id])
        self.assertIsNone(_op_id.OP_ID.get())

    async def test_auto_encryption_does_not_reuse_operation_id(self):
        class FakeAutoEncrypter:
            def __init__(self):
                self.op_ids = []

            async def encrypt(self, database, cmd):
                self.op_ids.append(_op_id.OP_ID.get())
                return cmd

            async def decrypt(self, response):
                self.op_ids.append(_op_id.OP_ID.get())
                return response

        encrypter = _Encrypter.__new__(_Encrypter)
        encrypter._closed = False
        encrypter._auto_encrypter = FakeAutoEncrypter()

        op_id = 42
        with _op_id._OpIdContext(op_id):
            await encrypter.encrypt("db", {"find": "test"}, DEFAULT_CODEC_OPTIONS)
            await encrypter.decrypt(b"")
            # The op's id is restored for the operation's own command.
            self.assertEqual(_op_id.OP_ID.get(), op_id)
        # Encryption's sub-commands must not inherit the in-flight op's id.
        self.assertEqual(encrypter._auto_encrypter.op_ids, [None, None])


if __name__ == "__main__":
    unittest.main()
