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
from pymongo import _op_id
from pymongo.errors import OperationFailure
from pymongo.helpers_shared import _REAUTHENTICATION_REQUIRED_CODE
from pymongo.operations import InsertOne
from pymongo.synchronous.helpers import _handle_reauth
from pymongo.synchronous.pool import Connection
from test import IntegrationTest, client_context, unittest
from test.utils_shared import AllowListEventListener

_IS_SYNC = True

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


def _agg(coll):
    cursor = coll.aggregate([{"$match": {"x": 1}}])
    return cursor.to_list()


def _list_indexes(coll):
    cursor = coll.list_indexes()
    return cursor.to_list()


class TestOperationIdRetry(IntegrationTest):
    RETRIES = 2

    @client_context.require_failCommand_fail_point
    def setUp(self) -> None:
        super().setUp()
        self.listener = AllowListEventListener(
            *{name for name, _ in _RETRYABLE_WRITES + _RETRYABLE_READS}
        )
        self.client = self.rs_or_single_client(event_listeners=[self.listener], appname=_APP_NAME)
        self.coll = self.client.pymongo_test.test_operation_id_retry
        self.coll.drop()
        self.coll.insert_many([{"_id": i, "x": i % 3} for i in range(5)])
        self.coll.create_index("x")

    def _run_under_failpoint(self, name, f, times):
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
        with self.fail_point(fail_point):
            # A CSOT timeout lets a single operation retry more than once.
            with pymongo.timeout(60):
                f(self.coll)

        def of(events):
            return [e for e in events if e.command_name == name]

        return (
            of(self.listener.started_events),
            of(self.listener.failed_events),
            of(self.listener.succeeded_events),
        )

    def _check_stable_operation_id(self, name, f, retries):
        """Assert every command event for ``name`` shares one integer operation_id."""
        started, failed, succeeded = self._run_under_failpoint(name, f, retries)
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

    @client_context.require_no_standalone
    def test_retryable_writes_reuse_operation_id(self):
        for name, f in _RETRYABLE_WRITES:
            with self.subTest(command=name):
                self._check_stable_operation_id(name, f, self.RETRIES)

    def test_retryable_reads_reuse_operation_id(self):
        for name, f in _RETRYABLE_READS:
            with self.subTest(command=name):
                self._check_stable_operation_id(name, f, self.RETRIES)

    def test_reauth_does_not_reuse_operation_id(self):
        class FakeConnection(Connection):
            def __init__(self):
                self.auth_op_ids = []

            def authenticate(self, reauthenticate=False):
                self.auth_op_ids.append(_op_id.OP_ID.get())

        conn = FakeConnection()
        attempt_op_ids = []

        @_handle_reauth
        def func(conn):
            attempt_op_ids.append(_op_id.OP_ID.get())
            if len(attempt_op_ids) == 1:
                raise OperationFailure("reauth required", _REAUTHENTICATION_REQUIRED_CODE)

        op_id = 42
        with _op_id._OpIdContext(op_id):
            func(conn)
        # Reauth's auth commands must not inherit the in-flight op's id.
        self.assertEqual(conn.auth_op_ids, [None])
        # The op's id is restored for the retried command after reauth.
        self.assertEqual(attempt_op_ids, [op_id, op_id])
        self.assertIsNone(_op_id.OP_ID.get())


if __name__ == "__main__":
    unittest.main()
