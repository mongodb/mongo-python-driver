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

"""Test retryable reads spec."""
from __future__ import annotations

import os
import pprint
import sys
import threading
from test.asynchronous.utils import async_set_fail_point
from unittest import mock

from pymongo import MongoClient
from pymongo.common import MAX_ADAPTIVE_RETRIES
from pymongo.errors import OperationFailure, PyMongoError

sys.path[0:0] = [""]

from test.asynchronous import (
    AsyncIntegrationTest,
    AsyncPyMongoTestCase,
    async_client_context,
    client_knobs,
    unittest,
)
from test.utils_shared import (
    CMAPListener,
    OvertCommandListener,
)

from pymongo.monitoring import (
    CommandFailedEvent,
    ConnectionCheckedOutEvent,
    ConnectionCheckOutFailedEvent,
    ConnectionCheckOutFailedReason,
    PoolClearedEvent,
)

_IS_SYNC = False


class TestClientOptions(AsyncPyMongoTestCase):
    async def test_default(self):
        client = self.simple_client(connect=False)
        self.assertEqual(client.options.retry_reads, True)

    async def test_kwargs(self):
        client = self.simple_client(retryReads=True, connect=False)
        self.assertEqual(client.options.retry_reads, True)
        client = self.simple_client(retryReads=False, connect=False)
        self.assertEqual(client.options.retry_reads, False)

    async def test_uri(self):
        client = self.simple_client("mongodb://h/?retryReads=true", connect=False)
        self.assertEqual(client.options.retry_reads, True)
        client = self.simple_client("mongodb://h/?retryReads=false", connect=False)
        self.assertEqual(client.options.retry_reads, False)


class FindThread(threading.Thread):
    def __init__(self, collection):
        super().__init__()
        self.daemon = True
        self.collection = collection
        self.passed = False

    async def run(self):
        await self.collection.find_one({})
        self.passed = True


class TestPoolPausedError(AsyncIntegrationTest):
    # Pools don't get paused in load balanced mode.
    RUN_ON_LOAD_BALANCER = False

    @async_client_context.require_sync
    @async_client_context.require_failCommand_blockConnection
    @client_knobs(heartbeat_frequency=0.05, min_heartbeat_interval=0.05)
    async def test_pool_paused_error_is_retryable(self):
        if "PyPy" in sys.version:
            # Tracked in PYTHON-3519
            self.skipTest("Test is flaky on PyPy")
        cmap_listener = CMAPListener()
        cmd_listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(
            maxPoolSize=1, event_listeners=[cmap_listener, cmd_listener]
        )
        for _ in range(10):
            cmap_listener.reset()
            cmd_listener.reset()
            threads = [FindThread(client.pymongo_test.test) for _ in range(2)]
            fail_command = {
                "mode": {"times": 1},
                "data": {
                    "failCommands": ["find"],
                    "blockConnection": True,
                    "blockTimeMS": 1000,
                    "errorCode": 91,
                },
            }
            async with self.fail_point(fail_command):
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                for thread in threads:
                    self.assertTrue(thread.passed)

            # It's possible that SDAM can rediscover the server and mark the
            # pool ready before the thread in the wait queue has a chance
            # to run. Repeat the test until the thread actually encounters
            # a PoolClearedError.
            if cmap_listener.event_count(ConnectionCheckOutFailedEvent):
                break

        # Via CMAP monitoring, assert that the first check out succeeds.
        cmap_events = cmap_listener.events_by_type(
            (ConnectionCheckedOutEvent, ConnectionCheckOutFailedEvent, PoolClearedEvent)
        )
        msg = pprint.pformat(cmap_listener.events)
        self.assertIsInstance(cmap_events[0], ConnectionCheckedOutEvent, msg)
        self.assertIsInstance(cmap_events[1], PoolClearedEvent, msg)
        self.assertIsInstance(cmap_events[2], ConnectionCheckOutFailedEvent, msg)
        self.assertEqual(cmap_events[2].reason, ConnectionCheckOutFailedReason.CONN_ERROR, msg)
        self.assertIsInstance(cmap_events[3], ConnectionCheckedOutEvent, msg)

        # Connection check out failures are not reflected in command
        # monitoring because we only publish command events _after_ checking
        # out a connection.
        started = cmd_listener.started_events
        msg = pprint.pformat(cmd_listener.results)
        self.assertEqual(3, len(started), msg)
        succeeded = cmd_listener.succeeded_events
        self.assertEqual(2, len(succeeded), msg)
        failed = cmd_listener.failed_events
        self.assertEqual(1, len(failed), msg)


class TestRetryableReads(AsyncIntegrationTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.setup_client = MongoClient(**async_client_context.client_options)
        self.addCleanup(self.setup_client.close)

    # TODO: After PYTHON-4595 we can use async event handlers and remove this workaround.
    def configure_fail_point_sync(self, command_args, off=False) -> None:
        cmd = {"configureFailPoint": "failCommand"}
        cmd.update(command_args)
        if off:
            cmd["mode"] = "off"
            cmd.pop("data", None)
        self.setup_client.admin.command(cmd)

    @async_client_context.require_multiple_mongoses
    @async_client_context.require_failCommand_fail_point
    async def test_retryable_reads_are_retried_on_a_different_mongos_when_one_is_available(self):
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {"failCommands": ["find"], "errorCode": 6},
        }

        mongos_clients = []

        for mongos in async_client_context.mongos_seeds().split(","):
            client = await self.async_rs_or_single_client(mongos)
            await async_set_fail_point(client, fail_command)
            mongos_clients.append(client)

        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(
            async_client_context.mongos_seeds(),
            event_listeners=[listener],
            retryReads=True,
        )

        with self.assertRaises(OperationFailure):
            await client.t.t.find_one({})

        # Disable failpoints on each mongos
        for client in mongos_clients:
            fail_command["mode"] = "off"
            await async_set_fail_point(client, fail_command)

        self.assertEqual(len(listener.failed_events), 2)
        self.assertEqual(len(listener.succeeded_events), 0)

        #  Assert that both events occurred on different mongos.
        assert listener.failed_events[0].connection_id != listener.failed_events[1].connection_id

    @async_client_context.require_multiple_mongoses
    @async_client_context.require_failCommand_fail_point
    async def test_retryable_reads_are_retried_on_the_same_mongos_when_no_others_are_available(
        self
    ):
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {"failCommands": ["find"], "errorCode": 6},
        }

        host = async_client_context.mongos_seeds().split(",")[0]
        mongos_client = await self.async_rs_or_single_client(host)
        await async_set_fail_point(mongos_client, fail_command)

        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(
            host,
            directConnection=False,
            event_listeners=[listener],
            retryReads=True,
        )

        await client.t.t.find_one({})

        # Disable failpoint.
        fail_command["mode"] = "off"
        await async_set_fail_point(mongos_client, fail_command)

        # Assert that exactly one failed command event and one succeeded command event occurred.
        self.assertEqual(len(listener.failed_events), 1)
        self.assertEqual(len(listener.succeeded_events), 1)

        #  Assert that both events occurred on the same mongos.
        assert listener.succeeded_events[0].connection_id == listener.failed_events[0].connection_id

    @async_client_context.require_failCommand_fail_point
    async def test_retryable_reads_are_retried_on_the_same_implicit_session(self):
        listener = OvertCommandListener()
        client = await self.async_rs_or_single_client(
            directConnection=False,
            event_listeners=[listener],
            retryReads=True,
        )

        await client.t.t.insert_one({"x": 1})

        commands = [
            ("aggregate", lambda: client.t.t.count_documents({})),
            ("aggregate", lambda: client.t.t.aggregate([{"$match": {}}])),
            ("count", lambda: client.t.t.estimated_document_count()),
            ("distinct", lambda: client.t.t.distinct("x")),
            ("find", lambda: client.t.t.find_one({})),
            ("listDatabases", lambda: client.list_databases()),
            ("listCollections", lambda: client.t.list_collections()),
            ("listIndexes", lambda: client.t.t.list_indexes()),
        ]

        for command_name, operation in commands:
            listener.reset()
            fail_command = {
                "configureFailPoint": "failCommand",
                "mode": {"times": 1},
                "data": {"failCommands": [command_name], "errorCode": 6},
            }

            async with self.fail_point(fail_command):
                await operation()

            #  Assert that both events occurred on the same session.
            command_docs = [
                event.command
                for event in listener.started_events
                if event.command_name == command_name
            ]
            self.assertEqual(len(command_docs), 2)
            self.assertEqual(command_docs[0]["lsid"], command_docs[1]["lsid"])
            self.assertIsNot(command_docs[0], command_docs[1])

    @async_client_context.require_replica_set
    @async_client_context.require_secondaries_count(1)
    @async_client_context.require_failCommand_fail_point
    @async_client_context.require_version_min(4, 4, 0)
    async def test_03_01_retryable_reads_caused_by_overload_errors_are_retried_on_a_different_replicaset_server_when_one_is_available_and_overload_retargeting_is_enabled(
        self
    ):
        listener = OvertCommandListener()

        # 1. Create a client `client` with `retryReads=true`, `readPreference=primaryPreferred`, `enableOverloadRetargeting=True`, and command event monitoring enabled.
        client = await self.async_rs_or_single_client(
            event_listeners=[listener],
            retryReads=True,
            readPreference="primaryPreferred",
            enableOverloadRetargeting=True,
        )

        # 2. Configure a fail point with the RetryableError and SystemOverloadedError error labels.
        command_args = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {
                "failCommands": ["find"],
                "errorLabels": ["RetryableError", "SystemOverloadedError"],
                "errorCode": 6,
            },
        }
        await async_set_fail_point(client, command_args)

        # 3. Reset the command event monitor to clear the fail point command from its stored events.
        listener.reset()

        # 4. Execute a `find` command with `client`.
        await client.t.t.find_one({})

        # 5. Assert that one failed command event and one successful command event occurred.
        self.assertEqual(len(listener.failed_events), 1)
        self.assertEqual(len(listener.succeeded_events), 1)

        # 6. Assert that both events occurred on different servers.
        assert listener.failed_events[0].connection_id != listener.succeeded_events[0].connection_id

    @async_client_context.require_replica_set
    @async_client_context.require_secondaries_count(1)
    @async_client_context.require_failCommand_fail_point
    @async_client_context.require_version_min(4, 4, 0)
    async def test_03_02_retryable_reads_caused_by_non_overload_errors_are_retried_on_the_same_replicaset_server(
        self
    ):
        listener = OvertCommandListener()

        # 1. Create a client `client` with `retryReads=true`, `readPreference=primaryPreferred`, and command event monitoring enabled.
        client = await self.async_rs_or_single_client(
            event_listeners=[listener], retryReads=True, readPreference="primaryPreferred"
        )

        # 2. Configure a fail point with the RetryableError error label.
        command_args = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {
                "failCommands": ["find"],
                "errorLabels": ["RetryableError"],
                "errorCode": 6,
            },
        }
        await async_set_fail_point(client, command_args)

        # 3. Reset the command event monitor to clear the fail point command from its stored events.
        listener.reset()

        # 4. Execute a `find` command with `client`.
        await client.t.t.find_one({})

        # 5. Assert that one failed command event and one successful command event occurred.
        self.assertEqual(len(listener.failed_events), 1)
        self.assertEqual(len(listener.succeeded_events), 1)

        # 6. Assert that both events occurred the same server.
        assert listener.failed_events[0].connection_id == listener.succeeded_events[0].connection_id

    @async_client_context.require_replica_set
    @async_client_context.require_secondaries_count(1)
    @async_client_context.require_failCommand_fail_point
    @async_client_context.require_version_min(4, 4, 0)
    async def test_03_03_retryable_reads_caused_by_overload_errors_are_retried_on_the_same_replicaset_server_when_one_is_available_and_overload_retargeting_is_disabled(
        self
    ):
        listener = OvertCommandListener()

        # 1. Create a client `client` with `retryReads=true`, `readPreference=primaryPreferred`, and command event monitoring enabled.
        client = await self.async_rs_or_single_client(
            event_listeners=[listener],
            retryReads=True,
            readPreference="primaryPreferred",
        )

        # 2. Configure a fail point with the RetryableError and SystemOverloadedError error labels.
        command_args = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {
                "failCommands": ["find"],
                "errorLabels": ["RetryableError", "SystemOverloadedError"],
                "errorCode": 6,
            },
        }
        await async_set_fail_point(client, command_args)

        # 3. Reset the command event monitor to clear the fail point command from its stored events.
        listener.reset()

        # 4. Execute a `find` command with `client`.
        await client.t.t.find_one({})

        # 5. Assert that one failed command event and one successful command event occurred.
        self.assertEqual(len(listener.failed_events), 1)
        self.assertEqual(len(listener.succeeded_events), 1)

        # 6. Assert that both events occurred on the same server.
        assert listener.failed_events[0].connection_id == listener.succeeded_events[0].connection_id

    @async_client_context.require_failCommand_fail_point
    @async_client_context.require_version_min(4, 4, 0)  # type:ignore[untyped-decorator]
    async def test_overload_then_nonoverload_retries_increased_reads(self) -> None:
        # Create a client.
        listener = OvertCommandListener()

        # Configure the client to listen to CommandFailedEvents. In the attached listener, configure a fail point with error
        # code `91` (ShutdownInProgress) and `RetryableError` and `SystemOverloadedError` labels.
        overload_fail_point = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {
                "failCommands": ["find"],
                "errorLabels": ["RetryableError", "SystemOverloadedError"],
                "errorCode": 91,
            },
        }

        # Configure a fail point with error code `91` (ShutdownInProgress) with only the `RetryableError` error label.
        non_overload_fail_point = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {
                "failCommands": ["find"],
                "errorCode": 91,
                "errorLabels": ["RetryableError"],
            },
        }

        def failed(event: CommandFailedEvent) -> None:
            # Configure the fail point command only if the failed event is for the 91 error configured in step 2.
            if listener.failed_events:
                return
            assert event.failure["code"] == 91
            self.configure_fail_point_sync(non_overload_fail_point)
            self.addCleanup(self.configure_fail_point_sync, {}, off=True)
            listener.failed_events.append(event)

        listener.failed = failed

        client = await self.async_rs_client(event_listeners=[listener])
        await client.test.test.insert_one({})

        self.configure_fail_point_sync(overload_fail_point)
        self.addCleanup(self.configure_fail_point_sync, {}, off=True)

        with self.assertRaises(PyMongoError):
            await client.test.test.find_one()

        started_finds = [e for e in listener.started_events if e.command_name == "find"]
        self.assertEqual(len(started_finds), MAX_ADAPTIVE_RETRIES + 1)

    @async_client_context.require_failCommand_fail_point
    @async_client_context.require_version_min(4, 4, 0)  # type:ignore[untyped-decorator]
    async def test_backoff_is_not_applied_for_non_overload_errors(self):
        if _IS_SYNC:
            mock_target = "pymongo.synchronous.helpers._RetryPolicy.backoff"
        else:
            mock_target = "pymongo.asynchronous.helpers._RetryPolicy.backoff"

        # Create a client.
        listener = OvertCommandListener()

        # Configure the client to listen to CommandFailedEvents. In the attached listener, configure a fail point with error
        # code `91` (ShutdownInProgress) and `RetryableError` and `SystemOverloadedError` labels.
        overload_fail_point = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {
                "failCommands": ["find"],
                "errorLabels": ["RetryableError", "SystemOverloadedError"],
                "errorCode": 91,
            },
        }

        # Configure a fail point with error code `91` (ShutdownInProgress) with only the `RetryableError` error label.
        non_overload_fail_point = {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {
                "failCommands": ["find"],
                "errorCode": 91,
                "errorLabels": ["RetryableError"],
            },
        }

        def failed(event: CommandFailedEvent) -> None:
            # Configure the fail point command only if the failed event is for the 91 error configured in step 2.
            if listener.failed_events:
                return
            assert event.failure["code"] == 91
            self.configure_fail_point_sync(non_overload_fail_point)
            self.addCleanup(self.configure_fail_point_sync, {}, off=True)
            listener.failed_events.append(event)

        listener.failed = failed

        client = await self.async_rs_client(event_listeners=[listener])
        await client.test.test.insert_one({})

        self.configure_fail_point_sync(overload_fail_point)
        self.addCleanup(self.configure_fail_point_sync, {}, off=True)

        # Perform a findOne operation with coll. Expect the operation to fail.
        with mock.patch(mock_target, return_value=0) as mock_backoff:
            with self.assertRaises(PyMongoError):
                await client.test.test.find_one()

        # Assert that backoff was applied only once for the initial overload error and not for the subsequent non-overload retryable errors.
        self.assertEqual(mock_backoff.call_count, 1)


if __name__ == "__main__":
    unittest.main()
