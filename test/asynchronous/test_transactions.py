# Copyright 2018-present MongoDB, Inc.
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

"""Execute Transactions Spec tests."""
from __future__ import annotations

import asyncio
import sys
from io import BytesIO

from gridfs.asynchronous.grid_file import AsyncGridFS, AsyncGridFSBucket
from pymongo.asynchronous.pool import PoolState
from pymongo.server_selectors import writable_server_selector

sys.path[0:0] = [""]

from test.asynchronous import AsyncIntegrationTest, async_client_context, unittest
from test.utils_shared import (
    OvertCommandListener,
    async_wait_until,
)
from typing import List

from bson import encode
from bson.raw_bson import RawBSONDocument
from pymongo import WriteConcern, _csot
from pymongo.asynchronous import client_session
from pymongo.asynchronous.client_session import TransactionOptions
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.cursor import AsyncCursor
from pymongo.asynchronous.helpers import anext
from pymongo.errors import (
    AutoReconnect,
    CollectionInvalid,
    ConfigurationError,
    ConnectionFailure,
    InvalidOperation,
    OperationFailure,
)
from pymongo.operations import IndexModel, InsertOne
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference

_IS_SYNC = False

# Max number of operations to perform after a transaction to prove unpinning
# occurs. Chosen so that there's a low false positive rate. With 2 mongoses,
# 50 attempts yields a one in a quadrillion chance of a false positive
# (1/(0.5^50)).
UNPIN_TEST_MAX_ATTEMPTS = 50


class AsyncTransactionsBase(AsyncIntegrationTest):
    pass


class TestTransactions(AsyncTransactionsBase):
    @async_client_context.require_transactions
    def test_transaction_options_validation(self):
        default_options = TransactionOptions()
        self.assertIsNone(default_options.read_concern)
        self.assertIsNone(default_options.write_concern)
        self.assertIsNone(default_options.read_preference)
        self.assertIsNone(default_options.max_commit_time_ms)
        # No error when valid options are provided.
        TransactionOptions(
            read_concern=ReadConcern(),
            write_concern=WriteConcern(),
            read_preference=ReadPreference.PRIMARY,
            max_commit_time_ms=10000,
        )
        with self.assertRaisesRegex(TypeError, "read_concern must be "):
            TransactionOptions(read_concern={})  # type: ignore
        with self.assertRaisesRegex(TypeError, "write_concern must be "):
            TransactionOptions(write_concern={})  # type: ignore
        with self.assertRaisesRegex(
            ConfigurationError, "transactions do not support unacknowledged write concern"
        ):
            TransactionOptions(write_concern=WriteConcern(w=0))
        with self.assertRaisesRegex(TypeError, "is not valid for read_preference"):
            TransactionOptions(read_preference={})  # type: ignore
        with self.assertRaisesRegex(TypeError, "max_commit_time_ms must be an integer or None"):
            TransactionOptions(max_commit_time_ms="10000")  # type: ignore

    @async_client_context.require_transactions
    async def test_transaction_write_concern_override(self):
        """Test txn overrides Client/Database/Collection write_concern."""
        client = await self.async_rs_client(w=0)
        db = client.test
        coll = db.test
        await coll.insert_one({})
        async with client.start_session() as s:
            async with await s.start_transaction(write_concern=WriteConcern(w=1)):
                self.assertTrue((await coll.insert_one({}, session=s)).acknowledged)
                self.assertTrue((await coll.insert_many([{}, {}], session=s)).acknowledged)
                self.assertTrue((await coll.bulk_write([InsertOne({})], session=s)).acknowledged)
                self.assertTrue((await coll.replace_one({}, {}, session=s)).acknowledged)
                self.assertTrue(
                    (await coll.update_one({}, {"$set": {"a": 1}}, session=s)).acknowledged
                )
                self.assertTrue(
                    (await coll.update_many({}, {"$set": {"a": 1}}, session=s)).acknowledged
                )
                self.assertTrue((await coll.delete_one({}, session=s)).acknowledged)
                self.assertTrue((await coll.delete_many({}, session=s)).acknowledged)
                await coll.find_one_and_delete({}, session=s)
                await coll.find_one_and_replace({}, {}, session=s)
                await coll.find_one_and_update({}, {"$set": {"a": 1}}, session=s)

        unsupported_txn_writes: list = [
            (client.drop_database, [db.name], {}),
            (db.drop_collection, ["collection"], {}),
            (coll.drop, [], {}),
            (coll.rename, ["collection2"], {}),
            # Drop collection2 between tests of "rename", above.
            (coll.database.drop_collection, ["collection2"], {}),
            (coll.create_indexes, [[IndexModel("a")]], {}),
            (coll.create_index, ["a"], {}),
            (coll.drop_index, ["a_1"], {}),
            (coll.drop_indexes, [], {}),
            (coll.aggregate, [[{"$out": "aggout"}]], {}),
        ]
        # Creating a collection in a transaction requires MongoDB 4.4+.
        if async_client_context.version < (4, 3, 4):
            unsupported_txn_writes.extend(
                [
                    (db.create_collection, ["collection"], {}),
                ]
            )

        for op in unsupported_txn_writes:
            op, args, kwargs = op
            async with client.start_session() as s:
                kwargs["session"] = s
                await s.start_transaction(write_concern=WriteConcern(w=1))
                with self.assertRaises(OperationFailure):
                    await op(*args, **kwargs)
                await s.abort_transaction()

    @async_client_context.require_transactions
    @async_client_context.require_multiple_mongoses
    async def test_unpin_for_next_transaction(self):
        # Increase localThresholdMS and wait until both nodes are discovered
        # to avoid false positives.
        client = await self.async_rs_client(
            async_client_context.mongos_seeds(), localThresholdMS=1000
        )
        await async_wait_until(lambda: len(client.nodes) > 1, "discover both mongoses")
        coll = client.test.test
        # Create the collection.
        await coll.insert_one({})
        async with client.start_session() as s:
            # Session is pinned to Mongos.
            async with await s.start_transaction():
                await coll.insert_one({}, session=s)

            addresses = set()
            for _ in range(UNPIN_TEST_MAX_ATTEMPTS):
                async with await s.start_transaction():
                    cursor = coll.find({}, session=s)
                    self.assertTrue(await anext(cursor))
                    addresses.add(cursor.address)
                # Break early if we can.
                if len(addresses) > 1:
                    break

            self.assertGreater(len(addresses), 1)

    @async_client_context.require_transactions
    @async_client_context.require_multiple_mongoses
    async def test_unpin_for_non_transaction_operation(self):
        # Increase localThresholdMS and wait until both nodes are discovered
        # to avoid false positives.
        client = await self.async_rs_client(
            async_client_context.mongos_seeds(), localThresholdMS=1000
        )
        await async_wait_until(lambda: len(client.nodes) > 1, "discover both mongoses")
        coll = client.test.test
        # Create the collection.
        await coll.insert_one({})
        async with client.start_session() as s:
            # Session is pinned to Mongos.
            async with await s.start_transaction():
                await coll.insert_one({}, session=s)

            addresses = set()
            for _ in range(UNPIN_TEST_MAX_ATTEMPTS):
                cursor = coll.find({}, session=s)
                self.assertTrue(await anext(cursor))
                addresses.add(cursor.address)
                # Break early if we can.
                if len(addresses) > 1:
                    break

            self.assertGreater(len(addresses), 1)

    @async_client_context.require_transactions
    @async_client_context.require_version_min(4, 3, 4)
    async def test_create_collection(self):
        client = async_client_context.client
        db = client.pymongo_test
        coll = db.test_create_collection
        self.addAsyncCleanup(coll.drop)

        # Use with_transaction to avoid StaleConfig errors on sharded clusters.
        async def create_and_insert(session):
            coll2 = await db.create_collection(coll.name, session=session)
            self.assertEqual(coll, coll2)
            await coll.insert_one({}, session=session)

        async with client.start_session() as s:
            await s.with_transaction(create_and_insert)

        # Outside a transaction we raise CollectionInvalid on existing colls.
        with self.assertRaises(CollectionInvalid):
            await db.create_collection(coll.name)

        # Inside a transaction we raise the OperationFailure from create.
        async with client.start_session() as s:
            await s.start_transaction()
            with self.assertRaises(OperationFailure) as ctx:
                await db.create_collection(coll.name, session=s)
            self.assertEqual(ctx.exception.code, 48)  # NamespaceExists

    @async_client_context.require_transactions
    async def test_gridfs_does_not_support_transactions(self):
        client = async_client_context.client
        db = client.pymongo_test
        gfs = AsyncGridFS(db)
        bucket = AsyncGridFSBucket(db)

        async def gridfs_find(*args, **kwargs):
            return await gfs.find(*args, **kwargs).next()

        async def gridfs_open_upload_stream(*args, **kwargs):
            await (await bucket.open_upload_stream(*args, **kwargs)).write(b"1")

        gridfs_ops = [
            (gfs.put, (b"123",)),
            (gfs.get, (1,)),
            (gfs.get_version, ("name",)),
            (gfs.get_last_version, ("name",)),
            (gfs.delete, (1,)),
            (gfs.list, ()),
            (gfs.find_one, ()),
            (gridfs_find, ()),
            (gfs.exists, ()),
            (gridfs_open_upload_stream, ("name",)),
            (
                bucket.upload_from_stream,
                (
                    "name",
                    b"data",
                ),
            ),
            (
                bucket.download_to_stream,
                (
                    1,
                    BytesIO(),
                ),
            ),
            (
                bucket.download_to_stream_by_name,
                (
                    "name",
                    BytesIO(),
                ),
            ),
            (bucket.delete, (1,)),
            (bucket.find, ()),
            (bucket.open_download_stream, (1,)),
            (bucket.open_download_stream_by_name, ("name",)),
            (
                bucket.rename,
                (
                    1,
                    "new-name",
                ),
            ),
            (
                bucket.rename_by_name,
                (
                    "new-name",
                    "new-name2",
                ),
            ),
            (bucket.delete_by_name, ("new-name2",)),
        ]

        async with client.start_session() as s, await s.start_transaction():
            for op, args in gridfs_ops:
                with self.assertRaisesRegex(
                    InvalidOperation,
                    "GridFS does not support multi-document transactions",
                ):
                    await op(*args, session=s)  # type: ignore

    # Require 4.2+ for large (16MB+) transactions.
    @async_client_context.require_version_min(4, 2)
    @async_client_context.require_transactions
    @unittest.skipIf(sys.platform == "win32", "Our Windows machines are too slow to pass this test")
    async def test_transaction_starts_with_batched_write(self):
        if "PyPy" in sys.version and async_client_context.tls:
            self.skipTest(
                "PYTHON-2937 PyPy is so slow sending large "
                "messages over TLS that this test fails"
            )
        # Start a transaction with a batch of operations that needs to be
        # split.
        listener = OvertCommandListener()
        client = await self.async_rs_client(event_listeners=[listener])
        coll = client[self.db.name].test
        await coll.delete_many({})
        listener.reset()
        self.addAsyncCleanup(coll.drop)
        large_str = "\0" * (1 * 1024 * 1024)
        ops: List[InsertOne[RawBSONDocument]] = [
            InsertOne(RawBSONDocument(encode({"a": large_str}))) for _ in range(48)
        ]
        async with client.start_session() as session:
            async with await session.start_transaction():
                await coll.bulk_write(ops, session=session)  # type: ignore[arg-type]
        # Assert commands were constructed properly.
        self.assertEqual(
            ["insert", "insert", "commitTransaction"], listener.started_command_names()
        )
        first_cmd = listener.started_events[0].command
        self.assertTrue(first_cmd["startTransaction"])
        lsid = first_cmd["lsid"]
        txn_number = first_cmd["txnNumber"]
        for event in listener.started_events[1:]:
            self.assertNotIn("startTransaction", event.command)
            self.assertEqual(lsid, event.command["lsid"])
            self.assertEqual(txn_number, event.command["txnNumber"])
        self.assertEqual(48, await coll.count_documents({}))

    @async_client_context.require_transactions
    async def test_transaction_direct_connection(self):
        client = await self.async_single_client()
        coll = client.pymongo_test.test

        # Make sure the collection exists.
        await coll.insert_one({})
        self.assertEqual(client.topology_description.topology_type_name, "Single")

        async def find(*args, **kwargs):
            return coll.find(*args, **kwargs)

        async def find_raw_batches(*args, **kwargs):
            return coll.find_raw_batches(*args, **kwargs)

        ops = [
            (coll.bulk_write, [[InsertOne[dict]({})]]),
            (coll.insert_one, [{}]),
            (coll.insert_many, [[{}, {}]]),
            (coll.replace_one, [{}, {}]),
            (coll.update_one, [{}, {"$set": {"a": 1}}]),
            (coll.update_many, [{}, {"$set": {"a": 1}}]),
            (coll.delete_one, [{}]),
            (coll.delete_many, [{}]),
            (coll.find_one_and_replace, [{}, {}]),
            (coll.find_one_and_update, [{}, {"$set": {"a": 1}}]),
            (coll.find_one_and_delete, [{}, {}]),
            (coll.find_one, [{}]),
            (coll.count_documents, [{}]),
            (coll.distinct, ["foo"]),
            (coll.aggregate, [[]]),
            (find, [{}]),
            (coll.aggregate_raw_batches, [[]]),
            (find_raw_batches, [{}]),
            (coll.database.command, ["find", coll.name]),
        ]
        for f, args in ops:
            async with client.start_session() as s, await s.start_transaction():
                res = await f(*args, session=s)  # type:ignore[operator]
                if isinstance(res, (AsyncCommandCursor, AsyncCursor)):
                    await res.to_list()

    @async_client_context.require_transactions
    async def test_transaction_pool_cleared_error_labelled_transient(self):
        c = await self.async_single_client()

        with self.assertRaises(AutoReconnect) as context:
            async with c.start_session() as session:
                async with await session.start_transaction():
                    server = await c._select_server(writable_server_selector, session, "test")
                    # Pause the server's pool, causing it to fail connection checkout.
                    server.pool.state = PoolState.PAUSED
                    async with c._checkout(server, session):
                        pass

        # Verify that the TransientTransactionError label is present in the error.
        self.assertTrue(context.exception.has_error_label("TransientTransactionError"))


class PatchSessionTimeout:
    """Patches the client_session's with_transaction timeout for testing."""

    def __init__(self, mock_timeout):
        self.real_timeout = client_session._WITH_TRANSACTION_RETRY_TIME_LIMIT
        self.mock_timeout = mock_timeout

    def __enter__(self):
        client_session._WITH_TRANSACTION_RETRY_TIME_LIMIT = self.mock_timeout
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        client_session._WITH_TRANSACTION_RETRY_TIME_LIMIT = self.real_timeout


class TestTransactionsConvenientAPI(AsyncTransactionsBase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.mongos_clients = []
        if async_client_context.supports_transactions():
            for address in async_client_context.mongoses:
                self.mongos_clients.append(await self.async_single_client("{}:{}".format(*address)))

    async def set_fail_point(self, command_args):
        clients = self.mongos_clients if self.mongos_clients else [self.client]
        for client in clients:
            await self.configure_fail_point(client, command_args)

    @async_client_context.require_transactions
    async def test_callback_raises_custom_error(self):
        class _MyException(Exception):
            pass

        async def raise_error(_):
            raise _MyException

        async with self.client.start_session() as s:
            with self.assertRaises(_MyException):
                await s.with_transaction(raise_error)

    @async_client_context.require_transactions
    async def test_callback_returns_value(self):
        async def callback(_):
            return "Foo"

        async with self.client.start_session() as s:
            self.assertEqual(await s.with_transaction(callback), "Foo")

        await self.db.test.insert_one({})

        async def callback2(session):
            await self.db.test.insert_one({}, session=session)
            return "Foo"

        async with self.client.start_session() as s:
            self.assertEqual(await s.with_transaction(callback2), "Foo")

    @async_client_context.require_transactions
    @async_client_context.require_async
    async def test_callback_awaitable_no_coroutine(self):
        def callback(_):
            future = asyncio.Future()
            future.set_result("Foo")
            return future

        async with self.client.start_session() as s:
            self.assertEqual(await s.with_transaction(callback), "Foo")

    @async_client_context.require_transactions
    async def test_callback_not_retried_after_timeout(self):
        listener = OvertCommandListener()
        client = await self.async_rs_client(event_listeners=[listener])
        coll = client[self.db.name].test

        async def callback(session):
            await coll.insert_one({}, session=session)
            err: dict = {
                "ok": 0,
                "errmsg": "Transaction 7819 has been aborted.",
                "code": 251,
                "codeName": "NoSuchTransaction",
                "errorLabels": ["TransientTransactionError"],
            }
            raise OperationFailure(err["errmsg"], err["code"], err)

        # Create the collection.
        await coll.insert_one({})
        listener.reset()
        async with client.start_session() as s:
            with PatchSessionTimeout(0):
                with self.assertRaises(OperationFailure):
                    await s.with_transaction(callback)

        self.assertEqual(listener.started_command_names(), ["insert", "abortTransaction"])

    @async_client_context.require_test_commands
    @async_client_context.require_transactions
    async def test_callback_not_retried_after_commit_timeout(self):
        listener = OvertCommandListener()
        client = await self.async_rs_client(event_listeners=[listener])
        coll = client[self.db.name].test

        async def callback(session):
            await coll.insert_one({}, session=session)

        # Create the collection.
        await coll.insert_one({})
        await self.set_fail_point(
            {
                "configureFailPoint": "failCommand",
                "mode": {"times": 1},
                "data": {
                    "failCommands": ["commitTransaction"],
                    "errorCode": 251,  # NoSuchTransaction
                },
            }
        )
        self.addAsyncCleanup(
            self.set_fail_point, {"configureFailPoint": "failCommand", "mode": "off"}
        )
        listener.reset()

        async with client.start_session() as s:
            with PatchSessionTimeout(0):
                with self.assertRaises(OperationFailure):
                    await s.with_transaction(callback)

        self.assertEqual(listener.started_command_names(), ["insert", "commitTransaction"])

    @async_client_context.require_test_commands
    @async_client_context.require_transactions
    async def test_commit_not_retried_after_timeout(self):
        listener = OvertCommandListener()
        client = await self.async_rs_client(event_listeners=[listener])
        coll = client[self.db.name].test

        async def callback(session):
            await coll.insert_one({}, session=session)

        # Create the collection.
        await coll.insert_one({})
        await self.set_fail_point(
            {
                "configureFailPoint": "failCommand",
                "mode": {"times": 2},
                "data": {"failCommands": ["commitTransaction"], "closeConnection": True},
            }
        )
        self.addAsyncCleanup(
            self.set_fail_point, {"configureFailPoint": "failCommand", "mode": "off"}
        )
        listener.reset()

        async with client.start_session() as s:
            with PatchSessionTimeout(0):
                with self.assertRaises(ConnectionFailure):
                    await s.with_transaction(callback)

        # One insert for the callback and two commits (includes the automatic
        # retry).
        self.assertEqual(
            listener.started_command_names(), ["insert", "commitTransaction", "commitTransaction"]
        )

    # Tested here because this supports Motor's convenient transactions API.
    @async_client_context.require_transactions
    async def test_in_transaction_property(self):
        client = async_client_context.client
        coll = client.test.testcollection
        await coll.insert_one({})
        self.addAsyncCleanup(coll.drop)

        async with client.start_session() as s:
            self.assertFalse(s.in_transaction)
            await s.start_transaction()
            self.assertTrue(s.in_transaction)
            await coll.insert_one({}, session=s)
            self.assertTrue(s.in_transaction)
            await s.commit_transaction()
            self.assertFalse(s.in_transaction)

        async with client.start_session() as s:
            await s.start_transaction()
            # commit empty transaction
            await s.commit_transaction()
            self.assertFalse(s.in_transaction)

        async with client.start_session() as s:
            await s.start_transaction()
            await s.abort_transaction()
            self.assertFalse(s.in_transaction)

        # Using a callback
        async def callback(session):
            self.assertTrue(session.in_transaction)

        async with client.start_session() as s:
            self.assertFalse(s.in_transaction)
            await s.with_transaction(callback)
            self.assertFalse(s.in_transaction)


class TestOptionsInsideTransactionProse(AsyncTransactionsBase):
    @async_client_context.require_transactions
    @async_client_context.require_no_standalone
    async def test_case_1(self):
        # Write concern not inherited from collection object inside transaction
        # Create a MongoClient running against a configured sharded/replica set/load balanced cluster.
        client = async_client_context.client
        coll = client[self.db.name].test
        await coll.delete_many({})
        # Start a new session on the client.
        async with client.start_session() as s:
            # Start a transaction on the session.
            await s.start_transaction()
            # Instantiate a collection object in the driver with a default write concern of { w: 0 }.
            inner_coll = coll.with_options(write_concern=WriteConcern(w=0))
            # Insert the document { n: 1 } on the instantiated collection.
            result = await inner_coll.insert_one({"n": 1}, session=s)
            # Commit the transaction.
            await s.commit_transaction()
        # End the session.
        # Ensure the document was inserted and no error was thrown from the transaction.
        assert result.inserted_id is not None


if __name__ == "__main__":
    unittest.main()
