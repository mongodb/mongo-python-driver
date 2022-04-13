# Copyright 2017 MongoDB, Inc.
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

"""Test retryable writes."""

import copy
import os
import pprint
import sys
import threading

sys.path[0:0] = [""]

from test import IntegrationTest, SkipTest, client_context, client_knobs, unittest
from test.utils import (
    CMAPListener,
    DeprecationFilter,
    OvertCommandListener,
    TestCreator,
    rs_or_single_client,
)
from test.utils_spec_runner import SpecRunner
from test.version import Version

from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.int64 import Int64
from bson.raw_bson import RawBSONDocument
from bson.son import SON
from pymongo.errors import (
    ConnectionFailure,
    OperationFailure,
    ServerSelectionTimeoutError,
    WriteConcernError,
)
from pymongo.mongo_client import MongoClient
from pymongo.monitoring import (
    ConnectionCheckedOutEvent,
    ConnectionCheckOutFailedEvent,
    ConnectionCheckOutFailedReason,
    PoolClearedEvent,
)
from pymongo.operations import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)
from pymongo.write_concern import WriteConcern

# Location of JSON test specifications.
_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "retryable_writes", "legacy")


class TestAllScenarios(SpecRunner):
    RUN_ON_LOAD_BALANCER = True
    RUN_ON_SERVERLESS = True

    def get_object_name(self, op):
        return op.get("object", "collection")

    def get_scenario_db_name(self, scenario_def):
        return scenario_def.get("database_name", "pymongo_test")

    def get_scenario_coll_name(self, scenario_def):
        return scenario_def.get("collection_name", "test")

    def run_test_ops(self, sessions, collection, test):
        # Transform retryable writes spec format into transactions.
        operation = test["operation"]
        outcome = test["outcome"]
        if "error" in outcome:
            operation["error"] = outcome["error"]
        if "result" in outcome:
            operation["result"] = outcome["result"]
        test["operations"] = [operation]
        super(TestAllScenarios, self).run_test_ops(sessions, collection, test)


def create_test(scenario_def, test, name):
    @client_context.require_test_commands
    @client_context.require_no_mmap
    def run_scenario(self):
        self.run_scenario(scenario_def, test)

    return run_scenario


test_creator = TestCreator(create_test, TestAllScenarios, _TEST_PATH)
test_creator.create_tests()


def retryable_single_statement_ops(coll):
    return [
        (coll.bulk_write, [[InsertOne({}), InsertOne({})]], {}),
        (coll.bulk_write, [[InsertOne({}), InsertOne({})]], {"ordered": False}),
        (coll.bulk_write, [[ReplaceOne({}, {})]], {}),
        (coll.bulk_write, [[ReplaceOne({}, {}), ReplaceOne({}, {})]], {}),
        (
            coll.bulk_write,
            [[UpdateOne({}, {"$set": {"a": 1}}), UpdateOne({}, {"$set": {"a": 1}})]],
            {},
        ),
        (coll.bulk_write, [[DeleteOne({})]], {}),
        (coll.bulk_write, [[DeleteOne({}), DeleteOne({})]], {}),
        (coll.insert_one, [{}], {}),
        (coll.insert_many, [[{}, {}]], {}),
        (coll.replace_one, [{}, {}], {}),
        (coll.update_one, [{}, {"$set": {"a": 1}}], {}),
        (coll.delete_one, [{}], {}),
        (coll.find_one_and_replace, [{}, {"a": 3}], {}),
        (coll.find_one_and_update, [{}, {"$set": {"a": 1}}], {}),
        (coll.find_one_and_delete, [{}, {}], {}),
    ]


def non_retryable_single_statement_ops(coll):
    return [
        (
            coll.bulk_write,
            [[UpdateOne({}, {"$set": {"a": 1}}), UpdateMany({}, {"$set": {"a": 1}})]],
            {},
        ),
        (coll.bulk_write, [[DeleteOne({}), DeleteMany({})]], {}),
        (coll.update_many, [{}, {"$set": {"a": 1}}], {}),
        (coll.delete_many, [{}], {}),
    ]


class IgnoreDeprecationsTest(IntegrationTest):
    RUN_ON_LOAD_BALANCER = True
    RUN_ON_SERVERLESS = True
    deprecation_filter: DeprecationFilter

    @classmethod
    def setUpClass(cls):
        super(IgnoreDeprecationsTest, cls).setUpClass()
        cls.deprecation_filter = DeprecationFilter()

    @classmethod
    def tearDownClass(cls):
        cls.deprecation_filter.stop()
        super(IgnoreDeprecationsTest, cls).tearDownClass()


class TestRetryableWritesMMAPv1(IgnoreDeprecationsTest):
    knobs: client_knobs

    @classmethod
    def setUpClass(cls):
        super(TestRetryableWritesMMAPv1, cls).setUpClass()
        # Speed up the tests by decreasing the heartbeat frequency.
        cls.knobs = client_knobs(heartbeat_frequency=0.1, min_heartbeat_interval=0.1)
        cls.knobs.enable()
        cls.client = rs_or_single_client(retryWrites=True)
        cls.db = cls.client.pymongo_test

    @classmethod
    def tearDownClass(cls):
        cls.knobs.disable()
        cls.client.close()
        super(TestRetryableWritesMMAPv1, cls).tearDownClass()

    @client_context.require_no_standalone
    def test_actionable_error_message(self):
        if client_context.storage_engine != "mmapv1":
            raise SkipTest("This cluster is not running MMAPv1")

        expected_msg = (
            "This MongoDB deployment does not support retryable "
            "writes. Please add retryWrites=false to your "
            "connection string."
        )
        for method, args, kwargs in retryable_single_statement_ops(self.db.retryable_write_test):
            with self.assertRaisesRegex(OperationFailure, expected_msg):
                method(*args, **kwargs)


class TestRetryableWrites(IgnoreDeprecationsTest):
    listener: OvertCommandListener
    knobs: client_knobs

    @classmethod
    @client_context.require_no_mmap
    def setUpClass(cls):
        super(TestRetryableWrites, cls).setUpClass()
        # Speed up the tests by decreasing the heartbeat frequency.
        cls.knobs = client_knobs(heartbeat_frequency=0.1, min_heartbeat_interval=0.1)
        cls.knobs.enable()
        cls.listener = OvertCommandListener()
        cls.client = rs_or_single_client(retryWrites=True, event_listeners=[cls.listener])
        cls.db = cls.client.pymongo_test

    @classmethod
    def tearDownClass(cls):
        cls.knobs.disable()
        cls.client.close()
        super(TestRetryableWrites, cls).tearDownClass()

    def setUp(self):
        if client_context.is_rs and client_context.test_commands_enabled:
            self.client.admin.command(
                SON([("configureFailPoint", "onPrimaryTransactionalWrite"), ("mode", "alwaysOn")])
            )

    def tearDown(self):
        if client_context.is_rs and client_context.test_commands_enabled:
            self.client.admin.command(
                SON([("configureFailPoint", "onPrimaryTransactionalWrite"), ("mode", "off")])
            )

    def test_supported_single_statement_no_retry(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(retryWrites=False, event_listeners=[listener])
        self.addCleanup(client.close)
        for method, args, kwargs in retryable_single_statement_ops(client.db.retryable_write_test):
            msg = "%s(*%r, **%r)" % (method.__name__, args, kwargs)
            listener.results.clear()
            method(*args, **kwargs)
            for event in listener.results["started"]:
                self.assertNotIn(
                    "txnNumber",
                    event.command,
                    "%s sent txnNumber with %s" % (msg, event.command_name),
                )

    @client_context.require_no_standalone
    def test_supported_single_statement_supported_cluster(self):
        for method, args, kwargs in retryable_single_statement_ops(self.db.retryable_write_test):
            msg = "%s(*%r, **%r)" % (method.__name__, args, kwargs)
            self.listener.results.clear()
            method(*args, **kwargs)
            commands_started = self.listener.results["started"]
            self.assertEqual(len(self.listener.results["succeeded"]), 1, msg)
            first_attempt = commands_started[0]
            self.assertIn(
                "lsid",
                first_attempt.command,
                "%s sent no lsid with %s" % (msg, first_attempt.command_name),
            )
            initial_session_id = first_attempt.command["lsid"]
            self.assertIn(
                "txnNumber",
                first_attempt.command,
                "%s sent no txnNumber with %s" % (msg, first_attempt.command_name),
            )

            # There should be no retry when the failpoint is not active.
            if client_context.is_mongos or not client_context.test_commands_enabled:
                self.assertEqual(len(commands_started), 1)
                continue

            initial_transaction_id = first_attempt.command["txnNumber"]
            retry_attempt = commands_started[1]
            self.assertIn(
                "lsid",
                retry_attempt.command,
                "%s sent no lsid with %s" % (msg, first_attempt.command_name),
            )
            self.assertEqual(retry_attempt.command["lsid"], initial_session_id, msg)
            self.assertIn(
                "txnNumber",
                retry_attempt.command,
                "%s sent no txnNumber with %s" % (msg, first_attempt.command_name),
            )
            self.assertEqual(retry_attempt.command["txnNumber"], initial_transaction_id, msg)

    def test_supported_single_statement_unsupported_cluster(self):
        if client_context.is_rs or client_context.is_mongos:
            raise SkipTest("This cluster supports retryable writes")

        for method, args, kwargs in retryable_single_statement_ops(self.db.retryable_write_test):
            msg = "%s(*%r, **%r)" % (method.__name__, args, kwargs)
            self.listener.results.clear()
            method(*args, **kwargs)

            for event in self.listener.results["started"]:
                self.assertNotIn(
                    "txnNumber",
                    event.command,
                    "%s sent txnNumber with %s" % (msg, event.command_name),
                )

    def test_unsupported_single_statement(self):
        coll = self.db.retryable_write_test
        coll.insert_many([{}, {}])
        coll_w0 = coll.with_options(write_concern=WriteConcern(w=0))
        for method, args, kwargs in non_retryable_single_statement_ops(
            coll
        ) + retryable_single_statement_ops(coll_w0):
            msg = "%s(*%r, **%r)" % (method.__name__, args, kwargs)
            self.listener.results.clear()
            method(*args, **kwargs)
            started_events = self.listener.results["started"]
            self.assertEqual(len(self.listener.results["succeeded"]), len(started_events), msg)
            self.assertEqual(len(self.listener.results["failed"]), 0, msg)
            for event in started_events:
                self.assertNotIn(
                    "txnNumber",
                    event.command,
                    "%s sent txnNumber with %s" % (msg, event.command_name),
                )

    def test_server_selection_timeout_not_retried(self):
        """A ServerSelectionTimeoutError is not retried."""
        listener = OvertCommandListener()
        client = MongoClient(
            "somedomainthatdoesntexist.org",
            serverSelectionTimeoutMS=1,
            retryWrites=True,
            event_listeners=[listener],
        )
        for method, args, kwargs in retryable_single_statement_ops(client.db.retryable_write_test):
            msg = "%s(*%r, **%r)" % (method.__name__, args, kwargs)
            listener.results.clear()
            with self.assertRaises(ServerSelectionTimeoutError, msg=msg):
                method(*args, **kwargs)
            self.assertEqual(len(listener.results["started"]), 0, msg)

    @client_context.require_replica_set
    @client_context.require_test_commands
    def test_retry_timeout_raises_original_error(self):
        """A ServerSelectionTimeoutError on the retry attempt raises the
        original error.
        """
        listener = OvertCommandListener()
        client = rs_or_single_client(retryWrites=True, event_listeners=[listener])
        self.addCleanup(client.close)
        topology = client._topology
        select_server = topology.select_server

        def mock_select_server(*args, **kwargs):
            server = select_server(*args, **kwargs)

            def raise_error(*args, **kwargs):
                raise ServerSelectionTimeoutError("No primary available for writes")

            # Raise ServerSelectionTimeout on the retry attempt.
            topology.select_server = raise_error
            return server

        for method, args, kwargs in retryable_single_statement_ops(client.db.retryable_write_test):
            msg = "%s(*%r, **%r)" % (method.__name__, args, kwargs)
            listener.results.clear()
            topology.select_server = mock_select_server
            with self.assertRaises(ConnectionFailure, msg=msg):
                method(*args, **kwargs)
            self.assertEqual(len(listener.results["started"]), 1, msg)

    @client_context.require_replica_set
    @client_context.require_test_commands
    def test_batch_splitting(self):
        """Test retry succeeds after failures during batch splitting."""
        large = "s" * 1024 * 1024 * 15
        coll = self.db.retryable_write_test
        coll.delete_many({})
        self.listener.results.clear()
        bulk_result = coll.bulk_write(
            [
                InsertOne({"_id": 1, "l": large}),
                InsertOne({"_id": 2, "l": large}),
                InsertOne({"_id": 3, "l": large}),
                UpdateOne({"_id": 1, "l": large}, {"$unset": {"l": 1}, "$inc": {"count": 1}}),
                UpdateOne({"_id": 2, "l": large}, {"$set": {"foo": "bar"}}),
                DeleteOne({"l": large}),
                DeleteOne({"l": large}),
            ]
        )
        # Each command should fail and be retried.
        # With OP_MSG 3 inserts are one batch. 2 updates another.
        # 2 deletes a third.
        self.assertEqual(len(self.listener.results["started"]), 6)
        self.assertEqual(coll.find_one(), {"_id": 1, "count": 1})
        # Assert the final result
        expected_result = {
            "writeErrors": [],
            "writeConcernErrors": [],
            "nInserted": 3,
            "nUpserted": 0,
            "nMatched": 2,
            "nModified": 2,
            "nRemoved": 2,
            "upserted": [],
        }
        self.assertEqual(bulk_result.bulk_api_result, expected_result)

    @client_context.require_replica_set
    @client_context.require_test_commands
    def test_batch_splitting_retry_fails(self):
        """Test retry fails during batch splitting."""
        large = "s" * 1024 * 1024 * 15
        coll = self.db.retryable_write_test
        coll.delete_many({})
        self.client.admin.command(
            SON(
                [
                    ("configureFailPoint", "onPrimaryTransactionalWrite"),
                    ("mode", {"skip": 3}),  # The number of _documents_ to skip.
                    ("data", {"failBeforeCommitExceptionCode": 1}),
                ]
            )
        )
        self.listener.results.clear()
        with self.client.start_session() as session:
            initial_txn = session._server_session._transaction_id
            try:
                coll.bulk_write(
                    [
                        InsertOne({"_id": 1, "l": large}),
                        InsertOne({"_id": 2, "l": large}),
                        InsertOne({"_id": 3, "l": large}),
                        InsertOne({"_id": 4, "l": large}),
                    ],
                    session=session,
                )
            except ConnectionFailure:
                pass
            else:
                self.fail("bulk_write should have failed")

            started = self.listener.results["started"]
            self.assertEqual(len(started), 3)
            self.assertEqual(len(self.listener.results["succeeded"]), 1)
            expected_txn = Int64(initial_txn + 1)
            self.assertEqual(started[0].command["txnNumber"], expected_txn)
            self.assertEqual(started[0].command["lsid"], session.session_id)
            expected_txn = Int64(initial_txn + 2)
            self.assertEqual(started[1].command["txnNumber"], expected_txn)
            self.assertEqual(started[1].command["lsid"], session.session_id)
            started[1].command.pop("$clusterTime")
            started[2].command.pop("$clusterTime")
            self.assertEqual(started[1].command, started[2].command)
            final_txn = session._server_session._transaction_id
            self.assertEqual(final_txn, expected_txn)
        self.assertEqual(coll.find_one(projection={"_id": True}), {"_id": 1})


class TestWriteConcernError(IntegrationTest):
    RUN_ON_LOAD_BALANCER = True
    RUN_ON_SERVERLESS = True
    fail_insert: dict

    @classmethod
    @client_context.require_replica_set
    @client_context.require_no_mmap
    @client_context.require_failCommand_fail_point
    def setUpClass(cls):
        super(TestWriteConcernError, cls).setUpClass()
        cls.fail_insert = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 2},
            "data": {
                "failCommands": ["insert"],
                "writeConcernError": {"code": 91, "errmsg": "Replication is being shut down"},
            },
        }

    @client_context.require_version_min(4, 0)
    def test_RetryableWriteError_error_label(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(retryWrites=True, event_listeners=[listener])

        # Ensure collection exists.
        client.pymongo_test.testcoll.insert_one({})

        with self.fail_point(self.fail_insert):
            with self.assertRaises(WriteConcernError) as cm:
                client.pymongo_test.testcoll.insert_one({})
            self.assertTrue(cm.exception.has_error_label("RetryableWriteError"))

        if client_context.version >= Version(4, 4):
            # In MongoDB 4.4+ we rely on the server returning the error label.
            self.assertIn(
                "RetryableWriteError", listener.results["succeeded"][-1].reply["errorLabels"]
            )

    @client_context.require_version_min(4, 4)
    def test_RetryableWriteError_error_label_RawBSONDocument(self):
        # using RawBSONDocument should not cause errorLabel parsing to fail
        with self.fail_point(self.fail_insert):
            with self.client.start_session() as s:
                s._start_retryable_write()
                result = self.client.pymongo_test.command(
                    "insert",
                    "testcoll",
                    documents=[{"_id": 1}],
                    txnNumber=s._server_session.transaction_id,
                    session=s,
                    codec_options=DEFAULT_CODEC_OPTIONS.with_options(
                        document_class=RawBSONDocument
                    ),
                )

        self.assertIn("writeConcernError", result)
        self.assertIn("RetryableWriteError", result["errorLabels"])


class InsertThread(threading.Thread):
    def __init__(self, collection):
        super().__init__()
        self.daemon = True
        self.collection = collection
        self.passed = False

    def run(self):
        self.collection.insert_one({})
        self.passed = True


class TestPoolPausedError(IntegrationTest):
    # Pools don't get paused in load balanced mode.
    RUN_ON_LOAD_BALANCER = False
    RUN_ON_SERVERLESS = False

    @client_context.require_failCommand_blockConnection
    @client_context.require_retryable_writes
    @client_knobs(heartbeat_frequency=0.05, min_heartbeat_interval=0.05)
    def test_pool_paused_error_is_retryable(self):
        cmap_listener = CMAPListener()
        cmd_listener = OvertCommandListener()
        client = rs_or_single_client(maxPoolSize=1, event_listeners=[cmap_listener, cmd_listener])
        self.addCleanup(client.close)
        for _ in range(10):
            cmap_listener.reset()
            cmd_listener.reset()
            threads = [InsertThread(client.pymongo_test.test) for _ in range(2)]
            fail_command = {
                "mode": {"times": 1},
                "data": {
                    "failCommands": ["insert"],
                    "blockConnection": True,
                    "blockTimeMS": 1000,
                    "errorCode": 91,
                    "errorLabels": ["RetryableWriteError"],
                },
            }
            with self.fail_point(fail_command):
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
        started = cmd_listener.results["started"]
        msg = pprint.pformat(cmd_listener.results)
        self.assertEqual(3, len(started), msg)
        succeeded = cmd_listener.results["succeeded"]
        self.assertEqual(2, len(succeeded), msg)
        failed = cmd_listener.results["failed"]
        self.assertEqual(1, len(failed), msg)


# TODO: Make this a real integration test where we stepdown the primary.
class TestRetryableWritesTxnNumber(IgnoreDeprecationsTest):
    @client_context.require_replica_set
    @client_context.require_no_mmap
    def test_increment_transaction_id_without_sending_command(self):
        """Test that the txnNumber field is properly incremented, even when
        the first attempt fails before sending the command.
        """
        listener = OvertCommandListener()
        client = rs_or_single_client(retryWrites=True, event_listeners=[listener])
        self.addCleanup(client.close)
        topology = client._topology
        select_server = topology.select_server

        def raise_connection_err_select_server(*args, **kwargs):
            # Raise ConnectionFailure on the first attempt and perform
            # normal selection on the retry attempt.
            topology.select_server = select_server
            raise ConnectionFailure("Connection refused")

        for method, args, kwargs in retryable_single_statement_ops(client.db.retryable_write_test):
            listener.results.clear()
            topology.select_server = raise_connection_err_select_server
            with client.start_session() as session:
                kwargs = copy.deepcopy(kwargs)
                kwargs["session"] = session
                msg = "%s(*%r, **%r)" % (method.__name__, args, kwargs)
                initial_txn_id = session._server_session.transaction_id

                # Each operation should fail on the first attempt and succeed
                # on the second.
                method(*args, **kwargs)
                self.assertEqual(len(listener.results["started"]), 1, msg)
                retry_cmd = listener.results["started"][0].command
                sent_txn_id = retry_cmd["txnNumber"]
                final_txn_id = session._server_session.transaction_id
                self.assertEqual(Int64(initial_txn_id + 1), sent_txn_id, msg)
                self.assertEqual(sent_txn_id, final_txn_id, msg)


if __name__ == "__main__":
    unittest.main()
