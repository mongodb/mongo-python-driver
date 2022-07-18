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

"""Utilities for testing driver specs."""

import functools
import threading
from collections import abc
from test import IntegrationTest, client_context, client_knobs
from test.utils import (
    CMAPListener,
    CompareType,
    EventListener,
    OvertCommandListener,
    ServerAndTopologyEventListener,
    camel_to_snake,
    camel_to_snake_args,
    parse_spec_options,
    prepare_spec_arguments,
    rs_client,
)
from typing import List

from bson import decode, encode
from bson.binary import Binary
from bson.int64 import Int64
from bson.son import SON
from gridfs import GridFSBucket
from pymongo import client_session
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.errors import BulkWriteError, OperationFailure, PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.results import BulkWriteResult, _WriteResult
from pymongo.write_concern import WriteConcern


class SpecRunnerThread(threading.Thread):
    def __init__(self, name):
        super(SpecRunnerThread, self).__init__()
        self.name = name
        self.exc = None
        self.setDaemon(True)
        self.cond = threading.Condition()
        self.ops = []
        self.stopped = False

    def schedule(self, work):
        self.ops.append(work)
        with self.cond:
            self.cond.notify()

    def stop(self):
        self.stopped = True
        with self.cond:
            self.cond.notify()

    def run(self):
        while not self.stopped or self.ops:
            if not self.ops:
                with self.cond:
                    self.cond.wait(10)
            if self.ops:
                try:
                    work = self.ops.pop(0)
                    work()
                except Exception as exc:
                    self.exc = exc
                    self.stop()


class SpecRunner(IntegrationTest):
    mongos_clients: List
    knobs: client_knobs
    listener: EventListener

    @classmethod
    def setUpClass(cls):
        super(SpecRunner, cls).setUpClass()
        cls.mongos_clients = []

        # Speed up the tests by decreasing the heartbeat frequency.
        cls.knobs = client_knobs(heartbeat_frequency=0.1, min_heartbeat_interval=0.1)
        cls.knobs.enable()

    @classmethod
    def tearDownClass(cls):
        cls.knobs.disable()
        super(SpecRunner, cls).tearDownClass()

    def setUp(self):
        super(SpecRunner, self).setUp()
        self.targets = {}
        self.listener = None  # type: ignore
        self.pool_listener = None
        self.server_listener = None
        self.maxDiff = None

    def _set_fail_point(self, client, command_args):
        cmd = SON([("configureFailPoint", "failCommand")])
        cmd.update(command_args)
        client.admin.command(cmd)

    def set_fail_point(self, command_args):
        clients = self.mongos_clients if self.mongos_clients else [self.client]
        for client in clients:
            self._set_fail_point(client, command_args)

    def targeted_fail_point(self, session, fail_point):
        """Run the targetedFailPoint test operation.

        Enable the fail point on the session's pinned mongos.
        """
        clients = {c.address: c for c in self.mongos_clients}
        client = clients[session._pinned_address]
        self._set_fail_point(client, fail_point)
        self.addCleanup(self.set_fail_point, {"mode": "off"})

    def assert_session_pinned(self, session):
        """Run the assertSessionPinned test operation.

        Assert that the given session is pinned.
        """
        self.assertIsNotNone(session._transaction.pinned_address)

    def assert_session_unpinned(self, session):
        """Run the assertSessionUnpinned test operation.

        Assert that the given session is not pinned.
        """
        self.assertIsNone(session._pinned_address)
        self.assertIsNone(session._transaction.pinned_address)

    def assert_collection_exists(self, database, collection):
        """Run the assertCollectionExists test operation."""
        db = self.client[database]
        self.assertIn(collection, db.list_collection_names())

    def assert_collection_not_exists(self, database, collection):
        """Run the assertCollectionNotExists test operation."""
        db = self.client[database]
        self.assertNotIn(collection, db.list_collection_names())

    def assert_index_exists(self, database, collection, index):
        """Run the assertIndexExists test operation."""
        coll = self.client[database][collection]
        self.assertIn(index, [doc["name"] for doc in coll.list_indexes()])

    def assert_index_not_exists(self, database, collection, index):
        """Run the assertIndexNotExists test operation."""
        coll = self.client[database][collection]
        self.assertNotIn(index, [doc["name"] for doc in coll.list_indexes()])

    def assertErrorLabelsContain(self, exc, expected_labels):
        labels = [l for l in expected_labels if exc.has_error_label(l)]
        self.assertEqual(labels, expected_labels)

    def assertErrorLabelsOmit(self, exc, omit_labels):
        for label in omit_labels:
            self.assertFalse(
                exc.has_error_label(label), msg="error labels should not contain %s" % (label,)
            )

    def kill_all_sessions(self):
        clients = self.mongos_clients if self.mongos_clients else [self.client]
        for client in clients:
            try:
                client.admin.command("killAllSessions", [])
            except OperationFailure:
                # "operation was interrupted" by killing the command's
                # own session.
                pass

    def check_command_result(self, expected_result, result):
        # Only compare the keys in the expected result.
        filtered_result = {}
        for key in expected_result:
            try:
                filtered_result[key] = result[key]
            except KeyError:
                pass
        self.assertEqual(filtered_result, expected_result)

    # TODO: factor the following function with test_crud.py.
    def check_result(self, expected_result, result):
        if isinstance(result, _WriteResult):
            for res in expected_result:
                prop = camel_to_snake(res)
                # SPEC-869: Only BulkWriteResult has upserted_count.
                if prop == "upserted_count" and not isinstance(result, BulkWriteResult):
                    if result.upserted_id is not None:
                        upserted_count = 1
                    else:
                        upserted_count = 0
                    self.assertEqual(upserted_count, expected_result[res], prop)
                elif prop == "inserted_ids":
                    # BulkWriteResult does not have inserted_ids.
                    if isinstance(result, BulkWriteResult):
                        self.assertEqual(len(expected_result[res]), result.inserted_count)
                    else:
                        # InsertManyResult may be compared to [id1] from the
                        # crud spec or {"0": id1} from the retryable write spec.
                        ids = expected_result[res]
                        if isinstance(ids, dict):
                            ids = [ids[str(i)] for i in range(len(ids))]

                        self.assertEqual(ids, result.inserted_ids, prop)
                elif prop == "upserted_ids":
                    # Convert indexes from strings to integers.
                    ids = expected_result[res]
                    expected_ids = {}
                    for str_index in ids:
                        expected_ids[int(str_index)] = ids[str_index]
                    self.assertEqual(expected_ids, result.upserted_ids, prop)
                else:
                    self.assertEqual(getattr(result, prop), expected_result[res], prop)

            return True
        else:

            def _helper(expected_result, result):
                if isinstance(expected_result, abc.Mapping):
                    for i in expected_result.keys():
                        self.assertEqual(expected_result[i], result[i])

                elif isinstance(expected_result, list):
                    for i, k in zip(expected_result, result):
                        _helper(i, k)
                else:
                    self.assertEqual(expected_result, result)

            _helper(expected_result, result)

    def get_object_name(self, op):
        """Allow subclasses to override handling of 'object'

        Transaction spec says 'object' is required.
        """
        return op["object"]

    @staticmethod
    def parse_options(opts):
        return parse_spec_options(opts)

    def run_operation(self, sessions, collection, operation):
        original_collection = collection
        name = camel_to_snake(operation["name"])
        if name == "run_command":
            name = "command"
        elif name == "download_by_name":
            name = "open_download_stream_by_name"
        elif name == "download":
            name = "open_download_stream"
        elif name == "map_reduce":
            self.skipTest("PyMongo does not support mapReduce")
        elif name == "count":
            self.skipTest("PyMongo does not support count")

        database = collection.database
        collection = database.get_collection(collection.name)
        if "collectionOptions" in operation:
            collection = collection.with_options(
                **self.parse_options(operation["collectionOptions"])
            )

        object_name = self.get_object_name(operation)
        if object_name == "gridfsbucket":
            # Only create the GridFSBucket when we need it (for the gridfs
            # retryable reads tests).
            obj = GridFSBucket(database, bucket_name=collection.name)
        else:
            objects = {
                "client": database.client,
                "database": database,
                "collection": collection,
                "testRunner": self,
            }
            objects.update(sessions)
            obj = objects[object_name]

        # Combine arguments with options and handle special cases.
        arguments = operation.get("arguments", {})
        arguments.update(arguments.pop("options", {}))
        self.parse_options(arguments)

        cmd = getattr(obj, name)

        with_txn_callback = functools.partial(
            self.run_operations, sessions, original_collection, in_with_transaction=True
        )
        prepare_spec_arguments(operation, arguments, name, sessions, with_txn_callback)

        if name == "run_on_thread":
            args = {"sessions": sessions, "collection": collection}
            args.update(arguments)
            arguments = args

        result = cmd(**dict(arguments))
        # Cleanup open change stream cursors.
        if name == "watch":
            self.addCleanup(result.close)

        if name == "aggregate":
            if arguments["pipeline"] and "$out" in arguments["pipeline"][-1]:
                # Read from the primary to ensure causal consistency.
                out = collection.database.get_collection(
                    arguments["pipeline"][-1]["$out"], read_preference=ReadPreference.PRIMARY
                )
                return out.find()
        if "download" in name:
            result = Binary(result.read())

        if isinstance(result, Cursor) or isinstance(result, CommandCursor):
            return list(result)

        return result

    def allowable_errors(self, op):
        """Allow encryption spec to override expected error classes."""
        return (PyMongoError,)

    def _run_op(self, sessions, collection, op, in_with_transaction):
        expected_result = op.get("result")
        if expect_error(op):
            with self.assertRaises(self.allowable_errors(op), msg=op["name"]) as context:
                out = self.run_operation(sessions, collection, op.copy())
            if expect_error_message(expected_result):
                if isinstance(context.exception, BulkWriteError):
                    errmsg = str(context.exception.details).lower()
                else:
                    errmsg = str(context.exception).lower()
                self.assertIn(expected_result["errorContains"].lower(), errmsg)
            if expect_error_code(expected_result):
                self.assertEqual(
                    expected_result["errorCodeName"], context.exception.details.get("codeName")
                )
            if expect_error_labels_contain(expected_result):
                self.assertErrorLabelsContain(
                    context.exception, expected_result["errorLabelsContain"]
                )
            if expect_error_labels_omit(expected_result):
                self.assertErrorLabelsOmit(context.exception, expected_result["errorLabelsOmit"])

            # Reraise the exception if we're in the with_transaction
            # callback.
            if in_with_transaction:
                raise context.exception
        else:
            result = self.run_operation(sessions, collection, op.copy())
            if "result" in op:
                if op["name"] == "runCommand":
                    self.check_command_result(expected_result, result)
                else:
                    self.check_result(expected_result, result)

    def run_operations(self, sessions, collection, ops, in_with_transaction=False):
        for op in ops:
            self._run_op(sessions, collection, op, in_with_transaction)

    # TODO: factor with test_command_monitoring.py
    def check_events(self, test, listener, session_ids):
        res = listener.results
        if not len(test["expectations"]):
            return

        # Give a nicer message when there are missing or extra events
        cmds = decode_raw([event.command for event in res["started"]])
        self.assertEqual(len(res["started"]), len(test["expectations"]), cmds)
        for i, expectation in enumerate(test["expectations"]):
            event_type = next(iter(expectation))
            event = res["started"][i]

            # The tests substitute 42 for any number other than 0.
            if event.command_name == "getMore" and event.command["getMore"]:
                event.command["getMore"] = Int64(42)
            elif event.command_name == "killCursors":
                event.command["cursors"] = [Int64(42)]
            elif event.command_name == "update":
                # TODO: remove this once PYTHON-1744 is done.
                # Add upsert and multi fields back into expectations.
                updates = expectation[event_type]["command"]["updates"]
                for update in updates:
                    update.setdefault("upsert", False)
                    update.setdefault("multi", False)

            # Replace afterClusterTime: 42 with actual afterClusterTime.
            expected_cmd = expectation[event_type]["command"]
            expected_read_concern = expected_cmd.get("readConcern")
            if expected_read_concern is not None:
                time = expected_read_concern.get("afterClusterTime")
                if time == 42:
                    actual_time = event.command.get("readConcern", {}).get("afterClusterTime")
                    if actual_time is not None:
                        expected_read_concern["afterClusterTime"] = actual_time

            recovery_token = expected_cmd.get("recoveryToken")
            if recovery_token == 42:
                expected_cmd["recoveryToken"] = CompareType(dict)

            # Replace lsid with a name like "session0" to match test.
            if "lsid" in event.command:
                for name, lsid in session_ids.items():
                    if event.command["lsid"] == lsid:
                        event.command["lsid"] = name
                        break

            for attr, expected in expectation[event_type].items():
                actual = getattr(event, attr)
                expected = wrap_types(expected)
                if isinstance(expected, dict):
                    for key, val in expected.items():
                        if val is None:
                            if key in actual:
                                self.fail("Unexpected key [%s] in %r" % (key, actual))
                        elif key not in actual:
                            self.fail("Expected key [%s] in %r" % (key, actual))
                        else:
                            self.assertEqual(
                                val, decode_raw(actual[key]), "Key [%s] in %s" % (key, actual)
                            )
                else:
                    self.assertEqual(actual, expected)

    def maybe_skip_scenario(self, test):
        if test.get("skipReason"):
            self.skipTest(test.get("skipReason"))

    def get_scenario_db_name(self, scenario_def):
        """Allow subclasses to override a test's database name."""
        return scenario_def["database_name"]

    def get_scenario_coll_name(self, scenario_def):
        """Allow subclasses to override a test's collection name."""
        return scenario_def["collection_name"]

    def get_outcome_coll_name(self, outcome, collection):
        """Allow subclasses to override outcome collection."""
        return collection.name

    def run_test_ops(self, sessions, collection, test):
        """Added to allow retryable writes spec to override a test's
        operation."""
        self.run_operations(sessions, collection, test["operations"])

    def parse_client_options(self, opts):
        """Allow encryption spec to override a clientOptions parsing."""
        # Convert test['clientOptions'] to dict to avoid a Jython bug using
        # "**" with ScenarioDict.
        return dict(opts)

    def setup_scenario(self, scenario_def):
        """Allow specs to override a test's setup."""
        db_name = self.get_scenario_db_name(scenario_def)
        coll_name = self.get_scenario_coll_name(scenario_def)
        documents = scenario_def["data"]

        # Setup the collection with as few majority writes as possible.
        db = client_context.client.get_database(db_name)
        coll_exists = bool(db.list_collection_names(filter={"name": coll_name}))
        if coll_exists:
            db[coll_name].delete_many({})
        # Only use majority wc only on the final write.
        wc = WriteConcern(w="majority")
        if documents:
            db.get_collection(coll_name, write_concern=wc).insert_many(documents)
        elif not coll_exists:
            # Ensure collection exists.
            db.create_collection(coll_name, write_concern=wc)

    def run_scenario(self, scenario_def, test):
        self.maybe_skip_scenario(test)

        # Kill all sessions before and after each test to prevent an open
        # transaction (from a test failure) from blocking collection/database
        # operations during test set up and tear down.
        self.kill_all_sessions()
        self.addCleanup(self.kill_all_sessions)
        self.setup_scenario(scenario_def)
        database_name = self.get_scenario_db_name(scenario_def)
        collection_name = self.get_scenario_coll_name(scenario_def)
        # SPEC-1245 workaround StaleDbVersion on distinct
        for c in self.mongos_clients:
            c[database_name][collection_name].distinct("x")

        # Configure the fail point before creating the client.
        if "failPoint" in test:
            fp = test["failPoint"]
            self.set_fail_point(fp)
            self.addCleanup(
                self.set_fail_point, {"configureFailPoint": fp["configureFailPoint"], "mode": "off"}
            )

        listener = OvertCommandListener()
        pool_listener = CMAPListener()
        server_listener = ServerAndTopologyEventListener()
        # Create a new client, to avoid interference from pooled sessions.
        client_options = self.parse_client_options(test["clientOptions"])
        # MMAPv1 does not support retryable writes.
        if client_options.get("retryWrites") is True and client_context.storage_engine == "mmapv1":
            self.skipTest("MMAPv1 does not support retryWrites=True")
        use_multi_mongos = test["useMultipleMongoses"]
        host = None
        if use_multi_mongos:
            if client_context.load_balancer or client_context.serverless:
                host = client_context.MULTI_MONGOS_LB_URI
            elif client_context.is_mongos:
                host = client_context.mongos_seeds()
        client = rs_client(
            h=host, event_listeners=[listener, pool_listener, server_listener], **client_options
        )
        self.scenario_client = client
        self.listener = listener
        self.pool_listener = pool_listener
        self.server_listener = server_listener
        # Close the client explicitly to avoid having too many threads open.
        self.addCleanup(client.close)

        # Create session0 and session1.
        sessions = {}
        session_ids = {}
        for i in range(2):
            # Don't attempt to create sessions if they are not supported by
            # the running server version.
            if not client_context.sessions_enabled:
                break
            session_name = "session%d" % i
            opts = camel_to_snake_args(test["sessionOptions"][session_name])
            if "default_transaction_options" in opts:
                txn_opts = self.parse_options(opts["default_transaction_options"])
                txn_opts = client_session.TransactionOptions(**txn_opts)
                opts["default_transaction_options"] = txn_opts

            s = client.start_session(**dict(opts))

            sessions[session_name] = s
            # Store lsid so we can access it after end_session, in check_events.
            session_ids[session_name] = s.session_id

        self.addCleanup(end_sessions, sessions)

        collection = client[database_name][collection_name]
        self.run_test_ops(sessions, collection, test)

        end_sessions(sessions)

        self.check_events(test, listener, session_ids)

        # Disable fail points.
        if "failPoint" in test:
            fp = test["failPoint"]
            self.set_fail_point({"configureFailPoint": fp["configureFailPoint"], "mode": "off"})

        # Assert final state is expected.
        outcome = test["outcome"]
        expected_c = outcome.get("collection")
        if expected_c is not None:
            outcome_coll_name = self.get_outcome_coll_name(outcome, collection)

            # Read from the primary with local read concern to ensure causal
            # consistency.
            outcome_coll = client_context.client[collection.database.name].get_collection(
                outcome_coll_name,
                read_preference=ReadPreference.PRIMARY,
                read_concern=ReadConcern("local"),
            )
            actual_data = list(outcome_coll.find(sort=[("_id", 1)]))

            # The expected data needs to be the left hand side here otherwise
            # CompareType(Binary) doesn't work.
            self.assertEqual(wrap_types(expected_c["data"]), actual_data)


def expect_any_error(op):
    if isinstance(op, dict):
        return op.get("error")

    return False


def expect_error_message(expected_result):
    if isinstance(expected_result, dict):
        return isinstance(expected_result["errorContains"], str)

    return False


def expect_error_code(expected_result):
    if isinstance(expected_result, dict):
        return expected_result["errorCodeName"]

    return False


def expect_error_labels_contain(expected_result):
    if isinstance(expected_result, dict):
        return expected_result["errorLabelsContain"]

    return False


def expect_error_labels_omit(expected_result):
    if isinstance(expected_result, dict):
        return expected_result["errorLabelsOmit"]

    return False


def expect_error(op):
    expected_result = op.get("result")
    return (
        expect_any_error(op)
        or expect_error_message(expected_result)
        or expect_error_code(expected_result)
        or expect_error_labels_contain(expected_result)
        or expect_error_labels_omit(expected_result)
    )


def end_sessions(sessions):
    for s in sessions.values():
        # Aborts the transaction if it's open.
        s.end_session()


def decode_raw(val):
    """Decode RawBSONDocuments in the given container."""
    if isinstance(val, (list, abc.Mapping)):
        return decode(encode({"v": val}))["v"]
    return val


TYPES = {
    "binData": Binary,
    "long": Int64,
}


def wrap_types(val):
    """Support $$type assertion in command results."""
    if isinstance(val, list):
        return [wrap_types(v) for v in val]
    if isinstance(val, abc.Mapping):
        typ = val.get("$$type")
        if typ:
            return CompareType(TYPES[typ])
        d = {}
        for key in val:
            d[key] = wrap_types(val[key])
        return d
    return val
