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

"""Test the change_stream module."""

import os
import random
import string
import sys
import threading
import time
import uuid
from itertools import product
from typing import no_type_check

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.unified_format import generate_test_classes
from test.utils import (
    AllowListEventListener,
    EventListener,
    rs_or_single_client,
    wait_until,
)

from bson import SON, ObjectId, Timestamp, encode
from bson.binary import ALL_UUID_REPRESENTATIONS, PYTHON_LEGACY, STANDARD, Binary
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument
from pymongo import MongoClient
from pymongo.command_cursor import CommandCursor
from pymongo.errors import (
    InvalidOperation,
    OperationFailure,
    ServerSelectionTimeoutError,
)
from pymongo.message import _CursorAddress
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern


class TestChangeStreamBase(IntegrationTest):
    RUN_ON_LOAD_BALANCER = True

    def change_stream_with_client(self, client, *args, **kwargs):
        """Create a change stream using the given client and return it."""
        raise NotImplementedError

    def change_stream(self, *args, **kwargs):
        """Create a change stream using the default client and return it."""
        return self.change_stream_with_client(self.client, *args, **kwargs)

    def client_with_listener(self, *commands):
        """Return a client with a AllowListEventListener."""
        listener = AllowListEventListener(*commands)
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        return client, listener

    def watched_collection(self, *args, **kwargs):
        """Return a collection that is watched by self.change_stream()."""
        # Construct a unique collection for each test.
        collname = ".".join(self.id().rsplit(".", 2)[1:])
        return self.db.get_collection(collname, *args, **kwargs)

    def generate_invalidate_event(self, change_stream):
        """Cause a change stream invalidate event."""
        raise NotImplementedError

    def generate_unique_collnames(self, numcolls):
        """Generate numcolls collection names unique to a test."""
        collnames = []
        for idx in range(1, numcolls + 1):
            collnames.append(self.id() + "_" + str(idx))
        return collnames

    def get_resume_token(self, invalidate=False):
        """Get a resume token to use for starting a change stream."""
        # Ensure targeted collection exists before starting.
        coll = self.watched_collection(write_concern=WriteConcern("majority"))
        coll.insert_one({})

        if invalidate:
            with self.change_stream([{"$match": {"operationType": "invalidate"}}]) as cs:
                if isinstance(cs._target, MongoClient):
                    self.skipTest("cluster-level change streams cannot be invalidated")
                self.generate_invalidate_event(cs)
                return cs.next()["_id"]
        else:
            with self.change_stream() as cs:
                coll.insert_one({"data": 1})
                return cs.next()["_id"]

    def get_start_at_operation_time(self):
        """Get an operationTime. Advances the operation clock beyond the most
        recently returned timestamp."""
        optime = self.client.admin.command("ping")["operationTime"]
        return Timestamp(optime.time, optime.inc + 1)

    def insert_one_and_check(self, change_stream, doc):
        """Insert a document and check that it shows up in the change stream."""
        raise NotImplementedError

    def kill_change_stream_cursor(self, change_stream):
        """Cause a cursor not found error on the next getMore."""
        cursor = change_stream._cursor
        address = _CursorAddress(cursor.address, cursor._CommandCursor__ns)
        client = self.watched_collection().database.client
        client._close_cursor_now(cursor.cursor_id, address)


class APITestsMixin(object):
    @no_type_check
    def test_watch(self):
        with self.change_stream(
            [{"$project": {"foo": 0}}],
            full_document="updateLookup",
            max_await_time_ms=1000,
            batch_size=100,
        ) as change_stream:
            self.assertEqual([{"$project": {"foo": 0}}], change_stream._pipeline)
            self.assertEqual("updateLookup", change_stream._full_document)
            self.assertEqual(1000, change_stream._max_await_time_ms)
            self.assertEqual(100, change_stream._batch_size)
            self.assertIsInstance(change_stream._cursor, CommandCursor)
            self.assertEqual(1000, change_stream._cursor._CommandCursor__max_await_time_ms)
            self.watched_collection(write_concern=WriteConcern("majority")).insert_one({})
            _ = change_stream.next()
            resume_token = change_stream.resume_token
        with self.assertRaises(TypeError):
            self.change_stream(pipeline={})
        with self.assertRaises(TypeError):
            self.change_stream(full_document={})
        # No Error.
        with self.change_stream(resume_after=resume_token):
            pass

    @no_type_check
    def test_try_next(self):
        # ChangeStreams only read majority committed data so use w:majority.
        coll = self.watched_collection().with_options(write_concern=WriteConcern("majority"))
        coll.drop()
        coll.insert_one({})
        self.addCleanup(coll.drop)
        with self.change_stream(max_await_time_ms=250) as stream:
            self.assertIsNone(stream.try_next())  # No changes initially.
            coll.insert_one({})  # Generate a change.
            # On sharded clusters, even majority-committed changes only show
            # up once an event that sorts after it shows up on the other
            # shard. So, we wait on try_next to eventually return changes.
            wait_until(lambda: stream.try_next() is not None, "get change from try_next")

    @no_type_check
    def test_try_next_runs_one_getmore(self):
        listener = EventListener()
        client = rs_or_single_client(event_listeners=[listener])
        # Connect to the cluster.
        client.admin.command("ping")
        listener.results.clear()
        # ChangeStreams only read majority committed data so use w:majority.
        coll = self.watched_collection().with_options(write_concern=WriteConcern("majority"))
        coll.drop()
        # Create the watched collection before starting the change stream to
        # skip any "create" events.
        coll.insert_one({"_id": 1})
        self.addCleanup(coll.drop)
        with self.change_stream_with_client(client, max_await_time_ms=250) as stream:
            self.assertEqual(listener.started_command_names(), ["aggregate"])
            listener.results.clear()

            # Confirm that only a single getMore is run even when no documents
            # are returned.
            self.assertIsNone(stream.try_next())
            self.assertEqual(listener.started_command_names(), ["getMore"])
            listener.results.clear()
            self.assertIsNone(stream.try_next())
            self.assertEqual(listener.started_command_names(), ["getMore"])
            listener.results.clear()

            # Get at least one change before resuming.
            coll.insert_one({"_id": 2})
            wait_until(lambda: stream.try_next() is not None, "get change from try_next")
            listener.results.clear()

            # Cause the next request to initiate the resume process.
            self.kill_change_stream_cursor(stream)
            listener.results.clear()

            # The sequence should be:
            # - getMore, fail
            # - resume with aggregate command
            # - no results, return immediately without another getMore
            self.assertIsNone(stream.try_next())
            self.assertEqual(listener.started_command_names(), ["getMore", "aggregate"])
            listener.results.clear()

            # Stream still works after a resume.
            coll.insert_one({"_id": 3})
            wait_until(lambda: stream.try_next() is not None, "get change from try_next")
            self.assertEqual(set(listener.started_command_names()), set(["getMore"]))
            self.assertIsNone(stream.try_next())

    @no_type_check
    def test_batch_size_is_honored(self):
        listener = EventListener()
        client = rs_or_single_client(event_listeners=[listener])
        # Connect to the cluster.
        client.admin.command("ping")
        listener.results.clear()
        # ChangeStreams only read majority committed data so use w:majority.
        coll = self.watched_collection().with_options(write_concern=WriteConcern("majority"))
        coll.drop()
        # Create the watched collection before starting the change stream to
        # skip any "create" events.
        coll.insert_one({"_id": 1})
        self.addCleanup(coll.drop)
        # Expected batchSize.
        expected = {"batchSize": 23}
        with self.change_stream_with_client(client, max_await_time_ms=250, batch_size=23) as stream:
            # Confirm that batchSize is honored for initial batch.
            cmd = listener.results["started"][0].command
            self.assertEqual(cmd["cursor"], expected)
            listener.results.clear()
            # Confirm that batchSize is honored by getMores.
            self.assertIsNone(stream.try_next())
            cmd = listener.results["started"][0].command
            key = next(iter(expected))
            self.assertEqual(expected[key], cmd[key])

    # $changeStream.startAtOperationTime was added in 4.0.0.
    @no_type_check
    @client_context.require_version_min(4, 0, 0)
    def test_start_at_operation_time(self):
        optime = self.get_start_at_operation_time()

        coll = self.watched_collection(write_concern=WriteConcern("majority"))
        ndocs = 3
        coll.insert_many([{"data": i} for i in range(ndocs)])

        with self.change_stream(start_at_operation_time=optime) as cs:
            for i in range(ndocs):
                cs.next()

    @no_type_check
    def _test_full_pipeline(self, expected_cs_stage):
        client, listener = self.client_with_listener("aggregate")
        results = listener.results
        with self.change_stream_with_client(client, [{"$project": {"foo": 0}}]) as _:
            pass

        self.assertEqual(1, len(results["started"]))
        command = results["started"][0]
        self.assertEqual("aggregate", command.command_name)
        self.assertEqual(
            [{"$changeStream": expected_cs_stage}, {"$project": {"foo": 0}}],
            command.command["pipeline"],
        )

    @no_type_check
    def test_full_pipeline(self):
        """$changeStream must be the first stage in a change stream pipeline
        sent to the server.
        """
        self._test_full_pipeline({})

    @no_type_check
    def test_iteration(self):
        with self.change_stream(batch_size=2) as change_stream:
            num_inserted = 10
            self.watched_collection().insert_many([{} for _ in range(num_inserted)])
            inserts_received = 0
            for change in change_stream:
                self.assertEqual(change["operationType"], "insert")
                inserts_received += 1
                if inserts_received == num_inserted:
                    break
            self._test_invalidate_stops_iteration(change_stream)

    @no_type_check
    def _test_next_blocks(self, change_stream):
        inserted_doc = {"_id": ObjectId()}
        changes = []
        t = threading.Thread(target=lambda: changes.append(change_stream.next()))
        t.start()
        # Sleep for a bit to prove that the call to next() blocks.
        time.sleep(1)
        self.assertTrue(t.is_alive())
        self.assertFalse(changes)
        self.watched_collection().insert_one(inserted_doc)
        # Join with large timeout to give the server time to return the change,
        # in particular for shard clusters.
        t.join(30)
        self.assertFalse(t.is_alive())
        self.assertEqual(1, len(changes))
        self.assertEqual(changes[0]["operationType"], "insert")
        self.assertEqual(changes[0]["fullDocument"], inserted_doc)

    @no_type_check
    def test_next_blocks(self):
        """Test that next blocks until a change is readable"""
        # Use a short await time to speed up the test.
        with self.change_stream(max_await_time_ms=250) as change_stream:
            self._test_next_blocks(change_stream)

    @no_type_check
    def test_aggregate_cursor_blocks(self):
        """Test that an aggregate cursor blocks until a change is readable."""
        with self.watched_collection().aggregate(
            [{"$changeStream": {}}], maxAwaitTimeMS=250
        ) as change_stream:
            self._test_next_blocks(change_stream)

    @no_type_check
    def test_concurrent_close(self):
        """Ensure a ChangeStream can be closed from another thread."""
        # Use a short await time to speed up the test.
        with self.change_stream(max_await_time_ms=250) as change_stream:

            def iterate_cursor():
                for _ in change_stream:
                    pass

            t = threading.Thread(target=iterate_cursor)
            t.start()
            self.watched_collection().insert_one({})
            time.sleep(1)
            change_stream.close()
            t.join(3)
            self.assertFalse(t.is_alive())

    @no_type_check
    def test_unknown_full_document(self):
        """Must rely on the server to raise an error on unknown fullDocument."""
        try:
            with self.change_stream(full_document="notValidatedByPyMongo"):
                pass
        except OperationFailure:
            pass

    @no_type_check
    def test_change_operations(self):
        """Test each operation type."""
        expected_ns = {
            "db": self.watched_collection().database.name,
            "coll": self.watched_collection().name,
        }
        with self.change_stream() as change_stream:
            # Insert.
            inserted_doc = {"_id": ObjectId(), "foo": "bar"}
            self.watched_collection().insert_one(inserted_doc)
            change = change_stream.next()
            self.assertTrue(change["_id"])
            self.assertEqual(change["operationType"], "insert")
            self.assertEqual(change["ns"], expected_ns)
            self.assertEqual(change["fullDocument"], inserted_doc)
            # Update.
            update_spec = {"$set": {"new": 1}, "$unset": {"foo": 1}}
            self.watched_collection().update_one(inserted_doc, update_spec)
            change = change_stream.next()
            self.assertTrue(change["_id"])
            self.assertEqual(change["operationType"], "update")
            self.assertEqual(change["ns"], expected_ns)
            self.assertNotIn("fullDocument", change)

            expected_update_description = {"updatedFields": {"new": 1}, "removedFields": ["foo"]}
            if client_context.version.at_least(4, 5, 0):
                expected_update_description["truncatedArrays"] = []
            self.assertEqual(expected_update_description, change["updateDescription"])
            # Replace.
            self.watched_collection().replace_one({"new": 1}, {"foo": "bar"})
            change = change_stream.next()
            self.assertTrue(change["_id"])
            self.assertEqual(change["operationType"], "replace")
            self.assertEqual(change["ns"], expected_ns)
            self.assertEqual(change["fullDocument"], inserted_doc)
            # Delete.
            self.watched_collection().delete_one({"foo": "bar"})
            change = change_stream.next()
            self.assertTrue(change["_id"])
            self.assertEqual(change["operationType"], "delete")
            self.assertEqual(change["ns"], expected_ns)
            self.assertNotIn("fullDocument", change)
            # Invalidate.
            self._test_get_invalidate_event(change_stream)

    @no_type_check
    @client_context.require_version_min(4, 1, 1)
    def test_start_after(self):
        resume_token = self.get_resume_token(invalidate=True)

        # resume_after cannot resume after invalidate.
        with self.assertRaises(OperationFailure):
            self.change_stream(resume_after=resume_token)

        # start_after can resume after invalidate.
        with self.change_stream(start_after=resume_token) as change_stream:
            self.watched_collection().insert_one({"_id": 2})
            change = change_stream.next()
            self.assertEqual(change["operationType"], "insert")
            self.assertEqual(change["fullDocument"], {"_id": 2})

    @no_type_check
    @client_context.require_version_min(4, 1, 1)
    def test_start_after_resume_process_with_changes(self):
        resume_token = self.get_resume_token(invalidate=True)

        with self.change_stream(start_after=resume_token, max_await_time_ms=250) as change_stream:
            self.watched_collection().insert_one({"_id": 2})
            change = change_stream.next()
            self.assertEqual(change["operationType"], "insert")
            self.assertEqual(change["fullDocument"], {"_id": 2})

            self.assertIsNone(change_stream.try_next())
            self.kill_change_stream_cursor(change_stream)

            self.watched_collection().insert_one({"_id": 3})
            change = change_stream.next()
            self.assertEqual(change["operationType"], "insert")
            self.assertEqual(change["fullDocument"], {"_id": 3})

    @no_type_check
    @client_context.require_no_mongos  # Remove after SERVER-41196
    @client_context.require_version_min(4, 1, 1)
    def test_start_after_resume_process_without_changes(self):
        resume_token = self.get_resume_token(invalidate=True)

        with self.change_stream(start_after=resume_token, max_await_time_ms=250) as change_stream:
            self.assertIsNone(change_stream.try_next())
            self.kill_change_stream_cursor(change_stream)

            self.watched_collection().insert_one({"_id": 2})
            change = change_stream.next()
            self.assertEqual(change["operationType"], "insert")
            self.assertEqual(change["fullDocument"], {"_id": 2})


class ProseSpecTestsMixin(object):
    @no_type_check
    def _client_with_listener(self, *commands):
        listener = AllowListEventListener(*commands)
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        return client, listener

    @no_type_check
    def _populate_and_exhaust_change_stream(self, change_stream, batch_size=3):
        self.watched_collection().insert_many([{"data": k} for k in range(batch_size)])
        for _ in range(batch_size):
            change = next(change_stream)
        return change

    def _get_expected_resume_token_legacy(self, stream, listener, previous_change=None):
        """Predicts what the resume token should currently be for server
        versions that don't support postBatchResumeToken. Assumes the stream
        has never returned any changes if previous_change is None."""
        if previous_change is None:
            agg_cmd = listener.results["started"][0]
            stage = agg_cmd.command["pipeline"][0]["$changeStream"]
            return stage.get("resumeAfter") or stage.get("startAfter")

        return previous_change["_id"]

    def _get_expected_resume_token(self, stream, listener, previous_change=None):
        """Predicts what the resume token should currently be for server
        versions that support postBatchResumeToken. Assumes the stream has
        never returned any changes if previous_change is None. Assumes
        listener is a AllowListEventListener that listens for aggregate and
        getMore commands."""
        if previous_change is None or stream._cursor._has_next():
            token = self._get_expected_resume_token_legacy(stream, listener, previous_change)
            if token is not None:
                return token

        response = listener.results["succeeded"][-1].reply
        return response["cursor"]["postBatchResumeToken"]

    @no_type_check
    def _test_raises_error_on_missing_id(self, expected_exception, expected_exception2):
        """ChangeStream will raise an exception if the server response is
        missing the resume token.
        """
        with self.change_stream([{"$project": {"_id": 0}}]) as change_stream:
            self.watched_collection().insert_one({})
            with self.assertRaises(expected_exception):
                next(change_stream)
            with self.assertRaises(expected_exception2):
                next(change_stream)

    @no_type_check
    def _test_update_resume_token(self, expected_rt_getter):
        """ChangeStream must continuously track the last seen resumeToken."""
        client, listener = self._client_with_listener("aggregate", "getMore")
        coll = self.watched_collection(write_concern=WriteConcern("majority"))
        with self.change_stream_with_client(client) as change_stream:
            self.assertEqual(
                change_stream.resume_token, expected_rt_getter(change_stream, listener)
            )
            for _ in range(3):
                coll.insert_one({})
                change = next(change_stream)
                self.assertEqual(
                    change_stream.resume_token, expected_rt_getter(change_stream, listener, change)
                )

    # Prose test no. 1
    @client_context.require_version_min(4, 0, 7)
    def test_update_resume_token(self):
        self._test_update_resume_token(self._get_expected_resume_token)

    # Prose test no. 1
    @client_context.require_version_max(4, 0, 7)
    def test_update_resume_token_legacy(self):
        self._test_update_resume_token(self._get_expected_resume_token_legacy)

    # Prose test no. 2
    @client_context.require_version_min(4, 1, 8)
    def test_raises_error_on_missing_id_418plus(self):
        # Server returns an error on 4.1.8+, subsequent next() resumes and gets the same error.
        self._test_raises_error_on_missing_id(OperationFailure, OperationFailure)

    # Prose test no. 2
    @client_context.require_version_max(4, 1, 8)
    def test_raises_error_on_missing_id_418minus(self):
        # PyMongo raises an error, closes the cursor, subsequent next() raises StopIteration.
        self._test_raises_error_on_missing_id(InvalidOperation, StopIteration)

    # Prose test no. 3
    @no_type_check
    def test_resume_on_error(self):
        with self.change_stream() as change_stream:
            self.insert_one_and_check(change_stream, {"_id": 1})
            # Cause a cursor not found error on the next getMore.
            self.kill_change_stream_cursor(change_stream)
            self.insert_one_and_check(change_stream, {"_id": 2})

    # Prose test no. 4
    @no_type_check
    @client_context.require_failCommand_fail_point
    def test_no_resume_attempt_if_aggregate_command_fails(self):
        # Set non-retryable error on aggregate command.
        fail_point = {"mode": {"times": 1}, "data": {"errorCode": 2, "failCommands": ["aggregate"]}}
        client, listener = self._client_with_listener("aggregate", "getMore")
        with self.fail_point(fail_point):
            try:
                _ = self.change_stream_with_client(client)
            except OperationFailure:
                pass

        # Driver should have attempted aggregate command only once.
        self.assertEqual(len(listener.results["started"]), 1)
        self.assertEqual(listener.results["started"][0].command_name, "aggregate")

    # Prose test no. 5 - REMOVED
    # Prose test no. 6 - SKIPPED
    # Reason: readPreference is not configurable using the watch() helpers
    #   so we can skip this test. Also, PyMongo performs server selection for
    #   each operation which ensure compliance with this prose test.

    # Prose test no. 7
    @no_type_check
    def test_initial_empty_batch(self):
        with self.change_stream() as change_stream:
            # The first batch should be empty.
            self.assertFalse(change_stream._cursor._has_next())
            cursor_id = change_stream._cursor.cursor_id
            self.assertTrue(cursor_id)
            self.insert_one_and_check(change_stream, {})
            # Make sure we're still using the same cursor.
            self.assertEqual(cursor_id, change_stream._cursor.cursor_id)

    # Prose test no. 8
    @no_type_check
    def test_kill_cursors(self):
        def raise_error():
            raise ServerSelectionTimeoutError("mock error")

        with self.change_stream() as change_stream:
            self.insert_one_and_check(change_stream, {"_id": 1})
            # Cause a cursor not found error on the next getMore.
            cursor = change_stream._cursor
            self.kill_change_stream_cursor(change_stream)
            cursor.close = raise_error
            self.insert_one_and_check(change_stream, {"_id": 2})

    # Prose test no. 9
    @no_type_check
    @client_context.require_version_min(4, 0, 0)
    @client_context.require_version_max(4, 0, 7)
    def test_start_at_operation_time_caching(self):
        # Case 1: change stream not started with startAtOperationTime
        client, listener = self.client_with_listener("aggregate")
        with self.change_stream_with_client(client) as cs:
            self.kill_change_stream_cursor(cs)
            cs.try_next()
        cmd = listener.results["started"][-1].command
        self.assertIsNotNone(cmd["pipeline"][0]["$changeStream"].get("startAtOperationTime"))

        # Case 2: change stream started with startAtOperationTime
        listener.results.clear()
        optime = self.get_start_at_operation_time()
        with self.change_stream_with_client(client, start_at_operation_time=optime) as cs:
            self.kill_change_stream_cursor(cs)
            cs.try_next()
        cmd = listener.results["started"][-1].command
        self.assertEqual(
            cmd["pipeline"][0]["$changeStream"].get("startAtOperationTime"),
            optime,
            str([k.command for k in listener.results["started"]]),
        )

    # Prose test no. 10 - SKIPPED
    # This test is identical to prose test no. 3.

    # Prose test no. 11
    @no_type_check
    @client_context.require_version_min(4, 0, 7)
    def test_resumetoken_empty_batch(self):
        client, listener = self._client_with_listener("getMore")
        with self.change_stream_with_client(client) as change_stream:
            self.assertIsNone(change_stream.try_next())
            resume_token = change_stream.resume_token

        response = listener.results["succeeded"][0].reply
        self.assertEqual(resume_token, response["cursor"]["postBatchResumeToken"])

    # Prose test no. 11
    @no_type_check
    @client_context.require_version_min(4, 0, 7)
    def test_resumetoken_exhausted_batch(self):
        client, listener = self._client_with_listener("getMore")
        with self.change_stream_with_client(client) as change_stream:
            self._populate_and_exhaust_change_stream(change_stream)
            resume_token = change_stream.resume_token

        response = listener.results["succeeded"][-1].reply
        self.assertEqual(resume_token, response["cursor"]["postBatchResumeToken"])

    # Prose test no. 12
    @no_type_check
    @client_context.require_version_max(4, 0, 7)
    def test_resumetoken_empty_batch_legacy(self):
        resume_point = self.get_resume_token()

        # Empty resume token when neither resumeAfter or startAfter specified.
        with self.change_stream() as change_stream:
            change_stream.try_next()
            self.assertIsNone(change_stream.resume_token)

        # Resume token value is same as resumeAfter.
        with self.change_stream(resume_after=resume_point) as change_stream:
            change_stream.try_next()
            resume_token = change_stream.resume_token
            self.assertEqual(resume_token, resume_point)

    # Prose test no. 12
    @no_type_check
    @client_context.require_version_max(4, 0, 7)
    def test_resumetoken_exhausted_batch_legacy(self):
        # Resume token is _id of last change.
        with self.change_stream() as change_stream:
            change = self._populate_and_exhaust_change_stream(change_stream)
            self.assertEqual(change_stream.resume_token, change["_id"])
            resume_point = change["_id"]

        # Resume token is _id of last change even if resumeAfter is specified.
        with self.change_stream(resume_after=resume_point) as change_stream:
            change = self._populate_and_exhaust_change_stream(change_stream)
            self.assertEqual(change_stream.resume_token, change["_id"])

    # Prose test no. 13
    @no_type_check
    def test_resumetoken_partially_iterated_batch(self):
        # When batch has been iterated up to but not including the last element.
        # Resume token should be _id of previous change document.
        with self.change_stream() as change_stream:
            self.watched_collection(write_concern=WriteConcern("majority")).insert_many(
                [{"data": k} for k in range(3)]
            )
            for _ in range(2):
                change = next(change_stream)
            resume_token = change_stream.resume_token

        self.assertEqual(resume_token, change["_id"])

    @no_type_check
    def _test_resumetoken_uniterated_nonempty_batch(self, resume_option):
        # When the batch is not empty and hasn't been iterated at all.
        # Resume token should be same as the resume option used.
        resume_point = self.get_resume_token()

        # Insert some documents so that firstBatch isn't empty.
        self.watched_collection(write_concern=WriteConcern("majority")).insert_many(
            [{"a": 1}, {"b": 2}, {"c": 3}]
        )

        # Resume token should be same as the resume option.
        with self.change_stream(**{resume_option: resume_point}) as change_stream:
            self.assertTrue(change_stream._cursor._has_next())
            resume_token = change_stream.resume_token
        self.assertEqual(resume_token, resume_point)

    # Prose test no. 14
    @no_type_check
    @client_context.require_no_mongos
    def test_resumetoken_uniterated_nonempty_batch_resumeafter(self):
        self._test_resumetoken_uniterated_nonempty_batch("resume_after")

    # Prose test no. 14
    @no_type_check
    @client_context.require_no_mongos
    @client_context.require_version_min(4, 1, 1)
    def test_resumetoken_uniterated_nonempty_batch_startafter(self):
        self._test_resumetoken_uniterated_nonempty_batch("start_after")

    # Prose test no. 17
    @no_type_check
    @client_context.require_version_min(4, 1, 1)
    def test_startafter_resume_uses_startafter_after_empty_getMore(self):
        # Resume should use startAfter after no changes have been returned.
        resume_point = self.get_resume_token()

        client, listener = self._client_with_listener("aggregate")
        with self.change_stream_with_client(client, start_after=resume_point) as change_stream:
            self.assertFalse(change_stream._cursor._has_next())  # No changes
            change_stream.try_next()  # No changes
            self.kill_change_stream_cursor(change_stream)
            change_stream.try_next()  # Resume attempt

        response = listener.results["started"][-1]
        self.assertIsNone(response.command["pipeline"][0]["$changeStream"].get("resumeAfter"))
        self.assertIsNotNone(response.command["pipeline"][0]["$changeStream"].get("startAfter"))

    # Prose test no. 18
    @no_type_check
    @client_context.require_version_min(4, 1, 1)
    def test_startafter_resume_uses_resumeafter_after_nonempty_getMore(self):
        # Resume should use resumeAfter after some changes have been returned.
        resume_point = self.get_resume_token()

        client, listener = self._client_with_listener("aggregate")
        with self.change_stream_with_client(client, start_after=resume_point) as change_stream:
            self.assertFalse(change_stream._cursor._has_next())  # No changes
            self.watched_collection().insert_one({})
            next(change_stream)  # Changes
            self.kill_change_stream_cursor(change_stream)
            change_stream.try_next()  # Resume attempt

        response = listener.results["started"][-1]
        self.assertIsNotNone(response.command["pipeline"][0]["$changeStream"].get("resumeAfter"))
        self.assertIsNone(response.command["pipeline"][0]["$changeStream"].get("startAfter"))


class TestClusterChangeStream(TestChangeStreamBase, APITestsMixin):
    dbs: list

    @classmethod
    @client_context.require_version_min(4, 0, 0, -1)
    @client_context.require_no_mmap
    @client_context.require_no_standalone
    def setUpClass(cls):
        super(TestClusterChangeStream, cls).setUpClass()
        cls.dbs = [cls.db, cls.client.pymongo_test_2]

    @classmethod
    def tearDownClass(cls):
        for db in cls.dbs:
            cls.client.drop_database(db)
        super(TestClusterChangeStream, cls).tearDownClass()

    def change_stream_with_client(self, client, *args, **kwargs):
        return client.watch(*args, **kwargs)

    def generate_invalidate_event(self, change_stream):
        self.skipTest("cluster-level change streams cannot be invalidated")

    def _test_get_invalidate_event(self, change_stream):
        # Cluster-level change streams don't get invalidated.
        pass

    def _test_invalidate_stops_iteration(self, change_stream):
        # Cluster-level change streams don't get invalidated.
        pass

    def _insert_and_check(self, change_stream, db, collname, doc):
        coll = db[collname]
        coll.insert_one(doc)
        change = next(change_stream)
        self.assertEqual(change["operationType"], "insert")
        self.assertEqual(change["ns"], {"db": db.name, "coll": collname})
        self.assertEqual(change["fullDocument"], doc)

    def insert_one_and_check(self, change_stream, doc):
        db = random.choice(self.dbs)
        collname = self.id()
        self._insert_and_check(change_stream, db, collname, doc)

    def test_simple(self):
        collnames = self.generate_unique_collnames(3)
        with self.change_stream() as change_stream:
            for db, collname in product(self.dbs, collnames):
                self._insert_and_check(change_stream, db, collname, {"_id": collname})

    def test_aggregate_cursor_blocks(self):
        """Test that an aggregate cursor blocks until a change is readable."""
        with self.client.admin.aggregate(
            [{"$changeStream": {"allChangesForCluster": True}}], maxAwaitTimeMS=250
        ) as change_stream:
            self._test_next_blocks(change_stream)

    def test_full_pipeline(self):
        """$changeStream must be the first stage in a change stream pipeline
        sent to the server.
        """
        self._test_full_pipeline({"allChangesForCluster": True})


class TestDatabaseChangeStream(TestChangeStreamBase, APITestsMixin):
    @classmethod
    @client_context.require_version_min(4, 0, 0, -1)
    @client_context.require_no_mmap
    @client_context.require_no_standalone
    def setUpClass(cls):
        super(TestDatabaseChangeStream, cls).setUpClass()

    def change_stream_with_client(self, client, *args, **kwargs):
        return client[self.db.name].watch(*args, **kwargs)

    def generate_invalidate_event(self, change_stream):
        # Dropping the database invalidates the change stream.
        change_stream._client.drop_database(self.db.name)

    def _test_get_invalidate_event(self, change_stream):
        # Cache collection names.
        dropped_colls = self.db.list_collection_names()
        # Drop the watched database to get an invalidate event.
        self.generate_invalidate_event(change_stream)
        change = change_stream.next()
        # 4.1+ returns "drop" events for each collection in dropped database
        # and a "dropDatabase" event for the database itself.
        if change["operationType"] == "drop":
            self.assertTrue(change["_id"])
            for _ in range(len(dropped_colls)):
                ns = change["ns"]
                self.assertEqual(ns["db"], change_stream._target.name)
                self.assertIn(ns["coll"], dropped_colls)
                change = change_stream.next()
            self.assertEqual(change["operationType"], "dropDatabase")
            self.assertTrue(change["_id"])
            self.assertEqual(change["ns"], {"db": change_stream._target.name})
            # Get next change.
            change = change_stream.next()
        self.assertTrue(change["_id"])
        self.assertEqual(change["operationType"], "invalidate")
        self.assertNotIn("ns", change)
        self.assertNotIn("fullDocument", change)
        # The ChangeStream should be dead.
        with self.assertRaises(StopIteration):
            change_stream.next()

    def _test_invalidate_stops_iteration(self, change_stream):
        # Drop the watched database to get an invalidate event.
        change_stream._client.drop_database(self.db.name)
        # Check drop and dropDatabase events.
        for change in change_stream:
            self.assertIn(change["operationType"], ("drop", "dropDatabase", "invalidate"))
        # Last change must be invalidate.
        self.assertEqual(change["operationType"], "invalidate")
        # Change stream must not allow further iteration.
        with self.assertRaises(StopIteration):
            change_stream.next()
        with self.assertRaises(StopIteration):
            next(change_stream)

    def _insert_and_check(self, change_stream, collname, doc):
        coll = self.db[collname]
        coll.insert_one(doc)
        change = next(change_stream)
        self.assertEqual(change["operationType"], "insert")
        self.assertEqual(change["ns"], {"db": self.db.name, "coll": collname})
        self.assertEqual(change["fullDocument"], doc)

    def insert_one_and_check(self, change_stream, doc):
        self._insert_and_check(change_stream, self.id(), doc)

    def test_simple(self):
        collnames = self.generate_unique_collnames(3)
        with self.change_stream() as change_stream:
            for collname in collnames:
                self._insert_and_check(
                    change_stream, collname, {"_id": Binary.from_uuid(uuid.uuid4())}
                )

    def test_isolation(self):
        # Ensure inserts to other dbs don't show up in our ChangeStream.
        other_db = self.client.pymongo_test_temp
        self.assertNotEqual(other_db, self.db, msg="Isolation must be tested on separate DBs")
        collname = self.id()
        with self.change_stream() as change_stream:
            other_db[collname].insert_one({"_id": Binary.from_uuid(uuid.uuid4())})
            self._insert_and_check(change_stream, collname, {"_id": Binary.from_uuid(uuid.uuid4())})
        self.client.drop_database(other_db)


class TestCollectionChangeStream(TestChangeStreamBase, APITestsMixin, ProseSpecTestsMixin):
    @classmethod
    @client_context.require_version_min(3, 5, 11)
    @client_context.require_no_mmap
    @client_context.require_no_standalone
    def setUpClass(cls):
        super(TestCollectionChangeStream, cls).setUpClass()

    def setUp(self):
        # Use a new collection for each test.
        self.watched_collection().drop()
        self.watched_collection().insert_one({})

    def change_stream_with_client(self, client, *args, **kwargs):
        return (
            client[self.db.name]
            .get_collection(self.watched_collection().name)
            .watch(*args, **kwargs)
        )

    def generate_invalidate_event(self, change_stream):
        # Dropping the collection invalidates the change stream.
        change_stream._target.drop()

    def _test_invalidate_stops_iteration(self, change_stream):
        self.generate_invalidate_event(change_stream)
        # Check drop and dropDatabase events.
        for change in change_stream:
            self.assertIn(change["operationType"], ("drop", "invalidate"))
        # Last change must be invalidate.
        self.assertEqual(change["operationType"], "invalidate")
        # Change stream must not allow further iteration.
        with self.assertRaises(StopIteration):
            change_stream.next()
        with self.assertRaises(StopIteration):
            next(change_stream)

    def _test_get_invalidate_event(self, change_stream):
        # Drop the watched database to get an invalidate event.
        change_stream._target.drop()
        change = change_stream.next()
        # 4.1+ returns a "drop" change document.
        if change["operationType"] == "drop":
            self.assertTrue(change["_id"])
            self.assertEqual(
                change["ns"],
                {"db": change_stream._target.database.name, "coll": change_stream._target.name},
            )
            # Last change should be invalidate.
            change = change_stream.next()
        self.assertTrue(change["_id"])
        self.assertEqual(change["operationType"], "invalidate")
        self.assertNotIn("ns", change)
        self.assertNotIn("fullDocument", change)
        # The ChangeStream should be dead.
        with self.assertRaises(StopIteration):
            change_stream.next()

    def insert_one_and_check(self, change_stream, doc):
        self.watched_collection().insert_one(doc)
        change = next(change_stream)
        self.assertEqual(change["operationType"], "insert")
        self.assertEqual(
            change["ns"],
            {"db": self.watched_collection().database.name, "coll": self.watched_collection().name},
        )
        self.assertEqual(change["fullDocument"], doc)

    def test_raw(self):
        """Test with RawBSONDocument."""
        raw_coll = self.watched_collection(codec_options=DEFAULT_RAW_BSON_OPTIONS)
        with raw_coll.watch() as change_stream:
            raw_doc = RawBSONDocument(encode({"_id": 1}))
            self.watched_collection().insert_one(raw_doc)
            change = next(change_stream)
            self.assertIsInstance(change, RawBSONDocument)
            self.assertEqual(change["operationType"], "insert")
            self.assertEqual(change["ns"]["db"], self.watched_collection().database.name)
            self.assertEqual(change["ns"]["coll"], self.watched_collection().name)
            self.assertEqual(change["fullDocument"], raw_doc)

    def test_uuid_representations(self):
        """Test with uuid document _ids and different uuid_representation."""
        for uuid_representation in ALL_UUID_REPRESENTATIONS:
            for id_subtype in (STANDARD, PYTHON_LEGACY):
                options = self.watched_collection().codec_options.with_options(
                    uuid_representation=uuid_representation
                )
                coll = self.watched_collection(codec_options=options)
                with coll.watch() as change_stream:
                    coll.insert_one({"_id": Binary(uuid.uuid4().bytes, id_subtype)})
                    _ = change_stream.next()
                    resume_token = change_stream.resume_token

                # Should not error.
                coll.watch(resume_after=resume_token)

    def test_document_id_order(self):
        """Test with document _ids that need their order preserved."""
        random_keys = random.sample(string.ascii_letters, len(string.ascii_letters))
        random_doc = {"_id": SON([(key, key) for key in random_keys])}
        for document_class in (dict, SON, RawBSONDocument):
            options = self.watched_collection().codec_options.with_options(
                document_class=document_class
            )
            coll = self.watched_collection(codec_options=options)
            with coll.watch() as change_stream:
                coll.insert_one(random_doc)
                _ = change_stream.next()
                resume_token = change_stream.resume_token

            # The resume token is always a document.
            self.assertIsInstance(resume_token, document_class)
            # Should not error.
            coll.watch(resume_after=resume_token)
            coll.delete_many({})

    def test_read_concern(self):
        """Test readConcern is not validated by the driver."""
        # Read concern 'local' is not allowed for $changeStream.
        coll = self.watched_collection(read_concern=ReadConcern("local"))
        with self.assertRaises(OperationFailure):
            coll.watch()

        # Does not error.
        coll = self.watched_collection(read_concern=ReadConcern("majority"))
        with coll.watch():
            pass


class TestAllLegacyScenarios(IntegrationTest):
    RUN_ON_LOAD_BALANCER = True
    listener: AllowListEventListener

    @classmethod
    @client_context.require_connection
    def setUpClass(cls):
        super(TestAllLegacyScenarios, cls).setUpClass()
        cls.listener = AllowListEventListener("aggregate", "getMore")
        cls.client = rs_or_single_client(event_listeners=[cls.listener])

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        super(TestAllLegacyScenarios, cls).tearDownClass()

    def setUp(self):
        super(TestAllLegacyScenarios, self).setUp()
        self.listener.results.clear()

    def setUpCluster(self, scenario_dict):
        assets = [
            (scenario_dict["database_name"], scenario_dict["collection_name"]),
            (
                scenario_dict.get("database2_name", "db2"),
                scenario_dict.get("collection2_name", "coll2"),
            ),
        ]
        for db, coll in assets:
            self.client.drop_database(db)
            self.client[db].create_collection(coll)

    def setFailPoint(self, scenario_dict):
        fail_point = scenario_dict.get("failPoint")
        if fail_point is None:
            return
        elif not client_context.test_commands_enabled:
            self.skipTest("Test commands must be enabled")

        fail_cmd = SON([("configureFailPoint", "failCommand")])
        fail_cmd.update(fail_point)
        client_context.client.admin.command(fail_cmd)
        self.addCleanup(
            client_context.client.admin.command,
            "configureFailPoint",
            fail_cmd["configureFailPoint"],
            mode="off",
        )

    def assert_list_contents_are_subset(self, superlist, sublist):
        """Check that each element in sublist is a subset of the corresponding
        element in superlist."""
        self.assertEqual(len(superlist), len(sublist))
        for sup, sub in zip(superlist, sublist):
            if isinstance(sub, dict):
                self.assert_dict_is_subset(sup, sub)
                continue
            if isinstance(sub, (list, tuple)):
                self.assert_list_contents_are_subset(sup, sub)
                continue
            self.assertEqual(sup, sub)

    def assert_dict_is_subset(self, superdict, subdict):
        """Check that subdict is a subset of superdict."""
        exempt_fields = ["documentKey", "_id", "getMore"]
        for key, value in subdict.items():
            if key not in superdict:
                self.fail("Key %s not found in %s" % (key, superdict))
            if isinstance(value, dict):
                self.assert_dict_is_subset(superdict[key], value)
                continue
            if isinstance(value, (list, tuple)):
                self.assert_list_contents_are_subset(superdict[key], value)
                continue
            if key in exempt_fields:
                # Only check for presence of these exempt fields, but not value.
                self.assertIn(key, superdict)
            else:
                self.assertEqual(superdict[key], value)

    def check_event(self, event, expectation_dict):
        if event is None:
            self.fail()
        for key, value in expectation_dict.items():
            if isinstance(value, dict):
                self.assert_dict_is_subset(getattr(event, key), value)
            else:
                self.assertEqual(getattr(event, key), value)

    def tearDown(self):
        self.listener.results.clear()


_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "change_streams")


globals().update(
    generate_test_classes(
        os.path.join(_TEST_PATH, "unified"),
        module=__name__,
    )
)


if __name__ == "__main__":
    unittest.main()
