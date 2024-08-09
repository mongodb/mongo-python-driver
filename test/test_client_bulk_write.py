# Copyright 2024-present MongoDB, Inc.
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

"""Test the client bulk write API."""
from __future__ import annotations

import sys

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.utils import (
    OvertCommandListener,
    rs_or_single_client,
)

from pymongo.encryption_options import _HAVE_PYMONGOCRYPT, AutoEncryptionOpts
from pymongo.errors import (
    ClientBulkWriteException,
    DocumentTooLarge,
    InvalidOperation,
    NetworkTimeout,
)
from pymongo.monitoring import *
from pymongo.operations import *
from pymongo.write_concern import WriteConcern

_IS_SYNC = True


class TestClientBulkWrite(IntegrationTest):
    @client_context.require_version_min(8, 0, 0, -24)
    def test_returns_error_if_no_namespace_provided(self):
        client = rs_or_single_client()
        self.addCleanup(client.close)

        models = [InsertOne(document={"a": "b"})]
        with self.assertRaises(InvalidOperation) as context:
            client.bulk_write(models=models)
        self.assertIn(
            "MongoClient.bulk_write requires a namespace to be provided for each write operation",
            context.exception._message,
        )


# https://github.com/mongodb/specifications/tree/master/source/crud/tests
class TestClientBulkWriteCRUD(IntegrationTest):
    def setUp(self):
        self.max_write_batch_size = client_context.max_write_batch_size
        self.max_bson_object_size = client_context.max_bson_size
        self.max_message_size_bytes = client_context.max_message_size_bytes

    @client_context.require_version_min(8, 0, 0, -24)
    def test_batch_splits_if_num_operations_too_large(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        models = []
        for _ in range(self.max_write_batch_size + 1):
            models.append(InsertOne(namespace="db.coll", document={"a": "b"}))
        self.addCleanup(client.db["coll"].drop)

        result = client.bulk_write(models=models)
        self.assertEqual(result.inserted_count, self.max_write_batch_size + 1)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)

        first_event, second_event = bulk_write_events
        self.assertEqual(len(first_event.command["ops"]), self.max_write_batch_size)
        self.assertEqual(len(second_event.command["ops"]), 1)
        self.assertEqual(first_event.operation_id, second_event.operation_id)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_batch_splits_if_ops_payload_too_large(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        models = []
        num_models = int(self.max_message_size_bytes / self.max_bson_object_size + 1)
        b_repeated = "b" * (self.max_bson_object_size - 500)
        for _ in range(num_models):
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"a": b_repeated},
                )
            )
        self.addCleanup(client.db["coll"].drop)

        result = client.bulk_write(models=models)
        self.assertEqual(result.inserted_count, num_models)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)

        first_event, second_event = bulk_write_events
        self.assertEqual(len(first_event.command["ops"]), num_models - 1)
        self.assertEqual(len(second_event.command["ops"]), 1)
        self.assertEqual(first_event.operation_id, second_event.operation_id)

    @client_context.require_version_min(8, 0, 0, -24)
    @client_context.require_failCommand_fail_point
    def test_collects_write_concern_errors_across_batches(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(
            event_listeners=[listener],
            retryWrites=False,
        )
        self.addCleanup(client.close)

        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 2},
            "data": {
                "failCommands": ["bulkWrite"],
                "writeConcernError": {"code": 91, "errmsg": "Replication is being shut down"},
            },
        }
        with self.fail_point(fail_command):
            models = []
            for _ in range(self.max_write_batch_size + 1):
                models.append(
                    InsertOne(
                        namespace="db.coll",
                        document={"a": "b"},
                    )
                )
            self.addCleanup(client.db["coll"].drop)

            with self.assertRaises(ClientBulkWriteException) as context:
                client.bulk_write(models=models)
            self.assertEqual(len(context.exception.write_concern_errors), 2)  # type: ignore[arg-type]
            self.assertIsNotNone(context.exception.partial_result)
            self.assertEqual(
                context.exception.partial_result.inserted_count, self.max_write_batch_size + 1
            )

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_collects_write_errors_across_batches_unordered(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()
        collection.insert_one(document={"_id": 1})

        models = []
        for _ in range(self.max_write_batch_size + 1):
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"_id": 1},
                )
            )

        with self.assertRaises(ClientBulkWriteException) as context:
            client.bulk_write(models=models, ordered=False)
        self.assertEqual(len(context.exception.write_errors), self.max_write_batch_size + 1)  # type: ignore[arg-type]

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_collects_write_errors_across_batches_ordered(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()
        collection.insert_one(document={"_id": 1})

        models = []
        for _ in range(self.max_write_batch_size + 1):
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"_id": 1},
                )
            )

        with self.assertRaises(ClientBulkWriteException) as context:
            client.bulk_write(models=models, ordered=True)
        self.assertEqual(len(context.exception.write_errors), 1)  # type: ignore[arg-type]

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 1)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_handles_cursor_requiring_getMore(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()

        models = []
        a_repeated = "a" * (self.max_bson_object_size // 2)
        b_repeated = "b" * (self.max_bson_object_size // 2)
        models.append(
            UpdateOne(
                namespace="db.coll",
                filter={"_id": a_repeated},
                update={"$set": {"x": 1}},
                upsert=True,
            )
        )
        models.append(
            UpdateOne(
                namespace="db.coll",
                filter={"_id": b_repeated},
                update={"$set": {"x": 1}},
                upsert=True,
            )
        )

        result = client.bulk_write(models=models, verbose_results=True)
        self.assertEqual(result.upserted_count, 2)
        self.assertEqual(len(result.update_results), 2)

        get_more_event = False
        for event in listener.started_events:
            if event.command_name == "getMore":
                get_more_event = True
        self.assertTrue(get_more_event)

    @client_context.require_version_min(8, 0, 0, -24)
    @client_context.require_no_standalone
    def test_handles_cursor_requiring_getMore_within_transaction(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()

        with client.start_session() as session:
            session.start_transaction()
            models = []
            a_repeated = "a" * (self.max_bson_object_size // 2)
            b_repeated = "b" * (self.max_bson_object_size // 2)
            models.append(
                UpdateOne(
                    namespace="db.coll",
                    filter={"_id": a_repeated},
                    update={"$set": {"x": 1}},
                    upsert=True,
                )
            )
            models.append(
                UpdateOne(
                    namespace="db.coll",
                    filter={"_id": b_repeated},
                    update={"$set": {"x": 1}},
                    upsert=True,
                )
            )
            result = client.bulk_write(models=models, session=session, verbose_results=True)

        self.assertEqual(result.upserted_count, 2)
        self.assertEqual(len(result.update_results), 2)

        get_more_event = False
        for event in listener.started_events:
            if event.command_name == "getMore":
                get_more_event = True
        self.assertTrue(get_more_event)

    @client_context.require_version_min(8, 0, 0, -24)
    @client_context.require_failCommand_fail_point
    def test_handles_getMore_error(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()

        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {"failCommands": ["getMore"], "errorCode": 8},
        }
        with self.fail_point(fail_command):
            models = []
            a_repeated = "a" * (self.max_bson_object_size // 2)
            b_repeated = "b" * (self.max_bson_object_size // 2)
            models.append(
                UpdateOne(
                    namespace="db.coll",
                    filter={"_id": a_repeated},
                    update={"$set": {"x": 1}},
                    upsert=True,
                )
            )
            models.append(
                UpdateOne(
                    namespace="db.coll",
                    filter={"_id": b_repeated},
                    update={"$set": {"x": 1}},
                    upsert=True,
                )
            )

            with self.assertRaises(ClientBulkWriteException) as context:
                client.bulk_write(models=models, verbose_results=True)
            self.assertIsNotNone(context.exception.error)
            self.assertEqual(context.exception.error["code"], 8)
            self.assertIsNotNone(context.exception.partial_result)
            self.assertEqual(context.exception.partial_result.upserted_count, 2)
            self.assertEqual(len(context.exception.partial_result.update_results), 1)

        get_more_event = False
        kill_cursors_event = False
        for event in listener.started_events:
            if event.command_name == "getMore":
                get_more_event = True
            if event.command_name == "killCursors":
                kill_cursors_event = True
        self.assertTrue(get_more_event)
        self.assertTrue(kill_cursors_event)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_returns_error_if_unacknowledged_too_large_insert(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        b_repeated = "b" * self.max_bson_object_size

        # Insert document.
        models_insert = [InsertOne(namespace="db.coll", document={"a": b_repeated})]
        with self.assertRaises(DocumentTooLarge):
            client.bulk_write(models=models_insert, write_concern=WriteConcern(w=0))

        # Replace document.
        models_replace = [ReplaceOne(namespace="db.coll", filter={}, replacement={"a": b_repeated})]
        with self.assertRaises(DocumentTooLarge):
            client.bulk_write(models=models_replace, write_concern=WriteConcern(w=0))

    def _setup_namespace_test_models(self):
        # See prose test specification below for details on these calculations.
        # https://github.com/mongodb/specifications/tree/master/source/crud/tests#details-on-size-calculations
        _EXISTING_BULK_WRITE_BYTES = 1122
        _OPERATION_DOC_BYTES = 57
        _NAMESPACE_DOC_BYTES = 217

        # When compression is enabled, max_message_size is
        # smaller to account for compression message header.
        if client_context.client_options.get("compressors"):
            max_message_size_bytes = self.max_message_size_bytes - 16
        else:
            max_message_size_bytes = self.max_message_size_bytes

        ops_bytes = max_message_size_bytes - _EXISTING_BULK_WRITE_BYTES
        num_models = ops_bytes // self.max_bson_object_size
        remainder_bytes = ops_bytes % self.max_bson_object_size

        models = []
        b_repeated = "b" * (self.max_bson_object_size - _OPERATION_DOC_BYTES)
        for _ in range(num_models):
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"a": b_repeated},
                )
            )
        if remainder_bytes >= _NAMESPACE_DOC_BYTES:
            num_models += 1
            b_repeated = "b" * (remainder_bytes - _OPERATION_DOC_BYTES)
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"a": b_repeated},
                )
            )
        return num_models, models

    @client_context.require_version_min(8, 0, 0, -24)
    def test_no_batch_splits_if_new_namespace_is_not_too_large(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        num_models, models = self._setup_namespace_test_models()
        models.append(
            InsertOne(
                namespace="db.coll",
                document={"a": "b"},
            )
        )
        self.addCleanup(client.db["coll"].drop)

        # No batch splitting required.
        result = client.bulk_write(models=models)
        self.assertEqual(result.inserted_count, num_models + 1)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)

        self.assertEqual(len(bulk_write_events), 1)
        event = bulk_write_events[0]

        self.assertEqual(len(event.command["ops"]), num_models + 1)
        self.assertEqual(len(event.command["nsInfo"]), 1)
        self.assertEqual(event.command["nsInfo"][0]["ns"], "db.coll")

    @client_context.require_version_min(8, 0, 0, -24)
    def test_batch_splits_if_new_namespace_is_too_large(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        num_models, models = self._setup_namespace_test_models()
        c_repeated = "c" * 200
        namespace = f"db.{c_repeated}"
        models.append(
            InsertOne(
                namespace=namespace,
                document={"a": "b"},
            )
        )
        self.addCleanup(client.db["coll"].drop)
        self.addCleanup(client.db[c_repeated].drop)

        # Batch splitting required.
        result = client.bulk_write(models=models)
        self.assertEqual(result.inserted_count, num_models + 1)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)

        self.assertEqual(len(bulk_write_events), 2)
        first_event, second_event = bulk_write_events

        self.assertEqual(len(first_event.command["ops"]), num_models)
        self.assertEqual(len(first_event.command["nsInfo"]), 1)
        self.assertEqual(first_event.command["nsInfo"][0]["ns"], "db.coll")

        self.assertEqual(len(second_event.command["ops"]), 1)
        self.assertEqual(len(second_event.command["nsInfo"]), 1)
        self.assertEqual(second_event.command["nsInfo"][0]["ns"], namespace)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_returns_error_if_no_writes_can_be_added_to_ops(self):
        client = rs_or_single_client()
        self.addCleanup(client.close)

        # Document too large.
        b_repeated = "b" * self.max_message_size_bytes
        models = [InsertOne(namespace="db.coll", document={"a": b_repeated})]
        with self.assertRaises(InvalidOperation) as context:
            client.bulk_write(models=models)
        self.assertIn("cannot do an empty bulk write", context.exception._message)

        # Namespace too large.
        c_repeated = "c" * self.max_message_size_bytes
        namespace = f"db.{c_repeated}"
        models = [InsertOne(namespace=namespace, document={"a": "b"})]
        with self.assertRaises(InvalidOperation) as context:
            client.bulk_write(models=models)
        self.assertIn("cannot do an empty bulk write", context.exception._message)

    @client_context.require_version_min(8, 0, 0, -24)
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    def test_returns_error_if_auto_encryption_configured(self):
        opts = AutoEncryptionOpts(
            key_vault_namespace="db.coll",
            kms_providers={"aws": {"accessKeyId": "foo", "secretAccessKey": "bar"}},
        )
        client = rs_or_single_client(auto_encryption_opts=opts)
        self.addCleanup(client.close)

        models = [InsertOne(namespace="db.coll", document={"a": "b"})]
        with self.assertRaises(InvalidOperation) as context:
            client.bulk_write(models=models)
        self.assertIn(
            "bulk_write does not currently support automatic encryption", context.exception._message
        )


# https://github.com/mongodb/specifications/blob/master/source/client-side-operations-timeout/tests/README.md#11-multi-batch-bulkwrites
class TestClientBulkWriteTimeout(IntegrationTest):
    def setUp(self):
        self.max_write_batch_size = client_context.max_write_batch_size
        self.max_bson_object_size = client_context.max_bson_size
        self.max_message_size_bytes = client_context.max_message_size_bytes

    @client_context.require_version_min(8, 0, 0, -24)
    @client_context.require_failCommand_fail_point
    def test_timeout_in_multi_batch_bulk_write(self):
        _OVERHEAD = 500

        internal_client = rs_or_single_client(timeoutMS=None)
        self.addCleanup(internal_client.close)

        collection = internal_client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()

        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 2},
            "data": {"failCommands": ["bulkWrite"], "blockConnection": True, "blockTimeMS": 1010},
        }
        with self.fail_point(fail_command):
            models = []
            num_models = int(self.max_message_size_bytes / self.max_bson_object_size + 1)
            b_repeated = "b" * (self.max_bson_object_size - _OVERHEAD)
            for _ in range(num_models):
                models.append(
                    InsertOne(
                        namespace="db.coll",
                        document={"a": b_repeated},
                    )
                )

            listener = OvertCommandListener()
            client = rs_or_single_client(
                event_listeners=[listener],
                readConcernLevel="majority",
                readPreference="primary",
                timeoutMS=2000,
                w="majority",
            )
            self.addCleanup(client.close)
            with self.assertRaises(ClientBulkWriteException) as context:
                client.bulk_write(models=models)
            self.assertIsInstance(context.exception.error, NetworkTimeout)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)
