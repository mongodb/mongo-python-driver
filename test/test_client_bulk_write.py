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

from pymongo.client_bulk_shared import ClientBulkWriteException
from pymongo.encryption_options import _HAVE_PYMONGOCRYPT, AutoEncryptionOpts
from pymongo.errors import DocumentTooLarge, InvalidOperation, NetworkTimeout
from pymongo.monitoring import *
from pymongo.operations import *
from pymongo.write_concern import WriteConcern

_IS_SYNC = True


# https://github.com/mongodb/specifications/tree/master/source/crud/tests
class TestClientBulkWriteCRUD(IntegrationTest):
    @client_context.require_version_min(8, 0, 0, -24)
    def test_batch_splits_if_num_operations_too_large(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        max_write_batch_size = (client_context.hello)["maxWriteBatchSize"]
        models = []
        for _ in range(max_write_batch_size + 1):
            models.append(InsertOne(namespace="db.coll", document={"a": "b"}))
        self.addCleanup(client.db["coll"].drop)

        result = client.bulk_write(models=models)
        self.assertEqual(result.inserted_count, max_write_batch_size + 1)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)

        first_event, second_event = bulk_write_events
        self.assertEqual(len(first_event.command["ops"]), max_write_batch_size)
        self.assertEqual(len(second_event.command["ops"]), 1)
        self.assertEqual(first_event.operation_id, second_event.operation_id)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_batch_splits_if_ops_payload_too_large(self):
        listener = OvertCommandListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)

        max_message_size_bytes = (client_context.hello)["maxMessageSizeBytes"]
        max_bson_object_size = (client_context.hello)["maxBsonObjectSize"]

        models = []
        num_models = int(max_message_size_bytes / max_bson_object_size + 1)
        b_repeated = "b" * (max_bson_object_size - 500)
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
        max_write_batch_size = (client_context.hello)["maxWriteBatchSize"]

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
            for _ in range(max_write_batch_size + 1):
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
                context.exception.partial_result.inserted_count, max_write_batch_size + 1
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

        max_write_batch_size = (client_context.hello)["maxWriteBatchSize"]
        models = []
        for _ in range(max_write_batch_size + 1):
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"_id": 1},
                )
            )

        with self.assertRaises(ClientBulkWriteException) as context:
            client.bulk_write(models=models, ordered=False)
        self.assertEqual(len(context.exception.write_errors), max_write_batch_size + 1)  # type: ignore[arg-type]

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

        max_write_batch_size = (client_context.hello)["maxWriteBatchSize"]
        models = []
        for _ in range(max_write_batch_size + 1):
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

        max_bson_object_size = (client_context.hello)["maxBsonObjectSize"]
        models = []
        a_repeated = "a" * (max_bson_object_size // 2)
        b_repeated = "b" * (max_bson_object_size // 2)
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

        max_bson_object_size = (client_context.hello)["maxBsonObjectSize"]
        with client.start_session() as session:
            session.start_transaction()
            models = []
            a_repeated = "a" * (max_bson_object_size // 2)
            b_repeated = "b" * (max_bson_object_size // 2)
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

        max_bson_object_size = (client_context.hello)["maxBsonObjectSize"]
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {"failCommands": ["getMore"], "errorCode": 8},
        }
        with self.fail_point(fail_command):
            models = []
            a_repeated = "a" * (max_bson_object_size // 2)
            b_repeated = "b" * (max_bson_object_size // 2)
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

        max_bson_object_size = (client_context.hello)["maxBsonObjectSize"]
        b_repeated = "b" * max_bson_object_size

        # Insert document.
        models_insert = [InsertOne(namespace="db.coll", document={"a": b_repeated})]
        with self.assertRaises(DocumentTooLarge):
            client.bulk_write(models=models_insert, write_concern=WriteConcern(w=0))

        # Replace document.
        models_replace = [ReplaceOne(namespace="db.coll", filter={}, replacement={"a": b_repeated})]
        with self.assertRaises(DocumentTooLarge):
            client.bulk_write(models=models_replace, write_concern=WriteConcern(w=0))

    def _setup_namespace_test_models(self):
        max_message_size_bytes = (client_context.hello)["maxMessageSizeBytes"]
        max_bson_object_size = (client_context.hello)["maxBsonObjectSize"]

        ops_bytes = max_message_size_bytes - 1122
        num_models = ops_bytes // max_bson_object_size
        remainder_bytes = ops_bytes % max_bson_object_size

        models = []
        b_repeated = "b" * (max_bson_object_size - 57)
        for _ in range(num_models):
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"a": b_repeated},
                )
            )
        if remainder_bytes >= 217:
            num_models += 1
            b_repeated = "b" * (remainder_bytes - 57)
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

        max_message_size_bytes = (client_context.hello)["maxMessageSizeBytes"]

        # Document too large.
        b_repeated = "b" * max_message_size_bytes
        models = [InsertOne(namespace="db.coll", document={"a": b_repeated})]
        with self.assertRaises(InvalidOperation) as context:
            client.bulk_write(models=models)
        self.assertIn("cannot do an empty bulk write", context.exception._message)

        # Namespace too large.
        c_repeated = "c" * max_message_size_bytes
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
    @client_context.require_version_min(8, 0, 0, -24)
    @client_context.require_failCommand_fail_point
    def test_timeout_in_multi_batch_bulk_write(self):
        internal_client = rs_or_single_client(timeoutMS=None)
        self.addCleanup(internal_client.close)

        collection = internal_client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()

        max_bson_object_size = (client_context.hello)["maxBsonObjectSize"]
        max_message_size_bytes = (client_context.hello)["maxMessageSizeBytes"]
        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 2},
            "data": {"failCommands": ["bulkWrite"], "blockConnection": True, "blockTimeMS": 1010},
        }
        with self.fail_point(fail_command):
            models = []
            num_models = int(max_message_size_bytes / max_bson_object_size + 1)
            b_repeated = "b" * (max_bson_object_size - 500)
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
            with self.assertRaises(NetworkTimeout):
                client.bulk_write(models=models)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)
