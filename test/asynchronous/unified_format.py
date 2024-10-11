# Copyright 2020-present MongoDB, Inc.
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

"""Unified test format runner.

https://github.com/mongodb/specifications/blob/master/source/unified-test-format/unified-test-format.rst
"""
from __future__ import annotations

import asyncio
import binascii
import copy
import functools
import os
import re
import sys
import time
import traceback
from asyncio import iscoroutinefunction
from collections import defaultdict
from test.asynchronous import (
    AsyncIntegrationTest,
    async_client_context,
    client_knobs,
    unittest,
)
from test.unified_format_shared import (
    IS_INTERRUPTED,
    KMS_TLS_OPTS,
    PLACEHOLDER_MAP,
    SKIP_CSOT_TESTS,
    EventListenerUtil,
    MatchEvaluatorUtil,
    coerce_result,
    parse_bulk_write_error_result,
    parse_bulk_write_result,
    parse_client_bulk_write_error_result,
    parse_collection_or_database_options,
    with_metaclass,
)
from test.utils import (
    async_get_pool,
    camel_to_snake,
    camel_to_snake_args,
    parse_spec_options,
    prepare_spec_arguments,
    snake_to_camel,
    wait_until,
)
from test.utils_spec_runner import SpecRunnerThread
from test.version import Version
from typing import Any, Dict, List, Mapping, Optional

import pymongo
from bson import SON, json_util
from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.objectid import ObjectId
from gridfs import AsyncGridFSBucket, GridOut
from pymongo import ASCENDING, AsyncMongoClient, CursorType, _csot
from pymongo.asynchronous.change_stream import AsyncChangeStream
from pymongo.asynchronous.client_session import AsyncClientSession, TransactionOptions, _TxnState
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.encryption import AsyncClientEncryption
from pymongo.asynchronous.helpers import anext
from pymongo.encryption_options import _HAVE_PYMONGOCRYPT
from pymongo.errors import (
    BulkWriteError,
    ClientBulkWriteException,
    ConfigurationError,
    ConnectionFailure,
    EncryptionError,
    InvalidOperation,
    NotPrimaryError,
    OperationFailure,
    PyMongoError,
)
from pymongo.monitoring import (
    CommandStartedEvent,
)
from pymongo.operations import (
    SearchIndexModel,
)
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.server_api import ServerApi
from pymongo.server_selectors import Selection, writable_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.topology_description import TopologyDescription
from pymongo.typings import _Address
from pymongo.write_concern import WriteConcern

_IS_SYNC = False


async def is_run_on_requirement_satisfied(requirement):
    topology_satisfied = True
    req_topologies = requirement.get("topologies")
    if req_topologies:
        topology_satisfied = await async_client_context.is_topology_type(req_topologies)

    server_version = Version(*async_client_context.version[:3])

    min_version_satisfied = True
    req_min_server_version = requirement.get("minServerVersion")
    if req_min_server_version:
        min_version_satisfied = Version.from_string(req_min_server_version) <= server_version

    max_version_satisfied = True
    req_max_server_version = requirement.get("maxServerVersion")
    if req_max_server_version:
        max_version_satisfied = Version.from_string(req_max_server_version) >= server_version

    serverless = requirement.get("serverless")
    if serverless == "require":
        serverless_satisfied = async_client_context.serverless
    elif serverless == "forbid":
        serverless_satisfied = not async_client_context.serverless
    else:  # unset or "allow"
        serverless_satisfied = True

    params_satisfied = True
    params = requirement.get("serverParameters")
    if params:
        for param, val in params.items():
            if param not in async_client_context.server_parameters:
                params_satisfied = False
            elif async_client_context.server_parameters[param] != val:
                params_satisfied = False

    auth_satisfied = True
    req_auth = requirement.get("auth")
    if req_auth is not None:
        if req_auth:
            auth_satisfied = async_client_context.auth_enabled
            if auth_satisfied and "authMechanism" in requirement:
                auth_satisfied = async_client_context.check_auth_type(requirement["authMechanism"])
        else:
            auth_satisfied = not async_client_context.auth_enabled

    csfle_satisfied = True
    req_csfle = requirement.get("csfle")
    if req_csfle is True:
        min_version_satisfied = Version.from_string("4.2") <= server_version
        csfle_satisfied = _HAVE_PYMONGOCRYPT and min_version_satisfied

    return (
        topology_satisfied
        and min_version_satisfied
        and max_version_satisfied
        and serverless_satisfied
        and params_satisfied
        and auth_satisfied
        and csfle_satisfied
    )


class NonLazyCursor:
    """A find cursor proxy that creates the remote cursor when initialized."""

    def __init__(self, find_cursor, client):
        self.client = client
        self.find_cursor = find_cursor
        # Create the server side cursor.
        self.first_result = None

    @classmethod
    async def create(cls, find_cursor, client):
        cursor = cls(find_cursor, client)
        try:
            cursor.first_result = await anext(cursor.find_cursor)
        except StopAsyncIteration:
            cursor.first_result = None
        return cursor

    @property
    def alive(self):
        return self.first_result is not None or self.find_cursor.alive

    async def __anext__(self):
        if self.first_result is not None:
            first = self.first_result
            self.first_result = None
            return first
        return await anext(self.find_cursor)

    # Added to support the iterateOnce operation.
    try_next = __anext__

    async def close(self):
        await self.find_cursor.close()
        self.client = None


class EntityMapUtil:
    """Utility class that implements an entity map as per the unified
    test format specification.
    """

    def __init__(self, test_class):
        self._entities: Dict[str, Any] = {}
        self._listeners: Dict[str, EventListenerUtil] = {}
        self._session_lsids: Dict[str, Mapping[str, Any]] = {}
        self.test: UnifiedSpecTestMixinV1 = test_class
        self._cluster_time: Mapping[str, Any] = {}

    def __contains__(self, item):
        return item in self._entities

    def __len__(self):
        return len(self._entities)

    def __getitem__(self, item):
        try:
            return self._entities[item]
        except KeyError:
            self.test.fail(f"Could not find entity named {item} in map")

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            self.test.fail("Expected entity name of type str, got %s" % (type(key)))

        if key in self._entities:
            self.test.fail(f"Entity named {key} already in map")

        self._entities[key] = value

    def _handle_placeholders(self, spec: dict, current: dict, path: str) -> Any:
        if "$$placeholder" in current:
            if path not in PLACEHOLDER_MAP:
                raise ValueError(f"Could not find a placeholder value for {path}")
            return PLACEHOLDER_MAP[path]

        for key in list(current):
            value = current[key]
            if isinstance(value, dict):
                subpath = f"{path}/{key}"
                current[key] = self._handle_placeholders(spec, value, subpath)
        return current

    async def _create_entity(self, entity_spec, uri=None):
        if len(entity_spec) != 1:
            self.test.fail(f"Entity spec {entity_spec} did not contain exactly one top-level key")

        entity_type, spec = next(iter(entity_spec.items()))
        spec = self._handle_placeholders(spec, spec, "")
        if entity_type == "client":
            kwargs: dict = {}
            observe_events = spec.get("observeEvents", [])

            # The unified tests use topologyOpeningEvent, we use topologyOpenedEvent
            for i in range(len(observe_events)):
                if "topologyOpeningEvent" == observe_events[i]:
                    observe_events[i] = "topologyOpenedEvent"
            ignore_commands = spec.get("ignoreCommandMonitoringEvents", [])
            observe_sensitive_commands = spec.get("observeSensitiveCommands", False)
            ignore_commands = [cmd.lower() for cmd in ignore_commands]
            listener = EventListenerUtil(
                observe_events,
                ignore_commands,
                observe_sensitive_commands,
                spec.get("storeEventsAsEntities"),
                self,
            )
            self._listeners[spec["id"]] = listener
            kwargs["event_listeners"] = [listener]
            if spec.get("useMultipleMongoses"):
                if async_client_context.load_balancer or async_client_context.serverless:
                    kwargs["h"] = async_client_context.MULTI_MONGOS_LB_URI
                elif async_client_context.is_mongos:
                    kwargs["h"] = async_client_context.mongos_seeds()
            kwargs.update(spec.get("uriOptions", {}))
            server_api = spec.get("serverApi")
            if "waitQueueSize" in kwargs:
                raise unittest.SkipTest("PyMongo does not support waitQueueSize")
            if "waitQueueMultiple" in kwargs:
                raise unittest.SkipTest("PyMongo does not support waitQueueMultiple")
            if server_api:
                kwargs["server_api"] = ServerApi(
                    server_api["version"],
                    strict=server_api.get("strict"),
                    deprecation_errors=server_api.get("deprecationErrors"),
                )
            if uri:
                kwargs["h"] = uri
            client = await self.test.async_rs_or_single_client(**kwargs)
            self[spec["id"]] = client
            self.test.addAsyncCleanup(client.close)
            return
        elif entity_type == "database":
            client = self[spec["client"]]
            if type(client).__name__ != "AsyncMongoClient":
                self.test.fail(
                    "Expected entity {} to be of type AsyncMongoClient, got {}".format(
                        spec["client"], type(client)
                    )
                )
            options = parse_collection_or_database_options(spec.get("databaseOptions", {}))
            self[spec["id"]] = client.get_database(spec["databaseName"], **options)
            return
        elif entity_type == "collection":
            database = self[spec["database"]]
            if not isinstance(database, AsyncDatabase):
                self.test.fail(
                    "Expected entity {} to be of type AsyncDatabase, got {}".format(
                        spec["database"], type(database)
                    )
                )
            options = parse_collection_or_database_options(spec.get("collectionOptions", {}))
            self[spec["id"]] = database.get_collection(spec["collectionName"], **options)
            return
        elif entity_type == "session":
            client = self[spec["client"]]
            if type(client).__name__ != "AsyncMongoClient":
                self.test.fail(
                    "Expected entity {} to be of type AsyncMongoClient, got {}".format(
                        spec["client"], type(client)
                    )
                )
            opts = camel_to_snake_args(spec.get("sessionOptions", {}))
            if "default_transaction_options" in opts:
                txn_opts = parse_spec_options(opts["default_transaction_options"])
                txn_opts = TransactionOptions(**txn_opts)
                opts = copy.deepcopy(opts)
                opts["default_transaction_options"] = txn_opts
            session = client.start_session(**dict(opts))
            self[spec["id"]] = session
            self._session_lsids[spec["id"]] = copy.deepcopy(session.session_id)
            self.test.addAsyncCleanup(session.end_session)
            return
        elif entity_type == "bucket":
            db = self[spec["database"]]
            kwargs = parse_spec_options(spec.get("bucketOptions", {}).copy())
            bucket = AsyncGridFSBucket(db, **kwargs)

            # PyMongo does not support AsyncGridFSBucket.drop(), emulate it.
            @_csot.apply
            async def drop(self: AsyncGridFSBucket, *args: Any, **kwargs: Any) -> None:
                await self._files.drop(*args, **kwargs)
                await self._chunks.drop(*args, **kwargs)

            if not hasattr(bucket, "drop"):
                bucket.drop = drop.__get__(bucket)
            self[spec["id"]] = bucket
            return
        elif entity_type == "clientEncryption":
            opts = camel_to_snake_args(spec["clientEncryptionOpts"].copy())
            if isinstance(opts["key_vault_client"], str):
                opts["key_vault_client"] = self[opts["key_vault_client"]]
            # Set TLS options for providers like "kmip:name1".
            kms_tls_options = {}
            for provider in opts["kms_providers"]:
                provider_type = provider.split(":")[0]
                if provider_type in KMS_TLS_OPTS:
                    kms_tls_options[provider] = KMS_TLS_OPTS[provider_type]
            self[spec["id"]] = AsyncClientEncryption(
                opts["kms_providers"],
                opts["key_vault_namespace"],
                opts["key_vault_client"],
                DEFAULT_CODEC_OPTIONS,
                opts.get("kms_tls_options", kms_tls_options),
            )
            return
        elif entity_type == "thread":
            name = spec["id"]
            thread = SpecRunnerThread(name)
            thread.start()
            self[name] = thread
            return

        self.test.fail(f"Unable to create entity of unknown type {entity_type}")

    async def create_entities_from_spec(self, entity_spec, uri=None):
        for spec in entity_spec:
            await self._create_entity(spec, uri=uri)

    def get_listener_for_client(self, client_name: str) -> EventListenerUtil:
        client = self[client_name]
        if type(client).__name__ != "AsyncMongoClient":
            self.test.fail(
                f"Expected entity {client_name} to be of type AsyncMongoClient, got {type(client)}"
            )

        listener = self._listeners.get(client_name)
        if not listener:
            self.test.fail(f"No listeners configured for client {client_name}")

        return listener

    def get_lsid_for_session(self, session_name):
        session = self[session_name]
        if not isinstance(session, AsyncClientSession):
            self.test.fail(
                f"Expected entity {session_name} to be of type AsyncClientSession, got {type(session)}"
            )

        try:
            return session.session_id
        except InvalidOperation:
            # session has been closed.
            return self._session_lsids[session_name]

    async def advance_cluster_times(self) -> None:
        """Manually synchronize entities when desired"""
        if not self._cluster_time:
            self._cluster_time = (await self.test.client.admin.command("ping")).get("$clusterTime")
        for entity in self._entities.values():
            if isinstance(entity, AsyncClientSession) and self._cluster_time:
                entity.advance_cluster_time(self._cluster_time)


class UnifiedSpecTestMixinV1(AsyncIntegrationTest):
    """Mixin class to run test cases from test specification files.

    Assumes that tests conform to the `unified test format
    <https://github.com/mongodb/specifications/blob/master/source/unified-test-format/unified-test-format.rst>`_.

    Specification of the test suite being currently run is available as
    a class attribute ``TEST_SPEC``.
    """

    SCHEMA_VERSION = Version.from_string("1.21")
    RUN_ON_LOAD_BALANCER = True
    RUN_ON_SERVERLESS = True
    TEST_SPEC: Any
    mongos_clients: list[AsyncMongoClient] = []

    @staticmethod
    async def should_run_on(run_on_spec):
        if not run_on_spec:
            # Always run these tests.
            return True

        for req in run_on_spec:
            if await is_run_on_requirement_satisfied(req):
                return True
        return False

    async def insert_initial_data(self, initial_data):
        for i, collection_data in enumerate(initial_data):
            coll_name = collection_data["collectionName"]
            db_name = collection_data["databaseName"]
            opts = collection_data.get("createOptions", {})
            documents = collection_data["documents"]

            # Setup the collection with as few majority writes as possible.
            db = self.client[db_name]
            await db.drop_collection(coll_name)
            # Only use majority wc only on the final write.
            if i == len(initial_data) - 1:
                wc = WriteConcern(w="majority")
            else:
                wc = WriteConcern(w=1)
            if documents:
                if opts:
                    await db.create_collection(coll_name, **opts)
                await db.get_collection(coll_name, write_concern=wc).insert_many(documents)
            else:
                # Ensure collection exists
                await db.create_collection(coll_name, write_concern=wc, **opts)

    @classmethod
    async def _setup_class(cls):
        # super call creates internal client cls.client
        await super()._setup_class()
        # process file-level runOnRequirements
        run_on_spec = cls.TEST_SPEC.get("runOnRequirements", [])
        if not await cls.should_run_on(run_on_spec):
            raise unittest.SkipTest(f"{cls.__name__} runOnRequirements not satisfied")

        # add any special-casing for skipping tests here
        if async_client_context.storage_engine == "mmapv1":
            if "retryable-writes" in cls.TEST_SPEC["description"] or "retryable_writes" in str(
                cls.TEST_PATH
            ):
                raise unittest.SkipTest("MMAPv1 does not support retryWrites=True")

        # Handle mongos_clients for transactions tests.
        cls.mongos_clients = []
        if (
            async_client_context.supports_transactions()
            and not async_client_context.load_balancer
            and not async_client_context.serverless
        ):
            for address in async_client_context.mongoses:
                cls.mongos_clients.append(
                    await cls.unmanaged_async_single_client("{}:{}".format(*address))
                )

        # Speed up the tests by decreasing the heartbeat frequency.
        cls.knobs = client_knobs(
            heartbeat_frequency=0.1,
            min_heartbeat_interval=0.1,
            kill_cursor_frequency=0.1,
            events_queue_frequency=0.1,
        )
        cls.knobs.enable()

    @classmethod
    async def _tearDown_class(cls):
        cls.knobs.disable()
        for client in cls.mongos_clients:
            await client.close()
        await super()._tearDown_class()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        # process schemaVersion
        # note: we check major schema version during class generation
        # note: we do this here because we cannot run assertions in setUpClass
        version = Version.from_string(self.TEST_SPEC["schemaVersion"])
        self.assertLessEqual(
            version,
            self.SCHEMA_VERSION,
            f"expected schema version {self.SCHEMA_VERSION} or lower, got {version}",
        )

        # initialize internals
        self.match_evaluator = MatchEvaluatorUtil(self)

    def maybe_skip_test(self, spec):
        # add any special-casing for skipping tests here
        if async_client_context.storage_engine == "mmapv1":
            if (
                "Dirty explicit session is discarded" in spec["description"]
                or "Dirty implicit session is discarded" in spec["description"]
                or "Cancel server check" in spec["description"]
            ):
                self.skipTest("MMAPv1 does not support retryWrites=True")
        if (
            "AsyncDatabase-level aggregate with $out includes read preference for 5.0+ server"
            in spec["description"]
        ):
            if async_client_context.version[0] == 8:
                self.skipTest("waiting on PYTHON-4356")
        if "Aggregate with $out includes read preference for 5.0+ server" in spec["description"]:
            if async_client_context.version[0] == 8:
                self.skipTest("waiting on PYTHON-4356")
        if "Client side error in command starting transaction" in spec["description"]:
            self.skipTest("Implement PYTHON-1894")
        if "timeoutMS applied to entire download" in spec["description"]:
            self.skipTest("PyMongo's open_download_stream does not cap the stream's lifetime")

        class_name = self.__class__.__name__.lower()
        description = spec["description"].lower()
        if "csot" in class_name:
            if "gridfs" in class_name and sys.platform == "win32":
                self.skipTest("PYTHON-3522 CSOT GridFS tests are flaky on Windows")
            if async_client_context.storage_engine == "mmapv1":
                self.skipTest(
                    "MMAPv1 does not support retryable writes which is required for CSOT tests"
                )
            if "change" in description or "change" in class_name:
                self.skipTest("CSOT not implemented for watch()")
            if "cursors" in class_name:
                self.skipTest("CSOT not implemented for cursors")
            if "tailable" in class_name:
                self.skipTest("CSOT not implemented for tailable cursors")
            if "sessions" in class_name:
                self.skipTest("CSOT not implemented for sessions")
            if "withtransaction" in description:
                self.skipTest("CSOT not implemented for with_transaction")
            if "transaction" in class_name or "transaction" in description:
                self.skipTest("CSOT not implemented for transactions")

        # Some tests need to be skipped based on the operations they try to run.
        for op in spec["operations"]:
            name = op["name"]
            if name == "count":
                self.skipTest("PyMongo does not support count()")
            if name == "listIndexNames":
                self.skipTest("PyMongo does not support list_index_names()")
            if async_client_context.storage_engine == "mmapv1":
                if name == "createChangeStream":
                    self.skipTest("MMAPv1 does not support change streams")
                if name == "withTransaction" or name == "startTransaction":
                    self.skipTest("MMAPv1 does not support document-level locking")
            if not async_client_context.test_commands_enabled:
                if name == "failPoint" or name == "targetedFailPoint":
                    self.skipTest("Test commands must be enabled to use fail points")
            if name == "modifyCollection":
                self.skipTest("PyMongo does not support modifyCollection")
            if "timeoutMode" in op.get("arguments", {}):
                self.skipTest("PyMongo does not support timeoutMode")

    def process_error(self, exception, spec):
        if isinstance(exception, unittest.SkipTest):
            raise
        is_error = spec.get("isError")
        is_client_error = spec.get("isClientError")
        is_timeout_error = spec.get("isTimeoutError")
        error_contains = spec.get("errorContains")
        error_code = spec.get("errorCode")
        error_code_name = spec.get("errorCodeName")
        error_labels_contain = spec.get("errorLabelsContain")
        error_labels_omit = spec.get("errorLabelsOmit")
        expect_result = spec.get("expectResult")
        error_response = spec.get("errorResponse")
        if error_response:
            if isinstance(exception, ClientBulkWriteException):
                self.match_evaluator.match_result(error_response, exception.error.details)
            else:
                self.match_evaluator.match_result(error_response, exception.details)

        if is_error:
            # already satisfied because exception was raised
            pass

        if is_client_error:
            if isinstance(exception, ClientBulkWriteException):
                error = exception.error
            else:
                error = exception
            # Connection errors are considered client errors.
            if isinstance(error, ConnectionFailure):
                self.assertNotIsInstance(error, NotPrimaryError)
            elif isinstance(error, (InvalidOperation, ConfigurationError, EncryptionError)):
                pass
            else:
                self.assertNotIsInstance(error, PyMongoError)

        if is_timeout_error:
            self.assertIsInstance(exception, PyMongoError)
            if not exception.timeout:
                # Re-raise the exception for better diagnostics.
                raise exception

        if error_contains:
            if isinstance(exception, BulkWriteError):
                errmsg = str(exception.details).lower()
            elif isinstance(exception, ClientBulkWriteException):
                errmsg = str(exception.details).lower()
            else:
                errmsg = str(exception).lower()
            self.assertIn(error_contains.lower(), errmsg)

        if error_code:
            if isinstance(exception, ClientBulkWriteException):
                self.assertEqual(error_code, exception.error.details.get("code"))
            else:
                self.assertEqual(error_code, exception.details.get("code"))

        if error_code_name:
            if isinstance(exception, ClientBulkWriteException):
                self.assertEqual(error_code, exception.error.details.get("codeName"))
            else:
                self.assertEqual(error_code_name, exception.details.get("codeName"))

        if error_labels_contain:
            if isinstance(exception, ClientBulkWriteException):
                error = exception.error
            else:
                error = exception
            labels = [
                err_label for err_label in error_labels_contain if error.has_error_label(err_label)
            ]
            self.assertEqual(labels, error_labels_contain)

        if error_labels_omit:
            for err_label in error_labels_omit:
                if exception.has_error_label(err_label):
                    self.fail(f"Exception '{exception}' unexpectedly had label '{err_label}'")

        if expect_result:
            if isinstance(exception, BulkWriteError):
                result = parse_bulk_write_error_result(exception)
                self.match_evaluator.match_result(expect_result, result)
            elif isinstance(exception, ClientBulkWriteException):
                result = parse_client_bulk_write_error_result(exception)
                self.match_evaluator.match_result(expect_result, result)
            else:
                self.fail(
                    f"expectResult can only be specified with {BulkWriteError} or {ClientBulkWriteException} exceptions"
                )

        return exception

    def __raise_if_unsupported(self, opname, target, *target_types):
        if not isinstance(target, target_types):
            self.fail(f"Operation {opname} not supported for entity of type {type(target)}")

    async def __entityOperation_createChangeStream(self, target, *args, **kwargs):
        if async_client_context.storage_engine == "mmapv1":
            self.skipTest("MMAPv1 does not support change streams")
        self.__raise_if_unsupported(
            "createChangeStream", target, AsyncMongoClient, AsyncDatabase, AsyncCollection
        )
        stream = await target.watch(*args, **kwargs)
        self.addAsyncCleanup(stream.close)
        return stream

    async def _clientOperation_createChangeStream(self, target, *args, **kwargs):
        return await self.__entityOperation_createChangeStream(target, *args, **kwargs)

    async def _databaseOperation_createChangeStream(self, target, *args, **kwargs):
        return await self.__entityOperation_createChangeStream(target, *args, **kwargs)

    async def _collectionOperation_createChangeStream(self, target, *args, **kwargs):
        return await self.__entityOperation_createChangeStream(target, *args, **kwargs)

    async def _databaseOperation_runCommand(self, target, **kwargs):
        self.__raise_if_unsupported("runCommand", target, AsyncDatabase)
        # Ensure the first key is the command name.
        ordered_command = SON([(kwargs.pop("command_name"), 1)])
        ordered_command.update(kwargs["command"])
        kwargs["command"] = ordered_command
        return await target.command(**kwargs)

    async def _databaseOperation_runCursorCommand(self, target, **kwargs):
        return list(await self._databaseOperation_createCommandCursor(target, **kwargs))

    async def _databaseOperation_createCommandCursor(self, target, **kwargs):
        self.__raise_if_unsupported("createCommandCursor", target, AsyncDatabase)
        # Ensure the first key is the command name.
        ordered_command = SON([(kwargs.pop("command_name"), 1)])
        ordered_command.update(kwargs["command"])
        kwargs["command"] = ordered_command
        batch_size = 0

        cursor_type = kwargs.pop("cursor_type", "nonTailable")
        if cursor_type == CursorType.TAILABLE:
            ordered_command["tailable"] = True
        elif cursor_type == CursorType.TAILABLE_AWAIT:
            ordered_command["tailable"] = True
            ordered_command["awaitData"] = True
        elif cursor_type != "nonTailable":
            self.fail(f"unknown cursorType: {cursor_type}")

        if "maxTimeMS" in kwargs:
            kwargs["max_await_time_ms"] = kwargs.pop("maxTimeMS")

        if "batch_size" in kwargs:
            batch_size = kwargs.pop("batch_size")

        cursor = await target.cursor_command(**kwargs)

        if batch_size > 0:
            cursor.batch_size(batch_size)

        return cursor

    async def kill_all_sessions(self):
        if getattr(self, "client", None) is None:
            return
        clients = self.mongos_clients if self.mongos_clients else [self.client]
        for client in clients:
            try:
                await client.admin.command("killAllSessions", [])
            except OperationFailure:
                # "operation was interrupted" by killing the command's
                # own session.
                pass

    async def _databaseOperation_listCollections(self, target, *args, **kwargs):
        if "batch_size" in kwargs:
            kwargs["cursor"] = {"batchSize": kwargs.pop("batch_size")}
        cursor = await target.list_collections(*args, **kwargs)
        return list(cursor)

    async def _databaseOperation_createCollection(self, target, *args, **kwargs):
        # PYTHON-1936 Ignore the listCollections event from create_collection.
        kwargs["check_exists"] = False
        ret = await target.create_collection(*args, **kwargs)
        return ret

    async def __entityOperation_aggregate(self, target, *args, **kwargs):
        self.__raise_if_unsupported("aggregate", target, AsyncDatabase, AsyncCollection)
        return await (await target.aggregate(*args, **kwargs)).to_list()

    async def _databaseOperation_aggregate(self, target, *args, **kwargs):
        return await self.__entityOperation_aggregate(target, *args, **kwargs)

    async def _collectionOperation_aggregate(self, target, *args, **kwargs):
        return await self.__entityOperation_aggregate(target, *args, **kwargs)

    async def _collectionOperation_find(self, target, *args, **kwargs):
        self.__raise_if_unsupported("find", target, AsyncCollection)
        find_cursor = target.find(*args, **kwargs)
        return await find_cursor.to_list()

    async def _collectionOperation_createFindCursor(self, target, *args, **kwargs):
        self.__raise_if_unsupported("find", target, AsyncCollection)
        if "filter" not in kwargs:
            self.fail('createFindCursor requires a "filter" argument')
        cursor = await NonLazyCursor.create(target.find(*args, **kwargs), target.database.client)
        self.addAsyncCleanup(cursor.close)
        return cursor

    def _collectionOperation_count(self, target, *args, **kwargs):
        self.skipTest("PyMongo does not support collection.count()")

    async def _collectionOperation_listIndexes(self, target, *args, **kwargs):
        if "batch_size" in kwargs:
            self.skipTest("PyMongo does not support batch_size for list_indexes")
        return await (await target.list_indexes(*args, **kwargs)).to_list()

    def _collectionOperation_listIndexNames(self, target, *args, **kwargs):
        self.skipTest("PyMongo does not support list_index_names")

    async def _collectionOperation_createSearchIndexes(self, target, *args, **kwargs):
        models = [SearchIndexModel(**i) for i in kwargs["models"]]
        return await target.create_search_indexes(models)

    async def _collectionOperation_listSearchIndexes(self, target, *args, **kwargs):
        name = kwargs.get("name")
        agg_kwargs = kwargs.get("aggregation_options", dict())
        return await (await target.list_search_indexes(name, **agg_kwargs)).to_list()

    async def _sessionOperation_withTransaction(self, target, *args, **kwargs):
        if async_client_context.storage_engine == "mmapv1":
            self.skipTest("MMAPv1 does not support document-level locking")
        self.__raise_if_unsupported("withTransaction", target, AsyncClientSession)
        return await target.with_transaction(*args, **kwargs)

    async def _sessionOperation_startTransaction(self, target, *args, **kwargs):
        if async_client_context.storage_engine == "mmapv1":
            self.skipTest("MMAPv1 does not support document-level locking")
        self.__raise_if_unsupported("startTransaction", target, AsyncClientSession)
        return await target.start_transaction(*args, **kwargs)

    async def _changeStreamOperation_iterateUntilDocumentOrError(self, target, *args, **kwargs):
        self.__raise_if_unsupported("iterateUntilDocumentOrError", target, AsyncChangeStream)
        return await anext(target)

    async def _cursor_iterateUntilDocumentOrError(self, target, *args, **kwargs):
        self.__raise_if_unsupported(
            "iterateUntilDocumentOrError", target, NonLazyCursor, AsyncCommandCursor
        )
        while target.alive:
            try:
                return await anext(target)
            except StopAsyncIteration:
                pass
        return None

    async def _cursor_close(self, target, *args, **kwargs):
        self.__raise_if_unsupported("close", target, NonLazyCursor, AsyncCommandCursor)
        return await target.close()

    async def _clientEncryptionOperation_createDataKey(self, target, *args, **kwargs):
        if "opts" in kwargs:
            kwargs.update(camel_to_snake_args(kwargs.pop("opts")))

        return await target.create_data_key(*args, **kwargs)

    async def _clientEncryptionOperation_getKeys(self, target, *args, **kwargs):
        return await (await target.get_keys(*args, **kwargs)).to_list()

    async def _clientEncryptionOperation_deleteKey(self, target, *args, **kwargs):
        result = await target.delete_key(*args, **kwargs)
        response = result.raw_result
        response["deletedCount"] = result.deleted_count
        return response

    async def _clientEncryptionOperation_rewrapManyDataKey(self, target, *args, **kwargs):
        if "opts" in kwargs:
            kwargs.update(camel_to_snake_args(kwargs.pop("opts")))
        data = await target.rewrap_many_data_key(*args, **kwargs)
        if data.bulk_write_result:
            return {"bulkWriteResult": parse_bulk_write_result(data.bulk_write_result)}
        return {}

    async def _clientEncryptionOperation_encrypt(self, target, *args, **kwargs):
        if "opts" in kwargs:
            kwargs.update(camel_to_snake_args(kwargs.pop("opts")))
        return await target.encrypt(*args, **kwargs)

    async def _bucketOperation_download(
        self, target: AsyncGridFSBucket, *args: Any, **kwargs: Any
    ) -> bytes:
        async with await target.open_download_stream(*args, **kwargs) as gout:
            return await gout.read()

    async def _bucketOperation_downloadByName(
        self, target: AsyncGridFSBucket, *args: Any, **kwargs: Any
    ) -> bytes:
        async with await target.open_download_stream_by_name(*args, **kwargs) as gout:
            return await gout.read()

    async def _bucketOperation_upload(
        self, target: AsyncGridFSBucket, *args: Any, **kwargs: Any
    ) -> ObjectId:
        kwargs["source"] = binascii.unhexlify(kwargs.pop("source")["$$hexBytes"])
        if "content_type" in kwargs:
            kwargs.setdefault("metadata", {})["contentType"] = kwargs.pop("content_type")
        return await target.upload_from_stream(*args, **kwargs)

    async def _bucketOperation_uploadWithId(
        self, target: AsyncGridFSBucket, *args: Any, **kwargs: Any
    ) -> Any:
        kwargs["source"] = binascii.unhexlify(kwargs.pop("source")["$$hexBytes"])
        if "content_type" in kwargs:
            kwargs.setdefault("metadata", {})["contentType"] = kwargs.pop("content_type")
        return await target.upload_from_stream_with_id(*args, **kwargs)

    async def _bucketOperation_find(
        self, target: AsyncGridFSBucket, *args: Any, **kwargs: Any
    ) -> List[GridOut]:
        return await target.find(*args, **kwargs).to_list()

    async def run_entity_operation(self, spec):
        target = self.entity_map[spec["object"]]
        opname = spec["name"]
        opargs = spec.get("arguments")
        expect_error = spec.get("expectError")
        save_as_entity = spec.get("saveResultAsEntity")
        expect_result = spec.get("expectResult")
        ignore = spec.get("ignoreResultAndError")
        if ignore and (expect_error or save_as_entity or expect_result):
            raise ValueError(
                "ignoreResultAndError is incompatible with saveResultAsEntity"
                ", expectError, and expectResult"
            )
        if opargs:
            arguments = parse_spec_options(copy.deepcopy(opargs))
            prepare_spec_arguments(
                spec,
                arguments,
                camel_to_snake(opname),
                self.entity_map,
                self.run_operations_and_throw,
            )
        else:
            arguments = {}

        if isinstance(target, AsyncMongoClient):
            method_name = f"_clientOperation_{opname}"
        elif isinstance(target, AsyncDatabase):
            method_name = f"_databaseOperation_{opname}"
        elif isinstance(target, AsyncCollection):
            method_name = f"_collectionOperation_{opname}"
            # contentType is always stored in metadata in pymongo.
            if target.name.endswith(".files") and opname == "find":
                for doc in spec.get("expectResult", []):
                    if "contentType" in doc:
                        doc.setdefault("metadata", {})["contentType"] = doc.pop("contentType")
        elif isinstance(target, AsyncChangeStream):
            method_name = f"_changeStreamOperation_{opname}"
        elif isinstance(target, (NonLazyCursor, AsyncCommandCursor)):
            method_name = f"_cursor_{opname}"
        elif isinstance(target, AsyncClientSession):
            method_name = f"_sessionOperation_{opname}"
        elif isinstance(target, AsyncGridFSBucket):
            method_name = f"_bucketOperation_{opname}"
            if "id" in arguments:
                arguments["file_id"] = arguments.pop("id")
            # MD5 is always disabled in pymongo.
            arguments.pop("disable_md5", None)
        elif isinstance(target, AsyncClientEncryption):
            method_name = f"_clientEncryptionOperation_{opname}"
        else:
            method_name = "doesNotExist"

        try:
            method = getattr(self, method_name)
        except AttributeError:
            target_opname = camel_to_snake(opname)
            if target_opname == "iterate_once":
                target_opname = "try_next"
            if target_opname == "client_bulk_write":
                target_opname = "bulk_write"
            try:
                cmd = getattr(target, target_opname)
            except AttributeError:
                self.fail(f"Unsupported operation {opname} on entity {target}")
        else:
            cmd = functools.partial(method, target)

        try:
            # CSOT: Translate the spec test "timeout" arg into pymongo's context timeout API.
            if "timeout" in arguments:
                timeout = arguments.pop("timeout")
                with pymongo.timeout(timeout):
                    result = await cmd(**dict(arguments))
            else:
                result = await cmd(**dict(arguments))
        except Exception as exc:
            # Ignore all operation errors but to avoid masking bugs don't
            # ignore things like TypeError and ValueError.
            if ignore and isinstance(exc, (PyMongoError,)):
                return exc
            if expect_error:
                if method_name == "_collectionOperation_bulkWrite":
                    self.skipTest("Skipping test pending PYTHON-4598")
                return self.process_error(exc, expect_error)
            raise
        else:
            if method_name == "_collectionOperation_bulkWrite":
                self.skipTest("Skipping test pending PYTHON-4598")
            if expect_error:
                self.fail(f'Excepted error {expect_error} but "{opname}" succeeded: {result}')

        if expect_result:
            actual = coerce_result(opname, result)
            self.match_evaluator.match_result(expect_result, actual)

        if save_as_entity:
            self.entity_map[save_as_entity] = result
            return None
        return None

    async def __set_fail_point(self, client, command_args):
        if not async_client_context.test_commands_enabled:
            self.skipTest("Test commands must be enabled")

        cmd_on = SON([("configureFailPoint", "failCommand")])
        cmd_on.update(command_args)
        await client.admin.command(cmd_on)
        self.addAsyncCleanup(
            client.admin.command, "configureFailPoint", cmd_on["configureFailPoint"], mode="off"
        )

    async def _testOperation_failPoint(self, spec):
        await self.__set_fail_point(
            client=self.entity_map[spec["client"]], command_args=spec["failPoint"]
        )

    async def _testOperation_targetedFailPoint(self, spec):
        session = self.entity_map[spec["session"]]
        if not session._pinned_address:
            self.fail(
                "Cannot use targetedFailPoint operation with unpinned " "session {}".format(
                    spec["session"]
                )
            )

        client = await self.async_single_client("{}:{}".format(*session._pinned_address))
        self.addAsyncCleanup(client.close)
        await self.__set_fail_point(client=client, command_args=spec["failPoint"])

    async def _testOperation_createEntities(self, spec):
        await self.entity_map.create_entities_from_spec(spec["entities"], uri=self._uri)
        await self.entity_map.advance_cluster_times()

    def _testOperation_assertSessionTransactionState(self, spec):
        session = self.entity_map[spec["session"]]
        expected_state = getattr(_TxnState, spec["state"].upper())
        self.assertEqual(expected_state, session._transaction.state)

    def _testOperation_assertSessionPinned(self, spec):
        session = self.entity_map[spec["session"]]
        self.assertIsNotNone(session._transaction.pinned_address)

    def _testOperation_assertSessionUnpinned(self, spec):
        session = self.entity_map[spec["session"]]
        self.assertIsNone(session._pinned_address)
        self.assertIsNone(session._transaction.pinned_address)

    def __get_last_two_command_lsids(self, listener):
        cmd_started_events = []
        for event in reversed(listener.events):
            if isinstance(event, CommandStartedEvent):
                cmd_started_events.append(event)
        if len(cmd_started_events) < 2:
            self.fail(
                "Needed 2 CommandStartedEvents to compare lsids, "
                "got %s" % (len(cmd_started_events))
            )
        return tuple([e.command["lsid"] for e in cmd_started_events][:2])

    def _testOperation_assertDifferentLsidOnLastTwoCommands(self, spec):
        listener = self.entity_map.get_listener_for_client(spec["client"])
        self.assertNotEqual(*self.__get_last_two_command_lsids(listener))

    def _testOperation_assertSameLsidOnLastTwoCommands(self, spec):
        listener = self.entity_map.get_listener_for_client(spec["client"])
        self.assertEqual(*self.__get_last_two_command_lsids(listener))

    def _testOperation_assertSessionDirty(self, spec):
        session = self.entity_map[spec["session"]]
        self.assertTrue(session._server_session.dirty)

    def _testOperation_assertSessionNotDirty(self, spec):
        session = self.entity_map[spec["session"]]
        return self.assertFalse(session._server_session.dirty)

    async def _testOperation_assertCollectionExists(self, spec):
        database_name = spec["databaseName"]
        collection_name = spec["collectionName"]
        collection_name_list = list(
            await self.client.get_database(database_name).list_collection_names()
        )
        self.assertIn(collection_name, collection_name_list)

    async def _testOperation_assertCollectionNotExists(self, spec):
        database_name = spec["databaseName"]
        collection_name = spec["collectionName"]
        collection_name_list = list(
            await self.client.get_database(database_name).list_collection_names()
        )
        self.assertNotIn(collection_name, collection_name_list)

    async def _testOperation_assertIndexExists(self, spec):
        collection = self.client[spec["databaseName"]][spec["collectionName"]]
        index_names = [idx["name"] async for idx in await collection.list_indexes()]
        self.assertIn(spec["indexName"], index_names)

    async def _testOperation_assertIndexNotExists(self, spec):
        collection = self.client[spec["databaseName"]][spec["collectionName"]]
        async for index in await collection.list_indexes():
            self.assertNotEqual(spec["indexName"], index["name"])

    async def _testOperation_assertNumberConnectionsCheckedOut(self, spec):
        client = self.entity_map[spec["client"]]
        pool = await async_get_pool(client)
        self.assertEqual(spec["connections"], pool.active_sockets)

    def _event_count(self, client_name, event):
        listener = self.entity_map.get_listener_for_client(client_name)
        actual_events = listener.get_events("all")
        count = 0
        for actual in actual_events:
            try:
                self.match_evaluator.match_event(event, actual)
            except AssertionError:
                continue
            else:
                count += 1
        return count

    def _testOperation_assertEventCount(self, spec):
        """Run the assertEventCount test operation.

        Assert the given event was published exactly `count` times.
        """
        client, event, count = spec["client"], spec["event"], spec["count"]
        self.assertEqual(self._event_count(client, event), count, f"expected {count} not {event!r}")

    def _testOperation_waitForEvent(self, spec):
        """Run the waitForEvent test operation.

        Wait for a number of events to be published, or fail.
        """
        client, event, count = spec["client"], spec["event"], spec["count"]
        wait_until(
            lambda: self._event_count(client, event) >= count,
            f"find {count} {event} event(s)",
        )

    async def _testOperation_wait(self, spec):
        """Run the "wait" test operation."""
        await asyncio.sleep(spec["ms"] / 1000.0)

    def _testOperation_recordTopologyDescription(self, spec):
        """Run the recordTopologyDescription test operation."""
        self.entity_map[spec["id"]] = self.entity_map[spec["client"]].topology_description

    def _testOperation_assertTopologyType(self, spec):
        """Run the assertTopologyType test operation."""
        description = self.entity_map[spec["topologyDescription"]]
        self.assertIsInstance(description, TopologyDescription)
        self.assertEqual(description.topology_type_name, spec["topologyType"])

    def _testOperation_waitForPrimaryChange(self, spec: dict) -> None:
        """Run the waitForPrimaryChange test operation."""
        client = self.entity_map[spec["client"]]
        old_description: TopologyDescription = self.entity_map[spec["priorTopologyDescription"]]
        timeout = spec["timeoutMS"] / 1000.0

        def get_primary(td: TopologyDescription) -> Optional[_Address]:
            servers = writable_server_selector(Selection.from_topology_description(td))
            if servers and servers[0].server_type == SERVER_TYPE.RSPrimary:
                return servers[0].address
            return None

        old_primary = get_primary(old_description)

        def primary_changed() -> bool:
            primary = client.primary
            if primary is None:
                return False
            return primary != old_primary

        wait_until(primary_changed, "change primary", timeout=timeout)

    def _testOperation_runOnThread(self, spec):
        """Run the 'runOnThread' operation."""
        thread = self.entity_map[spec["thread"]]
        thread.schedule(lambda: self.run_entity_operation(spec["operation"]))

    def _testOperation_waitForThread(self, spec):
        """Run the 'waitForThread' operation."""
        thread = self.entity_map[spec["thread"]]
        thread.stop()
        thread.join(10)
        if thread.exc:
            raise thread.exc
        self.assertFalse(thread.is_alive(), "Thread {} is still running".format(spec["thread"]))

    async def _testOperation_loop(self, spec):
        failure_key = spec.get("storeFailuresAsEntity")
        error_key = spec.get("storeErrorsAsEntity")
        successes_key = spec.get("storeSuccessesAsEntity")
        iteration_key = spec.get("storeIterationsAsEntity")
        iteration_limiter_key = spec.get("numIterations")
        for i in [failure_key, error_key]:
            if i:
                self.entity_map[i] = []
        for i in [successes_key, iteration_key]:
            if i:
                self.entity_map[i] = 0
        i = 0
        global IS_INTERRUPTED
        while True:
            if iteration_limiter_key and i >= iteration_limiter_key:
                break
            i += 1
            if IS_INTERRUPTED:
                break
            try:
                if iteration_key:
                    self.entity_map._entities[iteration_key] += 1
                for op in spec["operations"]:
                    await self.run_entity_operation(op)
                    if successes_key:
                        self.entity_map._entities[successes_key] += 1
            except Exception as exc:
                if isinstance(exc, AssertionError):
                    key = failure_key or error_key
                else:
                    key = error_key or failure_key
                if not key:
                    raise
                self.entity_map[key].append(
                    {"error": str(exc), "time": time.time(), "type": type(exc).__name__}
                )

    async def run_special_operation(self, spec):
        opname = spec["name"]
        method_name = f"_testOperation_{opname}"
        try:
            method = getattr(self, method_name)
        except AttributeError:
            self.fail(f"Unsupported special test operation {opname}")
        else:
            if iscoroutinefunction(method):
                await method(spec["arguments"])
            else:
                method(spec["arguments"])

    async def run_operations(self, spec):
        for op in spec:
            if op["object"] == "testRunner":
                await self.run_special_operation(op)
            else:
                await self.run_entity_operation(op)

    async def run_operations_and_throw(self, spec):
        for op in spec:
            if op["object"] == "testRunner":
                await self.run_special_operation(op)
            else:
                result = await self.run_entity_operation(op)
                if isinstance(result, Exception):
                    raise result

    def check_events(self, spec):
        for event_spec in spec:
            client_name = event_spec["client"]
            events = event_spec["events"]
            event_type = event_spec.get("eventType", "command")
            ignore_extra_events = event_spec.get("ignoreExtraEvents", False)
            server_connection_id = event_spec.get("serverConnectionId")
            has_server_connection_id = event_spec.get("hasServerConnectionId", False)
            listener = self.entity_map.get_listener_for_client(client_name)
            actual_events = listener.get_events(event_type)
            if ignore_extra_events:
                actual_events = actual_events[: len(events)]

            if len(events) == 0:
                self.assertEqual(actual_events, [])
                continue

            if len(actual_events) != len(events):
                expected = "\n".join(str(e) for e in events)
                actual = "\n".join(str(a) for a in actual_events)
                self.assertEqual(
                    len(actual_events),
                    len(events),
                    f"expected events:\n{expected}\nactual events:\n{actual}",
                )

            for idx, expected_event in enumerate(events):
                self.match_evaluator.match_event(expected_event, actual_events[idx])

            if has_server_connection_id:
                assert server_connection_id is not None
                assert server_connection_id >= 0
            else:
                assert server_connection_id is None

    def process_ignore_messages(self, ignore_logs, actual_logs):
        final_logs = []
        for log in actual_logs:
            ignored = False
            for ignore_log in ignore_logs:
                if log["data"]["message"] == ignore_log["data"][
                    "message"
                ] and self.match_evaluator.match_result(ignore_log, log, test=False):
                    ignored = True
                    break
            if not ignored:
                final_logs.append(log)
        return final_logs

    async def check_log_messages(self, operations, spec):
        def format_logs(log_list):
            client_to_log = defaultdict(list)
            for log in log_list:
                if log.module == "ocsp_support":
                    continue
                data = json_util.loads(log.getMessage())
                client = data.pop("clientId") if "clientId" in data else data.pop("topologyId")
                client_to_log[client].append(
                    {
                        "level": log.levelname.lower(),
                        "component": log.name.replace("pymongo.", "", 1),
                        "data": data,
                    }
                )
            return client_to_log

        with self.assertLogs("pymongo", level="DEBUG") as cm:
            await self.run_operations(operations)
            formatted_logs = format_logs(cm.records)
            for client in spec:
                components = set()
                for message in client["messages"]:
                    components.add(message["component"])

                clientid = self.entity_map[client["client"]]._topology_settings._topology_id
                actual_logs = formatted_logs[clientid]
                actual_logs = [log for log in actual_logs if log["component"] in components]

                ignore_logs = client.get("ignoreMessages", [])
                if ignore_logs:
                    actual_logs = self.process_ignore_messages(ignore_logs, actual_logs)

                if client.get("ignoreExtraMessages", False):
                    actual_logs = actual_logs[: len(client["messages"])]
                self.assertEqual(
                    len(client["messages"]),
                    len(actual_logs),
                    f"expected {client['messages']} but got {actual_logs}",
                )
                for expected_msg, actual_msg in zip(client["messages"], actual_logs):
                    expected_data, actual_data = expected_msg.pop("data"), actual_msg.pop("data")

                    if "failureIsRedacted" in expected_msg:
                        self.assertIn("failure", actual_data)
                        should_redact = expected_msg.pop("failureIsRedacted")
                        if should_redact:
                            actual_fields = set(json_util.loads(actual_data["failure"]).keys())
                            self.assertTrue(
                                {"code", "codeName", "errorLabels"}.issuperset(actual_fields)
                            )

                    self.match_evaluator.match_result(expected_data, actual_data)
                    self.match_evaluator.match_result(expected_msg, actual_msg)

    async def verify_outcome(self, spec):
        for collection_data in spec:
            coll_name = collection_data["collectionName"]
            db_name = collection_data["databaseName"]
            expected_documents = collection_data["documents"]

            coll = self.client.get_database(db_name).get_collection(
                coll_name,
                read_preference=ReadPreference.PRIMARY,
                read_concern=ReadConcern(level="local"),
            )

            if expected_documents:
                sorted_expected_documents = sorted(expected_documents, key=lambda doc: doc["_id"])
                actual_documents = await coll.find({}, sort=[("_id", ASCENDING)]).to_list()
                self.assertListEqual(sorted_expected_documents, actual_documents)

    async def run_scenario(self, spec, uri=None):
        if "csot" in self.id().lower() and SKIP_CSOT_TESTS:
            raise unittest.SkipTest("SKIP_CSOT_TESTS is set, skipping...")

        # Kill all sessions before and after each test to prevent an open
        # transaction (from a test failure) from blocking collection/database
        # operations during test set up and tear down.
        await self.kill_all_sessions()
        self.addAsyncCleanup(self.kill_all_sessions)

        if "csot" in self.id().lower():
            # Retry CSOT tests up to 2 times to deal with flakey tests.
            attempts = 3
            for i in range(attempts):
                try:
                    return await self._run_scenario(spec, uri)
                except AssertionError:
                    if i < attempts - 1:
                        print(
                            f"Retrying after attempt {i+1} of {self.id()} failed with:\n"
                            f"{traceback.format_exc()}",
                            file=sys.stderr,
                        )
                        await self.asyncSetUp()
                        continue
                    raise
            return None
        else:
            await self._run_scenario(spec, uri)
            return None

    async def _run_scenario(self, spec, uri=None):
        # maybe skip test manually
        self.maybe_skip_test(spec)

        # process test-level runOnRequirements
        run_on_spec = spec.get("runOnRequirements", [])
        if not await self.should_run_on(run_on_spec):
            raise unittest.SkipTest("runOnRequirements not satisfied")

        # process skipReason
        skip_reason = spec.get("skipReason", None)
        if skip_reason is not None:
            raise unittest.SkipTest(f"{skip_reason}")

        # process createEntities
        self._uri = uri
        self.entity_map = EntityMapUtil(self)
        await self.entity_map.create_entities_from_spec(
            self.TEST_SPEC.get("createEntities", []), uri=uri
        )
        # process initialData
        if "initialData" in self.TEST_SPEC:
            await self.insert_initial_data(self.TEST_SPEC["initialData"])
            self._cluster_time = (await self.client.admin.command("ping")).get("$clusterTime")
            await self.entity_map.advance_cluster_times()

        if "expectLogMessages" in spec:
            expect_log_messages = spec["expectLogMessages"]
            self.assertTrue(expect_log_messages, "expectEvents must be non-empty")
            await self.check_log_messages(spec["operations"], expect_log_messages)
        else:
            # process operations
            await self.run_operations(spec["operations"])

        # process expectEvents
        if "expectEvents" in spec:
            expect_events = spec["expectEvents"]
            self.assertTrue(expect_events, "expectEvents must be non-empty")
            self.check_events(expect_events)

        # process outcome
        await self.verify_outcome(spec.get("outcome", []))


class UnifiedSpecTestMeta(type):
    """Metaclass for generating test classes."""

    TEST_SPEC: Any
    EXPECTED_FAILURES: Any

    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def create_test(spec):
            async def test_case(self):
                await self.run_scenario(spec)

            return test_case

        for test_spec in cls.TEST_SPEC["tests"]:
            description = test_spec["description"]
            test_name = "test_{}".format(
                description.strip(". ").replace(" ", "_").replace(".", "_")
            )
            test_method = create_test(copy.deepcopy(test_spec))
            test_method.__name__ = str(test_name)

            for fail_pattern in cls.EXPECTED_FAILURES:
                if re.search(fail_pattern, description):
                    test_method = unittest.expectedFailure(test_method)
                    break

            setattr(cls, test_name, test_method)


_ALL_MIXIN_CLASSES = [
    UnifiedSpecTestMixinV1,
    # add mixin classes for new schema major versions here
]


_SCHEMA_VERSION_MAJOR_TO_MIXIN_CLASS = {
    KLASS.SCHEMA_VERSION[0]: KLASS for KLASS in _ALL_MIXIN_CLASSES
}


def generate_test_classes(
    test_path,
    module=__name__,
    class_name_prefix="",
    expected_failures=[],  # noqa: B006
    bypass_test_generation_errors=False,
    **kwargs,
):
    """Method for generating test classes. Returns a dictionary where keys are
    the names of test classes and values are the test class objects.
    """
    test_klasses = {}

    def test_base_class_factory(test_spec):
        """Utility that creates the base class to use for test generation.
        This is needed to ensure that cls.TEST_SPEC is appropriately set when
        the metaclass __init__ is invoked.
        """

        class SpecTestBase(with_metaclass(UnifiedSpecTestMeta)):  # type: ignore
            TEST_SPEC = test_spec
            EXPECTED_FAILURES = expected_failures

        return SpecTestBase

    for dirpath, _, filenames in os.walk(test_path):
        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:
            fpath = os.path.join(dirpath, filename)
            with open(fpath) as scenario_stream:
                # Use tz_aware=False to match how CodecOptions decodes
                # dates.
                opts = json_util.JSONOptions(tz_aware=False)
                scenario_def = json_util.loads(scenario_stream.read(), json_options=opts)

            test_type = os.path.splitext(filename)[0]
            snake_class_name = "Test{}_{}_{}".format(
                class_name_prefix,
                dirname.replace("-", "_"),
                test_type.replace("-", "_").replace(".", "_"),
            )
            class_name = snake_to_camel(snake_class_name)

            try:
                schema_version = Version.from_string(scenario_def["schemaVersion"])
                mixin_class = _SCHEMA_VERSION_MAJOR_TO_MIXIN_CLASS.get(schema_version[0])
                if mixin_class is None:
                    raise ValueError(
                        f"test file '{fpath}' has unsupported schemaVersion '{schema_version}'"
                    )
                module_dict = {"__module__": module, "TEST_PATH": test_path}
                module_dict.update(kwargs)
                test_klasses[class_name] = type(
                    class_name,
                    (
                        mixin_class,
                        test_base_class_factory(scenario_def),
                    ),
                    module_dict,
                )
            except Exception:
                if bypass_test_generation_errors:
                    continue
                raise

    return test_klasses
