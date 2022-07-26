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
import binascii
import collections
import copy
import datetime
import functools
import os
import re
import sys
import time
import types
from collections import abc
from test import (
    AWS_CREDS,
    AZURE_CREDS,
    CA_PEM,
    CLIENT_PEM,
    GCP_CREDS,
    KMIP_CREDS,
    LOCAL_MASTER_KEY,
    IntegrationTest,
    client_context,
    unittest,
)
from test.utils import (
    CMAPListener,
    camel_to_snake,
    camel_to_snake_args,
    get_pool,
    parse_collection_options,
    parse_spec_options,
    prepare_spec_arguments,
    rs_or_single_client,
    single_client,
    snake_to_camel,
)
from test.version import Version
from typing import Any, List

import pymongo
from bson import SON, Code, DBRef, Decimal128, Int64, MaxKey, MinKey, json_util
from bson.binary import Binary
from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.objectid import ObjectId
from bson.regex import RE_TYPE, Regex
from gridfs import GridFSBucket, GridOut
from pymongo import ASCENDING, MongoClient, _csot
from pymongo.change_stream import ChangeStream
from pymongo.client_session import ClientSession, TransactionOptions, _TxnState
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.encryption import ClientEncryption
from pymongo.encryption_options import _HAVE_PYMONGOCRYPT
from pymongo.errors import (
    BulkWriteError,
    ConfigurationError,
    ConnectionFailure,
    EncryptionError,
    InvalidOperation,
    NotPrimaryError,
    PyMongoError,
)
from pymongo.monitoring import (
    _SENSITIVE_COMMANDS,
    CommandFailedEvent,
    CommandListener,
    CommandStartedEvent,
    CommandSucceededEvent,
    ConnectionCheckedInEvent,
    ConnectionCheckedOutEvent,
    ConnectionCheckOutFailedEvent,
    ConnectionCheckOutStartedEvent,
    ConnectionClosedEvent,
    ConnectionCreatedEvent,
    ConnectionReadyEvent,
    PoolClearedEvent,
    PoolClosedEvent,
    PoolCreatedEvent,
    PoolReadyEvent,
)
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.results import BulkWriteResult
from pymongo.server_api import ServerApi
from pymongo.write_concern import WriteConcern

JSON_OPTS = json_util.JSONOptions(tz_aware=False)

IS_INTERRUPTED = False

KMS_TLS_OPTS = {
    "kmip": {
        "tlsCAFile": CA_PEM,
        "tlsCertificateKeyFile": CLIENT_PEM,
    }
}


# Build up a placeholder map.
PLACEHOLDER_MAP = dict()
for (provider_name, provider_data) in [
    ("local", {"key": LOCAL_MASTER_KEY}),
    ("aws", AWS_CREDS),
    ("azure", AZURE_CREDS),
    ("gcp", GCP_CREDS),
    ("kmip", KMIP_CREDS),
]:
    for (key, value) in provider_data.items():
        placeholder = f"/clientEncryptionOpts/kmsProviders/{provider_name}/{key}"
        PLACEHOLDER_MAP[placeholder] = value


def interrupt_loop():
    global IS_INTERRUPTED
    IS_INTERRUPTED = True


def with_metaclass(meta, *bases):
    """Create a base class with a metaclass.

    Vendored from six: https://github.com/benjaminp/six/blob/master/six.py
    """
    # This requires a bit of explanation: the basic idea is to make a dummy
    # metaclass for one level of class instantiation that replaces itself with
    # the actual metaclass.
    class metaclass(type):
        def __new__(cls, name, this_bases, d):
            if sys.version_info[:2] >= (3, 7):
                # This version introduced PEP 560 that requires a bit
                # of extra care (we mimic what is done by __build_class__).
                resolved_bases = types.resolve_bases(bases)
                if resolved_bases is not bases:
                    d["__orig_bases__"] = bases
            else:
                resolved_bases = bases
            return meta(name, resolved_bases, d)

        @classmethod
        def __prepare__(cls, name, this_bases):
            return meta.__prepare__(name, bases)

    return type.__new__(metaclass, "temporary_class", (), {})


def is_run_on_requirement_satisfied(requirement):
    topology_satisfied = True
    req_topologies = requirement.get("topologies")
    if req_topologies:
        topology_satisfied = client_context.is_topology_type(req_topologies)

    server_version = Version(*client_context.version[:3])

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
        serverless_satisfied = client_context.serverless
    elif serverless == "forbid":
        serverless_satisfied = not client_context.serverless
    else:  # unset or "allow"
        serverless_satisfied = True

    params_satisfied = True
    params = requirement.get("serverParameters")
    if params:
        for param, val in params.items():
            if param not in client_context.server_parameters:
                params_satisfied = False
            elif client_context.server_parameters[param] != val:
                params_satisfied = False

    auth_satisfied = True
    req_auth = requirement.get("auth")
    if req_auth is not None:
        if req_auth:
            auth_satisfied = client_context.auth_enabled
        else:
            auth_satisfied = not client_context.auth_enabled

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


def parse_collection_or_database_options(options):
    return parse_collection_options(options)


def parse_bulk_write_result(result):
    upserted_ids = {str(int_idx): result.upserted_ids[int_idx] for int_idx in result.upserted_ids}
    return {
        "deletedCount": result.deleted_count,
        "insertedCount": result.inserted_count,
        "matchedCount": result.matched_count,
        "modifiedCount": result.modified_count,
        "upsertedCount": result.upserted_count,
        "upsertedIds": upserted_ids,
    }


def parse_bulk_write_error_result(error):
    write_result = BulkWriteResult(error.details, True)
    return parse_bulk_write_result(write_result)


class NonLazyCursor(object):
    """A find cursor proxy that creates the remote cursor when initialized."""

    def __init__(self, find_cursor, client):
        self.client = client
        self.find_cursor = find_cursor
        # Create the server side cursor.
        self.first_result = next(find_cursor, None)

    @property
    def alive(self):
        return self.first_result is not None or self.find_cursor.alive

    def __next__(self):
        if self.first_result is not None:
            first = self.first_result
            self.first_result = None
            return first
        return next(self.find_cursor)

    # Added to support the iterateOnce operation.
    try_next = __next__

    def close(self):
        self.find_cursor.close()
        self.client = None


class EventListenerUtil(CMAPListener, CommandListener):
    def __init__(
        self, observe_events, ignore_commands, observe_sensitive_commands, store_events, entity_map
    ):
        self._event_types = set(name.lower() for name in observe_events)
        if observe_sensitive_commands:
            self._observe_sensitive_commands = True
            self._ignore_commands = set(ignore_commands)
        else:
            self._observe_sensitive_commands = False
            self._ignore_commands = _SENSITIVE_COMMANDS | set(ignore_commands)
            self._ignore_commands.add("configurefailpoint")
        self._event_mapping = collections.defaultdict(list)
        self.entity_map = entity_map
        if store_events:
            for i in store_events:
                id = i["id"]
                events = (i.lower() for i in i["events"])
                for i in events:
                    self._event_mapping[i].append(id)
                self.entity_map[id] = []
        super(EventListenerUtil, self).__init__()

    def get_events(self, event_type):
        if event_type == "command":
            return [e for e in self.events if "Command" in type(e).__name__]
        return [e for e in self.events if "Command" not in type(e).__name__]

    def add_event(self, event):
        event_name = type(event).__name__.lower()
        if event_name in self._event_types:
            super(EventListenerUtil, self).add_event(event)
        for id in self._event_mapping[event_name]:
            self.entity_map[id].append(
                {
                    "name": type(event).__name__,
                    "observedAt": time.time(),
                    "description": repr(event),
                }
            )

    def _command_event(self, event):
        if not event.command_name.lower() in self._ignore_commands:
            self.add_event(event)

    def started(self, event):
        if event.command == {}:
            # Command is redacted. Observe only if flag is set.
            if self._observe_sensitive_commands:
                self._command_event(event)
        else:
            self._command_event(event)

    def succeeded(self, event):
        if event.reply == {}:
            # Command is redacted. Observe only if flag is set.
            if self._observe_sensitive_commands:
                self._command_event(event)
        else:
            self._command_event(event)

    def failed(self, event):
        self._command_event(event)


class EntityMapUtil(object):
    """Utility class that implements an entity map as per the unified
    test format specification."""

    def __init__(self, test_class):
        self._entities = {}
        self._listeners = {}
        self._session_lsids = {}
        self.test = test_class

    def __contains__(self, item):
        return item in self._entities

    def __len__(self):
        return len(self._entities)

    def __getitem__(self, item):
        try:
            return self._entities[item]
        except KeyError:
            self.test.fail("Could not find entity named %s in map" % (item,))

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            self.test.fail("Expected entity name of type str, got %s" % (type(key)))

        if key in self._entities:
            self.test.fail("Entity named %s already in map" % (key,))

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

    def _create_entity(self, entity_spec, uri=None):
        if len(entity_spec) != 1:
            self.test.fail(
                "Entity spec %s did not contain exactly one top-level key" % (entity_spec,)
            )

        entity_type, spec = next(iter(entity_spec.items()))
        spec = self._handle_placeholders(spec, spec, "")
        if entity_type == "client":
            kwargs: dict = {}
            observe_events = spec.get("observeEvents", [])
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
                if client_context.load_balancer or client_context.serverless:
                    kwargs["h"] = client_context.MULTI_MONGOS_LB_URI
                elif client_context.is_mongos:
                    kwargs["h"] = client_context.mongos_seeds()
            kwargs.update(spec.get("uriOptions", {}))
            server_api = spec.get("serverApi")
            if server_api:
                kwargs["server_api"] = ServerApi(
                    server_api["version"],
                    strict=server_api.get("strict"),
                    deprecation_errors=server_api.get("deprecationErrors"),
                )
            if uri:
                kwargs["h"] = uri
            client = rs_or_single_client(**kwargs)
            self[spec["id"]] = client
            self.test.addCleanup(client.close)
            return
        elif entity_type == "database":
            client = self[spec["client"]]
            if not isinstance(client, MongoClient):
                self.test.fail(
                    "Expected entity %s to be of type MongoClient, got %s"
                    % (spec["client"], type(client))
                )
            options = parse_collection_or_database_options(spec.get("databaseOptions", {}))
            self[spec["id"]] = client.get_database(spec["databaseName"], **options)
            return
        elif entity_type == "collection":
            database = self[spec["database"]]
            if not isinstance(database, Database):
                self.test.fail(
                    "Expected entity %s to be of type Database, got %s"
                    % (spec["database"], type(database))
                )
            options = parse_collection_or_database_options(spec.get("collectionOptions", {}))
            self[spec["id"]] = database.get_collection(spec["collectionName"], **options)
            return
        elif entity_type == "session":
            client = self[spec["client"]]
            if not isinstance(client, MongoClient):
                self.test.fail(
                    "Expected entity %s to be of type MongoClient, got %s"
                    % (spec["client"], type(client))
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
            self.test.addCleanup(session.end_session)
            return
        elif entity_type == "bucket":
            db = self[spec["database"]]
            kwargs = parse_spec_options(spec.get("bucketOptions", {}).copy())
            bucket = GridFSBucket(db, **kwargs)

            # PyMongo does not support GridFSBucket.drop(), emulate it.
            @_csot.apply
            def drop(self: GridFSBucket, *args: Any, **kwargs: Any) -> None:
                self._files.drop(*args, **kwargs)
                self._chunks.drop(*args, **kwargs)

            if not hasattr(bucket, "drop"):
                bucket.drop = drop.__get__(bucket)
            self[spec["id"]] = bucket
            return
        elif entity_type == "clientEncryption":
            opts = camel_to_snake_args(spec["clientEncryptionOpts"].copy())
            if isinstance(opts["key_vault_client"], str):
                opts["key_vault_client"] = self[opts["key_vault_client"]]
            self[spec["id"]] = ClientEncryption(
                opts["kms_providers"],
                opts["key_vault_namespace"],
                opts["key_vault_client"],
                DEFAULT_CODEC_OPTIONS,
                opts.get("kms_tls_options", KMS_TLS_OPTS),
            )
            return

        self.test.fail("Unable to create entity of unknown type %s" % (entity_type,))

    def create_entities_from_spec(self, entity_spec, uri=None):
        for spec in entity_spec:
            self._create_entity(spec, uri=uri)

    def get_listener_for_client(self, client_name):
        client = self[client_name]
        if not isinstance(client, MongoClient):
            self.test.fail(
                "Expected entity %s to be of type MongoClient, got %s" % (client_name, type(client))
            )

        listener = self._listeners.get(client_name)
        if not listener:
            self.test.fail("No listeners configured for client %s" % (client_name,))

        return listener

    def get_lsid_for_session(self, session_name):
        session = self[session_name]
        if not isinstance(session, ClientSession):
            self.test.fail(
                "Expected entity %s to be of type ClientSession, got %s"
                % (session_name, type(session))
            )

        try:
            return session.session_id
        except InvalidOperation:
            # session has been closed.
            return self._session_lsids[session_name]


binary_types = (Binary, bytes)
long_types = (Int64,)
unicode_type = str


BSON_TYPE_ALIAS_MAP = {
    # https://mongodb.com/docs/manual/reference/operator/query/type/
    # https://pymongo.readthedocs.io/en/stable/api/bson/index.html
    "double": (float,),
    "string": (str,),
    "object": (abc.Mapping,),
    "array": (abc.MutableSequence,),
    "binData": binary_types,
    "undefined": (type(None),),
    "objectId": (ObjectId,),
    "bool": (bool,),
    "date": (datetime.datetime,),
    "null": (type(None),),
    "regex": (Regex, RE_TYPE),
    "dbPointer": (DBRef,),
    "javascript": (unicode_type, Code),
    "symbol": (unicode_type,),
    "javascriptWithScope": (unicode_type, Code),
    "int": (int,),
    "long": (Int64,),
    "decimal": (Decimal128,),
    "maxKey": (MaxKey,),
    "minKey": (MinKey,),
}


class MatchEvaluatorUtil(object):
    """Utility class that implements methods for evaluating matches as per
    the unified test format specification."""

    def __init__(self, test_class):
        self.test = test_class

    def _operation_exists(self, spec, actual, key_to_compare):
        if spec is True:
            if key_to_compare is None:
                assert actual is not None
            else:
                self.test.assertIn(key_to_compare, actual)
        elif spec is False:
            if key_to_compare is None:
                assert actual is None
            else:
                self.test.assertNotIn(key_to_compare, actual)
        else:
            self.test.fail("Expected boolean value for $$exists operator, got %s" % (spec,))

    def __type_alias_to_type(self, alias):
        if alias not in BSON_TYPE_ALIAS_MAP:
            self.test.fail("Unrecognized BSON type alias %s" % (alias,))
        return BSON_TYPE_ALIAS_MAP[alias]

    def _operation_type(self, spec, actual, key_to_compare):
        if isinstance(spec, abc.MutableSequence):
            permissible_types = tuple(
                [t for alias in spec for t in self.__type_alias_to_type(alias)]
            )
        else:
            permissible_types = self.__type_alias_to_type(spec)
        value = actual[key_to_compare] if key_to_compare else actual
        self.test.assertIsInstance(value, permissible_types)

    def _operation_matchesEntity(self, spec, actual, key_to_compare):
        expected_entity = self.test.entity_map[spec]
        self.test.assertEqual(expected_entity, actual[key_to_compare])

    def _operation_matchesHexBytes(self, spec, actual, key_to_compare):
        expected = binascii.unhexlify(spec)
        value = actual[key_to_compare] if key_to_compare else actual
        self.test.assertEqual(value, expected)

    def _operation_unsetOrMatches(self, spec, actual, key_to_compare):
        if key_to_compare is None and not actual:
            # top-level document can be None when unset
            return

        if key_to_compare not in actual:
            # we add a dummy value for the compared key to pass map size check
            actual[key_to_compare] = "dummyValue"
            return
        self.match_result(spec, actual[key_to_compare], in_recursive_call=True)

    def _operation_sessionLsid(self, spec, actual, key_to_compare):
        expected_lsid = self.test.entity_map.get_lsid_for_session(spec)
        self.test.assertEqual(expected_lsid, actual[key_to_compare])

    def _operation_lte(self, spec, actual, key_to_compare):
        if key_to_compare not in actual:
            self.test.fail(f"Actual command is missing the {key_to_compare} field: {spec}")
        self.test.assertLessEqual(actual[key_to_compare], spec)

    def _evaluate_special_operation(self, opname, spec, actual, key_to_compare):
        method_name = "_operation_%s" % (opname.strip("$"),)
        try:
            method = getattr(self, method_name)
        except AttributeError:
            self.test.fail("Unsupported special matching operator %s" % (opname,))
        else:
            method(spec, actual, key_to_compare)

    def _evaluate_if_special_operation(self, expectation, actual, key_to_compare=None):
        """Returns True if a special operation is evaluated, False
        otherwise. If the ``expectation`` map contains a single key,
        value pair we check it for a special operation.
        If given, ``key_to_compare`` is assumed to be the key in
        ``expectation`` whose corresponding value needs to be
        evaluated for a possible special operation. ``key_to_compare``
        is ignored when ``expectation`` has only one key."""
        if not isinstance(expectation, abc.Mapping):
            return False

        is_special_op, opname, spec = False, False, False

        if key_to_compare is not None:
            if key_to_compare.startswith("$$"):
                is_special_op = True
                opname = key_to_compare
                spec = expectation[key_to_compare]
                key_to_compare = None
            else:
                nested = expectation[key_to_compare]
                if isinstance(nested, abc.Mapping) and len(nested) == 1:
                    opname, spec = next(iter(nested.items()))
                    if opname.startswith("$$"):
                        is_special_op = True
        elif len(expectation) == 1:
            opname, spec = next(iter(expectation.items()))
            if opname.startswith("$$"):
                is_special_op = True
                key_to_compare = None

        if is_special_op:
            self._evaluate_special_operation(
                opname=opname, spec=spec, actual=actual, key_to_compare=key_to_compare
            )
            return True

        return False

    def _match_document(self, expectation, actual, is_root):
        if self._evaluate_if_special_operation(expectation, actual):
            return

        self.test.assertIsInstance(actual, abc.Mapping)
        for key, value in expectation.items():
            if self._evaluate_if_special_operation(expectation, actual, key):
                continue

            self.test.assertIn(key, actual)
            self.match_result(value, actual[key], in_recursive_call=True)

        if not is_root:
            expected_keys = set(expectation.keys())
            for key, value in expectation.items():
                if value == {"$$exists": False}:
                    expected_keys.remove(key)
            self.test.assertEqual(expected_keys, set(actual.keys()))

    def match_result(self, expectation, actual, in_recursive_call=False):
        if isinstance(expectation, abc.Mapping):
            return self._match_document(expectation, actual, is_root=not in_recursive_call)

        if isinstance(expectation, abc.MutableSequence):
            self.test.assertIsInstance(actual, abc.MutableSequence)
            for e, a in zip(expectation, actual):
                if isinstance(e, abc.Mapping):
                    self._match_document(e, a, is_root=not in_recursive_call)
                else:
                    self.match_result(e, a, in_recursive_call=True)
                return

        # account for flexible numerics in element-wise comparison
        if isinstance(expectation, int) or isinstance(expectation, float):
            self.test.assertEqual(expectation, actual)
        else:
            self.test.assertIsInstance(actual, type(expectation))
            self.test.assertEqual(expectation, actual)

    def assertHasServiceId(self, spec, actual):
        if "hasServiceId" in spec:
            if spec.get("hasServiceId"):
                self.test.assertIsNotNone(actual.service_id)
                self.test.assertIsInstance(actual.service_id, ObjectId)
            else:
                self.test.assertIsNone(actual.service_id)

    def match_event(self, event_type, expectation, actual):
        name, spec = next(iter(expectation.items()))

        # every command event has the commandName field
        if event_type == "command":
            command_name = spec.get("commandName")
            if command_name:
                self.test.assertEqual(command_name, actual.command_name)

        if name == "commandStartedEvent":
            self.test.assertIsInstance(actual, CommandStartedEvent)
            command = spec.get("command")
            database_name = spec.get("databaseName")
            if command:
                if actual.command_name == "update":
                    # TODO: remove this once PYTHON-1744 is done.
                    # Add upsert and multi fields back into expectations.
                    for update in command.get("updates", []):
                        update.setdefault("upsert", False)
                        update.setdefault("multi", False)
                self.match_result(command, actual.command)
            if database_name:
                self.test.assertEqual(database_name, actual.database_name)
            self.assertHasServiceId(spec, actual)
        elif name == "commandSucceededEvent":
            self.test.assertIsInstance(actual, CommandSucceededEvent)
            reply = spec.get("reply")
            if reply:
                self.match_result(reply, actual.reply)
            self.assertHasServiceId(spec, actual)
        elif name == "commandFailedEvent":
            self.test.assertIsInstance(actual, CommandFailedEvent)
            self.assertHasServiceId(spec, actual)
        elif name == "poolCreatedEvent":
            self.test.assertIsInstance(actual, PoolCreatedEvent)
        elif name == "poolReadyEvent":
            self.test.assertIsInstance(actual, PoolReadyEvent)
        elif name == "poolClearedEvent":
            self.test.assertIsInstance(actual, PoolClearedEvent)
            self.assertHasServiceId(spec, actual)
        elif name == "poolClosedEvent":
            self.test.assertIsInstance(actual, PoolClosedEvent)
        elif name == "connectionCreatedEvent":
            self.test.assertIsInstance(actual, ConnectionCreatedEvent)
        elif name == "connectionReadyEvent":
            self.test.assertIsInstance(actual, ConnectionReadyEvent)
        elif name == "connectionClosedEvent":
            self.test.assertIsInstance(actual, ConnectionClosedEvent)
            if "reason" in spec:
                self.test.assertEqual(actual.reason, spec["reason"])
        elif name == "connectionCheckOutStartedEvent":
            self.test.assertIsInstance(actual, ConnectionCheckOutStartedEvent)
        elif name == "connectionCheckOutFailedEvent":
            self.test.assertIsInstance(actual, ConnectionCheckOutFailedEvent)
            if "reason" in spec:
                self.test.assertEqual(actual.reason, spec["reason"])
        elif name == "connectionCheckedOutEvent":
            self.test.assertIsInstance(actual, ConnectionCheckedOutEvent)
        elif name == "connectionCheckedInEvent":
            self.test.assertIsInstance(actual, ConnectionCheckedInEvent)
        else:
            self.test.fail("Unsupported event type %s" % (name,))


def coerce_result(opname, result):
    """Convert a pymongo result into the spec's result format."""
    if hasattr(result, "acknowledged") and not result.acknowledged:
        return {"acknowledged": False}
    if opname == "bulkWrite":
        return parse_bulk_write_result(result)
    if opname == "insertOne":
        return {"insertedId": result.inserted_id}
    if opname == "insertMany":
        return {idx: _id for idx, _id in enumerate(result.inserted_ids)}
    if opname in ("deleteOne", "deleteMany"):
        return {"deletedCount": result.deleted_count}
    if opname in ("updateOne", "updateMany", "replaceOne"):
        return {
            "matchedCount": result.matched_count,
            "modifiedCount": result.modified_count,
            "upsertedCount": 0 if result.upserted_id is None else 1,
        }
    return result


class UnifiedSpecTestMixinV1(IntegrationTest):
    """Mixin class to run test cases from test specification files.

    Assumes that tests conform to the `unified test format
    <https://github.com/mongodb/specifications/blob/master/source/unified-test-format/unified-test-format.rst>`_.

    Specification of the test suite being currently run is available as
    a class attribute ``TEST_SPEC``.
    """

    SCHEMA_VERSION = Version.from_string("1.9")
    RUN_ON_LOAD_BALANCER = True
    RUN_ON_SERVERLESS = True
    TEST_SPEC: Any

    @staticmethod
    def should_run_on(run_on_spec):
        if not run_on_spec:
            # Always run these tests.
            return True

        for req in run_on_spec:
            if is_run_on_requirement_satisfied(req):
                return True
        return False

    def insert_initial_data(self, initial_data):
        for i, collection_data in enumerate(initial_data):
            coll_name = collection_data["collectionName"]
            db_name = collection_data["databaseName"]
            opts = collection_data.get("createOptions", {})
            documents = collection_data["documents"]

            # Setup the collection with as few majority writes as possible.
            db = self.client[db_name]
            db.drop_collection(coll_name)
            # Only use majority wc only on the final write.
            if i == len(initial_data) - 1:
                wc = WriteConcern(w="majority")
            else:
                wc = WriteConcern(w=1)
            if documents:
                if opts:
                    db.create_collection(coll_name, **opts)
                db.get_collection(coll_name, write_concern=wc).insert_many(documents)
            else:
                # Ensure collection exists
                db.create_collection(coll_name, write_concern=wc, **opts)

    @classmethod
    def setUpClass(cls):
        # super call creates internal client cls.client
        super(UnifiedSpecTestMixinV1, cls).setUpClass()
        # process file-level runOnRequirements
        run_on_spec = cls.TEST_SPEC.get("runOnRequirements", [])
        if not cls.should_run_on(run_on_spec):
            raise unittest.SkipTest("%s runOnRequirements not satisfied" % (cls.__name__,))

        # add any special-casing for skipping tests here
        if client_context.storage_engine == "mmapv1":
            if "retryable-writes" in cls.TEST_SPEC["description"]:
                raise unittest.SkipTest("MMAPv1 does not support retryWrites=True")

    def setUp(self):
        super(UnifiedSpecTestMixinV1, self).setUp()
        # process schemaVersion
        # note: we check major schema version during class generation
        # note: we do this here because we cannot run assertions in setUpClass
        version = Version.from_string(self.TEST_SPEC["schemaVersion"])
        self.assertLessEqual(
            version,
            self.SCHEMA_VERSION,
            "expected schema version %s or lower, got %s" % (self.SCHEMA_VERSION, version),
        )

        # initialize internals
        self.match_evaluator = MatchEvaluatorUtil(self)

    def maybe_skip_test(self, spec):
        # add any special-casing for skipping tests here
        if client_context.storage_engine == "mmapv1":
            if (
                "Dirty explicit session is discarded" in spec["description"]
                or "Dirty implicit session is discarded" in spec["description"]
            ):
                self.skipTest("MMAPv1 does not support retryWrites=True")
        if "Client side error in command starting transaction" in spec["description"]:
            self.skipTest("Implement PYTHON-1894")
        if "timeoutMS applied to entire download" in spec["description"]:
            self.skipTest("PyMongo's open_download_stream does not cap the stream's lifetime")

        class_name = self.__class__.__name__.lower()
        description = spec["description"].lower()
        if "csot" in class_name:
            if client_context.storage_engine == "mmapv1":
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
            if "socket timeout" in description:
                self.skipTest("CSOT not implemented for socket timeouts")

        # Some tests need to be skipped based on the operations they try to run.
        for op in spec["operations"]:
            name = op["name"]
            if name == "count":
                self.skipTest("PyMongo does not support count()")
            if name == "listIndexNames":
                self.skipTest("PyMongo does not support list_index_names()")
            if client_context.storage_engine == "mmapv1":
                if name == "createChangeStream":
                    self.skipTest("MMAPv1 does not support change streams")
                if name == "withTransaction" or name == "startTransaction":
                    self.skipTest("MMAPv1 does not support document-level locking")
            if not client_context.test_commands_enabled:
                if name == "failPoint" or name == "targetedFailPoint":
                    self.skipTest("Test commands must be enabled to use fail points")
            if name == "modifyCollection":
                self.skipTest("PyMongo does not support modifyCollection")
            if "timeoutMode" in op.get("arguments", {}):
                self.skipTest("PyMongo does not support timeoutMode")

    def process_error(self, exception, spec):
        is_error = spec.get("isError")
        is_client_error = spec.get("isClientError")
        is_timeout_error = spec.get("isTimeoutError")
        error_contains = spec.get("errorContains")
        error_code = spec.get("errorCode")
        error_code_name = spec.get("errorCodeName")
        error_labels_contain = spec.get("errorLabelsContain")
        error_labels_omit = spec.get("errorLabelsOmit")
        expect_result = spec.get("expectResult")

        if is_error:
            # already satisfied because exception was raised
            pass

        if is_client_error:
            # Connection errors are considered client errors.
            if isinstance(exception, ConnectionFailure):
                self.assertNotIsInstance(exception, NotPrimaryError)
            elif isinstance(exception, (InvalidOperation, ConfigurationError, EncryptionError)):
                pass
            else:
                self.assertNotIsInstance(exception, PyMongoError)

        if is_timeout_error:
            self.assertIsInstance(exception, PyMongoError)
            self.assertTrue(exception.timeout, msg=exception)

        if error_contains:
            if isinstance(exception, BulkWriteError):
                errmsg = str(exception.details).lower()
            else:
                errmsg = str(exception).lower()
            self.assertIn(error_contains.lower(), errmsg)

        if error_code:
            self.assertEqual(error_code, exception.details.get("code"))

        if error_code_name:
            self.assertEqual(error_code_name, exception.details.get("codeName"))

        if error_labels_contain:
            labels = [
                err_label
                for err_label in error_labels_contain
                if exception.has_error_label(err_label)
            ]
            self.assertEqual(labels, error_labels_contain)

        if error_labels_omit:
            for err_label in error_labels_omit:
                if exception.has_error_label(err_label):
                    self.fail("Exception '%s' unexpectedly had label '%s'" % (exception, err_label))

        if expect_result:
            if isinstance(exception, BulkWriteError):
                result = parse_bulk_write_error_result(exception)
                self.match_evaluator.match_result(expect_result, result)
            else:
                self.fail(
                    "expectResult can only be specified with %s exceptions" % (BulkWriteError,)
                )

    def __raise_if_unsupported(self, opname, target, *target_types):
        if not isinstance(target, target_types):
            self.fail("Operation %s not supported for entity of type %s" % (opname, type(target)))

    def __entityOperation_createChangeStream(self, target, *args, **kwargs):
        if client_context.storage_engine == "mmapv1":
            self.skipTest("MMAPv1 does not support change streams")
        self.__raise_if_unsupported("createChangeStream", target, MongoClient, Database, Collection)
        stream = target.watch(*args, **kwargs)
        self.addCleanup(stream.close)
        return stream

    def _clientOperation_createChangeStream(self, target, *args, **kwargs):
        return self.__entityOperation_createChangeStream(target, *args, **kwargs)

    def _databaseOperation_createChangeStream(self, target, *args, **kwargs):
        return self.__entityOperation_createChangeStream(target, *args, **kwargs)

    def _collectionOperation_createChangeStream(self, target, *args, **kwargs):
        return self.__entityOperation_createChangeStream(target, *args, **kwargs)

    def _databaseOperation_runCommand(self, target, **kwargs):
        self.__raise_if_unsupported("runCommand", target, Database)
        # Ensure the first key is the command name.
        ordered_command = SON([(kwargs.pop("command_name"), 1)])
        ordered_command.update(kwargs["command"])
        kwargs["command"] = ordered_command
        return target.command(**kwargs)

    def _databaseOperation_listCollections(self, target, *args, **kwargs):
        if "batch_size" in kwargs:
            kwargs["cursor"] = {"batchSize": kwargs.pop("batch_size")}
        cursor = target.list_collections(*args, **kwargs)
        return list(cursor)

    def _databaseOperation_createCollection(self, target, *args, **kwargs):
        # PYTHON-1936 Ignore the listCollections event from create_collection.
        kwargs["check_exists"] = False
        ret = target.create_collection(*args, **kwargs)
        return ret

    def __entityOperation_aggregate(self, target, *args, **kwargs):
        self.__raise_if_unsupported("aggregate", target, Database, Collection)
        return list(target.aggregate(*args, **kwargs))

    def _databaseOperation_aggregate(self, target, *args, **kwargs):
        return self.__entityOperation_aggregate(target, *args, **kwargs)

    def _collectionOperation_aggregate(self, target, *args, **kwargs):
        return self.__entityOperation_aggregate(target, *args, **kwargs)

    def _collectionOperation_find(self, target, *args, **kwargs):
        self.__raise_if_unsupported("find", target, Collection)
        find_cursor = target.find(*args, **kwargs)
        return list(find_cursor)

    def _collectionOperation_createFindCursor(self, target, *args, **kwargs):
        self.__raise_if_unsupported("find", target, Collection)
        if "filter" not in kwargs:
            self.fail('createFindCursor requires a "filter" argument')
        cursor = NonLazyCursor(target.find(*args, **kwargs), target.database.client)
        self.addCleanup(cursor.close)
        return cursor

    def _collectionOperation_count(self, target, *args, **kwargs):
        self.skipTest("PyMongo does not support collection.count()")

    def _collectionOperation_listIndexes(self, target, *args, **kwargs):
        if "batch_size" in kwargs:
            self.skipTest("PyMongo does not support batch_size for list_indexes")
        return list(target.list_indexes(*args, **kwargs))

    def _collectionOperation_listIndexNames(self, target, *args, **kwargs):
        self.skipTest("PyMongo does not support list_index_names")

    def _sessionOperation_withTransaction(self, target, *args, **kwargs):
        if client_context.storage_engine == "mmapv1":
            self.skipTest("MMAPv1 does not support document-level locking")
        self.__raise_if_unsupported("withTransaction", target, ClientSession)
        return target.with_transaction(*args, **kwargs)

    def _sessionOperation_startTransaction(self, target, *args, **kwargs):
        if client_context.storage_engine == "mmapv1":
            self.skipTest("MMAPv1 does not support document-level locking")
        self.__raise_if_unsupported("startTransaction", target, ClientSession)
        return target.start_transaction(*args, **kwargs)

    def _changeStreamOperation_iterateUntilDocumentOrError(self, target, *args, **kwargs):
        self.__raise_if_unsupported("iterateUntilDocumentOrError", target, ChangeStream)
        return next(target)

    def _cursor_iterateUntilDocumentOrError(self, target, *args, **kwargs):
        self.__raise_if_unsupported("iterateUntilDocumentOrError", target, NonLazyCursor)
        while target.alive:
            try:
                return next(target)
            except StopIteration:
                pass

    def _cursor_close(self, target, *args, **kwargs):
        self.__raise_if_unsupported("close", target, NonLazyCursor)
        return target.close()

    def _clientEncryptionOperation_createDataKey(self, target, *args, **kwargs):
        if "opts" in kwargs:
            opts = kwargs.pop("opts")
            kwargs["master_key"] = opts.get("masterKey")
            kwargs["key_alt_names"] = opts.get("keyAltNames")
            kwargs["key_material"] = opts.get("keyMaterial")
        return target.create_data_key(*args, **kwargs)

    def _clientEncryptionOperation_getKeys(self, target, *args, **kwargs):
        return list(target.get_keys(*args, **kwargs))

    def _clientEncryptionOperation_deleteKey(self, target, *args, **kwargs):
        result = target.delete_key(*args, **kwargs)
        response = result.raw_result
        response["deletedCount"] = result.deleted_count
        return response

    def _clientEncryptionOperation_rewrapManyDataKey(self, target, *args, **kwargs):
        if "opts" in kwargs:
            opts = kwargs.pop("opts")
            kwargs["provider"] = opts.get("provider")
            kwargs["master_key"] = opts.get("masterKey")
        data = target.rewrap_many_data_key(*args, **kwargs)
        if data.bulk_write_result:
            return dict(bulkWriteResult=parse_bulk_write_result(data.bulk_write_result))
        return dict()

    def _bucketOperation_download(self, target: GridFSBucket, *args: Any, **kwargs: Any) -> bytes:
        with target.open_download_stream(*args, **kwargs) as gout:
            return gout.read()

    def _bucketOperation_downloadByName(
        self, target: GridFSBucket, *args: Any, **kwargs: Any
    ) -> bytes:
        with target.open_download_stream_by_name(*args, **kwargs) as gout:
            return gout.read()

    def _bucketOperation_upload(self, target: GridFSBucket, *args: Any, **kwargs: Any) -> ObjectId:
        kwargs["source"] = binascii.unhexlify(kwargs.pop("source")["$$hexBytes"])
        if "content_type" in kwargs:
            kwargs.setdefault("metadata", {})["contentType"] = kwargs.pop("content_type")
        return target.upload_from_stream(*args, **kwargs)

    def _bucketOperation_uploadWithId(self, target: GridFSBucket, *args: Any, **kwargs: Any) -> Any:
        kwargs["source"] = binascii.unhexlify(kwargs.pop("source")["$$hexBytes"])
        if "content_type" in kwargs:
            kwargs.setdefault("metadata", {})["contentType"] = kwargs.pop("content_type")
        return target.upload_from_stream_with_id(*args, **kwargs)

    def _bucketOperation_find(
        self, target: GridFSBucket, *args: Any, **kwargs: Any
    ) -> List[GridOut]:
        return list(target.find(*args, **kwargs))

    def run_entity_operation(self, spec):
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
                spec, arguments, camel_to_snake(opname), self.entity_map, self.run_operations
            )
        else:
            arguments = {}

        if isinstance(target, MongoClient):
            method_name = "_clientOperation_%s" % (opname,)
        elif isinstance(target, Database):
            method_name = "_databaseOperation_%s" % (opname,)
        elif isinstance(target, Collection):
            method_name = "_collectionOperation_%s" % (opname,)
            # contentType is always stored in metadata in pymongo.
            if target.name.endswith(".files") and opname == "find":
                for doc in spec.get("expectResult", []):
                    if "contentType" in doc:
                        doc.setdefault("metadata", {})["contentType"] = doc.pop("contentType")
        elif isinstance(target, ChangeStream):
            method_name = "_changeStreamOperation_%s" % (opname,)
        elif isinstance(target, NonLazyCursor):
            method_name = "_cursor_%s" % (opname,)
        elif isinstance(target, ClientSession):
            method_name = "_sessionOperation_%s" % (opname,)
        elif isinstance(target, GridFSBucket):
            method_name = "_bucketOperation_%s" % (opname,)
            if "id" in arguments:
                arguments["file_id"] = arguments.pop("id")
            # MD5 is always disabled in pymongo.
            arguments.pop("disable_md5", None)
        elif isinstance(target, ClientEncryption):
            method_name = "_clientEncryptionOperation_%s" % (opname,)
        else:
            method_name = "doesNotExist"

        try:
            method = getattr(self, method_name)
        except AttributeError:
            target_opname = camel_to_snake(opname)
            if target_opname == "iterate_once":
                target_opname = "try_next"
            try:
                cmd = getattr(target, target_opname)
            except AttributeError:
                self.fail("Unsupported operation %s on entity %s" % (opname, target))
        else:
            cmd = functools.partial(method, target)

        try:
            # CSOT: Translate the spec test "timeout" arg into pymongo's context timeout API.
            if "timeout" in arguments:
                timeout = arguments.pop("timeout")
                with pymongo.timeout(timeout):
                    result = cmd(**dict(arguments))
            else:
                result = cmd(**dict(arguments))
        except Exception as exc:
            # Ignore all operation errors but to avoid masking bugs don't
            # ignore things like TypeError and ValueError.
            if ignore and isinstance(exc, (PyMongoError,)):
                return
            if expect_error:
                return self.process_error(exc, expect_error)
            raise
        else:
            if expect_error:
                self.fail(
                    'Excepted error %s but "%s" succeeded: %s' % (expect_error, opname, result)
                )

        if expect_result:
            actual = coerce_result(opname, result)
            self.match_evaluator.match_result(expect_result, actual)

        if save_as_entity:
            self.entity_map[save_as_entity] = result

    def __set_fail_point(self, client, command_args):
        if not client_context.test_commands_enabled:
            self.skipTest("Test commands must be enabled")

        cmd_on = SON([("configureFailPoint", "failCommand")])
        cmd_on.update(command_args)
        client.admin.command(cmd_on)
        self.addCleanup(
            client.admin.command, "configureFailPoint", cmd_on["configureFailPoint"], mode="off"
        )

    def _testOperation_failPoint(self, spec):
        self.__set_fail_point(
            client=self.entity_map[spec["client"]], command_args=spec["failPoint"]
        )

    def _testOperation_targetedFailPoint(self, spec):
        session = self.entity_map[spec["session"]]
        if not session._pinned_address:
            self.fail(
                "Cannot use targetedFailPoint operation with unpinned "
                "session %s" % (spec["session"],)
            )

        client = single_client("%s:%s" % session._pinned_address)
        self.addCleanup(client.close)
        self.__set_fail_point(client=client, command_args=spec["failPoint"])

    def _testOperation_createEntities(self, spec):
        self.entity_map.create_entities_from_spec(spec["entities"], uri=self._uri)

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

    def _testOperation_assertCollectionExists(self, spec):
        database_name = spec["databaseName"]
        collection_name = spec["collectionName"]
        collection_name_list = list(self.client.get_database(database_name).list_collection_names())
        self.assertIn(collection_name, collection_name_list)

    def _testOperation_assertCollectionNotExists(self, spec):
        database_name = spec["databaseName"]
        collection_name = spec["collectionName"]
        collection_name_list = list(self.client.get_database(database_name).list_collection_names())
        self.assertNotIn(collection_name, collection_name_list)

    def _testOperation_assertIndexExists(self, spec):
        collection = self.client[spec["databaseName"]][spec["collectionName"]]
        index_names = [idx["name"] for idx in collection.list_indexes()]
        self.assertIn(spec["indexName"], index_names)

    def _testOperation_assertIndexNotExists(self, spec):
        collection = self.client[spec["databaseName"]][spec["collectionName"]]
        for index in collection.list_indexes():
            self.assertNotEqual(spec["indexName"], index["name"])

    def _testOperation_assertNumberConnectionsCheckedOut(self, spec):
        client = self.entity_map[spec["client"]]
        pool = get_pool(client)
        self.assertEqual(spec["connections"], pool.active_sockets)

    def _testOperation_loop(self, spec):
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
                    self.run_entity_operation(op)
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

    def run_special_operation(self, spec):
        opname = spec["name"]
        method_name = "_testOperation_%s" % (opname,)
        try:
            method = getattr(self, method_name)
        except AttributeError:
            self.fail("Unsupported special test operation %s" % (opname,))
        else:
            method(spec["arguments"])

    def run_operations(self, spec):
        for op in spec:
            if op["object"] == "testRunner":
                self.run_special_operation(op)
            else:
                self.run_entity_operation(op)

    def check_events(self, spec):
        for event_spec in spec:
            client_name = event_spec["client"]
            events = event_spec["events"]
            # Valid types: 'command', 'cmap'
            event_type = event_spec.get("eventType", "command")
            ignore_extra_events = event_spec.get("ignoreExtraEvents", False)
            server_connection_id = event_spec.get("serverConnectionId")
            has_server_connection_id = event_spec.get("hasServerConnectionId", False)

            assert event_type in ("command", "cmap")

            listener = self.entity_map.get_listener_for_client(client_name)
            actual_events = listener.get_events(event_type)
            if ignore_extra_events:
                actual_events = actual_events[: len(events)]

            if len(events) == 0:
                self.assertEqual(actual_events, [])
                continue

            self.assertEqual(len(actual_events), len(events), actual_events)

            for idx, expected_event in enumerate(events):
                self.match_evaluator.match_event(event_type, expected_event, actual_events[idx])

            if has_server_connection_id:
                assert server_connection_id is not None
                assert server_connection_id >= 0
            else:
                assert server_connection_id is None

    def verify_outcome(self, spec):
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
                actual_documents = list(coll.find({}, sort=[("_id", ASCENDING)]))
                self.assertListEqual(sorted_expected_documents, actual_documents)

    def run_scenario(self, spec, uri=None):
        # maybe skip test manually
        self.maybe_skip_test(spec)

        # process test-level runOnRequirements
        run_on_spec = spec.get("runOnRequirements", [])
        if not self.should_run_on(run_on_spec):
            raise unittest.SkipTest("runOnRequirements not satisfied")

        # process skipReason
        skip_reason = spec.get("skipReason", None)
        if skip_reason is not None:
            raise unittest.SkipTest("%s" % (skip_reason,))

        # process createEntities
        self._uri = uri
        self.entity_map = EntityMapUtil(self)
        self.entity_map.create_entities_from_spec(self.TEST_SPEC.get("createEntities", []), uri=uri)
        # process initialData
        self.insert_initial_data(self.TEST_SPEC.get("initialData", []))

        # process operations
        self.run_operations(spec["operations"])

        # process expectEvents
        if "expectEvents" in spec:
            expect_events = spec["expectEvents"]
            self.assertTrue(expect_events, "expectEvents must be non-empty")
            self.check_events(expect_events)

        # process outcome
        self.verify_outcome(spec.get("outcome", []))


class UnifiedSpecTestMeta(type):
    """Metaclass for generating test classes."""

    TEST_SPEC: Any
    EXPECTED_FAILURES: Any

    def __init__(cls, *args, **kwargs):
        super(UnifiedSpecTestMeta, cls).__init__(*args, **kwargs)

        def create_test(spec):
            def test_case(self):
                self.run_scenario(spec)

            return test_case

        for test_spec in cls.TEST_SPEC["tests"]:
            description = test_spec["description"]
            test_name = "test_%s" % (description.strip(". ").replace(" ", "_").replace(".", "_"),)
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
    the names of test classes and values are the test class objects."""
    test_klasses = {}

    def test_base_class_factory(test_spec):
        """Utility that creates the base class to use for test generation.
        This is needed to ensure that cls.TEST_SPEC is appropriately set when
        the metaclass __init__ is invoked."""

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
            snake_class_name = "Test%s_%s_%s" % (
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
                        "test file '%s' has unsupported schemaVersion '%s'"
                        % (fpath, schema_version)
                    )
                module_dict = {"__module__": module}
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
