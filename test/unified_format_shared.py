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

"""Shared utility functions and constants for the unified test format runner.

https://github.com/mongodb/specifications/blob/master/source/unified-test-format/unified-test-format.md
"""
from __future__ import annotations

import binascii
import collections
import datetime
import os
import time
import types
from collections import abc
from test.helpers_shared import (
    AWS_CREDS,
    AWS_CREDS_2,
    AWS_TEMP_CREDS,
    AZURE_CREDS,
    CA_PEM,
    CLIENT_PEM,
    GCP_CREDS,
    KMIP_CREDS,
    LOCAL_MASTER_KEY,
)
from test.utils_shared import CMAPListener, camel_to_snake, parse_collection_options
from typing import Any, MutableMapping, Union

from bson import (
    RE_TYPE,
    Binary,
    Code,
    DBRef,
    Decimal128,
    Int64,
    MaxKey,
    MinKey,
    ObjectId,
    Regex,
    json_util,
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
    ServerClosedEvent,
    ServerDescriptionChangedEvent,
    ServerHeartbeatFailedEvent,
    ServerHeartbeatListener,
    ServerHeartbeatStartedEvent,
    ServerHeartbeatSucceededEvent,
    ServerListener,
    ServerOpeningEvent,
    TopologyClosedEvent,
    TopologyDescriptionChangedEvent,
    TopologyEvent,
    TopologyListener,
    TopologyOpenedEvent,
    _CommandEvent,
    _ConnectionEvent,
    _PoolEvent,
    _ServerEvent,
    _ServerHeartbeatEvent,
)
from pymongo.results import BulkWriteResult
from pymongo.server_description import ServerDescription
from pymongo.topology_description import TopologyDescription

JSON_OPTS = json_util.JSONOptions(tz_aware=False)

IS_INTERRUPTED = False

KMS_TLS_OPTS = {
    "kmip": {
        "tlsCAFile": CA_PEM,
        "tlsCertificateKeyFile": CLIENT_PEM,
    }
}


# Build up a placeholder maps.
PLACEHOLDER_MAP = {}
for provider_name, provider_data in [
    ("local", {"key": LOCAL_MASTER_KEY}),
    ("local:name1", {"key": LOCAL_MASTER_KEY}),
    ("aws_temp", AWS_TEMP_CREDS),
    ("aws", AWS_CREDS),
    ("aws:name1", AWS_CREDS),
    ("aws:name2", AWS_CREDS_2),
    ("azure", AZURE_CREDS),
    ("azure:name1", AZURE_CREDS),
    ("gcp", GCP_CREDS),
    ("gcp:name1", GCP_CREDS),
    ("kmip", KMIP_CREDS),
    ("kmip:name1", KMIP_CREDS),
]:
    for key, value in provider_data.items():
        placeholder = f"/clientEncryptionOpts/kmsProviders/{provider_name}/{key}"
        PLACEHOLDER_MAP[placeholder] = value

        placeholder = f"/autoEncryptOpts/kmsProviders/{provider_name}/{key}"
        PLACEHOLDER_MAP[placeholder] = value

OIDC_ENV = os.environ.get("OIDC_ENV", "test")
if OIDC_ENV == "test":
    PLACEHOLDER_MAP["/uriOptions/authMechanismProperties"] = {"ENVIRONMENT": "test"}
elif OIDC_ENV == "azure":
    PLACEHOLDER_MAP["/uriOptions/authMechanismProperties"] = {
        "ENVIRONMENT": "azure",
        "TOKEN_RESOURCE": os.environ["AZUREOIDC_RESOURCE"],
    }
elif OIDC_ENV == "gcp":
    PLACEHOLDER_MAP["/uriOptions/authMechanismProperties"] = {
        "ENVIRONMENT": "gcp",
        "TOKEN_RESOURCE": os.environ["GCPOIDC_AUDIENCE"],
    }
elif OIDC_ENV == "k8s":
    PLACEHOLDER_MAP["/uriOptions/authMechanismProperties"] = {"ENVIRONMENT": "k8s"}


def with_metaclass(meta, *bases):
    """Create a base class with a metaclass.

    Vendored from six: https://github.com/benjaminp/six/blob/master/six.py
    """

    # This requires a bit of explanation: the basic idea is to make a dummy
    # metaclass for one level of class instantiation that replaces itself with
    # the actual metaclass.
    class metaclass(type):
        def __new__(cls, name, this_bases, d):
            # __orig_bases__ is required by PEP 560.
            resolved_bases = types.resolve_bases(bases)
            if resolved_bases is not bases:
                d["__orig_bases__"] = bases
            return meta(name, resolved_bases, d)

        @classmethod
        def __prepare__(
            cls, name: str, this_bases: tuple[type, ...], /, **kwds: Any
        ) -> MutableMapping[str, object]:
            return meta.__prepare__(name, bases)

    return type.__new__(metaclass, "temporary_class", (), {})


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


def parse_client_bulk_write_individual(op_type, result):
    if op_type == "insert":
        return {"insertedId": result.inserted_id}
    if op_type == "update":
        if result.upserted_id:
            return {
                "matchedCount": result.matched_count,
                "modifiedCount": result.modified_count,
                "upsertedId": result.upserted_id,
            }
        else:
            return {
                "matchedCount": result.matched_count,
                "modifiedCount": result.modified_count,
            }
    if op_type == "delete":
        return {
            "deletedCount": result.deleted_count,
        }


def parse_client_bulk_write_result(result):
    insert_results, update_results, delete_results = {}, {}, {}
    if result.has_verbose_results:
        for idx, res in result.insert_results.items():
            insert_results[str(idx)] = parse_client_bulk_write_individual("insert", res)
        for idx, res in result.update_results.items():
            update_results[str(idx)] = parse_client_bulk_write_individual("update", res)
        for idx, res in result.delete_results.items():
            delete_results[str(idx)] = parse_client_bulk_write_individual("delete", res)

    return {
        "deletedCount": result.deleted_count,
        "insertedCount": result.inserted_count,
        "matchedCount": result.matched_count,
        "modifiedCount": result.modified_count,
        "upsertedCount": result.upserted_count,
        "insertResults": insert_results,
        "updateResults": update_results,
        "deleteResults": delete_results,
    }


def parse_bulk_write_error_result(error):
    write_result = BulkWriteResult(error.details, True)
    return parse_bulk_write_result(write_result)


def parse_client_bulk_write_error_result(error):
    write_result = error.partial_result
    if not write_result:
        return None
    return parse_client_bulk_write_result(write_result)


class EventListenerUtil(
    CMAPListener, CommandListener, ServerListener, ServerHeartbeatListener, TopologyListener
):
    def __init__(
        self, observe_events, ignore_commands, observe_sensitive_commands, store_events, entity_map
    ):
        self._event_types = {name.lower() for name in observe_events}
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
        super().__init__()

    def get_events(self, event_type):
        assert event_type in ("command", "cmap", "sdam", "all"), event_type
        if event_type == "all":
            return list(self.events)
        if event_type == "command":
            return [e for e in self.events if isinstance(e, _CommandEvent)]
        if event_type == "cmap":
            return [e for e in self.events if isinstance(e, (_ConnectionEvent, _PoolEvent))]
        return [
            e
            for e in self.events
            if isinstance(e, (_ServerEvent, TopologyEvent, _ServerHeartbeatEvent))
        ]

    def add_event(self, event):
        event_name = type(event).__name__.lower()
        if event_name in self._event_types:
            super().add_event(event)
        for id in self._event_mapping[event_name]:
            self.entity_map[id].append(
                {
                    "name": type(event).__name__,
                    "observedAt": time.time(),
                    "description": repr(event),
                }
            )

    def _command_event(self, event):
        if event.command_name.lower() not in self._ignore_commands:
            self.add_event(event)

    def started(self, event):
        if isinstance(event, CommandStartedEvent):
            if event.command == {}:
                # Command is redacted. Observe only if flag is set.
                if self._observe_sensitive_commands:
                    self._command_event(event)
            else:
                self._command_event(event)
        else:
            self.add_event(event)

    def succeeded(self, event):
        if isinstance(event, CommandSucceededEvent):
            if event.reply == {}:
                # Command is redacted. Observe only if flag is set.
                if self._observe_sensitive_commands:
                    self._command_event(event)
            else:
                self._command_event(event)
        else:
            self.add_event(event)

    def failed(self, event):
        if isinstance(event, CommandFailedEvent):
            self._command_event(event)
        else:
            self.add_event(event)

    def opened(self, event: Union[ServerOpeningEvent, TopologyOpenedEvent]) -> None:
        self.add_event(event)

    def description_changed(
        self, event: Union[ServerDescriptionChangedEvent, TopologyDescriptionChangedEvent]
    ) -> None:
        self.add_event(event)

    def topology_changed(self, event: TopologyDescriptionChangedEvent) -> None:
        self.add_event(event)

    def closed(self, event: Union[ServerClosedEvent, TopologyClosedEvent]) -> None:
        self.add_event(event)


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
    "number": (float, int, Int64, Decimal128),
}


class MatchEvaluatorUtil:
    """Utility class that implements methods for evaluating matches as per
    the unified test format specification.
    """

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
            self.test.fail(f"Expected boolean value for $$exists operator, got {spec}")

    def __type_alias_to_type(self, alias):
        if alias not in BSON_TYPE_ALIAS_MAP:
            self.test.fail(f"Unrecognized BSON type alias {alias}")
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

    def _operation_matchAsDocument(self, spec, actual, key_to_compare):
        self._match_document(spec, json_util.loads(actual[key_to_compare]), False, test=True)

    def _operation_matchAsRoot(self, spec, actual, key_to_compare):
        if key_to_compare:
            actual = actual[key_to_compare]
        self._match_document(spec, actual, True, test=True)

    def _evaluate_special_operation(self, opname, spec, actual, key_to_compare):
        method_name = "_operation_{}".format(opname.strip("$"))
        try:
            method = getattr(self, method_name)
        except AttributeError:
            self.test.fail(f"Unsupported special matching operator {opname}")
        else:
            method(spec, actual, key_to_compare)

    def _evaluate_if_special_operation(self, expectation, actual, key_to_compare=None):
        """Returns True if a special operation is evaluated, False
        otherwise. If the ``expectation`` map contains a single key,
        value pair we check it for a special operation.
        If given, ``key_to_compare`` is assumed to be the key in
        ``expectation`` whose corresponding value needs to be
        evaluated for a possible special operation. ``key_to_compare``
        is ignored when ``expectation`` has only one key.
        """
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

    def _match_document(self, expectation, actual, is_root, test=False):
        if self._evaluate_if_special_operation(expectation, actual):
            return True

        self.test.assertIsInstance(actual, abc.Mapping)
        for key, value in expectation.items():
            if self._evaluate_if_special_operation(expectation, actual, key):
                continue

            self.test.assertIn(key, actual)
            if not self.match_result(value, actual[key], in_recursive_call=True, test=test):
                return False

        if not is_root:
            expected_keys = set(expectation.keys())
            for key, value in expectation.items():
                if value == {"$$exists": False}:
                    expected_keys.remove(key)
            if test:
                self.test.assertEqual(expected_keys, set(actual.keys()))
            else:
                return set(expected_keys).issubset(set(actual.keys()))
        return True

    def match_result(self, expectation, actual, in_recursive_call=False, test=True):
        if isinstance(expectation, abc.Mapping):
            return self._match_document(
                expectation, actual, is_root=not in_recursive_call, test=test
            )

        if isinstance(expectation, abc.MutableSequence):
            self.test.assertIsInstance(actual, abc.MutableSequence)
            for e, a in zip(expectation, actual):
                if isinstance(e, abc.Mapping):
                    res = self._match_document(e, a, is_root=not in_recursive_call, test=test)
                else:
                    res = self.match_result(e, a, in_recursive_call=True, test=test)
                if not res:
                    return False
            return True

        # account for flexible numerics in element-wise comparison
        if isinstance(expectation, (int, float)):
            if test:
                self.test.assertEqual(expectation, actual)
            else:
                return expectation == actual
        else:
            if test:
                self.test.assertIsInstance(actual, type(expectation))
                self.test.assertEqual(expectation, actual)
            else:
                return isinstance(actual, type(expectation)) and expectation == actual
        return True

    def match_server_description(self, actual: ServerDescription, spec: dict) -> None:
        for field, expected in spec.items():
            field = camel_to_snake(field)
            if field == "type":
                field = "server_type_name"
            self.test.assertEqual(getattr(actual, field), expected)

    def match_topology_description(self, actual: TopologyDescription, spec: dict) -> None:
        for field, expected in spec.items():
            field = camel_to_snake(field)
            if field == "type":
                field = "topology_type_name"
            self.test.assertEqual(getattr(actual, field), expected)

    def match_event_fields(self, actual: Any, spec: dict) -> None:
        for field, expected in spec.items():
            if field == "command" and isinstance(actual, CommandStartedEvent):
                command = spec["command"]
                if command:
                    self.match_result(command, actual.command)
                continue
            if field == "reply" and isinstance(actual, CommandSucceededEvent):
                reply = spec["reply"]
                if reply:
                    self.match_result(reply, actual.reply)
                continue
            if field == "hasServiceId":
                if spec["hasServiceId"]:
                    self.test.assertIsNotNone(actual.service_id)
                    self.test.assertIsInstance(actual.service_id, ObjectId)
                else:
                    self.test.assertIsNone(actual.service_id)
                continue
            if field == "hasServerConnectionId":
                if spec["hasServerConnectionId"]:
                    self.test.assertIsNotNone(actual.server_connection_id)
                    self.test.assertIsInstance(actual.server_connection_id, int)
                else:
                    self.test.assertIsNone(actual.server_connection_id)
                continue
            if field in ("previousDescription", "newDescription"):
                if isinstance(actual, ServerDescriptionChangedEvent):
                    self.match_server_description(
                        getattr(actual, camel_to_snake(field)), spec[field]
                    )
                    continue
                if isinstance(actual, TopologyDescriptionChangedEvent):
                    self.match_topology_description(
                        getattr(actual, camel_to_snake(field)), spec[field]
                    )
                    continue

            if field == "interruptInUseConnections":
                field = "interrupt_connections"
            else:
                field = camel_to_snake(field)
            self.test.assertEqual(getattr(actual, field), expected)

    def match_event(self, expectation, actual):
        name, spec = next(iter(expectation.items()))
        if name == "commandStartedEvent":
            self.test.assertIsInstance(actual, CommandStartedEvent)
        elif name == "commandSucceededEvent":
            self.test.assertIsInstance(actual, CommandSucceededEvent)
        elif name == "commandFailedEvent":
            self.test.assertIsInstance(actual, CommandFailedEvent)
        elif name == "poolCreatedEvent":
            self.test.assertIsInstance(actual, PoolCreatedEvent)
        elif name == "poolReadyEvent":
            self.test.assertIsInstance(actual, PoolReadyEvent)
        elif name == "poolClearedEvent":
            self.test.assertIsInstance(actual, PoolClearedEvent)
            self.test.assertIsInstance(actual.interrupt_connections, bool)
        elif name == "poolClosedEvent":
            self.test.assertIsInstance(actual, PoolClosedEvent)
        elif name == "connectionCreatedEvent":
            self.test.assertIsInstance(actual, ConnectionCreatedEvent)
        elif name == "connectionReadyEvent":
            self.test.assertIsInstance(actual, ConnectionReadyEvent)
        elif name == "connectionClosedEvent":
            self.test.assertIsInstance(actual, ConnectionClosedEvent)
        elif name == "connectionCheckOutStartedEvent":
            self.test.assertIsInstance(actual, ConnectionCheckOutStartedEvent)
        elif name == "connectionCheckOutFailedEvent":
            self.test.assertIsInstance(actual, ConnectionCheckOutFailedEvent)
        elif name == "connectionCheckedOutEvent":
            self.test.assertIsInstance(actual, ConnectionCheckedOutEvent)
        elif name == "connectionCheckedInEvent":
            self.test.assertIsInstance(actual, ConnectionCheckedInEvent)
        elif name == "serverDescriptionChangedEvent":
            self.test.assertIsInstance(actual, ServerDescriptionChangedEvent)
        elif name == "serverHeartbeatStartedEvent":
            self.test.assertIsInstance(actual, ServerHeartbeatStartedEvent)
        elif name == "serverHeartbeatSucceededEvent":
            self.test.assertIsInstance(actual, ServerHeartbeatSucceededEvent)
        elif name == "serverHeartbeatFailedEvent":
            self.test.assertIsInstance(actual, ServerHeartbeatFailedEvent)
        elif name == "topologyDescriptionChangedEvent":
            self.test.assertIsInstance(actual, TopologyDescriptionChangedEvent)
        elif name == "topologyOpeningEvent":
            self.test.assertIsInstance(actual, TopologyOpenedEvent)
        elif name == "topologyClosedEvent":
            self.test.assertIsInstance(actual, TopologyClosedEvent)
        else:
            raise Exception(f"Unsupported event type {name}")

        self.match_event_fields(actual, spec)


def coerce_result(opname, result):
    """Convert a pymongo result into the spec's result format."""
    if hasattr(result, "acknowledged") and not result.acknowledged:
        return {"acknowledged": False}
    if opname == "bulkWrite":
        return parse_bulk_write_result(result)
    if opname == "clientBulkWrite":
        return parse_client_bulk_write_result(result)
    if opname == "insertOne":
        return {"insertedId": result.inserted_id}
    if opname == "insertMany":
        return dict(enumerate(result.inserted_ids))
    if opname in ("deleteOne", "deleteMany"):
        return {"deletedCount": result.deleted_count}
    if opname in ("updateOne", "updateMany", "replaceOne"):
        value = {
            "matchedCount": result.matched_count,
            "modifiedCount": result.modified_count,
            "upsertedCount": 0 if result.upserted_id is None else 1,
        }
        if result.upserted_id is not None:
            value["upsertedId"] = result.upserted_id
        return value
    return result
