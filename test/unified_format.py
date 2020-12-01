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

import copy
import functools
import os
import sys
import types

from bson import json_util, SON
from bson.py3compat import abc, iteritems, text_type

from pymongo import operations, ASCENDING, MongoClient
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.monitoring import (
    CommandFailedEvent, CommandListener, CommandStartedEvent,
    CommandSucceededEvent)
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

from test import client_context, unittest, IntegrationTest
from test.utils import (
    camel_to_snake, camel_to_snake_args, rs_or_single_client,
    snake_to_camel, ScenarioDict)

from test.version import Version
from test.utils import (camel_to_upper_camel,
                        parse_spec_options,
                        prepare_spec_arguments)


JSON_OPTS = json_util.JSONOptions(tz_aware=False)


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
                    d['__orig_bases__'] = bases
            else:
                resolved_bases = bases
            return meta(name, resolved_bases, d)

        @classmethod
        def __prepare__(cls, name, this_bases):
            return meta.__prepare__(name, bases)
    return type.__new__(metaclass, 'temporary_class', (), {})


def is_run_on_requirement_satisfied(requirement):
    topology_satisfied = True
    req_topologies = requirement.get('topologies')
    if req_topologies:
        topology_satisfied = client_context.is_topology_type(
            req_topologies)

    min_version_satisfied = True
    req_min_server_version = requirement.get('minServerVersion')
    if req_min_server_version:
        min_version_satisfied = Version.from_string(
            req_min_server_version) <= client_context.version

    max_version_satisfied = True
    req_max_server_version = requirement.get('maxServerVersion')
    if req_max_server_version:
        max_version_satisfied = Version.from_string(
            req_max_server_version) >= client_context.version

    return (topology_satisfied and min_version_satisfied and
            max_version_satisfied)


class EventListenerUtil(CommandListener):
    def __init__(self, observe_events, ignore_commands):
        self._event_types = set(observe_events)
        self._ignore_commands = set(ignore_commands)
        self._ignore_commands.add('configureFailPoint')
        self.results = []

    def _observe_event(self, event):
        if event.command_name not in self._ignore_commands:
            self.results.append(event)

    def started(self, event):
        if 'commandStartedEvent' in self._event_types:
            self._observe_event(event)

    def succeeded(self, event):
        if 'commandSucceededEvent' in self._event_types:
            self._observe_event(event)

    def failed(self, event):
        if 'commandFailedEvent' in self._event_types:
            self._observe_event(event)


class EntityMapUtil(object):
    """Utility class that implements an entity map as per the unified
    test format specification."""
    def __init__(self, test_class):
        self._entities = {}
        self._listeners = {}
        self._test_class = test_class

    def __getitem__(self, item):
        return self._entities[item]

    def __setitem__(self, key, value):
        if not isinstance(key, text_type):
            self._test_class.fail(
                'Expected entity name of type str, got %s' % (type(key)))

        if key in self._entities:
            self._test_class.fail('Entity named %s already in map' % (key,))

        self._entities[key] = value

    def _create_entity(self, entity_spec):
        if len(entity_spec) != 1:
            self._test_class.fail(
                "Entity spec %s did not contain exactly one top-level key" % (
                    entity_spec,))

        entity_type, spec = next(iteritems(entity_spec))
        if entity_type == 'client':
            # TODO
            # Add logic to respect the following fields
            # - uriOptions
            # - useMultipleMongoses
            # - observeEvents
            # - ignoreCommandMonitoringEvents
            observe_events = spec.get('observeEvents')
            ignore_commands = spec.get('ignoreCommandMonitoringEvents', [])
            if observe_events:
                listener = EventListenerUtil(observe_events, ignore_commands)
                client = rs_or_single_client(event_listeners=[listener])
            else:
                listener = None
                client = rs_or_single_client()
            self[spec['id']] = client
            self._listeners[spec['id']] = listener
            self._test_class.addCleanup(client.close)
            return
        elif entity_type == 'database':
            # TODO
            # Add logic to respect the following fields
            # - databaseOptions
            client = self[spec['client']]
            if not isinstance(client, MongoClient):
                self._test_class.fail(
                    'Expected entity %s to be of type MongoClient, got %s' % (
                        spec['client'], type(client)))
            self[spec['id']] = client.get_database(spec['databaseName'])
            return
        elif entity_type == 'collection':
            # TODO
            # Add logic to respect the following fields
            # - collectionOptions
            database = self[spec['database']]
            if not isinstance(database, Database):
                self._test_class.fail(
                    'Expected entity %s to be of type Database, got %s' % (
                        spec['database'], type(database)))
            self[spec['id']] = database.get_collection(spec['collectionName'])
            return
        # elif ...
            # TODO
            # Implement the following entity types:
            # - session
            # - bucket
        self._test_class.fail(
            'Unable to create entity of unknown type %s' % (entity_type,))

    def create_entities_from_spec(self, entity_spec):
        for spec in entity_spec:
            self._create_entity(spec)

    def get_listener_for_client(self, client_name):
        client = self[client_name]
        if not isinstance(client, MongoClient):
            self._test_class.fail(
                'Expected entity %s to be of type MongoClient, got %s' % (
                        client_name, type(client)))

        listener = self._listeners[client_name]
        if not listener:
            self._test_class.fail(
                'No listeners configured for client %s' % (client_name,))

        return listener


class MatchEvaluatorUtil(object):
    """Utility class that implements methods for evaluating matches as per
    the unified test format specification."""
    def __init__(self, test_class):
        self._test_class = test_class

    def _evaluate_special_operation(self, operation, expectation, actual):
        pass

    def _evaluate_if_special_operation(self, expectation, actual):
        if isinstance(expectation, abc.Mapping) and len(expectation) == 1:
            key, value = next(iteritems(expectation))
            if key.startswith('$$'):
                self._evaluate_special_operation(
                    operation=key,
                    expectation=value,
                    actual=actual)
                return True
        return False

    def _match_document(self, expectation, actual):
        if self._evaluate_if_special_operation(expectation, actual):
            return

        self._test_class.assertIsInstance(actual, abc.Mapping)
        for key, value in iteritems(expectation):
            if self._evaluate_if_special_operation({key: value}, actual):
                continue

            self._test_class.assertIn(key, actual)
            self.match_result(value, actual[key])

        # TODO: handle if expected is not root
        return

    def _match_array(self, expectation, actual):
        self._test_class.assertIsInstance(actual, abc.Iterable)

        for e, a in zip(expectation, actual):
            self.match_result(e, a)

    def match_result(self, expectation, actual):
        if isinstance(expectation, abc.Mapping):
            return self._match_document(expectation, actual)

        if isinstance(expectation, abc.MutableSequence):
            return self._match_array(expectation, actual)

        self._test_class.assertIsInstance(actual, type(expectation))
        self._test_class.assertEqual(expectation, actual)

    def match_event(self, expectation, actual):
        event_type, spec = next(iteritems(expectation))

        # every event type has the commandName field
        command_name = spec.get('commandName')
        if command_name:
            self._test_class.assertEqual(command_name, actual.command_name)

        if event_type == 'commandStartedEvent':
            self._test_class.assertIsInstance(actual, CommandStartedEvent)
            command = spec.get('command')
            database_name = spec.get('databaseName')
            if command:
                self.match_result(command, actual.command)
            if database_name:
                self._test_class.assertEqual(
                    database_name, actual.database_name)
        elif event_type == 'commandSucceededEvent':
            self._test_class.assertIsInstance(actual, CommandSucceededEvent)
            reply = spec.get('reply')
            if reply:
                self.match_result(reply, actual.reply)
        elif event_type == 'commandFailedEvent':
            self._test_class.assertIsInstance(actual, CommandFailedEvent)
        else:
            self._test_class.fail(
                'Unsupported event type %s' % (event_type,))


class UnifiedSpecTestMixin(IntegrationTest):
    """Mixin class to run test cases from test specification files.

    Assumes that tests conform to the `unified test format
    <https://github.com/mongodb/specifications/blob/master/source/unified-test-format/unified-test-format.rst>`_.

    Specification of the test suite being currently run is available as
    a class attribute ``TEST_SPEC``.
    """
    SCHEMA_VERSION = '1.0'

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
        for collection_data in initial_data:
            coll_name = collection_data['collectionName']
            db_name = collection_data['databaseName']
            documents = collection_data['documents']

            coll = self.client.get_database(db_name).get_collection(
                coll_name, write_concern=WriteConcern(w="majority"))
            coll.drop()

            # documents MAY be an empty list
            if documents:
                coll.insert_many(documents)

    @classmethod
    def setUpClass(cls):
        # super call creates internal client cls.client
        super(UnifiedSpecTestMixin, cls).setUpClass()

        # process schemaVersion
        version = cls.TEST_SPEC['schemaVersion']
        version_tuple = tuple(version.split('.', 2)[:2])
        max_version_tuple = tuple(cls.SCHEMA_VERSION.split('.', 2)[:2])
        if not version_tuple <= max_version_tuple:
            raise unittest.SkipTest(
                'expected schemaVersion %s or lower, got %s' % (
                    cls.SCHEMA_VERSION, version))

        # process file-level runOnRequirements
        run_on_spec = cls.TEST_SPEC.get('runOnRequirements', [])
        if not cls.should_run_on(run_on_spec):
            raise unittest.SkipTest('runOnRequirements not satisfied')

    @classmethod
    def tearDownClass(cls):
        super(UnifiedSpecTestMixin, cls).tearDownClass()
        cls.client.close()

    def setUp(self):
        super(UnifiedSpecTestMixin, self).setUp()

        # process createEntities
        self.entity_map = EntityMapUtil(self)
        self.entity_map.create_entities_from_spec(
            self.TEST_SPEC.get('createEntities', []))

        # process initialData
        self.insert_initial_data(self.TEST_SPEC.get('initialData', []))

        # initialize internals
        self.match_evaluator = MatchEvaluatorUtil(self)

    def run_entity_operation(self, spec):
        target = self.entity_map[spec['object']]
        opname = camel_to_snake(spec['name'])
        opargs = spec.get('arguments')
        expect_error = spec.get('expectError')
        if expect_error:
            # TODO: process expectedError object
            # See L420-446 of utils_spec_runner.py
            return

        # Operation expected to succeed
        arguments = parse_spec_options(copy.deepcopy(opargs))
        cmd = getattr(target, opname)
        prepare_spec_arguments(spec, arguments, opname, self.entity_map,
                               None)
        result = cmd(**dict(arguments))

        if isinstance(result, Cursor):
            result = list(result)

        expected_result = spec.get('expectResult')
        self.match_evaluator.match_result(expected_result, result)

        save_as_entity = spec.get('saveResultAsEntity')
        if save_as_entity:
            self.entity_map[save_as_entity] = result

    def _operation_failPoint(self, spec):
        client = self.entity_map[spec['client']]
        command_args = spec['failPoint']
        cmd_on = SON([('configureFailPoint', 'failCommand')])
        cmd_on.update(command_args)
        client.admin.command(cmd_on)
        self.addCleanup(
            client.admin.command,
            'configureFailPoint', cmd_on['configureFailPoint'], mode='off')

    def _operation_targetedFailPoint(self, spec):
        raise NotImplementedError

    def _operation_assertSessionTransactionState(self, spec):
        raise NotImplementedError

    def _operation_assertSessionPinned(self, spec):
        raise NotImplementedError

    def _operation_assertSessionUnpinned(self, spec):
        raise NotImplementedError

    def _operation_assertDifferentLsidOnLastTwoCommands(self, spec):
        raise NotImplementedError

    def _operation_assertSameLsidOnLastTwoCommands(self, spec):
        raise NotImplementedError

    def _operation_assertSessionDirty(self, spec):
        raise NotImplementedError

    def _operation_assertSessionNotDirty(self, spec):
        raise NotImplementedError

    def _operation_assertCollectionExists(self, spec):
        database_name = spec['databaseName']
        collection_name = spec['collectionName']
        collection_name_list = list(
            self.client.get_database(database_name).list_collection_names())
        self.assertIn(collection_name, collection_name_list)

    def _operation_assertCollectionNotExists(self, spec):
        database_name = spec['databaseName']
        collection_name = spec['collectionName']
        collection_name_list = list(
            self.client.get_database(database_name).list_collection_names())
        self.assertNotIn(collection_name, collection_name_list)

    def _operation_assertIndexExists(self, spec):
        raise NotImplementedError

    def _operation_assertIndexNotExists(self, spec):
        raise NotImplementedError

    def run_special_operation(self, spec):
        opname = spec['name']
        method_name = '_operation_%s' % (opname,)
        try:
            method = getattr(self, method_name)
        except AttributeError:
            self.fail('Unsupported special test operation %s' % (opname,))
        else:
            method(spec['arguments'])

    def run_operations(self, spec):
        for op in spec:
            target = op['object']
            if target != 'testRunner':
                self.run_entity_operation(op)
            else:
                self.run_special_operation(op)

    def check_events(self, spec):
        for event_spec in spec:
            client_name = event_spec['client']
            events = event_spec['events']
            listener = self.entity_map.get_listener_for_client(client_name)

            if len(events) == 0:
                self.assertEqual(listener.results, [])
                continue

            if len(events) > len(listener.results):
                self.fail('Expected to see %s events, got %s' % (
                    len(events), len(listener.results)))

            for idx, expected_event in enumerate(events):
                self.match_evaluator.match_event(
                    expected_event, listener.results[idx])

    def verify_outcome(self, spec):
        for collection_data in spec:
            coll_name = collection_data['collectionName']
            db_name = collection_data['databaseName']
            expected_documents = collection_data['documents']

            coll = self.client.get_database(db_name).get_collection(
                coll_name,
                read_preference=ReadPreference.PRIMARY,
                read_concern=ReadConcern(level='local'))

            if expected_documents:
                sorted_expected_documents = sorted(
                    expected_documents, key=lambda doc: doc['_id'])
                actual_documents = list(
                    coll.find({}, sort=[('_id', ASCENDING)]))
                self.assertListEqual(sorted_expected_documents,
                                     actual_documents)

    def run_scenario(self, spec):
        # process test-level runOnRequirements
        run_on_spec = spec.get('runOnRequirements', [])
        if not self.should_run_on(run_on_spec):
            raise unittest.SkipTest('runOnRequirements not satisfied')

        # process skipReason
        skip_reason = spec.get('skipReason', None)
        if skip_reason is not None:
            raise unittest.SkipTest('%s' % (skip_reason,))

        # process operations
        self.run_operations(spec['operations'])

        # process expectEvents
        self.check_events(spec.get('expectEvents', []))

        # process outcome
        self.verify_outcome(spec.get('outcome', []))


class UnifiedSpecTestMeta(type):
    """Metaclass for generating test classes."""
    def __init__(cls, *args, **kwargs):
        super(UnifiedSpecTestMeta, cls).__init__(*args, **kwargs)

        def create_test(spec):
            def test_case(self):
                self.run_scenario(spec)
            return test_case

        for test_spec in cls.TEST_SPEC['tests']:
            description = test_spec['description']
            test_name = 'test_%s' % (
                description.replace(' ', '_').replace('.', '_'),)
            test_method = create_test(copy.deepcopy(test_spec))
            test_method.__name__ = test_name
            setattr(cls, test_name, test_method)


def generate_test_classes(test_path, module=__name__, class_name_prefix=''):
    """Method for generating test classes. Returns a dictionary where keys are
    the names of test classes and values are the test class objects."""
    test_klasses = {}

    def test_base_class_factory(test_spec):
        """Utility that creates the base class to use for test generation.
        This is needed to ensure that cls.TEST_SPEC is appropriately set when
        the metaclass __init__ is invoked."""
        class SpecTestBase(with_metaclass(UnifiedSpecTestMeta)):
            TEST_SPEC = test_spec
        return SpecTestBase

    for dirpath, _, filenames in os.walk(test_path):
        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                # Use tz_aware=False to match how CodecOptions decodes
                # dates.
                opts = json_util.JSONOptions(tz_aware=False)
                scenario_def = ScenarioDict(
                    json_util.loads(scenario_stream.read(),
                                    json_options=opts))

            test_type = os.path.splitext(filename)[0]
            snake_class_name = 'Test%s_%s_%s' % (
                class_name_prefix, dirname.replace('-', '_'),
                test_type.replace('-', '_').replace('.', '_'))
            class_name = snake_to_camel(snake_class_name)

            test_klasses[class_name] = type(
                class_name,
                (UnifiedSpecTestMixin, test_base_class_factory(scenario_def),),
                {'__module__': module})

    return test_klasses
