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
import os
import sys
import types

from bson import json_util

from pymongo.write_concern import WriteConcern

from test import client_context, unittest, IntegrationTest
from test.utils import (
    camel_to_snake, camel_to_snake_args, rs_or_single_client,
    snake_to_camel, ScenarioDict)

from test.version import Version


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

    @staticmethod
    def insert_initial_data(client, collection_data):
        coll_name = collection_data['collectionName']
        db_name = collection_data['databaseName']
        documents = collection_data['documents']

        coll = client.get_database(db_name).get_collection(
            coll_name, write_concern=WriteConcern(w="majority"))
        coll.drop()

        # documents MAY be an empty list
        if documents:
            coll.insert_many(documents)

    @classmethod
    def setUpClass(cls):
        # The super call takes care of creating the internal client.
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

    def create_entity(self, entity_spec):
        if len(entity_spec) != 1:
            raise ValueError("Entity spec MUST contain exactly one top-level key.")

        for entity_type, spec in entity_spec.items():
            pass

        if entity_type == 'client':
            # TODO
            # Add logic to respect the following fields
            # - uriOptions
            # - useMultipleMongoses
            # - observeEvents
            # - ignoreCommandMonitoringEvents
            client = rs_or_single_client()
            self.entity_map[spec['id']] = client
            self.addCleanup(client.close)
            return
        elif entity_type == 'database':
            # TODO
            # Add logic to respect the following fields
            # - databaseOptions
            client = self.entity_map[spec['client']]
            self.entity_map[spec['id']] = client.get_database(spec['databaseName'])
            return
        elif entity_type == 'collection':
            # TODO
            # Add logic to respect the following fields
            # - collectionOptions
            database = self.entity_map[spec['database']]
            self.entity_map[spec['id']] = database.get_collection(spec['collectionName'])
            return
        # elif ...
            # TODO
            # Implement the following entity types:
            # - session
            # - bucket
        else:
            raise ValueError("Unknown entity type %s" % (entity_type,))

    def setUp(self):
        super(UnifiedSpecTestMixin, self).setUp()

        # process createEntities
        self.entity_map = {}
        entity_spec = self.TEST_SPEC.get('createEntities', [])
        for spec in entity_spec:
            self.create_entity(spec)

        # process initialData
        initial_data = self.TEST_SPEC.get('initialData', [])
        for data in initial_data:
            self.insert_initial_data(self.client, data)

        # PyMongo internals
        #self.test_assets = {}

    def run_entity_operation(self, entity_name, spec):
        target = self.entity_map[entity_name]
        opname = camel_to_snake(spec['name'])
        opargs = spec.get('arguments')
        expect_error = spec.get('expectError')
        if expect_error:
            # TODO: process expectedError object
            # See L420-446 of utils_spec_runner.py
            pass
        else:
            # Operation expected to succeed
            arguments = {}
            if opargs:
                if 'session' in arguments:
                    # TODO: resolve session to entity
                    pass
                if 'readConcern' in arguments:
                    from pymongo.read_concern import ReadConcern
                    arguments['read_concern'] = ReadConcern(
                        **opargs.pop('readConcern'))
                if 'readPreference' in arguments:
                    from pymongo.read_preferences import ReadPreference
                    pass

    def run_special_operation(self, spec):
        pass

    def run_operations(self, spec):
        for op in spec:
            target = op['object']
            if target != 'testRunner':
                self.run_entity_operation(target, op)
            else:
                self.run_special_operation(op)

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

        # process expectedEvents
        # TODO: process expectedEvents

        # process outcome
        # TODO: process outcome


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
