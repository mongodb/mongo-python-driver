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

import random
import os
import re
import sys
import string
import threading
import time
import uuid

from contextlib import contextmanager
from itertools import product

sys.path[0:0] = ['']

from bson import BSON, ObjectId, SON, json_util
from bson.binary import (ALL_UUID_REPRESENTATIONS,
                         Binary,
                         STANDARD,
                         PYTHON_LEGACY)
from bson.py3compat import iteritems
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument

from pymongo import monitoring
from pymongo.change_stream import _NON_RESUMABLE_GETMORE_ERRORS
from pymongo.command_cursor import CommandCursor
from pymongo.errors import (InvalidOperation, OperationFailure,
                            ServerSelectionTimeoutError)
from pymongo.message import _CursorAddress
from pymongo.read_concern import ReadConcern

from test import client_context, unittest, IntegrationTest
from test.utils import (
    EventListener, WhiteListEventListener, rs_or_single_client
)


class TestClusterChangeStream(IntegrationTest):

    @classmethod
    @client_context.require_version_min(4, 0, 0, -1)
    @client_context.require_no_standalone
    def setUpClass(cls):
        super(TestClusterChangeStream, cls).setUpClass()
        cls.dbs = [cls.db, cls.client.pymongo_test_2]

    @classmethod
    def tearDownClass(cls):
        for db in cls.dbs:
            cls.client.drop_database(db)
        super(TestClusterChangeStream, cls).tearDownClass()

    def change_stream(self, *args, **kwargs):
        return self.client.watch(*args, **kwargs)

    def generate_unique_collnames(self, numcolls):
        # Generate N collection names unique to a test.
        collnames = []
        for idx in range(1, numcolls + 1):
            collnames.append(self.id() + '_' + str(idx))
        return collnames

    def insert_and_check(self, change_stream, db, collname, doc):
        coll = db[collname]
        coll.insert_one(doc)
        change = next(change_stream)
        self.assertEqual(change['operationType'], 'insert')
        self.assertEqual(change['ns'], {'db': db.name,
                                        'coll': collname})
        self.assertEqual(change['fullDocument'], doc)

    def test_simple(self):
        collnames = self.generate_unique_collnames(3)
        with self.change_stream() as change_stream:
            for db, collname in product(self.dbs, collnames):
                self.insert_and_check(
                    change_stream, db, collname, {'_id': collname}
                )


class TestDatabaseChangeStream(IntegrationTest):

    @classmethod
    @client_context.require_version_min(4, 0, 0, -1)
    @client_context.require_no_standalone
    def setUpClass(cls):
        super(TestDatabaseChangeStream, cls).setUpClass()

    def change_stream(self, *args, **kwargs):
        return self.db.watch(*args, **kwargs)

    def generate_unique_collnames(self, numcolls):
        # Generate N collection names unique to a test.
        collnames = []
        for idx in range(1, numcolls + 1):
            collnames.append(self.id() + '_' + str(idx))
        return collnames

    def insert_and_check(self, change_stream, collname, doc):
        coll = self.db[collname]
        coll.insert_one(doc)
        change = next(change_stream)
        self.assertEqual(change['operationType'], 'insert')
        self.assertEqual(change['ns'], {'db': self.db.name,
                                        'coll': collname})
        self.assertEqual(change['fullDocument'], doc)

    def test_simple(self):
        collnames = self.generate_unique_collnames(3)
        with self.change_stream() as change_stream:
            for collname in collnames:
                self.insert_and_check(
                    change_stream, collname, {'_id': uuid.uuid4()}
                )

    def test_isolation(self):
        # Ensure inserts to other dbs don't show up in our ChangeStream.
        other_db = self.client.pymongo_test_temp
        self.assertNotEqual(
            other_db, self.db, msg="Isolation must be tested on separate DBs"
        )
        collname = self.id()
        with self.change_stream() as change_stream:
            other_db[collname].insert_one({'_id': uuid.uuid4()})
            self.insert_and_check(
                change_stream, collname, {'_id': uuid.uuid4()}
            )
        self.client.drop_database(other_db)


class TestCollectionChangeStream(IntegrationTest):

    @classmethod
    @client_context.require_version_min(3, 5, 11)
    @client_context.require_no_standalone
    def setUpClass(cls):
        super(TestCollectionChangeStream, cls).setUpClass()
        cls.coll = cls.db.change_stream_test
        # SERVER-31885 On a mongos the database must exist in order to create
        # a changeStream cursor. However, WiredTiger drops the database when
        # there are no more collections. Let's prevent that.
        cls.db.prevent_implicit_database_deletion.insert_one({})

    @classmethod
    def tearDownClass(cls):
        cls.db.prevent_implicit_database_deletion.drop()
        super(TestCollectionChangeStream, cls).tearDownClass()

    def setUp(self):
        # Use a new collection for each test.
        self.coll = self.db[self.id()]

    def tearDown(self):
        self.coll.drop()

    def insert_and_check(self, change_stream, doc):
        self.coll.insert_one(doc)
        change = next(change_stream)
        self.assertEqual(change['operationType'], 'insert')
        self.assertEqual(change['ns'], {'db': self.coll.database.name,
                                        'coll': self.coll.name})
        self.assertEqual(change['fullDocument'], doc)

    def test_watch(self):
        with self.coll.watch(
                [{'$project': {'foo': 0}}], full_document='updateLookup',
                max_await_time_ms=1000, batch_size=100) as change_stream:
            self.assertEqual([{'$project': {'foo': 0}}],
                             change_stream._pipeline)
            self.assertEqual('updateLookup', change_stream._full_document)
            self.assertIsNone(change_stream._resume_token)
            self.assertEqual(1000, change_stream._max_await_time_ms)
            self.assertEqual(100, change_stream._batch_size)
            self.assertIsInstance(change_stream._cursor, CommandCursor)
            self.assertEqual(
                1000, change_stream._cursor._CommandCursor__max_await_time_ms)
            self.coll.insert_one({})
            change = change_stream.next()
            resume_token = change['_id']
        with self.assertRaises(TypeError):
            self.coll.watch(pipeline={})
        with self.assertRaises(TypeError):
            self.coll.watch(full_document={})
        # No Error.
        with self.coll.watch(resume_after=resume_token):
            pass

    def test_full_pipeline(self):
        """$changeStream must be the first stage in a change stream pipeline
        sent to the server.
        """
        listener = WhiteListEventListener("aggregate")
        results = listener.results
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        coll = client[self.db.name][self.coll.name]
        with coll.watch([{'$project': {'foo': 0}}]) as _:
            pass

        self.assertEqual(1, len(results['started']))
        command = results['started'][0]
        self.assertEqual('aggregate', command.command_name)
        self.assertEqual([
            {'$changeStream': {'fullDocument': 'default'}},
            {'$project': {'foo': 0}}], 
            command.command['pipeline'])

    def test_iteration(self):
        with self.coll.watch(batch_size=2) as change_stream:
            num_inserted = 10
            self.coll.insert_many([{} for _ in range(num_inserted)])
            self.coll.drop()
            inserts_received = 0
            for change in change_stream:
                if change['operationType'] not in ('drop', 'invalidate'):
                    self.assertEqual(change['operationType'], 'insert')
                    inserts_received += 1
            self.assertEqual(num_inserted, inserts_received)
            # Last change should be invalidate.
            self.assertEqual(change['operationType'], 'invalidate')
            with self.assertRaises(StopIteration):
                change_stream.next()
            with self.assertRaises(StopIteration):
                next(change_stream)

    def _test_next_blocks(self, change_stream):
        inserted_doc = {'_id': ObjectId()}
        changes = []
        t = threading.Thread(
            target=lambda: changes.append(change_stream.next()))
        t.start()
        # Sleep for a bit to prove that the call to next() blocks.
        time.sleep(1)
        self.assertTrue(t.is_alive())
        self.assertFalse(changes)
        self.coll.insert_one(inserted_doc)
        # Join with large timeout to give the server time to return the change,
        # in particular for shard clusters.
        t.join(30)
        self.assertFalse(t.is_alive())
        self.assertEqual(1, len(changes))
        self.assertEqual(changes[0]['operationType'], 'insert')
        self.assertEqual(changes[0]['fullDocument'], inserted_doc)

    def test_next_blocks(self):
        """Test that next blocks until a change is readable"""
        # Use a short await time to speed up the test.
        with self.coll.watch(max_await_time_ms=250) as change_stream:
            self._test_next_blocks(change_stream)

    def test_aggregate_cursor_blocks(self):
        """Test that an aggregate cursor blocks until a change is readable."""
        with self.coll.aggregate([{'$changeStream': {}}],
                                 maxAwaitTimeMS=250) as change_stream:
            self._test_next_blocks(change_stream)

    def test_concurrent_close(self):
        """Ensure a ChangeStream can be closed from another thread."""
        # Use a short await time to speed up the test.
        with self.coll.watch(max_await_time_ms=250) as change_stream:
            def iterate_cursor():
                for _ in change_stream:
                    pass
            t = threading.Thread(target=iterate_cursor)
            t.start()
            self.coll.insert_one({})
            time.sleep(1)
            change_stream.close()
            t.join(3)
            self.assertFalse(t.is_alive())

    def test_update_resume_token(self):
        """ChangeStream must continuously track the last seen resumeToken."""
        with self.coll.watch() as change_stream:
            self.assertIsNone(change_stream._resume_token)
            for _ in range(3):
                self.coll.insert_one({})
                change = next(change_stream)
                self.assertEqual(change['_id'], change_stream._resume_token)

    def test_raises_error_on_missing_id(self):
        """ChangeStream will raise an exception if the server response is
        missing the resume token.
        """
        with self.coll.watch([{'$project': {'_id': 0}}]) as change_stream:
            self.coll.insert_one({})
            with self.assertRaises(InvalidOperation):
                next(change_stream)
            # The cursor should now be closed.
            with self.assertRaises(StopIteration):
                next(change_stream)

    def test_resume_on_error(self):
        """ChangeStream will automatically resume one time on a resumable
        error (including not master) with the initial pipeline and options,
        except for the addition/update of a resumeToken.
        """
        with self.coll.watch([]) as change_stream:
            self.insert_and_check(change_stream, {'_id': 1})
            # Cause a cursor not found error on the next getMore.
            cursor = change_stream._cursor
            address = _CursorAddress(cursor.address, self.coll.full_name)
            self.client._close_cursor_now(cursor.cursor_id, address)
            self.insert_and_check(change_stream, {'_id': 2})

    def test_does_not_resume_fatal_errors(self):
        """ChangeStream will not attempt to resume fatal server errors."""
        for code in _NON_RESUMABLE_GETMORE_ERRORS:
            with self.coll.watch() as change_stream:
                self.coll.insert_one({})

                def mock_next(*args, **kwargs):
                    change_stream._cursor.close()
                    raise OperationFailure('Mock server error', code=code)

                original_next = change_stream._cursor.next
                change_stream._cursor.next = mock_next

                with self.assertRaises(OperationFailure):
                    next(change_stream)
                change_stream._cursor.next = original_next
                with self.assertRaises(StopIteration):
                    next(change_stream)

    def test_initial_empty_batch(self):
        """Ensure that a cursor returned from an aggregate command with a
        cursor id, and an initial empty batch, is not closed on the driver
        side.
        """
        with self.coll.watch() as change_stream:
            # The first batch should be empty.
            self.assertEqual(
                0, len(change_stream._cursor._CommandCursor__data))
            cursor_id = change_stream._cursor.cursor_id
            self.assertTrue(cursor_id)
            self.insert_and_check(change_stream, {})
            # Make sure we're still using the same cursor.
            self.assertEqual(cursor_id, change_stream._cursor.cursor_id)

    def test_kill_cursors(self):
        """The killCursors command sent during the resume process must not be
        allowed to raise an exception.
        """
        def raise_error():
            raise ServerSelectionTimeoutError('mock error')
        with self.coll.watch([]) as change_stream:
            self.insert_and_check(change_stream, {'_id': 1})
            # Cause a cursor not found error on the next getMore.
            cursor = change_stream._cursor
            address = _CursorAddress(cursor.address, self.coll.full_name)
            self.client._close_cursor_now(cursor.cursor_id, address)
            cursor.close = raise_error
            self.insert_and_check(change_stream, {'_id': 2})

    def test_unknown_full_document(self):
        """Must rely on the server to raise an error on unknown fullDocument.
        """
        try:
            with self.coll.watch(full_document='notValidatedByPyMongo'):
                pass
        except OperationFailure:
            pass

    def test_change_operations(self):
        """Test each operation type."""
        expected_ns = {'db': self.coll.database.name, 'coll': self.coll.name}
        with self.coll.watch() as change_stream:
            # Insert.
            inserted_doc = {'_id': ObjectId(), 'foo': 'bar'}
            self.coll.insert_one(inserted_doc)
            change = change_stream.next()
            self.assertTrue(change['_id'])
            self.assertEqual(change['operationType'], 'insert')
            self.assertEqual(change['ns'], expected_ns)
            self.assertEqual(change['fullDocument'], inserted_doc)
            # Update.
            update_spec = {'$set': {'new': 1}, '$unset': {'foo': 1}}
            self.coll.update_one(inserted_doc, update_spec)
            change = change_stream.next()
            self.assertTrue(change['_id'])
            self.assertEqual(change['operationType'], 'update')
            self.assertEqual(change['ns'], expected_ns)
            self.assertNotIn('fullDocument', change)
            self.assertEqual({'updatedFields': {'new': 1},
                              'removedFields': ['foo']},
                             change['updateDescription'])
            # Replace.
            self.coll.replace_one({'new': 1}, {'foo': 'bar'})
            change = change_stream.next()
            self.assertTrue(change['_id'])
            self.assertEqual(change['operationType'], 'replace')
            self.assertEqual(change['ns'], expected_ns)
            self.assertEqual(change['fullDocument'], inserted_doc)
            # Delete.
            self.coll.delete_one({'foo': 'bar'})
            change = change_stream.next()
            self.assertTrue(change['_id'])
            self.assertEqual(change['operationType'], 'delete')
            self.assertEqual(change['ns'], expected_ns)
            self.assertNotIn('fullDocument', change)
            # Invalidate.
            self.coll.drop()
            change = change_stream.next()
            # 4.1 returns a "drop" change document.
            if change['operationType'] == 'drop':
                self.assertTrue(change['_id'])
                self.assertEqual(change['ns'], expected_ns)
                # Last change should be invalidate.
                change = change_stream.next()
            self.assertTrue(change['_id'])
            self.assertEqual(change['operationType'], 'invalidate')
            self.assertNotIn('ns', change)
            self.assertNotIn('fullDocument', change)
            # The ChangeStream should be dead.
            with self.assertRaises(StopIteration):
                change_stream.next()

    def test_raw(self):
        """Test with RawBSONDocument."""
        raw_coll = self.coll.with_options(
            codec_options=DEFAULT_RAW_BSON_OPTIONS)
        with raw_coll.watch() as change_stream:
            raw_doc = RawBSONDocument(BSON.encode({'_id': 1}))
            self.coll.insert_one(raw_doc)
            change = next(change_stream)
            self.assertIsInstance(change, RawBSONDocument)
            self.assertEqual(change['operationType'], 'insert')
            self.assertEqual(change['ns']['db'], self.coll.database.name)
            self.assertEqual(change['ns']['coll'], self.coll.name)
            self.assertEqual(change['fullDocument'], raw_doc)
            self.assertEqual(change['_id'], change_stream._resume_token)

    def test_uuid_representations(self):
        """Test with uuid document _ids and different uuid_representation."""
        for uuid_representation in ALL_UUID_REPRESENTATIONS:
            for id_subtype in (STANDARD, PYTHON_LEGACY):
                resume_token = None
                options = self.coll.codec_options.with_options(
                    uuid_representation=uuid_representation)
                coll = self.coll.with_options(codec_options=options)
                with coll.watch() as change_stream:
                    coll.insert_one(
                        {'_id': Binary(uuid.uuid4().bytes, id_subtype)})
                    resume_token = change_stream.next()['_id']

                # Should not error.
                coll.watch(resume_after=resume_token)

    def test_document_id_order(self):
        """Test with document _ids that need their order preserved."""
        random_keys = random.sample(string.ascii_letters,
                                    len(string.ascii_letters))
        random_doc = {'_id': SON([(key, key) for key in random_keys])}
        for document_class in (dict, SON, RawBSONDocument):
            options = self.coll.codec_options.with_options(
                document_class=document_class)
            coll = self.coll.with_options(codec_options=options)
            with coll.watch() as change_stream:
                coll.insert_one(random_doc)
                resume_token = change_stream.next()['_id']

            # The resume token is always a document.
            self.assertIsInstance(resume_token, document_class)
            # Should not error.
            coll.watch(resume_after=resume_token)
            coll.delete_many({})

    def test_read_concern(self):
        """Test readConcern is not validated by the driver."""
        # Read concern 'local' is not allowed for $changeStream.
        coll = self.coll.with_options(read_concern=ReadConcern('local'))
        with self.assertRaises(OperationFailure):
            coll.watch()

        # Does not error.
        coll = self.coll.with_options(read_concern=ReadConcern('majority'))
        with coll.watch():
            pass


class TestAllScenarios(unittest.TestCase):

    @classmethod
    @client_context.require_connection
    def setUpClass(cls):
        cls.listener = WhiteListEventListener("aggregate")
        cls.client = rs_or_single_client(event_listeners=[cls.listener])

    @classmethod
    def tearDownClass(cls):
        cls.client

    def setUp(self):
        self.listener.results.clear()

    def setUpCluster(self, scenario_dict):
        assets = [
            (scenario_dict["database_name"], scenario_dict["collection_name"]),
            (scenario_dict["database2_name"], scenario_dict["collection2_name"]),
        ]
        for db, coll in assets:
            self.client.drop_database(db)
            self.client[db].create_collection(coll)

    def tearDown(self):
        self.listener.results.clear()


_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'change_streams'
)


def camel_to_snake(camel):
    # Regex to convert CamelCase to snake_case.
    snake = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake).lower()


def get_change_stream(client, scenario_def, test):
    # Get target namespace on which to instantiate change stream
    target = test["target"]
    if target == "collection":
        db = client.get_database(scenario_def["database_name"])
        cs_target = db.get_collection(scenario_def["collection_name"])
    elif target == "database":
        cs_target = client.get_database(scenario_def["database_name"])
    elif target == "client":
        cs_target = client
    else:
        raise ValueError("Invalid target in spec")

    # Construct change stream kwargs dict
    cs_pipeline = test["changeStreamPipeline"]
    options = test["changeStreamOptions"]
    cs_options = {}
    for key, value in iteritems(options):
        cs_options[camel_to_snake(key)] = value
    
    # Create and return change stream
    return cs_target.watch(pipeline=cs_pipeline, **cs_options)


def run_operation(client, operation):
    # Apply specified operations
    opname = camel_to_snake(operation["name"])
    arguments = operation["arguments"]
    cmd = getattr(client.get_database(
        operation["database"]).get_collection(
        operation["collection"]), opname
    )
    return cmd(**arguments)


def assert_dict_is_subset(superdict, subdict):
    """Check that subdict is a subset of superdict."""
    exempt_fields = ["documentKey", "_id"]
    for key, value in iteritems(subdict):
        if key not in superdict:
            assert False
        if isinstance(value, dict):
            assert_dict_is_subset(superdict[key], value)
            continue
        if key in exempt_fields:
            superdict[key] = "42"
        assert superdict[key] == value


def check_event(event, expectation_dict):
    if event is None:
        raise AssertionError
    for key, value in iteritems(expectation_dict):
        if isinstance(value, dict):
            assert_dict_is_subset(
                getattr(event, key), value
            )
        else:
            assert getattr(event, key) == value


def create_test(scenario_def, test):
    def run_scenario(self):
        # Set up
        self.setUpCluster(scenario_def)
        try:
            with get_change_stream(
                self.client, scenario_def, test
            ) as change_stream:
                for operation in test["operations"]:
                    # Run specified operations
                    run_operation(self.client, operation)
                num_expected_changes = len(test["result"]["success"])
                changes = [
                    change_stream.next() for _ in range(num_expected_changes)
                ]

        except OperationFailure as exc:
            if test["result"].get("error") is None:
                raise
            expected_code = test["result"]["error"]["code"]
            self.assertEqual(exc.code, expected_code)

        else:
            # Check for expected output from change streams
            for change, expected_changes in zip(changes, test["result"]["success"]):
                assert_dict_is_subset(change, expected_changes)
            self.assertEqual(len(changes), len(test["result"]["success"]))
        
        finally:
            # Check for expected events
            results = self.listener.results
            for expectation in test["expectations"]:
                for idx, (event_type, event_desc) in enumerate(iteritems(expectation)):
                    results_key = event_type.split("_")[1]
                    event = results[results_key][idx] if len(results[results_key]) > idx else None
                    check_event(event, event_desc)

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json_util.loads(scenario_stream.read())

            test_type = os.path.splitext(filename)[0]

            for test in scenario_def['tests']:
                new_test = create_test(scenario_def, test)
                if 'minServerVersion' in test:
                    min_ver = tuple(
                        int(elt) for
                        elt in test['minServerVersion'].split('.'))
                    new_test = client_context.require_version_min(*min_ver)(
                        new_test)
                if 'maxServerVersion' in test:
                    max_ver = tuple(
                        int(elt) for
                        elt in test['maxServerVersion'].split('.'))
                    new_test = client_context.require_version_max(*max_ver)(
                        new_test)

                topologies = test['topology']
                new_test = client_context.require_cluster_type(topologies)(
                    new_test)

                test_name = 'test_%s_%s_%s' % (
                    dirname,
                    test_type.replace("-", "_"),
                    str(test['description'].replace(" ", "_")))

                new_test.__name__ = test_name
                setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()


if __name__ == '__main__':
    unittest.main()
