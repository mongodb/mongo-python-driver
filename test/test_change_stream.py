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
import sys
import string
import threading
import time
import uuid

sys.path[0:0] = ['']

from bson import BSON, ObjectId, SON
from bson.binary import (ALL_UUID_REPRESENTATIONS,
                         Binary,
                         STANDARD,
                         PYTHON_LEGACY)
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument

from pymongo.command_cursor import CommandCursor
from pymongo.errors import (InvalidOperation, OperationFailure,
                            ServerSelectionTimeoutError)
from pymongo.message import _CursorAddress
from pymongo.read_concern import ReadConcern

from test import client_context, unittest, IntegrationTest
from test.utils import WhiteListEventListener, rs_or_single_client


class TestChangeStream(IntegrationTest):

    @classmethod
    @client_context.require_version_min(3, 5, 11)
    @client_context.require_replica_set
    def setUpClass(cls):
        super(TestChangeStream, cls).setUpClass()
        # $changeStream requires read concern majority.
        cls.db = cls.client.get_database(
            cls.db.name, read_concern=ReadConcern('majority'))
        cls.coll = cls.db.change_stream_test

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

        with coll.watch([{'$project': {'foo': 0}}]) as change_stream:
            self.assertEqual([{'$changeStream': {'fullDocument': 'default'}},
                              {'$project': {'foo': 0}}],
                             change_stream._full_pipeline())

        self.assertEqual(1, len(results['started']))
        command = results['started'][0]
        self.assertEqual('aggregate', command.command_name)
        self.assertEqual([{'$changeStream':  {'fullDocument': 'default'}},
                          {'$project': {'foo': 0}}],
                         command.command['pipeline'])

    def test_iteration(self):
        with self.coll.watch(batch_size=2) as change_stream:
            num_inserted = 10
            self.coll.insert_many([{} for _ in range(num_inserted)])
            self.coll.drop()
            received = 0
            for change in change_stream:
                received += 1
                if change['operationType'] != 'invalidate':
                    self.assertEqual(change['operationType'], 'insert')
            self.assertEqual(num_inserted + 1, received)
            with self.assertRaises(StopIteration):
                change_stream.next()
            with self.assertRaises(StopIteration):
                next(change_stream)

    def test_next_blocks(self):
        """Test that next blocks until a change is readable"""
        inserted_doc = {'_id': ObjectId()}
        # Use a short await time to speed up the test.
        with self.coll.watch(max_await_time_ms=250) as change_stream:
            changes = []
            t = threading.Thread(
                target=lambda: changes.append(change_stream.next()))
            t.start()
            time.sleep(1)
            self.coll.insert_one(inserted_doc)
            t.join(3)
            self.assertFalse(t.is_alive())
            self.assertEqual(1, len(changes))
            self.assertEqual(changes[0]['operationType'], 'insert')
            self.assertEqual(changes[0]['fullDocument'], inserted_doc)

    def test_aggregate_cursor_blocks(self):
        """Test that an aggregate cursor blocks until a change is readable."""
        inserted_doc = {'_id': ObjectId()}
        with self.coll.aggregate([{'$changeStream': {}}],
                                 maxAwaitTimeMS=250) as change_stream:
            changes = []
            t = threading.Thread(
                target=lambda: changes.append(change_stream.next()))
            t.start()
            time.sleep(1)
            self.coll.insert_one(inserted_doc)
            t.join(3)
            self.assertFalse(t.is_alive())
            self.assertEqual(1, len(changes))
            self.assertEqual(changes[0]['operationType'], 'insert')
            self.assertEqual(changes[0]['fullDocument'], inserted_doc)

    def test_update_resume_token(self):
        """ChangeStream must continuously track the last seen resumeToken."""
        with self.coll.watch() as change_stream:
            self.assertIsNone(change_stream._resume_token)
            for i in range(10):
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

    def test_does_not_resume_on_server_error(self):
        """ChangeStream will not attempt to resume on a server error."""
        def mock_next(self, *args, **kwargs):
            self._CommandCursor__killed = True
            raise OperationFailure('Mock server error')

        original_next = CommandCursor.next
        CommandCursor.next = mock_next
        try:
            with self.coll.watch() as change_stream:
                with self.assertRaises(OperationFailure):
                    next(change_stream)
                CommandCursor.next = original_next
                with self.assertRaises(StopIteration):
                    next(change_stream)
        finally:
            CommandCursor.next = original_next

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


if __name__ == '__main__':
    unittest.main()
