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

"""Test the client_session module."""

import copy
import sys

from bson import DBRef
from bson.py3compat import StringIO
from gridfs import GridFS, GridFSBucket
from pymongo import InsertOne, IndexModel, OFF, monitoring
from pymongo.errors import (ConfigurationError,
                            InvalidOperation,
                            OperationFailure)
from test import IntegrationTest, client_context, db_user, db_pwd
from test.utils import ignore_deprecations, rs_or_single_client, EventListener


# Ignore auth commands like saslStart, so we can assert lsid is in all commands.
class SessionTestListener(EventListener):
    def __init__(self):
        super(SessionTestListener, self).__init__(ignore_lsid=False)

    def started(self, event):
        if not event.command_name.startswith('sasl'):
            super(SessionTestListener, self).started(event)

    def succeeded(self, event):
        if not event.command_name.startswith('sasl'):
            super(SessionTestListener, self).succeeded(event)

    def failed(self, event):
        if not event.command_name.startswith('sasl'):
            super(SessionTestListener, self).failed(event)

    def first_command_started(self):
        assert len(self.results['started']) >= 1, (
            "No command-started events")

        return self.results['started'][0]


def session_ids(client):
    return [s.session_id for s in client._topology._session_pool]


class TestSession(IntegrationTest):
    @client_context.require_sessions
    def setUp(self):
        super(TestSession, self).setUp()

        # Redact no commands, so we can test user-admin commands have "lsid".
        self.sensitive_commands = monitoring._SENSITIVE_COMMANDS.copy()
        monitoring._SENSITIVE_COMMANDS.clear()

    def tearDown(self):
        monitoring._SENSITIVE_COMMANDS.update(self.sensitive_commands)
        super(TestSession, self).tearDown()

    def _test_ops(self, client, *ops, **kwargs):
        listener = client.event_listeners()[0][0]

        for f, args, kw in ops:
            with client.start_session() as s:
                listener.results.clear()
                # In case "f" modifies its inputs.
                args = copy.copy(args)
                kw = copy.copy(kw)
                kw['session'] = s
                f(*args, **kw)
                self.assertGreaterEqual(len(listener.results['started']), 1)
                for event in listener.results['started']:
                    self.assertTrue(
                        'lsid' in event.command,
                        "%s sent no lsid with %s" % (
                            f.__name__, event.command_name))

                    self.assertEqual(
                        s.session_id,
                        event.command['lsid'],
                        "%s sent wrong lsid with %s" % (
                            f.__name__, event.command_name))

                self.assertFalse(s.has_ended)

            self.assertTrue(s.has_ended)
            with self.assertRaisesRegex(InvalidOperation, "ended session"):
                f(*args, **kw)

        if kwargs.get('explicit_only'):
            return

        # No explicit session.
        for f, args, kw in ops:
            listener.results.clear()
            f(*args, **kw)
            self.assertGreaterEqual(len(listener.results['started']), 1)
            lsids = []
            for event in listener.results['started']:
                self.assertTrue(
                    'lsid' in event.command,
                    "%s sent no lsid with %s" % (
                        f.__name__, event.command_name))

                lsids.append(event.command['lsid'])

            if not (sys.platform.startswith('java') or 'PyPy' in sys.version):
                # Server session was returned to pool. Ignore interpreters with
                # non-deterministic GC.
                for lsid in lsids:
                    self.assertIn(
                        lsid, session_ids(client),
                        "%s did not return implicit session to pool" % (
                            f.__name__,))

    def test_pool_lifo(self):
        # "Pool is LIFO" test from Driver Sessions Spec.
        a = self.client.start_session()
        b = self.client.start_session()
        a_id = a.session_id
        b_id = b.session_id
        a.end_session()
        b.end_session()

        s = self.client.start_session()
        self.assertEqual(b_id, s.session_id)
        self.assertNotEqual(a_id, s.session_id)

        s = self.client.start_session()
        self.assertEqual(a_id, s.session_id)
        self.assertNotEqual(b_id, s.session_id)

    def test_end_session(self):
        # We test elsewhere that using an ended session throws InvalidOperation.
        client = self.client
        s = client.start_session()
        self.assertFalse(s.has_ended)
        self.assertIsNotNone(s.session_id)

        s.end_session()
        self.assertTrue(s.has_ended)

        with self.assertRaisesRegex(InvalidOperation, "ended session"):
            s.session_id

    def test_client(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])

        # Make sure if the test fails we unlock the server.
        def unlock():
            try:
                client.unlock()
            except OperationFailure:
                pass

        self.addCleanup(unlock)

        ops = [
            (client.server_info, [], {}),
            (client.database_names, [], {}),
            (client.drop_database, ['pymongo_test'], {}),
        ]

        if not client_context.is_mongos:
            ops.extend([
                (client.fsync, [], {'lock': True}),
                (client.unlock, [], {}),
            ])

        self._test_ops(client, *ops)

    def test_database(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        client.drop_database('pymongo_test')
        self.addCleanup(client.drop_database, 'pymongo_test')

        db = client.pymongo_test
        ops = [
            (db.command, ['ping'], {}),
            (db.create_collection, ['collection'], {}),
            (db.collection_names, [], {}),
            (db.validate_collection, ['collection'], {}),
            (db.drop_collection, ['collection'], {}),
            (db.current_op, [], {}),
            (db.profiling_info, [], {}),
            (db.dereference, [DBRef('collection', 1)], {}),
        ]

        if not client_context.is_mongos:
            ops.append((db.set_profiling_level, [OFF], {}))
            ops.append((db.profiling_level, [], {}))

        self._test_ops(client, *ops)

    @client_context.require_auth
    def test_user_admin(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        client.drop_database('pymongo_test')
        self.addCleanup(client.drop_database, 'pymongo_test')
        db = client.pymongo_test

        # createUser sends lsid only with explicit session, SERVER-31116.
        self._test_ops(
            client,
            (db.add_user, ['session-test', 'pass'], {'roles': ['read']}),
            # Do it again to test updateUser command.
            (db.add_user, ['session-test', 'pass'], {'roles': ['read']}),
            (db.remove_user, ['session-test'], {}),
            explicit_only=True)

    def test_collection(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        client.drop_database('pymongo_test')
        self.addCleanup(client.drop_database, 'pymongo_test')

        coll = client.pymongo_test.collection
        self.addCleanup(self.client.pymongo_test.collection.drop)

        # Test some collection methods - the rest are in test_cursor.
        self._test_ops(
            client,
            (coll.drop, [], {}),
            (coll.bulk_write, [[InsertOne({})]], {}),
            (coll.insert_one, [{}], {}),
            (coll.insert_many, [[{}, {}]], {}),
            (coll.replace_one, [{}, {}], {}),
            (coll.update_one, [{}, {'$set': {'a': 1}}], {}),
            (coll.update_many, [{}, {'$set': {'a': 1}}], {}),
            (coll.delete_one, [{}], {}),
            (coll.delete_many, [{}], {}),
            (coll.map_reduce, ['function() {}', 'function() {}', 'output'], {}),
            (coll.inline_map_reduce, ['function() {}', 'function() {}'], {}),
            (coll.find_one_and_replace, [{}, {}], {}),
            (coll.find_one_and_update, [{}, {'$set': {'a': 1}}], {}),
            (coll.find_one_and_delete, [{}, {}], {}),
            (coll.rename, ['collection2'], {}),
            # Drop collection2 between tests of "rename", above.
            (client.pymongo_test.drop_collection, ['collection2'], {}),
            (coll.distinct, ['a'], {}),
            (coll.find_one, [], {}),
            (coll.count, [], {}),
            (coll.create_indexes, [[IndexModel('a')]], {}),
            (coll.create_index, ['a'], {}),
            (coll.drop_index, ['a_1'], {}),
            (coll.drop_indexes, [], {}),
            (coll.reindex, [], {}),
            (coll.list_indexes, [], {}),
            (coll.index_information, [], {}),
            (coll.options, [], {}),
            (coll.aggregate, [[]], {}))

    @client_context.require_no_mongos
    def test_parallel_collection_scan(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        coll = client.pymongo_test.collection
        coll.insert_many([{'_id': i} for i in range(1000)])
        self.addCleanup(self.client.pymongo_test.collection.drop)

        listener.results.clear()

        def scan(session=None):
            cursors = coll.parallel_scan(4, session=session)
            for c in cursors:
                c.batch_size(2)
                list(c)

        self._test_ops(client,
                       (scan, [], {}),
                       explicit_only=True)

        # Implicit session with parallel_scan is uncorrelated with cursors', but
        # each cursor's getMores all use the same lsid.
        listener.results.clear()
        scan()
        cursor_lsids = {}
        for event in listener.results['started']:
            self.assertIn(
                'lsid', event.command,
                "parallel_scan sent no lsid with %s" % (event.command_name, ))

            if event.command_name == 'getMore':
                cursor_id = event.command['getMore']
                if cursor_id in cursor_lsids:
                    self.assertEqual(cursor_lsids[cursor_id],
                                     event.command['lsid'])
                else:
                    cursor_lsids[cursor_id] = event.command['lsid']

    def test_cursor_clone(self):
        coll = self.client.pymongo_test.collection
        # Ensure some batches.
        coll.insert_many({} for _ in range(10))
        self.addCleanup(coll.drop)

        with self.client.start_session() as s:
            cursor = coll.find(session=s)
            self.assertTrue(cursor.session is s)
            clone = cursor.clone()
            self.assertTrue(clone.session is s)

        # No explicit session.
        cursor = coll.find(batch_size=2)
        next(cursor)
        # Session is "owned" by cursor.
        self.assertIsNone(cursor.session)
        self.assertIsNotNone(cursor._Cursor__session)
        clone = cursor.clone()
        next(clone)
        self.assertIsNone(clone.session)
        self.assertIsNotNone(clone._Cursor__session)
        self.assertFalse(cursor._Cursor__session is clone._Cursor__session)

    def test_cursor(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        client.drop_database('pymongo_test')
        self.addCleanup(client.drop_database, 'pymongo_test')

        coll = client.pymongo_test.collection
        coll.insert_many([{} for _ in range(1000)])
        self.addCleanup(self.client.pymongo_test.collection.drop)

        # Test all cursor methods.
        ops = [
            ('find', lambda session: list(coll.find(session=session))),
            ('find_raw_batches',
             lambda session: list(coll.find_raw_batches(session=session))),
            ('getitem', lambda session: coll.find(session=session)[0]),
            ('count', lambda session: coll.find(session=session).count()),
            ('distinct',
             lambda session: coll.find(session=session).distinct('a')),
            ('explain', lambda session: coll.find(session=session).explain()),
        ]

        for name, f in ops:
            with client.start_session() as s:
                listener.results.clear()
                f(session=s)
                self.assertGreaterEqual(len(listener.results['started']), 1)
                for event in listener.results['started']:
                    self.assertTrue(
                        'lsid' in event.command,
                        "%s sent no lsid with %s" % (
                            name, event.command_name))

                    self.assertEqual(
                        s.session_id,
                        event.command['lsid'],
                        "%s sent wrong lsid with %s" % (
                            name, event.command_name))

            with self.assertRaisesRegex(InvalidOperation, "ended session"):
                f(session=s)

        # No explicit session.
        for name, f in ops:
            listener.results.clear()
            f(session=None)
            event0 = listener.first_command_started()
            self.assertTrue(
                'lsid' in event0.command,
                "%s sent no lsid with %s" % (
                    name, event0.command_name))

            lsid = event0.command['lsid']

            for event in listener.results['started'][1:]:
                self.assertTrue(
                    'lsid' in event.command,
                    "%s sent no lsid with %s" % (
                        name, event.command_name))

                self.assertEqual(
                    lsid,
                    event.command['lsid'],
                    "%s sent wrong lsid with %s" % (
                        name, event.command_name))

    def test_gridfs(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        client.drop_database('pymongo_test')
        self.addCleanup(client.drop_database, 'pymongo_test')

        fs = GridFS(client.pymongo_test)

        def new_file(session=None):
            grid_file = fs.new_file(_id=1, filename='f', session=session)
            # 1 MB, 5 chunks, to test that each chunk is fetched with same lsid.
            grid_file.write(b'a' * 1048576)
            grid_file.close()

        def find(session=None):
            files = list(fs.find({'_id': 1}, session=session))
            for f in files:
                f.read()

        self._test_ops(
            client,
            (new_file, [], {}),
            (fs.put, [b'data'], {}),
            (lambda session=None: fs.get(1, session=session).read(), [], {}),
            (lambda session=None: fs.get_version('f', session=session).read(),
             [], {}),
            (lambda session=None:
             fs.get_last_version('f', session=session).read(), [], {}),
            (fs.list, [], {}),
            (fs.find_one, [1], {}),
            (lambda session=None: list(fs.find(session=session)), [], {}),
            (fs.exists, [1], {}),
            (find, [], {}),
            (fs.delete, [1], {}))

    def test_gridfs_bucket(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        client.drop_database('pymongo_test')
        self.addCleanup(client.drop_database, 'pymongo_test')

        bucket = GridFSBucket(client.pymongo_test)

        def upload(session=None):
            stream = bucket.open_upload_stream('f', session=session)
            stream.write(b'a' * 1048576)
            stream.close()

        def upload_with_id(session=None):
            stream = bucket.open_upload_stream_with_id(1, 'f1', session=session)
            stream.write(b'a' * 1048576)
            stream.close()

        def open_download_stream(session=None):
            stream = bucket.open_download_stream(1, session=session)
            stream.read()

        def open_download_stream_by_name(session=None):
            stream = bucket.open_download_stream_by_name('f', session=session)
            stream.read()

        def find(session=None):
            files = list(bucket.find({'_id': 1}, session=session))
            for f in files:
                f.read()

        sio = StringIO()

        self._test_ops(
            client,
            (upload, [], {}),
            (upload_with_id, [], {}),
            (bucket.upload_from_stream, ['f', b'data'], {}),
            (bucket.upload_from_stream_with_id, [2, 'f', b'data'], {}),
            (open_download_stream, [], {}),
            (open_download_stream_by_name, [], {}),
            (bucket.download_to_stream, [1, sio], {}),
            (bucket.download_to_stream_by_name, ['f', sio], {}),
            (find, [], {}),
            (bucket.rename, [1, 'f2'], {}),
            # Delete both files so _test_ops can run these operations twice.
            (bucket.delete, [1], {}),
            (bucket.delete, [2], {}))

    def test_gridfsbucket_cursor(self):
        client = self.client
        client.drop_database('pymongo_test')
        self.addCleanup(client.drop_database, 'pymongo_test')

        bucket = GridFSBucket(client.pymongo_test)
        for file_id in 1, 2:
            stream = bucket.open_upload_stream_with_id(file_id, str(file_id))
            stream.write(b'a' * 1048576)
            stream.close()

        with client.start_session() as s:
            cursor = bucket.find(session=s)
            for f in cursor:
                f.read()

            self.assertFalse(s.has_ended)

        self.assertTrue(s.has_ended)

        # No explicit session.
        cursor = bucket.find()
        files = list(cursor)

        s = cursor._Cursor__session
        self.assertFalse(s.has_ended)
        cursor.__del__()
        self.assertTrue(s.has_ended)

        # Files are still valid, they use their own sessions.
        for f in files:
            f.read()

        # Explicit session.
        with client.start_session() as s:
            cursor = bucket.find(session=s)
            s = cursor.session
            files = list(cursor)
            cursor.__del__()
            self.assertFalse(s.has_ended)

            for f in files:
                f.read()

        with self.assertRaisesRegex(InvalidOperation, "ended session"):
            files[0].read()

    def test_aggregate(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        coll = client.pymongo_test.collection
        coll.drop()

        def agg(session=None):
            list(coll.aggregate(
                [],
                batchSize=2,
                session=session))

        # With empty collection.
        self._test_ops(client, (agg, [], {}))

        # Now with documents.
        coll.insert_many([{} for _ in range(10)])
        self.addCleanup(coll.drop)
        self._test_ops(client, (agg, [], {}))

    def test_killcursors(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        coll = client.pymongo_test.collection
        coll.insert_many([{} for _ in range(10)])
        self.addCleanup(self.client.pymongo_test.collection.drop)

        def explicit_close(session=None):
            cursor = coll.find(batch_size=2, session=session)
            next(cursor)
            cursor.close()

        self._test_ops(client, (explicit_close, [], {}))

    def test_aggregate_error(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        coll = client.pymongo_test.collection
        with self.assertRaises(OperationFailure):
            coll.aggregate([{'$badOperation': {'bar': 1}}])

        event = listener.first_command_started()
        self.assertEqual(event.command_name, 'aggregate')
        lsid = event.command['lsid']
        # Session was returned to pool despite error.
        self.assertIn(lsid, session_ids(client))

    def _test_cursor_helper(self, create_cursor, close_cursor):
        coll = self.client.pymongo_test.collection
        coll.insert_many([{} for _ in range(1000)])
        self.addCleanup(coll.drop)

        cursor = create_cursor(coll, None)
        next(cursor)
        # Session is "owned" by cursor.
        session = getattr(cursor, '_%s__session' % cursor.__class__.__name__)
        self.assertIsNotNone(session)
        lsid = session.session_id
        next(cursor)

        # Cursor owns its session unto death.
        self.assertNotIn(lsid, session_ids(self.client))
        close_cursor(cursor)
        self.assertIn(lsid, session_ids(self.client))

        # An explicit session is not ended by cursor.close() or list(cursor).
        with self.client.start_session() as s:
            cursor = create_cursor(coll, s)
            next(cursor)
            close_cursor(cursor)
            self.assertFalse(s.has_ended)
            lsid = s.session_id

        self.assertTrue(s.has_ended)
        self.assertIn(lsid, session_ids(self.client))

    def test_cursor_close(self):
        self._test_cursor_helper(
            lambda coll, session: coll.find(session=session),
            lambda cursor: cursor.close())

    def test_command_cursor_close(self):
        self._test_cursor_helper(
            lambda coll, session: coll.aggregate([], session=session),
            lambda cursor: cursor.close())

    def test_cursor_del(self):
        self._test_cursor_helper(
            lambda coll, session: coll.find(session=session),
            lambda cursor: cursor.__del__())

    def test_command_cursor_del(self):
        self._test_cursor_helper(
            lambda coll, session: coll.aggregate([], session=session),
            lambda cursor: cursor.__del__())


class TestSessionsMultiAuth(IntegrationTest):
    @client_context.require_auth
    @client_context.require_sessions
    def setUp(self):
        super(TestSessionsMultiAuth, self).setUp()

        client = rs_or_single_client()  # Logged in as root.
        db = client.pymongo_test
        db.add_user('second-user', 'pass', roles=['readWrite'])
        self.addCleanup(db.remove_user, 'second-user')

    @ignore_deprecations
    def test_session_authenticate_multiple(self):
        listener = SessionTestListener()
        # Logged in as root.
        client = rs_or_single_client(event_listeners=[listener])
        db = client.pymongo_test
        db.authenticate('second-user', 'pass')

        with self.assertRaises(InvalidOperation):
            client.start_session()

        # No implicit sessions.
        listener.results.clear()
        db.collection.find_one()
        event = listener.first_command_started()
        self.assertNotIn(
            'lsid', event.command,
            "find_one with multi-auth shouldn't have sent lsid with %s" % (
                event.command_name))

    @ignore_deprecations
    def test_explicit_session_logout(self):
        listener = SessionTestListener()

        # Changing auth invalidates the session. Start as root.
        client = rs_or_single_client(event_listeners=[listener])
        db = client.pymongo_test
        db.collection.insert_many([{} for _ in range(10)])
        self.addCleanup(db.collection.drop)

        with client.start_session() as s:
            listener.results.clear()
            cursor = db.collection.find(session=s).batch_size(2)
            next(cursor)
            event = listener.first_command_started()
            self.assertEqual(event.command_name, 'find')
            self.assertEqual(
                s.session_id, event.command.get('lsid'),
                "find() sent wrong lsid with %s cmd" % (event.command_name,))

            client.admin.logout()
            db.authenticate('second-user', 'pass')

            err = 'session was used after authenticating with different' \
                  ' credentials'

            with self.assertRaisesRegex(InvalidOperation, err):
                # Auth has changed between find and getMore.
                list(cursor)

            with self.assertRaisesRegex(InvalidOperation, err):
                db.collection.bulk_write([InsertOne({})], session=s)

            with self.assertRaisesRegex(InvalidOperation, err):
                db.collection_names(session=s)

            with self.assertRaisesRegex(InvalidOperation, err):
                db.collection.find_one(session=s)

            with self.assertRaisesRegex(InvalidOperation, err):
                list(db.collection.aggregate([], session=s))

    @ignore_deprecations
    def test_implicit_session_logout(self):
        listener = SessionTestListener()

        # Changing auth doesn't invalidate the session. Start as root.
        client = rs_or_single_client(event_listeners=[listener])
        db = client.pymongo_test

        for name, f in [
            ('bulk_write', lambda: db.collection.bulk_write([InsertOne({})])),
            ('collection_names', db.collection_names),
            ('find_one', db.collection.find_one),
            ('aggregate', lambda: list(db.collection.aggregate([])))
        ]:
            def sub_test():
                listener.results.clear()
                f()
                for event in listener.results['started']:
                    self.assertIn(
                        'lsid', event.command,
                        "%s sent no lsid with %s" % (
                            name, event.command_name))

            # We switch auth without clearing the pool of session ids. The
            # server considers these to be new sessions since it's a new user.
            # The old sessions time out on the server after 30 minutes.
            client.admin.logout()
            db.authenticate('second-user', 'pass')
            sub_test()
            db.logout()
            client.admin.authenticate(db_user, db_pwd)
            sub_test()


class TestSessionsNotSupported(IntegrationTest):
    @client_context.require_version_max(3, 5, 10)
    def test_sessions_not_supported(self):
        with self.assertRaisesRegex(
                ConfigurationError, "Sessions are not supported"):
            self.client.start_session()
