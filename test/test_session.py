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

from pymongo import OFF, InsertOne, IndexModel
from pymongo.errors import InvalidOperation, ConfigurationError
from pymongo.monitoring import _SENSITIVE_COMMANDS
from test import IntegrationTest, client_context
from test.utils import ignore_deprecations, rs_or_single_client, EventListener


# Ignore redacted commands like saslStart, so we can assert lsid is in all cmds.
class SessionTestListener(EventListener):
    def started(self, event):
        if event.command_name not in _SENSITIVE_COMMANDS:
            super(SessionTestListener, self).started(event)

    def succeeded(self, event):
        if event.command_name not in _SENSITIVE_COMMANDS:
            super(SessionTestListener, self).succeeded(event)

    def failed(self, event):
        if event.command_name not in _SENSITIVE_COMMANDS:
            super(SessionTestListener, self).failed(event)


class TestSession(IntegrationTest):
    @client_context.require_auth
    @ignore_deprecations
    def test_session_authenticate_multiple(self):
        # Logged in as root.
        client = rs_or_single_client()
        client.pymongo_test.add_user('second-user', 'pass')
        self.addCleanup(client.pymongo_test.remove_user, 'second-user')

        client.pymongo_test.authenticate('second-user', 'pass')

        with self.assertRaises(InvalidOperation):
            client.start_session()

    @client_context.require_sessions
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

    @client_context.require_version_max(3, 5, 10)
    def test_sessions_not_supported(self):
        with self.assertRaisesRegex(
                ConfigurationError, "Sessions are not supported"):
            self.client.start_session()

    @client_context.require_sessions
    def test_database(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        client.drop_database('pymongo_test')
        self.addCleanup(client.drop_database, 'pymongo_test')

        db = client.pymongo_test

        # Test most database methods. Some, like add_user, we can't test with
        # command monitoring. See monitoring._SENSITIVE_COMMANDS.
        ops = [
            (db.command, ['ping'], {}),
            (db.create_collection, ['collection'], {}),
            (db.collection_names, [], {}),
            (db.validate_collection, ['collection'], {}),
            (db.drop_collection, ['collection'], {}),
            (db.current_op, [], {}),
        ]

        if not client_context.is_mongos:
            ops.append((db.set_profiling_level, [OFF], {}))
            ops.append((db.profiling_level, [], {}))

        with client.start_session() as s:
            for f, args, kwargs in ops:
                listener.results.clear()
                kwargs['session'] = s
                f(*args, **kwargs)
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

    @client_context.require_sessions
    def test_collection(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        client.drop_database('pymongo_test')
        self.addCleanup(client.drop_database, 'pymongo_test')

        coll = client.pymongo_test.collection

        # Test some collection methods - the rest are in test_cursor.
        ops = [
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
            (coll.aggregate, [[]], {}),
        ]

        if not client_context.is_mongos:
            def scan(session):
                cursors = coll.parallel_scan(4, session)
                for c in cursors:
                    list(c)

            ops.append((scan, [], {}))

        with client.start_session() as s:
            for f, args, kwargs in ops:
                listener.results.clear()
                kwargs['session'] = s
                f(*args, **kwargs)
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

    @client_context.require_sessions
    def test_cursor_clone(self):
        with self.client.start_session() as s:
            cursor = self.client.db.collection.find(session=s)
            clone = cursor.clone()
            self.assertTrue(cursor.session is clone.session)

    @client_context.require_sessions
    def test_cursor(self):
        listener = SessionTestListener()
        client = rs_or_single_client(event_listeners=[listener])
        client.drop_database('pymongo_test')
        self.addCleanup(client.drop_database, 'pymongo_test')

        coll = client.pymongo_test.collection
        coll.insert_one({})

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

        with client.start_session() as s:
            for name, f in ops:
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
