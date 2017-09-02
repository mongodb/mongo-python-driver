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

from pymongo import OFF
from pymongo.errors import InvalidOperation, ConfigurationError
from test import IntegrationTest, client_context
from test.utils import ignore_deprecations, rs_or_single_client, EventListener


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

    @client_context.require_version_min(3, 5, 12)
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

    @client_context.require_version_min(3, 5, 12)
    def test_command_with_session(self):
        listener = EventListener()
        client = rs_or_single_client(event_listeners=[listener])
        with client.start_session() as s:
            client.admin.command('ping', session=s)
            self.assertEqual(s.session_id,
                             listener.results['started'][0].command['lsid'])

    @client_context.require_version_min(3, 5, 12)
    def test_database_commands(self):
        listener = EventListener()
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.drop_database, 'pymongo_test')

        db = client.pymongo_test

        # Test most database methods. Some, like add_user, we can't test with
        # command monitoring. See monitoring._SENSITIVE_COMMANDS.
        ops = [
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
