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

from pymongo.errors import InvalidOperation
from test import IntegrationTest, client_context
from test.utils import ignore_deprecations, rs_or_single_client


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
