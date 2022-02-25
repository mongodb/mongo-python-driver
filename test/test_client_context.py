# Copyright 2018-present MongoDB, Inc.
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

import os
import sys

sys.path[0:0] = [""]

from test import SkipTest, client_context, unittest


class TestClientContext(unittest.TestCase):
    def test_must_connect(self):
        if "PYMONGO_MUST_CONNECT" not in os.environ:
            raise SkipTest("PYMONGO_MUST_CONNECT is not set")

        self.assertTrue(
            client_context.connected,
            "client context must be connected when "
            "PYMONGO_MUST_CONNECT is set. Failed attempts:\n%s"
            % (client_context.connection_attempt_info(),),
        )

    def test_serverless(self):
        if "TEST_SERVERLESS" not in os.environ:
            raise SkipTest("TEST_SERVERLESS is not set")

        self.assertTrue(
            client_context.connected and client_context.serverless,
            "client context must be connected to serverless when "
            "TEST_SERVERLESS is set. Failed attempts:\n%s"
            % (client_context.connection_attempt_info(),),
        )

    def test_enableTestCommands_is_disabled(self):
        if "PYMONGO_DISABLE_TEST_COMMANDS" not in os.environ:
            raise SkipTest("PYMONGO_DISABLE_TEST_COMMANDS is not set")

        self.assertFalse(
            client_context.test_commands_enabled,
            "enableTestCommands must be disabled when PYMONGO_DISABLE_TEST_COMMANDS is set.",
        )

    def test_setdefaultencoding_worked(self):
        if "SETDEFAULTENCODING" not in os.environ:
            raise SkipTest("SETDEFAULTENCODING is not set")

        self.assertEqual(sys.getdefaultencoding(), os.environ["SETDEFAULTENCODING"])


if __name__ == "__main__":
    unittest.main()
