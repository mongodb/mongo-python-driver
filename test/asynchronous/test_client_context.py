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
from __future__ import annotations

import os
import sys

sys.path[0:0] = [""]

from test.asynchronous import AsyncUnitTest, SkipTest, async_client_context, unittest

_IS_SYNC = False


class TestAsyncClientContext(AsyncUnitTest):
    def test_must_connect(self):
        if not os.environ.get("PYMONGO_MUST_CONNECT"):
            raise SkipTest("PYMONGO_MUST_CONNECT is not set")

        self.assertTrue(
            async_client_context.connected,
            "client context must be connected when "
            "PYMONGO_MUST_CONNECT is set. Failed attempts:\n{}".format(
                async_client_context.connection_attempt_info()
            ),
        )

    def test_enableTestCommands_is_disabled(self):
        if not os.environ.get("DISABLE_TEST_COMMANDS"):
            raise SkipTest("DISABLE_TEST_COMMANDS is not set")

        self.assertFalse(
            async_client_context.test_commands_enabled,
            "enableTestCommands must be disabled when DISABLE_TEST_COMMANDS is set.",
        )

    def test_free_threading_is_enabled(self):
        if "free-threading build" not in sys.version:
            raise SkipTest("this test requires the Python free-threading build")

        # If the GIL is enabled then pymongo or one of our deps does not support free-threading.
        self.assertFalse(sys._is_gil_enabled())  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
