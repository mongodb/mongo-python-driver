# Copyright 2025-present MongoDB, Inc.
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
import pathlib
import sys

sys.path[0:0] = [""]

from test.asynchronous import (
    AsyncIntegrationTest,
    async_client_context,
    unittest,
)
from test.asynchronous.unified_format import generate_test_classes
from test.utils_shared import EventListener, OvertCommandListener

_IS_SYNC = False


class AsyncTestClientBackpressure(AsyncIntegrationTest):
    listener: EventListener

    @classmethod
    def setUpClass(cls) -> None:
        cls.listener = OvertCommandListener()

    @async_client_context.require_connection
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.listener.reset()
        self.client = await self.async_rs_or_single_client(
            event_listeners=[self.listener], retryWrites=False
        )


# Location of JSON test specifications.
if _IS_SYNC:
    _TEST_PATH = os.path.join(pathlib.Path(__file__).resolve().parent, "client-backpressure")
else:
    _TEST_PATH = os.path.join(pathlib.Path(__file__).resolve().parent.parent, "client-backpressure")

globals().update(
    generate_test_classes(
        _TEST_PATH,
        module=__name__,
    )
)

if __name__ == "__main__":
    unittest.main()
