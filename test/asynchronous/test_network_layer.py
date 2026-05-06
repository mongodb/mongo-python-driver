# Copyright 2026-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for code in network_layer.py shared between sync and async APIs.

Async-only tests live in ``test_async_network_layer.py``.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

sys.path[0:0] = [""]

from test.asynchronous import AsyncUnitTest, unittest

from pymongo.network_layer import NetworkingInterfaceBase

_IS_SYNC = False


class TestNetworkingInterfaceBase(AsyncUnitTest):
    async def asyncSetUp(self):
        self.base = NetworkingInterfaceBase(MagicMock())

    def test_gettimeout_raises(self):
        with self.assertRaises(NotImplementedError):
            _ = self.base.gettimeout

    def test_settimeout_raises(self):
        with self.assertRaises(NotImplementedError):
            self.base.settimeout(1.0)

    def test_close_raises(self):
        with self.assertRaises(NotImplementedError):
            self.base.close()

    def test_is_closing_raises(self):
        with self.assertRaises(NotImplementedError):
            self.base.is_closing()

    def test_get_conn_raises(self):
        with self.assertRaises(NotImplementedError):
            _ = self.base.get_conn

    def test_sock_raises(self):
        with self.assertRaises(NotImplementedError):
            _ = self.base.sock


if __name__ == "__main__":
    unittest.main()
