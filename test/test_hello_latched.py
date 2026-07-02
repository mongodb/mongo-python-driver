# Copyright 2026-present MongoDB, Inc.
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

"""Test that connection continues to use hello after ismaster returns hello_ok"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from pymongo.synchronous.pool import Connection

_IS_SYNC = True


class TestHelloLatched(unittest.TestCase):
    def setUp(self):
        self._sent = []

    def create_connection(self) -> Connection:
        """Returns a minimal connection object for _hello"""
        conn = object.__new__(Connection)
        conn.hello_ok = False
        conn.performed_handshake = True
        conn.opts = SimpleNamespace(server_api=None, load_balanced=False, _credentials=None)

        return conn

    def mock_conn_command(self, db, cmd, **kwargs):
        """Returns mocked hello and ismaster results for conn.command"""
        self._sent.append(cmd.copy())
        if cmd.get("ismaster") == 1:
            return {"ok": 1, "helloOk": True, "ismaster": True, "maxWireVersion": 25}
        return {"ok": 1, "isWritablePrimary": True, "maxWireVersion": 25}

    def test_hello_is_latched(self):
        """
        Regression Test for PYTHON-5904
        Tests for connection hello_ok persistence when connection
        Switches from ismaster to hello
        """
        conn = self.create_connection()
        conn.command = Mock(side_effect=self.mock_conn_command)

        # First hello
        conn._hello(None, None)
        # Verify hello_ok is True
        self.assertTrue(conn.hello_ok)
        # Verify command sent is ismaster
        self.assertEqual(self._sent[0].get("ismaster"), 1)
        self.assertEqual(self._sent[0].get("helloOk"), True)

        # Second hello
        conn._hello(None, None)
        # Verify hello_ok has not changed
        self.assertTrue(conn.hello_ok)
        # Verify command sent is hello
        self.assertEqual(self._sent[1].get("hello"), 1)
        self.assertIsNone(self._sent[1].get("ismaster", None))

        # Third hello
        conn._hello(None, None)
        # Verify connection continues to use hello
        self.assertEqual(self._sent[2].get("hello"), 1)
        self.assertIsNone(self._sent[2].get("ismaster", None))


if __name__ == "__main__":
    unittest.main()
