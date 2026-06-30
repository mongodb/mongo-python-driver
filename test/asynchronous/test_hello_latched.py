import unittest
from unittest.mock import AsyncMock 
from types import SimpleNamespace
from pymongo.asynchronous.pool import AsyncConnection 

class TestHelloLatched(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._sent = []

    def create_connection(self) -> AsyncConnection:
        """Returns a minimal connection object for _hello"""
        conn = object.__new__(AsyncConnection)
        conn.hello_ok = False
        conn.performed_handshake = True
        conn.opts = SimpleNamespace(
            server_api = None,
            load_balanced = False,
            _credentials = None
        )

        return conn

    async def mock_conn_command(self, db, cmd, **kwargs):
        """Returns mocked hello and ismaster results for conn.command"""
        self._sent.append(cmd.copy())
        if cmd.get("ismaster") == 1:
            return {"ok":1, "helloOk": True, "ismaster": True, "maxWireVersion": 25}
        return {"ok":1, "isWritablePrimary": True, "maxWireVersion": 25}


    async def test_hello_is_latched(self):
        """
        Regression Test for PYTHON-5904
        Tests for connection hello_ok persistence when connection
        Switches from ismaster to hello
        """
        conn = self.create_connection()
        conn.command = AsyncMock(side_effect=self.mock_conn_command)

        # First hello
        await conn._hello(None, None)
        # Verify hello_ok is True
        self.assertTrue(conn.hello_ok)
        # Verify command sent is ismaster
        self.assertEqual(self._sent[0].get("ismaster"), 1)
        self.assertEqual(self._sent[0].get("helloOk"), True)

        # Second hello
        await conn._hello(None, None)
        # Verify hello_ok has not changed
        self.assertTrue(conn.hello_ok)
        # Verify command sent is hello
        self.assertEqual(self._sent[1].get("hello"), 1)
        self.assertIsNone(self._sent[1].get("ismaster", None))

        # Third hello
        await conn._hello(None, None)
        # Verify connection continues to use hello
        self.assertEqual(self._sent[2].get("hello"), 1)
        self.assertIsNone(self._sent[1].get("ismaster", None))


if __name__ == "__main__":
    unittest.main()


