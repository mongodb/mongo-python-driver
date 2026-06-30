import unittest
from unittest.mock import Mock 
from types import SimpleNamespace
from pymongo.synchronous.pool import Connection 


class TestHelloLatched(unittest.TestCase):
    """
    Test to Ensure that hello_ok remains latched
    when the RTT thread switches to hello
    """

    def setUp(self):
        self._sent = []

    def create_connection(self) -> Connection:
        """Returns a minimal connection object"""
        conn = object.__new__(Connection)
        conn.hello_ok = False
        # Isolate this test to post-handshake hello_ok persistence
        conn.performed_handshake = True
        conn.opts = SimpleNamespace(
            server_api = None,
            load_balanced = False,
            _credentials = None
        )

        return conn

    def mock_connection_command(self, db, cmd, **kwargs):
        """Returns mocked hello and ismaster results for conn.command"""
        self._sent.append(cmd.copy())
        if cmd.get("ismaster") == 1:
            return {"ok":1, "helloOk": True, "ismaster": True, "maxWireVersion": 25}
        return {"ok":1, "isWritablePrimary": True, "maxWireVersion": 25}


    def test_hello_is_latched(self):
        """
        Regression Test for PYTHON-5904
        Tests for connection hello_ok persistence when connection
        Switches from ismaster to hello
        """

        conn = self.create_connection()
        conn.command = Mock(side_effect=self.mock_connection_command)

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
        self.assertIsNone(self._sent[1].get("ismaster", None))


if __name__ == "__main__":
    unittest.main()


