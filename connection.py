"""Low level connection to Mongo."""

import socket
import struct
import types
import traceback
import os
import unittest

class ConnectionFailed(IOError):
    """Raised when a connection to the database cannot be made or is lost.
    """

class Connection(object):
    """A connection to Mongo.
    """
    def __init__(self, host="localhost", port=27017):
        """Open a new connection to the database at host:port.

        Raises TypeError if host is not an instance of string or port is not an
        instance of int. Raises ConnectionFailed if the connection cannot be
        made.

        Arguments:
        - `host` (optional): the hostname or IPv4 address of the database to
            connect to
        - `port` (optional): the port number on which to connect
        """
        if not isinstance(host, types.StringType):
            raise TypeError("host must be an instance of str")
        if not isinstance(port, types.IntType):
            raise TypeError("port must be an instance of int")
        self.__host = host
        self.__port = port
        self.__id = 1

        self.__connect()

    def __connect(self):
        """(Re-)connect to Mongo."""
        try:
            self.__socket = socket.socket()
            self.__socket.connect((self.__host, self.__port))
        except socket.error:
            raise ConnectionFailed("could not connect to %s:%s, got: %s" %
                                   (self.__host, self.__port,
                                    traceback.format_exc()))

    def send_message(self, operation, data):
        """Say something to Mongo.

        Raises ConnectionFailed if the message cannot be sent. Returns the
        request id of the sent message.

        Arguments:
        - `operation`: the opcode of the message
        - `data`: the data to send
        """
        # header
        to_send = struct.pack("<i", 16 + len(data))
        to_send += struct.pack("<i", self.__id)
        self.__id += 1
        to_send += struct.pack("<i", 0) # responseTo
        to_send += struct.pack("<i", operation)

        to_send += data

        total_sent = 0
        while total_sent < len(to_send):
            sent = self.__socket.send(to_send[total_sent:])
            if sent == 0:
                raise ConnectionFailed("connection closed")
            total_sent += sent

        return self.__id - 1

    def receive_message(self, operation, request_id):
        """Receive a message from Mongo.

        Returns the message body. Asserts that the message uses the given opcode
        and request id. Calls to receive_message and send_message should be done
        synchronously.

        Arguments:
        - `operation`: the opcode of the message
        - `request_id`: the request id that the message should be in response to
        """
        def receive(length):
            message = ""
            while len(message) < length:
                chunk = self.__socket.recv(length - len(message))
                if chunk == "":
                    raise ConnectionFailed("connection closed")
                message += chunk
            return message

        header = receive(16)
        length = struct.unpack("<i", header[:4])[0]
        assert request_id == struct.unpack("<i", header[8:12])[0]
        assert operation == struct.unpack("<i", header[12:])[0]

        return receive(length - 16)

    def __cmp__(self, other):
        if isinstance(other, Connection):
            return cmp((self.__host, self.__port), (other.__host, other.__port))
        return NotImplemented

    def __repr__(self):
        return "Connection(%r, %r)" % (self.__host, self.__port)

class TestConnection(unittest.TestCase):
    def setUp(self):
        self.host = os.environ.get("db_ip", "localhost")
        self.port = int(os.environ.get("db_port", 27017))

    def test_types(self):
        self.assertRaises(TypeError, Connection, 1)
        self.assertRaises(TypeError, Connection, 1.14)
        self.assertRaises(TypeError, Connection, None)
        self.assertRaises(TypeError, Connection, [])
        self.assertRaises(TypeError, Connection, "localhost", "27017")
        self.assertRaises(TypeError, Connection, "localhost", 1.14)
        self.assertRaises(TypeError, Connection, "localhost", None)
        self.assertRaises(TypeError, Connection, "localhost", [])

    def test_connect(self):
        self.assertRaises(ConnectionFailed, Connection, "somedomainthatdoesntexist.org")
        self.assertRaises(ConnectionFailed, Connection, self.host, 123456789)

        self.assertTrue(Connection(self.host, self.port))

if __name__ == "__main__":
    unittest.main()
