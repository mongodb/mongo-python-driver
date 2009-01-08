"""Driver for Mongo.

The database is accessed through an instance of the Mongo class."""

import unittest
import socket
import types
import traceback
import os

import bson
import objectid
import dbref

class ConnectionException(IOError):
    """Raised when a connection to the database cannot be made or is lost.
    """

class Mongo(object):
    """A connection to a Mongo database.
    """
    def __init__(self, host="localhost", port=27017):
        """Open a new connection to the database at host:port.

        Raises TypeError if host is not an instance of (str, unicode) or port is
        not an instance of int. Raises ConnectionException if the connection
        cannot be made.

        Arguments:
        - `host` (optional): the hostname or IPv4 address of the database to
                             connect to
        - `port` (optional): the port number on which to connect
        """
        if not isinstance(host, types.StringTypes):
            raise TypeError("host must be an instance of (str, unicode)")
        if not isinstance(port, types.IntType):
            raise TypeError("port must be an instance of int")

        try:
            self.__connection = socket.socket()
            self.__connection.connect((host, port))
        except socket.error:
            raise ConnectionException("could not connect to %s:%s, got: %s" %
                                      (host, port, traceback.format_exc()))

class TestMongo(unittest.TestCase):
    def setUp(self):
        self.host = os.environ.get("db_ip", "localhost")
        self.port = int(os.environ.get("db_port", 27017))

    def testConnection(self):
        self.assertRaises(TypeError, Mongo, 1)
        self.assertRaises(TypeError, Mongo, 1.14)
        self.assertRaises(TypeError, Mongo, None)
        self.assertRaises(TypeError, Mongo, [])
        self.assertRaises(TypeError, Mongo, "localhost", "27017")
        self.assertRaises(TypeError, Mongo, "localhost", 1.14)
        self.assertRaises(TypeError, Mongo, "localhost", None)
        self.assertRaises(TypeError, Mongo, "localhost", [])

        self.assertRaises(ConnectionException, Mongo, "somedomainthatdoesntexist.org")
        self.assertRaises(ConnectionException, Mongo, self.host, 123456789)

        self.assertTrue(Mongo(self.host, self.port))

if __name__ == "__main__":
    unittest.main()
