"""Low level connection to Mongo."""

import socket
import struct
import types
import traceback

from errors import ConnectionFailure, InvalidName
from database import Database
from cursor_manager import CursorManager

class Connection(object):
    """A connection to Mongo.
    """
    def __init__(self, host="localhost", port=27017):
        """Open a new connection to the database at host:port.

        Raises TypeError if host is not an instance of string or port is not an
        instance of int. Raises ConnectionFailure if the connection cannot be
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
        self.__cursor_manager = CursorManager(self)

        self.__connect()

    def set_cursor_manager(self, manager_class):
        """Set this connections cursor manager.

        Raises TypeError if manager_class is not a subclass of CursorManager. A
        cursor manager handles closing cursors. Different managers can implement
        different policies in terms of when to actually kill a cursor that has
        been closed.
        """
        manager = manager_class(self)
        if not isinstance(manager, CursorManager):
            raise TypeError("manager_class must be a subclass of CursorManager")

        self.__cursor_manager = manager

    def host(self):
        """Get the connection host.
        """
        return self.__host

    def port(self):
        """Get the connection port.
        """
        return self.__port

    def __connect(self):
        """(Re-)connect to Mongo."""
        try:
            self.__socket = socket.socket()
            self.__socket.connect((self.__host, self.__port))
        except socket.error:
            raise ConnectionFailure("could not connect to %s:%s, got: %s" %
                                    (self.__host, self.__port,
                                     traceback.format_exc()))

    def send_message(self, operation, data):
        """Say something to Mongo.

        Raises ConnectionFailure if the message cannot be sent. Returns the
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
                raise ConnectionFailure("connection closed")
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
                    raise ConnectionFailure("connection closed")
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

    def __getattr__(self, name):
        """Get a database by name.

        Raises InvalidName if an invalid database name is used.

        Arguments:
        - `name`: the name of the database to get
        """
        return Database(self, name)

    def __getitem__(self, name):
        """Get a database by name.

        Raises InvalidName if an invalid database name is used.

        Arguments:
        - `name`: the name of the database to get
        """
        return self.__getattr__(name)

    def close_cursor(self, cursor_id):
        """Close a single database cursor.

        Raises TypeError if cursor_id is not an instance of (int, long). What
        closing the cursor actually means depends on this connection's cursor
        manager.

        Arguments:
        - `cursor_id`: cursor id to close
        """
        if not isinstance(cursor_id, (types.IntType, types.LongType)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self.__cursor_manager.close(cursor_id)

    def kill_cursors(self, cursor_ids):
        """Kill database cursors with the given ids.

        Raises TypeError if cursor_ids is not an instance of list.

        Arguments:
        - `cursor_ids`: list of cursor ids to kill
        """
        if not isinstance(cursor_ids, types.ListType):
            raise TypeError("cursor_ids must be a list")
        message = "\x00\x00\x00\x00"
        message += struct.pack("<i", len(cursor_ids))
        for cursor_id in cursor_ids:
            message += struct.pack("<q", cursor_id)
        self.send_message(2007, message)

    # TODO implement and test this
    def database_names(self):
        """Get a list of all database names.
        """
        raise Exception("unimplemented")

    # TODO implement and test this
    def drop_database(self, name_or_database):
        """Drop a database.

        Arguments:
        - `name_or_database`: the name of a database to drop or the object itself
        """
        raise Exception("unimplemented")
