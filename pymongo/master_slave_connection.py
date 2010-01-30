# Copyright 2009-2010 10gen, Inc.
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

"""Master-Slave connection to Mongo.

Performs all writes to Master instance and distributes reads among all
instances."""

import random

from pymongo.connection import Connection
from pymongo.database import Database


class MasterSlaveConnection(object):
    """A master-slave connection to Mongo.
    """

    def __init__(self, master, slaves=[]):
        """Create a new Master-Slave connection.

        The resultant connection should be interacted with using the same
        mechanisms as a regular `Connection`. The `Connection` instances used
        to create this `MasterSlaveConnection` can themselves make use of
        connection pooling, etc. 'Connection' instances used as slaves should
        be created with the slave_okay option set to True.

        Raises TypeError if `master` is not an instance of `Connection` or
        slaves is not a list of at least one `Connection` instances.

        :Parameters:
          - `master`: `Connection` instance for the writable Master
          - `slaves` (optional): list of `Connection` instances for the
            read-only slaves
        """
        if not isinstance(master, Connection):
            raise TypeError("master must be a Connection instance")
        if not isinstance(slaves, list) or len(slaves) == 0:
            raise TypeError("slaves must be a list of length >= 1")

        for slave in slaves:
            if not isinstance(slave, Connection):
                raise TypeError("slave %r is not an instance of Connection" %
                                slave)

        self.__in_request = False
        self.__master = master
        self.__slaves = slaves

    @property
    def master(self):
        return self.__master

    @property
    def slaves(self):
        return self.__slaves

    @property
    def slave_okay(self):
        """Is it okay for this connection to connect directly to a slave?

        This is always True for MasterSlaveConnection instances.
        """
        return True

    def set_cursor_manager(self, manager_class):
        """Set the cursor manager for this connection.

        Helper to set cursor manager for each individual `Connection` instance
        that make up this `MasterSlaveConnection`.
        """
        self.__master.set_cursor_manager(manager_class)
        for slave in self.__slaves:
            slave.set_cursor_manager(manager_class)

    # _connection_to_use is a hack that we need to include to make sure
    # that killcursor operations can be sent to the same instance on which
    # the cursor actually resides...
    def _send_message(self, message, safe=False, _connection_to_use=None):
        """Say something to Mongo.

        Sends a message on the Master connection. This is used for inserts,
        updates, and deletes.

        Raises ConnectionFailure if the message cannot be sent. Returns the
        request id of the sent message.

        :Parameters:
          - `operation`: opcode of the message
          - `data`: data to send
          - `safe`: perform a getLastError after sending the message
        """
        if _connection_to_use is None or _connection_to_use == -1:
            return self.__master._send_message(message, safe)
        return self.__slaves[_connection_to_use]._send_message(message, safe)

    # _connection_to_use is a hack that we need to include to make sure
    # that getmore operations can be sent to the same instance on which
    # the cursor actually resides...
    def _send_message_with_response(self, message,
                                    _sock=None, _connection_to_use=None,
                                    _must_use_master=False):
        """Receive a message from Mongo.

        Sends the given message and returns a (connection_id, response) pair.

        :Parameters:
          - `operation`: opcode of the message to send
          - `data`: data to send
        """
        if _connection_to_use is not None:
            if _connection_to_use == -1:
                return (-1, self.__master._send_message_with_response(message,
                                                                      _sock))
            else:
                return (_connection_to_use,
                        self.__slaves[_connection_to_use]
                        ._send_message_with_response(message, _sock))

        # for now just load-balance randomly among slaves only...
        connection_id = random.randrange(0, len(self.__slaves))

        # _must_use_master is set for commands, which must be sent to the
        # master instance. any queries in a request must be sent to the
        # master since that is where writes go.
        if _must_use_master or self.__in_request or connection_id == -1:
            return (-1, self.__master._send_message_with_response(message,
                                                                  _sock))

        return (connection_id,
                self.__slaves[connection_id]._send_message_with_response(message,
                                                                         _sock))

    def start_request(self):
        """Start a "request".

        Start a sequence of operations in which order matters. Note
        that all operations performed within a request will be sent
        using the Master connection.
        """
        self.__in_request = True

    def end_request(self):
        """End the current "request".

        See documentation for `Connection.end_request`.
        """
        self.__in_request = False
        self.__master.end_request()

    def __cmp__(self, other):
        if isinstance(other, MasterSlaveConnection):
            return cmp((self.__master, self.__slaves),
                       (other.__master, other.__slaves))
        return NotImplemented

    def __repr__(self):
        return "MasterSlaveConnection(%r, %r)" % (self.__master, self.__slaves)

    def __getattr__(self, name):
        """Get a database by name.

        Raises InvalidName if an invalid database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return Database(self, name)

    def __getitem__(self, name):
        """Get a database by name.

        Raises InvalidName if an invalid database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return self.__getattr__(name)

    def close_cursor(self, cursor_id, connection_id):
        """Close a single database cursor.

        Raises TypeError if cursor_id is not an instance of (int, long). What
        closing the cursor actually means depends on this connection's cursor
        manager.

        :Parameters:
          - `cursor_id`: cursor id to close
          - `connection_id`: id of the `Connection` instance where the cursor
            was opened
        """
        if connection_id == -1:
            return self.__master.close_cursor(cursor_id)
        return self.__slaves[connection_id].close_cursor(cursor_id)

    def database_names(self):
        """Get a list of all database names.
        """
        return self.__master.database_names()

    def drop_database(self, name_or_database):
        """Drop a database.

        :Parameters:
          - `name_or_database`: the name of a database to drop or the object
            itself
        """
        return self.__master.drop_database(name_or_database)

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'MasterSlaveConnection' object is not iterable")

    def _cache_index(self, database_name, collection_name, index_name, ttl):
        return self.__master._cache_index(database_name, collection_name,
                                          index_name, ttl)

    def _purge_index(self, database_name,
                     collection_name=None, index_name=None):
        return self.__master._purge_index(database_name,
                                          collection_name,
                                          index_name)
