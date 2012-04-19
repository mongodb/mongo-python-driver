# Copyright 2009-2012 10gen, Inc.
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
slaves. Reads are tried on each slave in turn until the read succeeds
or all slaves failed.
"""

from pymongo import helpers
from pymongo import ReadPreference
from pymongo.common import BaseObject
from pymongo.connection import Connection
from pymongo.database import Database
from pymongo.errors import AutoReconnect


class MasterSlaveConnection(BaseObject):
    """A master-slave connection to Mongo.
    """

    def __init__(self, master, slaves=[], document_class=dict, tz_aware=False):
        """Create a new Master-Slave connection.

        The resultant connection should be interacted with using the same
        mechanisms as a regular `Connection`. The `Connection` instances used
        to create this `MasterSlaveConnection` can themselves make use of
        connection pooling, etc. 'Connection' instances used as slaves should
        be created with the read_preference option set to
        :attr:`~pymongo.ReadPreference.SECONDARY`. Safe options are
        inherited from `master` and can be changed in this instance.

        Raises TypeError if `master` is not an instance of `Connection` or
        slaves is not a list of at least one `Connection` instances.

        :Parameters:
          - `master`: `Connection` instance for the writable Master
          - `slaves` (optional): list of `Connection` instances for the
            read-only slaves
          - `document_class` (optional): default class to use for
            documents returned from queries on this connection
          - `tz_aware` (optional): if ``True``,
            :class:`~datetime.datetime` instances returned as values
            in a document by this :class:`MasterSlaveConnection` will be timezone
            aware (otherwise they will be naive)
        """
        if not isinstance(master, Connection):
            raise TypeError("master must be a Connection instance")
        if not isinstance(slaves, list) or len(slaves) == 0:
            raise TypeError("slaves must be a list of length >= 1")

        for slave in slaves:
            if not isinstance(slave, Connection):
                raise TypeError("slave %r is not an instance of Connection" %
                                slave)

        super(MasterSlaveConnection,
              self).__init__(read_preference=ReadPreference.SECONDARY,
                             safe=master.safe,
                             **(master.get_lasterror_options()))

        self.__in_request = False
        self.__master = master
        self.__slaves = slaves
        self.__document_class = document_class
        self.__tz_aware = tz_aware

    @property
    def master(self):
        return self.__master

    @property
    def slaves(self):
        return self.__slaves

    def get_document_class(self):
        return self.__document_class

    def set_document_class(self, klass):
        self.__document_class = klass

    document_class = property(get_document_class, set_document_class,
                              doc="""Default class to use for documents
                              returned on this connection.""")

    @property
    def tz_aware(self):
        return self.__tz_aware

    def disconnect(self):
        """Disconnect from MongoDB.

        Disconnecting will call disconnect on all master and slave
        connections.

        .. seealso:: Module :mod:`~pymongo.connection`
        .. versionadded:: 1.10.1
        """
        self.__master.disconnect()
        for slave in self.__slaves:
            slave.disconnect()

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
    def _send_message_with_response(self, message, _connection_to_use=None,
                                    _must_use_master=False, **kwargs):
        """Receive a message from Mongo.

        Sends the given message and returns a (connection_id, response) pair.

        :Parameters:
          - `operation`: opcode of the message to send
          - `data`: data to send
        """
        if _connection_to_use is not None:
            if _connection_to_use == -1:
                return (-1,
                         self.__master._send_message_with_response(message,
                                                                   **kwargs))
            else:
                return (_connection_to_use,
                        self.__slaves[_connection_to_use]
                        ._send_message_with_response(message, **kwargs))

        # _must_use_master is set for commands, which must be sent to the
        # master instance. any queries in a request must be sent to the
        # master since that is where writes go.
        if _must_use_master or self.__in_request:
            return (-1, self.__master._send_message_with_response(message,
                                                                  **kwargs))

        # Iterate through the slaves randomly until we have success. Raise
        # reconnect if they all fail.
        for connection_id in helpers.shuffled(xrange(len(self.__slaves))):
            try:
                slave = self.__slaves[connection_id]
                return (connection_id,
                        slave._send_message_with_response(message, **kwargs))
            except AutoReconnect:
                pass

        raise AutoReconnect("failed to connect to slaves")

    def start_request(self):
        """Start a "request".

        Start a sequence of operations in which order matters. Note
        that all operations performed within a request will be sent
        using the Master connection.
        """
        self.master.start_request()
        self.__in_request = True

    def end_request(self):
        """End the current "request".

        See documentation for `Connection.end_request`.
        """
        self.__in_request = False
        self.__master.end_request()

    def __eq__(self, other):
        if isinstance(other, MasterSlaveConnection):
            us = (self.__master, self.slaves)
            them = (other.__master, other.__slaves)
            return us == them
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

    def _cached(self, database_name, collection_name, index_name):
        return self.__master._cached(database_name,
                                     collection_name, index_name)

    def _cache_index(self, database_name, collection_name, index_name, ttl):
        return self.__master._cache_index(database_name, collection_name,
                                          index_name, ttl)

    def _purge_index(self, database_name,
                     collection_name=None, index_name=None):
        return self.__master._purge_index(database_name,
                                          collection_name,
                                          index_name)
