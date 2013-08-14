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

from pymongo import helpers, thread_util
from pymongo import ReadPreference
from pymongo.common import BaseObject
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.errors import AutoReconnect


class MasterSlaveConnection(BaseObject):
    """A master-slave connection to Mongo.
    """

    def __init__(self, master, slaves=[], document_class=dict, tz_aware=False):
        """Create a new Master-Slave connection.

        The resultant connection should be interacted with using the same
        mechanisms as a regular `MongoClient`. The `MongoClient` instances used
        to create this `MasterSlaveConnection` can themselves make use of
        connection pooling, etc. `MongoClient` instances used as slaves should
        be created with the read_preference option set to
        :attr:`~pymongo.read_preferences.ReadPreference.SECONDARY`. Write
        concerns are inherited from `master` and can be changed in this
        instance.

        Raises TypeError if `master` is not an instance of `MongoClient` or
        slaves is not a list of at least one `MongoClient` instances.

        :Parameters:
          - `master`: `MongoClient` instance for the writable Master
          - `slaves`: list of `MongoClient` instances for the
            read-only slaves
          - `document_class` (optional): default class to use for
            documents returned from queries on this connection
          - `tz_aware` (optional): if ``True``,
            :class:`~datetime.datetime` instances returned as values
            in a document by this :class:`MasterSlaveConnection` will be timezone
            aware (otherwise they will be naive)
        """
        if not isinstance(master, MongoClient):
            raise TypeError("master must be a MongoClient instance")
        if not isinstance(slaves, list) or len(slaves) == 0:
            raise TypeError("slaves must be a list of length >= 1")

        for slave in slaves:
            if not isinstance(slave, MongoClient):
                raise TypeError("slave %r is not an instance of MongoClient" %
                                slave)

        super(MasterSlaveConnection,
              self).__init__(read_preference=ReadPreference.SECONDARY,
                             safe=master.safe,
                             **master.write_concern)

        self.__master = master
        self.__slaves = slaves
        self.__document_class = document_class
        self.__tz_aware = tz_aware
        self.__request_counter = thread_util.Counter(master.use_greenlets)

    @property
    def master(self):
        return self.__master

    @property
    def slaves(self):
        return self.__slaves

    @property
    def is_mongos(self):
        """If this MasterSlaveConnection is connected to mongos (always False)

        .. versionadded:: 2.3
        """
        return False

    @property
    def use_greenlets(self):
        """Whether calling :meth:`start_request` assigns greenlet-local,
        rather than thread-local, sockets.

        .. versionadded:: 2.4.2
        """
        return self.master.use_greenlets

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

    @property
    def max_bson_size(self):
        """Return the maximum size BSON object the connected master
        accepts in bytes. Defaults to 4MB in server < 1.7.4.

        .. versionadded:: 2.6
        """
        return self.master.max_bson_size

    @property
    def max_message_size(self):
        """Return the maximum message size the connected master
        accepts in bytes.

        .. versionadded:: 2.6
        """
        return self.master.max_message_size


    def disconnect(self):
        """Disconnect from MongoDB.

        Disconnecting will call disconnect on all master and slave
        connections.

        .. seealso:: Module :mod:`~pymongo.mongo_client`
        .. versionadded:: 1.10.1
        """
        self.__master.disconnect()
        for slave in self.__slaves:
            slave.disconnect()

    def set_cursor_manager(self, manager_class):
        """Set the cursor manager for this connection.

        Helper to set cursor manager for each individual `MongoClient` instance
        that make up this `MasterSlaveConnection`.
        """
        self.__master.set_cursor_manager(manager_class)
        for slave in self.__slaves:
            slave.set_cursor_manager(manager_class)

    def _ensure_connected(self, sync):
        """Ensure the master is connected to a mongod/s.
        """
        self.__master._ensure_connected(sync)

    # _connection_to_use is a hack that we need to include to make sure
    # that killcursor operations can be sent to the same instance on which
    # the cursor actually resides...
    def _send_message(self, message,
                      with_last_error=False, _connection_to_use=None):
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
            return self.__master._send_message(message, with_last_error)
        return self.__slaves[_connection_to_use]._send_message(
            message, with_last_error, check_primary=False)

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
                member = self.__master
                conn = -1
            else:
                member = self.__slaves[_connection_to_use]
                conn = _connection_to_use
            return (conn,
                    member._send_message_with_response(message, **kwargs)[1])

        # _must_use_master is set for commands, which must be sent to the
        # master instance. any queries in a request must be sent to the
        # master since that is where writes go.
        if _must_use_master or self.in_request():
            return (-1, self.__master._send_message_with_response(message,
                                                                  **kwargs)[1])

        # Iterate through the slaves randomly until we have success. Raise
        # reconnect if they all fail.
        for connection_id in helpers.shuffled(xrange(len(self.__slaves))):
            try:
                slave = self.__slaves[connection_id]
                return (connection_id,
                        slave._send_message_with_response(message,
                                                          **kwargs)[1])
            except AutoReconnect:
                pass

        raise AutoReconnect("failed to connect to slaves")

    def start_request(self):
        """Start a "request".

        Start a sequence of operations in which order matters. Note
        that all operations performed within a request will be sent
        using the Master connection.
        """
        self.__request_counter.inc()
        self.master.start_request()

    def in_request(self):
        return bool(self.__request_counter.get())

    def end_request(self):
        """End the current "request".

        See documentation for `MongoClient.end_request`.
        """
        self.__request_counter.dec()
        self.master.end_request()

    def __eq__(self, other):
        if isinstance(other, MasterSlaveConnection):
            us = (self.__master, self.slaves)
            them = (other.__master, other.__slaves)
            return us == them
        return NotImplemented

    def __ne__(self, other):
        return not self == other

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
          - `connection_id`: id of the `MongoClient` instance where the cursor
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

    def _cache_index(self, database_name, collection_name,
                     index_name, cache_for):
        return self.__master._cache_index(database_name, collection_name,
                                          index_name, cache_for)

    def _purge_index(self, database_name,
                     collection_name=None, index_name=None):
        return self.__master._purge_index(database_name,
                                          collection_name,
                                          index_name)
