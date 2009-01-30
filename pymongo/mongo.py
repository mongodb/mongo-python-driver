# Copyright 2009 10gen, Inc.
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

"""Wrapper class for a Mongo database connection.

Adds some nice functionality."""

import socket
import types
import traceback
import struct

import database
from connection import Connection
from son import SON
from son_manipulator import NamespaceInjector
from objectid import ObjectId
from dbref import DBRef
from cursor_manager import BatchCursorManager

class Mongo(database.Database):
    """A connection to a Mongo database.
    """
    def __init__(self, name, host="localhost", port=27017, settings={}):
        """Open a new connection to the database at host:port.

        Raises TypeError if name or host is not an instance of string or port is
        not an instance of int. Raises ConnectionFailure if the connection
        cannot be made.

        Settings are passed in as a dictionary. Possible settings, along with
        their default values (in parens), are listed below:

          - "auto_dereference" (False): automatically dereference any `DBRef`
            contained within SON objects being returned from queries
          - "auto_reference" (False): automatically create `DBRef` out of any
            sub-objects that have already been saved in the database

        :Parameters:
          - `name`: the name of the database to connect to
          - `host` (optional): the hostname or IPv4 address of the database to
            connect to
          - `port` (optional): the port number on which to connect
          - `settings` (optional): a dictionary of settings
        """
        if not isinstance(settings, types.DictType):
            raise TypeError("settings must be an instance of dict")

        self.__auto_dereference = settings.get("auto_dereference", False)
        self.__auto_reference = settings.get("auto_reference", False)

        connection = Connection(host, port)
        connection.set_cursor_manager(BatchCursorManager)

        database.Database.__init__(self, connection, name)
        self.add_son_manipulator(NamespaceInjector(self), 0)

    def __repr__(self):
        return "Mongo(%r, %r, %r)" % (self.name(), self.connection().host(), self.connection().port())

    # TODO these should just be a SONManipulator, so we don't need to override
    # these two methods. I haven't done this yet because we aren't spending any
    # time on DBRef code right now.
    def _fix_outgoing(self, son, collection):
        """Fixes an object coming out of the database.

        Used to do things like auto dereferencing, if the option is enabled.

        :Parameters:
          - `son`: a SON object coming out of the database
          - `collection`: collection this object is being retrieved from
        """
        son = database.Database._fix_outgoing(self, son, collection)

        if not self.__auto_dereference:
            return son

        def fix_value(value):
            if isinstance(value, DBRef):
                deref = self.dereference(value)
                if deref is None:
                    return value
                return self._fix_outgoing(deref, collection)
            elif isinstance(value, (SON, types.DictType)):
                return self._fix_outgoing(value, collection)
            elif isinstance(value, types.ListType):
                return [fix_value(v) for v in value]
            return value

        for (key, value) in son.items():
            son[key] = fix_value(value)

        return son

    def _fix_incoming(self, to_save, collection):
        """Fixes an object going in to the database.

        Used to do things like auto referencing, if the option is enabled.
        Will also add _id and _ns if they are missing and desired (as specified
        by add_meta).

        :Parameters:
          - `to_save`: a SON object going into the database
          - `collection`: collection into which this object is being saved
        """
        to_save = database.Database._fix_incoming(self, to_save, collection)

        if not self.__auto_reference:
            return to_save

        # make a copy, so only what is being saved gets auto-ref'ed
        to_save = SON(to_save)

        def fix_value(value):
            if isinstance(value, (SON, types.DictType)):
                if "_id" in value and "_ns" in value:
                    return DBRef(value["_ns"], value["_id"])
            return value

        for (key, value) in to_save.items():
            to_save[key] = fix_value(value)

        return to_save
