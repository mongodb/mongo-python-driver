"""Wrapper class for a Mongo database connection.

Adds some nice functionality."""

import socket
import types
import traceback
import struct
import random

from database import Database
from connection import Connection
from son import SON
from objectid import ObjectId
from dbref import DBRef
from errors import ConnectionFailure, InvalidOperation
from collection import SYSTEM_INDEX_COLLECTION

_MAX_DYING_CURSORS = 20

ASCENDING = 1
DESCENDING = -1

class Mongo(Database):
    """A connection to a Mongo database.
    """
    def __init__(self, name, host="localhost", port=27017, settings={}):
        """Open a new connection to the database at host:port.

        Raises TypeError if name or host is not an instance of string or port is
        not an instance of int. Raises ConnectionFailure if the connection
        cannot be made.

        Settings are passed in as a dictionary. Possible settings, along with
        their default values (in parens), are listed below:
        - "auto_dereference" (False): automatically dereference any `DBRef`s
            contained within SON objects being returned from queries
        - "auto_reference" (False): automatically create `DBRef`s out of any
            sub-objects that have already been saved in the database

        Arguments:
        - `name`: the name of the database to connect to
        - `host` (optional): the hostname or IPv4 address of the database to
            connect to
        - `port` (optional): the port number on which to connect
        - `settings` (optional): a dictionary of settings
        """
        if not isinstance(settings, types.DictType):
            raise TypeError("settings must be an instance of dict")

        self.__dying_cursors = []
        self.__auto_dereference = settings.get("auto_dereference", False)
        self.__auto_reference = settings.get("auto_reference", False)

        Database.__init__(self, Connection(host, port), name)

    def _kill_cursors(self):
        message = "\x00\x00\x00\x00"
        message += struct.pack("<i", len(self.__dying_cursors))
        for cursor_id in self.__dying_cursors:
            message += struct.pack("<q", cursor_id)
        self.connection().send_message(2007, message)
        self.__dying_cursors = []

    def _kill_cursor(self, cursor_id):
        self.__dying_cursors.append(cursor_id)

        if len(self.__dying_cursors) > _MAX_DYING_CURSORS:
            self._kill_cursors()

    def __repr__(self):
        return "Mongo(%r, %r, %r)" % (self.name(), self.connection().host(), self.connection().port())

    def dereference(self, dbref):
        """Dereference a DBRef, getting the SON object it points to.

        Raises TypeError if dbref is not an instance of DBRef. Returns a SON
        object or None if the reference does not point to a valid object.

        Arguments:
        - `dbref`: the reference
        """
        if not isinstance(dbref, DBRef):
            raise TypeError("cannot dereference a %s" % type(dbref))
        return self[dbref.collection()].find_one(dbref.id())

    def _fix_outgoing(self, son):
        """Fixes an object coming out of the database.

        Used to do things like auto dereferencing, if the option is enabled.

        Arguments:
        - `son`: a SON object coming out of the database
        """
        if not self.__auto_dereference:
            return son

        def fix_value(value):
            if isinstance(value, DBRef):
                deref = self.dereference(value)
                if deref is None:
                    return value
                return self._fix_outgoing(deref)
            elif isinstance(value, (SON, types.DictType)):
                return self._fix_outgoing(value)
            elif isinstance(value, types.ListType):
                return [fix_value(v) for v in value]
            return value

        for (key, value) in son.items():
            son[key] = fix_value(value)

        return son

    def _fix_incoming(self, to_save, collection, add_meta):
        """Fixes an object going in to the database.

        Used to do things like auto referencing, if the option is enabled.
        Will also add _id and _ns if they are missing and desired (as specified
        by add_meta).

        Arguments:
        - `to_save`: a SON object going into the database
        - `collection`: collection into which this object is being saved
        - `add_meta`: should _id and other meta-fields be added to the object
        """
        if "_id" in to_save:
            assert isinstance(to_save["_id"], ObjectId), "'_id' must be an ObjectId"
        elif add_meta:
            to_save["_id"] = ObjectId()

        if add_meta:
            to_save["_ns"] = collection._name()

        if not self.__auto_reference:
            return to_save

        # make a copy, so only what is being saved gets auto-ref'ed
        to_save = SON(to_save)

        def fix_value(value):
            if isinstance(value, (SON, types.DictType)):
                if "_id" in value and not value["_id"].is_new() and "_ns" in value:
                    return DBRef(value["_ns"], value["_id"])
            return value

        for (key, value) in to_save.items():
            to_save[key] = fix_value(value)

        return to_save
