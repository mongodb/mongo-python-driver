"""Database level operations."""

import types

from son_manipulator import ObjectIdInjector
from collection import Collection
from errors import InvalidName, CollectionInvalid, OperationFailure

ASCENDING = 1
DESCENDING = -1

# profiling levels
OFF = 0
SLOW_ONLY = 1
ALL = 2

class Database(object):
    """A Mongo database.
    """
    def __init__(self, connection, name):
        """Get a database by connection and name.

        Raises TypeError if name is not an instance of (str, unicode). Raises
        InvalidName if name is not a valid database name.

        Arguments:
        - `connection`: a connection to Mongo
        - `name`: database name
        """
        if not isinstance(name, types.StringTypes):
            raise TypeError("name must be an instance of (str, unicode)")

        self.__check_name(name)

        self.__name = unicode(name)
        self.__connection = connection
        self.__manipulators = [ObjectIdInjector(self)]

    def __check_name(self, name):
        for invalid_char in " .$/\\":
            if invalid_char in name:
                raise InvalidName("database names cannot contain the character %r" % name)
        if not name:
            raise InvalidName("database name cannot be the empty string")

    def add_son_manipulator(self, manipulator):
        """Add a new son manipulator to this database.
        """
        self.__manipulators.append(manipulator)

    def connection(self):
        """Get the database connection.
        """
        return self.__connection

    def name(self):
        """Get the database name.
        """
        return self.__name

    def __cmp__(self, other):
        if isinstance(other, Database):
            return cmp((self.__connection, self.__name), (other.__connection, other.__name))
        return NotImplemented

    def __repr__(self):
        return "Database(%r, %r)" % (self.__connection, self.__name)

    def __getattr__(self, name):
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        Arguments:
        - `name`: the name of the collection to get
        """
        return Collection(self, name)

    def __getitem__(self, name):
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        Arguments:
        - `name`: the name of the collection to get
        """
        return self.__getattr__(name)

    def _fix_incoming(self, son, collection):
        """Apply manipulators to an incoming SON object before it gets stored.

        Arguments:
        - `son`: the son object going into the database
        - `collection`: the collection the son object is being saved in
        """
        for manipulator in self.__manipulators:
            son = manipulator.transform_incoming(son, collection)
        return son

    def _fix_outgoing(self, son, collection):
        """Apply manipulators to a SON object as it comes out of the database.

        Arguments:
        - `son`: the son object coming out of the database
        - `collection`: the collection the son object was saved in
        """
        for manipulator in self.__manipulators:
            son = manipulator.transform_outgoing(son, collection)
        return son

    def _command(self, command):
        """Issue a DB command.
        """
        return self["$cmd"].find_one(command)

    def collection_names(self):
        """Get a list of all the collection names in this database.
        """
        results = self.system.namespaces.find()
        names = [r["name"] for r in results]
        names = [n[len(self.__name) + 1:] for n in names
                 if n.startswith(self.__name + ".")]
        names = [n for n in names if "$" not in n]
        return names

    def drop_collection(self, name_or_collection):
        """Drop a collection.

        Arguments:
        - `name_or_collection`: the name of a collection to drop or the
            collection object itself
        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name()

        if not isinstance(name, types.StringTypes):
            raise TypeError("name_or_collection must be an instance of (Collection, str, unicode)")

        if name not in self.collection_names():
            return

        self[name].drop_indexes() # must manually drop indexes

        result = self._command({"drop": unicode(name)})
        if result["ok"] != 1:
            raise OperationFailure("failed to drop collection: %s" % result["errmsg"])

    def validate_collection(self, name_or_collection):
        """Validate a collection.

        Returns a string of validation info. Raises CollectionInvalid if
        validation fails.
        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name()

        if not isinstance(name, types.StringTypes):
            raise TypeError("name_or_collection must be an instance of (Collection, str, unicode)")

        result = self._command({"validate": unicode(name)})
        if result["ok"] != 1:
            raise OperationFailure("failed to validate collection: %s" % result["errmsg"])

        info = result["result"]
        if info.find("exception") != -1 or info.find("corrupt") != -1:
            raise CollectionInvalid("%s invalid: %s" % (name, info))
        return info

    def profiling_level(self):
        """Get the database's current profiling level.

        Returns one of (OFF, SLOW_ONLY, ALL).
        """
        result = self._command({"profile": -1})
        if result["ok"] != 1:
            raise OperationFailure("failed to get profiling level: %s" % result["errmsg"])

        assert result["was"] >= 0 and result["was"] <= 2
        return result["was"]

    def set_profiling_level(self, level):
        """Set the database's profiling level.

        Raises ValueError if level is not one of (OFF, SLOW_ONLY, ALL).

        Arguments:
        - `level`: the profiling level to use
        """
        if not isinstance(level, types.IntType) or level < 0 or level > 2:
            raise ValueError("level must be one of (OFF, SLOW_ONLY, ALL)")

        result = self._command({"profile": level})
        if result["ok"] != 1:
            raise OperationFailure("failed to set profiling level: %s" % result["errmsg"])

    def profiling_info(self):
        """Returns a list containing current profiling information.
        """
        return list(self.system.profile.find())
