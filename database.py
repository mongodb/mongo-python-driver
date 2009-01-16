"""Database level operations."""

import types

from collection import Collection
from errors import InvalidName

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

    def __check_name(self, name):
        for invalid_char in " .$/\\":
            if invalid_char in name:
                raise InvalidName("database names cannot contain the character %r" % name)
        if not name:
            raise InvalidName("database name cannot be the empty string")

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

    def collection_names(self):
        """Get a list of all the collection names in this database.
        """
        raise Exception("unimplemented")
