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

"""Database level operations."""

import types
try:
    import hashlib
    _md5func = hashlib.md5
except: # for Python < 2.5
    import md5
    _md5func = md5.new

from son import SON
from dbref import DBRef
from son_manipulator import ObjectIdInjector, ObjectIdShuffler
from collection import Collection
from errors import InvalidName, CollectionInvalid, OperationFailure
from code import Code
import helpers


class Database(object):
    """A Mongo database.
    """

    def __init__(self, connection, name):
        """Get a database by connection and name.

        Raises TypeError if name is not an instance of (str, unicode). Raises
        InvalidName if name is not a valid database name.

        :Parameters:
          - `connection`: a connection to Mongo
          - `name`: database name
        """
        if not isinstance(name, types.StringTypes):
            raise TypeError("name must be an instance of (str, unicode)")

        self.__check_name(name)

        self.__name = unicode(name)
        self.__connection = connection
        self.__incoming_manipulators = []
        self.__incoming_copying_manipulators = []
        self.__outgoing_manipulators = []
        self.__outgoing_copying_manipulators = []
        self.add_son_manipulator(ObjectIdInjector())

    def __check_name(self, name):
        for invalid_char in [" ", ".", "$", "/", "\\"]:
            if invalid_char in name:
                raise InvalidName("database names cannot contain the "
                                  "character %r" % invalid_char)
        if not name:
            raise InvalidName("database name cannot be the empty string")

    def add_son_manipulator(self, manipulator):
        """Add a new son manipulator to this database.

        Newly added manipulators will be applied before existing ones.

        :Parameters:
          - `manipulator`: the manipulator to add
        """
        def method_overwritten(instance, method):
            return getattr(instance, method) != getattr(super(instance.__class__, instance), method)


        if manipulator.will_copy():
            if method_overwritten(manipulator, "transform_incoming"):
                self.__incoming_copying_manipulators.insert(0, manipulator)
            if method_overwritten(manipulator, "transform_outgoing"):
                self.__outgoing_copying_manipulators.insert(0, manipulator)
        else:
            if method_overwritten(manipulator, "transform_incoming"):
                self.__incoming_manipulators.insert(0, manipulator)
            if method_overwritten(manipulator, "transform_outgoing"):
                self.__outgoing_manipulators.insert(0, manipulator)

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
            return cmp((self.__connection, self.__name),
                       (other.__connection, other.__name))
        return NotImplemented

    def __repr__(self):
        return "Database(%r, %r)" % (self.__connection, self.__name)

    def __getattr__(self, name):
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        return Collection(self, name)

    def __getitem__(self, name):
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        return self.__getattr__(name)

    def create_collection(self, name, options={}):
        """Create a new collection in this database.

        Normally collection creation is automatic. This method should only if
        you want to specify options on creation. CollectionInvalid is raised
        if the collection already exists.

        Options should be a dictionary, with any of the following options:

          - "size": desired initial size for the collection (in bytes). must be
            less than or equal to 10000000000. For capped collections this size
            is the max size of the collection.
          - "capped": if True, this is a capped collection
          - "max": maximum number of objects if capped (optional)

        :Parameters:
          - `name`: the name of the collection to create
          - `options` (optional): options to use on the new collection
        """
        if name in self.collection_names():
            raise CollectionInvalid("collection %s already exists" % name)

        return Collection(self, name, options)

    def _fix_incoming(self, son, collection):
        """Apply manipulators to an incoming SON object before it gets stored.

        :Parameters:
          - `son`: the son object going into the database
          - `collection`: the collection the son object is being saved in
        """
        for manipulator in self.__incoming_manipulators:
            son = manipulator.transform_incoming(son, collection)
        for manipulator in self.__incoming_copying_manipulators:
            son = manipulator.transform_incoming(son, collection)
        return son

    def _fix_outgoing(self, son, collection):
        """Apply manipulators to a SON object as it comes out of the database.

        :Parameters:
          - `son`: the son object coming out of the database
          - `collection`: the collection the son object was saved in
        """
        for manipulator in helpers._reversed(self.__outgoing_manipulators):
            son = manipulator.transform_outgoing(son, collection)
        for manipulator in helpers._reversed(self.__outgoing_copying_manipulators):
            son = manipulator.transform_outgoing(son, collection)
        return son

    def _command(self, command, allowable_errors=[], check=True, sock=None):
        """Issue a DB command.
        """
        result = self["$cmd"].find_one(command, _sock=sock,
                                       _must_use_master=True)

        if check and result["ok"] != 1:
            if result["errmsg"] in allowable_errors:
                return result
            raise OperationFailure("command %r failed: %s" %
                                   (command, result["errmsg"]))
        return result

    def collection_names(self):
        """Get a list of all the collection names in this database.
        """
        results = self["system.namespaces"].find(_must_use_master=True)
        names = [r["name"] for r in results]
        names = [n[len(self.__name) + 1:] for n in names
                 if n.startswith(self.__name + ".")]
        names = [n for n in names if "$" not in n]
        return names

    def drop_collection(self, name_or_collection):
        """Drop a collection.

        :Parameters:
          - `name_or_collection`: the name of a collection to drop or the
            collection object itself
        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name()

        if not isinstance(name, types.StringTypes):
            raise TypeError("name_or_collection must be an instance of "
                            "(Collection, str, unicode)")

        self.connection()._purge_index(self.name(), name)

        if name not in self.collection_names():
            return

        self._command({"drop": unicode(name)})

    def validate_collection(self, name_or_collection):
        """Validate a collection.

        Returns a string of validation info. Raises CollectionInvalid if
        validation fails.
        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name()

        if not isinstance(name, types.StringTypes):
            raise TypeError("name_or_collection must be an instance of "
                            "(Collection, str, unicode)")

        result = self._command({"validate": unicode(name)})

        info = result["result"]
        if info.find("exception") != -1 or info.find("corrupt") != -1:
            raise CollectionInvalid("%s invalid: %s" % (name, info))
        return info

    def profiling_level(self):
        """Get the database's current profiling level.

        Returns one of (:data:`~pymongo.OFF`,
        :data:`~pymongo.SLOW_ONLY`, :data:`~pymongo.ALL`).
        """
        result = self._command({"profile": -1})

        assert result["was"] >= 0 and result["was"] <= 2
        return result["was"]

    def set_profiling_level(self, level):
        """Set the database's profiling level.

        Raises :class:`ValueError` if level is not one of
        (:data:`~pymongo.OFF`, :data:`~pymongo.SLOW_ONLY`,
        :data:`~pymongo.ALL`).

        :Parameters:
          - `level`: the profiling level to use
        """
        if not isinstance(level, types.IntType) or level < 0 or level > 2:
            raise ValueError("level must be one of (OFF, SLOW_ONLY, ALL)")

        self._command({"profile": level})

    def profiling_info(self):
        """Returns a list containing current profiling information.
        """
        return list(self["system.profile"].find())

    def error(self):
        """Get a database error if one occured on the last operation.

        Return None if the last operation was error-free. Otherwise return the
        error that occurred.
        """
        error = self._command({"getlasterror": 1})
        if error.get("err", 0) is None:
            return None
        if error["err"] == "not master":
            self.__connection._reset()
        return error

    def last_status(self):
        """Get status information from the last operation.

        Returns a SON object with status information.
        """
        return self._command({"getlasterror": 1})

    def previous_error(self):
        """Get the most recent error to have occurred on this database.

        Only returns errors that have occurred since the last call to
        `Database.reset_error_history`. Returns None if no such errors have
        occurred.
        """
        error = self._command({"getpreverror": 1})
        if error.get("err", 0) is None:
            return None
        return error

    def reset_error_history(self):
        """Reset the error history of this database.

        Calls to `Database.previous_error` will only return errors that have
        occurred since the most recent call to this method.
        """
        self._command({"reseterror": 1})

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Database' object is not iterable")

    def _password_digest(self, username, password):
        """Get a password digest to use for authentication.
        """
        if not isinstance(password, types.StringTypes):
            raise TypeError("password must be an instance of (str, unicode)")
        if not isinstance(username, types.StringTypes):
            raise TypeError("username must be an instance of (str, unicode)")

        md5hash = _md5func()
        md5hash.update(username + ":mongo:" + password)
        return unicode(md5hash.hexdigest())

    def authenticate(self, name, password):
        """Authenticate to use this database.

        Once authenticated, the user has full read and write access to this
        database. Raises TypeError if either name or password is not an
        instance of (str, unicode). Authentication lasts for the life of the
        database connection, or until `Database.logout` is called.

        The "admin" database is special. Authenticating on "admin" gives access
        to *all* databases. Effectively, "admin" access means root access to
        the database.

        :Parameters:
          - `name`: the name of the user to authenticate
          - `password`: the password of the user to authenticate
        """
        if not isinstance(name, types.StringTypes):
            raise TypeError("name must be an instance of (str, unicode)")
        if not isinstance(password, types.StringTypes):
            raise TypeError("password must be an instance of (str, unicode)")

        result = self._command({"getnonce": 1})
        nonce = result["nonce"]
        digest = self._password_digest(name, password)
        md5hash = _md5func()
        md5hash.update("%s%s%s" % (nonce, unicode(name), digest))
        key = unicode(md5hash.hexdigest())
        try:
            result = self._command(SON([("authenticate", 1),
                                        ("user", unicode(name)),
                                        ("nonce", nonce),
                                        ("key", key)]))
            return True
        except OperationFailure:
            return False

    def logout(self):
        """Deauthorize use of this database for this connection.

        Note that other databases may still be authorized.
        """
        self._command({"logout": 1})

    def dereference(self, dbref):
        """Dereference a DBRef, getting the SON object it points to.

        Raises TypeError if `dbref` is not an instance of DBRef. Returns a SON
        object or None if the reference does not point to a valid object. Raises
        ValueError if `dbref` has a database specified that is different from
        the current database.

        :Parameters:
          - `dbref`: the reference
        """
        if not isinstance(dbref, DBRef):
            raise TypeError("cannot dereference a %s" % type(dbref))
        if dbref.database is not None and dbref.database != self.__name:
            raise ValueError("trying to dereference a DBRef that points to "
                             "another database (%r not %r)" % (dbref.database,
                                                               self.__name))
        return self[dbref.collection].find_one({"_id": dbref.id})

    def eval(self, code, *args):
        """Evaluate a JavaScript expression on the Mongo server.

        Useful if you need to touch a lot of data lightly; in such a scenario
        the network transfer of the data could be a bottleneck. The `code`
        argument must be a JavaScript function. Additional positional
        arguments will be passed to that function when it is run on the
        server.

        Raises TypeError if `code` is not an instance of (str, unicode,
        `Code`). Raises OperationFailure if the eval fails. Returns the result
        of the evaluation.

        :Parameters:
          - `code`: string representation of JavaScript code to be evaluated
          - `args` (optional): additional positional arguments are passed to
            the `code` being evaluated
        """
        if not isinstance(code, Code):
            code = Code(code)

        command = SON([("$eval", code), ("args", list(args))])
        result = self._command(command)
        return result.get("retval", None)

    def __call__(self, *args, **kwargs):
        """This is only here so that some API misusages are easier to debug.
        """
        raise TypeError("'Database' object is not callable. If you meant to "
                        "call the '%s' method on a 'Collection' object it is "
                        "failing because no such method exists." % self.__name)
