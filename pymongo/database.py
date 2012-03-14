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

"""Database level operations."""

import warnings

from bson.binary import OLD_UUID_SUBTYPE
from bson.code import Code
from bson.dbref import DBRef
from bson.son import SON
from pymongo import common, helpers
from pymongo.collection import Collection
from pymongo.errors import (CollectionInvalid,
                            InvalidName,
                            OperationFailure)
from pymongo.son_manipulator import ObjectIdInjector


def _check_name(name):
    """Check if a database name is valid.
    """
    if not name:
        raise InvalidName("database name cannot be the empty string")

    for invalid_char in [" ", ".", "$", "/", "\\"]:
        if invalid_char in name:
            raise InvalidName("database names cannot contain the "
                              "character %r" % invalid_char)


class Database(common.BaseObject):
    """A Mongo database.
    """

    def __init__(self, connection, name):
        """Get a database by connection and name.

        Raises :class:`TypeError` if `name` is not an instance of
        :class:`basestring`. Raises
        :class:`~pymongo.errors.InvalidName` if `name` is not a valid
        database name.

        :Parameters:
          - `connection`: a :class:`~pymongo.connection.Connection`
            instance
          - `name`: database name

        .. mongodoc:: databases
        """
        super(Database,
              self).__init__(slave_okay=connection.slave_okay,
                             read_preference=connection.read_preference,
                             safe=connection.safe,
                             **(connection.get_lasterror_options()))

        if not isinstance(name, basestring):
            raise TypeError("name must be an instance of basestring")

        _check_name(name)

        self.__name = unicode(name)
        self.__connection = connection

        self.__incoming_manipulators = []
        self.__incoming_copying_manipulators = []
        self.__outgoing_manipulators = []
        self.__outgoing_copying_manipulators = []
        self.add_son_manipulator(ObjectIdInjector())

    def add_son_manipulator(self, manipulator):
        """Add a new son manipulator to this database.

        Newly added manipulators will be applied before existing ones.

        :Parameters:
          - `manipulator`: the manipulator to add
        """
        def method_overwritten(instance, method):
            return getattr(instance, method) != \
                getattr(super(instance.__class__, instance), method)

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

    @property
    def system_js(self):
        """A :class:`SystemJS` helper for this :class:`Database`.

        See the documentation for :class:`SystemJS` for more details.

        .. versionadded:: 1.5
        """
        return SystemJS(self)

    @property
    def connection(self):
        """The :class:`~pymongo.connection.Connection` instance for this
        :class:`Database`.

        .. versionchanged:: 1.3
           ``connection`` is now a property rather than a method.
        """
        return self.__connection

    @property
    def name(self):
        """The name of this :class:`Database`.

        .. versionchanged:: 1.3
           ``name`` is now a property rather than a method.
        """
        return self.__name

    @property
    def incoming_manipulators(self):
        """List all incoming SON manipulators
        installed on this instance.

        .. versionadded:: 2.0
        """
        return [manipulator.__class__.__name__
                for manipulator in self.__incoming_manipulators]

    @property
    def incoming_copying_manipulators(self):
        """List all incoming SON copying manipulators
        installed on this instance.

        .. versionadded:: 2.0
        """
        return [manipulator.__class__.__name__
                for manipulator in self.__incoming_copying_manipulators]

    @property
    def outgoing_manipulators(self):
        """List all outgoing SON manipulators
        installed on this instance.

        .. versionadded:: 2.0
        """
        return [manipulator.__class__.__name__
                for manipulator in self.__outgoing_manipulators]

    @property
    def outgoing_copying_manipulators(self):
        """List all outgoing SON copying manipulators
        installed on this instance.

        .. versionadded:: 2.0
        """
        return [manipulator.__class__.__name__
                for manipulator in self.__outgoing_copying_manipulators]

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

    def create_collection(self, name, **kwargs):
        """Create a new :class:`~pymongo.collection.Collection` in this
        database.

        Normally collection creation is automatic. This method should
        only be used to specify options on
        creation. :class:`~pymongo.errors.CollectionInvalid` will be
        raised if the collection already exists.

        Options should be passed as keyword arguments to this
        method. Any of the following options are valid:

          - "size": desired initial size for the collection (in
            bytes). must be less than or equal to 10000000000. For
            capped collections this size is the max size of the
            collection.
          - "capped": if True, this is a capped collection
          - "max": maximum number of objects if capped (optional)

        :Parameters:
          - `name`: the name of the collection to create
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 2.1.1+
           Removed deprecated argument: options

        .. versionchanged:: 1.5
           deprecating `options` in favor of kwargs
        """
        opts = {"create": True}
        opts.update(kwargs)

        if name in self.collection_names():
            raise CollectionInvalid("collection %s already exists" % name)

        return Collection(self, name, **opts)

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
        for manipulator in reversed(self.__outgoing_manipulators):
            son = manipulator.transform_outgoing(son, collection)
        for manipulator in reversed(self.__outgoing_copying_manipulators):
            son = manipulator.transform_outgoing(son, collection)
        return son

    def command(self, command, value=1,
                check=True, allowable_errors=[],
                uuid_subtype=OLD_UUID_SUBTYPE, **kwargs):
        """Issue a MongoDB command.

        Send command `command` to the database and return the
        response. If `command` is an instance of :class:`basestring`
        then the command {`command`: `value`} will be sent. Otherwise,
        `command` must be an instance of :class:`dict` and will be
        sent as is.

        Any additional keyword arguments will be added to the final
        command document before it is sent.

        For example, a command like ``{buildinfo: 1}`` can be sent
        using:

        >>> db.command("buildinfo")

        For a command where the value matters, like ``{collstats:
        collection_name}`` we can do:

        >>> db.command("collstats", collection_name)

        For commands that take additional arguments we can use
        kwargs. So ``{filemd5: object_id, root: file_root}`` becomes:

        >>> db.command("filemd5", object_id, root=file_root)

        :Parameters:
          - `command`: document representing the command to be issued,
            or the name of the command (for simple commands only).

            .. note:: the order of keys in the `command` document is
               significant (the "verb" must come first), so commands
               which require multiple keys (e.g. `findandmodify`)
               should use an instance of :class:`~bson.son.SON` or
               a string and kwargs instead of a Python `dict`.

          - `value` (optional): value to use for the command verb when
            `command` is passed as a string
          - `check` (optional): check the response for errors, raising
            :class:`~pymongo.errors.OperationFailure` if there are any
          - `allowable_errors`: if `check` is ``True``, error messages
            in this list will be ignored by error-checking
          - `uuid_subtype` (optional): The BSON binary subtype to use
            for a UUID used in this command.
          - `**kwargs` (optional): additional keyword arguments will
            be added to the command document before it is sent

        .. versionchanged:: 1.6
           Added the `value` argument for string commands, and keyword
           arguments for additional command options.
        .. versionchanged:: 1.5
           `command` can be a string in addition to a full document.
        .. versionadded:: 1.4

        .. mongodoc:: commands
        """

        if isinstance(command, basestring):
            command = SON([(command, value)])

        extra_opts = {
            'read_preference': kwargs.pop('read_preference',
                                          self.read_preference),
            'slave_okay': kwargs.pop('slave_okay', self.slave_okay),
            '_must_use_master': kwargs.pop('_use_master', True),
            '_is_command': True,
            '_uuid_subtype': uuid_subtype
        }

        fields = kwargs.get('fields')
        if fields is not None and not isinstance(fields, dict):
            kwargs['fields'] = helpers._fields_list_to_dict(fields)

        command.update(kwargs)

        result = self["$cmd"].find_one(command, **extra_opts)

        if check:
            msg = "command %s failed: %%s" % repr(command).replace("%", "%%")
            helpers._check_command_response(result, self.connection.disconnect,
                                            msg, allowable_errors)

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
            name = name.name

        if not isinstance(name, basestring):
            raise TypeError("name_or_collection must be an instance of "
                            "(Collection, str, unicode)")

        self.__connection._purge_index(self.__name, name)

        self.command("drop", unicode(name), allowable_errors=["ns not found"])

    def validate_collection(self, name_or_collection,
                            scandata=False, full=False):
        """Validate a collection.

        Returns a dict of validation info. Raises CollectionInvalid if
        validation fails.

        With MongoDB < 1.9 the result dict will include a `result` key
        with a string value that represents the validation results. With
        MongoDB >= 1.9 the `result` key no longer exists and the results
        are split into individual fields in the result dict.

        :Parameters:
            `name_or_collection`: A Collection object or the name of a
                                  collection to validate.
            `scandata`: Do extra checks beyond checking the overall
                        structure of the collection.
            `full`: Have the server do a more thorough scan of the
                    collection. Use with `scandata` for a thorough scan
                    of the structure of the collection and the individual
                    documents. Ignored in MongoDB versions before 1.9.

        .. versionchanged:: 1.11
           validate_collection previously returned a string.
        .. versionadded:: 1.11
           Added `scandata` and `full` options.
        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name

        if not isinstance(name, basestring):
            raise TypeError("name_or_collection must be an instance of "
                            "(Collection, str, unicode)")

        result = self.command("validate", unicode(name),
                              scandata=scandata, full=full)

        valid = True
        # Pre 1.9 results
        if "result" in result:
            info = result["result"]
            if info.find("exception") != -1 or info.find("corrupt") != -1:
                raise CollectionInvalid("%s invalid: %s" % (name, info))
        # Sharded results
        elif "raw" in result:
            for repl, res in result["raw"].iteritems():
                if "result" in res:
                    info = res["result"]
                    if (info.find("exception") != -1 or
                        info.find("corrupt") != -1):
                        raise CollectionInvalid("%s invalid: "
                                                "%s" % (name, info))
                elif not res.get("valid", False):
                    valid = False
                    break
        # Post 1.9 non-sharded results.
        elif not result.get("valid", False):
            valid = False

        if not valid:
            raise CollectionInvalid("%s invalid: %r" % (name, result))

        return result

    def current_op(self):
        """Get information on operations currently running.
        """
        return self['$cmd.sys.inprog'].find_one()

    def profiling_level(self):
        """Get the database's current profiling level.

        Returns one of (:data:`~pymongo.OFF`,
        :data:`~pymongo.SLOW_ONLY`, :data:`~pymongo.ALL`).

        .. mongodoc:: profiling
        """
        result = self.command("profile", -1)

        assert result["was"] >= 0 and result["was"] <= 2
        return result["was"]

    def set_profiling_level(self, level):
        """Set the database's profiling level.

        Raises :class:`ValueError` if level is not one of
        (:data:`~pymongo.OFF`, :data:`~pymongo.SLOW_ONLY`,
        :data:`~pymongo.ALL`).

        :Parameters:
          - `level`: the profiling level to use

        .. mongodoc:: profiling
        """
        if not isinstance(level, int) or level < 0 or level > 2:
            raise ValueError("level must be one of (OFF, SLOW_ONLY, ALL)")

        self.command("profile", level)

    def profiling_info(self):
        """Returns a list containing current profiling information.

        .. mongodoc:: profiling
        """
        return list(self["system.profile"].find())

    def error(self):
        """Get a database error if one occured on the last operation.

        Return None if the last operation was error-free. Otherwise return the
        error that occurred.
        """
        error = self.command("getlasterror")
        error_msg = error.get("err", "")
        if error_msg is None:
            return None
        if error_msg.startswith("not master"):
            self.__connection.disconnect()
        return error

    def last_status(self):
        """Get status information from the last operation.

        Returns a SON object with status information.
        """
        return self.command("getlasterror")

    def previous_error(self):
        """Get the most recent error to have occurred on this database.

        Only returns errors that have occurred since the last call to
        `Database.reset_error_history`. Returns None if no such errors have
        occurred.
        """
        error = self.command("getpreverror")
        if error.get("err", 0) is None:
            return None
        return error

    def reset_error_history(self):
        """Reset the error history of this database.

        Calls to `Database.previous_error` will only return errors that have
        occurred since the most recent call to this method.
        """
        self.command("reseterror")

    def __iter__(self):
        return self

    def next(self):
        raise TypeError("'Database' object is not iterable")

    def add_user(self, name, password, read_only=False):
        """Create user `name` with password `password`.

        Add a new user with permissions for this :class:`Database`.

        .. note:: Will change the password if user `name` already exists.

        :Parameters:
          - `name`: the name of the user to create
          - `password`: the password of the user to create
          - `read_only` (optional): if ``True`` it will make user read only

        .. versionchanged:: 2.1.1+
           Added support for read only users

        .. versionadded:: 1.4
        """
        pwd = helpers._password_digest(name, password)
        self.system.users.update({"user": name},
                                 {"user": name,
                                  "pwd": pwd,
                                  "readOnly": read_only},
                                 upsert=True, safe=True)

    def remove_user(self, name):
        """Remove user `name` from this :class:`Database`.

        User `name` will no longer have permissions to access this
        :class:`Database`.

        :Parameters:
          - `name`: the name of the user to remove

        .. versionadded:: 1.4
        """
        self.system.users.remove({"user": name}, safe=True)

    def authenticate(self, name, password):
        """Authenticate to use this database.

        Once authenticated, the user has full read and write access to
        this database. Raises :class:`TypeError` if either `name` or
        `password` is not an instance of ``(str,
        unicode)``. Authentication lasts for the life of the underlying
        :class:`~pymongo.connection.Connection`, or until :meth:`logout`
        is called.

        The "admin" database is special. Authenticating on "admin"
        gives access to *all* databases. Effectively, "admin" access
        means root access to the database.

        .. note::
          This method authenticates the current connection, and
          will also cause all new :class:`~socket.socket` connections
          in the underlying :class:`~pymongo.connection.Connection` to
          be authenticated automatically.

         - When sharing a :class:`~pymongo.connection.Connection`
           between multiple threads, all threads will share the
           authentication. If you need different authentication profiles
           for different purposes (e.g. admin users) you must use
           distinct instances of :class:`~pymongo.connection.Connection`.

         - To get authentication to apply immediately to all
           existing sockets you may need to reset this Connection's
           sockets using :meth:`~pymongo.connection.Connection.disconnect`.

        .. warning::

          Currently, calls to
          :meth:`~pymongo.connection.Connection.end_request` will
          lead to unpredictable behavior in combination with
          auth. The :class:`~socket.socket` owned by the calling
          thread will be returned to the pool, so whichever thread
          uses that :class:`~socket.socket` next will have whatever
          permissions were granted to the calling thread.

        :Parameters:
          - `name`: the name of the user to authenticate
          - `password`: the password of the user to authenticate

        .. mongodoc:: authenticate
        """
        if not isinstance(name, basestring):
            raise TypeError("name must be an instance of basestring")
        if not isinstance(password, basestring):
            raise TypeError("password must be an instance of basestring")

        in_request = self.connection.in_request()
        try:
            if not in_request:
                self.connection.start_request()

            nonce = self.command("getnonce")["nonce"]
            key = helpers._auth_key(nonce, name, password)
            try:
                self.command("authenticate", user=unicode(name),
                             nonce=nonce, key=key)
                self.connection._cache_credentials(self.name,
                                                   unicode(name),
                                                   unicode(password))
                return True
            except OperationFailure:
                return False
        finally:
            if not in_request:
                self.connection.end_request()

    def logout(self):
        """Deauthorize use of this database for this connection
        and future connections.

        .. note:: Other databases may still be authenticated, and other
           existing :class:`~socket.socket` connections may remain
           authenticated for this database unless you reset all sockets
           with :meth:`~pymongo.connection.Connection.disconnect`.
        """
        self.command("logout")
        self.connection._purge_credentials(self.name)

    def dereference(self, dbref):
        """Dereference a :class:`~bson.dbref.DBRef`, getting the
        document it points to.

        Raises :class:`TypeError` if `dbref` is not an instance of
        :class:`~bson.dbref.DBRef`. Returns a document, or ``None`` if
        the reference does not point to a valid document.  Raises
        :class:`ValueError` if `dbref` has a database specified that
        is different from the current database.

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
        """Evaluate a JavaScript expression in MongoDB.

        Useful if you need to touch a lot of data lightly; in such a
        scenario the network transfer of the data could be a
        bottleneck. The `code` argument must be a JavaScript
        function. Additional positional arguments will be passed to
        that function when it is run on the server.

        Raises :class:`TypeError` if `code` is not an instance of
        (str, unicode, `Code`). Raises
        :class:`~pymongo.errors.OperationFailure` if the eval
        fails. Returns the result of the evaluation.

        :Parameters:
          - `code`: string representation of JavaScript code to be
            evaluated
          - `args` (optional): additional positional arguments are
            passed to the `code` being evaluated
        """
        if not isinstance(code, Code):
            code = Code(code)

        result = self.command("$eval", code, args=args)
        return result.get("retval", None)

    def __call__(self, *args, **kwargs):
        """This is only here so that some API misusages are easier to debug.
        """
        raise TypeError("'Database' object is not callable. If you meant to "
                        "call the '%s' method on a '%s' object it is "
                        "failing because no such method exists." % (
                            self.__name, self.__connection.__class__.__name__))


class SystemJS(object):
    """Helper class for dealing with stored JavaScript.
    """

    def __init__(self, database):
        """Get a system js helper for the database `database`.

        An instance of :class:`SystemJS` can be created with an instance
        of :class:`Database` through :attr:`Database.system_js`,
        manual instantiation of this class should not be necessary.

        :class:`SystemJS` instances allow for easy manipulation and
        access to server-side JavaScript:

        .. doctest::

          >>> db.system_js.add1 = "function (x) { return x + 1; }"
          >>> db.system.js.find({"_id": "add1"}).count()
          1
          >>> db.system_js.add1(5)
          6.0
          >>> del db.system_js.add1
          >>> db.system.js.find({"_id": "add1"}).count()
          0

        .. note:: Requires server version **>= 1.1.1**

        .. versionadded:: 1.5
        """
        # can't just assign it since we've overridden __setattr__
        object.__setattr__(self, "_db", database)

    def __setattr__(self, name, code):
        self._db.system.js.save({"_id": name, "value": Code(code)}, safe=True)

    def __setitem__(self, name, code):
        self.__setattr__(name, code)

    def __delattr__(self, name):
        self._db.system.js.remove({"_id": name}, safe=True)

    def __delitem__(self, name):
        self.__delattr__(name)

    def __getattr__(self, name):
        return lambda *args: self._db.eval(Code("function() { "
                                                "return this[name].apply("
                                                "this, arguments); }",
                                                scope={'name': name}), *args)

    def __getitem__(self, name):
        return self.__getattr__(name)

    def list(self):
        """Get a list of the names of the functions stored in this database.

        .. versionadded:: 1.9
        """
        return [x["_id"] for x in self._db.system.js.find(fields=["_id"])]
