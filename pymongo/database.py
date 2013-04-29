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

"""Database level operations."""

from bson.binary import OLD_UUID_SUBTYPE
from bson.code import Code
from bson.dbref import DBRef
from bson.son import SON
from pymongo import auth, common, helpers
from pymongo.collection import Collection
from pymongo.errors import (CollectionInvalid,
                            InvalidName,
                            OperationFailure)
from pymongo.son_manipulator import ObjectIdInjector
from pymongo import read_preferences as rp


def _check_name(name):
    """Check if a database name is valid.
    """
    if not name:
        raise InvalidName("database name cannot be the empty string")

    for invalid_char in [" ", ".", "$", "/", "\\", "\x00"]:
        if invalid_char in name:
            raise InvalidName("database names cannot contain the "
                              "character %r" % invalid_char)


class Database(common.BaseObject):
    """A Mongo database.
    """

    def __init__(self, connection, name):
        """Get a database by connection and name.

        Raises :class:`TypeError` if `name` is not an instance of
        :class:`basestring` (:class:`str` in python 3). Raises
        :class:`~pymongo.errors.InvalidName` if `name` is not a valid
        database name.

        :Parameters:
          - `connection`: a client instance
          - `name`: database name

        .. mongodoc:: databases
        """
        super(Database,
              self).__init__(slave_okay=connection.slave_okay,
                             read_preference=connection.read_preference,
                             tag_sets=connection.tag_sets,
                             secondary_acceptable_latency_ms=(
                                 connection.secondary_acceptable_latency_ms),
                             safe=connection.safe,
                             **connection.write_concern)

        if not isinstance(name, basestring):
            raise TypeError("name must be an instance "
                            "of %s" % (basestring.__name__,))

        if name != '$external':
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
        """The client instance for this :class:`Database`.

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

    def __eq__(self, other):
        if isinstance(other, Database):
            us = (self.__connection, self.__name)
            them = (other.__connection, other.__name)
            return us == them
        return NotImplemented

    def __ne__(self, other):
        return not self == other

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
            bytes). For capped collections this size is the max
            size of the collection.
          - "capped": if True, this is a capped collection
          - "max": maximum number of objects if capped (optional)

        :Parameters:
          - `name`: the name of the collection to create
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 2.2
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
        (:class:`str` in python 3) then the command {`command`: `value`}
        will be sent. Otherwise, `command` must be an instance of
        :class:`dict` and will be sent as is.

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
          - `read_preference`: The read preference for this connection.
            See :class:`~pymongo.read_preferences.ReadPreference` for available
            options.
          - `tag_sets`: Read from replica-set members with these tags.
            To specify a priority-order for tag sets, provide a list of
            tag sets: ``[{'dc': 'ny'}, {'dc': 'la'}, {}]``. A final, empty tag
            set, ``{}``, means "read from any member that matches the mode,
            ignoring tags." ReplicaSetConnection tries each set of tags in turn
            until it finds a set of tags with at least one matching member.
          - `secondary_acceptable_latency_ms`: Any replica-set member whose
            ping time is within secondary_acceptable_latency_ms of the nearest
            member may accept reads. Default 15 milliseconds.
            **Ignored by mongos** and must be configured on the command line.
            See the localThreshold_ option for more information.
          - `**kwargs` (optional): additional keyword arguments will
            be added to the command document before it is sent

        .. note:: ``command`` ignores the ``network_timeout`` parameter.

        .. versionchanged:: 2.3
           Added `tag_sets` and `secondary_acceptable_latency_ms` options.
        .. versionchanged:: 2.2
           Added support for `as_class` - the class you want to use for
           the resulting documents
        .. versionchanged:: 1.6
           Added the `value` argument for string commands, and keyword
           arguments for additional command options.
        .. versionchanged:: 1.5
           `command` can be a string in addition to a full document.
        .. versionadded:: 1.4

        .. mongodoc:: commands
        .. _localThreshold: http://docs.mongodb.org/manual/reference/mongos/#cmdoption-mongos--localThreshold
        """

        if isinstance(command, basestring):
            command = SON([(command, value)])

        command_name = command.keys()[0].lower()
        must_use_master = kwargs.pop('_use_master', False)
        if command_name not in rp.secondary_ok_commands:
            must_use_master = True

        # Special-case: mapreduce can go to secondaries only if inline
        if command_name == 'mapreduce':
            out = command.get('out') or kwargs.get('out')
            if not isinstance(out, dict) or not out.get('inline'):
                must_use_master = True

        extra_opts = {
            'as_class': kwargs.pop('as_class', None),
            'slave_okay': kwargs.pop('slave_okay', self.slave_okay),
            '_must_use_master': must_use_master,
            '_uuid_subtype': uuid_subtype
        }

        extra_opts['read_preference'] = kwargs.pop(
            'read_preference',
            self.read_preference)
        extra_opts['tag_sets'] = kwargs.pop(
            'tag_sets',
            self.tag_sets)
        extra_opts['secondary_acceptable_latency_ms'] = kwargs.pop(
            'secondary_acceptable_latency_ms',
            self.secondary_acceptable_latency_ms)

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

    def collection_names(self, include_system_collections=True):
        """Get a list of all the collection names in this database.

        :Parameters:
          - `include_system_collections` (optional): if ``False`` list
            will not include system collections (e.g ``system.indexes``)
        """
        results = self["system.namespaces"].find(_must_use_master=True)
        names = [r["name"] for r in results]
        names = [n[len(self.__name) + 1:] for n in names
                 if n.startswith(self.__name + ".") and "$" not in n]
        if not include_system_collections:
            names = [n for n in names if not n.startswith("system.")]
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
                            "%s or Collection" % (basestring.__name__,))

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
          - `name_or_collection`: A Collection object or the name of a
            collection to validate.
          - `scandata`: Do extra checks beyond checking the overall
            structure of the collection.
          - `full`: Have the server do a more thorough scan of the
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
                            "%s or Collection" % (basestring.__name__,))

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
            for _, res in result["raw"].iteritems():
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

    def current_op(self, include_all=False):
        """Get information on operations currently running.

        :Parameters:
          - `include_all` (optional): if ``True`` also list currently
            idle operations in the result
         """
        if include_all:
            return self['$cmd.sys.inprog'].find_one({"$all": True})
        else:
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

    def set_profiling_level(self, level, slow_ms=None):
        """Set the database's profiling level.

        :Parameters:
          - `level`: Specifies a profiling level, see list of possible values
            below.
          - `slow_ms`: Optionally modify the threshold for the profile to
            consider a query or operation.  Even if the profiler is off queries
            slower than the `slow_ms` level will get written to the logs.

        Possible `level` values:

        +----------------------------+------------------------------------+
        | Level                      | Setting                            |
        +============================+====================================+
        | :data:`~pymongo.OFF`       | Off. No profiling.                 |
        +----------------------------+------------------------------------+
        | :data:`~pymongo.SLOW_ONLY` | On. Only includes slow operations. |
        +----------------------------+------------------------------------+
        | :data:`~pymongo.ALL`       | On. Includes all operations.       |
        +----------------------------+------------------------------------+

        Raises :class:`ValueError` if level is not one of
        (:data:`~pymongo.OFF`, :data:`~pymongo.SLOW_ONLY`,
        :data:`~pymongo.ALL`).

        .. mongodoc:: profiling
        """
        if not isinstance(level, int) or level < 0 or level > 2:
            raise ValueError("level must be one of (OFF, SLOW_ONLY, ALL)")

        if slow_ms is not None and not isinstance(slow_ms, int):
            raise TypeError("slow_ms must be an integer")

        if slow_ms is not None:
            self.command("profile", level, slowms=slow_ms)
        else:
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

    def add_user(self, name, password=None, read_only=None, **kwargs):
        """Create user `name` with password `password`.

        Add a new user with permissions for this :class:`Database`.

        .. note:: Will change the password if user `name` already exists.

        :Parameters:
          - `name`: the name of the user to create
          - `password` (optional): the password of the user to create. Can not
            be used with the ``userSource`` argument.
          - `read_only` (optional): if ``True`` the user will be read only
          - `**kwargs` (optional): optional fields for the user document
            (e.g. ``userSource``, ``otherDBRoles``, or ``roles``). See
            `<http://docs.mongodb.org/manual/reference/privilege-documents>`_
            for more information.

        .. note:: The use of optional keyword arguments like ``userSource``,
           ``otherDBRoles``, or ``roles`` requires MongoDB >= 2.4.0

        .. versionchanged:: 2.5
           Added kwargs support for optional fields introduced in MongoDB 2.4

        .. versionchanged:: 2.2
           Added support for read only users

        .. versionadded:: 1.4
        """

        user = self.system.users.find_one({"user": name}) or {"user": name}
        if password is not None:
            user["pwd"] = auth._password_digest(name, password)
        if read_only is not None:
            user["readOnly"] = common.validate_boolean('read_only', read_only)
        user.update(kwargs)

        try:
            self.system.users.save(user, **self._get_wc_override())
        except OperationFailure, e:
            # First admin user add fails gle in MongoDB >= 2.1.2
            # See SERVER-4225 for more information.
            if 'login' in str(e):
                pass
            else:
                raise

    def remove_user(self, name):
        """Remove user `name` from this :class:`Database`.

        User `name` will no longer have permissions to access this
        :class:`Database`.

        :Parameters:
          - `name`: the name of the user to remove

        .. versionadded:: 1.4
        """
        self.system.users.remove({"user": name}, **self._get_wc_override())

    def authenticate(self, name, password=None,
                     source=None, mechanism='MONGODB-CR', **kwargs):
        """Authenticate to use this database.

        Authentication lasts for the life of the underlying client
        instance, or until :meth:`logout` is called.

        Raises :class:`TypeError` if (required) `name`, (optional) `password`,
        or (optional) `source` is not an instance of :class:`basestring`
        (:class:`str` in python 3).

        .. note::
          - This method authenticates the current connection, and
            will also cause all new :class:`~socket.socket` connections
            in the underlying client instance to be authenticated automatically.

          - Authenticating more than once on the same database with different
            credentials is not supported. You must call :meth:`logout` before
            authenticating with new credentials.

          - When sharing a client instance between multiple threads, all
            threads will share the authentication. If you need different
            authentication profiles for different purposes you must use
            distinct client instances.

          - To get authentication to apply immediately to all
            existing sockets you may need to reset this client instance's
            sockets using :meth:`~pymongo.mongo_client.MongoClient.disconnect`.

        :Parameters:
          - `name`: the name of the user to authenticate.
          - `password` (optional): the password of the user to authenticate.
            Not used with GSSAPI or MONGODB-X509 authentication.
          - `source` (optional): the database to authenticate on. If not
            specified the current database is used.
          - `mechanism` (optional): See
            :data:`~pymongo.auth.MECHANISMS` for options.
            Defaults to MONGODB-CR (MongoDB Challenge Response protocol)
          - `gssapiServiceName` (optional): Used with the GSSAPI mechanism
            to specify the service name portion of the service principal name.
            Defaults to 'mongodb'.

        .. versionchanged:: 2.5
           Added the `source` and `mechanism` parameters. :meth:`authenticate`
           now raises a subclass of :class:`~pymongo.errors.PyMongoError` if
           authentication fails due to invalid credentials or configuration
           issues.

        .. mongodoc:: authenticate
        """
        if not isinstance(name, basestring):
            raise TypeError("name must be an instance "
                            "of %s" % (basestring.__name__,))
        if password is not None and not isinstance(password, basestring):
            raise TypeError("password must be an instance "
                            "of %s" % (basestring.__name__,))
        if source is not None and not isinstance(source, basestring):
            raise TypeError("source must be an instance "
                            "of %s" % (basestring.__name__,))
        common.validate_auth_mechanism('mechanism', mechanism)

        validated_options = {}
        for option, value in kwargs.iteritems():
            normalized, val = common.validate_auth_option(option, value)
            validated_options[normalized] = val

        credentials = auth._build_credentials_tuple(mechanism,
                                source or self.name, unicode(name),
                                password and unicode(password) or None,
                                validated_options)
        self.connection._cache_credentials(self.name, credentials)
        return True

    def logout(self):
        """Deauthorize use of this database for this client instance.

        .. note:: Other databases may still be authenticated, and other
           existing :class:`~socket.socket` connections may remain
           authenticated for this database unless you reset all sockets
           with :meth:`~pymongo.mongo_client.MongoClient.disconnect`.
        """
        # Sockets will be deauthenticated as they are used.
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
        :class:`basestring` (:class:`str` in python 3) or `Code`.
        Raises :class:`~pymongo.errors.OperationFailure` if the eval
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
        self._db.system.js.save({"_id": name, "value": Code(code)},
                                **self._db._get_wc_override())

    def __setitem__(self, name, code):
        self.__setattr__(name, code)

    def __delattr__(self, name):
        self._db.system.js.remove({"_id": name}, **self._db._get_wc_override())

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
