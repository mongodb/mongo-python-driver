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

"""Cursor class to iterate over Mongo query results."""

from bson.code import Code
from bson.son import SON
from pymongo import (helpers,
                     message,
                     ReadPreference)
from pymongo.errors import (InvalidOperation,
                            AutoReconnect)

_QUERY_OPTIONS = {
    "tailable_cursor": 2,
    "slave_okay": 4,
    "oplog_replay": 8,
    "no_timeout": 16,
    "await_data": 32,
    "exhaust": 64,
    "partial": 128}


# TODO might be cool to be able to do find().include("foo") or
# find().exclude(["bar", "baz"]) or find().slice("a", 1, 2) as an
# alternative to the fields specifier.
class Cursor(object):
    """A cursor / iterator over Mongo query results.
    """

    def __init__(self, collection, spec=None, fields=None, skip=0, limit=0,
                 timeout=True, snapshot=False, tailable=False, sort=None,
                 max_scan=None, as_class=None, slave_okay=False,
                 await_data=False, partial=False, manipulate=True,
                 read_preference=ReadPreference.PRIMARY,
                 _must_use_master=False, _uuid_subtype=None, **kwargs):
        """Create a new cursor.

        Should not be called directly by application developers - see
        :meth:`~pymongo.collection.Collection.find` instead.

        .. mongodoc:: cursors
        """
        self.__id = None

        if spec is None:
            spec = {}

        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(skip, int):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, int):
            raise TypeError("limit must be an instance of int")
        if not isinstance(timeout, bool):
            raise TypeError("timeout must be an instance of bool")
        if not isinstance(snapshot, bool):
            raise TypeError("snapshot must be an instance of bool")
        if not isinstance(tailable, bool):
            raise TypeError("tailable must be an instance of bool")
        if not isinstance(slave_okay, bool):
            raise TypeError("slave_okay must be an instance of bool")
        if not isinstance(await_data, bool):
            raise TypeError("await_data must be an instance of bool")
        if not isinstance(partial, bool):
            raise TypeError("partial must be an instance of bool")

        if fields is not None:
            if not fields:
                fields = {"_id": 1}
            if not isinstance(fields, dict):
                fields = helpers._fields_list_to_dict(fields)

        if as_class is None:
            as_class = collection.database.connection.document_class

        self.__collection = collection
        self.__spec = spec
        self.__fields = fields
        self.__skip = skip
        self.__limit = limit
        self.__batch_size = 0

        # This is ugly. People want to be able to do cursor[5:5] and
        # get an empty result set (old behavior was an
        # exception). It's hard to do that right, though, because the
        # server uses limit(0) to mean 'no limit'. So we set __empty
        # in that case and check for it when iterating. We also unset
        # it anytime we change __limit.
        self.__empty = False

        self.__timeout = timeout
        self.__tailable = tailable
        self.__await_data = tailable and await_data
        self.__partial = partial
        self.__snapshot = snapshot
        self.__ordering = sort and helpers._index_document(sort) or None
        self.__max_scan = max_scan
        self.__explain = False
        self.__hint = None
        self.__as_class = as_class
        self.__slave_okay = slave_okay
        self.__manipulate = manipulate
        self.__read_preference = read_preference
        self.__tz_aware = collection.database.connection.tz_aware
        self.__must_use_master = _must_use_master
        self.__uuid_subtype = _uuid_subtype or collection.uuid_subtype
        self.__query_flags = 0

        self.__data = []
        self.__connection_id = None
        self.__retrieved = 0
        self.__killed = False

        # this is for passing network_timeout through if it's specified
        # need to use kwargs as None is a legit value for network_timeout
        self.__kwargs = kwargs

    @property
    def collection(self):
        """The :class:`~pymongo.collection.Collection` that this
        :class:`Cursor` is iterating.

        .. versionadded:: 1.1
        """
        return self.__collection

    def __del__(self):
        if self.__id and not self.__killed:
            self.__die()

    def rewind(self):
        """Rewind this cursor to it's unevaluated state.

        Reset this cursor if it has been partially or completely evaluated.
        Any options that are present on the cursor will remain in effect.
        Future iterating performed on this cursor will cause new queries to
        be sent to the server, even if the resultant data has already been
        retrieved by this cursor.
        """
        self.__data = []
        self.__id = None
        self.__connection_id = None
        self.__retrieved = 0
        self.__killed = False

        return self

    def clone(self):
        """Get a clone of this cursor.

        Returns a new Cursor instance with options matching those that have
        been set on the current instance. The clone will be completely
        unevaluated, even if the current instance has been partially or
        completely evaluated.
        """
        copy = Cursor(self.__collection, self.__spec, self.__fields,
                      self.__skip, self.__limit, self.__timeout,
                      self.__snapshot, self.__tailable)
        copy.__ordering = self.__ordering
        copy.__explain = self.__explain
        copy.__hint = self.__hint
        copy.__batch_size = self.__batch_size
        copy.__max_scan = self.__max_scan
        copy.__as_class = self.__as_class
        copy.__slave_okay = self.__slave_okay
        copy.__await_data = self.__await_data
        copy.__partial = self.__partial
        copy.__manipulate = self.__manipulate
        copy.__read_preference = self.__read_preference
        copy.__must_use_master = self.__must_use_master
        copy.__uuid_subtype = self.__uuid_subtype
        copy.__query_flags = self.__query_flags
        copy.__kwargs = self.__kwargs
        return copy

    def __die(self):
        """Closes this cursor.
        """
        if self.__id and not self.__killed:
            connection = self.__collection.database.connection
            if self.__connection_id is not None:
                connection.close_cursor(self.__id, self.__connection_id)
            else:
                connection.close_cursor(self.__id)
        self.__killed = True

    def close(self):
        """Explicitly close / kill this cursor. Required for PyPy, Jython and
        other Python implementations that don't use reference counting
        garbage collection.
        """
        self.__die()

    def __query_spec(self):
        """Get the spec to use for a query.
        """
        operators = {}
        if self.__ordering:
            operators["$orderby"] = self.__ordering
        if self.__explain:
            operators["$explain"] = True
        if self.__hint:
            operators["$hint"] = self.__hint
        if self.__snapshot:
            operators["$snapshot"] = True
        if self.__max_scan:
            operators["$maxScan"] = self.__max_scan

        if operators:
            # Make a shallow copy so we can cleanly rewind or clone.
            spec = self.__spec.copy()
            if "$query" not in spec:
                # $query has to come first
                spec = SON({"$query": spec})
            spec.update(operators)
            return spec
        # Have to wrap with $query if "query" is the first key.
        # We can't just use $query anytime "query" is a key as
        # that breaks commands like count and find_and_modify.
        # Checking spec.keys()[0] covers the case that the spec
        # was passed as an instance of SON or OrderedDict.
        elif ("query" in self.__spec and
              (len(self.__spec) == 1 or self.__spec.keys()[0] == "query")):
                return SON({"$query": self.__spec})

        return self.__spec

    def __query_options(self):
        """Get the query options string to use for this query.
        """
        options = self.__query_flags
        if self.__tailable:
            options |= _QUERY_OPTIONS["tailable_cursor"]
        if self.__slave_okay or self.__read_preference:
            options |= _QUERY_OPTIONS["slave_okay"]
        if not self.__timeout:
            options |= _QUERY_OPTIONS["no_timeout"]
        if self.__await_data:
            options |= _QUERY_OPTIONS["await_data"]
        if self.__partial:
            options |= _QUERY_OPTIONS["partial"]
        return options

    def __check_okay_to_chain(self):
        """Check if it is okay to chain more options onto this cursor.
        """
        if self.__retrieved or self.__id is not None:
            raise InvalidOperation("cannot set options after executing query")

    def add_option(self, mask):
        """Set arbitary query flags using a bitmask.

        To set the tailable flag:
        cursor.add_option(2)
        """
        if not isinstance(mask, int):
            raise TypeError("mask must be an int")
        self.__check_okay_to_chain()

        self.__query_flags |= mask
        return self

    def remove_option(self, mask):
        """Unset arbitrary query flags using a bitmask.

        To unset the tailable flag:
        cursor.remove_option(2)
        """
        if not isinstance(mask, int):
            raise TypeError("mask must be an int")
        self.__check_okay_to_chain()

        self.__query_flags &= ~mask
        return self

    def limit(self, limit):
        """Limits the number of results to be returned by this cursor.

        Raises TypeError if limit is not an instance of int. Raises
        InvalidOperation if this cursor has already been used. The
        last `limit` applied to this cursor takes precedence. A limit
        of ``0`` is equivalent to no limit.

        :Parameters:
          - `limit`: the number of results to return

        .. mongodoc:: limit
        """
        if not isinstance(limit, int):
            raise TypeError("limit must be an int")
        self.__check_okay_to_chain()

        self.__empty = False
        self.__limit = limit
        return self

    def batch_size(self, batch_size):
        """Limits the number of documents returned in one batch. Each batch
        requires a round trip to the server. It can be adjusted to optimize
        performance and limit data transfer.

        .. note:: batch_size can not override MongoDB's internal limits on the
           amount of data it will return to the client in a single batch (i.e
           if you set batch size to 1,000,000,000, MongoDB will currently only
           return 4-16MB of results per batch).

        Raises :class:`TypeError` if `batch_size` is not an instance
        of :class:`int`. Raises :class:`ValueError` if `batch_size` is
        less than ``0``. Raises
        :class:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used. The last `batch_size`
        applied to this cursor takes precedence.

        :Parameters:
          - `batch_size`: The size of each batch of results requested.

        .. versionadded:: 1.9
        """
        if not isinstance(batch_size, int):
            raise TypeError("batch_size must be an int")
        if batch_size < 0:
            raise ValueError("batch_size must be >= 0")
        self.__check_okay_to_chain()

        self.__batch_size = batch_size == 1 and 2 or batch_size
        return self

    def skip(self, skip):
        """Skips the first `skip` results of this cursor.

        Raises TypeError if skip is not an instance of int. Raises
        InvalidOperation if this cursor has already been used. The last `skip`
        applied to this cursor takes precedence.

        :Parameters:
          - `skip`: the number of results to skip
        """
        if not isinstance(skip, (int, long)):
            raise TypeError("skip must be an int")
        self.__check_okay_to_chain()

        self.__skip = skip
        return self

    def __getitem__(self, index):
        """Get a single document or a slice of documents from this cursor.

        Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used.

        To get a single document use an integral index, e.g.::

          >>> db.test.find()[50]

        An :class:`IndexError` will be raised if the index is negative
        or greater than the amount of documents in this cursor. Any
        limit applied to this cursor will be ignored.

        To get a slice of documents use a slice index, e.g.::

          >>> db.test.find()[20:25]

        This will return this cursor with a limit of ``5`` and skip of
        ``20`` applied.  Using a slice index will override any prior
        limits or skips applied to this cursor (including those
        applied through previous calls to this method). Raises
        :class:`IndexError` when the slice has a step, a negative
        start value, or a stop value less than or equal to the start
        value.

        :Parameters:
          - `index`: An integer or slice index to be applied to this cursor
        """
        self.__check_okay_to_chain()
        self.__empty = False
        if isinstance(index, slice):
            if index.step is not None:
                raise IndexError("Cursor instances do not support slice steps")

            skip = 0
            if index.start is not None:
                if index.start < 0:
                    raise IndexError("Cursor instances do not support"
                                     "negative indices")
                skip = index.start

            if index.stop is not None:
                limit = index.stop - skip
                if limit < 0:
                    raise IndexError("stop index must be greater than start"
                                     "index for slice %r" % index)
                if limit == 0:
                    self.__empty = True
            else:
                limit = 0

            self.__skip = skip
            self.__limit = limit
            return self

        if isinstance(index, (int, long)):
            if index < 0:
                raise IndexError("Cursor instances do not support negative"
                                 "indices")
            clone = self.clone()
            clone.skip(index + self.__skip)
            clone.limit(-1)  # use a hard limit
            for doc in clone:
                return doc
            raise IndexError("no such item for Cursor instance")
        raise TypeError("index %r cannot be applied to Cursor "
                        "instances" % index)

    def max_scan(self, max_scan):
        """Limit the number of documents to scan when performing the query.

        Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used. Only the last :meth:`max_scan`
        applied to this cursor has any effect.

        :Parameters:
          - `max_scan`: the maximum number of documents to scan

        .. note:: Requires server version **>= 1.5.1**

        .. versionadded:: 1.7
        """
        self.__check_okay_to_chain()
        self.__max_scan = max_scan
        return self

    def sort(self, key_or_list, direction=None):
        """Sorts this cursor's results.

        Takes either a single key and a direction, or a list of (key,
        direction) pairs. The key(s) must be an instance of ``(str,
        unicode)``, and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`,
        :data:`~pymongo.DESCENDING`). Raises
        :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used. Only the last :meth:`sort` applied to this
        cursor has any effect.

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction)
            pairs specifying the keys to sort on
          - `direction` (optional): only used if `key_or_list` is a single
            key, if not given :data:`~pymongo.ASCENDING` is assumed
        """
        self.__check_okay_to_chain()
        keys = helpers._index_list(key_or_list, direction)
        self.__ordering = helpers._index_document(keys)
        return self

    def count(self, with_limit_and_skip=False):
        """Get the size of the results set for this query.

        Returns the number of documents in the results set for this query. Does
        not take :meth:`limit` and :meth:`skip` into account by default - set
        `with_limit_and_skip` to ``True`` if that is the desired behavior.
        Raises :class:`~pymongo.errors.OperationFailure` on a database error.

        With :class:`~pymongo.replica_set_connection.ReplicaSetConnection`
        or :class:`~pymongo.master_slave_connection.MasterSlaveConnection`,
        if `read_preference` is not :attr:`pymongo.ReadPreference.PRIMARY` or
        (deprecated) `slave_okay` is `True` the count command will be sent to
        a secondary or slave.

        :Parameters:
          - `with_limit_and_skip` (optional): take any :meth:`limit` or
            :meth:`skip` that has been applied to this cursor into account when
            getting the count

        .. note:: The `with_limit_and_skip` parameter requires server
           version **>= 1.1.4-**

        .. versionadded:: 1.1.1
           The `with_limit_and_skip` parameter.
           :meth:`~pymongo.cursor.Cursor.__len__` was deprecated in favor of
           calling :meth:`count` with `with_limit_and_skip` set to ``True``.
        """
        command = {"query": self.__spec, "fields": self.__fields}

        command['read_preference'] = self.__read_preference
        command['slave_okay'] = self.__slave_okay
        use_master = not self.__slave_okay and not self.__read_preference
        command['_use_master'] = use_master

        if with_limit_and_skip:
            if self.__limit:
                command["limit"] = self.__limit
            if self.__skip:
                command["skip"] = self.__skip

        database = self.__collection.database
        r = database.command("count", self.__collection.name,
                             allowable_errors=["ns missing"],
                             uuid_subtype = self.__uuid_subtype,
                             **command)
        if r.get("errmsg", "") == "ns missing":
            return 0
        return int(r["n"])

    def distinct(self, key):
        """Get a list of distinct values for `key` among all documents
        in the result set of this query.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring` (:class:`str` in python 3).

        With :class:`~pymongo.replica_set_connection.ReplicaSetConnection`
        or :class:`~pymongo.master_slave_connection.MasterSlaveConnection`,
        if `read_preference` is not :attr:`pymongo.ReadPreference.PRIMARY` or
        (deprecated) `slave_okay` is `True` the distinct command will be sent
        to a secondary or slave.

        :Parameters:
          - `key`: name of key for which we want to get the distinct values

        .. note:: Requires server version **>= 1.1.3+**

        .. seealso:: :meth:`pymongo.collection.Collection.distinct`

        .. versionadded:: 1.2
        """
        if not isinstance(key, basestring):
            raise TypeError("key must be an instance "
                            "of %s" % (basestring.__name__,))

        options = {"key": key}
        if self.__spec:
            options["query"] = self.__spec

        options['read_preference'] = self.__read_preference
        options['slave_okay'] = self.__slave_okay
        use_master = not self.__slave_okay and not self.__read_preference
        options['_use_master'] = use_master

        database = self.__collection.database
        return database.command("distinct",
                                self.__collection.name,
                                uuid_subtype = self.__uuid_subtype,
                                **options)["values"]

    def explain(self):
        """Returns an explain plan record for this cursor.

        .. mongodoc:: explain
        """
        c = self.clone()
        c.__explain = True

        # always use a hard limit for explains
        if c.__limit:
            c.__limit = -abs(c.__limit)
        return c.next()

    def hint(self, index):
        """Adds a 'hint', telling Mongo the proper index to use for the query.

        Judicious use of hints can greatly improve query
        performance. When doing a query on multiple fields (at least
        one of which is indexed) pass the indexed field as a hint to
        the query. Hinting will not do anything if the corresponding
        index does not exist. Raises
        :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used.

        `index` should be an index as passed to
        :meth:`~pymongo.collection.Collection.create_index`
        (e.g. ``[('field', ASCENDING)]``). If `index`
        is ``None`` any existing hints for this query are cleared. The
        last hint applied to this cursor takes precedence over all
        others.

        :Parameters:
          - `index`: index to hint on (as an index specifier)
        """
        self.__check_okay_to_chain()
        if index is None:
            self.__hint = None
            return self

        self.__hint = helpers._index_document(index)
        return self

    def where(self, code):
        """Adds a $where clause to this query.

        The `code` argument must be an instance of :class:`basestring`
        (:class:`str` in python 3) or :class:`~bson.code.Code`
        containing a JavaScript expression. This expression will be
        evaluated for each document scanned. Only those documents
        for which the expression evaluates to *true* will be returned
        as results. The keyword *this* refers to the object currently
        being scanned.

        Raises :class:`TypeError` if `code` is not an instance of
        :class:`basestring` (:class:`str` in python 3). Raises
        :class:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used. Only the last call to
        :meth:`where` applied to a :class:`Cursor` has any effect.

        :Parameters:
          - `code`: JavaScript expression to use as a filter
        """
        self.__check_okay_to_chain()
        if not isinstance(code, Code):
            code = Code(code)

        self.__spec["$where"] = code
        return self

    def __send_message(self, message):
        """Send a query or getmore message and handles the response.
        """
        db = self.__collection.database
        kwargs = {"_must_use_master": self.__must_use_master}
        kwargs["read_preference"] = self.__read_preference
        if self.__connection_id is not None:
            kwargs["_connection_to_use"] = self.__connection_id
        kwargs.update(self.__kwargs)

        try:
            response = db.connection._send_message_with_response(message,
                                                                 **kwargs)
        except AutoReconnect:
            # Don't try to send kill cursors on another socket
            # or to another server. It can cause a _pinValue
            # assertion on some server releases if we get here
            # due to a socket timeout.
            self.__killed = True
            raise

        if isinstance(response, tuple):
            (connection_id, response) = response
        else:
            connection_id = None

        self.__connection_id = connection_id

        try:
            response = helpers._unpack_response(response, self.__id,
                                                self.__as_class,
                                                self.__tz_aware)
        except AutoReconnect:
            # Don't send kill cursors to another server after a "not master"
            # error. It's completely pointless.
            self.__killed = True
            db.connection.disconnect()
            raise
        self.__id = response["cursor_id"]

        # starting from doesn't get set on getmore's for tailable cursors
        if not self.__tailable:
            assert response["starting_from"] == self.__retrieved

        self.__retrieved += response["number_returned"]
        self.__data = response["data"]

        if self.__limit and self.__id and self.__limit <= self.__retrieved:
            self.__die()

    def _refresh(self):
        """Refreshes the cursor with more data from Mongo.

        Returns the length of self.__data after refresh. Will exit early if
        self.__data is already non-empty. Raises OperationFailure when the
        cursor cannot be refreshed due to an error on the query.
        """
        if len(self.__data) or self.__killed:
            return len(self.__data)

        if self.__id is None:  # Query
            ntoreturn = self.__batch_size
            if self.__limit:
                if self.__batch_size:
                    ntoreturn = min(self.__limit, self.__batch_size)
                else:
                    ntoreturn = self.__limit
            self.__send_message(
                message.query(self.__query_options(),
                              self.__collection.full_name,
                              self.__skip, ntoreturn,
                              self.__query_spec(), self.__fields,
                              self.__uuid_subtype))
            if not self.__id:
                self.__killed = True
        elif self.__id:  # Get More
            if self.__limit:
                limit = self.__limit - self.__retrieved
                if self.__batch_size:
                    limit = min(limit, self.__batch_size)
            else:
                limit = self.__batch_size

            self.__send_message(
                message.get_more(self.__collection.full_name,
                                 limit, self.__id))

        return len(self.__data)

    @property
    def alive(self):
        """Does this cursor have the potential to return more data?

        This is mostly useful with `tailable cursors
        <http://www.mongodb.org/display/DOCS/Tailable+Cursors>`_
        since they will stop iterating even though they *may* return more
        results in the future.

        .. versionadded:: 1.5
        """
        return bool(len(self.__data) or (not self.__killed))

    @property
    def cursor_id(self):
        """Returns the id of the cursor

        Useful if you need to manage cursor ids and want to handle killing
        cursors manually using
        :meth:`~pymongo.connection.Connection.kill_cursors`

        .. versionadded:: 2.2
        """
        return self.__id

    def __iter__(self):
        return self

    def next(self):
        if self.__empty:
            raise StopIteration
        db = self.__collection.database
        if len(self.__data) or self._refresh():
            if self.__manipulate:
                return db._fix_outgoing(self.__data.pop(0), self.__collection)
            else:
                return self.__data.pop(0)
        else:
            raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__die()

