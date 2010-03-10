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

"""Cursor class to iterate over Mongo query results."""

import struct
import warnings

from pymongo import (helpers,
                     message)
from pymongo.code import Code
from pymongo.errors import (InvalidOperation,
                            AutoReconnect)
from pymongo.son import SON

_QUERY_OPTIONS = {
    "tailable_cursor": 2,
    "slave_okay": 4,
    "oplog_replay": 8,
    "no_timeout": 16
}


class Cursor(object):
    """A cursor / iterator over Mongo query results.
    """

    def __init__(self, collection, spec, fields, skip, limit, slave_okay,
                 timeout, tailable, snapshot=False,
                 _sock=None, _must_use_master=False, _is_command=False):
        """Create a new cursor.

        Should not be called directly by application developers.

        .. mongodoc:: cursors
        """
        self.__collection = collection
        self.__spec = spec
        self.__fields = fields
        self.__skip = skip
        self.__limit = limit
        self.__slave_okay = slave_okay
        self.__timeout = timeout
        self.__tailable = tailable
        self.__snapshot = snapshot
        self.__ordering = None
        self.__explain = False
        self.__hint = None
        self.__socket = _sock
        self.__must_use_master = _must_use_master
        self.__is_command = _is_command

        self.__data = []
        self.__id = None
        self.__connection_id = None
        self.__retrieved = 0
        self.__killed = False

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
                      self.__skip, self.__limit, self.__slave_okay,
                      self.__timeout, self.__tailable, self.__snapshot)
        copy.__ordering = self.__ordering
        copy.__explain = self.__explain
        copy.__hint = self.__hint
        copy.__socket = self.__socket
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

    def __query_spec(self):
        """Get the spec to use for a query.
        """
        if self.__is_command or "$query" in self.__spec:
            return self.__spec
        spec = SON({"$query": self.__spec})
        if self.__ordering:
            spec["$orderby"] = self.__ordering
        if self.__explain:
            spec["$explain"] = True
        if self.__hint:
            spec["$hint"] = self.__hint
        if self.__snapshot:
            spec["$snapshot"] = True
        return spec

    def __query_options(self):
        """Get the query options string to use for this query.
        """
        options = 0
        if self.__tailable:
            options |= _QUERY_OPTIONS["tailable_cursor"]
        if self.__slave_okay:
            options |= _QUERY_OPTIONS["slave_okay"]
        if not self.__timeout:
            options |= _QUERY_OPTIONS["no_timeout"]
        return options

    def __check_okay_to_chain(self):
        """Check if it is okay to chain more options onto this cursor.
        """
        if self.__retrieved or self.__id is not None:
            raise InvalidOperation("cannot set options after executing query")

    def limit(self, limit):
        """Limits the number of results to be returned by this cursor.

        Raises TypeError if limit is not an instance of int. Raises
        InvalidOperation if this cursor has already been used. The last `limit`
        applied to this cursor takes precedence.

        :Parameters:
          - `limit`: the number of results to return

        .. mongodoc:: limit
        """
        if not isinstance(limit, int):
            raise TypeError("limit must be an int")
        self.__check_okay_to_chain()

        self.__limit = limit
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
        if isinstance(index, slice):
            if index.step is not None:
                raise IndexError("Cursor instances do not support slice steps")

            skip = 0
            if index.start is not None:
                if index.start < 0:
                    raise IndexError("Cursor instances do not support negative indices")
                skip = index.start

            if index.stop is not None:
                limit = index.stop - skip
                if limit <= 0:
                    raise IndexError("stop index must be greater than start index for slice %r" % index)
            else:
                limit = 0

            self.__skip = skip
            self.__limit = limit
            return self

        if isinstance(index, (int, long)):
            if index < 0:
                raise IndexError("Cursor instances do not support negative indices")
            clone = self.clone()
            clone.skip(index + self.__skip)
            clone.limit(-1) # use a hard limit
            for doc in clone:
                return doc
            raise IndexError("no such item for Cursor instance")
        raise TypeError("index %r cannot be applied to Cursor instances" % index)

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
        command = SON([("count", self.__collection.name),
                       ("query", self.__spec),
                       ("fields", self.__fields)])

        if with_limit_and_skip:
            if self.__limit:
                command["limit"] = self.__limit
            if self.__skip:
                command["skip"] = self.__skip

        response = self.__collection.database.command(command, allowable_errors=["ns missing"])
        if response.get("errmsg", "") == "ns missing":
            return 0
        return int(response["n"])

    def distinct(self, key):
        """Get a list of distinct values for `key` among all documents
        in the result set of this query.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring`.

        :Parameters:
          - `key`: name of key for which we want to get the distinct values

        .. note:: Requires server version **>= 1.1.3+**

        .. seealso:: :meth:`pymongo.collection.Collection.distinct`

        .. versionadded:: 1.2
        """
        if not isinstance(key, basestring):
            raise TypeError("key must be an instance of basestring")

        command = SON([("distinct", self.__collection.name), ("key", key)])

        if self.__spec:
            command["query"] = self.__spec

        return self.__collection.database.command(command)["values"]

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
        or :class:`~pymongo.code.Code` containing a JavaScript
        expression. This expression will be evaluated for each
        document scanned. Only those documents for which the
        expression evaluates to *true* will be returned as
        results. The keyword *this* refers to the object currently
        being scanned.

        Raises :class:`TypeError` if `code` is not an instance of
        :class:`basestring`. Raises
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
        kwargs = {"_sock": self.__socket,
                  "_must_use_master": self.__must_use_master}
        if self.__connection_id is not None:
            kwargs["_connection_to_use"] = self.__connection_id

        response = db.connection._send_message_with_response(message,
                                                             **kwargs)

        if isinstance(response, tuple):
            (connection_id, response) = response
        else:
            connection_id = None

        self.__connection_id = connection_id

        try:
            response = helpers._unpack_response(response, self.__id)
        except AutoReconnect:
            db.connection._reset()
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

        if self.__id is None:
            # Query
            self.__send_message(
                message.query(self.__query_options(),
                              self.__collection.full_name,
                              self.__skip, self.__limit,
                              self.__query_spec(), self.__fields))
            if not self.__id:
                self.__killed = True
        elif self.__id:
            # Get More
            limit = 0
            if self.__limit:
                if self.__limit > self.__retrieved:
                    limit = self.__limit - self.__retrieved
                else:
                    self.__killed = True
                    return 0

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

    def __iter__(self):
        return self

    def next(self):
        db = self.__collection.database
        if len(self.__data) or self._refresh():
            next = db._fix_outgoing(self.__data.pop(0), self.__collection)
        else:
            raise StopIteration
        return next
