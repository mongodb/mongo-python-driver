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
import copy
from collections import deque

from bson import RE_TYPE
from bson.code import Code
from bson.son import SON
from pymongo import helpers, message, read_preferences
from pymongo.read_preferences import ReadPreference, secondary_ok_commands
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


# This has to be an old style class due to
# http://bugs.jython.org/issue1057
class _SocketManager:
    """Used with exhaust cursors to ensure the socket is returned.
    """
    def __init__(self, sock, pool):
        self.sock = sock
        self.pool = pool
        self.__closed = False

    def __del__(self):
        self.close()

    def close(self):
        """Return this instance's socket to the connection pool.
        """
        if not self.__closed:
            self.__closed = True
            self.pool.maybe_return_socket(self.sock)
            self.sock, self.pool = None, None


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
                 read_preference=ReadPreference.PRIMARY, tag_sets=[{}],
                 secondary_acceptable_latency_ms=None, exhaust=False,
                 _must_use_master=False, _uuid_subtype=None,
                 _first_batch=None, _cursor_id=None,
                 **kwargs):
        """Create a new cursor.

        Should not be called directly by application developers - see
        :meth:`~pymongo.collection.Collection.find` instead.

        .. mongodoc:: cursors
        """
        self.__id = _cursor_id
        self.__is_command_cursor = _cursor_id is not None

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
        if not isinstance(exhaust, bool):
            raise TypeError("exhaust must be an instance of bool")

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

        # Exhaust cursor support
        if self.__collection.database.connection.is_mongos and exhaust:
            raise InvalidOperation('Exhaust cursors are '
                                   'not supported by mongos')
        if limit and exhaust:
            raise InvalidOperation("Can't use limit and exhaust together.")
        self.__exhaust = exhaust
        self.__exhaust_mgr = None

        # This is ugly. People want to be able to do cursor[5:5] and
        # get an empty result set (old behavior was an
        # exception). It's hard to do that right, though, because the
        # server uses limit(0) to mean 'no limit'. So we set __empty
        # in that case and check for it when iterating. We also unset
        # it anytime we change __limit.
        self.__empty = False

        self.__snapshot = snapshot
        self.__ordering = sort and helpers._index_document(sort) or None
        self.__max_scan = max_scan
        self.__explain = False
        self.__hint = None
        self.__as_class = as_class
        self.__slave_okay = slave_okay
        self.__manipulate = manipulate
        self.__read_preference = read_preference
        self.__tag_sets = tag_sets
        self.__secondary_acceptable_latency_ms = secondary_acceptable_latency_ms
        self.__tz_aware = collection.database.connection.tz_aware
        self.__must_use_master = _must_use_master
        self.__uuid_subtype = _uuid_subtype or collection.uuid_subtype

        self.__data = deque(_first_batch or [])
        self.__connection_id = None
        self.__retrieved = 0
        self.__killed = False

        self.__query_flags = 0
        if tailable:
            self.__query_flags |= _QUERY_OPTIONS["tailable_cursor"]
        if not timeout:
            self.__query_flags |= _QUERY_OPTIONS["no_timeout"]
        if tailable and await_data:
            self.__query_flags |= _QUERY_OPTIONS["await_data"]
        if exhaust:
            self.__query_flags |= _QUERY_OPTIONS["exhaust"]
        if partial:
            self.__query_flags |= _QUERY_OPTIONS["partial"]

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
        """Rewind this cursor to its unevaluated state.

        Reset this cursor if it has been partially or completely evaluated.
        Any options that are present on the cursor will remain in effect.
        Future iterating performed on this cursor will cause new queries to
        be sent to the server, even if the resultant data has already been
        retrieved by this cursor.
        """
        self.__check_not_command_cursor('rewind')
        self.__data = deque()
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
        return self.__clone(True)

    def __clone(self, deepcopy=True):
        self.__check_not_command_cursor('clone')
        clone = Cursor(self.__collection)
        values_to_clone = ("spec", "fields", "skip", "limit",
                           "snapshot", "ordering", "explain", "hint",
                           "batch_size", "max_scan", "as_class", "slave_okay",
                           "manipulate", "read_preference", "tag_sets",
                           "secondary_acceptable_latency_ms",
                           "must_use_master", "uuid_subtype", "query_flags",
                           "kwargs")
        data = dict((k, v) for k, v in self.__dict__.iteritems()
                    if k.startswith('_Cursor__') and k[9:] in values_to_clone)
        if deepcopy:
            data = self.__deepcopy(data)
        clone.__dict__.update(data)
        return clone

    def __die(self):
        """Closes this cursor.
        """
        if self.__id and not self.__killed:
            if self.__exhaust and self.__exhaust_mgr:
                # If this is an exhaust cursor and we haven't completely
                # exhausted the result set we *must* close the socket
                # to stop the server from sending more data.
                self.__exhaust_mgr.sock.close()
            else:
                connection = self.__collection.database.connection
                if self.__connection_id is not None:
                    connection.close_cursor(self.__id, self.__connection_id)
                else:
                    connection.close_cursor(self.__id)
        if self.__exhaust and self.__exhaust_mgr:
            self.__exhaust_mgr.close()
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
        # Only set $readPreference if it's something other than
        # PRIMARY to avoid problems with mongos versions that
        # don't support read preferences.
        if (self.__collection.database.connection.is_mongos and
            self.__read_preference != ReadPreference.PRIMARY):

            has_tags = self.__tag_sets and self.__tag_sets != [{}]

            # For maximum backwards compatibility, don't set $readPreference
            # for SECONDARY_PREFERRED unless tags are in use. Just rely on
            # the slaveOkay bit (set automatically if read preference is not
            # PRIMARY), which has the same behavior.
            if (self.__read_preference != ReadPreference.SECONDARY_PREFERRED or
                has_tags):

                read_pref = {
                    'mode': read_preferences.mongos_mode(self.__read_preference)
                }
                if has_tags:
                    read_pref['tags'] = self.__tag_sets

                operators['$readPreference'] = read_pref

        if operators:
            # Make a shallow copy so we can cleanly rewind or clone.
            spec = self.__spec.copy()

            # Only commands that can be run on secondaries should have any
            # operators added to the spec.  Command queries can be issued
            # by db.command or calling find_one on $cmd directly
            if self.collection.name == "$cmd":
                # Don't change commands that can't be sent to secondaries
                command_name = spec and spec.keys()[0].lower() or ""
                if command_name not in secondary_ok_commands:
                    return spec
                elif command_name == 'mapreduce':
                    # mapreduce shouldn't be changed if its not inline
                    out = spec.get('out')
                    if not isinstance(out, dict) or not out.get('inline'):
                        return spec

            # White-listed commands must be wrapped in $query.
            if "$query" not in spec:
                # $query has to come first
                spec = SON([("$query", spec)])

            if not isinstance(spec, SON):
                # Ensure the spec is SON. As order is important this will
                # ensure its set before merging in any extra operators.
                spec = SON(spec)

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
        if (self.__slave_okay
            or self.__read_preference != ReadPreference.PRIMARY
        ):
            options |= _QUERY_OPTIONS["slave_okay"]
        return options

    def __check_okay_to_chain(self):
        """Check if it is okay to chain more options onto this cursor.
        """
        if self.__retrieved or self.__id is not None:
            raise InvalidOperation("cannot set options after executing query")

    def __check_not_command_cursor(self, method_name):
        """Check if calling a method on this cursor is valid.
        """
        if self.__is_command_cursor:
            raise InvalidOperation(
                "cannot call %s on a command cursor" % method_name)

    def add_option(self, mask):
        """Set arbitary query flags using a bitmask.

        To set the tailable flag:
        cursor.add_option(2)
        """
        if not isinstance(mask, int):
            raise TypeError("mask must be an int")
        self.__check_okay_to_chain()

        if mask & _QUERY_OPTIONS["slave_okay"]:
            self.__slave_okay = True
        if mask & _QUERY_OPTIONS["exhaust"]:
            if self.__limit:
                raise InvalidOperation("Can't use limit and exhaust together.")
            if self.__collection.database.connection.is_mongos:
                raise InvalidOperation('Exhaust cursors are '
                                       'not supported by mongos')
            self.__exhaust = True

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

        if mask & _QUERY_OPTIONS["slave_okay"]:
            self.__slave_okay = False
        if mask & _QUERY_OPTIONS["exhaust"]:
            self.__exhaust = False

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
        if self.__exhaust:
            raise InvalidOperation("Can't use limit and exhaust together.")
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
        limit previously applied to this cursor will be ignored.

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

        With :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`
        or :class:`~pymongo.master_slave_connection.MasterSlaveConnection`,
        if `read_preference` is not
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY` or
        :attr:`pymongo.read_preferences.ReadPreference.PRIMARY_PREFERRED`, or
        (deprecated) `slave_okay` is `True`, the count command will be sent to
        a secondary or slave.

        :Parameters:
          - `with_limit_and_skip` (optional): take any :meth:`limit` or
            :meth:`skip` that has been applied to this cursor into account when
            getting the count

        .. note:: The `with_limit_and_skip` parameter requires server
           version **>= 1.1.4-**

        .. note:: ``count`` ignores ``network_timeout``. For example, the
          timeout is ignored in the following code::

            collection.find({}, network_timeout=1).count()

        .. versionadded:: 1.1.1
           The `with_limit_and_skip` parameter.
           :meth:`~pymongo.cursor.Cursor.__len__` was deprecated in favor of
           calling :meth:`count` with `with_limit_and_skip` set to ``True``.
        """
        self.__check_not_command_cursor('count')
        command = {"query": self.__spec, "fields": self.__fields}

        command['read_preference'] = self.__read_preference
        command['tag_sets'] = self.__tag_sets
        command['secondary_acceptable_latency_ms'] = (
            self.__secondary_acceptable_latency_ms)
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
                             uuid_subtype=self.__uuid_subtype,
                             **command)
        if r.get("errmsg", "") == "ns missing":
            return 0
        return int(r["n"])

    def distinct(self, key):
        """Get a list of distinct values for `key` among all documents
        in the result set of this query.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring` (:class:`str` in python 3).

        With :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`
        or :class:`~pymongo.master_slave_connection.MasterSlaveConnection`,
        if `read_preference` is
        not :attr:`pymongo.read_preferences.ReadPreference.PRIMARY` or
        (deprecated) `slave_okay` is `True` the distinct command will be sent
        to a secondary or slave.

        :Parameters:
          - `key`: name of key for which we want to get the distinct values

        .. note:: Requires server version **>= 1.1.3+**

        .. seealso:: :meth:`pymongo.collection.Collection.distinct`

        .. versionadded:: 1.2
        """
        self.__check_not_command_cursor('distinct')
        if not isinstance(key, basestring):
            raise TypeError("key must be an instance "
                            "of %s" % (basestring.__name__,))

        options = {"key": key}
        if self.__spec:
            options["query"] = self.__spec

        options['read_preference'] = self.__read_preference
        options['tag_sets'] = self.__tag_sets
        options['secondary_acceptable_latency_ms'] = (
            self.__secondary_acceptable_latency_ms)
        options['slave_okay'] = self.__slave_okay
        use_master = not self.__slave_okay and not self.__read_preference
        options['_use_master'] = use_master

        database = self.__collection.database
        return database.command("distinct",
                                self.__collection.name,
                                uuid_subtype=self.__uuid_subtype,
                                **options)["values"]

    def explain(self):
        """Returns an explain plan record for this cursor.

        .. mongodoc:: explain
        """
        self.__check_not_command_cursor('explain')
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

        If message is ``None`` this is an exhaust cursor, which reads
        the next result batch off the exhaust socket instead of
        sending getMore messages to the server.
        """
        client = self.__collection.database.connection

        if message:
            kwargs = {"_must_use_master": self.__must_use_master}
            kwargs["read_preference"] = self.__read_preference
            kwargs["tag_sets"] = self.__tag_sets
            kwargs["secondary_acceptable_latency_ms"] = (
                self.__secondary_acceptable_latency_ms)
            kwargs['exhaust'] = self.__exhaust
            if self.__connection_id is not None:
                kwargs["_connection_to_use"] = self.__connection_id
            kwargs.update(self.__kwargs)

            try:
                res = client._send_message_with_response(message, **kwargs)
                self.__connection_id, (response, sock, pool) = res
                if self.__exhaust:
                    self.__exhaust_mgr = _SocketManager(sock, pool)
            except AutoReconnect:
                # Don't try to send kill cursors on another socket
                # or to another server. It can cause a _pinValue
                # assertion on some server releases if we get here
                # due to a socket timeout.
                self.__killed = True
                raise
        else: # exhaust cursor - no getMore message
            response = client._exhaust_next(self.__exhaust_mgr.sock)

        try:
            response = helpers._unpack_response(response, self.__id,
                                                self.__as_class,
                                                self.__tz_aware,
                                                self.__uuid_subtype)
        except AutoReconnect:
            # Don't send kill cursors to another server after a "not master"
            # error. It's completely pointless.
            self.__killed = True
            client.disconnect()
            raise
        self.__id = response["cursor_id"]

        # starting from doesn't get set on getmore's for tailable cursors
        if not (self.__query_flags & _QUERY_OPTIONS["tailable_cursor"]):
            assert response["starting_from"] == self.__retrieved, (
                "Result batch started from %s, expected %s" % (
                    response['starting_from'], self.__retrieved))

        self.__retrieved += response["number_returned"]
        self.__data = deque(response["data"])

        if self.__limit and self.__id and self.__limit <= self.__retrieved:
            self.__die()

        # Don't wait for garbage collection to call __del__, return the
        # socket to the pool now.
        if self.__exhaust and self.__id == 0:
            self.__exhaust_mgr.close()

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

            # Exhaust cursors don't send getMore messages.
            if self.__exhaust:
                self.__send_message(None)
            else:
                self.__send_message(
                    message.get_more(self.__collection.full_name,
                                     limit, self.__id))

        else:  # Cursor id is zero nothing else to return
            self.__killed = True

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
        :meth:`~pymongo.mongo_client.MongoClient.kill_cursors`

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
                return db._fix_outgoing(self.__data.popleft(),
                                        self.__collection)
            else:
                return self.__data.popleft()
        else:
            raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__die()

    def __copy__(self):
        """Support function for `copy.copy()`.

        .. versionadded:: 2.4
        """
        return self.__clone(deepcopy=False)

    def __deepcopy__(self, memo):
        """Support function for `copy.deepcopy()`.

        .. versionadded:: 2.4
        """
        return self.__clone(deepcopy=True)

    def __deepcopy(self, x, memo=None):
        """Deepcopy helper for the data dictionary or list.

        Regular expressions cannot be deep copied but as they are immutable we
        don't have to copy them when cloning.
        """
        if not hasattr(x, 'items'):
            y, is_list, iterator = [], True, enumerate(x)
        else:
            y, is_list, iterator = {}, False, x.iteritems()

        if memo is None:
            memo = {}
        val_id = id(x)
        if val_id in memo:
            return memo.get(val_id)
        memo[val_id] = y

        for key, value in iterator:
            if isinstance(value, (dict, list)) and not isinstance(value, SON):
                value = self.__deepcopy(value, memo)
            elif not isinstance(value, RE_TYPE):
                value = copy.deepcopy(value, memo)

            if is_list:
                y.append(value)
            else:
                if not isinstance(key, RE_TYPE):
                    key = copy.deepcopy(key, memo)
                y[key] = value
        return y
