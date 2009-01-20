"""Cursor class to iterate over Mongo query results."""

import types
import struct

import bson
from son import SON
from errors import InvalidOperation, OperationFailure

class Cursor(object):
    """A cursor / iterator over Mongo query results.
    """
    def __init__(self, collection, spec, fields, skip, limit):
        """Create a new cursor.

        Should not be called directly by application developers.
        """
        self.__collection = collection
        self.__spec = spec
        self.__fields = fields
        self.__skip = skip
        self.__limit = limit
        self.__ordering = None

        self.__data = []
        self.__id = None
        self.__retrieved = 0
        self.__killed = False

    def __del__(self):
        if self.__id and not self.__killed:
            self.__die()

    def __die(self):
        """Closes this cursor.
        """
        self.__collection.database().connection().close_cursor(self.__id)
        self.__killed = True

    def __query_spec(self):
        """Get the spec to use for a query.

        Just `self.__spec`, unless this cursor needs special query fields, like
        orderby.
        """
        if not self.__ordering:
            return self.__spec
        return SON([("query", self.__spec),
                    ("orderby", self.__ordering)])

    def __check_okay_to_chain(self):
        """Check if it is okay to chain more options onto this cursor.
        """
        if self.__retrieved or self.__id is not None:
            raise InvalidOperation("cannot set options after executing query")

    def limit(self, limit):
        """Limits the number of results to be returned by this cursor.

        Raises TypeError if limit is not an instance of int. Raises
        InvalidOperation if this cursor has already been used.

        Arguments:
        - `limit`: the number of results to return
        """
        if not isinstance(limit, types.IntType):
            raise TypeError("limit must be an int")
        self.__check_okay_to_chain()

        self.__limit = limit
        return self

    def skip(self, skip):
        """Skips the first `skip` results of this cursor.

        Raises TypeError if skip is not an instance of int. Raises
        InvalidOperation if this cursor has already been used.

        Arguments:
        - `skip`: the number of results to skip
        """
        if not isinstance(skip, types.IntType):
            raise TypeError("skip must be an int")
        self.__check_okay_to_chain()

        self.__skip = skip
        return self

    def sort(self, key_or_list, direction=None):
        """Sorts this cursors results.

        Takes either a single key and a direction, or a list of (key, direction)
        pairs. The key(s) must be an instance of (str, unicode), and the
        direction(s) must be one of (Mongo.ASCENDING, Mongo.DESCENDING). Raises
        InvalidOperation if this cursor has already been used.

        Arguments:
        - `key_or_list`: a single key or a list of (key, direction) pairs
            specifying the keys to sort on
        - `direction` (optional): must be included if key_or_list is a single
            key, otherwise must be None
        """
        self.__check_okay_to_chain()

        # TODO a lot of this logic could be shared with create_index()
        if direction:
            keys = [(key_or_list, direction)]
        else:
            keys = key_or_list

        if not isinstance(keys, types.ListType):
            raise TypeError("if no direction is specified, key_or_list must be an instance of list")
        if not len(keys):
            raise ValueError("key_or_list must not be the empty list")

        orderby = SON()
        for (key, value) in keys:
            if not isinstance(key, types.StringTypes):
                raise TypeError("first item in each key pair must be a string")
            if not isinstance(value, types.IntType):
                raise TypeError("second item in each key pair must be Mongo.ASCENDING or Mongo.DESCENDING")
            orderby[key] = value

        self.__ordering = orderby
        return self

    def count(self):
        """Get the size of the results set for this query.

        Returns the number of objects in the results set for this query. Does
        not take limit and skip into account. Raises InvalidOperation if this
        cursor has already been used. Raises OperationFailure on a database
        error.
        """
        self.__check_okay_to_chain()

        command = SON([("count", self.__collection.name()),
                       ("query", self.__spec)])
        response = self.__collection.database()._command(command)
        if response["ok"] != 1:
            if response["errmsg"] == "ns does not exist":
                return 0
            raise OperationFailure("error getting count: %s" % response["errmsg"])
        return int(response["n"])

    def _refresh(self):
        """Refreshes the cursor with more data from Mongo.

        Returns the length of self.__data after refresh. Will exit early if
        self.__data is already non-empty. Raises OperationFailure when the
        cursor cannot be refreshed due to an error on the query.
        """
        if len(self.__data) or self.__killed:
            return len(self.__data)

        def send_message(operation, message):
            # TODO the send and receive here should be synchronized...
            request_id = self.__collection._send_message(operation, message)
            response = self.__collection.database().connection().receive_message(1, request_id)

            response_flag = struct.unpack("<i", response[:4])[0]
            if response_flag == 1:
                raise OperationFailure("cursor id '%s' not valid at server" % self.__id)
            elif response_flag == 2:
                error_object = bson.BSON(response[20:]).to_dict()
                raise OperationFailure("database error: %s" % error_object["$err"])
            else:
                assert response_flag == 0

            self.__id = struct.unpack("<q", response[4:12])[0]
            assert struct.unpack("<i", response[12:16])[0] == self.__retrieved

            number_returned = struct.unpack("<i", response[16:20])[0]
            self.__retrieved += number_returned
            self.__data = bson.to_dicts(response[20:])
            assert len(self.__data) == number_returned

        if self.__id is None:
            # Query
            message = struct.pack("<i", self.__skip)
            message += struct.pack("<i", self.__limit)
            message += bson.BSON.from_dict(self.__query_spec())
            if self.__fields:
                message += bson.BSON.from_dict(self.__fields)

            send_message(2004, message)
        elif self.__id != 0:
            # Get More
            limit = 0
            if self.__limit:
                if self.__limit > self.__retrieved:
                    limit = self.__limit - self.__retrieved
                else:
                    self.__die()
                    return 0

            message = struct.pack("<i", limit)
            message += struct.pack("<q", self.__id)

            send_message(2005, message)

        length = len(self.__data)
        if not length:
            self.__die()
        return length

    def __iter__(self):
        return self

    def next(self):
        if len(self.__data):
            return self.__collection.database()._fix_outgoing(self.__data.pop(0))
        if self._refresh():
            return self.__collection.database()._fix_outgoing(self.__data.pop(0))
        raise StopIteration
