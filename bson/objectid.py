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

"""Tools for working with MongoDB `ObjectIds
<http://dochub.mongodb.org/core/objectids>`_.
"""

import binascii
import calendar
import datetime
try:
    import hashlib
    _md5func = hashlib.md5
except ImportError:  # for Python < 2.5
    import md5
    _md5func = md5.new
import os
import random
import socket
import struct
import threading
import time

from bson.errors import InvalidId
from bson.py3compat import (PY3, b, binary_type, text_type,
                            bytes_from_hex, string_types)
from bson.tz_util import utc

EMPTY = b("")
ZERO  = b("\x00")

def _machine_bytes():
    """Get the machine portion of an ObjectId.
    """
    machine_hash = _md5func()
    machine_hash.update(socket.gethostname().encode())
    return machine_hash.digest()[0:3]


class ObjectId(object):
    """A MongoDB ObjectId.
    """

    _inc = random.randint(0, 0xFFFFFF)
    _inc_lock = threading.Lock()

    _machine_bytes = _machine_bytes()

    __slots__ = ('__id')

    def __init__(self, oid=None):
        """Initialize a new ObjectId.

        If `oid` is ``None``, create a new (unique) ObjectId. If `oid`
        is an instance of (:class:`basestring` (:class:`str` or :class:`bytes`
        in python 3), :class:`ObjectId`) validate it and use that.  Otherwise,
        a :class:`TypeError` is raised. If `oid` is invalid,
        :class:`~bson.errors.InvalidId` is raised.

        :Parameters:
          - `oid` (optional): a valid ObjectId (12 byte binary or 24 character
            hex string)

        .. versionadded:: 1.2.1
           The `oid` parameter can be a ``unicode`` instance (that contains
           only hexadecimal digits).

        .. mongodoc:: objectids
        """
        if oid is None:
            self.__generate()
        else:
            self.__validate(oid)

    @classmethod
    def from_datetime(cls, generation_time):
        """Create a dummy ObjectId instance with a specific generation time.

        This method is useful for doing range queries on a field
        containing :class:`ObjectId` instances.

        .. warning::
           It is not safe to insert a document containing an ObjectId
           generated using this method. This method deliberately
           eliminates the uniqueness guarantee that ObjectIds
           generally provide. ObjectIds generated with this method
           should be used exclusively in queries.

        `generation_time` will be converted to UTC. Naive datetime
        instances will be treated as though they already contain UTC.

        An example using this helper to get documents where ``"_id"``
        was generated before January 1, 2010 would be:

        >>> gen_time = datetime.datetime(2010, 1, 1)
        >>> dummy_id = ObjectId.from_datetime(gen_time)
        >>> result = collection.find({"_id": {"$lt": dummy_id}})

        :Parameters:
          - `generation_time`: :class:`~datetime.datetime` to be used
            as the generation time for the resulting ObjectId.

        .. versionchanged:: 1.8
           Properly handle timezone aware values for
           `generation_time`.

        .. versionadded:: 1.6
        """
        if generation_time.utcoffset() is not None:
            generation_time = generation_time - generation_time.utcoffset()
        ts = calendar.timegm(generation_time.timetuple())
        oid = struct.pack(">i", int(ts)) + ZERO * 8
        return cls(oid)

    def __generate(self):
        """Generate a new value for this ObjectId.
        """
        oid = EMPTY

        # 4 bytes current time
        oid += struct.pack(">i", int(time.time()))

        # 3 bytes machine
        oid += ObjectId._machine_bytes

        # 2 bytes pid
        oid += struct.pack(">H", os.getpid() % 0xFFFF)

        # 3 bytes inc
        ObjectId._inc_lock.acquire()
        oid += struct.pack(">i", ObjectId._inc)[1:4]
        ObjectId._inc = (ObjectId._inc + 1) % 0xFFFFFF
        ObjectId._inc_lock.release()

        self.__id = oid

    def __validate(self, oid):
        """Validate and use the given id for this ObjectId.

        Raises TypeError if id is not an instance of
        (:class:`basestring` (:class:`str` or :class:`bytes`
        in python 3), ObjectId) and InvalidId if it is not a
        valid ObjectId.

        :Parameters:
          - `oid`: a valid ObjectId
        """
        if isinstance(oid, ObjectId):
            self.__id = oid.__id
        elif isinstance(oid, string_types):
            if len(oid) == 12:
                if isinstance(oid, binary_type):
                    self.__id = oid
                else:
                    raise InvalidId("%s is not a valid ObjectId" % oid)
            elif len(oid) == 24:
                try:
                    self.__id = bytes_from_hex(oid)
                except (TypeError, ValueError):
                    raise InvalidId("%s is not a valid ObjectId" % oid)
            else:
                raise InvalidId("%s is not a valid ObjectId" % oid)
        else:
            raise TypeError("id must be an instance of (%s, %s, ObjectId), "
                            "not %s" % (binary_type.__name__,
                                        text_type.__name__, type(oid)))

    @property
    def binary(self):
        """12-byte binary representation of this ObjectId.
        """
        return self.__id

    @property
    def generation_time(self):
        """A :class:`datetime.datetime` instance representing the time of
        generation for this :class:`ObjectId`.

        The :class:`datetime.datetime` is timezone aware, and
        represents the generation time in UTC. It is precise to the
        second.

        .. versionchanged:: 1.8
           Now return an aware datetime instead of a naive one.

        .. versionadded:: 1.2
        """
        t = struct.unpack(">i", self.__id[0:4])[0]
        return datetime.datetime.fromtimestamp(t, utc)

    def __getstate__(self):
        """return value of object for pickling.
        needed explicitly because __slots__() defined.
        """
        return self.__id

    def __setstate__(self, value):
        """explicit state set from pickling
        """
        # Provide backwards compatability with OIDs
        # pickled with pymongo-1.9 or older.
        if isinstance(value, dict):
            oid = value["_ObjectId__id"]
        else:
            oid = value
        # ObjectIds pickled in python 2.x used `str` for __id.
        # In python 3.x this has to be converted to `bytes`
        # by encoding latin-1.
        if PY3 and isinstance(oid, text_type):
            self.__id = oid.encode('latin-1')
        else:
            self.__id = oid

    def __str__(self):
        return binascii.hexlify(self.__id).decode()

    def __repr__(self):
        return "ObjectId('%s')" % (str(self),)

    def __eq__(self, other):
        if isinstance(other, ObjectId):
            return self.__id == other.__id
        return NotImplemented

    def __ne__(self,other):
        if isinstance(other, ObjectId):
            return self.__id != other.__id
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, ObjectId):
            return self.__id < other.__id
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, ObjectId):
            return self.__id <= other.__id
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, ObjectId):
            return self.__id > other.__id
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, ObjectId):
            return self.__id >= other.__id
        return NotImplemented

    def __hash__(self):
        """Get a hash value for this :class:`ObjectId`.

        .. versionadded:: 1.1
        """
        return hash(self.__id)
