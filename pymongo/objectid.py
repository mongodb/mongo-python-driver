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

"""Tools for working with MongoDB `ObjectIds
<http://www.mongodb.org/display/DOCS/Object+IDs>`_.
"""

import datetime
try:
    import hashlib
    _md5func = hashlib.md5
except ImportError: # for Python < 2.5
    import md5
    _md5func = md5.new
import os
import socket
import struct
import threading
import time
import warnings

from pymongo.errors import InvalidId


def _machine_bytes():
    """Get the machine portion of an ObjectId.
    """
    machine_hash = _md5func()
    machine_hash.update(socket.gethostname())
    return machine_hash.digest()[0:3]


class ObjectId(object):
    """A Mongo ObjectId.
    """

    _inc = 0
    _inc_lock = threading.Lock()

    _machine_bytes = _machine_bytes()

    def __init__(self, oid=None):
        """Initialize a new ObjectId_.

        If `oid` is ``None``, create a new (unique)
        ObjectId_. If `oid` is an instance of (``basestring``,
        :class:`ObjectId`) validate it and use that.  Otherwise, a
        :class:`TypeError` is raised. If `oid` is invalid,
        :class:`~pymongo.errors.InvalidId` is raised.

        :Parameters:
          - `oid` (optional): a valid ObjectId_ (12 byte binary or 24 character
            hex string)

        .. versionadded:: 1.2.1
           The `oid` parameter can be a ``unicode`` instance (that contains only
           hexadecimal digits).

        .. _ObjectId: http://www.mongodb.org/display/DOCS/Object+IDs

        .. mongodoc:: objectids
        """
        if oid is None:
            self.__generate()
        else:
            self.__validate(oid)

    def __generate(self):
        """Generate a new value for this ObjectId.
        """
        oid = ""

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

        Raises TypeError if id is not an instance of (str, ObjectId) and
        InvalidId if it is not a valid ObjectId.

        :Parameters:
          - `oid`: a valid ObjectId
        """
        if isinstance(oid, ObjectId):
            self.__id = oid.__id
        elif isinstance(oid, basestring):
            if len(oid) == 12:
                self.__id = oid
            elif len(oid) == 24:
                try:
                    self.__id = oid.decode("hex")
                except TypeError:
                    raise InvalidId("%s is not a valid ObjectId" % oid)
            else:
                raise InvalidId("%s is not a valid ObjectId" % oid)
        else:
            raise TypeError("id must be an instance of (str, ObjectId), "
                            "not %s" % type(oid))

    @property
    def binary(self):
        """12-byte binary representation of this ObjectId.
        """
        return self.__id

    @property
    def generation_time(self):
        """A :class:`datetime.datetime` instance representing the time of
        generation for this :class:`ObjectId`.

        The :class:`datetime.datetime` is always naive and represents the
        generation time in UTC. It is precise to the second.

        .. versionadded:: 1.2
        """
        t = struct.unpack(">i", self.__id[0:4])[0]
        return datetime.datetime.utcfromtimestamp(t)

    def __str__(self):
        return self.__id.encode("hex")

    def __repr__(self):
        return "ObjectId('%s')" % self.__id.encode("hex")

    def __cmp__(self, other):
        if isinstance(other, ObjectId):
            return cmp(self.__id, other.__id)
        return NotImplemented

    def __hash__(self):
        """Get a hash value for this :class:`ObjectId`.

        .. versionadded:: 1.1
        """
        return hash(self.__id)


