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

"""Representation of an ObjectId for Mongo."""

import threading
import types
import time
import socket
import os
import struct
try:
    import hashlib
    _md5func = hashlib.md5
except: # for Python < 2.5
    import md5
    _md5func = md5.new

from errors import InvalidId


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

    def __init__(self, id=None):
        """Initialize a new ObjectId.

        If no value of id is given, create a new (unique) ObjectId. If given id
        is an instance of (string, ObjectId) validate it and use that.
        Otherwise, a TypeError is raised. If given an invalid id, InvalidId is
        raised.

        :Parameters:
          - `id` (optional): a valid ObjectId (12 byte binary or 24 character
            hex string)
        """
        if id is None:
            self.__generate()
        else:
            self.__validate(id)

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
        elif isinstance(oid, types.StringType):
            if len(oid) == 12:
                self.__id = oid
            elif len(oid) == 24:
                self.__id = oid.decode("hex")
            else:
                raise InvalidId("%s is not a valid ObjectId" % oid)
        else:
            raise TypeError("id must be an instance of (str, ObjectId), "
                            "not %s" % type(oid))

    def url_encode(self, legacy=False):
        """Get a string representation of this ObjectId safe for use in a url.

        The `legacy` parameter is for backwards compatibility only and should
        almost always be kept False. It might eventually be removed.

        The reverse can be achieved using `url_decode()`.

        :Parameters:
          - `legacy` (optional): use the legacy byte ordering to represent the
            ObjectId. if you aren't positive you need this it is probably best
            left as False.
        """
        if legacy:
            return self.legacy_str().encode("hex")
        else:
            return self.__id.encode("hex")

    def url_decode(cls, encoded_oid, legacy=False):
        """Create an ObjectId from an encoded hex string.

        The `legacy` parameter is for backwards compatibility only and should
        almost always be kept False. It might eventually be removed.

        The reverse can be achieved using `url_encode()`.

        :Parameters:
          - `encoded_oid`: string encoding of an ObjectId (as created
            by `url_encode()`)
          - `legacy` (optional): use the legacy byte ordering to represent the
            ObjectId. if you aren't positive you need this it is probably best
            left as False.
        """
        if legacy:
            oid = encoded_oid.decode("hex")
            return cls.from_legacy_str(oid)
        else:
            return cls(encoded_oid.decode("hex"))
    url_decode = classmethod(url_decode)

    def legacy_str(self):
        return self.__id[7::-1] + self.__id[:7:-1]

    def from_legacy_str(cls, legacy_str):
        return cls(legacy_str[7::-1] + legacy_str[:7:-1])
    from_legacy_str = classmethod(from_legacy_str)

    def get_binary(self):
        """Get the binary representation of this ObjectId.
        """
        return self.__id

    binary = property(get_binary)

    def __str__(self):
        return self.__id.encode("hex")

    def __repr__(self):
        return "ObjectId('%s')" % self.__id.encode("hex")

    def __cmp__(self, other):
        if isinstance(other, ObjectId):
            return cmp(self.__id, other.__id)
        return NotImplemented

    def __hash__(self):
        return hash(self.__id)


