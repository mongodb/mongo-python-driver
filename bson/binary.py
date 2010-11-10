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

"""Tools for representing BSON binary data.
"""

BINARY_SUBTYPE = 0
"""BSON binary subtype for binary data.

This is becomming the default subtype and should be the most commonly
used.

.. versionadded:: 1.5
"""

FUNCTION_SUBTYPE = 1
"""BSON binary subtype for functions.

.. versionadded:: 1.5
"""

OLD_BINARY_SUBTYPE = 2
"""Old BSON binary subtype for binary data.

This is still the default subtype, but that is changing to
:data:`BINARY_SUBTYPE`.

.. versionadded:: 1.7
"""

UUID_SUBTYPE = 3
"""BSON binary subtype for a UUID.

:class:`uuid.UUID` instances will automatically be encoded
by :mod:`bson` using this subtype.

.. versionadded:: 1.5
"""

MD5_SUBTYPE = 5
"""BSON binary subtype for an MD5 hash.

.. versionadded:: 1.5
"""

USER_DEFINED_SUBTYPE = 128
"""BSON binary subtype for any user defined structure.

.. versionadded:: 1.5
"""


class Binary(str):
    """Representation of BSON binary data.

    This is necessary because we want to represent Python strings as
    the BSON string type. We need to wrap binary data so we can tell
    the difference between what should be considered binary data and
    what should be considered a string when we encode to BSON.

    Raises TypeError if `data` is not an instance of str or `subtype`
    is not an instance of int. Raises ValueError if `subtype` is not
    in [0, 256).

    :Parameters:
      - `data`: the binary data to represent
      - `subtype` (optional): the `binary subtype
        <http://bsonspec.org/#/specification>`_
        to use
    """

    def __new__(cls, data, subtype=OLD_BINARY_SUBTYPE):
        if not isinstance(data, str):
            raise TypeError("data must be an instance of str")
        if not isinstance(subtype, int):
            raise TypeError("subtype must be an instance of int")
        if subtype >= 256 or subtype < 0:
            raise ValueError("subtype must be contained in [0, 256)")
        self = str.__new__(cls, data)
        self.__subtype = subtype
        return self

    @property
    def subtype(self):
        """Subtype of this binary data.
        """
        return self.__subtype

    def __eq__(self, other):
        if isinstance(other, Binary):
            return (self.__subtype, str(self)) == (other.subtype, str(other))
        # We don't return NotImplemented here because if we did then
        # Binary("foo") == "foo" would return True, since Binary is a
        # subclass of str...
        return False

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "Binary(%s, %s)" % (str.__repr__(self), self.__subtype)
