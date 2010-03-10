# Copyright 2010 10gen, Inc.
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

"""Tools for representing MongoDB internal Timestamps.
"""

import datetime

class Timestamp(object):
    """MongoDB internal timestamps used in the opLog.
    """

    def __init__(self, time, inc):
        """Create a new :class:`Timestamp`.

        This class is only for use with the MongoDB opLog. If you need
        to store a regular timestamp, please use a
        :class:`~datetime.datetime`.

        Raises :class:`TypeError` if `time` and `inc` are not
        instances of :class:`int`. Raises :class:`ValueError` if
        `time` or `inc` is not in [0, 2**32).

        :Parameters:
          - `time`: time in seconds since epoch UTC
          - `inc`: the incrementing counter
        """
        if not isinstance(time, int):
            raise TypeError("time must be an instance of int")
        if not isinstance(inc, int):
            raise TypeError("inc must be an instance of int")
        if not 0 <= time < 2**32:
            raise ValueError("time must be contained in [0, 2**32)")
        if not 0 <= inc < 2**32:
            raise ValueError("inc must be contained in [0, 2**32)")

        self.__time = time
        self.__inc = inc

    @property
    def time(self):
        """Get the time portion of this :class:`Timestamp`.
        """
        return self.__time

    @property
    def inc(self):
        """Get the inc portion of this :class:`Timestamp`.
        """
        return self.__inc

    def __eq__(self, other):
        if isinstance(other, Timestamp):
            return (self.__time == other.time and self.__inc == other.inc)
        else:
            return NotImplemented

    def __repr__(self):
        return "Timestamp(%s, %s)" % (self.__time, self.__inc)

    def as_datetime(self):
        """Return a :class:`~datetime.datetime` instance corresponding
        to the time portion of this :class:`Timestamp`.
        """
        return datetime.datetime.utcfromtimestamp(self.__time)
