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

    This class is only for use with the MongoDB opLog. If you need to store a
    regular timestamp, please use a :class:`datetime.datetime`.

    Raises TypeError if `time` and `inc` are not ints
    Raises ValueError if `time` or `inc` is not in [0, 2**32).

    :Parameters:
      - `time`: time in seconds since epoch UTC
      - `inc`: the incrementing counter
    """

    def __init__(self, time, inc):
        if not isinstance(time, int):
            raise TypeError("data must be an instance of str")
        if not isinstance(inc, int):
            raise TypeError("subtype must be an instance of int")
        if not 0 <= time < 2**32:
            raise ValueError("time must be contained in [0, 2**32)")
        if not 0 <= inc < 2**32:
            raise ValueError("inc must be contained in [0, 2**32)")

        self.time = time
        self.inc = inc

    def __eq__(self, other):
        if isinstance(other, Timestamp):
            return (self.time == other.time and self.inc == other.inc)
        else:
            return NotImplemented

    def __repr__(self):
        return "Timestamp(%s, %s)" % (self.time, self.inc)

    def as_datetime(self):
        return datetime.datetime.utcfromtimestamp(self.time)
