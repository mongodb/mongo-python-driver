# Copyright 2022-present MongoDB, Inc.
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

"""Representation of a UTC datetime in milliseconds as an int64.
"""
import numbers
from calendar import calendar
from datetime import datetime, timezone

EPOCH_AWARE = datetime.fromtimestamp(0, timezone.utc)
EPOCH_NAIVE = datetime.utcfromtimestamp(0)


class UTCDatetimeRaw:
    """
    Represents a BSON UTC datetime, which is defined as an int64 of
    milliseconds since the Unix epoch. Principal use is to represent
    datetimes outside the range of the Python builtin
    :class:`~datetime.datetime` class when encoding/decoding BSON.
    To decode UTC datetimes as a ``UTCDatetimeRaw``,
    `datetime_conversion` in :class:`~bson.CodecOptions` must be set
    to 'raw'.
    """

    def __init__(self, value: [int, datetime]):
        if isinstance(value, int):
            self._value = value
        elif isinstance(value, datetime):
            if value.utcoffset() is not None:
                value = value - value.utcoffset()  # type: ignore
            self._value = int(calendar.timegm(value.timetuple()) * 1000 + value.microsecond // 1000)
        else:
            raise TypeError(f"{type(value)} is not a valid type for UTCDatetimeRaw")

    def __hash__(self) -> int:
        return hash(self._value)

    def __repr__(self) -> str:
        return type(self).__name__ + "(" + str(self._value) + ")"

    __str__ = __repr__

    # Avoids using functools.total_ordering for speed.

    def __lt__(self, other):
        return self._value.__lt__(other._value)

    def __le__(self, other):
        return self._value.__le__(other._value)

    def __eq__(self, other):
        return self._value.__eq__(other._value)

    def __ne__(self, other):
        return self._value.__ne__(other._value)

    def __gt__(self, other):
        return self._value.__gt__(other._value)

    def __ge__(self, other):
        return self._value.__ge__(other._value)

    _type_marker = 9

    def to_datetime(self, tz_aware=True, tzinfo=timezone.utc) -> datetime:
        """
        Converts this ``UTCDatetimeRaw`` into a :class:`~datetime.datetime`
        object. If `opts` is not set, then it will default to a
        :class:`~bson.CodecOptions` with `tz_aware = True` and
        `tzinfo = datetime.timezone.utc`.
        """
        diff = ((self._value % 1000) + 1000) % 1000
        seconds = (self._value - diff) // 1000
        micros = diff * 1000

        if tz_aware:
            dt = EPOCH_AWARE + datetime.timedelta(seconds=seconds, microseconds=micros)
            if tzinfo:
                dt = dt.astimezone(tzinfo)
            return dt
        else:
            return EPOCH_NAIVE + datetime.timedelta(seconds=seconds, microseconds=micros)

    def __int__(self) -> int:
        return int(self._value)
