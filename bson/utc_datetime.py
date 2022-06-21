# Copyright 2010-2022 MongoDB, Inc.
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
from datetime import datetime, timezone


class UTCDatetimeRaw:
    def __hash__(self) -> int:
        return hash(self._value)

    def __repr__(self) -> str:
        return type(self).__name__ + "(" + str(self._value) + ")"

    def __str__(self) -> str:
        return self.__repr__()

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

    def __init__(self, value):
        if isinstance(value, numbers.Number):
            self._value = int(value)
        elif isinstance(value, datetime):
            self._value = value.timestamp() * 1000
        else:
            raise TypeError(f"{type(value)} is not a valid type for UTCDatetimeRaw")

    def to_datetime(self):
        return datetime.fromtimestamp(self._value / 1000, timezone.utc)

    def __int__(self) -> float:
        return int(self._value)
