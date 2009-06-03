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

"""A Mongo driver for Python."""

import types

from pymongo.connection import Connection as PyMongo_Connection
from pymongo.son import SON

ASCENDING = 1
"""Ascending sort order."""
DESCENDING = -1
"""Descending sort order."""

OFF = 0
"""Turn off database profiling."""
SLOW_ONLY = 1
"""Only profile slow operations."""
ALL = 2
"""Profile all operations."""

version = "0.11"
"""Current version of PyMongo."""

Connection = PyMongo_Connection
"""Alias for pymongo.connection.Connection."""


def _index_list(key_or_list, direction):
    """Helper to generate a list of (key, direction) pairs.

    Takes such a list, or a single key and direction.
    """
    if direction is not None:
        return [(key_or_list, direction)]
    else:
        return key_or_list


def _index_document(index_list):
    """Helper to generate an index specifying document.

    Takes a list of (key, direction) pairs.
    """
    if not isinstance(index_list, types.ListType):
        raise TypeError("if no direction is specified, key_or_list must be an"
                        "instance of list")
    if not len(index_list):
        raise ValueError("key_or_list must not be the empty list")

    index = SON()
    for (key, value) in index_list:
        if not isinstance(key, types.StringTypes):
            raise TypeError("first item in each key pair must be a string")
        if not isinstance(value, types.IntType):
            raise TypeError("second item in each key pair must be ASCENDING or"
                            "DESCENDING")
        index[key] = value
    return index
