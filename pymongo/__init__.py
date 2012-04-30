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

"""Python driver for MongoDB."""


ASCENDING = 1
"""Ascending sort order."""
DESCENDING = -1
"""Descending sort order."""

GEO2D = "2d"
"""Index specifier for a 2-dimensional `geospatial index`_.

.. versionadded:: 1.5.1

.. note:: Geo-spatial indexing requires server version **>= 1.3.3+**.

.. _geospatial index: http://www.mongodb.org/display/DOCS/Geospatial+Indexing
"""

GEOHAYSTACK = "geoHaystack"
"""Index specifier for a 2-dimensional `haystack index`_.

.. versionadded:: 2.1

.. note:: Geo-spatial indexing requires server version **>= 1.5.6+**.

.. _geospatial index: http://www.mongodb.org/display/DOCS/Geospatial+Haystack+Indexing
"""


OFF = 0
"""No database profiling."""
SLOW_ONLY = 1
"""Only profile slow operations."""
ALL = 2
"""Profile all operations."""

class ReadPreference:
    """An enum that defines the read preferences supported by PyMongo.

    +----------------------+--------------------------------------------------+
    |    Connection type   |                 Read Preference                  |
    +======================+================+================+================+
    |                      |`PRIMARY`       |`SECONDARY`     |`SECONDARY_ONLY`|
    +----------------------+----------------+----------------+----------------+
    |Connection to a single|Queries are     |Queries are     |Same as         |
    |host.                 |allowed if the  |allowed if the  |`SECONDARY`     |
    |                      |connection is to|connection is to|                |
    |                      |the replica set |the replica set |                |
    |                      |primary.        |primary or a    |                |
    |                      |                |secondary.      |                |
    +----------------------+----------------+----------------+----------------+
    |Connection to a       |Queries are sent|Queries are     |Same as         |
    |mongos.               |to the primary  |distributed     |`SECONDARY`     |
    |                      |of a shard.     |among shard     |                |
    |                      |                |secondaries.    |                |
    |                      |                |Queries are sent|                |
    |                      |                |to the primary  |                |
    |                      |                |if no           |                |
    |                      |                |secondaries are |                |
    |                      |                |available.      |                |
    |                      |                |                |                |
    +----------------------+----------------+----------------+----------------+
    |ReplicaSetConnection  |Queries are sent|Queries are     |Queries are     |
    |                      |to the primary  |distributed     |never sent to   |
    |                      |of the replica  |among replica   |the replica set |
    |                      |set.            |set secondaries.|primary. An     |
    |                      |                |Queries are sent|exception is    |
    |                      |                |to the primary  |raised if no    |
    |                      |                |if no           |secondary is    |
    |                      |                |secondaries are |available.      |
    |                      |                |available.      |                |
    |                      |                |                |                |
    +----------------------+----------------+----------------+----------------+
    """

    PRIMARY = 0
    SECONDARY = 1
    SECONDARY_ONLY = 2

version_tuple = (2, 2)

def get_version_string():
    if isinstance(version_tuple[-1], basestring):
        return '.'.join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return '.'.join(map(str, version_tuple))

version = get_version_string()
"""Current version of PyMongo."""

from pymongo.connection import Connection
from pymongo.replica_set_connection import ReplicaSetConnection

def has_c():
    """Is the C extension installed?

    .. versionadded:: 1.5
    """
    try:
        from pymongo import _cmessage
        return True
    except ImportError:
        return False
