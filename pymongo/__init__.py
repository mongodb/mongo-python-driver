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

.. note:: Geo-spatial indexing requires server version **>= 1.3.3**.

.. _geospatial index: http://docs.mongodb.org/manual/core/geospatial-indexes/
"""

GEOHAYSTACK = "geoHaystack"
"""Index specifier for a 2-dimensional `haystack index`_.

.. versionadded:: 2.1

.. note:: Geo-spatial indexing requires server version **>= 1.5.6**.

.. _haystack index: http://docs.mongodb.org/manual/core/geospatial-indexes/#haystack-indexes
"""

GEOSPHERE = "2dsphere"
"""Index specifier for a `spherical geospatial index`_.

.. versionadded:: 2.5

.. note:: 2dsphere indexing requires server version **>= 2.4.0**.

.. _spherical geospatial index: http://docs.mongodb.org/manual/release-notes/2.4/#new-geospatial-indexes-with-geojson-and-improved-spherical-geometry
"""

HASHED = "hashed"
"""Index specifier for a `hashed index`_.

.. versionadded:: 2.5

.. note:: hashed indexing requires server version **>= 2.4.0**.

.. _hashed index: http://docs.mongodb.org/manual/release-notes/2.4/#new-hashed-index-and-sharding-with-a-hashed-shard-key
"""

OFF = 0
"""No database profiling."""
SLOW_ONLY = 1
"""Only profile slow operations."""
ALL = 2
"""Profile all operations."""

version_tuple = (2, 5, 1, '+')

def get_version_string():
    if isinstance(version_tuple[-1], basestring):
        return '.'.join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return '.'.join(map(str, version_tuple))

version = get_version_string()
"""Current version of PyMongo."""

from pymongo.connection import Connection
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.replica_set_connection import ReplicaSetConnection
from pymongo.read_preferences import ReadPreference

def has_c():
    """Is the C extension installed?

    .. versionadded:: 1.5
    """
    try:
        from pymongo import _cmessage
        return True
    except ImportError:
        return False
