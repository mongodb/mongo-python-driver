# Copyright 2009-present MongoDB, Inc.
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

from typing import Any, Tuple, Union
from pymongo.common import MAX_SUPPORTED_WIRE_VERSION as MAX_SUPPORTED_WIRE_VERSION, MIN_SUPPORTED_WIRE_VERSION as MIN_SUPPORTED_WIRE_VERSION
from pymongo.mongo_replica_set_client import MongoReplicaSetClient as MongoReplicaSetClient
from pymongo.operations import DeleteMany as DeleteMany, DeleteOne as DeleteOne, IndexModel as IndexModel, InsertOne as InsertOne, ReplaceOne as ReplaceOne, UpdateMany as UpdateMany, UpdateOne as UpdateOne
GEOHAYSTACK: str
OFF: int
SLOW_ONLY: int
ALL: int
version: str
ASCENDING: int = 1
"""Ascending sort order."""
DESCENDING: int = -1
"""Descending sort order."""

GEO2D: str = "2d"
"""Index specifier for a 2-dimensional `geospatial index`_.

.. _geospatial index: http://docs.mongodb.org/manual/core/2d/
"""

GEOSPHERE: str = "2dsphere"
"""Index specifier for a `spherical geospatial index`_.

.. versionadded:: 2.5

.. _spherical geospatial index: http://docs.mongodb.org/manual/core/2dsphere/
"""

HASHED: str = "hashed"
"""Index specifier for a `hashed index`_.

.. versionadded:: 2.5

.. _hashed index: http://docs.mongodb.org/manual/core/index-hashed/
"""

TEXT: str = "text"
"""Index specifier for a `text index`_.

.. seealso:: MongoDB's `Atlas Search
   <https://docs.atlas.mongodb.com/atlas-search/>`_ which offers more advanced
   text search functionality.

.. versionadded:: 2.7.1

.. _text index: http://docs.mongodb.org/manual/core/index-text/
"""

version_tuple: Tuple[Union[int, str], ...] = (4, 1, 0, '.dev0')

def get_version_string() -> str:
    if isinstance(version_tuple[-1], str):
        return '.'.join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return '.'.join(map(str, version_tuple))

__version__: str = get_version_string()
version = __version__

"""Current version of PyMongo."""

from pymongo.collection import ReturnDocument
from pymongo.common import (MIN_SUPPORTED_WIRE_VERSION,
                            MAX_SUPPORTED_WIRE_VERSION)
from pymongo.cursor import CursorType
from pymongo.mongo_client import MongoClient
from pymongo.operations import (IndexModel,
                                InsertOne,
                                DeleteOne,
                                DeleteMany,
                                UpdateOne,
                                UpdateMany,
                                ReplaceOne)
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

def has_c() -> bool:
    """Is the C extension installed?"""
    try:
        from pymongo import _cmessage
        return True
    except ImportError:
        return False
