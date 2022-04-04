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

from typing import Tuple, Union

ASCENDING = 1
"""Ascending sort order."""
DESCENDING = -1
"""Descending sort order."""

GEO2D = "2d"
"""Index specifier for a 2-dimensional `geospatial index`_.

.. _geospatial index: http://docs.mongodb.org/manual/core/2d/
"""

GEOSPHERE = "2dsphere"
"""Index specifier for a `spherical geospatial index`_.

.. versionadded:: 2.5

.. _spherical geospatial index: http://docs.mongodb.org/manual/core/2dsphere/
"""

HASHED = "hashed"
"""Index specifier for a `hashed index`_.

.. versionadded:: 2.5

.. _hashed index: http://docs.mongodb.org/manual/core/index-hashed/
"""

TEXT = "text"
"""Index specifier for a `text index`_.

.. seealso:: MongoDB's `Atlas Search
   <https://docs.atlas.mongodb.com/atlas-search/>`_ which offers more advanced
   text search functionality.

.. versionadded:: 2.7.1

.. _text index: http://docs.mongodb.org/manual/core/index-text/
"""

version_tuple: Tuple[Union[int, str], ...] = (4, 1, 0)


def get_version_string() -> str:
    if isinstance(version_tuple[-1], str):
        return ".".join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return ".".join(map(str, version_tuple))


__version__: str = get_version_string()
version = __version__

"""Current version of PyMongo."""

from pymongo.collection import ReturnDocument  # noqa: F401
from pymongo.common import (  # noqa: F401
    MAX_SUPPORTED_WIRE_VERSION,
    MIN_SUPPORTED_WIRE_VERSION,
)
from pymongo.cursor import CursorType  # noqa: F401
from pymongo.mongo_client import MongoClient  # noqa: F401
from pymongo.operations import (  # noqa: F401
    DeleteMany,
    DeleteOne,
    IndexModel,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)
from pymongo.read_preferences import ReadPreference  # noqa: F401
from pymongo.write_concern import WriteConcern  # noqa: F401


def has_c() -> bool:
    """Is the C extension installed?"""
    try:
        from pymongo import _cmessage  # type: ignore[attr-defined] # noqa: F401

        return True
    except ImportError:
        return False
