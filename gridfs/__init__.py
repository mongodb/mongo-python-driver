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

"""GridFS is a specification for storing large objects in Mongo.

The :mod:`gridfs` package is an implementation of GridFS on top of
:mod:`pymongo`, exposing a file-like interface.

.. seealso:: The MongoDB documentation on `gridfs <https://dochub.mongodb.org/core/gridfs>`_.
"""
from __future__ import annotations

import sys

from gridfs.asynchronous.grid_file import (
    AsyncGridFS,
    AsyncGridFSBucket,
    AsyncGridIn,
    AsyncGridOut,
    AsyncGridOutCursor,
)
from gridfs.errors import NoFile
from gridfs.grid_file_shared import DEFAULT_CHUNK_SIZE
from gridfs.synchronous.grid_file import (
    GridFS,
    GridFSBucket,
    GridIn,
    GridOut,
    GridOutCursor,
)

# Export synchronous modules as top-level gridfs modules for compatibility
sync_modules = {}
for name in sys.modules:
    if name.startswith("gridfs.synchronous."):
        full_name = "{}.{}".format("gridfs", name.rsplit(".")[-1])
        sync_modules[full_name] = name

for module in sync_modules:
    sys.modules[module] = sys.modules[sync_modules[module]]

__all__ = [
    "AsyncGridFS",
    "GridFS",
    "AsyncGridFSBucket",
    "GridFSBucket",
    "NoFile",
    "DEFAULT_CHUNK_SIZE",
    "AsyncGridIn",
    "GridIn",
    "AsyncGridOut",
    "GridOut",
    "AsyncGridOutCursor",
    "GridOutCursor",
]
