# Copyright 2009-2014 MongoDB, Inc.
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

"""Exceptions raised by the :mod:`gridfs` package"""

from pymongo.errors import PyMongoError


class GridFSError(PyMongoError):
    """Base class for all GridFS exceptions.

    .. versionadded:: 1.5
    """


class CorruptGridFile(GridFSError):
    """Raised when a file in :class:`~gridfs.GridFS` is malformed.
    """


class NoFile(GridFSError):
    """Raised when trying to read from a non-existent file.

    .. versionadded:: 1.6
    """


class FileExists(GridFSError):
    """Raised when trying to create a file that already exists.

    .. versionadded:: 1.7
    """


class UnsupportedAPI(GridFSError):
    """Raised when trying to use the old GridFS API.

    In version 1.6 of the PyMongo distribution there were backwards
    incompatible changes to the GridFS API. Upgrading shouldn't be
    difficult, but the old API is no longer supported (with no
    deprecation period). This exception will be raised when attempting
    to use unsupported constructs from the old API.

    .. versionadded:: 1.6
    """
