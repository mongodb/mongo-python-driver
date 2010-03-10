# Copyright 2009-2010 10gen, Inc.
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

"""Exceptions raised by PyMongo."""

class PyMongoError(Exception):
    """Base class for all PyMongo exceptions.

    .. versionadded:: 1.4
    """


class ConnectionFailure(PyMongoError):
    """Raised when a connection to the database cannot be made or is lost.
    """


class AutoReconnect(ConnectionFailure):
    """Raised when a connection to the database is lost and an attempt to
    auto-reconnect will be made.

    In order to auto-reconnect you must handle this exception, recognizing that
    the operation which caused it has not necessarily succeeded. Future
    operations will attempt to open a new connection to the database (and
    will continue to raise this exception until the first successful
    connection is made).
    """


class ConfigurationError(PyMongoError):
    """Raised when something is incorrectly configured.
    """


class OperationFailure(PyMongoError):
    """Raised when a database operation fails.
    """


class DuplicateKeyError(OperationFailure):
    """Raised when a safe insert or update fails due to a duplicate key error.

    .. note:: Requires server version **>= 1.3.0**

    .. versionadded:: 1.4
    """


class InvalidOperation(PyMongoError):
    """Raised when a client attempts to perform an invalid operation.
    """


class CollectionInvalid(PyMongoError):
    """Raised when collection validation fails.
    """


class InvalidName(PyMongoError):
    """Raised when an invalid name is used.
    """


class InvalidBSON(PyMongoError):
    """Raised when trying to create a BSON object from invalid data.
    """


class InvalidStringData(PyMongoError):
    """Raised when trying to encode a string containing non-UTF8 data.
    """


class InvalidDocument(PyMongoError):
    """Raised when trying to create a BSON object from an invalid document.
    """


class InvalidId(PyMongoError):
    """Raised when trying to create an ObjectId from invalid data.
    """


class InvalidURI(PyMongoError):
    """Raised when trying to parse an invalid mongodb URI.

    .. versionadded:: 1.5
    """
