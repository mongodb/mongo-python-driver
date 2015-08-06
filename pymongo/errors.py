# Copyright 2009-2015 MongoDB, Inc.
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

from bson.errors import *

try:
    from ssl import CertificateError
except ImportError:
    from pymongo.ssl_match_hostname import CertificateError


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
    def __init__(self, message='', errors=None):
        self.errors = errors or []
        ConnectionFailure.__init__(self, message)


class ConfigurationError(PyMongoError):
    """Raised when something is incorrectly configured.
    """


class OperationFailure(PyMongoError):
    """Raised when a database operation fails.

    .. versionadded:: 2.7
       The :attr:`details` attribute.

    .. versionadded:: 1.8
       The :attr:`code` attribute.
    """

    def __init__(self, error, code=None, details=None):
        self.__code = code
        self.__details = details
        PyMongoError.__init__(self, error)

    @property
    def code(self):
        """The error code returned by the server, if any.
        """
        return self.__code

    @property
    def details(self):
        """The complete error document returned by the server.

        Depending on the error that occurred, the error document
        may include useful information beyond just the error
        message. When connected to a mongos the error document
        may contain one or more subdocuments if errors occurred
        on multiple shards.
        """
        return self.__details


class CursorNotFound(OperationFailure):
    """Raised while iterating query results if the cursor is
    invalidated on the server.

    .. versionadded:: 2.7
    """


class ExecutionTimeout(OperationFailure):
    """Raised when a database operation times out, exceeding the $maxTimeMS
    set in the query or command option.

    .. note:: Requires server version **>= 2.6.0**

    .. versionadded:: 2.7
    """


class TimeoutError(OperationFailure):
    """DEPRECATED - will be removed in PyMongo 3.0. See WTimeoutError instead.

    .. versionadded:: 1.8
    """


class WTimeoutError(TimeoutError):
    """Raised when a database operation times out (i.e. wtimeout expires)
    before replication completes.

    With newer versions of MongoDB the `details` attribute may include
    write concern fields like 'n', 'updatedExisting', or 'writtenTo'.

    .. versionadded:: 2.7
    """


class DuplicateKeyError(OperationFailure):
    """Raised when a safe insert or update fails due to a duplicate key error.

    .. note:: Requires server version **>= 1.3.0**

    .. versionadded:: 1.4
    """


class BulkWriteError(OperationFailure):
    """Exception class for bulk write errors.

    .. versionadded:: 2.7
    """
    def __init__(self, results):
        OperationFailure.__init__(
            self, "batch op errors occurred", 65, results)


class InvalidOperation(PyMongoError):
    """Raised when a client attempts to perform an invalid operation.
    """


class InvalidName(PyMongoError):
    """Raised when an invalid name is used.
    """


class CollectionInvalid(PyMongoError):
    """Raised when collection validation fails.
    """


class InvalidURI(ConfigurationError):
    """Raised when trying to parse an invalid mongodb URI.

    .. versionadded:: 1.5
    """


class UnsupportedOption(ConfigurationError):
    """Exception for unsupported options.

    .. versionadded:: 2.0
    """


class ExceededMaxWaiters(Exception):
    """Raised when a thread tries to get a connection from a pool and
    ``max_pool_size * waitQueueMultiple`` threads are already waiting.

    .. versionadded:: 2.6
    """
    pass


class DocumentTooLarge(InvalidDocument):
    """Raised when an encoded document is too large for the connected server.
    """
    pass
