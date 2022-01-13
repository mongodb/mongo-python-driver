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

"""Exceptions raised by PyMongo."""

from bson.errors import *

try:
    # CPython 3.7+
    from ssl import SSLCertVerificationError as _CertificateError
except ImportError:
    try:
        from ssl import CertificateError as _CertificateError
    except ImportError:
        class _CertificateError(ValueError):
            pass


class PyMongoError(Exception):
    """Base class for all PyMongo exceptions."""
    def __init__(self, message='', error_labels=None):
        super(PyMongoError, self).__init__(message)
        self._message = message
        self._error_labels = set(error_labels or [])

    def has_error_label(self, label):
        """Return True if this error contains the given label.

        .. versionadded:: 3.7
        """
        return label in self._error_labels

    def _add_error_label(self, label):
        """Add the given label to this error."""
        self._error_labels.add(label)

    def _remove_error_label(self, label):
        """Remove the given label from this error."""
        self._error_labels.discard(label)


class ProtocolError(PyMongoError):
    """Raised for failures related to the wire protocol."""


class ConnectionFailure(PyMongoError):
    """Raised when a connection to the database cannot be made or is lost."""


class AutoReconnect(ConnectionFailure):
    """Raised when a connection to the database is lost and an attempt to
    auto-reconnect will be made.

    In order to auto-reconnect you must handle this exception, recognizing that
    the operation which caused it has not necessarily succeeded. Future
    operations will attempt to open a new connection to the database (and
    will continue to raise this exception until the first successful
    connection is made).

    Subclass of :exc:`~pymongo.errors.ConnectionFailure`.
    """
    def __init__(self, message='', errors=None):
        error_labels = None
        if errors is not None and isinstance(errors, dict):
            error_labels = errors.get('errorLabels')
        super(AutoReconnect, self).__init__(message, error_labels)
        self.errors = self.details = errors or []


class NetworkTimeout(AutoReconnect):
    """An operation on an open connection exceeded socketTimeoutMS.

    The remaining connections in the pool stay open. In the case of a write
    operation, you cannot know whether it succeeded or failed.

    Subclass of :exc:`~pymongo.errors.AutoReconnect`.
    """


def _format_detailed_error(message, details):
    if details is not None:
        message = "%s, full error: %s" % (message, details)
    return message


class NotPrimaryError(AutoReconnect):
    """The server responded "not primary" or "node is recovering".

    These errors result from a query, write, or command. The operation failed
    because the client thought it was using the primary but the primary has
    stepped down, or the client thought it was using a healthy secondary but
    the secondary is stale and trying to recover.

    The client launches a refresh operation on a background thread, to update
    its view of the server as soon as possible after throwing this exception.

    Subclass of :exc:`~pymongo.errors.AutoReconnect`.

    .. versionadded:: 3.12
    """
    def __init__(self, message='', errors=None):
        super(NotPrimaryError, self).__init__(
            _format_detailed_error(message, errors), errors=errors)


class ServerSelectionTimeoutError(AutoReconnect):
    """Thrown when no MongoDB server is available for an operation

    If there is no suitable server for an operation PyMongo tries for
    ``serverSelectionTimeoutMS`` (default 30 seconds) to find one, then
    throws this exception. For example, it is thrown after attempting an
    operation when PyMongo cannot connect to any server, or if you attempt
    an insert into a replica set that has no primary and does not elect one
    within the timeout window, or if you attempt to query with a Read
    Preference that the replica set cannot satisfy.
    """


class ConfigurationError(PyMongoError):
    """Raised when something is incorrectly configured.
    """


class OperationFailure(PyMongoError):
    """Raised when a database operation fails.

    .. versionadded:: 2.7
       The :attr:`details` attribute.
    """

    def __init__(self, error, code=None, details=None, max_wire_version=None):
        error_labels = None
        if details is not None:
            error_labels = details.get('errorLabels')
        super(OperationFailure, self).__init__(
            _format_detailed_error(error, details), error_labels=error_labels)
        self.__code = code
        self.__details = details
        self.__max_wire_version = max_wire_version

    @property
    def _max_wire_version(self):
        return self.__max_wire_version

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


class WriteConcernError(OperationFailure):
    """Base exception type for errors raised due to write concern.

    .. versionadded:: 3.0
    """


class WriteError(OperationFailure):
    """Base exception type for errors raised during write operations.

    .. versionadded:: 3.0
    """


class WTimeoutError(WriteConcernError):
    """Raised when a database operation times out (i.e. wtimeout expires)
    before replication completes.

    With newer versions of MongoDB the `details` attribute may include
    write concern fields like 'n', 'updatedExisting', or 'writtenTo'.

    .. versionadded:: 2.7
    """


class DuplicateKeyError(WriteError):
    """Raised when an insert or update fails due to a duplicate key error."""


class BulkWriteError(OperationFailure):
    """Exception class for bulk write errors.

    .. versionadded:: 2.7
    """
    def __init__(self, results):
        super(BulkWriteError, self).__init__(
            "batch op errors occurred", 65, results)

    def __reduce__(self):
        return self.__class__, (self.details,)


class InvalidOperation(PyMongoError):
    """Raised when a client attempts to perform an invalid operation."""


class InvalidName(PyMongoError):
    """Raised when an invalid name is used."""


class CollectionInvalid(PyMongoError):
    """Raised when collection validation fails."""


class InvalidURI(ConfigurationError):
    """Raised when trying to parse an invalid mongodb URI."""


class DocumentTooLarge(InvalidDocument):
    """Raised when an encoded document is too large for the connected server.
    """
    pass


class EncryptionError(PyMongoError):
    """Raised when encryption or decryption fails.

    This error always wraps another exception which can be retrieved via the
    :attr:`cause` property.

    .. versionadded:: 3.9
    """

    def __init__(self, cause):
        super(EncryptionError, self).__init__(str(cause))
        self.__cause = cause

    @property
    def cause(self):
        """The exception that caused this encryption or decryption error."""
        return self.__cause


class _OperationCancelled(AutoReconnect):
    """Internal error raised when a socket operation is cancelled.
    """
    pass
