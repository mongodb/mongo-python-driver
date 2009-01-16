"""Exceptions raised by the Mongo driver."""

class ConnectionFailure(IOError):
    """Raised when a connection to the database cannot be made or is lost.
    """

class OperationFailure(Exception):
    """Raised when a database operation fails.
    """

class InvalidOperation(Exception):
    """Raised when a client attempts to perform an invalid operation.
    """

class InvalidName(ValueError):
    """Raised when an invalid name is used.
    """

class InvalidBSON(ValueError):
    """Raised when trying to create a BSON object from invalid data.
    """

class InvalidDocument(ValueError):
    """Raised when trying to create a BSON object from an invalid document.
    """

class UnsupportedTag(ValueError):
    """Raised when trying to parse an unsupported tag in an XML document.
    """

class InvalidId(ValueError):
    """Raised when trying to create an ObjectId from invalid data.
    """
