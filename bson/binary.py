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
from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, Union
from uuid import UUID

"""Tools for representing BSON binary data.
"""

BINARY_SUBTYPE = 0
"""BSON binary subtype for binary data.

This is the default subtype for binary data.
"""

FUNCTION_SUBTYPE = 1
"""BSON binary subtype for functions.
"""

OLD_BINARY_SUBTYPE = 2
"""Old BSON binary subtype for binary data.

This is the old default subtype, the current
default is :data:`BINARY_SUBTYPE`.
"""

OLD_UUID_SUBTYPE = 3
"""Old BSON binary subtype for a UUID.

:class:`uuid.UUID` instances will automatically be encoded
by :mod:`bson` using this subtype when using
:data:`UuidRepresentation.PYTHON_LEGACY`,
:data:`UuidRepresentation.JAVA_LEGACY`, or
:data:`UuidRepresentation.CSHARP_LEGACY`.

.. versionadded:: 2.1
"""

UUID_SUBTYPE = 4
"""BSON binary subtype for a UUID.

This is the standard BSON binary subtype for UUIDs.
:class:`uuid.UUID` instances will automatically be encoded
by :mod:`bson` using this subtype when using
:data:`UuidRepresentation.STANDARD`.
"""


if TYPE_CHECKING:
    from array import array as _array
    from mmap import mmap as _mmap


class UuidRepresentation:
    UNSPECIFIED = 0
    """An unspecified UUID representation.

    When configured, :class:`uuid.UUID` instances will **not** be
    automatically encoded to or decoded from :class:`~bson.binary.Binary`.
    When encoding a :class:`uuid.UUID` instance, an error will be raised.
    To encode a :class:`uuid.UUID` instance with this configuration, it must
    be wrapped in the :class:`~bson.binary.Binary` class by the application
    code. When decoding a BSON binary field with a UUID subtype, a
    :class:`~bson.binary.Binary` instance will be returned instead of a
    :class:`uuid.UUID` instance.

    See :ref:`unspecified-representation-details` for details.

    .. versionadded:: 3.11
    """

    STANDARD = UUID_SUBTYPE
    """The standard UUID representation.

    :class:`uuid.UUID` instances will automatically be encoded to
    and decoded from BSON binary, using RFC-4122 byte order with
    binary subtype :data:`UUID_SUBTYPE`.

    See :ref:`standard-representation-details` for details.

    .. versionadded:: 3.11
    """

    PYTHON_LEGACY = OLD_UUID_SUBTYPE
    """The Python legacy UUID representation.

    :class:`uuid.UUID` instances will automatically be encoded to
    and decoded from BSON binary, using RFC-4122 byte order with
    binary subtype :data:`OLD_UUID_SUBTYPE`.

    See :ref:`python-legacy-representation-details` for details.

    .. versionadded:: 3.11
    """

    JAVA_LEGACY = 5
    """The Java legacy UUID representation.

    :class:`uuid.UUID` instances will automatically be encoded to
    and decoded from BSON binary subtype :data:`OLD_UUID_SUBTYPE`,
    using the Java driver's legacy byte order.

    See :ref:`java-legacy-representation-details` for details.

    .. versionadded:: 3.11
    """

    CSHARP_LEGACY = 6
    """The C#/.net legacy UUID representation.

    :class:`uuid.UUID` instances will automatically be encoded to
    and decoded from BSON binary subtype :data:`OLD_UUID_SUBTYPE`,
    using the C# driver's legacy byte order.

    See :ref:`csharp-legacy-representation-details` for details.

    .. versionadded:: 3.11
    """


STANDARD = UuidRepresentation.STANDARD
"""An alias for :data:`UuidRepresentation.STANDARD`.

.. versionadded:: 3.0
"""

PYTHON_LEGACY = UuidRepresentation.PYTHON_LEGACY
"""An alias for :data:`UuidRepresentation.PYTHON_LEGACY`.

.. versionadded:: 3.0
"""

JAVA_LEGACY = UuidRepresentation.JAVA_LEGACY
"""An alias for :data:`UuidRepresentation.JAVA_LEGACY`.

.. versionchanged:: 3.6
   BSON binary subtype 4 is decoded using RFC-4122 byte order.
.. versionadded:: 2.3
"""

CSHARP_LEGACY = UuidRepresentation.CSHARP_LEGACY
"""An alias for :data:`UuidRepresentation.CSHARP_LEGACY`.

.. versionchanged:: 3.6
   BSON binary subtype 4 is decoded using RFC-4122 byte order.
.. versionadded:: 2.3
"""

ALL_UUID_SUBTYPES = (OLD_UUID_SUBTYPE, UUID_SUBTYPE)
ALL_UUID_REPRESENTATIONS = (
    UuidRepresentation.UNSPECIFIED,
    UuidRepresentation.STANDARD,
    UuidRepresentation.PYTHON_LEGACY,
    UuidRepresentation.JAVA_LEGACY,
    UuidRepresentation.CSHARP_LEGACY,
)
UUID_REPRESENTATION_NAMES = {
    UuidRepresentation.UNSPECIFIED: "UuidRepresentation.UNSPECIFIED",
    UuidRepresentation.STANDARD: "UuidRepresentation.STANDARD",
    UuidRepresentation.PYTHON_LEGACY: "UuidRepresentation.PYTHON_LEGACY",
    UuidRepresentation.JAVA_LEGACY: "UuidRepresentation.JAVA_LEGACY",
    UuidRepresentation.CSHARP_LEGACY: "UuidRepresentation.CSHARP_LEGACY",
}

MD5_SUBTYPE = 5
"""BSON binary subtype for an MD5 hash.
"""

COLUMN_SUBTYPE = 7
"""BSON binary subtype for columns.

.. versionadded:: 4.0
"""

SENSITIVE_SUBTYPE = 8
"""BSON binary subtype for sensitive data.

.. versionadded:: 4.5
"""


VECTOR_SUBTYPE = 9
"""BSON binary subtype for densely packed vector data.

.. versionadded:: 4.9
"""


USER_DEFINED_SUBTYPE = 128
"""BSON binary subtype for any user defined structure.
"""


class BinaryVectorDtype(Enum):
    """Datatypes of vector subtype.

    The PACKED_BIT value represents a special case where vector values themselves
    can only hold two values (0 or 1) but these are packed together into groups of 8,
    a byte. In Python, these are displayed as ints in range(0,128).
    """

    INT8 = b"\x03"
    FLOAT32 = b"\x27"
    PACKED_BIT = b"\x10"


# Map from bytes to enum value, for decoding.
DTYPE_FROM_HEX = {key.value: key for key in BinaryVectorDtype}


@dataclass
class BinaryVector:
    """Vector of numbers along with metadata for binary interoperability.

    dtype specifies the data type stored in binary.
    padding specifies the number of bits in the final byte that are to be ignored
    when a vector element's size is less than a byte
    and the length of the vector is not a multiple of 8."""

    data: list[float | int]
    dtype: BinaryVectorDtype
    padding: Optional[int] = 0


class Binary(bytes):
    """Representation of BSON binary data.

    # TODO Add Vector subtype description

    This is necessary because we want to represent Python strings as
    the BSON string type. We need to wrap binary data so we can tell
    the difference between what should be considered binary data and
    what should be considered a string when we encode to BSON.

    Raises TypeError if subtype` is not an instance of :class:`int`.
    Raises ValueError if `subtype` is not in [0, 256).

    .. note::
      Instances of Binary with subtype 0 will be decoded directly to :class:`bytes`.

    :param data: the binary data to represent. Can be any bytes-like type
        that implements the buffer protocol.
    :param subtype: the `binary subtype
        <https://bsonspec.org/spec.html>`_
        to use

    .. versionchanged:: 3.9
      Support any bytes-like type that implements the buffer protocol.
    """

    _type_marker = 5
    __subtype: int

    def __new__(
        cls: Type[Binary],
        data: Union[memoryview, bytes, _mmap, _array[Any]],
        subtype: int = BINARY_SUBTYPE,
    ) -> Binary:
        if not isinstance(subtype, int):
            raise TypeError("subtype must be an instance of int")
        if subtype >= 256 or subtype < 0:
            raise ValueError("subtype must be contained in [0, 256)")
        # Support any type that implements the buffer protocol.
        self = bytes.__new__(cls, memoryview(data).tobytes())
        self.__subtype = subtype
        return self

    @classmethod
    def from_uuid(
        cls: Type[Binary], uuid: UUID, uuid_representation: int = UuidRepresentation.STANDARD
    ) -> Binary:
        """Create a BSON Binary object from a Python UUID.

        Creates a :class:`~bson.binary.Binary` object from a
        :class:`uuid.UUID` instance. Assumes that the native
        :class:`uuid.UUID` instance uses the byte-order implied by the
        provided ``uuid_representation``.

        Raises :exc:`TypeError` if `uuid` is not an instance of
        :class:`~uuid.UUID`.

        :param uuid: A :class:`uuid.UUID` instance.
        :param uuid_representation: A member of
            :class:`~bson.binary.UuidRepresentation`. Default:
            :const:`~bson.binary.UuidRepresentation.STANDARD`.
            See :ref:`handling-uuid-data-example` for details.

        .. versionadded:: 3.11
        """
        if not isinstance(uuid, UUID):
            raise TypeError("uuid must be an instance of uuid.UUID")

        if uuid_representation not in ALL_UUID_REPRESENTATIONS:
            raise ValueError(
                "uuid_representation must be a value from bson.binary.UuidRepresentation"
            )

        if uuid_representation == UuidRepresentation.UNSPECIFIED:
            raise ValueError(
                "cannot encode native uuid.UUID with "
                "UuidRepresentation.UNSPECIFIED. UUIDs can be manually "
                "converted to bson.Binary instances using "
                "bson.Binary.from_uuid() or a different UuidRepresentation "
                "can be configured. See the documentation for "
                "UuidRepresentation for more information."
            )

        subtype = OLD_UUID_SUBTYPE
        if uuid_representation == UuidRepresentation.PYTHON_LEGACY:
            payload = uuid.bytes
        elif uuid_representation == UuidRepresentation.JAVA_LEGACY:
            from_uuid = uuid.bytes
            payload = from_uuid[0:8][::-1] + from_uuid[8:16][::-1]
        elif uuid_representation == UuidRepresentation.CSHARP_LEGACY:
            payload = uuid.bytes_le
        else:
            # uuid_representation == UuidRepresentation.STANDARD
            subtype = UUID_SUBTYPE
            payload = uuid.bytes

        return cls(payload, subtype)

    def as_uuid(self, uuid_representation: int = UuidRepresentation.STANDARD) -> UUID:
        """Create a Python UUID from this BSON Binary object.

        Decodes this binary object as a native :class:`uuid.UUID` instance
        with the provided ``uuid_representation``.

        Raises :exc:`ValueError` if this :class:`~bson.binary.Binary` instance
        does not contain a UUID.

        :param uuid_representation: A member of
            :class:`~bson.binary.UuidRepresentation`. Default:
            :const:`~bson.binary.UuidRepresentation.STANDARD`.
            See :ref:`handling-uuid-data-example` for details.

        .. versionadded:: 3.11
        """
        if self.subtype not in ALL_UUID_SUBTYPES:
            raise ValueError(f"cannot decode subtype {self.subtype} as a uuid")

        if uuid_representation not in ALL_UUID_REPRESENTATIONS:
            raise ValueError(
                "uuid_representation must be a value from bson.binary.UuidRepresentation"
            )

        if uuid_representation == UuidRepresentation.UNSPECIFIED:
            raise ValueError("uuid_representation cannot be UNSPECIFIED")
        elif uuid_representation == UuidRepresentation.PYTHON_LEGACY:
            if self.subtype == OLD_UUID_SUBTYPE:
                return UUID(bytes=self)
        elif uuid_representation == UuidRepresentation.JAVA_LEGACY:
            if self.subtype == OLD_UUID_SUBTYPE:
                return UUID(bytes=self[0:8][::-1] + self[8:16][::-1])
        elif uuid_representation == UuidRepresentation.CSHARP_LEGACY:
            if self.subtype == OLD_UUID_SUBTYPE:
                return UUID(bytes_le=self)
        else:
            # uuid_representation == UuidRepresentation.STANDARD
            if self.subtype == UUID_SUBTYPE:
                return UUID(bytes=self)

        raise ValueError(
            f"cannot decode subtype {self.subtype} to {UUID_REPRESENTATION_NAMES[uuid_representation]}"
        )

    @classmethod
    def from_vector(
        cls: Type[Binary],
        vector: list[int, float],
        dtype: BinaryVectorDtype,
        padding: Optional[int] = 0,
    ) -> Binary:
        """Create a BSON Binary Vector subtype from a list of python objects.

        The data type and byte padding are prepended to the vector itself.

        :param vector: List of values
        :param dtype: Data type of the values
        :param padding: For fractional bytes, number of bits to ignore at end of vector.
        :return: Binary packed data identified by dtype and padding.
        """
        if dtype == BinaryVectorDtype.INT8:  # pack ints in [-128, 127] as signed int8
            format_str = "b"
        elif dtype == BinaryVectorDtype.PACKED_BIT:  # pack ints in [0, 255] as unsigned uint8
            format_str = "B"
        elif dtype == BinaryVectorDtype.FLOAT32:  # pack floats as float32
            format_str = "f"
        else:
            raise NotImplementedError("%s not yet supported" % dtype)

        metadata = struct.pack("<sB", dtype.value, padding)
        data = struct.pack(f"{len(vector)}{format_str}", *vector)
        return cls(metadata + data, subtype=VECTOR_SUBTYPE)

    def as_vector(
        self, dtype: Optional[BinaryVectorDtype] = None, padding: Optional[int] = 0
    ) -> BinaryVector:
        """Create a list of python objects.

        The binary representation was created with a specific dtype and padding.
        The optional kwargs allow one to view data in other formats,
        which is particularly useful when one wishes to see binary/bit vectors
        as INT8, hence with their proper lengths.

        :param dtype: Optional dtype to use instead of self.dtype
        :param padding: Optional number of bytes to discard instead of self.padding
        :return: List of numbers.
        """

        position = 0
        orig_dtype, orig_padding = struct.unpack_from("<sB", self, position)
        position += 2
        orig_dtype = BinaryVectorDtype(orig_dtype)
        dtype = dtype or orig_dtype
        padding = padding or orig_padding
        n_values = len(self) - position

        if dtype == BinaryVectorDtype.PACKED_BIT:
            # data packed as uint8
            dtype_format = "B"
            unpacked_uint8s = struct.unpack_from(f"{n_values}{dtype_format}", self, position)
            return BinaryVector(list(unpacked_uint8s), dtype, padding)

        elif dtype == BinaryVectorDtype.INT8:
            if orig_dtype == BinaryVectorDtype.PACKED_BIT:
                # Special case for when wishes to see UNPACKED embedding
                unpacked_uint8s = struct.unpack_from(f"{n_values}B", self, position)
                bits = []
                for uint8 in unpacked_uint8s:
                    bits.extend([int(bit) for bit in f"{uint8:08b}"])
                if padding and padding > 0:
                    unpacked_data = bits[:-padding]
                else:
                    unpacked_data = bits
                return BinaryVector(list(unpacked_data), dtype, padding)

            else:
                dtype_format = "b"
                format_string = f"{n_values}{dtype_format}"
                unpacked_data = struct.unpack_from(format_string, self, position)
                return BinaryVector(list(unpacked_data), dtype, padding)

        elif dtype == BinaryVectorDtype.FLOAT32:
            n_bytes = len(self) - position
            n_values = n_bytes // 4
            assert n_bytes % 4 == 0
            unpacked_data = struct.unpack_from(f"{n_values}f", self, position)
            return BinaryVector(list(unpacked_data), dtype, padding)
        else:
            raise NotImplementedError("Binary Vector dtype %s not yet supported" % dtype.name)

    @property
    def subtype(self) -> int:
        """Subtype of this binary data."""
        return self.__subtype

    def __getnewargs__(self) -> Tuple[bytes, int]:  # type: ignore[override]
        # Work around http://bugs.python.org/issue7382
        data = super().__getnewargs__()[0]
        if not isinstance(data, bytes):
            data = data.encode("latin-1")
        return data, self.__subtype

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Binary):
            return (self.__subtype, bytes(self)) == (other.subtype, bytes(other))
        # We don't return NotImplemented here because if we did then
        # Binary("foo") == "foo" would return True, since Binary is a
        # subclass of str...
        return False

    def __hash__(self) -> int:
        return super().__hash__() ^ hash(self.__subtype)

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __repr__(self) -> str:
        if self.__subtype == SENSITIVE_SUBTYPE:
            return f"<Binary(REDACTED, {self.__subtype})>"
        else:
            return f"Binary({bytes.__repr__(self)}, {self.__subtype})"
