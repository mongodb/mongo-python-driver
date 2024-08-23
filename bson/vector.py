from __future__ import annotations

import logging
import struct
from enum import Enum
from typing import Any, List, Optional, Type, Union

import bson
from bson.binary import Binary

logger = logging.getLogger(__name__)


class DTYPES(Enum):
    """Datatypes of vector."""

    INT8 = b"\x03"
    FLOAT32 = b"\x27"
    BOOL = b"\x10"


# Map from bytes to enum value, for decoding.
DTYPE_FROM_HEX = {key.value: key for key in DTYPES}


class BinaryVector(Binary):
    """Binary subtype for efficient storage and retrieval of vectors.

    Vectors here refer to densely packed one dimensional arrays of numbers,
    all of the same data type (dtype).
    These types loosely match those of PyArrow and Numpy.

    BinaryVector includes two additional bytes of metadata to the length and subtype
    already prepended in the Binary class.
    One byte defines the data type, described by the bson.binary.vector.DTYPE Enum.
    Another byte declares the number of bits at the end that should be ignored,
    in the case that a vector's length and type do not require a whole number of bytes.
    This number is referred to as padding.
    """

    dtype: str
    padding: int = 0

    def __new__(cls, data: Any, dtype: Union[DTYPES, bytes], padding: int = 0) -> BinaryVector:
        self = Binary.__new__(cls, data, bson.binary.VECTOR_SUBTYPE)
        if isinstance(dtype, bytes):
            dtype = DTYPE_FROM_HEX[dtype]
        assert dtype in DTYPES
        self.dtype = dtype
        self.padding = padding
        return self

    @classmethod
    def from_list(
        cls: Type[BinaryVector], num_list: List, dtype: DTYPES, padding: int = 0
    ) -> BinaryVector:
        """Create a BSON Binary Vector subtype from a list of python objects.

        :param num_list: List of values
        :param dtype: Data type of the values
        :param padding: For fractional bytes, number of bits to ignore at end of vector.
        :return: Binary packed data identified by dtype and padding.
        """
        if dtype == DTYPES.INT8:  # pack ints in [-128, 127] as signed int8
            format_str = "b"
        elif dtype == DTYPES.BOOL:  # pack ints in [0, 255] as unsigned uint8
            format_str = "B"
        elif dtype == DTYPES.FLOAT32:  # pack floats as float32
            format_str = "f"
        else:
            raise NotImplementedError("%s not yet supported" % dtype)

        data = struct.pack(f"{len(num_list)}{format_str}", *num_list)
        return cls(data, dtype, padding)

    def as_list(self, dtype: Optional[DTYPES] = None, padding: Optional[int] = None) -> List[Any]:
        """Create a list of python objects.

        BinaryVector was created with a specific dtype and padding.
        The optional kwargs allow one to view data in other formats.

        :param dtype: Optional dtype to use instead of self.dtype
        :param padding: Optional number of bytes to discard instead of self.padding
        :return: List of numbers.
        """
        dtype = dtype or self.dtype
        padding = padding or self.padding

        if dtype == DTYPES.BOOL:
            n_values = len(self)  # data packed as uint8
            unpacked_uint8s = struct.unpack(f"{n_values}B", self)
            bits = []
            for uint8 in unpacked_uint8s:
                bits.extend([int(bit) for bit in f"{uint8:08b}"])
            return bits[:-padding]

        elif dtype == DTYPES.INT8:
            n_values = len(self)
            dtype_format = "b"
            format_string = f"{n_values}{dtype_format}"
            unpacked_data = struct.unpack(format_string, self)
            return list(unpacked_data)

        elif dtype == DTYPES.FLOAT32:
            n_bytes = len(self)
            n_values = n_bytes // 4
            assert n_bytes % 4 == 0
            unpacked_data = struct.unpack(f"{n_values}f", self)
            return list(unpacked_data)

        else:
            raise NotImplementedError("BinaryVector dtype %s not yet supported" % dtype.name)

    def __repr__(self) -> str:
        return f"BinaryVector({bytes.__repr__(self)}, dtype={self.dtype}, padding={self.padding})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, BinaryVector):
            return (
                self.subtype == other.subtype
                and self.dtype == other.dtype
                and self.padding == other.padding
                and bytes(self) == bytes(other)
            )
        return False

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return super().__hash__() ^ hash(self.dtype) ^ hash(self.padding)
