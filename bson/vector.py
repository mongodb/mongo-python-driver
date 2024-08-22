from __future__ import annotations

import logging
import struct
from typing import Any, List, Optional, Type

import bson
from bson.binary import Binary

logger = logging.getLogger(__name__)

DTYPE_CODES = {
    "int8": b"\x03",
    "float32": b"\x27",
    "bool": b"\x10",
}

INV_DTYPE_CODES = {ord(v): k for k, v in DTYPE_CODES.items()}

DTYPES_SUPPORTED = {
    "int8": 8,  # signed integers in [128, 127]. 8 bits
    "bool": 1,  # vector of bits received as ints in [0, 255]
    "float32": 4,  #
}


def int_to_bin(value, bit_width=8):
    """Twos-complement representation of binary values.
    Uses value of most-significant-bit (msb) to denote sign.
    0 is positive. 1 is negative
    """
    if value >= 0:
        binary_representation = bin(value)[2:].zfill(bit_width)
    else:
        # Compute two's complement
        binary_representation = bin((1 << bit_width) + value)[2:]
    return binary_representation


class BinaryVector(Binary):
    """

    TODO:
        1. Add bool_
            a. from_list
            b. handle padding
            c. as_list  (first version as list of ints)
        2. Take dtype and padding out of binary payload. Move logic to __init__.py
        3. Turn dtype into enum
        4. Add docs
        5. Add a few simple tests. e.g. For empty and non-sensible inputs
        6. Get to BSON Specs


    TODO:
        - CLASS STRUCTURE
            - Do we want to have a separate class or just add class methods?
            - If we roll into Binary, we bake the dtype into the bytes.
                - We just have to unroll this value when decoding
        - What benefit do we get from subclassing this? ==> Not having to massively change BSON Spec!
            - __eq__? not __hash__ though..
        -[DECODE] bson._get_binary
            - because of _ELEMENT_GETTER[bson_type] -> bson._get_binary, the first line will get called.
                - we then add dtype as 1 byte following that
            - if subtype == 9:  dtype = _UNPACK_DTYPE_FROM(data, position)[0];  position += 1 etc; _UNPACK_DTYPE_FROM = struct.Struct("<B").unpack_from
            - update length by reducing by 1-byte.
            - dtypes are given in gdoc: Technical Design: MongoDB BSON Specification for Vectors
        - [ENCODE] bson._encode_vector?
            - do we need a separate one? or has this already been taken care of in constructor?
            - _encode_binary
                - b"\x05" + name + _PACK_LENGTH_SUBTYPE(len(value), subtype) + value
                = type (len=1) + name:bytes(len=len(name) in bytes?) + len(value) as int32 + subtype as ubyte + value(bytes)
        - QUESTIONS
            - Do we need padding?
            - Handle all 3 types, or just int8?
            -
        ADD
            ** padding
            - __repr__ and __str__ methods
        TEST
            -bson.encode(int8_vector) where vector is BinaryVector
            - bson.encode({"a":int8_vector}) where we encode a doc
    """

    dtype: str

    def __new__(cls, data: Any, dtype: str, padding: int = 0) -> BinaryVector:
        self = Binary.__new__(cls, data, bson.binary.VECTOR_SUBTYPE)
        assert dtype in DTYPES_SUPPORTED
        self.dtype = dtype  # todo - decide if we wish to make private and expose via property
        self.padding = padding
        return self

    def __repr__(self) -> str:
        return f"BinaryVector({bytes.__repr__(self)}, dtype={self.dtype}, padding={self.padding})"

    @classmethod
    def from_list(
        cls: Type[BinaryVector], num_list: List, dtype: str, padding: int = 0
    ) -> BinaryVector:
        """Create a BSON Binary Vector subtype from a list of python objects.

        :param num_list: List of values
        :param dtype: Data type of the values
        :param padding: For fractional bytes, number of bits to ignore at end of vector.
        :return: Binary packed data identified by dtype and padding.
        """
        if dtype == "int8":  # pack ints in [-128, 127] as signed int8
            format_str = "b"
        elif dtype == "bool":  # pack ints in [0, 255] as unsigned uint8
            format_str = "B"
        elif dtype == "float32":  # pack floats as float32
            format_str = "f"
        else:
            raise NotImplementedError("%s not yet supported" % dtype)

        data = struct.pack(f"{len(num_list)}{format_str}", *num_list)
        return cls(data, dtype, padding)

    def as_list(self, dtype: Optional[str] = None, padding: Optional[int] = None) -> List[Any]:
        """Create a list of python objects.

        BinaryVector was created with a specific dtype and padding.
        The optional kwargs allow one to view data in other formats.

        :param dtype: Optional dtype to use instead of self.dtype
        :param padding: Optional number of bytes to discard instead of self.padding
        :return: List of numbers.
        """
        dtype = dtype or self.dtype
        padding = padding or self.padding

        if dtype == "bool":
            n_values = len(self)  # data packed as uint8
            unpacked_uint8s = struct.unpack(f"{n_values}B", self)
            bits = []
            for uint8 in unpacked_uint8s:
                bits.extend([int(bit) for bit in f"{uint8:08b}"])
            return bits[:-padding]

        elif dtype == "int8":
            n_values = len(self)
            dtype_format = "b"
            format_string = f"{n_values}{dtype_format}"
            unpacked_data = struct.unpack(format_string, self)
            return list(unpacked_data)

        elif dtype == "float32":
            n_bytes = len(self)
            n_values = n_bytes // 4
            assert n_bytes % 4 == 0
            unpacked_data = struct.unpack(f"{n_values}f", self)
            return list(unpacked_data)

        else:
            raise NotImplementedError("BinaryVector dtype %i not yet supported" % dtype)
